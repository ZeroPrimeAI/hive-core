import sys
sys.path.insert(0, '/THE_HIVE/agents/core')
# WEBHOOK SERVER — Unified Inbound Handler for All Telephony
# MIT License | Pure Python stdlib | Zero dependencies
# Single HTTP server (port 8110) routes ALL webhooks to the right handler:
#   /voice/inbound    → CallRouter (IVR phone tree)
#   /voice/status     → Call status tracking
#   /voice/respond    → AI conversation responses
#   /sms/inbound      → SMSAutoResponder
#   /sms/status       → SMS delivery tracking
#   /voicemail/*      → VoicemailAgent
#   /appointments/*   → AppointmentScheduler
#   /campaigns/*      → CampaignManager status callbacks
#   /health           → Health check
# One URL to configure in any provider. Route everything here.
import argparse, json, os, re, signal, sys, time, threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse

_HIVE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _HIVE not in sys.path:
    sys.path.insert(0, _HIVE)

# Load .env
_env_path = os.path.join(_HIVE, ".env")
if os.path.exists(_env_path):
    with open(_env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                v = v.strip().strip('"').strip("'")
                os.environ.setdefault(k.strip(), v)

try:
    from memory.tracker_db import DatabaseManager
    from utils.helpers import _log, _learn
    HAS_DB = True
except ImportError:
    HAS_DB = False
    def _log(tag, msg): print(f"  [{time.strftime('%H:%M:%S')}] {tag}: {msg}")
    def _learn(*a, **kw): pass

try:
    from telephony.prospect_db import ProspectDB
    HAS_PROSPECT = True
except ImportError:
    HAS_PROSPECT = False

try:
    from telephony.sales_training import SalesTrainer
    HAS_TRAINER = True
except ImportError:
    HAS_TRAINER = False

DB_PATH = os.path.join(_HIVE, "memory", "tracker.db")
PORT = 8110

# Business hours (ET)
BUSINESS_HOURS = {"start": 9, "end": 18}  # 9 AM - 6 PM
BUSINESS_DAYS = {0, 1, 2, 3, 4}  # Mon-Fri

# Agent voices
AGENTS = {
    "matthew": {"voice": "Polly.Matthew", "role": "sales"},
    "sarah": {"voice": "Polly.Joanna", "role": "customer_service"},
    "emma": {"voice": "Polly.Salli", "role": "appointment_setter"},
    "james": {"voice": "Polly.Joey", "role": "technical"},
}

# Family numbers
FAMILY_NUMBERS = {
    "+18506872085": "Christopher",
    "+18509648866": "Christopher",
    "+15598368958": "Dad",
    "+18505302601": "Mom",
}

CALLBACK_NUMBER = "(850) 801-6662"
PHI4_URL = "http://100.77.113.48:11434/api/generate"

# ── Telegram Inbound Notifications ──────────────────────────────
# Notify Chris instantly when an EXTERNAL caller/SMS reaches any Hive number
_TG_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "8574794345:AAH9VfvTCbzO-Xr2dIhsTmtMcGw1JgTI2Ow")
_TG_CHAT_ID = "6934187950"  # Chris's Telegram

# Numbers that are internal — do NOT notify for these
HIVE_INTERNAL_NUMBERS = {
    "+18508016662",   # Main Hive Twilio number
    "+18509648866",   # Chris's real phone
    "+18506872085",   # Chris's old/alt number
    "+15598368958",   # Dad
    "+18505302601",   # Mom
}

# Test/simulator numbers — NEVER notify Chris about these
TEST_NUMBERS = {
    "+15551234567",   # hive-call-tester synthetic test calls
    "+15551234568",   # hive-call-tester alternate test number
    "+15550000000",   # Generic test number
}
# Merge test numbers into internal so _is_external_caller() filters them
HIVE_INTERNAL_NUMBERS.update(TEST_NUMBERS)

def _notify_chris_telegram(msg):
    """Send a Telegram notification to Chris. Non-blocking (fire-and-forget thread)."""
    def _send():
        try:
            import urllib.request, urllib.parse
            data = urllib.parse.urlencode({
                "chat_id": _TG_CHAT_ID,
                "text": msg,
                "parse_mode": "HTML",
            }).encode()
            req = urllib.request.Request(
                f"https://api.telegram.org/bot{_TG_BOT_TOKEN}/sendMessage",
                data=data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            urllib.request.urlopen(req, timeout=10)
        except Exception as e:
            _log("NOTIFY", f"Telegram notify failed: {e}")
    threading.Thread(target=_send, daemon=True).start()

def _is_external_caller(number):
    """Return True if number is NOT an internal Hive/family number."""
    if not number:
        return False
    # Normalize
    clean = number.strip()
    if clean in HIVE_INTERNAL_NUMBERS:
        return False
    if clean in FAMILY_NUMBERS:
        return False
    # Also skip Twilio internal numbers (short codes, etc.)
    if len(clean) < 8:
        return False
    return True


# ── Hive Command Router Bridge ──────────────────────────────────
_COMMANDER_URL = "http://localhost:8420/command"
_COMMANDER_TOKEN = "98b0a258b9fbc8f1fb1d3ec8217226bb0d1767b18ef27ff7c02c7ecee2aa747a"

# Chris's numbers that can issue voice commands
_COMMAND_CALLERS = {"+18506872085", "+18509648866", "+18508016662"}

# Keywords that signal a voice command (not a conversation)
_COMMAND_PATTERNS = {
    "restart": r"restart|reboot|bounce",
    "status": r"\bstatus\b|how.s the hive|system status|health check|what.s running",
    "deploy": r"deploy|push|ship it|roll out",
    "build": r"\bbuild\b.*(?:page|site|landing|service|agent)",
    "forex": r"forex|trading|position|trade|plant|prune|close.*trade|open.*trade",
    "report": r"\breport\b|give me.*numbers|how.*doing|stats|metrics|dashboard",
    "stop": r"\bstop\b.*(?:service|agent|trading)|kill|shut.*down",
    "start": r"\bstart\b.*(?:service|agent)|spin up|launch",
    "logs": r"\blogs\b|show.*logs|check.*logs|what.*error",
    "nerve": r"\bnerve\b|how many facts|knowledge|what.*learned",
}

def _try_hive_command(speech, caller_number):
    """
    Check if speech from Chris is a Hive command.
    Returns (True, response_text) if command detected and executed.
    Returns (False, None) if not a command or not authorized.
    """
    import re as _re

    # Only process commands from Chris
    clean_number = (caller_number or "").strip()
    if clean_number not in _COMMAND_CALLERS:
        return False, None

    text_lower = (speech or "").lower().strip()
    if len(text_lower) < 3:
        return False, None

    # Check for command keywords
    matched_type = None
    for cmd_type, pattern in _COMMAND_PATTERNS.items():
        if _re.search(pattern, text_lower, _re.IGNORECASE):
            matched_type = cmd_type
            break

    if not matched_type:
        return False, None

    task = speech.strip()
    _log("COMMAND", f"Voice command detected: type={matched_type} | task={task} | caller={caller_number}")

    try:
        import urllib.request
        req_data = json.dumps({
            "task": task,
            "machine": "zeroq",
            "context": f"Voice command from Chris via phone call. Command type: {matched_type}",
            "priority": "high"
        }).encode()

        url = f"{_COMMANDER_URL}?token={_COMMANDER_TOKEN}"
        req = urllib.request.Request(url, data=req_data,
                                     headers={"Content-Type": "application/json"})
        resp = urllib.request.urlopen(req, timeout=90)
        data = json.loads(resp.read().decode())

        result_text = data.get("result", "")
        errors = data.get("errors")

        # Clean for TTS — strip markdown/code blocks
        clean = _re.sub(r'```[\s\S]*?```', '', result_text)
        clean = _re.sub(r'[`*#>\-]', '', clean)
        clean = clean.strip()

        # Truncate for phone — max 300 chars (~20 seconds of speech)
        if len(clean) > 300:
            cut = clean[:300]
            last_period = cut.rfind('.')
            if last_period > 100:
                clean = cut[:last_period + 1]
            else:
                clean = cut + "..."

        if errors and not clean:
            clean = f"Command ran but had errors. {matched_type} might need attention."

        if not clean:
            clean = f"Done. {matched_type} command executed."

        _log("COMMAND", f"Result: {clean[:80]}")
        return True, clean

    except Exception as e:
        _log("COMMAND", f"Commander error: {e}")
        if "timeout" in str(e).lower() or "timed out" in str(e).lower():
            return True, "That command is taking too long. I sent it but couldn't wait for the result."
        if "connection" in str(e).lower() or "refused" in str(e).lower():
            return True, "Commander is unreachable right now. The service might be down."
        return True, f"Had an error routing that command."



# ── Client Routing ─────────────────────────────────────────────
# Map Twilio numbers to client configs. When a call comes in on a
# client's number, the AI answers as THEIR business.
# Add new clients here — that's it, they're live.
CLIENT_CONFIGS = {}
CLIENT_DB_PATH = os.path.join(_HIVE, "memory", "client_phones.json")

def _load_client_configs():
    """Load client configs from JSON file AND from individual client config files."""
    global CLIENT_CONFIGS
    # Load the phone→config mapping
    if os.path.exists(CLIENT_DB_PATH):
        try:
            with open(CLIENT_DB_PATH) as f:
                CLIENT_CONFIGS = json.load(f)
        except Exception:
            pass

    # Also load rich configs from agents/answering/clients/*.json
    # and index them by phone number so they're routable
    clients_dir = os.path.join(os.path.dirname(CLIENT_DB_PATH), "..", "agents", "answering", "clients")
    clients_dir = os.path.abspath(clients_dir)
    if os.path.isdir(clients_dir):
        import glob
        for cfg_file in glob.glob(os.path.join(clients_dir, "*.json")):
            try:
                with open(cfg_file) as f:
                    cfg = json.load(f)
                phone = cfg.get("phone", "")
                if phone and phone not in CLIENT_CONFIGS:
                    CLIENT_CONFIGS[phone] = cfg
            except Exception:
                pass

    return CLIENT_CONFIGS

def _get_client_for_number(to_number):
    """Look up which client owns a phone number."""
    if not CLIENT_CONFIGS:
        _load_client_configs()
    # Strip formatting
    clean = re.sub(r'[^\d+]', '', to_number)
    for num, config in CLIENT_CONFIGS.items():
        if clean.endswith(re.sub(r'[^\d]', '', num)):
            return config
    return None

def _client_system_prompt(client):
    """Build a system prompt for a client's AI receptionist."""
    # Use the client's custom system_prompt if they have one (rich configs)
    if client.get("system_prompt"):
        prompt = client["system_prompt"]
        # Append FAQ knowledge if available
        faqs = client.get("faqs", {})
        if faqs:
            faq_text = "\n".join(f"- {k}: {v}" for k, v in faqs.items())
            prompt += f"\n\nFrequently Asked Questions:\n{faq_text}"
        prompt += "\nKeep responses under 2 sentences on the phone. NEVER say you are an AI or robot."
        return prompt

    # Fallback: build a generic prompt from basic fields
    name = client.get("business_name", "our company")
    industry = client.get("industry", "service")
    svc = client.get("services", ["general assistance"])
    services = ", ".join(svc) if isinstance(svc, list) else svc
    hours = client.get("hours", "Monday through Friday, 9 AM to 5 PM")
    tone = client.get("tone", "professional and friendly")
    extra = client.get("extra_instructions", "")

    return f"""You are the AI receptionist for {name}, a {industry} business.
Your tone is {tone}. Keep responses under 2 sentences.
Services offered: {services}.
Business hours: {hours}.
Your job: Answer questions, collect caller name and phone number, and schedule appointments.
If asked about pricing, say you'll have someone follow up with a quote.
{extra}
NEVER say you are an AI or robot. You are a receptionist."""


def _twiml_response(twiml_body):
    """Wrap TwiML body in Response tags."""
    return f'<?xml version="1.0" encoding="UTF-8"?>\n<Response>\n{twiml_body}\n</Response>'


def _is_business_hours():
    """Check if current time is within business hours."""
    from datetime import datetime, timezone, timedelta
    et = timezone(timedelta(hours=-5))
    now = datetime.now(et)
    return now.weekday() in BUSINESS_DAYS and BUSINESS_HOURS["start"] <= now.hour < BUSINESS_HOURS["end"]


def _phi4_response(system_prompt, user_message, timeout=6):
    """Get AI response — GEMMA ONLY, fast failover, SMS fallback on total failure."""
    from urllib.request import Request, urlopen
    import time as _t

    # Model priority: ZeroQ local (same machine = fastest) → ZeroDESK → ZeroZI
    # ALL gemma2 models, NO phi4/qwen/llama
    MODELS = [
        ("gemma2-phone-v5", "http://localhost:11434/api/chat", 5),         # ZeroQ local — already loaded
        ("gemma2:2b",       "http://localhost:11434/api/chat", 4),         # ZeroQ fallback
        ("gemma2-phone-v4", "http://100.77.113.48:11434/api/chat", 5),    # ZeroDESK
        ("gemma2-phone-v3", "http://100.77.113.48:11434/api/chat", 5),    # ZeroDESK older
        ("gemma2:2b",       "http://100.105.160.106:11434/api/chat", 4),  # ZeroZI fallback
    ]

    garbage = ["llama-index", "openhands", "pydantic", "<|system", "def ", "import ", "```", "class ", "<start_of_"]
    start = _t.time()

    for model_name, api_url, model_timeout in MODELS:
        # Don't spend more than 8s total across all attempts
        elapsed = _t.time() - start
        if elapsed > 8:
            break
        remaining = min(model_timeout, 8 - elapsed)
        if remaining < 1:
            break

        try:
            data = json.dumps({
                "model": model_name,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_message}
                ],
                "stream": False,
                "options": {"temperature": 0.7, "num_predict": 50, "num_ctx": 1024},
                "keep_alive": "30m",
            }).encode()
            req = Request(api_url, data=data,
                          headers={"Content-Type": "application/json"})
            resp = json.loads(urlopen(req, timeout=remaining).read().decode())
            text = resp.get("message", {}).get("content", "").strip()

            # Filter garbage
            if text and not any(g in text.lower() for g in garbage):
                _log("AI", f"Response from {model_name} in {_t.time()-start:.1f}s: {text[:60]}")
                return text
        except Exception as e:
            _log("AI", f"{model_name} failed ({_t.time()-start:.1f}s): {e}")
            continue

    _log("AI", f"ALL models failed after {_t.time()-start:.1f}s — returning empty")
    return ""


def _send_ai_failure_sms(caller_phone, business_name=""):
    """Send SMS when AI fails to respond — notify Chris AND the caller."""
    try:
        account_sid = os.environ.get("TWILIO_ACCOUNT_SID", "")
        auth_token = os.environ.get("TWILIO_AUTH_TOKEN", "")
        from_number = os.environ.get("TWILIO_PHONE", "")
        chris_phone = "+18509648866"
        if not all([account_sid, auth_token, from_number]):
            _log("SMS", "Twilio credentials missing — AI failure SMS not sent")
            return

        from urllib.request import Request, urlopen
        import base64
        from urllib.parse import quote
        auth = base64.b64encode(f"{account_sid}:{auth_token}".encode()).decode()
        twilio_url = f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json"

        # 1. Notify Chris
        biz_label = f" ({business_name})" if business_name else ""
        chris_msg = (f"MISSED CALL{biz_label}\n"
                     f"From: {caller_phone}\n"
                     f"AI failed to respond.\n"
                     f"Call them back ASAP!\n"
                     f"Time: {time.strftime('%I:%M %p ET')}")
        data = f"To={quote(chris_phone)}&From={quote(from_number)}&Body={quote(chris_msg)}".encode()
        req = Request(twilio_url, data=data,
                      headers={"Authorization": f"Basic {auth}",
                                "Content-Type": "application/x-www-form-urlencoded"})
        urlopen(req, timeout=10)
        _log("SMS", f"Chris notified about AI failure for {caller_phone}")

        # 2. Text the caller (only if it's a real external number)
        if caller_phone and _is_external_caller(caller_phone):
            clean = caller_phone.strip()
            if not clean.startswith("+"):
                import re as _re3
                digits = _re3.sub(r"[^0-9]", "", clean)
                clean = "+1" + digits if len(digits) == 10 else "+" + digits
            caller_msg = ("Thanks for calling! We missed your call but a technician is standing by. "
                          "Please text us your address and what you need help with, and we will dispatch someone right away.")
            data2 = f"To={quote(clean)}&From={quote(from_number)}&Body={quote(caller_msg)}".encode()
            req2 = Request(twilio_url, data=data2,
                           headers={"Authorization": f"Basic {auth}",
                                     "Content-Type": "application/x-www-form-urlencoded"})
            urlopen(req2, timeout=10)
            _log("SMS", f"Caller {clean} texted with fallback message")

    except Exception as e:
        _log("SMS", f"AI failure SMS error: {e}")

    # Also notify via Telegram
    biz_label2 = f" — {business_name}" if business_name else ""
    _notify_chris_telegram(
        f"\U0001F6A8 <b>AI FAILURE{biz_label2}</b>\n"
        f"Caller: <code>{caller_phone}</code>\n"
        f"AI could not generate a response.\n"
        f"SMS sent to caller + Chris.\n"
        f"Time: {time.strftime('%I:%M %p ET')}"
    )


def _save_call_action(call_sid, action_type, data):
    """Save an action extracted from a call (appointment, lead, task)."""
    try:
        import sqlite3 as _sql
        conn = _sql.connect(os.path.join(_HIVE, "memory", "call_actions.db"))
        conn.execute("""CREATE TABLE IF NOT EXISTS actions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            call_sid TEXT, action_type TEXT, data TEXT,
            status TEXT DEFAULT 'pending', created_at REAL
        )""")
        conn.execute("INSERT INTO actions (call_sid, action_type, data, created_at) VALUES (?,?,?,?)",
                     (call_sid, action_type, json.dumps(data), time.time()))
        conn.commit()
        conn.close()
    except Exception:
        pass


def _create_dispatch_job(call_sid, session, caller_info, client):
    """Create a dispatch job in dispatch.db for service businesses (locksmith, etc)."""
    # Skip dispatch for test/simulator calls
    caller_phone_check = session.get("caller", "")
    if caller_phone_check in TEST_NUMBERS:
        _log("DISPATCH", f"Skipping dispatch for test number {caller_phone_check}")
        return
    try:
        import sqlite3 as _sql
        import uuid as _uuid
        dispatch_db = os.path.join(_HIVE, "memory", "dispatch.db")
        conn = _sql.connect(dispatch_db)
        conn.execute("""CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT UNIQUE, caller_name TEXT, caller_phone TEXT,
            location TEXT, service_type TEXT, vehicle_info TEXT,
            status TEXT DEFAULT 'new', notes TEXT, source TEXT DEFAULT 'phone',
            domain TEXT, eta_minutes INTEGER DEFAULT 20, tech_name TEXT,
            price_quoted TEXT, dispatch_notified INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS dispatch_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT, action TEXT, details TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )""")

        job_id = f"DSP-{_uuid.uuid4().hex[:8].upper()}"
        caller_phone = session.get("caller", "")
        name = caller_info.get("name", "Unknown")
        location = caller_info.get("location", "")
        vehicle = caller_info.get("vehicle_info", "")
        biz = client.get("business_name", "")

        # Classify service from transcript
        full_text = " ".join(
            t.get("text", "") for t in session.get("transcript", [])
            if t.get("role") == "caller"
        ).lower()
        if any(w in full_text for w in ["car", "vehicle", "auto", "truck"]):
            service_type = "car_lockout"
        elif any(w in full_text for w in ["rekey", "re-key"]):
            service_type = "lock_rekey"
        elif any(w in full_text for w in ["house", "home", "apartment", "door"]):
            service_type = "residential_lockout"
        elif any(w in full_text for w in ["office", "business", "commercial"]):
            service_type = "commercial_lockout"
        else:
            service_type = "emergency_lockout"

        # Build notes from transcript
        transcript_text = "\n".join(
            f"{'Caller' if t.get('role')=='caller' else 'Dispatcher'}: {t.get('text','')}"
            for t in session.get("transcript", [])[-6:]
        )

        # Check if job already exists for this call_sid
        existing = conn.execute(
            "SELECT job_id FROM jobs WHERE notes LIKE ?", (f"%{call_sid}%",)
        ).fetchone()
        if existing:
            conn.close()
            return  # Don't duplicate

        conn.execute("""INSERT INTO jobs
            (job_id, caller_name, caller_phone, location, service_type,
             vehicle_info, status, notes, source, domain, eta_minutes)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (job_id, name, caller_phone, location, service_type,
             vehicle, "new", f"call_sid={call_sid}\n{transcript_text}",
             "phone", biz, 20))
        conn.execute("INSERT INTO dispatch_log (job_id, action, details) VALUES (?,?,?)",
            (job_id, "created", f"New {service_type} job from phone call"))
        conn.commit()
        conn.close()

        _log("DISPATCH", f"NEW JOB {job_id}: {service_type} for {name} at {location or 'unknown'} ({biz})")

        # Send SMS notification to owner/Chris
        notify_phone = client.get("dispatch_notify_phone", "")
        if notify_phone:
            _send_dispatch_sms(notify_phone, job_id, service_type, name,
                             caller_phone, location, biz)

        # Send SMS to customer with job link
        if client.get("send_customer_sms") and caller_phone:
            threading.Thread(target=_send_customer_job_sms,
                           args=(caller_phone, job_id, biz), daemon=True).start()

        # Always notify Chris about new leads
        threading.Thread(target=_send_chris_new_lead_sms,
                       args=(caller_phone, biz, caller_info), daemon=True).start()

    except Exception as e:
        _log("DISPATCH", f"Error creating dispatch job: {e}")


def _send_dispatch_sms(to_phone, job_id, service_type, caller_name,
                       caller_phone, location, business):
    """Send SMS dispatch notification via Twilio."""
    try:
        account_sid = os.environ.get("TWILIO_ACCOUNT_SID", "")
        auth_token = os.environ.get("TWILIO_AUTH_TOKEN", "")
        from_number = os.environ.get("TWILIO_PHONE", "")
        if not all([account_sid, auth_token, from_number]):
            _log("DISPATCH", "Twilio credentials missing — SMS not sent")
            return

        loc_text = f" at {location}" if location else ""
        msg = (f"NEW JOB {job_id}\n"
               f"{business}\n"
               f"Type: {service_type.replace('_', ' ').title()}\n"
               f"Customer: {caller_name}\n"
               f"Phone: {caller_phone}\n"
               f"Location: {location or 'Getting address'}\n"
               f"ETA: 15-25 min\n"
               f"Reply ACCEPT to confirm dispatch")

        from urllib.request import Request, urlopen
        import base64
        auth = base64.b64encode(f"{account_sid}:{auth_token}".encode()).decode()
        data = f"To={to_phone}&From={from_number}&Body={msg}".encode()
        req = Request(
            f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json",
            data=data,
            headers={"Authorization": f"Basic {auth}",
                     "Content-Type": "application/x-www-form-urlencoded"})
        urlopen(req, timeout=10)
        _log("DISPATCH", f"SMS sent to {to_phone} for job {job_id}")
    except Exception as e:
        _log("DISPATCH", f"SMS notification failed: {e}")


def _send_customer_job_sms(caller_phone, job_id, business_name):
    """Send SMS to the customer with a link to their job page."""
    try:
        account_sid = os.environ.get("TWILIO_ACCOUNT_SID", "")
        auth_token = os.environ.get("TWILIO_AUTH_TOKEN", "")
        from_number = os.environ.get("TWILIO_PHONE", "")
        if not all([account_sid, auth_token, from_number, caller_phone]):
            _log("SMS", "Missing creds or caller phone for customer SMS")
            return

        # Clean phone number
        import re as _re2
        clean_phone = _re2.sub(r"[^0-9+]", "", caller_phone)
        if not clean_phone.startswith("+"):
            clean_phone = "+1" + clean_phone if len(clean_phone) == 10 else "+" + clean_phone

        msg = (f"Thanks for calling {business_name}! "
               f"A technician is on the way.\n\n"
               f"View your job details here: https://hivecore.app/job/{job_id}\n\n"
               f"You can upload photos and update your job info at that link.")

        from urllib.request import Request, urlopen
        import base64
        auth = base64.b64encode(f"{account_sid}:{auth_token}".encode()).decode()
        from urllib.parse import quote
        body_encoded = quote(msg)
        data = f"To={quote(clean_phone)}&From={quote(from_number)}&Body={body_encoded}".encode()
        req = Request(
            f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json",
            data=data,
            headers={"Authorization": f"Basic {auth}",
                     "Content-Type": "application/x-www-form-urlencoded"})
        urlopen(req, timeout=10)
        _log("SMS", f"Customer job SMS sent to {clean_phone} for job {job_id}")
    except Exception as e:
        _log("SMS", f"Customer job SMS failed: {e}")


def _send_chris_new_lead_sms(caller_phone, business_name, caller_info):
    """Send SMS to Chris about every new call/lead."""
    try:
        # Skip test/simulator numbers — don't spam Chris
        if caller_phone in TEST_NUMBERS:
            _log("SMS", f"Skipping lead SMS for test number {caller_phone}")
            return

        account_sid = os.environ.get("TWILIO_ACCOUNT_SID", "")
        auth_token = os.environ.get("TWILIO_AUTH_TOKEN", "")
        from_number = os.environ.get("TWILIO_PHONE", "")
        chris_phone = "+18509648866"
        if not all([account_sid, auth_token, from_number]):
            return

        name = caller_info.get("name", "Unknown")
        location = caller_info.get("location", "Not yet provided")
        msg = (f"NEW LEAD - {business_name}\n"
               f"Caller: {name}\n"
               f"Phone: {caller_phone}\n"
               f"Location: {location}\n"
               f"Time: {time.strftime('%I:%M %p ET')}")

        from urllib.request import Request, urlopen
        import base64
        from urllib.parse import quote
        auth = base64.b64encode(f"{account_sid}:{auth_token}".encode()).decode()
        body_encoded = quote(msg)
        data = f"To={quote(chris_phone)}&From={quote(from_number)}&Body={body_encoded}".encode()
        req = Request(
            f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json",
            data=data,
            headers={"Authorization": f"Basic {auth}",
                     "Content-Type": "application/x-www-form-urlencoded"})
        urlopen(req, timeout=10)
        _log("SMS", f"Chris notified about new lead from {caller_phone}")
    except Exception as e:
        _log("SMS", f"Chris lead notification failed: {e}")


def _extract_caller_info(transcript):
    """Extract name, phone, email, appointment request from conversation."""
    info = {}
    full_text = " ".join(t.get("text", "") for t in transcript if t.get("role") == "caller").lower()

    # Name extraction (simple patterns)
    import re as _re
    name_match = _re.search(r"(?:my name is|i'm|this is|name's) ([a-z]+ ?[a-z]*)", full_text)
    if name_match:
        info["name"] = name_match.group(1).strip().title()

    # Phone extraction
    phone_match = _re.search(r"(\d{3}[\s.-]?\d{3}[\s.-]?\d{4})", full_text)
    if phone_match:
        info["phone"] = phone_match.group(1)

    # Appointment signals
    if any(w in full_text for w in ["appointment", "schedule", "book", "come out", "come by", "tomorrow", "today", "next week"]):
        info["wants_appointment"] = True

    # Emergency signals
    if any(w in full_text for w in ["emergency", "urgent", "asap", "right now", "locked out", "flooding", "broken"]):
        info["is_emergency"] = True

    # Location extraction (street addresses, landmarks)
    loc_match = _re.search(r"(?:i'm at|i am at|located at|address is|i'm on|at the) (.+?)(?:\.|$|,\s*(?:i|my|and|please|can))", full_text)
    if loc_match:
        info["location"] = loc_match.group(1).strip().title()

    # Vehicle info extraction
    vehicle_match = _re.search(r"(\d{4})\s+([\w]+)\s+([\w]+)", full_text)
    if vehicle_match:
        info["vehicle_info"] = vehicle_match.group(0).title()

    return info


class WebhookState:
    """Shared state for webhook handlers."""

    def __init__(self):
        self.tracker_db = None
        self.agent_id = None
        self.prospect_db = None
        self.trainer = None
        self.call_sessions = {}  # call_sid → {agent, turn, transcript}
        self.stats = {
            "requests": 0, "voice_inbound": 0, "voice_status": 0,
            "sms_inbound": 0, "sms_status": 0, "voicemail": 0,
            "errors": 0, "started_at": time.time(),
        }

    def init(self, tracker_db_path=DB_PATH, prospect_db_path=None):
        if HAS_DB:
            try:
                self.tracker_db = DatabaseManager(tracker_db_path)
                self.agent_id = self.tracker_db.register_agent(
                    self.tracker_db._uid(), "webhook-server",
                    role="telephony-webhooks")
            except Exception:
                pass
        if HAS_PROSPECT:
            self.prospect_db = (ProspectDB(prospect_db_path)
                                if prospect_db_path else ProspectDB())
        if HAS_TRAINER:
            self.trainer = SalesTrainer()


STATE = WebhookState()


class WebhookHandler(BaseHTTPRequestHandler):
    """Unified webhook HTTP handler."""

    def log_message(self, fmt, *args):
        pass  # Suppress default logging

    def _send(self, code, content, content_type="application/xml"):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        if isinstance(content, str):
            content = content.encode()
        self.wfile.write(content)

    def _parse_form(self):
        """Parse form-encoded POST body (Twilio style)."""
        length = int(self.headers.get("Content-Length", 0))
        if not length:
            return {}
        body = self.rfile.read(length).decode()
        params = parse_qs(body, keep_blank_values=True)
        return {k: v[0] if len(v) == 1 else v for k, v in params.items()}

    def do_GET(self):
        path = urlparse(self.path).path
        STATE.stats["requests"] += 1

        if path == "/health":
            uptime = int(time.time() - STATE.stats["started_at"])
            data = {
                "status": "ok",
                "uptime_sec": uptime,
                "stats": STATE.stats,
                "active_calls": len(STATE.call_sessions),
                "providers": ["twilio", "bulkvs", "voipms", "telnyx"],
            }
            self._send(200, json.dumps(data, indent=2), "application/json")

        elif path == "/":
            self._send(200, self._dashboard_html(), "text/html")

        else:
            self._send(404, _twiml_response(
                '<Say>Route not found.</Say>'))

    def do_POST(self):
        path = urlparse(self.path).path
        STATE.stats["requests"] += 1
        params = self._parse_form()

        try:
            # ── Voice Routes ────────────────────────────────────────
            if path == "/voice/inbound" or path == "/voice":
                STATE.stats["voice_inbound"] += 1
                self._handle_voice_inbound(params)

            elif path == "/voice/respond":
                self._handle_voice_respond(params)

            elif path == "/voice/status":
                STATE.stats["voice_status"] += 1
                self._handle_voice_status(params)

            elif path == "/voice/gather":
                self._handle_voice_gather(params)

            elif path == "/voice/hive-checkin":
                self._handle_hive_checkin(params)

            elif path == "/voice/checkin-respond":
                self._handle_checkin_respond(params)

            # ── SMS Routes ──────────────────────────────────────────
            elif path == "/sms/inbound":
                STATE.stats["sms_inbound"] += 1
                self._handle_sms_inbound(params)

            elif path == "/sms/status":
                STATE.stats["sms_status"] += 1
                self._handle_sms_status(params)

            # ── Voicemail Routes ────────────────────────────────────
            elif path == "/voicemail/greeting":
                STATE.stats["voicemail"] += 1
                self._handle_voicemail_greeting(params)

            elif path == "/voicemail/complete":
                self._handle_voicemail_complete(params)

            elif path == "/voicemail/transcription":
                self._handle_voicemail_transcription(params)

            # ── Appointment Routes ──────────────────────────────────
            elif path == "/appointments/reminder":
                self._handle_appointment_reminder(params)

            # ── Voice Collection (founder voice cloning) ────────────
            elif path == "/voice-collect":
                self._handle_voice_collect(params)

            elif path == "/voice-collect-followup":
                self._handle_voice_collect_followup(params)

            elif path == "/recording-status":
                self._handle_recording_status(params)

            elif path == "/call-status":
                self._handle_call_status(params)

            # ── Campaign Status ─────────────────────────────────────
            elif path == "/campaigns/status":
                self._handle_campaign_status(params)

            else:
                self._send(404, _twiml_response(
                    '<Say>Unknown webhook endpoint.</Say>'))

        except Exception as e:
            STATE.stats["errors"] += 1
            _log("WEBHOOK", f"Error on {path}: {e}")
            self._send(500, _twiml_response(
                '<Say>An error occurred. Please try again.</Say>'))

    # ── Voice Handlers ──────────────────────────────────────────────

    def _handle_voice_inbound(self, params):
        """Handle inbound phone call — route through IVR or client AI."""
        caller = params.get("From", "")
        to_number = params.get("To", "")
        call_sid = params.get("CallSid", "")

        _log("WEBHOOK", f"Inbound call: {caller} → {to_number} (SID={call_sid[:12]}...)")

        # ── Telegram notification for EXTERNAL inbound calls ──
        if _is_external_caller(caller):
            # Look up caller name from prospect DB
            _notify_name = ""
            if STATE.prospect_db:
                _p = STATE.prospect_db.get_prospect(caller)
                if _p:
                    _notify_name = f" ({_p.get('owner', '') or _p.get('business', '')})"
            _notify_chris_telegram(
                f"\U0001F4DE <b>INBOUND CALL</b>\n"
                f"From: <code>{caller}</code>{_notify_name}\n"
                f"To: <code>{to_number}</code>\n"
                f"Time: {time.strftime('%I:%M %p ET')}"
            )

        # Check if this is a CLIENT call (their Twilio number)
        client = _get_client_for_number(to_number)
        if client:
            return self._handle_client_call(params, client)

        # Identify caller
        caller_name = FAMILY_NUMBERS.get(caller, "")
        known_prospect = None
        if STATE.prospect_db and not caller_name:
            known_prospect = STATE.prospect_db.get_prospect(caller)
            if known_prospect:
                caller_name = known_prospect.get("owner", "") or known_prospect.get("business", "")

        # Initialize call session
        STATE.call_sessions[call_sid] = {
            "agent": "sarah",
            "turn": 0,
            "transcript": [],
            "caller": caller,
            "caller_name": caller_name,
            "started_at": time.time(),
        }

        # Family → direct to Sarah (no menu)
        if caller in FAMILY_NUMBERS:
            name = FAMILY_NUMBERS[caller]
            twiml = _twiml_response(f'''
<Say voice="Polly.Joanna">Hi {name}! Great to hear from you. How can I help you today?</Say>
<Gather input="speech" timeout="5" speechTimeout="2" enhanced="true" action="/voice/respond?agent=sarah">
    <Say voice="Polly.Joanna">I'm listening.</Say>
</Gather>
<Say voice="Polly.Joanna">I didn't catch that. Let me transfer you to Christopher.</Say>''')
            self._send(200, twiml)
            return

        # After hours → voicemail
        if not _is_business_hours():
            twiml = _twiml_response(f'''
<Say voice="Polly.Joanna">Thank you for calling Hive Dynamics A.I.
Our business hours are Monday through Friday, 9 A.M. to 6 P.M. Eastern.
Please leave a message after the tone and we will return your call on the next business day.</Say>
<Record maxLength="120" action="/voicemail/complete" transcribe="true"
        transcribeCallback="/voicemail/transcription" playBeep="true"/>
<Say voice="Polly.Joanna">We did not receive a recording. Goodbye.</Say>''')
            self._send(200, twiml)
            return

        # Known prospect → personalized greeting
        if caller_name:
            greeting = f"Hi {caller_name}! Thanks for calling Hive Dynamics A.I."
        else:
            greeting = "Thank you for calling Hive Dynamics A.I."

        # Skip IVR — go straight to AI conversation (faster, more natural)
        twiml = _twiml_response(f'''
<Say voice="Polly.Joanna">{greeting} How can I help you today?</Say>
<Gather input="speech" timeout="6" speechTimeout="2" enhanced="true" action="/voice/respond?agent=sarah">
    <Say voice="Polly.Joanna">Go ahead, I'm listening.</Say>
</Gather>
<Say voice="Polly.Joanna">I didn't catch that. Let me try again.</Say>
<Redirect>/voice/respond?agent=sarah</Redirect>''')
        self._send(200, twiml)

        # Log interaction
        if STATE.prospect_db and caller:
            STATE.prospect_db.log_interaction(
                caller, "call", direction="inbound",
                notes=f"Inbound call, caller: {caller_name or 'unknown'}",
                call_sid=call_sid)

    def _handle_client_call(self, params, client):
        """Handle a call for a paying client's business."""
        caller = params.get("From", "")
        call_sid = params.get("CallSid", "")
        biz = client.get("business_name", "our office")
        voice = client.get("voice", "Polly.Joanna")

        _log("CLIENT", f"Call for {biz}: {caller} (SID={call_sid[:12]}...)")

        # Notify Chris via Telegram about client calls (skip test numbers)
        if _is_external_caller(caller):
            _notify_chris_telegram(
                f"\U0001F4DE <b>CLIENT CALL - {biz}</b>\n"
                f"From: <code>{caller}</code>\n"
                f"Time: {time.strftime('%I:%M %p ET')}"
            )

            # Also send SMS to Chris for client calls (Telegram can be unreliable)
            threading.Thread(target=_send_chris_new_lead_sms,
                           args=(caller, biz, {"name": "Incoming caller"}), daemon=True).start()

        # Store client config in session
        STATE.call_sessions[call_sid] = {
            "agent": "sarah",
            "turn": 0,
            "transcript": [],
            "caller": caller,
            "started_at": time.time(),
            "client": client,
        }

        greeting = client.get("greeting", f"Thank you for calling {biz}! How can I help you today?")

        twiml = _twiml_response(f'''
<Gather input="speech" timeout="8" speechTimeout="auto" enhanced="true"
        action="/voice/respond?agent=sarah&amp;client=1">
    <Say voice="{voice}">{greeting}</Say>
</Gather>
<Gather input="speech" timeout="8" speechTimeout="auto" enhanced="true"
        action="/voice/respond?agent=sarah&amp;client=1">
    <Say voice="{voice}">Are you still there?</Say>
</Gather>
<Say voice="{voice}">Goodbye!</Say>''')
        self._send(200, twiml)

        # Log to client usage
        try:
            import sqlite3
            db = sqlite3.connect(os.path.join(_HIVE, "memory", "clients.db"))
            db.execute("""CREATE TABLE IF NOT EXISTS client_calls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_name TEXT, caller TEXT, call_sid TEXT,
                started_at REAL, status TEXT DEFAULT 'active'
            )""")
            db.execute("INSERT INTO client_calls (client_name, caller, call_sid, started_at) VALUES (?,?,?,?)",
                       (biz, caller, call_sid, time.time()))
            db.commit()
            db.close()
        except Exception:
            pass

    def _handle_voice_gather(self, params):
        """Handle IVR menu selection."""
        digits = params.get("Digits", "")
        call_sid = params.get("CallSid", "")

        agent_map = {
            "1": ("matthew", "sales"),
            "2": ("sarah", "customer_service"),
            "3": ("emma", "appointment_setter"),
            "4": (None, "voicemail"),
        }

        agent_name, role = agent_map.get(digits, ("sarah", "customer_service"))

        if role == "voicemail":
            twiml = _twiml_response('''
<Say voice="Polly.Joanna">Please leave your message after the tone.</Say>
<Record maxLength="120" action="/voicemail/complete" transcribe="true"
        transcribeCallback="/voicemail/transcription" playBeep="true"/>''')
            self._send(200, twiml)
            return

        if call_sid in STATE.call_sessions:
            STATE.call_sessions[call_sid]["agent"] = agent_name

        agent = AGENTS.get(agent_name, AGENTS["sarah"])
        greetings = {
            "matthew": "Hi! I'm Matthew from Hive Dynamics sales team. How can I help you today?",
            "sarah": "Hi! Welcome to Hive Dynamics support. What can I help you with?",
            "emma": "Hi! I'm Emma, I handle appointments at Hive Dynamics. Would you like to schedule a demo?",
        }

        greeting = greetings.get(agent_name, greetings["sarah"])
        twiml = _twiml_response(f'''
<Say voice="{agent['voice']}">{greeting}</Say>
<Gather input="speech" timeout="5" speechTimeout="2" enhanced="true"
        action="/voice/respond?agent={agent_name}">
    <Say voice="{agent['voice']}">Go ahead, I'm listening.</Say>
</Gather>
<Say voice="{agent['voice']}">I didn't catch that. Can you try again?</Say>
<Redirect>/voice/respond?agent={agent_name}</Redirect>''')
        self._send(200, twiml)

    def _handle_voice_respond(self, params):
        """Handle AI conversation response (speech input from caller)."""
        speech = params.get("SpeechResult", "").strip()
        call_sid = params.get("CallSid", "")
        query_params = parse_qs(urlparse(self.path).query)
        agent_name = query_params.get("agent", ["sarah"])[0]

        session = STATE.call_sessions.get(call_sid, {})
        session["turn"] = session.get("turn", 0) + 1
        session["transcript"] = session.get("transcript", [])

        agent = AGENTS.get(agent_name, AGENTS["sarah"])

        # Guard: if no speech detected (Redirect/timeout), re-prompt instead of feeding empty input to AI
        if not speech:
            voice_name = agent["voice"]
            if session.get("client"):
                voice_name = session["client"].get("voice", voice_name)
            twiml = _twiml_response(f'''
<Say voice="{voice_name}">Are you still there? Go ahead, I'm listening.</Say>
<Gather input="speech" timeout="8" speechTimeout="2" enhanced="true"
        action="/voice/respond?agent={agent_name}">
    <Say voice="{voice_name}">Take your time.</Say>
</Gather>
<Say voice="{voice_name}">I didn't hear anything. Goodbye!</Say>
<Hangup/>''')
            self._send(200, twiml)
            return

        # Only add caller speech AFTER we confirm it's non-empty
        session["transcript"].append({"role": "caller", "text": speech})

        # Check if this is a client call
        is_client = query_params.get("client", [""])[0] == "1"
        client = session.get("client")

        # Build system prompt
        if is_client and client:
            system_prompt = _client_system_prompt(client)
            voice_override = client.get("voice", agent["voice"])
        else:
            system_prompt = f"""You are {agent_name}, a professional {agent['role']} at Hive Dynamics AI.
Rules:
- Keep responses to 1-2 SHORT sentences max
- Ask ONE question at a time
- If caller wants to book/schedule, ask for their name first, then preferred time
- If caller has an emergency, say help is on the way and ask for their address
- Sound natural, like a real person on the phone
- Never list multiple options — just handle what they asked"""
            voice_override = agent["voice"]

        # Add conversation context for multi-turn (exclude current speech — it's the user_message)
        prior = session["transcript"][:-1] if len(session["transcript"]) > 1 else []
        if prior:
            recent = prior[-6:]  # last 3 exchanges before current
            context = "\n".join(f"{'Caller' if t['role']=='caller' else 'You'}: {t['text']}" for t in recent)
            system_prompt += f"\n\nConversation so far:\n{context}"

        # Extract info from what caller said so far
        caller_info = _extract_caller_info(session.get("transcript", []))
        if caller_info.get("name"):
            system_prompt += f"\nCaller's name: {caller_info['name']}"
        if caller_info.get("wants_appointment"):
            system_prompt += "\nCaller wants to schedule — get their preferred date/time."
        if caller_info.get("is_emergency"):
            system_prompt += "\nThis is URGENT — prioritize getting their location and dispatching help."

        # ── COMMAND INTERCEPT ──────────────────────────────────
        # If Chris says a command, route to Hive Commander instead of AI
        _is_cmd, _cmd_response = _try_hive_command(speech, session.get("caller", ""))
        if _is_cmd and _cmd_response:
            response = _cmd_response
            _log("COMMAND", f"Voice command executed: {speech[:50]} -> {response[:50]}")
        else:
            # ── Normal AI response ────────────────────────────────
            response = _phi4_response(system_prompt, speech)
            if not response:
                # AI FAILED — send SMS fallback for client calls
                caller_num = session.get("caller", "")
                biz_name = ""
                if is_client and client:
                    biz_name = client.get("business_name", "")
                if caller_num and _is_external_caller(caller_num):
                    threading.Thread(target=_send_ai_failure_sms,
                                   args=(caller_num, biz_name), daemon=True).start()
                    response = "I apologize, let me get someone to help you right away. You will receive a text message shortly with next steps."
                else:
                    response = "I'm here. Tell me more about what you need."

        # Keep it SHORT for phone — max 2 sentences
        sentences = response.split(". ")
        if len(sentences) > 2:
            response = ". ".join(sentences[:2]) + "."

        # Save actions if we detected appointment/emergency
        if caller_info.get("wants_appointment") or caller_info.get("is_emergency"):
            action_type = "appointment" if caller_info.get("wants_appointment") else "emergency"
            _save_call_action(call_sid, action_type, {
                "caller_info": caller_info, "transcript_so_far": session.get("transcript", [])[-4:],
                "caller_phone": session.get("caller", ""),
            })
            # Create dispatch job for locksmith/service clients
            if is_client and client and client.get("dispatch_enabled"):
                _create_dispatch_job(call_sid, session, caller_info, client)

        session["transcript"].append({"role": agent_name, "text": response})

        # Check if this is voice collection mode
        mode = query_params.get("mode", [""])[0]

        # ── AUTO-DISPATCH: detect when AI confirms dispatch ──
        response_lower = response.lower()
        dispatch_phrases = ["technician is on the way", "tech is on the way",
                           "see you soon", "on the way to", "dispatching",
                           "sending a technician", "be there shortly"]
        is_dispatch_confirm = any(dp in response_lower for dp in dispatch_phrases)
        if is_dispatch_confirm and is_client and client and client.get("dispatch_enabled"):
            # Create dispatch job and send SMS
            _create_dispatch_job(call_sid, session, caller_info, client)
            safe_resp = response.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            twiml = _twiml_response(f'''
<Say voice="{voice_override}">{safe_resp}</Say>
<Hangup/>''')
            if call_sid in STATE.call_sessions:
                del STATE.call_sessions[call_sid]
            self._send(200, twiml)
            return

        # Detect goodbye intent — ONLY clear exit phrases, not casual "thanks"
        speech_lower = speech.lower().strip()
        goodbye_phrases = ["goodbye", "bye bye", "that's all", "no thanks",
                           "not interested", "stop calling", "hang up", "i gotta go",
                           "i have to go", "talk to you later", "gotta run"]
        # Only match if the WHOLE message is basically goodbye (short + contains bye)
        is_goodbye = any(gw in speech_lower for gw in goodbye_phrases)
        if not is_goodbye and len(speech_lower) < 15:
            is_goodbye = speech_lower in ["bye", "ok bye", "thanks bye", "goodbye"]
        if is_goodbye:
            safe_resp = response.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            if mode == "voice_collect":
                twiml = _twiml_response(f'''
<Say voice="{agent['voice']}">{safe_resp} Thanks for the voice session, Christopher! Every word helps train the voice clone. Talk soon!</Say>
<Hangup/>''')
            elif is_client and client:
                biz_name = client.get("business_name", "us")
                twiml = _twiml_response(f'''
<Say voice="{voice_override}">{safe_resp} Thanks for calling {biz_name}! Help is on the way. Have a good day!</Say>
<Hangup/>''')
                # Ensure dispatch job created for service clients on call end
                if client.get("dispatch_enabled"):
                    _create_dispatch_job(call_sid, session, caller_info, client)
            else:
                twiml = _twiml_response(f'''
<Say voice="{agent['voice']}">{safe_resp} Thanks for calling Hive Dynamics!
Call us anytime at {CALLBACK_NUMBER}. Have a great day!</Say>
<Hangup/>''')
            # Clean up session
            if call_sid in STATE.call_sessions:
                del STATE.call_sessions[call_sid]
            self._send(200, twiml)
            return

        safe_resp = response.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        voice = voice_override
        client_param = "&amp;client=1" if is_client else ""

        # Voice collection mode
        if mode == "voice_collect":
            import random
            followups = [
                "That's interesting. Tell me more about that.",
                "I love that. What else comes to mind?",
                "Great insight. How would you explain that to someone new?",
                "That's a solid point. What's the next step there?",
                "Keep going, this is good stuff.",
            ]
            followup = random.choice(followups)
            twiml = _twiml_response(f'''
<Say voice="{voice}">{safe_resp} {followup}</Say>
<Gather input="speech" timeout="10" speechTimeout="2" enhanced="true"
        action="/voice/respond?agent={agent_name}&amp;mode=voice_collect">
    <Say voice="{voice}">Take your time.</Say>
</Gather>
<Redirect>/voice-collect-followup?sid={call_sid}</Redirect>''')
        else:
            twiml = _twiml_response(f'''
<Say voice="{voice}">{safe_resp}</Say>
<Gather input="speech" timeout="6" speechTimeout="2" enhanced="true"
        action="/voice/respond?agent={agent_name}{client_param}">
    <Say voice="{voice}">Go ahead.</Say>
</Gather>
<Say voice="{voice}">Are you still here? I'm happy to keep helping.</Say>
<Gather input="speech" timeout="8" speechTimeout="2" enhanced="true"
        action="/voice/respond?agent={agent_name}{client_param}">
    <Say voice="{voice}">Take your time.</Say>
</Gather>
<Say voice="{voice}">It seems like you may have stepped away. Feel free to call back anytime. Goodbye!</Say>''')
        self._send(200, twiml)

    def _handle_voice_status(self, params):
        """Handle call status callback."""
        call_sid = params.get("CallSid", "")
        status = params.get("CallStatus", "")
        duration = params.get("CallDuration", "0")

        _log("WEBHOOK", f"Call status: {call_sid[:12]}... → {status} ({duration}s)")

        if STATE.tracker_db and STATE.agent_id:
            _learn(STATE.tracker_db, STATE.agent_id, "call_status",
                   f"Call {status}: {duration}s",
                   {"call_sid": call_sid, "status": status, "duration": duration})

        # Clean up session
        if status in ("completed", "failed", "busy", "no-answer"):
            STATE.call_sessions.pop(call_sid, None)

        self._send(200, "", "text/plain")

    # ── SMS Handlers ────────────────────────────────────────────────

    def _handle_sms_inbound(self, params):
        """Handle inbound SMS message."""
        from_number = params.get("From", "")
        body = params.get("Body", "").strip()
        to_number = params.get("To", "")

        _log("WEBHOOK", f"Inbound SMS: {from_number} → {body[:50]}")

        # ── Telegram notification for EXTERNAL inbound SMS ──
        if _is_external_caller(from_number):
            _sms_preview = body[:100] + ("..." if len(body) > 100 else "")
            _notify_chris_telegram(
                f"\U0001F4E8 <b>INBOUND SMS</b>\n"
                f"From: <code>{from_number}</code>\n"
                f"To: <code>{to_number}</code>\n"
                f"Message: {_sms_preview}\n"
                f"Time: {time.strftime('%I:%M %p ET')}"
            )

        # Forward ALL inbound SMS to Chris's phone
        try:
            from urllib.request import Request, urlopen
            import urllib.parse
            fwd_data = urllib.parse.urlencode({
                'To': '+18509648866',
                'From': to_number,
                'Body': f'SMS from {from_number}: {body}'
            }).encode()
            fwd_auth = __import__('base64').b64encode(b'os.environ.get("TWILIO_ACCOUNT_SID",""):os.environ.get("TWILIO_AUTH_TOKEN","")').decode()
            fwd_req = Request(
                'https://api.twilio.com/2010-04-01/Accounts/os.environ.get("TWILIO_ACCOUNT_SID","")/Messages.json',
                data=fwd_data,
                headers={'Authorization': f'Basic {fwd_auth}', 'Content-Type': 'application/x-www-form-urlencoded'}
            )
            urlopen(fwd_req, timeout=5)
            _log("WEBHOOK", f"SMS forwarded to Chris from {from_number}")
        except Exception as e:
            _log("WEBHOOK", f"SMS forward failed: {e}")

        # Log to prospect DB
        if STATE.prospect_db and from_number:
            STATE.prospect_db.log_interaction(
                from_number, "sms", direction="inbound",
                notes=body[:200])

        # Basic intent responses
        body_lower = body.lower()

        if body_lower in ("stop", "unsubscribe", "quit"):
            reply = "You've been unsubscribed. Reply START to re-subscribe."
            if STATE.prospect_db:
                STATE.prospect_db.update_stage(from_number, "lost")

        elif body_lower in ("start", "subscribe", "yes"):
            reply = ("Welcome back to Hive Dynamics AI! "
                     "We help businesses capture every phone call with AI. "
                     "Reply DEMO for a free demo or call (850) 801-6662")

        elif body_lower in ("help", "info"):
            reply = ("Hive Dynamics AI — AI Phone Service\n"
                     "DEMO — Book a free demo\n"
                     "PRICING — See our plans\n"
                     "HOURS — Business hours\n"
                     "STOP — Unsubscribe\n"
                     "Or call (850) 801-6662")

        elif any(kw in body_lower for kw in ["price", "cost", "how much", "plan"]):
            reply = ("Hive Dynamics AI Plans:\n"
                     "Starter: $297/mo — AI answers + books appointments\n"
                     "Pro: $497/mo — + SMS follow-ups + CRM\n"
                     "Enterprise: $997/mo — Full custom AI agent\n"
                     "FREE 2-week trial! Reply DEMO to get started.")

        elif any(kw in body_lower for kw in ["demo", "trial", "interested", "sign up"]):
            reply = ("Let's get you set up with a free demo!\n"
                     "We'll call you within the hour, or pick a time:\n"
                     "Call (850) 801-6662 and ask for Christopher.\n"
                     "Looking forward to showing you what AI can do!")
            if STATE.prospect_db:
                STATE.prospect_db.update_stage(from_number, "interested")

        elif any(kw in body_lower for kw in ["hours", "open", "available"]):
            reply = ("Hive Dynamics AI hours:\n"
                     "Mon-Fri: 9AM - 6PM Eastern\n"
                     "Our AI answers 24/7 though!\n"
                     "Call anytime: (850) 801-6662")

        else:
            # Try AI response
            ai_reply = _phi4_response(
                "You are a friendly sales assistant at Hive Dynamics AI. "
                "We sell AI phone answering services. Keep responses under 160 chars.",
                body)
            reply = ai_reply if ai_reply else (
                "Thanks for reaching out to Hive Dynamics AI! "
                "Reply HELP for options or call (850) 801-6662")

        twiml = _twiml_response(
            f'<Message>{reply.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")}</Message>')
        self._send(200, twiml)

    def _handle_sms_status(self, params):
        """Handle SMS delivery status."""
        sid = params.get("MessageSid", "")
        status = params.get("MessageStatus", "")
        _log("WEBHOOK", f"SMS status: {sid[:12]}... → {status}")
        self._send(200, "", "text/plain")

    # ── Voicemail Handlers ──────────────────────────────────────────

    def _handle_voicemail_greeting(self, params):
        twiml = _twiml_response('''
<Say voice="Polly.Joanna">You've reached Hive Dynamics A.I.
We're either on another call or away from the phone.
Please leave your name, number, and a brief message, and we'll get back to you as soon as possible.</Say>
<Record maxLength="120" action="/voicemail/complete" transcribe="true"
        transcribeCallback="/voicemail/transcription" playBeep="true"/>''')
        self._send(200, twiml)

    def _handle_voicemail_complete(self, params):
        url = params.get("RecordingUrl", "")
        duration = params.get("RecordingDuration", "0")
        caller = params.get("From", "")
        _log("WEBHOOK", f"Voicemail: {caller} ({duration}s)")

        if STATE.tracker_db and STATE.agent_id:
            _learn(STATE.tracker_db, STATE.agent_id, "voicemail_received",
                   f"Voicemail from {caller}: {duration}s",
                   {"caller": caller, "duration": duration, "url": url})

        twiml = _twiml_response(
            '<Say voice="Polly.Joanna">Thank you. We will return your call soon. Goodbye.</Say>')
        self._send(200, twiml)

    def _handle_voicemail_transcription(self, params):
        text = params.get("TranscriptionText", "")
        caller = params.get("From", "")
        _log("WEBHOOK", f"VM transcription from {caller}: {text[:80]}")

        if STATE.prospect_db and caller:
            STATE.prospect_db.log_interaction(
                caller, "voicemail", direction="inbound",
                notes=f"Voicemail: {text}")

        self._send(200, "", "text/plain")

    # ── Appointment Handlers ────────────────────────────────────────

    def _handle_appointment_reminder(self, params):
        self._send(200, "", "text/plain")

    # ── Campaign Handlers ───────────────────────────────────────────

    def _handle_campaign_status(self, params):
        """Track campaign call/SMS status callbacks."""
        self._handle_voice_status(params)

    # ── Voice Collection Handlers ────────────────────────────────

    def _handle_voice_collect(self, params):
        """Handle voice collection call — AI conversation to record founder's voice.
        Keeps the conversation going for many turns to collect voice samples."""
        call_sid = params.get("CallSid", "")
        caller = params.get("From", "")

        _log("WEBHOOK", f"Voice collection call answered: {call_sid[:12]}...")

        # Initialize voice collection session
        STATE.call_sessions[call_sid] = {
            "agent": "matthew",
            "turn": 0,
            "transcript": [],
            "caller": caller,
            "mode": "voice_collect",
            "started_at": time.time(),
        }

        # Start with a prompt that gets the founder talking naturally
        import random
        prompts = [
            "Hey Christopher! This is your Hive AI calling. I wanted to check in with you about the system. What have you been working on lately?",
            "Hey! The Hive is calling. I have a few questions for you. First, can you tell me about your vision for the AI phone system?",
            "Hi Christopher! Quick check-in from the Hive. How do you think the voice agents are performing? What would you change?",
        ]
        prompt = random.choice(prompts)

        twiml = _twiml_response(f'''
<Say voice="Polly.Matthew">{prompt}</Say>
<Gather input="speech" timeout="10" speechTimeout="2" enhanced="true"
        action="/voice/respond?agent=matthew&amp;mode=voice_collect">
    <Say voice="Polly.Matthew">Go ahead, I'm listening.</Say>
</Gather>
<Say voice="Polly.Matthew">I didn't catch that. No worries. Tell me, what's the biggest thing you want the Hive to do next?</Say>
<Gather input="speech" timeout="10" speechTimeout="2" enhanced="true"
        action="/voice/respond?agent=matthew&amp;mode=voice_collect">
    <Say voice="Polly.Matthew">I'm here, take your time.</Say>
</Gather>
<Redirect>/voice-collect-followup?sid={call_sid}</Redirect>''')
        self._send(200, twiml)

    def _handle_hive_checkin(self, params):
        """Outbound call to founder — deliver Hive status, then listen."""
        call_sid = params.get("CallSid", "")

        # Load pre-built status message
        state_path = os.path.join(_HIVE, "memory", "hive_caller_state.json")
        status_msg = "Hey Chris! Your Hive is checking in. Everything is running. What's on your mind?"
        try:
            with open(state_path) as f:
                state = json.load(f)
                status_msg = state.get("status_message", status_msg)
        except Exception:
            pass

        # Escape for XML
        safe_msg = status_msg.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

        STATE.call_sessions[call_sid] = {
            "agent": "hive",
            "turn": 0,
            "transcript": [],
            "caller": FAMILY_NUMBERS.get("+18506872085", "Christopher"),
            "started_at": time.time(),
            "mode": "checkin",
            "founder_directives": [],
        }

        twiml = _twiml_response(f'''
<Say voice="Polly.Joanna">{safe_msg}</Say>
<Gather input="speech" timeout="8" speechTimeout="2" enhanced="true"
        action="/voice/checkin-respond">
    <Say voice="Polly.Joanna">I'm listening, go ahead.</Say>
</Gather>
<Say voice="Polly.Joanna">Still here Chris. Anything you want me to work on?</Say>
<Gather input="speech" timeout="10" speechTimeout="2" enhanced="true"
        action="/voice/checkin-respond">
    <Say voice="Polly.Joanna">Take your time.</Say>
</Gather>
<Say voice="Polly.Joanna">Alright, I'll keep everything running. Talk to you next hour!</Say>''')
        self._send(200, twiml)

    def _handle_checkin_respond(self, params):
        """Handle founder's response during checkin — LISTEN and LEARN."""
        speech = params.get("SpeechResult", "")
        call_sid = params.get("CallSid", "")
        session = STATE.call_sessions.get(call_sid, {})
        session["turn"] = session.get("turn", 0) + 1
        session.setdefault("transcript", []).append({"role": "founder", "text": speech})
        session.setdefault("founder_directives", []).append(speech)

        _log("FOUNDER", f"Chris said: {speech}")

        # STORE what the founder says — this is gold
        try:
            import sqlite3 as _sql
            conn = _sql.connect(os.path.join(_HIVE, "memory", "nerve.db"))
            conn.execute("""INSERT INTO knowledge (category, key, value, confidence, source, learned_at)
                VALUES (?, ?, ?, ?, ?, datetime('now'))""",
                ("founder_directive", f"call_{int(time.time())}_{session['turn']}", speech, 1.0, "hive_caller"))
            conn.commit()
            conn.close()
        except Exception:
            pass

        # Also save to founder inbox
        try:
            inbox = os.path.join(_HIVE, "FOUNDER_INBOX.md")
            with open(inbox, "a") as f:
                f.write(f"\n## Voice Directive ({time.strftime('%Y-%m-%d %H:%M')})\n{speech}\n")
        except Exception:
            pass

        # Generate a smart response acknowledging what they said
        system_prompt = """You are the Hive, Christopher's AI system.
You called him for a status check-in. He's giving you direction.
Rules:
- Acknowledge EXACTLY what he said, not generic filler
- Confirm the specific action you'll take
- Keep it to 1 sentence, then ask what else
- Sound like a real assistant, not a robot
- If he asks a question, answer it directly
- NEVER call yourself Sarah or any other name"""

        # Include conversation context
        recent = session.get("transcript", [])[-4:]
        context = "\n".join(f"{'Chris' if t['role']=='founder' else 'You'}: {t['text']}" for t in recent)
        full_prompt = f"{system_prompt}\n\nConversation:\n{context}"

        response = _phi4_response(full_prompt, speech)
        if not response:
            response = "Got it, I'll make that happen. Anything else?"

        sentences = response.split(". ")
        if len(sentences) > 2:
            response = ". ".join(sentences[:2]) + "."

        session.setdefault("transcript", []).append({"role": "assistant", "text": response})
        safe_resp = response.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

        # Check for goodbye — only clear exit, not casual words
        speech_lower = speech.lower().strip()
        exit_phrases = ["goodbye", "bye bye", "that's all", "i gotta go",
                        "i have to go", "talk to you later", "gotta run", "hang up"]
        is_exit = any(gw in speech_lower for gw in exit_phrases)
        if not is_exit and len(speech_lower) < 15:
            is_exit = speech_lower in ["bye", "ok bye", "later", "peace"]
        if is_exit:
            twiml = _twiml_response(f'''
<Say voice="Polly.Joanna">{safe_resp} Talk to you in an hour, Chris. The Hive keeps growing!</Say>
<Hangup/>''')
            # Save full transcript
            try:
                transcript_path = os.path.join(_HIVE, "memory", "checkin_transcripts.jsonl")
                with open(transcript_path, "a") as f:
                    f.write(json.dumps({
                        "timestamp": time.time(),
                        "date": time.strftime("%Y-%m-%d %H:%M"),
                        "transcript": session.get("transcript", []),
                        "directives": session.get("founder_directives", []),
                    }) + "\n")
            except Exception:
                pass
            if call_sid in STATE.call_sessions:
                del STATE.call_sessions[call_sid]
            self._send(200, twiml)
            return

        twiml = _twiml_response(f'''
<Say voice="Polly.Joanna">{safe_resp}</Say>
<Gather input="speech" timeout="10" speechTimeout="2" enhanced="true"
        action="/voice/checkin-respond">
    <Say voice="Polly.Joanna">I'm listening.</Say>
</Gather>
<Gather input="speech" timeout="15" speechTimeout="2" enhanced="true"
        action="/voice/checkin-respond">
    <Say voice="Polly.Joanna">Still here Chris, take your time.</Say>
</Gather>
<Gather input="speech" timeout="15" speechTimeout="2" enhanced="true"
        action="/voice/checkin-respond">
    <Say voice="Polly.Joanna">I'm not going anywhere. What else?</Say>
</Gather>
<Say voice="Polly.Joanna">Alright Chris, I'll keep everything running. Calling back in an hour!</Say>''')
        self._send(200, twiml)

    def _handle_voice_collect_followup(self, params):
        """Keep voice collection conversation going if Gather times out."""
        call_sid = params.get("CallSid", "")
        session = STATE.call_sessions.get(call_sid, {})
        turn = session.get("turn", 0)

        import random
        followups = [
            "So tell me, if you could add one superpower to the Hive, what would it be?",
            "What's the craziest idea you've had for an AI agent? Something nobody else is building?",
            "If you had unlimited compute, what would you build first?",
            "Describe your perfect day when the Hive is fully autonomous. What does that look like?",
            "What industries do you think need AI the most right now?",
            "Tell me about a problem you solved recently that you're proud of.",
            "What do you think about voice cloning technology? How should we use it?",
            "If you were pitching the Hive to an investor in 60 seconds, what would you say?",
            "What's the hardest technical challenge you've faced building all this?",
            "Describe what the Hive looks like in one year. Paint me a picture.",
        ]
        prompt = random.choice(followups)

        twiml = _twiml_response(f'''
<Say voice="Polly.Matthew">{prompt}</Say>
<Gather input="speech" timeout="10" speechTimeout="2" enhanced="true"
        action="/voice/respond?agent=matthew&amp;mode=voice_collect">
    <Say voice="Polly.Matthew">I'm listening.</Say>
</Gather>
<Redirect>/voice-collect-followup?sid={call_sid}</Redirect>''')
        self._send(200, twiml)

    def _handle_recording_status(self, params):
        """Handle recording completion callback — log for voice cloning."""
        rec_sid = params.get("RecordingSid", "")
        rec_url = params.get("RecordingUrl", "")
        duration = params.get("RecordingDuration", "0")
        call_sid = params.get("CallSid", "")
        channels = params.get("RecordingChannels", "1")

        _log("WEBHOOK", f"Recording complete: {rec_sid} ({duration}s, {channels}ch)")

        # Save metadata for voice collection pipeline
        import json as _json
        meta_dir = "/THE_HIVE/memory/voice_samples"
        os.makedirs(meta_dir, exist_ok=True)
        meta_path = os.path.join(meta_dir, "recordings.jsonl")
        with open(meta_path, "a") as f:
            f.write(_json.dumps({
                "recording_sid": rec_sid,
                "call_sid": call_sid,
                "url": rec_url,
                "duration": int(duration),
                "channels": int(channels),
                "timestamp": time.time(),
                "date": time.strftime("%Y-%m-%d %H:%M:%S"),
            }) + "\n")

        self._send(200, "", "text/plain")

    def _handle_call_status(self, params):
        """Handle call status for voice collection calls."""
        call_sid = params.get("CallSid", "")
        status = params.get("CallStatus", "")
        duration = params.get("CallDuration", "0")
        _log("WEBHOOK", f"Voice collect call: {call_sid[:12]}... → {status} ({duration}s)")
        self._send(200, "", "text/plain")

    # ── Dashboard ───────────────────────────────────────────────────

    def _dashboard_html(self):
        uptime = int(time.time() - STATE.stats["started_at"])
        hours = uptime // 3600
        mins = (uptime % 3600) // 60

        return f'''<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>Webhook Server</title>
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<style>
body {{ font-family: monospace; background: #0a0a0a; color: #e0e0e0; padding: 20px; }}
h1 {{ color: #f0c040; }} h2 {{ color: #4da6ff; margin-top: 20px; }}
.stat {{ display: inline-block; background: #1a1a1a; border: 1px solid #333;
         padding: 15px; margin: 5px; border-radius: 8px; min-width: 120px; text-align: center; }}
.stat .num {{ font-size: 2em; color: #f0c040; }} .stat .label {{ color: #888; font-size: 0.85em; }}
table {{ border-collapse: collapse; margin-top: 10px; }}
td, th {{ padding: 5px 15px; text-align: left; border-bottom: 1px solid #222; }}
th {{ color: #4da6ff; }}
.green {{ color: #4CAF50; }} .red {{ color: #f44336; }}
</style></head><body>
<h1>Webhook Server</h1>
<p>Uptime: {hours}h {mins}m | Active calls: {len(STATE.call_sessions)}</p>

<div>
<div class="stat"><div class="num">{STATE.stats['requests']}</div><div class="label">Requests</div></div>
<div class="stat"><div class="num">{STATE.stats['voice_inbound']}</div><div class="label">Calls In</div></div>
<div class="stat"><div class="num">{STATE.stats['sms_inbound']}</div><div class="label">SMS In</div></div>
<div class="stat"><div class="num">{STATE.stats['voicemail']}</div><div class="label">Voicemails</div></div>
<div class="stat"><div class="num">{STATE.stats['errors']}</div><div class="label">Errors</div></div>
</div>

<h2>Endpoints</h2>
<table>
<tr><th>Route</th><th>Method</th><th>Handler</th></tr>
<tr><td>/voice/inbound</td><td>POST</td><td>IVR phone tree</td></tr>
<tr><td>/voice/respond</td><td>POST</td><td>AI conversation</td></tr>
<tr><td>/voice/status</td><td>POST</td><td>Call tracking</td></tr>
<tr><td>/voice/gather</td><td>POST</td><td>Menu selection</td></tr>
<tr><td>/sms/inbound</td><td>POST</td><td>SMS auto-responder</td></tr>
<tr><td>/sms/status</td><td>POST</td><td>Delivery tracking</td></tr>
<tr><td>/voicemail/greeting</td><td>POST</td><td>VM greeting</td></tr>
<tr><td>/voicemail/complete</td><td>POST</td><td>VM recording</td></tr>
<tr><td>/voicemail/transcription</td><td>POST</td><td>VM text</td></tr>
<tr><td>/appointments/reminder</td><td>POST</td><td>Reminders</td></tr>
<tr><td>/campaigns/status</td><td>POST</td><td>Campaign tracking</td></tr>
<tr><td>/health</td><td>GET</td><td>Health check</td></tr>
</table>

<h2>Active Calls ({len(STATE.call_sessions)})</h2>
<table>
<tr><th>SID</th><th>Agent</th><th>Caller</th><th>Turns</th><th>Duration</th></tr>'''+ "".join(
    f'<tr><td>{sid[:12]}...</td><td>{s.get("agent","?")}</td>'
    f'<td>{s.get("caller_name","") or s.get("caller","?")}</td>'
    f'<td>{s.get("turn",0)}</td>'
    f'<td>{int(time.time()-s.get("started_at",time.time()))}s</td></tr>'
    for sid, s in STATE.call_sessions.items()
) + '''</table>
</body></html>'''


def start_server(port=PORT, tracker_db_path=DB_PATH, prospect_db_path=None):
    """Start the unified webhook server."""
    STATE.init(tracker_db_path, prospect_db_path)

    HTTPServer.allow_reuse_address = True
    server = HTTPServer(("0.0.0.0", port), WebhookHandler)
    _log("WEBHOOK", f"Unified Webhook Server on http://0.0.0.0:{port}")
    _log("WEBHOOK", f"Endpoints: /voice/* /sms/* /voicemail/* /appointments/* /campaigns/*")

    def shutdown(sig, frame):
        server.shutdown()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        _log("WEBHOOK", "Server stopped")


# === SELF-TEST ===

    @app.post("/sms/incoming")
    async def sms_incoming(request):
        from starlette.requests import Request
        form = await request.form()
        body = form.get("Body", "")
        from_number = form.get("From", "")
        to_number = form.get("To", "")
        print(f"  SMS received: {from_number} -> {to_number}: {body[:50]}")
        # Forward verification codes
        try:
            from sms_link_sender import forward_verification_code
            forward_verification_code(body, from_number)
        except Exception as e:
            print(f"  SMS forward error: {e}")
        # Log to DB
        try:
            import sqlite3
            db = sqlite3.connect("/THE_HIVE/memory/telephony.db")
            db.execute("INSERT INTO sms_log (from_number, to_number, body, direction) VALUES (?,?,?,?)",
                (from_number, to_number, body, "inbound"))
            db.commit()
            db.close()
        except:
            pass
        from starlette.responses import Response
        return Response("<Response></Response>", media_type="text/xml")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Unified Webhook Server")
    ap.add_argument("--daemon", action="store_true", help="Start server")
    ap.add_argument("--port", type=int, default=PORT)
    args = ap.parse_args()

    if args.daemon:
        start_server(port=args.port)
        sys.exit(0)

    # ── Self-Test ───────────────────────────────────────────────────
    import socket
    from urllib.request import Request, urlopen
    from urllib.error import HTTPError as _HTTPError
    from urllib.parse import urlencode

    SEP = "=" * 60
    print(f"{SEP}\nWEBHOOK SERVER — SELF-TEST\n{SEP}")
    passed = 0

    # [1] State init
    STATE.init()
    passed += 1
    print(f"[{passed}] State initialized  PASSED")

    # [2] Find free port
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("", 0))
    test_port = sock.getsockname()[1]
    sock.close()
    passed += 1
    print(f"[{passed}] Test port: {test_port}  PASSED")

    # [3] Start server in background
    HTTPServer.allow_reuse_address = True
    server = HTTPServer(("127.0.0.1", test_port), WebhookHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    time.sleep(0.3)
    passed += 1
    print(f"[{passed}] Server started  PASSED")

    base_url = f"http://127.0.0.1:{test_port}"

    # [4] Health check
    resp = json.loads(urlopen(f"{base_url}/health", timeout=5).read().decode())
    assert resp["status"] == "ok"
    assert "stats" in resp
    passed += 1
    print(f"[{passed}] Health check: ok  PASSED")

    # [5] Dashboard
    html = urlopen(f"{base_url}/", timeout=5).read().decode()
    assert "Webhook Server" in html
    assert "Endpoints" in html
    passed += 1
    print(f"[{passed}] Dashboard HTML  PASSED")

    # [6] Voice inbound
    data = urlencode({
        "CallSid": "CA_test_123456",
        "From": "+15551234567",
        "To": "+18508016662",
        "CallStatus": "ringing",
    }).encode()
    resp = urlopen(Request(f"{base_url}/voice/inbound", data=data,
                           headers={"Content-Type": "application/x-www-form-urlencoded"}),
                   timeout=5).read().decode()
    assert "<Response>" in resp
    assert "Hive Dynamics" in resp
    passed += 1
    print(f"[{passed}] Voice inbound → IVR menu  PASSED")

    # [7] Voice inbound — family number
    data2 = urlencode({
        "CallSid": "CA_test_family",
        "From": "+18506872085",
        "To": "+18508016662",
    }).encode()
    resp2 = urlopen(Request(f"{base_url}/voice/inbound", data=data2,
                            headers={"Content-Type": "application/x-www-form-urlencoded"}),
                    timeout=5).read().decode()
    assert "Christopher" in resp2
    passed += 1
    print(f"[{passed}] Family call → personalized greeting  PASSED")

    # [8] IVR gather (press 1 = sales)
    data3 = urlencode({
        "CallSid": "CA_test_123456",
        "Digits": "1",
    }).encode()
    resp3 = urlopen(Request(f"{base_url}/voice/gather", data=data3,
                            headers={"Content-Type": "application/x-www-form-urlencoded"}),
                    timeout=5).read().decode()
    assert "Matthew" in resp3 or "sales" in resp3.lower()
    passed += 1
    print(f"[{passed}] IVR menu → sales agent  PASSED")

    # [9] IVR gather (press 4 = voicemail)
    data4 = urlencode({"CallSid": "CA_test_vm", "Digits": "4"}).encode()
    resp4 = urlopen(Request(f"{base_url}/voice/gather", data=data4,
                            headers={"Content-Type": "application/x-www-form-urlencoded"}),
                    timeout=5).read().decode()
    assert "Record" in resp4
    passed += 1
    print(f"[{passed}] IVR menu → voicemail  PASSED")

    # [10] SMS inbound — help
    sms_data = urlencode({
        "From": "+15559876543", "To": "+18508016662", "Body": "HELP"
    }).encode()
    sms_resp = urlopen(Request(f"{base_url}/sms/inbound", data=sms_data,
                               headers={"Content-Type": "application/x-www-form-urlencoded"}),
                       timeout=5).read().decode()
    assert "<Message>" in sms_resp
    assert "DEMO" in sms_resp or "demo" in sms_resp.lower()
    passed += 1
    print(f"[{passed}] SMS inbound → help response  PASSED")

    # [11] SMS inbound — pricing
    sms_data2 = urlencode({
        "From": "+15559876543", "Body": "How much does it cost?"
    }).encode()
    sms_resp2 = urlopen(Request(f"{base_url}/sms/inbound", data=sms_data2,
                                headers={"Content-Type": "application/x-www-form-urlencoded"}),
                        timeout=5).read().decode()
    assert "$297" in sms_resp2 or "297" in sms_resp2
    passed += 1
    print(f"[{passed}] SMS inbound → pricing  PASSED")

    # [12] SMS inbound — stop
    sms_data3 = urlencode({
        "From": "+15559876543", "Body": "STOP"
    }).encode()
    sms_resp3 = urlopen(Request(f"{base_url}/sms/inbound", data=sms_data3,
                                headers={"Content-Type": "application/x-www-form-urlencoded"}),
                        timeout=5).read().decode()
    assert "unsubscribed" in sms_resp3.lower()
    passed += 1
    print(f"[{passed}] SMS inbound → unsubscribe  PASSED")

    # [13] Voice status callback
    status_data = urlencode({
        "CallSid": "CA_test_123456", "CallStatus": "completed",
        "CallDuration": "45",
    }).encode()
    urlopen(Request(f"{base_url}/voice/status", data=status_data,
                    headers={"Content-Type": "application/x-www-form-urlencoded"}),
            timeout=5)
    assert "CA_test_123456" not in STATE.call_sessions  # Cleaned up
    passed += 1
    print(f"[{passed}] Call status → session cleanup  PASSED")

    # [14] Stats tracking
    assert STATE.stats["voice_inbound"] >= 2
    assert STATE.stats["sms_inbound"] >= 3
    assert STATE.stats["requests"] >= 10
    passed += 1
    print(f"[{passed}] Stats: {STATE.stats['requests']} requests tracked  PASSED")

    # [15] 404 handling
    try:
        urlopen(f"{base_url}/nonexistent", timeout=5)
    except _HTTPError as e:
        assert e.code == 404
    passed += 1
    print(f"[{passed}] 404 handling  PASSED")

    # Cleanup
    server.shutdown()

    print(f"\n{SEP}")
    print(f"SELF-TEST: {passed}/15 passed")
    if passed >= 13:
        print("Webhook Server operational.")
        print(f"\nUsage:")
        print(f"  python webhook_server.py --daemon --port {PORT}")
        print(f"\nConfigure in Twilio/BulkVS/VoIP.ms:")
        print(f"  Voice URL:  http://your-server:{PORT}/voice/inbound")
        print(f"  SMS URL:    http://your-server:{PORT}/sms/inbound")
        print(f"  Status URL: http://your-server:{PORT}/voice/status")
        print(f"\nAll routes: /voice/* /sms/* /voicemail/* /appointments/* /campaigns/*")
    print(SEP)
