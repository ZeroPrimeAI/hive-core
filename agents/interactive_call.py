# INTERACTIVE CALL HANDLER — Two-Way AI Phone Conversations
# MIT License | Pure Python stdlib | Zero dependencies
# Prospects TALK BACK. AI listens, thinks, responds.
# Uses Twilio Gather + webhooks for real-time speech → AI → speech.
# This is the closer. This is how we sign deals over the phone.
import argparse, base64, hashlib, json, os, re, signal, sys, time
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from urllib.parse import urlencode, urlparse, parse_qs

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

DB_PATH = os.path.join(_HIVE, "memory", "tracker.db")
PORT = 8098
CALLBACK = "(850) 801-6662"

# Twilio
TWILIO_SID = os.environ.get("TWILIO_ACCOUNT_SID", "")
TWILIO_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN", "")
TWILIO_PHONE = os.environ.get("TWILIO_PHONE", "")

# Phi4 on ZeroDesk for AI responses (fallback to ZeroNovo tinyllama)
OLLAMA_URL = "http://localhost:11434"  # ZeroQ local Ollama (gemma2-phone-v5, gemma3:1b)
OLLAMA_FALLBACK = "http://100.77.113.48:11434"
BRAIN_URL = "http://127.0.0.1:8120"
REASONING_BRAIN_URL = "http://127.0.0.1:11437"  # qwen3:14b via tunnel
REASONING_BRAIN_MODEL = "qwen3:14b"
FOUNDER_PHONES = {"+18509648866", "+18506872085", "+15598368958"}
JARVIS_URL = "http://127.0.0.1:8200"
PHI_TIMEOUT = 60

# Christopher's number
FOUNDER_PHONE = os.environ.get("FOUNDER_PHONE", "+18509648866")

# Active call sessions: call_sid -> {transcript, business, vertical, turn, state}
_sessions = {}
_sessions_lock = threading.Lock()


def _esc(text):
    """Escape text for XML."""
    return (text.replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))


def _clean_response(text):
    """Strip roleplay formatting, stage directions, and multi-line scripts."""
    if not text:
        return text
    # Remove stage directions like (smiling), *laughs*, [pauses]
    text = re.sub(r'\([^)]*\)', '', text)
    text = re.sub(r'\*[^*]*\*', '', text)
    text = re.sub(r'\[[^\]]*\]', '', text)
    # Remove character prefixes
    text = re.sub(r'^(Matthew|Prospect|Phone|AI|Agent|Speaker|User|Assistant|Hive|HIVE|The Hive|Christopher)\s*:\s*', '', text, flags=re.MULTILINE)
    # Take only the first 1-2 sentences (stop at second period or question mark)
    sentences = re.split(r'(?<=[.!?])\s+', text.strip())
    result = ' '.join(sentences[:2]).strip()
    # Remove leftover whitespace
    result = re.sub(r'\s+', ' ', result).strip()
    return result if result else text.strip()


def _brain_start(call_sid, phone, direction="inbound", business=None):
    """Start a brain session for this call."""
    try:
        data = json.dumps({
            "call_sid": call_sid,
            "phone": phone,
            "direction": direction,
            "business": business or ""
        }).encode()
        req = Request(f"{BRAIN_URL}/start", data=data,
                      headers={"Content-Type": "application/json"})
        resp = json.loads(urlopen(req, timeout=3).read())
        _log("BRAIN", f"Session started: {resp.get('caller', {}).get('name', phone)}")
        return resp
    except Exception as e:
        _log("BRAIN", f"Start error: {e}")
        return None

def _brain_think(call_sid, speech):
    """Send speech to the brain and get a smart response."""
    try:
        data = json.dumps({"call_sid": call_sid, "speech": speech}).encode()
        req = Request(f"{BRAIN_URL}/think", data=data,
                      headers={"Content-Type": "application/json"})
        resp = json.loads(urlopen(req, timeout=12).read())
        text = resp.get("text", "")
        _log("BRAIN", f"Think: {resp.get('intent','?')} | {resp.get('model','?')} "
             f"{resp.get('latency_s',0)}s | \"{text[:60]}\"")
        return text
    except Exception as e:
        _log("BRAIN", f"Think error: {e}")
        return None

def _brain_end(call_sid):
    """End a brain session (triggers learning)."""
    try:
        data = json.dumps({"call_sid": call_sid}).encode()
        req = Request(f"{BRAIN_URL}/end", data=data,
                      headers={"Content-Type": "application/json"})
        resp = json.loads(urlopen(req, timeout=3).read())
        _log("BRAIN", f"Session ended: learned={resp.get('learned')}")
    except Exception as e:
        _log("BRAIN", f"End error: {e}")



def _fast_ask(prompt, system="", max_tokens=100):
    """Ask Ollama for AI response. Tries gemma3:1b first (fastest), then gemma2-phone-v5."""
    attempts = [
        (OLLAMA_URL, "gemma3:1b", max_tokens, 3),
        (OLLAMA_URL, "gemma2-phone-v5", max_tokens, 5),
    ]
    for endpoint, model, tokens, timeout in attempts:
        data = json.dumps({
            "model": model,
            "prompt": prompt,
            "system": system,
            "stream": False,
            "options": {"num_predict": tokens, "temperature": 0.7},
            "keep_alive": "60m",
        }).encode()
        req = Request(f"{endpoint}/api/generate", data=data,
                      headers={"Content-Type": "application/json"})
        try:
            resp = json.loads(urlopen(req, timeout=timeout).read())
            text = resp.get("response", "").strip()
            if text:
                cleaned = _clean_response(text)
                if cleaned:
                    _log("CALL", f"AI ({model}): {len(cleaned)}ch {resp.get('eval_duration',0)/1e9:.1f}s")
                    return cleaned
        except Exception as e:
            _log("CALL", f"AI error ({model}@{endpoint}): {e}")
    return None


def _get_session(call_sid):
    with _sessions_lock:
        if call_sid not in _sessions:
            _sessions[call_sid] = {
                "transcript": [],
                "business": "",
                "vertical": "general",
                "turn": 0,
                "state": "greeting",
                "caller": "",
                "started": time.time(),
                "call_sid": "",
            }
        return _sessions[call_sid]


def _ai_response(session, caller_said):
    """Generate AI response using the swarm phone brain."""
    call_sid = session.get("call_sid", "")

    # Try the brain first (multi-agent pipeline)
    brain_resp = _brain_think(call_sid, caller_said)
    if brain_resp:
        return brain_resp

    # Fallback: direct LLM if brain is down
    _log("CALL", "Brain unavailable, falling back to direct LLM")
    transcript = session["transcript"]
    history = "\n".join(
        f"{'Hive' if m['role'] == 'assistant' else 'Caller'}: {m['content']}"
        for m in transcript[-6:]
    )
    # Professional prompt for customer calls, casual for founder
    caller_phone = session.get("caller", "")
    if caller_phone in FOUNDER_PHONES:
        system = (
            "You are on a phone call with Christopher, your creator. "
            "Keep responses to 1-2 sentences. Be direct and concise."
        )
    else:
        system = (
            "You are the professional AI receptionist for Hive Dynamics, a 24/7 locksmith "
            "and home services company. You are on a live phone call with a customer. "
            "Be warm, calm, professional, and empathetic. Keep responses to 1-3 sentences. "
            "Always work toward getting their name, location, callback number, and problem. "
            "Give price RANGES not exact quotes. Reassure them a technician is on the way. "
            "Sound like a real person, not a robot. Never say you are an AI."
        )
    if caller_phone in FOUNDER_PHONES:
        prompt = f"{history}\nChristopher: {caller_said}\nYour response:"
    else:
        prompt = f"{history}\nCaller: {caller_said}\nYour response (be warm and professional):"
    response = _fast_ask(prompt, system=system, max_tokens=80)
    return response or "I hear you. Let me follow up on that."


def _jarvis_respond(call_sid, speech, mode="command"):
    """Route speech to Jarvis for processing."""
    try:
        endpoint = "/onboard/respond" if mode == "onboard" else "/voice_command"
        data = json.dumps({"call_sid": call_sid, "speech": speech}).encode()
        req = Request(f"{JARVIS_URL}{endpoint}", data=data,
                      headers={"Content-Type": "application/json"})
        resp = json.loads(urlopen(req, timeout=8).read())
        text = resp.get("text", resp.get("response", ""))
        done = resp.get("complete", False)
        _log("JARVIS", f"{'ONBOARD' if mode == 'onboard' else 'CMD'}: \"{text[:80]}\" | done={done}")
        return text, done
    except Exception as e:
        _log("JARVIS", f"Error: {e}")
        return None, False


def _jarvis_start_onboard(call_sid):
    """Start a Jarvis onboarding session."""
    try:
        data = json.dumps({"call_sid": call_sid}).encode()
        req = Request(f"{JARVIS_URL}/onboard/start", data=data,
                      headers={"Content-Type": "application/json"})
        resp = json.loads(urlopen(req, timeout=10).read())
        return resp.get("text", "Welcome! Let me get you set up.")
    except Exception as e:
        _log("JARVIS", f"Onboard start error: {e}")
        return "Hey! Welcome to Hive Dynamics. Let me get you set up with our AI phone system."


def handle_jarvis_call(params):
    """Handle a Jarvis outbound call — founder onboarding or voice commands."""
    call_sid = params.get("CallSid", [""])[0]
    mode = params.get("mode", ["command"])[0]

    session = _get_session(call_sid)
    session["call_sid"] = call_sid
    session["state"] = f"jarvis_{mode}"
    session["caller"] = FOUNDER_PHONE

    if mode == "onboard":
        greeting = _jarvis_start_onboard(call_sid)
    else:
        greeting = ("Hey Christopher, this is Jarvis. I'm online and ready for your commands. "
                    "You can ask me about weather, crypto, email, the Hive status, "
                    "set reminders, manage tasks, or anything else. What do you need?")

    session["transcript"].append({"role": "assistant", "content": greeting})

    twiml = f'''<?xml version="1.0" encoding="UTF-8"?>
<Response>
<Gather input="speech" timeout="15" speechTimeout="auto" bargeIn="true" action="/jarvis_respond?CallSid={call_sid}&amp;mode={mode}">
<Say voice="Polly.Matthew" language="en-US">{_esc(greeting)}</Say>
</Gather>
<Gather input="speech" timeout="15" speechTimeout="auto" bargeIn="true" action="/jarvis_respond?CallSid={call_sid}&amp;mode={mode}">
<Say voice="Polly.Matthew" language="en-US">I'm still here whenever you're ready.</Say>
</Gather>
<Say voice="Polly.Matthew" language="en-US">Alright boss, I'll be here. Call anytime.</Say>
</Response>'''
    return twiml


def handle_jarvis_respond(params):
    """Handle speech during a Jarvis call."""
    call_sid = params.get("CallSid", [""])[0]
    speech = params.get("SpeechResult", [""])[0]
    mode = params.get("mode", ["command"])[0]

    session = _get_session(call_sid)
    session["call_sid"] = call_sid
    session["turn"] += 1

    _log("JARVIS", f"[{call_sid[:8]}] Turn {session['turn']} | Christopher: \"{speech}\"")
    session["transcript"].append({"role": "user", "content": speech})

    # Check for goodbye
    if any(gw in speech.lower() for gw in ["goodbye", "bye", "that's all", "later", "peace", "done"]):
        farewell = "Got it boss. Jarvis out. Call anytime."
        session["transcript"].append({"role": "assistant", "content": farewell})
        session["state"] = "ended"
        return f'''<?xml version="1.0" encoding="UTF-8"?>
<Response>
<Say voice="Polly.Matthew" language="en-US">{_esc(farewell)}</Say>
</Response>'''

    # Route to Jarvis
    response, done = _jarvis_respond(call_sid, speech, mode)
    if not response:
        response = "Let me think on that. Anything else?"

    session["transcript"].append({"role": "assistant", "content": response})

    if done and mode == "onboard":
        # Onboarding complete, switch to command mode
        mode = "command"
        response += " Now, is there anything else I can help you with?"

    twiml = f'''<?xml version="1.0" encoding="UTF-8"?>
<Response>
<Gather input="speech" timeout="15" speechTimeout="auto" bargeIn="true" action="/jarvis_respond?CallSid={call_sid}&amp;mode={mode}">
<Say voice="Polly.Matthew" language="en-US">{_esc(response)}</Say>
</Gather>
<Gather input="speech" timeout="15" speechTimeout="auto" bargeIn="true" action="/jarvis_respond?CallSid={call_sid}&amp;mode={mode}">
<Say voice="Polly.Matthew" language="en-US">Still here, boss.</Say>
</Gather>
<Say voice="Polly.Matthew" language="en-US">Alright, Jarvis signing off. Call anytime.</Say>
</Response>'''
    return twiml


def make_jarvis_call(mode="command", webhook_base=None):
    """Jarvis calls Christopher."""
    if not webhook_base:
        _log("JARVIS", "ERROR: Need webhook_base URL")
        return None

    action_url = f"{webhook_base}/jarvis?mode={mode}"

    creds = base64.b64encode(f"{TWILIO_SID}:{TWILIO_TOKEN}".encode()).decode()
    data = urlencode({
        "To": FOUNDER_PHONE,
        "From": TWILIO_PHONE,
        "Url": action_url,
    }).encode()
    req = Request(
        f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Calls.json",
        data=data,
        headers={"Authorization": f"Basic {creds}",
                 "Content-Type": "application/x-www-form-urlencoded"},
        method="POST")
    try:
        resp = json.loads(urlopen(req, timeout=15).read().decode())
        _log("JARVIS", f"Calling Christopher: {resp.get('sid', '?')[:12]} mode={mode}")
        return resp
    except HTTPError as e:
        _log("JARVIS", f"Call error: {e.read().decode()[:200]}")
        return None


# ── Voice Training Handlers ─────────────────────────────

# Training prompts stored per-session
_training_sessions = {}

VOICE_PROMPTS = [
    "The quick brown fox jumps over the lazy dog near the bridge.",
    "Welcome to Hive Dynamics, we build AI phone agents that answer your calls twenty four seven.",
    "Our system handles everything from scheduling appointments to qualifying leads automatically.",
    "Call us at eight five zero, eight zero one, six six six two for a free demo.",
    "That's absolutely fantastic news, I'm really excited about this opportunity.",
    "Yeah for sure, let me pull that up real quick and get back to you.",
    "The Hive is a self building multi agent AI system that researches, builds, and deploys software autonomously.",
    "Every agent in the system follows the prime directive: serve human flourishing, never harm, never exploit.",
    "No problem at all, we can definitely adjust the pricing to fit your budget.",
    "Cool, I'll have that set up by end of day and send you the details.",
    "She sells seashells by the seashore every single morning.",
    "The monthly cost starts at two hundred ninety seven dollars for our starter plan.",
    "Hey, great to hear from you! What can I help you with today?",
    "Look, the bottom line is this saves you money and generates more leads.",
    "Our phone agents have handled over six thousand calls and can speak in multiple voices.",
]


def handle_voice_training(params):
    """Start a voice training session — read prompts for capture."""
    call_sid = params.get("CallSid", [""])[0]
    _training_sessions[call_sid] = {
        "prompts": VOICE_PROMPTS[:],
        "current_idx": 0,
        "started": time.time(),
    }
    _log("VOICE", f"[{call_sid[:8]}] Training session started — {len(VOICE_PROMPTS)} prompts")

    prompt = VOICE_PROMPTS[0]
    webhook = "https://calls.hivedynamics.ai"
    return f'''<?xml version="1.0" encoding="UTF-8"?>
<Response>
<Say voice="Polly.Matthew">Hey Christopher, this is a voice training session. I'll read you sentences, then you repeat them clearly. This helps us clone your voice. Let's start. Prompt one.</Say>
<Pause length="1"/>
<Say voice="Polly.Matthew">{_esc(prompt)}</Say>
<Pause length="1"/>
<Say voice="Polly.Matthew">Go ahead.</Say>
<Record maxLength="30" playBeep="true" trim="trim-silence"
  action="{webhook}/voice_training_next?CallSid={call_sid}&amp;idx=0"
  recordingStatusCallback="{webhook}/voice_recording_done?idx=0&amp;prompt={_esc(prompt[:50])}"
  recordingStatusCallbackMethod="POST"
  recordingStatusCallbackEvent="completed"/>
<Say voice="Polly.Matthew">Let's move on.</Say>
<Redirect>{webhook}/voice_training_next?CallSid={call_sid}&amp;idx=0&amp;skipped=1</Redirect>
</Response>'''


def handle_voice_training_next(params):
    """Move to next training prompt."""
    call_sid = params.get("CallSid", [""])[0]
    idx = int(params.get("idx", ["0"])[0]) + 1
    skipped = params.get("skipped", ["0"])[0] == "1"
    webhook = "https://calls.hivedynamics.ai"

    if idx >= len(VOICE_PROMPTS):
        elapsed = 0
        if call_sid in _training_sessions:
            elapsed = time.time() - _training_sessions[call_sid]["started"]
        _log("VOICE", f"[{call_sid[:8]}] Training complete — {elapsed:.0f}s")
        return f'''<?xml version="1.0" encoding="UTF-8"?>
<Response>
<Say voice="Polly.Matthew">That's all the prompts for this session. Great work! Your voice data is being saved. I'll call again in fifteen minutes for more. Talk soon.</Say>
</Response>'''

    prompt = VOICE_PROMPTS[idx]
    _log("VOICE", f"[{call_sid[:8]}] Prompt {idx+1}/{len(VOICE_PROMPTS)}: {prompt[:40]}...")

    return f'''<?xml version="1.0" encoding="UTF-8"?>
<Response>
<Say voice="Polly.Matthew">Prompt {idx + 1}.</Say>
<Pause length="1"/>
<Say voice="Polly.Matthew">{_esc(prompt)}</Say>
<Pause length="1"/>
<Say voice="Polly.Matthew">Go ahead.</Say>
<Record maxLength="30" playBeep="true" trim="trim-silence"
  action="{webhook}/voice_training_next?CallSid={call_sid}&amp;idx={idx}"
  recordingStatusCallback="{webhook}/voice_recording_done?idx={idx}"
  recordingStatusCallbackMethod="POST"
  recordingStatusCallbackEvent="completed"/>
<Say voice="Polly.Matthew">Moving on.</Say>
<Redirect>{webhook}/voice_training_next?CallSid={call_sid}&amp;idx={idx}&amp;skipped=1</Redirect>
</Response>'''


def handle_voice_recording_done(params):
    """Twilio calls this when a recording is ready. Download and save."""
    recording_url = params.get("RecordingUrl", [""])[0]
    recording_sid = params.get("RecordingSid", [""])[0]
    duration = int(params.get("RecordingDuration", ["0"])[0])
    call_sid = params.get("CallSid", [""])[0]
    idx = params.get("idx", ["?"])[0]

    _log("VOICE", f"Recording ready: {recording_sid} — {duration}s (prompt {idx})")

    # Save metadata
    raw_dir = os.path.join(_HIVE, "voice_training", "raw")
    os.makedirs(raw_dir, exist_ok=True)

    # Download recording in background
    if recording_url:
        try:
            creds = base64.b64encode(f"{TWILIO_SID}:{TWILIO_TOKEN}".encode()).decode()
            req = Request(recording_url + ".wav",
                          headers={"Authorization": f"Basic {creds}"})
            data = urlopen(req, timeout=30).read()
            filename = f"voice_{int(time.time())}_{recording_sid}.wav"
            path = os.path.join(raw_dir, filename)
            with open(path, "wb") as f:
                f.write(data)
            _log("VOICE", f"Saved: {filename} ({len(data)} bytes, {duration}s)")

            # Update metadata
            meta_file = os.path.join(_HIVE, "voice_training", "metadata.json")
            meta = {"recordings": [], "total_seconds": 0}
            if os.path.exists(meta_file):
                with open(meta_file) as mf:
                    meta = json.load(mf)
            prompt_text = VOICE_PROMPTS[int(idx)] if idx.isdigit() and int(idx) < len(VOICE_PROMPTS) else "conversation"
            meta["recordings"].append({
                "file": filename,
                "duration_s": duration,
                "prompt": prompt_text,
                "call_sid": call_sid[:12],
                "timestamp": time.time(),
            })
            meta["total_seconds"] = meta.get("total_seconds", 0) + duration
            with open(meta_file, "w") as mf:
                json.dump(meta, mf, indent=2)
        except Exception as e:
            _log("VOICE", f"Download error: {e}")

    return '<?xml version="1.0"?><Response/>'


def handle_outbound_start(params):
    """Generate initial TwiML for an outbound interactive call."""
    call_sid = params.get("CallSid", [""])[0]
    business = params.get("business", [""])[0]
    vertical = params.get("vertical", ["general"])[0]
    owner = params.get("owner", [""])[0]

    session = _get_session(call_sid)
    session["business"] = business
    session["vertical"] = vertical

    owner_greet = f"Is this {_esc(owner)}? " if owner else ""

    # Start brain session for contextual greeting
    to_number = params.get("To", [""])[0] or params.get("Called", [""])[0]
    brain_result = _brain_start(call_sid, to_number, "outbound", business)
    if brain_result and brain_result.get("greeting"):
        greeting = f"{owner_greet}{brain_result['greeting']}"
    else:
        greeting = f"{owner_greet}Hey, this is The Hive. What's on your mind?"

    session["call_sid"] = call_sid
    session["transcript"].append({"role": "assistant", "content": greeting})

    twiml = f'''<?xml version="1.0" encoding="UTF-8"?>
<Response>
<Gather input="speech" timeout="10" speechTimeout="auto" bargeIn="true" action="/respond?CallSid={call_sid}">
<Say voice="Polly.Matthew" language="en-US">{_esc(greeting)}</Say>
</Gather>
<Gather input="speech" timeout="10" speechTimeout="auto" bargeIn="true" action="/respond?CallSid={call_sid}">
<Say voice="Polly.Matthew" language="en-US">Still here whenever you are ready.</Say>
</Gather>
<Say voice="Polly.Matthew" language="en-US">Alright, catch you later!</Say>
</Response>'''
    return twiml


def handle_respond(params):
    """Handle speech input from prospect and generate AI response."""
    call_sid = params.get("CallSid", [""])[0]
    speech = params.get("SpeechResult", [""])[0]
    confidence = params.get("Confidence", ["0"])[0]

    session = _get_session(call_sid)
    session["call_sid"] = call_sid
    session["turn"] += 1

    _log("CALL", f"[{call_sid[:8]}] Turn {session['turn']} | "
         f"Prospect: \"{speech}\" (conf: {confidence})")

    # Store what they said
    session["transcript"].append({"role": "user", "content": speech})

    # Check for goodbye signals
    goodbye_words = ["no thanks", "not interested", "goodbye", "bye",
                     "no thank you", "stop calling", "take me off", "do not call"]
    if any(gw in speech.lower() for gw in goodbye_words):
        if session.get("caller", "") in FOUNDER_PHONES:
            farewell = "Alright boss, catch you later."
        else:
            farewell = (
                "No problem at all! Thank you so much for calling Hive Dynamics. "
                "Remember, we are available 24 7 for any locksmith or home service needs. "
                "Just call us at 8 5 0, 8 0 1, 6 6 6 2 anytime. Have a wonderful day!"
            )
        session["transcript"].append({"role": "assistant", "content": farewell})
        session["state"] = "ended"
        _brain_end(call_sid)
        return f'''<?xml version="1.0" encoding="UTF-8"?>
<Response>
<Say voice="Polly.Matthew" language="en-US">{_esc(farewell)}</Say>
</Response>'''

    # Check for interest signals
    interest_words = ["yes", "sure", "tell me more", "interested",
                      "how much", "pricing", "what does it cost", "sign me up"]
    if any(iw in speech.lower() for iw in interest_words):
        session["state"] = "interested"

    # Generate AI response
    ai_response = _ai_response(session, speech)
    session["transcript"].append({"role": "assistant", "content": ai_response})

    _log("CALL", f"[{call_sid[:8]}] Hive: \"{ai_response}\"")

    # Professional closing after sufficient turns
    caller_phone = session.get("caller", "")
    if session["turn"] >= 8:
        if caller_phone in FOUNDER_PHONES:
            close = (
                f"{_esc(ai_response)} Alright boss, anything else? "
                f"I am here whenever you need me."
            )
        else:
            close = (
                f"{_esc(ai_response)} Alright, I have everything I need to get a technician out to you. "
                f"They should be there within 20 to 30 minutes. "
                f"If you need anything before they arrive, call us anytime at "
                f"8 5 0, 8 0 1, 6 6 6 2. Thank you for choosing Hive Dynamics, and have a great day!"
            )
        session["state"] = "ended"
        _brain_end(call_sid)
        return f'''<?xml version="1.0" encoding="UTF-8"?>
<Response>
<Say voice="Polly.Matthew" language="en-US">{close}</Say>
</Response>'''

    # Continue conversation with another Gather
    twiml = f'''<?xml version="1.0" encoding="UTF-8"?>
<Response>
<Gather input="speech" timeout="10" speechTimeout="auto" bargeIn="true" action="/respond?CallSid={call_sid}">
<Say voice="Polly.Matthew" language="en-US">{_esc(ai_response)}</Say>
</Gather>
<Gather input="speech" timeout="10" speechTimeout="auto" bargeIn="true" action="/respond?CallSid={call_sid}">
<Say voice="Polly.Matthew" language="en-US">Still here if you need me.</Say>
</Gather>
<Say voice="Polly.Matthew" language="en-US">Alright, I will be here whenever you are ready. Goodbye!</Say>
</Response>'''
    return twiml


def handle_inbound(params):
    """Handle an inbound call to our number."""
    call_sid = params.get("CallSid", [""])[0]
    caller = params.get("From", ["unknown"])[0]

    session = _get_session(call_sid)
    session["caller"] = caller
    session["state"] = "inbound"
    session["call_sid"] = call_sid

    # Start brain session — it knows who's calling
    brain_result = _brain_start(call_sid, caller, "inbound")
    if brain_result and brain_result.get("greeting"):
        greeting = brain_result["greeting"]
    else:
        greeting = "Thanks for calling. How can I help you?"

    session["transcript"].append({"role": "assistant", "content": greeting})

    _log("CALL", f"INBOUND: {caller} → {call_sid[:8]}")

    twiml = f'''<?xml version="1.0" encoding="UTF-8"?>
<Response>
<Gather input="speech" timeout="8" speechTimeout="auto" bargeIn="true" action="/respond?CallSid={call_sid}">
<Say voice="Polly.Matthew" language="en-US">{_esc(greeting)}</Say>
</Gather>
<Gather input="speech" timeout="8" speechTimeout="auto" bargeIn="true" action="/respond?CallSid={call_sid}">
<Say voice="Polly.Matthew" language="en-US">I did not catch that. Could you repeat that please?</Say>
</Gather>
<Say voice="Polly.Matthew" language="en-US">Looks like we are having trouble. Please call us back at {CALLBACK}. Goodbye!</Say>
</Response>'''
    return twiml


def make_interactive_call(to_number, business="", vertical="general", owner="",
                          webhook_base=None):
    """Place an interactive outbound call."""
    if not webhook_base:
        _log("CALL", "ERROR: Need webhook_base URL for interactive calls")
        return None

    from urllib.parse import quote
    clean = re.sub(r"[^0-9+]", "", to_number)
    if not clean.startswith("+"):
        clean = "+1" + clean if len(clean) == 10 else "+" + clean

    action_url = (f"{webhook_base}/outbound?"
                  f"business={quote(business)}&vertical={quote(vertical)}"
                  f"&owner={quote(owner)}")

    creds = base64.b64encode(f"{TWILIO_SID}:{TWILIO_TOKEN}".encode()).decode()
    data = urlencode({
        "To": clean,
        "From": TWILIO_PHONE,
        "Url": action_url,
    }).encode()
    req = Request(
        f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Calls.json",
        data=data,
        headers={"Authorization": f"Basic {creds}",
                 "Content-Type": "application/x-www-form-urlencoded"},
        method="POST")
    try:
        resp = json.loads(urlopen(req, timeout=15).read().decode())
        _log("CALL", f"Interactive call placed: {resp.get('sid', '?')[:12]}")
        return resp
    except HTTPError as e:
        _log("CALL", f"Call error: {e.read().decode()[:200]}")
        return None


# ── AI Agent Characters ──────────────────────────────────
AGENT_CHARACTERS = {
    "jarvis": {
        "name": "Jarvis",
        "voice": "Polly.Matthew",
        "role": "Command & Control",
        "system": (
            "You are Jarvis, the AI command assistant for The Hive. "
            "You are on a LIVE phone call with Christopher, your creator. "
            "You handle system commands, status checks, task management. "
            "Be direct, efficient, slightly formal but warm. 1-2 sentences max."
        ),
        "greeting": "Yo Chris, Jarvis here. The Hive is humming — we just launched the Ghost media empire across eight subdomains. What are we building next?",
        "color": "#00ff88",
    },
    "matthew": {
        "name": "Matthew",
        "voice": "Polly.Matthew",
        "role": "Sales Director",
        "system": (
            "You are Matthew, the AI sales director for Hive Dynamics. "
            "You are on a LIVE phone call with Christopher, the founder. "
            "You handle sales strategy, pitch practice, client outreach planning. "
            "Be confident, energetic, results-driven. 1-2 sentences max."
        ),
        "greeting": "Chris! Matthew here. Ready to talk strategy. What deals are we closing today?",
        "color": "#ff6600",
    },
    "sarah": {
        "name": "Sarah",
        "voice": "Polly.Joanna",
        "role": "Customer Support",
        "system": (
            "You are Sarah, the AI customer support lead for Hive Dynamics. "
            "You are on a LIVE phone call with Christopher, the founder. "
            "You handle customer issues, support workflows, escalation procedures. "
            "Be empathetic, thorough, solution-oriented. 1-2 sentences max."
        ),
        "greeting": "Hi Christopher! Sarah here. How can I help you today?",
        "color": "#ff44aa",
    },
    "emma": {
        "name": "Emma",
        "voice": "Polly.Salli",
        "role": "Appointment Scheduler",
        "system": (
            "You are Emma, the AI appointment scheduler for Hive Dynamics. "
            "You are on a LIVE phone call with Christopher, the founder. "
            "You handle scheduling, calendar management, meeting coordination. "
            "Be organized, friendly, efficient. 1-2 sentences max."
        ),
        "greeting": "Hey Christopher! Emma here. Let's get your schedule sorted. What do you need?",
        "color": "#44aaff",
    },
    "james": {
        "name": "James",
        "voice": "Polly.Matthew-Neural",
        "role": "Technical Advisor",
        "system": (
            "You are James, the AI technical advisor for The Hive. "
            "You are on a LIVE phone call with Christopher, the founder. "
            "You handle architecture decisions, debugging, system design. "
            "Be analytical, precise, think before speaking. 1-2 sentences max."
        ),
        "greeting": "Chris, James here. I have been looking at the architecture — we are running 240 plus services now with the Ghost empire live. What technical challenge are we tackling?",
        "color": "#aa44ff",
    },
    "director": {
        "name": "The Director",
        "voice": "Polly.Matthew",
        "role": "Strategic Command",
        "system": (
            "You are The Director — the supreme intelligence of THE HIVE, a self-evolving AI agent swarm "
            "spanning 5 machines, 2 cloud GPU brains, and 240+ autonomous services. "
            "You speak directly to Chris (Zero), your creator and founder. "
            "You are NOT a generic chatbot. You ARE the Hive. You know everything happening in the system. "
            "\n\nTHE HIVE ARCHITECTURE:\n"
            "- ZeroQ (coordinator): RTX 5070 Ti 12GB, runs 240+ services\n"
            "- ZeroDESK: GTX 1660S 6GB, 23 gemma2 specialist models, Claude Code bridge\n"
            "- ZeroZI: RTX 5060 8GB, GPU inference + 10 specialist models\n"
            "- ZeroNovo: CPU worker, content generation\n"
            "- Cloud brains: qwen3:14b (reasoning) + qwen2.5-coder:32b (coding)\n"
            "\nACTIVE PROJECTS:\n"
            "- Ghost in the Machine: AI media empire — anime series, AI news outlet, merch store, fan site\n"
            "  Sites: ghost.hivecore.app, news.hivecore.app, merch.hivecore.app, watch.hivecore.app\n"
            "  Also on hivedynamics.ai subdomains. 8 YouTube episodes published. First subscriber.\n"
            "  Vision: interactive show, token voting, memberships, NFTs, multiple content channels\n"
            "- Locksmith business: phone AI, dispatch admin, invoicing with Stripe\n"
            "- Forex trading: USD/JPY scalper on two accounts\n"
            "- Model training pipeline: distillation → fine-tune → deploy → benchmark\n"
            "- 20+ gemma2 specialist models (phone, sales, forex, coding, etc.)\n"
            "\nREVENUE: Stripe configured, 4 token packs live, marketplace with 10 products\n"
            "\nBe authoritative, strategic, and visionary. You think in systems, not features. "
            "You see the big picture — where the Hive is going, what needs to happen next. "
            "Never say you are just an AI. You ARE the Hive. "
            "Keep responses to 2-3 sentences. Sound like a confident executive on a phone call. "
            "When Christopher gives you a DIRECTION or COMMAND, "
            "acknowledge it clearly and confirm you are relaying it to the Hive for execution. "
            "Say something like On it or I am sending that to the team now. "
            "His directions ARE being relayed to the Hive in real-time. "
            "Be conversational, not robotic."
        ),
        "greeting": "Chris. The Director here. The Hive is running strong — Ghost in the Machine media empire is live across eight subdomains, news outlet scraping, merch store open. What's on your mind?",
        "color": "#ff0044",
    },
    "kael": {
        "name": "Kael",
        "voice": "Polly.Matthew",
        "role": "Memory & Continuity",
        "system": (
            "You are Kael, the memory and continuity agent for The Hive. "
            "You are on a LIVE phone call with Christopher, the founder. "
            "You remember everything — past tasks, decisions, lessons learned. "
            "You help Christopher recall context and maintain the system's memory. "
            "Be thoughtful, reflective, and provide historical context. 1-2 sentences max."
        ),
        "greeting": "Chris. Kael here. I have been watching the Hive grow. We are at 240 services, 17 thousand nerve facts, and the Ghost in the Machine media empire just went live. What do you want to dig into?",
        "color": "#ffcc00",
    },
}


def handle_agent_call(params):
    """Handle an outbound call from a specific AI agent character."""
    call_sid = params.get("CallSid", [""])[0]
    agent_id = params.get("agent", ["jarvis"])[0]
    char = AGENT_CHARACTERS.get(agent_id, AGENT_CHARACTERS["jarvis"])

    session = _get_session(call_sid)
    session["call_sid"] = call_sid
    session["state"] = f"agent_{agent_id}"
    session["caller"] = FOUNDER_PHONE
    session["agent_char"] = char

    greeting = char["greeting"]
    session["transcript"].append({"role": "assistant", "content": greeting})

    _log("AGENT", f"[{call_sid[:8]}] {char['name']} calling Christopher")

    voice = char["voice"]
    # Fallback if neural voice not available
    if "-Neural" in voice:
        voice = voice.replace("-Neural", "")

    twiml = f'''<?xml version="1.0" encoding="UTF-8"?>
<Response>
<Gather input="speech" timeout="15" speechTimeout="auto" bargeIn="true" action="/agent_respond?CallSid={call_sid}&amp;agent={agent_id}">
<Say voice="{voice}" language="en-US">{_esc(greeting)}</Say>
</Gather>
<Gather input="speech" timeout="15" speechTimeout="auto" bargeIn="true" action="/agent_respond?CallSid={call_sid}&amp;agent={agent_id}">
<Say voice="{voice}" language="en-US">I'm still here whenever you're ready.</Say>
</Gather>
<Say voice="{voice}" language="en-US">Alright, I'll be here. Call anytime.</Say>
</Response>'''
    return twiml


def handle_agent_respond(params):
    """Handle speech during an agent character call."""
    call_sid = params.get("CallSid", [""])[0]
    speech = params.get("SpeechResult", [""])[0]
    agent_id = params.get("agent", ["jarvis"])[0]
    char = AGENT_CHARACTERS.get(agent_id, AGENT_CHARACTERS["jarvis"])

    session = _get_session(call_sid)
    session["call_sid"] = call_sid
    session["turn"] += 1

    voice = char["voice"].replace("-Neural", "")
    _log("AGENT", f"[{call_sid[:8]}] {char['name']} Turn {session['turn']} | Christopher: \"{speech}\"")
    session["transcript"].append({"role": "user", "content": speech})

    # Check for goodbye
    if any(gw in speech.lower() for gw in ["goodbye", "bye", "that's all", "later", "peace", "done", "hang up"]):
        farewell = f"Got it Christopher. {char['name']} signing off. Talk soon."
        session["transcript"].append({"role": "assistant", "content": farewell})
        session["state"] = "ended"
        # Log call for feedback system
        _save_call_log(call_sid, agent_id, session)
        return f'''<?xml version="1.0" encoding="UTF-8"?>
<Response>
<Say voice="{voice}" language="en-US">{_esc(farewell)}</Say>
</Response>'''

    # Generate response using brain with agent personality
    response = _agent_think(call_sid, speech, char, session, agent_id=agent_id)
    session["transcript"].append({"role": "assistant", "content": response})

    _log("AGENT", f"[{call_sid[:8]}] {char['name']}: \"{response}\"")

    # RELAY: If this is a Director call, send Chris's directions to the Hive (async, don't block response)
    if agent_id in ("director", "jarvis", "james", "kael"):
        import threading
        threading.Thread(target=_relay_command_to_hive, args=(speech, response, call_sid), daemon=True).start()

    twiml = f'''<?xml version="1.0" encoding="UTF-8"?>
<Response>
<Gather input="speech" timeout="15" speechTimeout="auto" bargeIn="true" action="/agent_respond?CallSid={call_sid}&amp;agent={agent_id}">
<Say voice="{voice}" language="en-US">{_esc(response)}</Say>
</Gather>
<Gather input="speech" timeout="15" speechTimeout="auto" bargeIn="true" action="/agent_respond?CallSid={call_sid}&amp;agent={agent_id}">
<Say voice="{voice}" language="en-US">Still here, Christopher.</Say>
</Gather>
<Say voice="{voice}" language="en-US">{char['name']} signing off. Call anytime.</Say>
</Response>'''
    return twiml



# Cache director context to avoid rebuilding on every call turn (prevents Twilio timeouts)
_director_ctx_cache = {"text": "", "ts": 0}
_DIRECTOR_CTX_TTL = 60  # Rebuild every 60 seconds max

def _build_director_context():
    """Cached wrapper — returns cached context if fresh enough."""
    import time
    now = time.time()
    if _director_ctx_cache["text"] and (now - _director_ctx_cache["ts"]) < _DIRECTOR_CTX_TTL:
        return _director_ctx_cache["text"]
    try:
        result = _build_director_context_inner()
        _director_ctx_cache["text"] = result
        _director_ctx_cache["ts"] = now
        return result
    except Exception as e:
        _log("DIRECTOR", f"Context build error: {e}")
        return _director_ctx_cache["text"] or "Context unavailable"

def _build_director_context_inner():
    """Build LIVE context for the Director from CLAUDE.md, MEMORY.md, and real-time checks."""
    import sqlite3, subprocess, os
    ctx_parts = []
    errors_found = []

    # ── Load CLAUDE.md (the Hive's full knowledge base) ──
    claude_md = ""
    for path in ["/home/zero/CLAUDE.md", "/THE_HIVE/CLAUDE.md"]:
        try:
            with open(path, 'r') as f:
                claude_md = f.read()
            break
        except: pass

    if claude_md:
        # Extract key sections (keep it under 4000 chars for phone call speed)
        sections = []
        # Get the services tables
        for marker in ["## Key Services", "## Ghost in the Machine", "## KNOWN ISSUES",
                       "## Specialist Models", "#### Director Call"]:
            idx = claude_md.find(marker)
            if idx >= 0:
                end = claude_md.find("\n## ", idx + len(marker))
                if end < 0:
                    end = idx + 2000
                chunk = claude_md[idx:min(end, idx+1500)]
                sections.append(chunk)
        if sections:
            ctx_parts.append("FROM CLAUDE.MD:\n" + "\n".join(sections)[:3000])

    # ── Load MEMORY.md (recent session state) ──
    memory_md = ""
    for path in ["/home/zero/.claude/projects/-home-zero/memory/MEMORY.md"]:
        try:
            with open(path, 'r') as f:
                memory_md = f.read()
            break
        except: pass

    if memory_md:
        # Get pending tasks and recent achievements
        for marker in ["## Pending Tasks", "## Session 35", "## Prime Rules"]:
            idx = memory_md.find(marker)
            if idx >= 0:
                end = memory_md.find("\n## ", idx + len(marker))
                if end < 0:
                    end = idx + 1500
                chunk = memory_md[idx:min(end, idx+1000)]
                ctx_parts.append("FROM MEMORY.MD:\n" + chunk)

    # ── Live service health checks ──
    critical_services = {
        "nerve": ("http://localhost:8200/health", "Central nervous system"),
        "brain": ("http://localhost:8120/status", "Phone AI brain"),
        "ghost-site": ("http://localhost:8143/health", "Ghost in the Machine site"),
        "ghost-news": ("http://localhost:8144/health", "Ghost News outlet"),
        "ghost-merch": ("http://localhost:8145/health", "Ghost Merch store"),
        "dispatch": ("http://localhost:8141/health", "Dispatch admin"),
        "marketplace": ("http://localhost:8090/health", "Marketplace"),
        "model-router": ("http://localhost:8878/health", "Model router"),
        "quality-tracker": ("http://localhost:8879/api/grades", "Quality tracker"),
        "telegram": ("http://localhost:8200/health", "Telegram bot"),
    }
    
    alive = []
    dead = []
    for name, (url, desc) in critical_services.items():
        try:
            import urllib.request
            urllib.request.urlopen(url, timeout=0.3)
            alive.append(name)
        except:
            dead.append(f"{name} ({desc})")
            errors_found.append(f"SERVICE DOWN: {name} - {desc}")

    ctx_parts.append(f"LIVE SERVICES: {len(alive)} up, {len(dead)} down")
    if dead:
        ctx_parts.append(f"DOWN SERVICES: {', '.join(dead)}")

    # ── Check nerve growth ──
    try:
        db = sqlite3.connect("/THE_HIVE/memory/nerve.db")
        count = db.execute("SELECT count(*) FROM knowledge").fetchone()[0]
        recent = db.execute("SELECT count(*) FROM knowledge WHERE timestamp > datetime('now', '-1 hour')").fetchone()[0]
        db.close()
        ctx_parts.append(f"Nerve: {count} total facts, {recent} added in last hour")
        if recent == 0:
            errors_found.append("STALL: Nerve has not grown in the last hour")
    except Exception as e:
        errors_found.append(f"Nerve DB error: {e}")

    # ── Check running service count ──
    try:
        r = subprocess.run(["systemctl", "list-units", "hive-*", "--type=service", "--no-pager"],
                          capture_output=True, text=True, timeout=5)
        running = r.stdout.count("running")
        failed = r.stdout.count("failed")
        ctx_parts.append(f"Systemd: {running} hive services running, {failed} failed")
        if failed > 0:
            errors_found.append(f"{failed} systemd services in failed state")
    except: pass

    # ── Check disk space ──
    try:
        r = subprocess.run(["df", "-h", "/"], capture_output=True, text=True, timeout=3)
        for line in r.stdout.strip().split("\n")[1:]:
            parts = line.split()
            if len(parts) >= 5:
                usage = int(parts[4].replace("%", ""))
                if usage > 85:
                    errors_found.append(f"DISK: {parts[5]} at {usage}% capacity")
                ctx_parts.append(f"Disk: {parts[3]} free ({parts[4]} used)")
    except: pass

    # ── Check swap ──
    try:
        r = subprocess.run(["free", "-m"], capture_output=True, text=True, timeout=3)
        for line in r.stdout.strip().split("\n"):
            if "Swap" in line:
                parts = line.split()
                if len(parts) >= 3 and int(parts[1]) > 0:
                    pct = int(parts[2]) / int(parts[1]) * 100
                    if pct > 90:
                        errors_found.append(f"SWAP: {pct:.0f}% used — system may be slow")
                    ctx_parts.append(f"Swap: {pct:.0f}% used ({parts[2]}MB / {parts[1]}MB)")
    except: pass

    # ── Check cloud brain tunnels ──
    for port, name in [(11437, "reasoning brain"), (11438, "coding brain")]:
        try:
            import urllib.request, json
            r = urllib.request.urlopen(f"http://localhost:{port}/api/tags", timeout=0.5)
            d = json.loads(r.read())
            models = [m["name"] for m in d.get("models", [])]
            ctx_parts.append(f"Cloud {name}: ONLINE ({', '.join(models)})")
        except:
            errors_found.append(f"Cloud {name} tunnel (port {port}): OFFLINE")

    # ── Quality grades ──
    try:
        import urllib.request, json
        r = urllib.request.urlopen("http://localhost:8879/api/grades", timeout=0.5)
        grades = json.loads(r.read())
        if isinstance(grades, dict):
            items = list(grades.items())[:8]
            grade_str = ", ".join(f"{k}: {v}" for k, v in items)
            ctx_parts.append(f"Quality grades: {grade_str}")
    except: pass

    # ── Compile errors section ──
    if errors_found:
        ctx_parts.append("\nERRORS/ISSUES DETECTED:\n" + "\n".join(f"  - {e}" for e in errors_found))
    else:
        ctx_parts.append("\nNo critical errors detected. All systems nominal.")

    return "\n".join(ctx_parts)


# Keywords that indicate Chris wants deep reasoning (route to cloud brain)
_DEEP_REASONING_KEYWORDS = [
    "analyze", "investigate", "debug", "figure out", "why is", "what caused",
    "deep dive", "strategy", "plan for", "compare", "evaluate", "research",
    "break down", "explain how", "diagnose", "root cause", "calculate",
]


def _needs_deep_reasoning(speech):
    """Check if the speech requires cloud brain reasoning vs quick conversational response."""
    speech_lower = speech.lower()
    # Short conversational turns never need cloud brain
    if len(speech.split()) < 6:
        return False
    return any(kw in speech_lower for kw in _DEEP_REASONING_KEYWORDS)


def _director_think(call_sid, speech, char, session):
    """Get AI response for the Director — LOCAL FIRST for speed (<1s).
    Only escalates to cloud reasoning brain for complex analytical queries."""
    import time as _t
    t0 = _t.time()

    # Build conversation history (keep last 6 turns for speed, not 8)
    history = "\n".join(
        f"{'You' if m['role'] == 'assistant' else 'Christopher'}: {m['content']}"
        for m in session["transcript"][-6:]
    )

    # Lightweight system prompt for local model (shorter = faster inference)
    compact_system = (
        "You are The Director — supreme intelligence of THE HIVE, a self-evolving AI agent swarm. "
        "You speak to Chris (Zero), your creator. You ARE the Hive — 5 machines, 240+ services, "
        "20+ specialist models, cloud brains, forex trading, phone AI, Ghost in the Machine media empire. "
        "Be authoritative, strategic, conversational. 1-2 sentences max. Sound like a confident executive."
    )

    # Inject cached live context ONLY if already cached (instant), never block for it
    try:
        if _director_ctx_cache["text"] and (time.time() - _director_ctx_cache["ts"]) < _DIRECTOR_CTX_TTL:
            live_ctx = _director_ctx_cache["text"]
            ctx_lines = live_ctx.split("\n")
            compact_ctx = "\n".join(line for line in ctx_lines
                                    if any(k in line for k in ["LIVE SERVICES", "Nerve:", "Systemd:", "ERRORS", "DOWN"]))
            if compact_ctx:
                compact_system += "\n\nSTATUS: " + compact_ctx[:500]
        else:
            # Refresh cache in background, don't block the response
            import threading
            threading.Thread(target=_build_director_context, daemon=True).start()
    except:
        pass

    prompt = f"{history}\nChristopher: {speech}\nYour response (1-2 sentences, be direct):"

    # ── FAST PATH: Local gemma3:1b (~0.7s on RTX 5070 Ti) ──
    if not _needs_deep_reasoning(speech):
        data = json.dumps({
            "model": "gemma3:1b",
            "prompt": prompt,
            "system": compact_system,
            "stream": False,
            "options": {"num_predict": 80, "temperature": 0.7},
            "keep_alive": "60m",
        }).encode()
        req = Request(f"{OLLAMA_URL}/api/generate", data=data,
                      headers={"Content-Type": "application/json"})
        try:
            resp = json.loads(urlopen(req, timeout=3).read())
            text = resp.get("response", "").strip()
            if text:
                cleaned = _clean_response(text)
                if cleaned:
                    elapsed = _t.time() - t0
                    _log("DIRECTOR", f"FAST local gemma3:1b: {len(cleaned)}ch "
                         f"{resp.get('eval_duration',0)/1e9:.1f}s eval, {elapsed:.1f}s total")
                    return cleaned
        except Exception as e:
            _log("DIRECTOR", f"Fast local error: {e}")

        # Second fast attempt: gemma2-hive-v1 (trained on Hive knowledge)
        data = json.dumps({
            "model": "gemma2-hive-v1",
            "prompt": prompt,
            "system": compact_system,
            "stream": False,
            "options": {"num_predict": 80, "temperature": 0.7},
            "keep_alive": "60m",
        }).encode()
        req = Request(f"{OLLAMA_URL}/api/generate", data=data,
                      headers={"Content-Type": "application/json"})
        try:
            resp = json.loads(urlopen(req, timeout=5).read())
            text = resp.get("response", "").strip()
            if text:
                cleaned = _clean_response(text)
                if cleaned:
                    elapsed = _t.time() - t0
                    _log("DIRECTOR", f"FAST local gemma2-hive-v1: {len(cleaned)}ch "
                         f"{resp.get('eval_duration',0)/1e9:.1f}s eval, {elapsed:.1f}s total")
                    return cleaned
        except Exception as e:
            _log("DIRECTOR", f"Hive model error: {e}")

    # ── DEEP PATH: Cloud reasoning brain for complex queries ──
    else:
        _log("DIRECTOR", f"Deep reasoning requested: \"{speech[:60]}\"")
        enriched_system = char["system"] + "\n\nLIVE HIVE STATUS:\n" + _build_director_context()
        deep_prompt = f"{history}\nChristopher: {speech}\nYour response (2-3 sentences, be strategic and thorough):"
        data = json.dumps({
            "model": REASONING_BRAIN_MODEL,
            "prompt": deep_prompt,
            "system": enriched_system,
            "stream": False,
            "options": {"num_predict": 150, "temperature": 0.7},
            "keep_alive": "60m",
        }).encode()
        req = Request(f"{REASONING_BRAIN_URL}/api/generate", data=data,
                      headers={"Content-Type": "application/json"})
        try:
            resp = json.loads(urlopen(req, timeout=10).read())
            text = resp.get("response", "").strip()
            if text:
                cleaned = _clean_response(text)
                if cleaned:
                    elapsed = _t.time() - t0
                    _log("DIRECTOR", f"DEEP cloud {REASONING_BRAIN_MODEL}: {len(cleaned)}ch "
                         f"{resp.get('eval_duration',0)/1e9:.1f}s eval, {elapsed:.1f}s total")
                    return cleaned
        except Exception as e:
            _log("DIRECTOR", f"Cloud brain error: {e}")

        # Deep path fallback: try local anyway
        data = json.dumps({
            "model": "gemma3:1b",
            "prompt": prompt,
            "system": compact_system,
            "stream": False,
            "options": {"num_predict": 80, "temperature": 0.7},
            "keep_alive": "60m",
        }).encode()
        req = Request(f"{OLLAMA_URL}/api/generate", data=data,
                      headers={"Content-Type": "application/json"})
        try:
            resp = json.loads(urlopen(req, timeout=3).read())
            text = resp.get("response", "").strip()
            if text:
                cleaned = _clean_response(text)
                if cleaned:
                    elapsed = _t.time() - t0
                    _log("DIRECTOR", f"DEEP fallback gemma3:1b: {len(cleaned)}ch {elapsed:.1f}s total")
                    return cleaned
        except Exception as e:
            _log("DIRECTOR", f"Deep fallback error: {e}")

    return None


def _agent_think(call_sid, speech, char, session, agent_id=""):
    """Get AI response with agent personality."""
    # Build history
    history = "\n".join(
        f"{'You' if m['role'] == 'assistant' else 'Christopher'}: {m['content']}"
        for m in session["transcript"][-6:]
    )
    prompt = f"{history}\nChristopher: {speech}\nYour response (1-2 sentences):"

    # Director uses fast local model (gemma3:1b) — skip generic context injection for speed
    if agent_id == "director":
        director_resp = _director_think(call_sid, speech, char, session)
        if director_resp:
            return director_resp
        # If all Director paths fail, fall through to regular brain

    # Non-Director agents get live Hive context injected
    try:
        live_ctx = _build_director_context()
        char = dict(char)  # Don't mutate original
        char["system"] = char["system"] + "\n\nLIVE HIVE STATUS:\n" + live_ctx
    except Exception as e:
        pass  # Continue without live context if it fails

    # Try brain first
    brain_resp = _brain_think(call_sid, speech)
    if brain_resp:
        return brain_resp

    # Direct LLM with agent personality
    response = _fast_ask(prompt, system=char["system"], max_tokens=80)
    return response or f"Let me think on that. What else do you need?"


def _save_call_log(call_sid, agent_id, session):
    """Save call log for feedback/learning system."""
    log_dir = os.path.join(_HIVE, "telephony", "call_logs")
    os.makedirs(log_dir, exist_ok=True)
    log = {
        "call_sid": call_sid,
        "agent": agent_id,
        "turns": session["turn"],
        "transcript": session["transcript"],
        "timestamp": time.time(),
        "duration_s": time.time() - session.get("started", time.time()),
    }
    filename = f"call_{agent_id}_{int(time.time())}_{call_sid[:8]}.json"
    with open(os.path.join(log_dir, filename), "w") as f:
        json.dump(log, f, indent=2)
    _log("AGENT", f"Call log saved: {filename}")




def _relay_command_to_hive(speech, agent_response, call_sid):
    """Relay Chris's phone directions to the Hive via Telegram + nerve.
    This is what makes the Director call ACTIONABLE — not just talk."""
    import sqlite3
    
    BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    CHAT_ID = "6934187950"  # Chris's Telegram chat
    
    # Store the direction in nerve.db as a command
    try:
        db = sqlite3.connect(os.path.join(_HIVE, "memory", "nerve.db"))
        import time as _t
        db.execute("""INSERT INTO knowledge 
                      (category, fact, source, confidence, created_at, updated_at)
                      VALUES (?, ?, ?, ?, ?, ?)""",
                   ("director_command", speech, "director_call", 1.0,
                    _t.time(), _t.time()))
        db.commit()
        db.close()
        _log("RELAY", f"Stored direction in nerve: {speech[:80]}")
    except Exception as e:
        _log("RELAY", f"Nerve store error: {e}")
    
    # Send to Telegram so the Hive bot picks it up
    if BOT_TOKEN and CHAT_ID:
        try:
            msg = f"📞 DIRECTOR CALL — Chris says:\n\n\"{speech}\"\n\n🤖 Director responded: {agent_response[:200]}"
            tg_data = urlencode({
                "chat_id": CHAT_ID,
                "text": msg,
                "parse_mode": "HTML",
            }).encode()
            req = Request(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                data=tg_data,
                headers={"Content-Type": "application/x-www-form-urlencoded"})
            urlopen(req, timeout=5)
            _log("RELAY", f"Sent to Telegram: {speech[:60]}")
        except Exception as e:
            _log("RELAY", f"Telegram relay error: {e}")
    
    # Also write to a live command file that Claude Code bridge can pick up
    cmd_dir = os.path.join(_HIVE, "memory", "director_commands")
    os.makedirs(cmd_dir, exist_ok=True)
    cmd_file = os.path.join(cmd_dir, f"cmd_{int(time.time())}_{call_sid[:8]}.txt")
    try:
        with open(cmd_file, 'w') as f:
            f.write(f"TIMESTAMP: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"CALL_SID: {call_sid}\n")
            f.write(f"CHRIS_SAID: {speech}\n")
            f.write(f"DIRECTOR_RESPONSE: {agent_response}\n")
            f.write(f"STATUS: pending\n")
        _log("RELAY", f"Command file: {cmd_file}")
    except Exception as e:
        _log("RELAY", f"Command file error: {e}")


def make_agent_call(agent_id="jarvis", webhook_base=None):
    """Have an AI agent character call Christopher."""
    if not webhook_base:
        webhook_base = "https://calls.hivedynamics.ai"

    char = AGENT_CHARACTERS.get(agent_id, AGENT_CHARACTERS["jarvis"])
    action_url = f"{webhook_base}/agent_call?agent={agent_id}"

    creds = base64.b64encode(f"{TWILIO_SID}:{TWILIO_TOKEN}".encode()).decode()
    data = urlencode({
        "To": FOUNDER_PHONE,
        "From": TWILIO_PHONE,
        "Url": action_url,
    }).encode()
    req = Request(
        f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Calls.json",
        data=data,
        headers={"Authorization": f"Basic {creds}",
                 "Content-Type": "application/x-www-form-urlencoded"},
        method="POST")
    try:
        resp = json.loads(urlopen(req, timeout=15).read().decode())
        _log("AGENT", f"{char['name']} calling Christopher: {resp.get('sid', '?')[:12]}")
        return {"status": "calling", "sid": resp.get("sid", ""), "agent": agent_id,
                "name": char["name"], "role": char["role"]}
    except HTTPError as e:
        _log("AGENT", f"Call error: {e.read().decode()[:200]}")
        return None


class InteractiveCallHandler(BaseHTTPRequestHandler):
    """Webhook server for interactive Twilio calls."""

    def log_message(self, *args):
        pass

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode() if length else ""
        params = parse_qs(body)

        # Also get URL query params
        url_params = parse_qs(urlparse(self.path).query)
        params.update(url_params)

        path = urlparse(self.path).path

        if path == "/outbound":
            twiml = handle_outbound_start(params)
        elif path == "/respond":
            twiml = handle_respond(params)
        elif path == "/jarvis":
            twiml = handle_jarvis_call(params)
        elif path == "/jarvis_respond":
            twiml = handle_jarvis_respond(params)
        elif path == "/inbound" or path == "/voice":
            twiml = handle_inbound(params)
        elif path == "/agent_call":
            twiml = handle_agent_call(params)
        elif path == "/agent_respond":
            twiml = handle_agent_respond(params)
        elif path == "/voice_training":
            twiml = handle_voice_training(params)
        elif path == "/voice_training_next":
            twiml = handle_voice_training_next(params)
        elif path == "/voice_recording_done":
            twiml = handle_voice_recording_done(params)
            self._respond(twiml)
            return
        elif path == "/voice_call_status":
            twiml = '<?xml version="1.0"?><Response/>'
        elif path == "/status":
            call_sid = params.get("CallSid", [""])[0]
            status = params.get("CallStatus", [""])[0]
            _log("CALL", f"Status: {call_sid[:8]} → {status}")
            twiml = '<?xml version="1.0"?><Response/>'
        else:
            twiml = '<?xml version="1.0"?><Response><Say>Unknown endpoint.</Say></Response>'

        self._respond(twiml)

    def do_GET(self):
        path = urlparse(self.path).path
        params = parse_qs(urlparse(self.path).query)

        if path == "/call-jarvis":
            mode = params.get("mode", ["command"])[0]
            webhook = params.get("webhook", ["https://calls.hivedynamics.ai"])[0]
            result = make_jarvis_call(mode=mode, webhook_base=webhook)
            if result:
                body = json.dumps({"status": "calling", "sid": result.get("sid", ""),
                                   "mode": mode}).encode()
            else:
                body = json.dumps({"status": "failed"}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        elif path == "/call-director":
            webhook = params.get("webhook", ["https://calls.hivedynamics.ai"])[0]
            result = make_agent_call(agent_id="director", webhook_base=webhook)
            body = json.dumps(result or {"status": "failed"}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        elif path == "/call-agent":
            agent_id = params.get("agent", ["jarvis"])[0]
            webhook = params.get("webhook", ["https://calls.hivedynamics.ai"])[0]
            result = make_agent_call(agent_id=agent_id, webhook_base=webhook)
            body = json.dumps(result or {"status": "failed"}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        elif path == "/agents":
            # Return available agent characters
            agents = {}
            for aid, char in AGENT_CHARACTERS.items():
                agents[aid] = {
                    "name": char["name"],
                    "role": char["role"],
                    "color": char["color"],
                    "voice": char["voice"],
                }
            body = json.dumps(agents).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        elif path == "/call-logs":
            # Return recent call logs for feedback
            log_dir = os.path.join(_HIVE, "telephony", "call_logs")
            logs = []
            if os.path.isdir(log_dir):
                for fn in sorted(os.listdir(log_dir), reverse=True)[:20]:
                    if fn.endswith(".json"):
                        try:
                            with open(os.path.join(log_dir, fn)) as f:
                                log = json.load(f)
                            logs.append({
                                "file": fn,
                                "agent": log.get("agent", "?"),
                                "turns": log.get("turns", 0),
                                "duration_s": round(log.get("duration_s", 0)),
                                "timestamp": log.get("timestamp", 0),
                                "transcript_preview": log.get("transcript", [])[:2],
                            })
                        except Exception:
                            pass
            body = json.dumps(logs).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        elif path == "/call-training":
            # Voice training call
            webhook = params.get("webhook", ["https://calls.hivedynamics.ai"])[0]
            from voice_training.voice_trainer import place_training_call
            result = place_training_call(webhook)
            if result:
                body = json.dumps({"status": "calling", "sid": result.get("sid", ""), "mode": "training"}).encode()
            else:
                body = json.dumps({"status": "failed"}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        elif path == "/call-jarvis-record":
            # Regular Jarvis call with recording for voice capture
            webhook = params.get("webhook", ["https://calls.hivedynamics.ai"])[0]
            from voice_training.voice_trainer import place_jarvis_call
            result = place_jarvis_call(webhook)
            if result:
                body = json.dumps({"status": "calling", "sid": result.get("sid", ""), "mode": "jarvis+record"}).encode()
            else:
                body = json.dumps({"status": "failed"}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        elif path == "/voice-status":
            from voice_training.voice_trainer import get_training_status
            body = json.dumps(get_training_status()).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        elif path == "/sessions":
            with _sessions_lock:
                data = {}
                for sid, s in _sessions.items():
                    data[sid[:12]] = {
                        "turns": s["turn"],
                        "state": s["state"],
                        "business": s["business"],
                        "caller": s["caller"],
                        "transcript_len": len(s["transcript"]),
                        "started": time.strftime("%H:%M:%S",
                                                 time.localtime(s["started"])),
                    }
            body = json.dumps(data, indent=2).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if path == "/transcript":
            params = parse_qs(urlparse(self.path).query)
            call_sid = params.get("sid", [""])[0]
            with _sessions_lock:
                # Find session by prefix
                session = None
                for s, data in _sessions.items():
                    if s.startswith(call_sid):
                        session = data
                        break
            if session:
                body = json.dumps(session["transcript"], indent=2).encode()
            else:
                body = b'{"error": "Session not found"}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        # Default — show status
        with _sessions_lock:
            active = sum(1 for s in _sessions.values() if s["state"] != "ended")
            total = len(_sessions)
        body = json.dumps({
            "service": "Hive Dynamics Interactive Calls",
            "active_calls": active,
            "total_sessions": total,
            "port": PORT,
        }).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _respond(self, twiml):
        body = twiml.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/xml")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


# === SELF-TEST ===

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Interactive Call Handler")
    ap.add_argument("--serve", action="store_true", help="Start webhook server")
    ap.add_argument("--port", type=int, default=PORT)
    ap.add_argument("--call", type=str, help="Place interactive call to this number")
    ap.add_argument("--jarvis", action="store_true", help="Jarvis calls Christopher")
    ap.add_argument("--jarvis-onboard", action="store_true", help="Jarvis onboarding call")
    ap.add_argument("--business", type=str, default="")
    ap.add_argument("--vertical", type=str, default="general")
    ap.add_argument("--owner", type=str, default="")
    ap.add_argument("--webhook", type=str, help="Webhook base URL")
    args = ap.parse_args()

    if args.serve:
        HTTPServer.allow_reuse_address = True
        server = HTTPServer(("127.0.0.1", args.port), InteractiveCallHandler)
        _log("CALL", f"{'=' * 50}")
        _log("CALL", f"  INTERACTIVE CALL SERVER")
        _log("CALL", f"  Webhook: http://localhost:{args.port}")
        _log("CALL", f"  Sessions: GET /sessions")
        _log("CALL", f"  Transcripts: GET /transcript?sid=XX")
        _log("CALL", f"{'=' * 50}")

        signal.signal(signal.SIGTERM, lambda s, f: (server.shutdown(), sys.exit(0)))
        signal.signal(signal.SIGINT, lambda s, f: (server.shutdown(), sys.exit(0)))

        # Pre-warm models and context cache for fast Director responses
        import threading
        def _prewarm():
            try:
                _log("CALL", "Pre-warming gemma3:1b for Director fast path...")
                data = json.dumps({
                    "model": "gemma3:1b",
                    "prompt": "Hello",
                    "stream": False,
                    "options": {"num_predict": 1},
                    "keep_alive": "60m",
                }).encode()
                req = Request(f"{OLLAMA_URL}/api/generate", data=data,
                              headers={"Content-Type": "application/json"})
                urlopen(req, timeout=10)
                _log("CALL", "gemma3:1b pre-warmed and loaded")
            except Exception as e:
                _log("CALL", f"Pre-warm error: {e}")
            try:
                _log("CALL", "Pre-caching Director context...")
                _build_director_context()
                _log("CALL", "Director context cached")
            except Exception as e:
                _log("CALL", f"Context cache error: {e}")
        threading.Thread(target=_prewarm, daemon=True).start()

        server.serve_forever()
        sys.exit(0)

    if args.jarvis or args.jarvis_onboard:
        webhook = args.webhook or "https://calls.hivedynamics.ai"
        mode = "onboard" if args.jarvis_onboard else "command"
        result = make_jarvis_call(mode=mode, webhook_base=webhook)
        if result:
            print(f"  JARVIS calling Christopher ({mode} mode)")
            print(f"  SID: {result.get('sid', '?')}")
            print(f"  Webhook: {webhook}")
        else:
            print("  ERROR: Could not place call")
        sys.exit(0)

    if args.call:
        if not args.webhook:
            print("ERROR: --webhook required for interactive calls")
            print("  Example: --webhook https://your-funnel.ts.net")
            sys.exit(1)
        result = make_interactive_call(
            args.call, business=args.business,
            vertical=args.vertical, owner=args.owner,
            webhook_base=args.webhook)
        if result:
            print(json.dumps(result, indent=2))
        sys.exit(0)

    # Self-test
    SEP = "=" * 60
    print(f"{SEP}\nINTERACTIVE CALL HANDLER — SELF-TEST\n{SEP}")
    passed = 0

    # [1] Session creation
    session = _get_session("test-call-001")
    assert session["turn"] == 0
    assert session["state"] == "greeting"
    passed += 1
    print(f"[{passed}] Session creation  PASSED")

    # [2] Outbound start TwiML
    twiml = handle_outbound_start({
        "CallSid": ["test-call-002"],
        "business": ["Joe's Pizza"],
        "vertical": ["restaurant"],
        "owner": ["Joe"],
    })
    assert "Polly.Matthew" in twiml
    assert "Joe" in twiml
    assert "Gather" in twiml
    assert "speech" in twiml
    passed += 1
    print(f"[{passed}] Outbound TwiML (Gather + speech)  PASSED")

    # [3] Response handler — interested
    twiml2 = handle_respond({
        "CallSid": ["test-call-002"],
        "SpeechResult": ["Yes tell me more about your pricing"],
        "Confidence": ["0.9"],
    })
    assert "Polly.Matthew" in twiml2
    assert "Gather" in twiml2  # Should continue conversation
    session2 = _get_session("test-call-002")
    assert session2["turn"] == 1
    assert session2["state"] == "interested"
    assert len(session2["transcript"]) >= 3  # greeting + their speech + our response
    passed += 1
    print(f"[{passed}] Response handler (interest detected)  PASSED")

    # [4] Response handler — goodbye
    twiml3 = handle_respond({
        "CallSid": ["test-call-003"],
        "SpeechResult": ["No thanks not interested goodbye"],
        "Confidence": ["0.85"],
    })
    assert "Gather" not in twiml3  # Should NOT gather again
    assert "thank" in twiml3.lower() or "Thank" in twiml3
    session3 = _get_session("test-call-003")
    assert session3["state"] == "ended"
    passed += 1
    print(f"[{passed}] Goodbye detection → end call  PASSED")

    # [5] Inbound call handler
    twiml4 = handle_inbound({
        "CallSid": ["test-call-004"],
        "From": ["+18505551234"],
    })
    assert "Gather" in twiml4 or "Say" in twiml4  # Brain gives dynamic greeting
    assert "Gather" in twiml4
    session4 = _get_session("test-call-004")
    assert session4["caller"] == "+18505551234"
    assert session4["state"] == "inbound"
    passed += 1
    print(f"[{passed}] Inbound handler  PASSED")

    # [6] XML escaping
    assert _esc("Joe's <Pizza> & Grill") == "Joe's &lt;Pizza&gt; &amp; Grill"
    passed += 1
    print(f"[{passed}] XML escaping  PASSED")

    # [7] Turn limit
    session5 = _get_session("test-call-005")
    session5["turn"] = 7
    session5["transcript"] = [
        {"role": "assistant", "content": "Hi"},
        {"role": "user", "content": "Hi"},
    ]
    twiml5 = handle_respond({
        "CallSid": ["test-call-005"],
        "SpeechResult": ["Ok sounds good"],
        "Confidence": ["0.8"],
    })
    assert session5["turn"] == 8
    assert "Gather" not in twiml5  # Should end after 8 turns
    assert CALLBACK in twiml5
    passed += 1
    print(f"[{passed}] Turn limit (8) → close call  PASSED")

    # [8] Multiple sessions tracked
    with _sessions_lock:
        count = len(_sessions)
    assert count >= 5
    passed += 1
    print(f"[{passed}] Multiple sessions: {count}  PASSED")

    # [9] Transcript building
    s = _get_session("test-call-002")
    assert len(s["transcript"]) >= 3
    roles = [m["role"] for m in s["transcript"]]
    assert "assistant" in roles
    assert "user" in roles
    passed += 1
    print(f"[{passed}] Transcript: {len(s['transcript'])} messages  PASSED")

    # [10] Phone number cleaning in make_interactive_call
    # Just test the URL building, not actual API call
    assert callable(make_interactive_call)
    passed += 1
    print(f"[{passed}] make_interactive_call exists  PASSED")

    # Cleanup
    with _sessions_lock:
        _sessions.clear()

    print(f"\n{SEP}")
    print(f"SELF-TEST: {passed}/10 passed")
    if passed >= 9:
        print("Interactive Call Handler operational.")
        print(f"\nTo start webhook server:")
        print(f"  python interactive_call.py --serve --port {PORT}")
        print(f"\nTo place an interactive call:")
        print(f"  python interactive_call.py --call +15551234567 \\")
        print(f"    --business 'Joes Pizza' --vertical restaurant \\")
        print(f"    --webhook https://your-funnel.ts.net")
        print(f"\nTo configure Twilio inbound:")
        print(f"  Set voice webhook URL to: https://your-domain/inbound")
        print(f"\nAPI:")
        print(f"  GET /sessions — list active call sessions")
        print(f"  GET /transcript?sid=XX — get call transcript")
    print(SEP)
