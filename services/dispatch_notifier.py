#!/usr/bin/env python3
"""Dispatch notification system — texts techs when jobs are assigned.
Runs as part of the operator backend or standalone.
Uses Twilio for SMS, Telegram bot for app notifications.
"""
import json, os, sqlite3, time, threading, logging
from urllib.request import Request, urlopen

log = logging.getLogger("dispatch-notify")

DB_PATH = "/THE_HIVE/memory/tracker.db"
TWILIO_SID = os.environ.get("TWILIO_ACCOUNT_SID", "")
TWILIO_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN", "")
TWILIO_PHONE = os.environ.get("TWILIO_PHONE", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")

# Load .env
env_path = "/THE_HIVE/.env"
if os.path.exists(env_path):
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"'))
    TWILIO_SID = os.environ.get("TWILIO_ACCOUNT_SID", TWILIO_SID)
    TWILIO_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN", TWILIO_TOKEN)
    TWILIO_PHONE = os.environ.get("TWILIO_PHONE", TWILIO_PHONE)
    TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", TELEGRAM_TOKEN)


def get_db():
    db = sqlite3.connect(DB_PATH, timeout=10)
    db.row_factory = sqlite3.Row
    return db


def send_sms(to_phone, message):
    """Send SMS via Twilio."""
    if not all([TWILIO_SID, TWILIO_TOKEN, TWILIO_PHONE]):
        log.warning("Twilio not configured")
        return False
    try:
        from twilio.rest import Client
        client = Client(TWILIO_SID, TWILIO_TOKEN)
        msg = client.messages.create(body=message, from_=TWILIO_PHONE, to=to_phone)
        log.info(f"SMS sent to {to_phone}: {msg.sid}")
        return True
    except Exception as e:
        log.error(f"SMS failed to {to_phone}: {e}")
        return False


def send_telegram(chat_id, message):
    """Send Telegram message."""
    if not TELEGRAM_TOKEN:
        return False
    try:
        data = json.dumps({"chat_id": int(chat_id), "text": message}).encode()
        req = Request(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data=data, headers={"Content-Type": "application/json"}
        )
        urlopen(req, timeout=10)
        return True
    except Exception as e:
        log.error(f"Telegram failed to {chat_id}: {e}")
        return False


def notify_tech_new_job(job):
    """Notify a technician about a new/assigned job."""
    assigned = job.get("assigned_to", "")
    if not assigned:
        return

    db = get_db()
    tech = db.execute(
        "SELECT * FROM dispatch_technicians WHERE name LIKE ? OR tech_id = ?",
        (f"%{assigned}%", assigned)
    ).fetchone()
    db.close()

    if not tech:
        log.warning(f"Tech not found: {assigned}")
        return

    msg = (
        f"NEW JOB ASSIGNED\n"
        f"Title: {job.get('title', 'No title')}\n"
        f"Address: {job.get('address', 'No address')}\n"
        f"Customer: {job.get('customer_name', '')} {job.get('customer_phone', '')}\n"
        f"Priority: {job.get('priority', 5)}/10\n"
        f"Notes: {job.get('notes', '')}\n\n"
        f"Reply to this bot or open tracking: http://100.70.226.103:7437/track/"
    )

    phone = dict(tech).get("phone", "")
    if phone:
        # Clean phone number
        clean = phone.replace("-", "").replace(" ", "").replace("(", "").replace(")", "")
        if not clean.startswith("+"):
            clean = "+1" + clean if len(clean) == 10 else clean
        send_sms(clean, msg)

    # Also try Telegram
    try:
        users = json.loads(open("/THE_HIVE/memory/.telegram_users.json").read())
        for uid, info in users.items():
            name = info.get("first_name", "").lower()
            if name and name in assigned.lower():
                send_telegram(info["chat_id"], msg)
                break
    except Exception:
        pass


def notify_dispatch_job_update(job_id, status, tech_name=""):
    """Notify dispatch/owner when a tech updates a job."""
    db = get_db()
    job = db.execute("SELECT * FROM dispatch_jobs WHERE job_id = ?", (job_id,)).fetchone()
    db.close()

    if not job:
        return

    job = dict(job)
    msg = (
        f"JOB UPDATE: {job.get('title', job_id)}\n"
        f"Status: {status}\n"
        f"Tech: {tech_name or job.get('assigned_to', 'unassigned')}\n"
        f"Address: {job.get('address', '')}"
    )

    # Send to owner
    try:
        owner_chat = open("/THE_HIVE/memory/.telegram_owner_id").read().strip()
        send_telegram(owner_chat, msg)
    except Exception:
        pass

    # Send to dispatch (Maria)
    try:
        users = json.loads(open("/THE_HIVE/memory/.telegram_users.json").read())
        roles = json.loads(open("/THE_HIVE/memory/.telegram_roles.json").read())
        for uid, role_info in roles.items():
            if role_info.get("role") == "admin" and role_info.get("approved"):
                if uid in users:
                    send_telegram(users[uid]["chat_id"], msg)
    except Exception:
        pass


def notify_owner(message):
    """Quick notify to owner via Telegram."""
    try:
        owner_chat = open("/THE_HIVE/memory/.telegram_owner_id").read().strip()
        send_telegram(owner_chat, message)
    except Exception:
        pass
