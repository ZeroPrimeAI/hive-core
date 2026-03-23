#!/usr/bin/env python3
"""
THE HIVE — AI Cold Caller & Sales Agent
Port 8332 | SQLite at /hiveAI/memory/cold_caller.db
MIT License

Autonomous outbound sales engine:
  - Cold calls businesses selling AI phone answering services
  - Email campaigns with drip sequences
  - Stripe payment link generation
  - Prospect sourcing (Google Maps, CSV, manual)
  - Full sales pipeline tracking
  - AI voice brain via Ollama (queen-bee-v2)
  - Dashboard with pipeline funnel, MRR, activity feed
"""

import json, sqlite3, time, threading, os, uuid, re, csv, io, smtplib, traceback
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any
from contextlib import contextmanager

import httpx
from fastapi import FastAPI, BackgroundTasks, HTTPException, Query, Request, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, Response, RedirectResponse
from pydantic import BaseModel
import uvicorn

# ==========================================================================
# CONFIG
# ==========================================================================
PORT = 8332
DB_PATH = "/hiveAI/memory/cold_caller.db"

TWILIO_ACCOUNT_SID = os.environ.get("TWILIO_ACCOUNT_SID", "os.environ.get("TWILIO_ACCOUNT_SID","")")
TWILIO_AUTH_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN", "")
TWILIO_PHONE = "+18508016662"
WEBHOOK_BASE = "https://calls.hivedynamics.ai"

OLLAMA_URL = "http://100.77.113.48:11434"
OLLAMA_MODEL = "queen-bee-v2"
OLLAMA_FALLBACK = "phi4-mini"

STRIPE_API_KEY = os.environ.get("STRIPE_API_KEY", "sk_test_demo")

MAIL_SERVER_URL = "http://100.70.226.103:8331"
SMTP_HOST = os.environ.get("SMTP_HOST", "")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASS = os.environ.get("SMTP_PASS", "")
FROM_EMAIL = os.environ.get("FROM_EMAIL", "sales@hivedynamics.ai")
FROM_NAME = "Hive Dynamics"

NERVE_URL = "http://100.70.226.103:8200"
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_OWNER_CHAT_ID", "")

BIZ_HOUR_START = 9
BIZ_HOUR_END = 17
CT_OFFSET = -6

TRACKING_PIXEL = (
    b'\x47\x49\x46\x38\x39\x61\x01\x00\x01\x00\x80\x00\x00'
    b'\xff\xff\xff\x00\x00\x00\x21\xf9\x04\x00\x00\x00\x00'
    b'\x00\x2c\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02'
    b'\x44\x01\x00\x3b'
)

CAMPAIGN_RUNNING = False
CAMPAIGN_THREAD = None

# ==========================================================================
# PACKAGES
# ==========================================================================
PACKAGES = {
    "ai_phone_agent": {
        "name": "AI Phone Agent",
        "price_cents": 9900, "price_display": "$99/mo",
        "setup_fee_cents": 49900, "setup_fee_display": "$499",
        "setup_includes": ["Custom AI voice training", "Phone number provisioning",
                           "Business hours configuration", "Greeting & script setup"],
        "description": "AI answers your calls 24/7. Never miss a customer again.",
        "features": ["24/7 AI phone answering", "Custom greeting & scripts",
                      "Call transcription & notes", "SMS follow-up to callers",
                      "Real-time notifications"],
    },
    "content_factory": {
        "name": "Content Factory",
        "price_cents": 14900, "price_display": "$149/mo",
        "setup_fee_cents": 99900, "setup_fee_display": "$999",
        "setup_includes": ["Brand voice training", "Content calendar setup",
                           "Social account integration", "SEO baseline audit"],
        "description": "AI generates blog posts, social media, and marketing content.",
        "features": ["8 blog posts per month", "30 social media posts",
                      "SEO optimization", "Brand voice training", "Content calendar"],
    },
    "full_hive": {
        "name": "Full Hive Package",
        "price_cents": 29900, "price_display": "$299/mo",
        "setup_fee_cents": 199900, "setup_fee_display": "$1,999",
        "setup_includes": ["Full AI buildout — phone, content, SEO",
                           "Dispatch & scheduling integration",
                           "CRM integration & data migration",
                           "Custom workflow configuration"],
        "description": "Phone + Content + SEO + Dispatch. The complete AI business suite.",
        "features": ["Everything in AI Phone Agent", "Everything in Content Factory",
                      "SEO monitoring & optimization", "AI dispatch & scheduling",
                      "Priority support"],
    },
    "enterprise": {
        "name": "Enterprise",
        "price_cents": 49900, "price_display": "$499/mo",
        "setup_fee_cents": 499900, "setup_fee_display": "$4,999",
        "setup_includes": ["Everything in Full Hive setup",
                           "Custom AI model training on your data",
                           "Dedicated support onboarding",
                           "White-label branding & customization",
                           "API integration & developer setup"],
        "description": "Everything + custom AI model training + dedicated support.",
        "features": ["Everything in Full Hive", "Custom AI model training",
                      "Dedicated account manager", "API access & integrations",
                      "White-label option", "Priority support SLA"],
    },
}

# ==========================================================================
# SALES SCRIPTS
# ==========================================================================
DEFAULT_SCRIPTS = {
    "cold_call_opener": {
        "name": "Cold Call Opener", "category": "opener", "industry": "general",
        "script": (
            "Hi, this is Alex from Hive Dynamics. We help businesses like {business_name} "
            "never miss a customer call with AI phone agents. Our AI answers 24/7, books "
            "appointments, and answers questions about your services. Are you the right "
            "person to talk to about your phone system?"
        ),
    },
    "cold_call_restaurant": {
        "name": "Restaurant Opener", "category": "opener", "industry": "restaurant",
        "script": (
            "Hi, this is Alex from Hive Dynamics. I work with restaurants in {location} "
            "and I noticed {business_name} probably gets a lot of calls for reservations, "
            "hours, and takeout orders. We built an AI phone agent that handles all of that "
            "automatically. Could I show you a quick 2-minute demo?"
        ),
    },
    "cold_call_medical": {
        "name": "Medical Office Opener", "category": "opener", "industry": "medical",
        "script": (
            "Hi, this is Alex from Hive Dynamics. We work with medical offices to solve "
            "missed calls and long hold times. Our AI handles appointment scheduling, "
            "insurance questions, and prescription refill requests 24/7. Is the office "
            "manager available?"
        ),
    },
    "cold_call_legal": {
        "name": "Law Firm Opener", "category": "opener", "industry": "legal",
        "script": (
            "Hi, this is Alex from Hive Dynamics. We help law firms capture every potential "
            "client call. Our AI qualifies leads, collects case details, and schedules "
            "consultations even at 2 AM. For {business_name}, a single missed call could "
            "be a $10,000 case. Do you have a minute?"
        ),
    },
    "cold_call_real_estate": {
        "name": "Real Estate Opener", "category": "opener", "industry": "real_estate",
        "script": (
            "Hi, this is Alex from Hive Dynamics. I work with real estate agents to make "
            "sure every buyer inquiry gets answered instantly. Our AI answers property "
            "questions, schedules showings, and qualifies leads so you never lose a deal. "
            "Is {business_name} open to a quick demo?"
        ),
    },
    "cold_call_contractor": {
        "name": "Contractor Opener", "category": "opener", "industry": "contractor",
        "script": (
            "Hi, this is Alex from Hive Dynamics. I work with contractors that miss calls "
            "when they're on the job. Our AI answers calls, collects job details, and "
            "schedules estimates so you never lose a job. Got 60 seconds?"
        ),
    },
    "cold_call_dental": {
        "name": "Dental Office Opener", "category": "opener", "industry": "dental",
        "script": (
            "Hi, this is Alex from Hive Dynamics. We help dental offices like {business_name} "
            "handle appointment scheduling calls automatically. Our AI books appointments, "
            "sends reminders, and answers after-hours questions. Is the office manager around?"
        ),
    },
    "cold_call_automotive": {
        "name": "Auto Shop Opener", "category": "opener", "industry": "automotive",
        "script": (
            "Hi, this is Alex from Hive Dynamics. We work with auto service centers to "
            "catch every service call. Our AI schedules appointments, quotes common services, "
            "and handles overflow when your team is busy. Quick question for the manager?"
        ),
    },
    "objection_price": {
        "name": "Price Objection", "category": "objection", "industry": "general",
        "script": (
            "I totally understand. Here's how to think about it: the $499 setup is a one-time "
            "investment to get your custom AI built — we train it on your business, set up your "
            "phone number, and configure everything. After that it's only $99 a month. If you "
            "miss just 2-3 calls a month, that's $500-2000 in lost revenue. Most clients pay back "
            "the setup fee in the first week. Want to try it risk-free for 14 days?"
        ),
    },
    "objection_have_service": {
        "name": "Existing Service Objection", "category": "objection", "industry": "general",
        "script": (
            "That's great! A lot of our clients switched from traditional answering services "
            "because our AI actually understands context, answers specific questions about your "
            "business, schedules appointments in real-time, and costs a fraction. Would you be "
            "open to a side-by-side comparison?"
        ),
    },
    "objection_not_interested": {
        "name": "Not Interested", "category": "objection", "industry": "general",
        "script": (
            "Totally fair. Quick question before I go: about how many calls does {business_name} "
            "get per day? Our clients typically see a 40% increase in booked appointments just "
            "from catching missed calls. Can I send you a one-page case study? No pressure."
        ),
    },
    "close_interested": {
        "name": "Close Interested", "category": "close", "industry": "general",
        "script": (
            "Awesome! I'm glad this sounds like a fit for {business_name}. I'll send you a "
            "secure payment link right now for the {package_name}. There's a one-time setup "
            "fee of {setup_fee} to get your custom AI built — that covers voice training, "
            "phone provisioning, everything. Then it's just {package_price} going forward. "
            "Your AI phone agent will be trained and live within 48 hours. Sound good?"
        ),
    },
    "close_demo": {
        "name": "Close Demo Request", "category": "close", "industry": "general",
        "script": (
            "Absolutely! Let me schedule a quick live demo. I'll call your business line and "
            "you can hear the AI in action answering as your receptionist. When works best "
            "this week? Morning or afternoon?"
        ),
    },
    "voicemail": {
        "name": "Voicemail", "category": "voicemail", "industry": "general",
        "script": (
            "Hi, this is Alex from Hive Dynamics. I was calling about {business_name}'s phone "
            "system. We have an AI agent that answers your business calls 24/7. I'll send you "
            "a quick email with details. You can reach me at 850-801-6662. Have a great day!"
        ),
    },
    "email_intro": {
        "name": "Email Intro", "category": "email", "industry": "general",
        "script": (
            "Subject: Never miss a customer call again, {business_name}\n\n"
            "Hi {contact_name},\n\nQuick question: how many calls does {business_name} miss "
            "each week?\n\nFor most {industry} businesses, it's 30-40% of incoming calls.\n\n"
            "At Hive Dynamics, we built an AI phone agent that:\n"
            "- Answers every call 24/7\n- Knows your business\n"
            "- Books appointments and sends texts\n- Starts at just $499 setup + $99/mo\n\n"
            "The setup fee covers building your custom AI — voice training, phone provisioning, "
            "and configuring everything for your business. After that, it's just $99/mo.\n\n"
            "Can I show you a 2-minute demo?\n\nBest,\nAlex\nHive Dynamics\n850-801-6662"
        ),
    },
    "email_followup": {
        "name": "Email Follow-up", "category": "email", "industry": "general",
        "script": (
            "Subject: Quick follow-up, {contact_name}\n\n"
            "Hi {contact_name},\n\nI reached out a few days ago about AI phone answering for "
            "{business_name}. Our clients see an average 40% increase in booked appointments.\n\n"
            "The #1 reason? Speed. Our AI answers on the first ring, every time.\n\n"
            "Worth a 5-minute call this week?\n\nBest,\nAlex\nHive Dynamics"
        ),
    },
    "email_case_study": {
        "name": "Email Case Study", "category": "email", "industry": "general",
        "script": (
            "Subject: How a {industry} business added $4,200/mo with AI\n\n"
            "Hi {contact_name},\n\nA {industry} business in Florida was missing ~35% of calls. "
            "After our AI Phone Agent:\n- Answer rate: 65% to 100%\n"
            "- Appointments: +42%\n- Revenue: +$4,200/mo\n- Setup: $499 one-time\n- Monthly: $99/month\n"
            "- ROI: Paid back setup cost in first 4 days\n\n"
            "Want to see what those numbers look like for {business_name}?\n\n"
            "Best,\nAlex\nHive Dynamics"
        ),
    },
    "email_final": {
        "name": "Email Final", "category": "email", "industry": "general",
        "script": (
            "Subject: Last note from me, {contact_name}\n\n"
            "Hi {contact_name},\n\nI don't want to be a pest, so this is my last email.\n\n"
            "If you ever want AI phone answering for {business_name}, we're here:\n"
            "{payment_link}\n\nWishing you a great rest of the year.\n\n"
            "Best,\nAlex\nHive Dynamics"
        ),
    },
}

EMAIL_SEQUENCE = {0: "email_intro", 3: "email_followup", 7: "email_case_study", 14: "email_final"}

INDUSTRY_PAIN_POINTS = {
    "restaurant": ["Missed reservation calls during rush", "Menu/hours questions tying up staff",
                    "Takeout order mistakes over the phone"],
    "medical": ["Patients on hold for scheduling", "After-hours urgent calls to voicemail",
                "Staff overwhelmed with insurance calls"],
    "legal": ["Potential clients calling after hours", "Missing high-value case inquiries",
              "Receptionist can't qualify leads"],
    "real_estate": ["Buyer inquiries missed while showing", "After-hours calls from listings",
                    "Lead qualification taking too much time"],
    "contractor": ["Missing calls on the job site", "Losing bids to faster competitors",
                   "No way to collect job details while working"],
    "dental": ["Missed appointment scheduling", "Post-procedure questions after hours",
               "Insurance verification bottleneck"],
    "automotive": ["Service scheduling overflow", "Parts calls during peak hours",
                   "After-hours emergency towing inquiries"],
    "general": ["Missing calls during busy periods", "After-hours voicemail",
                "Staff spending too much time on the phone"],
}

# ==========================================================================
# DATABASE
# ==========================================================================
def get_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn

def row_to_dict(row):
    return dict(row) if row else None

def rows_to_list(rows):
    return [dict(r) for r in rows]

def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS prospects (
            id TEXT PRIMARY KEY,
            business_name TEXT NOT NULL,
            contact_name TEXT DEFAULT '',
            phone TEXT DEFAULT '',
            email TEXT DEFAULT '',
            industry TEXT DEFAULT 'general',
            location TEXT DEFAULT '',
            website TEXT DEFAULT '',
            status TEXT DEFAULT 'active',
            stage TEXT DEFAULT 'prospect',
            source TEXT DEFAULT 'manual',
            package TEXT DEFAULT '',
            notes TEXT DEFAULT '',
            pain_points TEXT DEFAULT '[]',
            review_score REAL DEFAULT 0,
            review_count INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now')),
            last_contacted TEXT,
            next_followup TEXT
        );
        CREATE TABLE IF NOT EXISTS call_log (
            id TEXT PRIMARY KEY,
            prospect_id TEXT NOT NULL,
            direction TEXT DEFAULT 'outbound',
            phone TEXT DEFAULT '',
            status TEXT DEFAULT 'initiated',
            outcome TEXT DEFAULT '',
            duration_seconds INTEGER DEFAULT 0,
            transcript TEXT DEFAULT '',
            script_used TEXT DEFAULT '',
            notes TEXT DEFAULT '',
            twilio_sid TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (prospect_id) REFERENCES prospects(id)
        );
        CREATE TABLE IF NOT EXISTS email_log (
            id TEXT PRIMARY KEY,
            prospect_id TEXT NOT NULL,
            to_email TEXT DEFAULT '',
            subject TEXT DEFAULT '',
            body TEXT DEFAULT '',
            template TEXT DEFAULT '',
            status TEXT DEFAULT 'sent',
            opened INTEGER DEFAULT 0,
            clicked INTEGER DEFAULT 0,
            replied INTEGER DEFAULT 0,
            opened_at TEXT,
            clicked_at TEXT,
            tracking_id TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (prospect_id) REFERENCES prospects(id)
        );
        CREATE TABLE IF NOT EXISTS campaign_runs (
            id TEXT PRIMARY KEY,
            name TEXT DEFAULT '',
            status TEXT DEFAULT 'running',
            prospects_total INTEGER DEFAULT 0,
            calls_made INTEGER DEFAULT 0,
            emails_sent INTEGER DEFAULT 0,
            demos_booked INTEGER DEFAULT 0,
            deals_closed INTEGER DEFAULT 0,
            started_at TEXT DEFAULT (datetime('now')),
            stopped_at TEXT,
            config TEXT DEFAULT '{}'
        );
        CREATE TABLE IF NOT EXISTS scripts (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT DEFAULT 'general',
            industry TEXT DEFAULT 'general',
            script TEXT NOT NULL,
            usage_count INTEGER DEFAULT 0,
            success_rate REAL DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS payments (
            id TEXT PRIMARY KEY,
            prospect_id TEXT NOT NULL,
            package TEXT NOT NULL,
            amount_cents INTEGER NOT NULL,
            setup_fee_cents INTEGER DEFAULT 0,
            stripe_link TEXT DEFAULT '',
            stripe_session_id TEXT DEFAULT '',
            status TEXT DEFAULT 'pending',
            created_at TEXT DEFAULT (datetime('now')),
            paid_at TEXT,
            FOREIGN KEY (prospect_id) REFERENCES prospects(id)
        );
        CREATE TABLE IF NOT EXISTS activity_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            action TEXT NOT NULL,
            details TEXT DEFAULT '',
            prospect_id TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_prospects_stage ON prospects(stage);
        CREATE INDEX IF NOT EXISTS idx_prospects_industry ON prospects(industry);
        CREATE INDEX IF NOT EXISTS idx_call_log_prospect ON call_log(prospect_id);
        CREATE INDEX IF NOT EXISTS idx_email_log_prospect ON email_log(prospect_id);
        CREATE INDEX IF NOT EXISTS idx_email_log_tracking ON email_log(tracking_id);
        CREATE INDEX IF NOT EXISTS idx_activity_created ON activity_log(created_at);
    """)
    # Migrate: add setup_fee_cents column if missing
    try:
        conn.execute("ALTER TABLE payments ADD COLUMN setup_fee_cents INTEGER DEFAULT 0")
        conn.commit()
    except Exception:
        pass  # column already exists

    # Insert default scripts
    for sid, data in DEFAULT_SCRIPTS.items():
        conn.execute(
            "INSERT OR IGNORE INTO scripts (id,name,category,industry,script) VALUES (?,?,?,?,?)",
            (sid, data["name"], data["category"], data["industry"], data["script"]))
    # Sample prospects
    samples = [
        ("s001","Bella's Italian Kitchen","Maria Rossi","+18505551001","maria@bellaskitchen.com","restaurant","Pensacola, FL","bellaskitchen.com"),
        ("s002","Gulf Coast Family Medicine","Dr. James Carter","+18505551002","jcarter@gcfm.com","medical","Pensacola, FL","gcfamilymedicine.com"),
        ("s003","Sterling & Associates Law","David Sterling","+18505551003","david@sterlinglaw.com","legal","Tallahassee, FL","sterlinglaw.com"),
        ("s004","Emerald Coast Realty","Sarah Mitchell","+18505551004","sarah@ecrealty.com","real_estate","Destin, FL","emeraldcoastrealty.com"),
        ("s005","ProFlow Plumbing & HVAC","Mike Johnson","+18505551005","mike@proflowph.com","contractor","Pensacola, FL","proflowplumbing.com"),
        ("s006","Sunshine Dental Care","Dr. Lisa Chen","+18505551006","lisa@sunshinedental.com","dental","Fort Walton Beach, FL","sunshinedentalcare.com"),
        ("s007","AutoMax Service Center","Tom Rivera","+18505551007","tom@automaxsc.com","automotive","Pensacola, FL","automaxservice.com"),
        ("s008","The Corner Barbershop","James Williams","+18505551008","james@cornerbarbershop.com","general","Pensacola, FL",""),
        ("s009","Coastal Landscaping Co","Ryan O'Brien","+18505551009","ryan@coastallandscaping.com","contractor","Navarre, FL","coastallandscapingco.com"),
        ("s010","Bay Breeze Pet Clinic","Dr. Amanda Foster","+18505551010","amanda@baybreezevet.com","medical","Gulf Breeze, FL","baybreezepetclinic.com"),
    ]
    for s in samples:
        conn.execute(
            "INSERT OR IGNORE INTO prospects (id,business_name,contact_name,phone,email,industry,location,website) VALUES (?,?,?,?,?,?,?,?)", s)
    conn.commit()
    conn.close()

def log_activity(action, details="", prospect_id=""):
    try:
        conn = get_db()
        conn.execute("INSERT INTO activity_log (action,details,prospect_id) VALUES (?,?,?)",
                     (action, details, prospect_id))
        conn.commit(); conn.close()
    except Exception:
        pass

# ==========================================================================
# HELPERS
# ==========================================================================
def clean_phone(number):
    c = re.sub(r"[^0-9+]", "", str(number))
    if not c.startswith("+") and len(c) == 10:
        c = "+1" + c
    elif not c.startswith("+"):
        c = "+" + c.lstrip("+")
    return c

def is_business_hours():
    ct = datetime.now(timezone(timedelta(hours=CT_OFFSET)))
    return ct.weekday() < 5 and BIZ_HOUR_START <= ct.hour < BIZ_HOUR_END

def personalize_script(script, prospect, package_key="ai_phone_agent"):
    pkg = PACKAGES.get(package_key, PACKAGES["ai_phone_agent"])
    r = {"{business_name}": prospect.get("business_name","your business"),
         "{contact_name}": prospect.get("contact_name","there"),
         "{industry}": prospect.get("industry","").replace("_"," "),
         "{location}": prospect.get("location","your area"),
         "{phone}": prospect.get("phone",""),
         "{email}": prospect.get("email",""),
         "{website}": prospect.get("website",""),
         "{package_name}": pkg["name"],
         "{package_price}": pkg["price_display"],
         "{setup_fee}": pkg.get("setup_fee_display","$499"),
         "{payment_link}": ""}
    result = script
    for k, v in r.items():
        result = result.replace(k, v)
    return result

async def query_ollama(prompt, system="", timeout=30.0):
    payload = {"model": OLLAMA_MODEL, "prompt": prompt, "stream": False,
               "system": system or "You are Alex, a friendly sales rep for Hive Dynamics selling AI phone answering. Be concise.",
               "options": {"temperature": 0.7, "num_predict": 300}}
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(f"{OLLAMA_URL}/api/generate", json=payload)
            if resp.status_code == 200:
                return resp.json().get("response","").strip()
            payload["model"] = OLLAMA_FALLBACK
            resp = await client.post(f"{OLLAMA_URL}/api/generate", json=payload)
            if resp.status_code == 200:
                return resp.json().get("response","").strip()
    except Exception as e:
        print(f"[Ollama] {e}")
    return ""

async def notify_telegram(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            await c.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                         json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"})
    except Exception:
        pass

async def report_to_nerve(category, fact):
    try:
        async with httpx.AsyncClient(timeout=5) as c:
            await c.post(f"{NERVE_URL}/learn", json={"category": category, "fact": fact, "source": "cold_caller"})
    except Exception:
        pass

# ==========================================================================
# TWILIO CALLS
# ==========================================================================
async def make_outbound_call(prospect, script_text):
    phone = clean_phone(prospect.get("phone",""))
    if not phone or len(phone) < 10:
        return {"error": "Invalid phone", "status": "failed"}
    if not TWILIO_AUTH_TOKEN:
        cid = str(uuid.uuid4())
        return {"status": "demo_mode", "call_id": cid, "twilio_sid": f"DEMO_{cid[:8]}",
                "message": "Twilio not configured. Simulating call."}
    try:
        twiml_url = f"{WEBHOOK_BASE}/cold-caller/voice?prospect_id={prospect['id']}"
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Calls.json",
                auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN),
                data={"To": phone, "From": TWILIO_PHONE, "Url": twiml_url,
                      "StatusCallback": f"{WEBHOOK_BASE}/cold-caller/status",
                      "StatusCallbackEvent": ["initiated","ringing","answered","completed"],
                      "MachineDetection": "DetectMessageEnd", "Timeout": 30})
            if resp.status_code in (200, 201):
                d = resp.json()
                return {"status": "initiated", "twilio_sid": d.get("sid",""), "call_id": d.get("sid","")}
            return {"status": "failed", "error": resp.text}
    except Exception as e:
        return {"status": "failed", "error": str(e)}

# ==========================================================================
# EMAIL ENGINE
# ==========================================================================
async def send_email(prospect, template_key, custom_subject="", custom_body=""):
    to_email = prospect.get("email","")
    if not to_email:
        return {"error": "No email", "status": "failed"}

    if custom_body:
        body, subject = custom_body, custom_subject or f"AI Phone Answering for {prospect.get('business_name','')}"
    else:
        conn = get_db()
        row = conn.execute("SELECT script FROM scripts WHERE id=?", (template_key,)).fetchone()
        conn.close()
        text = personalize_script(dict(row)["script"] if row else DEFAULT_SCRIPTS.get(template_key,{}).get("script",""), prospect)
        if text.startswith("Subject:"):
            lines = text.split("\n", 1)
            subject = lines[0].replace("Subject:","").strip()
            body = lines[1].strip() if len(lines) > 1 else ""
        else:
            subject = f"AI Phone Answering for {prospect.get('business_name','')}"
            body = text

    tracking_id = str(uuid.uuid4())
    pixel = f'<img src="http://100.70.226.103:{PORT}/track/open/{tracking_id}" width="1" height="1" style="display:none"/>'
    html_body = f"""<div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;padding:20px">
<div style="white-space:pre-line;line-height:1.6;color:#333">{body}</div>
<hr style="border:none;border-top:1px solid #eee;margin:30px 0">
<p style="font-size:12px;color:#999">Hive Dynamics | AI-Powered Business Solutions<br>
<a href="https://hivedynamics.ai">hivedynamics.ai</a> | 850-801-6662</p>{pixel}</div>"""

    sent = False
    # Try Hive mail server
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.post(f"{MAIL_SERVER_URL}/send",
                             json={"to": to_email, "subject": subject, "body_html": html_body,
                                   "body_text": body, "from_email": FROM_EMAIL, "from_name": FROM_NAME})
            if r.status_code == 200: sent = True
    except Exception:
        pass
    # SMTP fallback
    if not sent and SMTP_HOST:
        try:
            msg = MIMEMultipart("alternative")
            msg["From"] = f"{FROM_NAME} <{FROM_EMAIL}>"
            msg["To"] = to_email
            msg["Subject"] = subject
            msg.attach(MIMEText(body, "plain"))
            msg.attach(MIMEText(html_body, "html"))
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as srv:
                srv.starttls(); srv.login(SMTP_USER, SMTP_PASS); srv.send_message(msg)
            sent = True
        except Exception as e:
            print(f"[SMTP] {e}")

    eid = str(uuid.uuid4())
    conn = get_db()
    conn.execute("INSERT INTO email_log (id,prospect_id,to_email,subject,body,template,status,tracking_id) VALUES (?,?,?,?,?,?,?,?)",
                 (eid, prospect["id"], to_email, subject, body, template_key, "sent" if sent else "queued", tracking_id))
    conn.commit(); conn.close()
    log_activity("email_sent", f"{template_key} to {prospect.get('business_name','')}", prospect["id"])
    return {"status": "sent" if sent else "queued", "email_id": eid, "tracking_id": tracking_id, "to": to_email, "subject": subject}

# ==========================================================================
# STRIPE PAYMENT LINKS
# ==========================================================================
async def create_payment_link(prospect, package_key):
    pkg = PACKAGES.get(package_key)
    if not pkg:
        return {"error": f"Unknown package: {package_key}"}
    pid = str(uuid.uuid4())
    setup_fee = pkg.get("setup_fee_cents", 0)
    total_first_payment = setup_fee + pkg["price_cents"]

    if STRIPE_API_KEY == "sk_test_demo" or not STRIPE_API_KEY:
        link = f"https://hivedynamics.ai/pay/{pid[:8]}"
        conn = get_db()
        conn.execute("INSERT INTO payments (id,prospect_id,package,amount_cents,setup_fee_cents,stripe_link,status) VALUES (?,?,?,?,?,?,?)",
                     (pid, prospect["id"], package_key, pkg["price_cents"], setup_fee, link, "pending"))
        conn.execute("UPDATE prospects SET package=?, updated_at=datetime('now') WHERE id=?", (package_key, prospect["id"]))
        conn.commit(); conn.close()
        log_activity("payment_link",
                     f"{pkg['name']} {pkg.get('setup_fee_display','$0')} setup + {pkg['price_display']} for {prospect.get('business_name','')}",
                     prospect["id"])
        return {"status": "demo_mode", "payment_id": pid, "link": link,
                "package": pkg["name"], "price": pkg["price_display"],
                "setup_fee": pkg.get("setup_fee_display", "$0"),
                "total_first_payment": f"${total_first_payment/100:,.2f}",
                "message": "Demo link. Set STRIPE_API_KEY for live payments."}

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            # Create one-time setup fee price
            setup_resp = await client.post("https://api.stripe.com/v1/prices",
                headers={"Authorization": f"Bearer {STRIPE_API_KEY}"},
                data={"unit_amount": setup_fee, "currency": "usd",
                      "product_data[name]": f"Setup Fee — {pkg['name']} — {prospect.get('business_name','')}"})
            if setup_resp.status_code != 200:
                return {"error": f"Stripe setup price failed: {setup_resp.text}"}
            setup_price_id = setup_resp.json()["id"]

            # Create recurring monthly price
            sub_resp = await client.post("https://api.stripe.com/v1/prices",
                headers={"Authorization": f"Bearer {STRIPE_API_KEY}"},
                data={"unit_amount": pkg["price_cents"], "currency": "usd",
                      "recurring[interval]": "month",
                      "product_data[name]": f"{pkg['name']} — {prospect.get('business_name','')}"})
            if sub_resp.status_code != 200:
                return {"error": f"Stripe subscription price failed: {sub_resp.text}"}
            sub_price_id = sub_resp.json()["id"]

            # Create payment link with both line items (setup fee + first month)
            lr = await client.post("https://api.stripe.com/v1/payment_links",
                headers={"Authorization": f"Bearer {STRIPE_API_KEY}"},
                data={"line_items[0][price]": setup_price_id, "line_items[0][quantity]": "1",
                      "line_items[1][price]": sub_price_id, "line_items[1][quantity]": "1",
                      "metadata[prospect_id]": prospect["id"],
                      "metadata[package]": package_key,
                      "metadata[setup_fee_cents]": str(setup_fee)})
            if lr.status_code != 200:
                return {"error": f"Stripe link failed: {lr.text}"}
            ld = lr.json()
            conn = get_db()
            conn.execute("INSERT INTO payments (id,prospect_id,package,amount_cents,setup_fee_cents,stripe_link,stripe_session_id,status) VALUES (?,?,?,?,?,?,?,?)",
                         (pid, prospect["id"], package_key, pkg["price_cents"], setup_fee, ld["url"], ld.get("id",""), "pending"))
            conn.execute("UPDATE prospects SET package=?, updated_at=datetime('now') WHERE id=?", (package_key, prospect["id"]))
            conn.commit(); conn.close()
            log_activity("payment_link",
                         f"{pkg['name']} {pkg.get('setup_fee_display','$0')} setup + {pkg['price_display']}",
                         prospect["id"])
            return {"status": "live", "payment_id": pid, "link": ld["url"],
                    "package": pkg["name"], "price": pkg["price_display"],
                    "setup_fee": pkg.get("setup_fee_display", "$0"),
                    "total_first_payment": f"${total_first_payment/100:,.2f}"}
    except Exception as e:
        return {"error": str(e)}

# ==========================================================================
# CAMPAIGN ENGINE
# ==========================================================================
async def run_campaign_cycle(campaign_id, config):
    import asyncio
    global CAMPAIGN_RUNNING
    conn = get_db()
    stages = config.get("target_stages", ["prospect","contacted"])
    industries = config.get("industries", [])
    limit = config.get("batch_size", 10)

    q = "SELECT * FROM prospects WHERE stage IN ({})".format(",".join("?" * len(stages)))
    p = list(stages)
    if industries:
        q += " AND industry IN ({})".format(",".join("?" * len(industries)))
        p.extend(industries)
    q += " ORDER BY last_contacted ASC NULLS FIRST LIMIT ?"
    p.append(limit)
    prospects = rows_to_list(conn.execute(q, p).fetchall())
    conn.close()

    calls_made = emails_sent = 0
    for pr in prospects:
        if not CAMPAIGN_RUNNING: break
        conn = get_db()
        nc = dict(conn.execute("SELECT COUNT(*) as c FROM call_log WHERE prospect_id=?", (pr["id"],)).fetchone())["c"]
        ne = dict(conn.execute("SELECT COUNT(*) as c FROM email_log WHERE prospect_id=?", (pr["id"],)).fetchone())["c"]
        conn.close()

        # Call if business hours and never called
        if config.get("enable_calls", True) and is_business_hours() and nc == 0 and pr.get("phone"):
            ind = pr.get("industry","general")
            sk = f"cold_call_{ind}" if f"cold_call_{ind}" in DEFAULT_SCRIPTS else "cold_call_opener"
            script = personalize_script(DEFAULT_SCRIPTS[sk]["script"], pr)
            result = await make_outbound_call(pr, script)
            cid = str(uuid.uuid4())
            conn = get_db()
            conn.execute("INSERT INTO call_log (id,prospect_id,direction,phone,status,script_used,twilio_sid) VALUES (?,?,?,?,?,?,?)",
                         (cid, pr["id"], "outbound", pr.get("phone",""), result.get("status","failed"), sk, result.get("twilio_sid","")))
            conn.execute("UPDATE prospects SET stage='contacted', last_contacted=datetime('now'), updated_at=datetime('now') WHERE id=? AND stage='prospect'", (pr["id"],))
            conn.commit(); conn.close()
            calls_made += 1
            log_activity("campaign_call", f"Called {pr.get('business_name','')}", pr["id"])
            await asyncio.sleep(5)

        # Email next in sequence
        if config.get("enable_emails", True) and pr.get("email") and ne < len(EMAIL_SEQUENCE):
            seq_keys = list(EMAIL_SEQUENCE.values())
            seq_days = list(EMAIL_SEQUENCE.keys())
            idx = min(ne, len(seq_keys)-1)
            should_send = True
            if ne > 0:
                conn = get_db()
                last = conn.execute("SELECT created_at FROM email_log WHERE prospect_id=? ORDER BY created_at DESC LIMIT 1", (pr["id"],)).fetchone()
                conn.close()
                if last:
                    ld = datetime.fromisoformat(dict(last)["created_at"])
                    if (datetime.now() - ld).days < seq_days[idx]:
                        should_send = False
            if should_send:
                await send_email(pr, seq_keys[idx])
                emails_sent += 1
                conn = get_db()
                conn.execute("UPDATE prospects SET stage='contacted', last_contacted=datetime('now'), updated_at=datetime('now') WHERE id=? AND stage='prospect'", (pr["id"],))
                conn.commit(); conn.close()
                await asyncio.sleep(2)

    conn = get_db()
    conn.execute("UPDATE campaign_runs SET calls_made=calls_made+?, emails_sent=emails_sent+? WHERE id=?",
                 (calls_made, emails_sent, campaign_id))
    conn.commit(); conn.close()
    return {"calls_made": calls_made, "emails_sent": emails_sent}

def campaign_loop(campaign_id, config):
    import asyncio
    global CAMPAIGN_RUNNING
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    interval = config.get("cycle_interval_minutes", 30) * 60
    while CAMPAIGN_RUNNING:
        try:
            loop.run_until_complete(run_campaign_cycle(campaign_id, config))
        except Exception as e:
            print(f"[Campaign] {e}"); traceback.print_exc()
        for _ in range(int(interval)):
            if not CAMPAIGN_RUNNING: break
            time.sleep(1)
    conn = get_db()
    conn.execute("UPDATE campaign_runs SET status='stopped', stopped_at=datetime('now') WHERE id=?", (campaign_id,))
    conn.commit(); conn.close()
    loop.close()

# ==========================================================================
# PROSPECT SCRAPING
# ==========================================================================
async def scrape_google_maps(query, location, limit=20):
    gkey = os.environ.get("GOOGLE_API_KEY","")
    results = []
    if gkey:
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get("https://maps.googleapis.com/maps/api/place/textsearch/json",
                    params={"query": f"{query} in {location}", "key": gkey, "type": "establishment"})
                if resp.status_code == 200:
                    for place in resp.json().get("results",[])[:limit]:
                        entry = {"business_name": place.get("name",""), "location": place.get("formatted_address",location),
                                 "rating": place.get("rating",0), "review_count": place.get("user_ratings_total",0)}
                        if place.get("place_id"):
                            dr = await client.get("https://maps.googleapis.com/maps/api/place/details/json",
                                params={"place_id": place["place_id"], "fields": "formatted_phone_number,website", "key": gkey})
                            if dr.status_code == 200:
                                d = dr.json().get("result",{})
                                entry["phone"] = d.get("formatted_phone_number","")
                                entry["website"] = d.get("website","")
                        results.append(entry)
        except Exception as e:
            print(f"[Scrape] {e}")
    else:
        results = [{"business_name": f"[Demo] {query} #{i+1}", "location": location, "phone":"", "website":"",
                     "rating": 0, "note": "Set GOOGLE_API_KEY for live scraping"} for i in range(min(limit,5))]
    return results

# ==========================================================================
# FASTAPI
# ==========================================================================
app = FastAPI(title="Hive Cold Caller & Sales Agent", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# --- Pydantic ---
class ProspectCreate(BaseModel):
    business_name: str; contact_name: str = ""; phone: str = ""; email: str = ""
    industry: str = "general"; location: str = ""; website: str = ""; notes: str = ""; source: str = "manual"

class ProspectUpdate(BaseModel):
    business_name: Optional[str] = None; contact_name: Optional[str] = None
    phone: Optional[str] = None; email: Optional[str] = None
    industry: Optional[str] = None; location: Optional[str] = None
    website: Optional[str] = None; stage: Optional[str] = None
    status: Optional[str] = None; notes: Optional[str] = None
    package: Optional[str] = None; next_followup: Optional[str] = None

class ScrapeRequest(BaseModel):
    query: str; location: str; limit: int = 20; auto_add: bool = True

class CampaignConfig(BaseModel):
    name: str = "Auto Campaign"; target_stages: List[str] = ["prospect","contacted"]
    industries: List[str] = []; enable_calls: bool = True; enable_emails: bool = True
    batch_size: int = 10; cycle_interval_minutes: int = 30

class ScriptCreate(BaseModel):
    name: str; category: str = "general"; industry: str = "general"; script: str

class CallRequest(BaseModel):
    script_override: Optional[str] = None; package: Optional[str] = None

class EmailRequest(BaseModel):
    template: Optional[str] = None; custom_subject: Optional[str] = None; custom_body: Optional[str] = None

class PaymentRequest(BaseModel):
    package: str = "ai_phone_agent"

# --- Startup ---
@app.on_event("startup")
async def startup():
    init_db()
    log_activity("system", "Cold Caller started on port 8332")
    print(f"[Cold Caller] Port {PORT} | Twilio: {'OK' if TWILIO_AUTH_TOKEN else 'DEMO'} | Stripe: {'LIVE' if STRIPE_API_KEY != 'sk_test_demo' else 'DEMO'}")

# ==========================================================================
# DASHBOARD
# ==========================================================================
@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    conn = get_db()
    pipeline = {}
    for st in ["prospect","contacted","interested","demo","proposal","closed_won","closed_lost"]:
        pipeline[st] = dict(conn.execute("SELECT COUNT(*) as c FROM prospects WHERE stage=?", (st,)).fetchone())["c"]
    total = sum(pipeline.values())
    calls_today = dict(conn.execute("SELECT COUNT(*) as c FROM call_log WHERE date(created_at)=date('now')").fetchone())["c"]
    calls_total = dict(conn.execute("SELECT COUNT(*) as c FROM call_log").fetchone())["c"]
    emails_total = dict(conn.execute("SELECT COUNT(*) as c FROM email_log").fetchone())["c"]
    emails_opened = dict(conn.execute("SELECT COUNT(*) as c FROM email_log WHERE opened=1").fetchone())["c"]
    mrr_r = conn.execute("SELECT COALESCE(SUM(p.amount_cents),0) as t FROM payments p JOIN prospects pr ON p.prospect_id=pr.id WHERE pr.stage='closed_won' AND p.status='paid'").fetchone()
    mrr = dict(mrr_r)["t"] / 100.0
    setup_r = conn.execute("SELECT COALESCE(SUM(p.setup_fee_cents),0) as t FROM payments p JOIN prospects pr ON p.prospect_id=pr.id WHERE pr.stage='closed_won' AND p.status='paid'").fetchone()
    setup_rev = dict(setup_r)["t"] / 100.0
    recent = rows_to_list(conn.execute("SELECT * FROM activity_log ORDER BY created_at DESC LIMIT 20").fetchall())
    conn.close()

    contacted = total - pipeline["prospect"]
    interested = pipeline["interested"] + pipeline["demo"] + pipeline["proposal"] + pipeline["closed_won"]
    cr = round(contacted / max(total,1) * 100, 1)
    ir = round(interested / max(contacted,1) * 100, 1)
    wr = round(pipeline["closed_won"] / max(interested,1) * 100, 1)

    acts = ""
    for a in recent:
        acts += f'<div class="ai"><span class="at">{a.get("action","").replace("_"," ").title()}</span> {a.get("details","")} <small>{a.get("created_at","")}</small></div>'

    bars = ""
    colors = {"prospect":"#6366f1","contacted":"#8b5cf6","interested":"#a78bfa","demo":"#f59e0b","proposal":"#f97316","closed_won":"#22c55e","closed_lost":"#ef4444"}
    for st, cnt in pipeline.items():
        h = max(cnt / max(total,1) * 180, 6)
        bars += f'<div class="ps"><div class="pc">{cnt}</div><div class="pb" style="height:{h}px;background:{colors[st]}"></div><div class="pl">{st.replace("_"," ")}</div></div>'

    pkgs = ""
    for p in PACKAGES.values():
        pkgs += f'<div class="pk"><div class="pn">{p["name"]}</div><div class="pp">{p["price_display"]}</div><div style="font-size:11px;color:#f59e0b;margin-top:2px">{p.get("setup_fee_display","$0")} setup</div></div>'

    html = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Hive Cold Caller</title><style>
*{{margin:0;padding:0;box-sizing:border-box}}body{{font-family:'Segoe UI',system-ui,sans-serif;background:#0a0a0f;color:#e0e0e0}}
.hd{{background:linear-gradient(135deg,#1a1a2e,#16213e,#0f3460);padding:20px 28px;border-bottom:2px solid #f59e0b;display:flex;justify-content:space-between;align-items:center}}
.hd h1{{font-size:22px;color:#f59e0b}}.hd .sub{{color:#9ca3af;font-size:13px}}
.bh{{padding:5px 14px;border-radius:16px;font-size:12px;font-weight:600}}
.bo{{background:#065f46;color:#34d399}}.bc{{background:#7f1d1d;color:#fca5a5}}
.ct{{max-width:1400px;margin:0 auto;padding:20px}}.g4{{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:20px}}
.g2{{display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:20px}}
.sc{{background:#1a1a2e;border:1px solid #2d2d44;border-radius:10px;padding:18px;text-align:center}}
.sc .v{{font-size:34px;font-weight:700;color:#f59e0b}}.sc .l{{font-size:12px;color:#9ca3af;text-transform:uppercase;letter-spacing:1px;margin-top:3px}}
.cd{{background:#1a1a2e;border:1px solid #2d2d44;border-radius:10px;padding:20px}}
.cd h2{{font-size:16px;color:#f59e0b;margin-bottom:14px;border-bottom:1px solid #2d2d44;padding-bottom:6px}}
.pp1{{display:flex;gap:6px;align-items:flex-end;height:200px;padding:14px 0}}
.ps{{flex:1;display:flex;flex-direction:column;align-items:center;justify-content:flex-end}}
.pb{{width:100%;border-radius:5px 5px 0 0;min-height:4px}}.pc{{font-size:18px;font-weight:700;margin-bottom:3px}}
.pl{{font-size:10px;color:#9ca3af;text-align:center;margin-top:6px;text-transform:uppercase}}
.mv{{font-size:44px;font-weight:800;color:#22c55e;text-align:center;margin:18px 0}}
.ml{{text-align:center;color:#9ca3af;font-size:13px}}
.rt{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:14px;margin-top:14px}}
.re{{text-align:center;padding:10px;background:#0f0f1a;border-radius:6px}}
.re .rp{{font-size:22px;font-weight:700;color:#60a5fa}}.re .rl{{font-size:11px;color:#9ca3af;margin-top:3px}}
.ai{{padding:8px 0;border-bottom:1px solid #1f1f33;font-size:13px}}.at{{font-weight:600;color:#f59e0b}}
.ai small{{color:#6b7280;font-size:11px;margin-left:8px}}
.btn{{padding:9px 20px;border-radius:7px;border:none;cursor:pointer;font-weight:600;font-size:13px}}
.bs{{background:#22c55e;color:#000}}.bp{{background:#ef4444;color:#fff}}.br{{background:#3b82f6;color:#fff}}
.pks{{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-top:14px}}
.pk{{background:#0f0f1a;border:1px solid #2d2d44;border-radius:7px;padding:12px;text-align:center}}
.pn{{font-weight:700;color:#f59e0b;font-size:13px}}.pp{{font-size:22px;font-weight:800;color:#22c55e;margin:6px 0}}
.cs{{padding:4px 12px;border-radius:12px;font-size:12px;font-weight:600}}
.cr{{background:#065f46;color:#34d399}}.cx{{background:#7f1d1d;color:#fca5a5}}
</style></head><body>
<div class="hd"><div><h1>HIVE COLD CALLER</h1><div class="sub">AI Sales Agent | Port {PORT}</div></div>
<div style="display:flex;gap:14px;align-items:center">
<span class="cs {'cr' if CAMPAIGN_RUNNING else 'cx'}">Campaign: {'RUNNING' if CAMPAIGN_RUNNING else 'STOPPED'}</span>
<span class="bh {'bo' if is_business_hours() else 'bc'}">{'BIZ HOURS' if is_business_hours() else 'AFTER HOURS'}</span>
<a href="/dashboard" class="btn br">Refresh</a></div></div>
<div class="ct">
<div class="g4">
<div class="sc"><div class="v">{total}</div><div class="l">Total Prospects</div></div>
<div class="sc"><div class="v">{calls_today}</div><div class="l">Calls Today</div></div>
<div class="sc"><div class="v">{emails_total}</div><div class="l">Emails Sent</div></div>
<div class="sc"><div class="v">{pipeline['closed_won']}</div><div class="l">Deals Closed</div></div></div>
<div class="g2">
<div class="cd"><h2>Sales Pipeline</h2><div class="pp1">{bars}</div>
<div class="rt"><div class="re"><div class="rp">{cr}%</div><div class="rl">Contact Rate</div></div>
<div class="re"><div class="rp">{ir}%</div><div class="rl">Interest Rate</div></div>
<div class="re"><div class="rp">{wr}%</div><div class="rl">Close Rate</div></div></div></div>
<div class="cd"><h2>Revenue</h2><div class="mv">${mrr:,.2f}</div><div class="ml">Monthly Recurring Revenue (MRR)</div>
<div style="text-align:center;margin:10px 0"><span style="font-size:28px;font-weight:700;color:#f59e0b">${setup_rev:,.2f}</span><br><span style="font-size:12px;color:#9ca3af">Setup Fee Revenue (one-time)</span></div>
<div style="text-align:center;margin-bottom:12px;padding:8px;background:#0f0f1a;border-radius:6px"><span style="font-size:14px;color:#60a5fa">Total Collected: </span><span style="font-size:18px;font-weight:700;color:#22c55e">${mrr + setup_rev:,.2f}</span></div>
<div class="pks">{pkgs}</div>
<div style="margin-top:16px"><h2>Campaign</h2><div style="display:flex;gap:10px;margin-top:10px">
<button class="btn bs" onclick="fetch('/campaign/start',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{name:'Dashboard Campaign'}})}}).then(r=>r.json()).then(d=>{{alert(JSON.stringify(d));location.reload()}})">Start</button>
<button class="btn bp" onclick="fetch('/campaign/stop',{{method:'POST'}}).then(r=>r.json()).then(d=>{{alert(JSON.stringify(d));location.reload()}})">Stop</button>
</div></div></div></div>
<div class="g2">
<div class="cd"><h2>Stats</h2><div class="g4" style="grid-template-columns:1fr 1fr">
<div class="sc"><div class="v">{calls_total}</div><div class="l">Total Calls</div></div>
<div class="sc"><div class="v">{emails_opened}</div><div class="l">Emails Opened</div></div></div></div>
<div class="cd" style="max-height:380px;overflow-y:auto"><h2>Recent Activity</h2>
{acts if acts else '<p style="color:#6b7280;text-align:center;padding:16px">No activity yet.</p>'}</div></div></div>
<script>setTimeout(()=>location.reload(),30000)</script></body></html>"""
    return HTMLResponse(html)

# ==========================================================================
# PROSPECT ENDPOINTS
# ==========================================================================
@app.get("/prospects")
async def list_prospects(stage: Optional[str]=None, industry: Optional[str]=None,
                         source: Optional[str]=None, search: Optional[str]=None,
                         limit: int=100, offset: int=0):
    conn = get_db()
    q, p = "SELECT * FROM prospects WHERE 1=1", []
    if stage: q += " AND stage=?"; p.append(stage)
    if industry: q += " AND industry=?"; p.append(industry)
    if source: q += " AND source=?"; p.append(source)
    if search: q += " AND (business_name LIKE ? OR contact_name LIKE ? OR email LIKE ?)"; p += [f"%{search}%"]*3
    q += " ORDER BY updated_at DESC LIMIT ? OFFSET ?"; p += [limit, offset]
    prospects = rows_to_list(conn.execute(q, p).fetchall())
    total = dict(conn.execute("SELECT COUNT(*) as c FROM prospects").fetchone())["c"]
    conn.close()
    return {"prospects": prospects, "total": total, "limit": limit, "offset": offset}

@app.get("/prospects/{pid}")
async def get_prospect(pid: str):
    conn = get_db()
    pr = row_to_dict(conn.execute("SELECT * FROM prospects WHERE id=?", (pid,)).fetchone())
    if not pr: conn.close(); raise HTTPException(404, "Not found")
    calls = rows_to_list(conn.execute("SELECT * FROM call_log WHERE prospect_id=? ORDER BY created_at DESC", (pid,)).fetchall())
    emails = rows_to_list(conn.execute("SELECT * FROM email_log WHERE prospect_id=? ORDER BY created_at DESC", (pid,)).fetchall())
    pays = rows_to_list(conn.execute("SELECT * FROM payments WHERE prospect_id=? ORDER BY created_at DESC", (pid,)).fetchall())
    conn.close()
    return {"prospect": pr, "calls": calls, "emails": emails, "payments": pays}

@app.post("/prospects")
async def create_prospect(data: ProspectCreate):
    pid = str(uuid.uuid4())
    pp = INDUSTRY_PAIN_POINTS.get(data.industry, INDUSTRY_PAIN_POINTS["general"])
    conn = get_db()
    conn.execute("INSERT INTO prospects (id,business_name,contact_name,phone,email,industry,location,website,notes,source,pain_points) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                 (pid, data.business_name, data.contact_name, clean_phone(data.phone) if data.phone else "",
                  data.email, data.industry, data.location, data.website, data.notes, data.source, json.dumps(pp)))
    conn.commit(); conn.close()
    log_activity("prospect_added", f"{data.business_name} ({data.industry})", pid)
    return {"id": pid, "business_name": data.business_name, "status": "created"}

@app.put("/prospects/{pid}")
async def update_prospect(pid: str, data: ProspectUpdate):
    conn = get_db()
    pr = row_to_dict(conn.execute("SELECT * FROM prospects WHERE id=?", (pid,)).fetchone())
    if not pr: conn.close(); raise HTTPException(404, "Not found")
    ups, ps = [], []
    for f, v in data.dict(exclude_none=True).items():
        ups.append(f"{f}=?"); ps.append(v)
    if ups:
        ups.append("updated_at=datetime('now')"); ps.append(pid)
        conn.execute(f"UPDATE prospects SET {', '.join(ups)} WHERE id=?", ps)
        conn.commit()
    conn.close()
    return {"status": "updated", "id": pid}

@app.post("/prospects/import")
async def import_prospects(file: UploadFile = File(...)):
    content = await file.read()
    reader = csv.DictReader(io.StringIO(content.decode("utf-8")))
    imported, errors = 0, []
    conn = get_db()
    for i, row in enumerate(reader):
        try:
            bn = row.get("business_name","").strip()
            if not bn: errors.append(f"Row {i+1}: no business_name"); continue
            ind = row.get("industry","general").strip().lower()
            pid = str(uuid.uuid4())
            conn.execute("INSERT INTO prospects (id,business_name,contact_name,phone,email,industry,location,website,source,pain_points) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (pid, bn, row.get("contact_name","").strip(), clean_phone(row.get("phone","").strip()) if row.get("phone","").strip() else "",
                 row.get("email","").strip(), ind, row.get("location","").strip(), row.get("website","").strip(), "csv_import",
                 json.dumps(INDUSTRY_PAIN_POINTS.get(ind, INDUSTRY_PAIN_POINTS["general"]))))
            imported += 1
        except Exception as e:
            errors.append(f"Row {i+1}: {e}")
    conn.commit(); conn.close()
    log_activity("csv_import", f"Imported {imported} from {file.filename}")
    return {"imported": imported, "errors": errors}

@app.post("/prospects/scrape")
async def scrape_prospects(data: ScrapeRequest):
    results = await scrape_google_maps(data.query, data.location, data.limit)
    added = 0
    if data.auto_add:
        conn = get_db()
        ql = data.query.lower()
        ind = "general"
        for k in ["restaurant","dental","automotive","contractor"]:
            if k in ql: ind = k; break
        if "lawyer" in ql or "attorney" in ql or "law" in ql: ind = "legal"
        elif "doctor" in ql or "clinic" in ql or "medical" in ql: ind = "medical"
        elif "realtor" in ql or "real estate" in ql: ind = "real_estate"
        elif "plumb" in ql or "hvac" in ql or "electric" in ql: ind = "contractor"
        pp = INDUSTRY_PAIN_POINTS.get(ind, INDUSTRY_PAIN_POINTS["general"])
        for r in results:
            try:
                conn.execute("INSERT INTO prospects (id,business_name,phone,industry,location,website,source,review_score,review_count,pain_points) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (str(uuid.uuid4()), r.get("business_name",""), r.get("phone",""), ind, r.get("location",data.location),
                     r.get("website",""), "google_maps", r.get("rating",0), r.get("review_count",0), json.dumps(pp)))
                added += 1
            except Exception: pass
        conn.commit(); conn.close()
    log_activity("scrape", f"'{data.query}' in {data.location}: {len(results)} found, {added} added")
    return {"scraped": len(results), "added": added, "results": results}

# ==========================================================================
# CALLING ENDPOINTS
# ==========================================================================
@app.post("/call/{pid}")
async def initiate_call(pid: str, data: CallRequest = CallRequest()):
    conn = get_db()
    pr = row_to_dict(conn.execute("SELECT * FROM prospects WHERE id=?", (pid,)).fetchone())
    conn.close()
    if not pr: raise HTTPException(404, "Not found")
    if not pr.get("phone"): raise HTTPException(400, "No phone number")
    if not is_business_hours():
        return {"status": "scheduled", "message": "Outside business hours (9-5 CT). Queued for next business day."}

    ind = pr.get("industry","general")
    if data.script_override:
        script_text, sk = data.script_override, "custom"
    else:
        sk = f"cold_call_{ind}" if f"cold_call_{ind}" in DEFAULT_SCRIPTS else "cold_call_opener"
        script_text = personalize_script(DEFAULT_SCRIPTS[sk]["script"], pr)

    result = await make_outbound_call(pr, script_text)
    cid = str(uuid.uuid4())
    conn = get_db()
    conn.execute("INSERT INTO call_log (id,prospect_id,direction,phone,status,script_used,twilio_sid,notes) VALUES (?,?,?,?,?,?,?,?)",
                 (cid, pid, "outbound", pr.get("phone",""), result.get("status","failed"), sk, result.get("twilio_sid",""), script_text[:500]))
    conn.execute("UPDATE prospects SET last_contacted=datetime('now'), updated_at=datetime('now') WHERE id=?", (pid,))
    if pr.get("stage") == "prospect":
        conn.execute("UPDATE prospects SET stage='contacted' WHERE id=?", (pid,))
    conn.commit(); conn.close()
    log_activity("campaign_call", f"Called {pr.get('business_name','')} ({result.get('status','')})", pid)
    if result.get("status") in ("initiated","demo_mode"):
        await notify_telegram(f"<b>Outbound Call</b>\n{pr.get('business_name','')}\n{pr.get('phone','')}\n{result.get('status','')}")
    return {"call_id": cid, "prospect": pr.get("business_name"), "phone": pr.get("phone"), "script_used": sk, "result": result}

@app.get("/call-log")
async def get_call_log(prospect_id: Optional[str]=None, status: Optional[str]=None, limit: int=50, offset: int=0):
    conn = get_db()
    q = "SELECT cl.*, p.business_name, p.industry FROM call_log cl LEFT JOIN prospects p ON cl.prospect_id=p.id WHERE 1=1"
    p = []
    if prospect_id: q += " AND cl.prospect_id=?"; p.append(prospect_id)
    if status: q += " AND cl.status=?"; p.append(status)
    q += " ORDER BY cl.created_at DESC LIMIT ? OFFSET ?"; p += [limit, offset]
    calls = rows_to_list(conn.execute(q, p).fetchall())
    total = dict(conn.execute("SELECT COUNT(*) as c FROM call_log").fetchone())["c"]
    conn.close()
    return {"calls": calls, "total": total}

# ==========================================================================
# TWILIO VOICE WEBHOOK
# ==========================================================================
@app.api_route("/voice", methods=["GET","POST"])
async def twilio_voice(request: Request):
    form = await request.form() if request.method == "POST" else {}
    pid = request.query_params.get("prospect_id","")
    speech = form.get("SpeechResult","")
    answered_by = form.get("AnsweredBy","human")

    prospect = {}
    if pid:
        conn = get_db()
        row = conn.execute("SELECT * FROM prospects WHERE id=?", (pid,)).fetchone()
        if row: prospect = dict(row)
        conn.close()
    ind = prospect.get("industry","general")

    # Voicemail
    if answered_by in ("machine_start","machine_end_beep","machine_end_silence","machine_end_other"):
        vm = personalize_script(DEFAULT_SCRIPTS["voicemail"]["script"], prospect)
        conn = get_db()
        conn.execute("UPDATE call_log SET outcome='voicemail', status='completed' WHERE prospect_id=? AND id=(SELECT id FROM call_log WHERE prospect_id=? ORDER BY created_at DESC LIMIT 1)", (pid,pid))
        conn.commit(); conn.close()
        return Response(content=f'<?xml version="1.0"?><Response><Pause length="1"/><Say voice="Polly.Matthew">{vm}</Say><Hangup/></Response>', media_type="application/xml")

    # First contact
    if not speech:
        sk = f"cold_call_{ind}" if f"cold_call_{ind}" in DEFAULT_SCRIPTS else "cold_call_opener"
        opener = personalize_script(DEFAULT_SCRIPTS[sk]["script"], prospect)
        return Response(content=f'<?xml version="1.0"?><Response><Say voice="Polly.Matthew">{opener}</Say><Gather input="speech" timeout="5" speechTimeout="2" action="/voice?prospect_id={pid}" method="POST"><Say voice="Polly.Matthew">I would love to hear your thoughts.</Say></Gather><Say voice="Polly.Matthew">No worries. I will follow up by email. Have a great day!</Say><Hangup/></Response>', media_type="application/xml")

    sl = speech.lower()
    interested = ["interested","tell me more","how much","sign me up","sounds good","demo","pricing","let's do it","try it"]
    objections = ["not interested","no thanks","don't need","too expensive","already have","busy","stop calling"]

    if any(s in sl for s in interested):
        close = personalize_script(DEFAULT_SCRIPTS["close_interested"]["script"], prospect, "ai_phone_agent")
        conn = get_db()
        conn.execute("UPDATE prospects SET stage='interested', updated_at=datetime('now') WHERE id=?", (pid,))
        conn.execute("UPDATE call_log SET outcome='interested', status='completed' WHERE prospect_id=? AND id=(SELECT id FROM call_log WHERE prospect_id=? ORDER BY created_at DESC LIMIT 1)", (pid,pid))
        conn.commit(); conn.close()
        await notify_telegram(f"<b>HOT LEAD!</b>\n{prospect.get('business_name','')}\nSaid: \"{speech}\"")
        return Response(content=f'<?xml version="1.0"?><Response><Say voice="Polly.Matthew">{close}</Say><Gather input="speech" timeout="5" speechTimeout="2" action="/voice?prospect_id={pid}" method="POST"><Say voice="Polly.Matthew">Should I send you the link right now?</Say></Gather><Say voice="Polly.Matthew">I will send you an email with details. Thanks!</Say><Hangup/></Response>', media_type="application/xml")

    elif any(s in sl for s in objections):
        if "expensive" in sl or "price" in sl or "cost" in sl:
            obj = personalize_script(DEFAULT_SCRIPTS["objection_price"]["script"], prospect)
        elif "already" in sl:
            obj = personalize_script(DEFAULT_SCRIPTS["objection_have_service"]["script"], prospect)
        else:
            obj = personalize_script(DEFAULT_SCRIPTS["objection_not_interested"]["script"], prospect)
        conn = get_db()
        conn.execute("UPDATE call_log SET outcome='objection', notes=? WHERE prospect_id=? AND id=(SELECT id FROM call_log WHERE prospect_id=? ORDER BY created_at DESC LIMIT 1)", (speech,pid,pid))
        conn.commit(); conn.close()
        return Response(content=f'<?xml version="1.0"?><Response><Say voice="Polly.Matthew">{obj}</Say><Gather input="speech" timeout="5" speechTimeout="2" action="/voice?prospect_id={pid}" method="POST"></Gather><Say voice="Polly.Matthew">No worries. I will send a case study by email. Have a great day!</Say><Hangup/></Response>', media_type="application/xml")

    else:
        # AI response via Ollama
        sys_p = (f"You are Alex from Hive Dynamics on a sales call with {prospect.get('contact_name','a business owner')} "
                 f"at {prospect.get('business_name','')} ({ind}). Sell AI phone answering. "
                 f"Packages: AI Phone Agent ($499 setup + $99/mo), Content Factory ($999 setup + $149/mo), "
                 f"Full Hive ($1,999 setup + $299/mo), Enterprise ($4,999 setup + $499/mo). "
                 f"Setup fees are a one-time investment to build their custom AI system. "
                 f"Be concise (2-3 sentences). Not pushy.")
        ai = await query_ollama(f'Prospect said: "{speech}"\nRespond as Alex:', system=sys_p)
        if not ai:
            ai = f"That is a great question. Our AI phone agent is built for {ind.replace('_',' ')} businesses. Would you like a quick demo?"
        ai = ai.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;").replace('"',"&quot;")
        conn = get_db()
        conn.execute("UPDATE call_log SET transcript=COALESCE(transcript,'')||? WHERE prospect_id=? AND id=(SELECT id FROM call_log WHERE prospect_id=? ORDER BY created_at DESC LIMIT 1)",
                     (f"\nProspect: {speech}\nAlex: {ai}", pid, pid))
        conn.commit(); conn.close()
        return Response(content=f'<?xml version="1.0"?><Response><Say voice="Polly.Matthew">{ai}</Say><Gather input="speech" timeout="6" speechTimeout="2" action="/voice?prospect_id={pid}" method="POST"></Gather><Say voice="Polly.Matthew">Thanks for your time. I will follow up by email. Have a great day!</Say><Hangup/></Response>', media_type="application/xml")

@app.post("/status")
async def twilio_status(request: Request):
    form = await request.form()
    sid = form.get("CallSid",""); st = form.get("CallStatus",""); dur = form.get("CallDuration","0")
    if sid:
        conn = get_db()
        conn.execute("UPDATE call_log SET status=?, duration_seconds=? WHERE twilio_sid=?", (st, int(dur), sid))
        conn.commit(); conn.close()
    return {"status": "ok"}

# ==========================================================================
# EMAIL ENDPOINTS
# ==========================================================================
@app.post("/email/{pid}")
async def send_prospect_email(pid: str, data: EmailRequest = EmailRequest()):
    conn = get_db()
    pr = row_to_dict(conn.execute("SELECT * FROM prospects WHERE id=?", (pid,)).fetchone())
    conn.close()
    if not pr: raise HTTPException(404, "Not found")
    if not pr.get("email"): raise HTTPException(400, "No email")
    result = await send_email(pr, data.template or "email_intro", custom_subject=data.custom_subject or "", custom_body=data.custom_body or "")
    conn = get_db()
    conn.execute("UPDATE prospects SET last_contacted=datetime('now'), updated_at=datetime('now') WHERE id=?", (pid,))
    if pr.get("stage") == "prospect":
        conn.execute("UPDATE prospects SET stage='contacted' WHERE id=?", (pid,))
    conn.commit(); conn.close()
    return result

@app.get("/track/open/{tid}")
async def track_open(tid: str):
    conn = get_db()
    conn.execute("UPDATE email_log SET opened=1, opened_at=datetime('now') WHERE tracking_id=?", (tid,))
    conn.commit(); conn.close()
    return Response(content=TRACKING_PIXEL, media_type="image/gif")

@app.get("/track/click/{tid}")
async def track_click(tid: str, url: str = ""):
    conn = get_db()
    conn.execute("UPDATE email_log SET clicked=1, clicked_at=datetime('now') WHERE tracking_id=?", (tid,))
    conn.commit(); conn.close()
    return RedirectResponse(url=url or "https://hivedynamics.ai")

# ==========================================================================
# CAMPAIGN ENDPOINTS
# ==========================================================================
@app.post("/campaign/start")
async def start_campaign(config: CampaignConfig = CampaignConfig()):
    global CAMPAIGN_RUNNING, CAMPAIGN_THREAD
    if CAMPAIGN_RUNNING:
        return {"status": "already_running", "message": "Stop current campaign first."}
    cid = str(uuid.uuid4())
    conn = get_db()
    conn.execute("INSERT INTO campaign_runs (id,name,status,config) VALUES (?,?,?,?)",
                 (cid, config.name, "running", json.dumps(config.dict())))
    conn.commit(); conn.close()
    CAMPAIGN_RUNNING = True
    CAMPAIGN_THREAD = threading.Thread(target=campaign_loop, args=(cid, config.dict()), daemon=True)
    CAMPAIGN_THREAD.start()
    log_activity("campaign_started", f"'{config.name}' started")
    await notify_telegram(f"<b>Campaign Started</b>\n{config.name}\nCalls: {'ON' if config.enable_calls else 'OFF'} | Emails: {'ON' if config.enable_emails else 'OFF'}\nBatch: {config.batch_size} | Cycle: {config.cycle_interval_minutes}min")
    return {"status": "started", "campaign_id": cid, "name": config.name, "config": config.dict()}

@app.post("/campaign/stop")
async def stop_campaign():
    global CAMPAIGN_RUNNING
    if not CAMPAIGN_RUNNING: return {"status": "not_running"}
    CAMPAIGN_RUNNING = False
    log_activity("campaign_stopped", "Stopped by user")
    await notify_telegram("<b>Campaign Stopped</b>")
    return {"status": "stopped", "message": "Stopping. May take up to a minute."}

@app.get("/campaign/history")
async def campaign_history(limit: int = 20):
    conn = get_db()
    runs = rows_to_list(conn.execute("SELECT * FROM campaign_runs ORDER BY started_at DESC LIMIT ?", (limit,)).fetchall())
    conn.close()
    return {"campaigns": runs}

# ==========================================================================
# PIPELINE & STATS
# ==========================================================================
@app.get("/pipeline")
async def get_pipeline():
    conn = get_db()
    stages = {}
    for st in ["prospect","contacted","interested","demo","proposal","closed_won","closed_lost"]:
        cnt = dict(conn.execute("SELECT COUNT(*) as c FROM prospects WHERE stage=?", (st,)).fetchone())["c"]
        prs = rows_to_list(conn.execute("SELECT id,business_name,contact_name,industry,last_contacted,package FROM prospects WHERE stage=? ORDER BY updated_at DESC LIMIT 10", (st,)).fetchall())
        stages[st] = {"count": cnt, "prospects": prs}
    total = sum(s["count"] for s in stages.values())
    contacted = total - stages["prospect"]["count"]
    interested = stages["interested"]["count"] + stages["demo"]["count"] + stages["proposal"]["count"] + stages["closed_won"]["count"]
    won = stages["closed_won"]["count"]
    conn.close()
    return {"stages": stages, "total_prospects": total,
            "conversion_rates": {"contact_rate": round(contacted/max(total,1)*100,1),
                                 "interest_rate": round(interested/max(contacted,1)*100,1),
                                 "close_rate": round(won/max(interested,1)*100,1)}}

@app.get("/stats")
async def get_stats():
    conn = get_db()
    ct = dict(conn.execute("SELECT COUNT(*) as c FROM call_log").fetchone())["c"]
    ctd = dict(conn.execute("SELECT COUNT(*) as c FROM call_log WHERE date(created_at)=date('now')").fetchone())["c"]
    ca = dict(conn.execute("SELECT COUNT(*) as c FROM call_log WHERE outcome IN ('interested','callback','answered','sold')").fetchone())["c"]
    et = dict(conn.execute("SELECT COUNT(*) as c FROM email_log").fetchone())["c"]
    eo = dict(conn.execute("SELECT COUNT(*) as c FROM email_log WHERE opened=1").fetchone())["c"]
    ec = dict(conn.execute("SELECT COUNT(*) as c FROM email_log WHERE clicked=1").fetchone())["c"]
    db_ = dict(conn.execute("SELECT COUNT(*) as c FROM prospects WHERE stage='demo'").fetchone())["c"]
    dw = dict(conn.execute("SELECT COUNT(*) as c FROM prospects WHERE stage='closed_won'").fetchone())["c"]
    dl = dict(conn.execute("SELECT COUNT(*) as c FROM prospects WHERE stage='closed_lost'").fetchone())["c"]
    mrr_r = conn.execute("SELECT COALESCE(SUM(p.amount_cents),0) as t FROM payments p JOIN prospects pr ON p.prospect_id=pr.id WHERE pr.stage='closed_won' AND p.status='paid'").fetchone()
    mrr = dict(mrr_r)["t"]
    setup_rev_r = conn.execute("SELECT COALESCE(SUM(p.setup_fee_cents),0) as t FROM payments p JOIN prospects pr ON p.prospect_id=pr.id WHERE pr.stage='closed_won' AND p.status='paid'").fetchone()
    setup_rev = dict(setup_rev_r)["t"]
    pv_r = conn.execute("SELECT COALESCE(SUM(amount_cents + setup_fee_cents),0) as t FROM payments WHERE status='pending'").fetchone()
    pv = dict(pv_r)["t"]
    industries = rows_to_list(conn.execute("SELECT industry, COUNT(*) as count FROM prospects GROUP BY industry ORDER BY count DESC").fetchall())
    sources = rows_to_list(conn.execute("SELECT source, COUNT(*) as count FROM prospects GROUP BY source ORDER BY count DESC").fetchall())
    conn.close()
    return {
        "calls": {"total": ct, "today": ctd, "answered": ca, "answer_rate": round(ca/max(ct,1)*100,1)},
        "emails": {"total": et, "opened": eo, "clicked": ec, "open_rate": round(eo/max(et,1)*100,1), "click_rate": round(ec/max(et,1)*100,1)},
        "pipeline": {"demos_booked": db_, "deals_won": dw, "deals_lost": dl, "win_rate": round(dw/max(dw+dl,1)*100,1)},
        "revenue": {"mrr": mrr/100.0, "mrr_display": f"${mrr/100.0:,.2f}",
                     "setup_fee_revenue": setup_rev/100.0, "setup_fee_display": f"${setup_rev/100.0:,.2f}",
                     "total_revenue": (mrr + setup_rev)/100.0, "total_display": f"${(mrr + setup_rev)/100.0:,.2f}",
                     "pipeline_value": pv/100.0, "pipeline_display": f"${pv/100.0:,.2f}"},
        "breakdown": {"by_industry": industries, "by_source": sources}
    }

# ==========================================================================
# PAYMENT LINK ENDPOINTS
# ==========================================================================
@app.post("/payment-link/{pid}")
async def create_prospect_payment(pid: str, data: PaymentRequest = PaymentRequest()):
    conn = get_db()
    pr = row_to_dict(conn.execute("SELECT * FROM prospects WHERE id=?", (pid,)).fetchone())
    conn.close()
    if not pr: raise HTTPException(404, "Not found")
    result = await create_payment_link(pr, data.package)
    if "error" in result: raise HTTPException(400, result["error"])
    # Email the link
    if pr.get("email") and result.get("link"):
        pkg = PACKAGES[data.package]
        setup_items = "\n".join(f"  - {s}" for s in pkg.get("setup_includes", []))
        body = (f"Hi {pr.get('contact_name','there')},\n\nThanks for your interest in {pkg['name']}!\n\n"
                f"Here's your secure payment link:\n{result['link']}\n\n"
                f"Package: {pkg['name']}\n"
                f"One-Time Setup Fee: {pkg.get('setup_fee_display','$0')}\n"
                f"Monthly Subscription: {pkg['price_display']}\n\n"
                f"What's included in your setup (one-time investment to get your custom AI built):\n{setup_items}\n\n"
                f"Features (included monthly):\n" + "\n".join(f"  - {f}" for f in pkg["features"]) +
                f"\n\nOnce we receive payment, our team builds out your custom AI system within 48 hours. "
                f"After that, your monthly subscription keeps everything running 24/7.\n\n"
                f"Best,\nAlex\nHive Dynamics")
        await send_email(pr, "payment_link", custom_subject=f"Your {pkg['name']} - Get Started", custom_body=body)
    conn = get_db()
    conn.execute("UPDATE prospects SET stage='proposal', package=?, updated_at=datetime('now') WHERE id=?", (data.package, pid))
    conn.commit(); conn.close()
    pkg_info = PACKAGES[data.package]
    await notify_telegram(f"<b>Payment Link Sent</b>\n{pr.get('business_name','')}\n{pkg_info['name']}\nSetup: {pkg_info.get('setup_fee_display','$0')} + {pkg_info['price_display']}\n{result.get('link','')}")
    return result

@app.get("/packages")
async def list_packages():
    return {"packages": PACKAGES}

# ==========================================================================
# SCRIPTS ENDPOINTS
# ==========================================================================
@app.get("/scripts")
async def list_scripts(category: Optional[str]=None, industry: Optional[str]=None):
    conn = get_db()
    q, p = "SELECT * FROM scripts WHERE 1=1", []
    if category: q += " AND category=?"; p.append(category)
    if industry: q += " AND industry=?"; p.append(industry)
    q += " ORDER BY category, industry, name"
    scripts = rows_to_list(conn.execute(q, p).fetchall())
    conn.close()
    return {"scripts": scripts}

@app.post("/scripts")
async def create_script(data: ScriptCreate):
    sid = re.sub(r"[^a-z0-9_]", "_", data.name.lower().strip())
    conn = get_db()
    exists = conn.execute("SELECT id FROM scripts WHERE id=?", (sid,)).fetchone()
    if exists:
        conn.execute("UPDATE scripts SET name=?,category=?,industry=?,script=?,updated_at=datetime('now') WHERE id=?",
                     (data.name, data.category, data.industry, data.script, sid))
    else:
        conn.execute("INSERT INTO scripts (id,name,category,industry,script) VALUES (?,?,?,?,?)",
                     (sid, data.name, data.category, data.industry, data.script))
    conn.commit(); conn.close()
    return {"id": sid, "status": "updated" if exists else "created"}

# ==========================================================================
# AI CONVERSATION HELPER
# ==========================================================================
@app.post("/ai/respond")
async def ai_respond(request: Request):
    body = await request.json()
    pid = body.get("prospect_id",""); msg = body.get("message",""); ctx = body.get("context","")
    prospect = {}
    if pid:
        conn = get_db()
        row = conn.execute("SELECT * FROM prospects WHERE id=?", (pid,)).fetchone()
        if row: prospect = dict(row)
        conn.close()
    sys_p = (f"You are Alex from Hive Dynamics selling AI phone answering to "
             f"{prospect.get('contact_name','')} at {prospect.get('business_name','')}. "
             f"Industry: {prospect.get('industry','general')}. "
             f"Packages: AI Phone Agent ($499 setup + $99/mo), Content Factory ($999 setup + $149/mo), "
             f"Full Hive ($1,999 setup + $299/mo), Enterprise ($4,999 setup + $499/mo). "
             f"Setup fees are a one-time investment to build their custom AI. Be concise. Not pushy.")
    if ctx: sys_p += f"\nContext: {ctx}"
    resp = await query_ollama(f'Prospect: "{msg}"\nRespond as Alex:', system=sys_p)
    if not resp:
        resp = "That is a great question. Would you like me to set up a quick demo so you can hear our AI in action?"
    return {"response": resp, "prospect_id": pid}

# ==========================================================================
# STRIPE WEBHOOK
# ==========================================================================
@app.post("/stripe/webhook")
async def stripe_webhook(request: Request):
    body = await request.body()
    try: event = json.loads(body)
    except Exception: raise HTTPException(400, "Invalid JSON")
    if event.get("type") == "checkout.session.completed":
        session = event.get("data",{}).get("object",{})
        meta = session.get("metadata",{})
        pid = meta.get("prospect_id",""); pkg = meta.get("package","")
        if pid:
            conn = get_db()
            conn.execute("UPDATE prospects SET stage='closed_won', updated_at=datetime('now') WHERE id=?", (pid,))
            conn.execute("UPDATE payments SET status='paid', paid_at=datetime('now') WHERE prospect_id=? AND status='pending'", (pid,))
            conn.commit()
            pr = row_to_dict(conn.execute("SELECT * FROM prospects WHERE id=?", (pid,)).fetchone())
            conn.close()
            log_activity("sale_closed", f"DEAL WON: {pr.get('business_name','')} - {pkg}", pid)
            pkg_data = PACKAGES.get(pkg, {})
            await notify_telegram(f"<b>SALE CLOSED!</b>\n{pr.get('business_name','')}\n{pkg_data.get('name',pkg)}\nSetup: {pkg_data.get('setup_fee_display','$0')} + {pkg_data.get('price_display','')}/mo\nPAYMENT CONFIRMED")
            await report_to_nerve("sales", f"Closed: {pr.get('business_name','')} - {pkg} ({pkg_data.get('setup_fee_display','$0')} setup + {pkg_data.get('price_display','')})")
    return {"status": "ok"}

# ==========================================================================
# HEALTH
# ==========================================================================
@app.get("/")
async def root():
    return {"service": "Hive Cold Caller & Sales Agent", "version": "1.0.0", "port": PORT,
            "campaign_active": CAMPAIGN_RUNNING, "business_hours": is_business_hours(),
            "endpoints": ["/dashboard","/prospects","/pipeline","/stats","/scripts","/call-log","/packages"],
            "integrations": {"twilio": "OK" if TWILIO_AUTH_TOKEN else "DEMO",
                             "stripe": "LIVE" if STRIPE_API_KEY != "sk_test_demo" else "DEMO",
                             "ollama": f"{OLLAMA_URL} ({OLLAMA_MODEL})", "nerve": NERVE_URL}}

@app.get("/health")
async def health():
    return {"status": "healthy", "timestamp": datetime.now().isoformat(), "port": PORT}

# ==========================================================================
if __name__ == "__main__":
    print(f"\n  HIVE COLD CALLER & SALES AGENT\n  Port: {PORT} | DB: {DB_PATH}\n  Ollama: {OLLAMA_URL} ({OLLAMA_MODEL})\n  Twilio: {'OK' if TWILIO_AUTH_TOKEN else 'DEMO'} | Stripe: {'LIVE' if STRIPE_API_KEY != 'sk_test_demo' else 'DEMO'}\n")
    uvicorn.run(app, host="0.0.0.0", port=PORT)
