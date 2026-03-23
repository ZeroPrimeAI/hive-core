#!/usr/bin/env python3
"""
AI Phone Answering — Demo & Lead Capture System
================================================
Professional demo page for potential clients.
Port: 8907 | DB: /home/zero/hivecode_sandbox/demos.db

Endpoints:
  GET  /             — Landing page (SaaS product page)
  GET  /health       — Health check
  POST /api/lead     — Capture a lead
  GET  /api/leads    — List captured leads
  GET  /api/demo-scripts — Sample phone scripts by industry
"""

import json
import sqlite3
import os
import time
from datetime import datetime, timezone
from contextlib import contextmanager
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PORT = 8907
DB_PATH = "/home/zero/hivecode_sandbox/demos.db"
DEMO_PHONE = "+1 (850) 801-6662"
DEMO_PHONE_RAW = "+18508016662"

app = FastAPI(title="AI Phone Answering Demo", version="1.0.0")

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS leads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                business TEXT,
                phone TEXT,
                email TEXT,
                industry TEXT,
                message TEXT,
                source TEXT DEFAULT 'demo_page',
                created_at TEXT DEFAULT (datetime('now')),
                status TEXT DEFAULT 'new'
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS demo_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id INTEGER,
                requested_at TEXT DEFAULT (datetime('now')),
                ip_address TEXT,
                user_agent TEXT
            )
        """)
        conn.commit()

@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

init_db()

# ---------------------------------------------------------------------------
# Demo Scripts
# ---------------------------------------------------------------------------
DEMO_SCRIPTS = {
    "locksmith": {
        "industry": "Locksmith",
        "icon": "&#128272;",
        "tagline": "24/7 Emergency Lockout Response",
        "scenarios": [
            {
                "title": "Emergency Lockout — 2:30 AM",
                "conversation": [
                    {"role": "caller", "text": "Hi, I'm locked out of my house. It's the middle of the night and I have my kids in the car."},
                    {"role": "ai", "text": "I'm so sorry about that — I know how stressful that is, especially with little ones. Let me get a technician headed your way right now. Can I get your address?"},
                    {"role": "caller", "text": "I'm at 4521 Oak Street, Fort Walton Beach."},
                    {"role": "ai", "text": "Got it — 4521 Oak Street, Fort Walton Beach. I'm dispatching our nearest available technician now. Estimated arrival is about 20 minutes. They'll call you when they're 5 minutes out. Is this the best number to reach you?"},
                    {"role": "caller", "text": "Yes, this number is fine. How much will it cost?"},
                    {"role": "ai", "text": "For a standard residential lockout, the service call starts at $65. The technician will give you an exact quote before starting any work — no surprises. Is there anything else I can help with while you wait?"},
                ],
            },
            {
                "title": "Commercial Rekey Appointment",
                "conversation": [
                    {"role": "caller", "text": "I just fired an employee and I need all the locks changed at my business. Can someone come today?"},
                    {"role": "ai", "text": "Absolutely, I understand the urgency. We can typically get someone out same-day for commercial rekeys. How many locks are we looking at, and what's the business address?"},
                    {"role": "caller", "text": "About 6 doors. We're at 200 Main Street, Destin."},
                    {"role": "ai", "text": "Perfect — 6 doors at 200 Main Street, Destin. I have a technician available this afternoon between 2 and 4 PM. He'll bring fresh cores for all six. Would that window work for you?"},
                ],
            },
        ],
    },
    "plumber": {
        "industry": "Plumber",
        "icon": "&#128688;",
        "tagline": "Emergency Plumbing & Scheduled Service",
        "scenarios": [
            {
                "title": "Burst Pipe Emergency",
                "conversation": [
                    {"role": "caller", "text": "Water is spraying everywhere in my kitchen! I need someone NOW!"},
                    {"role": "ai", "text": "I hear you — that's an emergency and we'll get someone out fast. First, can you locate the main water shutoff valve? It's usually near your water meter or where the line enters your house. Turn it clockwise to shut off the water."},
                    {"role": "caller", "text": "Okay, I think I found it. The water stopped."},
                    {"role": "ai", "text": "Great job — that will prevent further damage. Now let me dispatch an emergency plumber to you. What's your address? We have a tech who can be there within 30 minutes."},
                    {"role": "caller", "text": "1200 Coral Drive, Niceville."},
                    {"role": "ai", "text": "1200 Coral Drive, Niceville — our plumber Mike is on his way. He'll be there in about 25 minutes. In the meantime, if you can lay some towels around the area, that'll help. He'll call you when he's close."},
                ],
            },
            {
                "title": "Water Heater Replacement Quote",
                "conversation": [
                    {"role": "caller", "text": "My water heater is 15 years old and making weird noises. I think it's time to replace it."},
                    {"role": "ai", "text": "Fifteen years is a great run — you're right that it's probably time. We'd love to come take a look and give you options. Is it a gas or electric unit, and do you know the tank size?"},
                    {"role": "caller", "text": "Electric, 50 gallon I think."},
                    {"role": "ai", "text": "Perfect. For a 50-gallon electric replacement, we can schedule a free assessment where our tech evaluates the installation and gives you an exact quote — including options for a standard tank or a tankless upgrade. What day works best for you this week?"},
                ],
            },
        ],
    },
    "hvac": {
        "industry": "HVAC",
        "icon": "&#10052;&#65039;",
        "tagline": "Heating, Cooling & Maintenance Plans",
        "scenarios": [
            {
                "title": "AC Down in Summer",
                "conversation": [
                    {"role": "caller", "text": "Our AC just stopped blowing cold air. It's 95 degrees out and I have elderly parents at home."},
                    {"role": "ai", "text": "I understand the urgency — that's a health concern with elderly family members in this heat. Let me get a priority service call set up for you right away. Can I get your address?"},
                    {"role": "caller", "text": "3800 Pine Avenue, Crestview."},
                    {"role": "ai", "text": "3800 Pine Avenue, Crestview. I'm marking this as a priority call. Our technician can be there within 2 hours. In the meantime, I'd recommend closing blinds, using fans, and staying hydrated. The tech will diagnose the issue and provide repair options on-site before starting any work."},
                ],
            },
            {
                "title": "Seasonal Maintenance Plan",
                "conversation": [
                    {"role": "caller", "text": "Do you guys offer any kind of maintenance plan? I want to make sure my system doesn't break down."},
                    {"role": "ai", "text": "Absolutely — our Comfort Club plan is our most popular option. It includes two full tune-ups per year — one before summer and one before winter — plus priority scheduling and 15% off all repairs. It's $189 per year for a single system. Would you like me to sign you up?"},
                    {"role": "caller", "text": "That sounds reasonable. What does the tune-up include?"},
                    {"role": "ai", "text": "Each tune-up is a comprehensive 21-point inspection: refrigerant levels, electrical connections, coil cleaning, drain line flush, thermostat calibration, filter replacement, and more. It keeps your system running at peak efficiency and catches small problems before they become expensive repairs. Want me to schedule your first tune-up?"},
                ],
            },
        ],
    },
    "medical": {
        "industry": "Medical Office",
        "icon": "&#9877;&#65039;",
        "tagline": "Appointment Scheduling & Patient Support",
        "scenarios": [
            {
                "title": "New Patient Appointment",
                "conversation": [
                    {"role": "caller", "text": "Hi, I'd like to make an appointment. I'm a new patient."},
                    {"role": "ai", "text": "Welcome! We'd be happy to get you scheduled. Are you looking for a general check-up, or is there a specific concern you'd like to address?"},
                    {"role": "caller", "text": "I need a general check-up. It's been a while since I've seen a doctor."},
                    {"role": "ai", "text": "No problem at all. For a new patient comprehensive exam, we have availability this Thursday at 10 AM or next Monday at 2 PM. Which works better for you?"},
                    {"role": "caller", "text": "Thursday at 10 works. Do you take Blue Cross?"},
                    {"role": "ai", "text": "Yes, we're in-network with Blue Cross Blue Shield. I'll get you booked for Thursday at 10 AM. Please arrive 15 minutes early to complete your new patient paperwork, and bring your insurance card and a photo ID. We'll also send you digital forms by email so you can fill them out ahead of time. Can I get your full name and date of birth?"},
                ],
            },
            {
                "title": "Prescription Refill Request",
                "conversation": [
                    {"role": "caller", "text": "I need to refill my blood pressure medication. Dr. Martinez prescribed it."},
                    {"role": "ai", "text": "Of course. Let me pull up your record. Can I get your name and date of birth for verification?"},
                    {"role": "caller", "text": "Sarah Johnson, March 15th, 1978."},
                    {"role": "ai", "text": "Thank you, Sarah. I'll send a refill request to Dr. Martinez for review. He typically processes these within 24 hours. Which pharmacy should we send it to?"},
                ],
            },
        ],
    },
    "restaurant": {
        "industry": "Restaurant",
        "icon": "&#127869;",
        "tagline": "Reservations, Hours & Menu Info",
        "scenarios": [
            {
                "title": "Dinner Reservation",
                "conversation": [
                    {"role": "caller", "text": "Hi, I'd like to make a reservation for Saturday night. Party of 6."},
                    {"role": "ai", "text": "Saturday night for 6 — great! We have availability at 6:30 PM or 8:45 PM. Do you have a preference?"},
                    {"role": "caller", "text": "6:30 works. Is there outdoor seating available?"},
                    {"role": "ai", "text": "Yes! We have a beautiful patio and I can reserve an outdoor table for your party. Saturday's forecast looks clear, so it should be perfect. Can I get a name for the reservation?"},
                    {"role": "caller", "text": "Thompson. Also, we have someone with a shellfish allergy."},
                    {"role": "ai", "text": "Thompson, party of 6, outdoor patio, Saturday at 6:30 PM — confirmed. I've noted the shellfish allergy and our chef will be prepared with safe options. We have several dishes that are naturally shellfish-free, and your server will walk through everything when you arrive. Is there anything else I can help with?"},
                ],
            },
            {
                "title": "Catering Inquiry",
                "conversation": [
                    {"role": "caller", "text": "Do you do catering? I'm planning a corporate event for about 40 people."},
                    {"role": "ai", "text": "We absolutely do catering, and corporate events are one of our specialties. For a group of 40, we offer buffet-style and plated options. When is the event, and do you have a venue or would you like to use our private dining space?"},
                    {"role": "caller", "text": "It's in three weeks at our office. Buffet would be easier."},
                    {"role": "ai", "text": "Perfect. I'll have our catering coordinator reach out to you within 24 hours with menu options and pricing for a buffet for 40. We typically recommend 3 entree options, 2 sides, salad, and dessert for a corporate setting. Can I get your name, company, and the best email to send the proposal?"},
                ],
            },
        ],
    },
}

# ---------------------------------------------------------------------------
# Landing Page HTML
# ---------------------------------------------------------------------------
def build_landing_page():
    # Build conversation HTML for each industry
    industry_cards = ""
    script_tabs = ""
    script_panels = ""

    for idx, (key, data) in enumerate(DEMO_SCRIPTS.items()):
        active_tab = "active" if idx == 0 else ""
        active_panel = "" if idx > 0 else "active"

        script_tabs += f'<button class="tab-btn {active_tab}" onclick="showTab(\'{key}\')" id="tab-{key}">{data["icon"]} {data["industry"]}</button>\n'

        scenarios_html = ""
        for scenario in data["scenarios"]:
            convo_html = ""
            for msg in scenario["conversation"]:
                if msg["role"] == "caller":
                    convo_html += f'<div class="msg msg-caller"><div class="msg-label">Caller</div><div class="msg-bubble caller-bubble">{msg["text"]}</div></div>\n'
                else:
                    convo_html += f'<div class="msg msg-ai"><div class="msg-label">AI Assistant</div><div class="msg-bubble ai-bubble">{msg["text"]}</div></div>\n'

            scenarios_html += f"""
            <div class="scenario">
                <h4 class="scenario-title">{scenario["title"]}</h4>
                <div class="conversation">{convo_html}</div>
            </div>
            """

        script_panels += f"""
        <div class="tab-panel {"active" if idx == 0 else ""}" id="panel-{key}">
            <div class="industry-header">
                <span class="industry-icon">{data["icon"]}</span>
                <div>
                    <h3>{data["industry"]}</h3>
                    <p class="industry-tagline">{data["tagline"]}</p>
                </div>
            </div>
            {scenarios_html}
        </div>
        """

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>AI Phone Answering — Never Miss a Call | Hive Dynamics</title>
<style>
*, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}

:root {{
    --bg-primary: #0a0e17;
    --bg-secondary: #111827;
    --bg-card: #1a2236;
    --bg-card-hover: #1f2a42;
    --accent: #3b82f6;
    --accent-glow: rgba(59, 130, 246, 0.3);
    --accent-bright: #60a5fa;
    --accent-green: #10b981;
    --accent-green-glow: rgba(16, 185, 129, 0.3);
    --accent-orange: #f59e0b;
    --accent-purple: #8b5cf6;
    --text-primary: #f1f5f9;
    --text-secondary: #94a3b8;
    --text-muted: #64748b;
    --border: #1e293b;
    --border-accent: #2d3a52;
    --radius: 12px;
    --radius-lg: 20px;
    --shadow: 0 4px 24px rgba(0, 0, 0, 0.3);
    --shadow-lg: 0 12px 48px rgba(0, 0, 0, 0.4);
}}

body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Inter", sans-serif;
    background: var(--bg-primary);
    color: var(--text-primary);
    line-height: 1.7;
    overflow-x: hidden;
}}

a {{ color: var(--accent-bright); text-decoration: none; }}
a:hover {{ text-decoration: underline; }}

.container {{
    max-width: 1200px;
    margin: 0 auto;
    padding: 0 24px;
}}

/* ---- TOP BAR ---- */
.topbar {{
    background: rgba(10, 14, 23, 0.95);
    backdrop-filter: blur(12px);
    border-bottom: 1px solid var(--border);
    padding: 14px 0;
    position: sticky;
    top: 0;
    z-index: 100;
}}
.topbar .container {{
    display: flex;
    justify-content: space-between;
    align-items: center;
    flex-wrap: wrap;
    gap: 12px;
}}
.topbar-brand {{
    font-size: 1.25rem;
    font-weight: 700;
    color: var(--text-primary);
    display: flex;
    align-items: center;
    gap: 10px;
}}
.topbar-brand .dot {{
    width: 10px; height: 10px;
    background: var(--accent-green);
    border-radius: 50%;
    display: inline-block;
    animation: pulse-dot 2s infinite;
}}
@keyframes pulse-dot {{
    0%, 100% {{ opacity: 1; }}
    50% {{ opacity: 0.4; }}
}}
.topbar-cta {{
    display: flex;
    align-items: center;
    gap: 16px;
    flex-wrap: wrap;
}}
.topbar-phone {{
    font-size: 1.05rem;
    font-weight: 600;
    color: var(--accent-bright);
    letter-spacing: 0.3px;
}}
.btn {{
    display: inline-flex;
    align-items: center;
    justify-content: center;
    gap: 8px;
    padding: 10px 24px;
    border-radius: 8px;
    font-size: 0.95rem;
    font-weight: 600;
    border: none;
    cursor: pointer;
    transition: all 0.2s;
    text-decoration: none;
}}
.btn-primary {{
    background: var(--accent);
    color: #fff;
    box-shadow: 0 2px 12px var(--accent-glow);
}}
.btn-primary:hover {{
    background: #2563eb;
    transform: translateY(-1px);
    box-shadow: 0 4px 20px var(--accent-glow);
    text-decoration: none;
    color: #fff;
}}
.btn-outline {{
    background: transparent;
    color: var(--accent-bright);
    border: 1.5px solid var(--accent);
}}
.btn-outline:hover {{
    background: rgba(59, 130, 246, 0.1);
    text-decoration: none;
}}
.btn-green {{
    background: var(--accent-green);
    color: #fff;
    box-shadow: 0 2px 12px var(--accent-green-glow);
}}
.btn-green:hover {{
    background: #059669;
    transform: translateY(-1px);
    text-decoration: none;
    color: #fff;
}}
.btn-lg {{
    padding: 14px 36px;
    font-size: 1.1rem;
    border-radius: 10px;
}}

/* ---- HERO ---- */
.hero {{
    padding: 100px 0 80px;
    text-align: center;
    position: relative;
    overflow: hidden;
}}
.hero::before {{
    content: "";
    position: absolute;
    top: -40%;
    left: 50%;
    transform: translateX(-50%);
    width: 800px;
    height: 800px;
    background: radial-gradient(circle, var(--accent-glow) 0%, transparent 70%);
    pointer-events: none;
    z-index: 0;
}}
.hero > * {{ position: relative; z-index: 1; }}
.hero-badge {{
    display: inline-block;
    padding: 6px 16px;
    background: rgba(59, 130, 246, 0.15);
    border: 1px solid rgba(59, 130, 246, 0.3);
    border-radius: 50px;
    font-size: 0.85rem;
    color: var(--accent-bright);
    margin-bottom: 28px;
    letter-spacing: 0.5px;
}}
.hero h1 {{
    font-size: clamp(2.2rem, 5vw, 3.8rem);
    font-weight: 800;
    line-height: 1.15;
    margin-bottom: 20px;
    letter-spacing: -0.5px;
}}
.hero h1 span {{
    background: linear-gradient(135deg, var(--accent-bright), var(--accent-purple));
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
}}
.hero-sub {{
    font-size: 1.2rem;
    color: var(--text-secondary);
    max-width: 640px;
    margin: 0 auto 40px;
    line-height: 1.7;
}}
.hero-actions {{
    display: flex;
    gap: 16px;
    justify-content: center;
    flex-wrap: wrap;
    margin-bottom: 48px;
}}
.hero-stats {{
    display: flex;
    justify-content: center;
    gap: 48px;
    flex-wrap: wrap;
}}
.hero-stat {{
    text-align: center;
}}
.hero-stat-value {{
    font-size: 2rem;
    font-weight: 800;
    color: var(--accent-bright);
}}
.hero-stat-label {{
    font-size: 0.85rem;
    color: var(--text-muted);
    text-transform: uppercase;
    letter-spacing: 1px;
    margin-top: 4px;
}}

/* ---- SECTIONS ---- */
.section {{
    padding: 80px 0;
}}
.section-header {{
    text-align: center;
    margin-bottom: 56px;
}}
.section-header h2 {{
    font-size: 2rem;
    font-weight: 700;
    margin-bottom: 12px;
}}
.section-header p {{
    color: var(--text-secondary);
    font-size: 1.1rem;
    max-width: 600px;
    margin: 0 auto;
}}
.section-divider {{
    border: none;
    border-top: 1px solid var(--border);
    margin: 0;
}}

/* ---- HOW IT WORKS ---- */
.steps {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
    gap: 32px;
}}
.step {{
    text-align: center;
    padding: 36px 24px;
    background: var(--bg-card);
    border-radius: var(--radius-lg);
    border: 1px solid var(--border-accent);
    transition: transform 0.2s, box-shadow 0.2s;
}}
.step:hover {{
    transform: translateY(-4px);
    box-shadow: var(--shadow);
}}
.step-number {{
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 48px; height: 48px;
    background: rgba(59, 130, 246, 0.15);
    color: var(--accent-bright);
    font-weight: 800;
    font-size: 1.25rem;
    border-radius: 50%;
    margin-bottom: 18px;
}}
.step h3 {{
    font-size: 1.15rem;
    margin-bottom: 10px;
}}
.step p {{
    color: var(--text-secondary);
    font-size: 0.95rem;
}}

/* ---- FEATURES ---- */
.features-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
    gap: 24px;
}}
.feature-card {{
    padding: 28px;
    background: var(--bg-card);
    border-radius: var(--radius);
    border: 1px solid var(--border-accent);
    transition: border-color 0.2s;
}}
.feature-card:hover {{
    border-color: var(--accent);
}}
.feature-icon {{
    font-size: 1.6rem;
    margin-bottom: 14px;
}}
.feature-card h4 {{
    font-size: 1.05rem;
    margin-bottom: 8px;
}}
.feature-card p {{
    color: var(--text-secondary);
    font-size: 0.92rem;
}}

/* ---- DEMO SCRIPTS (TABS) ---- */
.tabs-bar {{
    display: flex;
    gap: 8px;
    overflow-x: auto;
    padding-bottom: 8px;
    margin-bottom: 24px;
    flex-wrap: wrap;
}}
.tab-btn {{
    padding: 10px 20px;
    border-radius: 8px;
    border: 1px solid var(--border-accent);
    background: var(--bg-card);
    color: var(--text-secondary);
    font-size: 0.92rem;
    font-weight: 600;
    cursor: pointer;
    transition: all 0.2s;
    white-space: nowrap;
}}
.tab-btn:hover {{ background: var(--bg-card-hover); color: var(--text-primary); }}
.tab-btn.active {{
    background: var(--accent);
    color: #fff;
    border-color: var(--accent);
}}
.tab-panel {{ display: none; }}
.tab-panel.active {{ display: block; }}

.industry-header {{
    display: flex;
    align-items: center;
    gap: 16px;
    margin-bottom: 28px;
}}
.industry-icon {{ font-size: 2.4rem; }}
.industry-header h3 {{ font-size: 1.3rem; margin-bottom: 2px; }}
.industry-tagline {{ color: var(--text-secondary); font-size: 0.95rem; }}

.scenario {{
    margin-bottom: 32px;
    padding: 24px;
    background: var(--bg-secondary);
    border-radius: var(--radius);
    border: 1px solid var(--border);
}}
.scenario-title {{
    font-size: 1rem;
    color: var(--accent-bright);
    margin-bottom: 16px;
    padding-bottom: 12px;
    border-bottom: 1px solid var(--border);
}}
.conversation {{ display: flex; flex-direction: column; gap: 14px; }}
.msg {{ display: flex; flex-direction: column; max-width: 85%; }}
.msg-caller {{ align-self: flex-end; align-items: flex-end; }}
.msg-ai {{ align-self: flex-start; align-items: flex-start; }}
.msg-label {{
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 1px;
    color: var(--text-muted);
    margin-bottom: 4px;
    padding: 0 4px;
}}
.msg-bubble {{
    padding: 12px 18px;
    border-radius: 14px;
    font-size: 0.93rem;
    line-height: 1.55;
}}
.caller-bubble {{
    background: #1e3a5f;
    border-bottom-right-radius: 4px;
    color: #d1e3ff;
}}
.ai-bubble {{
    background: #1a3329;
    border-bottom-left-radius: 4px;
    color: #b8f0d8;
    border: 1px solid rgba(16, 185, 129, 0.2);
}}

/* ---- PRICING ---- */
.pricing-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
    gap: 28px;
    align-items: start;
}}
.price-card {{
    padding: 36px 28px;
    background: var(--bg-card);
    border-radius: var(--radius-lg);
    border: 1px solid var(--border-accent);
    text-align: center;
    position: relative;
    transition: transform 0.2s, box-shadow 0.2s;
}}
.price-card:hover {{
    transform: translateY(-4px);
    box-shadow: var(--shadow-lg);
}}
.price-card.featured {{
    border-color: var(--accent);
    box-shadow: 0 0 40px var(--accent-glow);
}}
.price-badge {{
    position: absolute;
    top: -13px;
    left: 50%;
    transform: translateX(-50%);
    background: var(--accent);
    color: #fff;
    font-size: 0.75rem;
    font-weight: 700;
    padding: 5px 18px;
    border-radius: 50px;
    text-transform: uppercase;
    letter-spacing: 1px;
}}
.price-card h3 {{
    font-size: 1.3rem;
    margin-bottom: 8px;
}}
.price-card .price {{
    font-size: 2.8rem;
    font-weight: 800;
    margin: 16px 0 4px;
}}
.price-card .price span {{
    font-size: 1rem;
    font-weight: 400;
    color: var(--text-muted);
}}
.price-card .price-note {{
    color: var(--text-muted);
    font-size: 0.85rem;
    margin-bottom: 24px;
}}
.price-card ul {{
    list-style: none;
    text-align: left;
    margin-bottom: 28px;
}}
.price-card ul li {{
    padding: 8px 0;
    font-size: 0.92rem;
    color: var(--text-secondary);
    border-bottom: 1px solid var(--border);
    display: flex;
    align-items: center;
    gap: 10px;
}}
.price-card ul li:last-child {{ border-bottom: none; }}
.check {{ color: var(--accent-green); font-weight: 700; }}

/* ---- TESTIMONIALS ---- */
.testimonials-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
    gap: 24px;
}}
.testimonial {{
    padding: 28px;
    background: var(--bg-card);
    border-radius: var(--radius);
    border: 1px solid var(--border-accent);
}}
.testimonial-stars {{
    color: var(--accent-orange);
    font-size: 1.1rem;
    margin-bottom: 14px;
    letter-spacing: 2px;
}}
.testimonial-text {{
    color: var(--text-secondary);
    font-size: 0.95rem;
    line-height: 1.7;
    margin-bottom: 16px;
    font-style: italic;
}}
.testimonial-author {{
    font-weight: 600;
    font-size: 0.92rem;
}}
.testimonial-role {{
    color: var(--text-muted);
    font-size: 0.82rem;
}}

/* ---- LEAD FORM ---- */
.form-section {{
    background: var(--bg-card);
    border-radius: var(--radius-lg);
    border: 1px solid var(--border-accent);
    padding: 48px;
    max-width: 640px;
    margin: 0 auto;
}}
.form-section h2 {{
    font-size: 1.6rem;
    margin-bottom: 8px;
    text-align: center;
}}
.form-section .form-sub {{
    text-align: center;
    color: var(--text-secondary);
    margin-bottom: 32px;
    font-size: 0.95rem;
}}
.form-row {{
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 16px;
    margin-bottom: 16px;
}}
.form-group {{
    display: flex;
    flex-direction: column;
    margin-bottom: 16px;
}}
.form-group label {{
    font-size: 0.85rem;
    font-weight: 600;
    color: var(--text-secondary);
    margin-bottom: 6px;
}}
.form-group input, .form-group select, .form-group textarea {{
    padding: 12px 16px;
    background: var(--bg-secondary);
    border: 1px solid var(--border-accent);
    border-radius: 8px;
    color: var(--text-primary);
    font-size: 0.95rem;
    font-family: inherit;
    transition: border-color 0.2s;
}}
.form-group input:focus, .form-group select:focus, .form-group textarea:focus {{
    outline: none;
    border-color: var(--accent);
    box-shadow: 0 0 0 3px var(--accent-glow);
}}
.form-group textarea {{ resize: vertical; min-height: 80px; }}
.form-group select {{ cursor: pointer; }}
.form-group select option {{ background: var(--bg-secondary); }}
.form-success {{
    display: none;
    text-align: center;
    padding: 24px;
    background: rgba(16, 185, 129, 0.1);
    border: 1px solid rgba(16, 185, 129, 0.3);
    border-radius: 12px;
    color: var(--accent-green);
}}
.form-success h3 {{ margin-bottom: 8px; }}
.form-error {{
    display: none;
    text-align: center;
    padding: 12px;
    background: rgba(239, 68, 68, 0.1);
    border: 1px solid rgba(239, 68, 68, 0.3);
    border-radius: 8px;
    color: #f87171;
    margin-bottom: 16px;
    font-size: 0.9rem;
}}

/* ---- FOOTER ---- */
.footer {{
    padding: 48px 0 32px;
    text-align: center;
    border-top: 1px solid var(--border);
    margin-top: 40px;
}}
.footer-brand {{
    font-size: 1.15rem;
    font-weight: 700;
    margin-bottom: 8px;
}}
.footer-text {{
    color: var(--text-muted);
    font-size: 0.85rem;
    line-height: 1.8;
}}
.footer-phone {{
    font-size: 1.2rem;
    font-weight: 700;
    color: var(--accent-bright);
    margin: 16px 0;
}}

/* ---- RESPONSIVE ---- */
@media (max-width: 768px) {{
    .hero {{ padding: 60px 0 50px; }}
    .hero h1 {{ font-size: 2rem; }}
    .form-row {{ grid-template-columns: 1fr; }}
    .form-section {{ padding: 28px 20px; }}
    .hero-stats {{ gap: 24px; }}
    .pricing-grid {{ grid-template-columns: 1fr; max-width: 400px; margin: 0 auto; }}
    .topbar-phone {{ display: none; }}
}}
@media (max-width: 480px) {{
    .container {{ padding: 0 16px; }}
    .hero-actions {{ flex-direction: column; align-items: center; }}
    .btn-lg {{ width: 100%; }}
}}
</style>
</head>
<body>

<!-- Top Bar -->
<nav class="topbar">
<div class="container">
    <div class="topbar-brand"><span class="dot"></span> Hive Dynamics AI</div>
    <div class="topbar-cta">
        <a href="tel:{DEMO_PHONE_RAW}" class="topbar-phone">{DEMO_PHONE}</a>
        <a href="#quote" class="btn btn-primary">Get a Quote</a>
    </div>
</div>
</nav>

<!-- Hero -->
<section class="hero">
<div class="container">
    <div class="hero-badge">&#9889; AI-Powered Phone Answering</div>
    <h1>Never Miss a Call.<br><span>Never Lose a Customer.</span></h1>
    <p class="hero-sub">
        Your AI receptionist answers every call in under 2 seconds — 24/7, 365 days a year.
        It books appointments, dispatches techs, answers questions, and captures leads
        while sounding completely natural.
    </p>
    <div class="hero-actions">
        <a href="tel:{DEMO_PHONE_RAW}" class="btn btn-green btn-lg">&#128222; Try a Demo Call</a>
        <a href="#quote" class="btn btn-outline btn-lg">Get a Quote</a>
    </div>
    <div class="hero-stats">
        <div class="hero-stat">
            <div class="hero-stat-value">&lt; 2s</div>
            <div class="hero-stat-label">Answer Time</div>
        </div>
        <div class="hero-stat">
            <div class="hero-stat-value">24/7</div>
            <div class="hero-stat-label">Availability</div>
        </div>
        <div class="hero-stat">
            <div class="hero-stat-value">98%</div>
            <div class="hero-stat-label">Satisfaction</div>
        </div>
        <div class="hero-stat">
            <div class="hero-stat-value">5 min</div>
            <div class="hero-stat-label">Setup Time</div>
        </div>
    </div>
</div>
</section>

<hr class="section-divider">

<!-- How It Works -->
<section class="section">
<div class="container">
    <div class="section-header">
        <h2>How It Works</h2>
        <p>Three simple steps to never miss another call</p>
    </div>
    <div class="steps">
        <div class="step">
            <div class="step-number">1</div>
            <h3>Forward Your Calls</h3>
            <p>Set your business line to forward to your Hive AI number when you're busy, after hours, or all the time.</p>
        </div>
        <div class="step">
            <div class="step-number">2</div>
            <h3>AI Answers Instantly</h3>
            <p>Our AI picks up in under 2 seconds, greets callers by your business name, and handles the conversation naturally.</p>
        </div>
        <div class="step">
            <div class="step-number">3</div>
            <h3>You Get the Details</h3>
            <p>Receive instant notifications with call summaries, booked appointments, and captured leads — via text, email, or dashboard.</p>
        </div>
    </div>
</div>
</section>

<hr class="section-divider">

<!-- Features -->
<section class="section">
<div class="container">
    <div class="section-header">
        <h2>Built for Real Businesses</h2>
        <p>Everything you need to handle calls like a pro — without hiring staff</p>
    </div>
    <div class="features-grid">
        <div class="feature-card">
            <div class="feature-icon">&#128222;</div>
            <h4>Instant Call Answering</h4>
            <p>Every call answered in under 2 seconds. No hold music, no voicemail, no missed revenue. Your customers talk to someone immediately.</p>
        </div>
        <div class="feature-card">
            <div class="feature-icon">&#128197;</div>
            <h4>Appointment Booking</h4>
            <p>Integrates with your calendar to book appointments in real time. Sends confirmations to both you and the customer automatically.</p>
        </div>
        <div class="feature-card">
            <div class="feature-icon">&#127919;</div>
            <h4>Lead Capture</h4>
            <p>Collects name, phone, email, and service details from every caller. Sends you a lead notification in real time so you can follow up fast.</p>
        </div>
        <div class="feature-card">
            <div class="feature-icon">&#128666;</div>
            <h4>Emergency Dispatch</h4>
            <p>For service businesses: dispatches your nearest available technician to emergency calls. Gives the caller an ETA and keeps them informed.</p>
        </div>
        <div class="feature-card">
            <div class="feature-icon">&#129302;</div>
            <h4>Natural Conversations</h4>
            <p>Not a phone tree. Not a robot voice. Our AI has real conversations — it understands context, handles objections, and sounds human.</p>
        </div>
        <div class="feature-card">
            <div class="feature-icon">&#128202;</div>
            <h4>Analytics Dashboard</h4>
            <p>See every call, every lead, every booking. Track peak hours, common questions, and conversion rates. Data-driven phone management.</p>
        </div>
        <div class="feature-card">
            <div class="feature-icon">&#127760;</div>
            <h4>Bilingual Support</h4>
            <p>Handle calls in English and Spanish. The AI detects the caller's language and responds naturally in their preferred language.</p>
        </div>
        <div class="feature-card">
            <div class="feature-icon">&#128274;</div>
            <h4>Custom Scripts</h4>
            <p>Tailor the AI's responses to match your business. Set pricing info, service areas, FAQs, and special instructions. Your business, your rules.</p>
        </div>
        <div class="feature-card">
            <div class="feature-icon">&#128241;</div>
            <h4>SMS Follow-Up</h4>
            <p>Automatically texts callers with booking confirmations, directions to your location, or a link to your website after every call.</p>
        </div>
    </div>
</div>
</section>

<hr class="section-divider">

<!-- Live Demo Scripts -->
<section class="section" id="demos">
<div class="container">
    <div class="section-header">
        <h2>See It in Action</h2>
        <p>Real sample conversations showing how the AI handles calls for different industries</p>
    </div>
    <div class="tabs-bar">
        {script_tabs}
    </div>
    {script_panels}
    <div style="text-align:center; margin-top: 36px;">
        <a href="tel:{DEMO_PHONE_RAW}" class="btn btn-green btn-lg">&#128222; Call Now to Hear It Live</a>
    </div>
</div>
</section>

<hr class="section-divider">

<!-- Pricing -->
<section class="section" id="pricing">
<div class="container">
    <div class="section-header">
        <h2>Simple, Transparent Pricing</h2>
        <p>No contracts. No setup fees. Cancel anytime.</p>
    </div>
    <div class="pricing-grid">
        <!-- Starter -->
        <div class="price-card">
            <h3>Starter</h3>
            <p style="color:var(--text-muted); font-size:0.9rem;">For small businesses getting started</p>
            <div class="price">$299<span>/mo</span></div>
            <p class="price-note">Billed monthly</p>
            <ul>
                <li><span class="check">&#10003;</span> Up to 200 calls/month</li>
                <li><span class="check">&#10003;</span> 24/7 AI answering</li>
                <li><span class="check">&#10003;</span> Lead capture &amp; notifications</li>
                <li><span class="check">&#10003;</span> Call summaries via SMS</li>
                <li><span class="check">&#10003;</span> Basic analytics dashboard</li>
                <li><span class="check">&#10003;</span> 1 phone number</li>
                <li><span class="check">&#10003;</span> Email support</li>
            </ul>
            <a href="#quote" class="btn btn-outline" style="width:100%;">Get Started</a>
        </div>
        <!-- Pro -->
        <div class="price-card featured">
            <div class="price-badge">Most Popular</div>
            <h3>Pro</h3>
            <p style="color:var(--text-muted); font-size:0.9rem;">For growing businesses that need more</p>
            <div class="price">$499<span>/mo</span></div>
            <p class="price-note">Billed monthly</p>
            <ul>
                <li><span class="check">&#10003;</span> Up to 500 calls/month</li>
                <li><span class="check">&#10003;</span> Everything in Starter</li>
                <li><span class="check">&#10003;</span> Appointment booking</li>
                <li><span class="check">&#10003;</span> Emergency dispatch</li>
                <li><span class="check">&#10003;</span> Custom call scripts</li>
                <li><span class="check">&#10003;</span> Bilingual (EN/ES)</li>
                <li><span class="check">&#10003;</span> 3 phone numbers</li>
                <li><span class="check">&#10003;</span> Priority support</li>
            </ul>
            <a href="#quote" class="btn btn-primary" style="width:100%;">Get Started</a>
        </div>
        <!-- Enterprise -->
        <div class="price-card">
            <h3>Enterprise</h3>
            <p style="color:var(--text-muted); font-size:0.9rem;">For high-volume operations</p>
            <div class="price">$999<span>/mo</span></div>
            <p class="price-note">Billed monthly</p>
            <ul>
                <li><span class="check">&#10003;</span> Unlimited calls</li>
                <li><span class="check">&#10003;</span> Everything in Pro</li>
                <li><span class="check">&#10003;</span> Multi-location support</li>
                <li><span class="check">&#10003;</span> CRM integration</li>
                <li><span class="check">&#10003;</span> Custom AI training</li>
                <li><span class="check">&#10003;</span> Dedicated account manager</li>
                <li><span class="check">&#10003;</span> Unlimited phone numbers</li>
                <li><span class="check">&#10003;</span> SLA guarantee</li>
            </ul>
            <a href="#quote" class="btn btn-outline" style="width:100%;">Contact Sales</a>
        </div>
    </div>
</div>
</section>

<hr class="section-divider">

<!-- Testimonials -->
<section class="section">
<div class="container">
    <div class="section-header">
        <h2>Trusted by Local Businesses</h2>
        <p>Hear from businesses already using AI phone answering</p>
    </div>
    <div class="testimonials-grid">
        <div class="testimonial">
            <div class="testimonial-stars">&#9733;&#9733;&#9733;&#9733;&#9733;</div>
            <p class="testimonial-text">"We were missing 40% of our after-hours calls. Within the first week, Hive AI captured 23 new leads we would have lost. It paid for itself on day one."</p>
            <div class="testimonial-author">Mike R.</div>
            <div class="testimonial-role">Owner, 24/7 Locksmith Services</div>
        </div>
        <div class="testimonial">
            <div class="testimonial-stars">&#9733;&#9733;&#9733;&#9733;&#9733;</div>
            <p class="testimonial-text">"My receptionist was overwhelmed. Now the AI handles overflow calls during peak hours and every after-hours call. Patients love that someone always picks up."</p>
            <div class="testimonial-author">Dr. Sarah L.</div>
            <div class="testimonial-role">Family Practice, Destin FL</div>
        </div>
        <div class="testimonial">
            <div class="testimonial-stars">&#9733;&#9733;&#9733;&#9733;&#9733;</div>
            <p class="testimonial-text">"I was skeptical about AI answering my phones. Then a customer told me they thought they were talking to my best employee. That sold me."</p>
            <div class="testimonial-author">James T.</div>
            <div class="testimonial-role">Gulf Coast Plumbing &amp; HVAC</div>
        </div>
    </div>
</div>
</section>

<hr class="section-divider">

<!-- Lead Capture Form -->
<section class="section" id="quote">
<div class="container">
    <div class="form-section">
        <h2>Get a Quote</h2>
        <p class="form-sub">Tell us about your business and we'll build a custom solution. No obligation, no pressure.</p>

        <div id="form-error" class="form-error"></div>
        <div id="form-success" class="form-success">
            <h3>&#10003; Thank You!</h3>
            <p>We received your request and will reach out within 1 business hour.</p>
        </div>

        <form id="lead-form" onsubmit="return submitLead(event)">
            <div class="form-row">
                <div class="form-group">
                    <label for="lead-name">Your Name *</label>
                    <input type="text" id="lead-name" name="name" required placeholder="John Smith">
                </div>
                <div class="form-group">
                    <label for="lead-business">Business Name</label>
                    <input type="text" id="lead-business" name="business" placeholder="Smith's Plumbing">
                </div>
            </div>
            <div class="form-row">
                <div class="form-group">
                    <label for="lead-phone">Phone Number *</label>
                    <input type="tel" id="lead-phone" name="phone" required placeholder="(555) 123-4567">
                </div>
                <div class="form-group">
                    <label for="lead-email">Email Address</label>
                    <input type="email" id="lead-email" name="email" placeholder="john@example.com">
                </div>
            </div>
            <div class="form-group">
                <label for="lead-industry">Industry</label>
                <select id="lead-industry" name="industry">
                    <option value="">Select your industry...</option>
                    <option value="locksmith">Locksmith</option>
                    <option value="plumber">Plumber</option>
                    <option value="hvac">HVAC</option>
                    <option value="medical">Medical Office</option>
                    <option value="restaurant">Restaurant</option>
                    <option value="dental">Dental Office</option>
                    <option value="legal">Law Firm</option>
                    <option value="real_estate">Real Estate</option>
                    <option value="auto_repair">Auto Repair</option>
                    <option value="salon">Salon / Spa</option>
                    <option value="other">Other</option>
                </select>
            </div>
            <div class="form-group">
                <label for="lead-message">Tell us about your needs</label>
                <textarea id="lead-message" name="message" placeholder="How many calls do you get per day? What hours do you need coverage? Any special requirements?"></textarea>
            </div>
            <button type="submit" class="btn btn-green btn-lg" style="width:100%;" id="submit-btn">Request a Quote</button>
        </form>
    </div>
</div>
</section>

<!-- Footer -->
<footer class="footer">
<div class="container">
    <div class="footer-brand">Hive Dynamics AI</div>
    <div class="footer-phone"><a href="tel:{DEMO_PHONE_RAW}" style="color:var(--accent-bright);">{DEMO_PHONE}</a></div>
    <p class="footer-text">
        AI-powered phone answering for local businesses.<br>
        Northwest Florida &amp; nationwide.<br>
        &copy; 2026 Hive Dynamics. All rights reserved.
    </p>
</div>
</footer>

<script>
function showTab(key) {{
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
    document.getElementById('tab-' + key).classList.add('active');
    document.getElementById('panel-' + key).classList.add('active');
}}

async function submitLead(e) {{
    e.preventDefault();
    const btn = document.getElementById('submit-btn');
    const errDiv = document.getElementById('form-error');
    const successDiv = document.getElementById('form-success');
    const form = document.getElementById('lead-form');

    errDiv.style.display = 'none';
    btn.disabled = true;
    btn.textContent = 'Submitting...';

    const data = {{
        name: document.getElementById('lead-name').value.trim(),
        business: document.getElementById('lead-business').value.trim(),
        phone: document.getElementById('lead-phone').value.trim(),
        email: document.getElementById('lead-email').value.trim(),
        industry: document.getElementById('lead-industry').value,
        message: document.getElementById('lead-message').value.trim()
    }};

    try {{
        const resp = await fetch('/api/lead', {{
            method: 'POST',
            headers: {{ 'Content-Type': 'application/json' }},
            body: JSON.stringify(data)
        }});
        const result = await resp.json();
        if (resp.ok) {{
            form.style.display = 'none';
            successDiv.style.display = 'block';
        }} else {{
            errDiv.textContent = result.detail || 'Something went wrong. Please try again.';
            errDiv.style.display = 'block';
            btn.disabled = false;
            btn.textContent = 'Request a Quote';
        }}
    }} catch (err) {{
        errDiv.textContent = 'Network error. Please try again.';
        errDiv.style.display = 'block';
        btn.disabled = false;
        btn.textContent = 'Request a Quote';
    }}
}}
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
async def landing_page():
    """Serve the professional demo landing page."""
    return HTMLResponse(content=build_landing_page(), status_code=200)


@app.get("/health")
async def health():
    """Health check endpoint."""
    with get_db() as conn:
        lead_count = conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
        demo_count = conn.execute("SELECT COUNT(*) FROM demo_requests").fetchone()[0]
    return {
        "status": "healthy",
        "service": "ai-phone-demo",
        "port": PORT,
        "leads": lead_count,
        "demo_requests": demo_count,
        "uptime": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.post("/api/lead")
async def capture_lead(request: Request):
    """Capture a lead from the demo page form."""
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    name = (body.get("name") or "").strip()
    phone = (body.get("phone") or "").strip()

    if not name:
        raise HTTPException(status_code=422, detail="Name is required.")
    if not phone:
        raise HTTPException(status_code=422, detail="Phone number is required.")

    business = (body.get("business") or "").strip()
    email = (body.get("email") or "").strip()
    industry = (body.get("industry") or "").strip()
    message = (body.get("message") or "").strip()
    source = (body.get("source") or "demo_page").strip()

    with get_db() as conn:
        cur = conn.execute(
            """INSERT INTO leads (name, business, phone, email, industry, message, source)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (name, business, phone, email, industry, message, source),
        )
        conn.commit()
        lead_id = cur.lastrowid

        # Also log a demo request
        ip = request.client.host if request.client else "unknown"
        ua = request.headers.get("user-agent", "unknown")
        conn.execute(
            "INSERT INTO demo_requests (lead_id, ip_address, user_agent) VALUES (?, ?, ?)",
            (lead_id, ip, ua),
        )
        conn.commit()

    return {
        "status": "ok",
        "lead_id": lead_id,
        "message": f"Thank you, {name}. We'll be in touch within 1 business hour.",
    }


@app.get("/api/leads")
async def list_leads():
    """List all captured leads (for internal use)."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM leads ORDER BY created_at DESC LIMIT 100"
        ).fetchall()
    leads = []
    for row in rows:
        leads.append({
            "id": row["id"],
            "name": row["name"],
            "business": row["business"],
            "phone": row["phone"],
            "email": row["email"],
            "industry": row["industry"],
            "message": row["message"],
            "source": row["source"],
            "status": row["status"],
            "created_at": row["created_at"],
        })
    return {"leads": leads, "count": len(leads)}


@app.get("/api/demo-scripts")
async def demo_scripts():
    """Return all demo phone scripts organized by industry."""
    return {"industries": DEMO_SCRIPTS}


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print(f"AI Phone Answering Demo — starting on port {PORT}")
    print(f"Database: {DB_PATH}")
    print(f"Landing page: http://localhost:{PORT}/")
    print(f"Health: http://localhost:{PORT}/health")
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
