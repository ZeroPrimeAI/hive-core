#!/usr/bin/env python3
"""
HIVE DISPATCH ADMIN — Locksmith Dispatching & Invoicing Software
================================================================
Full admin dashboard for Chris to manage leads, jobs, invoices, notes.
Port: 8141
"""
import sqlite3
import json
import uuid
import time
import os
import hmac
import hashlib
from datetime import datetime, timedelta
from pathlib import Path

from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

PORT = 8141
DB_PATH = "/THE_HIVE/memory/dispatch.db"
LEADS_DB = "/THE_HIVE/memory/leads.db"

# Stripe
STRIPE_SECRET_KEY = os.environ.get("STRIPE_SECRET_KEY", "")
STRIPE_PUBLISHABLE_KEY = os.environ.get("STRIPE_PUBLISHABLE_KEY", "")

# Load from .env if not in environment
if not STRIPE_SECRET_KEY:
    env_path = "/THE_HIVE/.env"
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line.startswith("#") or "=" not in line:
                    continue
                key, val = line.split("=", 1)
                key = key.strip()
                val = val.strip().strip("'\"")
                if key == "STRIPE_SECRET_KEY":
                    STRIPE_SECRET_KEY = val
                elif key == "STRIPE_PUBLISHABLE_KEY":
                    STRIPE_PUBLISHABLE_KEY = val

try:
    import stripe
    stripe.api_key = STRIPE_SECRET_KEY
    STRIPE_AVAILABLE = bool(STRIPE_SECRET_KEY)
except ImportError:
    STRIPE_AVAILABLE = False

app = FastAPI(title="Hive Dispatch Admin")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── Database ─────────────────────────────────────────────────────
def get_db():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    return db

def get_leads_db():
    db = sqlite3.connect(LEADS_DB)
    db.row_factory = sqlite3.Row
    return db

def init_db():
    db = get_db()
    db.executescript("""
        CREATE TABLE IF NOT EXISTS dispatch_jobs (
            job_id TEXT PRIMARY KEY,
            customer_id TEXT,
            technician_id TEXT,
            status TEXT NOT NULL DEFAULT 'new',
            priority TEXT NOT NULL DEFAULT 'normal',
            title TEXT NOT NULL,
            description TEXT,
            address TEXT,
            scheduled_at REAL,
            dispatched_at REAL,
            started_at REAL,
            completed_at REAL,
            created_at REAL NOT NULL,
            notes TEXT,
            eta_minutes INTEGER,
            eta_updated_at REAL,
            customer_notified INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS dispatch_customers (
            customer_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            address TEXT,
            phone TEXT,
            email TEXT,
            notes TEXT,
            preferred_tech TEXT,
            created_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS dispatch_technicians (
            tech_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            phone TEXT,
            skills TEXT,
            zone TEXT,
            status TEXT NOT NULL DEFAULT 'offline',
            created_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS dispatch_invoices (
            invoice_id TEXT PRIMARY KEY,
            job_id TEXT,
            customer_id TEXT,
            technician_id TEXT,
            line_items TEXT,
            subtotal REAL DEFAULT 0,
            tax REAL DEFAULT 0,
            total REAL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'draft',
            created_at REAL NOT NULL,
            paid_at REAL,
            pdf_path TEXT,
            stripe_link TEXT
        );
        CREATE TABLE IF NOT EXISTS dispatch_photos (
            photo_id TEXT PRIMARY KEY,
            job_id TEXT NOT NULL,
            uploaded_by TEXT NOT NULL DEFAULT 'client',
            filename TEXT NOT NULL,
            filepath TEXT NOT NULL,
            caption TEXT,
            uploaded_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS dispatch_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT,
            action TEXT,
            details TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS dispatch_notes (
            note_id TEXT PRIMARY KEY,
            job_id TEXT NOT NULL,
            author TEXT DEFAULT 'Chris',
            content TEXT NOT NULL,
            created_at REAL NOT NULL
        );
    """)
    db.commit()
    db.close()

init_db()

def gen_id():
    return uuid.uuid4().hex[:16]

def now_ts():
    return time.time()

def fmt_ts(ts):
    if not ts:
        return "-"
    try:
        return datetime.fromtimestamp(float(ts)).strftime("%m/%d %I:%M %p")
    except Exception:
        return str(ts)[:16]

def fmt_money(amount):
    if not amount:
        return "$0.00"
    return f"${float(amount):,.2f}"

def log_action(db, job_id, action, details=""):
    db.execute("INSERT INTO dispatch_log (job_id, action, details) VALUES (?,?,?)",
               (job_id, action, details))

# ── CSS ──────────────────────────────────────────────────────────
ADMIN_CSS = """
*{margin:0;padding:0;box-sizing:border-box}
:root{
  --bg:#0a0a1a;--bg2:#111128;--bg3:#1a1a35;--bg4:#242450;
  --green:#00ff88;--green2:#00cc6a;--blue:#60a5fa;--purple:#a78bfa;
  --red:#ef4444;--orange:#f59e0b;--cyan:#22d3ee;--white:#e0e0e0;
  --gray:#6b7280;--gray2:#374151;--gray3:#1f2937;
}
html{scroll-behavior:smooth}
body{font-family:'Segoe UI',system-ui,sans-serif;background:var(--bg);color:var(--white);min-height:100vh}
a{color:var(--blue);text-decoration:none}
a:hover{text-decoration:underline}

/* Nav */
.topbar{background:var(--bg2);border-bottom:1px solid var(--bg4);padding:12px 24px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:100}
.topbar h1{font-size:18px;color:var(--green);font-weight:700;letter-spacing:1px}
.topbar nav{display:flex;gap:20px}
.topbar nav a{color:var(--gray);font-size:14px;font-weight:500;padding:6px 12px;border-radius:6px;transition:.2s}
.topbar nav a:hover,.topbar nav a.active{color:var(--green);background:rgba(0,255,136,.08);text-decoration:none}

/* Layout */
.page{max-width:1400px;margin:0 auto;padding:20px}
.page-title{font-size:24px;font-weight:700;color:var(--green);margin-bottom:20px}

/* Cards */
.stats-row{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:16px;margin-bottom:24px}
.stat-card{background:var(--bg2);border:1px solid var(--bg4);border-radius:12px;padding:20px;text-align:center}
.stat-card .num{font-size:32px;font-weight:800;margin-bottom:4px}
.stat-card .label{font-size:12px;color:var(--gray);text-transform:uppercase;letter-spacing:1px}

/* Tables */
table{width:100%;border-collapse:collapse;background:var(--bg2);border-radius:12px;overflow:hidden;border:1px solid var(--bg4)}
th{background:var(--bg3);padding:12px 16px;text-align:left;font-size:12px;text-transform:uppercase;letter-spacing:1px;color:var(--gray);font-weight:600;border-bottom:1px solid var(--bg4)}
td{padding:10px 16px;border-bottom:1px solid rgba(255,255,255,.04);font-size:14px}
tr:hover td{background:rgba(0,255,136,.02)}

/* Badges */
.badge{display:inline-block;padding:3px 10px;border-radius:20px;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.5px}
.badge-new{background:rgba(96,165,250,.15);color:var(--blue)}
.badge-dispatched{background:rgba(167,139,250,.15);color:var(--purple)}
.badge-enroute{background:rgba(34,211,238,.15);color:var(--cyan)}
.badge-in_progress,.badge-in-progress{background:rgba(245,158,11,.15);color:var(--orange)}
.badge-completed{background:rgba(0,255,136,.15);color:var(--green)}
.badge-cancelled{background:rgba(239,68,68,.15);color:var(--red)}
.badge-emergency{background:rgba(239,68,68,.2);color:var(--red)}
.badge-normal{background:rgba(107,114,128,.2);color:var(--gray)}
.badge-draft{background:rgba(107,114,128,.15);color:var(--gray)}
.badge-sent{background:rgba(96,165,250,.15);color:var(--blue)}
.badge-paid{background:rgba(0,255,136,.15);color:var(--green)}
.badge-overdue{background:rgba(239,68,68,.15);color:var(--red)}

/* Buttons */
.btn{display:inline-block;padding:8px 18px;border-radius:8px;font-size:13px;font-weight:600;border:none;cursor:pointer;transition:.2s;text-decoration:none;color:var(--bg)}
.btn:hover{transform:translateY(-1px);text-decoration:none}
.btn-green{background:var(--green);color:#000}.btn-green:hover{background:var(--green2)}
.btn-blue{background:var(--blue);color:#000}.btn-blue:hover{background:#3b82f6}
.btn-purple{background:var(--purple);color:#000}
.btn-red{background:var(--red);color:#fff}
.btn-orange{background:var(--orange);color:#000}
.btn-sm{padding:5px 12px;font-size:11px}
.btn-outline{background:transparent;border:1px solid var(--bg4);color:var(--white)}.btn-outline:hover{border-color:var(--green);color:var(--green)}
.actions{display:flex;gap:8px;flex-wrap:wrap;margin:16px 0}

/* Forms */
.form-group{margin-bottom:16px}
.form-group label{display:block;font-size:13px;font-weight:600;color:var(--gray);margin-bottom:6px;text-transform:uppercase;letter-spacing:.5px}
.form-group input,.form-group textarea,.form-group select{width:100%;padding:10px 14px;background:var(--bg3);border:1px solid var(--bg4);border-radius:8px;color:var(--white);font-size:14px;font-family:inherit}
.form-group input:focus,.form-group textarea:focus,.form-group select:focus{outline:none;border-color:var(--green);box-shadow:0 0 0 2px rgba(0,255,136,.1)}
.form-group textarea{min-height:80px;resize:vertical}
.form-row{display:grid;grid-template-columns:1fr 1fr;gap:16px}

/* Detail card */
.detail-card{background:var(--bg2);border:1px solid var(--bg4);border-radius:12px;padding:24px;margin-bottom:20px}
.detail-card h3{color:var(--green);font-size:16px;margin-bottom:16px;padding-bottom:8px;border-bottom:1px solid var(--bg4)}
.detail-row{display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid rgba(255,255,255,.03)}
.detail-row .key{color:var(--gray);font-size:13px}
.detail-row .val{font-size:14px;font-weight:500}

/* Timeline */
.timeline{margin:16px 0}
.timeline-item{display:flex;gap:12px;padding:8px 0}
.timeline-dot{width:10px;height:10px;border-radius:50%;background:var(--green);margin-top:5px;flex-shrink:0}
.timeline-content{flex:1}
.timeline-content .time{font-size:11px;color:var(--gray)}
.timeline-content .text{font-size:13px;margin-top:2px}

/* Notes */
.note{background:var(--bg3);border-radius:8px;padding:12px 16px;margin-bottom:10px;border-left:3px solid var(--green)}
.note .meta{font-size:11px;color:var(--gray);margin-bottom:4px}
.note .body{font-size:14px;line-height:1.5}

/* Filters */
.filters{display:flex;gap:10px;margin-bottom:20px;flex-wrap:wrap;align-items:center}
.filters select,.filters input{padding:8px 12px;background:var(--bg3);border:1px solid var(--bg4);border-radius:8px;color:var(--white);font-size:13px}

/* Invoice */
.invoice-line{display:grid;grid-template-columns:2fr 1fr 1fr 1fr auto;gap:10px;align-items:center;margin-bottom:8px}
.invoice-line input{padding:8px;background:var(--bg3);border:1px solid var(--bg4);border-radius:6px;color:var(--white);font-size:13px}
.invoice-totals{text-align:right;margin-top:16px;padding-top:16px;border-top:1px solid var(--bg4)}
.invoice-totals .row{display:flex;justify-content:flex-end;gap:24px;padding:4px 0;font-size:14px}
.invoice-totals .total{font-size:20px;font-weight:700;color:var(--green)}

/* Kanban */
.kanban{display:grid;grid-template-columns:repeat(5,1fr);gap:16px;margin-bottom:24px}
.kanban-col{background:var(--bg2);border:1px solid var(--bg4);border-radius:12px;padding:16px;min-height:200px}
.kanban-col h4{font-size:12px;text-transform:uppercase;letter-spacing:1px;color:var(--gray);margin-bottom:12px;padding-bottom:8px;border-bottom:1px solid var(--bg4)}
.kanban-card{background:var(--bg3);border:1px solid var(--bg4);border-radius:8px;padding:12px;margin-bottom:8px;transition:.2s;cursor:pointer}
.kanban-card:hover{border-color:var(--green);transform:translateY(-2px)}
.kanban-card .title{font-size:13px;font-weight:600;margin-bottom:4px}
.kanban-card .sub{font-size:11px;color:var(--gray)}

/* Mobile */
@media(max-width:900px){
  .kanban{grid-template-columns:1fr}
  .stats-row{grid-template-columns:repeat(2,1fr)}
  .form-row{grid-template-columns:1fr}
  .topbar nav{gap:8px}
  .topbar nav a{font-size:12px;padding:4px 8px}
  .invoice-line{grid-template-columns:1fr}
}
@media(max-width:600px){
  .stats-row{grid-template-columns:1fr}
  .page{padding:12px}
  .topbar{flex-direction:column;gap:8px}
}

/* Customer invoice (light theme) */
.invoice-public{background:#fff;color:#1a1a2e;max-width:800px;margin:0 auto;padding:40px;font-family:'Segoe UI',sans-serif}
.invoice-public h1{color:#1a1a2e;font-size:28px;margin-bottom:4px}
.invoice-public .company{color:#6366f1;font-size:14px;margin-bottom:24px}
.invoice-public table{border:1px solid #e5e7eb}
.invoice-public th{background:#f3f4f6;color:#374151;border-bottom:1px solid #e5e7eb}
.invoice-public td{color:#1a1a2e;border-bottom:1px solid #f3f4f6}
.invoice-public .totals{text-align:right;margin-top:20px}
.invoice-public .totals .row{display:flex;justify-content:flex-end;gap:24px;padding:4px 0;font-size:15px;color:#374151}
.invoice-public .totals .total-row{font-size:22px;font-weight:700;color:#1a1a2e;border-top:2px solid #1a1a2e;padding-top:8px;margin-top:8px}
.invoice-public .pay-btn{display:inline-block;background:#6366f1;color:#fff;padding:14px 36px;border-radius:8px;font-weight:700;font-size:16px;margin-top:24px;text-decoration:none}
.invoice-public .pay-btn:hover{background:#4f46e5}
"""

# ── Topbar HTML ──────────────────────────────────────────────────
def topbar(active="dashboard"):
    def cls(name):
        return "active" if name == active else ""
    return f"""
    <div class="topbar">
        <h1>HIVE DISPATCH</h1>
        <nav>
            <a href="/admin/" class="{cls('dashboard')}">Dashboard</a>
            <a href="/admin/leads" class="{cls('leads')}">Leads</a>
            <a href="/admin/jobs" class="{cls('jobs')}">Jobs</a>
            <a href="/admin/invoices" class="{cls('invoices')}">Invoices</a>
            <a href="/admin/customers" class="{cls('customers')}">Customers</a>
            <a href="/admin/new-job" class="{cls('new-job')}">+ New Job</a>
        </nav>
    </div>"""

def page_wrap(title, content, active="dashboard"):
    return HTMLResponse(f"""<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title} — Hive Dispatch</title>
<style>{ADMIN_CSS}</style>
</head><body>
{topbar(active)}
<div class="page">
{content}
</div>
<script>
function postAction(url, data={{}}) {{
    fetch(url, {{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify(data)}})
    .then(r=>r.json()).then(()=>location.reload()).catch(e=>alert('Error: '+e));
}}
function confirmAction(msg, url, data={{}}) {{
    if(confirm(msg)) postAction(url, data);
}}
</script>
</body></html>""")


# ── Health ───────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {"status": "alive", "service": "hive-dispatch-admin", "port": PORT}


# ── Dashboard ────────────────────────────────────────────────────
@app.get("/admin/", response_class=HTMLResponse)
@app.get("/admin", response_class=HTMLResponse)
async def dashboard():
    db = get_db()
    today_start = datetime.now().replace(hour=0, minute=0, second=0).timestamp()

    total_jobs = db.execute("SELECT count(*) c FROM dispatch_jobs").fetchone()["c"]
    new_jobs = db.execute("SELECT count(*) c FROM dispatch_jobs WHERE status='new'").fetchone()["c"]
    active_jobs = db.execute("SELECT count(*) c FROM dispatch_jobs WHERE status IN ('dispatched','enroute','in_progress')").fetchone()["c"]
    completed_today = db.execute("SELECT count(*) c FROM dispatch_jobs WHERE status='completed' AND completed_at>=?", (today_start,)).fetchone()["c"]
    jobs_today = db.execute("SELECT count(*) c FROM dispatch_jobs WHERE created_at>=?", (today_start,)).fetchone()["c"]

    revenue_today = db.execute("SELECT COALESCE(SUM(total),0) t FROM dispatch_invoices WHERE status='paid' AND paid_at>=?", (today_start,)).fetchone()["t"]
    total_revenue = db.execute("SELECT COALESCE(SUM(total),0) t FROM dispatch_invoices WHERE status='paid'").fetchone()["t"]
    pending_invoices = db.execute("SELECT count(*) c FROM dispatch_invoices WHERE status IN ('draft','sent')").fetchone()["c"]

    # Get leads count
    leads_today = 0
    try:
        ldb = get_leads_db()
        leads_today = ldb.execute("SELECT count(*) c FROM leads WHERE created_at >= datetime('now','-1 day')").fetchone()["c"]
        ldb.close()
    except Exception:
        pass

    # Recent jobs
    recent_jobs = db.execute(
        "SELECT j.*, c.name as cust_name, c.phone as cust_phone FROM dispatch_jobs j "
        "LEFT JOIN dispatch_customers c ON j.customer_id=c.customer_id "
        "ORDER BY j.created_at DESC LIMIT 15"
    ).fetchall()

    # Recent activity
    recent_log = db.execute(
        "SELECT * FROM dispatch_log ORDER BY id DESC LIMIT 10"
    ).fetchall()

    db.close()

    job_rows = ""
    for j in recent_jobs:
        status = j["status"]
        priority = j["priority"] or "normal"
        cust = j["cust_name"] or "Unknown"
        phone = j["cust_phone"] or ""
        job_rows += f"""<tr>
            <td><a href="/admin/job/{j['job_id']}" style="color:var(--green);font-weight:600">{j['job_id'][:8]}...</a></td>
            <td>{j['title']}</td>
            <td>{cust}</td>
            <td>{phone}</td>
            <td><span class="badge badge-{status}">{status}</span></td>
            <td><span class="badge badge-{priority}">{priority}</span></td>
            <td>{fmt_ts(j['created_at'])}</td>
        </tr>"""

    activity_rows = ""
    for log in recent_log:
        activity_rows += f"""<div class="timeline-item">
            <div class="timeline-dot"></div>
            <div class="timeline-content">
                <div class="time">{log['created_at']}</div>
                <div class="text"><strong>{log['action']}</strong> — {log['details'] or ''} <span style="color:var(--gray)">(Job: {(log['job_id'] or '')[:8]})</span></div>
            </div>
        </div>"""

    content = f"""
    <h2 class="page-title">Dashboard</h2>
    <div class="stats-row">
        <div class="stat-card"><div class="num" style="color:var(--blue)">{leads_today}</div><div class="label">Leads Today</div></div>
        <div class="stat-card"><div class="num" style="color:var(--orange)">{new_jobs}</div><div class="label">New Jobs</div></div>
        <div class="stat-card"><div class="num" style="color:var(--cyan)">{active_jobs}</div><div class="label">Active Jobs</div></div>
        <div class="stat-card"><div class="num" style="color:var(--green)">{completed_today}</div><div class="label">Completed Today</div></div>
        <div class="stat-card"><div class="num" style="color:var(--green)">{fmt_money(revenue_today)}</div><div class="label">Revenue Today</div></div>
        <div class="stat-card"><div class="num" style="color:var(--purple)">{fmt_money(total_revenue)}</div><div class="label">Total Revenue</div></div>
        <div class="stat-card"><div class="num" style="color:var(--orange)">{pending_invoices}</div><div class="label">Pending Invoices</div></div>
        <div class="stat-card"><div class="num" style="color:var(--white)">{total_jobs}</div><div class="label">Total Jobs</div></div>
    </div>

    <div class="actions">
        <a href="/admin/new-job" class="btn btn-green">+ New Job</a>
        <a href="/admin/leads" class="btn btn-blue">View Leads</a>
        <a href="/admin/jobs" class="btn btn-purple">All Jobs</a>
        <a href="/admin/invoices" class="btn btn-orange">Invoices</a>
    </div>

    <div class="detail-card">
        <h3>Recent Jobs</h3>
        <div style="overflow-x:auto">
        <table>
            <tr><th>ID</th><th>Title</th><th>Customer</th><th>Phone</th><th>Status</th><th>Priority</th><th>Created</th></tr>
            {job_rows}
        </table>
        </div>
    </div>

    <div class="detail-card">
        <h3>Recent Activity</h3>
        <div class="timeline">
            {activity_rows if activity_rows else '<div style="color:var(--gray);padding:20px;text-align:center">No recent activity</div>'}
        </div>
    </div>
    """
    return page_wrap("Dashboard", content, "dashboard")


# ── Leads ────────────────────────────────────────────────────────
@app.get("/admin/leads", response_class=HTMLResponse)
async def leads_page():
    # Get leads from leads.db
    leads = []
    try:
        ldb = get_leads_db()
        leads = ldb.execute(
            "SELECT * FROM leads ORDER BY created_at DESC LIMIT 100"
        ).fetchall()
        ldb.close()
    except Exception:
        pass

    # Also get bookings from dispatch.db
    db = get_db()
    bookings = db.execute(
        "SELECT * FROM bookings ORDER BY booked_at DESC LIMIT 100"
    ).fetchall()
    db.close()

    lead_rows = ""
    for l in leads:
        try:
            source = l["source"] if "source" in l.keys() else "unknown"
            name = l["name"] if "name" in l.keys() else "Unknown"
            phone = l["phone"] if "phone" in l.keys() else ""
            email = l["email"] if "email" in l.keys() else ""
            status = l["status"] if "status" in l.keys() else "new"
            created = l["created_at"] if "created_at" in l.keys() else ""
            lead_id = l["id"] if "id" in l.keys() else ""
            business = l["business"] if "business" in l.keys() else ""
            lead_rows += f"""<tr>
                <td><span class="badge badge-{'completed' if source=='phone' else 'new'}">{source}</span></td>
                <td>{name}</td>
                <td>{phone}</td>
                <td>{email}</td>
                <td>{business}</td>
                <td><span class="badge badge-{status}">{status}</span></td>
                <td>{str(created)[:16]}</td>
                <td><button class="btn btn-green btn-sm" onclick="convertLead('{lead_id}','{name}','{phone}')">Convert to Job</button></td>
            </tr>"""
        except Exception:
            continue

    booking_rows = ""
    for b in bookings:
        try:
            booking_rows += f"""<tr>
                <td><span class="badge badge-dispatched">booking</span></td>
                <td>{b['customer_name']}</td>
                <td>{b['customer_phone']}</td>
                <td>{b['customer_email'] or ''}</td>
                <td>{b['service_category']}</td>
                <td><span class="badge badge-{b['status']}">{b['status']}</span></td>
                <td>{fmt_ts(b['booked_at'])}</td>
                <td><button class="btn btn-green btn-sm" onclick="convertBooking('{b['id']}')">Convert</button></td>
            </tr>"""
        except Exception:
            continue

    content = f"""
    <h2 class="page-title">Leads</h2>

    <div class="detail-card">
        <h3>Incoming Leads ({len(leads)})</h3>
        <div style="overflow-x:auto">
        <table>
            <tr><th>Source</th><th>Name</th><th>Phone</th><th>Email</th><th>Business</th><th>Status</th><th>When</th><th>Action</th></tr>
            {lead_rows if lead_rows else '<tr><td colspan="8" style="text-align:center;color:var(--gray)">No leads yet</td></tr>'}
        </table>
        </div>
    </div>

    <div class="detail-card">
        <h3>Bookings ({len(bookings)})</h3>
        <div style="overflow-x:auto">
        <table>
            <tr><th>Source</th><th>Name</th><th>Phone</th><th>Email</th><th>Service</th><th>Status</th><th>When</th><th>Action</th></tr>
            {booking_rows if booking_rows else '<tr><td colspan="8" style="text-align:center;color:var(--gray)">No bookings yet</td></tr>'}
        </table>
        </div>
    </div>

    <script>
    function convertLead(id, name, phone) {{
        window.location.href = '/admin/new-job?name='+encodeURIComponent(name)+'&phone='+encodeURIComponent(phone)+'&lead_id='+id;
    }}
    function convertBooking(id) {{
        fetch('/admin/api/lead/'+id+'/convert', {{method:'POST'}}).then(r=>r.json()).then(d=>{{
            if(d.job_id) window.location.href='/admin/job/'+d.job_id;
            else alert(d.error||'Failed');
        }});
    }}
    </script>
    """
    return page_wrap("Leads", content, "leads")


# ── Jobs List / Kanban ───────────────────────────────────────────
@app.get("/admin/jobs", response_class=HTMLResponse)
async def jobs_page(status: str = "", view: str = "kanban"):
    db = get_db()

    where = ""
    if status:
        where = f"WHERE j.status='{status}'"

    jobs = db.execute(
        f"SELECT j.*, c.name as cust_name, c.phone as cust_phone FROM dispatch_jobs j "
        f"LEFT JOIN dispatch_customers c ON j.customer_id=c.customer_id "
        f"{where} ORDER BY j.created_at DESC"
    ).fetchall()
    db.close()

    # Kanban columns
    columns = {"new": [], "dispatched": [], "enroute": [], "in_progress": [], "completed": []}
    for j in jobs:
        s = j["status"]
        if s in columns:
            columns[s].append(j)

    kanban_html = ""
    col_labels = {"new": "New", "dispatched": "Dispatched", "enroute": "En Route", "in_progress": "In Progress", "completed": "Completed"}
    col_colors = {"new": "var(--blue)", "dispatched": "var(--purple)", "enroute": "var(--cyan)", "in_progress": "var(--orange)", "completed": "var(--green)"}

    for col_key, col_label in col_labels.items():
        cards = ""
        for j in columns[col_key][:20]:
            cust = j["cust_name"] or "Unknown"
            cards += f"""<a href="/admin/job/{j['job_id']}" class="kanban-card" style="text-decoration:none;color:inherit">
                <div class="title">{j['title'][:30]}</div>
                <div class="sub">{cust} &middot; {fmt_ts(j['created_at'])}</div>
                {'<div class="sub" style="color:var(--red)">EMERGENCY</div>' if j['priority']=='emergency' else ''}
            </a>"""
        kanban_html += f"""<div class="kanban-col">
            <h4 style="color:{col_colors[col_key]}">{col_label} ({len(columns[col_key])})</h4>
            {cards if cards else '<div style="color:var(--gray);font-size:12px;text-align:center;padding:20px">Empty</div>'}
        </div>"""

    # Table view
    table_rows = ""
    for j in jobs:
        cust = j["cust_name"] or "Unknown"
        phone = j["cust_phone"] or ""
        table_rows += f"""<tr>
            <td><a href="/admin/job/{j['job_id']}" style="color:var(--green);font-weight:600">{j['job_id'][:8]}...</a></td>
            <td>{j['title']}</td>
            <td>{cust}</td>
            <td>{phone}</td>
            <td>{j['address'] or ''}</td>
            <td><span class="badge badge-{j['status']}">{j['status']}</span></td>
            <td><span class="badge badge-{j['priority'] or 'normal'}">{j['priority'] or 'normal'}</span></td>
            <td>{fmt_ts(j['created_at'])}</td>
        </tr>"""

    content = f"""
    <h2 class="page-title">Jobs ({len(jobs)})</h2>

    <div class="filters">
        <a href="/admin/jobs" class="btn btn-sm {'btn-green' if not status else 'btn-outline'}">All</a>
        <a href="/admin/jobs?status=new" class="btn btn-sm {'btn-blue' if status=='new' else 'btn-outline'}">New</a>
        <a href="/admin/jobs?status=dispatched" class="btn btn-sm {'btn-purple' if status=='dispatched' else 'btn-outline'}">Dispatched</a>
        <a href="/admin/jobs?status=enroute" class="btn btn-sm {'btn-outline' if status!='enroute' else 'btn-blue'}">En Route</a>
        <a href="/admin/jobs?status=in_progress" class="btn btn-sm {'btn-orange' if status=='in_progress' else 'btn-outline'}">In Progress</a>
        <a href="/admin/jobs?status=completed" class="btn btn-sm {'btn-green' if status=='completed' else 'btn-outline'}">Completed</a>
        <a href="/admin/jobs?status=cancelled" class="btn btn-sm {'btn-red' if status=='cancelled' else 'btn-outline'}">Cancelled</a>
        <span style="margin-left:auto"></span>
        <a href="/admin/new-job" class="btn btn-green btn-sm">+ New Job</a>
    </div>

    <div class="kanban">
        {kanban_html}
    </div>

    <div class="detail-card">
        <h3>All Jobs</h3>
        <div style="overflow-x:auto">
        <table>
            <tr><th>ID</th><th>Title</th><th>Customer</th><th>Phone</th><th>Address</th><th>Status</th><th>Priority</th><th>Created</th></tr>
            {table_rows if table_rows else '<tr><td colspan="8" style="text-align:center;color:var(--gray)">No jobs</td></tr>'}
        </table>
        </div>
    </div>
    """
    return page_wrap("Jobs", content, "jobs")


# ── Job Detail ───────────────────────────────────────────────────
@app.get("/admin/job/{job_id}", response_class=HTMLResponse)
async def job_detail(job_id: str):
    db = get_db()
    job = db.execute(
        "SELECT j.*, c.name as cust_name, c.phone as cust_phone, c.email as cust_email, c.address as cust_address "
        "FROM dispatch_jobs j LEFT JOIN dispatch_customers c ON j.customer_id=c.customer_id "
        "WHERE j.job_id=?", (job_id,)
    ).fetchone()

    if not job:
        db.close()
        return page_wrap("Not Found", '<h2 class="page-title">Job not found</h2>', "jobs")

    # Notes
    notes = db.execute(
        "SELECT * FROM dispatch_notes WHERE job_id=? ORDER BY created_at DESC", (job_id,)
    ).fetchall()

    # Photos
    photos = db.execute(
        "SELECT * FROM dispatch_photos WHERE job_id=? ORDER BY uploaded_at DESC", (job_id,)
    ).fetchall()

    # Activity log
    logs = db.execute(
        "SELECT * FROM dispatch_log WHERE job_id=? ORDER BY id DESC LIMIT 20", (job_id,)
    ).fetchall()

    # Invoice
    invoice = db.execute(
        "SELECT * FROM dispatch_invoices WHERE job_id=? ORDER BY created_at DESC LIMIT 1", (job_id,)
    ).fetchone()

    db.close()

    status = job["status"]
    priority = job["priority"] or "normal"

    # Status action buttons
    status_buttons = ""
    if status == "new":
        status_buttons = """
            <button class="btn btn-purple btn-sm" onclick="postAction('/admin/api/job/{jid}/status',{status:'dispatched'})">Dispatch</button>
            <button class="btn btn-red btn-sm" onclick="confirmAction('Cancel this job?','/admin/api/job/{jid}/status',{status:'cancelled'})">Cancel</button>
        """.replace("{jid}", job_id)
    elif status == "dispatched":
        status_buttons = """
            <button class="btn btn-blue btn-sm" onclick="postAction('/admin/api/job/{jid}/status',{status:'enroute'})">En Route</button>
            <button class="btn btn-red btn-sm" onclick="confirmAction('Cancel?','/admin/api/job/{jid}/status',{status:'cancelled'})">Cancel</button>
        """.replace("{jid}", job_id)
    elif status == "enroute":
        status_buttons = """
            <button class="btn btn-orange btn-sm" onclick="postAction('/admin/api/job/{jid}/status',{status:'in_progress'})">Start Work</button>
        """.replace("{jid}", job_id)
    elif status == "in_progress":
        status_buttons = """
            <button class="btn btn-green btn-sm" onclick="postAction('/admin/api/job/{jid}/status',{status:'completed'})">Complete</button>
        """.replace("{jid}", job_id)
    elif status == "completed" and not invoice:
        status_buttons = """
            <a href="/admin/new-invoice/{jid}" class="btn btn-orange btn-sm">Create Invoice</a>
        """.replace("{jid}", job_id)

    notes_html = ""
    for n in notes:
        notes_html += f"""<div class="note">
            <div class="meta">{n['author']} &middot; {fmt_ts(n['created_at'])}</div>
            <div class="body">{n['content']}</div>
        </div>"""

    photos_html = ""
    for p in photos:
        photos_html += f"""<div style="display:inline-block;margin:4px">
            <a href="/api/job/{job_id}/photo/{p['photo_id']}" target="_blank" style="color:var(--blue)">{p['filename']}</a>
            <div style="font-size:11px;color:var(--gray)">{p['caption'] or ''} &middot; {fmt_ts(p['uploaded_at'])}</div>
        </div>"""

    log_html = ""
    for l in logs:
        log_html += f"""<div class="timeline-item">
            <div class="timeline-dot"></div>
            <div class="timeline-content">
                <div class="time">{l['created_at']}</div>
                <div class="text"><strong>{l['action']}</strong> {l['details'] or ''}</div>
            </div>
        </div>"""

    invoice_section = ""
    if invoice:
        invoice_section = f"""<div class="detail-card">
            <h3>Invoice</h3>
            <div class="detail-row"><span class="key">Invoice ID</span><span class="val"><a href="/admin/invoice/{invoice['invoice_id']}">{invoice['invoice_id'][:12]}...</a></span></div>
            <div class="detail-row"><span class="key">Total</span><span class="val" style="color:var(--green);font-weight:700">{fmt_money(invoice['total'])}</span></div>
            <div class="detail-row"><span class="key">Status</span><span class="val"><span class="badge badge-{invoice['status']}">{invoice['status']}</span></span></div>
        </div>"""

    content = f"""
    <div style="display:flex;align-items:center;gap:16px;margin-bottom:20px;flex-wrap:wrap">
        <h2 class="page-title" style="margin:0">Job: {job['title']}</h2>
        <span class="badge badge-{status}" style="font-size:14px;padding:6px 16px">{status.upper()}</span>
        <span class="badge badge-{priority}" style="font-size:14px;padding:6px 16px">{priority.upper()}</span>
    </div>

    <div class="actions">
        {status_buttons}
        <a href="/admin/jobs" class="btn btn-outline btn-sm">Back to Jobs</a>
    </div>

    <div style="display:grid;grid-template-columns:1fr 1fr;gap:20px">
        <div class="detail-card">
            <h3>Job Details</h3>
            <div class="detail-row"><span class="key">Job ID</span><span class="val">{job_id}</span></div>
            <div class="detail-row"><span class="key">Title</span><span class="val">{job['title']}</span></div>
            <div class="detail-row"><span class="key">Description</span><span class="val">{job['description'] or '-'}</span></div>
            <div class="detail-row"><span class="key">Address</span><span class="val">{job['address'] or '-'}</span></div>
            <div class="detail-row"><span class="key">Created</span><span class="val">{fmt_ts(job['created_at'])}</span></div>
            <div class="detail-row"><span class="key">Dispatched</span><span class="val">{fmt_ts(job['dispatched_at'])}</span></div>
            <div class="detail-row"><span class="key">Started</span><span class="val">{fmt_ts(job['started_at'])}</span></div>
            <div class="detail-row"><span class="key">Completed</span><span class="val">{fmt_ts(job['completed_at'])}</span></div>
        </div>

        <div class="detail-card">
            <h3>Customer</h3>
            <div class="detail-row"><span class="key">Name</span><span class="val">{job['cust_name'] or 'Unknown'}</span></div>
            <div class="detail-row"><span class="key">Phone</span><span class="val">{job['cust_phone'] or '-'}</span></div>
            <div class="detail-row"><span class="key">Email</span><span class="val">{job['cust_email'] or '-'}</span></div>
            <div class="detail-row"><span class="key">Address</span><span class="val">{job['cust_address'] or job['address'] or '-'}</span></div>
        </div>
    </div>

    {invoice_section}

    <div class="detail-card">
        <h3>Notes ({len(notes)})</h3>
        {notes_html if notes_html else '<div style="color:var(--gray);padding:10px">No notes yet</div>'}
        <form onsubmit="event.preventDefault();addNote()" style="margin-top:16px;display:flex;gap:8px">
            <input type="text" id="noteInput" placeholder="Add a note..." style="flex:1;padding:10px 14px;background:var(--bg3);border:1px solid var(--bg4);border-radius:8px;color:var(--white);font-size:14px">
            <button type="submit" class="btn btn-green btn-sm">Add Note</button>
        </form>
    </div>

    <div class="detail-card">
        <h3>Photos ({len(photos)})</h3>
        {photos_html if photos_html else '<div style="color:var(--gray);padding:10px">No photos uploaded</div>'}
    </div>

    <div class="detail-card">
        <h3>Activity Log</h3>
        <div class="timeline">
            {log_html if log_html else '<div style="color:var(--gray);padding:10px">No activity yet</div>'}
        </div>
    </div>

    <script>
    function addNote() {{
        const note = document.getElementById('noteInput').value;
        if(!note) return;
        fetch('/admin/api/job/{job_id}/note', {{
            method:'POST',
            headers:{{'Content-Type':'application/json'}},
            body:JSON.stringify({{content:note}})
        }}).then(r=>r.json()).then(()=>location.reload());
    }}
    </script>
    """
    return page_wrap(f"Job: {job['title']}", content, "jobs")


# ── New Job Form ─────────────────────────────────────────────────
@app.get("/admin/new-job", response_class=HTMLResponse)
async def new_job_form(name: str = "", phone: str = "", lead_id: str = ""):
    content = f"""
    <h2 class="page-title">Create New Job</h2>
    <div class="detail-card" style="max-width:700px">
        <form method="POST" action="/admin/api/job/create">
            <input type="hidden" name="lead_id" value="{lead_id}">
            <div class="form-row">
                <div class="form-group">
                    <label>Customer Name</label>
                    <input type="text" name="customer_name" value="{name}" required>
                </div>
                <div class="form-group">
                    <label>Phone</label>
                    <input type="text" name="customer_phone" value="{phone}" required>
                </div>
            </div>
            <div class="form-row">
                <div class="form-group">
                    <label>Email</label>
                    <input type="email" name="customer_email">
                </div>
                <div class="form-group">
                    <label>Priority</label>
                    <select name="priority">
                        <option value="normal">Normal</option>
                        <option value="emergency">Emergency</option>
                    </select>
                </div>
            </div>
            <div class="form-group">
                <label>Address</label>
                <input type="text" name="address" required>
            </div>
            <div class="form-group">
                <label>Job Title</label>
                <input type="text" name="title" placeholder="e.g. Car Lockout, Lock Rekey" required>
            </div>
            <div class="form-group">
                <label>Description</label>
                <textarea name="description" placeholder="Job details..."></textarea>
            </div>
            <button type="submit" class="btn btn-green" style="width:100%">Create Job</button>
        </form>
    </div>
    """
    return page_wrap("New Job", content, "new-job")


# ── Invoices List ────────────────────────────────────────────────
@app.get("/admin/invoices", response_class=HTMLResponse)
async def invoices_page():
    db = get_db()
    invoices = db.execute(
        "SELECT i.*, c.name as cust_name, j.title as job_title FROM dispatch_invoices i "
        "LEFT JOIN dispatch_customers c ON i.customer_id=c.customer_id "
        "LEFT JOIN dispatch_jobs j ON i.job_id=j.job_id "
        "ORDER BY i.created_at DESC"
    ).fetchall()

    total_paid = db.execute("SELECT COALESCE(SUM(total),0) t FROM dispatch_invoices WHERE status='paid'").fetchone()["t"]
    total_pending = db.execute("SELECT COALESCE(SUM(total),0) t FROM dispatch_invoices WHERE status IN ('draft','sent')").fetchone()["t"]
    db.close()

    rows = ""
    for inv in invoices:
        inv_id = inv['invoice_id']
        mark_paid_btn = ""
        if inv['status'] in ('draft', 'sent'):
            mark_paid_btn = f'<button class="btn btn-green btn-sm" onclick="postAction(&quot;/admin/api/invoice/{inv_id}/status&quot;,{{status:&quot;paid&quot;}})">Mark Paid</button>'
        rows += f"""<tr>
            <td><a href="/admin/invoice/{inv_id}" style="color:var(--green);font-weight:600">{inv_id[:12]}...</a></td>
            <td>{inv['job_title'] or '-'}</td>
            <td>{inv['cust_name'] or 'Unknown'}</td>
            <td style="font-weight:600">{fmt_money(inv['total'])}</td>
            <td><span class="badge badge-{inv['status']}">{inv['status']}</span></td>
            <td>{fmt_ts(inv['created_at'])}</td>
            <td>{fmt_ts(inv['paid_at']) if inv['paid_at'] else '-'}</td>
            <td>
                <a href="/invoice/{inv_id}" target="_blank" class="btn btn-outline btn-sm">View</a>
                {mark_paid_btn}
            </td>
        </tr>"""

    content = f"""
    <h2 class="page-title">Invoices</h2>
    <div class="stats-row">
        <div class="stat-card"><div class="num" style="color:var(--green)">{fmt_money(total_paid)}</div><div class="label">Total Paid</div></div>
        <div class="stat-card"><div class="num" style="color:var(--orange)">{fmt_money(total_pending)}</div><div class="label">Pending</div></div>
        <div class="stat-card"><div class="num" style="color:var(--white)">{len(invoices)}</div><div class="label">Total Invoices</div></div>
    </div>

    <div class="detail-card">
        <h3>All Invoices</h3>
        <div style="overflow-x:auto">
        <table>
            <tr><th>ID</th><th>Job</th><th>Customer</th><th>Total</th><th>Status</th><th>Created</th><th>Paid</th><th>Actions</th></tr>
            {rows if rows else '<tr><td colspan="8" style="text-align:center;color:var(--gray)">No invoices yet</td></tr>'}
        </table>
        </div>
    </div>
    """
    return page_wrap("Invoices", content, "invoices")


# ── Invoice Detail (Admin) ───────────────────────────────────────
@app.get("/admin/invoice/{invoice_id}", response_class=HTMLResponse)
async def invoice_detail(invoice_id: str):
    db = get_db()
    inv = db.execute(
        "SELECT i.*, c.name as cust_name, c.phone as cust_phone, c.email as cust_email, c.address as cust_address, j.title as job_title "
        "FROM dispatch_invoices i "
        "LEFT JOIN dispatch_customers c ON i.customer_id=c.customer_id "
        "LEFT JOIN dispatch_jobs j ON i.job_id=j.job_id "
        "WHERE i.invoice_id=?", (invoice_id,)
    ).fetchone()
    db.close()

    if not inv:
        return page_wrap("Not Found", '<h2 class="page-title">Invoice not found</h2>', "invoices")

    line_items = []
    try:
        line_items = json.loads(inv["line_items"] or "[]")
    except Exception:
        pass

    items_html = ""
    for item in line_items:
        desc = item.get("description", "")
        qty = item.get("qty", 1)
        price = item.get("price", 0)
        total = qty * price
        items_html += f"<tr><td>{desc}</td><td>{qty}</td><td>{fmt_money(price)}</td><td style='font-weight:600'>{fmt_money(total)}</td></tr>"

    actions = ""
    if inv["status"] == "draft":
        actions = f"""
            <button class="btn btn-blue btn-sm" onclick="postAction('/admin/api/invoice/{invoice_id}/status',{{status:'sent'}})">Mark Sent</button>
            <button class="btn btn-green btn-sm" onclick="postAction('/admin/api/invoice/{invoice_id}/status',{{status:'paid'}})">Mark Paid</button>
        """
    elif inv["status"] == "sent":
        actions = f"""
            <button class="btn btn-green btn-sm" onclick="postAction('/admin/api/invoice/{invoice_id}/status',{{status:'paid'}})">Mark Paid</button>
        """

    content = f"""
    <div style="display:flex;align-items:center;gap:16px;margin-bottom:20px;flex-wrap:wrap">
        <h2 class="page-title" style="margin:0">Invoice: {invoice_id[:12]}...</h2>
        <span class="badge badge-{inv['status']}" style="font-size:14px;padding:6px 16px">{inv['status'].upper()}</span>
    </div>

    <div class="actions">
        {actions}
        <a href="/invoice/{invoice_id}" target="_blank" class="btn btn-outline btn-sm">Customer View</a>
        <a href="/admin/invoices" class="btn btn-outline btn-sm">Back</a>
    </div>

    <div style="display:grid;grid-template-columns:1fr 1fr;gap:20px">
        <div class="detail-card">
            <h3>Invoice Info</h3>
            <div class="detail-row"><span class="key">Invoice ID</span><span class="val">{invoice_id}</span></div>
            <div class="detail-row"><span class="key">Job</span><span class="val">{inv['job_title'] or '-'}</span></div>
            <div class="detail-row"><span class="key">Created</span><span class="val">{fmt_ts(inv['created_at'])}</span></div>
            <div class="detail-row"><span class="key">Paid At</span><span class="val">{fmt_ts(inv['paid_at'])}</span></div>
        </div>
        <div class="detail-card">
            <h3>Customer</h3>
            <div class="detail-row"><span class="key">Name</span><span class="val">{inv['cust_name'] or 'Unknown'}</span></div>
            <div class="detail-row"><span class="key">Phone</span><span class="val">{inv['cust_phone'] or '-'}</span></div>
            <div class="detail-row"><span class="key">Email</span><span class="val">{inv['cust_email'] or '-'}</span></div>
            <div class="detail-row"><span class="key">Address</span><span class="val">{inv['cust_address'] or '-'}</span></div>
        </div>
    </div>

    <div class="detail-card">
        <h3>Line Items</h3>
        <table>
            <tr><th>Description</th><th>Qty</th><th>Price</th><th>Total</th></tr>
            {items_html if items_html else '<tr><td colspan="4" style="text-align:center;color:var(--gray)">No line items</td></tr>'}
        </table>
        <div class="invoice-totals">
            <div class="row"><span>Subtotal:</span><span>{fmt_money(inv['subtotal'])}</span></div>
            <div class="row"><span>Tax (7.5%):</span><span>{fmt_money(inv['tax'])}</span></div>
            <div class="row total"><span>Total:</span><span style="color:var(--green);font-size:24px;font-weight:700">{fmt_money(inv['total'])}</span></div>
        </div>
    </div>
    """
    return page_wrap(f"Invoice {invoice_id[:12]}", content, "invoices")


# ── New Invoice Form ─────────────────────────────────────────────
@app.get("/admin/new-invoice/{job_id}", response_class=HTMLResponse)
async def new_invoice_form(job_id: str):
    db = get_db()
    job = db.execute(
        "SELECT j.*, c.name as cust_name FROM dispatch_jobs j "
        "LEFT JOIN dispatch_customers c ON j.customer_id=c.customer_id "
        "WHERE j.job_id=?", (job_id,)
    ).fetchone()
    db.close()

    if not job:
        return page_wrap("Not Found", '<h2 class="page-title">Job not found</h2>', "invoices")

    content = f"""
    <h2 class="page-title">Create Invoice for: {job['title']}</h2>
    <div class="detail-card" style="max-width:800px">
        <div style="margin-bottom:20px;padding:12px;background:var(--bg3);border-radius:8px">
            <strong>Job:</strong> {job['title']} &middot; <strong>Customer:</strong> {job['cust_name'] or 'Unknown'}
        </div>

        <div id="lineItems">
            <div class="invoice-line" data-idx="0">
                <input type="text" placeholder="Service description" class="line-desc">
                <input type="number" placeholder="Qty" value="1" min="1" class="line-qty">
                <input type="number" placeholder="Price" step="0.01" class="line-price">
                <span class="line-total" style="font-weight:600;color:var(--green)">$0.00</span>
                <button class="btn btn-red btn-sm" onclick="removeLine(this)" style="padding:5px 8px">X</button>
            </div>
        </div>

        <button class="btn btn-outline btn-sm" onclick="addLine()" style="margin:12px 0">+ Add Line Item</button>

        <div class="invoice-totals" id="totals">
            <div class="row"><span>Subtotal:</span><span id="subtotal">$0.00</span></div>
            <div class="row"><span>Tax (7.5%):</span><span id="tax">$0.00</span></div>
            <div class="row total"><span>Total:</span><span id="total" style="color:var(--green);font-size:24px;font-weight:700">$0.00</span></div>
        </div>

        <button class="btn btn-green" style="width:100%;margin-top:20px" onclick="submitInvoice()">Create Invoice</button>
    </div>

    <script>
    let lineIdx = 1;

    function addLine() {{
        const div = document.createElement('div');
        div.className = 'invoice-line';
        div.dataset.idx = lineIdx++;
        div.innerHTML = '<input type="text" placeholder="Service description" class="line-desc"><input type="number" placeholder="Qty" value="1" min="1" class="line-qty"><input type="number" placeholder="Price" step="0.01" class="line-price"><span class="line-total" style="font-weight:600;color:var(--green)">$0.00</span><button class="btn btn-red btn-sm" onclick="removeLine(this)" style="padding:5px 8px">X</button>';
        document.getElementById('lineItems').appendChild(div);
        attachListeners(div);
    }}

    function removeLine(btn) {{
        btn.closest('.invoice-line').remove();
        recalc();
    }}

    function attachListeners(el) {{
        el.querySelectorAll('.line-qty,.line-price').forEach(inp => inp.addEventListener('input', recalc));
    }}

    function recalc() {{
        let subtotal = 0;
        document.querySelectorAll('.invoice-line').forEach(line => {{
            const qty = parseFloat(line.querySelector('.line-qty').value) || 0;
            const price = parseFloat(line.querySelector('.line-price').value) || 0;
            const total = qty * price;
            subtotal += total;
            line.querySelector('.line-total').textContent = '$' + total.toFixed(2);
        }});
        const tax = subtotal * 0.075;
        document.getElementById('subtotal').textContent = '$' + subtotal.toFixed(2);
        document.getElementById('tax').textContent = '$' + tax.toFixed(2);
        document.getElementById('total').textContent = '$' + (subtotal + tax).toFixed(2);
    }}

    document.querySelectorAll('.invoice-line').forEach(attachListeners);

    function submitInvoice() {{
        const items = [];
        document.querySelectorAll('.invoice-line').forEach(line => {{
            const desc = line.querySelector('.line-desc').value;
            const qty = parseFloat(line.querySelector('.line-qty').value) || 0;
            const price = parseFloat(line.querySelector('.line-price').value) || 0;
            if(desc && price > 0) items.push({{description:desc, qty:qty, price:price}});
        }});
        if(items.length === 0) {{ alert('Add at least one line item'); return; }}
        fetch('/admin/api/invoice/create', {{
            method:'POST',
            headers:{{'Content-Type':'application/json'}},
            body:JSON.stringify({{job_id:'{job_id}', line_items:items}})
        }}).then(r=>r.json()).then(d=>{{
            if(d.invoice_id) window.location.href='/admin/invoice/'+d.invoice_id;
            else alert(d.error||'Failed');
        }});
    }}
    </script>
    """
    return page_wrap("Create Invoice", content, "invoices")


# ── Customers ────────────────────────────────────────────────────
@app.get("/admin/customers", response_class=HTMLResponse)
async def customers_page():
    db = get_db()
    customers = db.execute(
        "SELECT c.*, (SELECT count(*) FROM dispatch_jobs WHERE customer_id=c.customer_id) as job_count, "
        "(SELECT COALESCE(SUM(total),0) FROM dispatch_invoices WHERE customer_id=c.customer_id AND status='paid') as total_paid "
        "FROM dispatch_customers c ORDER BY c.created_at DESC"
    ).fetchall()
    db.close()

    rows = ""
    for c in customers:
        rows += f"""<tr>
            <td style="font-weight:600">{c['name']}</td>
            <td>{c['phone'] or '-'}</td>
            <td>{c['email'] or '-'}</td>
            <td>{c['address'] or '-'}</td>
            <td>{c['job_count']}</td>
            <td style="color:var(--green);font-weight:600">{fmt_money(c['total_paid'])}</td>
            <td>{c['notes'] or '-'}</td>
            <td>{fmt_ts(c['created_at'])}</td>
        </tr>"""

    content = f"""
    <h2 class="page-title">Customers ({len(customers)})</h2>
    <div class="detail-card">
        <div style="overflow-x:auto">
        <table>
            <tr><th>Name</th><th>Phone</th><th>Email</th><th>Address</th><th>Jobs</th><th>Paid</th><th>Notes</th><th>Since</th></tr>
            {rows if rows else '<tr><td colspan="8" style="text-align:center;color:var(--gray)">No customers yet</td></tr>'}
        </table>
        </div>
    </div>
    """
    return page_wrap("Customers", content, "customers")


def _payment_section(inv, invoice_id):
    """Generate payment/tip section for customer invoice."""
    if inv["status"] == "paid":
        return '<div style="text-align:center;margin:30px 0"><div style="background:#10b981;color:#fff;display:inline-block;padding:12px 32px;border-radius:8px;font-weight:700;font-size:18px">PAID — Thank you!</div></div>'

    total_val = float(inv["total"])
    total_fmt = fmt_money(inv["total"])
    return f"""
    <div style="text-align:center;margin:30px 0">
        <p style="font-size:16px;color:#374151;margin-bottom:16px">Add a tip?</p>
        <div style="display:flex;gap:12px;justify-content:center;flex-wrap:wrap;margin-bottom:20px">
            <button onclick="selectTip(0)" class="tip-btn" id="tip-0" style="padding:12px 24px;border:2px solid #e5e7eb;border-radius:8px;background:#fff;cursor:pointer;font-size:15px;font-weight:600;transition:.2s">No Tip</button>
            <button onclick="selectTip(15)" class="tip-btn" id="tip-15" style="padding:12px 24px;border:2px solid #e5e7eb;border-radius:8px;background:#fff;cursor:pointer;font-size:15px;font-weight:600;transition:.2s">15%</button>
            <button onclick="selectTip(20)" class="tip-btn" id="tip-20" style="padding:12px 24px;border:2px solid #6366f1;border-radius:8px;background:#eef2ff;cursor:pointer;font-size:15px;font-weight:600;color:#4f46e5;transition:.2s">20%</button>
            <button onclick="selectTip(25)" class="tip-btn" id="tip-25" style="padding:12px 24px;border:2px solid #e5e7eb;border-radius:8px;background:#fff;cursor:pointer;font-size:15px;font-weight:600;transition:.2s">25%</button>
        </div>
        <div style="font-size:14px;color:#6b7280;margin-bottom:16px" id="tipSummary">Total: {total_fmt}</div>
        <button onclick="payNow()" style="display:inline-block;background:#6366f1;color:#fff;padding:16px 48px;border:none;border-radius:8px;font-weight:700;font-size:18px;cursor:pointer;transition:.2s">Pay Now</button>
    </div>
    <script>
    let selectedTip = 20;
    const invoiceTotal = {total_val};
    selectTip(20);
    function selectTip(pct) {{
        selectedTip = pct;
        document.querySelectorAll(".tip-btn").forEach(function(b) {{
            b.style.borderColor = "#e5e7eb";
            b.style.background = "#fff";
            b.style.color = "#1a1a2e";
        }});
        var btn = document.getElementById("tip-" + pct);
        if(btn) {{
            btn.style.borderColor = "#6366f1";
            btn.style.background = "#eef2ff";
            btn.style.color = "#4f46e5";
        }}
        var tip = invoiceTotal * pct / 100;
        var total = invoiceTotal + tip;
        var summary = "Total: $" + total.toFixed(2);
        if(pct > 0) summary += " (includes $" + tip.toFixed(2) + " tip)";
        document.getElementById("tipSummary").textContent = summary;
    }}
    function payNow() {{
        fetch("/api/pay/{invoice_id}", {{
            method:"POST",
            headers:{{"Content-Type":"application/json"}},
            body:JSON.stringify({{tip_percent:selectedTip}})
        }}).then(function(r){{return r.json()}}).then(function(d){{
            if(d.url) window.location.href = d.url;
            else alert(d.error || "Payment unavailable");
        }});
    }}
    </script>
    """


# ── Customer-facing Invoice ──────────────────────────────────────
@app.get("/invoice/{invoice_id}", response_class=HTMLResponse)
async def public_invoice(invoice_id: str):
    db = get_db()
    inv = db.execute(
        "SELECT i.*, c.name as cust_name, c.phone as cust_phone, c.email as cust_email, c.address as cust_address, j.title as job_title, j.description as job_desc "
        "FROM dispatch_invoices i "
        "LEFT JOIN dispatch_customers c ON i.customer_id=c.customer_id "
        "LEFT JOIN dispatch_jobs j ON i.job_id=j.job_id "
        "WHERE i.invoice_id=?", (invoice_id,)
    ).fetchone()
    db.close()

    if not inv:
        return HTMLResponse("<h1>Invoice not found</h1>", status_code=404)

    line_items = []
    try:
        line_items = json.loads(inv["line_items"] or "[]")
    except Exception:
        pass

    items_html = ""
    for item in line_items:
        desc = item.get("description", "")
        qty = item.get("qty", 1)
        price = item.get("price", 0)
        total = qty * price
        items_html += f"<tr><td>{desc}</td><td style='text-align:center'>{qty}</td><td style='text-align:right'>{fmt_money(price)}</td><td style='text-align:right;font-weight:600'>{fmt_money(total)}</td></tr>"

    paid_badge = ""
    if inv["status"] == "paid":
        paid_badge = '<div style="background:#10b981;color:#fff;display:inline-block;padding:8px 24px;border-radius:8px;font-weight:700;font-size:18px;margin-bottom:20px">PAID</div>'
    elif inv["status"] == "overdue":
        paid_badge = '<div style="background:#ef4444;color:#fff;display:inline-block;padding:8px 24px;border-radius:8px;font-weight:700;font-size:18px;margin-bottom:20px">OVERDUE</div>'

    return HTMLResponse(f"""<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Invoice — Hive Dynamics</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:'Segoe UI',system-ui,sans-serif;background:#f3f4f6;color:#1a1a2e;padding:20px}}
.invoice-public{{background:#fff;max-width:800px;margin:0 auto;padding:40px;border-radius:12px;box-shadow:0 4px 20px rgba(0,0,0,.08)}}
h1{{font-size:28px;margin-bottom:4px}}
.company{{color:#6366f1;font-size:14px;margin-bottom:24px;font-weight:600}}
.meta-grid{{display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:30px}}
.meta-box{{padding:16px;background:#f9fafb;border-radius:8px}}
.meta-box h4{{font-size:12px;text-transform:uppercase;letter-spacing:1px;color:#6b7280;margin-bottom:8px}}
.meta-box p{{font-size:14px;margin:2px 0}}
table{{width:100%;border-collapse:collapse;margin:20px 0}}
th{{background:#f3f4f6;padding:12px 16px;text-align:left;font-size:12px;text-transform:uppercase;letter-spacing:1px;color:#6b7280;border-bottom:2px solid #e5e7eb}}
td{{padding:12px 16px;border-bottom:1px solid #f3f4f6;font-size:14px}}
.totals{{text-align:right;margin-top:20px;padding-top:16px;border-top:2px solid #e5e7eb}}
.totals .row{{display:flex;justify-content:flex-end;gap:24px;padding:6px 0;font-size:15px;color:#374151}}
.totals .total-row{{font-size:24px;font-weight:700;color:#1a1a2e;border-top:2px solid #1a1a2e;padding-top:12px;margin-top:8px}}
.footer{{text-align:center;margin-top:30px;padding-top:20px;border-top:1px solid #e5e7eb;color:#6b7280;font-size:13px}}
</style>
</head><body>
<div class="invoice-public">
    <h1>Invoice</h1>
    <div class="company">Hive Dynamics AI &middot; Pensacola, FL &middot; (850) 801-6662</div>
    {paid_badge}

    <div class="meta-grid">
        <div class="meta-box">
            <h4>Bill To</h4>
            <p><strong>{inv['cust_name'] or 'Customer'}</strong></p>
            <p>{inv['cust_address'] or ''}</p>
            <p>{inv['cust_phone'] or ''}</p>
            <p>{inv['cust_email'] or ''}</p>
        </div>
        <div class="meta-box">
            <h4>Invoice Details</h4>
            <p><strong>Invoice #:</strong> {invoice_id[:12].upper()}</p>
            <p><strong>Date:</strong> {fmt_ts(inv['created_at'])}</p>
            <p><strong>Job:</strong> {inv['job_title'] or '-'}</p>
            <p><strong>Status:</strong> {inv['status'].upper()}</p>
        </div>
    </div>

    <table>
        <tr><th>Description</th><th style="text-align:center">Qty</th><th style="text-align:right">Price</th><th style="text-align:right">Total</th></tr>
        {items_html}
    </table>

    <div class="totals">
        <div class="row"><span>Subtotal:</span><span>{fmt_money(inv['subtotal'])}</span></div>
        <div class="row"><span>Tax (7.5%):</span><span>{fmt_money(inv['tax'])}</span></div>
        <div class="row total-row"><span>Total Due:</span><span>{fmt_money(inv['total'])}</span></div>
    </div>

    {_payment_section(inv, invoice_id)}

    <div class="footer">
        <p>Thank you for choosing Hive Dynamics AI</p>
        <p>hivecore.app &middot; (850) 801-6662</p>
    </div>
</div>
</body></html>""")


# ═══════════════════════════════════════════════════════════════════
# API ENDPOINTS
# ═══════════════════════════════════════════════════════════════════

@app.post("/admin/api/job/create")
async def api_create_job(request: Request):
    # Handle both form and JSON
    content_type = request.headers.get("content-type", "")
    if "json" in content_type:
        data = await request.json()
    else:
        form = await request.form()
        data = dict(form)

    name = data.get("customer_name", "").strip()
    phone = data.get("customer_phone", "").strip()
    email = data.get("customer_email", "").strip()
    address = data.get("address", "").strip()
    title = data.get("title", "").strip()
    description = data.get("description", "").strip()
    priority = data.get("priority", "normal")

    if not name or not title:
        if "json" in content_type:
            return JSONResponse({"error": "Name and title required"}, status_code=400)
        return RedirectResponse("/admin/new-job", status_code=303)

    db = get_db()

    # Create or find customer
    customer_id = gen_id()
    existing = db.execute("SELECT customer_id FROM dispatch_customers WHERE phone=? AND phone!=''", (phone,)).fetchone()
    if existing:
        customer_id = existing["customer_id"]
        db.execute("UPDATE dispatch_customers SET name=?, address=?, email=? WHERE customer_id=?",
                   (name, address, email, customer_id))
    else:
        db.execute("INSERT INTO dispatch_customers (customer_id, name, address, phone, email, created_at) VALUES (?,?,?,?,?,?)",
                   (customer_id, name, address, phone, email, now_ts()))

    # Create job
    job_id = gen_id()
    db.execute(
        "INSERT INTO dispatch_jobs (job_id, customer_id, status, priority, title, description, address, created_at) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (job_id, customer_id, "new", priority, title, description, address, now_ts())
    )
    log_action(db, job_id, "job_created", f"New job: {title} for {name}")
    db.commit()
    db.close()

    if "json" in content_type:
        return JSONResponse({"job_id": job_id, "status": "created"})
    return RedirectResponse(f"/admin/job/{job_id}", status_code=303)


@app.post("/admin/api/job/{job_id}/status")
async def api_update_job_status(job_id: str, request: Request):
    data = await request.json()
    new_status = data.get("status", "")

    valid = {"new", "dispatched", "enroute", "in_progress", "completed", "cancelled"}
    if new_status not in valid:
        return JSONResponse({"error": f"Invalid status. Use: {valid}"}, status_code=400)

    db = get_db()
    job = db.execute("SELECT * FROM dispatch_jobs WHERE job_id=?", (job_id,)).fetchone()
    if not job:
        db.close()
        return JSONResponse({"error": "Job not found"}, status_code=404)

    updates = {"status": new_status}
    if new_status == "dispatched":
        updates["dispatched_at"] = now_ts()
    elif new_status == "enroute":
        pass
    elif new_status == "in_progress":
        updates["started_at"] = now_ts()
    elif new_status == "completed":
        updates["completed_at"] = now_ts()

    set_clause = ", ".join(f"{k}=?" for k in updates)
    values = list(updates.values()) + [job_id]
    db.execute(f"UPDATE dispatch_jobs SET {set_clause} WHERE job_id=?", values)
    log_action(db, job_id, "status_changed", f"{job['status']} -> {new_status}")
    db.commit()
    db.close()

    return JSONResponse({"job_id": job_id, "status": new_status})


@app.post("/admin/api/job/{job_id}/note")
async def api_add_note(job_id: str, request: Request):
    data = await request.json()
    content = data.get("content", "").strip()
    author = data.get("author", "Chris")

    if not content:
        return JSONResponse({"error": "Note content required"}, status_code=400)

    db = get_db()
    note_id = gen_id()
    db.execute(
        "INSERT INTO dispatch_notes (note_id, job_id, author, content, created_at) VALUES (?,?,?,?,?)",
        (note_id, job_id, author, content, now_ts())
    )

    # Also append to job notes field
    job = db.execute("SELECT notes FROM dispatch_jobs WHERE job_id=?", (job_id,)).fetchone()
    existing_notes = job["notes"] or "" if job else ""
    timestamp = datetime.now().strftime("%m/%d %I:%M %p")
    new_notes = f"{existing_notes}\n[{timestamp}] {author}: {content}".strip()
    db.execute("UPDATE dispatch_jobs SET notes=? WHERE job_id=?", (new_notes, job_id))

    log_action(db, job_id, "note_added", f"{author}: {content[:50]}")
    db.commit()
    db.close()

    return JSONResponse({"note_id": note_id, "status": "added"})


@app.post("/admin/api/lead/{lead_id}/convert")
async def api_convert_lead(lead_id: str):
    # Try to find in bookings first
    db = get_db()
    booking = db.execute("SELECT * FROM bookings WHERE id=?", (lead_id,)).fetchone()

    if booking:
        # Create customer
        customer_id = gen_id()
        db.execute(
            "INSERT OR IGNORE INTO dispatch_customers (customer_id, name, address, phone, email, created_at) VALUES (?,?,?,?,?,?)",
            (customer_id, booking["customer_name"], booking["address"], booking["customer_phone"],
             booking["customer_email"] or "", now_ts())
        )

        # Create job
        job_id = gen_id()
        db.execute(
            "INSERT INTO dispatch_jobs (job_id, customer_id, status, priority, title, description, address, created_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (job_id, customer_id, "new",
             "emergency" if booking["urgency"] == "emergency" else "normal",
             booking["service_type"] or booking["service_category"] or "Service Call",
             booking["description"] or "",
             booking["address"],
             now_ts())
        )
        log_action(db, job_id, "converted_from_booking", f"Booking {lead_id}")
        db.commit()
        db.close()
        return JSONResponse({"job_id": job_id, "status": "converted"})

    # Try leads.db
    try:
        ldb = get_leads_db()
        lead = ldb.execute("SELECT * FROM leads WHERE id=?", (lead_id,)).fetchone()
        ldb.close()

        if lead:
            name = lead["name"] if "name" in lead.keys() else "Unknown"
            phone = lead["phone"] if "phone" in lead.keys() else ""
            email = lead["email"] if "email" in lead.keys() else ""

            customer_id = gen_id()
            db.execute(
                "INSERT OR IGNORE INTO dispatch_customers (customer_id, name, phone, email, created_at) VALUES (?,?,?,?,?)",
                (customer_id, name, phone, email, now_ts())
            )

            job_id = gen_id()
            db.execute(
                "INSERT INTO dispatch_jobs (job_id, customer_id, status, priority, title, address, created_at) "
                "VALUES (?,?,?,?,?,?,?)",
                (job_id, customer_id, "new", "normal", "Service Call", "", now_ts())
            )
            log_action(db, job_id, "converted_from_lead", f"Lead {lead_id}")
            db.commit()
            db.close()
            return JSONResponse({"job_id": job_id, "status": "converted"})
    except Exception:
        pass

    db.close()
    return JSONResponse({"error": "Lead not found"}, status_code=404)


@app.post("/admin/api/invoice/create")
async def api_create_invoice(request: Request):
    data = await request.json()
    job_id = data.get("job_id", "")
    line_items = data.get("line_items", [])

    if not job_id or not line_items:
        return JSONResponse({"error": "job_id and line_items required"}, status_code=400)

    db = get_db()
    job = db.execute(
        "SELECT j.*, c.customer_id FROM dispatch_jobs j "
        "LEFT JOIN dispatch_customers c ON j.customer_id=c.customer_id WHERE j.job_id=?",
        (job_id,)
    ).fetchone()

    if not job:
        db.close()
        return JSONResponse({"error": "Job not found"}, status_code=404)

    subtotal = sum(item.get("qty", 1) * item.get("price", 0) for item in line_items)
    tax = round(subtotal * 0.075, 2)
    total = round(subtotal + tax, 2)

    invoice_id = gen_id()
    db.execute(
        "INSERT INTO dispatch_invoices (invoice_id, job_id, customer_id, technician_id, line_items, subtotal, tax, total, status, created_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?)",
        (invoice_id, job_id, job["customer_id"], job["technician_id"] or "",
         json.dumps(line_items), subtotal, tax, total, "draft", now_ts())
    )
    log_action(db, job_id, "invoice_created", f"Invoice {invoice_id[:12]} for {fmt_money(total)}")
    db.commit()
    db.close()

    return JSONResponse({"invoice_id": invoice_id, "total": total, "status": "draft"})


@app.post("/admin/api/invoice/{invoice_id}/status")
async def api_update_invoice_status(invoice_id: str, request: Request):
    data = await request.json()
    new_status = data.get("status", "")

    valid = {"draft", "sent", "paid", "overdue"}
    if new_status not in valid:
        return JSONResponse({"error": f"Invalid status. Use: {valid}"}, status_code=400)

    db = get_db()
    inv = db.execute("SELECT * FROM dispatch_invoices WHERE invoice_id=?", (invoice_id,)).fetchone()
    if not inv:
        db.close()
        return JSONResponse({"error": "Invoice not found"}, status_code=404)

    if new_status == "paid":
        db.execute("UPDATE dispatch_invoices SET status=?, paid_at=? WHERE invoice_id=?",
                   (new_status, now_ts(), invoice_id))
    else:
        db.execute("UPDATE dispatch_invoices SET status=? WHERE invoice_id=?", (new_status, invoice_id))

    log_action(db, inv["job_id"], "invoice_status_changed", f"Invoice {invoice_id[:12]}: {inv['status']} -> {new_status}")
    db.commit()
    db.close()

    return JSONResponse({"invoice_id": invoice_id, "status": new_status})


@app.get("/admin/api/stats")
async def api_stats():
    db = get_db()
    today_start = datetime.now().replace(hour=0, minute=0, second=0).timestamp()

    stats = {
        "total_jobs": db.execute("SELECT count(*) c FROM dispatch_jobs").fetchone()["c"],
        "new_jobs": db.execute("SELECT count(*) c FROM dispatch_jobs WHERE status='new'").fetchone()["c"],
        "active_jobs": db.execute("SELECT count(*) c FROM dispatch_jobs WHERE status IN ('dispatched','enroute','in_progress')").fetchone()["c"],
        "completed_today": db.execute("SELECT count(*) c FROM dispatch_jobs WHERE status='completed' AND completed_at>=?", (today_start,)).fetchone()["c"],
        "revenue_today": db.execute("SELECT COALESCE(SUM(total),0) t FROM dispatch_invoices WHERE status='paid' AND paid_at>=?", (today_start,)).fetchone()["t"],
        "total_revenue": db.execute("SELECT COALESCE(SUM(total),0) t FROM dispatch_invoices WHERE status='paid'").fetchone()["t"],
        "pending_invoices": db.execute("SELECT count(*) c FROM dispatch_invoices WHERE status IN ('draft','sent')").fetchone()["c"],
        "total_customers": db.execute("SELECT count(*) c FROM dispatch_customers").fetchone()["c"],
    }
    db.close()
    return stats


# ── Webhook for external systems to create jobs ──────────────────
@app.post("/api/dispatch/new-job")
async def webhook_new_job(request: Request):
    """External webhook — phone system can call this to create dispatch jobs."""
    data = await request.json()
    name = data.get("customer_name", "").strip()
    phone = data.get("customer_phone", "").strip()
    address = data.get("address", "").strip()
    title = data.get("title", "Service Call").strip()
    description = data.get("description", "").strip()
    priority = data.get("priority", "normal")
    business = data.get("business", "")

    db = get_db()

    customer_id = gen_id()
    existing = db.execute("SELECT customer_id FROM dispatch_customers WHERE phone=? AND phone!=''", (phone,)).fetchone()
    if existing:
        customer_id = existing["customer_id"]
    else:
        db.execute("INSERT INTO dispatch_customers (customer_id, name, address, phone, created_at) VALUES (?,?,?,?,?)",
                   (customer_id, name, address, phone, now_ts()))

    job_id = gen_id()
    db.execute(
        "INSERT INTO dispatch_jobs (job_id, customer_id, status, priority, title, description, address, created_at) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (job_id, customer_id, "new", priority, title, description, address, now_ts())
    )
    log_action(db, job_id, "job_created_webhook", f"{title} for {name} via {business}")
    db.commit()
    db.close()

    return JSONResponse({"job_id": job_id, "status": "created"})


# ── Stripe Payment ───────────────────────────────────────────────
@app.post("/api/pay/{invoice_id}")
async def create_payment(invoice_id: str, request: Request):
    """Create Stripe checkout session for invoice payment + optional tip."""
    if not STRIPE_AVAILABLE:
        return JSONResponse({"error": "Stripe not configured"}, status_code=500)

    data = await request.json()
    tip_percent = data.get("tip_percent", 0)

    db = get_db()
    inv = db.execute(
        "SELECT i.*, c.name as cust_name, c.email as cust_email, j.title as job_title "
        "FROM dispatch_invoices i "
        "LEFT JOIN dispatch_customers c ON i.customer_id=c.customer_id "
        "LEFT JOIN dispatch_jobs j ON i.job_id=j.job_id "
        "WHERE i.invoice_id=?", (invoice_id,)
    ).fetchone()
    db.close()

    if not inv:
        return JSONResponse({"error": "Invoice not found"}, status_code=404)

    if inv["status"] == "paid":
        return JSONResponse({"error": "Already paid"}, status_code=400)

    invoice_total = float(inv["total"])
    tip_amount = round(invoice_total * tip_percent / 100, 2)
    charge_total = round(invoice_total + tip_amount, 2)

    line_items = [
        {
            "price_data": {
                "currency": "usd",
                "product_data": {"name": f"Invoice: {inv['job_title'] or 'Service'}"},
                "unit_amount": int(invoice_total * 100),
            },
            "quantity": 1,
        }
    ]

    if tip_amount > 0:
        line_items.append({
            "price_data": {
                "currency": "usd",
                "product_data": {"name": f"Tip ({tip_percent}%)"},
                "unit_amount": int(tip_amount * 100),
            },
            "quantity": 1,
        })

    try:
        host = request.headers.get("host", "hivecore.app")
        scheme = request.headers.get("x-forwarded-proto", "https")
        base_url = f"{scheme}://{host}"

        session = stripe.checkout.Session.create(
            payment_method_types=["card"],
            line_items=line_items,
            mode="payment",
            success_url=f"{base_url}/invoice/{invoice_id}?paid=1",
            cancel_url=f"{base_url}/invoice/{invoice_id}",
            customer_email=inv["cust_email"] or None,
            metadata={"invoice_id": invoice_id, "tip_percent": str(tip_percent)},
        )

        # Save stripe link
        sdb = get_db()
        sdb.execute("UPDATE dispatch_invoices SET stripe_link=? WHERE invoice_id=?",
                     (session.url, invoice_id))
        sdb.commit()
        sdb.close()

        return JSONResponse({"url": session.url, "session_id": session.id})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/stripe-webhook")
async def stripe_webhook(request: Request):
    """Handle Stripe payment confirmation."""
    payload = await request.body()

    try:
        event = json.loads(payload)
    except Exception:
        return JSONResponse({"error": "Bad payload"}, status_code=400)

    if event.get("type") == "checkout.session.completed":
        session = event["data"]["object"]
        invoice_id = session.get("metadata", {}).get("invoice_id")

        if invoice_id:
            db = get_db()
            db.execute("UPDATE dispatch_invoices SET status='paid', paid_at=? WHERE invoice_id=?",
                       (now_ts(), invoice_id))
            log_action(db, None, "payment_received", f"Invoice {invoice_id[:12]} paid via Stripe")
            db.commit()
            db.close()

    return JSONResponse({"received": True})


if __name__ == "__main__":
    print(f"=== HIVE DISPATCH ADMIN — Port {PORT} ===")
    print(f"Stripe: {'CONFIGURED' if STRIPE_AVAILABLE else 'NOT CONFIGURED'}")
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="warning")
