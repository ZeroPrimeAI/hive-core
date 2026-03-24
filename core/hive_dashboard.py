#!/usr/bin/env python3
"""
THE HIVE — Mission Control Dashboard
======================================
Single-page HTML dashboard showing the ENTIRE Hive status.
What Chris opens in his browser to see everything at once.

Port: 8920
Auto-refresh: 30 seconds
Dark navy + gold accents + green/red health indicators

Aggregates data from:
  - All service /health endpoints
  - Producer (8900) for production stats
  - Quality Grader (8901) for quality grades
  - Market Scanner (8903) for market signals
  - Revenue Hunter (8904) for revenue opportunities
  - Competitive Intel (8902) for competitor data
  - Morning Briefing (8905) for briefing highlights
  - Nerve (8200) for knowledge stats
  - Ollama on all machines for model counts
  - Hive Mind (8751) for council messages
  - SSH/disk checks on all machines
"""

import asyncio
import json
import os
import subprocess
import time
from datetime import datetime
from pathlib import Path

import httpx
import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Configuration
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PORT = 8920

MACHINES = {
    "ZeroDESK": {"ip": "100.77.113.48", "role": "Claude Code Hub", "gpu": "GTX 1660S 6GB", "local": True},
    "ZeroQ": {"ip": "100.70.226.103", "role": "Coordinator", "gpu": "RTX 5070 Ti 12GB", "ssh": "ssh -o ConnectTimeout=5 zero@100.70.226.103"},
    "ZeroZI": {"ip": "100.105.160.106", "role": "Primary GPU", "gpu": "RTX 5060 8GB", "ssh": "ssh -o ConnectTimeout=5 zero@100.105.160.106"},
    "ZeroNovo": {"ip": "100.103.183.91", "role": "CPU Worker", "gpu": "None", "ssh": "ssh -o ConnectTimeout=5 zero@100.103.183.91"},
}

# All services to monitor — name: (url, category)
SERVICES = {
    # Core Infrastructure
    "Nerve (CNS)":          ("http://100.105.160.106:8200/health", "core"),
    "Nerve Backup":         ("http://100.103.183.91:8200/health", "core"),
    "Model Router":         ("http://100.105.160.106:8878/health", "core"),
    "Hive Mind (Orion)":    ("http://localhost:8751/health", "core"),
    "HiveSwarm":            ("http://localhost:8750/health", "core"),
    # Production
    "Producer":             ("http://localhost:8900/health", "production"),
    "Quality Grader":       ("http://localhost:8901/health", "production"),
    "Competitive Intel":    ("http://localhost:8902/health", "intelligence"),
    "Market Scanner":       ("http://localhost:8903/health", "intelligence"),
    "Revenue Hunter":       ("http://localhost:8904/health", "intelligence"),
    "Morning Briefing":     ("http://localhost:8905/health", "intelligence"),
    "Failover Manager":     ("http://localhost:8899/health", "core"),
    # Telephony (on ZeroZI or ZeroQ depending)
    "Phone Webhook":        ("http://100.105.160.106:8110/health", "telephony"),
    "Director Line":        ("http://100.105.160.106:8098/health", "telephony"),
    # Workers
    "ZeroZI Worker":        ("http://100.105.160.106:8880/health", "worker"),
    "CPU Worker":           ("http://100.103.183.91:8880/health", "worker"),
    # Business
    "Dispatch Admin":       ("http://100.103.183.91:8141/health", "business"),
    "Ghost Site":           ("http://100.70.226.103:8143/health", "business"),
    "Marketplace":          ("http://100.70.226.103:8090/health", "business"),
    # vLLM
    "vLLM (ZeroZI)":       ("http://100.105.160.106:8000/v1/models", "inference"),
    # Ollama instances
    "Ollama (ZeroDESK)":   ("http://localhost:11434/api/tags", "inference"),
    "Ollama (ZeroZI)":     ("http://100.105.160.106:11434/api/tags", "inference"),
    "Ollama (ZeroNovo)":   ("http://100.103.183.91:11434/api/tags", "inference"),
}

# Data endpoints
DATA_ENDPOINTS = {
    "production":   "http://localhost:8900/health",
    "quality":      "http://localhost:8901/api/grades?limit=50",
    "signals":      "http://localhost:8903/api/signals",
    "opportunities":"http://localhost:8904/api/opportunities",
    "intel":        "http://localhost:8902/api/latest",
    "briefing":     "http://localhost:8905/api/latest",
    "nerve":        "http://100.105.160.106:8200/api/stats",
    "council":      "http://localhost:8751/api/decisions?limit=10",
}

app = FastAPI(title="THE HIVE — Mission Control")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Data Collection
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def run_cmd(cmd, timeout=10):
    """Run a local command with timeout."""
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        return r.stdout.strip() if r.returncode == 0 else ""
    except Exception:
        return ""


async def _check_one_service(client, name, url, cat):
    """Check a single service health endpoint."""
    try:
        r = await client.get(url)
        if r.status_code == 200:
            try:
                data = r.json()
            except Exception:
                data = {}
            return name, {"status": "UP", "category": cat, "details": data}
        else:
            return name, {"status": "ERROR", "category": cat, "code": r.status_code}
    except Exception as e:
        return name, {"status": "DOWN", "category": cat, "error": str(e)[:50]}


async def check_services():
    """Check health of all services in parallel."""
    results = {}
    async with httpx.AsyncClient(timeout=4.0) as client:
        coros = [
            _check_one_service(client, name, url, cat)
            for name, (url, cat) in SERVICES.items()
        ]
        done = await asyncio.gather(*coros, return_exceptions=True)
        for item in done:
            if isinstance(item, tuple):
                results[item[0]] = item[1]
    return results


def _get_disk_usage_sync():
    """Get disk usage for all reachable machines (blocking, runs in thread)."""
    disks = {}
    # Local
    local = run_cmd("df -h / --output=pcent,size,used,avail | tail -1")
    if local:
        parts = local.split()
        disks["ZeroDESK"] = {"percent": parts[0] if parts else "?", "size": parts[1] if len(parts) > 1 else "?",
                             "used": parts[2] if len(parts) > 2 else "?", "avail": parts[3] if len(parts) > 3 else "?"}

    for name, info in MACHINES.items():
        if info.get("local"):
            continue
        ssh = info.get("ssh", "")
        if ssh:
            out = run_cmd(f'{ssh} "df -h / --output=pcent,size,used,avail | tail -1"')
            if out:
                parts = out.split()
                disks[name] = {"percent": parts[0] if parts else "?", "size": parts[1] if len(parts) > 1 else "?",
                               "used": parts[2] if len(parts) > 2 else "?", "avail": parts[3] if len(parts) > 3 else "?"}
            else:
                disks[name] = {"percent": "?", "size": "?", "used": "?", "avail": "?", "offline": True}
    return disks


async def get_data_endpoint(client, key, url):
    """Fetch a data endpoint."""
    try:
        r = await client.get(url)
        if r.status_code == 200:
            return key, r.json()
    except Exception:
        pass
    return key, {}


async def collect_all_data():
    """Collect all dashboard data in parallel."""
    async def fetch_data_endpoints():
        async with httpx.AsyncClient(timeout=5.0) as client:
            tasks = [get_data_endpoint(client, k, v) for k, v in DATA_ENDPOINTS.items()]
            return await asyncio.gather(*tasks, return_exceptions=True)

    # Run all async fetches in parallel; disk uses blocking subprocess via run_in_executor
    loop = asyncio.get_event_loop()
    services, data_results_raw, disks = await asyncio.gather(
        check_services(),
        fetch_data_endpoints(),
        loop.run_in_executor(None, _get_disk_usage_sync),
        return_exceptions=True,
    )

    if isinstance(services, Exception):
        services = {}
    if isinstance(data_results_raw, Exception):
        data_results_raw = []
    if isinstance(disks, Exception):
        disks = {}

    data = {}
    for item in data_results_raw:
        if isinstance(item, tuple):
            data[item[0]] = item[1]

    return {
        "services": services,
        "disks": disks,
        "data": data,
        "timestamp": datetime.now().isoformat(),
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# API Endpoints
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.get("/health")
async def health():
    return {"status": "alive", "service": "hive-dashboard", "port": PORT}


@app.get("/api/status")
async def api_status():
    """Return all dashboard data as JSON."""
    return await collect_all_data()


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Serve the mission control dashboard."""
    return DASHBOARD_HTML


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# The Dashboard HTML
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>THE HIVE — Mission Control</title>
<style>
/* ── Reset & Base ─────────────────────────────────────────────────── */
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

:root {
  --bg:       #0a0a1e;
  --bg2:      #0f0f2a;
  --bg3:      #141435;
  --gold:     #ffd700;
  --green:    #00ff88;
  --red:      #ff4444;
  --orange:   #ff8844;
  --blue:     #4488ff;
  --cyan:     #00ddff;
  --text:     #c8c8e0;
  --textdim:  #6a6a8e;
  --border:   #1e1e48;
  --shadow:   0 2px 20px rgba(0,0,0,0.4);
}

body {
  font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, sans-serif;
  background: var(--bg);
  color: var(--text);
  min-height: 100vh;
  overflow-x: hidden;
}

/* ── Header ───────────────────────────────────────────────────────── */
.header {
  background: linear-gradient(135deg, #0d0d28 0%, #1a1040 50%, #0d0d28 100%);
  border-bottom: 1px solid var(--border);
  padding: 16px 32px;
  display: flex;
  justify-content: space-between;
  align-items: center;
  position: sticky;
  top: 0;
  z-index: 100;
  backdrop-filter: blur(10px);
}

.header-left {
  display: flex;
  align-items: center;
  gap: 16px;
}

.logo {
  font-size: 28px;
  font-weight: 800;
  letter-spacing: 3px;
  background: linear-gradient(90deg, var(--gold), #ffaa00);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  text-shadow: 0 0 30px rgba(255,215,0,0.3);
}

.logo-sub {
  font-size: 11px;
  color: var(--textdim);
  letter-spacing: 5px;
  text-transform: uppercase;
}

.header-right {
  display: flex;
  align-items: center;
  gap: 24px;
}

.header-stat {
  text-align: center;
}

.header-stat .val {
  font-size: 24px;
  font-weight: 700;
}

.header-stat .lbl {
  font-size: 10px;
  color: var(--textdim);
  text-transform: uppercase;
  letter-spacing: 1px;
}

.refresh-badge {
  font-size: 11px;
  color: var(--textdim);
  display: flex;
  align-items: center;
  gap: 6px;
}

.pulse-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: var(--green);
  animation: pulse 2s ease-in-out infinite;
}

@keyframes pulse {
  0%, 100% { opacity: 1; box-shadow: 0 0 4px var(--green); }
  50% { opacity: 0.4; box-shadow: 0 0 12px var(--green); }
}

/* ── Grid Layout ──────────────────────────────────────────────────── */
.grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(380px, 1fr));
  gap: 16px;
  padding: 20px 24px;
  max-width: 1900px;
  margin: 0 auto;
}

/* ── Card ─────────────────────────────────────────────────────────── */
.card {
  background: var(--bg2);
  border: 1px solid var(--border);
  border-radius: 12px;
  overflow: hidden;
  box-shadow: var(--shadow);
  transition: border-color 0.3s;
}

.card:hover {
  border-color: rgba(255,215,0,0.3);
}

.card-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 14px 18px 10px;
  border-bottom: 1px solid var(--border);
}

.card-title {
  font-size: 13px;
  font-weight: 700;
  letter-spacing: 2px;
  text-transform: uppercase;
  color: var(--gold);
}

.card-badge {
  font-size: 11px;
  padding: 2px 10px;
  border-radius: 20px;
  font-weight: 600;
}

.card-body {
  padding: 14px 18px 18px;
}

/* special full-width card */
.card-wide {
  grid-column: 1 / -1;
}

/* ── Service Grid ─────────────────────────────────────────────────── */
.svc-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(170px, 1fr));
  gap: 8px;
}

.svc-item {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 10px;
  border-radius: 8px;
  background: var(--bg3);
  font-size: 12px;
  transition: background 0.2s;
}

.svc-item:hover {
  background: rgba(255,255,255,0.04);
}

.svc-dot {
  width: 10px;
  height: 10px;
  border-radius: 50%;
  flex-shrink: 0;
}

.svc-dot.up   { background: var(--green); box-shadow: 0 0 6px var(--green); }
.svc-dot.down { background: var(--red); box-shadow: 0 0 6px var(--red); }
.svc-dot.err  { background: var(--orange); box-shadow: 0 0 6px var(--orange); }

.svc-name {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  flex: 1;
}

.svc-cat {
  font-size: 9px;
  color: var(--textdim);
  text-transform: uppercase;
}

/* ── Stats Row ────────────────────────────────────────────────────── */
.stat-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 8px 0;
  border-bottom: 1px solid rgba(255,255,255,0.04);
}

.stat-row:last-child { border-bottom: none; }

.stat-label {
  font-size: 12px;
  color: var(--textdim);
}

.stat-value {
  font-size: 14px;
  font-weight: 600;
}

/* ── Disk Bars ────────────────────────────────────────────────────── */
.disk-row {
  margin-bottom: 12px;
}

.disk-header {
  display: flex;
  justify-content: space-between;
  margin-bottom: 4px;
}

.disk-machine {
  font-size: 12px;
  font-weight: 600;
  color: var(--text);
}

.disk-detail {
  font-size: 11px;
  color: var(--textdim);
}

.disk-bar-bg {
  width: 100%;
  height: 8px;
  background: var(--bg);
  border-radius: 4px;
  overflow: hidden;
}

.disk-bar-fill {
  height: 100%;
  border-radius: 4px;
  transition: width 0.5s ease;
}

/* ── List Items ───────────────────────────────────────────────────── */
.list-item {
  padding: 10px 12px;
  border-radius: 8px;
  background: var(--bg3);
  margin-bottom: 8px;
  font-size: 12px;
  line-height: 1.5;
  border-left: 3px solid var(--border);
}

.list-item:last-child { margin-bottom: 0; }

.list-item.gold { border-left-color: var(--gold); }
.list-item.green { border-left-color: var(--green); }
.list-item.blue { border-left-color: var(--blue); }
.list-item.cyan { border-left-color: var(--cyan); }
.list-item.red { border-left-color: var(--red); }

.list-item .item-title {
  font-weight: 600;
  color: var(--text);
  margin-bottom: 2px;
}

.list-item .item-sub {
  color: var(--textdim);
  font-size: 11px;
}

/* ── Big Numbers ──────────────────────────────────────────────────── */
.big-nums {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(100px, 1fr));
  gap: 12px;
}

.big-num {
  text-align: center;
  padding: 12px 8px;
  background: var(--bg3);
  border-radius: 8px;
}

.big-num .num {
  font-size: 28px;
  font-weight: 800;
  line-height: 1.2;
}

.big-num .desc {
  font-size: 10px;
  color: var(--textdim);
  text-transform: uppercase;
  letter-spacing: 1px;
  margin-top: 4px;
}

/* ── Signal & Intel ───────────────────────────────────────────────── */
.signal-tag {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 4px;
  font-size: 10px;
  font-weight: 700;
  text-transform: uppercase;
}

.signal-tag.bullish { background: rgba(0,255,136,0.15); color: var(--green); }
.signal-tag.bearish { background: rgba(255,68,68,0.15); color: var(--red); }
.signal-tag.neutral { background: rgba(255,215,0,0.15); color: var(--gold); }

/* ── Empty State ──────────────────────────────────────────────────── */
.empty {
  text-align: center;
  padding: 24px;
  color: var(--textdim);
  font-size: 12px;
  font-style: italic;
}

/* ── Loading Shimmer ──────────────────────────────────────────────── */
.shimmer {
  background: linear-gradient(90deg, var(--bg3) 25%, var(--bg2) 50%, var(--bg3) 75%);
  background-size: 200% 100%;
  animation: shimmer 1.5s infinite;
  border-radius: 6px;
  height: 14px;
  margin: 6px 0;
}

@keyframes shimmer {
  0% { background-position: 200% 0; }
  100% { background-position: -200% 0; }
}

/* ── Footer ───────────────────────────────────────────────────────── */
.footer {
  text-align: center;
  padding: 20px;
  color: var(--textdim);
  font-size: 11px;
  border-top: 1px solid var(--border);
  margin-top: 12px;
}

.footer a { color: var(--gold); text-decoration: none; }

/* ── Responsive ───────────────────────────────────────────────────── */
@media (max-width: 768px) {
  .header { padding: 12px 16px; flex-direction: column; gap: 8px; }
  .grid { padding: 12px; gap: 12px; grid-template-columns: 1fr; }
  .svc-grid { grid-template-columns: 1fr 1fr; }
  .big-nums { grid-template-columns: repeat(2, 1fr); }
}

/* ── Countdown Ring ───────────────────────────────────────────────── */
.countdown {
  font-size: 12px;
  color: var(--textdim);
  font-variant-numeric: tabular-nums;
}
</style>
</head>
<body>

<!-- ═══════════════ HEADER ═══════════════ -->
<div class="header">
  <div class="header-left">
    <div>
      <div class="logo">THE HIVE</div>
      <div class="logo-sub">Mission Control</div>
    </div>
  </div>
  <div class="header-right">
    <div class="header-stat">
      <div class="val" id="h-up" style="color:var(--green)">--</div>
      <div class="lbl">Services Up</div>
    </div>
    <div class="header-stat">
      <div class="val" id="h-down" style="color:var(--red)">--</div>
      <div class="lbl">Down</div>
    </div>
    <div class="header-stat">
      <div class="val" id="h-machines" style="color:var(--cyan)">--</div>
      <div class="lbl">Machines</div>
    </div>
    <div class="refresh-badge">
      <div class="pulse-dot"></div>
      <span>LIVE</span>
      <span class="countdown" id="countdown">30s</span>
    </div>
  </div>
</div>

<!-- ═══════════════ GRID ═══════════════ -->
<div class="grid">

  <!-- ── Services ────────────────────────────── -->
  <div class="card card-wide">
    <div class="card-head">
      <div class="card-title">Service Health</div>
      <div class="card-badge" id="svc-badge" style="background:rgba(0,255,136,0.15);color:var(--green)">Loading...</div>
    </div>
    <div class="card-body">
      <div class="svc-grid" id="services-grid">
        <div class="shimmer" style="width:100%;height:200px;grid-column:1/-1;"></div>
      </div>
    </div>
  </div>

  <!-- ── Production Stats ────────────────────── -->
  <div class="card">
    <div class="card-head">
      <div class="card-title">Production</div>
      <div class="card-badge" style="background:rgba(0,221,255,0.15);color:var(--cyan)">Content Pipeline</div>
    </div>
    <div class="card-body">
      <div class="big-nums" id="production-nums">
        <div class="big-num"><div class="num" style="color:var(--cyan)" id="p-episodes">--</div><div class="desc">Episodes</div></div>
        <div class="big-num"><div class="num" style="color:var(--green)" id="p-shorts">--</div><div class="desc">Shorts</div></div>
        <div class="big-num"><div class="num" style="color:var(--gold)" id="p-music">--</div><div class="desc">Music Tracks</div></div>
        <div class="big-num"><div class="num" style="color:var(--blue)" id="p-uploads">--</div><div class="desc">Uploaded</div></div>
      </div>
      <div style="margin-top:12px" id="production-details"></div>
    </div>
  </div>

  <!-- ── Quality Grades ──────────────────────── -->
  <div class="card">
    <div class="card-head">
      <div class="card-title">Quality Gate</div>
      <div class="card-badge" id="quality-badge" style="background:rgba(255,215,0,0.15);color:var(--gold)">--</div>
    </div>
    <div class="card-body">
      <div class="big-nums" id="quality-nums">
        <div class="big-num"><div class="num" style="color:var(--gold)" id="q-avg">--</div><div class="desc">Avg Score</div></div>
        <div class="big-num"><div class="num" style="color:var(--green)" id="q-ready">--</div><div class="desc">Upload Ready</div></div>
        <div class="big-num"><div class="num" style="color:var(--red)" id="q-reject">--</div><div class="desc">Rejected</div></div>
        <div class="big-num"><div class="num" style="color:var(--text)" id="q-total">--</div><div class="desc">Total Graded</div></div>
      </div>
      <div style="margin-top:12px" id="quality-details"></div>
    </div>
  </div>

  <!-- ── Market Signals ──────────────────────── -->
  <div class="card">
    <div class="card-head">
      <div class="card-title">Market Signals</div>
      <div class="card-badge" style="background:rgba(68,136,255,0.15);color:var(--blue)">Forex / Crypto</div>
    </div>
    <div class="card-body" id="market-body">
      <div class="empty">Waiting for scanner data...</div>
    </div>
  </div>

  <!-- ── Revenue Opportunities ───────────────── -->
  <div class="card">
    <div class="card-head">
      <div class="card-title">Revenue Opportunities</div>
      <div class="card-badge" style="background:rgba(0,255,136,0.15);color:var(--green)">Top 5</div>
    </div>
    <div class="card-body" id="revenue-body">
      <div class="empty">Waiting for hunter data...</div>
    </div>
  </div>

  <!-- ── Competitive Intel ───────────────────── -->
  <div class="card">
    <div class="card-head">
      <div class="card-title">Competitive Intel</div>
      <div class="card-badge" style="background:rgba(255,136,68,0.15);color:var(--orange)">Latest</div>
    </div>
    <div class="card-body" id="intel-body">
      <div class="empty">Waiting for intel data...</div>
    </div>
  </div>

  <!-- ── Training Status ─────────────────────── -->
  <div class="card">
    <div class="card-head">
      <div class="card-title">Training Pipeline</div>
      <div class="card-badge" style="background:rgba(0,221,255,0.15);color:var(--cyan)">Models</div>
    </div>
    <div class="card-body" id="training-body">
      <div class="stat-row">
        <span class="stat-label">Specialist Models (ZeroDESK)</span>
        <span class="stat-value" id="t-desk-models">--</span>
      </div>
      <div class="stat-row">
        <span class="stat-label">Models (ZeroZI)</span>
        <span class="stat-value" id="t-zi-models">--</span>
      </div>
      <div class="stat-row">
        <span class="stat-label">Models (ZeroNovo)</span>
        <span class="stat-value" id="t-novo-models">--</span>
      </div>
      <div class="stat-row">
        <span class="stat-label">vLLM LoRA Adapters</span>
        <span class="stat-value" id="t-vllm">--</span>
      </div>
      <div class="stat-row">
        <span class="stat-label">Nerve Knowledge Facts</span>
        <span class="stat-value" style="color:var(--gold)" id="t-nerve">--</span>
      </div>
      <div style="margin-top:12px" id="training-details"></div>
    </div>
  </div>

  <!-- ── Disk Space ──────────────────────────── -->
  <div class="card">
    <div class="card-head">
      <div class="card-title">Disk Space</div>
      <div class="card-badge" style="background:rgba(255,215,0,0.15);color:var(--gold)">All Machines</div>
    </div>
    <div class="card-body" id="disk-body">
      <div class="shimmer" style="height:120px"></div>
    </div>
  </div>

  <!-- ── Council Messages ────────────────────── -->
  <div class="card">
    <div class="card-head">
      <div class="card-title">Hive Mind Council</div>
      <div class="card-badge" style="background:rgba(255,215,0,0.15);color:var(--gold)">Latest 10</div>
    </div>
    <div class="card-body" id="council-body">
      <div class="empty">Waiting for council data...</div>
    </div>
  </div>

  <!-- ── Morning Briefing ────────────────────── -->
  <div class="card">
    <div class="card-head">
      <div class="card-title">Morning Briefing</div>
      <div class="card-badge" style="background:rgba(0,221,255,0.15);color:var(--cyan)">Highlights</div>
    </div>
    <div class="card-body" id="briefing-body">
      <div class="empty">Waiting for briefing data...</div>
    </div>
  </div>

</div>

<!-- ═══════════════ FOOTER ═══════════════ -->
<div class="footer">
  THE HIVE &mdash; Autonomous AI Infrastructure &bull; 5 Machines &bull; 2 Cloud Brains &bull; 23+ Specialists &bull;
  <span id="footer-time"></span>
</div>

<!-- ═══════════════ SCRIPT ═══════════════ -->
<script>
const API = '/api/status';
const REFRESH_INTERVAL = 30;
let countdown = REFRESH_INTERVAL;
let refreshTimer = null;

// ── Helpers ──────────────────────────────────────────────────────────
function esc(s) {
  if (typeof s !== 'string') return String(s ?? '--');
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}

function diskColor(pct) {
  const n = parseInt(pct);
  if (isNaN(n)) return 'var(--textdim)';
  if (n >= 90) return 'var(--red)';
  if (n >= 75) return 'var(--orange)';
  return 'var(--green)';
}

function truncate(s, len) {
  if (!s) return '';
  return s.length > len ? s.substring(0, len) + '...' : s;
}

// ── Render Services ──────────────────────────────────────────────────
function renderServices(services) {
  const grid = document.getElementById('services-grid');
  const badge = document.getElementById('svc-badge');

  if (!services || Object.keys(services).length === 0) {
    grid.innerHTML = '<div class="empty">No service data available</div>';
    return;
  }

  let up = 0, down = 0, err = 0;
  const cats = {};

  Object.entries(services).forEach(([name, info]) => {
    const cat = info.category || 'other';
    if (!cats[cat]) cats[cat] = [];
    cats[cat].push({ name, ...info });
    if (info.status === 'UP') up++;
    else if (info.status === 'ERROR') err++;
    else down++;
  });

  document.getElementById('h-up').textContent = up;
  document.getElementById('h-down').textContent = down + err;

  const total = up + down + err;
  const pct = total > 0 ? Math.round(up / total * 100) : 0;
  badge.textContent = `${up}/${total} UP (${pct}%)`;
  badge.style.background = pct >= 80 ? 'rgba(0,255,136,0.15)' : pct >= 50 ? 'rgba(255,136,68,0.15)' : 'rgba(255,68,68,0.15)';
  badge.style.color = pct >= 80 ? 'var(--green)' : pct >= 50 ? 'var(--orange)' : 'var(--red)';

  const catOrder = ['core', 'production', 'intelligence', 'telephony', 'worker', 'business', 'inference'];
  const catLabels = {
    core: 'Core Infrastructure', production: 'Content Production', intelligence: 'Intelligence',
    telephony: 'Phone System', worker: 'Workers', business: 'Business', inference: 'Inference Engines'
  };

  let html = '';
  catOrder.forEach(cat => {
    const items = cats[cat];
    if (!items) return;
    html += `<div style="grid-column:1/-1;margin-top:8px;margin-bottom:4px;">
      <span style="font-size:10px;color:var(--textdim);text-transform:uppercase;letter-spacing:2px;">${catLabels[cat] || cat}</span>
    </div>`;
    items.forEach(item => {
      const cls = item.status === 'UP' ? 'up' : item.status === 'ERROR' ? 'err' : 'down';
      html += `<div class="svc-item" title="${esc(item.error || item.status)}">
        <div class="svc-dot ${cls}"></div>
        <span class="svc-name">${esc(item.name)}</span>
      </div>`;
    });
  });

  grid.innerHTML = html;
}

// ── Render Production ────────────────────────────────────────────────
function renderProduction(data) {
  const prod = data?.data?.production || {};
  document.getElementById('p-episodes').textContent = prod.episodes_produced ?? prod.episodes ?? '--';
  document.getElementById('p-shorts').textContent = prod.shorts_produced ?? prod.shorts ?? '--';
  document.getElementById('p-music').textContent = prod.music_tracks ?? prod.music ?? '--';
  document.getElementById('p-uploads').textContent = prod.uploads_successful ?? prod.uploads ?? '--';

  const details = document.getElementById('production-details');
  const fails = prod.uploads_failed ?? 0;
  if (fails > 0) {
    details.innerHTML = `<div class="list-item red"><div class="item-title">${fails} Upload(s) Failed</div><div class="item-sub">Check producer logs for details</div></div>`;
  } else if (prod.last_cycle) {
    details.innerHTML = `<div class="list-item green"><div class="item-sub">Last cycle: ${esc(prod.last_cycle)}</div></div>`;
  } else {
    details.innerHTML = '';
  }
}

// ── Render Quality ───────────────────────────────────────────────────
function renderQuality(data) {
  const q = data?.data?.quality || {};
  const grades = q.grades || [];
  const badge = document.getElementById('quality-badge');

  if (grades.length === 0) {
    badge.textContent = 'No Data';
    return;
  }

  const scores = grades.map(g => g.score || 0);
  const avg = scores.reduce((a, b) => a + b, 0) / scores.length;
  const ready = grades.filter(g => g.verdict === 'UPLOAD_READY').length;
  const rejected = grades.filter(g => g.verdict === 'REJECT').length;

  document.getElementById('q-avg').textContent = avg.toFixed(0);
  document.getElementById('q-ready').textContent = ready;
  document.getElementById('q-reject').textContent = rejected;
  document.getElementById('q-total').textContent = grades.length;

  const rejectRate = grades.length > 0 ? (rejected / grades.length * 100) : 0;
  badge.textContent = `Reject Rate: ${rejectRate.toFixed(0)}%`;
  badge.style.color = rejectRate > 30 ? 'var(--red)' : rejectRate > 15 ? 'var(--orange)' : 'var(--green)';

  const details = document.getElementById('quality-details');
  const recent = grades.slice(0, 3);
  if (recent.length > 0) {
    details.innerHTML = recent.map(g => {
      const cls = g.verdict === 'UPLOAD_READY' ? 'green' : g.verdict === 'REJECT' ? 'red' : 'gold';
      return `<div class="list-item ${cls}">
        <div class="item-title">${esc(g.build_file || g.file || 'Unknown')} &mdash; ${g.score}/100</div>
        <div class="item-sub">${esc(g.verdict)} | ${esc(g.content_type || '')} | ${esc(g.graded_at || '')}</div>
      </div>`;
    }).join('');
  }
}

// ── Render Market Signals ────────────────────────────────────────────
function renderMarket(data) {
  const body = document.getElementById('market-body');
  const signals = data?.data?.signals || {};

  if (signals.status === 'scanner not running' || Object.keys(signals).length === 0) {
    body.innerHTML = '<div class="empty">Market scanner not running or no signals available</div>';
    return;
  }

  const items = signals.signals || signals.data || [];
  if (Array.isArray(items) && items.length > 0) {
    body.innerHTML = items.slice(0, 6).map(s => {
      const sentiment = (s.sentiment || s.signal || 'neutral').toLowerCase();
      const cls = sentiment.includes('bull') || sentiment.includes('buy') || sentiment.includes('long') ? 'bullish' :
                  sentiment.includes('bear') || sentiment.includes('sell') || sentiment.includes('short') ? 'bearish' : 'neutral';
      return `<div class="list-item ${cls === 'bullish' ? 'green' : cls === 'bearish' ? 'red' : 'gold'}">
        <div class="item-title">${esc(s.pair || s.symbol || s.market || 'Signal')}
          <span class="signal-tag ${cls}">${esc(sentiment)}</span></div>
        <div class="item-sub">${esc(s.reason || s.note || s.description || '')}</div>
      </div>`;
    }).join('');
  } else if (typeof signals === 'object') {
    // Render key-value pairs
    body.innerHTML = Object.entries(signals).slice(0, 6).map(([k, v]) =>
      `<div class="stat-row"><span class="stat-label">${esc(k)}</span><span class="stat-value">${esc(typeof v === 'object' ? JSON.stringify(v).substring(0,40) : v)}</span></div>`
    ).join('');
  }
}

// ── Render Revenue ───────────────────────────────────────────────────
function renderRevenue(data) {
  const body = document.getElementById('revenue-body');
  const opps = data?.data?.opportunities || {};

  if (opps.status === 'hunter not running' || Object.keys(opps).length === 0) {
    body.innerHTML = '<div class="empty">Revenue hunter not running or no opportunities found</div>';
    return;
  }

  const items = opps.opportunities || opps.data || [];
  if (Array.isArray(items) && items.length > 0) {
    body.innerHTML = items.slice(0, 5).map((o, i) => {
      const colors = ['gold', 'green', 'cyan', 'blue', 'gold'];
      return `<div class="list-item ${colors[i % colors.length]}">
        <div class="item-title">${esc(o.title || o.name || o.opportunity || `Opportunity ${i+1}`)}</div>
        <div class="item-sub">${esc(o.description || o.details || o.value || '')}</div>
      </div>`;
    }).join('');
  } else if (typeof opps === 'object') {
    body.innerHTML = Object.entries(opps).slice(0, 5).map(([k, v]) =>
      `<div class="stat-row"><span class="stat-label">${esc(k)}</span><span class="stat-value">${esc(typeof v === 'object' ? JSON.stringify(v).substring(0,40) : v)}</span></div>`
    ).join('');
  }
}

// ── Render Intel ─────────────────────────────────────────────────────
function renderIntel(data) {
  const body = document.getElementById('intel-body');
  const intel = data?.data?.intel || {};

  if (Object.keys(intel).length === 0) {
    body.innerHTML = '<div class="empty">Competitive intel not available</div>';
    return;
  }

  const items = intel.findings || intel.data || intel.latest || [];
  if (Array.isArray(items) && items.length > 0) {
    body.innerHTML = items.slice(0, 5).map(f =>
      `<div class="list-item blue">
        <div class="item-title">${esc(f.title || f.competitor || f.finding || 'Finding')}</div>
        <div class="item-sub">${esc(truncate(f.description || f.details || f.summary || '', 120))}</div>
      </div>`
    ).join('');
  } else if (typeof intel === 'object') {
    body.innerHTML = Object.entries(intel).slice(0, 5).map(([k, v]) =>
      `<div class="stat-row"><span class="stat-label">${esc(k)}</span><span class="stat-value">${esc(typeof v === 'object' ? JSON.stringify(v).substring(0,50) : v)}</span></div>`
    ).join('');
  }
}

// ── Render Training ──────────────────────────────────────────────────
function renderTraining(data) {
  const services = data?.services || {};

  // Count models from Ollama services
  const ollamaDesk = services['Ollama (ZeroDESK)'];
  const ollamaZI = services['Ollama (ZeroZI)'];
  const ollamaNovo = services['Ollama (ZeroNovo)'];
  const vllm = services['vLLM (ZeroZI)'];

  if (ollamaDesk?.status === 'UP' && ollamaDesk.details?.models) {
    document.getElementById('t-desk-models').textContent = ollamaDesk.details.models.length;
  } else if (ollamaDesk?.status === 'UP') {
    document.getElementById('t-desk-models').textContent = 'UP';
  } else {
    document.getElementById('t-desk-models').textContent = 'offline';
    document.getElementById('t-desk-models').style.color = 'var(--red)';
  }

  if (ollamaZI?.status === 'UP' && ollamaZI.details?.models) {
    document.getElementById('t-zi-models').textContent = ollamaZI.details.models.length;
  } else {
    document.getElementById('t-zi-models').textContent = ollamaZI?.status === 'UP' ? 'UP' : 'offline';
    if (ollamaZI?.status !== 'UP') document.getElementById('t-zi-models').style.color = 'var(--red)';
  }

  if (ollamaNovo?.status === 'UP' && ollamaNovo.details?.models) {
    document.getElementById('t-novo-models').textContent = ollamaNovo.details.models.length;
  } else {
    document.getElementById('t-novo-models').textContent = ollamaNovo?.status === 'UP' ? 'UP' : 'offline';
    if (ollamaNovo?.status !== 'UP') document.getElementById('t-novo-models').style.color = 'var(--red)';
  }

  if (vllm?.status === 'UP' && vllm.details?.data) {
    document.getElementById('t-vllm').textContent = vllm.details.data.length;
  } else {
    document.getElementById('t-vllm').textContent = vllm?.status === 'UP' ? 'UP' : 'offline';
    if (vllm?.status !== 'UP') document.getElementById('t-vllm').style.color = 'var(--red)';
  }

  // Nerve stats
  const nerve = data?.data?.nerve || {};
  document.getElementById('t-nerve').textContent = nerve.total
    ? Number(nerve.total).toLocaleString()
    : nerve.facts ? Number(nerve.facts).toLocaleString() : '--';
}

// ── Render Disk ──────────────────────────────────────────────────────
function renderDisk(data) {
  const body = document.getElementById('disk-body');
  const disks = data?.disks || {};

  if (Object.keys(disks).length === 0) {
    body.innerHTML = '<div class="empty">No disk data available</div>';
    return;
  }

  const machines = {
    ZeroDESK:  { role: 'Claude Code Hub',  gpu: 'GTX 1660S' },
    ZeroQ:     { role: 'Coordinator',       gpu: 'RTX 5070 Ti' },
    ZeroZI:    { role: 'Primary GPU',       gpu: 'RTX 5060' },
    ZeroNovo:  { role: 'CPU Worker',        gpu: 'None' },
  };

  let reachable = 0;
  let html = '';

  Object.entries(disks).forEach(([name, info]) => {
    const pct = parseInt(info.percent) || 0;
    const color = diskColor(info.percent);
    const offline = info.offline;
    if (!offline) reachable++;
    const m = machines[name] || {};

    html += `<div class="disk-row">
      <div class="disk-header">
        <span class="disk-machine">${esc(name)} <span style="color:var(--textdim);font-size:10px;font-weight:400">${esc(m.role || '')}</span></span>
        <span class="disk-detail" style="color:${color}">${offline ? 'OFFLINE' : `${esc(info.used)} / ${esc(info.size)} (${esc(info.percent)})`}</span>
      </div>
      <div class="disk-bar-bg">
        <div class="disk-bar-fill" style="width:${offline ? 0 : pct}%;background:${color};"></div>
      </div>
    </div>`;
  });

  document.getElementById('h-machines').textContent = `${reachable}/${Object.keys(disks).length}`;
  body.innerHTML = html;
}

// ── Render Council ───────────────────────────────────────────────────
function renderCouncil(data) {
  const body = document.getElementById('council-body');
  const council = data?.data?.council || {};

  const decisions = council.decisions || council.data || council.messages || [];
  if (Array.isArray(decisions) && decisions.length > 0) {
    body.innerHTML = decisions.slice(0, 10).map((d, i) => {
      const colors = ['gold', 'cyan', 'green', 'blue', 'gold', 'cyan', 'green', 'blue', 'gold', 'cyan'];
      return `<div class="list-item ${colors[i]}">
        <div class="item-title">${esc(d.decision || d.title || d.action || d.message || `Decision ${i+1}`)}</div>
        <div class="item-sub">${esc(truncate(d.reasoning || d.details || d.queens || d.timestamp || '', 120))}</div>
      </div>`;
    }).join('');
  } else if (typeof council === 'object' && Object.keys(council).length > 0) {
    body.innerHTML = Object.entries(council).slice(0, 10).map(([k, v]) =>
      `<div class="stat-row"><span class="stat-label">${esc(k)}</span><span class="stat-value">${esc(typeof v === 'object' ? JSON.stringify(v).substring(0,50) : v)}</span></div>`
    ).join('');
  } else {
    body.innerHTML = '<div class="empty">No council decisions available. Hive Mind may be offline.</div>';
  }
}

// ── Render Briefing ──────────────────────────────────────────────────
function renderBriefing(data) {
  const body = document.getElementById('briefing-body');
  const briefing = data?.data?.briefing || {};
  const report = briefing.report || '';

  if (!report) {
    body.innerHTML = '<div class="empty">No briefing generated yet. Starts at 6 AM.</div>';
    return;
  }

  // Extract key sections from the text report
  const lines = report.split('\n').filter(l => l.trim());
  const highlights = [];
  let inSection = '';
  const importantSections = ['SYSTEM HEALTH', 'CONTENT PRODUCTION', 'QUALITY', 'ISSUES', 'RECOMMENDED'];

  lines.forEach(line => {
    const trimmed = line.trim();
    importantSections.forEach(sec => {
      if (trimmed.includes(sec)) inSection = sec;
    });
    if (inSection && trimmed && !trimmed.startsWith('===') && !trimmed.startsWith('---') && !trimmed.includes('━━━')) {
      if (trimmed.length > 5 && trimmed.length < 200) {
        highlights.push({ section: inSection, text: trimmed });
      }
    }
  });

  if (highlights.length > 0) {
    const colors = { 'SYSTEM HEALTH': 'green', 'CONTENT PRODUCTION': 'cyan', 'QUALITY': 'gold', 'ISSUES': 'red', 'RECOMMENDED': 'blue' };
    body.innerHTML = highlights.slice(0, 8).map(h =>
      `<div class="list-item ${colors[h.section] || 'gold'}">
        <div class="item-sub" style="font-size:9px;text-transform:uppercase;letter-spacing:1px;margin-bottom:2px;">${esc(h.section)}</div>
        <div style="font-size:12px;">${esc(h.text)}</div>
      </div>`
    ).join('');
  } else {
    // Just show the raw report truncated
    body.innerHTML = `<pre style="font-size:11px;color:var(--text);white-space:pre-wrap;max-height:300px;overflow-y:auto;background:var(--bg3);padding:12px;border-radius:8px;">${esc(report.substring(0, 1000))}</pre>`;
  }
}

// ── Master Render ────────────────────────────────────────────────────
function render(data) {
  renderServices(data.services);
  renderProduction(data);
  renderQuality(data);
  renderMarket(data);
  renderRevenue(data);
  renderIntel(data);
  renderTraining(data);
  renderDisk(data);
  renderCouncil(data);
  renderBriefing(data);

  document.getElementById('footer-time').textContent =
    'Last update: ' + new Date(data.timestamp).toLocaleString();
}

// ── Fetch & Refresh ──────────────────────────────────────────────────
async function fetchData() {
  try {
    const resp = await fetch(API);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    render(data);
  } catch (e) {
    console.error('Dashboard fetch error:', e);
  }
  countdown = REFRESH_INTERVAL;
}

function startCountdown() {
  setInterval(() => {
    countdown--;
    document.getElementById('countdown').textContent = countdown + 's';
    if (countdown <= 0) {
      fetchData();
    }
  }, 1000);
}

// ── Init ─────────────────────────────────────────────────────────────
fetchData();
startCountdown();
</script>
</body>
</html>"""

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Entrypoint
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

if __name__ == "__main__":
    print(f"""
    ╔════════════════════════════════════════════════╗
    ║   THE HIVE — Mission Control Dashboard         ║
    ║   Port: {PORT}                                   ║
    ║   http://localhost:{PORT}                        ║
    ╚════════════════════════════════════════════════╝
    """)
    uvicorn.run(app, host="0.0.0.0", port=PORT)
