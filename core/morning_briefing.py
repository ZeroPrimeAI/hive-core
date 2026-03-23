#!/usr/bin/env python3
"""
HIVE MORNING BRIEFING — Chris's Daily Report
=============================================
Runs at 6 AM every day. Compiles full system status, opportunities,
issues, and action items. Sends to Chris via Telegram + saves to file.

This is what Chris wakes up to. Make it COUNT.
Port: 8905
"""

import asyncio
import json
import os
import time
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import httpx
import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

PORT = 8905
REPORT_DIR = Path("/home/zero/hivecode_sandbox/reports")
REPORT_DIR.mkdir(parents=True, exist_ok=True)

# Service endpoints
SERVICES = {
    "producer": "http://localhost:8900/health",
    "grader": "http://localhost:8901/health",
    "failover": "http://localhost:8899/health",
    "hive_mind": "http://localhost:8751/health",
    "hive_swarm": "http://localhost:8750/health",
    "nerve": "http://100.105.160.106:8200/health",
    "director": "http://100.105.160.106:8098/health",
    "phone": "http://100.105.160.106:8110/health",
    "model_router": "http://100.105.160.106:8878/health",
    "dispatch": "http://100.103.183.91:8141/health",
    "nerve_backup": "http://100.103.183.91:8200/health",
    "competitive_intel": "http://localhost:8902/health",
    "market_scanner": "http://localhost:8903/health",
    "revenue_hunter": "http://localhost:8904/health",
}

OLLAMA_URLS = {
    "ZeroDESK": "http://localhost:11434/api/tags",
    "ZeroZI": "http://100.105.160.106:11434/api/tags",
    "ZeroNovo": "http://100.103.183.91:11434/api/tags",
}

app = FastAPI(title="Hive Morning Briefing")


async def check_all_services():
    """Check health of every service."""
    results = {}
    async with httpx.AsyncClient(timeout=5.0) as client:
        for name, url in SERVICES.items():
            try:
                r = await client.get(url)
                if r.status_code == 200:
                    data = r.json()
                    results[name] = {"status": "UP", "details": data}
                else:
                    results[name] = {"status": "ERROR", "code": r.status_code}
            except Exception as e:
                results[name] = {"status": "DOWN", "error": str(e)[:60]}
    return results


async def check_models():
    """Count models on each machine."""
    models = {}
    async with httpx.AsyncClient(timeout=5.0) as client:
        for machine, url in OLLAMA_URLS.items():
            try:
                r = await client.get(url)
                if r.status_code == 200:
                    count = len(r.json().get("models", []))
                    models[machine] = count
            except:
                models[machine] = "unreachable"
    return models


async def check_training():
    """Check training status on coding brain."""
    import subprocess
    try:
        result = subprocess.run(
            ["ssh", "-o", "ConnectTimeout=5", "-p", "13788", "root@ssh8.vast.ai",
             "tail -3 /root/continuous_train.log 2>/dev/null"],
            capture_output=True, text=True, timeout=15
        )
        return result.stdout.strip() if result.stdout else "unreachable"
    except:
        return "unreachable"


async def get_production_stats():
    """Get content production numbers."""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get("http://localhost:8900/health")
            return r.json() if r.status_code == 200 else {}
    except:
        return {}


async def get_quality_summary():
    """Get quality grade summary."""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get("http://localhost:8901/api/grades?limit=50")
            if r.status_code == 200:
                grades = r.json().get("grades", [])
                if grades:
                    scores = [g.get("score", 0) for g in grades]
                    verdicts = {}
                    for g in grades:
                        v = g.get("verdict", "?")
                        verdicts[v] = verdicts.get(v, 0) + 1
                    return {
                        "total": len(grades),
                        "avg_score": sum(scores) / len(scores),
                        "verdicts": verdicts,
                    }
    except:
        pass
    return {}


async def get_nerve_stats():
    """Get nerve knowledge stats."""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get("http://100.105.160.106:8200/api/stats")
            return r.json() if r.status_code == 200 else {}
    except:
        return {}


async def get_market_signals():
    """Get latest market signals if scanner is running."""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get("http://localhost:8903/api/signals")
            return r.json() if r.status_code == 200 else {}
    except:
        return {"status": "scanner not running"}


async def get_revenue_opportunities():
    """Get top revenue opportunities if hunter is running."""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get("http://localhost:8904/api/opportunities")
            return r.json() if r.status_code == 200 else {}
    except:
        return {"status": "hunter not running"}


async def generate_briefing():
    """Generate the full morning briefing."""
    now = datetime.now()

    # Gather all data in parallel
    services, models, training, production, quality, nerve, signals, opportunities = await asyncio.gather(
        check_all_services(),
        check_models(),
        check_training(),
        get_production_stats(),
        get_quality_summary(),
        get_nerve_stats(),
        get_market_signals(),
        get_revenue_opportunities(),
    )

    # Count service status
    up = sum(1 for s in services.values() if s["status"] == "UP")
    down = sum(1 for s in services.values() if s["status"] == "DOWN")
    total = len(services)

    # Build the report
    report = f"""
╔══════════════════════════════════════════════════════════════╗
║              THE HIVE — MORNING BRIEFING                     ║
║              {now.strftime('%A, %B %d, %Y %I:%M %p')}           ║
╚══════════════════════════════════════════════════════════════╝

━━━ SYSTEM HEALTH ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Services: {up}/{total} UP | {down} DOWN
"""
    for name, info in services.items():
        icon = "✓" if info["status"] == "UP" else "✗"
        report += f"  {icon} {name}: {info['status']}\n"

    report += f"""
━━━ MODELS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
    for machine, count in models.items():
        report += f"  {machine}: {count} models\n"

    report += f"""
━━━ TRAINING ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  {training[-200:] if training else 'No data'}

━━━ CONTENT PRODUCTION ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Episodes: {production.get('episodes_produced', '?')}
  Shorts: {production.get('shorts_produced', '?')}
  Uploads OK: {production.get('uploads_successful', '?')}
  Uploads Failed: {production.get('uploads_failed', '?')}

━━━ QUALITY ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Total graded: {quality.get('total', '?')}
  Average score: {quality.get('avg_score', '?'):.1f}/100
  Verdicts: {quality.get('verdicts', {})}

━━━ KNOWLEDGE (NERVE) ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Total facts: {nerve.get('total', '?')}

━━━ MARKET SIGNALS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  {json.dumps(signals, indent=2)[:300] if signals else 'Scanner not running'}

━━━ REVENUE OPPORTUNITIES ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  {json.dumps(opportunities, indent=2)[:300] if opportunities else 'Hunter not running'}

━━━ ISSUES TO RESOLVE ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
    issues = []
    for name, info in services.items():
        if info["status"] == "DOWN":
            issues.append(f"  ! {name} is DOWN — needs restart")
    if production.get("uploads_failed", 0) > 0:
        issues.append(f"  ! {production['uploads_failed']} uploads failed")
    if quality.get("avg_score", 100) < 60:
        issues.append(f"  ! Content quality low (avg {quality['avg_score']:.0f}/100)")
    if not issues:
        issues.append("  No critical issues found")
    report += "\n".join(issues)

    report += f"""

━━━ RECOMMENDED ACTIONS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  1. Revenue is still $0 — demo AI phone to a local business TODAY
  2. Check training progress — is gemma3:1b getting better?
  3. Review top market signals — any trade opportunities?
  4. Check competitive intel — what are competitors doing?
  5. Push any code changes to GitHub

══════════════════════════════════════════════════════════════
  THE HIVE EVOLVES. EVERY DAY WE GET BETTER.
══════════════════════════════════════════════════════════════
"""

    # Save report
    report_file = REPORT_DIR / f"briefing_{now.strftime('%Y%m%d_%H%M')}.txt"
    report_file.write_text(report)

    # Also save as latest
    (REPORT_DIR / "LATEST_BRIEFING.txt").write_text(report)

    return report


@app.get("/health")
async def health():
    return {"status": "alive", "service": "morning-briefing", "port": PORT}


@app.get("/api/briefing")
async def get_briefing():
    report = await generate_briefing()
    return {"report": report, "generated": datetime.now().isoformat()}


@app.get("/api/briefing/html")
async def get_briefing_html():
    report = await generate_briefing()
    html = f"<html><body><pre style='font-family:monospace;background:#0a0a1e;color:#00ff88;padding:20px;'>{report}</pre></body></html>"
    return HTMLResponse(html)


@app.get("/api/latest")
async def latest():
    f = REPORT_DIR / "LATEST_BRIEFING.txt"
    if f.exists():
        return {"report": f.read_text(), "file": str(f)}
    return {"report": "No briefing generated yet", "file": ""}


async def briefing_loop():
    """Generate briefing at 6 AM and every 4 hours."""
    while True:
        now = datetime.now()
        # Generate on startup, then every 4 hours
        try:
            report = await generate_briefing()
            print(f"[{now.strftime('%H:%M')}] Briefing generated ({len(report)} chars)")
        except Exception as e:
            print(f"[{now.strftime('%H:%M')}] Briefing error: {e}")
        await asyncio.sleep(14400)  # 4 hours


@app.on_event("startup")
async def startup():
    asyncio.create_task(briefing_loop())


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
