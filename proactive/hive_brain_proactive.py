#!/usr/bin/env python3
"""
PROACTIVE HIVE BRAIN — Autonomous decision-making daemon
=========================================================
FastAPI on port 8909.  Runs a 30-minute cycle that reads system state,
evaluates conditions, makes decisions, and takes action — without human
intervention.

Cycle phases:
  1. READ   — gather health, stats, quality, market signals, revenue
  2. EVAL   — apply decision rules to the collected state
  3. ACT    — restart services, trigger production, send alerts
  4. BRIEF  — daily 06:00 UTC morning summary posted to council #general
"""

import asyncio
import json
import logging
import subprocess
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Optional

import httpx
import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse

# ---------------------------------------------------------------------------
# Local imports
# ---------------------------------------------------------------------------

from alert import send_alert, send_info, send_status, send_decision

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

PORT = 8909
CYCLE_INTERVAL_SECONDS = 30 * 60  # 30 minutes
MORNING_BRIEF_HOUR_UTC = 6  # 06:00 UTC  (roughly midnight CST)
LOG_DIR = Path("/home/zero/logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "proactive_brain.log"

MACHINE = "ZeroDESK"
AGENT_ID = "proactive-brain"

# ---------------------------------------------------------------------------
# Service map — endpoints to monitor
# ---------------------------------------------------------------------------

# Local ZeroDESK services
LOCAL_SERVICES: dict[str, str] = {
    "council":          "http://localhost:8766/health",
    "hive-mind":        "http://localhost:8751/health",
    "hive-swarm":       "http://localhost:8750/health",
    "agentic-loop":     "http://localhost:8899/health",
    "producer":         "http://localhost:8900/health",
    "grader":           "http://localhost:8901/health",
    "shorts-engine":    "http://localhost:8902/health",
    "market-scanner":   "http://localhost:8903/health",
    "revenue-hunter":   "http://localhost:8904/health",
    "competitive-intel":"http://localhost:8905/health",
    "content-scheduler":"http://localhost:8906/health",
    "quality-guard":    "http://localhost:8907/health",
    "trend-watcher":    "http://localhost:8908/health",
    "proactive-brain":  "http://localhost:8909/health",
}

# ZeroZI (100.105.160.106) services
ZEROZI_SERVICES: dict[str, str] = {
    "zerozi-nerve":          "http://100.105.160.106:8200/health",
    "zerozi-interactive":    "http://100.105.160.106:8098/health",
    "zerozi-webhook":        "http://100.105.160.106:8110/health",
    "zerozi-model-router":   "http://100.105.160.106:8878/health",
}

# ZeroNovo (100.103.183.91) services
ZERONOVO_SERVICES: dict[str, str] = {
    "zeronovo-nerve":        "http://100.103.183.91:8200/health",
    "zeronovo-dispatch":     "http://100.103.183.91:8141/health",
}

# Data endpoints (not health checks — actual data we pull)
DATA_ENDPOINTS: dict[str, str] = {
    "producer_stats":    "http://localhost:8900/api/stats",
    "quality_grades":    "http://localhost:8901/api/grades",
    "market_signals":    "http://localhost:8903/api/signals",
    "revenue_opps":      "http://localhost:8904/api/opportunities",
}

# systemd service names for restart (only local ZeroDESK services we manage)
SYSTEMD_MAP: dict[str, str] = {
    "council":          "hive-council",
    "hive-mind":        "hive-mind",
    "hive-swarm":       "hive-swarm",
    "producer":         "hive-producer",
    "grader":           "hive-grader",
    "shorts-engine":    "hive-shorts-engine",
    "market-scanner":   "hive-market-scanner",
    "revenue-hunter":   "hive-revenue-hunter",
    "competitive-intel":"hive-competitive-intel",
    "content-scheduler":"hive-content-scheduler",
    "quality-guard":    "hive-quality-guard",
    "trend-watcher":    "hive-trend-watcher",
    "agentic-loop":     "hive-agentic-loop",
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("proactive-brain")

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

# Persistent state across cycles
cycle_history: list[dict] = []       # last N cycle results
action_history: list[dict] = []      # all actions taken
last_brief_date: Optional[str] = None  # track daily brief
last_production_ts: float = time.time()  # when we last saw content produced


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def fetch_health(client: httpx.AsyncClient, name: str, url: str) -> dict:
    """Check a single health endpoint. Returns {name, url, ok, status, detail}."""
    try:
        resp = await client.get(url)
        ok = resp.status_code == 200
        try:
            detail = resp.json()
        except Exception:
            detail = resp.text[:200]
        return {"name": name, "url": url, "ok": ok, "status": resp.status_code, "detail": detail}
    except httpx.ConnectError:
        return {"name": name, "url": url, "ok": False, "status": 0, "detail": "connection refused"}
    except httpx.TimeoutException:
        return {"name": name, "url": url, "ok": False, "status": 0, "detail": "timeout"}
    except Exception as e:
        return {"name": name, "url": url, "ok": False, "status": 0, "detail": str(e)[:200]}


async def fetch_data(client: httpx.AsyncClient, key: str, url: str) -> dict:
    """Fetch a data endpoint. Returns parsed JSON or error dict."""
    try:
        resp = await client.get(url)
        if resp.status_code == 200:
            return {"key": key, "ok": True, "data": resp.json()}
        return {"key": key, "ok": False, "data": None, "error": f"HTTP {resp.status_code}"}
    except Exception as e:
        return {"key": key, "ok": False, "data": None, "error": str(e)[:200]}


def check_coding_brain() -> dict:
    """
    SSH into the coding brain to check training status.
    Non-blocking — we run a quick command.
    """
    try:
        result = subprocess.run(
            [
                "ssh", "-o", "ConnectTimeout=8", "-o", "StrictHostKeyChecking=no",
                "-p", "13788", "root@ssh8.vast.ai",
                "nvidia-smi --query-gpu=utilization.gpu,memory.used,memory.total --format=csv,noheader,nounits 2>/dev/null; "
                "ls -t /workspace/*.log 2>/dev/null | head -3; "
                "ps aux | grep -E 'train|fine.?tune' | grep -v grep | head -3"
            ],
            capture_output=True, text=True, timeout=15,
        )
        return {
            "reachable": result.returncode == 0,
            "stdout": result.stdout.strip()[:500],
            "stderr": result.stderr.strip()[:200],
        }
    except subprocess.TimeoutExpired:
        return {"reachable": False, "stdout": "", "stderr": "SSH timeout"}
    except Exception as e:
        return {"reachable": False, "stdout": "", "stderr": str(e)[:200]}


def restart_service(service_name: str) -> dict:
    """Attempt to restart a local systemd service."""
    systemd_name = SYSTEMD_MAP.get(service_name)
    if not systemd_name:
        return {"restarted": False, "reason": f"No systemd mapping for '{service_name}'"}

    log.info(f"RESTARTING service: {systemd_name}")
    try:
        result = subprocess.run(
            ["sudo", "systemctl", "restart", systemd_name],
            capture_output=True, text=True, timeout=30,
        )
        success = result.returncode == 0
        if success:
            log.info(f"Restart OK: {systemd_name}")
        else:
            log.warning(f"Restart FAILED: {systemd_name} — {result.stderr.strip()}")
        return {
            "restarted": success,
            "service": systemd_name,
            "stderr": result.stderr.strip()[:200],
        }
    except Exception as e:
        log.error(f"Restart exception for {systemd_name}: {e}")
        return {"restarted": False, "service": systemd_name, "reason": str(e)[:200]}


def record_action(action_type: str, target: str, detail: str, success: bool):
    """Record an action in the action history."""
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "type": action_type,
        "target": target,
        "detail": detail,
        "success": success,
    }
    action_history.append(entry)
    # Keep last 500 actions in memory
    if len(action_history) > 500:
        action_history[:] = action_history[-500:]


# ---------------------------------------------------------------------------
# Core Cycle
# ---------------------------------------------------------------------------

async def run_cycle() -> dict:
    """
    Execute one full proactive cycle: READ -> EVAL -> ACT.
    Returns a summary dict of everything discovered and done.
    """
    global last_production_ts

    cycle_start = time.time()
    now_utc = datetime.now(timezone.utc)
    log.info("=" * 60)
    log.info(f"CYCLE START — {now_utc.isoformat()}")

    # ------------------------------------------------------------------
    # PHASE 1: READ
    # ------------------------------------------------------------------
    log.info("PHASE 1: READ — gathering system state")

    async with httpx.AsyncClient(timeout=8) as client:
        # Health checks — all in parallel
        all_services = {}
        all_services.update(LOCAL_SERVICES)
        all_services.update(ZEROZI_SERVICES)
        all_services.update(ZERONOVO_SERVICES)

        health_tasks = [
            fetch_health(client, name, url)
            for name, url in all_services.items()
        ]
        health_results = await asyncio.gather(*health_tasks)

        # Data fetches — all in parallel
        data_tasks = [
            fetch_data(client, key, url)
            for key, url in DATA_ENDPOINTS.items()
        ]
        data_results = await asyncio.gather(*data_tasks)

    # Coding brain check (synchronous, short timeout)
    coding_brain = check_coding_brain()

    # Organize results
    services_up = [h for h in health_results if h["ok"]]
    services_down = [h for h in health_results if not h["ok"]]
    total_services = len(health_results)

    data_map: dict[str, Any] = {}
    for d in data_results:
        data_map[d["key"]] = d

    log.info(f"Services: {len(services_up)}/{total_services} UP, {len(services_down)} DOWN")
    if services_down:
        log.info(f"DOWN: {[s['name'] for s in services_down]}")

    # Extract specific data
    producer_stats = data_map.get("producer_stats", {}).get("data") or {}
    quality_grades = data_map.get("quality_grades", {}).get("data") or {}
    market_signals = data_map.get("market_signals", {}).get("data") or {}
    revenue_opps = data_map.get("revenue_opps", {}).get("data") or {}

    # ------------------------------------------------------------------
    # PHASE 2: EVALUATE + DECIDE
    # ------------------------------------------------------------------
    log.info("PHASE 2: EVAL — applying decision rules")

    decisions: list[dict] = []
    alerts_to_send: list[dict] = []

    # Rule 1: Service down? -> restart (local only)
    for svc in services_down:
        svc_name = svc["name"]
        if svc_name in SYSTEMD_MAP:
            decisions.append({
                "rule": "service_restart",
                "target": svc_name,
                "reason": f"{svc_name} is DOWN ({svc['detail']})",
            })
        elif svc_name.startswith("zerozi-") or svc_name.startswith("zeronovo-"):
            alerts_to_send.append({
                "channel": "ops",
                "message": f"REMOTE SERVICE DOWN: {svc_name} — {svc['detail']}",
                "type": "alert",
            })

    # Rule 2: Producer crashed? -> restart
    producer_ok = any(h["ok"] for h in health_results if h["name"] == "producer")
    if not producer_ok and "producer" in SYSTEMD_MAP:
        # Already covered by Rule 1 but ensure it's flagged
        log.info("Producer confirmed down — restart already queued")

    # Rule 3: Quality average below 60 -> alert
    quality_avg = None
    if quality_grades:
        grades_list = quality_grades if isinstance(quality_grades, list) else quality_grades.get("grades", [])
        if grades_list and isinstance(grades_list, list):
            scores = []
            for g in grades_list:
                score = g.get("score") or g.get("grade") or g.get("quality")
                if score is not None:
                    try:
                        scores.append(float(score))
                    except (ValueError, TypeError):
                        pass
            if scores:
                quality_avg = sum(scores) / len(scores)
                log.info(f"Quality average: {quality_avg:.1f} from {len(scores)} grades")
                if quality_avg < 60:
                    alerts_to_send.append({
                        "channel": "alerts",
                        "message": f"QUALITY ALERT: Average quality {quality_avg:.1f}/100 is below threshold (60). Immediate attention needed.",
                        "type": "alert",
                    })

    # Rule 4: Market signal confidence > 80 -> log to nerve + alert
    high_confidence_signals = []
    if market_signals:
        signals_list = market_signals if isinstance(market_signals, list) else market_signals.get("signals", [])
        if isinstance(signals_list, list):
            for sig in signals_list:
                conf = sig.get("confidence") or sig.get("score") or 0
                try:
                    conf = float(conf)
                except (ValueError, TypeError):
                    conf = 0
                if conf > 80:
                    high_confidence_signals.append(sig)
            if high_confidence_signals:
                log.info(f"HIGH CONFIDENCE market signals: {len(high_confidence_signals)}")
                alerts_to_send.append({
                    "channel": "revenue",
                    "message": f"MARKET SIGNAL: {len(high_confidence_signals)} high-confidence signal(s) detected (>80%). Top: {json.dumps(high_confidence_signals[0], default=str)[:300]}",
                    "type": "alert",
                })

    # Rule 5: Revenue opportunity score > 80 -> alert Chris
    high_value_opps = []
    if revenue_opps:
        opps_list = revenue_opps if isinstance(revenue_opps, list) else revenue_opps.get("opportunities", [])
        if isinstance(opps_list, list):
            for opp in opps_list:
                score = opp.get("score") or opp.get("value") or opp.get("priority") or 0
                try:
                    score = float(score)
                except (ValueError, TypeError):
                    score = 0
                if score > 80:
                    high_value_opps.append(opp)
            if high_value_opps:
                log.info(f"HIGH VALUE revenue opps: {len(high_value_opps)}")
                alerts_to_send.append({
                    "channel": "revenue",
                    "message": f"REVENUE OPPORTUNITY: {len(high_value_opps)} high-value opportunity(ies) found (score >80). Top: {json.dumps(high_value_opps[0], default=str)[:300]}",
                    "type": "alert",
                })

    # Rule 6: No content produced in 2 hours -> trigger production
    content_stalled = False
    if producer_stats:
        last_produced = producer_stats.get("last_produced_at") or producer_stats.get("last_run")
        if last_produced:
            try:
                if isinstance(last_produced, str):
                    # Try ISO format
                    lp_dt = datetime.fromisoformat(last_produced.replace("Z", "+00:00"))
                    last_production_ts = lp_dt.timestamp()
                elif isinstance(last_produced, (int, float)):
                    last_production_ts = float(last_produced)
            except Exception:
                pass

    hours_since_production = (time.time() - last_production_ts) / 3600
    if hours_since_production > 2:
        content_stalled = True
        decisions.append({
            "rule": "trigger_production",
            "target": "producer",
            "reason": f"No content produced in {hours_since_production:.1f} hours",
        })

    # ------------------------------------------------------------------
    # PHASE 3: ACT
    # ------------------------------------------------------------------
    log.info(f"PHASE 3: ACT — {len(decisions)} decisions, {len(alerts_to_send)} alerts")

    actions_taken: list[dict] = []

    # Execute restarts
    for dec in decisions:
        if dec["rule"] == "service_restart":
            target = dec["target"]
            log.info(f"ACTION: Restarting {target}")
            result = restart_service(target)
            success = result.get("restarted", False)
            record_action("restart", target, dec["reason"], success)
            actions_taken.append({
                "action": "restart",
                "target": target,
                "reason": dec["reason"],
                "success": success,
            })

            # Post to council about the restart
            send_decision(
                f"AUTO-RESTART: {target} — {dec['reason']}. Result: {'OK' if success else 'FAILED'}",
                channel="ops",
            )

        elif dec["rule"] == "trigger_production":
            log.info("ACTION: Triggering content production")
            triggered = False
            try:
                async with httpx.AsyncClient(timeout=10) as client:
                    resp = await client.post("http://localhost:8900/api/produce")
                    triggered = resp.status_code == 200
            except Exception as e:
                log.warning(f"Failed to trigger production: {e}")

            record_action("trigger_production", "producer", dec["reason"], triggered)
            actions_taken.append({
                "action": "trigger_production",
                "reason": dec["reason"],
                "success": triggered,
            })

            if triggered:
                last_production_ts = time.time()
                send_info(
                    f"PRODUCTION TRIGGERED: Content was stalled for {hours_since_production:.1f}h — kicked off new production run.",
                    channel="ops",
                )

    # Send alerts
    for alert in alerts_to_send:
        send_alert(alert["message"], channel=alert["channel"], message_type=alert["type"])
        record_action("alert", alert["channel"], alert["message"][:100], True)
        actions_taken.append({
            "action": "alert",
            "channel": alert["channel"],
            "message": alert["message"][:200],
        })

    # ------------------------------------------------------------------
    # Build cycle summary
    # ------------------------------------------------------------------
    elapsed = time.time() - cycle_start
    summary = {
        "ts": now_utc.isoformat(),
        "elapsed_sec": round(elapsed, 2),
        "services_total": total_services,
        "services_up": len(services_up),
        "services_down_names": [s["name"] for s in services_down],
        "coding_brain": coding_brain,
        "quality_avg": round(quality_avg, 1) if quality_avg is not None else None,
        "market_signals_high": len(high_confidence_signals),
        "revenue_opps_high": len(high_value_opps),
        "content_stalled": content_stalled,
        "decisions": len(decisions),
        "actions_taken": actions_taken,
        "producer_stats_available": bool(producer_stats),
    }

    cycle_history.append(summary)
    # Keep last 100 cycles
    if len(cycle_history) > 100:
        cycle_history[:] = cycle_history[-100:]

    log.info(f"CYCLE COMPLETE in {elapsed:.1f}s — {len(services_up)}/{total_services} up, "
             f"{len(actions_taken)} actions taken")

    # Post summary to council
    send_status(
        f"CYCLE COMPLETE — {len(services_up)}/{total_services} services up | "
        f"Quality: {quality_avg if quality_avg is not None else 'N/A'} | "
        f"Actions: {len(actions_taken)} | "
        f"Down: {[s['name'] for s in services_down] if services_down else 'none'}",
        channel="ops",
    )

    return summary


# ---------------------------------------------------------------------------
# Morning Brief
# ---------------------------------------------------------------------------

async def maybe_send_morning_brief():
    """Send the daily morning brief at 06:00 UTC if not already sent today."""
    global last_brief_date

    now = datetime.now(timezone.utc)
    today_str = now.strftime("%Y-%m-%d")

    if now.hour != MORNING_BRIEF_HOUR_UTC:
        return
    if last_brief_date == today_str:
        return

    log.info("GENERATING MORNING BRIEF")
    last_brief_date = today_str

    # Gather stats from recent cycles
    recent = cycle_history[-48:] if cycle_history else []  # last 24 hours of 30-min cycles

    if recent:
        latest = recent[-1]
        services_up = latest.get("services_up", "?")
        services_total = latest.get("services_total", "?")
        quality = latest.get("quality_avg", "N/A")
        market_count = sum(c.get("market_signals_high", 0) for c in recent)
        revenue_count = sum(c.get("revenue_opps_high", 0) for c in recent)
        down_names = latest.get("services_down_names", [])
    else:
        services_up = "?"
        services_total = "?"
        quality = "N/A"
        market_count = 0
        revenue_count = 0
        down_names = []

    # Count actions from last 24 hours
    cutoff = (now - timedelta(hours=24)).isoformat()
    recent_actions = [a for a in action_history if a.get("ts", "") > cutoff]
    restarts = [a for a in recent_actions if a["type"] == "restart"]
    alerts_sent = [a for a in recent_actions if a["type"] == "alert"]

    # Count content produced (from producer stats if available)
    episodes = "?"
    shorts = "?"
    if recent and recent[-1].get("producer_stats_available"):
        # Best effort — actual counts depend on producer API shape
        episodes = "check producer"
        shorts = "check producer"

    issues = down_names if down_names else ["none"]

    brief = (
        f"DAILY BRIEF — {today_str}\n"
        f"Running services: {services_up}/{services_total}\n"
        f"Content produced: {episodes} episodes, {shorts} shorts\n"
        f"Quality avg: {quality}/100\n"
        f"Market signals: {market_count} active (last 24h)\n"
        f"Revenue opportunities: {revenue_count} found (last 24h)\n"
        f"Issues: {', '.join(str(i) for i in issues)}\n"
        f"Actions taken overnight: {len(restarts)} restarts, {len(alerts_sent)} alerts"
    )

    log.info(f"MORNING BRIEF:\n{brief}")
    send_info(brief, channel="general")


# ---------------------------------------------------------------------------
# Background loop
# ---------------------------------------------------------------------------

async def cycle_loop():
    """Main background loop: run cycle every 30 minutes."""
    # Wait a few seconds on startup so the HTTP server is ready
    await asyncio.sleep(5)

    log.info(f"Cycle loop started — interval: {CYCLE_INTERVAL_SECONDS}s ({CYCLE_INTERVAL_SECONDS // 60}m)")

    # Run an initial cycle immediately
    try:
        await run_cycle()
    except Exception as e:
        log.error(f"Initial cycle failed: {e}", exc_info=True)

    while True:
        try:
            await asyncio.sleep(CYCLE_INTERVAL_SECONDS)
            await run_cycle()
            await maybe_send_morning_brief()
        except asyncio.CancelledError:
            log.info("Cycle loop cancelled — shutting down")
            break
        except Exception as e:
            log.error(f"Cycle error: {e}", exc_info=True)
            # Don't crash the loop — wait and try again
            await asyncio.sleep(60)


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start the background cycle loop on startup."""
    task = asyncio.create_task(cycle_loop())
    log.info(f"Proactive Hive Brain starting on port {PORT}")
    send_info(f"Proactive Hive Brain ONLINE on {MACHINE}:{PORT}", channel="ops")
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    log.info("Proactive Hive Brain shut down")


app = FastAPI(
    title="Proactive Hive Brain",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
def health():
    last_cycle_ts = cycle_history[-1]["ts"] if cycle_history else None
    return {
        "status": "healthy",
        "service": "proactive-brain",
        "machine": MACHINE,
        "port": PORT,
        "cycles_completed": len(cycle_history),
        "actions_total": len(action_history),
        "last_cycle": last_cycle_ts,
        "uptime_cycles": len(cycle_history),
    }


@app.get("/api/last-cycle")
def last_cycle():
    """Return the results of the most recent cycle."""
    if not cycle_history:
        return JSONResponse(
            status_code=404,
            content={"error": "No cycles completed yet"},
        )
    return cycle_history[-1]


@app.get("/api/actions")
def all_actions(limit: int = 100):
    """Return all actions taken, most recent first."""
    return {
        "total": len(action_history),
        "showing": min(limit, len(action_history)),
        "actions": list(reversed(action_history[-limit:])),
    }


@app.get("/api/cycles")
def all_cycles(limit: int = 20):
    """Return recent cycle summaries."""
    return {
        "total": len(cycle_history),
        "showing": min(limit, len(cycle_history)),
        "cycles": list(reversed(cycle_history[-limit:])),
    }


@app.post("/api/run-cycle")
async def manual_cycle():
    """Manually trigger a cycle right now."""
    log.info("MANUAL CYCLE TRIGGERED via API")
    try:
        summary = await run_cycle()
        return {"ok": True, "summary": summary}
    except Exception as e:
        log.error(f"Manual cycle failed: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": str(e)},
        )


@app.post("/api/morning-brief")
async def manual_brief():
    """Manually trigger a morning brief right now (bypasses time check)."""
    global last_brief_date
    saved_date = last_brief_date
    last_brief_date = None  # force it to send
    # Temporarily set hour check to pass
    old_hour = MORNING_BRIEF_HOUR_UTC
    now = datetime.now(timezone.utc)

    log.info("MANUAL MORNING BRIEF TRIGGERED via API")

    # Build and send brief inline (same logic as maybe_send_morning_brief)
    today_str = now.strftime("%Y-%m-%d")
    last_brief_date = today_str

    recent = cycle_history[-48:] if cycle_history else []

    if recent:
        latest = recent[-1]
        services_up = latest.get("services_up", "?")
        services_total = latest.get("services_total", "?")
        quality = latest.get("quality_avg", "N/A")
        market_count = sum(c.get("market_signals_high", 0) for c in recent)
        revenue_count = sum(c.get("revenue_opps_high", 0) for c in recent)
        down_names = latest.get("services_down_names", [])
    else:
        services_up = "?"
        services_total = "?"
        quality = "N/A"
        market_count = 0
        revenue_count = 0
        down_names = []

    cutoff = (now - timedelta(hours=24)).isoformat()
    recent_actions = [a for a in action_history if a.get("ts", "") > cutoff]
    restarts = [a for a in recent_actions if a["type"] == "restart"]
    alerts_sent = [a for a in recent_actions if a["type"] == "alert"]

    issues = down_names if down_names else ["none"]

    brief = (
        f"DAILY BRIEF — {today_str}\n"
        f"Running services: {services_up}/{services_total}\n"
        f"Quality avg: {quality}/100\n"
        f"Market signals: {market_count} active (last 24h)\n"
        f"Revenue opportunities: {revenue_count} found (last 24h)\n"
        f"Issues: {', '.join(str(i) for i in issues)}\n"
        f"Actions taken overnight: {len(restarts)} restarts, {len(alerts_sent)} alerts"
    )

    send_info(brief, channel="general")
    return {"ok": True, "brief": brief}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
