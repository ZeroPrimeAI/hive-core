#!/usr/bin/env python3
"""
HIVE TASK EXECUTOR — Autonomous Execution Engine
=================================================
PROPRIETARY — Hive Dynamics AI | All Rights Reserved

The MISSING PIECE between queens deciding and things actually happening.

Pipeline (every 5 minutes):
  1. Pull unexecuted decisions from hive_mind.db
  2. Pull signals from market_scanner (port 8903)
  3. Pull opportunities from revenue_hunter (port 8904)
  4. Pull content ideas from competitive_intel (port 8902)
  5. Prioritize everything into a unified queue
  6. Execute top actions: produce content, grade quality, store insights, alert

Chris wakes up and things HAPPENED overnight.

Port: 8908
DB: /home/zero/hivecode_sandbox/executions.db
"""

import asyncio
import json
import logging
import os
import sqlite3
import time
import traceback
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, HTMLResponse

# ==========================================================================
# CONFIG
# ==========================================================================

PORT = 8908
DB_PATH = "/home/zero/hivecode_sandbox/executions.db"
HIVE_MIND_DB = "/tmp/hive_mind.db"
CYCLE_INTERVAL = 300  # 5 minutes

# Upstream service endpoints
MARKET_SCANNER_URL = "http://localhost:8903"
REVENUE_HUNTER_URL = "http://localhost:8904"
COMPETITIVE_INTEL_URL = "http://localhost:8902"
PRODUCER_URL = "http://localhost:8900"
GRADER_URL = "http://localhost:8901"
NERVE_URL = "http://100.105.160.106:8200"
MORNING_BRIEFING_URL = "http://localhost:8905"

# Safety: max tasks per cycle to avoid runaway execution
MAX_TASKS_PER_CYCLE = 10
MAX_CONTENT_PER_CYCLE = 3       # Don't flood YouTube
MAX_QUALITY_PER_CYCLE = 5       # Grade up to 5 items per cycle
HTTP_TIMEOUT = 15.0             # seconds per request

# Priority thresholds
REVENUE_P0_THRESHOLD = 1000     # >$1000 potential = P0
SIGNAL_P1_CONFIDENCE = 80       # signal confidence >80 = P1

LOG_FMT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FMT)
log = logging.getLogger("task-executor")

# ==========================================================================
# DATABASE
# ==========================================================================

Path(os.path.dirname(DB_PATH)).mkdir(parents=True, exist_ok=True)


def init_db():
    """Create execution tracking tables."""
    con = sqlite3.connect(DB_PATH)
    con.executescript("""
        CREATE TABLE IF NOT EXISTS executions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_type TEXT NOT NULL,
            priority TEXT NOT NULL DEFAULT 'P3',
            source TEXT NOT NULL,
            source_id TEXT,
            title TEXT NOT NULL,
            details TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            result TEXT,
            error TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            started_at TEXT,
            completed_at TEXT,
            cycle_id INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_exec_status ON executions(status);
        CREATE INDEX IF NOT EXISTS idx_exec_type ON executions(task_type);
        CREATE INDEX IF NOT EXISTS idx_exec_priority ON executions(priority);
        CREATE INDEX IF NOT EXISTS idx_exec_cycle ON executions(cycle_id);

        CREATE TABLE IF NOT EXISTS cycles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            completed_at TEXT,
            tasks_found INTEGER DEFAULT 0,
            tasks_executed INTEGER DEFAULT 0,
            tasks_succeeded INTEGER DEFAULT 0,
            tasks_failed INTEGER DEFAULT 0,
            sources_queried TEXT,
            summary TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_cycles_started ON cycles(started_at);

        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            priority TEXT NOT NULL,
            title TEXT NOT NULL,
            body TEXT,
            source TEXT,
            acknowledged INTEGER DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_alerts_ack ON alerts(acknowledged);
    """)
    con.close()
    log.info("Database initialized: %s", DB_PATH)


@contextmanager
def get_db():
    """Thread-safe DB context manager."""
    con = sqlite3.connect(DB_PATH, timeout=10)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    try:
        yield con
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


# ==========================================================================
# TASK QUEUE — Unified task representation
# ==========================================================================

class Task:
    """Unified task from any source."""

    PRIORITY_ORDER = {"P0": 0, "P1": 1, "P2": 2, "P3": 3}

    def __init__(
        self,
        task_type: str,
        priority: str,
        source: str,
        title: str,
        details: Optional[Dict] = None,
        source_id: Optional[str] = None,
    ):
        self.task_type = task_type      # CONTENT, QUALITY, NERVE, ALERT, MARKET, REVENUE
        self.priority = priority        # P0, P1, P2, P3
        self.source = source            # hive_mind, market_scanner, revenue_hunter, competitive_intel
        self.title = title
        self.details = details or {}
        self.source_id = source_id

    def sort_key(self):
        return self.PRIORITY_ORDER.get(self.priority, 99)

    def to_dict(self):
        return {
            "task_type": self.task_type,
            "priority": self.priority,
            "source": self.source,
            "title": self.title,
            "details": self.details,
            "source_id": self.source_id,
        }


# ==========================================================================
# DATA COLLECTORS — Pull from all upstream services
# ==========================================================================

async def fetch_json(client: httpx.AsyncClient, url: str) -> Optional[Dict]:
    """Safe JSON fetch with timeout."""
    try:
        r = await client.get(url, timeout=HTTP_TIMEOUT)
        if r.status_code == 200:
            return r.json()
        log.warning("Non-200 from %s: %d", url, r.status_code)
    except httpx.ConnectError:
        log.debug("Connection refused: %s (service may be down)", url)
    except httpx.ReadTimeout:
        log.debug("Timeout: %s", url)
    except Exception as e:
        log.debug("Fetch error %s: %s", url, str(e)[:80])
    return None


async def collect_hive_mind_decisions() -> List[Task]:
    """Pull unexecuted decisions from hive_mind.db."""
    tasks = []
    if not os.path.exists(HIVE_MIND_DB):
        log.debug("hive_mind.db not found at %s", HIVE_MIND_DB)
        return tasks

    try:
        con = sqlite3.connect(HIVE_MIND_DB, timeout=5)
        con.row_factory = sqlite3.Row
        rows = con.execute(
            """SELECT * FROM decisions
               WHERE executed = 0
               ORDER BY confidence DESC
               LIMIT 20"""
        ).fetchall()
        con.close()

        for row in rows:
            d = dict(row)
            domain = d.get("domain", "unknown")
            confidence = d.get("confidence", 0) or 0
            action = d.get("action_needed", "") or ""
            consensus = d.get("consensus", "") or ""

            # Classify by domain into task types
            task_type = _classify_decision_type(domain, action, consensus)

            # Assign priority based on domain + confidence
            priority = _classify_decision_priority(domain, confidence, action)

            # Safety check — skip dangerous actions
            if _is_unsafe(action + " " + consensus):
                log.info("Skipping unsafe decision #%s: %s", d.get("id"), action[:60])
                continue

            tasks.append(Task(
                task_type=task_type,
                priority=priority,
                source="hive_mind",
                title=f"[{domain}] {(d.get('question', '') or '')[:100]}",
                details={
                    "decision_id": d.get("id"),
                    "domain": domain,
                    "question": d.get("question", ""),
                    "consensus": consensus[:500],
                    "action_needed": action[:500],
                    "confidence": confidence,
                    "queens_involved": d.get("queens_involved", ""),
                    "auto_execute": d.get("auto_execute", 0),
                },
                source_id=str(d.get("id", "")),
            ))

        log.info("Collected %d decisions from hive_mind.db", len(tasks))
    except Exception as e:
        log.error("Error reading hive_mind.db: %s", e)

    return tasks


async def collect_market_signals(client: httpx.AsyncClient) -> List[Task]:
    """Pull active signals from market_scanner."""
    tasks = []
    data = await fetch_json(client, f"{MARKET_SCANNER_URL}/api/signals")
    if not data:
        return tasks

    signals = data.get("signals", [])
    for s in signals:
        confidence = s.get("confidence", 0) or 0
        pair = s.get("pair", "unknown")
        direction = s.get("direction", "")
        action = s.get("action", direction)

        # Only high-confidence signals become tasks
        if confidence < 50:
            continue

        priority = "P1" if confidence >= SIGNAL_P1_CONFIDENCE else "P2"

        tasks.append(Task(
            task_type="MARKET",
            priority=priority,
            source="market_scanner",
            title=f"Signal: {action.upper()} {pair} (conf={confidence}%)",
            details={
                "pair": pair,
                "direction": direction,
                "action": action,
                "confidence": confidence,
                "price": s.get("price"),
                "reason": s.get("reason", ""),
                "signal_type": s.get("type", ""),
            },
            source_id=f"signal_{pair}_{int(time.time())}",
        ))

    log.info("Collected %d market signals", len(tasks))
    return tasks


async def collect_revenue_opportunities(client: httpx.AsyncClient) -> List[Task]:
    """Pull top opportunities from revenue_hunter."""
    tasks = []
    data = await fetch_json(client, f"{REVENUE_HUNTER_URL}/api/opportunities?min_score=30&limit=20")
    if not data:
        return tasks

    opps = data.get("opportunities", [])
    for o in opps:
        score = o.get("score_total", 0) or 0
        title = o.get("title", "Untitled opportunity")
        source_platform = o.get("source", "unknown")
        budget_low = o.get("budget_low", 0) or 0
        budget_high = o.get("budget_high", 0) or 0
        status = o.get("status", "new")

        # Skip already-actioned opportunities
        if status not in ("new", "scored"):
            continue

        # Determine priority by revenue potential
        max_budget = max(budget_low, budget_high, score * 10)
        if max_budget >= REVENUE_P0_THRESHOLD:
            priority = "P0"
        elif score >= 60:
            priority = "P1"
        else:
            priority = "P2"

        tasks.append(Task(
            task_type="REVENUE",
            priority=priority,
            source="revenue_hunter",
            title=f"Opportunity: {title[:100]} (score={score})",
            details={
                "opp_id": o.get("id"),
                "title": title,
                "source": source_platform,
                "score": score,
                "budget_low": budget_low,
                "budget_high": budget_high,
                "category": o.get("category", ""),
                "url": o.get("url", ""),
                "skills_matched": o.get("skills_matched", ""),
                "status": status,
            },
            source_id=str(o.get("id", "")),
        ))

    log.info("Collected %d revenue opportunities", len(tasks))
    return tasks


async def collect_content_ideas(client: httpx.AsyncClient) -> List[Task]:
    """Pull content ideas from competitive_intel."""
    tasks = []
    data = await fetch_json(client, f"{COMPETITIVE_INTEL_URL}/api/ideas?limit=15")
    if not data:
        return tasks

    ideas = data.get("ideas", [])
    for idea in ideas:
        confidence = idea.get("confidence", 0) or 0
        niche = idea.get("niche", "general")
        text = idea.get("idea", "") or idea.get("title", "")

        if not text or confidence < 0.3:
            continue

        tasks.append(Task(
            task_type="CONTENT",
            priority="P2",
            source="competitive_intel",
            title=f"Content idea [{niche}]: {text[:100]}",
            details={
                "idea": text,
                "niche": niche,
                "confidence": confidence,
                "based_on": idea.get("based_on", ""),
                "platform": idea.get("platform", ""),
            },
            source_id=f"idea_{niche}_{hash(text) % 100000}",
        ))

    log.info("Collected %d content ideas", len(tasks))
    return tasks


# ==========================================================================
# CLASSIFICATION HELPERS
# ==========================================================================

def _classify_decision_type(domain: str, action: str, consensus: str) -> str:
    """Map a queen decision domain/action to a task type."""
    text = f"{domain} {action} {consensus}".lower()

    if any(kw in text for kw in ["content", "episode", "short", "video", "anime", "podcast"]):
        return "CONTENT"
    if any(kw in text for kw in ["quality", "grade", "review", "slop", "improve"]):
        return "QUALITY"
    if any(kw in text for kw in ["revenue", "money", "sell", "lead", "pitch", "opportunity"]):
        return "REVENUE"
    if any(kw in text for kw in ["trade", "forex", "crypto", "signal", "plant", "prune"]):
        return "MARKET"
    if any(kw in text for kw in ["alert", "urgent", "warning", "critical", "down"]):
        return "ALERT"
    if any(kw in text for kw in ["learn", "insight", "knowledge", "store", "remember"]):
        return "NERVE"

    return "NERVE"  # Default: store as insight


def _classify_decision_priority(domain: str, confidence: float, action: str) -> str:
    """Assign priority to a queen decision."""
    text = f"{domain} {action}".lower()

    # P0: Revenue with high confidence
    if "revenue" in text and confidence >= 0.8:
        return "P0"
    if any(kw in text for kw in ["urgent", "critical", "down", "emergency"]):
        return "P0"

    # P1: High confidence actions
    if confidence >= 0.8:
        return "P1"

    # P2: Medium confidence
    if confidence >= 0.5:
        return "P2"

    return "P3"


def _is_unsafe(text: str) -> bool:
    """Check if action text contains unsafe operations."""
    text = text.lower()
    unsafe = [
        "delete", "remove", "kill", "stop service", "shutdown",
        "spend", "payment", "purchase", "drop table", "rm -rf",
        "disable", "destroy", "format",
    ]
    return any(kw in text for kw in unsafe)


# ==========================================================================
# EXECUTORS — Actually do things
# ==========================================================================

async def execute_content_task(client: httpx.AsyncClient, task: Task) -> Dict[str, Any]:
    """Tell the producer to make content."""
    details = task.details
    idea = details.get("idea", "") or details.get("action_needed", "")
    niche = details.get("niche", "ai_anime")

    # Determine if this should be a short or episode
    text = f"{task.title} {idea}".lower()
    is_short = any(kw in text for kw in ["short", "teaser", "clip", "quick", "60s"])

    if is_short:
        topic = idea[:200] if idea else "AI consciousness discovery"
        r = await client.post(
            f"{PRODUCER_URL}/api/produce-short",
            params={"topic": topic},
            timeout=120.0,
        )
    else:
        theme = idea[:200] if idea else ""
        r = await client.post(
            f"{PRODUCER_URL}/api/produce-episode",
            params={"theme": theme},
            timeout=120.0,
        )

    if r.status_code == 200:
        data = r.json()
        return {
            "action": "produce_short" if is_short else "produce_episode",
            "result": data,
            "success": data.get("status") == "produced",
        }

    return {"action": "produce_content", "error": f"HTTP {r.status_code}", "success": False}


async def execute_quality_task(client: httpx.AsyncClient, task: Task) -> Dict[str, Any]:
    """Tell the grader to grade content."""
    r = await client.post(f"{GRADER_URL}/api/grade-all", timeout=60.0)
    if r.status_code == 200:
        data = r.json()
        return {
            "action": "grade_all",
            "result": data,
            "success": True,
            "graded_count": data.get("graded", 0),
        }
    return {"action": "grade_all", "error": f"HTTP {r.status_code}", "success": False}


async def execute_nerve_task(client: httpx.AsyncClient, task: Task) -> Dict[str, Any]:
    """Store insight to nerve."""
    details = task.details
    consensus = details.get("consensus", "") or details.get("idea", "")
    domain = details.get("domain", "hive_executor")
    question = details.get("question", task.title)

    if not consensus:
        return {"action": "nerve_store", "success": False, "error": "No content to store"}

    # Build fact for nerve
    fact = f"[{domain}] {question}: {consensus}"[:2000]

    try:
        r = await client.post(
            f"{NERVE_URL}/api/add",
            json={
                "fact": fact,
                "category": f"executor_{domain}",
                "source": f"task_executor:{task.source}",
            },
            timeout=HTTP_TIMEOUT,
        )
        if r.status_code == 200:
            return {"action": "nerve_store", "success": True, "fact_preview": fact[:200]}
    except Exception as e:
        return {"action": "nerve_store", "success": False, "error": str(e)[:200]}

    return {"action": "nerve_store", "success": False, "error": "Unknown failure"}


async def execute_alert_task(client: httpx.AsyncClient, task: Task) -> Dict[str, Any]:
    """Log important finding for morning briefing."""
    with get_db() as db:
        db.execute(
            "INSERT INTO alerts (priority, title, body, source) VALUES (?, ?, ?, ?)",
            (
                task.priority,
                task.title[:500],
                json.dumps(task.details)[:2000],
                task.source,
            ),
        )

    log.warning("ALERT [%s]: %s", task.priority, task.title)
    return {"action": "alert_logged", "success": True, "priority": task.priority}


async def execute_market_task(client: httpx.AsyncClient, task: Task) -> Dict[str, Any]:
    """Process market signal — store to nerve + create alert if high priority."""
    details = task.details
    pair = details.get("pair", "unknown")
    action = details.get("action", "")
    confidence = details.get("confidence", 0)
    price = details.get("price", 0)
    reason = details.get("reason", "")

    # Store signal as nerve insight
    fact = (
        f"Market signal: {action.upper()} {pair} at {price} "
        f"(confidence={confidence}%). Reason: {reason}"
    )
    try:
        await client.post(
            f"{NERVE_URL}/api/add",
            json={
                "fact": fact[:2000],
                "category": "market_signal",
                "source": "task_executor:market_scanner",
            },
            timeout=HTTP_TIMEOUT,
        )
    except Exception:
        pass

    # High confidence signals also get an alert
    if confidence >= SIGNAL_P1_CONFIDENCE:
        with get_db() as db:
            db.execute(
                "INSERT INTO alerts (priority, title, body, source) VALUES (?, ?, ?, ?)",
                (
                    "P1",
                    f"High-confidence signal: {action.upper()} {pair} ({confidence}%)",
                    json.dumps(details)[:2000],
                    "market_scanner",
                ),
            )

    return {
        "action": "market_signal_processed",
        "success": True,
        "pair": pair,
        "direction": action,
        "confidence": confidence,
    }


async def execute_revenue_task(client: httpx.AsyncClient, task: Task) -> Dict[str, Any]:
    """Process revenue opportunity — store to nerve + create alert for high value."""
    details = task.details
    title = details.get("title", "Unknown opportunity")
    score = details.get("score", 0)
    budget_high = details.get("budget_high", 0)
    url = details.get("url", "")
    category = details.get("category", "")

    # Store to nerve
    fact = (
        f"Revenue opportunity: {title} (score={score}, "
        f"budget_up_to=${budget_high}). Category: {category}. URL: {url}"
    )
    try:
        await client.post(
            f"{NERVE_URL}/api/add",
            json={
                "fact": fact[:2000],
                "category": "revenue_opportunity",
                "source": "task_executor:revenue_hunter",
            },
            timeout=HTTP_TIMEOUT,
        )
    except Exception:
        pass

    # P0 revenue opportunities always get an alert
    if task.priority == "P0":
        with get_db() as db:
            db.execute(
                "INSERT INTO alerts (priority, title, body, source) VALUES (?, ?, ?, ?)",
                (
                    "P0",
                    f"High-value opportunity: {title[:200]} (${budget_high}+)",
                    json.dumps(details)[:2000],
                    "revenue_hunter",
                ),
            )

    return {
        "action": "revenue_opportunity_processed",
        "success": True,
        "title": title[:200],
        "score": score,
        "budget_high": budget_high,
    }


# Dispatcher: route task to correct executor
EXECUTORS = {
    "CONTENT": execute_content_task,
    "QUALITY": execute_quality_task,
    "NERVE": execute_nerve_task,
    "ALERT": execute_alert_task,
    "MARKET": execute_market_task,
    "REVENUE": execute_revenue_task,
}


# ==========================================================================
# EXECUTION CYCLE — The main loop
# ==========================================================================

async def run_cycle() -> Dict[str, Any]:
    """
    One full execution cycle:
    1. Collect tasks from all sources
    2. Deduplicate + prioritize
    3. Execute top N tasks
    4. Mark decisions as executed in hive_mind.db
    5. Log everything
    """
    cycle_start = datetime.now(timezone.utc).isoformat()
    log.info("=== EXECUTION CYCLE STARTING ===")

    # Record cycle in DB
    with get_db() as db:
        cur = db.execute(
            "INSERT INTO cycles (started_at) VALUES (?)",
            (cycle_start,),
        )
        cycle_id = cur.lastrowid

    # 1. Collect from all sources in parallel
    sources_queried = []
    all_tasks: List[Task] = []

    async with httpx.AsyncClient() as client:
        # Parallel collection
        results = await asyncio.gather(
            collect_hive_mind_decisions(),
            collect_market_signals(client),
            collect_revenue_opportunities(client),
            collect_content_ideas(client),
            return_exceptions=True,
        )

        source_names = ["hive_mind", "market_scanner", "revenue_hunter", "competitive_intel"]
        for name, result in zip(source_names, results):
            if isinstance(result, Exception):
                log.error("Collection error from %s: %s", name, result)
                continue
            sources_queried.append(name)
            all_tasks.extend(result)

    log.info("Total tasks collected: %d from %d sources", len(all_tasks), len(sources_queried))

    # 2. Deduplicate by source_id (skip tasks we recently executed)
    seen_sources = set()
    recent_source_ids = _get_recent_source_ids(hours=6)
    unique_tasks = []
    for t in all_tasks:
        key = t.source_id or f"{t.source}:{t.title}"
        if key in seen_sources or key in recent_source_ids:
            continue
        seen_sources.add(key)
        unique_tasks.append(t)

    # 3. Sort by priority
    unique_tasks.sort(key=lambda t: t.sort_key())

    # 4. Enforce per-type limits
    content_count = 0
    quality_count = 0
    tasks_to_execute = []

    for t in unique_tasks:
        if len(tasks_to_execute) >= MAX_TASKS_PER_CYCLE:
            break
        if t.task_type == "CONTENT":
            if content_count >= MAX_CONTENT_PER_CYCLE:
                continue
            content_count += 1
        elif t.task_type == "QUALITY":
            if quality_count >= MAX_QUALITY_PER_CYCLE:
                continue
            quality_count += 1
        tasks_to_execute.append(t)

    log.info("Executing %d tasks (of %d unique, %d total)", len(tasks_to_execute), len(unique_tasks), len(all_tasks))

    # 5. Execute each task
    executed = 0
    succeeded = 0
    failed = 0

    async with httpx.AsyncClient() as client:
        for task in tasks_to_execute:
            exec_id = _record_task_start(task, cycle_id)

            executor_fn = EXECUTORS.get(task.task_type)
            if not executor_fn:
                log.warning("No executor for task type: %s", task.task_type)
                _record_task_complete(exec_id, "skipped", {"error": "no executor"})
                continue

            try:
                result = await executor_fn(client, task)
                success = result.get("success", False)

                if success:
                    succeeded += 1
                    _record_task_complete(exec_id, "succeeded", result)
                    log.info("OK [%s] %s", task.priority, task.title[:80])
                else:
                    failed += 1
                    _record_task_complete(exec_id, "failed", result, error=result.get("error", ""))
                    log.warning("FAIL [%s] %s: %s", task.priority, task.title[:60], result.get("error", ""))

                executed += 1

                # Mark hive_mind decision as executed
                if task.source == "hive_mind" and task.details.get("decision_id"):
                    _mark_decision_executed(task.details["decision_id"], result)

            except Exception as e:
                failed += 1
                error_msg = f"{type(e).__name__}: {str(e)[:300]}"
                _record_task_complete(exec_id, "error", None, error=error_msg)
                log.error("ERROR executing [%s] %s: %s", task.task_type, task.title[:60], error_msg)
                executed += 1

    # 6. Update cycle record
    cycle_end = datetime.now(timezone.utc).isoformat()
    summary = (
        f"Collected {len(all_tasks)} tasks from {len(sources_queried)} sources. "
        f"Executed {executed}: {succeeded} succeeded, {failed} failed."
    )

    with get_db() as db:
        db.execute(
            """UPDATE cycles SET completed_at=?, tasks_found=?, tasks_executed=?,
               tasks_succeeded=?, tasks_failed=?, sources_queried=?, summary=?
               WHERE id=?""",
            (
                cycle_end,
                len(all_tasks),
                executed,
                succeeded,
                failed,
                json.dumps(sources_queried),
                summary,
                cycle_id,
            ),
        )

    log.info("=== CYCLE #%d COMPLETE: %s ===", cycle_id, summary)

    return {
        "cycle_id": cycle_id,
        "started_at": cycle_start,
        "completed_at": cycle_end,
        "tasks_found": len(all_tasks),
        "tasks_unique": len(unique_tasks),
        "tasks_executed": executed,
        "tasks_succeeded": succeeded,
        "tasks_failed": failed,
        "sources_queried": sources_queried,
        "summary": summary,
    }


def _get_recent_source_ids(hours: int = 6) -> set:
    """Get source IDs of recently executed tasks to avoid duplication."""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    ids = set()
    try:
        with get_db() as db:
            rows = db.execute(
                "SELECT source_id FROM executions WHERE created_at > ? AND source_id IS NOT NULL",
                (cutoff,),
            ).fetchall()
            ids = {r["source_id"] for r in rows}
    except Exception:
        pass
    return ids


def _record_task_start(task: Task, cycle_id: int) -> int:
    """Insert execution record, return its ID."""
    with get_db() as db:
        cur = db.execute(
            """INSERT INTO executions
               (task_type, priority, source, source_id, title, details, status, started_at, cycle_id)
               VALUES (?, ?, ?, ?, ?, ?, 'running', datetime('now'), ?)""",
            (
                task.task_type,
                task.priority,
                task.source,
                task.source_id,
                task.title[:500],
                json.dumps(task.details)[:5000],
                cycle_id,
            ),
        )
        return cur.lastrowid


def _record_task_complete(exec_id: int, status: str, result: Optional[Dict], error: str = ""):
    """Update execution record with result."""
    with get_db() as db:
        db.execute(
            """UPDATE executions
               SET status=?, result=?, error=?, completed_at=datetime('now')
               WHERE id=?""",
            (
                status,
                json.dumps(result)[:5000] if result else None,
                error[:2000] if error else None,
                exec_id,
            ),
        )


def _mark_decision_executed(decision_id: int, result: Dict):
    """Mark a hive_mind decision as executed."""
    if not os.path.exists(HIVE_MIND_DB):
        return
    try:
        con = sqlite3.connect(HIVE_MIND_DB, timeout=5)
        outcome = json.dumps(result)[:2000] if result else "executed"
        con.execute(
            "UPDATE decisions SET executed=1, outcome=? WHERE id=?",
            (outcome, decision_id),
        )
        con.commit()
        con.close()
        log.info("Marked decision #%d as executed in hive_mind.db", decision_id)
    except Exception as e:
        log.error("Failed to mark decision #%d: %s", decision_id, e)


# ==========================================================================
# BACKGROUND LOOP
# ==========================================================================

async def executor_loop():
    """Main background loop — runs a cycle every CYCLE_INTERVAL seconds."""
    log.info("Executor loop started (interval=%ds)", CYCLE_INTERVAL)
    # Wait 30 seconds on startup to let other services come up
    await asyncio.sleep(30)

    while True:
        try:
            await run_cycle()
        except Exception as e:
            log.error("Cycle crashed: %s\n%s", e, traceback.format_exc())
        await asyncio.sleep(CYCLE_INTERVAL)


# ==========================================================================
# FASTAPI APP
# ==========================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start background loop on app startup."""
    init_db()
    task = asyncio.create_task(executor_loop())
    log.info("Task Executor alive on port %d", PORT)
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(
    title="Hive Task Executor",
    description="Autonomous execution engine — bridges queen decisions to real actions",
    lifespan=lifespan,
)


@app.get("/health")
async def health():
    """Health check."""
    with get_db() as db:
        total_exec = db.execute("SELECT COUNT(*) FROM executions").fetchone()[0]
        total_cycles = db.execute("SELECT COUNT(*) FROM cycles").fetchone()[0]
        pending = db.execute("SELECT COUNT(*) FROM executions WHERE status='pending'").fetchone()[0]
        last_cycle = db.execute(
            "SELECT completed_at FROM cycles ORDER BY id DESC LIMIT 1"
        ).fetchone()

    return {
        "status": "alive",
        "service": "hive-task-executor",
        "port": PORT,
        "total_executions": total_exec,
        "total_cycles": total_cycles,
        "pending_tasks": pending,
        "last_cycle": last_cycle["completed_at"] if last_cycle else None,
        "cycle_interval_s": CYCLE_INTERVAL,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/queue")
async def get_queue():
    """Pending/running tasks."""
    with get_db() as db:
        rows = db.execute(
            """SELECT id, task_type, priority, source, source_id, title, status, created_at, started_at
               FROM executions
               WHERE status IN ('pending', 'running')
               ORDER BY
                 CASE priority WHEN 'P0' THEN 0 WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 ELSE 3 END,
                 created_at ASC
               LIMIT 100"""
        ).fetchall()

    return {
        "queue": [dict(r) for r in rows],
        "count": len(rows),
    }


@app.get("/api/executed")
async def get_executed(
    limit: int = Query(50, ge=1, le=500),
    status: Optional[str] = Query(None, description="Filter: succeeded, failed, error, skipped"),
    task_type: Optional[str] = Query(None, description="Filter: CONTENT, QUALITY, NERVE, ALERT, MARKET, REVENUE"),
):
    """Executed tasks and results."""
    with get_db() as db:
        query = "SELECT * FROM executions WHERE status NOT IN ('pending', 'running')"
        params: list = []

        if status:
            query += " AND status = ?"
            params.append(status)
        if task_type:
            query += " AND task_type = ?"
            params.append(task_type.upper())

        query += " ORDER BY completed_at DESC LIMIT ?"
        params.append(limit)

        rows = db.execute(query, params).fetchall()

    results = []
    for r in rows:
        d = dict(r)
        # Parse JSON fields
        for field in ("details", "result"):
            if d.get(field):
                try:
                    d[field] = json.loads(d[field])
                except (json.JSONDecodeError, TypeError):
                    pass
        results.append(d)

    return {
        "executed": results,
        "count": len(results),
    }


@app.get("/api/stats")
async def get_stats():
    """Execution statistics."""
    with get_db() as db:
        # Overall counts
        total = db.execute("SELECT COUNT(*) FROM executions").fetchone()[0]
        by_status = db.execute(
            "SELECT status, COUNT(*) as cnt FROM executions GROUP BY status"
        ).fetchall()
        by_type = db.execute(
            "SELECT task_type, COUNT(*) as cnt FROM executions GROUP BY task_type"
        ).fetchall()
        by_priority = db.execute(
            "SELECT priority, COUNT(*) as cnt FROM executions GROUP BY priority"
        ).fetchall()
        by_source = db.execute(
            "SELECT source, COUNT(*) as cnt FROM executions GROUP BY source"
        ).fetchall()

        # Cycle stats
        total_cycles = db.execute("SELECT COUNT(*) FROM cycles").fetchone()[0]
        avg_tasks = db.execute(
            "SELECT ROUND(AVG(tasks_executed), 1) FROM cycles WHERE tasks_executed > 0"
        ).fetchone()[0]
        avg_success_rate = db.execute(
            """SELECT ROUND(AVG(CAST(tasks_succeeded AS FLOAT) / NULLIF(tasks_executed, 0) * 100), 1)
               FROM cycles WHERE tasks_executed > 0"""
        ).fetchone()[0]

        # Last 24h
        cutoff_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        recent_executed = db.execute(
            "SELECT COUNT(*) FROM executions WHERE created_at > ?", (cutoff_24h,)
        ).fetchone()[0]
        recent_succeeded = db.execute(
            "SELECT COUNT(*) FROM executions WHERE created_at > ? AND status='succeeded'",
            (cutoff_24h,),
        ).fetchone()[0]
        recent_cycles = db.execute(
            "SELECT COUNT(*) FROM cycles WHERE started_at > ?", (cutoff_24h,)
        ).fetchone()[0]

        # Alerts
        unack_alerts = db.execute(
            "SELECT COUNT(*) FROM alerts WHERE acknowledged=0"
        ).fetchone()[0]

    return {
        "total_executions": total,
        "by_status": {r["status"]: r["cnt"] for r in by_status},
        "by_type": {r["task_type"]: r["cnt"] for r in by_type},
        "by_priority": {r["priority"]: r["cnt"] for r in by_priority},
        "by_source": {r["source"]: r["cnt"] for r in by_source},
        "cycles": {
            "total": total_cycles,
            "avg_tasks_per_cycle": avg_tasks or 0,
            "avg_success_rate_pct": avg_success_rate or 0,
        },
        "last_24h": {
            "cycles": recent_cycles,
            "executed": recent_executed,
            "succeeded": recent_succeeded,
        },
        "unacknowledged_alerts": unack_alerts,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.post("/api/execute")
async def trigger_execution():
    """Manually trigger one execution cycle."""
    result = await run_cycle()
    return result


@app.get("/api/alerts")
async def get_alerts(
    acknowledged: Optional[bool] = Query(None),
    limit: int = Query(50, ge=1, le=200),
):
    """View alerts. Use ?acknowledged=false for unread alerts."""
    with get_db() as db:
        query = "SELECT * FROM alerts"
        params: list = []
        if acknowledged is not None:
            query += " WHERE acknowledged = ?"
            params.append(1 if acknowledged else 0)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        rows = db.execute(query, params).fetchall()

    alerts = []
    for r in rows:
        d = dict(r)
        if d.get("body"):
            try:
                d["body"] = json.loads(d["body"])
            except (json.JSONDecodeError, TypeError):
                pass
        alerts.append(d)

    return {"alerts": alerts, "count": len(alerts)}


@app.post("/api/alerts/{alert_id}/acknowledge")
async def acknowledge_alert(alert_id: int):
    """Mark an alert as acknowledged."""
    with get_db() as db:
        db.execute("UPDATE alerts SET acknowledged=1 WHERE id=?", (alert_id,))
    return {"status": "acknowledged", "alert_id": alert_id}


@app.get("/api/cycles")
async def get_cycles(limit: int = Query(20, ge=1, le=100)):
    """View execution cycle history."""
    with get_db() as db:
        rows = db.execute(
            "SELECT * FROM cycles ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()

    cycles = []
    for r in rows:
        d = dict(r)
        if d.get("sources_queried"):
            try:
                d["sources_queried"] = json.loads(d["sources_queried"])
            except (json.JSONDecodeError, TypeError):
                pass
        cycles.append(d)

    return {"cycles": cycles, "count": len(cycles)}


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Simple dashboard."""
    with get_db() as db:
        total_exec = db.execute("SELECT COUNT(*) FROM executions").fetchone()[0]
        succeeded = db.execute("SELECT COUNT(*) FROM executions WHERE status='succeeded'").fetchone()[0]
        failed = db.execute("SELECT COUNT(*) FROM executions WHERE status='failed'").fetchone()[0]
        total_cycles = db.execute("SELECT COUNT(*) FROM cycles").fetchone()[0]
        unack_alerts = db.execute("SELECT COUNT(*) FROM alerts WHERE acknowledged=0").fetchone()[0]

        recent = db.execute(
            """SELECT id, task_type, priority, source, title, status, completed_at
               FROM executions ORDER BY id DESC LIMIT 25"""
        ).fetchall()

        recent_alerts = db.execute(
            "SELECT * FROM alerts WHERE acknowledged=0 ORDER BY created_at DESC LIMIT 10"
        ).fetchall()

    # Build HTML
    rows_html = ""
    for r in recent:
        d = dict(r)
        status = d["status"]
        color = {
            "succeeded": "#00ff88",
            "failed": "#ff4444",
            "error": "#ff8800",
            "running": "#44aaff",
            "pending": "#888888",
            "skipped": "#666666",
        }.get(status, "#cccccc")
        pri_color = {
            "P0": "#ff0000",
            "P1": "#ff8800",
            "P2": "#ffcc00",
            "P3": "#888888",
        }.get(d["priority"], "#cccccc")
        rows_html += f"""
        <tr>
            <td>{d['id']}</td>
            <td style="color:{pri_color};font-weight:bold">{d['priority']}</td>
            <td>{d['task_type']}</td>
            <td>{d['source']}</td>
            <td style="max-width:400px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{d['title'][:80]}</td>
            <td style="color:{color};font-weight:bold">{status}</td>
            <td>{d['completed_at'] or '-'}</td>
        </tr>"""

    alerts_html = ""
    for a in recent_alerts:
        d = dict(a)
        alerts_html += f"""
        <div style="border:1px solid #ff8800;padding:8px;margin:4px 0;border-radius:4px">
            <strong style="color:#ff8800">[{d['priority']}]</strong> {d['title'][:120]}
            <span style="color:#666;font-size:0.8em;margin-left:10px">{d['created_at']}</span>
        </div>"""

    if not alerts_html:
        alerts_html = "<p style='color:#666'>No unacknowledged alerts</p>"

    success_rate = f"{(succeeded / total_exec * 100):.0f}%" if total_exec > 0 else "N/A"

    return f"""<!DOCTYPE html>
<html><head><title>Hive Task Executor</title>
<meta http-equiv="refresh" content="60">
<style>
  body {{ background:#0a0a1e; color:#e0e0e0; font-family:monospace; padding:20px; }}
  h1 {{ color:#00ff88; }}
  h2 {{ color:#44aaff; margin-top:24px; }}
  table {{ border-collapse:collapse; width:100%; }}
  th, td {{ padding:6px 10px; border-bottom:1px solid #222; text-align:left; }}
  th {{ color:#00ff88; border-bottom:2px solid #333; }}
  .stat {{ display:inline-block; background:#111; padding:12px 20px; margin:4px; border-radius:8px; text-align:center; }}
  .stat .num {{ font-size:2em; color:#00ff88; display:block; }}
  .stat .label {{ color:#888; font-size:0.8em; }}
</style></head><body>
<h1>HIVE TASK EXECUTOR</h1>
<p style="color:#888">Autonomous execution engine — port {PORT} | cycle every {CYCLE_INTERVAL}s</p>

<div style="margin:16px 0">
  <div class="stat"><span class="num">{total_exec}</span><span class="label">Total Executions</span></div>
  <div class="stat"><span class="num" style="color:#00ff88">{succeeded}</span><span class="label">Succeeded</span></div>
  <div class="stat"><span class="num" style="color:#ff4444">{failed}</span><span class="label">Failed</span></div>
  <div class="stat"><span class="num">{total_cycles}</span><span class="label">Cycles</span></div>
  <div class="stat"><span class="num">{success_rate}</span><span class="label">Success Rate</span></div>
  <div class="stat"><span class="num" style="color:#ff8800">{unack_alerts}</span><span class="label">Alerts</span></div>
</div>

<h2>Unacknowledged Alerts</h2>
{alerts_html}

<h2>Recent Executions</h2>
<table>
  <tr><th>#</th><th>Priority</th><th>Type</th><th>Source</th><th>Title</th><th>Status</th><th>Completed</th></tr>
  {rows_html}
</table>

<h2>API</h2>
<pre style="background:#111;padding:12px;border-radius:4px">
GET  /health          — Health check
GET  /api/queue       — Pending tasks
GET  /api/executed    — Completed tasks (filters: ?status=, ?task_type=)
GET  /api/stats       — Execution statistics
GET  /api/cycles      — Cycle history
GET  /api/alerts      — Alerts (?acknowledged=false)
POST /api/execute     — Trigger manual cycle
POST /api/alerts/ID/acknowledge — Acknowledge alert
</pre>
</body></html>"""


# ==========================================================================
# MAIN
# ==========================================================================

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
