#!/usr/bin/env python3
"""
Queen Executor — Bridge between Orion's Belt decisions and actual execution.
Reads queen decisions from hive_mind.db, converts to tasks in sandbox.
The agentic loop picks them up and executes safe ones.
"""
import sqlite3
import json
import os
import time
from pathlib import Path

DB = "/tmp/hive_mind.db"
SANDBOX = "/home/zero/hivecode_sandbox/projects"
LOG = "/home/zero/logs/queen_executor.log"

Path(SANDBOX).mkdir(parents=True, exist_ok=True)
Path("/home/zero/logs").mkdir(exist_ok=True)

def log(msg):
    with open(LOG, "a") as f:
        f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}\n")

def get_pending_decisions(limit=5):
    """Get unexecuted auto_execute decisions."""
    try:
        conn = sqlite3.connect(DB)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM decisions WHERE auto_execute=1 AND executed=0 ORDER BY confidence DESC LIMIT ?",
            (limit,)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        log(f"DB error: {e}")
        return []

def classify_safety(decision):
    """Classify if a decision is safe to auto-execute."""
    text = ((decision.get("consensus") or "") + (decision.get("action_needed") or "")).lower()

    # UNSAFE — needs human review
    unsafe_keywords = ["delete", "remove", "kill", "stop service", "shutdown", "spend", "payment", "purchase"]
    for kw in unsafe_keywords:
        if kw in text:
            return "UNSAFE"

    # SAFE — can auto-execute
    safe_keywords = ["analyze", "check", "monitor", "report", "generate", "create content", "train", "optimize"]
    for kw in safe_keywords:
        if kw in text:
            return "SAFE"

    return "REVIEW"  # Needs human look

def decision_to_task(decision):
    """Convert a queen decision to a sandbox task file."""
    task = {
        "id": decision["id"],
        "domain": decision.get("domain", "unknown"),
        "question": decision.get("question", ""),
        "consensus": decision.get("consensus", "")[:500],
        "confidence": decision.get("confidence", 0),
        "action_needed": decision.get("action_needed", ""),
        "safety": classify_safety(decision),
        "queens_involved": decision.get("queens_involved", ""),
        "created": decision.get("created_at", ""),
        "status": "pending"
    }

    filename = f"task_{decision['id']}_{decision.get('domain', 'unknown')}.json"
    filepath = os.path.join(SANDBOX, filename)

    with open(filepath, "w") as f:
        json.dump(task, f, indent=2)

    return filepath

def mark_executed(decision_id):
    """Mark a decision as executed in the database."""
    try:
        conn = sqlite3.connect(DB)
        conn.execute("UPDATE decisions SET executed=1 WHERE id=?", (decision_id,))
        conn.commit()
        conn.close()
    except:
        pass

def process_pending(limit=5):
    """Process pending queen decisions into sandbox tasks."""
    decisions = get_pending_decisions(limit)
    if not decisions:
        log("No pending decisions")
        return 0

    created = 0
    for d in decisions:
        filepath = decision_to_task(d)
        mark_executed(d["id"])
        safety = classify_safety(d)
        log(f"Task created: {os.path.basename(filepath)} [{safety}] conf={d.get('confidence', 0)}")
        created += 1

    log(f"Processed {created} queen decisions → sandbox tasks")
    return created

if __name__ == "__main__":
    import sys
    if "--once" in sys.argv:
        n = process_pending(10)
        print(f"Processed {n} decisions")
    elif "--stats" in sys.argv:
        conn = sqlite3.connect(DB)
        total = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
        auto = conn.execute("SELECT COUNT(*) FROM decisions WHERE auto_execute=1").fetchone()[0]
        executed = conn.execute("SELECT COUNT(*) FROM decisions WHERE executed=1").fetchone()[0]
        pending = conn.execute("SELECT COUNT(*) FROM decisions WHERE auto_execute=1 AND executed=0").fetchone()[0]
        conn.close()
        print(f"Total: {total} | Auto-execute: {auto} | Executed: {executed} | Pending: {pending}")
    else:
        print("Usage: queen_executor.py --once | --stats")
