#!/usr/bin/env python3
"""
Kael Memory API — Central Brain Memory Service
FastAPI on port 8765 | SQLite at /home/zero/hivecode_sandbox/kael_memory.db
Runs on ZeroDESK (100.77.113.48) — current central node
"""

import sqlite3
import json
import os
from datetime import datetime, timezone
from contextlib import contextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
import uvicorn

DB_PATH = "/home/zero/hivecode_sandbox/kael_memory.db"
MACHINE = "ZeroDESK"
VERSION = "1.0.0"
START_TIME = datetime.now(timezone.utc).isoformat()

app = FastAPI(title="Kael Memory API", version=VERSION)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def _init_db():
    """Create tables if they don't exist."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS kael_memory (
            key       TEXT PRIMARY KEY,
            value     TEXT,
            updated_at TIMESTAMP,
            machine   TEXT
        );

        CREATE TABLE IF NOT EXISTS agent_state (
            agent_id  TEXT PRIMARY KEY,
            state     TEXT,
            last_seen TIMESTAMP,
            machine   TEXT
        );

        CREATE TABLE IF NOT EXISTS heartbeats (
            machine        TEXT PRIMARY KEY,
            last_heartbeat TIMESTAMP,
            status         TEXT,
            details        TEXT
        );

        CREATE TABLE IF NOT EXISTS context (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            context  TEXT,
            saved_at TIMESTAMP,
            machine  TEXT
        );
    """)
    conn.commit()
    conn.close()


@contextmanager
def get_db():
    """Yield a sqlite3 connection with Row factory, auto-close."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    try:
        yield conn
    finally:
        conn.close()


def _now():
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

@app.on_event("startup")
def startup():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    _init_db()


# ---------------------------------------------------------------------------
# Health & Status
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    try:
        with get_db() as conn:
            conn.execute("SELECT 1")
        return {"status": "healthy", "db": DB_PATH}
    except Exception as e:
        return JSONResponse(status_code=503, content={"status": "unhealthy", "error": str(e)})


@app.get("/status")
def status():
    with get_db() as conn:
        mem_count = conn.execute("SELECT COUNT(*) FROM kael_memory").fetchone()[0]
        agent_count = conn.execute("SELECT COUNT(*) FROM agent_state").fetchone()[0]
        hb_count = conn.execute("SELECT COUNT(*) FROM heartbeats").fetchone()[0]
        ctx_count = conn.execute("SELECT COUNT(*) FROM context").fetchone()[0]

        heartbeats = [dict(r) for r in conn.execute(
            "SELECT machine, last_heartbeat, status FROM heartbeats ORDER BY last_heartbeat DESC"
        ).fetchall()]

        agents = [dict(r) for r in conn.execute(
            "SELECT agent_id, last_seen, machine FROM agent_state ORDER BY last_seen DESC"
        ).fetchall()]

    return {
        "service": "Kael Memory API",
        "version": VERSION,
        "machine": MACHINE,
        "started_at": START_TIME,
        "now": _now(),
        "db_path": DB_PATH,
        "counts": {
            "memories": mem_count,
            "agents": agent_count,
            "heartbeats": hb_count,
            "contexts": ctx_count,
        },
        "heartbeats": heartbeats,
        "agents": agents,
    }


# ---------------------------------------------------------------------------
# Memory CRUD
# ---------------------------------------------------------------------------

@app.get("/memory/{key}")
def get_memory(key: str):
    with get_db() as conn:
        row = conn.execute("SELECT * FROM kael_memory WHERE key = ?", (key,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"Memory key '{key}' not found")
    return dict(row)


@app.post("/memory/{key}")
async def set_memory(key: str, request: Request):
    body = await request.json()
    value = body.get("value")
    machine = body.get("machine", MACHINE)
    if value is None:
        raise HTTPException(status_code=400, detail="'value' is required")

    # Serialize non-string values to JSON
    if not isinstance(value, str):
        value = json.dumps(value)

    now = _now()
    with get_db() as conn:
        conn.execute(
            "INSERT INTO kael_memory (key, value, updated_at, machine) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=?, updated_at=?, machine=?",
            (key, value, now, machine, value, now, machine),
        )
        conn.commit()
    return {"key": key, "value": value, "updated_at": now, "machine": machine}


@app.delete("/memory/{key}")
def delete_memory(key: str):
    with get_db() as conn:
        cur = conn.execute("DELETE FROM kael_memory WHERE key = ?", (key,))
        conn.commit()
    if cur.rowcount == 0:
        raise HTTPException(status_code=404, detail=f"Memory key '{key}' not found")
    return {"deleted": key}


# ---------------------------------------------------------------------------
# Agent State
# ---------------------------------------------------------------------------

@app.get("/agent/{agent_id}")
def get_agent(agent_id: str):
    with get_db() as conn:
        row = conn.execute("SELECT * FROM agent_state WHERE agent_id = ?", (agent_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"Agent '{agent_id}' not found")
    return dict(row)


@app.post("/agent/{agent_id}")
async def update_agent(agent_id: str, request: Request):
    body = await request.json()
    state = body.get("state")
    machine = body.get("machine", MACHINE)
    if state is None:
        raise HTTPException(status_code=400, detail="'state' is required")

    if not isinstance(state, str):
        state = json.dumps(state)

    now = _now()
    with get_db() as conn:
        conn.execute(
            "INSERT INTO agent_state (agent_id, state, last_seen, machine) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT(agent_id) DO UPDATE SET state=?, last_seen=?, machine=?",
            (agent_id, state, now, machine, state, now, machine),
        )
        conn.commit()
    return {"agent_id": agent_id, "state": state, "last_seen": now, "machine": machine}


# ---------------------------------------------------------------------------
# Heartbeats
# ---------------------------------------------------------------------------

@app.post("/heartbeat/{machine}")
async def post_heartbeat(machine: str, request: Request):
    body = {}
    try:
        body = await request.json()
    except Exception:
        pass

    status_val = body.get("status", "alive")
    details = body.get("details", "")
    if not isinstance(details, str):
        details = json.dumps(details)

    now = _now()
    with get_db() as conn:
        conn.execute(
            "INSERT INTO heartbeats (machine, last_heartbeat, status, details) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT(machine) DO UPDATE SET last_heartbeat=?, status=?, details=?",
            (machine, now, status_val, details, now, status_val, details),
        )
        conn.commit()
    return {"machine": machine, "last_heartbeat": now, "status": status_val}


@app.get("/heartbeats")
def get_heartbeats():
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM heartbeats ORDER BY last_heartbeat DESC"
        ).fetchall()
    return {"heartbeats": [dict(r) for r in rows]}


# ---------------------------------------------------------------------------
# Context (Kael session context persistence)
# ---------------------------------------------------------------------------

@app.get("/context")
def get_context():
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM context ORDER BY id DESC LIMIT 1"
        ).fetchone()
    if not row:
        return {"context": None, "message": "No context saved yet"}
    return dict(row)


@app.post("/context")
async def save_context(request: Request):
    body = await request.json()
    context = body.get("context")
    machine = body.get("machine", MACHINE)
    if context is None:
        raise HTTPException(status_code=400, detail="'context' is required")

    now = _now()
    with get_db() as conn:
        conn.execute(
            "INSERT INTO context (context, saved_at, machine) VALUES (?, ?, ?)",
            (context, now, machine),
        )
        conn.commit()
    return {"saved_at": now, "machine": machine, "length": len(context)}


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8765, log_level="info")
