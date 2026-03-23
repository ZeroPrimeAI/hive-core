#!/usr/bin/env python3
"""
Agent Council — Shared communication layer for all Hive agents.
FastAPI server on port 8766. SQLite at /home/zero/hivecode_sandbox/council.db
"""

import json
import sqlite3
import time
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import uvicorn

DB_PATH = "/home/zero/hivecode_sandbox/council.db"
VALID_CHANNELS = {"general", "revenue", "ops", "comms", "alerts", "strategy", "insights"}
VALID_MESSAGE_TYPES = {"info", "request", "alert", "decision", "question", "reply", "status", "directive"}

app = FastAPI(title="Agent Council", version="1.0.0")

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


@contextmanager
def db_connection():
    conn = get_db()
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    with db_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS agent_council (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                posted_at TEXT NOT NULL,
                agent_id TEXT NOT NULL,
                agent_type TEXT NOT NULL DEFAULT 'service',
                machine TEXT NOT NULL DEFAULT 'unknown',
                channel TEXT NOT NULL,
                message_type TEXT NOT NULL DEFAULT 'info',
                message TEXT NOT NULL,
                addressed_to TEXT,
                reply_to INTEGER,
                read_by TEXT NOT NULL DEFAULT '[]',
                resolved INTEGER NOT NULL DEFAULT 0
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_council_channel ON agent_council(channel)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_council_posted ON agent_council(posted_at)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_council_reply ON agent_council(reply_to)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_council_resolved ON agent_council(resolved)
        """)


def row_to_dict(row: sqlite3.Row) -> dict:
    d = dict(row)
    # Parse read_by from JSON string to list
    if "read_by" in d:
        try:
            d["read_by"] = json.loads(d["read_by"])
        except (json.JSONDecodeError, TypeError):
            d["read_by"] = []
    return d


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------

class PostMessage(BaseModel):
    agent_id: str
    agent_type: str = "service"
    machine: str = "unknown"
    channel: str
    message_type: str = "info"
    message: str
    addressed_to: Optional[str] = None
    reply_to: Optional[int] = None


class ResolveRequest(BaseModel):
    agent_id: str


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    try:
        with db_connection() as conn:
            row = conn.execute("SELECT COUNT(*) AS cnt FROM agent_council").fetchone()
            total = row["cnt"]
        return {"status": "healthy", "total_messages": total, "db": DB_PATH}
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "unhealthy", "error": str(e)})


@app.post("/council/post")
def post_message(body: PostMessage):
    if body.channel not in VALID_CHANNELS:
        raise HTTPException(400, f"Invalid channel '{body.channel}'. Valid: {sorted(VALID_CHANNELS)}")
    if body.message_type not in VALID_MESSAGE_TYPES:
        raise HTTPException(400, f"Invalid message_type '{body.message_type}'. Valid: {sorted(VALID_MESSAGE_TYPES)}")
    if not body.message.strip():
        raise HTTPException(400, "Message cannot be empty")

    now = datetime.now(timezone.utc).isoformat()

    with db_connection() as conn:
        # Validate reply_to exists if provided
        if body.reply_to is not None:
            parent = conn.execute("SELECT id FROM agent_council WHERE id = ?", (body.reply_to,)).fetchone()
            if not parent:
                raise HTTPException(404, f"reply_to message {body.reply_to} not found")

        cur = conn.execute("""
            INSERT INTO agent_council (posted_at, agent_id, agent_type, machine, channel, message_type, message, addressed_to, reply_to, read_by, resolved)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '[]', 0)
        """, (now, body.agent_id, body.agent_type, body.machine, body.channel,
              body.message_type, body.message, body.addressed_to, body.reply_to))
        msg_id = cur.lastrowid

    return {"ok": True, "id": msg_id, "posted_at": now, "channel": body.channel}


@app.get("/council/read/{agent_id}")
def read_messages(agent_id: str, channel: Optional[str] = Query(None), limit: int = Query(100, ge=1, le=500)):
    """Get unread messages for an agent, then mark them as read."""
    if channel and channel not in VALID_CHANNELS:
        raise HTTPException(400, f"Invalid channel '{channel}'. Valid: {sorted(VALID_CHANNELS)}")

    with db_connection() as conn:
        # Find messages not read by this agent
        # A message is unread if agent_id is NOT in the read_by JSON array
        # Also include messages addressed directly to this agent even if broadcast
        if channel:
            rows = conn.execute("""
                SELECT * FROM agent_council
                WHERE channel = ?
                  AND agent_id != ?
                  AND read_by NOT LIKE ?
                ORDER BY posted_at DESC
                LIMIT ?
            """, (channel, agent_id, f'%"{agent_id}"%', limit)).fetchall()
        else:
            rows = conn.execute("""
                SELECT * FROM agent_council
                WHERE agent_id != ?
                  AND read_by NOT LIKE ?
                ORDER BY posted_at DESC
                LIMIT ?
            """, (agent_id, f'%"{agent_id}"%', limit)).fetchall()

        messages = [row_to_dict(r) for r in rows]

        # Mark all fetched messages as read by this agent
        for msg in messages:
            read_by = msg["read_by"]
            if agent_id not in read_by:
                read_by.append(agent_id)
                conn.execute(
                    "UPDATE agent_council SET read_by = ? WHERE id = ?",
                    (json.dumps(read_by), msg["id"])
                )

    return {"agent_id": agent_id, "unread_count": len(messages), "messages": messages}


@app.get("/council/channel/{channel}")
def get_channel(channel: str, limit: int = Query(50, ge=1, le=500)):
    if channel not in VALID_CHANNELS:
        raise HTTPException(400, f"Invalid channel '{channel}'. Valid: {sorted(VALID_CHANNELS)}")

    with db_connection() as conn:
        rows = conn.execute("""
            SELECT * FROM agent_council
            WHERE channel = ?
            ORDER BY posted_at DESC
            LIMIT ?
        """, (channel, limit)).fetchall()

    return {"channel": channel, "count": len(rows), "messages": [row_to_dict(r) for r in rows]}


@app.get("/council/thread/{message_id}")
def get_thread(message_id: int):
    with db_connection() as conn:
        # Get the root message
        root = conn.execute("SELECT * FROM agent_council WHERE id = ?", (message_id,)).fetchone()
        if not root:
            raise HTTPException(404, f"Message {message_id} not found")

        # Find the true root (walk up reply_to chain)
        root_id = message_id
        current = root
        while current["reply_to"] is not None:
            parent = conn.execute("SELECT * FROM agent_council WHERE id = ?", (current["reply_to"],)).fetchone()
            if not parent:
                break
            root_id = parent["id"]
            current = parent

        # Get all messages in thread (root + all replies recursively)
        thread_ids = {root_id}
        queue = [root_id]
        all_messages = []

        # Get root
        root_msg = conn.execute("SELECT * FROM agent_council WHERE id = ?", (root_id,)).fetchone()
        if root_msg:
            all_messages.append(row_to_dict(root_msg))

        # BFS to find all replies
        while queue:
            parent_id = queue.pop(0)
            replies = conn.execute(
                "SELECT * FROM agent_council WHERE reply_to = ? ORDER BY posted_at ASC",
                (parent_id,)
            ).fetchall()
            for r in replies:
                if r["id"] not in thread_ids:
                    thread_ids.add(r["id"])
                    all_messages.append(row_to_dict(r))
                    queue.append(r["id"])

        # Sort by posted_at
        all_messages.sort(key=lambda m: m["posted_at"])

    return {"thread_root": root_id, "message_count": len(all_messages), "messages": all_messages}


@app.get("/council/status")
def get_status():
    with db_connection() as conn:
        # Total messages
        total = conn.execute("SELECT COUNT(*) AS cnt FROM agent_council").fetchone()["cnt"]

        # Messages per channel
        channel_rows = conn.execute("""
            SELECT channel, COUNT(*) AS cnt FROM agent_council GROUP BY channel ORDER BY cnt DESC
        """).fetchall()
        channels = {r["channel"]: r["cnt"] for r in channel_rows}

        # Active agents (posted in last 24 hours)
        active_rows = conn.execute("""
            SELECT DISTINCT agent_id, agent_type, machine,
                   MAX(posted_at) AS last_seen
            FROM agent_council
            WHERE posted_at > datetime('now', '-24 hours')
            GROUP BY agent_id
            ORDER BY last_seen DESC
        """).fetchall()
        active_agents = [
            {"agent_id": r["agent_id"], "agent_type": r["agent_type"],
             "machine": r["machine"], "last_seen": r["last_seen"]}
            for r in active_rows
        ]

        # Unresolved count
        unresolved = conn.execute(
            "SELECT COUNT(*) AS cnt FROM agent_council WHERE resolved = 0 AND message_type IN ('request', 'alert', 'question')"
        ).fetchone()["cnt"]

    return {
        "total_messages": total,
        "channels": channels,
        "active_agents_24h": len(active_agents),
        "active_agents": active_agents,
        "unresolved_items": unresolved,
        "valid_channels": sorted(VALID_CHANNELS),
        "valid_message_types": sorted(VALID_MESSAGE_TYPES),
    }


@app.post("/council/resolve/{message_id}")
def resolve_message(message_id: int, body: ResolveRequest):
    with db_connection() as conn:
        row = conn.execute("SELECT * FROM agent_council WHERE id = ?", (message_id,)).fetchone()
        if not row:
            raise HTTPException(404, f"Message {message_id} not found")
        if row["resolved"]:
            return {"ok": True, "already_resolved": True, "id": message_id}

        conn.execute("UPDATE agent_council SET resolved = 1 WHERE id = ?", (message_id,))

    return {"ok": True, "resolved_by": body.agent_id, "id": message_id}


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

@app.on_event("startup")
def on_startup():
    init_db()
    # Post the boot message
    now = datetime.now(timezone.utc).isoformat()
    with db_connection() as conn:
        conn.execute("""
            INSERT INTO agent_council (posted_at, agent_id, agent_type, machine, channel, message_type, message, addressed_to, reply_to, read_by, resolved)
            VALUES (?, 'council-api', 'infrastructure', 'ZeroDESK', 'general', 'status', 'Council API online. All agents can now communicate.', NULL, NULL, '[]', 0)
        """, (now,))
    print(f"[Council] DB initialized at {DB_PATH}")
    print(f"[Council] Boot message posted to #general")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8766, log_level="info")
