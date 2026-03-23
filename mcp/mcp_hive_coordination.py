#!/usr/bin/env python3
"""
Hive Coordination MCP Server
==============================
MCP tools for Claude Code sessions to coordinate through the central
coordination service on ZeroQ (port 8733).

Each Claude Code session uses these tools to:
- Register itself and announce what it's working on
- See what other sessions are doing
- Claim tasks to prevent duplicate work
- Share findings/context across sessions
- Send messages between sessions

Usage in Claude Code settings.json:
{
    "mcpServers": {
        "hive-coordination": {
            "command": "python3",
            "args": ["/home/zero/mcp_hive_coordination.py"]
        }
    }
}
"""

import json
import os
import socket
import subprocess
import urllib.request
import urllib.error
from typing import Optional

from mcp.server.fastmcp import FastMCP

# ─── Configuration ──────────────────────────────────────────────────────────

COORDINATOR_HOST = "100.70.226.103"
COORDINATOR_PORT = 8733
COORDINATOR_URL = f"http://{COORDINATOR_HOST}:{COORDINATOR_PORT}"

# Detect which machine we're on
HOSTNAME = socket.gethostname()
MACHINE_MAP = {
    "ZeroDESK": "ZeroDESK",
    "zerodesk": "ZeroDESK",
    "ZeroQ": "ZeroQ",
    "zeroq": "ZeroQ",
    "ZeroZI": "ZeroZI",
    "zerozi": "ZeroZI",
    "ZeroNovo": "ZeroNovo",
    "zeronovo": "ZeroNovo",
    "ZeroG7": "ZeroG7",
    "zerog7": "ZeroG7",
}
MACHINE_NAME = MACHINE_MAP.get(HOSTNAME, HOSTNAME)

# Session state (persisted in memory for this MCP server instance)
_session_id: Optional[str] = None

mcp = FastMCP(
    "hive-coordination",
    instructions=(
        "Hive Coordination tools for connecting Claude Code sessions across the mesh. "
        "Register your session first, then use heartbeat to stay active. "
        "Claim tasks before starting work to prevent duplicates. "
        "Share findings so other sessions can benefit."
    ),
)


# ─── HTTP Helpers ───────────────────────────────────────────────────────────

def _api_call(method: str, path: str, data: dict = None) -> dict:
    """Make an HTTP request to the coordination service."""
    url = f"{COORDINATOR_URL}{path}"
    headers = {"Content-Type": "application/json"}

    if data is not None:
        body = json.dumps(data).encode("utf-8")
        req = urllib.request.Request(url, data=body, headers=headers, method=method)
    else:
        req = urllib.request.Request(url, headers=headers, method=method)

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8") if e.fp else str(e)
        try:
            error_json = json.loads(error_body)
            return {"error": error_json.get("detail", str(e)), "status_code": e.code}
        except Exception:
            return {"error": error_body, "status_code": e.code}
    except urllib.error.URLError as e:
        return {"error": f"Cannot reach coordination service at {COORDINATOR_URL}: {e.reason}"}
    except Exception as e:
        return {"error": f"Request failed: {str(e)}"}


def _get(path: str) -> dict:
    return _api_call("GET", path)


def _post(path: str, data: dict) -> dict:
    return _api_call("POST", path, data)


# ─── MCP Tools ──────────────────────────────────────────────────────────────

@mcp.tool()
def register_session(machine: str = "", description: str = "") -> str:
    """Register this Claude Code session with the Hive coordination service.
    Call this at the start of every session so other sessions know you exist.

    Args:
        machine: Machine name (auto-detected if empty). E.g. ZeroDESK, ZeroQ, ZeroZI
        description: What this session plans to work on
    """
    global _session_id
    machine = machine or MACHINE_NAME

    result = _post("/register", {
        "machine": machine,
        "description": description,
        "ip_address": "",
    })

    if "error" in result:
        return f"ERROR: {result['error']}"

    _session_id = result.get("session_id", "")
    active = result.get("active_sessions", [])

    lines = [
        f"Registered as: {_session_id}",
        f"Machine: {machine}",
        "",
        f"Active sessions ({len(active)}):",
    ]
    for s in active:
        task = s.get("current_task", "") or "idle"
        lines.append(f"  - {s['id']} ({s['machine']}): {task}")

    if result.get("tip"):
        lines.append(f"\n{result['tip']}")

    return "\n".join(lines)


@mcp.tool()
def heartbeat(current_task: str = "", context_summary: str = "") -> str:
    """Send a heartbeat to keep this session active and update what you're working on.
    Call this periodically (every 5 min) or whenever you switch tasks.

    Args:
        current_task: Brief description of what you're currently doing
        context_summary: Longer summary of session context/state
    """
    global _session_id
    if not _session_id:
        return "ERROR: Not registered. Call register_session() first."

    result = _post("/heartbeat", {
        "session_id": _session_id,
        "current_task": current_task,
        "context_summary": context_summary,
    })

    if "error" in result:
        return f"ERROR: {result['error']}"

    unread = result.get("unread_messages", 0)
    msg = "Heartbeat sent."
    if unread > 0:
        msg += f" You have {unread} unread message(s) -- use read_messages() to see them."
    return msg


@mcp.tool()
def list_sessions(include_inactive: bool = False) -> str:
    """See all active Claude Code sessions and what they're doing.
    Use this to understand what's happening across the mesh before starting work.

    Args:
        include_inactive: Also show sessions that have timed out
    """
    path = "/sessions"
    if include_inactive:
        path += "?include_inactive=true"
    result = _get(path)

    if "error" in result:
        return f"ERROR: {result['error']}"

    sessions = result.get("sessions", [])
    if not sessions:
        return "No active sessions. You might be the first one!"

    lines = [f"Active Claude Code Sessions ({len(sessions)}):"]
    lines.append("-" * 60)

    for s in sessions:
        status_icon = "[ACTIVE]" if s["status"] == "active" else "[INACTIVE]"
        task = s.get("current_task", "") or "idle"
        desc = s.get("description", "")
        lines.append(f"{status_icon} {s['id']}")
        lines.append(f"  Machine: {s['machine']} | Last seen: {s['minutes_ago']}m ago")
        lines.append(f"  Working on: {task}")
        if desc:
            lines.append(f"  Description: {desc}")
        if s.get("context_summary"):
            lines.append(f"  Context: {s['context_summary']}")
        lines.append("")

    return "\n".join(lines)


@mcp.tool()
def claim_task(description: str) -> str:
    """Claim a task so no other session duplicates the work.
    ALWAYS claim before starting significant work.

    Args:
        description: What you're about to work on (be specific)
    """
    global _session_id
    if not _session_id:
        return "ERROR: Not registered. Call register_session() first."

    result = _post("/claim-task", {
        "session_id": _session_id,
        "description": description,
    })

    if "error" in result:
        return f"ERROR: {result['error']}"

    if result.get("warning") == "POTENTIAL DUPLICATE":
        existing = result.get("existing_task", {})
        return (
            f"WARNING: POTENTIAL DUPLICATE WORK DETECTED!\n"
            f"Existing task: {existing.get('description', '?')}\n"
            f"  Claimed by: {existing.get('session_id', '?')} on {existing.get('machine', '?')}\n"
            f"  Overlap: {', '.join(result.get('overlap_words', []))}\n\n"
            f"If this is genuinely different work, use claim_task_force() instead."
        )

    task_id = result.get("task_id", "")
    return f"Task claimed: {task_id}\nDescription: {description}\nNo other session will duplicate this work."


@mcp.tool()
def claim_task_force(description: str) -> str:
    """Force-claim a task even if a duplicate warning was raised.
    Use only after confirming your work is genuinely different.

    Args:
        description: What you're working on
    """
    global _session_id
    if not _session_id:
        return "ERROR: Not registered. Call register_session() first."

    result = _post("/claim-task-force", {
        "session_id": _session_id,
        "description": description,
    })

    if "error" in result:
        return f"ERROR: {result['error']}"

    return f"Task force-claimed: {result.get('task_id', '')}\nDescription: {description}"


@mcp.tool()
def complete_task(task_id: str, result: str = "") -> str:
    """Mark a claimed task as done.

    Args:
        task_id: The task ID returned by claim_task
        result: Summary of what was accomplished
    """
    global _session_id
    if not _session_id:
        return "ERROR: Not registered. Call register_session() first."

    resp = _post("/complete", {
        "session_id": _session_id,
        "task_id": task_id,
        "result": result,
    })

    if "error" in resp:
        return f"ERROR: {resp['error']}"

    return f"Task {task_id} marked as completed."


@mcp.tool()
def share_finding(key: str, value: str) -> str:
    """Share a discovery, result, or piece of context with all sessions.
    Other sessions can read this with read_shared().

    Args:
        key: Short identifier (e.g. 'port-8733-status', 'training-results', 'bug-found-in-nerve')
        value: The information to share (can be multi-line)
    """
    global _session_id
    if not _session_id:
        return "ERROR: Not registered. Call register_session() first."

    result = _post("/share", {
        "session_id": _session_id,
        "key": key,
        "value": value,
    })

    if "error" in result:
        return f"ERROR: {result['error']}"

    return f"Shared '{key}' with all sessions."


@mcp.tool()
def read_shared(since_minutes: int = 0) -> str:
    """Read all shared context from all sessions.
    Use this to see what other sessions have discovered.

    Args:
        since_minutes: Only show items updated in the last N minutes (0 = all)
    """
    path = "/shared"
    if since_minutes > 0:
        path += f"?since_minutes={since_minutes}"
    result = _get(path)

    if "error" in result:
        return f"ERROR: {result['error']}"

    items = result.get("shared", [])
    if not items:
        return "No shared context yet."

    lines = [f"Shared Context ({len(items)} items):"]
    lines.append("-" * 60)

    for item in items:
        lines.append(f"[{item['key']}] from {item['source_machine']} ({item['updated_at']})")
        lines.append(f"  {item['value']}")
        lines.append("")

    return "\n".join(lines)


@mcp.tool()
def send_message(to_session: str = "", message: str = "") -> str:
    """Send a message to another Claude Code session, or broadcast to all.

    Args:
        to_session: Target session ID (leave empty to broadcast to ALL sessions)
        message: The message to send
    """
    global _session_id
    if not _session_id:
        return "ERROR: Not registered. Call register_session() first."

    if not message:
        return "ERROR: Message cannot be empty."

    data = {
        "from_session": _session_id,
        "message": message,
    }
    if to_session:
        data["to_session"] = to_session

    result = _post("/message", data)

    if "error" in result:
        return f"ERROR: {result['error']}"

    target = to_session if to_session else "ALL sessions (broadcast)"
    return f"Message sent to {target}: {result.get('message_id', '')}"


@mcp.tool()
def read_messages(unread_only: bool = True) -> str:
    """Read messages sent to this session.

    Args:
        unread_only: Only show unread messages (default True)
    """
    global _session_id
    if not _session_id:
        return "ERROR: Not registered. Call register_session() first."

    path = f"/messages/{_session_id}"
    if not unread_only:
        path += "?unread_only=false"
    result = _get(path)

    if "error" in result:
        return f"ERROR: {result['error']}"

    messages = result.get("messages", [])
    if not messages:
        return "No messages." if unread_only else "No messages in history."

    lines = [f"Messages ({len(messages)}):"]
    lines.append("-" * 60)

    for m in messages:
        broadcast = " [BROADCAST]" if m.get("is_broadcast") else ""
        lines.append(f"From: {m['from_session']} ({m['from_machine']}){broadcast}")
        lines.append(f"  {m['message']}")
        lines.append(f"  Sent: {m['created_at']}")
        lines.append("")

    return "\n".join(lines)


@mcp.tool()
def whats_happening() -> str:
    """Comprehensive view of the entire coordination mesh.
    Shows all sessions, all active tasks, recent shared context, and unread messages.
    Use this to get a complete picture before deciding what to work on.
    """
    global _session_id

    status = _get("/status")
    if "error" in status:
        return f"ERROR: {status['error']}"

    overview = status.get("overview", {})
    sessions = status.get("active_sessions", [])
    tasks = status.get("recent_tasks", [])

    lines = [
        "=== HIVE COORDINATION STATUS ===",
        "",
        f"Active Sessions: {overview.get('active_sessions', 0)}",
        f"Total Sessions:  {overview.get('total_sessions', 0)}",
        f"Active Tasks:    {overview.get('claimed_tasks', 0)}",
        f"Completed Tasks: {overview.get('completed_tasks', 0)}",
        f"Shared Context:  {overview.get('shared_context_items', 0)}",
        f"Unread Messages: {overview.get('unread_messages', 0)}",
        "",
    ]

    if sessions:
        lines.append("--- ACTIVE SESSIONS ---")
        for s in sessions:
            task = s.get("current_task", "") or "idle"
            lines.append(f"  [{s['machine']}] {s['id']}: {task} ({s['minutes_ago']}m ago)")
        lines.append("")

    if tasks:
        lines.append("--- RECENT TASKS ---")
        for t in tasks:
            lines.append(f"  [{t['status']}] {t['machine']}: {t['description']}")
        lines.append("")

    # Get shared context
    shared = _get("/shared")
    shared_items = shared.get("shared", []) if "error" not in shared else []
    if shared_items:
        lines.append("--- SHARED CONTEXT ---")
        for item in shared_items[:10]:
            val = item["value"][:80] + "..." if len(item["value"]) > 80 else item["value"]
            lines.append(f"  [{item['key']}] ({item['source_machine']}): {val}")
        lines.append("")

    # Get messages for this session
    if _session_id:
        msgs = _get(f"/messages/{_session_id}")
        msg_list = msgs.get("messages", []) if "error" not in msgs else []
        if msg_list:
            lines.append("--- YOUR UNREAD MESSAGES ---")
            for m in msg_list[:5]:
                lines.append(f"  From {m['from_machine']}: {m['message'][:80]}")
            lines.append("")

    return "\n".join(lines)


if __name__ == "__main__":
    mcp.run()
