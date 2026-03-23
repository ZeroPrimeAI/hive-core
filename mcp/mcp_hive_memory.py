#!/usr/bin/env python3
"""
MCP Server: hive-memory
=======================
The most important MCP in the Hive. This server keeps Claude Code coherent
across sessions by providing deep access to:

  - Session history and achievements (MEMORY.md, session logs)
  - Chris's Director call transcripts and spoken directives
  - Full Hive state in one call (services, nerve, quality, GPU, errors)
  - Persistent insights database (architecture decisions, patterns, bugs)
  - CLAUDE.md architecture knowledge refresh
  - Full session startup checkin (replaces manual 8-step checklist)
  - Quick broken-things scan (what needs fixing RIGHT NOW)

Transport: stdio
Author: Claude Code + Chris (The Hive)
Created: 2026-03-15
"""

import json
import os
import re
import subprocess
import time
from datetime import datetime
from pathlib import Path
from mcp.server.fastmcp import FastMCP

# ── Server ────────────────────────────────────────────────────────────────────

mcp = FastMCP(
    name="hive-memory",
    instructions=(
        "Cross-session coherence engine for The Hive. "
        "Use these tools to understand what happened in previous sessions, "
        "what Chris said on Director calls, the current state of all systems, "
        "and to persist insights for future sessions. "
        "Start every new session with session_checkin. "
        "Use whats_broken when you need to find problems fast. "
        "Use save_insight whenever you discover something important."
    ),
)

# ── Constants ─────────────────────────────────────────────────────────────────

ZEROQ = "zero@100.70.226.103"
SSH_TIMEOUT = 30

CLAUDE_MD = "/home/zero/CLAUDE.md"
MEMORY_MD = "/home/zero/.claude/projects/-home-zero/memory/MEMORY.md"
INSIGHTS_FILE = "/home/zero/.claude/projects/-home-zero/memory/insights.md"
SESSION_LOGS_DIR = "/home/zero/.hive/hivecode_sessions"

NERVE_DB = "/THE_HIVE/memory/nerve.db"
DIRECTOR_COMMANDS_DIR = "/THE_HIVE/memory/director_commands"
INTERACTIVE_CALL_LOG = "/THE_HIVE/telephony"

# Valid insight categories
INSIGHT_CATEGORIES = [
    "architecture",
    "decision",
    "pattern",
    "bug",
    "chris_preference",
    "strategy",
    "revenue",
    "model",
    "security",
    "workflow",
]

# ── SSH / Shell Helpers ───────────────────────────────────────────────────────


def _ssh(host: str, command: str, timeout: int = SSH_TIMEOUT) -> tuple[str, str, int]:
    """Run a command on a remote machine via SSH."""
    try:
        result = subprocess.run(
            ["ssh", host, command],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return result.stdout, result.stderr, result.returncode
    except subprocess.TimeoutExpired:
        return "", f"SSH command timed out after {timeout}s", 1
    except Exception as e:
        return "", f"SSH error: {str(e)}", 1


def _ssh_zeroq(command: str, timeout: int = SSH_TIMEOUT) -> tuple[str, str, int]:
    """Run a command on ZeroQ via SSH."""
    return _ssh(ZEROQ, command, timeout)


def _curl_zeroq(url: str, timeout: int = 20) -> str:
    """Curl an API endpoint on ZeroQ via SSH."""
    cmd = f"curl -sf --connect-timeout 10 --max-time {timeout} '{url}'"
    stdout, stderr, rc = _ssh_zeroq(cmd, timeout=timeout + 15)
    if rc != 0 and not stdout:
        return f"ERROR: {stderr.strip() or 'connection failed'}"
    return stdout


def _local_cmd(command: str, timeout: int = 30) -> tuple[str, str, int]:
    """Run a local command."""
    try:
        result = subprocess.run(
            command, shell=True, capture_output=True, text=True, timeout=timeout
        )
        return result.stdout, result.stderr, result.returncode
    except subprocess.TimeoutExpired:
        return "", f"Command timed out after {timeout}s", 1
    except Exception as e:
        return "", f"Error: {str(e)}", 1


def _read_file(path: str) -> str:
    """Read a local file safely."""
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return f.read()
    except FileNotFoundError:
        return f"FILE_NOT_FOUND: {path}"
    except Exception as e:
        return f"READ_ERROR: {e}"


def _safe_json(raw: str) -> dict | list | None:
    """Parse JSON safely, return None on failure."""
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None


# ── Tool 1: read_session_history ──────────────────────────────────────────────


@mcp.tool(
    name="read_session_history",
    description=(
        "Read the last N session summaries from MEMORY.md and session log files. "
        "Returns structured session history: achievements, pending tasks, decisions made, "
        "services deployed, and current Hive grade. Use this at session start to understand "
        "what happened before this session."
    ),
)
def read_session_history(last_n_sessions: int = 5) -> str:
    """Parse MEMORY.md and session logs for recent history."""
    sections = []

    # ── Parse MEMORY.md ──
    memory_content = _read_file(MEMORY_MD)
    if memory_content.startswith("FILE_NOT_FOUND") or memory_content.startswith("READ_ERROR"):
        sections.append(f"MEMORY.md: {memory_content}")
    else:
        # Extract session sections (## Session NNN ...)
        session_pattern = re.compile(
            r"^(## Session \S+.*?)(?=^## |\Z)", re.MULTILINE | re.DOTALL
        )
        session_blocks = session_pattern.findall(memory_content)

        if session_blocks:
            # Take the last N
            recent = session_blocks[-last_n_sessions:]
            sections.append("=== SESSION HISTORY (from MEMORY.md) ===\n")
            for block in recent:
                sections.append(block.strip())
                sections.append("")  # blank line between sessions
        else:
            sections.append("=== MEMORY.md (no session blocks found, showing full) ===")
            # Show the whole thing but truncated
            sections.append(memory_content[:5000])

        # Extract pending tasks section
        pending_match = re.search(
            r"^## Pending Tasks\s*\n(.*?)(?=^## |\Z)",
            memory_content,
            re.MULTILINE | re.DOTALL,
        )
        if pending_match:
            sections.append("\n=== PENDING TASKS (from MEMORY.md) ===")
            sections.append(pending_match.group(1).strip())

        # Extract prime rules
        rules_match = re.search(
            r"^## Prime Rules\s*\n(.*?)(?=^## |\Z)",
            memory_content,
            re.MULTILINE | re.DOTALL,
        )
        if rules_match:
            sections.append("\n=== PRIME RULES ===")
            sections.append(rules_match.group(1).strip())

    # ── Parse session log files ──
    log_dir = Path(SESSION_LOGS_DIR)
    if log_dir.is_dir():
        log_files = sorted(log_dir.glob("*.json"), key=lambda p: p.name)
        recent_logs = log_files[-last_n_sessions:]
        if recent_logs:
            sections.append("\n=== RECENT SESSION LOGS ===")
            for log_file in recent_logs:
                try:
                    data = json.loads(log_file.read_text(encoding="utf-8", errors="replace"))
                    ts = log_file.stem  # timestamp from filename
                    sections.append(f"\n--- Session {ts} ---")
                    if isinstance(data, dict):
                        for key in ["summary", "status", "achievements", "errors", "notes"]:
                            if key in data:
                                val = data[key]
                                if isinstance(val, list):
                                    sections.append(f"  {key}:")
                                    for item in val[:10]:
                                        sections.append(f"    - {item}")
                                else:
                                    sections.append(f"  {key}: {val}")
                    else:
                        sections.append(f"  {str(data)[:500]}")
                except Exception as e:
                    sections.append(f"  Error reading {log_file.name}: {e}")

    # ── Check for session state file (from 80% handoff) ──
    state_file = "/tmp/session_state.md"
    if os.path.exists(state_file):
        state_age = time.time() - os.path.getmtime(state_file)
        if state_age < 3600:  # less than 1 hour old
            sections.append("\n=== PREVIOUS SESSION STATE (from 80% handoff) ===")
            sections.append(_read_file(state_file)[:3000])

    return "\n".join(sections)


# ── Tool 2: read_director_calls ───────────────────────────────────────────────


@mcp.tool(
    name="read_director_calls",
    description=(
        "Read Chris's Director call transcripts and action items. "
        "Checks director_commands/ on ZeroQ for speech transcripts and action items, "
        "plus interactive_call.py logs for recent call activity. "
        "Use this to know what Chris said on the phone — his spoken directives are LAW."
    ),
)
def read_director_calls(last_n: int = 10) -> str:
    """Read Director call transcripts and action items from ZeroQ."""
    sections = []

    # ── Check director_commands directory ──
    stdout, stderr, rc = _ssh_zeroq(
        f"ls -lt {DIRECTOR_COMMANDS_DIR}/ 2>/dev/null | head -20"
    )
    if rc == 0 and stdout.strip():
        sections.append("=== DIRECTOR COMMAND FILES ===")
        sections.append(stdout.strip())

        # Read the most recent command files
        file_stdout, _, _ = _ssh_zeroq(
            f"ls -t {DIRECTOR_COMMANDS_DIR}/*.json {DIRECTOR_COMMANDS_DIR}/*.txt 2>/dev/null | head -{last_n}"
        )
        if file_stdout.strip():
            for fpath in file_stdout.strip().split("\n"):
                fpath = fpath.strip()
                if not fpath:
                    continue
                content_out, _, content_rc = _ssh_zeroq(f"cat '{fpath}' 2>/dev/null | head -100")
                if content_rc == 0 and content_out.strip():
                    fname = os.path.basename(fpath)
                    sections.append(f"\n--- {fname} ---")
                    # Try to parse as JSON for structured output
                    parsed = _safe_json(content_out)
                    if parsed and isinstance(parsed, dict):
                        for key in ["action_items", "directives", "transcript", "speech", "text", "command"]:
                            if key in parsed:
                                sections.append(f"  {key}: {parsed[key]}")
                        # Show all other keys too
                        for key, val in parsed.items():
                            if key not in ["action_items", "directives", "transcript", "speech", "text", "command"]:
                                sections.append(f"  {key}: {str(val)[:200]}")
                    else:
                        sections.append(content_out.strip()[:1000])
    else:
        sections.append("Director commands directory: empty or not found")

    # ── Check director_monitor.db if it exists ──
    stdout, stderr, rc = _ssh_zeroq(
        "sqlite3 /THE_HIVE/memory/director_monitor.db "
        "\"SELECT * FROM action_items ORDER BY created_at DESC LIMIT 10\" 2>/dev/null"
    )
    if rc == 0 and stdout.strip():
        sections.append("\n=== DIRECTOR ACTION ITEMS (from DB) ===")
        sections.append(stdout.strip())

    # ── Check call_transcripts table in any DB ──
    stdout, stderr, rc = _ssh_zeroq(
        "sqlite3 /THE_HIVE/memory/director_monitor.db "
        "\"SELECT datetime, speaker, text FROM transcripts ORDER BY datetime DESC LIMIT 20\" 2>/dev/null"
    )
    if rc == 0 and stdout.strip():
        sections.append("\n=== RECENT CALL TRANSCRIPTS ===")
        sections.append(stdout.strip())

    # ── Check interactive_call logs ──
    stdout, stderr, rc = _ssh_zeroq(
        "journalctl -u hive-interactive-call -n 50 --no-pager --output=short-iso 2>/dev/null "
        "| grep -iE '(transcript|speech|director|action|command|chris)' | tail -20"
    )
    if rc == 0 and stdout.strip():
        sections.append("\n=== INTERACTIVE CALL LOG (relevant lines) ===")
        sections.append(stdout.strip())

    # ── Check for any .txt/.json in director_commands with recent timestamps ──
    stdout, stderr, rc = _ssh_zeroq(
        f"find {DIRECTOR_COMMANDS_DIR} -type f -mmin -1440 2>/dev/null | wc -l"
    )
    if rc == 0 and stdout.strip():
        count = stdout.strip()
        sections.append(f"\nDirector command files in last 24h: {count}")

    # ── Check nerve.db for Chris's directives logged there ──
    stdout, stderr, rc = _ssh_zeroq(
        f"sqlite3 {NERVE_DB} "
        "\"SELECT key || ': ' || substr(value, 1, 150) FROM knowledge "
        "WHERE category IN ('directive', 'chris', 'command', 'director') "
        "ORDER BY timestamp DESC LIMIT 15\" 2>/dev/null"
    )
    if rc == 0 and stdout.strip():
        sections.append("\n=== CHRIS'S DIRECTIVES (from nerve.db) ===")
        sections.append(stdout.strip())

    if not sections or all("not found" in s.lower() or "empty" in s.lower() for s in sections):
        sections.append(
            "\nNo Director call data found. "
            "This could mean: (1) No recent calls, (2) director_monitor.db not set up, "
            "or (3) transcripts stored in a different location. "
            "Check: ssh zero@100.70.226.103 'find /THE_HIVE -name \"*director*\" -o -name \"*transcript*\"'"
        )

    return "\n".join(sections)


# ── Tool 3: read_hive_state ───────────────────────────────────────────────────


@mcp.tool(
    name="read_hive_state",
    description=(
        "Comprehensive snapshot of the entire Hive in one call. Returns: "
        "service counts (running/failed), nerve facts + recent growth, quality grades, "
        "distillation pair counts by domain, GPU/RAM/disk status across all machines, "
        "cloud brain connectivity, recent errors, and latest autonomous agent actions. "
        "This is your single source of truth for the Hive's health."
    ),
)
def read_hive_state() -> str:
    """Gather comprehensive Hive state from all sources."""
    sections = [f"=== HIVE STATE SNAPSHOT === ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})\n"]

    # ── 1. Service counts ──
    stdout, _, rc = _ssh_zeroq(
        "systemctl list-units 'hive-*' --type=service --all --no-pager --no-legend"
    )
    if rc == 0 and stdout.strip():
        lines = [l.strip() for l in stdout.strip().split("\n") if l.strip()]
        running = sum(1 for l in lines if "running" in l)
        failed = sum(1 for l in lines if "failed" in l)
        dead = sum(1 for l in lines if "dead" in l and "failed" not in l)
        sections.append(f"SERVICES: {running} running, {failed} failed, {dead} dead, {len(lines)} total")
        if failed > 0:
            failed_names = [l.split()[0].replace(".service", "") for l in lines if "failed" in l]
            sections.append(f"  FAILED: {', '.join(failed_names)}")
    else:
        sections.append("SERVICES: unable to reach ZeroQ")

    # ── 2. Nerve facts ──
    stdout, _, rc = _ssh_zeroq(f"sqlite3 {NERVE_DB} 'SELECT count(*) FROM knowledge' 2>/dev/null")
    nerve_total = stdout.strip() if rc == 0 else "?"

    stdout, _, rc = _ssh_zeroq(
        f"sqlite3 {NERVE_DB} "
        "\"SELECT count(*) FROM knowledge WHERE timestamp > datetime('now', '-1 day')\" 2>/dev/null"
    )
    nerve_24h = stdout.strip() if rc == 0 else "?"

    stdout, _, rc = _ssh_zeroq(
        f"sqlite3 {NERVE_DB} "
        "\"SELECT count(*) FROM knowledge WHERE timestamp > datetime('now', '-1 hour')\" 2>/dev/null"
    )
    nerve_1h = stdout.strip() if rc == 0 else "?"
    sections.append(f"NERVE: {nerve_total} total facts, {nerve_24h} new in 24h, {nerve_1h} new in 1h")

    # ── 3. Quality grades ──
    raw = _curl_zeroq("http://localhost:8879/api/grades")
    if not raw.startswith("ERROR"):
        data = _safe_json(raw)
        if data and isinstance(data, dict):
            grades = data.get("grades", data)
            if isinstance(grades, dict):
                grade_summary = []
                for proc, info in sorted(grades.items()):
                    if isinstance(info, dict):
                        grade_summary.append(f"{proc}={info.get('grade', '?')}")
                    else:
                        grade_summary.append(f"{proc}={info}")
                sections.append(f"QUALITY: {', '.join(grade_summary[:15])}")
                if len(grade_summary) > 15:
                    sections.append(f"  ... and {len(grade_summary) - 15} more")
            else:
                sections.append(f"QUALITY: {str(grades)[:300]}")
        else:
            sections.append(f"QUALITY: {raw[:300]}")
    else:
        sections.append(f"QUALITY: tracker unreachable ({raw})")

    # ── 4. Distillation pairs ──
    raw = _curl_zeroq("http://localhost:8870/api/stats")
    if not raw.startswith("ERROR"):
        data = _safe_json(raw)
        if data:
            sections.append(f"DISTILLATION: {json.dumps(data)[:400]}")
        else:
            sections.append(f"DISTILLATION: {raw[:300]}")
    else:
        # Fall back to DB query
        stdout, _, rc = _ssh_zeroq(
            "sqlite3 /THE_HIVE/memory/distillation.db "
            "\"SELECT domain || ':' || count(*) FROM training_pairs GROUP BY domain\" 2>/dev/null"
        )
        if rc == 0 and stdout.strip():
            pairs = stdout.strip().replace("\n", ", ")
            sections.append(f"DISTILLATION: {pairs}")
        else:
            sections.append("DISTILLATION: unable to query")

    # ── 5. GPU status across machines ──
    # ZeroDESK (local)
    gpu_out, _, gpu_rc = _local_cmd(
        "nvidia-smi --query-gpu=name,memory.used,memory.total,utilization.gpu "
        "--format=csv,noheader,nounits 2>/dev/null"
    )
    if gpu_rc == 0 and gpu_out.strip():
        sections.append(f"GPU ZeroDESK: {gpu_out.strip()}")
    else:
        sections.append("GPU ZeroDESK: unable to query")

    # ZeroQ
    stdout, _, rc = _ssh_zeroq(
        "nvidia-smi --query-gpu=name,memory.used,memory.total,utilization.gpu "
        "--format=csv,noheader,nounits 2>/dev/null"
    )
    if rc == 0 and stdout.strip():
        sections.append(f"GPU ZeroQ: {stdout.strip()}")

    # ZeroZI
    stdout, _, rc = _ssh(
        "zero@100.105.160.106",
        "nvidia-smi --query-gpu=name,memory.used,memory.total,utilization.gpu "
        "--format=csv,noheader,nounits 2>/dev/null",
    )
    if rc == 0 and stdout.strip():
        sections.append(f"GPU ZeroZI: {stdout.strip()}")

    # ── 6. RAM / Swap on ZeroQ ──
    stdout, _, rc = _ssh_zeroq("free -h | grep -E '^(Mem|Swap):'")
    if rc == 0 and stdout.strip():
        sections.append(f"MEMORY ZeroQ:\n  {stdout.strip().replace(chr(10), chr(10) + '  ')}")

    # ── 7. Disk on ZeroQ ──
    stdout, _, rc = _ssh_zeroq("df -h / /hiveAI 2>/dev/null | tail -n +2")
    if rc == 0 and stdout.strip():
        sections.append(f"DISK ZeroQ:\n  {stdout.strip().replace(chr(10), chr(10) + '  ')}")

    # ── 8. Cloud brain connectivity ──
    for port, name in [(11437, "Reasoning (qwen3:14b)"), (11438, "Coding (qwen2.5-coder:32b)")]:
        stdout, _, rc = _ssh_zeroq(
            f"curl -sf --connect-timeout 5 --max-time 10 "
            f"http://localhost:{port}/api/tags 2>/dev/null | head -c 200"
        )
        if rc == 0 and stdout.strip():
            sections.append(f"CLOUD BRAIN {name}: ONLINE")
        else:
            sections.append(f"CLOUD BRAIN {name}: OFFLINE or unreachable")

    # ── 9. Recent errors from self-coder / hivecode ──
    errors_file = "/home/zero/.hive/hivecode_errors.json"
    if os.path.exists(errors_file):
        try:
            with open(errors_file, "r") as f:
                errors = json.load(f)
            if isinstance(errors, list) and errors:
                recent_errors = errors[-5:]
                sections.append(f"RECENT ERRORS ({len(errors)} total, showing last {len(recent_errors)}):")
                for err in recent_errors:
                    if isinstance(err, dict):
                        ts = err.get("timestamp", err.get("time", "?"))
                        msg = err.get("error", err.get("message", str(err)))
                        sections.append(f"  [{ts}] {str(msg)[:200]}")
                    else:
                        sections.append(f"  {str(err)[:200]}")
            elif isinstance(errors, dict):
                sections.append(f"ERRORS: {json.dumps(errors)[:400]}")
        except Exception as e:
            sections.append(f"ERRORS: failed to read ({e})")

    # ── 10. Latest autonomous actions ──
    stdout, _, rc = _ssh_zeroq(
        "journalctl -u hive-auto-evolve -u hive-autonomous-builder -u hive-cycle "
        "-n 10 --no-pager --output=short-iso 2>/dev/null | tail -10"
    )
    if rc == 0 and stdout.strip():
        sections.append(f"AUTONOMOUS ACTIONS (recent):\n  {stdout.strip().replace(chr(10), chr(10) + '  ')}")

    # ── 11. Ollama models loaded on ZeroDESK ──
    stdout, _, rc = _local_cmd("curl -sf http://localhost:11434/api/ps 2>/dev/null")
    if rc == 0 and stdout.strip():
        data = _safe_json(stdout)
        if data and isinstance(data, dict):
            models = data.get("models", [])
            if models:
                loaded = [m.get("name", "?") for m in models if isinstance(m, dict)]
                sections.append(f"OLLAMA LOADED (ZeroDESK): {', '.join(loaded)}")
            else:
                sections.append("OLLAMA LOADED (ZeroDESK): none currently loaded")

    return "\n".join(sections)


# ── Tool 4: save_insight ──────────────────────────────────────────────────────


@mcp.tool(
    name="save_insight",
    description=(
        "Save a discovered insight, pattern, or decision for future sessions. "
        "Categories: architecture, decision, pattern, bug, chris_preference, strategy, "
        "revenue, model, security, workflow. "
        "Use this EVERY TIME you discover something important that future sessions need to know. "
        "Insights persist across sessions and are the key to long-term coherence."
    ),
)
def save_insight(category: str, insight: str) -> str:
    """Save an insight to the persistent insights file."""
    # Validate category
    category = category.lower().strip()
    if category not in INSIGHT_CATEGORIES:
        return (
            f"Invalid category '{category}'. "
            f"Valid categories: {', '.join(INSIGHT_CATEGORIES)}"
        )

    if not insight.strip():
        return "Insight text cannot be empty."

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    insights_path = Path(INSIGHTS_FILE)

    # Create directory if needed
    insights_path.parent.mkdir(parents=True, exist_ok=True)

    # Create file with header if it doesn't exist
    if not insights_path.exists():
        header = (
            "# THE HIVE -- Persistent Insights\n"
            "# Auto-maintained by hive-memory MCP\n"
            "# These insights survive across sessions to maintain coherence.\n"
            "# Categories: architecture, decision, pattern, bug, chris_preference, "
            "strategy, revenue, model, security, workflow\n\n"
        )
        insights_path.write_text(header, encoding="utf-8")

    # Append the insight
    entry = f"\n## [{category.upper()}] {timestamp}\n{insight.strip()}\n"

    with open(insights_path, "a", encoding="utf-8") as f:
        f.write(entry)

    # Count total insights
    content = insights_path.read_text(encoding="utf-8")
    total = content.count("\n## [")

    return f"Saved [{category}] insight (total: {total} insights across all categories)"


# ── Tool 5: read_insights ─────────────────────────────────────────────────────


@mcp.tool(
    name="read_insights",
    description=(
        "Read saved insights, optionally filtered by category. "
        "Returns all persistent insights from previous sessions. "
        "Use this to recall architectural decisions, patterns, bugs, "
        "Chris's preferences, and strategies discovered over time."
    ),
)
def read_insights(category: str = "") -> str:
    """Read insights, optionally filtered by category."""
    insights_path = Path(INSIGHTS_FILE)

    if not insights_path.exists():
        return (
            "No insights file found. No insights have been saved yet. "
            "Use save_insight to start building cross-session knowledge."
        )

    content = insights_path.read_text(encoding="utf-8", errors="replace")

    if not category or category.strip() == "":
        # Return everything
        lines = content.split("\n")
        total = sum(1 for l in lines if l.startswith("## ["))
        # Count by category
        cat_counts = {}
        for l in lines:
            if l.startswith("## ["):
                match = re.match(r"## \[(\w+)\]", l)
                if match:
                    cat = match.group(1).lower()
                    cat_counts[cat] = cat_counts.get(cat, 0) + 1

        summary = "=== ALL INSIGHTS ===\n"
        summary += f"Total: {total} insights\n"
        if cat_counts:
            summary += "By category: " + ", ".join(
                f"{cat}={cnt}" for cat, cnt in sorted(cat_counts.items())
            )
            summary += "\n\n"
        summary += content
        return summary

    # Filter by category
    category = category.upper().strip()
    blocks = re.split(r"(?=\n## \[)", content)
    filtered = [b for b in blocks if f"## [{category}]" in b]

    if not filtered:
        return f"No insights found for category '{category}'."

    result = f"=== INSIGHTS: {category} ({len(filtered)} found) ===\n"
    result += "\n".join(b.strip() for b in filtered)
    return result


# ── Tool 6: read_claude_md ────────────────────────────────────────────────────


@mcp.tool(
    name="read_claude_md",
    description=(
        "Read the full CLAUDE.md to refresh knowledge of the Hive's architecture. "
        "This file contains: machine inventory, service tables, decision trees, "
        "model policy, file ownership rules, conventions, and the intelligence flow map. "
        "Use this when you need to remember how the Hive is structured."
    ),
)
def read_claude_md() -> str:
    """Read the full CLAUDE.md architecture document."""
    content = _read_file(CLAUDE_MD)
    if content.startswith("FILE_NOT_FOUND") or content.startswith("READ_ERROR"):
        return f"Cannot read CLAUDE.md: {content}"

    # Add metadata
    stat = os.stat(CLAUDE_MD)
    mtime = datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
    size = stat.st_size

    header = (
        f"=== CLAUDE.md ===\n"
        f"Path: {CLAUDE_MD}\n"
        f"Last modified: {mtime}\n"
        f"Size: {size:,} bytes ({len(content.splitlines())} lines)\n"
        f"{'=' * 60}\n\n"
    )
    return header + content


# ── Tool 7: session_checkin ───────────────────────────────────────────────────


@mcp.tool(
    name="session_checkin",
    description=(
        "Run the full session startup checklist and return ALL results in one call. "
        "Replaces the manual 8-step SESSION STARTUP CHECKLIST from CLAUDE.md. "
        "Runs: mesh register + status, service count, nerve growth, quality grades, "
        "model inventory summary, pending tasks from MEMORY.md. "
        "ALWAYS run this at the start of every new session."
    ),
)
def session_checkin() -> str:
    """Run all session startup checks and return consolidated results."""
    sections = [
        f"=== SESSION CHECKIN === ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})\n"
    ]

    # ── 1. Mesh register ──
    mesh_script = os.path.expanduser("~/.hive/mesh.py")
    if os.path.exists(mesh_script):
        stdout, stderr, rc = _local_cmd(f"python3 {mesh_script} register 2>&1")
        sections.append(f"1. MESH REGISTER: {stdout.strip() or stderr.strip() or 'done'}")

        stdout, stderr, rc = _local_cmd(f"python3 {mesh_script} status 2>&1")
        sections.append(f"2. MESH STATUS:\n   {(stdout.strip() or stderr.strip() or 'no peers').replace(chr(10), chr(10) + '   ')}")
    else:
        sections.append("1-2. MESH: mesh.py not found at ~/.hive/mesh.py")

    # ── 3. Service count ──
    stdout, _, rc = _ssh_zeroq(
        "systemctl list-units 'hive-*' --type=service --no-pager --no-legend 2>/dev/null | wc -l"
    )
    running_count = stdout.strip() if rc == 0 else "?"

    stdout, _, rc = _ssh_zeroq(
        "systemctl list-units 'hive-*' --type=service --all --no-pager --no-legend 2>/dev/null "
        "| grep -c failed"
    )
    failed_count = stdout.strip() if rc == 0 else "?"
    sections.append(f"3. SERVICES: {running_count} running, {failed_count} failed")

    # ── 4. Nerve growth ──
    stdout, _, rc = _ssh_zeroq(
        f"sqlite3 {NERVE_DB} 'SELECT count(*) FROM knowledge' 2>/dev/null"
    )
    nerve_total = stdout.strip() if rc == 0 else "?"

    stdout, _, rc = _ssh_zeroq(
        f"sqlite3 {NERVE_DB} "
        "\"SELECT count(*) FROM knowledge WHERE timestamp > datetime('now', '-1 hour')\" 2>/dev/null"
    )
    nerve_hourly = stdout.strip() if rc == 0 else "?"
    sections.append(f"4. NERVE: {nerve_total} facts total, {nerve_hourly} new in last hour")

    # ── 5. Quality grades ──
    raw = _curl_zeroq("http://localhost:8879/api/grades")
    if not raw.startswith("ERROR"):
        data = _safe_json(raw)
        if data and isinstance(data, dict):
            grades = data.get("grades", data)
            if isinstance(grades, dict):
                worst = None
                worst_score = 1.0
                for proc, info in grades.items():
                    if isinstance(info, dict):
                        score = info.get("score", 1.0)
                        if isinstance(score, (int, float)) and score < worst_score:
                            worst_score = score
                            worst = proc
                summary_parts = []
                for proc, info in sorted(grades.items()):
                    if isinstance(info, dict):
                        summary_parts.append(f"{proc}:{info.get('grade', '?')}")
                sections.append(f"5. QUALITY: {', '.join(summary_parts[:10])}")
                if worst:
                    sections.append(f"   WORST: {worst} at {worst_score:.2f}")
            else:
                sections.append(f"5. QUALITY: {str(grades)[:200]}")
        else:
            sections.append(f"5. QUALITY: {raw[:200]}")
    else:
        sections.append(f"5. QUALITY: tracker unreachable")

    # ── 6. Model inventory summary ──
    raw = _curl_zeroq("http://localhost:8878/api/inventory")
    if not raw.startswith("ERROR"):
        data = _safe_json(raw)
        if data and isinstance(data, dict):
            inv_parts = []
            for machine, info in data.items():
                if machine == "cloud_brains":
                    continue
                if isinstance(info, dict):
                    models = info.get("models", [])
                    status = "?"
                    health = info.get("health", {})
                    if isinstance(health, dict):
                        status = health.get("status", "?")
                    inv_parts.append(f"{machine}: {len(models)} models ({status})")
            sections.append(f"6. MODEL INVENTORY: {'; '.join(inv_parts)}")
        else:
            sections.append(f"6. MODEL INVENTORY: {raw[:200]}")
    else:
        sections.append(f"6. MODEL INVENTORY: router unreachable")

    # ── 7. Pending tasks from MEMORY.md ──
    memory_content = _read_file(MEMORY_MD)
    if not memory_content.startswith("FILE_NOT_FOUND"):
        pending_match = re.search(
            r"^## Pending Tasks\s*\n(.*?)(?=^## |\Z)",
            memory_content,
            re.MULTILINE | re.DOTALL,
        )
        if pending_match:
            tasks = pending_match.group(1).strip()
            # Count numbered items
            task_lines = [l for l in tasks.split("\n") if re.match(r"\s*\d+\.", l)]
            sections.append(f"7. PENDING TASKS ({len(task_lines)} items):")
            for tl in task_lines[:15]:
                sections.append(f"   {tl.strip()}")
        else:
            sections.append("7. PENDING TASKS: none found in MEMORY.md")
    else:
        sections.append("7. PENDING TASKS: MEMORY.md not found")

    # ── 8. Log to mesh ──
    if os.path.exists(mesh_script):
        _local_cmd(f'python3 {mesh_script} log "session checkin complete" 2>&1')
        sections.append("8. MESH LOG: logged checkin")

    # ── Bonus: quick health indicators ──
    sections.append("\n--- HEALTH INDICATORS ---")

    # ZeroQ swap
    stdout, _, rc = _ssh_zeroq("free -m | grep Swap | awk '{print $3/$2*100}'")
    if rc == 0 and stdout.strip():
        try:
            swap_pct = float(stdout.strip())
            status = "OK" if swap_pct < 50 else "WARNING" if swap_pct < 80 else "CRITICAL"
            sections.append(f"ZeroQ Swap: {swap_pct:.1f}% used [{status}]")
        except ValueError:
            pass

    # ZeroDESK disk
    stdout, _, rc = _local_cmd("df -h / | tail -1 | awk '{print $5}'")
    if rc == 0 and stdout.strip():
        sections.append(f"ZeroDESK Disk: {stdout.strip()} used")

    # Cloud brains
    for port, name in [(11437, "Reasoning"), (11438, "Coding")]:
        stdout, _, rc = _ssh_zeroq(
            f"curl -sf --connect-timeout 5 http://localhost:{port}/api/tags 2>/dev/null | head -c 50"
        )
        status = "ONLINE" if rc == 0 and stdout.strip() else "OFFLINE"
        sections.append(f"Cloud Brain ({name}): {status}")

    sections.append("\n=== CHECKIN COMPLETE ===")
    return "\n".join(sections)


# ── Tool 8: whats_broken ──────────────────────────────────────────────────────


@mcp.tool(
    name="whats_broken",
    description=(
        "Quick scan for broken things across the entire Hive. Returns: "
        "failed services, recent errors, high error rates, GPU/RAM/disk pressure, "
        "cloud brain connectivity, swap warnings, stale nerve, and any other red flags. "
        "Use this when you need to find problems FAST."
    ),
)
def whats_broken() -> str:
    """Scan all systems for broken or degraded things."""
    problems = []
    warnings = []
    ok_items = []

    # ── 1. Failed services ──
    stdout, _, rc = _ssh_zeroq(
        "systemctl list-units 'hive-*' --type=service --all --no-pager --no-legend 2>/dev/null "
        "| grep -E '(failed|dead)'"
    )
    if rc == 0 and stdout.strip():
        failed_lines = [l.strip() for l in stdout.strip().split("\n") if l.strip()]
        truly_failed = [l for l in failed_lines if "failed" in l]
        dead = [l for l in failed_lines if "dead" in l and "failed" not in l]
        if truly_failed:
            failed_names = [l.split()[0].replace(".service", "") for l in truly_failed]
            problems.append(f"FAILED SERVICES ({len(truly_failed)}): {', '.join(failed_names)}")
        if dead:
            dead_names = [l.split()[0].replace(".service", "") for l in dead]
            warnings.append(f"Dead/inactive services ({len(dead)}): {', '.join(dead_names[:10])}")
    else:
        ok_items.append("All hive-* services running")

    # ── 2. Recent systemd errors ──
    stdout, _, rc = _ssh_zeroq(
        "journalctl -p err --since '1 hour ago' -u 'hive-*' --no-pager -o short-iso 2>/dev/null | tail -15"
    )
    if rc == 0 and stdout.strip():
        error_lines = [l.strip() for l in stdout.strip().split("\n") if l.strip()]
        if error_lines:
            problems.append(f"RECENT ERRORS ({len(error_lines)} in last hour):")
            for el in error_lines[-5:]:
                problems.append(f"  {el[:200]}")

    # ── 3. ZeroQ swap pressure ──
    stdout, _, rc = _ssh_zeroq("free -m | grep Swap")
    if rc == 0 and stdout.strip():
        parts = stdout.strip().split()
        if len(parts) >= 3:
            try:
                total = int(parts[1])
                used = int(parts[2])
                if total > 0:
                    pct = used / total * 100
                    if pct > 90:
                        problems.append(f"ZeroQ SWAP CRITICAL: {pct:.1f}% ({used}MB/{total}MB)")
                    elif pct > 70:
                        warnings.append(f"ZeroQ swap high: {pct:.1f}% ({used}MB/{total}MB)")
                    else:
                        ok_items.append(f"ZeroQ swap: {pct:.1f}%")
            except ValueError:
                pass

    # ── 4. ZeroQ RAM pressure ──
    stdout, _, rc = _ssh_zeroq("free -m | grep Mem")
    if rc == 0 and stdout.strip():
        parts = stdout.strip().split()
        if len(parts) >= 7:
            try:
                total = int(parts[1])
                available = int(parts[6])
                pct_avail = available / total * 100
                if pct_avail < 5:
                    problems.append(f"ZeroQ RAM CRITICAL: only {available}MB available ({pct_avail:.1f}%)")
                elif pct_avail < 15:
                    warnings.append(f"ZeroQ RAM low: {available}MB available ({pct_avail:.1f}%)")
                else:
                    ok_items.append(f"ZeroQ RAM: {available}MB available ({pct_avail:.1f}%)")
            except ValueError:
                pass

    # ── 5. Disk pressure across machines ──
    for machine, host in [
        ("ZeroDESK", None),
        ("ZeroQ", ZEROQ),
        ("ZeroZI", "zero@100.105.160.106"),
    ]:
        if host is None:
            stdout, _, rc = _local_cmd("df -h / | tail -1")
        else:
            stdout, _, rc = _ssh(host, "df -h / | tail -1") if host != ZEROQ else _ssh_zeroq("df -h / | tail -1")
        if rc == 0 and stdout.strip():
            parts = stdout.strip().split()
            if len(parts) >= 5:
                pct_str = parts[4].replace("%", "")
                try:
                    pct = int(pct_str)
                    if pct > 90:
                        problems.append(f"{machine} DISK CRITICAL: {pct}% full")
                    elif pct > 80:
                        warnings.append(f"{machine} disk high: {pct}% full")
                    else:
                        ok_items.append(f"{machine} disk: {pct}%")
                except ValueError:
                    pass

    # ── 6. GPU pressure (ZeroDESK) ──
    stdout, _, rc = _local_cmd(
        "nvidia-smi --query-gpu=memory.used,memory.total --format=csv,noheader,nounits 2>/dev/null"
    )
    if rc == 0 and stdout.strip():
        try:
            used, total = [int(x.strip()) for x in stdout.strip().split(",")]
            pct = used / total * 100
            if pct > 90:
                warnings.append(f"ZeroDESK GPU memory high: {used}MB/{total}MB ({pct:.0f}%)")
            else:
                ok_items.append(f"ZeroDESK GPU: {used}MB/{total}MB ({pct:.0f}%)")
        except (ValueError, IndexError):
            pass

    # ── 7. Cloud brain connectivity ──
    for port, name in [(11437, "Reasoning"), (11438, "Coding")]:
        stdout, _, rc = _ssh_zeroq(
            f"curl -sf --connect-timeout 5 http://localhost:{port}/api/tags 2>/dev/null | head -c 50"
        )
        if rc != 0 or not stdout.strip():
            problems.append(f"Cloud Brain ({name}) OFFLINE on port {port}")
        else:
            ok_items.append(f"Cloud Brain ({name}): online")

    # ── 8. Nerve growth (stale?) ──
    stdout, _, rc = _ssh_zeroq(
        f"sqlite3 {NERVE_DB} "
        "\"SELECT count(*) FROM knowledge WHERE timestamp > datetime('now', '-2 hours')\" 2>/dev/null"
    )
    if rc == 0 and stdout.strip():
        try:
            recent = int(stdout.strip())
            if recent == 0:
                warnings.append("Nerve STALE: 0 new facts in 2 hours")
            else:
                ok_items.append(f"Nerve: {recent} facts in last 2h")
        except ValueError:
            pass

    # ── 9. Quality tracker reachability ──
    raw = _curl_zeroq("http://localhost:8879/api/grades")
    if raw.startswith("ERROR"):
        warnings.append("Quality tracker unreachable (port 8879)")
    else:
        # Check for any F grades
        data = _safe_json(raw)
        if data and isinstance(data, dict):
            grades = data.get("grades", data)
            if isinstance(grades, dict):
                f_grades = [
                    proc for proc, info in grades.items()
                    if isinstance(info, dict) and info.get("grade") == "F"
                ]
                if f_grades:
                    problems.append(f"F-GRADE processes: {', '.join(f_grades)}")

    # ── 10. Ollama on ZeroDESK ──
    stdout, _, rc = _local_cmd("curl -sf http://localhost:11434/api/tags 2>/dev/null | head -c 50")
    if rc != 0 or not stdout.strip():
        problems.append("Ollama on ZeroDESK not responding (port 11434)")
    else:
        ok_items.append("Ollama ZeroDESK: responding")

    # ── 11. Model router reachability ──
    raw = _curl_zeroq("http://localhost:8878/api/inventory")
    if raw.startswith("ERROR"):
        warnings.append("Model router unreachable (port 8878)")
    else:
        ok_items.append("Model router: responding")

    # ── 12. Self-coder errors ──
    errors_file = "/home/zero/.hive/hivecode_errors.json"
    if os.path.exists(errors_file):
        try:
            mtime = os.path.getmtime(errors_file)
            age_hours = (time.time() - mtime) / 3600
            with open(errors_file, "r") as f:
                errors = json.load(f)
            if isinstance(errors, list) and len(errors) > 0:
                recent_errs = [
                    e for e in errors
                    if isinstance(e, dict) and "timestamp" in e
                ]
                if len(recent_errs) > 5:
                    warnings.append(f"Self-coder: {len(recent_errs)} errors logged (file age: {age_hours:.1f}h)")
        except Exception:
            pass

    # ── 13. Machine reachability ──
    for machine, host in [
        ("ZeroZI", "zero@100.105.160.106"),
        ("ZeroNovo", "zero@100.103.183.91"),
    ]:
        stdout, _, rc = _ssh(host, "echo ok", timeout=10)
        if rc != 0 or stdout.strip() != "ok":
            warnings.append(f"{machine} unreachable via SSH")
        else:
            ok_items.append(f"{machine}: reachable")

    # ── Build report ──
    report = [f"=== WHAT'S BROKEN === ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})\n"]

    if problems:
        report.append(f"PROBLEMS ({len(problems)}):")
        for p in problems:
            report.append(f"  [!!!] {p}")
    else:
        report.append("PROBLEMS: None found!")

    if warnings:
        report.append(f"\nWARNINGS ({len(warnings)}):")
        for w in warnings:
            report.append(f"  [!] {w}")

    report.append(f"\nOK ({len(ok_items)}):")
    for o in ok_items:
        report.append(f"  [OK] {o}")

    # Priority recommendation
    report.append("\n--- RECOMMENDED ACTION ---")
    if problems:
        report.append(f"Fix the {len(problems)} problem(s) above first (P0/P1 priority).")
    elif warnings:
        report.append(f"Address the {len(warnings)} warning(s) when possible (P2 priority).")
    else:
        report.append("All clear! Focus on growth, revenue, or quality improvements.")

    return "\n".join(report)


# ── Cost Tracker ─────────────────────────────────────────────────────────────

@mcp.tool()
def get_costs() -> str:
    """Get current Hive spending: Claude Code token costs, cloud brain costs, and burn rate.
    Reads from the cost tracker dashboard at localhost:8198."""
    import urllib.request
    try:
        with urllib.request.urlopen("http://localhost:8198/api/costs", timeout=5) as resp:
            data = json.loads(resp.read().decode())
    except Exception as e:
        return f"Cost tracker unavailable (is hive-cost-tracker running on port 8198?): {e}"

    lines = [
        "=== HIVE COST REPORT ===",
        f"Plan: Claude Max ($200/mo flat subscription)",
        f"",
        f"Claude Code (API-equivalent value):",
        f"  Today: ${data.get('claude_today', 0):.2f}",
        f"  This week: ${data.get('claude_week', 0):.2f}",
        f"  All time: ${data.get('claude_total', 0):.2f}",
        f"",
        f"Cloud Brains (Vast.ai actual cost):",
    ]
    for name, info in data.get("cloud_brains", {}).items():
        lines.append(f"  {name}: {info.get('status', '?')} | ${info.get('daily', 0):.2f}/day")
    lines.extend([
        f"  Total cloud: ${data.get('cloud_total', 0):.2f}",
        f"",
        f"Combined:",
        f"  Daily average: ${data.get('daily_average', 0):.2f}/day",
        f"  Monthly projected: ${data.get('monthly_projected', 0):.2f}/mo",
        f"  Total spent (API equiv): ${data.get('combined_total', 0):.2f}",
        f"",
        f"ROI: Paying $200/mo for ${data.get('claude_total', 0):.2f} of API-equivalent value",
        f"Dashboard: http://localhost:8198/",
    ])
    return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    mcp.run(transport="stdio")
