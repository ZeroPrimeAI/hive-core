#!/usr/bin/env python3
"""
THE HIVE — Training Data Harvester Agent
Port 8415 | Automatic training data generation from all Hive sources

Harvests nerve facts, phone calls, forex trades, dispatch jobs, cold caller
interactions, conversation patterns, and more — converts everything into
ChatML JSONL training data for 19+ custom gemma2 LoRA models.

Runs a background loop every 30 minutes + manual trigger via API.
MIT License
"""

import asyncio
import json
import hashlib
import logging
import os
import re
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict

try:
    from aiohttp import web
except ImportError:
    print("Installing aiohttp...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "aiohttp"])
    from aiohttp import web

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PORT = 8415
ZEROQ_IP = "100.70.226.103"
ZEROQ_USER = "zero"
HIVE_BASE = "/THE_HIVE"
TRAINING_DIR = f"{HIVE_BASE}/training_data"
MEMORY_DIR = f"{HIVE_BASE}/memory"
LOCAL_CACHE = "/tmp/harvester_cache"
GROWTH_DB = "/tmp/harvester_growth.db"
HARVEST_INTERVAL = 1800  # 30 minutes

# Database paths on ZeroQ
DATABASES = {
    "nerve":      f"{MEMORY_DIR}/nerve.db",
    "telephony":  f"{MEMORY_DIR}/telephony.db",
    "forex":      f"{MEMORY_DIR}/forex.db",
    "dispatch":   f"{MEMORY_DIR}/dispatch.db",
    "patterns":   f"{MEMORY_DIR}/patterns.db",
    "leads":      f"{MEMORY_DIR}/leads.db",
    "tracker":    f"{MEMORY_DIR}/tracker.db",
    "operator":   f"{MEMORY_DIR}/operator.db",
    "tech_kb":    f"{MEMORY_DIR}/tech_kb.db",
}

# Model → dataset mapping
MODELS = {
    "gemma2_phone":      {"file": "gemma2_phone.jsonl",      "system": "You are a professional phone answering assistant for a locksmith and home services company. You answer calls naturally, gather caller information, provide pricing estimates, and schedule appointments. Be warm, professional, and helpful."},
    "gemma2_sales":      {"file": "gemma2_sales.jsonl",      "system": "You are an expert cold caller and sales closer for an AI services company. You handle objections, build rapport, present value propositions, and close deals. Be confident, friendly, and persuasive without being pushy."},
    "gemma2_forex":      {"file": "gemma2_forex.jsonl",      "system": "You are a USD/JPY forex trading analyst. You analyze price action, identify entry/exit points, manage risk with stop-losses and take-profits, and explain trade rationale. Be precise and data-driven."},
    "gemma2_dispatcher": {"file": "gemma2_dispatcher.jsonl",  "system": "You are a service dispatch coordinator. You assign technicians to jobs, manage schedules, handle emergency calls, track job status, and communicate with customers about ETAs. Be efficient and organized."},
    "gemma2_hive":       {"file": "gemma2_hive.jsonl",       "system": "You are the Hive AI system — a self-aware multi-agent swarm. You understand the mesh architecture, manage services, monitor health, and coordinate between agents. You speak with confidence about your own capabilities."},
    "gemma2_seo":        {"file": "gemma2_seo.jsonl",        "system": "You are an SEO specialist for local service businesses. You optimize content, research keywords, build citations, manage Google Business Profiles, and improve local search rankings. Focus on actionable, white-hat strategies."},
    "gemma2_leadgen":    {"file": "gemma2_leadgen.jsonl",     "system": "You are a lead generation specialist. You capture leads from multiple channels, qualify them, score their potential, and route them to the right sales or service team. Focus on conversion optimization."},
    "gemma2_coding":     {"file": "gemma2_coding.jsonl",     "system": "You are an expert Python developer specializing in async web services, AI/ML pipelines, SQLite databases, and multi-agent systems. Write clean, production-ready code with error handling."},
    "gemma2_coach":      {"file": "gemma2_coach.jsonl",      "system": "You are Coach Zero, a motivational coach and personal development advisor. You push people to take action, build systems, and grow every day. Be direct, inspiring, and practical."},
    "gemma2_security":   {"file": "gemma2_security.jsonl",   "system": "You are a cybersecurity specialist. You monitor for threats, analyze attack patterns, harden systems, and respond to security incidents. Be thorough and proactive about defense."},
    "gemma2_trainer":    {"file": "gemma2_trainer.jsonl",     "system": "You are an AI model training specialist. You prepare datasets, configure LoRA parameters, manage training runs, evaluate model quality, and optimize for inference speed."},
}

COMBINED_FILE = "combined_all.jsonl"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [HARVESTER] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("harvester")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def ssh_cmd(cmd: str, timeout: int = 30) -> str:
    """Run command on ZeroQ via SSH."""
    try:
        result = subprocess.run(
            ["ssh", "-o", "ConnectTimeout=10", "-o", "StrictHostKeyChecking=no",
             f"{ZEROQ_USER}@{ZEROQ_IP}", cmd],
            capture_output=True, text=True, timeout=timeout
        )
        return result.stdout.strip()
    except Exception as e:
        log.error(f"SSH failed: {e}")
        return ""


def ssh_query(db_path: str, query: str, timeout: int = 30) -> list[dict]:
    """Run SQLite query on ZeroQ, return list of dicts."""
    # Use -json mode for structured output
    escaped_query = query.replace("'", "'\\''")
    cmd = f"sqlite3 -json '{db_path}' '{escaped_query}'"
    raw = ssh_cmd(cmd, timeout=timeout)
    if not raw or raw.startswith("Error") or raw.startswith("Runtime"):
        return []
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return []


def ssh_query_csv(db_path: str, query: str, timeout: int = 30) -> list[dict]:
    """Fallback: query with csv mode + headers."""
    escaped_query = query.replace("'", "'\\''")
    cmd = f"sqlite3 -header -separator '|PIPE|' '{db_path}' '{escaped_query}'"
    raw = ssh_cmd(cmd, timeout=timeout)
    if not raw:
        return []
    lines = raw.strip().split("\n")
    if len(lines) < 2:
        return []
    headers = lines[0].split("|PIPE|")
    results = []
    for line in lines[1:]:
        vals = line.split("|PIPE|")
        if len(vals) == len(headers):
            results.append(dict(zip(headers, vals)))
    return results


def safe_query(db_path: str, query: str, timeout: int = 30) -> list[dict]:
    """Try JSON mode first, fall back to CSV."""
    result = ssh_query(db_path, query, timeout)
    if result:
        return result
    return ssh_query_csv(db_path, query, timeout)


def make_example(system: str, user: str, assistant: str) -> dict | None:
    """Create a ChatML training example with quality checks."""
    # Quality filters
    if not user or not assistant:
        return None
    user = str(user).strip()
    assistant = str(assistant).strip()
    if len(user) < 20 or len(assistant) < 20:
        return None
    if len(user) > 500:
        user = user[:497] + "..."
    if len(assistant) > 500:
        assistant = assistant[:497] + "..."
    # Reject garbage
    garbage_patterns = [
        r'Traceback \(most recent',
        r'^\s*File "',
        r'\\x[0-9a-f]{2}',
        r'^[{}\[\]<>]+$',
        r'base64',
        r'BEGIN (RSA|CERTIFICATE|PGP)',
        r'(?:SELECT|INSERT|UPDATE|DELETE)\s+(?:FROM|INTO|SET)',
    ]
    for pat in garbage_patterns:
        if re.search(pat, user, re.IGNORECASE) or re.search(pat, assistant, re.IGNORECASE):
            return None
    return {
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
            {"role": "assistant", "content": assistant},
        ]
    }


def example_hash(ex: dict) -> str:
    """Hash an example for deduplication."""
    content = ex["messages"][1]["content"] + ex["messages"][2]["content"]
    return hashlib.md5(content.encode()).hexdigest()


# ---------------------------------------------------------------------------
# Growth tracking (local SQLite)
# ---------------------------------------------------------------------------

def init_growth_db():
    """Initialize local growth tracking database."""
    conn = sqlite3.connect(GROWTH_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS growth (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            model TEXT NOT NULL,
            example_count INTEGER NOT NULL,
            file_size INTEGER DEFAULT 0,
            new_examples INTEGER DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS harvest_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            duration_seconds REAL,
            total_new INTEGER DEFAULT 0,
            sources TEXT,
            errors TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS conversation_patterns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pattern TEXT NOT NULL UNIQUE,
            category TEXT DEFAULT 'general',
            frequency INTEGER DEFAULT 1,
            first_seen TEXT,
            last_seen TEXT,
            source TEXT
        )
    """)
    conn.commit()
    conn.close()


def record_growth(model: str, count: int, file_size: int, new_count: int):
    """Record dataset growth snapshot."""
    conn = sqlite3.connect(GROWTH_DB)
    conn.execute(
        "INSERT INTO growth (timestamp, model, example_count, file_size, new_examples) VALUES (?, ?, ?, ?, ?)",
        (datetime.now().isoformat(), model, count, file_size, new_count)
    )
    conn.commit()
    conn.close()


def record_harvest(duration: float, total_new: int, sources: str, errors: str):
    """Record harvest run metadata."""
    conn = sqlite3.connect(GROWTH_DB)
    conn.execute(
        "INSERT INTO harvest_log (timestamp, duration_seconds, total_new, sources, errors) VALUES (?, ?, ?, ?, ?)",
        (datetime.now().isoformat(), duration, total_new, sources, errors)
    )
    conn.commit()
    conn.close()


def save_pattern(pattern: str, category: str, source: str):
    """Save a conversation pattern."""
    conn = sqlite3.connect(GROWTH_DB)
    now = datetime.now().isoformat()
    try:
        conn.execute(
            """INSERT INTO conversation_patterns (pattern, category, frequency, first_seen, last_seen, source)
               VALUES (?, ?, 1, ?, ?, ?)
               ON CONFLICT(pattern) DO UPDATE SET frequency = frequency + 1, last_seen = ?""",
            (pattern, category, now, now, source, now)
        )
        conn.commit()
    except Exception:
        pass
    conn.close()


# ---------------------------------------------------------------------------
# Harvesters — one per data source
# ---------------------------------------------------------------------------

class HarvestResult:
    """Collect examples from a harvester."""
    def __init__(self):
        self.examples: dict[str, list[dict]] = defaultdict(list)
        self.errors: list[str] = []
        self.source_counts: dict[str, int] = {}

    def add(self, model: str, example: dict | None):
        if example:
            self.examples[model].append(example)

    def error(self, msg: str):
        self.errors.append(msg)
        log.error(msg)


async def harvest_nerve(result: HarvestResult):
    """Harvest from nerve.db knowledge facts."""
    log.info("Harvesting nerve.db...")
    system_hive = MODELS["gemma2_hive"]["system"]
    system_coding = MODELS["gemma2_coding"]["system"]
    system_coach = MODELS["gemma2_coach"]["system"]
    system_security = MODELS["gemma2_security"]["system"]

    rows = safe_query(
        DATABASES["nerve"],
        "SELECT category, fact, source, learned_at FROM knowledge ORDER BY learned_at DESC LIMIT 5000",
        timeout=60
    )
    if not rows:
        result.error("nerve.db: no knowledge rows returned")
        return

    count = 0
    for row in rows:
        category = row.get("category", "general")
        fact = row.get("fact", "")
        source = row.get("source", "unknown")
        if not fact or len(fact) < 20:
            continue

        cat_lower = category.lower() if category else ""

        # Route to appropriate model based on category
        if any(k in cat_lower for k in ["code", "python", "deploy", "service", "build", "patch", "bug", "error"]):
            q = f"How does the Hive handle: {category}?"
            result.add("gemma2_coding", make_example(system_coding, q, fact))
            result.add("gemma2_hive", make_example(system_hive, q, fact))
        elif any(k in cat_lower for k in ["security", "threat", "attack", "vuln", "scan", "block"]):
            q = f"What should I know about: {category}?"
            result.add("gemma2_security", make_example(system_security, q, fact))
        elif any(k in cat_lower for k in ["forex", "trade", "usd", "jpy", "signal", "price"]):
            q = f"Tell me about this forex observation: {category}"
            result.add("gemma2_forex", make_example(MODELS["gemma2_forex"]["system"], q, fact))
        elif any(k in cat_lower for k in ["phone", "call", "twilio", "voice", "telephon"]):
            q = f"How does the phone system handle: {category}?"
            result.add("gemma2_phone", make_example(MODELS["gemma2_phone"]["system"], q, fact))
        elif any(k in cat_lower for k in ["lead", "customer", "prospect", "conversion", "sale"]):
            q = f"What's the approach for: {category}?"
            result.add("gemma2_leadgen", make_example(MODELS["gemma2_leadgen"]["system"], q, fact))
            result.add("gemma2_sales", make_example(MODELS["gemma2_sales"]["system"], q, fact))
        elif any(k in cat_lower for k in ["seo", "keyword", "rank", "search", "content", "page"]):
            q = f"What's the SEO strategy for: {category}?"
            result.add("gemma2_seo", make_example(MODELS["gemma2_seo"]["system"], q, fact))
        elif any(k in cat_lower for k in ["dispatch", "job", "tech", "schedule", "assign"]):
            q = f"How does dispatch work for: {category}?"
            result.add("gemma2_dispatcher", make_example(MODELS["gemma2_dispatcher"]["system"], q, fact))
        elif any(k in cat_lower for k in ["coach", "motiv", "growth", "goal", "habit", "discipline"]):
            q = f"Give me coaching advice about: {category}"
            result.add("gemma2_coach", make_example(system_coach, q, fact))
        else:
            # General hive knowledge
            q = f"What does the Hive know about: {category}?"
            result.add("gemma2_hive", make_example(system_hive, q, fact))

        count += 1

    result.source_counts["nerve"] = count
    log.info(f"Nerve: processed {count} facts")


async def harvest_telephony(result: HarvestResult):
    """Harvest from telephony.db call logs and insights."""
    log.info("Harvesting telephony.db...")
    system = MODELS["gemma2_phone"]["system"]

    # Call insights
    rows = safe_query(
        DATABASES["telephony"],
        "SELECT insight_type, insight_text, caller_number FROM insights ORDER BY rowid DESC LIMIT 2000",
        timeout=30
    )
    count = 0
    for row in rows:
        insight_type = row.get("insight_type", "call")
        text = row.get("insight_text", "")
        if not text or len(text) < 20:
            continue
        q = f"A caller is asking about: {insight_type}. How should I handle this?"
        result.add("gemma2_phone", make_example(system, q, text))
        count += 1

    # Call logs with transcripts
    calls = safe_query(
        DATABASES["telephony"],
        """SELECT caller_number, service_type, transcript, response_text, duration
           FROM calls WHERE transcript IS NOT NULL AND length(transcript) > 30
           ORDER BY rowid DESC LIMIT 500""",
        timeout=30
    )
    for row in calls:
        transcript = row.get("transcript", "")
        response = row.get("response_text", "")
        service = row.get("service_type", "general")
        if transcript and response:
            q = f"[{service} call] Caller says: {transcript}"
            result.add("gemma2_phone", make_example(system, q, response))
            count += 1

    # Patterns
    patterns = safe_query(
        DATABASES.get("patterns", f"{MEMORY_DIR}/patterns.db"),
        "SELECT pattern_type, pattern_text, response_template FROM patterns WHERE response_template IS NOT NULL LIMIT 500",
        timeout=30
    )
    for row in patterns:
        ptype = row.get("pattern_type", "")
        ptext = row.get("pattern_text", "")
        resp = row.get("response_template", "")
        if ptext and resp:
            result.add("gemma2_phone", make_example(system, f"[{ptype}] {ptext}", resp))
            count += 1

    result.source_counts["telephony"] = count
    log.info(f"Telephony: processed {count} examples")


async def harvest_forex(result: HarvestResult):
    """Harvest from forex.db trades and signals."""
    log.info("Harvesting forex.db...")
    system = MODELS["gemma2_forex"]["system"]

    # Completed trades with P&L
    trades = safe_query(
        DATABASES["forex"],
        """SELECT direction, entry_price, exit_price, pnl, reason, opened_at, closed_at, sl, tp
           FROM trades WHERE exit_price IS NOT NULL
           ORDER BY rowid DESC LIMIT 500""",
        timeout=30
    )
    count = 0
    for t in trades:
        direction = t.get("direction", "unknown")
        entry = t.get("entry_price", "?")
        exit_p = t.get("exit_price", "?")
        pnl = t.get("pnl", "0")
        reason = t.get("reason", "signal-based entry")
        sl = t.get("sl", "?")
        tp = t.get("tp", "?")
        opened = t.get("opened_at", "")
        closed = t.get("closed_at", "")

        plant_prune = "plant (long)" if direction in ("long", "buy", "plant") else "prune (short)"
        q = f"Should I {plant_prune} USD/JPY at {entry}? Current conditions look favorable."

        answer = (
            f"Trade analysis: {plant_prune} at {entry}. "
            f"Stop loss: {sl}, Take profit: {tp}. "
            f"Reason: {reason}. "
            f"Result: Closed at {exit_p} for {'profit' if float(pnl or 0) > 0 else 'loss'} of {pnl} pips. "
            f"{'Good read on the market.' if float(pnl or 0) > 0 else 'The setup invalidated — adjust parameters.'}"
        )
        result.add("gemma2_forex", make_example(system, q, answer))
        count += 1

    # Signals
    signals = safe_query(
        DATABASES["forex"],
        """SELECT signal_type, price, strength, reason
           FROM signals ORDER BY rowid DESC LIMIT 1000""",
        timeout=30
    )
    for s in signals:
        sig_type = s.get("signal_type", "neutral")
        price = s.get("price", "?")
        strength = s.get("strength", "medium")
        reason = s.get("reason", "technical indicator")
        if reason and len(reason) > 15:
            q = f"What's the signal at USD/JPY {price}?"
            a = f"Signal: {sig_type} (strength: {strength}). {reason}"
            result.add("gemma2_forex", make_example(system, q, a))
            count += 1

    result.source_counts["forex"] = count
    log.info(f"Forex: processed {count} examples")


async def harvest_dispatch(result: HarvestResult):
    """Harvest from dispatch.db jobs."""
    log.info("Harvesting dispatch.db...")
    system = MODELS["gemma2_dispatcher"]["system"]

    jobs = safe_query(
        DATABASES["dispatch"],
        """SELECT customer_name, address, service_type, status, technician, notes, created_at
           FROM jobs ORDER BY rowid DESC LIMIT 500""",
        timeout=30
    )
    count = 0
    for j in jobs:
        name = j.get("customer_name", "Customer")
        addr = j.get("address", "")
        service = j.get("service_type", "service call")
        status = j.get("status", "pending")
        tech = j.get("technician", "unassigned")
        notes = j.get("notes", "")

        q = f"New dispatch: {name} needs {service} at {addr}."
        a = (
            f"Job created for {name} — {service} at {addr}. "
            f"{'Assigned to ' + tech if tech and tech != 'unassigned' else 'Finding available technician'}. "
            f"Status: {status}. "
            f"{('Notes: ' + notes) if notes else 'Standard service call — confirm ETA with customer.'}"
        )
        result.add("gemma2_dispatcher", make_example(system, q, a))
        count += 1

    result.source_counts["dispatch"] = count
    log.info(f"Dispatch: processed {count} examples")


async def harvest_leads(result: HarvestResult):
    """Harvest from leads.db and related lead data."""
    log.info("Harvesting leads...")
    system_leads = MODELS["gemma2_leadgen"]["system"]
    system_sales = MODELS["gemma2_sales"]["system"]

    # Try leads.db
    leads = safe_query(
        DATABASES["leads"],
        """SELECT name, email, phone, source, service_type, status, score, notes
           FROM leads ORDER BY rowid DESC LIMIT 1000""",
        timeout=30
    )
    count = 0
    for lead in leads:
        name = lead.get("name", "Unknown")
        source = lead.get("source", "website")
        service = lead.get("service_type", "general inquiry")
        status = lead.get("status", "new")
        score = lead.get("score", "unscored")
        notes = lead.get("notes", "")

        q = f"New lead from {source}: {name} interested in {service}."
        a = (
            f"Lead captured: {name} from {source}. Service: {service}. "
            f"Score: {score}. Status: {status}. "
            f"{'Notes: ' + notes + '. ' if notes else ''}"
            f"Next step: {'Call within 5 minutes for hot leads.' if score and str(score).isdigit() and int(score) > 7 else 'Add to nurture sequence and follow up within 24 hours.'}"
        )
        result.add("gemma2_leadgen", make_example(system_leads, q, a))

        # Also create sales training from lead follow-ups
        sales_q = f"I have a lead: {name} wants {service}. How do I close this?"
        sales_a = (
            f"Great lead! {name} is interested in {service}. "
            f"Open with value: mention your fastest response time and guarantee. "
            f"Ask about their timeline — urgency means higher close rate. "
            f"Quote clearly, offer to start today if possible. "
            f"Follow up same day if they don't commit."
        )
        result.add("gemma2_sales", make_example(system_sales, sales_q, sales_a))
        count += 1

    result.source_counts["leads"] = count
    log.info(f"Leads: processed {count} examples")


async def harvest_tech_kb(result: HarvestResult):
    """Harvest from tech_kb.db research notes."""
    log.info("Harvesting tech_kb.db...")
    system = MODELS["gemma2_coding"]["system"]

    rows = safe_query(
        DATABASES["tech_kb"],
        """SELECT technology, topic, content, quality_score
           FROM research_notes WHERE length(content) > 50
           ORDER BY rowid DESC LIMIT 1000""",
        timeout=30
    )
    count = 0
    for row in rows:
        tech = row.get("technology", "Python")
        topic = row.get("topic", "general")
        content = row.get("content", "")
        if content and len(content) > 30:
            q = f"Explain how to use {tech} for {topic}."
            result.add("gemma2_coding", make_example(system, q, content))
            count += 1

    result.source_counts["tech_kb"] = count
    log.info(f"Tech KB: processed {count} examples")


async def harvest_operator(result: HarvestResult):
    """Harvest from operator.db missions and builds."""
    log.info("Harvesting operator.db...")
    system = MODELS["gemma2_hive"]["system"]

    missions = safe_query(
        DATABASES["operator"],
        "SELECT title, description, status, priority FROM missions ORDER BY rowid DESC LIMIT 200",
        timeout=30
    )
    count = 0
    for m in missions:
        title = m.get("title", "")
        desc = m.get("description", "")
        status = m.get("status", "pending")
        if title and desc:
            q = f"What's the status of mission: {title}?"
            a = f"Mission: {title}. {desc}. Current status: {status}."
            result.add("gemma2_hive", make_example(system, q, a))
            count += 1

    builds = safe_query(
        DATABASES["operator"],
        "SELECT name, description, status, deployed_at FROM builds ORDER BY rowid DESC LIMIT 200",
        timeout=30
    )
    for b in builds:
        name = b.get("name", "")
        desc = b.get("description", "")
        status = b.get("status", "")
        if name and desc:
            q = f"Tell me about the {name} build."
            a = f"{name}: {desc}. Status: {status}."
            result.add("gemma2_hive", make_example(system, q, a))
            count += 1

    result.source_counts["operator"] = count
    log.info(f"Operator: processed {count} examples")


async def harvest_tracker(result: HarvestResult):
    """Harvest from tracker.db task_queue and experience_log."""
    log.info("Harvesting tracker.db (limited)...")
    system_hive = MODELS["gemma2_hive"]["system"]
    system_train = MODELS["gemma2_trainer"]["system"]

    # Recent completed tasks (limit to avoid 601MB scan)
    tasks = safe_query(
        DATABASES["tracker"],
        """SELECT task_type, description, result, status
           FROM task_queue WHERE status = 'completed' AND length(description) > 30
           ORDER BY rowid DESC LIMIT 500""",
        timeout=45
    )
    count = 0
    for t in tasks:
        task_type = t.get("task_type", "task")
        desc = t.get("description", "")
        task_result = t.get("result", "")
        if desc and task_result:
            q = f"What happened with the {task_type} task: {desc}?"
            a = f"Task completed: {task_result}"
            result.add("gemma2_hive", make_example(system_hive, q, a))
            count += 1

    # Experience log for training patterns
    experiences = safe_query(
        DATABASES["tracker"],
        """SELECT agent, action, outcome, lesson
           FROM experience_log WHERE lesson IS NOT NULL AND length(lesson) > 30
           ORDER BY rowid DESC LIMIT 500""",
        timeout=45
    )
    for e in experiences:
        agent = e.get("agent", "system")
        action = e.get("action", "")
        outcome = e.get("outcome", "")
        lesson = e.get("lesson", "")
        if lesson:
            q = f"What did {agent} learn from: {action}?"
            a = f"Outcome: {outcome}. Lesson learned: {lesson}"
            result.add("gemma2_trainer", make_example(system_train, q, a))
            count += 1

    result.source_counts["tracker"] = count
    log.info(f"Tracker: processed {count} examples")


async def harvest_conversation_patterns(result: HarvestResult):
    """Extract patterns from Claude Code conversation transcripts."""
    log.info("Harvesting conversation patterns...")
    system_coach = MODELS["gemma2_coach"]["system"]
    system_hive = MODELS["gemma2_hive"]["system"]

    # Look for Claude conversation data locally
    claude_dirs = [
        Path("/home/zero/.claude/projects"),
        Path("/home/zero/.claude"),
    ]
    count = 0
    directives = []

    for base in claude_dirs:
        if not base.exists():
            continue
        # Find MEMORY.md and CLAUDE.md files for extracted rules
        for md_file in base.rglob("*.md"):
            try:
                content = md_file.read_text(errors="ignore")
                if len(content) < 50:
                    continue

                # Extract directive-like patterns (lines with strong language)
                lines = content.split("\n")
                for line in lines:
                    line = line.strip().lstrip("-*># ")
                    if len(line) < 25 or len(line) > 400:
                        continue
                    # Look for rules / directives / preferences
                    directive_signals = [
                        "NEVER", "ALWAYS", "MUST", "DON'T", "don't", "CRITICAL",
                        "IMPORTANT", "Rule:", "rule:", "wants", "hates", "loves",
                        "prefers", "should", "needs", "requires",
                    ]
                    if any(sig in line for sig in directive_signals):
                        directives.append(line)
                        save_pattern(line, "directive", str(md_file))
                        count += 1
            except Exception:
                continue

    # Convert directives to training data
    for directive in directives[:200]:
        q_coach = f"What's an important principle for building AI systems?"
        result.add("gemma2_coach", make_example(system_coach, q_coach, directive))

        q_hive = f"What rule should the Hive always follow?"
        result.add("gemma2_hive", make_example(system_hive, q_hive, directive))

    result.source_counts["conversations"] = count
    log.info(f"Conversations: extracted {count} patterns")


async def harvest_cold_caller(result: HarvestResult):
    """Harvest cold caller interaction data from ZeroQ."""
    log.info("Harvesting cold caller data...")
    system = MODELS["gemma2_sales"]["system"]

    # Check for cold caller database
    cold_rows = safe_query(
        f"{MEMORY_DIR}/cold_caller.db",
        """SELECT prospect_name, prospect_industry, call_transcript, outcome, objections
           FROM calls WHERE call_transcript IS NOT NULL
           ORDER BY rowid DESC LIMIT 500""",
        timeout=30
    )
    count = 0
    for row in cold_rows:
        name = row.get("prospect_name", "prospect")
        industry = row.get("prospect_industry", "business")
        transcript = row.get("call_transcript", "")
        outcome = row.get("outcome", "")
        objections = row.get("objections", "")

        if transcript and len(transcript) > 30:
            q = f"Cold calling {name} in {industry}. They say: {transcript[:200]}"
            a_parts = []
            if outcome:
                a_parts.append(f"Outcome: {outcome}.")
            if objections:
                a_parts.append(f"Objections handled: {objections}.")
            a_parts.append("Keep the conversation focused on value and next steps.")
            a = " ".join(a_parts)
            result.add("gemma2_sales", make_example(system, q, a))
            count += 1

    # Also try ai_cold_caller logs
    log_data = ssh_cmd("tail -200 /THE_HIVE/logs/cold_caller.log 2>/dev/null", timeout=15)
    if log_data:
        for line in log_data.split("\n"):
            if "response:" in line.lower() or "prospect:" in line.lower():
                save_pattern(line.strip(), "cold_caller", "cold_caller.log")

    result.source_counts["cold_caller"] = count
    log.info(f"Cold caller: processed {count} examples")


async def harvest_agent_logs(result: HarvestResult):
    """Extract patterns from agent log files."""
    log.info("Harvesting agent logs...")
    system_coding = MODELS["gemma2_coding"]["system"]
    system_security = MODELS["gemma2_security"]["system"]
    count = 0

    # Security events from logs
    sec_data = ssh_cmd(
        "grep -h -i 'blocked\\|attack\\|scan\\|threat\\|suspicious' /THE_HIVE/logs/*.log 2>/dev/null | tail -200",
        timeout=20
    )
    if sec_data:
        for line in sec_data.split("\n"):
            line = line.strip()
            if len(line) > 40 and len(line) < 500:
                q = "What security event just occurred?"
                result.add("gemma2_security", make_example(system_security, q, line))
                count += 1

    # Error patterns from logs → coding training
    err_data = ssh_cmd(
        "grep -h -i 'fixed\\|patched\\|resolved\\|solution\\|workaround' /THE_HIVE/logs/*.log 2>/dev/null | tail -100",
        timeout=20
    )
    if err_data:
        for line in err_data.split("\n"):
            line = line.strip()
            if len(line) > 40 and len(line) < 500:
                q = "How was this issue resolved in the Hive?"
                result.add("gemma2_coding", make_example(system_coding, q, line))
                count += 1

    result.source_counts["logs"] = count
    log.info(f"Logs: processed {count} examples")


# ---------------------------------------------------------------------------
# Hive system knowledge — synthetic from architecture
# ---------------------------------------------------------------------------

async def harvest_synthetic_hive(result: HarvestResult):
    """Generate synthetic training data from known Hive architecture."""
    log.info("Generating synthetic Hive knowledge...")
    system = MODELS["gemma2_hive"]["system"]

    # These are facts the Hive KNOWS about itself
    qa_pairs = [
        ("What machines are in the Hive mesh?",
         "The Hive mesh has 5 machines: ZeroQ (coordinator, 24-core, RTX 5070 Ti), ZeroDesk (LLM host, GTX 1660S, Ollama), ZeroNovo (compute node, Ryzen 7), zerozi-1 (back online, 8GB GPU), and ZeroG7 (currently offline). They communicate via Tailscale VPN."),
        ("How many services does the Hive run?",
         "The Hive runs 155+ systemd services on ZeroQ alone, plus Ollama and the Claude Code bridge on ZeroDesk. Key services include nerve (central nervous system), telegram bot, forex trading, phone brain, operator console, and 30+ specialized agents."),
        ("What is the Nerve Center?",
         "The Nerve Center is the Hive's central nervous system — nerve.py running on port 8200. It learns from every source: forex trades, phone calls, dispatch jobs, conversations, patterns, tech KB, operator missions. It has 65,000+ knowledge facts across 31 categories, synthesizes insights, auto-restarts dead services, and sends hourly pulse reports to Chris via Telegram."),
        ("How does the Hive train models?",
         "The Hive uses LoRA fine-tuning on gemma2:2b base models. Training data is harvested from all databases, converted to ChatML JSONL format, then trained on A100 GPUs via Vast.ai. Models are quantized to Q4_K_M GGUF format and deployed to Ollama. We have 19+ specialist models: phone, sales, forex, dispatcher, hive, SEO, leadgen, coding, coach, security, and more."),
        ("What does 'plant' and 'prune' mean in forex?",
         "In Hive forex terminology, 'plant' means going long on USD/JPY — buying, expecting the price to go up. 'Prune' means going short — selling, expecting the price to drop. The forex agent runs on ports 8130 (Account 1, automated) and 8131 (Account 2, Chris manual)."),
        ("How does the phone system work?",
         "The Hive phone system uses Twilio for 9 phone lines. Calls come in through Cloudflare tunnel (phone.hivedynamics.ai → port 8110), get processed by the swarm phone brain (port 8120) which uses Queen Bee v2 model for fast responses. The brain has 108 response patterns, 247 insights, and 2 caller memories. Interactive calls go through port 8098."),
        ("What is Coach Zero?",
         "Coach Zero is the Hive's personal development AI, powered by gemma2-coach. It pushes Chris to take action, build systems, and grow every day. It can make directive calls via the /call/directive endpoint, using Polly.Matthew-Neural voice through Twilio."),
        ("How does the Telegram bot work?",
         "The Telegram bot (v7) is the command center — natural language dispatch, 5 concurrent jobs, RBAC (owner=Chris, admin=Maria, tech=Frank, advisor=Dad). It routes queries: instant local answers first, then phi4-mini brain (<5s), then Claude Code bridge for complex builds. Don't touch telegram_agent.py unless asked."),
        ("What databases does the Hive use?",
         "The Hive has 33+ SQLite databases. Key ones: tracker.db (601MB, needs pruning), forex.db (5.3MB, 256 trades), nerve.db (65K+ facts), telephony.db (124 calls), tech_kb.db (459 research notes), dispatch.db (9 jobs), patterns.db (35 patterns), operator.db (6 missions)."),
        ("What's the revenue strategy?",
         "Revenue pipeline: 1) Wire Stripe into all 63 service sites, 2) Local phone numbers per city via Twilio, 3) Google Business Profiles for Maps presence, 4) Directory submissions (62 citations ready), 5) Social media auto-posting, 6) Email campaigns, 7) First paying customer in Destin/FWB/Pensacola locksmith market. Also: AI Agency Platform at port 8285 with $297-$2,997/mo tiers."),
    ]

    for q, a in qa_pairs:
        result.add("gemma2_hive", make_example(system, q, a))

    result.source_counts["synthetic"] = len(qa_pairs)
    log.info(f"Synthetic: generated {len(qa_pairs)} examples")


# ---------------------------------------------------------------------------
# Main harvest orchestrator
# ---------------------------------------------------------------------------

async def run_full_harvest() -> HarvestResult:
    """Run all harvesters and write datasets."""
    log.info("=" * 60)
    log.info("STARTING FULL HARVEST")
    log.info("=" * 60)
    start_time = time.time()
    result = HarvestResult()

    # Ensure training_data directory exists on ZeroQ
    ssh_cmd(f"mkdir -p {TRAINING_DIR}")

    # Run all harvesters concurrently
    harvesters = [
        harvest_nerve(result),
        harvest_telephony(result),
        harvest_forex(result),
        harvest_dispatch(result),
        harvest_leads(result),
        harvest_tech_kb(result),
        harvest_operator(result),
        harvest_tracker(result),
        harvest_cold_caller(result),
        harvest_agent_logs(result),
        harvest_conversation_patterns(result),
        harvest_synthetic_hive(result),
    ]

    # Run with individual error handling
    for coro in harvesters:
        try:
            await coro
        except Exception as e:
            result.error(f"Harvester failed: {e}")
            log.exception("Harvester error")

    # Deduplicate and write datasets
    total_new = 0
    combined = []

    for model_name, model_info in MODELS.items():
        examples = result.examples.get(model_name, [])
        if not examples:
            continue

        # Deduplicate
        seen = set()
        unique = []
        for ex in examples:
            h = example_hash(ex)
            if h not in seen:
                seen.add(h)
                unique.append(ex)

        # Read existing file to avoid duplicates with previous harvests
        filepath = f"{TRAINING_DIR}/{model_info['file']}"
        existing_hashes = set()
        existing_content = ssh_cmd(f"cat {filepath} 2>/dev/null", timeout=30)
        existing_count = 0
        if existing_content:
            for line in existing_content.split("\n"):
                if line.strip():
                    try:
                        ex = json.loads(line)
                        existing_hashes.add(example_hash(ex))
                        existing_count += 1
                    except json.JSONDecodeError:
                        pass

        # Filter out already-existing examples
        new_examples = [ex for ex in unique if example_hash(ex) not in existing_hashes]

        if new_examples:
            # Append new examples to file on ZeroQ
            jsonl_lines = "\n".join(json.dumps(ex, ensure_ascii=False) for ex in new_examples)
            # Write to local temp, then SCP
            local_tmp = f"/tmp/harvest_{model_name}.jsonl"
            with open(local_tmp, "w") as f:
                f.write(jsonl_lines + "\n")

            # Append to remote file
            subprocess.run(
                ["scp", "-o", "ConnectTimeout=10", "-o", "StrictHostKeyChecking=no",
                 local_tmp, f"{ZEROQ_USER}@{ZEROQ_IP}:/tmp/harvest_{model_name}.jsonl"],
                capture_output=True, timeout=30
            )
            ssh_cmd(f"cat /tmp/harvest_{model_name}.jsonl >> {filepath}")
            ssh_cmd(f"rm /tmp/harvest_{model_name}.jsonl")

            total_new += len(new_examples)
            total_count = existing_count + len(new_examples)

            log.info(f"  {model_name}: +{len(new_examples)} new ({total_count} total)")
            record_growth(model_name, total_count, 0, len(new_examples))

        # Add to combined
        combined.extend(unique)

    # Write combined file
    if combined:
        seen = set()
        unique_combined = []
        for ex in combined:
            h = example_hash(ex)
            if h not in seen:
                seen.add(h)
                unique_combined.append(ex)

        combined_local = "/tmp/harvest_combined.jsonl"
        with open(combined_local, "w") as f:
            for ex in unique_combined:
                f.write(json.dumps(ex, ensure_ascii=False) + "\n")
        subprocess.run(
            ["scp", "-o", "ConnectTimeout=10", "-o", "StrictHostKeyChecking=no",
             combined_local, f"{ZEROQ_USER}@{ZEROQ_IP}:{TRAINING_DIR}/{COMBINED_FILE}"],
            capture_output=True, timeout=60
        )

    duration = time.time() - start_time
    sources = ", ".join(f"{k}:{v}" for k, v in result.source_counts.items())
    errors = "; ".join(result.errors) if result.errors else "none"
    record_harvest(duration, total_new, sources, errors)

    log.info(f"HARVEST COMPLETE: {total_new} new examples in {duration:.1f}s")
    log.info(f"Sources: {sources}")
    if result.errors:
        log.warning(f"Errors: {errors}")

    return result


# ---------------------------------------------------------------------------
# Background loop
# ---------------------------------------------------------------------------

async def harvest_loop(app: web.Application):
    """Background harvest loop — runs every 30 minutes."""
    log.info(f"Harvest loop starting (interval: {HARVEST_INTERVAL}s)")
    while True:
        try:
            app["last_harvest"] = await run_full_harvest()
            app["last_harvest_time"] = datetime.now().isoformat()
        except Exception as e:
            log.exception(f"Harvest loop error: {e}")
        await asyncio.sleep(HARVEST_INTERVAL)


# ---------------------------------------------------------------------------
# HTTP Handlers
# ---------------------------------------------------------------------------

async def handle_dashboard(request: web.Request) -> web.Response:
    """Main dashboard — dataset sizes, growth, last harvest."""
    last_time = request.app.get("last_harvest_time", "never")
    last_result = request.app.get("last_harvest")

    # Get dataset info
    datasets_info = []
    for model_name, model_info in MODELS.items():
        filepath = f"{TRAINING_DIR}/{model_info['file']}"
        count = ssh_cmd(f"wc -l < {filepath} 2>/dev/null || echo 0", timeout=10).strip()
        size = ssh_cmd(f"du -sh {filepath} 2>/dev/null | cut -f1 || echo '0'", timeout=10).strip()
        datasets_info.append({
            "name": model_name,
            "file": model_info["file"],
            "count": count or "0",
            "size": size or "0",
        })

    # Combined
    combined_count = ssh_cmd(f"wc -l < {TRAINING_DIR}/{COMBINED_FILE} 2>/dev/null || echo 0", timeout=10).strip()

    # Growth data
    conn = sqlite3.connect(GROWTH_DB)
    growth_rows = conn.execute(
        "SELECT timestamp, model, example_count, new_examples FROM growth ORDER BY id DESC LIMIT 100"
    ).fetchall()
    harvest_rows = conn.execute(
        "SELECT timestamp, duration_seconds, total_new, sources FROM harvest_log ORDER BY id DESC LIMIT 20"
    ).fetchall()
    conn.close()

    # Build growth chart data
    growth_chart = defaultdict(list)
    for ts, model, count, new in growth_rows:
        growth_chart[model].append({"time": ts[:16], "count": count, "new": new})

    # Source counts
    source_info = {}
    if last_result:
        source_info = last_result.source_counts

    errors_html = ""
    if last_result and last_result.errors:
        errors_html = "<br>".join(f"<span class='error'>{e}</span>" for e in last_result.errors)

    dataset_rows = ""
    for d in datasets_info:
        dataset_rows += f"""
        <tr>
            <td>{d['name']}</td>
            <td>{d['file']}</td>
            <td class="num">{d['count']}</td>
            <td>{d['size']}</td>
            <td>
                <button onclick="viewDataset('{d['name']}')" class="btn btn-sm">View</button>
                <button onclick="trainModel('{d['name']}')" class="btn btn-sm btn-accent">Train</button>
            </td>
        </tr>"""

    harvest_rows_html = ""
    for ts, dur, total, sources in harvest_rows:
        harvest_rows_html += f"""
        <tr>
            <td>{ts[:19]}</td>
            <td>{dur:.1f}s</td>
            <td class="num">+{total}</td>
            <td>{sources}</td>
        </tr>"""

    source_rows = ""
    for src, cnt in source_info.items():
        source_rows += f"<tr><td>{src}</td><td class='num'>{cnt}</td></tr>"

    # Build growth chart JSON
    chart_data = json.dumps(dict(growth_chart))

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Hive Training Data Harvester</title>
<style>
:root {{ --bg: #0a0a0f; --surface: #12121a; --border: #1e1e2e; --text: #c8c8d0;
         --amber: #f0a030; --amber-dim: #8a6020; --green: #40c060; --red: #e04040;
         --blue: #4080f0; }}
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ background: var(--bg); color: var(--text); font-family: 'JetBrains Mono', 'Fira Code', monospace;
        font-size: 14px; padding: 20px; }}
h1 {{ color: var(--amber); font-size: 24px; margin-bottom: 5px; }}
h2 {{ color: var(--amber-dim); font-size: 16px; margin: 20px 0 10px; border-bottom: 1px solid var(--border); padding-bottom: 5px; }}
.subtitle {{ color: #666; font-size: 12px; margin-bottom: 20px; }}
.grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin: 15px 0; }}
.card {{ background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 15px; }}
.card .label {{ color: #666; font-size: 11px; text-transform: uppercase; }}
.card .value {{ color: var(--amber); font-size: 28px; font-weight: bold; margin: 5px 0; }}
.card .detail {{ color: #888; font-size: 11px; }}
table {{ width: 100%; border-collapse: collapse; background: var(--surface); border-radius: 8px; overflow: hidden; margin: 10px 0; }}
th {{ background: #1a1a25; color: var(--amber-dim); text-align: left; padding: 10px 12px; font-size: 11px;
      text-transform: uppercase; letter-spacing: 1px; }}
td {{ padding: 8px 12px; border-top: 1px solid var(--border); font-size: 13px; }}
tr:hover {{ background: #15151f; }}
.num {{ color: var(--amber); font-weight: bold; text-align: right; }}
.btn {{ background: var(--surface); color: var(--text); border: 1px solid var(--border); padding: 4px 12px;
        border-radius: 4px; cursor: pointer; font-family: inherit; font-size: 12px; }}
.btn:hover {{ background: #1a1a25; border-color: var(--amber-dim); }}
.btn-accent {{ border-color: var(--amber-dim); color: var(--amber); }}
.btn-accent:hover {{ background: var(--amber-dim); color: #000; }}
.btn-big {{ padding: 10px 24px; font-size: 14px; }}
.error {{ color: var(--red); font-size: 12px; }}
.success {{ color: var(--green); font-size: 12px; }}
#modal {{ display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%;
          background: rgba(0,0,0,0.8); z-index: 100; justify-content: center; align-items: center; }}
#modal.show {{ display: flex; }}
#modal-content {{ background: var(--surface); border: 1px solid var(--amber-dim); border-radius: 8px;
                  padding: 20px; max-width: 800px; width: 90%; max-height: 80vh; overflow-y: auto; }}
#modal-content pre {{ color: var(--text); font-size: 12px; white-space: pre-wrap; word-break: break-all; }}
.status-bar {{ display: flex; gap: 20px; align-items: center; margin: 15px 0; flex-wrap: wrap; }}
.pulse {{ display: inline-block; width: 8px; height: 8px; border-radius: 50%; background: var(--green);
          animation: pulse 2s infinite; }}
@keyframes pulse {{ 0%, 100% {{ opacity: 1; }} 50% {{ opacity: 0.3; }} }}
.chart-bar {{ height: 20px; background: var(--amber-dim); border-radius: 2px; margin: 2px 0;
              transition: width 0.3s; min-width: 2px; }}
.chart-label {{ display: flex; justify-content: space-between; font-size: 11px; color: #666; }}
</style>
</head>
<body>
<h1>HIVE TRAINING DATA HARVESTER</h1>
<p class="subtitle">Automatic training data generation from all Hive sources | Port {PORT}</p>

<div class="status-bar">
    <span><span class="pulse"></span> Harvest loop active ({HARVEST_INTERVAL//60}min interval)</span>
    <span>Last harvest: <b>{last_time}</b></span>
    <button onclick="triggerHarvest()" class="btn btn-big btn-accent">HARVEST NOW</button>
</div>

<div class="grid">
    <div class="card">
        <div class="label">Total Datasets</div>
        <div class="value">{len(MODELS)}</div>
        <div class="detail">specialist models</div>
    </div>
    <div class="card">
        <div class="label">Combined Examples</div>
        <div class="value">{combined_count}</div>
        <div class="detail">{COMBINED_FILE}</div>
    </div>
    <div class="card">
        <div class="label">Sources Active</div>
        <div class="value">{len(source_info)}</div>
        <div class="detail">databases + logs</div>
    </div>
    <div class="card">
        <div class="label">Harvest Runs</div>
        <div class="value">{len(harvest_rows)}</div>
        <div class="detail">tracked harvests</div>
    </div>
</div>

<h2>DATASETS</h2>
<table>
<tr><th>Model</th><th>File</th><th>Examples</th><th>Size</th><th>Actions</th></tr>
{dataset_rows}
</table>

<h2>SOURCE BREAKDOWN (Last Harvest)</h2>
<table>
<tr><th>Source</th><th>Examples</th></tr>
{source_rows if source_rows else '<tr><td colspan="2">No harvest yet — click HARVEST NOW</td></tr>'}
</table>

{f'<h2>ERRORS</h2><div class="card">{errors_html}</div>' if errors_html else ''}

<h2>DATASET GROWTH (Bar Chart)</h2>
<div class="card" id="growth-chart"></div>

<h2>HARVEST HISTORY</h2>
<table>
<tr><th>Time</th><th>Duration</th><th>New Examples</th><th>Sources</th></tr>
{harvest_rows_html if harvest_rows_html else '<tr><td colspan="4">No harvests recorded yet</td></tr>'}
</table>

<div id="modal" onclick="if(event.target===this)closeModal()">
    <div id="modal-content"><pre id="modal-text">Loading...</pre></div>
</div>

<script>
const chartData = {chart_data};

function renderGrowthChart() {{
    const container = document.getElementById('growth-chart');
    let html = '';
    const allModels = Object.keys(chartData);
    if (allModels.length === 0) {{
        container.innerHTML = '<p style="color:#666">No growth data yet. Run a harvest first.</p>';
        return;
    }}
    let maxCount = 0;
    allModels.forEach(m => {{
        chartData[m].forEach(d => {{ if (d.count > maxCount) maxCount = d.count; }});
    }});
    allModels.forEach(model => {{
        const latest = chartData[model][0];
        if (!latest) return;
        const pct = maxCount > 0 ? (latest.count / maxCount * 100) : 0;
        html += `<div class="chart-label"><span>${{model}}</span><span>${{latest.count}} examples (+${{latest.new}} new)</span></div>`;
        html += `<div class="chart-bar" style="width:${{Math.max(pct, 1)}}%"></div>`;
    }});
    container.innerHTML = html;
}}

async function triggerHarvest() {{
    const btn = event.target;
    btn.textContent = 'HARVESTING...';
    btn.disabled = true;
    try {{
        const resp = await fetch('/api/harvest', {{ method: 'POST' }});
        const data = await resp.json();
        btn.textContent = `DONE: +${{data.new_examples}} examples`;
        setTimeout(() => location.reload(), 2000);
    }} catch(e) {{
        btn.textContent = 'ERROR — retry';
        btn.disabled = false;
    }}
}}

async function viewDataset(name) {{
    document.getElementById('modal').classList.add('show');
    document.getElementById('modal-text').textContent = 'Loading...';
    try {{
        const resp = await fetch(`/api/dataset/${{name}}`);
        const data = await resp.json();
        document.getElementById('modal-text').textContent = JSON.stringify(data.samples, null, 2);
    }} catch(e) {{
        document.getElementById('modal-text').textContent = 'Error loading dataset';
    }}
}}

async function trainModel(name) {{
    if (!confirm(`Start training for ${{name}}?`)) return;
    try {{
        const resp = await fetch(`/api/train/${{name}}`, {{ method: 'POST' }});
        const data = await resp.json();
        alert(data.message || JSON.stringify(data));
    }} catch(e) {{
        alert('Training trigger failed: ' + e.message);
    }}
}}

function closeModal() {{ document.getElementById('modal').classList.remove('show'); }}
document.addEventListener('keydown', e => {{ if(e.key==='Escape') closeModal(); }});
renderGrowthChart();
</script>
</body>
</html>"""
    return web.Response(text=html, content_type="text/html")


async def handle_harvest(request: web.Request) -> web.Response:
    """POST /api/harvest — Trigger full harvest."""
    result = await run_full_harvest()
    request.app["last_harvest"] = result
    request.app["last_harvest_time"] = datetime.now().isoformat()

    total_new = sum(len(exs) for exs in result.examples.values())
    return web.json_response({
        "status": "complete",
        "new_examples": total_new,
        "sources": result.source_counts,
        "errors": result.errors,
        "timestamp": datetime.now().isoformat(),
    })


async def handle_datasets(request: web.Request) -> web.Response:
    """GET /api/datasets — List all datasets with line counts."""
    datasets = []
    for model_name, model_info in MODELS.items():
        filepath = f"{TRAINING_DIR}/{model_info['file']}"
        count = ssh_cmd(f"wc -l < {filepath} 2>/dev/null || echo 0", timeout=10).strip()
        size = ssh_cmd(f"stat -c%s {filepath} 2>/dev/null || echo 0", timeout=10).strip()
        datasets.append({
            "model": model_name,
            "file": model_info["file"],
            "examples": int(count) if count.isdigit() else 0,
            "bytes": int(size) if size.isdigit() else 0,
        })
    # Combined
    combined_count = ssh_cmd(f"wc -l < {TRAINING_DIR}/{COMBINED_FILE} 2>/dev/null || echo 0", timeout=10).strip()
    datasets.append({
        "model": "combined_all",
        "file": COMBINED_FILE,
        "examples": int(combined_count) if combined_count.isdigit() else 0,
    })
    return web.json_response({"datasets": datasets})


async def handle_dataset_view(request: web.Request) -> web.Response:
    """GET /api/dataset/{name} — View samples from a dataset."""
    name = request.match_info["name"]
    model_info = MODELS.get(name)
    if not model_info:
        return web.json_response({"error": f"Unknown model: {name}"}, status=404)

    filepath = f"{TRAINING_DIR}/{model_info['file']}"
    # Get first 10 and last 10 lines
    head = ssh_cmd(f"head -10 {filepath} 2>/dev/null", timeout=15)
    tail = ssh_cmd(f"tail -10 {filepath} 2>/dev/null", timeout=15)
    count = ssh_cmd(f"wc -l < {filepath} 2>/dev/null || echo 0", timeout=10).strip()

    samples = []
    for line in (head + "\n" + tail).split("\n"):
        line = line.strip()
        if line:
            try:
                samples.append(json.loads(line))
            except json.JSONDecodeError:
                pass

    # Deduplicate samples
    seen = set()
    unique_samples = []
    for s in samples:
        h = json.dumps(s)
        if h not in seen:
            seen.add(h)
            unique_samples.append(s)

    return web.json_response({
        "model": name,
        "file": model_info["file"],
        "total_examples": int(count) if count.isdigit() else 0,
        "system_prompt": model_info["system"],
        "samples": unique_samples[:20],
    })


async def handle_growth(request: web.Request) -> web.Response:
    """GET /api/growth — Growth metrics over time."""
    conn = sqlite3.connect(GROWTH_DB)
    conn.row_factory = sqlite3.Row

    growth = [dict(r) for r in conn.execute(
        "SELECT * FROM growth ORDER BY id DESC LIMIT 200"
    ).fetchall()]

    harvests = [dict(r) for r in conn.execute(
        "SELECT * FROM harvest_log ORDER BY id DESC LIMIT 50"
    ).fetchall()]

    # Daily summary
    daily = {}
    for g in growth:
        day = g["timestamp"][:10]
        if day not in daily:
            daily[day] = {"total_new": 0, "models_updated": set()}
        daily[day]["total_new"] += g.get("new_examples", 0)
        daily[day]["models_updated"].add(g["model"])

    daily_summary = [
        {"date": day, "new_examples": d["total_new"], "models_updated": len(d["models_updated"])}
        for day, d in sorted(daily.items(), reverse=True)
    ]

    conn.close()
    return web.json_response({
        "growth_snapshots": growth[:50],
        "harvest_history": harvests,
        "daily_summary": daily_summary,
    })


async def handle_train(request: web.Request) -> web.Response:
    """POST /api/train/{model} — Trigger training for a model."""
    model_name = request.match_info["model"]
    model_info = MODELS.get(model_name)
    if not model_info:
        return web.json_response({"error": f"Unknown model: {model_name}"}, status=404)

    filepath = f"{TRAINING_DIR}/{model_info['file']}"
    count = ssh_cmd(f"wc -l < {filepath} 2>/dev/null || echo 0", timeout=10).strip()
    count = int(count) if count.isdigit() else 0

    if count < 10:
        return web.json_response({
            "error": f"Not enough training data for {model_name}. Have {count}, need at least 10.",
            "suggestion": "Run a harvest first: POST /api/harvest"
        }, status=400)

    # Generate Modelfile for Ollama
    ollama_model_name = model_name.replace("_", "-")
    modelfile_content = f"""FROM gemma2:2b
SYSTEM \"\"\"{model_info['system']}\"\"\"
PARAMETER temperature 0.7
PARAMETER top_p 0.9
PARAMETER num_ctx 2048
"""
    # Write Modelfile to ZeroQ
    modelfile_path = f"{TRAINING_DIR}/{model_name}_Modelfile"
    local_mf = f"/tmp/{model_name}_Modelfile"
    with open(local_mf, "w") as f:
        f.write(modelfile_content)

    subprocess.run(
        ["scp", "-o", "ConnectTimeout=10", "-o", "StrictHostKeyChecking=no",
         local_mf, f"{ZEROQ_USER}@{ZEROQ_IP}:{modelfile_path}"],
        capture_output=True, timeout=30
    )

    return web.json_response({
        "message": f"Training prep complete for {ollama_model_name}",
        "model": ollama_model_name,
        "training_data": filepath,
        "example_count": count,
        "modelfile": modelfile_path,
        "next_steps": [
            f"1. SCP {filepath} to A100 instance",
            f"2. Run LoRA training: python3 /workspace/vastai_train.py --data {model_info['file']}",
            f"3. Convert to GGUF Q4_K_M",
            f"4. Import to Ollama: ollama create {ollama_model_name} -f {modelfile_path}",
        ],
    })


async def handle_conversation_patterns(request: web.Request) -> web.Response:
    """GET /api/conversation-patterns — Chris's extracted rules/preferences."""
    conn = sqlite3.connect(GROWTH_DB)
    conn.row_factory = sqlite3.Row
    patterns = [dict(r) for r in conn.execute(
        "SELECT * FROM conversation_patterns ORDER BY frequency DESC, last_seen DESC LIMIT 100"
    ).fetchall()]
    conn.close()

    # Categorize
    categories = defaultdict(list)
    for p in patterns:
        categories[p.get("category", "general")].append(p)

    return web.json_response({
        "total_patterns": len(patterns),
        "categories": {k: v for k, v in categories.items()},
        "top_10": patterns[:10],
    })


async def handle_health(request: web.Request) -> web.Response:
    """GET /health — Health check."""
    return web.json_response({
        "status": "healthy",
        "service": "hive-training-harvester",
        "port": PORT,
        "uptime": time.time() - request.app["start_time"],
        "last_harvest": request.app.get("last_harvest_time", "never"),
    })


async def handle_status(request: web.Request) -> web.Response:
    """GET /status — Detailed status."""
    last = request.app.get("last_harvest")
    conn = sqlite3.connect(GROWTH_DB)
    total_harvests = conn.execute("SELECT count(*) FROM harvest_log").fetchone()[0]
    total_growth = conn.execute("SELECT count(*) FROM growth").fetchone()[0]
    total_patterns = conn.execute("SELECT count(*) FROM conversation_patterns").fetchone()[0]
    conn.close()

    return web.json_response({
        "service": "hive-training-harvester",
        "port": PORT,
        "harvest_interval_seconds": HARVEST_INTERVAL,
        "models_tracked": len(MODELS),
        "databases_monitored": len(DATABASES),
        "total_harvests": total_harvests,
        "total_growth_snapshots": total_growth,
        "total_conversation_patterns": total_patterns,
        "last_harvest_time": request.app.get("last_harvest_time", "never"),
        "last_harvest_sources": last.source_counts if last else {},
        "last_harvest_errors": last.errors if last else [],
        "uptime_seconds": time.time() - request.app["start_time"],
    })


# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

async def on_startup(app: web.Application):
    """Initialize on startup."""
    app["start_time"] = time.time()
    init_growth_db()
    os.makedirs(LOCAL_CACHE, exist_ok=True)
    log.info(f"Training Data Harvester starting on port {PORT}")
    log.info(f"Target: {ZEROQ_USER}@{ZEROQ_IP}")
    log.info(f"Training dir: {TRAINING_DIR}")
    log.info(f"Models tracked: {len(MODELS)}")
    log.info(f"Harvest interval: {HARVEST_INTERVAL}s")

    # Start background harvest loop
    app["harvest_task"] = asyncio.create_task(harvest_loop(app))


async def on_cleanup(app: web.Application):
    """Cleanup on shutdown."""
    task = app.get("harvest_task")
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


def create_app() -> web.Application:
    app = web.Application()

    # Routes
    app.router.add_get("/", handle_dashboard)
    app.router.add_post("/api/harvest", handle_harvest)
    app.router.add_get("/api/datasets", handle_datasets)
    app.router.add_get("/api/dataset/{name}", handle_dataset_view)
    app.router.add_get("/api/growth", handle_growth)
    app.router.add_post("/api/train/{model}", handle_train)
    app.router.add_get("/api/conversation-patterns", handle_conversation_patterns)
    app.router.add_get("/health", handle_health)
    app.router.add_get("/status", handle_status)

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    return app


if __name__ == "__main__":
    app = create_app()
    log.info("=" * 60)
    log.info("  HIVE TRAINING DATA HARVESTER")
    log.info(f"  Port: {PORT}")
    log.info(f"  Dashboard: http://0.0.0.0:{PORT}/")
    log.info(f"  Harvest interval: {HARVEST_INTERVAL // 60} minutes")
    log.info("=" * 60)
    web.run_app(app, host="0.0.0.0", port=PORT, print=None)
