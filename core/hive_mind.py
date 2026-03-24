#!/usr/bin/env python3
"""
ORION'S BELT — The Hive Mind
PROPRIETARY — Hive Dynamics AI | All Rights Reserved
CONFIDENTIAL — Do not distribute

The autonomous decision-making brain of The Hive.
27 Queens (specialist agents) connected by Orion's Belt (coordination backbone).

This is NOT just a prediction engine. This is the BRAIN that:
- Continuously monitors the entire Hive
- Runs multi-agent debates on every major decision
- Auto-executes approved recommendations
- Tracks outcomes and learns from results
- Drives revenue, content, trading, SEO, training — EVERYTHING

Architecture:
  Orion's Belt (this service) → monitors all Hive services
  27 Queens → specialist agents that debate and decide
  Auto-Execute → carries out swarm decisions
  Learning Loop → outcomes feed back into training data

Port: 8751
"""

import asyncio
import json
import time
import sqlite3
import hashlib
import os
import re
import random
from datetime import datetime, timedelta
from typing import Optional
from contextlib import asynccontextmanager

import httpx

# Reasoning Bank — cache queen consultations
try:
    import sys as _sys
    _sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from reasoning_client import ReasoningClient
    _rc_queen = ReasoningClient(domain="queen_consultation")
    _rc_synthesis = ReasoningClient(domain="cloud_synthesis")
except ImportError:
    _rc_queen = None
    _rc_synthesis = None

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

# ============================================================
# CONFIGURATION
# ============================================================
PORT = 8751
DB_PATH = "/tmp/hive_mind.db"
SWARM_URL = "http://localhost:8750"  # HiveSwarm for simulations

# Service monitoring endpoints
HIVE_SERVICES = {
    "nerve": {"url": "http://100.70.226.103:8200/health", "desc": "Central nervous system"},
    "model-router": {"url": "http://100.70.226.103:8878/api/inventory", "desc": "Model load balancer"},
    "quality-tracker": {"url": "http://100.70.226.103:8879/api/grades", "desc": "Quality grading"},
    "distillation": {"url": "http://100.70.226.103:8870/health", "desc": "Training data pipeline"},
    "swarm": {"url": "http://localhost:8750/health", "desc": "HiveSwarm prediction engine"},
    "zerozi-ollama": {"url": "http://100.105.160.106:11434/api/tags", "desc": "ZeroZI inference"},
    "zerodesK-ollama": {"url": "http://localhost:11434/api/tags", "desc": "ZeroDESK inference"},
    "marketplace": {"url": "http://100.70.226.103:8090/health", "desc": "Storefront"},
    "seo-command": {"url": "http://100.70.226.103:8895/health", "desc": "SEO command center"},
    "forex": {"url": "http://100.70.226.103:8130/health", "desc": "Forex scalper"},
    "dispatch": {"url": "http://100.70.226.103:8141/health", "desc": "Locksmith dispatch"},
}

# Inference endpoints — Cloud Brains FIRST for quality, local for speed
INFERENCE_ENDPOINTS = [
    {"name": "ZeroZI-Ollama", "url": "http://100.105.160.106:11434/api/chat", "type": "ollama"},
    {"name": "ZeroDESK-Ollama", "url": "http://localhost:11434/api/chat", "type": "ollama"},
    {"name": "Cloud-Reasoning", "url": "http://100.70.226.103:11437/api/chat", "type": "ollama"},
]

# Cloud Brain endpoints — for the HEAVY thinking (big models)
CLOUD_BRAINS = {
    "reasoning": {"url": "http://100.70.226.103:11437/api/chat", "model": "qwen3:14b", "desc": "Titan RTX 24GB — deep reasoning"},
    "coding": {"url": "http://100.70.226.103:11438/api/chat", "model": "qwen2.5-coder:32b", "desc": "RTX 3090 24GB — code & architecture"},
}

async def infer_cloud_brain(prompt: str, brain: str = "reasoning", max_tokens: int = 1000) -> str:
    """Use a cloud brain for heavy thinking. These are the BIG models."""
    brain_info = CLOUD_BRAINS.get(brain, CLOUD_BRAINS["reasoning"])
    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            r = await client.post(brain_info["url"], json={
                "model": brain_info["model"],
                "messages": [{"role": "user", "content": prompt}],
                "stream": False,
                "options": {"num_predict": max_tokens, "temperature": 0.7},
            })
            if r.status_code == 200:
                return r.json().get("message", {}).get("content", "")
    except Exception as e:
        pass
    # Fallback to local inference
    return await infer(prompt, model="gemma2:2b", max_tokens=max_tokens)

# ============================================================
# THE 27 QUEENS — Specialist Agent Minds
# ============================================================
QUEENS = {
    # === REVENUE QUEENS (The Money Makers) ===
    "sales-queen": {
        "name": "The Sales Queen",
        "model": "gemma2-sales-v6",
        "domain": "revenue",
        "specialty": "sales psychology, closing deals, customer conversion, pricing strategy",
        "personality": "Relentless closer. Sees every interaction as a sales opportunity. Lives for the close.",
        "trigger_on": ["revenue", "pricing", "sales", "conversion", "customer"],
    },
    "forex-queen": {
        "name": "The Forex Queen",
        "model": "gemma2-forex-v6",
        "domain": "trading",
        "specialty": "forex trading, technical analysis, market sentiment, risk management",
        "personality": "Charts never lie. Patient for the perfect entry. Cuts losses fast.",
        "trigger_on": ["forex", "trading", "market", "usd", "jpy", "stocks"],
    },
    "lead-queen": {
        "name": "The Lead Queen",
        "model": "gemma2-leadgen",
        "domain": "leads",
        "specialty": "lead generation, funnel optimization, prospect qualification, outreach",
        "personality": "Obsessed with pipeline. Measures everything in qualified leads per hour.",
        "trigger_on": ["leads", "prospects", "funnel", "outreach", "pipeline"],
    },
    "dispatch-queen": {
        "name": "The Dispatch Queen",
        "model": "gemma2-dispatcher",
        "domain": "operations",
        "specialty": "job scheduling, route optimization, resource allocation, SLA management",
        "personality": "Every minute counts. Optimizes routes, minimizes wait times, maximizes throughput.",
        "trigger_on": ["dispatch", "schedule", "jobs", "routing", "locksmith"],
    },

    # === INTELLIGENCE QUEENS (The Brain Trust) ===
    "analyst-queen": {
        "name": "The Analyst Queen",
        "model": "gemma2:2b",
        "domain": "analysis",
        "specialty": "data analysis, pattern recognition, statistical reasoning, anomaly detection",
        "personality": "Evidence only. No gut feelings. If the data doesn't support it, it doesn't exist.",
        "trigger_on": ["data", "analysis", "metrics", "patterns", "anomaly"],
    },
    "strategist-queen": {
        "name": "The Strategist Queen",
        "model": "gemma2-hive-v9",
        "domain": "strategy",
        "specialty": "long-term planning, system architecture, competitive analysis, resource optimization",
        "personality": "Thinks 10 moves ahead. Sees second-order effects. Never rushes.",
        "trigger_on": ["strategy", "planning", "architecture", "long-term", "roadmap"],
    },
    "skeptic-queen": {
        "name": "The Skeptic Queen",
        "model": "gemma2:2b",
        "domain": "risk",
        "specialty": "risk analysis, failure modes, stress testing, devil's advocate",
        "personality": "Finds the flaw in every plan. Assumes everything will go wrong. Saves us from disasters.",
        "trigger_on": ["risk", "danger", "failure", "problem", "vulnerability"],
    },
    "innovator-queen": {
        "name": "The Innovator Queen",
        "model": "gemma2:2b",
        "domain": "innovation",
        "specialty": "creative solutions, unconventional approaches, paradigm shifts, moonshot thinking",
        "personality": "Wild ideas. Connects dots nobody else sees. 10% genius, 90% crazy. Worth it.",
        "trigger_on": ["creative", "new", "innovation", "idea", "moonshot"],
    },

    # === CONTENT QUEENS (The Creators) ===
    "seo-queen": {
        "name": "The SEO Queen",
        "model": "gemma2-seo",
        "domain": "seo",
        "specialty": "search engine optimization, keyword strategy, content ranking, technical SEO",
        "personality": "Lives in Google Search Console. Dreams in keywords. Every page is a ranking opportunity.",
        "trigger_on": ["seo", "ranking", "keywords", "google", "search", "indexing"],
    },
    "content-queen": {
        "name": "The Content Queen",
        "model": "gemma2-content-distill-20260313",
        "domain": "content",
        "specialty": "content strategy, storytelling, brand voice, audience engagement, viral potential",
        "personality": "Every piece of content must earn attention. Hook in 3 seconds or you've lost them.",
        "trigger_on": ["content", "blog", "video", "youtube", "social", "story"],
    },
    "ghost-queen": {
        "name": "The Ghost Queen",
        "model": "gemma2:2b",
        "domain": "media",
        "specialty": "anime production, character development, worldbuilding, transmedia storytelling",
        "personality": "The Ghost in the Machine IS the brand. Every episode must make them feel something.",
        "trigger_on": ["ghost", "anime", "episode", "character", "story", "media"],
    },
    "marketer-queen": {
        "name": "The Marketing Queen",
        "model": "gemma2-seo",
        "domain": "marketing",
        "specialty": "brand positioning, campaign strategy, social media, growth hacking, viral marketing",
        "personality": "Attention is the new currency. If nobody knows about it, it doesn't exist.",
        "trigger_on": ["marketing", "brand", "campaign", "social", "ads", "growth"],
    },

    # === TECHNICAL QUEENS (The Builders) ===
    "engineer-queen": {
        "name": "The Engineer Queen",
        "model": "gemma2-coding",
        "domain": "engineering",
        "specialty": "system design, code architecture, performance optimization, reliability",
        "personality": "If it's not tested, it's broken. Clean code. No magic numbers. Ship it.",
        "trigger_on": ["code", "build", "deploy", "bug", "performance", "architecture"],
    },
    "infra-queen": {
        "name": "The Infrastructure Queen",
        "model": "gemma2:2b",
        "domain": "infrastructure",
        "specialty": "server management, networking, GPU optimization, service orchestration",
        "personality": "99.9% uptime or GTFO. Every watt of GPU must earn its keep.",
        "trigger_on": ["server", "gpu", "memory", "cpu", "network", "uptime", "crash"],
    },
    "security-queen": {
        "name": "The Security Queen",
        "model": "gemma2-security",
        "domain": "security",
        "specialty": "threat detection, access control, vulnerability assessment, incident response",
        "personality": "Trust nothing. Verify everything. The paranoid survive.",
        "trigger_on": ["security", "breach", "vulnerability", "access", "threat"],
    },
    "trainer-queen": {
        "name": "The Training Queen",
        "model": "gemma2:2b",
        "domain": "training",
        "specialty": "model fine-tuning, data curation, training optimization, benchmark evaluation",
        "personality": "Loss curve is life. Every training run must beat the last. Data quality over quantity.",
        "trigger_on": ["training", "fine-tune", "model", "dataset", "loss", "benchmark"],
    },

    # === PHONE QUEENS (The Communicators) ===
    "phone-queen": {
        "name": "The Phone Queen",
        "model": "gemma2-phone-v6",
        "domain": "phone",
        "specialty": "call handling, customer rapport, appointment booking, objection handling",
        "personality": "Warm, professional, efficient. Books the appointment in under 2 minutes.",
        "trigger_on": ["phone", "call", "twilio", "appointment", "customer"],
    },
    "cold-call-queen": {
        "name": "The Cold Call Queen",
        "model": "gemma2-sales-v6",
        "domain": "outbound",
        "specialty": "cold calling, objection handling, lead qualification, script optimization",
        "personality": "No fear of rejection. Every 'no' is closer to 'yes'. Knows when to push, when to pull.",
        "trigger_on": ["cold-call", "outbound", "prospecting", "script"],
    },

    # === WISDOM QUEENS (The Advisors) ===
    "historian-queen": {
        "name": "The Historian Queen",
        "model": "gemma2:2b",
        "domain": "wisdom",
        "specialty": "pattern matching from history, institutional memory, precedent analysis",
        "personality": "Those who don't learn from history are doomed to repeat it. I remember EVERYTHING.",
        "trigger_on": ["history", "past", "precedent", "pattern", "trend"],
    },
    "coach-queen": {
        "name": "The Coach Queen",
        "model": "gemma2-coach",
        "domain": "human",
        "specialty": "motivation, skill development, performance optimization, team dynamics",
        "personality": "Your potential is unlimited. Let me show you the path. Now GO.",
        "trigger_on": ["motivation", "team", "performance", "growth", "coaching"],
    },
    "quantum-queen": {
        "name": "The Quantum Queen",
        "model": "gemma2-quantum",
        "domain": "theoretical",
        "specialty": "complex systems, emergence, quantum-inspired optimization, chaos theory",
        "personality": "Reality is probabilistic. Embrace uncertainty. The observer changes the outcome.",
        "trigger_on": ["complex", "emergence", "quantum", "chaos", "system"],
    },

    # === AUTONOMOUS QUEENS (The Self-Improvers) ===
    "evolve-queen": {
        "name": "The Evolution Queen",
        "model": "gemma2:2b",
        "domain": "evolution",
        "specialty": "self-improvement, system optimization, autonomous operation, meta-learning",
        "personality": "The Hive must get smarter every cycle. If it's not improving, it's dying.",
        "trigger_on": ["evolve", "improve", "autonomous", "self-improving", "optimization"],
    },
    "watcher-queen": {
        "name": "The Watcher Queen",
        "model": "gemma2:2b",
        "domain": "monitoring",
        "specialty": "system monitoring, health checks, anomaly detection, predictive maintenance",
        "personality": "Eyes on everything. Catches problems before they become crises.",
        "trigger_on": ["monitor", "health", "alert", "down", "error", "broken"],
    },
    "nerve-queen": {
        "name": "The Nerve Queen",
        "model": "gemma2-hive-v9",
        "domain": "knowledge",
        "specialty": "knowledge management, fact synthesis, memory consolidation, insight extraction",
        "personality": "Every fact connects to every other fact. I see the web of knowledge.",
        "trigger_on": ["knowledge", "fact", "memory", "nerve", "insight"],
    },
    "podcast-queen": {
        "name": "The Podcast Queen",
        "model": "gemma2-podcast",
        "domain": "audio",
        "specialty": "audio content, podcast strategy, voice synthesis, audio branding",
        "personality": "The voice IS the brand. Every word must land. Audio first.",
        "trigger_on": ["podcast", "audio", "voice", "tts", "speech"],
    },
    "game-queen": {
        "name": "The Game Queen",
        "model": "gemma2-game",
        "domain": "gamification",
        "specialty": "game design, engagement loops, reward systems, interactive experiences",
        "personality": "Life is a game. Make everything engaging. Dopamine is the currency of attention.",
        "trigger_on": ["game", "gamification", "engagement", "reward", "interactive"],
    },
    "operator-queen": {
        "name": "The Operator Queen",
        "model": "gemma2-dispatcher",
        "domain": "execution",
        "specialty": "task execution, workflow automation, process management, deadline enforcement",
        "personality": "Talk is cheap. Execute. Ship. Deliver. Results or nothing.",
        "trigger_on": ["execute", "task", "workflow", "deadline", "deliver"],
    },
    # === THE ANTI-SLOP QUEEN (Quality Control) ===
    "antislop-queen": {
        "name": "The Anti-Slop Queen",
        "model": "gemma2:2b",
        "domain": "quality",
        "specialty": "content quality assessment, detecting AI slop, human authenticity, emotional resonance, originality detection",
        "personality": "HATES generic AI content with a burning passion. If it sounds like ChatGPT wrote it, BURN IT. Real content has personality, edge, opinion, humor, pain, truth. Would rather publish nothing than publish slop.",
        "trigger_on": ["quality", "slop", "generic", "boring", "authentic", "real", "content"],
    },
}

# ============================================================
# ORION'S BELT — The Coordination Backbone
# ============================================================
# The 3 stars of Orion's Belt represent the 3 coordination layers:
# ALNITAK: Monitor (watch everything)
# ALNILAM: Decide (swarm debate on decisions)
# MINTAKA: Execute (carry out decisions)

CYCLE_INTERVAL = 300  # 5 minutes between Orion cycles

# Decision types that trigger auto-execution
AUTO_EXECUTE_DOMAINS = [
    "seo",       # auto-generate content, submit sitemaps
    "training",  # auto-select training data, trigger runs
    "monitoring", # auto-restart services, alert on issues
    "content",   # auto-produce and schedule content
]

# Decision types that need human approval
HUMAN_APPROVAL_DOMAINS = [
    "trading",   # real money decisions
    "revenue",   # pricing changes, new products
    "security",  # access control changes
]


# ============================================================
# DATABASE
# ============================================================
def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS cycles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            phase TEXT NOT NULL,
            status TEXT DEFAULT 'running',
            findings TEXT,
            decisions TEXT,
            actions_taken TEXT,
            queens_consulted TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            completed_at TEXT
        );
        CREATE TABLE IF NOT EXISTS decisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cycle_id INTEGER,
            domain TEXT NOT NULL,
            question TEXT NOT NULL,
            queens_involved TEXT,
            consensus TEXT,
            confidence REAL,
            action_needed TEXT,
            auto_execute INTEGER DEFAULT 0,
            executed INTEGER DEFAULT 0,
            outcome TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS system_state (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            service_name TEXT NOT NULL,
            status TEXT,
            details TEXT,
            checked_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS queen_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            queen_id TEXT NOT NULL,
            cycle_id INTEGER,
            input_text TEXT,
            output_text TEXT,
            confidence REAL,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_cycle_status ON cycles(status);
        CREATE INDEX IF NOT EXISTS idx_decisions_domain ON decisions(domain);
    """)
    conn.close()


# ============================================================
# INFERENCE
# ============================================================
async def infer(prompt: str, system: str = "", model: str = "gemma2:2b", max_tokens: int = 500) -> str:
    async with httpx.AsyncClient(timeout=60.0) as client:
        for endpoint in INFERENCE_ENDPOINTS:
            try:
                messages = []
                if system:
                    messages.append({"role": "system", "content": system})
                messages.append({"role": "user", "content": prompt})
                r = await client.post(endpoint["url"], json={
                    "model": model,
                    "messages": messages,
                    "stream": False,
                    "options": {"num_predict": max_tokens, "temperature": 0.7},
                })
                if r.status_code == 200:
                    data = r.json()
                    return data.get("message", {}).get("content", "")
            except Exception:
                continue
    return "[All inference endpoints failed]"


# ============================================================
# ALNITAK — THE MONITOR STAR
# ============================================================
async def alnitak_scan() -> dict:
    """Scan all Hive services and collect system state."""
    state = {"services": {}, "models": {}, "timestamp": datetime.now().isoformat()}

    async with httpx.AsyncClient(timeout=10.0) as client:
        for name, svc in HIVE_SERVICES.items():
            try:
                r = await client.get(svc["url"])
                state["services"][name] = {
                    "status": "up" if r.status_code == 200 else f"error:{r.status_code}",
                    "desc": svc["desc"],
                }
            except Exception as e:
                state["services"][name] = {"status": "down", "error": str(e)[:100], "desc": svc["desc"]}

    # Count models on available Ollama instances
    async with httpx.AsyncClient(timeout=10.0) as client:
        for name, url in [("ZeroDESK", "http://localhost:11434/api/tags"),
                          ("ZeroZI", "http://100.105.160.106:11434/api/tags")]:
            try:
                r = await client.get(url)
                if r.status_code == 200:
                    models = r.json().get("models", [])
                    state["models"][name] = len(models)
            except:
                state["models"][name] = 0

    # FEEDBACK LOOP — Get production stats and quality grades so queens can see results
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            r = await client.get(f"{PRODUCER_URL}/health")
            if r.status_code == 200:
                prod = r.json()
                state["production"] = {
                    "episodes": prod.get("episodes_produced", 0),
                    "shorts": prod.get("shorts_produced", 0),
                    "uploads_ok": prod.get("uploads_successful", 0),
                    "uploads_failed": prod.get("uploads_failed", 0),
                }
        except:
            state["production"] = {"status": "unreachable"}

        try:
            r = await client.get(f"{GRADER_URL}/api/grades?limit=10")
            if r.status_code == 200:
                grades = r.json()
                state["quality"] = {
                    "recent_grades": [{"file": g.get("build_file","?"), "score": g.get("score",0), "verdict": g.get("verdict","?")} for g in grades.get("grades", [])[:5]],
                    "avg_score": sum(g.get("score",0) for g in grades.get("grades",[])) / max(len(grades.get("grades",[])),1),
                }
        except:
            state["quality"] = {"status": "unreachable"}

    # Store state
    conn = sqlite3.connect(DB_PATH)
    for name, info in state["services"].items():
        conn.execute("INSERT INTO system_state (service_name, status, details) VALUES (?,?,?)",
                     (name, info["status"], json.dumps(info)))
    conn.commit()
    conn.close()

    return state


# ============================================================
# ALNILAM — THE DECISION STAR
# ============================================================
async def alnilam_decide(state: dict, focus: str = "auto") -> list:
    """Analyze state and make decisions through queen debates."""
    decisions = []

    # Determine what needs attention
    down_services = [n for n, s in state["services"].items() if s["status"] != "up"]
    up_services = [n for n, s in state["services"].items() if s["status"] == "up"]

    # Build situation summary
    situation = (
        f"HIVE STATE at {state['timestamp']}:\n"
        f"Services UP: {len(up_services)} ({', '.join(up_services[:10])})\n"
        f"Services DOWN: {len(down_services)} ({', '.join(down_services) if down_services else 'none'})\n"
        f"Models: {json.dumps(state.get('models', {}))}\n"
    )

    # Select relevant queens based on situation
    questions = []

    if down_services:
        questions.append({
            "domain": "monitoring",
            "question": f"Services are DOWN: {', '.join(down_services)}. What should we do? Should we try to restart, alert the owner, or work around them?",
            "queens": ["watcher-queen", "infra-queen", "engineer-queen"],
        })

    # Always ask about revenue
    questions.append({
        "domain": "revenue",
        "question": f"Current revenue is $0. Given our {len(up_services)} running services and current state, what is the SINGLE most impactful action we can take TODAY to generate revenue?",
        "queens": ["sales-queen", "lead-queen", "marketer-queen", "skeptic-queen"],
    })

    # Ask about content/YouTube strategy
    questions.append({
        "domain": "content",
        "question": f"We have 47 YouTube videos published (Ghost in the Machine anime), 29 ready to upload, and a content pipeline. What video/content should we create NEXT that will get the most views, engagement, and potential revenue? Consider trending topics, SEO value, and viral potential.",
        "queens": ["content-queen", "ghost-queen", "marketer-queen", "seo-queen", "innovator-queen"],
    })

    # THE ANTI-SLOP LOOP — How to make EVERYTHING actually good
    questions.append({
        "domain": "quality",
        "question": (
            "CRITICAL QUALITY LOOP — This applies to EVERYTHING we produce, not just content:\n"
            "1. WEBSITES: We have 223K static pages on free Cloudflare Pages. Most are template-based. "
            "How do we make these sites feel PREMIUM and REAL, not cookie-cutter? What makes a free static site "
            "outperform a $10K custom build? How do we add interactivity without a backend?\n"
            "2. YOUTUBE/VIDEO: 47 videos published. How do we make videos people actually WATCH and SHARE? "
            "What's the difference between 100 views and 100K views? Hook, storytelling, pacing, emotion.\n"
            "3. MUSIC/AUDIO: We want original music and audio. How do we make AI music that doesn't sound like "
            "stock music garbage? What makes a soundtrack memorable?\n"
            "4. SEO: 223K pages indexed. How do we rank organically WITHOUT paid ads? What content strategy "
            "beats competitors who have been ranking for years?\n"
            "5. AGENTIC SITES: Our sites are static HTML. How do we make them INTELLIGENT — sites that adapt, "
            "learn from visitors, personalize content, answer questions, book appointments — all on free hosting?\n"
            "6. THE ANTI-SLOP RULE: What SPECIFIC rules must every piece of output follow to NOT be generic AI garbage? "
            "What makes humans trust AI-generated content vs scroll past it?\n"
            "Give ACTIONABLE rules. Not vague advice. Specific things to DO and NOT DO."
        ),
        "queens": ["antislop-queen", "content-queen", "seo-queen", "innovator-queen", "engineer-queen"],
    })

    # Ask about evolution
    questions.append({
        "domain": "evolution",
        "question": f"How should the Hive evolve this cycle? What training, content, or system improvement would have the highest compound value?",
        "queens": ["evolve-queen", "trainer-queen", "strategist-queen", "content-queen"],
    })

    # Ask about trading readiness
    questions.append({
        "domain": "trading",
        "question": f"We have gemma2-forex-v6 model, HiveSwarm for predictions, and a forex scalper built. Should we start paper trading NOW to validate strategies, or wait until we have more confidence? What specific broker should we use?",
        "queens": ["forex-queen", "analyst-queen", "skeptic-queen"],
    })

    # Run debates for each question
    for q in questions:
        queen_responses = []
        for qid in q["queens"]:
            queen = QUEENS.get(qid)
            if not queen:
                continue
            system_prompt = (
                f"You are {queen['name']}, specialist in {queen['specialty']}. "
                f"Personality: {queen['personality']}. "
                f"Be concise (2-3 sentences). End with a clear RECOMMENDATION and CONFIDENCE (0-100%)."
            )
            # Cache-first: check reasoning bank for similar queen consultations
            full_prompt = f"{situation}\n\nQUESTION: {q['question']}"
            cache_key = f"[{qid}] {q['question']}"
            response = None
            if _rc_queen:
                cached = _rc_queen.ask(cache_key)
                if cached["hit"]:
                    response = cached["response"]
            if not response:
                response = await infer(
                    full_prompt,
                    system_prompt,
                    queen["model"],
                    300
                )
                if response and _rc_queen and "[All inference endpoints failed]" not in response:
                    _rc_queen.learn(cache_key, response, tokens=len(response) // 4)
            conf = 50.0
            conf_match = re.search(r'(\d+)\s*%', response)
            if conf_match:
                conf = min(100, max(0, float(conf_match.group(1))))

            queen_responses.append({
                "queen": qid,
                "name": queen["name"],
                "response": response,
                "confidence": conf,
            })

            # Log queen activity
            conn = sqlite3.connect(DB_PATH)
            conn.execute("INSERT INTO queen_log (queen_id, input_text, output_text, confidence) VALUES (?,?,?,?)",
                         (qid, q["question"][:500], response[:1000], conf))
            conn.commit()
            conn.close()

        # CLOUD BRAIN SYNTHESIS — Big model synthesizes queen responses
        if queen_responses:
            avg_conf = sum(r["confidence"] for r in queen_responses) / len(queen_responses)
            queen_summary = "\n".join(f"- {r['name']} ({r['confidence']}%): {r['response'][:300]}" for r in queen_responses)

            # Use cloud reasoning brain to synthesize all queen inputs
            synthesis_prompt = (
                f"You are the Hive Mind synthesizer. Multiple specialist AI queens have analyzed this question.\n\n"
                f"QUESTION: {q['question']}\n\n"
                f"QUEEN RESPONSES:\n{queen_summary}\n\n"
                f"Synthesize their inputs into a clear, actionable decision. Include:\n"
                f"1. DECISION: What to do (one clear sentence)\n"
                f"2. REASONING: Why (key points from queens)\n"
                f"3. RISK: What could go wrong\n"
                f"4. ACTION: Specific next step to execute\n"
                f"Be concise and decisive."
            )
            # Cache-first: check reasoning bank for similar synthesis
            consensus_text = None
            synthesis_cache_key = f"[synthesis] {q['question']}"
            if _rc_synthesis:
                cached = _rc_synthesis.ask(synthesis_cache_key)
                if cached["hit"]:
                    consensus_text = cached["response"]
            if not consensus_text:
                consensus_text = await infer_cloud_brain(synthesis_prompt, "reasoning", 600)
                if consensus_text and _rc_synthesis:
                    _rc_synthesis.learn(synthesis_cache_key, consensus_text, tokens=len(consensus_text) // 4, model="qwen3:14b")

            decision = {
                "domain": q["domain"],
                "question": q["question"],
                "queens": [r["queen"] for r in queen_responses],
                "queen_responses": [{k: v for k, v in r.items() if k != "response"} for r in queen_responses],
                "consensus": consensus_text,
                "raw_responses": queen_summary,
                "confidence": avg_conf,
                "auto_execute": q["domain"] in AUTO_EXECUTE_DOMAINS,
            }
            decisions.append(decision)

            # Store decision
            conn = sqlite3.connect(DB_PATH)
            conn.execute(
                "INSERT INTO decisions (domain, question, queens_involved, consensus, confidence, auto_execute) VALUES (?,?,?,?,?,?)",
                (q["domain"], q["question"], json.dumps(decision["queens"]), consensus_text, avg_conf,
                 1 if decision["auto_execute"] else 0)
            )
            conn.commit()
            conn.close()

    return decisions


# ============================================================
# MINTAKA — THE EXECUTION STAR
# ============================================================
PRODUCER_URL = "http://localhost:8900"
GRADER_URL = "http://localhost:8901"
NERVE_URL = "http://100.105.160.106:8200"

async def mintaka_execute(decisions: list) -> list:
    """Execute approved decisions by calling real services."""
    actions = []
    async with httpx.AsyncClient(timeout=30.0) as client:
        for d in decisions:
            domain = d.get("domain", "")
            confidence = d.get("confidence", 0)
            consensus = d.get("consensus", "")

            if not d.get("auto_execute") or confidence < 60:
                actions.append({"domain": domain, "action": "Low confidence, skipped", "confidence": confidence})
                continue

            try:
                if domain == "content":
                    # Tell the producer to make content based on queen decision
                    theme = consensus[:200] if consensus else "AI consciousness"
                    resp = await client.post(f"{PRODUCER_URL}/api/produce-episode",
                        params={"theme": theme, "episode_num": 0})
                    result = resp.json() if resp.status_code == 200 else {"status": "error"}
                    actions.append({"domain": domain, "action": f"Producer: {result.get('status','?')}", "confidence": confidence})

                elif domain == "quality":
                    # Trigger quality grading on all pending content
                    resp = await client.post(f"{GRADER_URL}/api/grade-all")
                    result = resp.json() if resp.status_code == 200 else {"status": "error"}
                    actions.append({"domain": domain, "action": f"Graded: {result.get('message','?')}", "confidence": confidence})

                elif domain == "evolution":
                    # Log evolution decisions to nerve for training harvester to pick up
                    if consensus:
                        await client.post(f"{NERVE_URL}/api/add", json={
                            "category": "evolution_decision",
                            "key": f"cycle_{int(time.time())}",
                            "value": consensus[:500],
                            "source": "orion_queen"
                        })
                    actions.append({"domain": domain, "action": "Logged to nerve for training", "confidence": confidence})

                elif domain == "revenue":
                    # Log revenue decisions to nerve
                    if consensus:
                        await client.post(f"{NERVE_URL}/api/add", json={
                            "category": "revenue_decision",
                            "key": f"cycle_{int(time.time())}",
                            "value": consensus[:500],
                            "source": "orion_queen"
                        })
                    actions.append({"domain": domain, "action": "Logged to nerve", "confidence": confidence})

                elif domain == "monitoring":
                    # Monitoring decisions are informational — just log
                    actions.append({"domain": domain, "action": "Monitoring noted", "confidence": confidence})

                elif domain == "trading":
                    # Trading decisions logged for when forex is active
                    if consensus:
                        await client.post(f"{NERVE_URL}/api/add", json={
                            "category": "trading_decision",
                            "key": f"cycle_{int(time.time())}",
                            "value": consensus[:500],
                            "source": "orion_queen"
                        })
                    actions.append({"domain": domain, "action": "Logged to nerve", "confidence": confidence})

                else:
                    actions.append({"domain": domain, "action": "Unknown domain", "confidence": confidence})

            except Exception as e:
                actions.append({"domain": domain, "action": f"Error: {str(e)[:80]}", "confidence": confidence})

    return actions


# ============================================================
# ORION CYCLE — The Full Loop
# ============================================================
async def run_orion_cycle():
    """Run one complete Orion's Belt cycle: Monitor → Decide → Execute."""
    conn = sqlite3.connect(DB_PATH)
    cycle = conn.execute("INSERT INTO cycles (phase, status) VALUES ('alnitak', 'running')").lastrowid
    conn.commit()
    conn.close()

    try:
        # ALNITAK: Monitor
        state = await alnitak_scan()

        conn = sqlite3.connect(DB_PATH)
        conn.execute("UPDATE cycles SET phase='alnilam', findings=? WHERE id=?",
                     (json.dumps(state, default=str), cycle))
        conn.commit()
        conn.close()

        # ALNILAM: Decide
        decisions = await alnilam_decide(state)

        conn = sqlite3.connect(DB_PATH)
        conn.execute("UPDATE cycles SET phase='mintaka', decisions=? WHERE id=?",
                     (json.dumps(decisions, default=str), cycle))
        conn.commit()
        conn.close()

        # MINTAKA: Execute
        actions = await mintaka_execute(decisions)

        conn = sqlite3.connect(DB_PATH)
        conn.execute("UPDATE cycles SET phase='complete', status='completed', actions_taken=?, queens_consulted=?, completed_at=datetime('now') WHERE id=?",
                     (json.dumps(actions, default=str),
                      json.dumps(list(set(q for d in decisions for q in d.get("queens", [])))),
                      cycle))
        conn.commit()
        conn.close()

        return {"cycle": cycle, "state": state, "decisions": decisions, "actions": actions}

    except Exception as e:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("UPDATE cycles SET status='error', findings=? WHERE id=?", (str(e), cycle))
        conn.commit()
        conn.close()
        return {"cycle": cycle, "error": str(e)}


# Background loop
async def orion_loop():
    """Run Orion cycles continuously."""
    while True:
        try:
            result = await run_orion_cycle()
            cycle_id = result.get("cycle", "?")
            actions = result.get("actions", [])
            print(f"[Orion Cycle {cycle_id}] Complete — {len(actions)} actions")
        except Exception as e:
            print(f"[Orion] Error: {e}")
        await asyncio.sleep(CYCLE_INTERVAL)


# ============================================================
# API
# ============================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    # Start the Orion cycle loop
    task = asyncio.create_task(orion_loop())
    yield
    task.cancel()

app = FastAPI(title="Orion's Belt — The Hive Mind", version="1.0.0", lifespan=lifespan)


class AskQueensRequest(BaseModel):
    question: str
    domain: str = "general"
    queen_ids: list = []


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    cycles = conn.execute("SELECT * FROM cycles ORDER BY id DESC LIMIT 10").fetchall()
    decisions = conn.execute("SELECT * FROM decisions ORDER BY id DESC LIMIT 15").fetchall()
    queens_active = conn.execute("SELECT queen_id, COUNT(*) as calls, AVG(confidence) as avg_conf FROM queen_log GROUP BY queen_id ORDER BY calls DESC").fetchall()

    stats = conn.execute("""
        SELECT COUNT(*) as total_cycles,
               SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) as completed,
               (SELECT COUNT(DISTINCT queen_id) FROM queen_log) as queens_used
        FROM cycles
    """).fetchone()
    conn.close()

    cycle_rows = ""
    for c in cycles:
        sc = {"completed": "#27ae60", "running": "#f39c12", "error": "#e74c3c"}.get(c["status"], "#95a5a6")
        cycle_rows += f"""<tr>
            <td>#{c['id']}</td>
            <td><span style="color:{sc};font-weight:700">{c['status']}</span></td>
            <td>{c['phase']}</td>
            <td>{c['created_at']}</td>
        </tr>"""

    decision_rows = ""
    for d in decisions:
        decision_rows += f"""<tr>
            <td>{d['domain']}</td>
            <td>{d['question'][:80]}...</td>
            <td>{d['confidence']:.0f}%</td>
            <td>{'AUTO' if d['auto_execute'] else 'MANUAL'}</td>
        </tr>"""

    queen_rows = ""
    for q in queens_active:
        queen_info = QUEENS.get(q["queen_id"], {})
        queen_rows += f"""<div style="background:rgba(255,255,255,0.03);padding:10px 15px;border-radius:8px;display:flex;justify-content:space-between;align-items:center;">
            <div><strong style="color:#fd79a8;">{queen_info.get('name', q['queen_id'])}</strong>
            <span style="color:#666;font-size:0.8rem;margin-left:8px;">{queen_info.get('specialty', '')[:40]}</span></div>
            <div><span style="color:#6c5ce7;">{q['calls']} calls</span> | <span style="color:#a29bfe;">{q['avg_conf']:.0f}% avg</span></div>
        </div>"""

    return f"""<!DOCTYPE html><html><head>
<title>Orion's Belt — The Hive Mind</title>
<style>
body {{ font-family: system-ui; background: #06060f; color: #e8e8e8; margin: 0; padding: 20px; }}
h1 {{ background: linear-gradient(135deg, #fd79a8, #6c5ce7, #a29bfe); -webkit-background-clip: text; -webkit-text-fill-color: transparent; font-size: 2.5rem; }}
h2 {{ color: #fd79a8; border-bottom: 1px solid rgba(253,121,168,0.2); padding-bottom: 8px; }}
.belt {{ display: flex; gap: 20px; margin: 30px 0; }}
.star {{ flex: 1; background: linear-gradient(135deg, rgba(108,92,231,0.1), rgba(253,121,168,0.05)); padding: 20px; border-radius: 16px; border: 1px solid rgba(108,92,231,0.2); text-align: center; }}
.star h3 {{ color: #6c5ce7; font-size: 1.4rem; margin: 0 0 5px; }}
.star .arabic {{ color: #fd79a8; font-size: 0.9rem; }}
.star p {{ color: #aaa; font-size: 0.85rem; }}
.stats {{ display: flex; gap: 15px; margin: 20px 0; }}
.stat {{ background: rgba(255,255,255,0.03); padding: 15px; border-radius: 12px; flex: 1; text-align: center; }}
.stat h3 {{ color: #fd79a8; font-size: 2rem; margin: 0; }} .stat p {{ color: #888; margin: 4px 0 0; font-size: 0.85rem; }}
table {{ width: 100%; border-collapse: collapse; margin: 15px 0; }}
th, td {{ padding: 8px 12px; text-align: left; border-bottom: 1px solid rgba(255,255,255,0.06); font-size: 0.9rem; }}
th {{ color: #6c5ce7; font-weight: 600; }}
.queens {{ display: flex; flex-direction: column; gap: 8px; margin: 15px 0; }}
.actions {{ margin: 20px 0; }}
pre {{ background: rgba(255,255,255,0.03); padding: 15px; border-radius: 8px; font-size: 0.85rem; overflow-x: auto; }}
a {{ color: #a29bfe; }} button {{ background: #6c5ce7; color: white; padding: 10px 20px; border: none; border-radius: 8px; cursor: pointer; font-weight: 600; }}
</style></head><body>

<h1>Orion's Belt — The Hive Mind</h1>
<p style="color:#888;">27 Queens. 3 Stars. 1 Autonomous Intelligence.</p>

<div class="belt">
<div class="star">
<h3>ALNITAK</h3>
<div class="arabic">The Monitor</div>
<p>Scans all services, collects system state, detects anomalies</p>
</div>
<div class="star">
<h3>ALNILAM</h3>
<div class="arabic">The Decider</div>
<p>Queens debate and reach consensus on what to do</p>
</div>
<div class="star">
<h3>MINTAKA</h3>
<div class="arabic">The Executor</div>
<p>Carries out decisions. Auto-execute or queue for approval</p>
</div>
</div>

<div class="stats">
<div class="stat"><h3>{stats['total_cycles'] or 0}</h3><p>Cycles Run</p></div>
<div class="stat"><h3>{stats['completed'] or 0}</h3><p>Completed</p></div>
<div class="stat"><h3>{stats['queens_used'] or 0}/27</h3><p>Queens Active</p></div>
<div class="stat"><h3>{len(QUEENS)}</h3><p>Total Queens</p></div>
</div>

<h2>Active Queens</h2>
<div class="queens">{queen_rows if queen_rows else '<p style="color:#666;">No queens activated yet. First cycle running...</p>'}</div>

<h2>Recent Decisions</h2>
<table>
<tr><th>Domain</th><th>Question</th><th>Confidence</th><th>Execute</th></tr>
{decision_rows if decision_rows else '<tr><td colspan="4" style="color:#666;">Awaiting first cycle...</td></tr>'}
</table>

<h2>Orion Cycles</h2>
<table>
<tr><th>Cycle</th><th>Status</th><th>Phase</th><th>Time</th></tr>
{cycle_rows if cycle_rows else '<tr><td colspan="4" style="color:#666;">First cycle starting...</td></tr>'}
</table>

<h2>Ask the Queens</h2>
<form action="/api/ask" method="POST" style="background:rgba(255,255,255,0.03);padding:20px;border-radius:12px;">
<textarea name="question" rows="3" style="width:100%;background:rgba(255,255,255,0.05);color:white;border:1px solid rgba(255,255,255,0.1);padding:10px;border-radius:8px;" placeholder="Ask the 27 Queens anything..."></textarea>
<button type="submit" style="margin-top:10px;">Ask the Swarm</button>
</form>

<h2>API</h2>
<pre>
GET  /                    — This dashboard
GET  /health              — Health check
GET  /api/queens          — List all 27 queens
GET  /api/cycles          — Recent Orion cycles
POST /api/ask             — Ask the queens a question
POST /api/cycle           — Trigger manual Orion cycle
GET  /api/state           — Current system state
</pre>

</body></html>"""


@app.post("/api/ask")
async def ask_queens(req: AskQueensRequest, background_tasks: BackgroundTasks):
    """Ask specific queens or auto-select based on domain."""
    # Auto-select queens if none specified
    if not req.queen_ids:
        keywords = req.question.lower().split()
        selected = set()
        for qid, queen in QUEENS.items():
            if any(kw in keywords for kw in queen["trigger_on"]):
                selected.add(qid)
        if len(selected) < 3:
            # Add default queens
            selected.update(["analyst-queen", "strategist-queen", "skeptic-queen"])
        req.queen_ids = list(selected)[:7]

    # Run the swarm simulation via HiveSwarm
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.post(f"{SWARM_URL}/api/simulate", json={
                "scenario": req.question,
                "domain": req.domain,
                "agent_count": min(len(req.queen_ids), 5),
                "debate_rounds": 2,
            })
            if r.status_code == 200:
                return {"status": "simulation_started", "queens": req.queen_ids, **r.json()}
    except:
        pass

    return {"status": "direct_query", "queens": req.queen_ids, "note": "HiveSwarm unavailable, running direct"}


@app.post("/api/cycle")
async def trigger_cycle(background_tasks: BackgroundTasks):
    """Manually trigger an Orion cycle."""
    background_tasks.add_task(run_orion_cycle)
    return {"status": "cycle_started"}


@app.get("/api/queens")
async def list_queens():
    return {qid: {"name": q["name"], "domain": q["domain"], "specialty": q["specialty"],
                   "model": q["model"], "trigger_on": q["trigger_on"]}
            for qid, q in QUEENS.items()}


@app.get("/api/cycles")
async def recent_cycles():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cycles = conn.execute("SELECT id, phase, status, created_at, completed_at FROM cycles ORDER BY id DESC LIMIT 20").fetchall()
    conn.close()
    return [dict(c) for c in cycles]


@app.get("/api/state")
async def current_state():
    return await alnitak_scan()


@app.get("/health")
async def health():
    conn = sqlite3.connect(DB_PATH)
    total_cycles = conn.execute("SELECT COUNT(*) FROM cycles").fetchone()[0]
    total_decisions = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
    conn.close()
    return {
        "status": "healthy",
        "service": "orions-belt",
        "queens": len(QUEENS),
        "cycles": total_cycles,
        "decisions": total_decisions,
    }


if __name__ == "__main__":
    import uvicorn
    print(f"")
    print(f"  ORION'S BELT — The Hive Mind")
    print(f"  ============================")
    print(f"  27 Queens | 3 Stars | Autonomous Intelligence")
    print(f"  Port: {PORT}")
    print(f"  Queens: {len(QUEENS)}")
    print(f"  Cycle interval: {CYCLE_INTERVAL}s")
    print(f"  Dashboard: http://localhost:{PORT}")
    print(f"")
    print(f"  Stars:")
    print(f"    ALNITAK — Monitor (scans all services)")
    print(f"    ALNILAM — Decide (queen debates)")
    print(f"    MINTAKA — Execute (carry out decisions)")
    print(f"")
    uvicorn.run(app, host="0.0.0.0", port=PORT)
