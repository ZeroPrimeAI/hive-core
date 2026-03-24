#!/usr/bin/env python3
"""
HIVE REASONING BANK — Inspired by RuFlow (MIT Licensed)
========================================================
Cache what works. Retrieve instead of regenerating.
Every query + response pair is stored. Similar future queries
get cached answers instead of burning LLM tokens.

This is the pattern-matching brain that makes the Hive FASTER
and CHEAPER with every interaction.

Port: 8910
"""

import sqlite3
import json
import time
import hashlib
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI
from fastapi.responses import JSONResponse
import uvicorn

DB_PATH = "/home/zero/hivecode_sandbox/reasoning_bank.db"
PORT = 8910

# Cache settings
CACHE_TTL = 3600  # 1 hour for exact matches
SIMILARITY_THRESHOLD = 0.7  # For fuzzy matching
MAX_CACHE_SIZE = 50000

app = FastAPI(title="Hive Reasoning Bank")


def get_db():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    return db


def init_db():
    db = get_db()
    db.executescript("""
        CREATE TABLE IF NOT EXISTS reasoning_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query_hash TEXT UNIQUE NOT NULL,
            query TEXT NOT NULL,
            response TEXT NOT NULL,
            domain TEXT DEFAULT 'general',
            model_used TEXT DEFAULT 'gemma3:1b',
            confidence REAL DEFAULT 1.0,
            hit_count INTEGER DEFAULT 0,
            success_count INTEGER DEFAULT 0,
            fail_count INTEGER DEFAULT 0,
            tokens_saved INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            last_hit TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS reasoning_patterns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pattern_type TEXT NOT NULL,
            pattern_key TEXT NOT NULL,
            pattern_value TEXT NOT NULL,
            frequency INTEGER DEFAULT 1,
            effectiveness REAL DEFAULT 0.5,
            domain TEXT DEFAULT 'general',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(pattern_type, pattern_key)
        );

        CREATE TABLE IF NOT EXISTS reasoning_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
            total_queries INTEGER DEFAULT 0,
            cache_hits INTEGER DEFAULT 0,
            cache_misses INTEGER DEFAULT 0,
            tokens_saved INTEGER DEFAULT 0,
            avg_confidence REAL DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS idx_cache_hash ON reasoning_cache(query_hash);
        CREATE INDEX IF NOT EXISTS idx_cache_domain ON reasoning_cache(domain);
        CREATE INDEX IF NOT EXISTS idx_patterns_type ON reasoning_patterns(pattern_type, pattern_key);

        CREATE VIRTUAL TABLE IF NOT EXISTS cache_fts USING fts5(
            query, response, domain, content=reasoning_cache, content_rowid=id
        );
    """)
    db.commit()
    db.close()


def hash_query(query: str, domain: str = "general") -> str:
    """Create a hash for cache lookup."""
    normalized = query.strip().lower()
    return hashlib.sha256(f"{domain}:{normalized}".encode()).hexdigest()[:32]


def tokenize(text: str) -> set:
    """Simple tokenizer for similarity matching."""
    return set(text.lower().split())


def jaccard_similarity(a: set, b: set) -> float:
    """Jaccard similarity between two token sets."""
    if not a or not b:
        return 0.0
    intersection = a & b
    union = a | b
    return len(intersection) / len(union)


# ── Core Operations ──────────────────────────────────────────

def store(query: str, response: str, domain: str = "general",
          model: str = "gemma3:1b", confidence: float = 1.0, tokens: int = 0):
    """Store a query-response pair in the reasoning bank."""
    db = get_db()
    qhash = hash_query(query, domain)

    existing = db.execute("SELECT id, hit_count FROM reasoning_cache WHERE query_hash=?", (qhash,)).fetchone()
    if existing:
        db.execute(
            "UPDATE reasoning_cache SET response=?, confidence=?, last_hit=CURRENT_TIMESTAMP, hit_count=hit_count+1 WHERE id=?",
            (response, confidence, existing["id"])
        )
    else:
        cur = db.execute(
            "INSERT INTO reasoning_cache (query_hash, query, response, domain, model_used, confidence, tokens_saved) VALUES (?,?,?,?,?,?,?)",
            (qhash, query, response, domain, model, confidence, tokens)
        )
        # Update FTS
        db.execute(
            "INSERT INTO cache_fts(rowid, query, response, domain) VALUES (?,?,?,?)",
            (cur.lastrowid, query, response, domain)
        )
    db.commit()
    db.close()


def retrieve(query: str, domain: str = "general") -> Optional[dict]:
    """Look up a cached response. Returns None on miss."""
    db = get_db()

    # Try exact hash match first
    qhash = hash_query(query, domain)
    row = db.execute(
        "SELECT * FROM reasoning_cache WHERE query_hash=? AND confidence > 0.3",
        (qhash,)
    ).fetchone()

    if row:
        # Exact hit
        db.execute(
            "UPDATE reasoning_cache SET hit_count=hit_count+1, last_hit=CURRENT_TIMESTAMP WHERE id=?",
            (row["id"],)
        )
        db.commit()
        db.close()
        return {
            "source": "exact",
            "response": row["response"],
            "confidence": row["confidence"],
            "hit_count": row["hit_count"] + 1,
            "domain": row["domain"],
            "tokens_saved": row["tokens_saved"],
        }

    # Try FTS similarity search
    try:
        # Extract key terms for FTS query
        terms = query.strip().split()[:10]
        fts_query = " OR ".join(f'"{t}"' for t in terms if len(t) > 2)
        if fts_query:
            rows = db.execute("""
                SELECT c.*, rank FROM cache_fts f
                JOIN reasoning_cache c ON f.rowid = c.id
                WHERE cache_fts MATCH ? AND c.domain=?
                ORDER BY rank LIMIT 5
            """, (fts_query, domain)).fetchall()

            # Check similarity
            query_tokens = tokenize(query)
            best = None
            best_sim = 0

            for r in rows:
                sim = jaccard_similarity(query_tokens, tokenize(r["query"]))
                if sim > best_sim and sim >= SIMILARITY_THRESHOLD:
                    best = r
                    best_sim = sim

            if best:
                db.execute(
                    "UPDATE reasoning_cache SET hit_count=hit_count+1, last_hit=CURRENT_TIMESTAMP WHERE id=?",
                    (best["id"],)
                )
                db.commit()
                db.close()
                return {
                    "source": "similar",
                    "similarity": round(best_sim, 3),
                    "response": best["response"],
                    "confidence": best["confidence"] * best_sim,
                    "hit_count": best["hit_count"] + 1,
                    "original_query": best["query"],
                    "domain": best["domain"],
                    "tokens_saved": best["tokens_saved"],
                }
    except Exception:
        pass

    db.close()
    return None  # Cache miss


def feedback(query_hash: str, success: bool):
    """Report whether a cached response was useful."""
    db = get_db()
    if success:
        db.execute(
            "UPDATE reasoning_cache SET success_count=success_count+1, confidence=MIN(1.0, confidence+0.05) WHERE query_hash=?",
            (query_hash,)
        )
    else:
        db.execute(
            "UPDATE reasoning_cache SET fail_count=fail_count+1, confidence=MAX(0.0, confidence-0.1) WHERE query_hash=?",
            (query_hash,)
        )
    db.commit()
    db.close()


def learn_pattern(pattern_type: str, key: str, value: str, domain: str = "general"):
    """Store a reusable pattern (hook, CTA, prompt template, etc.)."""
    db = get_db()
    existing = db.execute(
        "SELECT id FROM reasoning_patterns WHERE pattern_type=? AND pattern_key=?",
        (pattern_type, key)
    ).fetchone()
    if existing:
        db.execute(
            "UPDATE reasoning_patterns SET frequency=frequency+1, pattern_value=? WHERE id=?",
            (value, existing["id"])
        )
    else:
        db.execute(
            "INSERT INTO reasoning_patterns (pattern_type, pattern_key, pattern_value, domain) VALUES (?,?,?,?)",
            (pattern_type, key, value, domain)
        )
    db.commit()
    db.close()


# ── 3-TIER TASK ROUTER ──────────────────────────────────────

def route_task(task: str, domain: str = "general") -> dict:
    """
    Route a task to the appropriate tier:
    Tier 1: Cache hit (FREE, <1ms)
    Tier 2: Local gemma3:1b (cheap, ~500ms)
    Tier 3: Cloud brain qwen3:14b (expensive, 2-5s)
    """
    # Tier 1: Check reasoning bank cache
    cached = retrieve(task, domain)
    if cached and cached["confidence"] > 0.6:
        return {
            "tier": 1,
            "handler": "reasoning_bank_cache",
            "response": cached["response"],
            "confidence": cached["confidence"],
            "cost": 0,
            "latency_ms": 1,
            "tokens_saved": cached.get("tokens_saved", 100),
        }

    # Tier 2 vs Tier 3 decision based on task complexity
    complexity = estimate_complexity(task)

    if complexity <= 3:
        return {
            "tier": 2,
            "handler": "gemma3_1b_local",
            "model": "gemma3:1b",
            "endpoint": "http://100.105.160.106:11434",
            "complexity": complexity,
            "cost": 0,  # Own hardware
            "latency_ms": 500,
        }
    else:
        return {
            "tier": 3,
            "handler": "cloud_brain",
            "model": "qwen3:14b",
            "endpoint": "http://localhost:11437",
            "complexity": complexity,
            "cost": 0.001,  # Cloud brain time
            "latency_ms": 3000,
        }


def estimate_complexity(task: str) -> int:
    """Estimate task complexity 1-5."""
    task_lower = task.lower()
    score = 2  # Default medium

    # Simple indicators
    simple = ["what is", "list", "show", "check", "status", "count", "get"]
    complex_words = ["analyze", "design", "architect", "strategy", "synthesize", "evaluate", "compare", "debate"]
    very_complex = ["build", "implement", "create system", "full pipeline", "end to end"]

    for w in simple:
        if w in task_lower:
            score -= 1
    for w in complex_words:
        if w in task_lower:
            score += 1
    for w in very_complex:
        if w in task_lower:
            score += 2

    # Length indicates complexity
    if len(task) > 500:
        score += 1
    if len(task) > 1000:
        score += 1

    return max(1, min(5, score))


# ── API ──────────────────────────────────────────────────────

@app.get("/health")
async def health():
    db = get_db()
    total = db.execute("SELECT COUNT(*) FROM reasoning_cache").fetchone()[0]
    patterns = db.execute("SELECT COUNT(*) FROM reasoning_patterns").fetchone()[0]
    total_hits = db.execute("SELECT SUM(hit_count) FROM reasoning_cache").fetchone()[0] or 0
    db.close()
    return {
        "status": "alive",
        "service": "reasoning-bank",
        "cached_responses": total,
        "patterns": patterns,
        "total_cache_hits": total_hits,
        "port": PORT,
    }


@app.post("/api/store")
async def api_store(query: str, response: str, domain: str = "general",
                    model: str = "gemma3:1b", confidence: float = 1.0, tokens: int = 0):
    store(query, response, domain, model, confidence, tokens)
    return {"status": "stored", "hash": hash_query(query, domain)}


@app.get("/api/retrieve")
async def api_retrieve(query: str, domain: str = "general"):
    result = retrieve(query, domain)
    if result:
        return {"hit": True, **result}
    return {"hit": False, "message": "cache miss"}


@app.post("/api/route")
async def api_route(task: str, domain: str = "general"):
    return route_task(task, domain)


@app.post("/api/feedback")
async def api_feedback(query_hash: str, success: bool):
    feedback(query_hash, success)
    return {"status": "recorded"}


@app.post("/api/learn-pattern")
async def api_learn_pattern(pattern_type: str, key: str, value: str, domain: str = "general"):
    learn_pattern(pattern_type, key, value, domain)
    return {"status": "learned"}


@app.get("/api/patterns")
async def api_patterns(pattern_type: str = None):
    db = get_db()
    if pattern_type:
        rows = db.execute(
            "SELECT * FROM reasoning_patterns WHERE pattern_type=? ORDER BY frequency DESC LIMIT 50",
            (pattern_type,)
        ).fetchall()
    else:
        rows = db.execute(
            "SELECT * FROM reasoning_patterns ORDER BY frequency DESC LIMIT 50"
        ).fetchall()
    db.close()
    return {"patterns": [dict(r) for r in rows]}


@app.get("/api/stats")
async def api_stats():
    db = get_db()
    total = db.execute("SELECT COUNT(*) FROM reasoning_cache").fetchone()[0]
    total_hits = db.execute("SELECT SUM(hit_count) FROM reasoning_cache").fetchone()[0] or 0
    total_success = db.execute("SELECT SUM(success_count) FROM reasoning_cache").fetchone()[0] or 0
    total_fail = db.execute("SELECT SUM(fail_count) FROM reasoning_cache").fetchone()[0] or 0
    total_tokens = db.execute("SELECT SUM(tokens_saved) FROM reasoning_cache").fetchone()[0] or 0
    domains = db.execute(
        "SELECT domain, COUNT(*) as cnt FROM reasoning_cache GROUP BY domain ORDER BY cnt DESC"
    ).fetchall()
    db.close()
    return {
        "cached_responses": total,
        "total_hits": total_hits,
        "success_rate": round(total_success / max(total_success + total_fail, 1), 3),
        "tokens_saved": total_tokens,
        "domains": {r["domain"]: r["cnt"] for r in domains},
    }


@app.on_event("startup")
async def startup():
    init_db()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
