#!/usr/bin/env python3
"""
THE HIVE — Memory Researcher Agent
Port 8906 | SQLite at /home/zero/hivecode_sandbox/memory_research.db

Continuously researches AI memory solutions, RAG systems, knowledge graphs,
long-term memory architectures, and MCP patterns — then suggests concrete
improvements to the Hive's own memory system.

Runs every 2 hours (or on-demand via POST /api/search).
Feeds high-value insights to Nerve.
"""

import json
import sqlite3
import time
import threading
import os
import re
import hashlib
import traceback
import html as html_mod
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any
from contextlib import contextmanager
from urllib.parse import quote_plus, urlencode

import httpx
from fastapi import FastAPI, BackgroundTasks, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
import uvicorn

# ==========================================================================
# CONFIG
# ==========================================================================
PORT = 8906
DB_PATH = "/home/zero/hivecode_sandbox/memory_research.db"

OLLAMA_URL = "http://localhost:11434"
OLLAMA_MODEL = "gemma2:2b"
NERVE_URL = "http://100.105.160.106:8200/api/add"

SCAN_INTERVAL_HOURS = 2

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# GitHub API (unauthenticated — 10 req/min for search)
GITHUB_API = "https://api.github.com"

# Search domains
GITHUB_SEARCH_QUERIES = [
    "AI memory MIT license",
    "long term memory LLM",
    "RAG system MIT",
    "knowledge graph AI",
    "MCP memory server",
    "context management AI agent",
    "vector database lightweight",
    "semantic memory AI agent",
    "persistent memory chatbot",
    "AI memory consolidation",
    "episodic memory LLM",
    "memory augmented neural network",
    "retrieval augmented generation framework",
    "knowledge distillation memory",
    "Claude MCP server memory",
]

# Research paper search queries (via public APIs)
PAPER_SEARCH_QUERIES = [
    "long-term memory large language models",
    "retrieval augmented generation memory",
    "AI agent memory architecture",
    "knowledge graph context management LLM",
    "episodic semantic memory artificial intelligence",
    "memory consolidation neural networks",
    "continual learning language models",
    "context window management AI",
]

# Known systems to track
KNOWN_SYSTEMS = [
    "MemGPT", "Letta", "mem0", "LangChain memory",
    "LlamaIndex memory", "Zep", "Motorhead",
    "Chroma", "Weaviate", "Qdrant", "Milvus",
    "knowledge graph memory", "GraphRAG",
    "MemoryScope", "cognitive architecture memory",
]

# What we currently have (for comparison scoring)
CURRENT_SYSTEM = {
    "components": [
        "CLAUDE.md — project instructions, loaded every session",
        "MEMORY.md — index file linking ~35 topic-specific memory files",
        "SessionStart hook loads mission context from MEMORY.md",
        "Nerve v2 — SQLite with 37K+ facts, keyword search",
        "Hive Mind DB — 44K+ cycles, 4K+ decisions",
        "Memory files in ~/.claude/projects/-home-zero/memory/",
        "Distillation DB — 2.5K+ training pairs",
        "RAG vectors DB — 53MB vector search",
    ],
    "weaknesses": [
        "No automatic context selection based on current task",
        "Memory grows but does not consolidate or prune",
        "No learning from outcomes (what worked vs failed)",
        "Session resets lose working memory",
        "MEMORY.md manual curation bottleneck",
        "No semantic search across memory files",
        "No memory importance scoring or decay",
        "No cross-session continuity beyond file drops",
    ],
    "goals": [
        "Perfect recall across sessions — never forget what Chris said",
        "Automatic context loading based on current task",
        "Learning from outcomes (what worked, what failed)",
        "Memory that gets BETTER over time, not just bigger",
        "Best AI memory system in the world",
    ],
}

# Acceptable licenses
GOOD_LICENSES = {
    "mit", "apache-2.0", "bsd-2-clause", "bsd-3-clause",
    "unlicense", "cc0-1.0", "isc", "0bsd", "wtfpl",
}

# ==========================================================================
# DATABASE
# ==========================================================================

@contextmanager
def get_db():
    """Thread-safe database connection."""
    conn = sqlite3.connect(DB_PATH, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """Initialize all database tables."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with get_db() as db:
        db.executescript("""
            CREATE TABLE IF NOT EXISTS findings (
                id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                source_type TEXT NOT NULL,
                title TEXT NOT NULL,
                url TEXT,
                description TEXT,
                stars INTEGER DEFAULT 0,
                license TEXT,
                license_ok INTEGER DEFAULT 0,
                language TEXT,
                topics TEXT,
                relevance_score INTEGER DEFAULT 0,
                key_techniques TEXT,
                comparison_to_ours TEXT,
                improvement_suggestion TEXT,
                raw_data TEXT,
                search_query TEXT,
                found_at TEXT DEFAULT (datetime('now')),
                analyzed INTEGER DEFAULT 0,
                fed_to_nerve INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS techniques (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                category TEXT,
                description TEXT,
                source_finding_id TEXT,
                relevance_score INTEGER DEFAULT 0,
                difficulty TEXT,
                impact TEXT,
                applicable_to TEXT,
                implementation_notes TEXT,
                extracted_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (source_finding_id) REFERENCES findings(id)
            );

            CREATE TABLE IF NOT EXISTS improvements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                description TEXT,
                priority TEXT DEFAULT 'P2',
                category TEXT,
                current_state TEXT,
                proposed_state TEXT,
                implementation_steps TEXT,
                estimated_effort TEXT,
                source_findings TEXT,
                source_techniques TEXT,
                status TEXT DEFAULT 'proposed',
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS scans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scan_type TEXT NOT NULL,
                query TEXT,
                results_count INTEGER DEFAULT 0,
                findings_new INTEGER DEFAULT 0,
                status TEXT DEFAULT 'pending',
                started_at TEXT DEFAULT (datetime('now')),
                finished_at TEXT,
                error TEXT
            );

            CREATE TABLE IF NOT EXISTS papers (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                authors TEXT,
                abstract TEXT,
                url TEXT,
                published TEXT,
                source TEXT,
                relevance_score INTEGER DEFAULT 0,
                key_ideas TEXT,
                applicable_techniques TEXT,
                search_query TEXT,
                found_at TEXT DEFAULT (datetime('now')),
                analyzed INTEGER DEFAULT 0,
                fed_to_nerve INTEGER DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_findings_relevance ON findings(relevance_score DESC);
            CREATE INDEX IF NOT EXISTS idx_findings_source_type ON findings(source_type);
            CREATE INDEX IF NOT EXISTS idx_findings_analyzed ON findings(analyzed);
            CREATE INDEX IF NOT EXISTS idx_techniques_relevance ON techniques(relevance_score DESC);
            CREATE INDEX IF NOT EXISTS idx_techniques_category ON techniques(category);
            CREATE INDEX IF NOT EXISTS idx_improvements_priority ON improvements(priority);
            CREATE INDEX IF NOT EXISTS idx_improvements_status ON improvements(status);
            CREATE INDEX IF NOT EXISTS idx_papers_relevance ON papers(relevance_score DESC);
            CREATE INDEX IF NOT EXISTS idx_papers_analyzed ON papers(analyzed);
        """)
    print(f"[DB] Initialized at {DB_PATH}")


# ==========================================================================
# HTTP CLIENT
# ==========================================================================

def get_client() -> httpx.Client:
    """Create an httpx client with sane defaults."""
    return httpx.Client(
        timeout=30.0,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        },
        follow_redirects=True,
    )


# ==========================================================================
# GITHUB SEARCH
# ==========================================================================

def search_github_repos(query: str, per_page: int = 15) -> List[Dict]:
    """Search GitHub repos via the public API (no key needed for basic search)."""
    results = []
    try:
        with get_client() as client:
            params = {
                "q": query,
                "sort": "stars",
                "order": "desc",
                "per_page": per_page,
            }
            resp = client.get(
                f"{GITHUB_API}/search/repositories",
                params=params,
                headers={"Accept": "application/vnd.github.v3+json"},
            )
            if resp.status_code == 200:
                data = resp.json()
                for item in data.get("items", []):
                    results.append({
                        "full_name": item.get("full_name", ""),
                        "name": item.get("name", ""),
                        "description": item.get("description", "") or "",
                        "url": item.get("html_url", ""),
                        "stars": item.get("stargazers_count", 0),
                        "language": item.get("language", ""),
                        "license": (item.get("license") or {}).get("key", "unknown"),
                        "topics": item.get("topics", []),
                        "updated_at": item.get("updated_at", ""),
                        "created_at": item.get("created_at", ""),
                        "forks": item.get("forks_count", 0),
                        "open_issues": item.get("open_issues_count", 0),
                    })
            elif resp.status_code == 403:
                print(f"[GitHub] Rate limited on query: {query}")
            else:
                print(f"[GitHub] HTTP {resp.status_code} for query: {query}")
    except Exception as e:
        print(f"[GitHub] Error searching '{query}': {e}")
    return results


def get_github_readme(full_name: str) -> str:
    """Fetch the README contents for a GitHub repo."""
    try:
        with get_client() as client:
            resp = client.get(
                f"{GITHUB_API}/repos/{full_name}/readme",
                headers={
                    "Accept": "application/vnd.github.v3.raw",
                },
            )
            if resp.status_code == 200:
                text = resp.text
                # Truncate to save space
                if len(text) > 3000:
                    text = text[:3000] + "\n... [truncated]"
                return text
    except Exception as e:
        print(f"[GitHub] Error fetching README for {full_name}: {e}")
    return ""


# ==========================================================================
# PAPER SEARCH (Semantic Scholar public API — no key needed)
# ==========================================================================

def search_papers(query: str, limit: int = 10) -> List[Dict]:
    """Search Semantic Scholar for research papers."""
    results = []
    try:
        with get_client() as client:
            params = {
                "query": query,
                "limit": limit,
                "fields": "title,authors,abstract,url,year,citationCount,publicationDate,externalIds",
            }
            resp = client.get(
                "https://api.semanticscholar.org/graph/v1/paper/search",
                params=params,
            )
            if resp.status_code == 200:
                data = resp.json()
                for paper in data.get("data", []):
                    authors = ", ".join(
                        a.get("name", "") for a in (paper.get("authors") or [])[:5]
                    )
                    arxiv_id = (paper.get("externalIds") or {}).get("ArXiv", "")
                    paper_url = paper.get("url") or ""
                    if arxiv_id:
                        paper_url = f"https://arxiv.org/abs/{arxiv_id}"

                    results.append({
                        "paper_id": paper.get("paperId", ""),
                        "title": paper.get("title", ""),
                        "authors": authors,
                        "abstract": paper.get("abstract", "") or "",
                        "url": paper_url,
                        "year": paper.get("year"),
                        "citations": paper.get("citationCount", 0),
                        "published": paper.get("publicationDate", ""),
                    })
            elif resp.status_code == 429:
                print(f"[Papers] Rate limited on query: {query}")
            else:
                print(f"[Papers] HTTP {resp.status_code} for query: {query}")
    except Exception as e:
        print(f"[Papers] Error searching '{query}': {e}")
    return results


# ==========================================================================
# WEB SEARCH (DuckDuckGo HTML — no API key)
# ==========================================================================

def search_web(query: str, max_results: int = 8) -> List[Dict]:
    """Search via DuckDuckGo HTML (no API key needed)."""
    results = []
    try:
        with get_client() as client:
            resp = client.get(
                "https://html.duckduckgo.com/html/",
                params={"q": query},
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "text/html",
                },
            )
            if resp.status_code == 200:
                text = resp.text
                # Parse result snippets from DDG HTML
                # Results are in <div class="result__body"> blocks
                result_blocks = re.findall(
                    r'<a rel="nofollow" class="result__a" href="([^"]*)"[^>]*>(.*?)</a>.*?'
                    r'<a class="result__snippet"[^>]*>(.*?)</a>',
                    text, re.DOTALL
                )
                for url, title, snippet in result_blocks[:max_results]:
                    # DDG wraps URLs in a redirect; extract the actual URL
                    actual_url = url
                    uddg_match = re.search(r'uddg=([^&]+)', url)
                    if uddg_match:
                        from urllib.parse import unquote
                        actual_url = unquote(uddg_match.group(1))

                    clean_title = re.sub(r'<[^>]+>', '', title).strip()
                    clean_snippet = re.sub(r'<[^>]+>', '', snippet).strip()
                    clean_snippet = html_mod.unescape(clean_snippet)
                    clean_title = html_mod.unescape(clean_title)

                    if clean_title:
                        results.append({
                            "title": clean_title,
                            "url": actual_url,
                            "snippet": clean_snippet,
                        })
    except Exception as e:
        print(f"[Web] Error searching '{query}': {e}")
    return results


# ==========================================================================
# LLM ANALYSIS (via local Ollama)
# ==========================================================================

def llm_analyze(prompt: str, max_tokens: int = 1024) -> str:
    """Ask the local Ollama gemma2:2b for analysis."""
    try:
        with get_client() as client:
            resp = client.post(
                f"{OLLAMA_URL}/api/generate",
                json={
                    "model": OLLAMA_MODEL,
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "num_predict": max_tokens,
                        "temperature": 0.3,
                    },
                },
                timeout=120.0,
            )
            if resp.status_code == 200:
                return resp.json().get("response", "").strip()
    except Exception as e:
        print(f"[LLM] Error: {e}")
    return ""


def score_relevance(finding: Dict) -> int:
    """Score a finding's relevance to our memory needs (0-100).

    Uses a keyword heuristic + LLM boost for speed.
    """
    score = 0
    text = (
        f"{finding.get('title', '')} "
        f"{finding.get('description', '')} "
        f"{' '.join(finding.get('topics', []))}"
    ).lower()

    # Core relevance keywords (high weight)
    high_keywords = [
        "memory", "recall", "context", "session", "persistent",
        "long-term", "long term", "continuity", "remember",
        "knowledge graph", "rag", "retrieval", "vector",
        "episodic", "semantic memory", "consolidat",
    ]
    for kw in high_keywords:
        if kw in text:
            score += 8

    # AI/LLM specific (medium weight)
    medium_keywords = [
        "llm", "language model", "agent", "chatbot", "assistant",
        "mcp", "claude", "gpt", "embedding", "transformer",
        "ai memory", "cognitive", "neural",
    ]
    for kw in medium_keywords:
        if kw in text:
            score += 5

    # Implementation relevance (medium weight)
    impl_keywords = [
        "sqlite", "lightweight", "local", "self-hosted", "fast",
        "python", "api", "server", "framework", "toolkit",
    ]
    for kw in impl_keywords:
        if kw in text:
            score += 3

    # License bonus
    lic = finding.get("license", "unknown").lower()
    if lic in GOOD_LICENSES:
        score += 10
    elif lic == "unknown":
        score += 0
    else:
        score -= 15  # GPL etc.

    # Stars bonus (popularity signal)
    stars = finding.get("stars", 0)
    if stars > 10000:
        score += 15
    elif stars > 1000:
        score += 10
    elif stars > 100:
        score += 5
    elif stars > 10:
        score += 2

    # Recent activity bonus
    updated = finding.get("updated_at", "")
    if updated:
        try:
            updated_dt = datetime.fromisoformat(updated.replace("Z", "+00:00"))
            days_ago = (datetime.now(timezone.utc) - updated_dt).days
            if days_ago < 30:
                score += 10
            elif days_ago < 90:
                score += 5
            elif days_ago < 365:
                score += 2
        except Exception:
            pass

    return min(100, max(0, score))


def score_paper_relevance(paper: Dict) -> int:
    """Score a paper's relevance to our memory needs (0-100)."""
    score = 0
    text = f"{paper.get('title', '')} {paper.get('abstract', '')}".lower()

    high_keywords = [
        "memory", "long-term", "long term", "persistent", "recall",
        "knowledge graph", "retrieval augment", "rag",
        "context management", "session", "continuity",
        "episodic memory", "semantic memory", "consolidation",
        "memory architecture", "memory system",
    ]
    for kw in high_keywords:
        if kw in text:
            score += 10

    medium_keywords = [
        "large language model", "llm", "agent", "chatbot",
        "transformer", "embedding", "vector", "attention",
        "knowledge base", "question answering",
    ]
    for kw in medium_keywords:
        if kw in text:
            score += 5

    # Citation bonus
    citations = paper.get("citations", 0) or 0
    if citations > 500:
        score += 15
    elif citations > 100:
        score += 10
    elif citations > 20:
        score += 5

    # Recency bonus
    year = paper.get("year")
    if year:
        if year >= 2024:
            score += 15
        elif year >= 2023:
            score += 10
        elif year >= 2022:
            score += 5

    return min(100, max(0, score))


def analyze_finding_with_llm(finding: Dict) -> Dict:
    """Use LLM to extract techniques, compare to ours, suggest improvements."""
    title = finding.get("title", "")
    desc = finding.get("description", "")
    topics = ", ".join(finding.get("topics", []))
    readme_excerpt = finding.get("readme_excerpt", "")

    context = f"Title: {title}\nDescription: {desc}"
    if topics:
        context += f"\nTopics: {topics}"
    if readme_excerpt:
        context += f"\nREADME excerpt:\n{readme_excerpt[:2000]}"

    our_system = "\n".join(f"- {c}" for c in CURRENT_SYSTEM["components"])
    our_weaknesses = "\n".join(f"- {w}" for w in CURRENT_SYSTEM["weaknesses"])

    prompt = f"""You are analyzing an AI memory/RAG project for potential improvements to our system.

PROJECT:
{context}

OUR CURRENT MEMORY SYSTEM:
{our_system}

OUR WEAKNESSES:
{our_weaknesses}

Respond in this EXACT JSON format (no extra text):
{{
  "key_techniques": ["technique 1", "technique 2", "technique 3"],
  "comparison": "How this compares to what we already have (1-2 sentences)",
  "improvement_suggestion": "Specific, actionable improvement we could make (1-2 sentences)",
  "difficulty": "easy|medium|hard",
  "impact": "low|medium|high"
}}"""

    result = llm_analyze(prompt, max_tokens=512)

    # Parse JSON from LLM response
    try:
        # Try to find JSON in the response
        json_match = re.search(r'\{[^{}]*\}', result, re.DOTALL)
        if json_match:
            parsed = json.loads(json_match.group())
            return {
                "key_techniques": parsed.get("key_techniques", []),
                "comparison": parsed.get("comparison", ""),
                "improvement_suggestion": parsed.get("improvement_suggestion", ""),
                "difficulty": parsed.get("difficulty", "medium"),
                "impact": parsed.get("impact", "medium"),
            }
    except (json.JSONDecodeError, AttributeError):
        pass

    # Fallback: extract what we can
    return {
        "key_techniques": [],
        "comparison": f"Found: {title}. Needs manual review.",
        "improvement_suggestion": "",
        "difficulty": "medium",
        "impact": "medium",
    }


def analyze_paper_with_llm(paper: Dict) -> Dict:
    """Use LLM to extract applicable ideas from a research paper."""
    title = paper.get("title", "")
    abstract = paper.get("abstract", "")

    our_weaknesses = "\n".join(f"- {w}" for w in CURRENT_SYSTEM["weaknesses"])

    prompt = f"""You are analyzing a research paper on AI memory for practical applicability to our system.

PAPER: {title}
ABSTRACT: {abstract[:1500]}

OUR WEAKNESSES:
{our_weaknesses}

Respond in this EXACT JSON format (no extra text):
{{
  "key_ideas": ["idea 1", "idea 2"],
  "applicable_techniques": ["technique we could implement"],
  "practical_value": "How we could use this (1-2 sentences)"
}}"""

    result = llm_analyze(prompt, max_tokens=400)

    try:
        json_match = re.search(r'\{[^{}]*\}', result, re.DOTALL)
        if json_match:
            parsed = json.loads(json_match.group())
            return {
                "key_ideas": parsed.get("key_ideas", []),
                "applicable_techniques": parsed.get("applicable_techniques", []),
                "practical_value": parsed.get("practical_value", ""),
            }
    except (json.JSONDecodeError, AttributeError):
        pass

    return {
        "key_ideas": [],
        "applicable_techniques": [],
        "practical_value": "",
    }


# ==========================================================================
# NERVE INTEGRATION
# ==========================================================================

def feed_to_nerve(category: str, fact: str, confidence: float = 0.8):
    """Send a finding/insight to Nerve."""
    try:
        with get_client() as client:
            resp = client.post(
                NERVE_URL,
                json={
                    "category": category,
                    "fact": fact,
                    "source": "memory_researcher",
                    "confidence": confidence,
                },
                timeout=10.0,
            )
            if resp.status_code == 200:
                print(f"[Nerve] Fed: {fact[:80]}...")
                return True
            else:
                print(f"[Nerve] HTTP {resp.status_code}: {resp.text[:200]}")
    except Exception as e:
        print(f"[Nerve] Error: {e}")
    return False


# ==========================================================================
# SCAN ENGINE
# ==========================================================================

def run_github_scan(queries: Optional[List[str]] = None):
    """Search GitHub for memory-related repos."""
    queries = queries or GITHUB_SEARCH_QUERIES
    total_new = 0

    for query in queries:
        scan_id = log_scan_start("github", query)
        try:
            repos = search_github_repos(query, per_page=10)
            new_count = 0

            for repo in repos:
                finding_id = hashlib.md5(
                    repo["full_name"].encode()
                ).hexdigest()

                # Check if we already have this
                with get_db() as db:
                    existing = db.execute(
                        "SELECT id FROM findings WHERE id = ?",
                        (finding_id,)
                    ).fetchone()
                    if existing:
                        continue

                # Score relevance
                rel_score = score_relevance(repo)

                # Check license
                lic = repo.get("license", "unknown").lower()
                lic_ok = 1 if lic in GOOD_LICENSES else 0

                # Fetch README for high-relevance repos
                readme = ""
                if rel_score >= 30:
                    readme = get_github_readme(repo["full_name"])
                    time.sleep(0.5)  # Rate limit politeness

                # LLM analysis for high-relevance repos
                analysis = {}
                if rel_score >= 40:
                    repo_with_readme = {**repo, "readme_excerpt": readme}
                    analysis = analyze_finding_with_llm(repo_with_readme)

                # Store finding
                with get_db() as db:
                    db.execute("""
                        INSERT OR IGNORE INTO findings
                        (id, source, source_type, title, url, description,
                         stars, license, license_ok, language, topics,
                         relevance_score, key_techniques, comparison_to_ours,
                         improvement_suggestion, raw_data, search_query, analyzed)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        finding_id,
                        repo["full_name"],
                        "github_repo",
                        repo.get("name", ""),
                        repo.get("url", ""),
                        repo.get("description", ""),
                        repo.get("stars", 0),
                        lic,
                        lic_ok,
                        repo.get("language", ""),
                        json.dumps(repo.get("topics", [])),
                        rel_score,
                        json.dumps(analysis.get("key_techniques", [])),
                        analysis.get("comparison", ""),
                        analysis.get("improvement_suggestion", ""),
                        json.dumps(repo),
                        query,
                        1 if analysis else 0,
                    ))

                new_count += 1

                # Store extracted techniques
                for tech in analysis.get("key_techniques", []):
                    store_technique(
                        name=tech,
                        category=categorize_technique(tech),
                        description=f"From {repo['full_name']}: {tech}",
                        source_id=finding_id,
                        relevance=rel_score,
                        difficulty=analysis.get("difficulty", "medium"),
                        impact=analysis.get("impact", "medium"),
                    )

                # Feed high-relevance findings to nerve
                if rel_score >= 60 and lic_ok:
                    feed_to_nerve(
                        "memory_research",
                        f"[Memory Research] {repo['name']} ({repo.get('stars', 0)} stars, "
                        f"{lic}): {repo.get('description', '')[:200]}. "
                        f"Relevance: {rel_score}/100. "
                        f"{analysis.get('improvement_suggestion', '')}",
                        confidence=min(0.95, rel_score / 100),
                    )
                    mark_fed_to_nerve(finding_id)

            total_new += new_count
            log_scan_finish(scan_id, len(repos), new_count)
            print(f"[GitHub] '{query}': {len(repos)} results, {new_count} new")

            # Rate limit: GitHub allows 10 search req/min unauthenticated
            time.sleep(7)

        except Exception as e:
            log_scan_error(scan_id, str(e))
            print(f"[GitHub] Scan error for '{query}': {e}")
            traceback.print_exc()

    return total_new


def run_paper_scan(queries: Optional[List[str]] = None):
    """Search for research papers on AI memory."""
    queries = queries or PAPER_SEARCH_QUERIES
    total_new = 0

    for query in queries:
        scan_id = log_scan_start("papers", query)
        try:
            papers = search_papers(query, limit=8)
            new_count = 0

            for paper in papers:
                paper_id = paper.get("paper_id", "")
                if not paper_id:
                    continue

                pid_hash = hashlib.md5(paper_id.encode()).hexdigest()

                with get_db() as db:
                    existing = db.execute(
                        "SELECT id FROM papers WHERE id = ?",
                        (pid_hash,)
                    ).fetchone()
                    if existing:
                        continue

                rel_score = score_paper_relevance(paper)

                # LLM analysis for relevant papers
                analysis = {}
                if rel_score >= 30 and paper.get("abstract"):
                    analysis = analyze_paper_with_llm(paper)

                with get_db() as db:
                    db.execute("""
                        INSERT OR IGNORE INTO papers
                        (id, title, authors, abstract, url, published,
                         source, relevance_score, key_ideas,
                         applicable_techniques, search_query, analyzed)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        pid_hash,
                        paper.get("title", ""),
                        paper.get("authors", ""),
                        paper.get("abstract", ""),
                        paper.get("url", ""),
                        paper.get("published", ""),
                        "semantic_scholar",
                        rel_score,
                        json.dumps(analysis.get("key_ideas", [])),
                        json.dumps(analysis.get("applicable_techniques", [])),
                        query,
                        1 if analysis else 0,
                    ))

                new_count += 1

                # Feed high-relevance papers to nerve
                if rel_score >= 50:
                    feed_to_nerve(
                        "memory_research",
                        f"[Memory Paper] '{paper['title']}' ({paper.get('year', '?')}, "
                        f"{paper.get('citations', 0)} citations): "
                        f"{analysis.get('practical_value', paper.get('abstract', '')[:200])}",
                        confidence=min(0.90, rel_score / 100),
                    )
                    with get_db() as db:
                        db.execute(
                            "UPDATE papers SET fed_to_nerve = 1 WHERE id = ?",
                            (pid_hash,)
                        )

            total_new += new_count
            log_scan_finish(scan_id, len(papers), new_count)
            print(f"[Papers] '{query}': {len(papers)} results, {new_count} new")

            # Rate limit for Semantic Scholar
            time.sleep(4)

        except Exception as e:
            log_scan_error(scan_id, str(e))
            print(f"[Papers] Scan error for '{query}': {e}")
            traceback.print_exc()

    return total_new


def run_web_scan(queries: Optional[List[str]] = None):
    """Search the web for blog posts, discussions, etc. about AI memory."""
    if queries is None:
        queries = [
            "MemGPT Letta memory architecture 2024 2025",
            "Claude Code memory best practices MCP",
            "AI agent long term memory solutions 2025",
            "LLM memory system comparison RAG vs knowledge graph",
            "MCP server memory persistence pattern",
            "best AI memory frameworks 2025",
        ]

    total_new = 0

    for query in queries:
        scan_id = log_scan_start("web", query)
        try:
            results = search_web(query, max_results=6)
            new_count = 0

            for result in results:
                finding_id = hashlib.md5(
                    result["url"].encode()
                ).hexdigest()

                with get_db() as db:
                    existing = db.execute(
                        "SELECT id FROM findings WHERE id = ?",
                        (finding_id,)
                    ).fetchone()
                    if existing:
                        continue

                # Score based on title + snippet
                pseudo_finding = {
                    "title": result["title"],
                    "description": result["snippet"],
                    "topics": [],
                    "license": "unknown",
                    "stars": 0,
                }
                rel_score = score_relevance(pseudo_finding)

                with get_db() as db:
                    db.execute("""
                        INSERT OR IGNORE INTO findings
                        (id, source, source_type, title, url, description,
                         relevance_score, search_query, analyzed)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        finding_id,
                        "web",
                        "web_article",
                        result["title"],
                        result["url"],
                        result["snippet"],
                        rel_score,
                        query,
                        0,
                    ))

                new_count += 1

            total_new += new_count
            log_scan_finish(scan_id, len(results), new_count)
            print(f"[Web] '{query}': {len(results)} results, {new_count} new")

            time.sleep(3)

        except Exception as e:
            log_scan_error(scan_id, str(e))
            print(f"[Web] Scan error for '{query}': {e}")

    return total_new


def run_full_scan():
    """Run all scan types and generate improvement proposals."""
    print(f"\n{'='*60}")
    print(f"[SCAN] Full memory research scan starting at {datetime.now()}")
    print(f"{'='*60}\n")

    gh_new = run_github_scan()
    paper_new = run_paper_scan()
    web_new = run_web_scan()

    total = gh_new + paper_new + web_new
    print(f"\n[SCAN] Complete: {total} new findings "
          f"(GitHub: {gh_new}, Papers: {paper_new}, Web: {web_new})")

    # Generate improvement proposals from new findings
    if total > 0:
        generate_improvement_proposals()

    return total


# ==========================================================================
# IMPROVEMENT PROPOSALS
# ==========================================================================

def generate_improvement_proposals():
    """Analyze top unprocessed findings and generate concrete improvement proposals."""
    with get_db() as db:
        # Get top findings that have been analyzed but not turned into improvements
        top_findings = db.execute("""
            SELECT id, title, description, relevance_score,
                   key_techniques, comparison_to_ours, improvement_suggestion,
                   source, license_ok
            FROM findings
            WHERE analyzed = 1
              AND relevance_score >= 50
              AND improvement_suggestion IS NOT NULL
              AND improvement_suggestion != ''
            ORDER BY relevance_score DESC
            LIMIT 10
        """).fetchall()

    if not top_findings:
        print("[Improvements] No new high-relevance findings to process.")
        return

    for finding in top_findings:
        # Check if we already have a similar improvement
        suggestion = finding["improvement_suggestion"]
        if not suggestion:
            continue

        with get_db() as db:
            similar = db.execute(
                "SELECT id FROM improvements WHERE title LIKE ? OR description LIKE ?",
                (f"%{finding['title'][:30]}%", f"%{suggestion[:50]}%"),
            ).fetchone()
            if similar:
                continue

        # Categorize the improvement
        category = categorize_improvement(suggestion, finding["title"])

        # Determine priority based on relevance and impact
        rel = finding["relevance_score"]
        if rel >= 80:
            priority = "P1"
        elif rel >= 60:
            priority = "P2"
        else:
            priority = "P3"

        techniques = finding["key_techniques"]
        try:
            tech_list = json.loads(techniques) if techniques else []
        except json.JSONDecodeError:
            tech_list = []

        # Build implementation steps
        steps = build_implementation_steps(category, suggestion, tech_list)

        with get_db() as db:
            db.execute("""
                INSERT INTO improvements
                (title, description, priority, category, current_state,
                 proposed_state, implementation_steps, estimated_effort,
                 source_findings, source_techniques, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                f"Improve: {finding['title'][:80]}",
                suggestion,
                priority,
                category,
                finding["comparison_to_ours"] or "Not yet assessed",
                suggestion,
                json.dumps(steps),
                estimate_effort(category, tech_list),
                json.dumps([finding["id"]]),
                techniques or "[]",
                "proposed",
            ))

        print(f"[Improvements] New proposal: {finding['title'][:60]} ({priority})")


def categorize_technique(tech_name: str) -> str:
    """Categorize a technique into our taxonomy."""
    tech_lower = tech_name.lower()
    if any(kw in tech_lower for kw in ["rag", "retrieval", "search", "query"]):
        return "retrieval"
    if any(kw in tech_lower for kw in ["graph", "knowledge", "ontology", "relation"]):
        return "knowledge_graph"
    if any(kw in tech_lower for kw in ["vector", "embedding", "semantic", "similarity"]):
        return "vector_search"
    if any(kw in tech_lower for kw in ["consolidat", "prune", "decay", "forget", "compress"]):
        return "consolidation"
    if any(kw in tech_lower for kw in ["context", "window", "select", "load"]):
        return "context_management"
    if any(kw in tech_lower for kw in ["episod", "event", "timeline", "temporal"]):
        return "episodic_memory"
    if any(kw in tech_lower for kw in ["learn", "outcome", "feedback", "reward"]):
        return "learning"
    if any(kw in tech_lower for kw in ["mcp", "server", "tool", "plugin", "protocol"]):
        return "mcp_pattern"
    if any(kw in tech_lower for kw in ["cache", "tier", "layer", "hierarchy"]):
        return "memory_hierarchy"
    return "general"


def categorize_improvement(suggestion: str, title: str) -> str:
    """Categorize an improvement suggestion."""
    text = f"{suggestion} {title}".lower()
    if any(kw in text for kw in ["rag", "retrieval", "search"]):
        return "retrieval"
    if any(kw in text for kw in ["graph", "knowledge graph"]):
        return "knowledge_graph"
    if any(kw in text for kw in ["vector", "embedding"]):
        return "vector_search"
    if any(kw in text for kw in ["consolidat", "prune", "compress", "decay"]):
        return "consolidation"
    if any(kw in text for kw in ["context", "auto-load", "task-based"]):
        return "context_management"
    if any(kw in text for kw in ["outcome", "learn", "feedback"]):
        return "learning"
    if any(kw in text for kw in ["mcp", "server", "tool"]):
        return "mcp_pattern"
    return "general"


def build_implementation_steps(category: str, suggestion: str, techniques: List[str]) -> List[str]:
    """Build concrete implementation steps for a given category."""
    steps = {
        "retrieval": [
            "Review current RAG vectors DB (53MB) for coverage gaps",
            "Evaluate suggested retrieval approach against current keyword search",
            "Prototype hybrid retrieval (keyword + semantic) on Nerve data",
            "Benchmark recall quality on 20 sample queries",
            "Deploy if recall improves by 15%+",
        ],
        "knowledge_graph": [
            "Assess Nerve v2 fact structure for graph conversion feasibility",
            "Design entity-relationship schema for Hive knowledge",
            "Prototype graph storage alongside existing SQLite facts",
            "Build query interface for multi-hop reasoning",
            "Evaluate vs current flat-fact approach on 10 complex queries",
        ],
        "vector_search": [
            "Check current embedding model (nomic-embed) performance",
            "Evaluate suggested embedding approach",
            "Benchmark on Hive-specific terminology recall",
            "Test hybrid search (vector + keyword) accuracy",
            "Deploy improved embedding pipeline",
        ],
        "consolidation": [
            "Audit current memory growth rate (MEMORY.md, Nerve, etc.)",
            "Design importance scoring for facts (recency, frequency, impact)",
            "Implement decay/consolidation prototype on test data",
            "Validate no critical knowledge is lost after consolidation",
            "Schedule automated consolidation cycles",
        ],
        "context_management": [
            "Map current context loading path (CLAUDE.md -> MEMORY.md -> topic files)",
            "Design task-detection heuristic from first user message",
            "Build context selector that loads relevant memory files",
            "Test on 20 real session starts for accuracy",
            "Deploy as SessionStart hook enhancement",
        ],
        "learning": [
            "Design outcome tracking schema (action -> result -> quality)",
            "Add outcome logging to key Hive operations",
            "Build feedback loop: successful patterns get promoted in memory",
            "Test on 10 historical decisions (would the system learn correctly?)",
            "Deploy as Nerve annotation layer",
        ],
        "mcp_pattern": [
            "Review current 4 MCP servers for pattern gaps",
            "Design new MCP tool for suggested capability",
            "Implement as FastAPI-based MCP server on ZeroDESK",
            "Test via Claude Code tool calls",
            "Document in CLAUDE.md MCP table",
        ],
        "memory_hierarchy": [
            "Map current memory tiers (working -> MEMORY.md -> Nerve -> topic files)",
            "Design explicit tier promotion/demotion rules",
            "Build automated tier management",
            "Test memory access latency across tiers",
            "Deploy tier manager as background service",
        ],
    }
    return steps.get(category, [
        f"Evaluate: {suggestion[:100]}",
        "Prototype on small dataset",
        "Benchmark against current approach",
        "Deploy if measurable improvement",
    ])


def estimate_effort(category: str, techniques: List[str]) -> str:
    """Estimate implementation effort."""
    complex_categories = {"knowledge_graph", "learning", "memory_hierarchy"}
    if category in complex_categories:
        return "2-3 sessions"
    if len(techniques) > 3:
        return "2 sessions"
    return "1 session"


# ==========================================================================
# HELPER DB FUNCTIONS
# ==========================================================================

def store_technique(name: str, category: str, description: str,
                    source_id: str, relevance: int,
                    difficulty: str = "medium", impact: str = "medium"):
    """Store an extracted technique."""
    with get_db() as db:
        # Avoid exact duplicates
        existing = db.execute(
            "SELECT id FROM techniques WHERE name = ? AND source_finding_id = ?",
            (name, source_id)
        ).fetchone()
        if existing:
            return

        db.execute("""
            INSERT INTO techniques
            (name, category, description, source_finding_id,
             relevance_score, difficulty, impact)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (name, category, description, source_id, relevance, difficulty, impact))


def log_scan_start(scan_type: str, query: str) -> int:
    """Log the start of a scan."""
    with get_db() as db:
        cursor = db.execute(
            "INSERT INTO scans (scan_type, query, status) VALUES (?, ?, 'running')",
            (scan_type, query)
        )
        return cursor.lastrowid


def log_scan_finish(scan_id: int, total: int, new: int):
    """Log scan completion."""
    with get_db() as db:
        db.execute(
            "UPDATE scans SET results_count=?, findings_new=?, "
            "status='complete', finished_at=datetime('now') WHERE id=?",
            (total, new, scan_id)
        )


def log_scan_error(scan_id: int, error: str):
    """Log scan error."""
    with get_db() as db:
        db.execute(
            "UPDATE scans SET status='error', error=?, "
            "finished_at=datetime('now') WHERE id=?",
            (error[:500], scan_id)
        )


def mark_fed_to_nerve(finding_id: str):
    """Mark a finding as fed to Nerve."""
    with get_db() as db:
        db.execute(
            "UPDATE findings SET fed_to_nerve = 1 WHERE id = ?",
            (finding_id,)
        )


# ==========================================================================
# BACKGROUND SCHEDULER
# ==========================================================================

_scheduler_running = False
_scan_lock = threading.Lock()


def scheduler_loop():
    """Run full scans every SCAN_INTERVAL_HOURS."""
    global _scheduler_running
    _scheduler_running = True
    print(f"[Scheduler] Started — scanning every {SCAN_INTERVAL_HOURS} hours")

    # Initial scan after 30 seconds (let the server start)
    time.sleep(30)

    while _scheduler_running:
        try:
            with _scan_lock:
                run_full_scan()
        except Exception as e:
            print(f"[Scheduler] Scan failed: {e}")
            traceback.print_exc()

        # Sleep for the interval (check every 60s for shutdown)
        for _ in range(SCAN_INTERVAL_HOURS * 60):
            if not _scheduler_running:
                break
            time.sleep(60)

    print("[Scheduler] Stopped")


def stop_scheduler():
    """Stop the background scheduler."""
    global _scheduler_running
    _scheduler_running = False


# ==========================================================================
# FASTAPI APP
# ==========================================================================

app = FastAPI(
    title="Hive Memory Researcher",
    description="Continuously researches AI memory solutions and suggests improvements",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class SearchRequest(BaseModel):
    topic: str
    sources: Optional[List[str]] = None  # ["github", "papers", "web"]


# --- Lifecycle ---

@app.on_event("startup")
async def startup():
    init_db()
    thread = threading.Thread(target=scheduler_loop, daemon=True)
    thread.start()
    print(f"[Memory Researcher] Running on port {PORT}")


@app.on_event("shutdown")
async def shutdown():
    stop_scheduler()


# --- Health ---

@app.get("/health")
async def health():
    """Health check."""
    with get_db() as db:
        findings_count = db.execute("SELECT COUNT(*) FROM findings").fetchone()[0]
        papers_count = db.execute("SELECT COUNT(*) FROM papers").fetchone()[0]
        techniques_count = db.execute("SELECT COUNT(*) FROM techniques").fetchone()[0]
        improvements_count = db.execute("SELECT COUNT(*) FROM improvements").fetchone()[0]
        last_scan = db.execute(
            "SELECT finished_at FROM scans WHERE status='complete' "
            "ORDER BY finished_at DESC LIMIT 1"
        ).fetchone()

    return {
        "status": "healthy",
        "service": "hive-memory-researcher",
        "port": PORT,
        "stats": {
            "findings": findings_count,
            "papers": papers_count,
            "techniques": techniques_count,
            "improvements": improvements_count,
            "last_scan": last_scan["finished_at"] if last_scan else None,
        },
        "scheduler_running": _scheduler_running,
        "scan_interval_hours": SCAN_INTERVAL_HOURS,
        "timestamp": datetime.now().isoformat(),
    }


# --- Findings ---

@app.get("/api/findings")
async def get_findings(
    min_relevance: int = Query(0, ge=0, le=100),
    source_type: Optional[str] = Query(None),
    license_ok: Optional[bool] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """Get research findings, ordered by relevance."""
    with get_db() as db:
        conditions = ["1=1"]
        params = []

        if min_relevance > 0:
            conditions.append("relevance_score >= ?")
            params.append(min_relevance)
        if source_type:
            conditions.append("source_type = ?")
            params.append(source_type)
        if license_ok is not None:
            conditions.append("license_ok = ?")
            params.append(1 if license_ok else 0)

        where = " AND ".join(conditions)
        params.extend([limit, offset])

        rows = db.execute(f"""
            SELECT id, source, source_type, title, url, description,
                   stars, license, license_ok, language, topics,
                   relevance_score, key_techniques, comparison_to_ours,
                   improvement_suggestion, search_query, found_at,
                   analyzed, fed_to_nerve
            FROM findings
            WHERE {where}
            ORDER BY relevance_score DESC
            LIMIT ? OFFSET ?
        """, params).fetchall()

        total = db.execute(
            f"SELECT COUNT(*) FROM findings WHERE {where}",
            params[:-2]
        ).fetchone()[0]

    findings = []
    for row in rows:
        f = dict(row)
        # Parse JSON fields
        for field in ("topics", "key_techniques"):
            try:
                f[field] = json.loads(f[field]) if f[field] else []
            except json.JSONDecodeError:
                f[field] = []
        findings.append(f)

    return {
        "findings": findings,
        "total": total,
        "limit": limit,
        "offset": offset,
    }


# --- Papers ---

@app.get("/api/papers")
async def get_papers(
    min_relevance: int = Query(0, ge=0, le=100),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """Get research papers, ordered by relevance."""
    with get_db() as db:
        conditions = ["1=1"]
        params = []

        if min_relevance > 0:
            conditions.append("relevance_score >= ?")
            params.append(min_relevance)

        where = " AND ".join(conditions)
        params.extend([limit, offset])

        rows = db.execute(f"""
            SELECT id, title, authors, abstract, url, published,
                   source, relevance_score, key_ideas,
                   applicable_techniques, search_query, found_at,
                   analyzed, fed_to_nerve
            FROM papers
            WHERE {where}
            ORDER BY relevance_score DESC
            LIMIT ? OFFSET ?
        """, params).fetchall()

        total = db.execute(
            f"SELECT COUNT(*) FROM papers WHERE {where}",
            params[:-2]
        ).fetchone()[0]

    papers = []
    for row in rows:
        p = dict(row)
        for field in ("key_ideas", "applicable_techniques"):
            try:
                p[field] = json.loads(p[field]) if p[field] else []
            except json.JSONDecodeError:
                p[field] = []
        papers.append(p)

    return {
        "papers": papers,
        "total": total,
        "limit": limit,
        "offset": offset,
    }


# --- Techniques ---

@app.get("/api/techniques")
async def get_techniques(
    category: Optional[str] = Query(None),
    min_relevance: int = Query(0, ge=0, le=100),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """Get extracted techniques, ranked by relevance."""
    with get_db() as db:
        conditions = ["1=1"]
        params = []

        if category:
            conditions.append("category = ?")
            params.append(category)
        if min_relevance > 0:
            conditions.append("relevance_score >= ?")
            params.append(min_relevance)

        where = " AND ".join(conditions)
        params.extend([limit, offset])

        rows = db.execute(f"""
            SELECT t.*, f.title as source_title, f.url as source_url
            FROM techniques t
            LEFT JOIN findings f ON t.source_finding_id = f.id
            WHERE {where}
            ORDER BY t.relevance_score DESC
            LIMIT ? OFFSET ?
        """, params).fetchall()

        total = db.execute(
            f"SELECT COUNT(*) FROM techniques t WHERE {where}",
            params[:-2]
        ).fetchone()[0]

        # Category stats
        cats = db.execute("""
            SELECT category, COUNT(*) as count, AVG(relevance_score) as avg_relevance
            FROM techniques
            GROUP BY category
            ORDER BY avg_relevance DESC
        """).fetchall()

    return {
        "techniques": [dict(r) for r in rows],
        "total": total,
        "categories": [dict(c) for c in cats],
        "limit": limit,
        "offset": offset,
    }


# --- Improvements ---

@app.get("/api/improvements")
async def get_improvements(
    priority: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """Get suggested improvements to our memory system."""
    with get_db() as db:
        conditions = ["1=1"]
        params = []

        if priority:
            conditions.append("priority = ?")
            params.append(priority)
        if category:
            conditions.append("category = ?")
            params.append(category)
        if status:
            conditions.append("status = ?")
            params.append(status)

        where = " AND ".join(conditions)
        params.extend([limit, offset])

        rows = db.execute(f"""
            SELECT * FROM improvements
            WHERE {where}
            ORDER BY
                CASE priority WHEN 'P0' THEN 0 WHEN 'P1' THEN 1
                     WHEN 'P2' THEN 2 ELSE 3 END,
                created_at DESC
            LIMIT ? OFFSET ?
        """, params).fetchall()

        total = db.execute(
            f"SELECT COUNT(*) FROM improvements WHERE {where}",
            params[:-2]
        ).fetchone()[0]

        # Priority breakdown
        breakdown = db.execute("""
            SELECT priority, status, COUNT(*) as count
            FROM improvements
            GROUP BY priority, status
            ORDER BY priority, status
        """).fetchall()

    improvements = []
    for row in rows:
        imp = dict(row)
        for field in ("implementation_steps", "source_findings", "source_techniques"):
            try:
                imp[field] = json.loads(imp[field]) if imp[field] else []
            except json.JSONDecodeError:
                imp[field] = []
        improvements.append(imp)

    return {
        "improvements": improvements,
        "total": total,
        "breakdown": [dict(b) for b in breakdown],
        "limit": limit,
        "offset": offset,
    }


@app.patch("/api/improvements/{improvement_id}")
async def update_improvement(improvement_id: int, request: Request):
    """Update an improvement's status."""
    body = await request.json()
    allowed_fields = {"status", "priority", "notes"}
    updates = {k: v for k, v in body.items() if k in allowed_fields}

    if not updates:
        raise HTTPException(400, "No valid fields to update")

    set_clause = ", ".join(f"{k} = ?" for k in updates)
    values = list(updates.values())
    values.append(improvement_id)

    with get_db() as db:
        db.execute(
            f"UPDATE improvements SET {set_clause}, "
            f"updated_at = datetime('now') WHERE id = ?",
            values
        )

    return {"status": "updated", "id": improvement_id, "fields": list(updates.keys())}


# --- Manual Search ---

@app.post("/api/search")
async def manual_search(req: SearchRequest, background_tasks: BackgroundTasks):
    """Trigger a manual search on a specific topic."""
    sources = req.sources or ["github", "papers", "web"]

    def do_search():
        results = {"github": 0, "papers": 0, "web": 0}
        with _scan_lock:
            if "github" in sources:
                queries = [
                    f"{req.topic} MIT license",
                    f"{req.topic} AI",
                    req.topic,
                ]
                results["github"] = run_github_scan(queries)
            if "papers" in sources:
                queries = [
                    req.topic,
                    f"{req.topic} large language model",
                ]
                results["papers"] = run_paper_scan(queries)
            if "web" in sources:
                queries = [
                    f"{req.topic} AI 2025",
                    f"{req.topic} best practices",
                ]
                results["web"] = run_web_scan(queries)

            if sum(results.values()) > 0:
                generate_improvement_proposals()

        print(f"[Manual Search] '{req.topic}': {results}")

    background_tasks.add_task(do_search)

    return {
        "status": "search_started",
        "topic": req.topic,
        "sources": sources,
        "message": "Search running in background. Check /api/findings for results.",
    }


# --- Summary / Dashboard ---

@app.get("/api/summary")
async def get_summary():
    """Get a high-level summary of all research."""
    with get_db() as db:
        # Overall stats
        findings_total = db.execute("SELECT COUNT(*) FROM findings").fetchone()[0]
        findings_high = db.execute(
            "SELECT COUNT(*) FROM findings WHERE relevance_score >= 60"
        ).fetchone()[0]
        papers_total = db.execute("SELECT COUNT(*) FROM papers").fetchone()[0]
        papers_high = db.execute(
            "SELECT COUNT(*) FROM papers WHERE relevance_score >= 50"
        ).fetchone()[0]
        techniques_total = db.execute("SELECT COUNT(*) FROM techniques").fetchone()[0]
        improvements_total = db.execute("SELECT COUNT(*) FROM improvements").fetchone()[0]
        improvements_pending = db.execute(
            "SELECT COUNT(*) FROM improvements WHERE status = 'proposed'"
        ).fetchone()[0]
        fed_to_nerve = db.execute(
            "SELECT COUNT(*) FROM findings WHERE fed_to_nerve = 1"
        ).fetchone()[0]

        # Top repos
        top_repos = db.execute("""
            SELECT title, url, stars, license, relevance_score, improvement_suggestion
            FROM findings
            WHERE source_type = 'github_repo'
            ORDER BY relevance_score DESC
            LIMIT 5
        """).fetchall()

        # Top papers
        top_papers = db.execute("""
            SELECT title, url, relevance_score, key_ideas
            FROM papers
            ORDER BY relevance_score DESC
            LIMIT 5
        """).fetchall()

        # Top improvements
        top_improvements = db.execute("""
            SELECT title, description, priority, category, status
            FROM improvements
            ORDER BY
                CASE priority WHEN 'P0' THEN 0 WHEN 'P1' THEN 1
                     WHEN 'P2' THEN 2 ELSE 3 END
            LIMIT 5
        """).fetchall()

        # Technique categories
        tech_cats = db.execute("""
            SELECT category, COUNT(*) as count, ROUND(AVG(relevance_score), 1) as avg_rel
            FROM techniques
            GROUP BY category
            ORDER BY avg_rel DESC
        """).fetchall()

        # Scan history
        recent_scans = db.execute("""
            SELECT scan_type, query, results_count, findings_new, status, finished_at
            FROM scans
            ORDER BY id DESC
            LIMIT 10
        """).fetchall()

    top_papers_list = []
    for p in top_papers:
        pd = dict(p)
        try:
            pd["key_ideas"] = json.loads(pd["key_ideas"]) if pd["key_ideas"] else []
        except json.JSONDecodeError:
            pd["key_ideas"] = []
        top_papers_list.append(pd)

    return {
        "stats": {
            "findings_total": findings_total,
            "findings_high_relevance": findings_high,
            "papers_total": papers_total,
            "papers_high_relevance": papers_high,
            "techniques_extracted": techniques_total,
            "improvements_total": improvements_total,
            "improvements_pending": improvements_pending,
            "insights_fed_to_nerve": fed_to_nerve,
        },
        "top_repos": [dict(r) for r in top_repos],
        "top_papers": top_papers_list,
        "top_improvements": [dict(i) for i in top_improvements],
        "technique_categories": [dict(c) for c in tech_cats],
        "recent_scans": [dict(s) for s in recent_scans],
        "current_system": CURRENT_SYSTEM,
    }


# --- Dashboard HTML ---

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Serve a simple HTML dashboard."""
    with get_db() as db:
        findings_total = db.execute("SELECT COUNT(*) FROM findings").fetchone()[0]
        papers_total = db.execute("SELECT COUNT(*) FROM papers").fetchone()[0]
        techniques_total = db.execute("SELECT COUNT(*) FROM techniques").fetchone()[0]
        improvements_total = db.execute("SELECT COUNT(*) FROM improvements").fetchone()[0]
        fed_nerve = db.execute(
            "SELECT COUNT(*) FROM findings WHERE fed_to_nerve = 1"
        ).fetchone()[0]
        last_scan = db.execute(
            "SELECT finished_at FROM scans WHERE status='complete' "
            "ORDER BY finished_at DESC LIMIT 1"
        ).fetchone()

        top5 = db.execute("""
            SELECT title, url, stars, relevance_score, license, improvement_suggestion
            FROM findings
            WHERE source_type = 'github_repo'
            ORDER BY relevance_score DESC LIMIT 10
        """).fetchall()

        top_papers = db.execute("""
            SELECT title, url, relevance_score, key_ideas
            FROM papers
            ORDER BY relevance_score DESC LIMIT 10
        """).fetchall()

        top_improvements = db.execute("""
            SELECT id, title, description, priority, category, status
            FROM improvements
            ORDER BY CASE priority WHEN 'P0' THEN 0 WHEN 'P1' THEN 1
                          WHEN 'P2' THEN 2 ELSE 3 END
            LIMIT 10
        """).fetchall()

    ls = last_scan["finished_at"] if last_scan else "Never"

    repos_html = ""
    for r in top5:
        stars = r["stars"] or 0
        lic = r["license"] or "?"
        suggestion = html_mod.escape(r["improvement_suggestion"] or "")[:200]
        repos_html += f"""
        <tr>
            <td><a href="{r['url']}" target="_blank">{html_mod.escape(r['title'])}</a></td>
            <td>{stars:,}</td>
            <td>{lic}</td>
            <td><span class="score score-{_score_class(r['relevance_score'])}">{r['relevance_score']}</span></td>
            <td class="suggestion">{suggestion}</td>
        </tr>"""

    papers_html = ""
    for p in top_papers:
        ideas = ""
        try:
            ideas_list = json.loads(p["key_ideas"]) if p["key_ideas"] else []
            ideas = "; ".join(ideas_list[:2])
        except Exception:
            pass
        papers_html += f"""
        <tr>
            <td><a href="{p['url']}" target="_blank">{html_mod.escape(p['title'][:80])}</a></td>
            <td><span class="score score-{_score_class(p['relevance_score'])}">{p['relevance_score']}</span></td>
            <td class="suggestion">{html_mod.escape(ideas)[:200]}</td>
        </tr>"""

    improv_html = ""
    for imp in top_improvements:
        pclass = {"P0": "p0", "P1": "p1", "P2": "p2"}.get(imp["priority"], "p3")
        improv_html += f"""
        <tr>
            <td><span class="priority {pclass}">{imp['priority']}</span></td>
            <td>{html_mod.escape(imp['title'][:60])}</td>
            <td>{imp['category'] or '-'}</td>
            <td class="suggestion">{html_mod.escape(imp['description'] or '')[:200]}</td>
            <td>{imp['status']}</td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html>
<head>
    <title>Hive Memory Researcher</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
               background: #0a0a0f; color: #e0e0e0; padding: 20px; }}
        h1 {{ color: #00ff88; margin-bottom: 5px; }}
        h2 {{ color: #88ccff; margin: 20px 0 10px; font-size: 1.1em; }}
        .subtitle {{ color: #888; margin-bottom: 20px; }}
        .stats {{ display: flex; gap: 15px; flex-wrap: wrap; margin: 15px 0; }}
        .stat {{ background: #151520; border: 1px solid #333; border-radius: 8px;
                padding: 12px 18px; min-width: 140px; }}
        .stat .num {{ font-size: 1.8em; color: #00ff88; font-weight: bold; }}
        .stat .label {{ font-size: 0.8em; color: #888; margin-top: 2px; }}
        table {{ width: 100%; border-collapse: collapse; margin: 10px 0; }}
        th {{ background: #1a1a2e; color: #88ccff; text-align: left; padding: 8px; font-size: 0.85em; }}
        td {{ padding: 8px; border-bottom: 1px solid #222; font-size: 0.85em; }}
        tr:hover {{ background: #151520; }}
        a {{ color: #00cc66; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
        .score {{ padding: 2px 8px; border-radius: 10px; font-weight: bold; font-size: 0.8em; }}
        .score-high {{ background: #00442a; color: #00ff88; }}
        .score-med {{ background: #443a00; color: #ffcc00; }}
        .score-low {{ background: #441a1a; color: #ff6666; }}
        .suggestion {{ color: #aaa; font-size: 0.8em; max-width: 300px; }}
        .priority {{ padding: 2px 8px; border-radius: 4px; font-weight: bold; font-size: 0.8em; }}
        .p0 {{ background: #661111; color: #ff4444; }}
        .p1 {{ background: #664411; color: #ffaa00; }}
        .p2 {{ background: #335533; color: #88cc88; }}
        .p3 {{ background: #333355; color: #8888cc; }}
        .search-form {{ background: #151520; padding: 15px; border-radius: 8px; margin: 15px 0; }}
        .search-form input {{ background: #0a0a0f; color: #e0e0e0; border: 1px solid #444;
                             padding: 8px 12px; border-radius: 4px; width: 300px; }}
        .search-form button {{ background: #00aa55; color: #fff; border: none;
                              padding: 8px 16px; border-radius: 4px; cursor: pointer; margin-left: 8px; }}
        .search-form button:hover {{ background: #00cc66; }}
        #search-result {{ color: #00ff88; margin-top: 8px; font-size: 0.9em; }}
    </style>
</head>
<body>
    <h1>Hive Memory Researcher</h1>
    <p class="subtitle">Continuously discovering better AI memory solutions | Last scan: {ls}</p>

    <div class="stats">
        <div class="stat"><div class="num">{findings_total}</div><div class="label">Findings</div></div>
        <div class="stat"><div class="num">{papers_total}</div><div class="label">Papers</div></div>
        <div class="stat"><div class="num">{techniques_total}</div><div class="label">Techniques</div></div>
        <div class="stat"><div class="num">{improvements_total}</div><div class="label">Improvements</div></div>
        <div class="stat"><div class="num">{fed_nerve}</div><div class="label">Fed to Nerve</div></div>
    </div>

    <div class="search-form">
        <strong>Manual Search:</strong><br><br>
        <input type="text" id="search-topic" placeholder="e.g. episodic memory AI agent" />
        <button onclick="doSearch()">Search Now</button>
        <div id="search-result"></div>
    </div>

    <h2>Top GitHub Repos</h2>
    <table>
        <tr><th>Repo</th><th>Stars</th><th>License</th><th>Score</th><th>Suggestion</th></tr>
        {repos_html or '<tr><td colspan="5" style="color:#666">No findings yet — first scan in 30 seconds</td></tr>'}
    </table>

    <h2>Top Research Papers</h2>
    <table>
        <tr><th>Paper</th><th>Score</th><th>Key Ideas</th></tr>
        {papers_html or '<tr><td colspan="3" style="color:#666">No papers yet</td></tr>'}
    </table>

    <h2>Suggested Improvements</h2>
    <table>
        <tr><th>Priority</th><th>Title</th><th>Category</th><th>Description</th><th>Status</th></tr>
        {improv_html or '<tr><td colspan="5" style="color:#666">No improvements yet</td></tr>'}
    </table>

    <h2>API Endpoints</h2>
    <table>
        <tr><td><a href="/health">/health</a></td><td>Health check + stats</td></tr>
        <tr><td><a href="/api/findings">/api/findings</a></td><td>All research findings (GitHub repos, web articles)</td></tr>
        <tr><td><a href="/api/papers">/api/papers</a></td><td>Research papers from Semantic Scholar</td></tr>
        <tr><td><a href="/api/techniques">/api/techniques</a></td><td>Extracted techniques ranked by relevance</td></tr>
        <tr><td><a href="/api/improvements">/api/improvements</a></td><td>Suggested improvements to our memory system</td></tr>
        <tr><td><a href="/api/summary">/api/summary</a></td><td>Full summary with top items from each category</td></tr>
        <tr><td>POST /api/search</td><td>Trigger manual search (body: {{"topic": "...", "sources": ["github","papers","web"]}})</td></tr>
    </table>

    <script>
        async function doSearch() {{
            const topic = document.getElementById('search-topic').value.trim();
            if (!topic) return;
            const el = document.getElementById('search-result');
            el.textContent = 'Searching...';
            try {{
                const resp = await fetch('/api/search', {{
                    method: 'POST',
                    headers: {{'Content-Type': 'application/json'}},
                    body: JSON.stringify({{topic: topic}})
                }});
                const data = await resp.json();
                el.textContent = data.message || 'Search started!';
                setTimeout(() => location.reload(), 30000);
            }} catch(e) {{
                el.textContent = 'Error: ' + e.message;
            }}
        }}
    </script>
</body>
</html>"""


def _score_class(score: int) -> str:
    """CSS class for a relevance score."""
    if score >= 60:
        return "high"
    if score >= 30:
        return "med"
    return "low"


# ==========================================================================
# MAIN
# ==========================================================================

if __name__ == "__main__":
    print(f"""
╔══════════════════════════════════════════════════════╗
║  THE HIVE — Memory Researcher Agent                  ║
║  Port: {PORT}                                          ║
║  DB: {DB_PATH}  ║
║  Scan interval: every {SCAN_INTERVAL_HOURS} hours                        ║
║  Sources: GitHub, Semantic Scholar, DuckDuckGo       ║
║  Dashboard: http://localhost:{PORT}                    ║
╚══════════════════════════════════════════════════════╝
""")
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
