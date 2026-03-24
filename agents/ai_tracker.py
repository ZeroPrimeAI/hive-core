#!/usr/bin/env python3
"""
THE HIVE — AI Industry Tracker Agent
Port 8917 | SQLite at /home/zero/hivecode_sandbox/ai_tracker.db
MIT License

The Hive's EYES on the AI industry. Autonomous discovery system that:
  - Tracks GitHub trending AI/ML repos (MIT/Apache only)
  - Searches awesome-lists for new tools
  - Monitors competitor star counts (AutoGPT, CrewAI, LangChain, MemGPT, etc.)
  - Discovers new HuggingFace models (especially gemma3 fine-tunes)
  - Searches for new Claude Code tools/plugins
  - Grades every finding with a Hive-Fit Score (0-100)
  - Auto-absorbs high-scoring repos (80+) into the sandbox
  - Tracks AI news: new models, frameworks, free-tier changes, free compute
  - Feeds discoveries to nerve

Runs every 2 hours. All data in SQLite. Full HTML dashboard + JSON API.
"""

import asyncio
import hashlib
import json
import logging
import os
import re
import shutil
import sqlite3
import subprocess
import time
import traceback
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PORT = 8917
DB_PATH = "/home/zero/hivecode_sandbox/ai_tracker.db"
DISCOVERIES_DIR = "/home/zero/hivecode_sandbox/discoveries"
SCAN_INTERVAL = 7200  # 2 hours in seconds
NERVE_URL = "http://100.70.226.103:8200/api/add"  # ZeroQ nerve

LOG_FMT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FMT)
log = logging.getLogger("ai-tracker")

# GitHub unauthenticated: 60 requests/hour — we stay well under
GITHUB_API = "https://api.github.com"
HUGGINGFACE_API = "https://huggingface.co/api"

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)

# ---------------------------------------------------------------------------
# Competitors to track
# ---------------------------------------------------------------------------
COMPETITORS = {
    "AutoGPT": "Significant-Gravitas/AutoGPT",
    "CrewAI": "crewAIInc/crewAI",
    "LangChain": "langchain-ai/langchain",
    "MemGPT": "cpacker/MemGPT",
    "MetaGPT": "geekan/MetaGPT",
    "OpenDevin": "All-Hands-AI/OpenHands",
    "Aider": "Aider-AI/aider",
    "Ollama": "ollama/ollama",
    "vLLM": "vllm-project/vllm",
    "LiteLLM": "BerriAI/litellm",
    "LlamaIndex": "run-llama/llama_index",
    "Dify": "langgenius/dify",
    "AnythingLLM": "Mintplex-Labs/anything-llm",
    "Open-WebUI": "open-webui/open-webui",
    "LocalAI": "mudler/LocalAI",
    "Jan": "janhq/jan",
    "GPT-Pilot": "Pythagora-io/gpt-pilot",
}

# Keywords for GitHub search
AI_SEARCH_QUERIES = [
    "AI agent framework",
    "LLM tool",
    "autonomous agent",
    "multi-agent",
    "AI coding assistant",
    "gemma fine-tune",
    "gemma3",
    "local LLM",
    "voice AI",
    "TTS text to speech",
    "AI phone",
    "AI content generation",
    "AI trading bot",
    "knowledge graph AI",
    "RAG retrieval augmented",
    "claude code plugin",
    "claude code tool",
    "MCP server",
    "model context protocol",
    "AI SEO",
    "AI lead generation",
    "AI cold calling",
    "AI dispatcher",
    "AI memory system",
    "AI distillation",
    "LoRA adapter",
    "edge-tts",
    "anime AI generation",
]

# Awesome lists to scan
AWESOME_LISTS = [
    "sindresorhus/awesome",
    "vinta/awesome-python",
    "josephmisiti/awesome-machine-learning",
    "dair-ai/Prompt-Engineering-Guide",
    "f/awesome-chatgpt-prompts",
    "e2b-dev/awesome-ai-agents",
    "kairichard/awesome-tts",
    "eugeneyan/open-llms",
    "jxzhangjhu/Awesome-LLM",
    "steven2358/awesome-generative-ai",
]

# HuggingFace model searches
HF_MODEL_SEARCHES = [
    "gemma3",
    "gemma-3",
    "gemma2 fine-tune",
    "gemma lora",
    "whisper fine-tune",
    "tts",
    "text-to-speech",
    "code-generation",
    "function-calling",
]

# Hive needs — for relevance scoring
HIVE_NEEDS = [
    "phone", "telephony", "twilio", "voip", "call",
    "content", "blog", "seo", "marketing", "social media",
    "trading", "forex", "crypto", "financial",
    "memory", "knowledge graph", "rag", "vector",
    "agent", "multi-agent", "swarm", "autonomous",
    "tts", "text-to-speech", "voice", "audio",
    "gemma", "fine-tune", "lora", "qlora", "distillation",
    "mcp", "claude", "tool use", "function calling",
    "anime", "image generation", "stable diffusion",
    "dispatcher", "locksmith", "lead generation",
    "scraping", "crawling", "data extraction",
    "deployment", "systemd", "docker", "kubernetes",
]

# License policy
ACCEPTED_LICENSES = {
    "mit": 30,
    "apache-2.0": 30,
    "bsd-2-clause": 20,
    "bsd-3-clause": 20,
    "unlicense": 20,
    "cc0-1.0": 20,
    "isc": 20,
    "0bsd": 20,
}
REJECTED_LICENSES = {
    "gpl-2.0", "gpl-3.0", "agpl-3.0", "lgpl-2.1", "lgpl-3.0",
    "sspl-1.0", "busl-1.1", "cc-by-sa-4.0", "cc-by-nc-4.0",
    "eupl-1.1", "eupl-1.2",
}

# Free compute/API sources to track
FREE_TIERS = [
    {"name": "Google Colab", "url": "https://colab.research.google.com", "type": "compute", "gpu": "T4 15GB", "limits": "12h session, 1 GPU"},
    {"name": "Kaggle Notebooks", "url": "https://www.kaggle.com/code", "type": "compute", "gpu": "T4/P100 16GB", "limits": "30h/week GPU"},
    {"name": "Lightning AI", "url": "https://lightning.ai", "type": "compute", "gpu": "T4 16GB", "limits": "22 GPU-hours free"},
    {"name": "Hugging Face Spaces", "url": "https://huggingface.co/spaces", "type": "compute", "gpu": "T4 free tier", "limits": "Limited free GPU"},
    {"name": "Vast.ai", "url": "https://vast.ai", "type": "compute", "gpu": "Various", "limits": "Pay per hour, cheap GPUs"},
    {"name": "RunPod", "url": "https://runpod.io", "type": "compute", "gpu": "Various", "limits": "Pay per hour"},
    {"name": "Replicate", "url": "https://replicate.com", "type": "api", "gpu": "N/A", "limits": "Free predictions on some models"},
    {"name": "Groq", "url": "https://groq.com", "type": "api", "gpu": "LPU", "limits": "Free tier API, fast inference"},
    {"name": "Together AI", "url": "https://together.ai", "type": "api", "gpu": "N/A", "limits": "$1 free credit"},
    {"name": "Cerebras", "url": "https://cerebras.ai", "type": "api", "gpu": "WSE", "limits": "Free inference API"},
    {"name": "SambaNova", "url": "https://sambanova.ai", "type": "api", "gpu": "RDU", "limits": "Free cloud API"},
    {"name": "GitHub Codespaces", "url": "https://github.com/codespaces", "type": "compute", "gpu": "None (CPU)", "limits": "60h/month free"},
    {"name": "Google AI Studio", "url": "https://aistudio.google.com", "type": "api", "gpu": "N/A", "limits": "Free Gemini API"},
    {"name": "OpenRouter", "url": "https://openrouter.ai", "type": "api", "gpu": "N/A", "limits": "Free models available"},
]

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

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
    os.makedirs(DISCOVERIES_DIR, exist_ok=True)
    with get_db() as db:
        db.executescript("""
            -- Discovered repos/tools
            CREATE TABLE IF NOT EXISTS discoveries (
                id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                name TEXT NOT NULL,
                full_name TEXT,
                url TEXT NOT NULL,
                description TEXT,
                license TEXT,
                language TEXT,
                stars INTEGER DEFAULT 0,
                forks INTEGER DEFAULT 0,
                open_issues INTEGER DEFAULT 0,
                created_at TEXT,
                updated_at TEXT,
                pushed_at TEXT,
                size_kb INTEGER DEFAULT 0,
                topics TEXT,
                hive_fit_score INTEGER DEFAULT 0,
                score_breakdown TEXT,
                status TEXT DEFAULT 'discovered',
                absorbed INTEGER DEFAULT 0,
                absorbed_at TEXT,
                local_path TEXT,
                relevance_tags TEXT,
                notes TEXT,
                found_at TEXT DEFAULT (datetime('now')),
                last_checked TEXT DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_disc_score ON discoveries(hive_fit_score DESC);
            CREATE INDEX IF NOT EXISTS idx_disc_source ON discoveries(source);
            CREATE INDEX IF NOT EXISTS idx_disc_status ON discoveries(status);
            CREATE INDEX IF NOT EXISTS idx_disc_found ON discoveries(found_at DESC);

            -- Competitor tracking
            CREATE TABLE IF NOT EXISTS competitors (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                repo TEXT NOT NULL,
                url TEXT NOT NULL,
                stars INTEGER DEFAULT 0,
                forks INTEGER DEFAULT 0,
                open_issues INTEGER DEFAULT 0,
                watchers INTEGER DEFAULT 0,
                description TEXT,
                language TEXT,
                last_release TEXT,
                last_release_date TEXT,
                star_history TEXT,
                checked_at TEXT DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_comp_stars ON competitors(stars DESC);

            -- HuggingFace models
            CREATE TABLE IF NOT EXISTS models (
                id TEXT PRIMARY KEY,
                model_id TEXT NOT NULL,
                author TEXT,
                url TEXT NOT NULL,
                pipeline_tag TEXT,
                tags TEXT,
                downloads INTEGER DEFAULT 0,
                likes INTEGER DEFAULT 0,
                library TEXT,
                license TEXT,
                created_at TEXT,
                last_modified TEXT,
                hive_fit_score INTEGER DEFAULT 0,
                relevance_tags TEXT,
                notes TEXT,
                found_at TEXT DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_models_score ON models(hive_fit_score DESC);
            CREATE INDEX IF NOT EXISTS idx_models_downloads ON models(downloads DESC);

            -- AI news/announcements
            CREATE TABLE IF NOT EXISTS news (
                id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                title TEXT NOT NULL,
                url TEXT,
                summary TEXT,
                category TEXT,
                importance INTEGER DEFAULT 5,
                found_at TEXT DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_news_importance ON news(importance DESC);
            CREATE INDEX IF NOT EXISTS idx_news_found ON news(found_at DESC);

            -- Free tiers / compute sources
            CREATE TABLE IF NOT EXISTS free_tiers (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                url TEXT NOT NULL,
                tier_type TEXT,
                gpu TEXT,
                limits TEXT,
                status TEXT DEFAULT 'active',
                last_checked TEXT DEFAULT (datetime('now')),
                notes TEXT
            );

            -- Scan history
            CREATE TABLE IF NOT EXISTS scans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scan_type TEXT NOT NULL,
                started_at TEXT DEFAULT (datetime('now')),
                finished_at TEXT,
                repos_found INTEGER DEFAULT 0,
                models_found INTEGER DEFAULT 0,
                news_found INTEGER DEFAULT 0,
                absorbed INTEGER DEFAULT 0,
                errors TEXT,
                status TEXT DEFAULT 'running'
            );
            CREATE INDEX IF NOT EXISTS idx_scans_started ON scans(started_at DESC);
        """)

    # Seed free tiers
    with get_db() as db:
        for ft in FREE_TIERS:
            ft_id = hashlib.md5(ft["name"].encode()).hexdigest()[:16]
            db.execute("""
                INSERT OR REPLACE INTO free_tiers (id, name, url, tier_type, gpu, limits)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (ft_id, ft["name"], ft["url"], ft["type"], ft["gpu"], ft["limits"]))

    log.info("Database initialized: %s", DB_PATH)


# ---------------------------------------------------------------------------
# HTTP client helpers
# ---------------------------------------------------------------------------

def _headers(accept: str = "application/json") -> dict:
    return {
        "Accept": accept,
        "User-Agent": USER_AGENT,
    }


def _github_headers() -> dict:
    return {
        "Accept": "application/vnd.github+json",
        "User-Agent": USER_AGENT,
        "X-GitHub-Api-Version": "2022-11-28",
    }


# ---------------------------------------------------------------------------
# Scoring engine
# ---------------------------------------------------------------------------

def compute_hive_fit_score(repo: dict) -> tuple[int, dict]:
    """
    Grade a repo/tool on Hive-Fit (0-100).
    Returns (score, breakdown_dict).
    """
    breakdown = {}
    score = 0

    # 1. License (+30 MIT/Apache, +20 BSD, -100 GPL)
    license_key = (repo.get("license") or "").lower().strip()
    # Normalize common variants
    license_map = {
        "mit license": "mit", "mit": "mit",
        "apache license 2.0": "apache-2.0", "apache-2.0": "apache-2.0",
        "bsd 2-clause \"simplified\" license": "bsd-2-clause", "bsd-2-clause": "bsd-2-clause",
        "bsd 3-clause \"new\" or \"revised\" license": "bsd-3-clause", "bsd-3-clause": "bsd-3-clause",
        "the unlicense": "unlicense", "unlicense": "unlicense",
        "isc license": "isc", "isc": "isc",
        "cc0-1.0": "cc0-1.0", "0bsd": "0bsd",
    }
    normalized = license_map.get(license_key, license_key)

    if normalized in REJECTED_LICENSES or "gpl" in normalized:
        breakdown["license"] = -100
        return -100, breakdown  # Instant reject

    license_score = ACCEPTED_LICENSES.get(normalized, 0)
    if license_score == 0 and normalized:
        # Unknown license — small penalty
        license_score = -5
    breakdown["license"] = license_score
    score += license_score

    # 2. Relevance to Hive needs (+0 to +20)
    text = " ".join([
        repo.get("name", ""),
        repo.get("description", ""),
        repo.get("topics", ""),
    ]).lower()
    matches = sum(1 for need in HIVE_NEEDS if need in text)
    relevance = min(20, matches * 5)
    breakdown["relevance"] = relevance
    score += relevance

    # 3. Quality — star count (+0 to +20)
    stars = repo.get("stars", 0)
    if stars >= 1000:
        quality = 20
    elif stars >= 100:
        quality = 10
    elif stars >= 10:
        quality = 5
    else:
        quality = 0
    breakdown["quality"] = quality
    score += quality

    # 4. Freshness — updated recently (+0 to +10)
    updated = repo.get("updated_at") or repo.get("pushed_at") or ""
    freshness = 0
    if updated:
        try:
            updated_dt = datetime.fromisoformat(updated.replace("Z", "+00:00"))
            age = datetime.now(timezone.utc) - updated_dt
            if age.days <= 7:
                freshness = 10
            elif age.days <= 30:
                freshness = 5
            elif age.days <= 90:
                freshness = 2
        except (ValueError, TypeError):
            pass
    breakdown["freshness"] = freshness
    score += freshness

    # 5. Size — smaller is easier to absorb (+0 to +10)
    size_kb = repo.get("size_kb", 0)
    if 0 < size_kb < 500:
        size_score = 10
    elif size_kb < 5000:
        size_score = 7
    elif size_kb < 20000:
        size_score = 3
    else:
        size_score = 0
    breakdown["size"] = size_score
    score += size_score

    # 6. Language — Python preferred (+10)
    lang = (repo.get("language") or "").lower()
    lang_score = 10 if lang == "python" else (5 if lang in ("javascript", "typescript", "rust", "go") else 0)
    breakdown["language"] = lang_score
    score += lang_score

    return min(100, max(0, score)), breakdown


def compute_model_hive_fit(model: dict) -> tuple[int, dict]:
    """Grade a HuggingFace model on Hive-Fit."""
    breakdown = {}
    score = 0

    # License
    license_key = (model.get("license") or "").lower().strip()
    normalized = license_key.replace(" ", "-")
    if any(rej in normalized for rej in ["gpl", "agpl", "sspl", "busl"]):
        breakdown["license"] = -100
        return -100, breakdown
    if any(acc in normalized for acc in ["mit", "apache", "bsd"]):
        breakdown["license"] = 30
        score += 30
    elif "gemma" in normalized or "llama" in normalized:
        # Community licenses — usable but restricted
        breakdown["license"] = 15
        score += 15
    else:
        breakdown["license"] = 0

    # Relevance — is this a model type we need?
    tags = " ".join(model.get("tags", [])).lower()
    model_id = model.get("model_id", "").lower()
    text = f"{model_id} {tags} {model.get('pipeline_tag', '')}"

    relevance = 0
    if "gemma" in text:
        relevance += 15
    if any(k in text for k in ["tts", "text-to-speech", "voice"]):
        relevance += 10
    if any(k in text for k in ["code", "coding", "instruct"]):
        relevance += 10
    if any(k in text for k in ["lora", "adapter", "fine-tune", "qlora"]):
        relevance += 10
    if any(k in text for k in ["function-calling", "tool-use"]):
        relevance += 10
    if any(k in text for k in ["small", "tiny", "1b", "2b", "3b"]):
        relevance += 5  # Small models we can run
    relevance = min(25, relevance)
    breakdown["relevance"] = relevance
    score += relevance

    # Popularity
    downloads = model.get("downloads", 0)
    likes = model.get("likes", 0)
    if downloads >= 10000 or likes >= 100:
        pop = 20
    elif downloads >= 1000 or likes >= 10:
        pop = 10
    elif downloads >= 100:
        pop = 5
    else:
        pop = 0
    breakdown["popularity"] = pop
    score += pop

    # Freshness
    last_mod = model.get("last_modified") or model.get("created_at", "")
    freshness = 0
    if last_mod:
        try:
            mod_dt = datetime.fromisoformat(last_mod.replace("Z", "+00:00"))
            age = datetime.now(timezone.utc) - mod_dt
            if age.days <= 7:
                freshness = 15
            elif age.days <= 30:
                freshness = 10
            elif age.days <= 90:
                freshness = 5
        except (ValueError, TypeError):
            pass
    breakdown["freshness"] = freshness
    score += freshness

    return min(100, max(0, score)), breakdown


# ---------------------------------------------------------------------------
# GitHub scanning
# ---------------------------------------------------------------------------

async def search_github_repos(client: httpx.AsyncClient, query: str, sort: str = "stars") -> list[dict]:
    """Search GitHub for repos matching query. Returns normalized repo dicts."""
    repos = []
    try:
        url = f"{GITHUB_API}/search/repositories"
        params = {
            "q": f"{query} language:python",
            "sort": sort,
            "order": "desc",
            "per_page": 10,
        }
        resp = await client.get(url, params=params, headers=_github_headers(), timeout=20)
        if resp.status_code == 200:
            data = resp.json()
            for item in data.get("items", []):
                license_name = ""
                if item.get("license") and isinstance(item["license"], dict):
                    license_name = item["license"].get("spdx_id", "") or item["license"].get("key", "")
                repos.append({
                    "id": f"gh-{item['id']}",
                    "source": "github",
                    "name": item["name"],
                    "full_name": item["full_name"],
                    "url": item["html_url"],
                    "description": (item.get("description") or "")[:500],
                    "license": license_name,
                    "language": item.get("language", ""),
                    "stars": item.get("stargazers_count", 0),
                    "forks": item.get("forks_count", 0),
                    "open_issues": item.get("open_issues_count", 0),
                    "created_at": item.get("created_at", ""),
                    "updated_at": item.get("updated_at", ""),
                    "pushed_at": item.get("pushed_at", ""),
                    "size_kb": item.get("size", 0),
                    "topics": ",".join(item.get("topics", [])),
                })
        elif resp.status_code == 403:
            log.warning("GitHub rate limit hit for query: %s", query)
        else:
            log.warning("GitHub search %s returned %d", query, resp.status_code)
    except Exception as e:
        log.error("GitHub search error for %s: %s", query, e)
    return repos


async def get_github_repo_info(client: httpx.AsyncClient, owner_repo: str) -> Optional[dict]:
    """Get repo info for a specific owner/repo."""
    try:
        resp = await client.get(
            f"{GITHUB_API}/repos/{owner_repo}",
            headers=_github_headers(),
            timeout=15
        )
        if resp.status_code == 200:
            item = resp.json()
            license_name = ""
            if item.get("license") and isinstance(item["license"], dict):
                license_name = item["license"].get("spdx_id", "") or item["license"].get("key", "")
            return {
                "stars": item.get("stargazers_count", 0),
                "forks": item.get("forks_count", 0),
                "open_issues": item.get("open_issues_count", 0),
                "watchers": item.get("subscribers_count", 0),
                "description": (item.get("description") or "")[:500],
                "language": item.get("language", ""),
                "url": item.get("html_url", ""),
            }
    except Exception as e:
        log.error("Failed to get repo info for %s: %s", owner_repo, e)
    return None


async def get_github_latest_release(client: httpx.AsyncClient, owner_repo: str) -> Optional[dict]:
    """Get the latest release for a repo."""
    try:
        resp = await client.get(
            f"{GITHUB_API}/repos/{owner_repo}/releases/latest",
            headers=_github_headers(),
            timeout=15
        )
        if resp.status_code == 200:
            data = resp.json()
            return {
                "tag": data.get("tag_name", ""),
                "name": data.get("name", ""),
                "published_at": data.get("published_at", ""),
            }
    except Exception:
        pass
    return None


async def search_github_trending(client: httpx.AsyncClient) -> list[dict]:
    """Search for recently created, trending AI repos."""
    since = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
    repos = []
    for query_suffix in ["AI agent", "LLM tool", "machine learning", "MCP server", "gemma"]:
        try:
            url = f"{GITHUB_API}/search/repositories"
            params = {
                "q": f"{query_suffix} created:>{since} language:python",
                "sort": "stars",
                "order": "desc",
                "per_page": 5,
            }
            resp = await client.get(url, params=params, headers=_github_headers(), timeout=20)
            if resp.status_code == 200:
                data = resp.json()
                for item in data.get("items", []):
                    license_name = ""
                    if item.get("license") and isinstance(item["license"], dict):
                        license_name = item["license"].get("spdx_id", "") or item["license"].get("key", "")
                    repos.append({
                        "id": f"gh-{item['id']}",
                        "source": "github_trending",
                        "name": item["name"],
                        "full_name": item["full_name"],
                        "url": item["html_url"],
                        "description": (item.get("description") or "")[:500],
                        "license": license_name,
                        "language": item.get("language", ""),
                        "stars": item.get("stargazers_count", 0),
                        "forks": item.get("forks_count", 0),
                        "open_issues": item.get("open_issues_count", 0),
                        "created_at": item.get("created_at", ""),
                        "updated_at": item.get("updated_at", ""),
                        "pushed_at": item.get("pushed_at", ""),
                        "size_kb": item.get("size", 0),
                        "topics": ",".join(item.get("topics", [])),
                    })
            elif resp.status_code == 403:
                log.warning("Rate limit on trending search")
                break
            # Be gentle with rate limits
            await asyncio.sleep(2)
        except Exception as e:
            log.error("Trending search error: %s", e)
    return repos


async def scan_awesome_lists(client: httpx.AsyncClient) -> list[dict]:
    """Scan awesome-lists for new tools/repos mentioned."""
    repos = []
    for awesome_repo in AWESOME_LISTS[:5]:  # Limit to avoid rate limits
        try:
            # Get the README content
            resp = await client.get(
                f"{GITHUB_API}/repos/{awesome_repo}/readme",
                headers=_github_headers(),
                timeout=15
            )
            if resp.status_code == 200:
                import base64
                data = resp.json()
                content = ""
                if data.get("content"):
                    try:
                        content = base64.b64decode(data["content"]).decode("utf-8", errors="ignore")
                    except Exception:
                        content = ""

                # Extract GitHub URLs from README
                gh_links = re.findall(r'https://github\.com/([\w\-\.]+/[\w\-\.]+)', content)
                seen = set()
                for full_name in gh_links[:20]:  # Limit per list
                    full_name = full_name.rstrip("/").rstrip(".")
                    if full_name in seen or "/" not in full_name:
                        continue
                    seen.add(full_name)
                    repos.append({
                        "id": f"awesome-{hashlib.md5(full_name.encode()).hexdigest()[:12]}",
                        "source": f"awesome:{awesome_repo}",
                        "name": full_name.split("/")[-1],
                        "full_name": full_name,
                        "url": f"https://github.com/{full_name}",
                        "description": "",
                        "license": "",
                        "language": "",
                        "stars": 0,
                        "forks": 0,
                        "open_issues": 0,
                        "created_at": "",
                        "updated_at": "",
                        "pushed_at": "",
                        "size_kb": 0,
                        "topics": "",
                    })
            await asyncio.sleep(3)  # Rate limit friendly
        except Exception as e:
            log.error("Awesome list scan error for %s: %s", awesome_repo, e)
    return repos


# ---------------------------------------------------------------------------
# HuggingFace scanning
# ---------------------------------------------------------------------------

async def search_huggingface_models(client: httpx.AsyncClient) -> list[dict]:
    """Search HuggingFace for new models we might want."""
    models = []
    for query in HF_MODEL_SEARCHES:
        try:
            resp = await client.get(
                f"{HUGGINGFACE_API}/models",
                params={
                    "search": query,
                    "sort": "downloads",
                    "direction": -1,
                    "limit": 10,
                },
                headers=_headers(),
                timeout=20,
            )
            if resp.status_code == 200:
                data = resp.json()
                for item in data:
                    model_id = item.get("modelId", item.get("id", ""))
                    if not model_id:
                        continue
                    mid = f"hf-{hashlib.md5(model_id.encode()).hexdigest()[:12]}"
                    author = model_id.split("/")[0] if "/" in model_id else ""
                    models.append({
                        "id": mid,
                        "model_id": model_id,
                        "author": author,
                        "url": f"https://huggingface.co/{model_id}",
                        "pipeline_tag": item.get("pipeline_tag", ""),
                        "tags": item.get("tags", []),
                        "downloads": item.get("downloads", 0),
                        "likes": item.get("likes", 0),
                        "library": item.get("library_name", ""),
                        "license": item.get("license", ""),
                        "created_at": item.get("createdAt", ""),
                        "last_modified": item.get("lastModified", ""),
                    })
            await asyncio.sleep(1)
        except Exception as e:
            log.error("HuggingFace search error for %s: %s", query, e)
    return models


# ---------------------------------------------------------------------------
# News discovery
# ---------------------------------------------------------------------------

async def discover_ai_news(client: httpx.AsyncClient) -> list[dict]:
    """
    Discover AI news from GitHub events and HuggingFace.
    Uses publicly available APIs only.
    """
    news = []

    # 1. GitHub — recent popular repos as proxy for news
    try:
        since = (datetime.now(timezone.utc) - timedelta(days=3)).strftime("%Y-%m-%d")
        resp = await client.get(
            f"{GITHUB_API}/search/repositories",
            params={
                "q": f"AI OR LLM OR GPT created:>{since}",
                "sort": "stars",
                "order": "desc",
                "per_page": 5,
            },
            headers=_github_headers(),
            timeout=20,
        )
        if resp.status_code == 200:
            for item in resp.json().get("items", []):
                nid = f"news-gh-{item['id']}"
                desc = (item.get("description") or "No description")[:300]
                news.append({
                    "id": nid,
                    "source": "github_trending",
                    "title": f"New AI repo: {item['full_name']} ({item.get('stargazers_count', 0)} stars)",
                    "url": item["html_url"],
                    "summary": desc,
                    "category": "new_repo",
                    "importance": min(10, 5 + item.get("stargazers_count", 0) // 500),
                })
    except Exception as e:
        log.error("News from GitHub failed: %s", e)

    # 2. HuggingFace — recently trending models
    try:
        resp = await client.get(
            f"{HUGGINGFACE_API}/models",
            params={"sort": "trending", "direction": -1, "limit": 10},
            headers=_headers(),
            timeout=20,
        )
        if resp.status_code == 200:
            for item in resp.json():
                model_id = item.get("modelId", "")
                if not model_id:
                    continue
                nid = f"news-hf-{hashlib.md5(model_id.encode()).hexdigest()[:12]}"
                news.append({
                    "id": nid,
                    "source": "huggingface_trending",
                    "title": f"Trending model: {model_id}",
                    "url": f"https://huggingface.co/{model_id}",
                    "summary": f"Pipeline: {item.get('pipeline_tag', 'N/A')}, "
                               f"Downloads: {item.get('downloads', 0):,}, "
                               f"Likes: {item.get('likes', 0)}",
                    "category": "trending_model",
                    "importance": min(10, 3 + item.get("likes", 0) // 50),
                })
    except Exception as e:
        log.error("News from HuggingFace failed: %s", e)

    # 3. GitHub events for key projects (releases, etc.)
    for name, repo_path in list(COMPETITORS.items())[:5]:
        try:
            release = await get_github_latest_release(client, repo_path)
            if release and release.get("published_at"):
                pub_dt = datetime.fromisoformat(release["published_at"].replace("Z", "+00:00"))
                if (datetime.now(timezone.utc) - pub_dt).days <= 7:
                    tag = release["tag"]
                    nid = f"news-rel-{hashlib.md5(f'{repo_path}{tag}'.encode()).hexdigest()[:12]}"
                    news.append({
                        "id": nid,
                        "source": "github_release",
                        "title": f"{name} released {release['tag']}: {release.get('name', '')}",
                        "url": f"https://github.com/{repo_path}/releases/tag/{release['tag']}",
                        "summary": f"{name} ({repo_path}) published new release {release['tag']}",
                        "category": "competitor_release",
                        "importance": 8,
                    })
            await asyncio.sleep(1)
        except Exception as e:
            log.error("Release check failed for %s: %s", name, e)

    return news


# ---------------------------------------------------------------------------
# Auto-absorb high-scoring repos
# ---------------------------------------------------------------------------

def absorb_repo(repo: dict) -> bool:
    """
    Clone a high-scoring repo into the discoveries sandbox.
    Returns True if successful.
    """
    if repo.get("absorbed"):
        return True

    full_name = repo.get("full_name", "")
    if not full_name:
        return False

    safe_name = full_name.replace("/", "__")
    target_dir = os.path.join(DISCOVERIES_DIR, safe_name)

    if os.path.exists(target_dir):
        log.info("Already absorbed: %s", full_name)
        return True

    try:
        log.info("Absorbing repo: %s -> %s", full_name, target_dir)
        result = subprocess.run(
            ["git", "clone", "--depth=1", f"https://github.com/{full_name}.git", target_dir],
            capture_output=True, text=True, timeout=120,
        )
        if result.returncode == 0:
            log.info("Successfully absorbed: %s", full_name)
            return True
        else:
            log.error("Clone failed for %s: %s", full_name, result.stderr[:200])
            return False
    except subprocess.TimeoutExpired:
        log.error("Clone timed out for %s", full_name)
        if os.path.exists(target_dir):
            shutil.rmtree(target_dir, ignore_errors=True)
        return False
    except Exception as e:
        log.error("Absorb error for %s: %s", full_name, e)
        return False


# ---------------------------------------------------------------------------
# Nerve integration
# ---------------------------------------------------------------------------

async def post_to_nerve(client: httpx.AsyncClient, fact: str, category: str = "ai_discovery"):
    """Post a discovery to the Hive nerve."""
    try:
        resp = await client.post(
            NERVE_URL,
            json={"fact": fact, "category": category, "source": "ai_tracker"},
            timeout=10,
        )
        if resp.status_code in (200, 201):
            log.info("Posted to nerve: %s", fact[:80])
        else:
            log.warning("Nerve post returned %d", resp.status_code)
    except Exception as e:
        log.debug("Nerve unreachable: %s", e)


# ---------------------------------------------------------------------------
# Main scan orchestrator
# ---------------------------------------------------------------------------

async def run_full_scan():
    """Execute a complete scan cycle."""
    scan_id = None
    with get_db() as db:
        cur = db.execute(
            "INSERT INTO scans (scan_type, status) VALUES ('full', 'running')"
        )
        scan_id = cur.lastrowid

    repos_found = 0
    models_found = 0
    news_found = 0
    absorbed = 0
    errors = []

    log.info("=== FULL SCAN STARTING ===")
    start = time.time()

    async with httpx.AsyncClient() as client:

        # ------ Phase 1: GitHub search ------
        log.info("Phase 1: GitHub AI search")
        all_repos = []
        # Pick a rotating subset of queries to stay under rate limits
        # Use 3 queries per scan + trending
        import random
        selected_queries = random.sample(AI_SEARCH_QUERIES, min(3, len(AI_SEARCH_QUERIES)))
        for query in selected_queries:
            repos = await search_github_repos(client, query)
            all_repos.extend(repos)
            await asyncio.sleep(3)  # Rate limit friendly

        # ------ Phase 2: GitHub trending ------
        log.info("Phase 2: GitHub trending")
        trending = await search_github_trending(client)
        all_repos.extend(trending)

        # ------ Phase 3: Awesome lists ------
        log.info("Phase 3: Awesome lists scan")
        awesome = await scan_awesome_lists(client)
        all_repos.extend(awesome)

        # ------ Phase 4: Score and store repos ------
        log.info("Phase 4: Scoring %d repos", len(all_repos))
        seen_ids = set()
        for repo in all_repos:
            rid = repo["id"]
            if rid in seen_ids:
                continue
            seen_ids.add(rid)

            score, breakdown = compute_hive_fit_score(repo)
            repo["hive_fit_score"] = score
            repo["score_breakdown"] = json.dumps(breakdown)

            # Determine relevance tags
            text = f"{repo.get('name', '')} {repo.get('description', '')} {repo.get('topics', '')}".lower()
            tags = []
            for need in HIVE_NEEDS:
                if need in text:
                    tags.append(need)
            repo["relevance_tags"] = ",".join(tags[:10])

            # Skip rejected (GPL etc.)
            if score < 0:
                continue

            repos_found += 1

            with get_db() as db:
                db.execute("""
                    INSERT INTO discoveries (
                        id, source, name, full_name, url, description, license,
                        language, stars, forks, open_issues, created_at, updated_at,
                        pushed_at, size_kb, topics, hive_fit_score, score_breakdown,
                        relevance_tags, last_checked
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                    ON CONFLICT(id) DO UPDATE SET
                        stars=excluded.stars, forks=excluded.forks,
                        open_issues=excluded.open_issues,
                        updated_at=excluded.updated_at, pushed_at=excluded.pushed_at,
                        hive_fit_score=excluded.hive_fit_score,
                        score_breakdown=excluded.score_breakdown,
                        relevance_tags=excluded.relevance_tags,
                        last_checked=datetime('now')
                """, (
                    rid, repo["source"], repo["name"], repo.get("full_name", ""),
                    repo["url"], repo.get("description", ""), repo.get("license", ""),
                    repo.get("language", ""), repo.get("stars", 0), repo.get("forks", 0),
                    repo.get("open_issues", 0), repo.get("created_at", ""),
                    repo.get("updated_at", ""), repo.get("pushed_at", ""),
                    repo.get("size_kb", 0), repo.get("topics", ""),
                    score, repo["score_breakdown"], repo["relevance_tags"],
                ))

            # Auto-absorb high scores
            if score >= 80 and repo.get("full_name"):
                if absorb_repo(repo):
                    absorbed += 1
                    with get_db() as db:
                        db.execute("""
                            UPDATE discoveries SET absorbed=1, absorbed_at=datetime('now'),
                            local_path=?, status='absorbed'
                            WHERE id=?
                        """, (
                            os.path.join(DISCOVERIES_DIR, repo["full_name"].replace("/", "__")),
                            rid,
                        ))
                    # Notify nerve
                    await post_to_nerve(
                        client,
                        f"Absorbed high-fit repo: {repo['full_name']} "
                        f"(score={score}, stars={repo.get('stars', 0)}) — {repo.get('description', '')[:100]}",
                        category="ai_discovery",
                    )

        # ------ Phase 5: Competitor tracking ------
        log.info("Phase 5: Competitor tracking (%d competitors)", len(COMPETITORS))
        for name, repo_path in COMPETITORS.items():
            info = await get_github_repo_info(client, repo_path)
            if info:
                comp_id = hashlib.md5(repo_path.encode()).hexdigest()[:16]

                # Get latest release
                release = await get_github_latest_release(client, repo_path)
                release_tag = release["tag"] if release else ""
                release_date = release["published_at"] if release else ""

                # Load existing star history
                existing_history = "[]"
                with get_db() as db:
                    row = db.execute("SELECT star_history FROM competitors WHERE id=?", (comp_id,)).fetchone()
                    if row and row["star_history"]:
                        existing_history = row["star_history"]

                try:
                    history = json.loads(existing_history)
                except (json.JSONDecodeError, TypeError):
                    history = []

                # Append current star count
                now_str = datetime.now(timezone.utc).isoformat()
                history.append({"date": now_str, "stars": info["stars"]})
                # Keep last 500 data points
                history = history[-500:]

                with get_db() as db:
                    db.execute("""
                        INSERT INTO competitors (
                            id, name, repo, url, stars, forks, open_issues,
                            watchers, description, language, last_release,
                            last_release_date, star_history, checked_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                        ON CONFLICT(id) DO UPDATE SET
                            stars=excluded.stars, forks=excluded.forks,
                            open_issues=excluded.open_issues,
                            watchers=excluded.watchers,
                            description=excluded.description,
                            last_release=excluded.last_release,
                            last_release_date=excluded.last_release_date,
                            star_history=excluded.star_history,
                            checked_at=datetime('now')
                    """, (
                        comp_id, name, repo_path, info["url"],
                        info["stars"], info["forks"], info["open_issues"],
                        info["watchers"], info.get("description", ""),
                        info.get("language", ""), release_tag, release_date,
                        json.dumps(history),
                    ))
            await asyncio.sleep(2)

        # ------ Phase 6: HuggingFace models ------
        log.info("Phase 6: HuggingFace model search")
        hf_models = await search_huggingface_models(client)
        for model in hf_models:
            score, breakdown = compute_model_hive_fit(model)
            if score < 0:
                continue

            model["hive_fit_score"] = score
            models_found += 1

            # Relevance tags
            text = f"{model['model_id']} {' '.join(model.get('tags', []))} {model.get('pipeline_tag', '')}".lower()
            rel_tags = [n for n in HIVE_NEEDS if n in text]

            with get_db() as db:
                db.execute("""
                    INSERT INTO models (
                        id, model_id, author, url, pipeline_tag, tags,
                        downloads, likes, library, license, created_at,
                        last_modified, hive_fit_score, relevance_tags
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        downloads=excluded.downloads, likes=excluded.likes,
                        hive_fit_score=excluded.hive_fit_score,
                        last_modified=excluded.last_modified
                """, (
                    model["id"], model["model_id"], model.get("author", ""),
                    model["url"], model.get("pipeline_tag", ""),
                    json.dumps(model.get("tags", [])),
                    model.get("downloads", 0), model.get("likes", 0),
                    model.get("library", ""), model.get("license", ""),
                    model.get("created_at", ""), model.get("last_modified", ""),
                    score, ",".join(rel_tags[:10]),
                ))

            # Notify nerve about high-score models
            if score >= 70:
                await post_to_nerve(
                    client,
                    f"High-fit HF model: {model['model_id']} "
                    f"(score={score}, downloads={model.get('downloads', 0):,}) — "
                    f"{model.get('pipeline_tag', 'N/A')}",
                    category="ai_model_discovery",
                )

        # ------ Phase 7: AI News ------
        log.info("Phase 7: AI news discovery")
        news_items = await discover_ai_news(client)
        for item in news_items:
            news_found += 1
            with get_db() as db:
                db.execute("""
                    INSERT OR IGNORE INTO news (id, source, title, url, summary, category, importance)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    item["id"], item["source"], item["title"], item.get("url", ""),
                    item.get("summary", ""), item.get("category", ""),
                    item.get("importance", 5),
                ))

    # ------ Finalize scan ------
    elapsed = time.time() - start
    log.info(
        "=== SCAN COMPLETE in %.1fs: %d repos, %d models, %d news, %d absorbed ===",
        elapsed, repos_found, models_found, news_found, absorbed,
    )

    if scan_id:
        with get_db() as db:
            db.execute("""
                UPDATE scans SET
                    finished_at=datetime('now'),
                    repos_found=?, models_found=?, news_found=?,
                    absorbed=?, errors=?, status='complete'
                WHERE id=?
            """, (repos_found, models_found, news_found, absorbed,
                  json.dumps(errors) if errors else None, scan_id))

    return {
        "repos_found": repos_found,
        "models_found": models_found,
        "news_found": news_found,
        "absorbed": absorbed,
        "elapsed_seconds": round(elapsed, 1),
    }


# ---------------------------------------------------------------------------
# Background scan loop
# ---------------------------------------------------------------------------

_scan_running = False
_last_scan_result: Optional[dict] = None
_last_scan_time: Optional[str] = None


async def scan_loop():
    """Background loop that runs a full scan every SCAN_INTERVAL seconds."""
    global _scan_running, _last_scan_result, _last_scan_time
    # Initial delay — let the server start
    await asyncio.sleep(10)

    while True:
        try:
            _scan_running = True
            log.info("Starting scheduled scan cycle...")
            result = await run_full_scan()
            _last_scan_result = result
            _last_scan_time = datetime.now(timezone.utc).isoformat()
            _scan_running = False
        except Exception as e:
            log.error("Scan cycle failed: %s\n%s", e, traceback.format_exc())
            _scan_running = False

        await asyncio.sleep(SCAN_INTERVAL)


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    task = asyncio.create_task(scan_loop())
    log.info("AI Tracker started on port %d — scanning every %d seconds", PORT, SCAN_INTERVAL)
    yield
    task.cancel()


app = FastAPI(
    title="THE HIVE — AI Industry Tracker",
    description="Autonomous AI industry scanner, competitor tracker, and discovery engine",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# API Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "service": "ai-tracker",
        "port": PORT,
        "scan_running": _scan_running,
        "last_scan": _last_scan_time,
        "last_scan_result": _last_scan_result,
        "uptime": "ok",
    }


@app.get("/api/trending")
async def api_trending(limit: int = Query(default=30, le=100)):
    """Get trending AI repos discovered recently."""
    with get_db() as db:
        rows = db.execute("""
            SELECT * FROM discoveries
            WHERE source IN ('github_trending', 'github')
            ORDER BY found_at DESC, hive_fit_score DESC
            LIMIT ?
        """, (limit,)).fetchall()
    return {"count": len(rows), "trending": [dict(r) for r in rows]}


@app.get("/api/discoveries")
async def api_discoveries(
    min_score: int = Query(default=0, ge=0, le=100),
    limit: int = Query(default=50, le=200),
    source: Optional[str] = None,
    absorbed_only: bool = False,
):
    """Get high-scoring discoveries."""
    query = "SELECT * FROM discoveries WHERE hive_fit_score >= ?"
    params: list = [min_score]
    if source:
        query += " AND source = ?"
        params.append(source)
    if absorbed_only:
        query += " AND absorbed = 1"
    query += " ORDER BY hive_fit_score DESC, stars DESC LIMIT ?"
    params.append(limit)

    with get_db() as db:
        rows = db.execute(query, params).fetchall()
    return {"count": len(rows), "discoveries": [dict(r) for r in rows]}


@app.get("/api/competitors")
async def api_competitors():
    """Get competitor tracking data."""
    with get_db() as db:
        rows = db.execute("""
            SELECT id, name, repo, url, stars, forks, open_issues, watchers,
                   description, language, last_release, last_release_date, checked_at
            FROM competitors
            ORDER BY stars DESC
        """).fetchall()
    competitors = []
    for r in rows:
        d = dict(r)
        # Also get star history for sparkline
        with get_db() as db:
            hist_row = db.execute("SELECT star_history FROM competitors WHERE id=?", (d["id"],)).fetchone()
            if hist_row and hist_row["star_history"]:
                try:
                    history = json.loads(hist_row["star_history"])
                    d["star_trend"] = [h["stars"] for h in history[-20:]]
                except (json.JSONDecodeError, TypeError):
                    d["star_trend"] = []
            else:
                d["star_trend"] = []
        competitors.append(d)
    return {"count": len(competitors), "competitors": competitors}


@app.get("/api/news")
async def api_news(limit: int = Query(default=30, le=100), category: Optional[str] = None):
    """Get AI industry news."""
    query = "SELECT * FROM news"
    params: list = []
    if category:
        query += " WHERE category = ?"
        params.append(category)
    query += " ORDER BY found_at DESC, importance DESC LIMIT ?"
    params.append(limit)

    with get_db() as db:
        rows = db.execute(query, params).fetchall()
    return {"count": len(rows), "news": [dict(r) for r in rows]}


@app.get("/api/free-tiers")
async def api_free_tiers():
    """Get tracked free compute/API sources."""
    with get_db() as db:
        rows = db.execute("SELECT * FROM free_tiers ORDER BY name").fetchall()
    return {"count": len(rows), "free_tiers": [dict(r) for r in rows]}


@app.get("/api/models")
async def api_models(
    min_score: int = Query(default=0, ge=0, le=100),
    limit: int = Query(default=50, le=200),
    pipeline: Optional[str] = None,
):
    """Get discovered HuggingFace models."""
    query = "SELECT * FROM models WHERE hive_fit_score >= ?"
    params: list = [min_score]
    if pipeline:
        query += " AND pipeline_tag = ?"
        params.append(pipeline)
    query += " ORDER BY hive_fit_score DESC, downloads DESC LIMIT ?"
    params.append(limit)

    with get_db() as db:
        rows = db.execute(query, params).fetchall()
    return {"count": len(rows), "models": [dict(r) for r in rows]}


@app.post("/api/scan")
async def api_scan_trigger():
    """Manually trigger a full scan."""
    global _scan_running
    if _scan_running:
        return {"status": "already_running", "message": "A scan is already in progress"}

    # Run in background
    asyncio.create_task(_manual_scan())
    return {"status": "started", "message": "Full scan triggered"}


async def _manual_scan():
    global _scan_running, _last_scan_result, _last_scan_time
    try:
        _scan_running = True
        result = await run_full_scan()
        _last_scan_result = result
        _last_scan_time = datetime.now(timezone.utc).isoformat()
    except Exception as e:
        log.error("Manual scan failed: %s", e)
    finally:
        _scan_running = False


@app.get("/api/stats")
async def api_stats():
    """Get overall tracker statistics."""
    with get_db() as db:
        total_discoveries = db.execute("SELECT COUNT(*) as c FROM discoveries").fetchone()["c"]
        high_score = db.execute("SELECT COUNT(*) as c FROM discoveries WHERE hive_fit_score >= 80").fetchone()["c"]
        absorbed_count = db.execute("SELECT COUNT(*) as c FROM discoveries WHERE absorbed = 1").fetchone()["c"]
        total_models = db.execute("SELECT COUNT(*) as c FROM models").fetchone()["c"]
        total_news = db.execute("SELECT COUNT(*) as c FROM news").fetchone()["c"]
        total_competitors = db.execute("SELECT COUNT(*) as c FROM competitors").fetchone()["c"]
        total_scans = db.execute("SELECT COUNT(*) as c FROM scans").fetchone()["c"]
        last_scan = db.execute("SELECT * FROM scans ORDER BY started_at DESC LIMIT 1").fetchone()
        top_discovery = db.execute(
            "SELECT name, full_name, hive_fit_score, stars FROM discoveries ORDER BY hive_fit_score DESC LIMIT 1"
        ).fetchone()
        top_model = db.execute(
            "SELECT model_id, hive_fit_score, downloads FROM models ORDER BY hive_fit_score DESC LIMIT 1"
        ).fetchone()

    return {
        "total_discoveries": total_discoveries,
        "high_score_discoveries": high_score,
        "absorbed": absorbed_count,
        "total_models": total_models,
        "total_news": total_news,
        "competitors_tracked": total_competitors,
        "total_scans": total_scans,
        "last_scan": dict(last_scan) if last_scan else None,
        "top_discovery": dict(top_discovery) if top_discovery else None,
        "top_model": dict(top_model) if top_model else None,
        "scan_running": _scan_running,
        "last_scan_time": _last_scan_time,
    }


@app.get("/api/scans")
async def api_scans(limit: int = Query(default=20, le=50)):
    """Get scan history."""
    with get_db() as db:
        rows = db.execute("SELECT * FROM scans ORDER BY started_at DESC LIMIT ?", (limit,)).fetchall()
    return {"count": len(rows), "scans": [dict(r) for r in rows]}


# ---------------------------------------------------------------------------
# HTML Dashboard
# ---------------------------------------------------------------------------

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>THE HIVE - AI Industry Tracker</title>
<style>
:root {
    --bg: #0a0e17; --bg2: #111827; --bg3: #1a2332;
    --accent: #00ff88; --accent2: #00ccff; --warn: #ff6600; --danger: #ff3366;
    --text: #e0e7ff; --muted: #7a8ba7; --border: #1e2d42;
}
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: 'Segoe UI', system-ui, sans-serif; background: var(--bg); color: var(--text); }
.header {
    background: linear-gradient(135deg, var(--bg2), var(--bg3));
    border-bottom: 2px solid var(--accent);
    padding: 20px 30px; display: flex; align-items: center; justify-content: space-between;
}
.header h1 { font-size: 1.6em; color: var(--accent); }
.header h1 span { color: var(--accent2); font-size: 0.6em; }
.header .status { display: flex; gap: 15px; align-items: center; }
.status-dot { width: 10px; height: 10px; border-radius: 50%; display: inline-block; }
.status-dot.active { background: var(--accent); animation: pulse 2s infinite; }
.status-dot.idle { background: var(--muted); }
@keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.4; } }
.container { max-width: 1600px; margin: 0 auto; padding: 20px; }
.stats-grid {
    display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 15px; margin-bottom: 25px;
}
.stat-card {
    background: var(--bg2); border: 1px solid var(--border); border-radius: 10px;
    padding: 18px; text-align: center;
}
.stat-card .number { font-size: 2em; font-weight: 700; color: var(--accent); }
.stat-card .label { color: var(--muted); font-size: 0.85em; margin-top: 5px; }
.tabs {
    display: flex; gap: 5px; margin-bottom: 20px; flex-wrap: wrap;
    border-bottom: 2px solid var(--border); padding-bottom: 10px;
}
.tab {
    padding: 8px 18px; background: var(--bg2); border: 1px solid var(--border);
    border-radius: 8px 8px 0 0; cursor: pointer; color: var(--muted);
    transition: all 0.2s; font-size: 0.9em;
}
.tab:hover { color: var(--text); border-color: var(--accent2); }
.tab.active { color: var(--accent); border-color: var(--accent); background: var(--bg3); }
.panel { display: none; }
.panel.active { display: block; }
table {
    width: 100%; border-collapse: collapse; background: var(--bg2);
    border: 1px solid var(--border); border-radius: 10px; overflow: hidden;
}
th { background: var(--bg3); color: var(--accent2); padding: 12px 15px; text-align: left; font-size: 0.85em; }
td { padding: 10px 15px; border-top: 1px solid var(--border); font-size: 0.9em; }
tr:hover td { background: var(--bg3); }
.score {
    display: inline-block; padding: 3px 10px; border-radius: 12px;
    font-weight: 700; font-size: 0.85em;
}
.score.high { background: rgba(0,255,136,0.2); color: var(--accent); }
.score.mid { background: rgba(0,204,255,0.2); color: var(--accent2); }
.score.low { background: rgba(255,102,0,0.2); color: var(--warn); }
.score.reject { background: rgba(255,51,102,0.2); color: var(--danger); }
a { color: var(--accent2); text-decoration: none; }
a:hover { text-decoration: underline; color: var(--accent); }
.badge {
    display: inline-block; padding: 2px 8px; border-radius: 10px;
    font-size: 0.75em; margin: 1px;
}
.badge.absorbed { background: rgba(0,255,136,0.2); color: var(--accent); }
.badge.discovered { background: rgba(0,204,255,0.15); color: var(--accent2); }
.badge.tag { background: rgba(122,139,167,0.2); color: var(--muted); }
.btn {
    padding: 10px 24px; background: var(--accent); color: var(--bg); border: none;
    border-radius: 8px; cursor: pointer; font-weight: 700; font-size: 0.9em;
    transition: all 0.2s;
}
.btn:hover { background: #33ffa0; transform: translateY(-1px); }
.btn:disabled { opacity: 0.5; cursor: not-allowed; }
.toolbar { display: flex; gap: 10px; align-items: center; margin-bottom: 15px; flex-wrap: wrap; }
.filter-input {
    padding: 8px 14px; background: var(--bg3); border: 1px solid var(--border);
    border-radius: 8px; color: var(--text); font-size: 0.9em; min-width: 120px;
}
.competitor-card {
    background: var(--bg2); border: 1px solid var(--border); border-radius: 10px;
    padding: 18px; margin-bottom: 12px; display: flex; justify-content: space-between;
    align-items: center; flex-wrap: wrap; gap: 10px;
}
.competitor-card h3 { color: var(--accent2); margin-bottom: 4px; }
.competitor-card .stars { font-size: 1.4em; font-weight: 700; color: var(--accent); }
.competitor-card .meta { color: var(--muted); font-size: 0.85em; }
.sparkline { display: inline-block; height: 30px; }
.news-item {
    background: var(--bg2); border: 1px solid var(--border); border-radius: 8px;
    padding: 14px 18px; margin-bottom: 8px;
}
.news-item .title { font-weight: 600; margin-bottom: 4px; }
.news-item .meta { color: var(--muted); font-size: 0.8em; }
.importance {
    display: inline-block; padding: 2px 8px; border-radius: 10px;
    font-size: 0.75em; font-weight: 700;
}
.importance.high { background: rgba(255,51,102,0.2); color: var(--danger); }
.importance.mid { background: rgba(255,102,0,0.2); color: var(--warn); }
.importance.low { background: rgba(0,204,255,0.15); color: var(--accent2); }
.free-tier-card {
    background: var(--bg2); border: 1px solid var(--border); border-radius: 10px;
    padding: 16px; display: flex; justify-content: space-between; align-items: center;
    margin-bottom: 8px; flex-wrap: wrap; gap: 8px;
}
.free-tier-card h4 { color: var(--accent2); }
.free-tier-card .type { color: var(--accent); font-size: 0.85em; }
#loading { text-align: center; padding: 40px; color: var(--muted); }
@media (max-width: 768px) {
    .stats-grid { grid-template-columns: repeat(2, 1fr); }
    .header { flex-direction: column; gap: 10px; }
}
</style>
</head>
<body>

<div class="header">
    <h1>THE HIVE &mdash; AI Industry Tracker <span>v1.0 | Port 8917</span></h1>
    <div class="status">
        <span id="scan-status"></span>
        <button class="btn" id="scan-btn" onclick="triggerScan()">Run Scan</button>
    </div>
</div>

<div class="container">
    <div class="stats-grid" id="stats-grid">
        <div class="stat-card"><div class="number" id="st-disc">--</div><div class="label">Discoveries</div></div>
        <div class="stat-card"><div class="number" id="st-high">--</div><div class="label">High Score (80+)</div></div>
        <div class="stat-card"><div class="number" id="st-absorbed">--</div><div class="label">Absorbed</div></div>
        <div class="stat-card"><div class="number" id="st-models">--</div><div class="label">Models</div></div>
        <div class="stat-card"><div class="number" id="st-news">--</div><div class="label">News Items</div></div>
        <div class="stat-card"><div class="number" id="st-comp">--</div><div class="label">Competitors</div></div>
        <div class="stat-card"><div class="number" id="st-scans">--</div><div class="label">Scans Run</div></div>
    </div>

    <div class="tabs">
        <div class="tab active" data-tab="discoveries">Discoveries</div>
        <div class="tab" data-tab="competitors">Competitors</div>
        <div class="tab" data-tab="models">Models</div>
        <div class="tab" data-tab="news">AI News</div>
        <div class="tab" data-tab="free-tiers">Free Tiers</div>
        <div class="tab" data-tab="scans">Scan History</div>
    </div>

    <!-- Discoveries -->
    <div class="panel active" id="panel-discoveries">
        <div class="toolbar">
            <label style="color:var(--muted)">Min Score:</label>
            <input type="number" class="filter-input" id="min-score" value="0" min="0" max="100" style="width:80px">
            <label style="color:var(--muted)">Absorbed only:</label>
            <input type="checkbox" id="absorbed-only" style="width:18px;height:18px">
            <button class="btn" onclick="loadDiscoveries()" style="padding:6px 14px">Filter</button>
        </div>
        <table>
            <thead><tr>
                <th>Score</th><th>Name</th><th>Description</th><th>Stars</th><th>License</th>
                <th>Language</th><th>Tags</th><th>Status</th><th>Found</th>
            </tr></thead>
            <tbody id="disc-body"><tr><td colspan="9" id="loading">Loading...</td></tr></tbody>
        </table>
    </div>

    <!-- Competitors -->
    <div class="panel" id="panel-competitors">
        <div id="comp-list"></div>
    </div>

    <!-- Models -->
    <div class="panel" id="panel-models">
        <table>
            <thead><tr>
                <th>Score</th><th>Model ID</th><th>Pipeline</th><th>Downloads</th>
                <th>Likes</th><th>License</th><th>Library</th><th>Tags</th>
            </tr></thead>
            <tbody id="models-body"><tr><td colspan="8">Loading...</td></tr></tbody>
        </table>
    </div>

    <!-- News -->
    <div class="panel" id="panel-news">
        <div id="news-list"></div>
    </div>

    <!-- Free Tiers -->
    <div class="panel" id="panel-free-tiers">
        <div id="ft-list"></div>
    </div>

    <!-- Scans -->
    <div class="panel" id="panel-scans">
        <table>
            <thead><tr>
                <th>ID</th><th>Type</th><th>Started</th><th>Finished</th>
                <th>Repos</th><th>Models</th><th>News</th><th>Absorbed</th><th>Status</th>
            </tr></thead>
            <tbody id="scans-body"><tr><td colspan="9">Loading...</td></tr></tbody>
        </table>
    </div>
</div>

<script>
const API = '';

function scoreClass(s) {
    if (s >= 80) return 'high';
    if (s >= 50) return 'mid';
    if (s >= 20) return 'low';
    return 'reject';
}

function importanceClass(i) {
    if (i >= 8) return 'high';
    if (i >= 5) return 'mid';
    return 'low';
}

function formatNum(n) {
    if (n >= 1000000) return (n/1000000).toFixed(1) + 'M';
    if (n >= 1000) return (n/1000).toFixed(1) + 'K';
    return n.toString();
}

async function loadStats() {
    try {
        const r = await fetch(API + '/api/stats');
        const d = await r.json();
        document.getElementById('st-disc').textContent = d.total_discoveries;
        document.getElementById('st-high').textContent = d.high_score_discoveries;
        document.getElementById('st-absorbed').textContent = d.absorbed;
        document.getElementById('st-models').textContent = d.total_models;
        document.getElementById('st-news').textContent = d.total_news;
        document.getElementById('st-comp').textContent = d.competitors_tracked;
        document.getElementById('st-scans').textContent = d.total_scans;

        const statusEl = document.getElementById('scan-status');
        if (d.scan_running) {
            statusEl.innerHTML = '<span class="status-dot active"></span> Scanning...';
            document.getElementById('scan-btn').disabled = true;
        } else {
            statusEl.innerHTML = '<span class="status-dot idle"></span> Idle' +
                (d.last_scan_time ? ' (last: ' + new Date(d.last_scan_time).toLocaleString() + ')' : '');
            document.getElementById('scan-btn').disabled = false;
        }
    } catch(e) { console.error('Stats error:', e); }
}

async function loadDiscoveries() {
    const minScore = document.getElementById('min-score').value || 0;
    const absorbed = document.getElementById('absorbed-only').checked;
    try {
        const url = `${API}/api/discoveries?min_score=${minScore}&limit=100&absorbed_only=${absorbed}`;
        const r = await fetch(url);
        const d = await r.json();
        const tbody = document.getElementById('disc-body');
        if (!d.discoveries.length) { tbody.innerHTML = '<tr><td colspan="9" style="text-align:center;color:var(--muted)">No discoveries yet. Run a scan!</td></tr>'; return; }
        tbody.innerHTML = d.discoveries.map(item => {
            const tags = (item.relevance_tags || '').split(',').filter(Boolean).slice(0,4)
                .map(t => `<span class="badge tag">${t}</span>`).join('');
            const status = item.absorbed ?
                '<span class="badge absorbed">ABSORBED</span>' :
                '<span class="badge discovered">discovered</span>';
            return `<tr>
                <td><span class="score ${scoreClass(item.hive_fit_score)}">${item.hive_fit_score}</span></td>
                <td><a href="${item.url}" target="_blank">${item.full_name || item.name}</a></td>
                <td style="max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${item.description || '-'}</td>
                <td>${formatNum(item.stars)}</td>
                <td>${item.license || '?'}</td>
                <td>${item.language || '-'}</td>
                <td>${tags}</td>
                <td>${status}</td>
                <td style="white-space:nowrap">${(item.found_at || '').slice(0,10)}</td>
            </tr>`;
        }).join('');
    } catch(e) { console.error('Discoveries error:', e); }
}

async function loadCompetitors() {
    try {
        const r = await fetch(API + '/api/competitors');
        const d = await r.json();
        const el = document.getElementById('comp-list');
        if (!d.competitors.length) { el.innerHTML = '<p style="color:var(--muted)">No competitors tracked yet.</p>'; return; }
        el.innerHTML = d.competitors.map(c => {
            const trend = (c.star_trend || []);
            let sparkSvg = '';
            if (trend.length > 1) {
                const min = Math.min(...trend), max = Math.max(...trend);
                const range = max - min || 1;
                const w = 120, h = 30;
                const pts = trend.map((v,i) =>
                    `${(i/(trend.length-1))*w},${h - ((v-min)/range)*h}`
                ).join(' ');
                sparkSvg = `<svg width="${w}" height="${h}" class="sparkline">
                    <polyline points="${pts}" fill="none" stroke="var(--accent)" stroke-width="1.5"/>
                </svg>`;
            }
            return `<div class="competitor-card">
                <div>
                    <h3><a href="${c.url}" target="_blank">${c.name}</a></h3>
                    <div class="meta">${c.repo} &bull; ${c.language || '-'} &bull;
                        ${c.last_release ? 'Latest: ' + c.last_release : 'No releases tracked'}</div>
                    <div class="meta" style="margin-top:4px">${c.description || ''}</div>
                </div>
                <div style="text-align:right">
                    <div class="stars">${formatNum(c.stars)} &#9733;</div>
                    <div class="meta">${formatNum(c.forks)} forks &bull; ${formatNum(c.open_issues)} issues</div>
                    ${sparkSvg}
                </div>
            </div>`;
        }).join('');
    } catch(e) { console.error('Competitors error:', e); }
}

async function loadModels() {
    try {
        const r = await fetch(API + '/api/models?limit=100');
        const d = await r.json();
        const tbody = document.getElementById('models-body');
        if (!d.models.length) { tbody.innerHTML = '<tr><td colspan="8" style="text-align:center;color:var(--muted)">No models yet.</td></tr>'; return; }
        tbody.innerHTML = d.models.map(m => {
            let tags = [];
            try { tags = JSON.parse(m.tags || '[]'); } catch(e) {}
            const tagHtml = tags.slice(0,3).map(t => `<span class="badge tag">${t}</span>`).join('');
            return `<tr>
                <td><span class="score ${scoreClass(m.hive_fit_score)}">${m.hive_fit_score}</span></td>
                <td><a href="${m.url}" target="_blank">${m.model_id}</a></td>
                <td>${m.pipeline_tag || '-'}</td>
                <td>${formatNum(m.downloads)}</td>
                <td>${m.likes}</td>
                <td>${m.license || '?'}</td>
                <td>${m.library || '-'}</td>
                <td>${tagHtml}</td>
            </tr>`;
        }).join('');
    } catch(e) { console.error('Models error:', e); }
}

async function loadNews() {
    try {
        const r = await fetch(API + '/api/news?limit=50');
        const d = await r.json();
        const el = document.getElementById('news-list');
        if (!d.news.length) { el.innerHTML = '<p style="color:var(--muted)">No news yet.</p>'; return; }
        el.innerHTML = d.news.map(n => `<div class="news-item">
            <div class="title">
                <span class="importance ${importanceClass(n.importance)}">${n.importance}/10</span>
                <a href="${n.url}" target="_blank">${n.title}</a>
            </div>
            <div class="meta">${n.source} &bull; ${n.category} &bull; ${(n.found_at || '').slice(0,16)}</div>
            ${n.summary ? '<div style="margin-top:6px;font-size:0.85em;color:var(--muted)">' + n.summary + '</div>' : ''}
        </div>`).join('');
    } catch(e) { console.error('News error:', e); }
}

async function loadFreeTiers() {
    try {
        const r = await fetch(API + '/api/free-tiers');
        const d = await r.json();
        const el = document.getElementById('ft-list');
        el.innerHTML = d.free_tiers.map(ft => `<div class="free-tier-card">
            <div>
                <h4><a href="${ft.url}" target="_blank">${ft.name}</a></h4>
                <div class="meta">${ft.limits || ''}</div>
            </div>
            <div style="text-align:right">
                <div class="type">${ft.tier_type}</div>
                <div class="meta">${ft.gpu || 'N/A'}</div>
            </div>
        </div>`).join('');
    } catch(e) { console.error('Free tiers error:', e); }
}

async function loadScans() {
    try {
        const r = await fetch(API + '/api/scans');
        const d = await r.json();
        const tbody = document.getElementById('scans-body');
        if (!d.scans.length) { tbody.innerHTML = '<tr><td colspan="9" style="text-align:center;color:var(--muted)">No scans yet.</td></tr>'; return; }
        tbody.innerHTML = d.scans.map(s => `<tr>
            <td>${s.id}</td>
            <td>${s.scan_type}</td>
            <td style="white-space:nowrap">${(s.started_at || '').slice(0,19)}</td>
            <td style="white-space:nowrap">${(s.finished_at || '-').slice(0,19)}</td>
            <td>${s.repos_found}</td>
            <td>${s.models_found}</td>
            <td>${s.news_found}</td>
            <td>${s.absorbed}</td>
            <td><span class="badge ${s.status === 'complete' ? 'absorbed' : 'discovered'}">${s.status}</span></td>
        </tr>`).join('');
    } catch(e) { console.error('Scans error:', e); }
}

async function triggerScan() {
    document.getElementById('scan-btn').disabled = true;
    try {
        await fetch(API + '/api/scan', { method: 'POST' });
        document.getElementById('scan-status').innerHTML = '<span class="status-dot active"></span> Scanning...';
        // Poll for completion
        const poll = setInterval(async () => {
            const r = await fetch(API + '/api/stats');
            const d = await r.json();
            if (!d.scan_running) {
                clearInterval(poll);
                loadAll();
            }
        }, 5000);
    } catch(e) { console.error('Trigger error:', e); document.getElementById('scan-btn').disabled = false; }
}

// Tab switching
document.querySelectorAll('.tab').forEach(tab => {
    tab.addEventListener('click', () => {
        document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
        document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
        tab.classList.add('active');
        document.getElementById('panel-' + tab.dataset.tab).classList.add('active');
    });
});

function loadAll() {
    loadStats();
    loadDiscoveries();
    loadCompetitors();
    loadModels();
    loadNews();
    loadFreeTiers();
    loadScans();
}

// Auto-refresh every 30 seconds
loadAll();
setInterval(loadStats, 30000);
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return HTMLResponse(content=DASHBOARD_HTML)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
