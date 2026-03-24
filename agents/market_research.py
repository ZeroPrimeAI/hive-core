#!/usr/bin/env python3
"""
THE HIVE — Market Research Agent
Port 8915 | SQLite at /home/zero/hivecode_sandbox/market_research.db
MIT License

Autonomous market research engine that finds:
  - Pain points: what people wish existed (Reddit, HN, Twitter)
  - Trending products: what's getting traction (Product Hunt, HN Show, indie)
  - Revenue signals: what's actually making money (SaaS, Fiverr, Gumroad, Chrome)
  - Opportunities: software we could build, scored and ranked
  - Quick wins: things buildable in < 1 day with our stack
  - Strategy: product recommendations matched to Hive capabilities

Runs research cycles every 4 hours. No paid APIs — scrapes public data via
httpx + DuckDuckGo HTML search as backbone.
"""

import json
import sqlite3
import time
import threading
import os
import re
import hashlib
import traceback
import html as html_lib
import random
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
PORT = 8915
DB_PATH = "/home/zero/hivecode_sandbox/market_research.db"

OLLAMA_URL = "http://localhost:11434"
OLLAMA_MODEL = "gemma2:2b"

SCAN_INTERVAL_HOURS = 4
MAX_RESULTS_PER_QUERY = 15

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# ==========================================================================
# HIVE CAPABILITIES — what we can actually build
# ==========================================================================
HIVE_CAPABILITIES = {
    "ai_inference": {
        "name": "AI Inference (local GPUs)",
        "description": "23+ fine-tuned gemma2 models, vLLM multi-LoRA, cloud brains (qwen3:14b, qwen2.5-coder:32b)",
        "strength": 95,
    },
    "web_scraping": {
        "name": "Web Scraping & Data Pipeline",
        "description": "httpx, Playwright, automated scraping infrastructure, Cloudflare deployment",
        "strength": 90,
    },
    "content_production": {
        "name": "AI Content Production",
        "description": "Video generation, TTS (5 engines), anime production, podcast, YouTube automation",
        "strength": 85,
    },
    "phone_system": {
        "name": "AI Phone System",
        "description": "Twilio integration, interactive calls, AI agents (Jarvis/James/Director/Kael), cold caller",
        "strength": 90,
    },
    "website_builder": {
        "name": "Website/Landing Page Builder",
        "description": "4000+ pages deployed on Cloudflare, SEO-optimized, automated site generation",
        "strength": 95,
    },
    "fine_tuning": {
        "name": "Model Fine-Tuning",
        "description": "LoRA training, auto-evolve pipeline, distillation from cloud brains, 23+ specialists",
        "strength": 85,
    },
    "chrome_extensions": {
        "name": "Chrome Extension Development",
        "description": "Can build with JS/HTML/CSS, deploy to Chrome Web Store",
        "strength": 70,
    },
    "api_services": {
        "name": "API-as-a-Service",
        "description": "FastAPI, multi-machine infrastructure, load balancing, 242+ services running",
        "strength": 90,
    },
    "bots": {
        "name": "Discord/Slack/Telegram Bots",
        "description": "Telegram bot live, WhatsApp bot live, can extend to Discord/Slack",
        "strength": 80,
    },
    "automation": {
        "name": "Business Automation",
        "description": "Dispatch systems, lead gen, invoicing, CRM-like flows, autonomous builder",
        "strength": 85,
    },
    "cli_tools": {
        "name": "CLI Tools & Dev Utilities",
        "description": "Python packaging, PyPI deployment, developer tooling",
        "strength": 75,
    },
}

# ==========================================================================
# RESEARCH QUERIES — organized by category
# ==========================================================================
PAIN_POINT_QUERIES = [
    # Reddit-style pain points
    'site:reddit.com "I wish there was an app"',
    'site:reddit.com "why isn\'t there" app OR tool OR software',
    'site:reddit.com "someone should build" app OR saas OR tool',
    'site:reddit.com "I would pay for" app OR tool OR service OR software',
    'site:reddit.com "is there a tool" that OR which OR for',
    'site:reddit.com "frustrated with" software OR app OR tool',
    'site:reddit.com "looking for a tool" OR "looking for an app" OR "looking for software"',
    'site:reddit.com "does anyone know" tool OR app OR software that',
    # Twitter/X indie hacker signals
    '"just launched" saas OR tool OR app site:twitter.com OR site:x.com',
    '"building in public" launched OR revenue OR MRR',
    '"indie hacker" launched OR building OR revenue',
    # General pain points
    '"I need a tool that" OR "I need software that"',
    '"automate" "small business" pain OR frustrating OR tedious',
]

TRENDING_PRODUCT_QUERIES = [
    # Product Hunt
    'site:producthunt.com trending today 2026',
    'site:producthunt.com "product of the day" AI',
    'site:producthunt.com upvotes AI tool 2026',
    # Hacker News Show HN
    'site:news.ycombinator.com "Show HN" 2026',
    'site:news.ycombinator.com "Show HN" AI tool',
    'site:news.ycombinator.com "Show HN" launched saas',
    # Indie Hackers
    'site:indiehackers.com revenue OR MRR OR launched 2026',
    'site:indiehackers.com "making money" OR "$" OR "revenue"',
    # General trending
    'trending AI tools 2026 launched',
    'new saas products launched 2026 AI',
]

REVENUE_SIGNAL_QUERIES = [
    # SaaS revenue
    '"MRR" OR "monthly recurring revenue" indie OR solo OR bootstrap 2026',
    'site:indiehackers.com "$" "per month" OR "MRR" OR "ARR"',
    '"crossed" "$" "MRR" OR "ARR" saas OR tool 2026',
    # Fiverr successful gigs
    'site:fiverr.com AI OR automation OR chatbot OR scraping reviews',
    'fiverr "1000+ reviews" AI OR automation OR bot',
    # Gumroad products
    'site:gumroad.com AI OR template OR automation bestseller OR popular',
    'gumroad "top products" OR "best selling" AI OR developer OR tool',
    # Chrome extensions making money
    'chrome extension revenue OR "making money" OR profitable 2026',
    'chrome web store popular AI tool extension',
    '"chrome extension" "users" "$" revenue OR monetize',
    # API services
    'api-as-a-service revenue OR pricing OR "per request" AI',
    '"API" "pricing" "per call" OR "per request" AI startup',
    # Marketplaces
    'sell digital products AI tools templates 2026 revenue',
]

BUILDABLE_SOFTWARE_QUERIES = [
    # AI tools people pay for
    '"AI tool" "pricing" OR "$" OR "plan" popular OR growing 2026',
    'AI SaaS ideas 2026 profitable OR revenue',
    # Automation
    '"automation tool" small business "save time" OR "save hours"',
    'zapier alternative OR automation tool self-hosted',
    # Chrome extensions
    '"chrome extension" idea OR "I built" developer OR AI 2026',
    'chrome extension "users" launched indie developer',
    # CLI tools
    '"CLI tool" developer OR DevOps "open source" OR launched',
    'developer tools CLI "I built" OR launched 2026',
    # Bots
    'discord bot "paying" OR "premium" OR "subscribers" OR revenue',
    'slack bot saas OR revenue OR pricing OR "per workspace"',
    'telegram bot monetize OR revenue OR premium',
    # Quick-build opportunities
    '"built in a weekend" OR "built in a day" saas OR tool revenue',
    '"micro saas" idea OR launched OR revenue 2026',
]

# ==========================================================================
# DATABASE
# ==========================================================================
def init_db():
    """Initialize SQLite database with all required tables."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS findings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hash TEXT UNIQUE,
            category TEXT NOT NULL,
            subcategory TEXT DEFAULT '',
            title TEXT NOT NULL,
            description TEXT DEFAULT '',
            source_url TEXT DEFAULT '',
            source_name TEXT DEFAULT '',
            raw_snippet TEXT DEFAULT '',
            market_score INTEGER DEFAULT 0,
            build_effort_hours INTEGER DEFAULT 0,
            monthly_revenue_est INTEGER DEFAULT 0,
            product_concept TEXT DEFAULT '',
            hive_match_score INTEGER DEFAULT 0,
            matched_capabilities TEXT DEFAULT '[]',
            tags TEXT DEFAULT '[]',
            status TEXT DEFAULT 'new',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS research_cycles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            completed_at TEXT,
            findings_count INTEGER DEFAULT 0,
            categories_scanned TEXT DEFAULT '[]',
            status TEXT DEFAULT 'running',
            error TEXT DEFAULT ''
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS opportunities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            finding_id INTEGER,
            name TEXT NOT NULL,
            product_type TEXT DEFAULT '',
            description TEXT DEFAULT '',
            target_audience TEXT DEFAULT '',
            revenue_model TEXT DEFAULT '',
            monthly_revenue_low INTEGER DEFAULT 0,
            monthly_revenue_high INTEGER DEFAULT 0,
            build_hours INTEGER DEFAULT 0,
            required_capabilities TEXT DEFAULT '[]',
            difficulty TEXT DEFAULT 'medium',
            time_to_market TEXT DEFAULT '',
            competitive_advantage TEXT DEFAULT '',
            score INTEGER DEFAULT 0,
            status TEXT DEFAULT 'identified',
            created_at TEXT NOT NULL,
            FOREIGN KEY (finding_id) REFERENCES findings(id)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS strategies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            summary TEXT DEFAULT '',
            recommendations TEXT DEFAULT '[]',
            based_on_findings TEXT DEFAULT '[]',
            priority TEXT DEFAULT 'medium',
            created_at TEXT NOT NULL
        )
    """)

    # Indexes
    c.execute("CREATE INDEX IF NOT EXISTS idx_findings_category ON findings(category)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_findings_score ON findings(market_score DESC)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_findings_status ON findings(status)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_findings_created ON findings(created_at DESC)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_opp_score ON opportunities(score DESC)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_opp_hours ON opportunities(build_hours)")

    conn.commit()
    conn.close()


@contextmanager
def get_db():
    """Thread-safe database connection context manager."""
    conn = sqlite3.connect(DB_PATH, timeout=10)
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


# ==========================================================================
# SCRAPING ENGINE — DuckDuckGo HTML search + direct page scraping
# ==========================================================================
class Scraper:
    """Scrapes public web data using DuckDuckGo HTML search and direct page fetches."""

    def __init__(self):
        self._client = None
        self._lock = threading.Lock()

    @property
    def client(self) -> httpx.Client:
        if self._client is None or self._client.is_closed:
            with self._lock:
                if self._client is None or self._client.is_closed:
                    self._client = httpx.Client(
                        headers=HEADERS,
                        timeout=httpx.Timeout(20.0, connect=10.0),
                        follow_redirects=True,
                        limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
                    )
        return self._client

    def ddg_search(self, query: str, max_results: int = MAX_RESULTS_PER_QUERY) -> List[Dict]:
        """Search DuckDuckGo HTML version and extract results."""
        results = []
        try:
            url = "https://html.duckduckgo.com/html/"
            data = {"q": query, "kl": "us-en"}
            resp = self.client.post(url, data=data)
            resp.raise_for_status()
            html = resp.text

            # Parse results from DDG HTML
            # Each result is in a div with class "result"
            result_blocks = re.findall(
                r'<div[^>]*class="[^"]*result[^"]*"[^>]*>(.*?)</div>\s*</div>',
                html, re.DOTALL
            )
            if not result_blocks:
                # Alternative: extract links and snippets
                result_blocks = re.findall(
                    r'<a[^>]*class="result__a"[^>]*href="([^"]*)"[^>]*>(.*?)</a>.*?'
                    r'<a[^>]*class="result__snippet"[^>]*>(.*?)</a>',
                    html, re.DOTALL
                )
                for link, title, snippet in result_blocks[:max_results]:
                    # DDG wraps URLs in redirect
                    actual_url = self._extract_ddg_url(link)
                    clean_title = self._strip_html(title).strip()
                    clean_snippet = self._strip_html(snippet).strip()
                    if clean_title:
                        results.append({
                            "title": clean_title,
                            "url": actual_url,
                            "snippet": clean_snippet,
                        })
                if results:
                    return results

            # Broader extraction: find all result links and snippets
            links = re.findall(r'<a[^>]*class="result__a"[^>]*href="([^"]*)"[^>]*>(.*?)</a>', html, re.DOTALL)
            snippets = re.findall(r'<a[^>]*class="result__snippet"[^>]*>(.*?)</a>', html, re.DOTALL)

            for i, (link, title) in enumerate(links[:max_results]):
                actual_url = self._extract_ddg_url(link)
                clean_title = self._strip_html(title).strip()
                clean_snippet = self._strip_html(snippets[i]).strip() if i < len(snippets) else ""
                if clean_title:
                    results.append({
                        "title": clean_title,
                        "url": actual_url,
                        "snippet": clean_snippet,
                    })

        except Exception as e:
            print(f"[DDG] Search error for '{query[:50]}': {e}")

        return results

    def fetch_page_text(self, url: str, max_chars: int = 5000) -> str:
        """Fetch a page and extract readable text content."""
        try:
            resp = self.client.get(url, timeout=15.0)
            resp.raise_for_status()
            text = self._extract_text(resp.text)
            return text[:max_chars]
        except Exception as e:
            return f"[fetch error: {e}]"

    def scrape_hn_show(self, max_items: int = 30) -> List[Dict]:
        """Scrape Hacker News Show HN posts directly."""
        results = []
        try:
            # Use HN Algolia API (free, no auth)
            resp = self.client.get(
                "https://hn.algolia.com/api/v1/search?query=show%20hn&tags=show_hn&hitsPerPage=" + str(max_items),
                timeout=15.0,
            )
            resp.raise_for_status()
            data = resp.json()
            for hit in data.get("hits", []):
                results.append({
                    "title": hit.get("title", ""),
                    "url": hit.get("url", f"https://news.ycombinator.com/item?id={hit.get('objectID', '')}"),
                    "snippet": f"Points: {hit.get('points', 0)} | Comments: {hit.get('num_comments', 0)}",
                    "points": hit.get("points", 0),
                    "comments": hit.get("num_comments", 0),
                    "created_at": hit.get("created_at", ""),
                })
        except Exception as e:
            print(f"[HN] Scrape error: {e}")
        return results

    def scrape_hn_ask(self, max_items: int = 20) -> List[Dict]:
        """Scrape Hacker News Ask HN for pain points."""
        results = []
        try:
            resp = self.client.get(
                "https://hn.algolia.com/api/v1/search?query=ask%20hn&tags=ask_hn&hitsPerPage=" + str(max_items),
                timeout=15.0,
            )
            resp.raise_for_status()
            data = resp.json()
            for hit in data.get("hits", []):
                title = hit.get("title", "")
                if any(kw in title.lower() for kw in [
                    "tool", "app", "software", "build", "wish", "need",
                    "recommend", "alternative", "looking for", "best",
                    "automate", "how do you", "what do you use",
                ]):
                    results.append({
                        "title": title,
                        "url": f"https://news.ycombinator.com/item?id={hit.get('objectID', '')}",
                        "snippet": f"Points: {hit.get('points', 0)} | Comments: {hit.get('num_comments', 0)}",
                        "points": hit.get("points", 0),
                        "comments": hit.get("num_comments", 0),
                    })
        except Exception as e:
            print(f"[HN Ask] Scrape error: {e}")
        return results

    def scrape_product_hunt_trending(self) -> List[Dict]:
        """Scrape Product Hunt trending via DDG (no API needed)."""
        results = []
        queries = [
            'site:producthunt.com "AI" launched 2026',
            'site:producthunt.com "upvotes" tool launched today',
            'producthunt.com trending AI automation tool',
        ]
        for q in queries:
            hits = self.ddg_search(q, max_results=10)
            for h in hits:
                if "producthunt.com" in h.get("url", ""):
                    results.append(h)
            time.sleep(random.uniform(1.5, 3.0))
        return results

    def _extract_ddg_url(self, raw_url: str) -> str:
        """Extract actual URL from DDG redirect wrapper."""
        if "uddg=" in raw_url:
            match = re.search(r'uddg=([^&]+)', raw_url)
            if match:
                from urllib.parse import unquote
                return unquote(match.group(1))
        return raw_url

    def _strip_html(self, text: str) -> str:
        """Remove HTML tags and decode entities."""
        text = re.sub(r'<[^>]+>', '', text)
        text = html_lib.unescape(text)
        return text.strip()

    def _extract_text(self, html: str) -> str:
        """Extract readable text from HTML."""
        # Remove script/style
        html = re.sub(r'<(script|style)[^>]*>.*?</\1>', '', html, flags=re.DOTALL | re.IGNORECASE)
        # Remove tags
        text = re.sub(r'<[^>]+>', ' ', html)
        text = html_lib.unescape(text)
        # Collapse whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        return text


scraper = Scraper()

# ==========================================================================
# ANALYSIS ENGINE — scores findings, matches to Hive capabilities
# ==========================================================================
class Analyzer:
    """Scores findings, generates opportunities, matches to Hive capabilities."""

    # Keywords that signal high-value pain points
    PAIN_KEYWORDS = {
        "wish": 15, "need": 12, "pay": 20, "frustrated": 15, "tedious": 12,
        "automate": 14, "manual": 10, "hours": 10, "expensive": 12,
        "hate": 12, "annoying": 10, "slow": 10, "broken": 8, "replace": 12,
        "alternative": 10, "better": 8, "looking for": 10, "anyone know": 8,
    }

    # Keywords that signal revenue potential
    REVENUE_KEYWORDS = {
        "mrr": 20, "arr": 20, "revenue": 18, "paying": 16, "subscribers": 14,
        "customers": 12, "pricing": 14, "per month": 16, "$": 15,
        "profitable": 18, "income": 14, "sales": 12, "launched": 10,
        "growing": 12, "1000": 10, "users": 8, "premium": 12,
    }

    # Product types we can build
    PRODUCT_TYPES = {
        "chrome_extension": {"effort_base": 8, "revenue_base": 500, "match_cap": "chrome_extensions"},
        "api_service": {"effort_base": 16, "revenue_base": 1000, "match_cap": "api_services"},
        "saas_tool": {"effort_base": 40, "revenue_base": 2000, "match_cap": "website_builder"},
        "discord_bot": {"effort_base": 12, "revenue_base": 300, "match_cap": "bots"},
        "slack_bot": {"effort_base": 12, "revenue_base": 500, "match_cap": "bots"},
        "telegram_bot": {"effort_base": 8, "revenue_base": 200, "match_cap": "bots"},
        "cli_tool": {"effort_base": 8, "revenue_base": 200, "match_cap": "cli_tools"},
        "automation_tool": {"effort_base": 16, "revenue_base": 800, "match_cap": "automation"},
        "ai_tool": {"effort_base": 24, "revenue_base": 1500, "match_cap": "ai_inference"},
        "content_tool": {"effort_base": 16, "revenue_base": 600, "match_cap": "content_production"},
        "scraping_service": {"effort_base": 12, "revenue_base": 500, "match_cap": "web_scraping"},
        "phone_service": {"effort_base": 24, "revenue_base": 1000, "match_cap": "phone_system"},
        "template_pack": {"effort_base": 4, "revenue_base": 300, "match_cap": "website_builder"},
        "course_content": {"effort_base": 20, "revenue_base": 500, "match_cap": "content_production"},
    }

    def score_finding(self, title: str, snippet: str, source: str, category: str) -> Dict:
        """Score a finding on market potential, build effort, and revenue estimate."""
        text = f"{title} {snippet}".lower()

        # Base score from keyword matches
        pain_score = sum(v for k, v in self.PAIN_KEYWORDS.items() if k in text)
        revenue_score = sum(v for k, v in self.REVENUE_KEYWORDS.items() if k in text)

        # Boost by source quality
        source_boosts = {
            "hn_show": 15, "hn_ask": 10, "producthunt": 20,
            "reddit": 12, "indiehackers": 18, "twitter": 8,
            "fiverr": 10, "gumroad": 10, "ddg": 5,
        }
        source_boost = source_boosts.get(source, 5)

        # Category boost
        cat_boosts = {
            "pain_point": 1.2,
            "trending": 1.1,
            "revenue_signal": 1.3,
            "buildable": 1.0,
        }
        cat_mult = cat_boosts.get(category, 1.0)

        # Extract revenue numbers from text
        rev_numbers = re.findall(r'\$[\d,]+(?:\.\d+)?(?:\s*[kK])?\s*(?:/\s*(?:mo|month|m))?', text)
        if rev_numbers:
            revenue_score += 15

        # Points from HN
        points_match = re.search(r'points:\s*(\d+)', text)
        if points_match:
            pts = int(points_match.group(1))
            if pts > 100:
                revenue_score += 20
            elif pts > 50:
                revenue_score += 10
            elif pts > 20:
                revenue_score += 5

        # Combine into market score (0-100)
        raw = (pain_score + revenue_score + source_boost) * cat_mult
        market_score = min(100, max(0, int(raw)))

        # Determine product type
        product_type = self._detect_product_type(text)
        pt_info = self.PRODUCT_TYPES.get(product_type, {"effort_base": 20, "revenue_base": 500})

        # Estimate build effort
        build_hours = pt_info["effort_base"]
        if "complex" in text or "enterprise" in text:
            build_hours = int(build_hours * 1.5)
        if "simple" in text or "basic" in text or "minimal" in text:
            build_hours = int(build_hours * 0.6)

        # Estimate monthly revenue
        monthly_rev = pt_info["revenue_base"]
        if market_score > 70:
            monthly_rev = int(monthly_rev * 2.0)
        elif market_score > 50:
            monthly_rev = int(monthly_rev * 1.5)
        elif market_score < 30:
            monthly_rev = int(monthly_rev * 0.5)

        # Parse any explicit revenue numbers
        for num_str in rev_numbers:
            clean = re.sub(r'[^\d.]', '', num_str.replace(',', ''))
            try:
                val = float(clean)
                if 'k' in num_str.lower():
                    val *= 1000
                if val > monthly_rev:
                    monthly_rev = int(val)
            except (ValueError, TypeError):
                pass

        # Match to Hive capabilities
        matched_caps = self._match_capabilities(text, product_type)
        hive_match = self._calc_hive_match(matched_caps)

        return {
            "market_score": market_score,
            "build_effort_hours": build_hours,
            "monthly_revenue_est": monthly_rev,
            "hive_match_score": hive_match,
            "matched_capabilities": matched_caps,
            "product_type": product_type,
            "tags": self._extract_tags(text),
        }

    def _detect_product_type(self, text: str) -> str:
        """Detect what kind of product this finding is about."""
        type_signals = {
            "chrome_extension": ["chrome extension", "browser extension", "extension"],
            "api_service": ["api", "endpoint", "rest api", "api-as-a-service"],
            "discord_bot": ["discord bot", "discord server"],
            "slack_bot": ["slack bot", "slack app", "slack integration"],
            "telegram_bot": ["telegram bot", "telegram"],
            "cli_tool": ["cli tool", "command line", "terminal", "cli"],
            "automation_tool": ["automation", "automate", "workflow", "zapier", "n8n"],
            "ai_tool": ["ai tool", "gpt", "llm", "chatbot", "ai-powered", "machine learning"],
            "content_tool": ["content", "video", "youtube", "podcast", "blog"],
            "scraping_service": ["scraper", "scraping", "data extraction", "web scraping"],
            "phone_service": ["phone", "calling", "voice", "twilio", "ivr"],
            "template_pack": ["template", "boilerplate", "starter kit"],
            "course_content": ["course", "tutorial", "training", "bootcamp"],
            "saas_tool": ["saas", "subscription", "dashboard", "platform"],
        }
        for ptype, keywords in type_signals.items():
            if any(kw in text for kw in keywords):
                return ptype
        return "saas_tool"

    def _match_capabilities(self, text: str, product_type: str) -> List[str]:
        """Match finding to Hive capabilities."""
        matched = []
        cap_keywords = {
            "ai_inference": ["ai", "model", "inference", "llm", "gpt", "chatbot", "ml", "machine learning"],
            "web_scraping": ["scraping", "scraper", "data", "extraction", "crawl", "monitor"],
            "content_production": ["content", "video", "youtube", "podcast", "tts", "voice", "anime"],
            "phone_system": ["phone", "call", "voice", "twilio", "ivr", "receptionist"],
            "website_builder": ["website", "landing page", "seo", "deploy", "site", "page"],
            "fine_tuning": ["fine-tune", "training", "model", "custom ai", "lora"],
            "chrome_extensions": ["chrome", "extension", "browser"],
            "api_services": ["api", "endpoint", "service", "backend"],
            "bots": ["bot", "discord", "slack", "telegram"],
            "automation": ["automation", "automate", "workflow", "process"],
            "cli_tools": ["cli", "command line", "terminal", "developer tool"],
        }
        for cap, keywords in cap_keywords.items():
            if any(kw in text for kw in keywords):
                matched.append(cap)

        # Always include the product type's default capability
        pt_info = self.PRODUCT_TYPES.get(product_type, {})
        default_cap = pt_info.get("match_cap")
        if default_cap and default_cap not in matched:
            matched.append(default_cap)

        return matched

    def _calc_hive_match(self, matched_caps: List[str]) -> int:
        """Calculate how well an opportunity matches Hive's capabilities (0-100)."""
        if not matched_caps:
            return 20  # We can probably build anything
        strengths = [HIVE_CAPABILITIES.get(c, {}).get("strength", 50) for c in matched_caps]
        return int(sum(strengths) / len(strengths))

    def _extract_tags(self, text: str) -> List[str]:
        """Extract relevant tags from text."""
        all_tags = [
            "ai", "saas", "automation", "chrome", "api", "bot", "cli",
            "scraping", "voice", "phone", "content", "video", "seo",
            "template", "course", "free", "paid", "subscription",
            "open-source", "indie", "bootstrap", "revenue", "profitable",
            "discord", "slack", "telegram", "gumroad", "fiverr",
            "small-business", "developer", "startup",
        ]
        return [t for t in all_tags if t in text]

    def generate_product_concept(self, finding: Dict) -> str:
        """Generate a quick product concept from a finding."""
        title = finding.get("title", "")
        snippet = finding.get("snippet", "")
        product_type = finding.get("product_type", "tool")
        rev = finding.get("monthly_revenue_est", 0)

        concept = f"Product: {title}\n"
        concept += f"Type: {product_type}\n"
        concept += f"Estimated Revenue: ${rev}/mo\n"
        concept += f"Market Signal: {snippet[:200]}\n"

        # Try Ollama for better concept
        try:
            ollama_concept = self._ollama_concept(title, snippet, product_type)
            if ollama_concept:
                concept = ollama_concept
        except Exception:
            pass  # Fall back to template

        return concept

    def _ollama_concept(self, title: str, snippet: str, product_type: str) -> Optional[str]:
        """Use local Ollama to generate a product concept."""
        prompt = f"""You are a product strategist. Given this market signal, draft a 3-sentence product concept.

Market Signal: {title}
Context: {snippet[:300]}
Product Type: {product_type}

Your concept should include:
1. What to build (one clear product)
2. Who would pay for it
3. How to monetize it (pricing model)

Be specific and actionable. Response in 3 sentences only."""

        try:
            resp = httpx.post(
                f"{OLLAMA_URL}/api/generate",
                json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False},
                timeout=30.0,
            )
            if resp.status_code == 200:
                return resp.json().get("response", "").strip()
        except Exception:
            pass
        return None

    def generate_opportunity(self, finding: Dict) -> Dict:
        """Generate a structured opportunity from a finding."""
        score_data = finding.get("_score_data", {})
        product_type = score_data.get("product_type", "saas_tool")
        pt_info = self.PRODUCT_TYPES.get(product_type, {"effort_base": 20, "revenue_base": 500})

        rev_low = int(score_data.get("monthly_revenue_est", pt_info["revenue_base"]) * 0.5)
        rev_high = int(score_data.get("monthly_revenue_est", pt_info["revenue_base"]) * 1.5)

        difficulty = "easy"
        hours = score_data.get("build_effort_hours", pt_info["effort_base"])
        if hours > 30:
            difficulty = "hard"
        elif hours > 15:
            difficulty = "medium"

        if hours <= 8:
            ttm = "1 day"
        elif hours <= 24:
            ttm = "2-3 days"
        elif hours <= 48:
            ttm = "1 week"
        else:
            ttm = "2+ weeks"

        # Competitive advantage based on Hive
        caps = score_data.get("matched_capabilities", [])
        advantages = []
        if "ai_inference" in caps:
            advantages.append("23+ fine-tuned AI models ready to deploy")
        if "phone_system" in caps:
            advantages.append("Production phone system with Twilio already integrated")
        if "website_builder" in caps:
            advantages.append("4000+ page deployment pipeline on Cloudflare")
        if "web_scraping" in caps:
            advantages.append("Battle-tested scraping infrastructure")
        if "automation" in caps:
            advantages.append("242+ automated services running 24/7")
        if "bots" in caps:
            advantages.append("Telegram + WhatsApp bots already live")
        if not advantages:
            advantages.append("Multi-GPU inference cluster with 5 machines")

        rev_models = {
            "chrome_extension": "Freemium + Pro tier ($5-15/mo)",
            "api_service": "Usage-based ($0.001-0.01 per call) + monthly plans",
            "saas_tool": "Monthly subscription ($19-99/mo)",
            "discord_bot": "Premium tier ($3-10/server/mo)",
            "slack_bot": "Per-workspace pricing ($5-25/mo)",
            "telegram_bot": "Premium features ($3-10/mo)",
            "cli_tool": "One-time purchase ($10-50) or subscription ($5-15/mo)",
            "automation_tool": "Monthly subscription ($29-99/mo)",
            "ai_tool": "Usage-based + monthly plans ($19-149/mo)",
            "content_tool": "Monthly subscription ($15-49/mo)",
            "scraping_service": "Per-request + monthly ($29-199/mo)",
            "phone_service": "Monthly subscription ($99-499/mo)",
            "template_pack": "One-time purchase ($19-99)",
            "course_content": "One-time purchase ($49-199)",
        }

        # Composite score: market_score * hive_match * (1/effort)
        market = score_data.get("market_score", 50)
        hive = score_data.get("hive_match_score", 50)
        effort_penalty = max(1, hours / 8)  # Penalize high effort
        opp_score = min(100, int((market * 0.4 + hive * 0.3 + (100 / effort_penalty) * 0.3)))

        return {
            "name": finding.get("title", "Unknown")[:200],
            "product_type": product_type,
            "description": finding.get("snippet", "")[:500],
            "target_audience": self._guess_audience(product_type),
            "revenue_model": rev_models.get(product_type, "Monthly subscription"),
            "monthly_revenue_low": rev_low,
            "monthly_revenue_high": rev_high,
            "build_hours": hours,
            "required_capabilities": json.dumps(caps),
            "difficulty": difficulty,
            "time_to_market": ttm,
            "competitive_advantage": "; ".join(advantages),
            "score": opp_score,
        }

    def _guess_audience(self, product_type: str) -> str:
        audiences = {
            "chrome_extension": "Developers, knowledge workers, content creators",
            "api_service": "Developers, SaaS builders, startups",
            "saas_tool": "Small businesses, freelancers, startups",
            "discord_bot": "Community managers, server owners, gamers",
            "slack_bot": "Teams, project managers, developers",
            "telegram_bot": "Crypto communities, international users",
            "cli_tool": "Developers, DevOps engineers, sysadmins",
            "automation_tool": "Small business owners, freelancers, agencies",
            "ai_tool": "Content creators, marketers, developers",
            "content_tool": "YouTubers, bloggers, social media managers",
            "scraping_service": "Marketers, researchers, data analysts",
            "phone_service": "Small businesses, agencies, service providers",
            "template_pack": "Developers, designers, entrepreneurs",
            "course_content": "Beginners, career switchers, hobbyists",
        }
        return audiences.get(product_type, "General tech users")

    def generate_strategy(self, findings: List[Dict], opportunities: List[Dict]) -> Dict:
        """Generate strategic product recommendations based on all findings."""
        now = datetime.now(timezone.utc).isoformat()

        # Aggregate signals
        type_counts = {}
        avg_scores_by_type = {}
        total_rev_by_type = {}

        for opp in opportunities:
            pt = opp.get("product_type", "unknown")
            type_counts[pt] = type_counts.get(pt, 0) + 1
            if pt not in avg_scores_by_type:
                avg_scores_by_type[pt] = []
            avg_scores_by_type[pt].append(opp.get("score", 0))
            rev_hi = opp.get("monthly_revenue_high", 0)
            total_rev_by_type[pt] = total_rev_by_type.get(pt, 0) + rev_hi

        # Rank product types by composite signal
        type_rankings = []
        for pt, count in type_counts.items():
            avg_score = sum(avg_scores_by_type.get(pt, [0])) / max(1, len(avg_scores_by_type.get(pt, [1])))
            total_rev = total_rev_by_type.get(pt, 0)
            type_rankings.append({
                "type": pt,
                "count": count,
                "avg_score": round(avg_score, 1),
                "total_revenue_potential": total_rev,
                "composite": round(count * 0.3 + avg_score * 0.4 + (total_rev / 1000) * 0.3, 1),
            })
        type_rankings.sort(key=lambda x: x["composite"], reverse=True)

        # Top quick wins (< 8 hours build time)
        quick_wins = [o for o in opportunities if o.get("build_hours", 99) <= 8]
        quick_wins.sort(key=lambda x: x.get("score", 0), reverse=True)

        # Generate recommendations
        recommendations = []
        if type_rankings:
            top = type_rankings[0]
            recommendations.append({
                "priority": "high",
                "action": f"Build a {top['type'].replace('_', ' ')} — strongest market signal with {top['count']} findings, avg score {top['avg_score']}",
                "reasoning": f"Total revenue potential: ${top['total_revenue_potential']}/mo across {top['count']} opportunities",
            })

        if quick_wins:
            qw = quick_wins[0]
            recommendations.append({
                "priority": "high",
                "action": f"Quick win: {qw['name'][:100]} — only {qw['build_hours']}h to build",
                "reasoning": f"Score: {qw['score']}, Revenue: ${qw.get('monthly_revenue_low', 0)}-${qw.get('monthly_revenue_high', 0)}/mo",
            })

        # Capability recommendations
        strong_caps = sorted(
            HIVE_CAPABILITIES.items(),
            key=lambda x: x[1]["strength"],
            reverse=True
        )[:3]
        cap_names = [c[1]["name"] for c in strong_caps]
        recommendations.append({
            "priority": "medium",
            "action": f"Leverage top capabilities: {', '.join(cap_names)}",
            "reasoning": "Build products that use our strongest tech for fastest time-to-market",
        })

        # Pain point insights
        pain_findings = [f for f in findings if f.get("category") == "pain_point"]
        if pain_findings:
            top_pain = sorted(pain_findings, key=lambda x: x.get("market_score", 0), reverse=True)[:3]
            pain_summary = "; ".join(f.get("title", "")[:60] for f in top_pain)
            recommendations.append({
                "priority": "medium",
                "action": f"Address top pain points: {pain_summary}",
                "reasoning": "Direct user demand with validated willingness to pay",
            })

        return {
            "title": f"Market Research Strategy — {datetime.now().strftime('%Y-%m-%d')}",
            "summary": f"Analyzed {len(findings)} findings across {len(type_counts)} product categories. "
                       f"Top signal: {type_rankings[0]['type'] if type_rankings else 'N/A'}. "
                       f"Quick wins available: {len(quick_wins)}.",
            "recommendations": json.dumps(recommendations),
            "based_on_findings": json.dumps([f.get("id", 0) for f in findings[:50]]),
            "priority": "high" if quick_wins else "medium",
            "created_at": now,
            "type_rankings": type_rankings,
        }


analyzer = Analyzer()

# ==========================================================================
# RESEARCH ENGINE — orchestrates scraping + analysis + storage
# ==========================================================================
class ResearchEngine:
    """Runs research cycles: scrape -> analyze -> score -> store -> strategize."""

    def __init__(self):
        self.running = False
        self._lock = threading.Lock()

    def run_full_cycle(self) -> Dict:
        """Run a complete research cycle across all categories."""
        if self.running:
            return {"status": "already_running"}

        with self._lock:
            self.running = True

        now = datetime.now(timezone.utc).isoformat()
        cycle_id = None

        try:
            # Log cycle start
            with get_db() as conn:
                c = conn.execute(
                    "INSERT INTO research_cycles (started_at, status) VALUES (?, 'running')",
                    (now,)
                )
                cycle_id = c.lastrowid

            all_findings = []
            all_opportunities = []

            # Phase 1: Scrape pain points
            print("[Research] Phase 1: Scraping pain points...")
            pain_findings = self._research_pain_points()
            all_findings.extend(pain_findings)
            print(f"  -> {len(pain_findings)} pain points found")

            # Phase 2: Scrape trending products
            print("[Research] Phase 2: Scraping trending products...")
            trending = self._research_trending()
            all_findings.extend(trending)
            print(f"  -> {len(trending)} trending products found")

            # Phase 3: Scrape revenue signals
            print("[Research] Phase 3: Scraping revenue signals...")
            revenue = self._research_revenue_signals()
            all_findings.extend(revenue)
            print(f"  -> {len(revenue)} revenue signals found")

            # Phase 4: Scrape buildable software
            print("[Research] Phase 4: Scraping buildable software ideas...")
            buildable = self._research_buildable()
            all_findings.extend(buildable)
            print(f"  -> {len(buildable)} buildable ideas found")

            # Phase 5: Store findings
            print("[Research] Phase 5: Storing findings...")
            stored = self._store_findings(all_findings)
            print(f"  -> {stored} new findings stored")

            # Phase 6: Generate opportunities from top findings
            print("[Research] Phase 6: Generating opportunities...")
            top_findings = sorted(all_findings, key=lambda x: x.get("market_score", 0), reverse=True)[:50]
            for f in top_findings:
                opp = analyzer.generate_opportunity(f)
                all_opportunities.append(opp)
            opp_stored = self._store_opportunities(all_opportunities)
            print(f"  -> {opp_stored} opportunities stored")

            # Phase 7: Generate strategy
            print("[Research] Phase 7: Generating strategy...")
            strategy = analyzer.generate_strategy(all_findings, all_opportunities)
            self._store_strategy(strategy)
            print(f"  -> Strategy generated: {strategy['title']}")

            # Update cycle record
            completed_at = datetime.now(timezone.utc).isoformat()
            with get_db() as conn:
                conn.execute(
                    "UPDATE research_cycles SET completed_at=?, findings_count=?, "
                    "categories_scanned=?, status='completed' WHERE id=?",
                    (completed_at, stored, json.dumps(["pain_point", "trending", "revenue_signal", "buildable"]), cycle_id)
                )

            result = {
                "status": "completed",
                "cycle_id": cycle_id,
                "total_scraped": len(all_findings),
                "new_stored": stored,
                "opportunities_generated": opp_stored,
                "strategy": strategy.get("title", ""),
                "duration_sec": round(
                    (datetime.fromisoformat(completed_at) - datetime.fromisoformat(now)).total_seconds(), 1
                ),
            }
            print(f"[Research] Cycle complete: {result}")
            return result

        except Exception as e:
            err = traceback.format_exc()
            print(f"[Research] Cycle error: {err}")
            if cycle_id:
                with get_db() as conn:
                    conn.execute(
                        "UPDATE research_cycles SET status='error', error=? WHERE id=?",
                        (str(e)[:500], cycle_id)
                    )
            return {"status": "error", "error": str(e)}
        finally:
            self.running = False

    def search_topic(self, topic: str, max_results: int = 20) -> List[Dict]:
        """Run targeted search on a specific topic."""
        findings = []
        queries = [
            f'{topic} app OR tool OR saas',
            f'{topic} "I wish" OR "I need" OR "looking for"',
            f'{topic} revenue OR pricing OR "$" OR MRR',
            f'site:reddit.com {topic} tool OR app OR software',
            f'site:news.ycombinator.com {topic}',
        ]
        for q in queries:
            results = scraper.ddg_search(q, max_results=max_results // len(queries))
            for r in results:
                score_data = analyzer.score_finding(r["title"], r.get("snippet", ""), "ddg", "buildable")
                findings.append({
                    "title": r["title"],
                    "snippet": r.get("snippet", ""),
                    "url": r.get("url", ""),
                    "source": "ddg",
                    "category": "custom_search",
                    "subcategory": topic,
                    **score_data,
                    "_score_data": score_data,
                })
            time.sleep(random.uniform(1.5, 3.0))

        # Store them
        self._store_findings(findings)
        findings.sort(key=lambda x: x.get("market_score", 0), reverse=True)
        return findings[:max_results]

    def _research_pain_points(self) -> List[Dict]:
        """Research what people need / pain points."""
        findings = []

        # DDG searches for Reddit/Twitter pain points
        for q in PAIN_POINT_QUERIES:
            results = scraper.ddg_search(q, max_results=10)
            source = "reddit" if "reddit.com" in q else "twitter" if "twitter.com" in q or "x.com" in q else "ddg"
            for r in results:
                score_data = analyzer.score_finding(r["title"], r.get("snippet", ""), source, "pain_point")
                findings.append({
                    "title": r["title"],
                    "snippet": r.get("snippet", ""),
                    "url": r.get("url", ""),
                    "source": source,
                    "category": "pain_point",
                    "subcategory": "user_wish" if "wish" in q.lower() else "user_need",
                    **score_data,
                    "_score_data": score_data,
                })
            time.sleep(random.uniform(2.0, 4.0))

        # HN Ask — direct pain points
        hn_asks = scraper.scrape_hn_ask(max_items=25)
        for r in hn_asks:
            score_data = analyzer.score_finding(r["title"], r.get("snippet", ""), "hn_ask", "pain_point")
            findings.append({
                "title": r["title"],
                "snippet": r.get("snippet", ""),
                "url": r.get("url", ""),
                "source": "hn_ask",
                "category": "pain_point",
                "subcategory": "hn_ask",
                **score_data,
                "_score_data": score_data,
            })

        return findings

    def _research_trending(self) -> List[Dict]:
        """Research trending products and launches."""
        findings = []

        # HN Show — direct scrape
        hn_shows = scraper.scrape_hn_show(max_items=30)
        for r in hn_shows:
            score_data = analyzer.score_finding(r["title"], r.get("snippet", ""), "hn_show", "trending")
            findings.append({
                "title": r["title"],
                "snippet": r.get("snippet", ""),
                "url": r.get("url", ""),
                "source": "hn_show",
                "category": "trending",
                "subcategory": "show_hn",
                **score_data,
                "_score_data": score_data,
            })

        # Product Hunt via DDG
        ph_results = scraper.scrape_product_hunt_trending()
        for r in ph_results:
            score_data = analyzer.score_finding(r["title"], r.get("snippet", ""), "producthunt", "trending")
            findings.append({
                "title": r["title"],
                "snippet": r.get("snippet", ""),
                "url": r.get("url", ""),
                "source": "producthunt",
                "category": "trending",
                "subcategory": "product_hunt",
                **score_data,
                "_score_data": score_data,
            })

        # DDG trending queries
        for q in TRENDING_PRODUCT_QUERIES:
            results = scraper.ddg_search(q, max_results=8)
            for r in results:
                source = "indiehackers" if "indiehackers.com" in r.get("url", "") else "ddg"
                score_data = analyzer.score_finding(r["title"], r.get("snippet", ""), source, "trending")
                findings.append({
                    "title": r["title"],
                    "snippet": r.get("snippet", ""),
                    "url": r.get("url", ""),
                    "source": source,
                    "category": "trending",
                    "subcategory": "general",
                    **score_data,
                    "_score_data": score_data,
                })
            time.sleep(random.uniform(2.0, 4.0))

        return findings

    def _research_revenue_signals(self) -> List[Dict]:
        """Research what's actually making money."""
        findings = []

        for q in REVENUE_SIGNAL_QUERIES:
            results = scraper.ddg_search(q, max_results=8)
            for r in results:
                url = r.get("url", "")
                if "fiverr.com" in url:
                    source = "fiverr"
                elif "gumroad.com" in url:
                    source = "gumroad"
                elif "indiehackers.com" in url:
                    source = "indiehackers"
                else:
                    source = "ddg"
                score_data = analyzer.score_finding(r["title"], r.get("snippet", ""), source, "revenue_signal")
                findings.append({
                    "title": r["title"],
                    "snippet": r.get("snippet", ""),
                    "url": url,
                    "source": source,
                    "category": "revenue_signal",
                    "subcategory": source,
                    **score_data,
                    "_score_data": score_data,
                })
            time.sleep(random.uniform(2.0, 4.0))

        return findings

    def _research_buildable(self) -> List[Dict]:
        """Research software we could build."""
        findings = []

        for q in BUILDABLE_SOFTWARE_QUERIES:
            results = scraper.ddg_search(q, max_results=8)
            for r in results:
                score_data = analyzer.score_finding(r["title"], r.get("snippet", ""), "ddg", "buildable")
                findings.append({
                    "title": r["title"],
                    "snippet": r.get("snippet", ""),
                    "url": r.get("url", ""),
                    "source": "ddg",
                    "category": "buildable",
                    "subcategory": analyzer._detect_product_type(f"{r['title']} {r.get('snippet', '')}".lower()),
                    **score_data,
                    "_score_data": score_data,
                })
            time.sleep(random.uniform(2.0, 4.0))

        return findings

    def _store_findings(self, findings: List[Dict]) -> int:
        """Store findings in DB, deduplicating by hash."""
        stored = 0
        now = datetime.now(timezone.utc).isoformat()
        with get_db() as conn:
            for f in findings:
                # Generate hash for dedup
                hash_input = f"{f.get('title', '')}{f.get('url', '')}".encode()
                h = hashlib.md5(hash_input).hexdigest()

                # Check if exists
                existing = conn.execute("SELECT id FROM findings WHERE hash=?", (h,)).fetchone()
                if existing:
                    # Update score if higher
                    conn.execute(
                        "UPDATE findings SET market_score = MAX(market_score, ?), updated_at=? WHERE hash=?",
                        (f.get("market_score", 0), now, h)
                    )
                    continue

                try:
                    concept = analyzer.generate_product_concept(f) if f.get("market_score", 0) > 40 else ""

                    conn.execute("""
                        INSERT INTO findings (hash, category, subcategory, title, description,
                            source_url, source_name, raw_snippet, market_score, build_effort_hours,
                            monthly_revenue_est, product_concept, hive_match_score,
                            matched_capabilities, tags, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        h,
                        f.get("category", ""),
                        f.get("subcategory", ""),
                        f.get("title", "")[:500],
                        f.get("snippet", "")[:1000],
                        f.get("url", "")[:500],
                        f.get("source", ""),
                        f.get("snippet", "")[:2000],
                        f.get("market_score", 0),
                        f.get("build_effort_hours", 0),
                        f.get("monthly_revenue_est", 0),
                        concept[:2000] if concept else "",
                        f.get("hive_match_score", 0),
                        json.dumps(f.get("matched_capabilities", [])),
                        json.dumps(f.get("tags", [])),
                        now,
                        now,
                    ))
                    stored += 1
                except sqlite3.IntegrityError:
                    pass  # Duplicate hash
                except Exception as e:
                    print(f"[Store] Error storing finding: {e}")
        return stored

    def _store_opportunities(self, opportunities: List[Dict]) -> int:
        """Store opportunities in DB."""
        stored = 0
        now = datetime.now(timezone.utc).isoformat()
        with get_db() as conn:
            for opp in opportunities:
                try:
                    conn.execute("""
                        INSERT INTO opportunities (name, product_type, description,
                            target_audience, revenue_model, monthly_revenue_low,
                            monthly_revenue_high, build_hours, required_capabilities,
                            difficulty, time_to_market, competitive_advantage, score, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        opp.get("name", "")[:200],
                        opp.get("product_type", ""),
                        opp.get("description", "")[:500],
                        opp.get("target_audience", ""),
                        opp.get("revenue_model", ""),
                        opp.get("monthly_revenue_low", 0),
                        opp.get("monthly_revenue_high", 0),
                        opp.get("build_hours", 0),
                        opp.get("required_capabilities", "[]"),
                        opp.get("difficulty", "medium"),
                        opp.get("time_to_market", ""),
                        opp.get("competitive_advantage", ""),
                        opp.get("score", 0),
                        now,
                    ))
                    stored += 1
                except Exception as e:
                    print(f"[Store] Error storing opportunity: {e}")
        return stored

    def _store_strategy(self, strategy: Dict) -> int:
        """Store strategy recommendation in DB."""
        with get_db() as conn:
            c = conn.execute("""
                INSERT INTO strategies (title, summary, recommendations, based_on_findings, priority, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                strategy.get("title", ""),
                strategy.get("summary", ""),
                strategy.get("recommendations", "[]"),
                strategy.get("based_on_findings", "[]"),
                strategy.get("priority", "medium"),
                strategy.get("created_at", datetime.now(timezone.utc).isoformat()),
            ))
            return c.lastrowid


engine = ResearchEngine()

# ==========================================================================
# SCHEDULER — runs research every 4 hours
# ==========================================================================
def scheduler_loop():
    """Background thread that triggers research cycles."""
    # Wait 30 seconds after startup before first run
    time.sleep(30)
    while True:
        try:
            print(f"[Scheduler] Starting research cycle at {datetime.now()}")
            result = engine.run_full_cycle()
            print(f"[Scheduler] Cycle result: {result.get('status', 'unknown')}")
        except Exception as e:
            print(f"[Scheduler] Error: {e}")
        # Sleep for configured interval
        time.sleep(SCAN_INTERVAL_HOURS * 3600)


# ==========================================================================
# FASTAPI APP
# ==========================================================================
app = FastAPI(
    title="Hive Market Research Agent",
    description="Autonomous market research — finds pain points, trending products, and revenue opportunities",
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
    max_results: int = 20


# ---------- Health ----------
@app.get("/health")
def health():
    """Health check endpoint."""
    with get_db() as conn:
        findings_count = conn.execute("SELECT COUNT(*) FROM findings").fetchone()[0]
        opp_count = conn.execute("SELECT COUNT(*) FROM opportunities").fetchone()[0]
        last_cycle = conn.execute(
            "SELECT * FROM research_cycles ORDER BY id DESC LIMIT 1"
        ).fetchone()

    return {
        "status": "healthy",
        "service": "hive-market-research",
        "port": PORT,
        "findings_count": findings_count,
        "opportunities_count": opp_count,
        "research_running": engine.running,
        "last_cycle": dict(last_cycle) if last_cycle else None,
        "scan_interval_hours": SCAN_INTERVAL_HOURS,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ---------- Pain Points ----------
@app.get("/api/pain-points")
def get_pain_points(
    limit: int = Query(50, ge=1, le=500),
    min_score: int = Query(0, ge=0, le=100),
    source: Optional[str] = Query(None),
):
    """What people need — pain points from Reddit, HN, Twitter."""
    with get_db() as conn:
        query = "SELECT * FROM findings WHERE category = 'pain_point'"
        params = []
        if min_score > 0:
            query += " AND market_score >= ?"
            params.append(min_score)
        if source:
            query += " AND source_name = ?"
            params.append(source)
        query += " ORDER BY market_score DESC LIMIT ?"
        params.append(limit)

        rows = conn.execute(query, params).fetchall()

    results = []
    for r in rows:
        d = dict(r)
        d["matched_capabilities"] = json.loads(d.get("matched_capabilities", "[]"))
        d["tags"] = json.loads(d.get("tags", "[]"))
        results.append(d)

    return {
        "count": len(results),
        "pain_points": results,
        "summary": _summarize_category(results, "pain_point"),
    }


# ---------- Trending ----------
@app.get("/api/trending")
def get_trending(
    limit: int = Query(50, ge=1, le=500),
    source: Optional[str] = Query(None),
):
    """Trending products — Product Hunt, Show HN, Indie Hackers."""
    with get_db() as conn:
        query = "SELECT * FROM findings WHERE category = 'trending'"
        params = []
        if source:
            query += " AND source_name = ?"
            params.append(source)
        query += " ORDER BY market_score DESC, created_at DESC LIMIT ?"
        params.append(limit)

        rows = conn.execute(query, params).fetchall()

    results = []
    for r in rows:
        d = dict(r)
        d["matched_capabilities"] = json.loads(d.get("matched_capabilities", "[]"))
        d["tags"] = json.loads(d.get("tags", "[]"))
        results.append(d)

    return {
        "count": len(results),
        "trending": results,
        "summary": _summarize_category(results, "trending"),
    }


# ---------- Opportunities ----------
@app.get("/api/opportunities")
def get_opportunities(
    limit: int = Query(50, ge=1, le=500),
    min_score: int = Query(0, ge=0, le=100),
    product_type: Optional[str] = Query(None),
    difficulty: Optional[str] = Query(None),
):
    """Software we could build, ranked by opportunity score."""
    with get_db() as conn:
        query = "SELECT * FROM opportunities WHERE 1=1"
        params = []
        if min_score > 0:
            query += " AND score >= ?"
            params.append(min_score)
        if product_type:
            query += " AND product_type = ?"
            params.append(product_type)
        if difficulty:
            query += " AND difficulty = ?"
            params.append(difficulty)
        query += " ORDER BY score DESC LIMIT ?"
        params.append(limit)

        rows = conn.execute(query, params).fetchall()

    results = []
    for r in rows:
        d = dict(r)
        d["required_capabilities"] = json.loads(d.get("required_capabilities", "[]"))
        results.append(d)

    # Aggregate stats
    total_rev_low = sum(r.get("monthly_revenue_low", 0) for r in results)
    total_rev_high = sum(r.get("monthly_revenue_high", 0) for r in results)
    avg_hours = round(sum(r.get("build_hours", 0) for r in results) / max(1, len(results)), 1)

    return {
        "count": len(results),
        "opportunities": results,
        "stats": {
            "total_monthly_revenue_range": f"${total_rev_low:,} - ${total_rev_high:,}",
            "average_build_hours": avg_hours,
            "by_type": _count_by_field(results, "product_type"),
            "by_difficulty": _count_by_field(results, "difficulty"),
        },
    }


# ---------- Quick Wins ----------
@app.get("/api/quick-wins")
def get_quick_wins(
    max_hours: int = Query(8, ge=1, le=48),
    limit: int = Query(20, ge=1, le=100),
):
    """Things we can build in < 1 day, ranked by ROI potential."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT * FROM opportunities
            WHERE build_hours <= ?
            ORDER BY score DESC, monthly_revenue_high DESC
            LIMIT ?
        """, (max_hours, limit)).fetchall()

    results = []
    for r in rows:
        d = dict(r)
        d["required_capabilities"] = json.loads(d.get("required_capabilities", "[]"))
        # Calculate ROI score: revenue per hour of work
        hours = max(1, d.get("build_hours", 1))
        rev = d.get("monthly_revenue_high", 0)
        d["roi_per_hour"] = round(rev / hours, 2)
        results.append(d)

    results.sort(key=lambda x: x.get("roi_per_hour", 0), reverse=True)

    return {
        "count": len(results),
        "max_hours": max_hours,
        "quick_wins": results,
        "top_pick": results[0] if results else None,
        "total_potential_revenue": sum(r.get("monthly_revenue_high", 0) for r in results),
    }


# ---------- Strategy ----------
@app.get("/api/strategy")
def get_strategy(limit: int = Query(5, ge=1, le=20)):
    """Product strategy recommendations based on all research."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM strategies ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()

    results = []
    for r in rows:
        d = dict(r)
        d["recommendations"] = json.loads(d.get("recommendations", "[]"))
        d["based_on_findings"] = json.loads(d.get("based_on_findings", "[]"))
        results.append(d)

    # Also compute live stats
    with get_db() as conn:
        total_findings = conn.execute("SELECT COUNT(*) FROM findings").fetchone()[0]
        total_opps = conn.execute("SELECT COUNT(*) FROM opportunities").fetchone()[0]
        top_types = conn.execute("""
            SELECT product_type, COUNT(*) as cnt, AVG(score) as avg_score,
                   SUM(monthly_revenue_high) as total_rev
            FROM opportunities
            GROUP BY product_type
            ORDER BY avg_score DESC
            LIMIT 10
        """).fetchall()

    type_breakdown = [
        {
            "type": t["product_type"],
            "count": t["cnt"],
            "avg_score": round(t["avg_score"], 1),
            "total_revenue_potential": t["total_rev"],
        }
        for t in top_types
    ]

    return {
        "strategies": results,
        "live_stats": {
            "total_findings": total_findings,
            "total_opportunities": total_opps,
            "product_type_breakdown": type_breakdown,
        },
        "hive_capabilities": {
            k: {"name": v["name"], "strength": v["strength"]}
            for k, v in HIVE_CAPABILITIES.items()
        },
    }


# ---------- Search Specific Topic ----------
@app.post("/api/search")
def search_topic(req: SearchRequest):
    """Search a specific topic for market opportunities."""
    findings = engine.search_topic(req.topic, max_results=req.max_results)

    # Clean internal fields
    clean = []
    for f in findings:
        c = {k: v for k, v in f.items() if not k.startswith("_")}
        clean.append(c)

    return {
        "topic": req.topic,
        "count": len(clean),
        "findings": clean,
    }


# ---------- Trigger Manual Cycle ----------
@app.post("/api/cycle")
def trigger_cycle(background_tasks: BackgroundTasks):
    """Trigger a manual research cycle."""
    if engine.running:
        return {"status": "already_running"}
    background_tasks.add_task(engine.run_full_cycle)
    return {"status": "started", "message": "Research cycle triggered in background"}


# ---------- Stats ----------
@app.get("/api/stats")
def get_stats():
    """Get research statistics."""
    with get_db() as conn:
        total_findings = conn.execute("SELECT COUNT(*) FROM findings").fetchone()[0]
        total_opps = conn.execute("SELECT COUNT(*) FROM opportunities").fetchone()[0]
        total_cycles = conn.execute("SELECT COUNT(*) FROM research_cycles").fetchone()[0]

        by_category = conn.execute("""
            SELECT category, COUNT(*) as cnt, AVG(market_score) as avg_score
            FROM findings GROUP BY category ORDER BY cnt DESC
        """).fetchall()

        by_source = conn.execute("""
            SELECT source_name, COUNT(*) as cnt
            FROM findings GROUP BY source_name ORDER BY cnt DESC
        """).fetchall()

        recent = conn.execute("""
            SELECT * FROM findings ORDER BY created_at DESC LIMIT 10
        """).fetchall()

        top_scored = conn.execute("""
            SELECT * FROM findings ORDER BY market_score DESC LIMIT 10
        """).fetchall()

    return {
        "total_findings": total_findings,
        "total_opportunities": total_opps,
        "total_cycles": total_cycles,
        "by_category": [{"category": r["category"], "count": r["cnt"], "avg_score": round(r["avg_score"], 1)} for r in by_category],
        "by_source": [{"source": r["source_name"], "count": r["cnt"]} for r in by_source],
        "recent_findings": [dict(r) for r in recent],
        "top_scored_findings": [dict(r) for r in top_scored],
    }


# ---------- Revenue Signals ----------
@app.get("/api/revenue-signals")
def get_revenue_signals(
    limit: int = Query(50, ge=1, le=500),
    min_score: int = Query(0, ge=0, le=100),
):
    """What's making money — SaaS revenue, Fiverr gigs, Gumroad products."""
    with get_db() as conn:
        query = "SELECT * FROM findings WHERE category = 'revenue_signal'"
        params = []
        if min_score > 0:
            query += " AND market_score >= ?"
            params.append(min_score)
        query += " ORDER BY monthly_revenue_est DESC, market_score DESC LIMIT ?"
        params.append(limit)

        rows = conn.execute(query, params).fetchall()

    results = []
    for r in rows:
        d = dict(r)
        d["matched_capabilities"] = json.loads(d.get("matched_capabilities", "[]"))
        d["tags"] = json.loads(d.get("tags", "[]"))
        results.append(d)

    return {
        "count": len(results),
        "revenue_signals": results,
        "total_observed_revenue": sum(r.get("monthly_revenue_est", 0) for r in results),
    }


# ---------- HTML Dashboard ----------
@app.get("/", response_class=HTMLResponse)
def dashboard():
    """HTML dashboard showing research findings and opportunities."""
    with get_db() as conn:
        total_findings = conn.execute("SELECT COUNT(*) FROM findings").fetchone()[0]
        total_opps = conn.execute("SELECT COUNT(*) FROM opportunities").fetchone()[0]

        by_cat = conn.execute("""
            SELECT category, COUNT(*) as cnt, ROUND(AVG(market_score),1) as avg
            FROM findings GROUP BY category ORDER BY cnt DESC
        """).fetchall()

        top_opps = conn.execute("""
            SELECT * FROM opportunities ORDER BY score DESC LIMIT 15
        """).fetchall()

        quick_wins = conn.execute("""
            SELECT * FROM opportunities WHERE build_hours <= 8 ORDER BY score DESC LIMIT 10
        """).fetchall()

        top_pain = conn.execute("""
            SELECT * FROM findings WHERE category='pain_point' ORDER BY market_score DESC LIMIT 10
        """).fetchall()

        top_trending = conn.execute("""
            SELECT * FROM findings WHERE category='trending' ORDER BY market_score DESC LIMIT 10
        """).fetchall()

        top_revenue = conn.execute("""
            SELECT * FROM findings WHERE category='revenue_signal' ORDER BY monthly_revenue_est DESC LIMIT 10
        """).fetchall()

        last_cycle = conn.execute(
            "SELECT * FROM research_cycles ORDER BY id DESC LIMIT 1"
        ).fetchone()

        latest_strategy = conn.execute(
            "SELECT * FROM strategies ORDER BY created_at DESC LIMIT 1"
        ).fetchone()

    # Build category rows
    cat_rows = ""
    for c in by_cat:
        cat_rows += f"<tr><td>{_esc(c['category'])}</td><td>{c['cnt']}</td><td>{c['avg']}</td></tr>"

    # Build opportunity rows
    opp_rows = ""
    for o in top_opps:
        caps = json.loads(o["required_capabilities"]) if o["required_capabilities"] else []
        cap_badges = " ".join(f'<span class="badge">{_esc(c)}</span>' for c in caps[:3])
        opp_rows += f"""<tr>
            <td>{_esc(o['name'][:80])}</td>
            <td><span class="type-badge">{_esc(o['product_type'])}</span></td>
            <td class="score">{o['score']}</td>
            <td>{o['build_hours']}h</td>
            <td>${o['monthly_revenue_low']:,} - ${o['monthly_revenue_high']:,}</td>
            <td><span class="diff-{o['difficulty']}">{o['difficulty']}</span></td>
            <td>{o['time_to_market']}</td>
            <td>{cap_badges}</td>
        </tr>"""

    # Quick wins rows
    qw_rows = ""
    for q in quick_wins:
        rev_per_hr = round(q["monthly_revenue_high"] / max(1, q["build_hours"]), 0)
        qw_rows += f"""<tr>
            <td>{_esc(q['name'][:80])}</td>
            <td>{q['build_hours']}h</td>
            <td>${q['monthly_revenue_high']:,}/mo</td>
            <td>${rev_per_hr:,.0f}/hr</td>
            <td class="score">{q['score']}</td>
        </tr>"""

    # Pain point rows
    pain_rows = ""
    for p in top_pain:
        pain_rows += f"""<tr>
            <td>{_esc(p['title'][:80])}</td>
            <td>{_esc(p['source_name'])}</td>
            <td class="score">{p['market_score']}</td>
            <td>${p['monthly_revenue_est']:,}</td>
            <td><a href="{_esc(p['source_url'])}" target="_blank">link</a></td>
        </tr>"""

    # Trending rows
    trend_rows = ""
    for t in top_trending:
        trend_rows += f"""<tr>
            <td>{_esc(t['title'][:80])}</td>
            <td>{_esc(t['source_name'])}</td>
            <td class="score">{t['market_score']}</td>
            <td><a href="{_esc(t['source_url'])}" target="_blank">link</a></td>
        </tr>"""

    # Revenue rows
    rev_rows = ""
    for rv in top_revenue:
        rev_rows += f"""<tr>
            <td>{_esc(rv['title'][:80])}</td>
            <td>{_esc(rv['source_name'])}</td>
            <td>${rv['monthly_revenue_est']:,}</td>
            <td class="score">{rv['market_score']}</td>
            <td><a href="{_esc(rv['source_url'])}" target="_blank">link</a></td>
        </tr>"""

    # Strategy
    strategy_html = "<p>No strategy generated yet. Trigger a research cycle first.</p>"
    if latest_strategy:
        recs = json.loads(latest_strategy["recommendations"]) if latest_strategy["recommendations"] else []
        rec_items = ""
        for rec in recs:
            rec_items += f"""<div class="rec-card rec-{rec.get('priority', 'medium')}">
                <strong>[{rec.get('priority', '?').upper()}]</strong> {_esc(rec.get('action', ''))}
                <br><small>{_esc(rec.get('reasoning', ''))}</small>
            </div>"""
        strategy_html = f"""
            <h3>{_esc(latest_strategy['title'])}</h3>
            <p>{_esc(latest_strategy['summary'])}</p>
            {rec_items}
        """

    cycle_status = "Never run"
    if last_cycle:
        cycle_status = f"{last_cycle['status']} at {last_cycle.get('completed_at', last_cycle['started_at'])} ({last_cycle.get('findings_count', 0)} findings)"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Hive Market Research</title>
<style>
:root {{ --bg: #0a0e17; --card: #131a2b; --border: #1e2d4a; --text: #c8d6e5; --bright: #f0f4f8;
    --accent: #00d4ff; --green: #00e676; --orange: #ff9100; --red: #ff5252; --purple: #b388ff; }}
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{ background: var(--bg); color: var(--text); font-family: 'Segoe UI', system-ui, sans-serif; padding: 20px; }}
h1 {{ color: var(--accent); margin-bottom: 5px; font-size: 1.8em; }}
h2 {{ color: var(--bright); margin: 20px 0 10px; font-size: 1.3em; border-bottom: 1px solid var(--border); padding-bottom: 5px; }}
h3 {{ color: var(--accent); margin: 10px 0 5px; }}
.subtitle {{ color: var(--text); opacity: 0.7; margin-bottom: 15px; }}
.stats-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin-bottom: 20px; }}
.stat-card {{ background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 16px; text-align: center; }}
.stat-card .value {{ font-size: 2em; font-weight: bold; color: var(--accent); }}
.stat-card .label {{ color: var(--text); opacity: 0.7; font-size: 0.85em; margin-top: 4px; }}
table {{ width: 100%; border-collapse: collapse; background: var(--card); border-radius: 8px; overflow: hidden; margin-bottom: 15px; }}
th {{ background: #1a2540; color: var(--accent); padding: 10px 12px; text-align: left; font-size: 0.85em; text-transform: uppercase; }}
td {{ padding: 8px 12px; border-bottom: 1px solid var(--border); font-size: 0.9em; }}
tr:hover {{ background: rgba(0,212,255,0.05); }}
a {{ color: var(--accent); text-decoration: none; }}
a:hover {{ text-decoration: underline; }}
.score {{ color: var(--green); font-weight: bold; }}
.badge {{ background: var(--border); color: var(--text); padding: 2px 8px; border-radius: 4px; font-size: 0.75em; margin-right: 3px; }}
.type-badge {{ background: rgba(0,212,255,0.15); color: var(--accent); padding: 2px 8px; border-radius: 4px; font-size: 0.8em; }}
.diff-easy {{ color: var(--green); font-weight: bold; }}
.diff-medium {{ color: var(--orange); font-weight: bold; }}
.diff-hard {{ color: var(--red); font-weight: bold; }}
.rec-card {{ background: var(--card); border-left: 3px solid var(--border); padding: 10px 14px; margin: 8px 0; border-radius: 0 6px 6px 0; }}
.rec-high {{ border-left-color: var(--red); }}
.rec-medium {{ border-left-color: var(--orange); }}
.rec-low {{ border-left-color: var(--green); }}
.btn {{ display: inline-block; background: var(--accent); color: var(--bg); padding: 8px 16px; border: none; border-radius: 6px;
    cursor: pointer; font-size: 0.9em; text-decoration: none; margin: 5px 5px 5px 0; font-weight: bold; }}
.btn:hover {{ opacity: 0.85; text-decoration: none; }}
.btn-secondary {{ background: var(--border); color: var(--text); }}
.section {{ background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 16px; margin-bottom: 16px; }}
.search-box {{ display: flex; gap: 8px; margin: 12px 0; }}
.search-box input {{ flex:1; padding: 8px 12px; border-radius: 6px; border: 1px solid var(--border); background: var(--bg); color: var(--bright); font-size: 0.95em; }}
.search-box button {{ padding: 8px 20px; border-radius: 6px; border: none; background: var(--accent); color: var(--bg); font-weight: bold; cursor: pointer; }}
#search-results {{ margin-top: 12px; }}
.cycle-status {{ padding: 6px 12px; background: rgba(0,230,118,0.1); border-radius: 6px; display: inline-block; margin: 5px 0; font-size: 0.85em; }}
</style>
</head>
<body>
<h1>Hive Market Research</h1>
<p class="subtitle">Autonomous market intelligence — pain points, trending products, revenue signals, opportunities</p>

<div class="stats-grid">
    <div class="stat-card"><div class="value">{total_findings}</div><div class="label">Total Findings</div></div>
    <div class="stat-card"><div class="value">{total_opps}</div><div class="label">Opportunities</div></div>
    <div class="stat-card"><div class="value">{len(quick_wins)}</div><div class="label">Quick Wins (≤8h)</div></div>
    <div class="stat-card"><div class="value">{'Running' if engine.running else 'Idle'}</div><div class="label">Engine Status</div></div>
</div>

<div style="margin-bottom:15px;">
    <span class="cycle-status">Last cycle: {_esc(cycle_status)}</span>
    <a href="#" class="btn" onclick="triggerCycle(); return false;">Run Research Cycle</a>
    <a href="/api/opportunities" class="btn btn-secondary">API: Opportunities</a>
    <a href="/api/quick-wins" class="btn btn-secondary">API: Quick Wins</a>
    <a href="/api/strategy" class="btn btn-secondary">API: Strategy</a>
</div>

<div class="section">
    <h2>Search a Topic</h2>
    <div class="search-box">
        <input type="text" id="search-input" placeholder="e.g., AI writing assistant, chrome extension for developers..." />
        <button onclick="searchTopic()">Research</button>
    </div>
    <div id="search-results"></div>
</div>

<div class="section">
    <h2>Strategy & Recommendations</h2>
    {strategy_html}
</div>

<h2>Quick Wins (Build in ≤ 8 hours)</h2>
<table>
    <tr><th>Opportunity</th><th>Build Time</th><th>Revenue</th><th>ROI/hr</th><th>Score</th></tr>
    {qw_rows if qw_rows else '<tr><td colspan="5">No quick wins yet. Run a research cycle.</td></tr>'}
</table>

<h2>Top Opportunities (All)</h2>
<table>
    <tr><th>Name</th><th>Type</th><th>Score</th><th>Build</th><th>Revenue/mo</th><th>Difficulty</th><th>TTM</th><th>Capabilities</th></tr>
    {opp_rows if opp_rows else '<tr><td colspan="8">No opportunities yet. Run a research cycle.</td></tr>'}
</table>

<h2>Pain Points (What People Need)</h2>
<table>
    <tr><th>Finding</th><th>Source</th><th>Score</th><th>Rev Est</th><th>Link</th></tr>
    {pain_rows if pain_rows else '<tr><td colspan="5">No pain points yet.</td></tr>'}
</table>

<h2>Trending Products</h2>
<table>
    <tr><th>Product</th><th>Source</th><th>Score</th><th>Link</th></tr>
    {trend_rows if trend_rows else '<tr><td colspan="4">No trending products yet.</td></tr>'}
</table>

<h2>Revenue Signals</h2>
<table>
    <tr><th>Signal</th><th>Source</th><th>Revenue</th><th>Score</th><th>Link</th></tr>
    {rev_rows if rev_rows else '<tr><td colspan="5">No revenue signals yet.</td></tr>'}
</table>

<h2>Findings by Category</h2>
<table>
    <tr><th>Category</th><th>Count</th><th>Avg Score</th></tr>
    {cat_rows if cat_rows else '<tr><td colspan="3">No data yet.</td></tr>'}
</table>

<script>
async function triggerCycle() {{
    const resp = await fetch('/api/cycle', {{method: 'POST'}});
    const data = await resp.json();
    alert('Research cycle ' + data.status + (data.message ? ': ' + data.message : ''));
    if (data.status === 'started') setTimeout(() => location.reload(), 5000);
}}

async function searchTopic() {{
    const input = document.getElementById('search-input');
    const topic = input.value.trim();
    if (!topic) return;
    const results = document.getElementById('search-results');
    results.innerHTML = '<p>Researching "' + topic + '"...</p>';
    try {{
        const resp = await fetch('/api/search', {{
            method: 'POST',
            headers: {{'Content-Type': 'application/json'}},
            body: JSON.stringify({{topic: topic, max_results: 20}})
        }});
        const data = await resp.json();
        if (data.findings && data.findings.length > 0) {{
            let html = '<table><tr><th>Finding</th><th>Score</th><th>Revenue Est</th><th>Build Hours</th><th>Hive Match</th></tr>';
            data.findings.forEach(f => {{
                html += '<tr><td>' + (f.title || '').substring(0, 80) + '</td>';
                html += '<td class="score">' + (f.market_score || 0) + '</td>';
                html += '<td>$' + (f.monthly_revenue_est || 0).toLocaleString() + '</td>';
                html += '<td>' + (f.build_effort_hours || 0) + 'h</td>';
                html += '<td>' + (f.hive_match_score || 0) + '</td></tr>';
            }});
            html += '</table>';
            results.innerHTML = '<p><strong>' + data.count + ' findings for "' + topic + '"</strong></p>' + html;
        }} else {{
            results.innerHTML = '<p>No findings for "' + topic + '". Try different keywords.</p>';
        }}
    }} catch (e) {{
        results.innerHTML = '<p style="color:var(--red);">Error: ' + e.message + '</p>';
    }}
}}

document.getElementById('search-input').addEventListener('keypress', function(e) {{
    if (e.key === 'Enter') searchTopic();
}});
</script>
</body>
</html>"""
    return HTMLResponse(content=html)


# ==========================================================================
# HELPER FUNCTIONS
# ==========================================================================
def _esc(text: str) -> str:
    """Escape HTML."""
    if not text:
        return ""
    return html_lib.escape(str(text))


def _summarize_category(items: List[Dict], category: str) -> Dict:
    """Summarize a category of findings."""
    if not items:
        return {"count": 0, "avg_score": 0, "top_sources": []}
    scores = [i.get("market_score", 0) for i in items]
    sources = {}
    for i in items:
        src = i.get("source_name", "unknown")
        sources[src] = sources.get(src, 0) + 1
    top_sources = sorted(sources.items(), key=lambda x: x[1], reverse=True)[:5]
    return {
        "count": len(items),
        "avg_score": round(sum(scores) / len(scores), 1),
        "max_score": max(scores),
        "top_sources": [{"source": s, "count": c} for s, c in top_sources],
    }


def _count_by_field(items: List[Dict], field: str) -> Dict:
    """Count items by a field value."""
    counts = {}
    for i in items:
        val = i.get(field, "unknown")
        counts[val] = counts.get(val, 0) + 1
    return dict(sorted(counts.items(), key=lambda x: x[1], reverse=True))


# ==========================================================================
# STARTUP
# ==========================================================================
@app.on_event("startup")
def on_startup():
    """Initialize DB and start background scheduler."""
    print(f"[Market Research] Starting on port {PORT}")
    print(f"[Market Research] DB: {DB_PATH}")
    init_db()

    # Start scheduler in background thread
    t = threading.Thread(target=scheduler_loop, daemon=True)
    t.start()
    print(f"[Market Research] Scheduler started (every {SCAN_INTERVAL_HOURS}h)")


if __name__ == "__main__":
    init_db()
    uvicorn.run(app, host="0.0.0.0", port=PORT)
