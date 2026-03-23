#!/usr/bin/env python3
"""
THE HIVE — Revenue Hunter Agent
Port 8904 | SQLite at /home/zero/hivecode_sandbox/revenue.db
MIT License

Autonomous revenue opportunity finder:
  - Scans freelance platforms (Upwork, Fiverr, Freelancer)
  - Monitors remote work boards (RemoteOK, WeWorkRemotely)
  - Tracks local NW Florida business opportunities
  - Scores and ranks every opportunity
  - Drafts pitches/proposals via Ollama
  - Feeds top opportunities to nerve
  - Tracks real market rates for AI services

Goal: Find the FASTEST path from $0 to $1,000/month.
Location: Destin / Fort Walton Beach, Florida (NW Florida / Emerald Coast)
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
PORT = 8904
DB_PATH = "/home/zero/hivecode_sandbox/revenue.db"

OLLAMA_URL = "http://localhost:11434"
OLLAMA_MODEL = "gemma2:2b"
NERVE_URL = "http://100.105.160.106:8200/api/add"

SCAN_INTERVAL_MINUTES = 15

# Location context
LOCATION = "Destin / Fort Walton Beach, Florida"
REGION = "NW Florida"
OKALOOSA_CITIES = [
    "Destin", "Fort Walton Beach", "Niceville", "Crestview",
    "Mary Esther", "Shalimar", "Valparaiso", "Navarre",
    "Panama City", "Pensacola", "Gulf Breeze", "Milton",
    "DeFuniak Springs", "Santa Rosa Beach", "Miramar Beach",
]

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
# WHAT WE CAN SELL — our service catalog with realistic pricing
# ==========================================================================
OUR_SERVICES = {
    "ai_phone_agent": {
        "name": "AI Phone Answering Agent",
        "description": "24/7 AI-powered phone answering with natural conversation, appointment booking, FAQ handling. Custom-trained on your business.",
        "price_range": "$299-$999/mo",
        "price_low": 299,
        "price_high": 999,
        "setup_time": "3-5 days",
        "tech_ready": True,
        "keywords": ["phone", "answering", "call", "receptionist", "IVR", "voice", "virtual assistant"],
    },
    "ai_chatbot": {
        "name": "AI Chatbot for Website",
        "description": "Custom AI chatbot trained on your business data. Handles customer questions, collects leads, books appointments 24/7.",
        "price_range": "$199-$499/mo",
        "price_low": 199,
        "price_high": 499,
        "setup_time": "2-3 days",
        "tech_ready": True,
        "keywords": ["chatbot", "chat", "customer service", "support", "widget", "live chat"],
    },
    "website_build": {
        "name": "Professional Website (SEO-Optimized)",
        "description": "Fast, mobile-first website deployed on Cloudflare CDN. Unlimited pages, SEO-optimized, conversion-focused. Includes local SEO for service areas.",
        "price_range": "$500-$2,500 one-time + $49/mo hosting",
        "price_low": 500,
        "price_high": 2500,
        "setup_time": "1-3 days",
        "tech_ready": True,
        "keywords": ["website", "web design", "landing page", "SEO", "web development", "site"],
    },
    "ai_automation": {
        "name": "Business Process Automation",
        "description": "Automate repetitive tasks: data entry, report generation, email processing, lead routing, CRM updates. Custom Python scripts + AI.",
        "price_range": "$500-$5,000 per project",
        "price_low": 500,
        "price_high": 5000,
        "setup_time": "1-2 weeks",
        "tech_ready": True,
        "keywords": ["automation", "automate", "workflow", "script", "bot", "RPA", "process"],
    },
    "content_production": {
        "name": "AI Content Production Pipeline",
        "description": "YouTube videos, shorts, podcasts, social media content — produced with AI tools at scale. Includes SEO optimization.",
        "price_range": "$200-$1,000/mo",
        "price_low": 200,
        "price_high": 1000,
        "setup_time": "1 week",
        "tech_ready": True,
        "keywords": ["content", "video", "youtube", "podcast", "social media", "shorts", "tiktok"],
    },
    "custom_ai_model": {
        "name": "Custom AI Model Training",
        "description": "Fine-tune AI models on your business data. Customer service, sales scripts, domain-specific Q&A. Runs on your infrastructure or ours.",
        "price_range": "$1,000-$10,000 per model",
        "price_low": 1000,
        "price_high": 10000,
        "setup_time": "2-4 weeks",
        "tech_ready": True,
        "keywords": ["model", "training", "fine-tune", "custom AI", "machine learning", "LLM"],
    },
    "voice_cloning": {
        "name": "AI Voice Cloning / TTS",
        "description": "Clone any voice for content production, IVR systems, podcast hosting. Multiple TTS engines available.",
        "price_range": "$200-$1,000 per voice",
        "price_low": 200,
        "price_high": 1000,
        "setup_time": "2-5 days",
        "tech_ready": True,
        "keywords": ["voice", "TTS", "text to speech", "clone", "voiceover", "narration"],
    },
    "web_scraping": {
        "name": "Web Scraping & Data Collection",
        "description": "Custom scrapers for any public data source. Competitor monitoring, price tracking, lead generation, market research.",
        "price_range": "$200-$2,000 per project",
        "price_low": 200,
        "price_high": 2000,
        "setup_time": "1-5 days",
        "tech_ready": True,
        "keywords": ["scraping", "scraper", "data", "extraction", "crawl", "mining"],
    },
    "lead_generation": {
        "name": "AI Lead Generation System",
        "description": "Automated lead gen: SEO landing pages, Google Business optimization, AI cold outreach, follow-up sequences.",
        "price_range": "$299-$799/mo",
        "price_low": 299,
        "price_high": 799,
        "setup_time": "1-2 weeks",
        "tech_ready": True,
        "keywords": ["lead", "leads", "generation", "prospect", "outreach", "marketing"],
    },
}

# ==========================================================================
# SEARCH QUERIES BY CATEGORY
# ==========================================================================
FREELANCE_QUERIES = [
    "AI chatbot development",
    "AI automation",
    "Python automation",
    "voice agent",
    "AI phone system",
    "web scraping",
    "chatbot for business",
    "AI integration",
    "machine learning",
    "data analysis Python",
    "AI content generation",
    "custom GPT",
    "LLM fine-tuning",
    "AI voice assistant",
    "Twilio voice",
    "automated calling system",
    "website builder",
    "landing page design",
    "SEO optimization",
    "lead generation automation",
]

REMOTE_JOB_QUERIES = [
    "AI engineer",
    "ML engineer",
    "Python developer",
    "automation engineer",
    "AI developer remote",
    "NLP engineer",
    "chatbot developer",
    "voice AI engineer",
]

LOCAL_QUERIES = [
    "locksmith {city}",
    "plumber {city}",
    "electrician {city}",
    "HVAC {city}",
    "pest control {city}",
    "cleaning service {city}",
    "lawn care {city}",
    "pool service {city}",
    "roofing {city}",
    "garage door {city}",
    "auto repair {city}",
    "dentist {city}",
    "veterinarian {city}",
    "restaurant {city}",
    "hair salon {city}",
]

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
            CREATE TABLE IF NOT EXISTS opportunities (
                id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                category TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                url TEXT,
                client_name TEXT,
                location TEXT,
                budget_low REAL,
                budget_high REAL,
                budget_text TEXT,
                skills_needed TEXT,
                posted_at TEXT,
                found_at TEXT DEFAULT (datetime('now')),
                score_revenue REAL DEFAULT 0,
                score_effort REAL DEFAULT 0,
                score_skill_match REAL DEFAULT 0,
                score_timeline REAL DEFAULT 0,
                score_total REAL DEFAULT 0,
                matching_service TEXT,
                status TEXT DEFAULT 'new',
                pitch_id TEXT,
                notes TEXT
            );

            CREATE TABLE IF NOT EXISTS pitches (
                id TEXT PRIMARY KEY,
                opportunity_id TEXT,
                service_type TEXT,
                pitch_text TEXT NOT NULL,
                subject_line TEXT,
                price_proposed TEXT,
                generated_at TEXT DEFAULT (datetime('now')),
                status TEXT DEFAULT 'draft',
                sent_at TEXT,
                response TEXT,
                FOREIGN KEY (opportunity_id) REFERENCES opportunities(id)
            );

            CREATE TABLE IF NOT EXISTS market_rates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                service_type TEXT NOT NULL,
                platform TEXT NOT NULL,
                title TEXT,
                price_low REAL,
                price_high REAL,
                price_text TEXT,
                reviews_count INTEGER DEFAULT 0,
                rating REAL,
                seller_name TEXT,
                seller_url TEXT,
                url TEXT,
                found_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS local_businesses (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                category TEXT,
                city TEXT,
                phone TEXT,
                website TEXT,
                has_website INTEGER DEFAULT 0,
                website_quality TEXT,
                google_rating REAL,
                review_count INTEGER DEFAULT 0,
                needs_identified TEXT,
                opportunity_score REAL DEFAULT 0,
                found_at TEXT DEFAULT (datetime('now')),
                contacted INTEGER DEFAULT 0,
                notes TEXT
            );

            CREATE TABLE IF NOT EXISTS scans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scan_type TEXT NOT NULL,
                source TEXT,
                query TEXT,
                results_count INTEGER DEFAULT 0,
                status TEXT DEFAULT 'pending',
                started_at TEXT DEFAULT (datetime('now')),
                finished_at TEXT,
                error TEXT
            );

            CREATE TABLE IF NOT EXISTS strategy_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recommendation TEXT NOT NULL,
                category TEXT,
                priority INTEGER DEFAULT 5,
                revenue_potential TEXT,
                action_items TEXT,
                generated_at TEXT DEFAULT (datetime('now')),
                acted_on INTEGER DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_opp_score ON opportunities(score_total DESC);
            CREATE INDEX IF NOT EXISTS idx_opp_source ON opportunities(source);
            CREATE INDEX IF NOT EXISTS idx_opp_status ON opportunities(status);
            CREATE INDEX IF NOT EXISTS idx_opp_category ON opportunities(category);
            CREATE INDEX IF NOT EXISTS idx_pitch_status ON pitches(status);
            CREATE INDEX IF NOT EXISTS idx_rates_service ON market_rates(service_type);
            CREATE INDEX IF NOT EXISTS idx_local_score ON local_businesses(opportunity_score DESC);
            CREATE INDEX IF NOT EXISTS idx_local_city ON local_businesses(city);
        """)


# ==========================================================================
# OLLAMA
# ==========================================================================

def ollama_generate(prompt: str, timeout: float = 45.0) -> str:
    """Synchronous Ollama call for background threads."""
    try:
        with httpx.Client(timeout=timeout) as client:
            resp = client.post(
                f"{OLLAMA_URL}/api/generate",
                json={
                    "model": OLLAMA_MODEL,
                    "prompt": prompt,
                    "stream": False,
                    "options": {"temperature": 0.4, "num_predict": 500}
                }
            )
            if resp.status_code == 200:
                return resp.json().get("response", "").strip()
    except Exception:
        pass
    return ""


async def ollama_generate_async(prompt: str, timeout: float = 45.0) -> str:
    """Async Ollama call for API endpoints."""
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(
                f"{OLLAMA_URL}/api/generate",
                json={
                    "model": OLLAMA_MODEL,
                    "prompt": prompt,
                    "stream": False,
                    "options": {"temperature": 0.4, "num_predict": 500}
                }
            )
            if resp.status_code == 200:
                return resp.json().get("response", "").strip()
    except Exception:
        pass
    return ""


# ==========================================================================
# SCORING ENGINE
# ==========================================================================

def score_opportunity(opp: dict) -> dict:
    """Score an opportunity 0-100 on four dimensions plus total."""
    title = (opp.get("title") or "").lower()
    desc = (opp.get("description") or "").lower()
    combined = f"{title} {desc}"
    budget_low = opp.get("budget_low") or 0
    budget_high = opp.get("budget_high") or 0
    source = opp.get("source", "")

    # --- Revenue potential (0-100) ---
    if budget_high >= 5000:
        rev = 95
    elif budget_high >= 2000:
        rev = 85
    elif budget_high >= 1000:
        rev = 75
    elif budget_high >= 500:
        rev = 60
    elif budget_high >= 200:
        rev = 45
    elif budget_high >= 50:
        rev = 30
    else:
        # No budget listed — estimate from keywords
        rev = 50  # unknown budget, moderate assumption
        if any(w in combined for w in ["enterprise", "saas", "monthly", "recurring", "ongoing"]):
            rev = 70
        if any(w in combined for w in ["free", "volunteer", "nonprofit", "student project"]):
            rev = 15

    # --- Effort required (0-100, higher = LESS effort = better) ---
    effort = 50  # default moderate
    # Quick wins
    if any(w in combined for w in ["simple", "quick", "small", "basic", "easy"]):
        effort += 20
    if any(w in combined for w in ["landing page", "one page", "single page"]):
        effort += 25
    if any(w in combined for w in ["scraping", "scraper", "data collection"]):
        effort += 15
    if any(w in combined for w in ["chatbot", "chat bot", "widget"]):
        effort += 15
    # High effort
    if any(w in combined for w in ["complex", "large scale", "enterprise", "full stack"]):
        effort -= 20
    if any(w in combined for w in ["mobile app", "ios", "android", "react native"]):
        effort -= 30
    if any(w in combined for w in ["blockchain", "crypto", "nft", "web3"]):
        effort -= 25
    effort = max(5, min(100, effort))

    # --- Skill match (0-100) ---
    skill = 20  # baseline
    best_match = None
    best_match_score = 0
    for svc_key, svc in OUR_SERVICES.items():
        match_count = sum(1 for kw in svc["keywords"] if kw.lower() in combined)
        if match_count > best_match_score:
            best_match_score = match_count
            best_match = svc_key
    if best_match_score >= 3:
        skill = 95
    elif best_match_score >= 2:
        skill = 80
    elif best_match_score >= 1:
        skill = 60

    # Boost for our core strengths
    if any(w in combined for w in ["python", "fastapi", "flask", "django"]):
        skill = min(100, skill + 15)
    if any(w in combined for w in ["ai", "artificial intelligence", "machine learning", "llm", "gpt"]):
        skill = min(100, skill + 10)
    if any(w in combined for w in ["twilio", "telephony", "voip", "sip"]):
        skill = min(100, skill + 20)
    if any(w in combined for w in ["selenium", "playwright", "beautifulsoup", "httpx"]):
        skill = min(100, skill + 15)

    # --- Timeline (0-100, higher = faster revenue) ---
    timeline = 50
    if any(w in combined for w in ["urgent", "asap", "immediately", "today", "rush"]):
        timeline = 90
    if any(w in combined for w in ["this week", "few days", "quick turnaround"]):
        timeline = 80
    if any(w in combined for w in ["ongoing", "long term", "monthly", "retainer"]):
        timeline = 85  # recurring = great
    if any(w in combined for w in ["no rush", "whenever", "flexible timeline"]):
        timeline = 40

    # --- Total (weighted) ---
    total = (rev * 0.35) + (skill * 0.30) + (effort * 0.20) + (timeline * 0.15)

    return {
        "score_revenue": round(rev, 1),
        "score_effort": round(effort, 1),
        "score_skill_match": round(skill, 1),
        "score_timeline": round(timeline, 1),
        "score_total": round(total, 1),
        "matching_service": best_match,
    }


# ==========================================================================
# SCRAPING — UPWORK (RSS feeds, public search)
# ==========================================================================

def scrape_upwork(query: str) -> List[dict]:
    """Scrape Upwork RSS feed for public job postings."""
    results = []
    try:
        # Try multiple RSS URL formats
        rss_urls = [
            f"https://www.upwork.com/ab/feed/jobs/rss?q={quote_plus(query)}&sort=recency",
            f"https://www.upwork.com/ab/feed/jobs/atom?q={quote_plus(query)}&sort=recency",
        ]
        text = ""
        upwork_headers = {
            **HEADERS,
            "Accept": "application/rss+xml, application/xml, text/xml, */*",
            "Referer": "https://www.upwork.com/",
        }
        for rss_url in rss_urls:
            with httpx.Client(timeout=20, headers=upwork_headers, follow_redirects=True) as client:
                resp = client.get(rss_url)
                if resp.status_code == 200 and ("<item>" in resp.text or "<entry>" in resp.text):
                    text = resp.text
                    break
        if not text:
            return results

        # Parse RSS items (simple regex for XML)
        items = re.findall(r'<item>(.*?)</item>', text, re.DOTALL)
        for item_xml in items[:10]:
            title_m = re.search(r'<title><!\[CDATA\[(.*?)\]\]></title>', item_xml)
            if not title_m:
                title_m = re.search(r'<title>(.*?)</title>', item_xml)
            link_m = re.search(r'<link>(.*?)</link>', item_xml)
            desc_m = re.search(r'<description><!\[CDATA\[(.*?)\]\]></description>', item_xml, re.DOTALL)
            if not desc_m:
                desc_m = re.search(r'<description>(.*?)</description>', item_xml, re.DOTALL)
            pub_m = re.search(r'<pubDate>(.*?)</pubDate>', item_xml)

            title = html_lib.unescape(title_m.group(1).strip()) if title_m else ""
            url = link_m.group(1).strip() if link_m else ""
            desc_raw = desc_m.group(1).strip() if desc_m else ""
            posted = pub_m.group(1).strip() if pub_m else ""

            # Clean HTML from description
            desc_clean = re.sub(r'<[^>]+>', ' ', desc_raw)
            desc_clean = re.sub(r'\s+', ' ', desc_clean).strip()

            # Extract budget
            budget_low, budget_high, budget_text = 0, 0, ""
            budget_m = re.search(r'\$[\d,]+(?:\s*-\s*\$[\d,]+)?', desc_clean)
            if budget_m:
                budget_text = budget_m.group(0)
                nums = re.findall(r'[\d,]+', budget_text)
                nums = [int(n.replace(',', '')) for n in nums]
                if len(nums) >= 2:
                    budget_low, budget_high = nums[0], nums[1]
                elif nums:
                    budget_low = budget_high = nums[0]

            # Extract skills
            skills = []
            skills_m = re.findall(r'(?:Skills?|Category):\s*(.*?)(?:\n|$)', desc_clean)
            if skills_m:
                for s in skills_m:
                    skills.extend([x.strip() for x in s.split(',') if x.strip()])

            if title:
                opp_id = hashlib.md5(f"upwork:{url or title}".encode()).hexdigest()[:16]
                results.append({
                    "id": opp_id,
                    "source": "upwork",
                    "category": "freelance",
                    "title": title[:500],
                    "description": desc_clean[:2000],
                    "url": url,
                    "budget_low": budget_low,
                    "budget_high": budget_high,
                    "budget_text": budget_text,
                    "skills_needed": json.dumps(skills[:20]),
                    "posted_at": posted,
                })
    except Exception as e:
        print(f"[upwork] Error scraping '{query}': {e}")
    return results


# ==========================================================================
# SCRAPING — FIVERR (market rates research)
# ==========================================================================

def scrape_fiverr_rates(query: str) -> List[dict]:
    """Scrape Fiverr search to understand market rates and what sells."""
    results = []
    try:
        url = f"https://www.fiverr.com/search/gigs?query={quote_plus(query)}&source=top-bar&ref_ctx_id=search"
        with httpx.Client(timeout=20, headers=HEADERS, follow_redirects=True) as client:
            resp = client.get(url)
            if resp.status_code != 200:
                return results
            text = resp.text

        # Extract gig data from page (JSON-LD or structured data)
        # Fiverr embeds gig data in script tags
        gig_pattern = re.compile(
            r'"gig_title"\s*:\s*"([^"]+)".*?'
            r'"price"\s*:\s*(\d+)',
            re.DOTALL
        )
        matches = gig_pattern.findall(text)

        # Also try to get from visible HTML
        card_titles = re.findall(r'<h3[^>]*class="[^"]*"[^>]*>(.*?)</h3>', text)
        card_prices = re.findall(r'<span[^>]*class="[^"]*price[^"]*"[^>]*>\$?([\d,.]+)</span>', text, re.I)
        card_ratings = re.findall(r'<span[^>]*class="[^"]*rating[^"]*"[^>]*>([\d.]+)</span>', text, re.I)
        card_reviews = re.findall(r'\((\d+[kK]?)\)', text)

        # Parse JSON data if available
        json_match = re.search(r'window\.__INITIAL_PROPS__\s*=\s*({.*?});', text, re.DOTALL)
        if json_match:
            try:
                data = json.loads(json_match.group(1))
                gigs = []
                # Navigate Fiverr's JSON structure
                if isinstance(data, dict):
                    for key in ["props", "pageProps", "searchResults"]:
                        if key in data:
                            data = data[key]
                    if isinstance(data, dict) and "gigs" in data:
                        gigs = data["gigs"]
                    elif isinstance(data, list):
                        gigs = data

                for gig in gigs[:15]:
                    if isinstance(gig, dict):
                        title = gig.get("title") or gig.get("gig_title", "")
                        price = gig.get("price") or gig.get("starting_price", 0)
                        rating = gig.get("rating") or gig.get("seller_rating", 0)
                        reviews = gig.get("reviews_count") or gig.get("num_of_reviews", 0)
                        seller = gig.get("seller_name") or gig.get("seller", {}).get("username", "")
                        gig_url = gig.get("url") or gig.get("gig_url", "")
                        if title:
                            results.append({
                                "service_type": query,
                                "platform": "fiverr",
                                "title": html_lib.unescape(str(title))[:500],
                                "price_low": float(price) if price else 0,
                                "price_high": float(price) if price else 0,
                                "price_text": f"${price}" if price else "",
                                "reviews_count": int(reviews) if reviews else 0,
                                "rating": float(rating) if rating else 0,
                                "seller_name": str(seller),
                                "seller_url": f"https://www.fiverr.com{gig_url}" if gig_url and gig_url.startswith("/") else str(gig_url),
                                "url": f"https://www.fiverr.com{gig_url}" if gig_url and gig_url.startswith("/") else str(gig_url),
                            })
            except (json.JSONDecodeError, KeyError, TypeError):
                pass

        # Fallback: parse from HTML card titles
        if not results and card_titles:
            for i, title_raw in enumerate(card_titles[:15]):
                title = re.sub(r'<[^>]+>', '', title_raw).strip()
                title = html_lib.unescape(title)
                price = 0
                if i < len(card_prices):
                    try:
                        price = float(card_prices[i].replace(',', ''))
                    except ValueError:
                        pass
                rating = 0
                if i < len(card_ratings):
                    try:
                        rating = float(card_ratings[i])
                    except ValueError:
                        pass
                revs = 0
                if i < len(card_reviews):
                    r = card_reviews[i]
                    if 'k' in r.lower():
                        revs = int(float(r.lower().replace('k', '')) * 1000)
                    else:
                        try:
                            revs = int(r)
                        except ValueError:
                            pass
                if title:
                    results.append({
                        "service_type": query,
                        "platform": "fiverr",
                        "title": title[:500],
                        "price_low": price,
                        "price_high": price,
                        "price_text": f"${price}" if price else "",
                        "reviews_count": revs,
                        "rating": rating,
                        "seller_name": "",
                        "seller_url": "",
                        "url": f"https://www.fiverr.com/search/gigs?query={quote_plus(query)}",
                    })

    except Exception as e:
        print(f"[fiverr] Error scraping '{query}': {e}")
    return results


# ==========================================================================
# SCRAPING — FREELANCER.COM
# ==========================================================================

def scrape_freelancer(query: str) -> List[dict]:
    """Scrape Freelancer.com public API for projects."""
    results = []
    try:
        # Freelancer has a public API for searching projects
        api_url = "https://www.freelancer.com/api/projects/0.1/projects/active/"
        params = {
            "query": query,
            "compact": "true",
            "limit": 10,
            "sort_field": "time_updated",
            "job_details": "true",
        }
        with httpx.Client(timeout=20, headers=HEADERS, follow_redirects=True) as client:
            resp = client.get(api_url, params=params)
            if resp.status_code != 200:
                return results
            data = resp.json()

        projects = data.get("result", {}).get("projects", [])
        for proj in projects[:10]:
            title = proj.get("title", "")
            desc = proj.get("preview_description") or proj.get("description", "")
            seo_url = proj.get("seo_url", "")
            budget = proj.get("budget", {})
            budget_min = budget.get("minimum", 0)
            budget_max = budget.get("maximum", 0)
            currency = proj.get("currency", {}).get("code", "USD")
            jobs = proj.get("jobs", [])
            skills = [j.get("name", "") for j in jobs if isinstance(j, dict)]
            proj_id = proj.get("id", "")

            if title:
                opp_id = hashlib.md5(f"freelancer:{proj_id or title}".encode()).hexdigest()[:16]
                results.append({
                    "id": opp_id,
                    "source": "freelancer",
                    "category": "freelance",
                    "title": title[:500],
                    "description": desc[:2000],
                    "url": f"https://www.freelancer.com/projects/{seo_url}" if seo_url else "",
                    "budget_low": float(budget_min) if budget_min else 0,
                    "budget_high": float(budget_max) if budget_max else 0,
                    "budget_text": f"${budget_min}-${budget_max} {currency}" if budget_min else "",
                    "skills_needed": json.dumps(skills[:20]),
                    "posted_at": "",
                })
    except Exception as e:
        print(f"[freelancer] Error scraping '{query}': {e}")
    return results


# ==========================================================================
# SCRAPING — REMOTEOK
# ==========================================================================

def scrape_remoteok(query: str) -> List[dict]:
    """Scrape RemoteOK JSON API for remote jobs."""
    results = []
    try:
        # RemoteOK API requires a specific user agent
        remoteok_headers = {
            "User-Agent": "HiveDynamics/1.0 (revenue research)",
            "Accept": "application/json",
        }
        # Try tag-based and query-based endpoints
        urls_to_try = [
            f"https://remoteok.com/api?tag={quote_plus(query.lower().replace(' ', '-'))}",
            f"https://remoteok.com/api?tag={quote_plus(query.split()[0].lower())}",
        ]
        data = None
        for url in urls_to_try:
            try:
                with httpx.Client(timeout=20, headers=remoteok_headers, follow_redirects=True) as client:
                    resp = client.get(url)
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, list) and len(data) > 1:
                        break
            except Exception:
                continue
        if not data or not isinstance(data, list):
            return results

        # First item is metadata, rest are jobs
        jobs = data[1:] if len(data) > 1 else []
        for job in jobs[:10]:
            title = job.get("position", "")
            company = job.get("company", "")
            desc = job.get("description", "")
            salary_min = job.get("salary_min", 0)
            salary_max = job.get("salary_max", 0)
            tags = job.get("tags", [])
            job_url = job.get("url", "")
            posted = job.get("date", "")

            # Clean HTML
            desc_clean = re.sub(r'<[^>]+>', ' ', str(desc))
            desc_clean = re.sub(r'\s+', ' ', desc_clean).strip()[:2000]

            if title:
                opp_id = hashlib.md5(f"remoteok:{job_url or title}".encode()).hexdigest()[:16]
                salary_text = ""
                if salary_min and salary_max:
                    salary_text = f"${salary_min:,.0f}-${salary_max:,.0f}/yr"
                elif salary_min:
                    salary_text = f"${salary_min:,.0f}/yr"

                results.append({
                    "id": opp_id,
                    "source": "remoteok",
                    "category": "remote_job",
                    "title": f"{title} at {company}" if company else title,
                    "description": desc_clean,
                    "url": f"https://remoteok.com{job_url}" if job_url.startswith("/") else job_url,
                    "client_name": company,
                    "budget_low": float(salary_min) / 12 if salary_min else 0,  # monthly
                    "budget_high": float(salary_max) / 12 if salary_max else 0,
                    "budget_text": salary_text,
                    "skills_needed": json.dumps(tags[:20]) if tags else "[]",
                    "posted_at": posted,
                })
    except Exception as e:
        print(f"[remoteok] Error scraping '{query}': {e}")
    return results


# ==========================================================================
# SCRAPING — WE WORK REMOTELY
# ==========================================================================

def scrape_weworkremotely(query: str) -> List[dict]:
    """Scrape We Work Remotely RSS for remote jobs."""
    results = []
    try:
        # WWR has category RSS feeds
        categories = {
            "programming": "https://weworkremotely.com/categories/remote-full-stack-programming-jobs.rss",
            "devops": "https://weworkremotely.com/categories/remote-devops-sysadmin-jobs.rss",
        }
        for cat, rss_url in categories.items():
            with httpx.Client(timeout=20, headers=HEADERS, follow_redirects=True) as client:
                resp = client.get(rss_url)
                if resp.status_code != 200:
                    continue
                text = resp.text

            items = re.findall(r'<item>(.*?)</item>', text, re.DOTALL)
            query_lower = query.lower()
            for item_xml in items[:20]:
                title_m = re.search(r'<title><!\[CDATA\[(.*?)\]\]></title>', item_xml)
                if not title_m:
                    title_m = re.search(r'<title>(.*?)</title>', item_xml)
                link_m = re.search(r'<link>(.*?)</link>', item_xml)
                desc_m = re.search(r'<description><!\[CDATA\[(.*?)\]\]></description>', item_xml, re.DOTALL)
                pub_m = re.search(r'<pubDate>(.*?)</pubDate>', item_xml)

                title = html_lib.unescape(title_m.group(1).strip()) if title_m else ""
                url = link_m.group(1).strip() if link_m else ""
                desc_raw = desc_m.group(1).strip() if desc_m else ""
                posted = pub_m.group(1).strip() if pub_m else ""

                # Filter by query relevance
                combined = f"{title} {desc_raw}".lower()
                if not any(w in combined for w in query_lower.split()):
                    continue

                desc_clean = re.sub(r'<[^>]+>', ' ', desc_raw)
                desc_clean = re.sub(r'\s+', ' ', desc_clean).strip()[:2000]

                if title:
                    opp_id = hashlib.md5(f"wwr:{url or title}".encode()).hexdigest()[:16]
                    results.append({
                        "id": opp_id,
                        "source": "weworkremotely",
                        "category": "remote_job",
                        "title": title[:500],
                        "description": desc_clean,
                        "url": url,
                        "budget_low": 0,
                        "budget_high": 0,
                        "budget_text": "",
                        "skills_needed": "[]",
                        "posted_at": posted,
                    })
    except Exception as e:
        print(f"[wwr] Error scraping '{query}': {e}")
    return results


# ==========================================================================
# SCRAPING — LOCAL BUSINESSES (Google)
# ==========================================================================

def scrape_local_google(query: str) -> List[dict]:
    """Search Google for local businesses to prospect."""
    results = []
    try:
        search_url = f"https://www.google.com/search?q={quote_plus(query)}&num=10"
        with httpx.Client(timeout=20, headers=HEADERS, follow_redirects=True) as client:
            resp = client.get(search_url)
            if resp.status_code != 200:
                return results
            text = resp.text

        # Extract business info from search results
        # Look for local pack results (map results)
        # Extract business names, ratings, phone numbers, websites
        biz_pattern = re.compile(
            r'class="[^"]*"[^>]*>([\w\s&\'-]+(?:Locksmith|Plumbing|Electric|HVAC|Cleaning|Lawn|Pool|Roofing|Garage|Auto|Dental|Vet|Salon)[^<]*)</[^>]+>',
            re.I
        )
        names_found = biz_pattern.findall(text)

        # Extract ratings
        rating_pattern = re.compile(r'(\d\.\d)\s*(?:star|rating|\([\d,]+\))', re.I)
        ratings = rating_pattern.findall(text)

        # Extract phone numbers (850 area code)
        phone_pattern = re.compile(r'\(?(850)\)?\s*[-.]?\s*(\d{3})\s*[-.]?\s*(\d{4})')
        phones = phone_pattern.findall(text)

        # Extract URLs from search results
        url_pattern = re.compile(r'href="(https?://(?!www\.google|maps\.google|play\.google|support\.google)[^"]+)"')
        urls = url_pattern.findall(text)

        # Determine city from query
        city = ""
        for c in OKALOOSA_CITIES:
            if c.lower() in query.lower():
                city = c
                break

        # Combine what we found
        for i, name in enumerate(names_found[:10]):
            name = re.sub(r'<[^>]+>', '', name).strip()
            name = html_lib.unescape(name)
            if len(name) < 3 or len(name) > 100:
                continue

            phone = ""
            if i < len(phones):
                p = phones[i]
                phone = f"({p[0]}) {p[1]}-{p[2]}"

            website = ""
            has_website = 0
            if i < len(urls):
                website = urls[i]
                has_website = 1

            rating = 0
            if i < len(ratings):
                try:
                    rating = float(ratings[i])
                except ValueError:
                    pass

            biz_id = hashlib.md5(f"local:{name}:{city}".encode()).hexdigest()[:16]
            results.append({
                "id": biz_id,
                "name": name[:200],
                "category": query.split()[0] if query else "business",
                "city": city or REGION,
                "phone": phone,
                "website": website,
                "has_website": has_website,
                "google_rating": rating,
            })
    except Exception as e:
        print(f"[google] Error scraping '{query}': {e}")
    return results


# ==========================================================================
# PITCH GENERATION
# ==========================================================================

def generate_pitch(opp: dict, service: dict) -> dict:
    """Generate a pitch/proposal for an opportunity using Ollama."""
    opp_title = opp.get("title", "Unknown")
    opp_desc = (opp.get("description") or "")[:500]
    opp_budget = opp.get("budget_text") or "Not specified"
    svc_name = service.get("name", "")
    svc_desc = service.get("description", "")
    svc_price = service.get("price_range", "")
    svc_setup = service.get("setup_time", "")

    prompt = f"""You are a professional freelancer writing a proposal for a client.
Write a concise, compelling pitch for this opportunity.

JOB: {opp_title}
DESCRIPTION: {opp_desc}
BUDGET: {opp_budget}

OUR SERVICE: {svc_name}
WHAT WE OFFER: {svc_desc}
OUR PRICE: {svc_price}
DELIVERY TIME: {svc_setup}

Write the pitch in this format:
SUBJECT: [one-line subject]
---
[2-3 paragraph pitch that]:
- Opens with understanding their specific need
- Shows relevant expertise (AI, Python, automation)
- Proposes a clear solution with deliverables
- Mentions timeline and price (competitive with their budget)
- Ends with a clear call to action

Keep it under 200 words. Be specific, not generic. Sound human, not robotic."""

    pitch_text = ollama_generate(prompt, timeout=60)

    if not pitch_text:
        # Fallback template
        pitch_text = f"""SUBJECT: Expert {svc_name} — Ready to Start Today
---
Hi,

I noticed your project "{opp_title}" and it's exactly what I specialize in.

I offer {svc_name}: {svc_desc}

I can deliver this in {svc_setup} at a competitive rate ({svc_price}).

My tech stack includes Python, FastAPI, multiple AI/ML frameworks, and production deployment on Cloudflare CDN. I've built similar systems for other clients and can show you examples.

Would you like to discuss the details? I'm available to start immediately.

Best regards,
Hive Dynamics"""

    # Extract subject line
    subject = ""
    lines = pitch_text.split('\n')
    for line in lines:
        if line.strip().upper().startswith("SUBJECT:"):
            subject = line.split(":", 1)[1].strip()
            break
    if not subject:
        subject = f"Expert {svc_name} for Your Project"

    # Determine proposed price
    budget_high = opp.get("budget_high", 0)
    if budget_high > 0:
        proposed = f"${int(budget_high * 0.8)}-${int(budget_high)}"
    else:
        proposed = svc_price

    pitch_id = hashlib.md5(f"pitch:{opp.get('id', '')}:{time.time()}".encode()).hexdigest()[:16]
    return {
        "id": pitch_id,
        "opportunity_id": opp.get("id", ""),
        "service_type": service.get("name", ""),
        "pitch_text": pitch_text,
        "subject_line": subject,
        "price_proposed": proposed,
    }


# ==========================================================================
# NERVE INTEGRATION
# ==========================================================================

def feed_to_nerve(fact: str, category: str = "revenue_opportunity"):
    """Send a fact to the nerve on ZeroZI."""
    try:
        with httpx.Client(timeout=10) as client:
            client.post(NERVE_URL, json={
                "fact": fact[:500],
                "category": category,
                "source": "revenue_hunter",
                "confidence": 0.8,
            })
    except Exception:
        pass


# ==========================================================================
# STRATEGY ENGINE
# ==========================================================================

def generate_strategy() -> List[dict]:
    """Analyze all data and generate prioritized strategy recommendations."""
    strategies = []

    with get_db() as db:
        # Count opportunities by source
        source_counts = {}
        for row in db.execute("SELECT source, COUNT(*) as cnt, AVG(score_total) as avg_score FROM opportunities GROUP BY source"):
            source_counts[row["source"]] = {"count": row["cnt"], "avg_score": row["avg_score"]}

        # Top scoring opportunities
        top_opps = db.execute(
            "SELECT * FROM opportunities WHERE status='new' ORDER BY score_total DESC LIMIT 5"
        ).fetchall()

        # Market rate analysis
        rate_data = {}
        for row in db.execute(
            "SELECT service_type, AVG(price_low) as avg_low, AVG(price_high) as avg_high, "
            "AVG(reviews_count) as avg_reviews, COUNT(*) as cnt "
            "FROM market_rates GROUP BY service_type"
        ):
            rate_data[row["service_type"]] = {
                "avg_low": row["avg_low"],
                "avg_high": row["avg_high"],
                "avg_reviews": row["avg_reviews"],
                "count": row["cnt"],
            }

        # Local business analysis
        local_stats = db.execute(
            "SELECT category, COUNT(*) as cnt, AVG(google_rating) as avg_rating "
            "FROM local_businesses GROUP BY category"
        ).fetchall()

    # Strategy 1: Highest-value immediate opportunities
    if top_opps:
        items = []
        for opp in top_opps:
            items.append(f"- {opp['title'][:60]} (score: {opp['score_total']}, budget: {opp['budget_text'] or 'N/A'})")
        strategies.append({
            "recommendation": "Apply to these top-scoring opportunities NOW",
            "category": "immediate_action",
            "priority": 1,
            "revenue_potential": f"${sum(o['budget_high'] for o in top_opps if o['budget_high']):,.0f} total",
            "action_items": json.dumps(items),
        })

    # Strategy 2: Most in-demand services
    for svc_type, data in rate_data.items():
        if data["avg_reviews"] > 50 and data["count"] >= 3:
            strategies.append({
                "recommendation": f"Create Fiverr gig for '{svc_type}' — high demand ({data['count']} competitors, avg {data['avg_reviews']:.0f} reviews)",
                "category": "fiverr_gig",
                "priority": 2,
                "revenue_potential": f"${data['avg_low']:.0f}-${data['avg_high']:.0f} per gig",
                "action_items": json.dumps([
                    f"Create gig titled: Professional {svc_type.replace('_', ' ').title()}",
                    f"Price competitively: ${data['avg_low']:.0f}-${data['avg_high']:.0f}",
                    "Create portfolio samples using our existing tech",
                    "Set delivery time to 2-3 days (faster than avg)",
                ]),
            })

    # Strategy 3: Local market gaps
    for stat in local_stats:
        if stat["cnt"] < 5 or stat["avg_rating"] and stat["avg_rating"] < 4.0:
            strategies.append({
                "recommendation": f"Target local {stat['category']} businesses — {stat['cnt']} found, avg rating {stat['avg_rating']:.1f}",
                "category": "local_outreach",
                "priority": 3,
                "revenue_potential": "$299-$999/mo per client (AI phone + website)",
                "action_items": json.dumps([
                    f"Call local {stat['category']} businesses with low ratings",
                    "Offer free AI phone demo",
                    "Show them their competitor's better online presence",
                    "Package: Website + AI Phone = $499/mo starter",
                ]),
            })

    # Strategy 4: Fastest path to $1K/mo
    strategies.append({
        "recommendation": "FASTEST $1K/mo path: 2 local businesses on AI Phone ($499/mo each)",
        "category": "fastest_revenue",
        "priority": 1,
        "revenue_potential": "$998/mo recurring",
        "action_items": json.dumps([
            "Cold call 20 local service businesses (locksmith, plumber, HVAC)",
            "Offer free 7-day trial of AI phone answering",
            "Demonstrate missed call recovery (businesses lose $1000+/mo to missed calls)",
            "Close at $499/mo with 3-month commitment",
            "ALTERNATIVE: 5 website builds at $500 each = $2,500 one-time",
            "ALTERNATIVE: 10 Fiverr gigs at $100 each = $1,000",
        ]),
    })

    # Strategy 5: Recurring vs one-time
    strategies.append({
        "recommendation": "Prioritize RECURRING revenue: AI Phone ($499/mo) > Chatbot ($199/mo) > Lead Gen ($299/mo)",
        "category": "revenue_model",
        "priority": 2,
        "revenue_potential": "$997/mo per client on full stack",
        "action_items": json.dumps([
            "Bundle: AI Phone + Chatbot + Website = $699/mo (save $248)",
            "Target businesses spending $500+/mo on Google Ads (they have budget)",
            "Show ROI: 'Our AI answers 24/7, you never miss a lead'",
            "Upsell path: phone -> chatbot -> lead gen -> full marketing",
        ]),
    })

    return strategies


# ==========================================================================
# BACKGROUND SCANNER
# ==========================================================================

_scanner_running = False
_last_scan_time = None
_scan_stats = {"total_scans": 0, "total_opportunities": 0, "total_pitches": 0, "errors": 0}


def run_full_scan():
    """Run a complete scan across all sources."""
    global _last_scan_time, _scan_stats
    print(f"[scan] Starting full scan at {datetime.now().isoformat()}")

    total_new = 0
    total_rates = 0
    total_local = 0
    errors = 0

    # ── PHASE 1: Freelance platforms ──
    print("[scan] Phase 1: Freelance platforms")
    for query in FREELANCE_QUERIES[:8]:  # Limit to avoid rate-limiting
        scan_id = _log_scan_start("freelance", "upwork", query)
        try:
            opps = scrape_upwork(query)
            count = _store_opportunities(opps)
            total_new += count
            _log_scan_end(scan_id, count)
            time.sleep(2)  # polite delay
        except Exception as e:
            errors += 1
            _log_scan_end(scan_id, 0, str(e))

        scan_id = _log_scan_start("freelance", "freelancer", query)
        try:
            opps = scrape_freelancer(query)
            count = _store_opportunities(opps)
            total_new += count
            _log_scan_end(scan_id, count)
            time.sleep(2)
        except Exception as e:
            errors += 1
            _log_scan_end(scan_id, 0, str(e))

    # ── PHASE 2: Market rates (Fiverr) ──
    print("[scan] Phase 2: Market rates (Fiverr)")
    fiverr_queries = [
        "AI chatbot development", "AI voice agent", "Python automation",
        "web scraping", "AI phone system", "website design",
        "SEO optimization", "lead generation", "AI content creation",
    ]
    for query in fiverr_queries[:5]:
        scan_id = _log_scan_start("market_rates", "fiverr", query)
        try:
            rates = scrape_fiverr_rates(query)
            count = _store_market_rates(rates)
            total_rates += count
            _log_scan_end(scan_id, count)
            time.sleep(3)
        except Exception as e:
            errors += 1
            _log_scan_end(scan_id, 0, str(e))

    # ── PHASE 3: Remote jobs ──
    print("[scan] Phase 3: Remote jobs")
    for query in REMOTE_JOB_QUERIES[:4]:
        scan_id = _log_scan_start("remote_jobs", "remoteok", query)
        try:
            opps = scrape_remoteok(query)
            count = _store_opportunities(opps)
            total_new += count
            _log_scan_end(scan_id, count)
            time.sleep(2)
        except Exception as e:
            errors += 1
            _log_scan_end(scan_id, 0, str(e))

        scan_id = _log_scan_start("remote_jobs", "weworkremotely", query)
        try:
            opps = scrape_weworkremotely(query)
            count = _store_opportunities(opps)
            total_new += count
            _log_scan_end(scan_id, count)
            time.sleep(2)
        except Exception as e:
            errors += 1
            _log_scan_end(scan_id, 0, str(e))

    # ── PHASE 4: Local businesses ──
    print("[scan] Phase 4: Local businesses")
    local_categories = ["locksmith", "plumber", "HVAC", "electrician", "cleaning service"]
    cities_to_scan = ["Destin", "Fort Walton Beach", "Niceville", "Crestview", "Navarre"]
    for cat in local_categories[:3]:
        for city in cities_to_scan[:3]:
            query = f"{cat} {city} FL"
            scan_id = _log_scan_start("local", "google", query)
            try:
                bizs = scrape_local_google(query)
                count = _store_local_businesses(bizs)
                total_local += count
                _log_scan_end(scan_id, count)
                time.sleep(3)
            except Exception as e:
                errors += 1
                _log_scan_end(scan_id, 0, str(e))

    # ── PHASE 5: Generate pitches for top opportunities ──
    print("[scan] Phase 5: Generating pitches for top opportunities")
    pitches_generated = _generate_top_pitches(5)

    # ── PHASE 6: Generate strategy ──
    print("[scan] Phase 6: Generating strategy")
    strategies = generate_strategy()
    _store_strategies(strategies)

    # ── PHASE 7: Feed top to nerve ──
    print("[scan] Phase 7: Feeding top opportunities to nerve")
    _feed_top_to_nerve()

    _last_scan_time = datetime.now().isoformat()
    _scan_stats["total_scans"] += 1
    _scan_stats["total_opportunities"] += total_new
    _scan_stats["total_pitches"] += pitches_generated
    _scan_stats["errors"] += errors

    summary = (
        f"[scan] Complete: {total_new} new opportunities, {total_rates} market rates, "
        f"{total_local} local businesses, {pitches_generated} pitches, "
        f"{len(strategies)} strategies, {errors} errors"
    )
    print(summary)
    return summary


def _log_scan_start(scan_type: str, source: str, query: str) -> int:
    """Log the start of a scan."""
    with get_db() as db:
        cursor = db.execute(
            "INSERT INTO scans (scan_type, source, query, status) VALUES (?, ?, ?, 'running')",
            (scan_type, source, query)
        )
        return cursor.lastrowid


def _log_scan_end(scan_id: int, results_count: int, error: str = None):
    """Log scan completion."""
    with get_db() as db:
        if error:
            db.execute(
                "UPDATE scans SET status='error', results_count=?, finished_at=datetime('now'), error=? WHERE id=?",
                (results_count, error[:500], scan_id)
            )
        else:
            db.execute(
                "UPDATE scans SET status='done', results_count=?, finished_at=datetime('now') WHERE id=?",
                (results_count, scan_id)
            )


def _store_opportunities(opps: List[dict]) -> int:
    """Score and store opportunities. Returns count of new ones."""
    new_count = 0
    with get_db() as db:
        for opp in opps:
            # Check if exists
            existing = db.execute("SELECT id FROM opportunities WHERE id=?", (opp["id"],)).fetchone()
            if existing:
                continue

            scores = score_opportunity(opp)
            db.execute("""
                INSERT OR IGNORE INTO opportunities
                (id, source, category, title, description, url, client_name, location,
                 budget_low, budget_high, budget_text, skills_needed, posted_at,
                 score_revenue, score_effort, score_skill_match, score_timeline,
                 score_total, matching_service)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                opp["id"], opp.get("source", ""), opp.get("category", ""),
                opp.get("title", ""), opp.get("description", ""),
                opp.get("url", ""), opp.get("client_name", ""),
                opp.get("location", ""), opp.get("budget_low", 0),
                opp.get("budget_high", 0), opp.get("budget_text", ""),
                opp.get("skills_needed", "[]"), opp.get("posted_at", ""),
                scores["score_revenue"], scores["score_effort"],
                scores["score_skill_match"], scores["score_timeline"],
                scores["score_total"], scores["matching_service"],
            ))
            new_count += 1
    return new_count


def _store_market_rates(rates: List[dict]) -> int:
    """Store market rate data."""
    count = 0
    with get_db() as db:
        for rate in rates:
            # Dedup by title+platform
            dup_check = hashlib.md5(
                f"{rate.get('platform', '')}:{rate.get('title', '')}".encode()
            ).hexdigest()[:16]
            existing = db.execute(
                "SELECT id FROM market_rates WHERE id=?", (dup_check,)
            ).fetchone()
            # Use hash as an alternate check via title
            existing2 = db.execute(
                "SELECT id FROM market_rates WHERE platform=? AND title=?",
                (rate.get("platform", ""), rate.get("title", ""))
            ).fetchone()
            if existing2:
                continue

            db.execute("""
                INSERT INTO market_rates
                (service_type, platform, title, price_low, price_high, price_text,
                 reviews_count, rating, seller_name, seller_url, url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                rate.get("service_type", ""), rate.get("platform", ""),
                rate.get("title", ""), rate.get("price_low", 0),
                rate.get("price_high", 0), rate.get("price_text", ""),
                rate.get("reviews_count", 0), rate.get("rating", 0),
                rate.get("seller_name", ""), rate.get("seller_url", ""),
                rate.get("url", ""),
            ))
            count += 1
    return count


def _store_local_businesses(bizs: List[dict]) -> int:
    """Store local business data."""
    count = 0
    with get_db() as db:
        for biz in bizs:
            existing = db.execute("SELECT id FROM local_businesses WHERE id=?", (biz["id"],)).fetchone()
            if existing:
                continue

            # Score the opportunity
            opp_score = 50
            if not biz.get("has_website"):
                opp_score += 25  # No website = big opportunity
            if biz.get("google_rating", 5) < 4.0:
                opp_score += 15  # Low rating = needs help
            if biz.get("google_rating", 5) < 3.0:
                opp_score += 10

            db.execute("""
                INSERT OR IGNORE INTO local_businesses
                (id, name, category, city, phone, website, has_website,
                 google_rating, opportunity_score)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                biz["id"], biz.get("name", ""), biz.get("category", ""),
                biz.get("city", ""), biz.get("phone", ""),
                biz.get("website", ""), biz.get("has_website", 0),
                biz.get("google_rating", 0), opp_score,
            ))
            count += 1
    return count


def _generate_top_pitches(count: int = 5) -> int:
    """Generate pitches for the top N unmatched opportunities."""
    generated = 0
    with get_db() as db:
        top = db.execute("""
            SELECT * FROM opportunities
            WHERE status='new' AND pitch_id IS NULL AND matching_service IS NOT NULL
            ORDER BY score_total DESC LIMIT ?
        """, (count,)).fetchall()

    for opp in top:
        svc_key = opp["matching_service"]
        if svc_key not in OUR_SERVICES:
            continue
        service = OUR_SERVICES[svc_key]
        opp_dict = dict(opp)

        pitch = generate_pitch(opp_dict, service)
        with get_db() as db:
            db.execute("""
                INSERT OR REPLACE INTO pitches
                (id, opportunity_id, service_type, pitch_text, subject_line, price_proposed)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                pitch["id"], pitch["opportunity_id"], pitch["service_type"],
                pitch["pitch_text"], pitch["subject_line"], pitch["price_proposed"],
            ))
            db.execute(
                "UPDATE opportunities SET pitch_id=?, status='pitched' WHERE id=?",
                (pitch["id"], opp["id"])
            )
        generated += 1
        time.sleep(1)  # Don't overload Ollama

    return generated


def _store_strategies(strategies: List[dict]):
    """Store strategy recommendations."""
    with get_db() as db:
        # Clear old strategies (keep last 50)
        db.execute("""
            DELETE FROM strategy_log WHERE id NOT IN (
                SELECT id FROM strategy_log ORDER BY generated_at DESC LIMIT 50
            )
        """)
        for s in strategies:
            db.execute("""
                INSERT INTO strategy_log
                (recommendation, category, priority, revenue_potential, action_items)
                VALUES (?, ?, ?, ?, ?)
            """, (
                s["recommendation"], s.get("category", ""),
                s.get("priority", 5), s.get("revenue_potential", ""),
                s.get("action_items", "[]"),
            ))


def _feed_top_to_nerve():
    """Feed top 3 opportunities and strategy to nerve."""
    with get_db() as db:
        top = db.execute(
            "SELECT title, score_total, budget_text, source FROM opportunities "
            "WHERE status IN ('new', 'pitched') ORDER BY score_total DESC LIMIT 3"
        ).fetchall()

    for opp in top:
        fact = (
            f"Revenue opportunity: {opp['title'][:100]} "
            f"(score: {opp['score_total']}, budget: {opp['budget_text'] or 'N/A'}, "
            f"source: {opp['source']})"
        )
        feed_to_nerve(fact, "revenue_opportunity")

    # Feed strategy summary
    with get_db() as db:
        strats = db.execute(
            "SELECT recommendation FROM strategy_log ORDER BY generated_at DESC, priority ASC LIMIT 3"
        ).fetchall()
    if strats:
        summary = "Revenue strategy: " + " | ".join(s["recommendation"][:100] for s in strats)
        feed_to_nerve(summary, "revenue_strategy")


def scanner_loop():
    """Background loop that runs scans every SCAN_INTERVAL_MINUTES."""
    global _scanner_running
    _scanner_running = True
    print(f"[scanner] Background scanner started (interval: {SCAN_INTERVAL_MINUTES}min)")

    # Initial scan after 30 seconds (let server start)
    time.sleep(30)

    while _scanner_running:
        try:
            run_full_scan()
        except Exception as e:
            print(f"[scanner] Error in scan loop: {e}")
            traceback.print_exc()
        # Sleep in small increments so we can stop gracefully
        for _ in range(SCAN_INTERVAL_MINUTES * 60 // 5):
            if not _scanner_running:
                break
            time.sleep(5)


# ==========================================================================
# FASTAPI APP
# ==========================================================================

app = FastAPI(
    title="THE HIVE — Revenue Hunter",
    description="Autonomous revenue opportunity finder. Scans freelance platforms, remote job boards, and local markets.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def startup():
    init_db()
    # Start background scanner
    t = threading.Thread(target=scanner_loop, daemon=True, name="revenue-scanner")
    t.start()
    print(f"[startup] Revenue Hunter v1.0 on port {PORT}")
    print(f"[startup] DB: {DB_PATH}")
    print(f"[startup] Scan interval: {SCAN_INTERVAL_MINUTES}min")
    print(f"[startup] Services catalog: {len(OUR_SERVICES)} services")


@app.on_event("shutdown")
def shutdown():
    global _scanner_running
    _scanner_running = False


# ── Health ──

@app.get("/health")
def health():
    with get_db() as db:
        opp_count = db.execute("SELECT COUNT(*) FROM opportunities").fetchone()[0]
        pitch_count = db.execute("SELECT COUNT(*) FROM pitches").fetchone()[0]
        rate_count = db.execute("SELECT COUNT(*) FROM market_rates").fetchone()[0]
        local_count = db.execute("SELECT COUNT(*) FROM local_businesses").fetchone()[0]
    return {
        "status": "healthy",
        "service": "revenue-hunter",
        "port": PORT,
        "version": "1.0.0",
        "scanner_running": _scanner_running,
        "last_scan": _last_scan_time,
        "scan_stats": _scan_stats,
        "counts": {
            "opportunities": opp_count,
            "pitches": pitch_count,
            "market_rates": rate_count,
            "local_businesses": local_count,
        },
    }


# ── Opportunities ──

@app.get("/api/opportunities")
def get_opportunities(
    source: Optional[str] = Query(None, description="Filter by source (upwork, freelancer, remoteok, etc.)"),
    category: Optional[str] = Query(None, description="Filter by category (freelance, remote_job)"),
    status: Optional[str] = Query(None, description="Filter by status (new, pitched, applied, won, lost)"),
    min_score: float = Query(0, description="Minimum total score"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    with get_db() as db:
        query = "SELECT * FROM opportunities WHERE score_total >= ?"
        params: list = [min_score]
        if source:
            query += " AND source=?"
            params.append(source)
        if category:
            query += " AND category=?"
            params.append(category)
        if status:
            query += " AND status=?"
            params.append(status)
        query += " ORDER BY score_total DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        rows = db.execute(query, params).fetchall()
        total = db.execute(
            "SELECT COUNT(*) FROM opportunities WHERE score_total >= ?", (min_score,)
        ).fetchone()[0]

    return {
        "opportunities": [dict(r) for r in rows],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


# ── Pitches ──

@app.get("/api/pitches")
def get_pitches(
    status: Optional[str] = Query(None, description="Filter: draft, sent, accepted, rejected"),
    limit: int = Query(50, ge=1, le=200),
):
    with get_db() as db:
        if status:
            rows = db.execute(
                "SELECT p.*, o.title as opp_title, o.url as opp_url, o.source as opp_source, "
                "o.score_total as opp_score "
                "FROM pitches p LEFT JOIN opportunities o ON p.opportunity_id = o.id "
                "WHERE p.status=? ORDER BY p.generated_at DESC LIMIT ?",
                (status, limit)
            ).fetchall()
        else:
            rows = db.execute(
                "SELECT p.*, o.title as opp_title, o.url as opp_url, o.source as opp_source, "
                "o.score_total as opp_score "
                "FROM pitches p LEFT JOIN opportunities o ON p.opportunity_id = o.id "
                "ORDER BY p.generated_at DESC LIMIT ?",
                (limit,)
            ).fetchall()
    return {"pitches": [dict(r) for r in rows]}


# ── Market Rates ──

@app.get("/api/market-rates")
def get_market_rates(
    service_type: Optional[str] = Query(None, description="Filter by service type"),
    platform: Optional[str] = Query(None, description="Filter by platform"),
    limit: int = Query(100, ge=1, le=500),
):
    with get_db() as db:
        query = "SELECT * FROM market_rates WHERE 1=1"
        params: list = []
        if service_type:
            query += " AND service_type LIKE ?"
            params.append(f"%{service_type}%")
        if platform:
            query += " AND platform=?"
            params.append(platform)
        query += " ORDER BY reviews_count DESC, rating DESC LIMIT ?"
        params.append(limit)

        rows = db.execute(query, params).fetchall()

        # Also get aggregated stats
        stats = db.execute("""
            SELECT service_type,
                   COUNT(*) as gig_count,
                   AVG(price_low) as avg_price_low,
                   AVG(price_high) as avg_price_high,
                   MAX(price_high) as max_price,
                   AVG(reviews_count) as avg_reviews,
                   AVG(rating) as avg_rating
            FROM market_rates
            GROUP BY service_type
            ORDER BY avg_reviews DESC
        """).fetchall()

    return {
        "rates": [dict(r) for r in rows],
        "summary": [dict(s) for s in stats],
    }


# ── Strategy ──

@app.get("/api/strategy")
def get_strategy():
    with get_db() as db:
        strategies = db.execute(
            "SELECT * FROM strategy_log ORDER BY generated_at DESC, priority ASC LIMIT 20"
        ).fetchall()

        # Also include quick stats
        opp_by_source = db.execute(
            "SELECT source, COUNT(*) as cnt, AVG(score_total) as avg_score, "
            "SUM(CASE WHEN budget_high > 0 THEN budget_high ELSE 0 END) as total_budget "
            "FROM opportunities GROUP BY source"
        ).fetchall()

        top_services = db.execute(
            "SELECT matching_service, COUNT(*) as cnt, AVG(score_total) as avg_score "
            "FROM opportunities WHERE matching_service IS NOT NULL "
            "GROUP BY matching_service ORDER BY cnt DESC"
        ).fetchall()

        local_hot = db.execute(
            "SELECT * FROM local_businesses WHERE opportunity_score >= 70 "
            "ORDER BY opportunity_score DESC LIMIT 10"
        ).fetchall()

    return {
        "strategies": [dict(s) for s in strategies],
        "pipeline_by_source": [dict(r) for r in opp_by_source],
        "demand_by_service": [dict(r) for r in top_services],
        "hot_local_prospects": [dict(r) for r in local_hot],
        "our_services": {k: {
            "name": v["name"],
            "price_range": v["price_range"],
            "setup_time": v["setup_time"],
            "tech_ready": v["tech_ready"],
        } for k, v in OUR_SERVICES.items()},
        "fastest_to_1k": {
            "option_1": "2 local businesses on AI Phone Agent @ $499/mo = $998/mo recurring",
            "option_2": "5 Fiverr web scraping gigs @ $200 each = $1,000 one-time",
            "option_3": "3 website builds for local businesses @ $500 each = $1,500 one-time",
            "option_4": "1 Upwork AI automation project @ $1,000-$3,000 = immediate",
            "option_5": "10 Fiverr AI chatbot gigs @ $100 each = $1,000 one-time",
        },
    }


# ── Local Businesses ──

@app.get("/api/local-businesses")
def get_local_businesses(
    city: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    min_score: float = Query(0),
    limit: int = Query(50, ge=1, le=200),
):
    with get_db() as db:
        query = "SELECT * FROM local_businesses WHERE opportunity_score >= ?"
        params: list = [min_score]
        if city:
            query += " AND city=?"
            params.append(city)
        if category:
            query += " AND category LIKE ?"
            params.append(f"%{category}%")
        query += " ORDER BY opportunity_score DESC LIMIT ?"
        params.append(limit)
        rows = db.execute(query, params).fetchall()
    return {"businesses": [dict(r) for r in rows]}


# ── Manual scan trigger ──

@app.post("/api/scan")
async def trigger_scan(background_tasks: BackgroundTasks):
    """Trigger a manual scan."""
    background_tasks.add_task(run_full_scan)
    return {
        "status": "scan_started",
        "message": "Full scan triggered in background. Check /health for progress.",
        "scan_interval_minutes": SCAN_INTERVAL_MINUTES,
    }


# ── Scan history ──

@app.get("/api/scans")
def get_scans(limit: int = Query(50, ge=1, le=200)):
    with get_db() as db:
        rows = db.execute(
            "SELECT * FROM scans ORDER BY started_at DESC LIMIT ?", (limit,)
        ).fetchall()
        stats = db.execute("""
            SELECT source,
                   COUNT(*) as total_scans,
                   SUM(results_count) as total_results,
                   SUM(CASE WHEN status='error' THEN 1 ELSE 0 END) as errors,
                   MAX(finished_at) as last_scan
            FROM scans GROUP BY source
        """).fetchall()
    return {
        "scans": [dict(r) for r in rows],
        "stats_by_source": [dict(s) for s in stats],
    }


# ── Generate pitch on demand ──

class PitchRequest(BaseModel):
    opportunity_id: str
    service_key: Optional[str] = None


@app.post("/api/generate-pitch")
async def generate_pitch_endpoint(req: PitchRequest):
    """Generate a pitch for a specific opportunity."""
    with get_db() as db:
        opp = db.execute("SELECT * FROM opportunities WHERE id=?", (req.opportunity_id,)).fetchone()
    if not opp:
        raise HTTPException(404, "Opportunity not found")

    opp_dict = dict(opp)
    svc_key = req.service_key or opp_dict.get("matching_service")
    if not svc_key or svc_key not in OUR_SERVICES:
        # Auto-detect best service
        scores = score_opportunity(opp_dict)
        svc_key = scores.get("matching_service")
    if not svc_key or svc_key not in OUR_SERVICES:
        raise HTTPException(400, "Could not determine matching service. Specify service_key.")

    service = OUR_SERVICES[svc_key]
    pitch = generate_pitch(opp_dict, service)

    with get_db() as db:
        db.execute("""
            INSERT OR REPLACE INTO pitches
            (id, opportunity_id, service_type, pitch_text, subject_line, price_proposed)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            pitch["id"], pitch["opportunity_id"], pitch["service_type"],
            pitch["pitch_text"], pitch["subject_line"], pitch["price_proposed"],
        ))
        db.execute(
            "UPDATE opportunities SET pitch_id=?, status='pitched' WHERE id=?",
            (pitch["id"], req.opportunity_id)
        )

    return {"pitch": pitch}


# ── Our services catalog ──

@app.get("/api/services")
def get_services():
    """Return our full service catalog with pricing."""
    return {"services": OUR_SERVICES}


# ── Dashboard ──

@app.get("/", response_class=HTMLResponse)
def dashboard():
    with get_db() as db:
        opp_count = db.execute("SELECT COUNT(*) FROM opportunities").fetchone()[0]
        pitch_count = db.execute("SELECT COUNT(*) FROM pitches").fetchone()[0]
        rate_count = db.execute("SELECT COUNT(*) FROM market_rates").fetchone()[0]
        local_count = db.execute("SELECT COUNT(*) FROM local_businesses").fetchone()[0]

        top_opps = db.execute(
            "SELECT title, score_total, budget_text, source, url, status "
            "FROM opportunities ORDER BY score_total DESC LIMIT 15"
        ).fetchall()

        top_local = db.execute(
            "SELECT name, city, category, phone, opportunity_score, has_website "
            "FROM local_businesses ORDER BY opportunity_score DESC LIMIT 10"
        ).fetchall()

        recent_pitches = db.execute(
            "SELECT p.subject_line, p.price_proposed, p.status, o.title as opp_title "
            "FROM pitches p LEFT JOIN opportunities o ON p.opportunity_id = o.id "
            "ORDER BY p.generated_at DESC LIMIT 10"
        ).fetchall()

        strategies = db.execute(
            "SELECT recommendation, priority, revenue_potential, category "
            "FROM strategy_log ORDER BY generated_at DESC, priority ASC LIMIT 10"
        ).fetchall()

    opp_rows = ""
    for o in top_opps:
        status_color = {"new": "#4CAF50", "pitched": "#2196F3", "applied": "#FF9800", "won": "#FFD700"}.get(o["status"], "#888")
        opp_rows += f"""<tr>
            <td><a href="{o['url'] or '#'}" target="_blank" style="color:#4FC3F7">{html_lib.escape(o['title'][:70])}</a></td>
            <td><b>{o['score_total']:.0f}</b></td>
            <td>{html_lib.escape(o['budget_text'] or 'N/A')}</td>
            <td>{html_lib.escape(o['source'])}</td>
            <td style="color:{status_color}">{o['status']}</td>
        </tr>"""

    local_rows = ""
    for b in top_local:
        web_icon = "&#x2705;" if b["has_website"] else "&#x274C;"
        local_rows += f"""<tr>
            <td>{html_lib.escape(b['name'][:50])}</td>
            <td>{html_lib.escape(b['city'] or '')}</td>
            <td>{html_lib.escape(b['category'] or '')}</td>
            <td>{html_lib.escape(b['phone'] or 'N/A')}</td>
            <td>{web_icon}</td>
            <td><b>{b['opportunity_score']:.0f}</b></td>
        </tr>"""

    pitch_rows = ""
    for p in recent_pitches:
        pitch_rows += f"""<tr>
            <td>{html_lib.escape(p['subject_line'] or 'No subject')}</td>
            <td>{html_lib.escape((p['opp_title'] or '')[:50])}</td>
            <td>{html_lib.escape(p['price_proposed'] or '')}</td>
            <td>{p['status']}</td>
        </tr>"""

    strategy_rows = ""
    for s in strategies:
        priority_badge = {1: "P0 NOW", 2: "P1 HIGH", 3: "P2 MED"}.get(s["priority"], f"P{s['priority']}")
        priority_color = {1: "#f44336", 2: "#FF9800", 3: "#4CAF50"}.get(s["priority"], "#888")
        strategy_rows += f"""<tr>
            <td style="color:{priority_color}"><b>{priority_badge}</b></td>
            <td>{html_lib.escape(s['recommendation'][:80])}</td>
            <td>{html_lib.escape(s['revenue_potential'] or '')}</td>
            <td>{html_lib.escape(s['category'] or '')}</td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html><head>
<title>Revenue Hunter - THE HIVE</title>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<style>
    * {{ margin:0; padding:0; box-sizing:border-box; }}
    body {{ background:#0a0a0a; color:#e0e0e0; font-family:'Segoe UI',sans-serif; padding:20px; }}
    h1 {{ color:#FFD700; margin-bottom:5px; }}
    h2 {{ color:#4FC3F7; margin:20px 0 10px; border-bottom:1px solid #333; padding-bottom:5px; }}
    .stats {{ display:flex; gap:15px; flex-wrap:wrap; margin:15px 0; }}
    .stat {{ background:#1a1a2e; border:1px solid #333; border-radius:8px; padding:15px 20px; min-width:150px; text-align:center; }}
    .stat .num {{ font-size:2em; font-weight:bold; color:#FFD700; }}
    .stat .label {{ font-size:0.85em; color:#aaa; margin-top:5px; }}
    table {{ width:100%; border-collapse:collapse; margin:10px 0; }}
    th {{ background:#1a1a2e; color:#4FC3F7; padding:8px; text-align:left; font-size:0.85em; }}
    td {{ padding:8px; border-bottom:1px solid #1a1a2e; font-size:0.85em; }}
    tr:hover {{ background:#1a1a2e; }}
    a {{ color:#4FC3F7; text-decoration:none; }}
    a:hover {{ text-decoration:underline; }}
    .scan-btn {{ background:#FFD700; color:#000; border:none; padding:10px 20px;
                 border-radius:5px; cursor:pointer; font-weight:bold; font-size:1em; margin:10px 0; }}
    .scan-btn:hover {{ background:#FFC107; }}
    .scanner-status {{ color:#4CAF50; font-size:0.9em; margin:5px 0; }}
    .section {{ background:#111; border:1px solid #222; border-radius:8px; padding:15px; margin:15px 0; }}
</style>
</head><body>
<h1>Revenue Hunter</h1>
<p style="color:#aaa">Autonomous opportunity scanner | THE HIVE</p>
<p class="scanner-status">Scanner: {'RUNNING' if _scanner_running else 'STOPPED'} |
Last scan: {_last_scan_time or 'Never'} |
Total scans: {_scan_stats['total_scans']}</p>
<button class="scan-btn" onclick="fetch('/api/scan',{{method:'POST'}}).then(()=>location.reload())">
Trigger Manual Scan
</button>

<div class="stats">
    <div class="stat"><div class="num">{opp_count}</div><div class="label">Opportunities</div></div>
    <div class="stat"><div class="num">{pitch_count}</div><div class="label">Pitches</div></div>
    <div class="stat"><div class="num">{rate_count}</div><div class="label">Market Rates</div></div>
    <div class="stat"><div class="num">{local_count}</div><div class="label">Local Businesses</div></div>
</div>

<div class="section">
<h2>Strategy Recommendations</h2>
<table><tr><th>Priority</th><th>Recommendation</th><th>Revenue</th><th>Category</th></tr>
{strategy_rows or '<tr><td colspan="4" style="color:#888">No strategies yet. Trigger a scan first.</td></tr>'}
</table></div>

<div class="section">
<h2>Top Opportunities (by Score)</h2>
<table><tr><th>Title</th><th>Score</th><th>Budget</th><th>Source</th><th>Status</th></tr>
{opp_rows or '<tr><td colspan="5" style="color:#888">No opportunities yet. Trigger a scan first.</td></tr>'}
</table></div>

<div class="section">
<h2>Local Business Prospects (NW Florida)</h2>
<table><tr><th>Name</th><th>City</th><th>Category</th><th>Phone</th><th>Website?</th><th>Score</th></tr>
{local_rows or '<tr><td colspan="6" style="color:#888">No local businesses found yet.</td></tr>'}
</table></div>

<div class="section">
<h2>Ready Pitches</h2>
<table><tr><th>Subject</th><th>Opportunity</th><th>Price</th><th>Status</th></tr>
{pitch_rows or '<tr><td colspan="4" style="color:#888">No pitches generated yet.</td></tr>'}
</table></div>

<div class="section">
<h2>API Endpoints</h2>
<table>
<tr><td><a href="/health">/health</a></td><td>Service health + counts</td></tr>
<tr><td><a href="/api/opportunities">/api/opportunities</a></td><td>All opportunities ranked by score</td></tr>
<tr><td><a href="/api/pitches">/api/pitches</a></td><td>Generated proposals</td></tr>
<tr><td><a href="/api/market-rates">/api/market-rates</a></td><td>Competitor pricing data</td></tr>
<tr><td><a href="/api/strategy">/api/strategy</a></td><td>Recommended actions + fastest path to $1K</td></tr>
<tr><td><a href="/api/local-businesses">/api/local-businesses</a></td><td>Local business prospects</td></tr>
<tr><td><a href="/api/services">/api/services</a></td><td>Our service catalog</td></tr>
<tr><td><a href="/api/scans">/api/scans</a></td><td>Scan history + stats</td></tr>
<tr><td>POST /api/scan</td><td>Trigger manual scan</td></tr>
<tr><td>POST /api/generate-pitch</td><td>Generate pitch for specific opportunity</td></tr>
</table></div>

<p style="color:#555;margin-top:20px;font-size:0.8em">Revenue Hunter v1.0 | THE HIVE | Port {PORT}</p>
</body></html>"""


# ==========================================================================
# MAIN
# ==========================================================================

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
