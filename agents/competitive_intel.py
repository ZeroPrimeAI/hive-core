#!/usr/bin/env python3
"""
THE HIVE — Competitive Intelligence Agent
Port 8902 | SQLite at /home/zero/hivecode_sandbox/intel.db
MIT License

Watches public social media for competitive intelligence:
  - YouTube (via Data API v3)
  - TikTok (public embed/RSS endpoints)
  - Instagram (public hashtag pages)
  - Facebook (public page scraping)

Analyzes: hooks, ad copy, CTAs, trending hashtags, engagement metrics,
products/services being sold, and what's working in our niches.

Feeds insights to nerve on ZeroZI.
"""

import json
import sqlite3
import time
import threading
import os
import re
import hashlib
import traceback
import html
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any
from contextlib import contextmanager
from urllib.parse import quote_plus, urlencode

import httpx

# Reasoning Bank — cache analysis results
try:
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'core'))
    from reasoning_client import ReasoningClient
    _rc_intel = ReasoningClient(domain="competitive_intel")
except ImportError:
    _rc_intel = None

from fastapi import FastAPI, BackgroundTasks, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
import uvicorn

# ==========================================================================
# CONFIG
# ==========================================================================
PORT = 8902
DB_PATH = "/home/zero/hivecode_sandbox/intel.db"

YOUTUBE_API_KEY = "AIzaSyADbG33gAk_voyC8ZtuGpjNW6BZUMjVS5Q"
OLLAMA_URL = "http://localhost:11434"
OLLAMA_MODEL = "gemma2:2b"
NERVE_URL = "http://100.105.160.106:8200/api/add"

SCAN_INTERVAL_MINUTES = 30

# Niches to monitor
NICHES = {
    "ai_anime": [
        "AI anime", "AI generated anime", "ghost in the machine anime",
        "AI animation", "AI art anime"
    ],
    "locksmith": [
        "locksmith", "emergency locksmith", "24/7 locksmith",
        "locksmith business", "locksmith marketing"
    ],
    "ai_business": [
        "AI business", "AI automation", "make money with AI",
        "AI agency", "AI tools for business"
    ],
    "small_business": [
        "small business marketing", "local business ads",
        "small business social media", "local SEO tips"
    ],
}

# All search queries flattened
ALL_QUERIES = []
for queries in NICHES.values():
    ALL_QUERIES.extend(queries)

# User agents for scraping
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

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
            CREATE TABLE IF NOT EXISTS content (
                id TEXT PRIMARY KEY,
                platform TEXT NOT NULL,
                niche TEXT,
                title TEXT,
                description TEXT,
                url TEXT,
                author TEXT,
                author_url TEXT,
                views INTEGER DEFAULT 0,
                likes INTEGER DEFAULT 0,
                comments INTEGER DEFAULT 0,
                shares INTEGER DEFAULT 0,
                engagement_rate REAL DEFAULT 0.0,
                hook TEXT,
                cta TEXT,
                selling TEXT,
                hashtags TEXT,
                analysis TEXT,
                why_it_works TEXT,
                found_at TEXT DEFAULT (datetime('now')),
                query TEXT
            );

            CREATE TABLE IF NOT EXISTS trends (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform TEXT NOT NULL,
                niche TEXT,
                trend_type TEXT,
                value TEXT,
                score REAL DEFAULT 0.0,
                examples TEXT,
                found_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS hooks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform TEXT,
                niche TEXT,
                hook_text TEXT NOT NULL,
                content_id TEXT,
                engagement_score REAL DEFAULT 0.0,
                hook_type TEXT,
                found_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (content_id) REFERENCES content(id)
            );

            CREATE TABLE IF NOT EXISTS ads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform TEXT,
                niche TEXT,
                ad_copy TEXT,
                cta TEXT,
                product TEXT,
                price_point TEXT,
                content_id TEXT,
                engagement_score REAL DEFAULT 0.0,
                found_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (content_id) REFERENCES content(id)
            );

            CREATE TABLE IF NOT EXISTS scans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform TEXT,
                niche TEXT,
                query TEXT,
                results_count INTEGER DEFAULT 0,
                status TEXT DEFAULT 'pending',
                started_at TEXT DEFAULT (datetime('now')),
                finished_at TEXT,
                error TEXT
            );

            CREATE TABLE IF NOT EXISTS ideas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                niche TEXT,
                idea TEXT NOT NULL,
                based_on TEXT,
                platform TEXT,
                confidence REAL DEFAULT 0.5,
                generated_at TEXT DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_content_platform ON content(platform);
            CREATE INDEX IF NOT EXISTS idx_content_niche ON content(niche);
            CREATE INDEX IF NOT EXISTS idx_content_views ON content(views DESC);
            CREATE INDEX IF NOT EXISTS idx_hooks_engagement ON hooks(engagement_score DESC);
            CREATE INDEX IF NOT EXISTS idx_trends_score ON trends(score DESC);
            CREATE INDEX IF NOT EXISTS idx_ads_engagement ON ads(engagement_score DESC);
        """)


# ==========================================================================
# OLLAMA ANALYSIS
# ==========================================================================

async def analyze_with_ollama(prompt: str, timeout: float = 30.0) -> str:
    """Send a prompt to local Ollama gemma2:2b for analysis. Cache-first via reasoning bank."""
    # Check reasoning bank cache first
    if _rc_intel:
        cached = _rc_intel.ask(prompt)
        if cached["hit"]:
            return cached["response"]
    # Cache miss — call Ollama
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(
                f"{OLLAMA_URL}/api/generate",
                json={
                    "model": OLLAMA_MODEL,
                    "prompt": prompt,
                    "stream": False,
                    "options": {"temperature": 0.3, "num_predict": 300}
                }
            )
            if resp.status_code == 200:
                result = resp.json().get("response", "").strip()
                if result and _rc_intel:
                    _rc_intel.learn(prompt, result, tokens=len(result) // 4)
                return result
    except Exception as e:
        pass
    return ""


def analyze_with_ollama_sync(prompt: str, timeout: float = 30.0) -> str:
    """Synchronous version for background threads. Cache-first via reasoning bank."""
    # Check reasoning bank cache first
    if _rc_intel:
        cached = _rc_intel.ask(prompt)
        if cached["hit"]:
            return cached["response"]
    # Cache miss — call Ollama
    try:
        with httpx.Client(timeout=timeout) as client:
            resp = client.post(
                f"{OLLAMA_URL}/api/generate",
                json={
                    "model": OLLAMA_MODEL,
                    "prompt": prompt,
                    "stream": False,
                    "options": {"temperature": 0.3, "num_predict": 300}
                }
            )
            if resp.status_code == 200:
                result = resp.json().get("response", "").strip()
                if result and _rc_intel:
                    _rc_intel.learn(prompt, result, tokens=len(result) // 4)
                return result
    except Exception:
        pass
    return ""


def extract_hook(title: str, description: str = "") -> str:
    """Extract the hook (attention-grabbing opening) from title + first line of description."""
    hook = title.strip()
    if description:
        first_line = description.strip().split("\n")[0][:120]
        if first_line and first_line != title:
            hook = f"{title} | {first_line}"
    return hook[:300]


def detect_cta(text: str) -> str:
    """Detect call-to-action patterns in text."""
    if not text:
        return ""
    text_lower = text.lower()
    cta_patterns = [
        r"(?:click|tap|hit)\s+(?:the\s+)?(?:link|button|subscribe|here)",
        r"(?:sign\s*up|join|register|enroll|subscribe)\s+(?:now|today|free|here)",
        r"(?:get|grab|claim|download)\s+(?:your|my|the|a)?\s*(?:free|copy|guide|ebook|template)",
        r"(?:buy|order|shop|purchase)\s+(?:now|today|here)",
        r"(?:book|schedule)\s+(?:a\s+)?(?:call|demo|appointment|consultation)",
        r"(?:use\s+code|discount|coupon|promo)",
        r"(?:limited\s+time|act\s+now|don'?t\s+miss|hurry|last\s+chance)",
        r"(?:link\s+in\s+(?:bio|description))",
        r"(?:check\s+(?:out|it\s+out)|learn\s+more|find\s+out)",
        r"(?:call\s+(?:us|now|today))\s*(?:at|for)?",
        r"(?:free\s+(?:trial|quote|estimate|consultation))",
        r"(?:DM\s+(?:me|us))\s+(?:for|to)",
    ]
    found = []
    for pattern in cta_patterns:
        matches = re.findall(pattern, text_lower)
        found.extend(matches)
    return "; ".join(found[:5]) if found else ""


def detect_selling(text: str) -> str:
    """Detect what product/service is being sold."""
    if not text:
        return ""
    text_lower = text.lower()
    selling_signals = []
    price_matches = re.findall(r"\$\d+[\d,.]*", text)
    if price_matches:
        selling_signals.append(f"Price: {', '.join(price_matches[:3])}")
    product_patterns = [
        (r"(?:course|masterclass|workshop|bootcamp|class)", "Course/Education"),
        (r"(?:ebook|e-book|guide|pdf|playbook|template)", "Digital Product"),
        (r"(?:software|app|tool|platform|SaaS|plugin)", "Software/Tool"),
        (r"(?:coaching|mentoring|consulting|1-on-1|one-on-one)", "Coaching/Consulting"),
        (r"(?:membership|community|group|discord|slack)", "Membership/Community"),
        (r"(?:merch|t-shirt|hoodie|sticker|print)", "Merchandise"),
        (r"(?:affiliate|partner|referral|commission)", "Affiliate"),
        (r"(?:service|hire\s+(?:me|us)|freelance|agency)", "Service"),
        (r"(?:subscription|monthly|yearly|annual\s+plan)", "Subscription"),
    ]
    for pattern, label in product_patterns:
        if re.search(pattern, text_lower):
            selling_signals.append(label)
    return "; ".join(selling_signals[:4]) if selling_signals else ""


def compute_content_id(platform: str, url_or_key: str) -> str:
    """Generate a unique content ID."""
    raw = f"{platform}:{url_or_key}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def calc_engagement_rate(views: int, likes: int, comments: int, shares: int = 0) -> float:
    """Calculate engagement rate as a percentage."""
    if views <= 0:
        return 0.0
    return round(((likes + comments * 2 + shares * 3) / views) * 100, 4)


# ==========================================================================
# YOUTUBE SCRAPER (via Data API v3)
# ==========================================================================

def youtube_search(query: str, max_results: int = 10) -> List[Dict]:
    """Search YouTube using the Data API v3."""
    results = []
    try:
        with httpx.Client(timeout=15) as client:
            # Step 1: Search for videos
            search_resp = client.get(
                "https://www.googleapis.com/youtube/v3/search",
                params={
                    "key": YOUTUBE_API_KEY,
                    "q": query,
                    "part": "snippet",
                    "type": "video",
                    "maxResults": max_results,
                    "order": "viewCount",
                    "relevanceLanguage": "en",
                    "publishedAfter": (
                        datetime.now(timezone.utc) - timedelta(days=30)
                    ).strftime("%Y-%m-%dT%H:%M:%SZ"),
                }
            )
            if search_resp.status_code != 200:
                return results
            search_data = search_resp.json()
            items = search_data.get("items", [])
            if not items:
                return results

            # Step 2: Get statistics for all videos in one batch
            video_ids = [item["id"]["videoId"] for item in items if "videoId" in item.get("id", {})]
            if not video_ids:
                return results

            stats_resp = client.get(
                "https://www.googleapis.com/youtube/v3/videos",
                params={
                    "key": YOUTUBE_API_KEY,
                    "id": ",".join(video_ids),
                    "part": "statistics,snippet,contentDetails"
                }
            )
            stats_map = {}
            if stats_resp.status_code == 200:
                for v in stats_resp.json().get("items", []):
                    stats_map[v["id"]] = v

            # Step 3: Build result objects
            for item in items:
                vid = item.get("id", {}).get("videoId")
                if not vid:
                    continue
                snippet = item.get("snippet", {})
                full_data = stats_map.get(vid, {})
                stats = full_data.get("statistics", {})

                title = snippet.get("title", "")
                description = snippet.get("description", "")
                channel = snippet.get("channelTitle", "")
                channel_id = snippet.get("channelId", "")
                published = snippet.get("publishedAt", "")

                views = int(stats.get("viewCount", 0))
                likes = int(stats.get("likeCount", 0))
                comments_count = int(stats.get("commentCount", 0))

                url = f"https://www.youtube.com/watch?v={vid}"
                hook = extract_hook(title, description)
                cta = detect_cta(f"{title} {description}")
                selling = detect_selling(f"{title} {description}")
                engagement = calc_engagement_rate(views, likes, comments_count)

                # Extract hashtags from description
                hashtags = re.findall(r"#\w+", description)

                results.append({
                    "platform": "youtube",
                    "video_id": vid,
                    "title": title,
                    "description": description[:500],
                    "url": url,
                    "author": channel,
                    "author_url": f"https://www.youtube.com/channel/{channel_id}",
                    "views": views,
                    "likes": likes,
                    "comments": comments_count,
                    "shares": 0,
                    "engagement_rate": engagement,
                    "hook": hook,
                    "cta": cta,
                    "selling": selling,
                    "hashtags": json.dumps(hashtags[:20]),
                    "published": published,
                    "query": query,
                })

    except Exception as e:
        print(f"[YouTube] Error searching '{query}': {e}")
    return results


# ==========================================================================
# TIKTOK SCRAPER (public endpoints)
# ==========================================================================

def tiktok_search(query: str, max_results: int = 10) -> List[Dict]:
    """Scrape TikTok public search results via embed/oembed endpoints."""
    results = []
    try:
        with httpx.Client(
            timeout=15,
            headers={"User-Agent": USER_AGENT},
            follow_redirects=True
        ) as client:
            # TikTok search page (public)
            encoded_q = quote_plus(query)
            search_url = f"https://www.tiktok.com/api/search/general/full/?keyword={encoded_q}&offset=0&search_id=0"

            # Try the web search endpoint
            resp = client.get(
                f"https://www.tiktok.com/search?q={encoded_q}",
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "text/html,application/xhtml+xml",
                    "Accept-Language": "en-US,en;q=0.9",
                }
            )

            if resp.status_code != 200:
                return results

            page_html = resp.text

            # Extract video data from the SSR hydration script
            # TikTok embeds JSON data in a SIGI_STATE or __UNIVERSAL_DATA script tag
            json_patterns = [
                r'"ItemModule"\s*:\s*(\{.+?\})\s*,\s*"',
                r'"video"\s*:\s*\{[^}]*"id"\s*:\s*"(\d+)"',
                r'<script\s+id="__UNIVERSAL_DATA_FOR_REHYDRATION__"\s+type="application/json">(.*?)</script>',
                r'<script\s+id="SIGI_STATE"\s+type="application/json">(.*?)</script>',
            ]

            video_urls = re.findall(
                r'href="(https://www\.tiktok\.com/@[^/]+/video/\d+)"', page_html
            )
            # Also try data-video patterns
            video_urls += re.findall(
                r'"(https?://(?:www\.)?tiktok\.com/@[\w.]+/video/\d+)"', page_html
            )
            # Deduplicate
            seen = set()
            unique_urls = []
            for u in video_urls:
                clean = u.split("?")[0]
                if clean not in seen:
                    seen.add(clean)
                    unique_urls.append(clean)

            # For each found video, try oembed to get metadata
            for vurl in unique_urls[:max_results]:
                try:
                    oembed_resp = client.get(
                        "https://www.tiktok.com/oembed",
                        params={"url": vurl}
                    )
                    if oembed_resp.status_code == 200:
                        data = oembed_resp.json()
                        title = data.get("title", "")
                        author = data.get("author_name", "")
                        author_url = data.get("author_url", "")

                        hook = extract_hook(title)
                        cta = detect_cta(title)
                        selling = detect_selling(title)
                        hashtags = re.findall(r"#\w+", title)

                        results.append({
                            "platform": "tiktok",
                            "video_id": vurl.split("/")[-1],
                            "title": title[:500],
                            "description": title[:500],
                            "url": vurl,
                            "author": author,
                            "author_url": author_url,
                            "views": 0,  # oembed doesn't give views
                            "likes": 0,
                            "comments": 0,
                            "shares": 0,
                            "engagement_rate": 0.0,
                            "hook": hook,
                            "cta": cta,
                            "selling": selling,
                            "hashtags": json.dumps(hashtags[:20]),
                            "published": "",
                            "query": query,
                        })
                except Exception:
                    continue

            # If no video URLs found from page scrape, try extracting from JSON
            if not results:
                for pattern in json_patterns:
                    matches = re.findall(pattern, page_html, re.DOTALL)
                    if matches:
                        for m in matches[:1]:
                            try:
                                data = json.loads(m)
                                # Handle different data structures
                                if isinstance(data, dict):
                                    for key, item in list(data.items())[:max_results]:
                                        if isinstance(item, dict) and "desc" in item:
                                            title = item.get("desc", "")
                                            vid_id = item.get("id", key)
                                            author_info = item.get("author", "")
                                            stats = item.get("stats", {})
                                            vurl = f"https://www.tiktok.com/@{author_info}/video/{vid_id}" if author_info else ""

                                            results.append({
                                                "platform": "tiktok",
                                                "video_id": str(vid_id),
                                                "title": title[:500],
                                                "description": title[:500],
                                                "url": vurl,
                                                "author": str(author_info),
                                                "author_url": "",
                                                "views": int(stats.get("playCount", 0)),
                                                "likes": int(stats.get("diggCount", 0)),
                                                "comments": int(stats.get("commentCount", 0)),
                                                "shares": int(stats.get("shareCount", 0)),
                                                "engagement_rate": 0.0,
                                                "hook": extract_hook(title),
                                                "cta": detect_cta(title),
                                                "selling": detect_selling(title),
                                                "hashtags": json.dumps(re.findall(r"#\w+", title)[:20]),
                                                "published": "",
                                                "query": query,
                                            })
                            except (json.JSONDecodeError, TypeError):
                                continue

    except Exception as e:
        print(f"[TikTok] Error searching '{query}': {e}")
    return results


# ==========================================================================
# INSTAGRAM SCRAPER (public hashtag/explore pages)
# ==========================================================================

def instagram_search(query: str, max_results: int = 10) -> List[Dict]:
    """Scrape Instagram public hashtag pages."""
    results = []
    try:
        # Convert query to hashtag format
        hashtag = re.sub(r"[^a-zA-Z0-9]", "", query.lower())

        with httpx.Client(
            timeout=15,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "en-US,en;q=0.9",
            },
            follow_redirects=True
        ) as client:
            # Try the public hashtag page
            resp = client.get(f"https://www.instagram.com/explore/tags/{hashtag}/")

            if resp.status_code != 200:
                # Try web search endpoint
                resp = client.get(
                    f"https://www.instagram.com/web/search/topsearch/",
                    params={"query": query, "context": "blended"}
                )
                if resp.status_code == 200:
                    try:
                        data = resp.json()
                        for user in data.get("users", [])[:max_results]:
                            u = user.get("user", {})
                            results.append({
                                "platform": "instagram",
                                "video_id": u.get("pk", ""),
                                "title": f"@{u.get('username', '')} - {u.get('full_name', '')}",
                                "description": u.get("biography", "")[:500] if u.get("biography") else "",
                                "url": f"https://www.instagram.com/{u.get('username', '')}/",
                                "author": u.get("username", ""),
                                "author_url": f"https://www.instagram.com/{u.get('username', '')}/",
                                "views": 0,
                                "likes": 0,
                                "comments": 0,
                                "shares": 0,
                                "engagement_rate": 0.0,
                                "hook": u.get("full_name", ""),
                                "cta": detect_cta(u.get("biography", "")),
                                "selling": detect_selling(u.get("biography", "")),
                                "hashtags": json.dumps([f"#{hashtag}"]),
                                "published": "",
                                "query": query,
                            })
                    except (json.JSONDecodeError, TypeError):
                        pass
                return results

            page_html = resp.text

            # Extract data from Instagram's SSR JSON
            json_patterns = [
                r'<script type="application/ld\+json">(.*?)</script>',
                r'window\._sharedData\s*=\s*(\{.+?\});</script>',
                r'"edge_hashtag_to_media"\s*:\s*(\{.+?\})\s*,',
            ]

            for pattern in json_patterns:
                matches = re.findall(pattern, page_html, re.DOTALL)
                for m in matches[:1]:
                    try:
                        data = json.loads(m)
                        # Handle shared data structure
                        if "entry_data" in data:
                            tag_page = data.get("entry_data", {}).get("TagPage", [{}])[0]
                            media = tag_page.get("graphql", {}).get("hashtag", {}).get("edge_hashtag_to_media", {})
                            edges = media.get("edges", [])
                            for edge in edges[:max_results]:
                                node = edge.get("node", {})
                                caption_edges = node.get("edge_media_to_caption", {}).get("edges", [])
                                caption = caption_edges[0]["node"]["text"] if caption_edges else ""
                                shortcode = node.get("shortcode", "")

                                results.append({
                                    "platform": "instagram",
                                    "video_id": shortcode,
                                    "title": caption[:200] if caption else "",
                                    "description": caption[:500] if caption else "",
                                    "url": f"https://www.instagram.com/p/{shortcode}/" if shortcode else "",
                                    "author": node.get("owner", {}).get("username", ""),
                                    "author_url": "",
                                    "views": int(node.get("video_view_count", 0)),
                                    "likes": int(node.get("edge_liked_by", {}).get("count", 0)),
                                    "comments": int(node.get("edge_media_to_comment", {}).get("count", 0)),
                                    "shares": 0,
                                    "engagement_rate": 0.0,
                                    "hook": extract_hook(caption[:200] if caption else ""),
                                    "cta": detect_cta(caption or ""),
                                    "selling": detect_selling(caption or ""),
                                    "hashtags": json.dumps(re.findall(r"#\w+", caption or "")[:20]),
                                    "published": "",
                                    "query": query,
                                })
                    except (json.JSONDecodeError, TypeError, IndexError, KeyError):
                        continue

            # Fallback: extract shortcodes from the raw HTML
            if not results:
                shortcodes = re.findall(r'/p/([A-Za-z0-9_-]+)/', page_html)
                seen_codes = set()
                for sc in shortcodes[:max_results]:
                    if sc in seen_codes:
                        continue
                    seen_codes.add(sc)
                    results.append({
                        "platform": "instagram",
                        "video_id": sc,
                        "title": f"Instagram post #{hashtag}",
                        "description": "",
                        "url": f"https://www.instagram.com/p/{sc}/",
                        "author": "",
                        "author_url": "",
                        "views": 0,
                        "likes": 0,
                        "comments": 0,
                        "shares": 0,
                        "engagement_rate": 0.0,
                        "hook": "",
                        "cta": "",
                        "selling": "",
                        "hashtags": json.dumps([f"#{hashtag}"]),
                        "published": "",
                        "query": query,
                    })

    except Exception as e:
        print(f"[Instagram] Error searching '{query}': {e}")
    return results


# ==========================================================================
# FACEBOOK SCRAPER (public pages)
# ==========================================================================

def facebook_search(query: str, max_results: int = 10) -> List[Dict]:
    """Scrape Facebook public search results."""
    results = []
    try:
        with httpx.Client(
            timeout=15,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "en-US,en;q=0.9",
            },
            follow_redirects=True
        ) as client:
            # Facebook public search
            encoded_q = quote_plus(query)
            resp = client.get(
                f"https://www.facebook.com/search/pages/?q={encoded_q}"
            )

            if resp.status_code != 200:
                # Try mobile endpoint
                resp = client.get(
                    f"https://m.facebook.com/search/pages/?q={encoded_q}",
                    headers={"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X)"}
                )

            if resp.status_code != 200:
                return results

            page_html = resp.text

            # Extract page links and names
            # Facebook pages: /pagename/ or /pages/category/pagename/id
            page_links = re.findall(
                r'href="(https?://(?:www\.)?facebook\.com/[^"?]+)"[^>]*>([^<]{3,80})<',
                page_html
            )
            # Also try structured data
            ld_json = re.findall(
                r'<script type="application/ld\+json">(.*?)</script>',
                page_html, re.DOTALL
            )
            for ld in ld_json:
                try:
                    data = json.loads(ld)
                    if isinstance(data, list):
                        for item in data[:max_results]:
                            name = item.get("name", "")
                            url = item.get("url", "")
                            desc = item.get("description", "")
                            if name and url:
                                results.append({
                                    "platform": "facebook",
                                    "video_id": url.split("/")[-1] or url.split("/")[-2],
                                    "title": name[:200],
                                    "description": desc[:500],
                                    "url": url,
                                    "author": name,
                                    "author_url": url,
                                    "views": 0,
                                    "likes": 0,
                                    "comments": 0,
                                    "shares": 0,
                                    "engagement_rate": 0.0,
                                    "hook": extract_hook(name, desc),
                                    "cta": detect_cta(desc),
                                    "selling": detect_selling(desc),
                                    "hashtags": json.dumps(re.findall(r"#\w+", desc)[:20]),
                                    "published": "",
                                    "query": query,
                                })
                except (json.JSONDecodeError, TypeError):
                    continue

            # Fallback: extract from raw links
            if not results:
                seen = set()
                for url, name in page_links:
                    name = html.unescape(name).strip()
                    url = url.split("?")[0]
                    if url in seen or not name or len(name) < 3:
                        continue
                    # Skip navigation/generic links
                    if any(skip in url.lower() for skip in [
                        "/login", "/help", "/privacy", "/policies",
                        "/groups/", "/events/", "/marketplace/",
                        "facebook.com/search", "/directory/"
                    ]):
                        continue
                    seen.add(url)
                    results.append({
                        "platform": "facebook",
                        "video_id": url.split("/")[-1] or url.split("/")[-2],
                        "title": name[:200],
                        "description": "",
                        "url": url,
                        "author": name,
                        "author_url": url,
                        "views": 0,
                        "likes": 0,
                        "comments": 0,
                        "shares": 0,
                        "engagement_rate": 0.0,
                        "hook": name,
                        "cta": "",
                        "selling": "",
                        "hashtags": "[]",
                        "published": "",
                        "query": query,
                    })
                    if len(results) >= max_results:
                        break

    except Exception as e:
        print(f"[Facebook] Error searching '{query}': {e}")
    return results


# ==========================================================================
# CORE SCAN ENGINE
# ==========================================================================

def store_content(item: Dict, niche: str) -> Optional[str]:
    """Store a piece of content in the database. Returns content_id or None if duplicate."""
    content_id = compute_content_id(item["platform"], item.get("url", item.get("video_id", "")))
    item["engagement_rate"] = calc_engagement_rate(
        item.get("views", 0), item.get("likes", 0),
        item.get("comments", 0), item.get("shares", 0)
    )

    with get_db() as db:
        # Check if already exists
        existing = db.execute("SELECT id FROM content WHERE id = ?", (content_id,)).fetchone()
        if existing:
            # Update stats if they changed
            db.execute("""
                UPDATE content SET views = MAX(views, ?), likes = MAX(likes, ?),
                comments = MAX(comments, ?), shares = MAX(shares, ?),
                engagement_rate = MAX(engagement_rate, ?)
                WHERE id = ?
            """, (
                item.get("views", 0), item.get("likes", 0),
                item.get("comments", 0), item.get("shares", 0),
                item["engagement_rate"], content_id
            ))
            return None  # Not new

        db.execute("""
            INSERT INTO content (id, platform, niche, title, description, url,
                author, author_url, views, likes, comments, shares,
                engagement_rate, hook, cta, selling, hashtags, query)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            content_id, item["platform"], niche,
            item.get("title", ""), item.get("description", ""),
            item.get("url", ""), item.get("author", ""),
            item.get("author_url", ""), item.get("views", 0),
            item.get("likes", 0), item.get("comments", 0),
            item.get("shares", 0), item["engagement_rate"],
            item.get("hook", ""), item.get("cta", ""),
            item.get("selling", ""), item.get("hashtags", "[]"),
            item.get("query", ""),
        ))

        # Store hook if non-empty
        hook = item.get("hook", "").strip()
        if hook:
            db.execute("""
                INSERT INTO hooks (platform, niche, hook_text, content_id,
                    engagement_score, hook_type)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                item["platform"], niche, hook, content_id,
                item["engagement_rate"],
                classify_hook_type(hook),
            ))

        # Store ad if CTA or selling detected
        cta = item.get("cta", "").strip()
        selling = item.get("selling", "").strip()
        if cta or selling:
            db.execute("""
                INSERT INTO ads (platform, niche, ad_copy, cta, product,
                    content_id, engagement_score)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                item["platform"], niche,
                item.get("title", "")[:300],
                cta, selling, content_id,
                item["engagement_rate"],
            ))

    return content_id


def classify_hook_type(hook: str) -> str:
    """Classify the type of hook used."""
    h = hook.lower()
    if any(w in h for w in ["how to", "how i", "step by step", "tutorial", "guide"]):
        return "how-to"
    if any(w in h for w in ["secret", "nobody", "they don't want", "hidden"]):
        return "secret"
    if any(w in h for w in ["mistake", "wrong", "stop doing", "never do"]):
        return "fear"
    if re.search(r"\d+\s+(?:ways|tips|reasons|things|hacks|secrets)", h):
        return "listicle"
    if any(w in h for w in ["$", "money", "income", "revenue", "profit", "earn"]):
        return "money"
    if "?" in hook:
        return "question"
    if any(w in h for w in ["this", "watch", "wait", "look"]):
        return "curiosity"
    if any(w in h for w in ["hack", "cheat", "shortcut", "trick"]):
        return "hack"
    return "statement"


def analyze_content_batch(content_ids: List[str], niche: str):
    """Use Ollama to analyze a batch of content and generate insights."""
    if not content_ids:
        return

    with get_db() as db:
        placeholders = ",".join(["?"] * len(content_ids))
        rows = db.execute(f"""
            SELECT id, platform, title, description, hook, cta, selling, views, likes
            FROM content WHERE id IN ({placeholders})
            ORDER BY views DESC LIMIT 10
        """, content_ids).fetchall()

    if not rows:
        return

    # Build analysis prompt
    content_summary = []
    for r in rows:
        content_summary.append(
            f"- [{r['platform'].upper()}] \"{r['title'][:100]}\" "
            f"(views: {r['views']:,}, likes: {r['likes']:,}) "
            f"CTA: {r['cta'] or 'none'} | Selling: {r['selling'] or 'none'}"
        )

    prompt = f"""Analyze these top-performing social media posts in the "{niche}" niche.
For each, explain WHY it works (what makes it engaging). Then give 3 content ideas we could create.
Keep it concise and actionable.

Content:
{chr(10).join(content_summary)}

Format:
ANALYSIS: (why these work)
PATTERNS: (common patterns you see)
IDEAS:
1. ...
2. ...
3. ..."""

    analysis = analyze_with_ollama_sync(prompt, timeout=45)
    if not analysis:
        return

    # Store analysis on top content
    with get_db() as db:
        for cid in content_ids[:5]:
            db.execute(
                "UPDATE content SET analysis = ? WHERE id = ? AND analysis IS NULL",
                (analysis[:500], cid)
            )

    # Parse and store ideas
    idea_lines = re.findall(r"\d+\.\s*(.+)", analysis)
    if idea_lines:
        with get_db() as db:
            for idea in idea_lines[:5]:
                idea = idea.strip()
                if len(idea) > 10:
                    db.execute("""
                        INSERT INTO ideas (niche, idea, based_on, platform, confidence)
                        VALUES (?, ?, ?, ?, ?)
                    """, (niche, idea, "competitive_scan", "multi", 0.6))

    # Extract and store trends
    pattern_match = re.search(r"PATTERNS?:\s*(.+?)(?:IDEAS?:|$)", analysis, re.DOTALL)
    if pattern_match:
        pattern_text = pattern_match.group(1).strip()
        if pattern_text:
            with get_db() as db:
                db.execute("""
                    INSERT INTO trends (platform, niche, trend_type, value, score, examples)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, ("multi", niche, "pattern", pattern_text[:300], 0.7,
                      json.dumps([r["title"][:100] for r in rows[:3]])))


def feed_to_nerve(niche: str, new_count: int, platform: str):
    """Send intelligence summary to nerve."""
    try:
        with get_db() as db:
            top = db.execute("""
                SELECT title, views, platform FROM content
                WHERE niche = ? ORDER BY found_at DESC, views DESC LIMIT 5
            """, (niche,)).fetchall()

        if not top:
            return

        summary_parts = [f"{r['platform']}: \"{r['title'][:60]}\" ({r['views']:,} views)" for r in top]
        fact = (
            f"Competitive intel scan [{niche}]: {new_count} new items found. "
            f"Top content: {'; '.join(summary_parts[:3])}"
        )

        with httpx.Client(timeout=10) as client:
            client.post(NERVE_URL, json={
                "fact": fact[:500],
                "category": "competitive_intelligence",
                "source": f"competitive_intel_agent/{platform}",
                "confidence": 0.7,
            })
    except Exception as e:
        print(f"[Nerve] Failed to feed: {e}")


def run_scan(niche: str = None, platform: str = None, query: str = None):
    """Run a competitive intelligence scan."""
    if query and platform:
        # Single query, single platform
        niches_to_scan = {niche or "general": [query]}
        platforms = [platform]
    elif niche and niche in NICHES:
        niches_to_scan = {niche: NICHES[niche]}
        platforms = [platform] if platform else ["youtube", "tiktok", "instagram", "facebook"]
    else:
        niches_to_scan = NICHES
        platforms = [platform] if platform else ["youtube", "tiktok", "instagram", "facebook"]

    total_new = 0
    scan_results = []

    for niche_name, queries in niches_to_scan.items():
        for q in queries:
            for plat in platforms:
                scan_id = None
                with get_db() as db:
                    cur = db.execute(
                        "INSERT INTO scans (platform, niche, query, status) VALUES (?, ?, ?, 'running')",
                        (plat, niche_name, q)
                    )
                    scan_id = cur.lastrowid

                try:
                    if plat == "youtube":
                        items = youtube_search(q, max_results=10)
                    elif plat == "tiktok":
                        items = tiktok_search(q, max_results=10)
                    elif plat == "instagram":
                        items = instagram_search(q, max_results=10)
                    elif plat == "facebook":
                        items = facebook_search(q, max_results=10)
                    else:
                        items = []

                    new_ids = []
                    for item in items:
                        cid = store_content(item, niche_name)
                        if cid:
                            new_ids.append(cid)

                    total_new += len(new_ids)

                    with get_db() as db:
                        db.execute("""
                            UPDATE scans SET status = 'done', results_count = ?,
                            finished_at = datetime('now') WHERE id = ?
                        """, (len(items), scan_id))

                    scan_results.append({
                        "platform": plat, "niche": niche_name, "query": q,
                        "found": len(items), "new": len(new_ids)
                    })

                    # Analyze new content with Ollama (limit to avoid overloading)
                    if new_ids:
                        analyze_content_batch(new_ids, niche_name)

                except Exception as e:
                    with get_db() as db:
                        db.execute(
                            "UPDATE scans SET status = 'error', error = ?, finished_at = datetime('now') WHERE id = ?",
                            (str(e)[:500], scan_id)
                        )
                    scan_results.append({
                        "platform": plat, "niche": niche_name, "query": q,
                        "error": str(e)[:200]
                    })

                # Brief pause between requests to be polite
                time.sleep(1)

        # Feed niche summary to nerve
        if total_new > 0:
            feed_to_nerve(niche_name, total_new, ",".join(platforms))

    return {"total_new": total_new, "scans": scan_results}


# ==========================================================================
# BACKGROUND SCHEDULER
# ==========================================================================

_scheduler_running = False


def scheduler_loop():
    """Background loop that runs scans every SCAN_INTERVAL_MINUTES."""
    global _scheduler_running
    _scheduler_running = True
    print(f"[Scheduler] Started — scanning every {SCAN_INTERVAL_MINUTES} minutes")

    while _scheduler_running:
        try:
            print(f"[Scheduler] Starting full scan at {datetime.now().isoformat()}")
            result = run_scan()
            print(f"[Scheduler] Scan complete: {result['total_new']} new items")
        except Exception as e:
            print(f"[Scheduler] Error: {e}")
            traceback.print_exc()

        # Sleep in small increments so we can stop cleanly
        for _ in range(SCAN_INTERVAL_MINUTES * 60):
            if not _scheduler_running:
                break
            time.sleep(1)


def start_scheduler():
    """Start the background scheduler thread."""
    t = threading.Thread(target=scheduler_loop, daemon=True, name="intel-scheduler")
    t.start()
    return t


# ==========================================================================
# FASTAPI APP
# ==========================================================================

app = FastAPI(
    title="Hive Competitive Intelligence",
    description="Watches social media for competitive intelligence across AI, locksmith, anime, and small business niches.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class ScanRequest(BaseModel):
    niche: Optional[str] = None
    platform: Optional[str] = None
    query: Optional[str] = None


@app.on_event("startup")
async def startup():
    init_db()
    start_scheduler()
    print(f"[CompetitiveIntel] Running on port {PORT}")
    print(f"[CompetitiveIntel] Monitoring {len(ALL_QUERIES)} queries across {len(NICHES)} niches")


@app.get("/health")
async def health():
    with get_db() as db:
        content_count = db.execute("SELECT COUNT(*) as c FROM content").fetchone()["c"]
        scan_count = db.execute("SELECT COUNT(*) as c FROM scans").fetchone()["c"]
        last_scan = db.execute(
            "SELECT finished_at FROM scans WHERE status='done' ORDER BY finished_at DESC LIMIT 1"
        ).fetchone()
    return {
        "status": "healthy",
        "service": "competitive-intel",
        "port": PORT,
        "content_items": content_count,
        "total_scans": scan_count,
        "last_scan": last_scan["finished_at"] if last_scan else None,
        "scheduler_running": _scheduler_running,
        "niches": list(NICHES.keys()),
        "scan_interval_minutes": SCAN_INTERVAL_MINUTES,
    }


@app.get("/api/trends")
async def get_trends(
    niche: Optional[str] = Query(None),
    platform: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
):
    """Get latest trends across all platforms."""
    with get_db() as db:
        conditions = []
        params = []
        if niche:
            conditions.append("t.niche = ?")
            params.append(niche)
        if platform:
            conditions.append("t.platform = ?")
            params.append(platform)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(limit)

        trends = db.execute(f"""
            SELECT t.*, COUNT(*) OVER (PARTITION BY t.niche) as niche_count
            FROM trends t {where}
            ORDER BY t.found_at DESC, t.score DESC
            LIMIT ?
        """, params).fetchall()

        # Also get top hashtags from content
        hashtag_rows = db.execute("""
            SELECT hashtags, platform, niche FROM content
            WHERE hashtags != '[]' AND hashtags IS NOT NULL
            ORDER BY found_at DESC LIMIT 200
        """).fetchall()

    # Aggregate hashtags
    hashtag_counts: Dict[str, int] = {}
    for row in hashtag_rows:
        try:
            tags = json.loads(row["hashtags"])
            for tag in tags:
                tag = tag.lower()
                hashtag_counts[tag] = hashtag_counts.get(tag, 0) + 1
        except (json.JSONDecodeError, TypeError):
            continue

    top_hashtags = sorted(hashtag_counts.items(), key=lambda x: x[1], reverse=True)[:30]

    return {
        "trends": [dict(t) for t in trends],
        "top_hashtags": [{"tag": tag, "count": count} for tag, count in top_hashtags],
        "niches_available": list(NICHES.keys()),
    }


@app.get("/api/hooks")
async def get_hooks(
    niche: Optional[str] = Query(None),
    platform: Optional[str] = Query(None),
    hook_type: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
):
    """Get best performing hooks found across platforms."""
    with get_db() as db:
        conditions = []
        params = []
        if niche:
            conditions.append("h.niche = ?")
            params.append(niche)
        if platform:
            conditions.append("h.platform = ?")
            params.append(platform)
        if hook_type:
            conditions.append("h.hook_type = ?")
            params.append(hook_type)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(limit)

        hooks = db.execute(f"""
            SELECT h.*, c.views, c.likes, c.url, c.author
            FROM hooks h
            LEFT JOIN content c ON h.content_id = c.id
            {where}
            ORDER BY h.engagement_score DESC, c.views DESC
            LIMIT ?
        """, params).fetchall()

        # Hook type distribution
        type_dist = db.execute("""
            SELECT hook_type, COUNT(*) as count, AVG(engagement_score) as avg_engagement
            FROM hooks GROUP BY hook_type ORDER BY count DESC
        """).fetchall()

    return {
        "hooks": [dict(h) for h in hooks],
        "hook_types": [dict(t) for t in type_dist],
        "total_hooks": len(hooks),
    }


@app.get("/api/ads")
async def get_ads(
    niche: Optional[str] = Query(None),
    platform: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
):
    """Get ad copy and selling strategies found."""
    with get_db() as db:
        conditions = []
        params = []
        if niche:
            conditions.append("a.niche = ?")
            params.append(niche)
        if platform:
            conditions.append("a.platform = ?")
            params.append(platform)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(limit)

        ads = db.execute(f"""
            SELECT a.*, c.views, c.likes, c.url, c.author, c.title as content_title
            FROM ads a
            LEFT JOIN content c ON a.content_id = c.id
            {where}
            ORDER BY a.engagement_score DESC
            LIMIT ?
        """, params).fetchall()

        # Product type distribution
        product_dist = db.execute("""
            SELECT product, COUNT(*) as count, AVG(engagement_score) as avg_engagement
            FROM ads WHERE product != '' AND product IS NOT NULL
            GROUP BY product ORDER BY count DESC LIMIT 20
        """).fetchall()

        # CTA distribution
        cta_dist = db.execute("""
            SELECT cta, COUNT(*) as count
            FROM ads WHERE cta != '' AND cta IS NOT NULL
            GROUP BY cta ORDER BY count DESC LIMIT 20
        """).fetchall()

    return {
        "ads": [dict(a) for a in ads],
        "product_types": [dict(p) for p in product_dist],
        "top_ctas": [dict(c) for c in cta_dist],
        "total_ads": len(ads),
    }


@app.get("/api/ideas")
async def get_ideas(
    niche: Optional[str] = Query(None),
    limit: int = Query(30, ge=1, le=100),
):
    """Get content ideas based on what's working in the market."""
    with get_db() as db:
        if niche:
            ideas = db.execute("""
                SELECT * FROM ideas WHERE niche = ?
                ORDER BY confidence DESC, generated_at DESC LIMIT ?
            """, (niche, limit)).fetchall()
        else:
            ideas = db.execute("""
                SELECT * FROM ideas
                ORDER BY confidence DESC, generated_at DESC LIMIT ?
            """, (limit,)).fetchall()

        # Also generate on-the-fly ideas from top content if we have few stored
        top_content = db.execute("""
            SELECT platform, niche, title, views, hook, cta, selling
            FROM content
            ORDER BY views DESC LIMIT 20
        """).fetchall()

    # If we have top content but few ideas, generate some
    result_ideas = [dict(i) for i in ideas]

    if len(result_ideas) < 5 and top_content:
        # Quick pattern-based ideas
        for row in top_content[:5]:
            title = row["title"]
            if title:
                result_ideas.append({
                    "niche": row["niche"],
                    "idea": f"Create our version of: \"{title[:80]}\" (got {row['views']:,} views on {row['platform']})",
                    "based_on": "top_performer",
                    "platform": row["platform"],
                    "confidence": 0.5,
                    "generated_at": datetime.now().isoformat(),
                })

    return {
        "ideas": result_ideas,
        "total": len(result_ideas),
        "niches": list(NICHES.keys()),
    }


@app.post("/api/scan")
async def trigger_scan(req: ScanRequest, background_tasks: BackgroundTasks):
    """Trigger a competitive intelligence scan."""
    # Validate inputs
    if req.platform and req.platform not in ["youtube", "tiktok", "instagram", "facebook"]:
        raise HTTPException(400, f"Invalid platform: {req.platform}. Use youtube/tiktok/instagram/facebook.")
    if req.niche and req.niche not in NICHES and not req.query:
        raise HTTPException(400, f"Invalid niche: {req.niche}. Available: {list(NICHES.keys())}")

    background_tasks.add_task(run_scan, req.niche, req.platform, req.query)
    return {
        "status": "scan_started",
        "niche": req.niche or "all",
        "platform": req.platform or "all",
        "query": req.query,
        "message": "Scan running in background. Check /api/trends for results.",
    }


@app.get("/api/stats")
async def get_stats():
    """Get overall statistics."""
    with get_db() as db:
        total_content = db.execute("SELECT COUNT(*) as c FROM content").fetchone()["c"]
        total_hooks = db.execute("SELECT COUNT(*) as c FROM hooks").fetchone()["c"]
        total_ads = db.execute("SELECT COUNT(*) as c FROM ads").fetchone()["c"]
        total_ideas = db.execute("SELECT COUNT(*) as c FROM ideas").fetchone()["c"]
        total_trends = db.execute("SELECT COUNT(*) as c FROM trends").fetchone()["c"]
        total_scans = db.execute("SELECT COUNT(*) as c FROM scans").fetchone()["c"]

        # Per-platform counts
        platform_counts = db.execute("""
            SELECT platform, COUNT(*) as count, SUM(views) as total_views,
                   AVG(engagement_rate) as avg_engagement
            FROM content GROUP BY platform
        """).fetchall()

        # Per-niche counts
        niche_counts = db.execute("""
            SELECT niche, COUNT(*) as count, SUM(views) as total_views,
                   MAX(views) as top_views
            FROM content GROUP BY niche
        """).fetchall()

        # Recent scans
        recent_scans = db.execute("""
            SELECT platform, niche, query, results_count, status, finished_at
            FROM scans ORDER BY id DESC LIMIT 10
        """).fetchall()

    return {
        "totals": {
            "content": total_content,
            "hooks": total_hooks,
            "ads": total_ads,
            "ideas": total_ideas,
            "trends": total_trends,
            "scans": total_scans,
        },
        "by_platform": [dict(p) for p in platform_counts],
        "by_niche": [dict(n) for n in niche_counts],
        "recent_scans": [dict(s) for s in recent_scans],
    }


@app.get("/api/top")
async def get_top_content(
    niche: Optional[str] = Query(None),
    platform: Optional[str] = Query(None),
    days: int = Query(7, ge=1, le=90),
    limit: int = Query(20, ge=1, le=100),
):
    """Get top performing content from the last N days."""
    with get_db() as db:
        conditions = [f"found_at >= datetime('now', '-{days} days')"]
        params = []
        if niche:
            conditions.append("niche = ?")
            params.append(niche)
        if platform:
            conditions.append("platform = ?")
            params.append(platform)
        where = f"WHERE {' AND '.join(conditions)}"
        params.append(limit)

        content = db.execute(f"""
            SELECT * FROM content {where}
            ORDER BY views DESC, engagement_rate DESC
            LIMIT ?
        """, params).fetchall()

    return {
        "top_content": [dict(c) for c in content],
        "count": len(content),
        "period_days": days,
    }


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Simple HTML dashboard."""
    with get_db() as db:
        total_content = db.execute("SELECT COUNT(*) as c FROM content").fetchone()["c"]
        total_hooks = db.execute("SELECT COUNT(*) as c FROM hooks").fetchone()["c"]
        total_ads = db.execute("SELECT COUNT(*) as c FROM ads").fetchone()["c"]
        total_ideas = db.execute("SELECT COUNT(*) as c FROM ideas").fetchone()["c"]

        top_content = db.execute("""
            SELECT platform, niche, title, views, likes, engagement_rate, hook, url, author
            FROM content ORDER BY views DESC LIMIT 20
        """).fetchall()

        top_hooks = db.execute("""
            SELECT h.hook_text, h.hook_type, h.engagement_score, h.platform, h.niche,
                   c.views, c.url
            FROM hooks h LEFT JOIN content c ON h.content_id = c.id
            ORDER BY h.engagement_score DESC, c.views DESC LIMIT 15
        """).fetchall()

        latest_ideas = db.execute("""
            SELECT * FROM ideas ORDER BY generated_at DESC LIMIT 10
        """).fetchall()

        last_scan = db.execute(
            "SELECT finished_at FROM scans WHERE status='done' ORDER BY finished_at DESC LIMIT 1"
        ).fetchone()

    content_rows = ""
    for c in top_content:
        platform_emoji = {"youtube": "YT", "tiktok": "TT", "instagram": "IG", "facebook": "FB"}.get(c["platform"], c["platform"])
        title_short = html.escape(c["title"][:80]) if c["title"] else "N/A"
        content_rows += f"""
        <tr>
            <td><span class="badge badge-{c['platform']}">{platform_emoji}</span></td>
            <td>{c['niche'] or ''}</td>
            <td><a href="{c['url'] or '#'}" target="_blank">{title_short}</a></td>
            <td>{c['author'] or ''}</td>
            <td>{c['views']:,}</td>
            <td>{c['likes']:,}</td>
            <td>{c['engagement_rate']:.2f}%</td>
        </tr>"""

    hook_rows = ""
    for h in top_hooks:
        hook_text = html.escape(h["hook_text"][:100]) if h["hook_text"] else ""
        hook_rows += f"""
        <tr>
            <td>{h['platform'] or ''}</td>
            <td>{h['niche'] or ''}</td>
            <td>{hook_text}</td>
            <td><span class="hook-type">{h['hook_type'] or ''}</span></td>
            <td>{h['engagement_score']:.2f}%</td>
            <td>{h['views'] or 0:,}</td>
        </tr>"""

    idea_rows = ""
    for i in latest_ideas:
        idea_text = html.escape(i["idea"][:120]) if i["idea"] else ""
        idea_rows += f"""
        <tr>
            <td>{i['niche'] or ''}</td>
            <td>{idea_text}</td>
            <td>{i['confidence']:.0%}</td>
            <td>{i['generated_at'] or ''}</td>
        </tr>"""

    last_scan_time = last_scan["finished_at"] if last_scan else "Never"

    return f"""<!DOCTYPE html>
<html>
<head>
    <title>Hive Competitive Intelligence</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0a0a0a; color: #e0e0e0; padding: 20px; }}
        h1 {{ color: #00ff88; margin-bottom: 5px; }}
        h2 {{ color: #00ccff; margin: 20px 0 10px; border-bottom: 1px solid #333; padding-bottom: 5px; }}
        .subtitle {{ color: #888; margin-bottom: 20px; }}
        .stats {{ display: flex; gap: 15px; margin: 15px 0; flex-wrap: wrap; }}
        .stat {{ background: #1a1a2e; padding: 15px 20px; border-radius: 8px; border: 1px solid #333; min-width: 140px; }}
        .stat .value {{ font-size: 28px; font-weight: bold; color: #00ff88; }}
        .stat .label {{ color: #888; font-size: 12px; text-transform: uppercase; }}
        table {{ width: 100%; border-collapse: collapse; margin: 10px 0; }}
        th {{ background: #1a1a2e; color: #00ccff; padding: 10px; text-align: left; font-size: 12px; text-transform: uppercase; }}
        td {{ padding: 8px 10px; border-bottom: 1px solid #222; font-size: 13px; }}
        tr:hover {{ background: #1a1a2e; }}
        a {{ color: #00ccff; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
        .badge {{ padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: bold; }}
        .badge-youtube {{ background: #ff0000; color: white; }}
        .badge-tiktok {{ background: #000; color: #00f2ea; border: 1px solid #00f2ea; }}
        .badge-instagram {{ background: #833ab4; color: white; }}
        .badge-facebook {{ background: #1877f2; color: white; }}
        .hook-type {{ background: #333; padding: 2px 6px; border-radius: 3px; font-size: 11px; }}
        .actions {{ margin: 15px 0; }}
        .btn {{ background: #00ff88; color: #000; padding: 8px 16px; border: none; border-radius: 5px; cursor: pointer; font-weight: bold; margin-right: 8px; }}
        .btn:hover {{ background: #00cc6a; }}
        .btn-secondary {{ background: #333; color: #00ccff; }}
        .api-links {{ background: #1a1a2e; padding: 10px; border-radius: 5px; margin: 10px 0; }}
        .api-links a {{ margin-right: 15px; }}
    </style>
</head>
<body>
    <h1>HIVE Competitive Intelligence</h1>
    <p class="subtitle">Social media monitoring across {len(NICHES)} niches | Last scan: {last_scan_time}</p>

    <div class="stats">
        <div class="stat"><div class="value">{total_content}</div><div class="label">Content Items</div></div>
        <div class="stat"><div class="value">{total_hooks}</div><div class="label">Hooks Found</div></div>
        <div class="stat"><div class="value">{total_ads}</div><div class="label">Ads/CTAs</div></div>
        <div class="stat"><div class="value">{total_ideas}</div><div class="label">Content Ideas</div></div>
    </div>

    <div class="api-links">
        <strong>API:</strong>
        <a href="/api/trends">Trends</a>
        <a href="/api/hooks">Hooks</a>
        <a href="/api/ads">Ads</a>
        <a href="/api/ideas">Ideas</a>
        <a href="/api/stats">Stats</a>
        <a href="/api/top">Top Content</a>
        <a href="/health">Health</a>
    </div>

    <h2>Top Content by Views</h2>
    <table>
        <tr><th>Platform</th><th>Niche</th><th>Title</th><th>Author</th><th>Views</th><th>Likes</th><th>Engagement</th></tr>
        {content_rows if content_rows else '<tr><td colspan="7" style="text-align:center;color:#666;">No content yet. First scan running...</td></tr>'}
    </table>

    <h2>Best Hooks</h2>
    <table>
        <tr><th>Platform</th><th>Niche</th><th>Hook</th><th>Type</th><th>Engagement</th><th>Views</th></tr>
        {hook_rows if hook_rows else '<tr><td colspan="6" style="text-align:center;color:#666;">No hooks yet.</td></tr>'}
    </table>

    <h2>Content Ideas</h2>
    <table>
        <tr><th>Niche</th><th>Idea</th><th>Confidence</th><th>Generated</th></tr>
        {idea_rows if idea_rows else '<tr><td colspan="4" style="text-align:center;color:#666;">No ideas yet. Waiting for analysis...</td></tr>'}
    </table>

    <div class="actions" style="margin-top: 20px;">
        <button class="btn" onclick="fetch('/api/scan', {{method:'POST', headers:{{'Content-Type':'application/json'}}, body:'{{}}'}}).then(r=>r.json()).then(d=>alert('Scan started!'))">Trigger Full Scan</button>
        <button class="btn btn-secondary" onclick="location.reload()">Refresh</button>
    </div>

    <script>
        // Auto-refresh every 60 seconds
        setTimeout(() => location.reload(), 60000);
    </script>
</body>
</html>"""


# ==========================================================================
# MAIN
# ==========================================================================

if __name__ == "__main__":
    init_db()
    print(f"[CompetitiveIntel] Starting on port {PORT}")
    print(f"[CompetitiveIntel] DB: {DB_PATH}")
    print(f"[CompetitiveIntel] Niches: {list(NICHES.keys())}")
    print(f"[CompetitiveIntel] Scan interval: {SCAN_INTERVAL_MINUTES} min")
    uvicorn.run(app, host="0.0.0.0", port=PORT)
