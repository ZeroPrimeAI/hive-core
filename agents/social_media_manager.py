#!/usr/bin/env python3
"""
THE HIVE -- Social Media Manager
Port 8913 | SQLite at /home/zero/hivecode_sandbox/social_media.db
MIT License

Centralized social media management for all Hive brands:
  - Tracks accounts across 7 platforms (YouTube, TikTok, Instagram, Facebook, X, SoundCloud, Reddit)
  - Content scheduling with automated calendar (daily/weekly/monthly cadence)
  - Pulls content from producer (8900), shorts (/tmp/ghost_shorts/), studio (8911)
  - Cross-platform analytics tracking
  - Growth strategy engine with hashtag + engagement recommendations
  - Content repurposing pipeline (1 episode -> shorts -> TikToks -> reels)

Brands: Ghost in the Machine (anime/AI), Hive Dynamics AI (tech), Locksmith businesses
"""

import json
import sqlite3
import time
import threading
import os
import glob
import hashlib
import traceback
import html as html_lib
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any
from contextlib import contextmanager

import httpx
from fastapi import FastAPI, BackgroundTasks, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
import uvicorn

# ==========================================================================
# CONFIG
# ==========================================================================
PORT = 8913
DB_PATH = "/home/zero/hivecode_sandbox/social_media.db"

PRODUCER_URL = "http://localhost:8900"
STUDIO_URL = "http://localhost:8911"
SHORTS_DIR = "/tmp/ghost_shorts/"
NERVE_URL = "http://100.105.160.106:8200/api/add"

SCAN_INTERVAL_MINUTES = 30  # scan for new content every 30 min
ANALYTICS_INTERVAL_MINUTES = 60  # refresh analytics hourly

# ==========================================================================
# PLATFORM DEFINITIONS
# ==========================================================================
PLATFORMS = {
    "youtube": {
        "name": "YouTube",
        "icon": "YT",
        "color": "#FF0000",
        "content_types": ["video", "short", "live", "podcast"],
        "max_hashtags": 15,
        "best_posting_hours": [9, 12, 15, 17, 20],  # UTC
        "char_limit_title": 100,
        "char_limit_description": 5000,
    },
    "tiktok": {
        "name": "TikTok",
        "icon": "TT",
        "color": "#000000",
        "content_types": ["video", "short", "live"],
        "max_hashtags": 5,
        "best_posting_hours": [7, 10, 14, 19, 22],
        "char_limit_title": 0,
        "char_limit_description": 2200,
    },
    "instagram": {
        "name": "Instagram",
        "icon": "IG",
        "color": "#E1306C",
        "content_types": ["reel", "post", "story", "carousel"],
        "max_hashtags": 30,
        "best_posting_hours": [8, 11, 14, 17, 21],
        "char_limit_title": 0,
        "char_limit_description": 2200,
    },
    "facebook": {
        "name": "Facebook",
        "icon": "FB",
        "color": "#1877F2",
        "content_types": ["video", "reel", "post", "live"],
        "max_hashtags": 10,
        "best_posting_hours": [9, 13, 16, 19],
        "char_limit_title": 0,
        "char_limit_description": 63206,
    },
    "x_twitter": {
        "name": "X / Twitter",
        "icon": "X",
        "color": "#000000",
        "content_types": ["tweet", "thread", "video", "poll"],
        "max_hashtags": 3,
        "best_posting_hours": [8, 12, 17, 21],
        "char_limit_title": 0,
        "char_limit_description": 280,
    },
    "soundcloud": {
        "name": "SoundCloud",
        "icon": "SC",
        "color": "#FF5500",
        "content_types": ["track", "playlist", "album"],
        "max_hashtags": 5,
        "best_posting_hours": [10, 14, 20],
        "char_limit_title": 100,
        "char_limit_description": 4000,
    },
    "reddit": {
        "name": "Reddit",
        "icon": "RD",
        "color": "#FF4500",
        "content_types": ["post", "video", "link", "crosspost"],
        "max_hashtags": 0,
        "best_posting_hours": [8, 10, 13, 18],
        "char_limit_title": 300,
        "char_limit_description": 40000,
    },
}

# ==========================================================================
# BRAND DEFINITIONS
# ==========================================================================
BRANDS = {
    "ghost": {
        "name": "Ghost in the Machine",
        "description": "AI anime series -- fictional Japanese setting, AI awakening story",
        "platforms": ["youtube", "tiktok", "instagram", "x_twitter", "soundcloud", "reddit"],
        "hashtags": {
            "core": ["#GhostInTheMachine", "#AIAnime", "#AnimeAI"],
            "youtube": ["#anime", "#artificialintelligence", "#AIart", "#animation", "#scifi",
                        "#japanime", "#AIgenerated", "#newanime", "#indieAnime"],
            "tiktok": ["#anime", "#AIanime", "#fyp", "#viral", "#animeart"],
            "instagram": ["#anime", "#AIart", "#animeart", "#digitalart", "#scifi",
                          "#animefan", "#animelife", "#aiartist", "#indieanimation",
                          "#animecommunity", "#japanime", "#animeedit"],
            "x_twitter": ["#AIAnime", "#IndieAnime", "#AIArt"],
            "soundcloud": ["#anime", "#ost", "#ambient", "#electronic", "#AImusic"],
            "reddit": [],  # no hashtags on reddit
        },
        "subreddits": ["r/anime", "r/IndieAnimation", "r/AIart", "r/artificial",
                        "r/MachineLearning", "r/Futurology"],
        "tone": "mysterious, philosophical, cinematic",
    },
    "hiveai": {
        "name": "Hive Dynamics AI",
        "description": "AI technology company -- products, tools, services",
        "platforms": ["youtube", "instagram", "x_twitter", "reddit"],
        "hashtags": {
            "core": ["#HiveDynamics", "#AITools", "#AIBusiness"],
            "youtube": ["#AI", "#machinelearning", "#automation", "#techstartup",
                        "#artificialintelligence", "#AIagent", "#SaaS"],
            "instagram": ["#AI", "#techstartup", "#AItools", "#automation",
                          "#coding", "#developer", "#buildinpublic"],
            "x_twitter": ["#AI", "#BuildInPublic", "#IndieHacker"],
            "reddit": [],
        },
        "subreddits": ["r/artificial", "r/MachineLearning", "r/SideProject",
                        "r/startups", "r/Entrepreneur"],
        "tone": "professional, innovative, transparent",
    },
    "locksmith": {
        "name": "Locksmith Services",
        "description": "NW Florida locksmith businesses -- local SEO, emergency services",
        "platforms": ["youtube", "instagram", "facebook", "tiktok"],
        "hashtags": {
            "core": ["#Locksmith", "#EmergencyLocksmith", "#NWFlorida"],
            "youtube": ["#locksmith", "#lockpicking", "#homesecurity", "#carsecurity",
                        "#destin", "#fortwaltonbeach", "#emeraldcoast"],
            "tiktok": ["#locksmith", "#lockedout", "#fyp", "#satisfying", "#keys"],
            "instagram": ["#locksmith", "#locksmithlife", "#homesecurity", "#destin",
                          "#fortwaltonbeach", "#emeraldcoast", "#nwflorida",
                          "#carsecurity", "#lockedout", "#247service"],
            "facebook": ["#locksmith", "#destin", "#fortwaltonbeach", "#emeraldcoast",
                          "#homesecurity", "#emergency"],
        },
        "subreddits": [],
        "tone": "trustworthy, urgent, local, helpful",
    },
}

# ==========================================================================
# CONTENT CALENDAR DEFINITIONS
# ==========================================================================
DAILY_TARGETS = {
    "youtube_short": 2,
    "tiktok_video": 1,
    "instagram_reel": 1,
    "x_tweet": 3,
}

WEEKLY_TARGETS = {
    "youtube_episode": 1,
    "soundcloud_track": 1,
    "blog_post": 1,
    "reddit_post": 2,
    "instagram_carousel": 1,
    "facebook_video": 1,
}

MONTHLY_TARGETS = {
    "compilation_video": 1,
    "behind_the_scenes": 1,
    "community_poll": 2,
    "collaboration_outreach": 4,
}

# ==========================================================================
# GROWTH STRATEGY
# ==========================================================================
GROWTH_STRATEGY = {
    "phase_1_launch": {
        "name": "Phase 1: Foundation (Month 1-2)",
        "focus": "Build consistent posting rhythm, establish brand presence",
        "priorities": [
            "YouTube Shorts (fastest organic reach, algorithm-friendly)",
            "TikTok (massive organic discovery, young demo overlap with anime)",
            "Instagram Reels (cross-post from TikTok, build visual brand)",
        ],
        "daily_actions": [
            "Post 2 YouTube Shorts (Ghost anime clips + behind-scenes)",
            "Post 1 TikTok (repurposed Short with TikTok-native captions)",
            "Post 1 Instagram Reel (repurposed with IG hashtags)",
            "Post 3 tweets/threads (engagement bait, anime takes, AI news)",
        ],
        "milestones": {
            "youtube_subs": 100,
            "tiktok_followers": 500,
            "instagram_followers": 200,
            "total_views": 10000,
        },
    },
    "phase_2_growth": {
        "name": "Phase 2: Growth (Month 3-6)",
        "focus": "Content repurposing machine, community building, collaborations",
        "priorities": [
            "Content repurposing pipeline (1 episode = 5+ shorts across platforms)",
            "Community engagement (Reddit, Discord, comment replies)",
            "Collaborations with small anime/AI creators",
            "SoundCloud presence for anime OST (drives to YouTube)",
        ],
        "tactics": [
            "Reply to EVERY comment within 1 hour",
            "Duet/stitch trending anime TikToks",
            "Share production process as behind-the-scenes content",
            "Cross-promote: SoundCloud OST -> YouTube episode -> TikTok clips",
            "Reddit AMAs in anime/AI subreddits",
        ],
        "milestones": {
            "youtube_subs": 1000,
            "tiktok_followers": 5000,
            "instagram_followers": 1000,
            "soundcloud_plays": 5000,
            "monthly_views": 50000,
        },
    },
    "phase_3_monetize": {
        "name": "Phase 3: Monetization (Month 6-12)",
        "focus": "YouTube Partner Program, brand deals, merch, Patreon",
        "priorities": [
            "Hit YouTube Partner (1K subs + 4K watch hours OR 10M Shorts views)",
            "Launch Patreon with exclusive content tiers",
            "Merch drops tied to episode releases",
            "Brand partnerships with AI tools / anime services",
        ],
        "revenue_targets": {
            "youtube_adsense": "$50-200/mo",
            "patreon": "$100-500/mo",
            "merch": "$50-200/mo",
            "brand_deals": "$200-1000/mo",
            "total_target": "$400-1900/mo",
        },
        "milestones": {
            "youtube_subs": 5000,
            "tiktok_followers": 25000,
            "instagram_followers": 5000,
            "monthly_revenue": 500,
        },
    },
    "content_repurposing": {
        "name": "Content Repurposing Pipeline",
        "description": "Turn 1 piece of content into 10+ posts across platforms",
        "pipeline": [
            "1 full anime episode (10-15 min)",
            "-> 5 YouTube Shorts (best scenes, cliffhangers, reveals)",
            "-> 5 TikToks (same clips, TikTok-native captions + trending audio)",
            "-> 5 Instagram Reels (same clips, IG hashtags, carousels for stills)",
            "-> 1 SoundCloud track (episode OST / ambient mix)",
            "-> 3-5 tweets (quotes, stills, episode discussion threads)",
            "-> 1 Reddit post (episode discussion + artwork)",
            "-> 1 behind-the-scenes (production process, AI tools used)",
            "TOTAL: 1 episode = 20-25 pieces of content",
        ],
    },
    "hashtag_strategy": {
        "youtube": "Mix of broad (#anime, #AI) + niche (#AIanime, #indieanimation). "
                   "Use 5-8 in description, put primary in title if natural.",
        "tiktok": "Max 3-5 hashtags. Always include #fyp. Mix trending + niche. "
                  "Put in caption, not comments. Trending sounds > hashtags.",
        "instagram": "Use all 30 slots. Mix: 10 broad (1M+), 10 medium (100K-1M), "
                     "10 niche (<100K). Rotate sets to avoid shadowban. "
                     "Put in first comment, not caption.",
        "x_twitter": "Max 1-2 hashtags per tweet. Engagement > hashtags. "
                     "Quote tweet trending topics with anime/AI angle.",
        "reddit": "NO hashtags. Title is everything. Match subreddit culture. "
                  "Be a community member first, promoter second.",
        "soundcloud": "Genre tags + mood tags. Consistent naming: "
                      "'Ghost in the Machine OST - Episode X - Track Name'",
    },
    "engagement_tactics": [
        "Reply to every comment within 1 hour (builds algorithm favor)",
        "Ask questions at the end of every video ('What would you do if AI woke up?')",
        "Pin controversial/thought-provoking comments to spark discussion",
        "Use Community tab for polls, behind-scenes, teasers",
        "Go live during episode premieres for watch-along",
        "Create a Discord for superfans (link in all bios)",
        "Duet/stitch/remix other creators' content (TikTok, IG)",
        "Share fan art / fan theories (builds community ownership)",
        "Consistent posting schedule (train the algorithm AND your audience)",
        "End screens + cards on every YouTube video (chain viewing)",
        "Create a series playlist with auto-play enabled",
        "Use 'Subscribe' CTA in first 30 seconds of every video",
    ],
}

# ==========================================================================
# PYDANTIC MODELS
# ==========================================================================
class AccountCreate(BaseModel):
    platform: str
    brand: str
    username: str
    url: Optional[str] = None
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    access_token: Optional[str] = None
    notes: Optional[str] = None


class SchedulePost(BaseModel):
    platform: str
    brand: str
    content_type: str  # video, short, reel, tweet, track, post
    title: str
    description: Optional[str] = ""
    hashtags: Optional[List[str]] = []
    file_path: Optional[str] = None
    scheduled_time: Optional[str] = None  # ISO format, or null for next best slot
    thumbnail_path: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = {}


class AnalyticsUpdate(BaseModel):
    platform: str
    brand: str
    content_id: Optional[str] = None
    views: Optional[int] = 0
    likes: Optional[int] = 0
    comments: Optional[int] = 0
    shares: Optional[int] = 0
    subscribers_gained: Optional[int] = 0
    watch_time_hours: Optional[float] = 0.0
    impressions: Optional[int] = 0
    click_through_rate: Optional[float] = 0.0


# ==========================================================================
# DATABASE
# ==========================================================================
def get_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


@contextmanager
def db_cursor():
    conn = get_db()
    try:
        cur = conn.cursor()
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    with db_cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform TEXT NOT NULL,
                brand TEXT NOT NULL,
                username TEXT NOT NULL,
                url TEXT DEFAULT '',
                api_key TEXT DEFAULT '',
                api_secret TEXT DEFAULT '',
                access_token TEXT DEFAULT '',
                status TEXT DEFAULT 'needs_setup',
                followers INTEGER DEFAULT 0,
                total_posts INTEGER DEFAULT 0,
                notes TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                UNIQUE(platform, brand)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS scheduled_posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform TEXT NOT NULL,
                brand TEXT NOT NULL,
                content_type TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT DEFAULT '',
                hashtags TEXT DEFAULT '[]',
                file_path TEXT DEFAULT '',
                thumbnail_path TEXT DEFAULT '',
                scheduled_time TEXT NOT NULL,
                status TEXT DEFAULT 'scheduled',
                posted_url TEXT DEFAULT '',
                content_id TEXT DEFAULT '',
                error_message TEXT DEFAULT '',
                metadata TEXT DEFAULT '{}',
                created_at TEXT DEFAULT (datetime('now')),
                posted_at TEXT DEFAULT ''
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS analytics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform TEXT NOT NULL,
                brand TEXT NOT NULL,
                content_id TEXT DEFAULT '',
                post_id INTEGER DEFAULT 0,
                date TEXT NOT NULL,
                views INTEGER DEFAULT 0,
                likes INTEGER DEFAULT 0,
                comments INTEGER DEFAULT 0,
                shares INTEGER DEFAULT 0,
                subscribers_gained INTEGER DEFAULT 0,
                watch_time_hours REAL DEFAULT 0.0,
                impressions INTEGER DEFAULT 0,
                click_through_rate REAL DEFAULT 0.0,
                engagement_rate REAL DEFAULT 0.0,
                created_at TEXT DEFAULT (datetime('now')),
                UNIQUE(platform, brand, content_id, date)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS content_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                source_id TEXT DEFAULT '',
                file_path TEXT NOT NULL,
                content_type TEXT NOT NULL,
                title TEXT DEFAULT '',
                description TEXT DEFAULT '',
                brand TEXT DEFAULT 'ghost',
                duration_seconds REAL DEFAULT 0,
                metadata TEXT DEFAULT '{}',
                status TEXT DEFAULT 'queued',
                discovered_at TEXT DEFAULT (datetime('now')),
                processed_at TEXT DEFAULT ''
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS daily_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                platform TEXT NOT NULL,
                brand TEXT NOT NULL,
                content_type TEXT NOT NULL,
                count INTEGER DEFAULT 0,
                target INTEGER DEFAULT 0,
                UNIQUE(date, platform, brand, content_type)
            )
        """)

        # Seed default accounts
        for brand_key, brand_info in BRANDS.items():
            for plat in brand_info["platforms"]:
                try:
                    cur.execute("""
                        INSERT OR IGNORE INTO accounts (platform, brand, username, status, notes)
                        VALUES (?, ?, ?, ?, ?)
                    """, (
                        plat,
                        brand_key,
                        f"{brand_key}_{plat}",
                        "active" if (plat == "youtube" and brand_key == "ghost") else "needs_setup",
                        f"Auto-created for {brand_info['name']} on {PLATFORMS[plat]['name']}"
                    ))
                except sqlite3.IntegrityError:
                    pass

        # Create indices for performance
        cur.execute("CREATE INDEX IF NOT EXISTS idx_scheduled_status ON scheduled_posts(status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_scheduled_time ON scheduled_posts(scheduled_time)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_analytics_date ON analytics(date)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_content_queue_status ON content_queue(status)")


# ==========================================================================
# CONTENT DISCOVERY
# ==========================================================================
def scan_shorts_directory() -> List[Dict]:
    """Scan /tmp/ghost_shorts/ for new content to queue."""
    discovered = []
    shorts_dir = SHORTS_DIR

    if not os.path.isdir(shorts_dir):
        return discovered

    # Scan root for numbered shorts
    for f in sorted(glob.glob(os.path.join(shorts_dir, "short_*.mp4"))):
        basename = os.path.basename(f)
        num = basename.replace("short_", "").replace(".mp4", "")
        discovered.append({
            "source": "ghost_shorts",
            "source_id": f"short_{num}",
            "file_path": f,
            "content_type": "short",
            "title": f"Ghost in the Machine - Short #{num}",
            "brand": "ghost",
        })

    # Scan subdirectories (locksmith, ghost, etc.)
    for subdir in ["ghost", "locksmith", "hiveai"]:
        sub_path = os.path.join(shorts_dir, subdir)
        if os.path.isdir(sub_path):
            for f in sorted(glob.glob(os.path.join(sub_path, "*.mp4"))):
                basename = os.path.basename(f).replace(".mp4", "")
                meta_path = f.replace(".mp4", "_meta.json")
                meta = {}
                if os.path.isfile(meta_path):
                    try:
                        with open(meta_path) as mf:
                            meta = json.load(mf)
                    except Exception:
                        pass
                brand = subdir if subdir in BRANDS else "ghost"
                discovered.append({
                    "source": "ghost_shorts",
                    "source_id": f"{subdir}/{basename}",
                    "file_path": f,
                    "content_type": "short",
                    "title": meta.get("title", basename.replace("_", " ").title()),
                    "description": meta.get("description", ""),
                    "brand": brand,
                    "metadata": meta,
                })

    return discovered


def scan_producer_content() -> List[Dict]:
    """Pull content from the producer service (port 8900)."""
    discovered = []
    try:
        with httpx.Client(timeout=10) as client:
            resp = client.get(f"{PRODUCER_URL}/api/content")
            if resp.status_code == 200:
                data = resp.json()
                items = data if isinstance(data, list) else data.get("content", [])
                for item in items:
                    discovered.append({
                        "source": "producer",
                        "source_id": str(item.get("id", "")),
                        "file_path": item.get("file_path", ""),
                        "content_type": item.get("type", "video"),
                        "title": item.get("title", "Untitled"),
                        "description": item.get("description", ""),
                        "brand": item.get("brand", "ghost"),
                        "metadata": item,
                    })
    except Exception:
        pass  # producer may be offline
    return discovered


def scan_studio_music() -> List[Dict]:
    """Pull music tracks from the studio service (port 8911)."""
    discovered = []
    try:
        with httpx.Client(timeout=10) as client:
            resp = client.get(f"{STUDIO_URL}/api/tracks")
            if resp.status_code == 200:
                data = resp.json()
                tracks = data if isinstance(data, list) else data.get("tracks", [])
                for track in tracks:
                    discovered.append({
                        "source": "studio",
                        "source_id": str(track.get("id", "")),
                        "file_path": track.get("file_path", ""),
                        "content_type": "track",
                        "title": track.get("title", "Untitled Track"),
                        "description": track.get("description", ""),
                        "brand": track.get("brand", "ghost"),
                        "metadata": track,
                    })
    except Exception:
        pass  # studio may be offline
    return discovered


def discover_and_queue_content():
    """Run all content scanners and queue new items."""
    all_content = []
    all_content.extend(scan_shorts_directory())
    all_content.extend(scan_producer_content())
    all_content.extend(scan_studio_music())

    new_count = 0
    with db_cursor() as cur:
        for item in all_content:
            try:
                cur.execute("""
                    INSERT OR IGNORE INTO content_queue
                    (source, source_id, file_path, content_type, title, description, brand, metadata)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    item["source"],
                    item.get("source_id", ""),
                    item["file_path"],
                    item["content_type"],
                    item.get("title", ""),
                    item.get("description", ""),
                    item.get("brand", "ghost"),
                    json.dumps(item.get("metadata", {})),
                ))
                if cur.rowcount > 0:
                    new_count += 1
            except sqlite3.IntegrityError:
                pass

    return new_count


# ==========================================================================
# SCHEDULING ENGINE
# ==========================================================================
def get_next_best_slot(platform: str, brand: str) -> str:
    """Find the next optimal posting time for a platform."""
    plat_info = PLATFORMS.get(platform, {})
    best_hours = plat_info.get("best_posting_hours", [12])

    now = datetime.now(timezone.utc)

    # Check already-scheduled posts for today to avoid conflicts
    with db_cursor() as cur:
        cur.execute("""
            SELECT scheduled_time FROM scheduled_posts
            WHERE platform = ? AND brand = ? AND status = 'scheduled'
            AND date(scheduled_time) >= date('now')
            ORDER BY scheduled_time
        """, (platform, brand))
        taken_times = [row["scheduled_time"] for row in cur.fetchall()]

    # Find next available slot in best hours
    for day_offset in range(0, 7):
        check_date = now + timedelta(days=day_offset)
        for hour in sorted(best_hours):
            candidate = check_date.replace(hour=hour, minute=0, second=0, microsecond=0)
            if candidate <= now:
                continue
            candidate_str = candidate.strftime("%Y-%m-%d %H:%M:%S")
            # Check if slot is taken (within 1 hour)
            slot_taken = False
            for taken in taken_times:
                try:
                    taken_dt = datetime.fromisoformat(taken.replace("Z", "+00:00"))
                    if abs((candidate - taken_dt).total_seconds()) < 3600:
                        slot_taken = True
                        break
                except Exception:
                    pass
            if not slot_taken:
                return candidate_str

    # Fallback: tomorrow at noon
    tomorrow_noon = (now + timedelta(days=1)).replace(hour=12, minute=0, second=0, microsecond=0)
    return tomorrow_noon.strftime("%Y-%m-%d %H:%M:%S")


def auto_schedule_from_queue():
    """Take unscheduled content from queue and create scheduled posts across platforms."""
    with db_cursor() as cur:
        # Get queued content not yet processed
        cur.execute("""
            SELECT * FROM content_queue
            WHERE status = 'queued'
            ORDER BY discovered_at
            LIMIT 20
        """)
        queued = [dict(row) for row in cur.fetchall()]

    scheduled_count = 0
    for item in queued:
        brand = item["brand"]
        brand_info = BRANDS.get(brand, BRANDS["ghost"])
        content_type = item["content_type"]

        # Decide which platforms to schedule on
        target_platforms = []
        if content_type in ("short", "reel"):
            # Shorts go everywhere that supports video/short/reel
            if "youtube" in brand_info["platforms"]:
                target_platforms.append(("youtube", "short"))
            if "tiktok" in brand_info["platforms"]:
                target_platforms.append(("tiktok", "video"))
            if "instagram" in brand_info["platforms"]:
                target_platforms.append(("instagram", "reel"))
            if "facebook" in brand_info["platforms"]:
                target_platforms.append(("facebook", "reel"))
        elif content_type == "video":
            if "youtube" in brand_info["platforms"]:
                target_platforms.append(("youtube", "video"))
            if "facebook" in brand_info["platforms"]:
                target_platforms.append(("facebook", "video"))
        elif content_type == "track":
            if "soundcloud" in brand_info["platforms"]:
                target_platforms.append(("soundcloud", "track"))
        elif content_type == "post":
            if "x_twitter" in brand_info["platforms"]:
                target_platforms.append(("x_twitter", "tweet"))
            if "reddit" in brand_info["platforms"]:
                target_platforms.append(("reddit", "post"))

        with db_cursor() as cur:
            for platform, plat_content_type in target_platforms:
                # Build hashtags
                brand_hashtags = brand_info.get("hashtags", {})
                tags = list(brand_hashtags.get("core", []))
                tags.extend(brand_hashtags.get(platform, [])[:5])

                scheduled_time = get_next_best_slot(platform, brand)

                try:
                    cur.execute("""
                        INSERT INTO scheduled_posts
                        (platform, brand, content_type, title, description, hashtags,
                         file_path, thumbnail_path, scheduled_time, metadata)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        platform,
                        brand,
                        plat_content_type,
                        item["title"],
                        item.get("description", ""),
                        json.dumps(tags),
                        item.get("file_path", ""),
                        "",
                        scheduled_time,
                        item.get("metadata", "{}"),
                    ))
                    scheduled_count += 1
                except sqlite3.IntegrityError:
                    pass

            # Mark content as processed
            cur.execute("""
                UPDATE content_queue SET status = 'scheduled', processed_at = datetime('now')
                WHERE id = ?
            """, (item["id"],))

    return scheduled_count


# ==========================================================================
# CALENDAR HELPERS
# ==========================================================================
def get_calendar_status() -> Dict:
    """Get today's posting progress vs targets."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    status = {"date": today, "daily": {}, "weekly": {}, "monthly": {}}

    with db_cursor() as cur:
        # Daily progress
        cur.execute("""
            SELECT platform, content_type, COUNT(*) as cnt
            FROM scheduled_posts
            WHERE date(scheduled_time) = ? AND status IN ('scheduled', 'posted')
            GROUP BY platform, content_type
        """, (today,))
        posted_today = {}
        for row in cur.fetchall():
            key = f"{row['platform']}_{row['content_type']}"
            posted_today[key] = row["cnt"]

        for target_key, target_count in DAILY_TARGETS.items():
            actual = posted_today.get(target_key, 0)
            status["daily"][target_key] = {
                "target": target_count,
                "actual": actual,
                "met": actual >= target_count,
            }

        # Weekly progress (this week)
        week_start = (datetime.now(timezone.utc) - timedelta(
            days=datetime.now(timezone.utc).weekday()
        )).strftime("%Y-%m-%d")
        cur.execute("""
            SELECT platform, content_type, COUNT(*) as cnt
            FROM scheduled_posts
            WHERE date(scheduled_time) >= ? AND status IN ('scheduled', 'posted')
            GROUP BY platform, content_type
        """, (week_start,))
        posted_week = {}
        for row in cur.fetchall():
            key = f"{row['platform']}_{row['content_type']}"
            posted_week[key] = row["cnt"]

        for target_key, target_count in WEEKLY_TARGETS.items():
            actual = posted_week.get(target_key, 0)
            status["weekly"][target_key] = {
                "target": target_count,
                "actual": actual,
                "met": actual >= target_count,
            }

    return status


# ==========================================================================
# ANALYTICS HELPERS
# ==========================================================================
def compute_analytics_summary() -> Dict:
    """Compute cross-platform analytics summary."""
    summary = {
        "platforms": {},
        "totals": {
            "views": 0, "likes": 0, "comments": 0, "shares": 0,
            "subscribers_gained": 0, "watch_time_hours": 0.0,
        },
        "best_content": [],
        "growth_rate": {},
        "best_posting_times": {},
    }

    with db_cursor() as cur:
        # Per-platform totals
        for plat_key in PLATFORMS:
            cur.execute("""
                SELECT
                    COALESCE(SUM(views), 0) as total_views,
                    COALESCE(SUM(likes), 0) as total_likes,
                    COALESCE(SUM(comments), 0) as total_comments,
                    COALESCE(SUM(shares), 0) as total_shares,
                    COALESCE(SUM(subscribers_gained), 0) as total_subs,
                    COALESCE(SUM(watch_time_hours), 0) as total_watch_time,
                    COALESCE(AVG(engagement_rate), 0) as avg_engagement
                FROM analytics WHERE platform = ?
            """, (plat_key,))
            row = cur.fetchone()
            if row:
                plat_data = {
                    "views": row["total_views"],
                    "likes": row["total_likes"],
                    "comments": row["total_comments"],
                    "shares": row["total_shares"],
                    "subscribers_gained": row["total_subs"],
                    "watch_time_hours": round(row["total_watch_time"], 1),
                    "avg_engagement_rate": round(row["avg_engagement"] * 100, 2),
                }
                summary["platforms"][plat_key] = plat_data
                summary["totals"]["views"] += plat_data["views"]
                summary["totals"]["likes"] += plat_data["likes"]
                summary["totals"]["comments"] += plat_data["comments"]
                summary["totals"]["shares"] += plat_data["shares"]
                summary["totals"]["subscribers_gained"] += plat_data["subscribers_gained"]
                summary["totals"]["watch_time_hours"] += plat_data["watch_time_hours"]

        # Best content (top 10 by views)
        cur.execute("""
            SELECT a.*, sp.title FROM analytics a
            LEFT JOIN scheduled_posts sp ON a.post_id = sp.id
            ORDER BY a.views DESC LIMIT 10
        """)
        for row in cur.fetchall():
            summary["best_content"].append({
                "platform": row["platform"],
                "brand": row["brand"],
                "title": row["title"] or f"Content {row['content_id']}",
                "views": row["views"],
                "likes": row["likes"],
                "engagement_rate": round(row["engagement_rate"] * 100, 2),
            })

        # 7-day vs prior 7-day growth
        for plat_key in PLATFORMS:
            cur.execute("""
                SELECT COALESCE(SUM(views), 0) as v FROM analytics
                WHERE platform = ? AND date >= date('now', '-7 days')
            """, (plat_key,))
            recent = cur.fetchone()["v"]
            cur.execute("""
                SELECT COALESCE(SUM(views), 0) as v FROM analytics
                WHERE platform = ? AND date >= date('now', '-14 days')
                AND date < date('now', '-7 days')
            """, (plat_key,))
            prior = cur.fetchone()["v"]
            if prior > 0:
                growth = round(((recent - prior) / prior) * 100, 1)
            elif recent > 0:
                growth = 100.0
            else:
                growth = 0.0
            summary["growth_rate"][plat_key] = {
                "last_7d_views": recent,
                "prior_7d_views": prior,
                "growth_pct": growth,
            }

    summary["totals"]["watch_time_hours"] = round(summary["totals"]["watch_time_hours"], 1)
    return summary


# ==========================================================================
# BACKGROUND TASKS
# ==========================================================================
def background_content_scanner():
    """Periodic background task: scan sources, queue content, auto-schedule."""
    while True:
        try:
            new = discover_and_queue_content()
            if new > 0:
                scheduled = auto_schedule_from_queue()
                print(f"[Scanner] Discovered {new} new content items, scheduled {scheduled} posts")
        except Exception as e:
            print(f"[Scanner] Error: {e}")
        time.sleep(SCAN_INTERVAL_MINUTES * 60)


def background_posting_check():
    """Check for posts that are due and mark them (actual posting is manual or via API hooks)."""
    while True:
        try:
            now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            with db_cursor() as cur:
                cur.execute("""
                    SELECT id, platform, brand, title, content_type FROM scheduled_posts
                    WHERE status = 'scheduled' AND scheduled_time <= ?
                """, (now_str,))
                due = [dict(row) for row in cur.fetchall()]

                for post in due:
                    # Mark as ready_to_post (actual posting requires platform API integration)
                    cur.execute("""
                        UPDATE scheduled_posts SET status = 'ready_to_post'
                        WHERE id = ?
                    """, (post["id"],))
                    print(f"[PostCheck] Ready to post: [{post['platform']}] {post['title']}")

        except Exception as e:
            print(f"[PostCheck] Error: {e}")
        time.sleep(300)  # check every 5 min


# ==========================================================================
# FASTAPI APP
# ==========================================================================
app = FastAPI(title="Hive Social Media Manager", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

START_TIME = time.time()


# --------------------------------------------------------------------------
# HEALTH
# --------------------------------------------------------------------------
@app.get("/health")
def health():
    uptime = int(time.time() - START_TIME)
    with db_cursor() as cur:
        cur.execute("SELECT COUNT(*) as c FROM accounts")
        accounts = cur.fetchone()["c"]
        cur.execute("SELECT COUNT(*) as c FROM scheduled_posts WHERE status = 'scheduled'")
        scheduled = cur.fetchone()["c"]
        cur.execute("SELECT COUNT(*) as c FROM content_queue WHERE status = 'queued'")
        queued = cur.fetchone()["c"]
    return {
        "status": "healthy",
        "service": "hive-social-media-manager",
        "port": PORT,
        "uptime_seconds": uptime,
        "accounts": accounts,
        "scheduled_posts": scheduled,
        "queued_content": queued,
    }


# --------------------------------------------------------------------------
# ACCOUNTS
# --------------------------------------------------------------------------
@app.get("/api/accounts")
def get_accounts(platform: Optional[str] = None, brand: Optional[str] = None):
    with db_cursor() as cur:
        query = "SELECT * FROM accounts WHERE 1=1"
        params = []
        if platform:
            query += " AND platform = ?"
            params.append(platform)
        if brand:
            query += " AND brand = ?"
            params.append(brand)
        query += " ORDER BY brand, platform"
        cur.execute(query, params)
        accounts = [dict(row) for row in cur.fetchall()]

    # Enrich with platform info
    for acc in accounts:
        plat = PLATFORMS.get(acc["platform"], {})
        acc["platform_name"] = plat.get("name", acc["platform"])
        acc["platform_color"] = plat.get("color", "#666")
        acc["brand_name"] = BRANDS.get(acc["brand"], {}).get("name", acc["brand"])

    return {"accounts": accounts, "count": len(accounts)}


@app.post("/api/accounts")
def create_or_update_account(account: AccountCreate):
    if account.platform not in PLATFORMS:
        raise HTTPException(400, f"Unknown platform: {account.platform}")
    if account.brand not in BRANDS:
        raise HTTPException(400, f"Unknown brand: {account.brand}")

    with db_cursor() as cur:
        cur.execute("""
            INSERT INTO accounts (platform, brand, username, url, api_key, api_secret,
                                  access_token, status, notes, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'active', ?, datetime('now'))
            ON CONFLICT(platform, brand) DO UPDATE SET
                username = excluded.username,
                url = COALESCE(NULLIF(excluded.url, ''), url),
                api_key = COALESCE(NULLIF(excluded.api_key, ''), api_key),
                api_secret = COALESCE(NULLIF(excluded.api_secret, ''), api_secret),
                access_token = COALESCE(NULLIF(excluded.access_token, ''), access_token),
                status = 'active',
                notes = COALESCE(NULLIF(excluded.notes, ''), notes),
                updated_at = datetime('now')
        """, (
            account.platform, account.brand, account.username,
            account.url or "", account.api_key or "", account.api_secret or "",
            account.access_token or "", account.notes or "",
        ))
    return {"status": "ok", "message": f"Account {account.username} on {account.platform} saved"}


# --------------------------------------------------------------------------
# CALENDAR
# --------------------------------------------------------------------------
@app.get("/api/calendar")
def get_calendar(days: int = Query(default=7, ge=1, le=30)):
    with db_cursor() as cur:
        end_date = (datetime.now(timezone.utc) + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        cur.execute("""
            SELECT * FROM scheduled_posts
            WHERE scheduled_time <= ? AND status IN ('scheduled', 'ready_to_post')
            ORDER BY scheduled_time
        """, (end_date,))
        posts = []
        for row in cur.fetchall():
            post = dict(row)
            post["hashtags"] = json.loads(post.get("hashtags", "[]"))
            post["metadata"] = json.loads(post.get("metadata", "{}"))
            post["platform_name"] = PLATFORMS.get(post["platform"], {}).get("name", post["platform"])
            post["brand_name"] = BRANDS.get(post["brand"], {}).get("name", post["brand"])
            posts.append(post)

    calendar_status = get_calendar_status()
    return {
        "upcoming_posts": posts,
        "count": len(posts),
        "days": days,
        "calendar_status": calendar_status,
    }


# --------------------------------------------------------------------------
# SCHEDULE
# --------------------------------------------------------------------------
@app.post("/api/schedule")
def schedule_post(post: SchedulePost):
    if post.platform not in PLATFORMS:
        raise HTTPException(400, f"Unknown platform: {post.platform}")
    if post.brand not in BRANDS:
        raise HTTPException(400, f"Unknown brand: {post.brand}")

    plat_info = PLATFORMS[post.platform]
    if post.content_type not in plat_info["content_types"]:
        raise HTTPException(400,
            f"Content type '{post.content_type}' not supported on {plat_info['name']}. "
            f"Supported: {plat_info['content_types']}")

    # Auto-pick time if not specified
    scheduled_time = post.scheduled_time
    if not scheduled_time:
        scheduled_time = get_next_best_slot(post.platform, post.brand)

    # Auto-add brand hashtags if none provided
    hashtags = post.hashtags or []
    if not hashtags:
        brand_info = BRANDS.get(post.brand, {})
        brand_hashtags = brand_info.get("hashtags", {})
        hashtags = list(brand_hashtags.get("core", []))
        hashtags.extend(brand_hashtags.get(post.platform, [])[:5])

    # Enforce platform hashtag limits
    max_tags = plat_info.get("max_hashtags", 30)
    hashtags = hashtags[:max_tags]

    with db_cursor() as cur:
        cur.execute("""
            INSERT INTO scheduled_posts
            (platform, brand, content_type, title, description, hashtags,
             file_path, thumbnail_path, scheduled_time, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            post.platform,
            post.brand,
            post.content_type,
            post.title,
            post.description or "",
            json.dumps(hashtags),
            post.file_path or "",
            post.thumbnail_path or "",
            scheduled_time,
            json.dumps(post.metadata or {}),
        ))
        post_id = cur.lastrowid

    return {
        "status": "scheduled",
        "post_id": post_id,
        "platform": post.platform,
        "brand": post.brand,
        "scheduled_time": scheduled_time,
        "hashtags": hashtags,
    }


@app.post("/api/schedule/bulk")
def bulk_schedule_from_queue():
    """Auto-schedule all queued content across appropriate platforms."""
    new_discovered = discover_and_queue_content()
    new_scheduled = auto_schedule_from_queue()
    return {
        "status": "ok",
        "new_content_discovered": new_discovered,
        "new_posts_scheduled": new_scheduled,
    }


@app.post("/api/posts/{post_id}/mark-posted")
def mark_as_posted(post_id: int, posted_url: Optional[str] = None, content_id: Optional[str] = None):
    with db_cursor() as cur:
        cur.execute("""
            UPDATE scheduled_posts
            SET status = 'posted', posted_at = datetime('now'),
                posted_url = ?, content_id = ?
            WHERE id = ?
        """, (posted_url or "", content_id or "", post_id))
        if cur.rowcount == 0:
            raise HTTPException(404, "Post not found")
    return {"status": "ok", "post_id": post_id, "marked": "posted"}


@app.post("/api/posts/{post_id}/cancel")
def cancel_post(post_id: int):
    with db_cursor() as cur:
        cur.execute("""
            UPDATE scheduled_posts SET status = 'cancelled' WHERE id = ? AND status = 'scheduled'
        """, (post_id,))
        if cur.rowcount == 0:
            raise HTTPException(404, "Post not found or already processed")
    return {"status": "ok", "post_id": post_id, "marked": "cancelled"}


# --------------------------------------------------------------------------
# CONTENT QUEUE
# --------------------------------------------------------------------------
@app.get("/api/content-queue")
def get_content_queue(status: Optional[str] = None, brand: Optional[str] = None):
    with db_cursor() as cur:
        query = "SELECT * FROM content_queue WHERE 1=1"
        params = []
        if status:
            query += " AND status = ?"
            params.append(status)
        if brand:
            query += " AND brand = ?"
            params.append(brand)
        query += " ORDER BY discovered_at DESC LIMIT 100"
        cur.execute(query, params)
        items = [dict(row) for row in cur.fetchall()]
    return {"content_queue": items, "count": len(items)}


@app.post("/api/content-queue/scan")
def trigger_content_scan():
    new = discover_and_queue_content()
    return {"status": "ok", "new_items_discovered": new}


# --------------------------------------------------------------------------
# ANALYTICS
# --------------------------------------------------------------------------
@app.get("/api/analytics")
def get_analytics(platform: Optional[str] = None, days: int = Query(default=30, ge=1, le=365)):
    summary = compute_analytics_summary()

    with db_cursor() as cur:
        # Daily breakdown for the requested period
        query = """
            SELECT date, platform, brand,
                   SUM(views) as views, SUM(likes) as likes,
                   SUM(comments) as comments, SUM(shares) as shares,
                   SUM(subscribers_gained) as subs_gained,
                   SUM(watch_time_hours) as watch_hours
            FROM analytics
            WHERE date >= date('now', ?)
        """
        params = [f"-{days} days"]
        if platform:
            query += " AND platform = ?"
            params.append(platform)
        query += " GROUP BY date, platform ORDER BY date DESC"
        cur.execute(query, params)
        daily = [dict(row) for row in cur.fetchall()]

        # Posting activity
        cur.execute("""
            SELECT platform, brand, content_type, COUNT(*) as cnt, status
            FROM scheduled_posts
            WHERE created_at >= datetime('now', ?)
            GROUP BY platform, brand, content_type, status
            ORDER BY cnt DESC
        """, (f"-{days} days",))
        posting_activity = [dict(row) for row in cur.fetchall()]

    summary["daily_breakdown"] = daily
    summary["posting_activity"] = posting_activity
    summary["period_days"] = days
    return summary


@app.post("/api/analytics")
def record_analytics(data: AnalyticsUpdate):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Compute engagement rate
    engagement_rate = 0.0
    if data.views and data.views > 0:
        engagement_rate = (data.likes + data.comments + data.shares) / data.views

    with db_cursor() as cur:
        cur.execute("""
            INSERT INTO analytics
            (platform, brand, content_id, date, views, likes, comments, shares,
             subscribers_gained, watch_time_hours, impressions, click_through_rate,
             engagement_rate)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(platform, brand, content_id, date) DO UPDATE SET
                views = excluded.views,
                likes = excluded.likes,
                comments = excluded.comments,
                shares = excluded.shares,
                subscribers_gained = excluded.subscribers_gained,
                watch_time_hours = excluded.watch_time_hours,
                impressions = excluded.impressions,
                click_through_rate = excluded.click_through_rate,
                engagement_rate = excluded.engagement_rate
        """, (
            data.platform, data.brand, data.content_id or "", today,
            data.views, data.likes, data.comments, data.shares,
            data.subscribers_gained, data.watch_time_hours,
            data.impressions, data.click_through_rate, engagement_rate,
        ))

    return {"status": "ok", "date": today, "engagement_rate": round(engagement_rate * 100, 2)}


# --------------------------------------------------------------------------
# STRATEGY
# --------------------------------------------------------------------------
@app.get("/api/strategy")
def get_strategy():
    calendar_status = get_calendar_status()
    summary = compute_analytics_summary()

    # Determine current phase based on follower counts
    with db_cursor() as cur:
        cur.execute("SELECT platform, SUM(followers) as total FROM accounts GROUP BY platform")
        follower_counts = {row["platform"]: row["total"] for row in cur.fetchall()}

    total_followers = sum(follower_counts.values())
    if total_followers < 500:
        current_phase = "phase_1_launch"
    elif total_followers < 5000:
        current_phase = "phase_2_growth"
    else:
        current_phase = "phase_3_monetize"

    # Build actionable recommendations
    recommendations = []

    # Check daily targets
    for target_key, target_info in calendar_status.get("daily", {}).items():
        if not target_info.get("met"):
            plat, ctype = target_key.rsplit("_", 1)
            plat_name = PLATFORMS.get(plat, {}).get("name", plat)
            recommendations.append({
                "priority": "high",
                "action": f"Post {target_info['target'] - target_info['actual']} more "
                          f"{ctype}(s) on {plat_name} today",
                "reason": f"Daily target: {target_info['target']}, current: {target_info['actual']}",
            })

    # Platform-specific recs
    for plat_key, plat_info in PLATFORMS.items():
        growth = summary.get("growth_rate", {}).get(plat_key, {})
        if growth.get("growth_pct", 0) < 0:
            recommendations.append({
                "priority": "medium",
                "action": f"Increase posting frequency on {plat_info['name']}",
                "reason": f"Views declined {growth['growth_pct']}% week-over-week",
            })

    # Account setup recs
    with db_cursor() as cur:
        cur.execute("SELECT platform, brand FROM accounts WHERE status = 'needs_setup'")
        for row in cur.fetchall():
            plat_name = PLATFORMS.get(row["platform"], {}).get("name", row["platform"])
            brand_name = BRANDS.get(row["brand"], {}).get("name", row["brand"])
            recommendations.append({
                "priority": "medium",
                "action": f"Set up {brand_name} account on {plat_name}",
                "reason": "Account not yet configured",
            })

    # Content pipeline recs
    with db_cursor() as cur:
        cur.execute("SELECT COUNT(*) as c FROM content_queue WHERE status = 'queued'")
        queued = cur.fetchone()["c"]
    if queued > 10:
        recommendations.append({
            "priority": "high",
            "action": f"Schedule {queued} queued content items",
            "reason": "Content is piling up in queue without being scheduled",
        })

    return {
        "current_phase": GROWTH_STRATEGY.get(current_phase, {}),
        "content_repurposing": GROWTH_STRATEGY["content_repurposing"],
        "hashtag_strategy": GROWTH_STRATEGY["hashtag_strategy"],
        "engagement_tactics": GROWTH_STRATEGY["engagement_tactics"],
        "calendar_status": calendar_status,
        "recommendations": recommendations,
        "follower_summary": follower_counts,
        "total_followers": total_followers,
        "all_phases": {
            k: v for k, v in GROWTH_STRATEGY.items()
            if k.startswith("phase_")
        },
    }


# --------------------------------------------------------------------------
# REPURPOSE ENDPOINT
# --------------------------------------------------------------------------
@app.post("/api/repurpose")
def repurpose_content(
    source_post_id: int,
    target_platforms: Optional[List[str]] = None,
):
    """Take a posted piece of content and create scheduled posts on other platforms."""
    with db_cursor() as cur:
        cur.execute("SELECT * FROM scheduled_posts WHERE id = ?", (source_post_id,))
        source = cur.fetchone()
        if not source:
            raise HTTPException(404, "Source post not found")
        source = dict(source)

    brand = source["brand"]
    brand_info = BRANDS.get(brand, BRANDS["ghost"])
    all_plats = target_platforms or brand_info["platforms"]

    created = []
    for plat in all_plats:
        if plat == source["platform"]:
            continue  # skip the source platform
        if plat not in PLATFORMS:
            continue

        plat_info = PLATFORMS[plat]
        # Map content type to platform equivalent
        type_mapping = {
            "short": {"tiktok": "video", "instagram": "reel", "facebook": "reel", "youtube": "short"},
            "video": {"tiktok": "video", "instagram": "reel", "facebook": "video", "youtube": "video"},
            "reel": {"tiktok": "video", "youtube": "short", "facebook": "reel", "instagram": "reel"},
        }
        mapped_type = type_mapping.get(source["content_type"], {}).get(plat, "video")
        if mapped_type not in plat_info["content_types"]:
            mapped_type = plat_info["content_types"][0]

        # Build platform-specific hashtags
        brand_hashtags = brand_info.get("hashtags", {})
        tags = list(brand_hashtags.get("core", []))
        tags.extend(brand_hashtags.get(plat, [])[:5])
        tags = tags[:plat_info.get("max_hashtags", 30)]

        # Adapt description length
        desc = source.get("description", "")
        char_limit = plat_info.get("char_limit_description", 5000)
        if len(desc) > char_limit:
            desc = desc[:char_limit - 3] + "..."

        scheduled_time = get_next_best_slot(plat, brand)

        with db_cursor() as cur:
            cur.execute("""
                INSERT INTO scheduled_posts
                (platform, brand, content_type, title, description, hashtags,
                 file_path, thumbnail_path, scheduled_time, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                plat, brand, mapped_type, source["title"], desc,
                json.dumps(tags), source.get("file_path", ""),
                source.get("thumbnail_path", ""), scheduled_time,
                json.dumps({"repurposed_from": source_post_id}),
            ))
            created.append({
                "platform": plat,
                "content_type": mapped_type,
                "scheduled_time": scheduled_time,
                "post_id": cur.lastrowid,
            })

    return {
        "status": "ok",
        "source_post_id": source_post_id,
        "repurposed_to": created,
        "count": len(created),
    }


# --------------------------------------------------------------------------
# DASHBOARD HTML
# --------------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
def dashboard():
    # Gather data
    with db_cursor() as cur:
        cur.execute("SELECT * FROM accounts ORDER BY brand, platform")
        accounts = [dict(row) for row in cur.fetchall()]

        cur.execute("""
            SELECT * FROM scheduled_posts
            WHERE status IN ('scheduled', 'ready_to_post')
            ORDER BY scheduled_time LIMIT 20
        """)
        upcoming = [dict(row) for row in cur.fetchall()]

        cur.execute("""
            SELECT * FROM scheduled_posts WHERE status = 'posted'
            ORDER BY posted_at DESC LIMIT 10
        """)
        recent_posts = [dict(row) for row in cur.fetchall()]

        cur.execute("SELECT COUNT(*) as c FROM content_queue WHERE status = 'queued'")
        queued_count = cur.fetchone()["c"]

        cur.execute("SELECT COUNT(*) as c FROM scheduled_posts WHERE status = 'scheduled'")
        scheduled_count = cur.fetchone()["c"]

        cur.execute("SELECT COUNT(*) as c FROM scheduled_posts WHERE status = 'posted'")
        posted_count = cur.fetchone()["c"]

    calendar_status = get_calendar_status()
    analytics_summary = compute_analytics_summary()

    # Build account rows
    account_rows = ""
    for acc in accounts:
        plat = PLATFORMS.get(acc["platform"], {})
        color = plat.get("color", "#666")
        icon = plat.get("icon", "?")
        brand_name = BRANDS.get(acc["brand"], {}).get("name", acc["brand"])
        status_badge = (
            f'<span class="badge active">Active</span>'
            if acc["status"] == "active"
            else f'<span class="badge setup">Needs Setup</span>'
        )
        account_rows += f"""
        <tr>
            <td><span class="plat-badge" style="background:{color}">{html_lib.escape(icon)}</span>
                {html_lib.escape(plat.get('name', acc['platform']))}</td>
            <td>{html_lib.escape(brand_name)}</td>
            <td>{html_lib.escape(acc['username'])}</td>
            <td>{status_badge}</td>
            <td>{acc['followers']}</td>
            <td>{acc['total_posts']}</td>
        </tr>"""

    # Build upcoming posts
    upcoming_rows = ""
    for post in upcoming:
        plat = PLATFORMS.get(post["platform"], {})
        color = plat.get("color", "#666")
        icon = plat.get("icon", "?")
        status_class = "ready" if post["status"] == "ready_to_post" else "scheduled"
        upcoming_rows += f"""
        <tr>
            <td><span class="plat-badge" style="background:{color}">{html_lib.escape(icon)}</span></td>
            <td>{html_lib.escape(post['title'][:50])}</td>
            <td>{html_lib.escape(post['content_type'])}</td>
            <td>{html_lib.escape(post['brand'])}</td>
            <td>{html_lib.escape(post['scheduled_time'][:16])}</td>
            <td><span class="badge {status_class}">{html_lib.escape(post['status'])}</span></td>
        </tr>"""

    # Daily targets
    daily_html = ""
    for key, info in calendar_status.get("daily", {}).items():
        met = "met" if info["met"] else "unmet"
        daily_html += f"""
        <div class="target-card {met}">
            <div class="target-name">{html_lib.escape(key.replace('_', ' ').title())}</div>
            <div class="target-progress">{info['actual']}/{info['target']}</div>
        </div>"""

    # Platform analytics
    plat_analytics_html = ""
    for plat_key, plat_data in analytics_summary.get("platforms", {}).items():
        plat = PLATFORMS.get(plat_key, {})
        color = plat.get("color", "#666")
        name = plat.get("name", plat_key)
        growth = analytics_summary.get("growth_rate", {}).get(plat_key, {})
        growth_pct = growth.get("growth_pct", 0)
        growth_class = "positive" if growth_pct >= 0 else "negative"
        plat_analytics_html += f"""
        <div class="analytics-card" style="border-left: 4px solid {color}">
            <h4>{html_lib.escape(name)}</h4>
            <div class="stat-row">
                <span>Views:</span><strong>{plat_data['views']:,}</strong>
            </div>
            <div class="stat-row">
                <span>Likes:</span><strong>{plat_data['likes']:,}</strong>
            </div>
            <div class="stat-row">
                <span>Comments:</span><strong>{plat_data['comments']:,}</strong>
            </div>
            <div class="stat-row">
                <span>Engagement:</span><strong>{plat_data['avg_engagement_rate']}%</strong>
            </div>
            <div class="stat-row growth {growth_class}">
                <span>7d Growth:</span><strong>{growth_pct:+.1f}%</strong>
            </div>
        </div>"""

    if not plat_analytics_html:
        plat_analytics_html = '<p class="muted">No analytics data yet. Record analytics via POST /api/analytics.</p>'

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Hive Social Media Manager</title>
<style>
:root {{
    --bg: #0a0e17;
    --surface: #131a2b;
    --surface2: #1a2340;
    --accent: #00d4ff;
    --accent2: #7c3aed;
    --green: #10b981;
    --red: #ef4444;
    --yellow: #f59e0b;
    --text: #e0e7ff;
    --muted: #64748b;
    --border: #1e293b;
}}
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{
    font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
    background: var(--bg);
    color: var(--text);
    line-height: 1.6;
}}
.container {{ max-width: 1400px; margin: 0 auto; padding: 20px; }}
h1 {{
    font-size: 2rem;
    background: linear-gradient(135deg, var(--accent), var(--accent2));
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    margin-bottom: 5px;
}}
.subtitle {{ color: var(--muted); margin-bottom: 30px; }}
.stats-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 15px;
    margin-bottom: 30px;
}}
.stat-box {{
    background: var(--surface);
    border-radius: 12px;
    padding: 20px;
    text-align: center;
    border: 1px solid var(--border);
}}
.stat-box .number {{
    font-size: 2rem;
    font-weight: 700;
    color: var(--accent);
}}
.stat-box .label {{ color: var(--muted); font-size: 0.85rem; text-transform: uppercase; }}
.section {{
    background: var(--surface);
    border-radius: 12px;
    padding: 25px;
    margin-bottom: 25px;
    border: 1px solid var(--border);
}}
.section h2 {{
    font-size: 1.3rem;
    margin-bottom: 15px;
    color: var(--accent);
}}
table {{ width: 100%; border-collapse: collapse; }}
th, td {{ padding: 10px 12px; text-align: left; border-bottom: 1px solid var(--border); }}
th {{ color: var(--muted); font-size: 0.85rem; text-transform: uppercase; font-weight: 600; }}
tr:hover {{ background: var(--surface2); }}
.plat-badge {{
    display: inline-block;
    padding: 2px 8px;
    border-radius: 4px;
    color: #fff;
    font-weight: 700;
    font-size: 0.75rem;
    margin-right: 6px;
}}
.badge {{
    display: inline-block;
    padding: 3px 10px;
    border-radius: 20px;
    font-size: 0.75rem;
    font-weight: 600;
}}
.badge.active {{ background: var(--green); color: #000; }}
.badge.setup {{ background: var(--yellow); color: #000; }}
.badge.scheduled {{ background: var(--accent); color: #000; }}
.badge.ready {{ background: var(--accent2); color: #fff; }}
.targets-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 12px;
}}
.target-card {{
    padding: 15px;
    border-radius: 10px;
    background: var(--surface2);
    border: 2px solid var(--border);
}}
.target-card.met {{ border-color: var(--green); }}
.target-card.unmet {{ border-color: var(--red); }}
.target-name {{ font-size: 0.85rem; color: var(--muted); }}
.target-progress {{ font-size: 1.5rem; font-weight: 700; }}
.target-card.met .target-progress {{ color: var(--green); }}
.target-card.unmet .target-progress {{ color: var(--red); }}
.analytics-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    gap: 15px;
}}
.analytics-card {{
    background: var(--surface2);
    border-radius: 10px;
    padding: 15px;
}}
.analytics-card h4 {{ margin-bottom: 10px; }}
.stat-row {{
    display: flex;
    justify-content: space-between;
    padding: 3px 0;
    font-size: 0.9rem;
}}
.stat-row span {{ color: var(--muted); }}
.growth.positive strong {{ color: var(--green); }}
.growth.negative strong {{ color: var(--red); }}
.muted {{ color: var(--muted); font-style: italic; }}
.two-col {{
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 25px;
}}
@media (max-width: 900px) {{
    .two-col {{ grid-template-columns: 1fr; }}
}}
.api-list {{ list-style: none; }}
.api-list li {{
    padding: 6px 0;
    border-bottom: 1px solid var(--border);
    font-family: monospace;
    font-size: 0.85rem;
}}
.api-list .method {{
    display: inline-block;
    min-width: 50px;
    font-weight: 700;
    margin-right: 8px;
}}
.api-list .get {{ color: var(--green); }}
.api-list .post {{ color: var(--yellow); }}
</style>
</head>
<body>
<div class="container">
    <h1>Hive Social Media Manager</h1>
    <p class="subtitle">Cross-platform content scheduling, analytics, and growth strategy</p>

    <div class="stats-grid">
        <div class="stat-box">
            <div class="number">{len(accounts)}</div>
            <div class="label">Accounts</div>
        </div>
        <div class="stat-box">
            <div class="number">{queued_count}</div>
            <div class="label">Queued Content</div>
        </div>
        <div class="stat-box">
            <div class="number">{scheduled_count}</div>
            <div class="label">Scheduled</div>
        </div>
        <div class="stat-box">
            <div class="number">{posted_count}</div>
            <div class="label">Posted</div>
        </div>
        <div class="stat-box">
            <div class="number">{analytics_summary['totals']['views']:,}</div>
            <div class="label">Total Views</div>
        </div>
        <div class="stat-box">
            <div class="number">{len(PLATFORMS)}</div>
            <div class="label">Platforms</div>
        </div>
    </div>

    <div class="section">
        <h2>Daily Targets</h2>
        <div class="targets-grid">{daily_html if daily_html else '<p class="muted">Calendar targets load once content is scheduled.</p>'}</div>
    </div>

    <div class="section">
        <h2>Accounts ({len(accounts)})</h2>
        <table>
            <thead><tr>
                <th>Platform</th><th>Brand</th><th>Username</th>
                <th>Status</th><th>Followers</th><th>Posts</th>
            </tr></thead>
            <tbody>{account_rows}</tbody>
        </table>
    </div>

    <div class="two-col">
        <div class="section">
            <h2>Upcoming Posts ({len(upcoming)})</h2>
            {f'''<table>
                <thead><tr><th></th><th>Title</th><th>Type</th><th>Brand</th><th>Time</th><th>Status</th></tr></thead>
                <tbody>{upcoming_rows}</tbody>
            </table>''' if upcoming_rows else '<p class="muted">No upcoming posts. Schedule content via POST /api/schedule or POST /api/schedule/bulk.</p>'}
        </div>

        <div class="section">
            <h2>Platform Analytics</h2>
            <div class="analytics-grid">{plat_analytics_html}</div>
        </div>
    </div>

    <div class="section">
        <h2>API Reference</h2>
        <ul class="api-list">
            <li><span class="method get">GET</span> /health -- Service health check</li>
            <li><span class="method get">GET</span> /api/accounts -- All accounts with status</li>
            <li><span class="method post">POST</span> /api/accounts -- Create/update account</li>
            <li><span class="method get">GET</span> /api/calendar?days=7 -- Upcoming scheduled posts</li>
            <li><span class="method get">GET</span> /api/analytics?days=30 -- Cross-platform analytics</li>
            <li><span class="method post">POST</span> /api/analytics -- Record analytics data</li>
            <li><span class="method get">GET</span> /api/content-queue -- Content ready to schedule</li>
            <li><span class="method post">POST</span> /api/content-queue/scan -- Trigger content discovery scan</li>
            <li><span class="method post">POST</span> /api/schedule -- Schedule a post (auto-picks best time)</li>
            <li><span class="method post">POST</span> /api/schedule/bulk -- Auto-schedule all queued content</li>
            <li><span class="method post">POST</span> /api/repurpose -- Repurpose 1 post to N platforms</li>
            <li><span class="method post">POST</span> /api/posts/ID/mark-posted -- Mark post as published</li>
            <li><span class="method post">POST</span> /api/posts/ID/cancel -- Cancel scheduled post</li>
            <li><span class="method get">GET</span> /api/strategy -- Growth strategy + recommendations</li>
        </ul>
    </div>

    <div class="section">
        <h2>Growth Strategy</h2>
        <p style="margin-bottom: 10px;">
            <strong>Current Phase:</strong> Phase 1 -- Foundation (establish consistent posting rhythm)
        </p>
        <p style="margin-bottom: 10px;">
            <strong>Priority Platforms:</strong> YouTube Shorts (algorithm boost) &gt; TikTok (organic discovery) &gt; Instagram Reels (visual brand)
        </p>
        <p style="margin-bottom: 10px;">
            <strong>Content Pipeline:</strong> 1 episode = 5 shorts + 5 TikToks + 5 reels + 1 track + 3 tweets + 1 Reddit post = 20+ pieces
        </p>
        <p>
            <strong>Key Metric:</strong> Post daily. Consistency beats perfection. Reply to every comment within 1 hour.
        </p>
    </div>
</div>
</body>
</html>"""
    return HTMLResponse(html)


# ==========================================================================
# STARTUP
# ==========================================================================
@app.on_event("startup")
def on_startup():
    init_db()
    # Initial content scan
    try:
        new = discover_and_queue_content()
        if new > 0:
            print(f"[Startup] Discovered {new} content items from sources")
    except Exception as e:
        print(f"[Startup] Content scan error: {e}")

    # Background threads
    scanner_thread = threading.Thread(target=background_content_scanner, daemon=True)
    scanner_thread.start()

    post_check_thread = threading.Thread(target=background_posting_check, daemon=True)
    post_check_thread.start()

    print(f"[SocialMediaManager] Running on port {PORT}")
    print(f"[SocialMediaManager] Tracking {len(PLATFORMS)} platforms, {len(BRANDS)} brands")
    print(f"[SocialMediaManager] Dashboard: http://localhost:{PORT}")


# ==========================================================================
# MAIN
# ==========================================================================
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
