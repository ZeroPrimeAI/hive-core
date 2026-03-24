#!/usr/bin/env python3
"""
Product Factory — The Hive's Software Product Builder & Launcher
Port: 8916
DB: /home/zero/hivecode_sandbox/products.db

Manages the full lifecycle of software products:
  Idea -> Research -> Build -> Test -> Launch -> Grow

Every internal Hive service (phone, content, inference, TTS, dispatch)
can be wrapped, packaged, and sold. This service tracks that pipeline.
"""

import sqlite3
import json
import os
import time
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from contextlib import contextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field
from typing import Optional, List
import uvicorn

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DB_PATH = "/home/zero/hivecode_sandbox/products.db"
PORT = 8916
STAGES = ["idea", "research", "build", "test", "launch", "grow"]
REVENUE_MODELS = ["one-time", "subscription", "usage-based", "freemium", "ad-supported"]
CHANNELS = [
    "Chrome Web Store", "npm", "PyPI", "GitHub", "Direct / Website",
    "Discord Marketplace", "Slack App Directory", "Telegram BotFather",
    "Apple App Store", "Google Play", "API Gateway",
]

# ---------------------------------------------------------------------------
# Product Templates — the catalogue of things we CAN build right now
# ---------------------------------------------------------------------------
PRODUCT_TEMPLATES = {
    # ---- Chrome Extensions ----
    "chrome_ai_writer": {
        "name": "AI Writing Assistant (Chrome)",
        "category": "chrome_extension",
        "description": "Browser extension that rewrites, summarizes, and generates text on any webpage. Powered by our gemma2 inference.",
        "estimated_hours": 16,
        "revenue_model": "freemium",
        "price_suggestion": "$4.99/mo or $39.99/yr",
        "distribution": "Chrome Web Store",
        "tech_stack": ["manifest_v3", "javascript", "gemma2_api"],
        "marketing_plan": "Product Hunt launch, Reddit r/ChatGPT and r/chrome_extensions, SEO landing page, 5 YouTube shorts demoing features.",
        "skeleton": {
            "manifest.json": '{"manifest_version":3,"name":"Hive AI Writer","version":"1.0.0","description":"AI writing assistant powered by Hive","permissions":["activeTab","storage"],"action":{"default_popup":"popup.html"},"content_scripts":[{"matches":["<all_urls>"],"js":["content.js"]}]}',
            "popup.html": "<!-- Popup UI: rewrite / summarize / expand buttons -->",
            "popup.js": "// Connect to Hive inference API, handle user actions",
            "content.js": "// Inject floating toolbar on text selection",
            "background.js": "// Service worker: manage auth tokens, API calls",
        },
    },
    "chrome_security_checker": {
        "name": "Website Security Checker (Chrome)",
        "category": "chrome_extension",
        "description": "Instant security score for any website — SSL, headers, known vulnerabilities, phishing detection.",
        "estimated_hours": 12,
        "revenue_model": "freemium",
        "price_suggestion": "$2.99/mo pro tier",
        "distribution": "Chrome Web Store",
        "tech_stack": ["manifest_v3", "javascript", "security_apis"],
        "marketing_plan": "Security subreddits, Hacker News Show HN, InfoSec Twitter/X, SEO landing page.",
        "skeleton": {
            "manifest.json": '{"manifest_version":3,"name":"Hive Security Check","version":"1.0.0","description":"Instant website security scoring","permissions":["activeTab","webRequest"],"action":{"default_popup":"popup.html"}}',
            "popup.html": "<!-- Security score dashboard -->",
            "popup.js": "// Fetch headers, check SSL, score site",
            "background.js": "// webRequest listener for real-time analysis",
        },
    },
    "chrome_seo_analyzer": {
        "name": "SEO Analyzer (Chrome)",
        "category": "chrome_extension",
        "description": "One-click SEO audit for any page — meta tags, headings, links, performance, accessibility.",
        "estimated_hours": 14,
        "revenue_model": "freemium",
        "price_suggestion": "$5.99/mo pro with AI suggestions",
        "distribution": "Chrome Web Store",
        "tech_stack": ["manifest_v3", "javascript", "gemma2_api"],
        "marketing_plan": "SEO subreddits, IndieHackers, affiliate bloggers, YouTube SEO tutorials.",
        "skeleton": {
            "manifest.json": '{"manifest_version":3,"name":"Hive SEO Analyzer","version":"1.0.0","description":"AI-powered SEO audits","permissions":["activeTab"]}',
            "popup.html": "<!-- SEO score + recommendations -->",
            "content.js": "// Parse DOM for SEO signals",
        },
    },
    "chrome_price_tracker": {
        "name": "Price Tracker (Chrome)",
        "category": "chrome_extension",
        "description": "Track prices on Amazon, eBay, Walmart. Get alerts when prices drop. Historical charts.",
        "estimated_hours": 20,
        "revenue_model": "freemium",
        "price_suggestion": "$3.99/mo or affiliate commissions",
        "distribution": "Chrome Web Store",
        "tech_stack": ["manifest_v3", "javascript", "sqlite_local"],
        "marketing_plan": "r/frugal, r/deals, deal-hunting forums, comparison to Honey/Camelcamelcamel.",
        "skeleton": {
            "manifest.json": '{"manifest_version":3,"name":"Hive Price Tracker","version":"1.0.0","permissions":["activeTab","storage","alarms"]}',
            "popup.html": "<!-- Price history chart + alert settings -->",
            "content.js": "// Extract price from supported retailers",
            "background.js": "// Periodic price checks via alarms API",
        },
    },
    "chrome_ai_tabs": {
        "name": "AI Tab Manager (Chrome)",
        "category": "chrome_extension",
        "description": "AI-powered tab grouping, search, and memory. Never lose a tab again. Auto-categorizes by topic.",
        "estimated_hours": 18,
        "revenue_model": "freemium",
        "price_suggestion": "$2.99/mo",
        "distribution": "Chrome Web Store",
        "tech_stack": ["manifest_v3", "javascript", "gemma2_api"],
        "marketing_plan": "Productivity subreddits, Product Hunt, Chrome extension directories.",
        "skeleton": {
            "manifest.json": '{"manifest_version":3,"name":"Hive Tab Mind","version":"1.0.0","permissions":["tabs","tabGroups","storage"]}',
            "popup.html": "<!-- Tab overview with AI categories -->",
            "background.js": "// Auto-group tabs by AI classification",
        },
    },
    # ---- API Services ----
    "api_phone_answering": {
        "name": "AI Phone Answering API",
        "category": "api_service",
        "description": "REST API for AI-powered phone call handling. Wraps our Twilio + gemma2-phone pipeline. Businesses plug in, AI answers their phones.",
        "estimated_hours": 40,
        "revenue_model": "usage-based",
        "price_suggestion": "$0.10/min or $49/mo for 500 min",
        "distribution": "Direct / Website",
        "tech_stack": ["fastapi", "twilio", "gemma2-phone", "tts"],
        "marketing_plan": "Cold outreach to small businesses, locksmith/plumber verticals, SEO for 'AI phone answering service', demo video.",
        "skeleton": {
            "server.py": "# FastAPI wrapper around interactive_call.py\n# POST /v1/calls/answer — handle inbound\n# POST /v1/calls/outbound — make outbound\n# GET /v1/calls/{id}/transcript",
            "config.yaml": "# Customer configs: greeting, business info, routing rules",
            "models.py": "# Pydantic models for call data",
        },
    },
    "api_content_gen": {
        "name": "Content Generation API",
        "category": "api_service",
        "description": "API for generating blog posts, product descriptions, social media content. Multi-model routing for quality.",
        "estimated_hours": 24,
        "revenue_model": "usage-based",
        "price_suggestion": "$0.01/1K tokens or $29/mo for 1M tokens",
        "distribution": "Direct / Website",
        "tech_stack": ["fastapi", "gemma2_models", "model_router"],
        "marketing_plan": "Developer communities, API marketplaces (RapidAPI), content marketing blogs.",
        "skeleton": {
            "server.py": "# POST /v1/generate — text generation\n# POST /v1/rewrite — rewrite existing text\n# POST /v1/summarize — summarization",
            "router.py": "# Route to best model per task type",
        },
    },
    "api_image_gen": {
        "name": "Image Generation API",
        "category": "api_service",
        "description": "REST API proxy to our SDXL pipeline on RTX 3090. Anime art, product photos, marketing images.",
        "estimated_hours": 20,
        "revenue_model": "usage-based",
        "price_suggestion": "$0.03/image or $19/mo for 1K images",
        "distribution": "Direct / Website",
        "tech_stack": ["fastapi", "sdxl", "diffusers", "rtx3090"],
        "marketing_plan": "AI art communities, developer docs, API comparison sites.",
        "skeleton": {
            "server.py": "# POST /v1/images/generate — text-to-image\n# POST /v1/images/edit — image-to-image\n# GET /v1/images/{id} — retrieve result",
            "pipeline.py": "# SDXL pipeline wrapper with queue management",
        },
    },
    "api_tts": {
        "name": "Text-to-Speech API",
        "category": "api_service",
        "description": "Multi-engine TTS API — edge-tts (free), Kokoro (local), Chatterbox (voice cloning). 50+ voices.",
        "estimated_hours": 16,
        "revenue_model": "usage-based",
        "price_suggestion": "$0.005/1K chars or $14.99/mo for 500K chars",
        "distribution": "Direct / Website",
        "tech_stack": ["fastapi", "edge_tts", "kokoro", "chatterbox"],
        "marketing_plan": "Developer forums, podcast/audiobook creators, accessibility communities.",
        "skeleton": {
            "server.py": "# POST /v1/speech — generate audio\n# GET /v1/voices — list available voices\n# POST /v1/clone — clone a voice (premium)",
            "engines.py": "# Unified interface for edge-tts, Kokoro, Chatterbox",
        },
    },
    "api_website_builder": {
        "name": "Website Builder API",
        "category": "api_service",
        "description": "API that generates complete static sites from a business description. We already build 4000+ page empires — now sell it.",
        "estimated_hours": 32,
        "revenue_model": "usage-based",
        "price_suggestion": "$9.99/site or $49/mo unlimited",
        "distribution": "Direct / Website",
        "tech_stack": ["fastapi", "jinja2", "cloudflare_pages"],
        "marketing_plan": "Freelancer communities, web agency partnerships, SEO for 'AI website generator'.",
        "skeleton": {
            "server.py": "# POST /v1/sites/generate — generate full site\n# GET /v1/sites/{id}/status — build progress\n# POST /v1/sites/{id}/deploy — deploy to CF Pages",
            "generator.py": "# Site generation engine (from our empire builder)",
            "templates/": "# Base HTML templates for different industries",
        },
    },
    # ---- CLI Tools ----
    "cli_code_reviewer": {
        "name": "AI Code Reviewer (CLI)",
        "category": "cli_tool",
        "description": "CLI tool that reviews code diffs using AI. Catches bugs, security issues, style problems. Works with git hooks.",
        "estimated_hours": 20,
        "revenue_model": "freemium",
        "price_suggestion": "$9.99/mo pro with cloud analysis",
        "distribution": "npm",
        "tech_stack": ["nodejs", "gemma2_api", "git_hooks"],
        "marketing_plan": "GitHub README, npm weekly downloads, dev.to articles, HN Show HN.",
        "skeleton": {
            "index.js": "#!/usr/bin/env node\n// Parse git diff, send to AI, display results",
            "package.json": '{"name":"@hive/code-review","version":"1.0.0","bin":{"hive-review":"index.js"}}',
        },
    },
    "cli_deploy_automator": {
        "name": "Deployment Automator (CLI)",
        "category": "cli_tool",
        "description": "One command to deploy anywhere — Cloudflare, Vercel, Netlify, AWS, bare metal. Auto-detects framework.",
        "estimated_hours": 30,
        "revenue_model": "freemium",
        "price_suggestion": "$14.99/mo for team features",
        "distribution": "npm",
        "tech_stack": ["nodejs", "docker", "cloudflare", "ssh"],
        "marketing_plan": "DevOps communities, comparison blogs, GitHub stars campaign.",
        "skeleton": {
            "index.js": "#!/usr/bin/env node\n// Detect project type, build, deploy to target",
            "package.json": '{"name":"@hive/deploy","version":"1.0.0","bin":{"hive-deploy":"index.js"}}',
            "adapters/": "// Platform-specific deploy adapters",
        },
    },
    "cli_log_analyzer": {
        "name": "AI Log Analyzer (CLI)",
        "category": "cli_tool",
        "description": "Pipe any log file through AI for instant root cause analysis. Pattern detection, anomaly alerts.",
        "estimated_hours": 16,
        "revenue_model": "freemium",
        "price_suggestion": "$7.99/mo pro",
        "distribution": "PyPI",
        "tech_stack": ["python", "gemma2_api", "rich_cli"],
        "marketing_plan": "SRE/DevOps subreddits, sysadmin forums, YouTube tutorials.",
        "skeleton": {
            "hive_logs/__init__.py": "",
            "hive_logs/cli.py": "# CLI entry point — stdin pipe or file argument",
            "hive_logs/analyzer.py": "# AI-powered log pattern analysis",
            "setup.py": "# PyPI package config",
        },
    },
    "cli_security_scanner": {
        "name": "Security Scanner (CLI)",
        "category": "cli_tool",
        "description": "Scan repos for secrets, vulnerabilities, misconfigurations. AI-powered triage of findings.",
        "estimated_hours": 24,
        "revenue_model": "freemium",
        "price_suggestion": "$12.99/mo team plan",
        "distribution": "PyPI",
        "tech_stack": ["python", "regex", "gemma2_api"],
        "marketing_plan": "Security conferences, HN, InfoSec Twitter, GitHub Action integration.",
        "skeleton": {
            "hive_scan/__init__.py": "",
            "hive_scan/cli.py": "# CLI scanner entry point",
            "hive_scan/rules.py": "# Secret patterns, vuln signatures",
            "hive_scan/ai_triage.py": "# AI severity classification",
        },
    },
    # ---- Bots ----
    "bot_discord_mod": {
        "name": "Discord AI Moderator",
        "category": "bot",
        "description": "AI-powered Discord bot that moderates chat, answers questions, manages community. Uses gemma2 for understanding context.",
        "estimated_hours": 28,
        "revenue_model": "subscription",
        "price_suggestion": "$9.99/mo per server",
        "distribution": "Discord Marketplace",
        "tech_stack": ["python", "discord.py", "gemma2_api"],
        "marketing_plan": "Discord server lists, bot aggregator sites, Reddit r/discordapp.",
        "skeleton": {
            "bot.py": "# Discord bot with AI moderation\n# Auto-detect toxic content, spam, raids\n# Smart Q&A from server knowledge base",
            "config.py": "# Server-specific settings",
            "cogs/moderation.py": "# Moderation commands and auto-mod",
            "cogs/qa.py": "# AI question answering",
        },
    },
    "bot_slack_productivity": {
        "name": "Slack Productivity Bot",
        "category": "bot",
        "description": "AI Slack bot for standups, meeting summaries, task tracking, and smart search across channels.",
        "estimated_hours": 32,
        "revenue_model": "subscription",
        "price_suggestion": "$4.99/user/mo",
        "distribution": "Slack App Directory",
        "tech_stack": ["python", "slack_bolt", "gemma2_api"],
        "marketing_plan": "Product Hunt, Slack app directory, productivity blogs, LinkedIn posts.",
        "skeleton": {
            "app.py": "# Slack Bolt app with AI features",
            "handlers/standup.py": "# Automated standup collection and summary",
            "handlers/meetings.py": "# Meeting note summarization",
            "handlers/search.py": "# AI-powered channel search",
        },
    },
    "bot_telegram_signals": {
        "name": "Telegram Trading Signals Bot",
        "category": "bot",
        "description": "AI-powered trading signals via Telegram. Technical analysis, news sentiment, risk scoring. Wraps our forex engine.",
        "estimated_hours": 24,
        "revenue_model": "subscription",
        "price_suggestion": "$29.99/mo",
        "distribution": "Telegram BotFather",
        "tech_stack": ["python", "python_telegram_bot", "forex_engine"],
        "marketing_plan": "Trading Telegram groups, forex forums, YouTube trading channels, affiliate partners.",
        "skeleton": {
            "bot.py": "# Telegram bot for trading signals\n# /signal — get current signal\n# /analysis — full technical analysis\n# /risk — risk assessment",
            "signals.py": "# Signal generation from forex engine",
            "analysis.py": "# Technical analysis wrappers",
        },
    },
    "bot_whatsapp_business": {
        "name": "WhatsApp Business Assistant",
        "category": "bot",
        "description": "AI assistant for WhatsApp Business — auto-reply, appointment booking, order tracking. We already run one internally.",
        "estimated_hours": 28,
        "revenue_model": "subscription",
        "price_suggestion": "$19.99/mo",
        "distribution": "Direct / Website",
        "tech_stack": ["python", "whatsapp_api", "gemma2_api"],
        "marketing_plan": "Small business forums, local business groups, SEO for 'WhatsApp AI assistant'.",
        "skeleton": {
            "server.py": "# WhatsApp webhook handler\n# Auto-reply with AI\n# Appointment booking flow\n# Order status lookup",
            "flows.py": "# Conversation flow definitions",
            "integrations.py": "# Calendar, CRM, order system connectors",
        },
    },
}

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
def get_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS products (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            description TEXT,
            stage TEXT NOT NULL DEFAULT 'idea',
            template_key TEXT,
            revenue_model TEXT,
            price TEXT,
            distribution TEXT,
            tech_stack TEXT,
            marketing_plan TEXT,
            skeleton TEXT,
            estimated_hours REAL DEFAULT 0,
            actual_hours REAL DEFAULT 0,
            monthly_revenue REAL DEFAULT 0,
            total_revenue REAL DEFAULT 0,
            users INTEGER DEFAULT 0,
            rating REAL DEFAULT 0,
            launch_url TEXT,
            repo_url TEXT,
            notes TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            researched_at TEXT,
            build_started_at TEXT,
            tested_at TEXT,
            launched_at TEXT
        );

        CREATE TABLE IF NOT EXISTS stage_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id TEXT NOT NULL,
            from_stage TEXT,
            to_stage TEXT NOT NULL,
            notes TEXT,
            timestamp TEXT NOT NULL,
            FOREIGN KEY (product_id) REFERENCES products(id)
        );

        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id TEXT NOT NULL,
            title TEXT NOT NULL,
            description TEXT,
            status TEXT NOT NULL DEFAULT 'todo',
            priority INTEGER DEFAULT 3,
            assigned_to TEXT,
            created_at TEXT NOT NULL,
            completed_at TEXT,
            FOREIGN KEY (product_id) REFERENCES products(id)
        );

        CREATE TABLE IF NOT EXISTS metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id TEXT NOT NULL,
            metric_name TEXT NOT NULL,
            metric_value REAL NOT NULL,
            recorded_at TEXT NOT NULL,
            FOREIGN KEY (product_id) REFERENCES products(id)
        );

        CREATE INDEX IF NOT EXISTS idx_products_stage ON products(stage);
        CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);
        CREATE INDEX IF NOT EXISTS idx_stage_log_product ON stage_log(product_id);
        CREATE INDEX IF NOT EXISTS idx_tasks_product ON tasks(product_id);
        CREATE INDEX IF NOT EXISTS idx_metrics_product ON metrics(product_id);
    """)
    conn.commit()
    conn.close()


def make_id(name: str) -> str:
    slug = name.lower().replace(" ", "-").replace("(", "").replace(")", "")
    slug = "".join(c for c in slug if c.isalnum() or c == "-")
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug.strip("-")


# ---------------------------------------------------------------------------
# Pydantic Models
# ---------------------------------------------------------------------------
class ProductCreate(BaseModel):
    name: str
    category: str = "general"
    description: str = ""
    template_key: Optional[str] = None
    revenue_model: str = "subscription"
    price: str = ""
    distribution: str = "Direct / Website"
    tech_stack: List[str] = Field(default_factory=list)
    marketing_plan: str = ""
    estimated_hours: float = 0
    notes: str = ""


class StageAdvance(BaseModel):
    notes: str = ""


class TaskCreate(BaseModel):
    title: str
    description: str = ""
    priority: int = 3
    assigned_to: str = ""


class MetricRecord(BaseModel):
    metric_name: str
    metric_value: float


# ---------------------------------------------------------------------------
# FastAPI App
# ---------------------------------------------------------------------------
app = FastAPI(title="Hive Product Factory", version="1.0.0")


@app.on_event("startup")
def startup():
    init_db()


# ---- Health ----
@app.get("/health")
def health():
    conn = get_db()
    try:
        row = conn.execute("SELECT COUNT(*) as cnt FROM products").fetchone()
        count = row["cnt"]
    except Exception:
        count = 0
    finally:
        conn.close()
    return {
        "status": "ok",
        "service": "product-factory",
        "port": PORT,
        "products": count,
        "templates": len(PRODUCT_TEMPLATES),
        "timestamp": datetime.utcnow().isoformat(),
    }


# ---- Templates ----
@app.get("/api/templates")
def list_templates():
    """All product templates ready to build."""
    result = {}
    for key, tpl in PRODUCT_TEMPLATES.items():
        result[key] = {
            "name": tpl["name"],
            "category": tpl["category"],
            "description": tpl["description"],
            "estimated_hours": tpl["estimated_hours"],
            "revenue_model": tpl["revenue_model"],
            "price_suggestion": tpl["price_suggestion"],
            "distribution": tpl["distribution"],
            "tech_stack": tpl["tech_stack"],
        }
    by_category = {}
    for key, tpl in PRODUCT_TEMPLATES.items():
        cat = tpl["category"]
        if cat not in by_category:
            by_category[cat] = []
        by_category[cat].append({"key": key, "name": tpl["name"], "hours": tpl["estimated_hours"]})
    return {
        "total": len(PRODUCT_TEMPLATES),
        "by_category": by_category,
        "templates": result,
    }


# ---- Products CRUD ----
@app.get("/api/products")
def list_products(stage: Optional[str] = None, category: Optional[str] = None):
    """All product ideas with status."""
    conn = get_db()
    query = "SELECT * FROM products WHERE 1=1"
    params = []
    if stage:
        query += " AND stage = ?"
        params.append(stage)
    if category:
        query += " AND category = ?"
        params.append(category)
    query += " ORDER BY created_at DESC"
    rows = conn.execute(query, params).fetchall()
    conn.close()
    products = []
    for r in rows:
        p = dict(r)
        p["tech_stack"] = json.loads(p["tech_stack"]) if p["tech_stack"] else []
        p["skeleton"] = json.loads(p["skeleton"]) if p["skeleton"] else {}
        products.append(p)

    stage_counts = {}
    for p in products:
        s = p["stage"]
        stage_counts[s] = stage_counts.get(s, 0) + 1

    return {
        "total": len(products),
        "stage_counts": stage_counts,
        "products": products,
    }


@app.get("/api/products/{product_id}")
def get_product(product_id: str):
    conn = get_db()
    row = conn.execute("SELECT * FROM products WHERE id = ?", (product_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, f"Product '{product_id}' not found")
    product = dict(row)
    product["tech_stack"] = json.loads(product["tech_stack"]) if product["tech_stack"] else []
    product["skeleton"] = json.loads(product["skeleton"]) if product["skeleton"] else {}

    # Get stage history
    log_rows = conn.execute(
        "SELECT * FROM stage_log WHERE product_id = ? ORDER BY timestamp", (product_id,)
    ).fetchall()
    product["stage_history"] = [dict(r) for r in log_rows]

    # Get tasks
    task_rows = conn.execute(
        "SELECT * FROM tasks WHERE product_id = ? ORDER BY priority, created_at", (product_id,)
    ).fetchall()
    product["tasks"] = [dict(r) for r in task_rows]

    # Get recent metrics
    metric_rows = conn.execute(
        "SELECT * FROM metrics WHERE product_id = ? ORDER BY recorded_at DESC LIMIT 50",
        (product_id,),
    ).fetchall()
    product["metrics"] = [dict(r) for r in metric_rows]

    conn.close()
    return product


@app.post("/api/create")
def create_product(req: ProductCreate):
    """Create a new product idea, optionally from a template."""
    now = datetime.utcnow().isoformat()
    pid = make_id(req.name)

    # If creating from template, merge template data
    tpl = PRODUCT_TEMPLATES.get(req.template_key) if req.template_key else None
    name = req.name or (tpl["name"] if tpl else "Unnamed Product")
    category = req.category if req.category != "general" else (tpl["category"] if tpl else "general")
    description = req.description or (tpl["description"] if tpl else "")
    revenue_model = req.revenue_model or (tpl["revenue_model"] if tpl else "subscription")
    price = req.price or (tpl.get("price_suggestion", "") if tpl else "")
    distribution = req.distribution or (tpl["distribution"] if tpl else "Direct / Website")
    tech_stack = req.tech_stack or (tpl["tech_stack"] if tpl else [])
    marketing_plan = req.marketing_plan or (tpl.get("marketing_plan", "") if tpl else "")
    estimated_hours = req.estimated_hours or (tpl["estimated_hours"] if tpl else 0)
    skeleton = tpl.get("skeleton", {}) if tpl else {}

    conn = get_db()
    # Check for duplicates
    existing = conn.execute("SELECT id FROM products WHERE id = ?", (pid,)).fetchone()
    if existing:
        conn.close()
        raise HTTPException(409, f"Product '{pid}' already exists")

    conn.execute(
        """INSERT INTO products
        (id, name, category, description, stage, template_key, revenue_model,
         price, distribution, tech_stack, marketing_plan, skeleton,
         estimated_hours, notes, created_at, updated_at)
        VALUES (?, ?, ?, ?, 'idea', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            pid, name, category, description, req.template_key,
            revenue_model, price, distribution,
            json.dumps(tech_stack), marketing_plan, json.dumps(skeleton),
            estimated_hours, req.notes, now, now,
        ),
    )
    conn.execute(
        "INSERT INTO stage_log (product_id, from_stage, to_stage, notes, timestamp) VALUES (?, NULL, 'idea', 'Product created', ?)",
        (pid, now),
    )
    conn.commit()
    conn.close()

    return {
        "status": "created",
        "product_id": pid,
        "name": name,
        "stage": "idea",
        "template_used": req.template_key,
    }


@app.post("/api/create-from-template/{template_key}")
def create_from_template(template_key: str):
    """Quick-create a product directly from a template key."""
    tpl = PRODUCT_TEMPLATES.get(template_key)
    if not tpl:
        raise HTTPException(404, f"Template '{template_key}' not found. Use GET /api/templates for list.")
    return create_product(ProductCreate(
        name=tpl["name"],
        category=tpl["category"],
        description=tpl["description"],
        template_key=template_key,
        revenue_model=tpl["revenue_model"],
        price=tpl.get("price_suggestion", ""),
        distribution=tpl["distribution"],
        tech_stack=tpl["tech_stack"],
        marketing_plan=tpl.get("marketing_plan", ""),
        estimated_hours=tpl["estimated_hours"],
    ))


@app.post("/api/products/{product_id}/advance")
def advance_stage(product_id: str, req: StageAdvance):
    """Move product to the next lifecycle stage."""
    conn = get_db()
    row = conn.execute("SELECT * FROM products WHERE id = ?", (product_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, f"Product '{product_id}' not found")

    current = row["stage"]
    if current not in STAGES:
        conn.close()
        raise HTTPException(400, f"Unknown current stage: {current}")

    idx = STAGES.index(current)
    if idx >= len(STAGES) - 1:
        conn.close()
        raise HTTPException(400, f"Product is already at final stage: {current}")

    next_stage = STAGES[idx + 1]
    now = datetime.utcnow().isoformat()

    # Update stage and relevant timestamp
    timestamp_col = {
        "research": "researched_at",
        "build": "build_started_at",
        "test": "tested_at",
        "launch": "launched_at",
    }
    extra_set = ""
    if next_stage in timestamp_col:
        extra_set = f", {timestamp_col[next_stage]} = ?"

    if extra_set:
        conn.execute(
            f"UPDATE products SET stage = ?, updated_at = ?{extra_set} WHERE id = ?",
            (next_stage, now, now, product_id),
        )
    else:
        conn.execute(
            "UPDATE products SET stage = ?, updated_at = ? WHERE id = ?",
            (next_stage, now, product_id),
        )

    conn.execute(
        "INSERT INTO stage_log (product_id, from_stage, to_stage, notes, timestamp) VALUES (?, ?, ?, ?, ?)",
        (product_id, current, next_stage, req.notes or f"Advanced to {next_stage}", now),
    )
    conn.commit()
    conn.close()

    return {
        "product_id": product_id,
        "previous_stage": current,
        "new_stage": next_stage,
        "notes": req.notes,
    }


@app.post("/api/products/{product_id}/set-stage/{stage}")
def set_stage(product_id: str, stage: str, req: StageAdvance):
    """Force-set a product to a specific stage (for corrections)."""
    if stage not in STAGES:
        raise HTTPException(400, f"Invalid stage '{stage}'. Valid: {STAGES}")
    conn = get_db()
    row = conn.execute("SELECT stage FROM products WHERE id = ?", (product_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, f"Product '{product_id}' not found")
    old_stage = row["stage"]
    now = datetime.utcnow().isoformat()
    conn.execute(
        "UPDATE products SET stage = ?, updated_at = ? WHERE id = ?",
        (stage, now, product_id),
    )
    conn.execute(
        "INSERT INTO stage_log (product_id, from_stage, to_stage, notes, timestamp) VALUES (?, ?, ?, ?, ?)",
        (product_id, old_stage, stage, req.notes or f"Stage set to {stage}", now),
    )
    conn.commit()
    conn.close()
    return {"product_id": product_id, "previous_stage": old_stage, "new_stage": stage}


@app.put("/api/products/{product_id}")
def update_product(product_id: str, req: Request):
    """Update product fields (partial update)."""
    import asyncio
    # We need to handle this synchronously for simplicity
    # FastAPI will handle the async body reading
    return _update_product_sync(product_id)


@app.api_route("/api/products/{product_id}/update", methods=["POST"])
async def update_product_post(product_id: str, request: Request):
    """Update product fields via POST (easier for curl)."""
    body = await request.json()
    conn = get_db()
    row = conn.execute("SELECT id FROM products WHERE id = ?", (product_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, f"Product '{product_id}' not found")

    allowed = [
        "name", "description", "revenue_model", "price", "distribution",
        "marketing_plan", "estimated_hours", "actual_hours", "monthly_revenue",
        "total_revenue", "users", "rating", "launch_url", "repo_url", "notes",
    ]
    sets = []
    params = []
    for key in allowed:
        if key in body:
            sets.append(f"{key} = ?")
            params.append(body[key])

    if "tech_stack" in body:
        sets.append("tech_stack = ?")
        params.append(json.dumps(body["tech_stack"]))

    if not sets:
        conn.close()
        return {"status": "no changes", "product_id": product_id}

    now = datetime.utcnow().isoformat()
    sets.append("updated_at = ?")
    params.append(now)
    params.append(product_id)

    conn.execute(f"UPDATE products SET {', '.join(sets)} WHERE id = ?", params)
    conn.commit()
    conn.close()
    return {"status": "updated", "product_id": product_id, "fields_updated": len(sets) - 1}


# ---- Tasks ----
@app.post("/api/products/{product_id}/tasks")
def add_task(product_id: str, req: TaskCreate):
    conn = get_db()
    row = conn.execute("SELECT id FROM products WHERE id = ?", (product_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, f"Product '{product_id}' not found")
    now = datetime.utcnow().isoformat()
    cursor = conn.execute(
        "INSERT INTO tasks (product_id, title, description, priority, assigned_to, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (product_id, req.title, req.description, req.priority, req.assigned_to, now),
    )
    task_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return {"status": "created", "task_id": task_id, "product_id": product_id}


@app.post("/api/tasks/{task_id}/complete")
def complete_task(task_id: int):
    conn = get_db()
    row = conn.execute("SELECT id FROM tasks WHERE id = ?", (task_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, f"Task {task_id} not found")
    now = datetime.utcnow().isoformat()
    conn.execute("UPDATE tasks SET status = 'done', completed_at = ? WHERE id = ?", (now, task_id))
    conn.commit()
    conn.close()
    return {"status": "completed", "task_id": task_id}


# ---- Metrics ----
@app.post("/api/products/{product_id}/metrics")
def record_metric(product_id: str, req: MetricRecord):
    conn = get_db()
    row = conn.execute("SELECT id FROM products WHERE id = ?", (product_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, f"Product '{product_id}' not found")
    now = datetime.utcnow().isoformat()
    conn.execute(
        "INSERT INTO metrics (product_id, metric_name, metric_value, recorded_at) VALUES (?, ?, ?, ?)",
        (product_id, req.metric_name, req.metric_value, now),
    )
    conn.commit()
    conn.close()
    return {"status": "recorded", "product_id": product_id, "metric": req.metric_name}


# ---- Pipeline View ----
@app.get("/api/pipeline")
def pipeline():
    """Products in active development (research, build, test stages)."""
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM products WHERE stage IN ('research', 'build', 'test') ORDER BY stage, updated_at DESC"
    ).fetchall()
    conn.close()
    result = {"research": [], "build": [], "test": []}
    for r in rows:
        p = dict(r)
        p["tech_stack"] = json.loads(p["tech_stack"]) if p["tech_stack"] else []
        p.pop("skeleton", None)
        result[p["stage"]].append(p)
    total = sum(len(v) for v in result.values())
    return {"total_in_pipeline": total, "pipeline": result}


# ---- Launched Products ----
@app.get("/api/launched")
def launched():
    """Live products (launch and grow stages)."""
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM products WHERE stage IN ('launch', 'grow') ORDER BY monthly_revenue DESC, launched_at DESC"
    ).fetchall()
    conn.close()
    products = []
    total_mrr = 0
    total_users = 0
    for r in rows:
        p = dict(r)
        p["tech_stack"] = json.loads(p["tech_stack"]) if p["tech_stack"] else []
        p.pop("skeleton", None)
        total_mrr += p.get("monthly_revenue") or 0
        total_users += p.get("users") or 0
        products.append(p)
    return {
        "total_launched": len(products),
        "total_mrr": total_mrr,
        "total_users": total_users,
        "products": products,
    }


# ---- Roadmap ----
@app.get("/api/roadmap")
def roadmap():
    """Product roadmap — what's planned, in progress, and launched."""
    conn = get_db()
    rows = conn.execute("SELECT * FROM products ORDER BY stage, estimated_hours").fetchall()
    conn.close()

    roadmap_data = {stage: [] for stage in STAGES}
    total_hours_remaining = 0
    total_estimated = 0

    for r in rows:
        p = dict(r)
        p["tech_stack"] = json.loads(p["tech_stack"]) if p["tech_stack"] else []
        p.pop("skeleton", None)
        roadmap_data[p["stage"]].append(p)
        est = p.get("estimated_hours") or 0
        act = p.get("actual_hours") or 0
        total_estimated += est
        if p["stage"] not in ("launch", "grow"):
            total_hours_remaining += max(est - act, 0)

    # Calculate capacity / timeline estimates
    # Assume 4 productive hours/day, 5 days/week
    hours_per_week = 20
    weeks_needed = total_hours_remaining / hours_per_week if hours_per_week > 0 else 0

    summary = {
        "total_products": sum(len(v) for v in roadmap_data.values()),
        "by_stage": {stage: len(items) for stage, items in roadmap_data.items()},
        "total_estimated_hours": total_estimated,
        "hours_remaining": round(total_hours_remaining, 1),
        "weeks_to_complete_all": round(weeks_needed, 1),
        "capacity_assumption": f"{hours_per_week} productive hours/week",
    }

    # Quick-win analysis: products under 20 hours in idea/research
    quick_wins = []
    for stage in ("idea", "research"):
        for p in roadmap_data[stage]:
            if (p.get("estimated_hours") or 0) <= 20:
                quick_wins.append({
                    "id": p["id"],
                    "name": p["name"],
                    "hours": p.get("estimated_hours", 0),
                    "revenue_model": p.get("revenue_model"),
                    "stage": stage,
                })
    quick_wins.sort(key=lambda x: x["hours"])

    # Revenue potential
    revenue_categories = {}
    for stage_products in roadmap_data.values():
        for p in stage_products:
            rm = p.get("revenue_model") or "unknown"
            if rm not in revenue_categories:
                revenue_categories[rm] = 0
            revenue_categories[rm] += 1

    return {
        "summary": summary,
        "quick_wins": quick_wins,
        "revenue_model_distribution": revenue_categories,
        "roadmap": roadmap_data,
    }


# ---- Stats ----
@app.get("/api/stats")
def stats():
    """Aggregate stats across all products."""
    conn = get_db()

    total = conn.execute("SELECT COUNT(*) as cnt FROM products").fetchone()["cnt"]
    by_stage = {}
    for row in conn.execute("SELECT stage, COUNT(*) as cnt FROM products GROUP BY stage").fetchall():
        by_stage[row["stage"]] = row["cnt"]
    by_cat = {}
    for row in conn.execute("SELECT category, COUNT(*) as cnt FROM products GROUP BY category").fetchall():
        by_cat[row["category"]] = row["cnt"]

    revenue_row = conn.execute(
        "SELECT COALESCE(SUM(monthly_revenue), 0) as mrr, COALESCE(SUM(total_revenue), 0) as total_rev, COALESCE(SUM(users), 0) as total_users FROM products WHERE stage IN ('launch', 'grow')"
    ).fetchone()

    hours_row = conn.execute(
        "SELECT COALESCE(SUM(estimated_hours), 0) as est, COALESCE(SUM(actual_hours), 0) as act FROM products"
    ).fetchone()

    task_row = conn.execute(
        "SELECT COUNT(*) as total, SUM(CASE WHEN status='done' THEN 1 ELSE 0 END) as done FROM tasks"
    ).fetchone()

    transitions = conn.execute("SELECT COUNT(*) as cnt FROM stage_log").fetchone()["cnt"]

    conn.close()

    return {
        "total_products": total,
        "by_stage": by_stage,
        "by_category": by_cat,
        "revenue": {
            "monthly_recurring": revenue_row["mrr"],
            "total_lifetime": revenue_row["total_rev"],
            "total_users": revenue_row["total_users"],
        },
        "hours": {
            "estimated": hours_row["est"],
            "actual": hours_row["act"],
        },
        "tasks": {
            "total": task_row["total"],
            "completed": task_row["done"] or 0,
        },
        "stage_transitions": transitions,
        "templates_available": len(PRODUCT_TEMPLATES),
    }


# ---- Delete ----
@app.delete("/api/products/{product_id}")
def delete_product(product_id: str):
    conn = get_db()
    row = conn.execute("SELECT id FROM products WHERE id = ?", (product_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, f"Product '{product_id}' not found")
    conn.execute("DELETE FROM metrics WHERE product_id = ?", (product_id,))
    conn.execute("DELETE FROM tasks WHERE product_id = ?", (product_id,))
    conn.execute("DELETE FROM stage_log WHERE product_id = ?", (product_id,))
    conn.execute("DELETE FROM products WHERE id = ?", (product_id,))
    conn.commit()
    conn.close()
    return {"status": "deleted", "product_id": product_id}


# ---- Dashboard ----
@app.get("/", response_class=HTMLResponse)
def dashboard():
    """HTML dashboard showing the full product pipeline."""
    conn = get_db()
    products = conn.execute("SELECT * FROM products ORDER BY stage, updated_at DESC").fetchall()

    stage_counts = {s: 0 for s in STAGES}
    total_mrr = 0
    total_hours = 0
    for p in products:
        stage_counts[p["stage"]] = stage_counts.get(p["stage"], 0) + 1
        total_mrr += p["monthly_revenue"] or 0
        total_hours += p["estimated_hours"] or 0

    template_count = len(PRODUCT_TEMPLATES)
    conn.close()

    # Build product rows by stage
    stage_html = {}
    for stage in STAGES:
        rows_html = ""
        for p in products:
            if p["stage"] != stage:
                continue
            tech = json.loads(p["tech_stack"]) if p["tech_stack"] else []
            tech_str = ", ".join(tech[:3])
            if len(tech) > 3:
                tech_str += f" +{len(tech)-3}"
            mrr_str = f"${p['monthly_revenue']:.0f}/mo" if p["monthly_revenue"] else "-"
            users_str = f"{p['users']:,}" if p["users"] else "-"
            rows_html += f"""
                <tr>
                    <td><strong>{p['name']}</strong><br><small style="color:#888">{p['id']}</small></td>
                    <td>{p['category']}</td>
                    <td>{p['revenue_model'] or '-'}</td>
                    <td>{p['estimated_hours'] or 0}h</td>
                    <td>{mrr_str}</td>
                    <td>{users_str}</td>
                    <td><small>{tech_str}</small></td>
                </tr>"""
        stage_html[stage] = rows_html

    # Build template cards
    tpl_cards = ""
    for key, tpl in PRODUCT_TEMPLATES.items():
        cat_color = {
            "chrome_extension": "#4285f4",
            "api_service": "#34a853",
            "cli_tool": "#fbbc05",
            "bot": "#ea4335",
        }.get(tpl["category"], "#666")
        tpl_cards += f"""
            <div class="tpl-card">
                <div class="tpl-cat" style="background:{cat_color}">{tpl['category'].replace('_',' ').title()}</div>
                <h4>{tpl['name']}</h4>
                <p>{tpl['description'][:120]}{'...' if len(tpl['description'])>120 else ''}</p>
                <div class="tpl-meta">
                    <span>{tpl['estimated_hours']}h build</span>
                    <span>{tpl['revenue_model']}</span>
                    <span>{tpl['price_suggestion']}</span>
                </div>
                <code class="tpl-key">{key}</code>
            </div>"""

    stage_colors = {
        "idea": "#6c757d",
        "research": "#0d6efd",
        "build": "#fd7e14",
        "test": "#ffc107",
        "launch": "#198754",
        "grow": "#20c997",
    }
    stage_icons = {
        "idea": "&#x1f4a1;",
        "research": "&#x1f50d;",
        "build": "&#x1f528;",
        "test": "&#x1f9ea;",
        "launch": "&#x1f680;",
        "grow": "&#x1f4c8;",
    }

    pipeline_boxes = ""
    for stage in STAGES:
        c = stage_colors[stage]
        icon = stage_icons[stage]
        cnt = stage_counts.get(stage, 0)
        pipeline_boxes += f"""
            <div class="stage-box" style="border-color:{c}">
                <div class="stage-icon">{icon}</div>
                <div class="stage-name" style="color:{c}">{stage.upper()}</div>
                <div class="stage-count">{cnt}</div>
            </div>
            {'<div class="stage-arrow">&#x27A1;</div>' if stage != 'grow' else ''}"""

    # Stage sections
    stage_sections = ""
    for stage in STAGES:
        c = stage_colors[stage]
        rows = stage_html.get(stage, "")
        if not rows:
            rows = '<tr><td colspan="7" style="text-align:center;color:#888;padding:20px">No products in this stage</td></tr>'
        stage_sections += f"""
        <div class="stage-section">
            <h3 style="color:{c}">{stage_icons[stage]} {stage.upper()} ({stage_counts.get(stage,0)})</h3>
            <table>
                <thead>
                    <tr><th>Product</th><th>Category</th><th>Revenue</th><th>Est.</th><th>MRR</th><th>Users</th><th>Tech</th></tr>
                </thead>
                <tbody>{rows}</tbody>
            </table>
        </div>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Hive Product Factory</title>
<style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0a0a0a; color: #e0e0e0; padding: 20px; }}
    h1 {{ color: #ff9800; margin-bottom: 5px; font-size: 28px; }}
    h2 {{ color: #ccc; margin: 30px 0 15px; font-size: 20px; border-bottom: 1px solid #333; padding-bottom: 8px; }}
    h3 {{ margin-bottom: 10px; font-size: 16px; }}
    h4 {{ margin: 8px 0 4px; font-size: 14px; }}
    .subtitle {{ color: #888; margin-bottom: 25px; font-size: 14px; }}

    /* KPI strip */
    .kpi-strip {{ display: flex; gap: 15px; flex-wrap: wrap; margin-bottom: 30px; }}
    .kpi {{ background: #1a1a1a; border: 1px solid #333; border-radius: 8px; padding: 15px 20px; min-width: 140px; text-align: center; }}
    .kpi-value {{ font-size: 28px; font-weight: bold; color: #ff9800; }}
    .kpi-label {{ font-size: 11px; color: #888; text-transform: uppercase; letter-spacing: 1px; margin-top: 4px; }}

    /* Pipeline visualization */
    .pipeline {{ display: flex; align-items: center; gap: 8px; margin-bottom: 30px; flex-wrap: wrap; justify-content: center; }}
    .stage-box {{ background: #1a1a1a; border: 2px solid; border-radius: 10px; padding: 12px 18px; text-align: center; min-width: 90px; }}
    .stage-icon {{ font-size: 24px; }}
    .stage-name {{ font-size: 11px; font-weight: bold; letter-spacing: 1px; margin-top: 4px; }}
    .stage-count {{ font-size: 24px; font-weight: bold; color: #fff; margin-top: 2px; }}
    .stage-arrow {{ color: #555; font-size: 20px; }}

    /* Stage sections */
    .stage-section {{ background: #1a1a1a; border: 1px solid #2a2a2a; border-radius: 8px; padding: 15px; margin-bottom: 15px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    th {{ text-align: left; padding: 8px 10px; color: #888; border-bottom: 1px solid #333; font-size: 11px; text-transform: uppercase; }}
    td {{ padding: 8px 10px; border-bottom: 1px solid #1e1e1e; }}
    tr:hover {{ background: #222; }}

    /* Template cards */
    .tpl-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); gap: 12px; }}
    .tpl-card {{ background: #1a1a1a; border: 1px solid #2a2a2a; border-radius: 8px; padding: 14px; }}
    .tpl-card:hover {{ border-color: #ff9800; }}
    .tpl-cat {{ display: inline-block; padding: 2px 8px; border-radius: 4px; color: #fff; font-size: 10px; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 6px; }}
    .tpl-card p {{ color: #999; font-size: 12px; margin: 6px 0; line-height: 1.4; }}
    .tpl-meta {{ display: flex; gap: 10px; font-size: 11px; color: #888; margin-top: 8px; flex-wrap: wrap; }}
    .tpl-meta span {{ background: #222; padding: 2px 6px; border-radius: 3px; }}
    .tpl-key {{ display: block; margin-top: 8px; color: #ff9800; font-size: 11px; }}

    /* API reference */
    .api-ref {{ background: #1a1a1a; border: 1px solid #2a2a2a; border-radius: 8px; padding: 15px; }}
    .api-endpoint {{ display: flex; gap: 10px; align-items: baseline; padding: 6px 0; border-bottom: 1px solid #1e1e1e; font-size: 13px; }}
    .api-method {{ font-weight: bold; min-width: 50px; }}
    .api-method.get {{ color: #34a853; }}
    .api-method.post {{ color: #4285f4; }}
    .api-method.put {{ color: #fbbc05; }}
    .api-method.delete {{ color: #ea4335; }}
    .api-path {{ color: #ccc; font-family: monospace; }}
    .api-desc {{ color: #888; font-size: 12px; }}

    /* Quick actions */
    .actions {{ background: #1a1a1a; border: 1px solid #2a2a2a; border-radius: 8px; padding: 15px; }}
    .action-code {{ background: #0d0d0d; padding: 10px; border-radius: 5px; font-family: monospace; font-size: 12px; color: #aaa; margin: 8px 0; overflow-x: auto; white-space: pre; }}

    @media (max-width: 768px) {{
        .kpi-strip {{ gap: 8px; }}
        .kpi {{ min-width: 100px; padding: 10px; }}
        .kpi-value {{ font-size: 20px; }}
        .tpl-grid {{ grid-template-columns: 1fr; }}
    }}
</style>
</head>
<body>

<h1>Hive Product Factory</h1>
<p class="subtitle">Software product builder and launcher &mdash; every Hive service is a product waiting to ship</p>

<div class="kpi-strip">
    <div class="kpi">
        <div class="kpi-value">{sum(stage_counts.values())}</div>
        <div class="kpi-label">Total Products</div>
    </div>
    <div class="kpi">
        <div class="kpi-value">{template_count}</div>
        <div class="kpi-label">Templates Ready</div>
    </div>
    <div class="kpi">
        <div class="kpi-value">{stage_counts.get('build',0)+stage_counts.get('test',0)}</div>
        <div class="kpi-label">In Pipeline</div>
    </div>
    <div class="kpi">
        <div class="kpi-value">{stage_counts.get('launch',0)+stage_counts.get('grow',0)}</div>
        <div class="kpi-label">Launched</div>
    </div>
    <div class="kpi">
        <div class="kpi-value">${total_mrr:,.0f}</div>
        <div class="kpi-label">Monthly Revenue</div>
    </div>
    <div class="kpi">
        <div class="kpi-value">{total_hours:,.0f}h</div>
        <div class="kpi-label">Total Est. Hours</div>
    </div>
</div>

<div class="pipeline">{pipeline_boxes}</div>

{stage_sections}

<h2>Product Templates ({template_count} ready to build)</h2>
<div class="tpl-grid">{tpl_cards}</div>

<h2>API Reference</h2>
<div class="api-ref">
    <div class="api-endpoint"><span class="api-method get">GET</span> <span class="api-path">/health</span> <span class="api-desc">Service health check</span></div>
    <div class="api-endpoint"><span class="api-method get">GET</span> <span class="api-path">/api/products</span> <span class="api-desc">All products (filter: ?stage=build&amp;category=bot)</span></div>
    <div class="api-endpoint"><span class="api-method get">GET</span> <span class="api-path">/api/products/{{id}}</span> <span class="api-desc">Product detail with tasks, metrics, history</span></div>
    <div class="api-endpoint"><span class="api-method get">GET</span> <span class="api-path">/api/templates</span> <span class="api-desc">All product templates grouped by category</span></div>
    <div class="api-endpoint"><span class="api-method get">GET</span> <span class="api-path">/api/pipeline</span> <span class="api-desc">Products in research/build/test stages</span></div>
    <div class="api-endpoint"><span class="api-method get">GET</span> <span class="api-path">/api/launched</span> <span class="api-desc">Live products with revenue stats</span></div>
    <div class="api-endpoint"><span class="api-method get">GET</span> <span class="api-path">/api/roadmap</span> <span class="api-desc">Full roadmap with quick-win analysis</span></div>
    <div class="api-endpoint"><span class="api-method get">GET</span> <span class="api-path">/api/stats</span> <span class="api-desc">Aggregate statistics</span></div>
    <div class="api-endpoint"><span class="api-method post">POST</span> <span class="api-path">/api/create</span> <span class="api-desc">Create new product (JSON body)</span></div>
    <div class="api-endpoint"><span class="api-method post">POST</span> <span class="api-path">/api/create-from-template/{{key}}</span> <span class="api-desc">Quick-create from template</span></div>
    <div class="api-endpoint"><span class="api-method post">POST</span> <span class="api-path">/api/products/{{id}}/advance</span> <span class="api-desc">Advance to next lifecycle stage</span></div>
    <div class="api-endpoint"><span class="api-method post">POST</span> <span class="api-path">/api/products/{{id}}/set-stage/{{stage}}</span> <span class="api-desc">Force-set stage</span></div>
    <div class="api-endpoint"><span class="api-method post">POST</span> <span class="api-path">/api/products/{{id}}/update</span> <span class="api-desc">Update product fields</span></div>
    <div class="api-endpoint"><span class="api-method post">POST</span> <span class="api-path">/api/products/{{id}}/tasks</span> <span class="api-desc">Add task to product</span></div>
    <div class="api-endpoint"><span class="api-method post">POST</span> <span class="api-path">/api/tasks/{{id}}/complete</span> <span class="api-desc">Mark task completed</span></div>
    <div class="api-endpoint"><span class="api-method post">POST</span> <span class="api-path">/api/products/{{id}}/metrics</span> <span class="api-desc">Record product metric</span></div>
    <div class="api-endpoint"><span class="api-method delete">DELETE</span> <span class="api-path">/api/products/{{id}}</span> <span class="api-desc">Delete product</span></div>
</div>

<h2>Quick Actions</h2>
<div class="actions">
    <p><strong>Create from template:</strong></p>
    <div class="action-code">curl -s -X POST http://localhost:8916/api/create-from-template/chrome_ai_writer | python3 -m json.tool</div>

    <p><strong>Create custom product:</strong></p>
    <div class="action-code">curl -s -X POST http://localhost:8916/api/create \\
  -H "Content-Type: application/json" \\
  -d '{{"name": "My Product", "category": "api_service", "revenue_model": "subscription", "estimated_hours": 20}}'</div>

    <p><strong>Advance to next stage:</strong></p>
    <div class="action-code">curl -s -X POST http://localhost:8916/api/products/my-product/advance \\
  -H "Content-Type: application/json" -d '{{"notes": "Research complete, ready to build"}}'</div>

    <p><strong>Seed all templates as ideas:</strong></p>
    <div class="action-code">for key in $(curl -s http://localhost:8916/api/templates | python3 -c "import sys,json; [print(k) for k in json.load(sys.stdin)['templates']]"); do
  curl -s -X POST http://localhost:8916/api/create-from-template/$key
done</div>
</div>

<p style="text-align:center; color:#555; margin-top:30px; font-size:12px">
    Hive Product Factory v1.0 &mdash; Port {PORT} &mdash; {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}
</p>

</body>
</html>"""
    return HTMLResponse(html)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print(f"[Product Factory] Starting on port {PORT}")
    print(f"[Product Factory] DB: {DB_PATH}")
    print(f"[Product Factory] Templates: {len(PRODUCT_TEMPLATES)}")
    init_db()
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
