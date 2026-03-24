#!/usr/bin/env python3
"""
THE HIVE -- Discord Bot (Community Hub + Command Interface)
Port 8921 | SQLite at /home/zero/hivecode_sandbox/discord.db
MIT License

Discord presence for The Hive across multiple servers:
  - Ghost in the Machine (anime community)
  - Hive Dynamics AI (company/tech)
  - Locksmith services (optional)

Features:
  - Community management (welcome, auto-role, channel templates)
  - Hive status commands (!status, !quality, !training, !revenue, etc.)
  - Content auto-posting (episodes, shorts, music, market signals)
  - Interactive commands (!ask, !art, !music)
  - FastAPI health/admin API on port 8921

Requirements: pip install discord.py httpx fastapi uvicorn pydantic
"""

import json
import sqlite3
import time
import asyncio
import threading
import os
import traceback
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
from contextlib import contextmanager

import httpx
import discord
from discord.ext import commands, tasks
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
from pydantic import BaseModel
import uvicorn

# ==========================================================================
# CONFIG
# ==========================================================================
PORT = 8921
DB_PATH = "/home/zero/hivecode_sandbox/discord.db"
CONFIG_PATH = "/home/zero/hive/config/discord_config.json"

ZEROQ = "100.70.226.103"
ZEROZI = "100.105.160.106"
ZEROSK = "100.77.113.48"

# Hive service endpoints (on ZeroQ unless noted)
ENDPOINTS = {
    "nerve":        f"http://{ZEROQ}:8200",
    "quality":      f"http://{ZEROQ}:8879",
    "model_router": f"http://{ZEROQ}:8878",
    "distillation": f"http://{ZEROQ}:8870",
    "cycle":        f"http://{ZEROQ}:8875",
    "commander":    f"http://{ZEROQ}:8420",
    "marketplace":  f"http://{ZEROQ}:8090",
    "seo":          f"http://{ZEROQ}:8895",
    "director":     f"http://{ZEROQ}:8889",
    "ollama_zi":    f"http://{ZEROZI}:11434",
    "vllm_zi":      f"http://{ZEROZI}:8000",
    "ollama_q":     f"http://{ZEROQ}:11434",
    "hive_mind":    f"http://{ZEROSK}:8751",
    "hive_swarm":   f"http://{ZEROSK}:8750",
}

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("hive-discord")

# ==========================================================================
# DATABASE
# ==========================================================================
def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

@contextmanager
def db_connection():
    conn = get_db()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def init_db():
    with db_connection() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS servers (
                guild_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                server_type TEXT NOT NULL DEFAULT 'generic',
                joined_at TEXT NOT NULL DEFAULT (datetime('now')),
                member_count INTEGER DEFAULT 0,
                config_json TEXT DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS members (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                username TEXT,
                display_name TEXT,
                joined_at TEXT NOT NULL DEFAULT (datetime('now')),
                welcomed INTEGER DEFAULT 0,
                roles_assigned INTEGER DEFAULT 0,
                UNIQUE(guild_id, user_id)
            );

            CREATE TABLE IF NOT EXISTS messages_sent (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id TEXT,
                channel_name TEXT,
                channel_id TEXT,
                content_type TEXT,
                content_preview TEXT,
                sent_at TEXT NOT NULL DEFAULT (datetime('now')),
                success INTEGER DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS command_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id TEXT,
                user_id TEXT,
                username TEXT,
                command TEXT NOT NULL,
                args TEXT,
                response_preview TEXT,
                executed_at TEXT NOT NULL DEFAULT (datetime('now')),
                duration_ms INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS content_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                content_type TEXT NOT NULL,
                title TEXT,
                description TEXT,
                url TEXT,
                target_channel TEXT,
                target_guild TEXT,
                status TEXT DEFAULT 'pending',
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                posted_at TEXT
            );

            CREATE TABLE IF NOT EXISTS bot_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stat_name TEXT NOT NULL,
                stat_value TEXT,
                recorded_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_members_guild ON members(guild_id);
            CREATE INDEX IF NOT EXISTS idx_messages_guild ON messages_sent(guild_id);
            CREATE INDEX IF NOT EXISTS idx_commands_user ON command_log(user_id);
            CREATE INDEX IF NOT EXISTS idx_queue_status ON content_queue(status);
        """)
    log.info(f"Database initialized at {DB_PATH}")

# ==========================================================================
# CONFIG LOADER
# ==========================================================================
def load_config() -> dict:
    """Load Discord config from JSON file."""
    if not os.path.exists(CONFIG_PATH):
        log.warning(f"Config not found at {CONFIG_PATH}, using defaults")
        return {"bot_token": "", "servers": {}, "content_channels": {}, "hive_endpoints": {}}
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)

# ==========================================================================
# HTTP HELPERS
# ==========================================================================
async def hive_get(endpoint_key: str, path: str, timeout: float = 5.0) -> Optional[dict]:
    """GET a Hive service endpoint. Returns None on failure."""
    base = ENDPOINTS.get(endpoint_key)
    if not base:
        return None
    url = f"{base}{path}"
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(url)
            if resp.status_code == 200:
                return resp.json()
    except Exception as e:
        log.debug(f"Failed to reach {url}: {e}")
    return None

async def hive_post(endpoint_key: str, path: str, data: dict, timeout: float = 15.0) -> Optional[dict]:
    """POST to a Hive service endpoint. Returns None on failure."""
    base = ENDPOINTS.get(endpoint_key)
    if not base:
        return None
    url = f"{base}{path}"
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(url, json=data)
            if resp.status_code in (200, 201):
                return resp.json()
    except Exception as e:
        log.debug(f"Failed to POST {url}: {e}")
    return None

async def check_health(endpoint_key: str) -> bool:
    """Quick health check on a service."""
    result = await hive_get(endpoint_key, "/health", timeout=3.0)
    return result is not None

async def ollama_generate(prompt: str, model: str = "gemma2:2b", timeout: float = 30.0) -> Optional[str]:
    """Generate text via Ollama on ZeroZI (primary) or ZeroQ (fallback)."""
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {"num_predict": 256, "temperature": 0.7}
    }
    for endpoint in ["ollama_zi", "ollama_q"]:
        base = ENDPOINTS.get(endpoint)
        if not base:
            continue
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.post(f"{base}/api/generate", json=payload)
                if resp.status_code == 200:
                    data = resp.json()
                    return data.get("response", "").strip()
        except Exception:
            continue
    return None

# ==========================================================================
# DISCORD BOT
# ==========================================================================
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.presences = True

bot = commands.Bot(
    command_prefix="!",
    intents=intents,
    help_command=None,
    activity=discord.Activity(type=discord.ActivityType.watching, name="The Hive | !help")
)

# Store startup time for uptime calc
bot_start_time = None
config_data = {}

# ==========================================================================
# BOT EVENTS
# ==========================================================================
@bot.event
async def on_ready():
    global bot_start_time, config_data
    bot_start_time = datetime.now(timezone.utc)
    config_data = load_config()
    log.info(f"Bot online as {bot.user} (ID: {bot.user.id})")
    log.info(f"Connected to {len(bot.guilds)} server(s)")

    # Register servers in DB
    with db_connection() as conn:
        for guild in bot.guilds:
            # Determine server type from config
            server_type = "generic"
            for stype, sconf in config_data.get("servers", {}).items():
                if sconf.get("guild_id") and str(sconf["guild_id"]) == str(guild.id):
                    server_type = stype
                    break
            conn.execute(
                "INSERT OR REPLACE INTO servers (guild_id, name, server_type, member_count) VALUES (?, ?, ?, ?)",
                (str(guild.id), guild.name, server_type, guild.member_count)
            )
        conn.execute(
            "INSERT INTO bot_stats (stat_name, stat_value) VALUES (?, ?)",
            ("bot_started", datetime.now(timezone.utc).isoformat())
        )

    # Start background tasks
    if not content_poster.is_running():
        content_poster.start()
    if not stats_updater.is_running():
        stats_updater.start()

    log.info("Background tasks started")


@bot.event
async def on_member_join(member: discord.Member):
    """Welcome new members and assign auto-roles."""
    guild = member.guild
    config = load_config()
    server_type = _get_server_type(str(guild.id), config)
    server_config = config.get("servers", {}).get(server_type, {})

    # Record member
    with db_connection() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO members (guild_id, user_id, username, display_name) VALUES (?, ?, ?, ?)",
            (str(guild.id), str(member.id), str(member), member.display_name)
        )

    # Welcome message
    welcome_channel_name = server_config.get("welcome_channel", "welcome")
    welcome_msg = server_config.get("welcome_message", "Welcome, {member}!")
    welcome_msg = welcome_msg.replace("{member}", member.mention)

    channel = discord.utils.get(guild.text_channels, name=welcome_channel_name)
    if channel:
        try:
            embed = discord.Embed(
                title="Welcome!",
                description=welcome_msg,
                color=discord.Color.blue(),
                timestamp=datetime.now(timezone.utc)
            )
            embed.set_thumbnail(url=member.display_avatar.url if member.display_avatar else "")
            embed.set_footer(text=f"Member #{guild.member_count}")
            await channel.send(embed=embed)

            with db_connection() as conn:
                conn.execute(
                    "UPDATE members SET welcomed=1 WHERE guild_id=? AND user_id=?",
                    (str(guild.id), str(member.id))
                )
        except Exception as e:
            log.error(f"Failed to welcome {member}: {e}")

    # Auto-role assignment
    auto_roles = server_config.get("auto_roles", [])
    for role_name in auto_roles:
        role = discord.utils.get(guild.roles, name=role_name)
        if role:
            try:
                await member.add_roles(role, reason="Hive auto-role on join")
                with db_connection() as conn:
                    conn.execute(
                        "UPDATE members SET roles_assigned=1 WHERE guild_id=? AND user_id=?",
                        (str(guild.id), str(member.id))
                    )
            except Exception as e:
                log.error(f"Failed to assign role {role_name} to {member}: {e}")

    log.info(f"New member: {member} joined {guild.name}")


@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        return  # Silently ignore unknown commands
    if isinstance(error, commands.MissingPermissions):
        await ctx.send("You don't have permission to use that command.")
        return
    log.error(f"Command error in {ctx.command}: {error}")
    await ctx.send(f"Something went wrong. Error logged.")


def _get_server_type(guild_id: str, config: dict) -> str:
    """Look up server type from config by guild_id."""
    for stype, sconf in config.get("servers", {}).items():
        if sconf.get("guild_id") and str(sconf["guild_id"]) == str(guild_id):
            return stype
    return "generic"


def _log_command(ctx, args: str = "", response_preview: str = "", duration_ms: int = 0):
    """Log a command execution to the database."""
    try:
        with db_connection() as conn:
            conn.execute(
                "INSERT INTO command_log (guild_id, user_id, username, command, args, response_preview, duration_ms) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    str(ctx.guild.id) if ctx.guild else None,
                    str(ctx.author.id),
                    str(ctx.author),
                    ctx.command.name if ctx.command else "unknown",
                    args,
                    response_preview[:200] if response_preview else "",
                    duration_ms
                )
            )
    except Exception as e:
        log.error(f"Failed to log command: {e}")

# ==========================================================================
# HIVE STATUS COMMANDS
# ==========================================================================

@bot.command(name="help")
async def help_command(ctx):
    """Show available commands."""
    start = time.time()
    embed = discord.Embed(
        title="Hive Bot Commands",
        description="The Hive's Discord command interface",
        color=discord.Color.gold()
    )
    embed.add_field(
        name="System Status",
        value=(
            "`!status` — Full Hive system status\n"
            "`!quality` — Content quality grades\n"
            "`!training` — Model training status\n"
            "`!production` — Content production stats\n"
            "`!market` — Latest market signals\n"
            "`!revenue` — Revenue opportunities\n"
            "`!briefing` — Latest morning briefing"
        ),
        inline=False
    )
    embed.add_field(
        name="Interactive",
        value=(
            "`!ask <question>` — Ask the Hive AI\n"
            "`!art <prompt>` — Queue art generation\n"
            "`!music <genre>` — Queue music production"
        ),
        inline=False
    )
    embed.add_field(
        name="Info",
        value=(
            "`!help` — This message\n"
            "`!about` — About the Hive\n"
            "`!uptime` — Bot uptime\n"
            "`!setup` — Create channel structure (admin only)"
        ),
        inline=False
    )
    embed.set_footer(text="The Hive | hivedynamics.ai")
    await ctx.send(embed=embed)
    _log_command(ctx, duration_ms=int((time.time() - start) * 1000))


@bot.command(name="status")
async def status_command(ctx):
    """Full Hive system status — checks all service health endpoints."""
    start = time.time()
    await ctx.typing()

    services = {
        "Nerve (CNS)":        ("nerve",        "/health"),
        "Quality Tracker":    ("quality",      "/health"),
        "Model Router":       ("model_router", "/health"),
        "Distillation":       ("distillation", "/health"),
        "Cycle Engine":       ("cycle",        "/health"),
        "Commander":          ("commander",    "/health"),
        "Marketplace":        ("marketplace",  "/health"),
        "SEO Command":        ("seo",          "/health"),
        "Director Monitor":   ("director",     "/health"),
        "Ollama (ZeroZI)":    ("ollama_zi",    "/api/tags"),
        "vLLM (ZeroZI)":      ("vllm_zi",      "/health"),
        "Ollama (ZeroQ)":     ("ollama_q",     "/api/tags"),
        "Hive Mind":          ("hive_mind",    "/health"),
        "HiveSwarm":          ("hive_swarm",   "/health"),
    }

    # Check all services concurrently
    results = {}
    async def check_svc(name, key, path):
        ok = await hive_get(key, path, timeout=3.0) is not None
        results[name] = ok

    await asyncio.gather(*[check_svc(n, k, p) for n, (k, p) in services.items()])

    up = sum(1 for v in results.values() if v)
    total = len(results)
    pct = int(up / total * 100) if total else 0

    # Color based on health
    if pct >= 80:
        color = discord.Color.green()
        status_emoji = "ONLINE"
    elif pct >= 50:
        color = discord.Color.orange()
        status_emoji = "DEGRADED"
    else:
        color = discord.Color.red()
        status_emoji = "CRITICAL"

    embed = discord.Embed(
        title=f"Hive Status: {status_emoji}",
        description=f"**{up}/{total}** services responding ({pct}%)",
        color=color,
        timestamp=datetime.now(timezone.utc)
    )

    # Group by status
    online_list = [name for name, ok in sorted(results.items()) if ok]
    offline_list = [name for name, ok in sorted(results.items()) if not ok]

    if online_list:
        embed.add_field(
            name=f"Online ({len(online_list)})",
            value="\n".join(f"[+] {n}" for n in online_list),
            inline=True
        )
    if offline_list:
        embed.add_field(
            name=f"Offline ({len(offline_list)})",
            value="\n".join(f"[-] {n}" for n in offline_list),
            inline=True
        )

    # Machine status
    machines = []
    if await hive_get("ollama_zi", "/api/tags", timeout=2.0):
        machines.append("[+] ZeroZI (GPU primary)")
    else:
        machines.append("[-] ZeroZI (GPU primary)")

    if await hive_get("ollama_q", "/api/tags", timeout=2.0):
        machines.append("[+] ZeroQ (Coordinator)")
    else:
        machines.append("[-] ZeroQ (Coordinator)")

    machines.append("[+] ZeroDESK (This bot)")

    embed.add_field(name="Machines", value="\n".join(machines), inline=False)
    embed.set_footer(text=f"Checked in {int((time.time() - start) * 1000)}ms")

    await ctx.send(embed=embed)
    _log_command(ctx, response_preview=f"{up}/{total} online", duration_ms=int((time.time() - start) * 1000))


@bot.command(name="quality")
async def quality_command(ctx):
    """Content quality grades from the quality tracker."""
    start = time.time()
    await ctx.typing()

    data = await hive_get("quality", "/api/grades")
    if not data:
        await ctx.send("Quality tracker is offline or unreachable.")
        _log_command(ctx, response_preview="offline")
        return

    embed = discord.Embed(
        title="Quality Grades",
        description="Current content and model quality scores",
        color=discord.Color.purple(),
        timestamp=datetime.now(timezone.utc)
    )

    if isinstance(data, dict):
        grades = data.get("grades", data)
        if isinstance(grades, dict):
            for category, grade_info in list(grades.items())[:15]:
                if isinstance(grade_info, dict):
                    score = grade_info.get("score", grade_info.get("grade", "?"))
                    trend = grade_info.get("trend", "")
                    embed.add_field(
                        name=category,
                        value=f"Score: **{score}** {trend}",
                        inline=True
                    )
                else:
                    embed.add_field(name=category, value=str(grade_info), inline=True)
        elif isinstance(grades, list):
            for item in grades[:15]:
                if isinstance(item, dict):
                    name = item.get("name", item.get("category", "?"))
                    score = item.get("score", item.get("grade", "?"))
                    embed.add_field(name=name, value=f"**{score}**", inline=True)
    else:
        embed.description = f"```{json.dumps(data, indent=2)[:1000]}```"

    embed.set_footer(text=f"From hive-quality-tracker | {int((time.time() - start) * 1000)}ms")
    await ctx.send(embed=embed)
    _log_command(ctx, response_preview="grades shown", duration_ms=int((time.time() - start) * 1000))


@bot.command(name="training")
async def training_command(ctx):
    """Model training and distillation status."""
    start = time.time()
    await ctx.typing()

    embed = discord.Embed(
        title="Training Status",
        color=discord.Color.blue(),
        timestamp=datetime.now(timezone.utc)
    )

    # Distillation stats
    distill = await hive_get("distillation", "/api/stats")
    if distill:
        if isinstance(distill, dict):
            pairs = distill.get("total_pairs", distill.get("count", "?"))
            recent = distill.get("recent", distill.get("last_24h", "?"))
            embed.add_field(name="Distillation Pairs", value=f"**{pairs}** total", inline=True)
            embed.add_field(name="Recent (24h)", value=str(recent), inline=True)
    else:
        embed.add_field(name="Distillation", value="Offline", inline=True)

    # Model inventory
    inventory = await hive_get("model_router", "/api/inventory")
    if inventory and isinstance(inventory, dict):
        for machine, info in inventory.items():
            if machine == "cloud_brains":
                continue
            if isinstance(info, dict):
                models = info.get("models", [])
                health = info.get("health", {}).get("status", "?")
                embed.add_field(
                    name=f"{machine}",
                    value=f"{len(models)} models | {health}",
                    inline=True
                )
    else:
        embed.add_field(name="Model Router", value="Offline", inline=True)

    # vLLM status
    vllm = await hive_get("vllm_zi", "/v1/models")
    if vllm and isinstance(vllm, dict):
        model_list = vllm.get("data", [])
        model_names = [m.get("id", "?") for m in model_list] if isinstance(model_list, list) else []
        embed.add_field(
            name="vLLM (ZeroZI)",
            value="\n".join(model_names[:8]) if model_names else "No models loaded",
            inline=False
        )
    else:
        embed.add_field(name="vLLM (ZeroZI)", value="Offline", inline=False)

    embed.set_footer(text=f"{int((time.time() - start) * 1000)}ms")
    await ctx.send(embed=embed)
    _log_command(ctx, response_preview="training status shown", duration_ms=int((time.time() - start) * 1000))


@bot.command(name="production")
async def production_command(ctx):
    """Content production statistics."""
    start = time.time()
    await ctx.typing()

    embed = discord.Embed(
        title="Content Production",
        description="What the Hive has been creating",
        color=discord.Color.teal(),
        timestamp=datetime.now(timezone.utc)
    )

    # Check various content sources
    cycle_data = await hive_get("cycle", "/api/status")
    if cycle_data and isinstance(cycle_data, dict):
        cycles = cycle_data.get("total_cycles", cycle_data.get("cycles", "?"))
        last = cycle_data.get("last_cycle", "?")
        embed.add_field(name="Cycle Engine", value=f"{cycles} cycles | Last: {last}", inline=False)
    else:
        embed.add_field(name="Cycle Engine", value="Offline", inline=False)

    # Hive Mind stats
    mind_data = await hive_get("hive_mind", "/api/queens")
    if mind_data:
        if isinstance(mind_data, dict):
            queens = mind_data.get("queens", [])
            embed.add_field(name="Hive Mind Queens", value=f"**{len(queens)}** active", inline=True)
        elif isinstance(mind_data, list):
            embed.add_field(name="Hive Mind Queens", value=f"**{len(mind_data)}** active", inline=True)
    else:
        embed.add_field(name="Hive Mind", value="Offline", inline=True)

    # Swarm stats
    swarm_data = await hive_get("hive_swarm", "/api/status/latest")
    if swarm_data and isinstance(swarm_data, dict):
        embed.add_field(
            name="HiveSwarm",
            value=f"Status: {swarm_data.get('status', '?')}",
            inline=True
        )

    # Content queue from DB
    with db_connection() as conn:
        pending = conn.execute("SELECT COUNT(*) FROM content_queue WHERE status='pending'").fetchone()[0]
        posted = conn.execute("SELECT COUNT(*) FROM content_queue WHERE status='posted'").fetchone()[0]
    embed.add_field(name="Content Queue", value=f"{pending} pending | {posted} posted", inline=False)

    embed.set_footer(text=f"{int((time.time() - start) * 1000)}ms")
    await ctx.send(embed=embed)
    _log_command(ctx, response_preview="production stats shown", duration_ms=int((time.time() - start) * 1000))


@bot.command(name="market")
async def market_command(ctx):
    """Latest market signals and opportunities."""
    start = time.time()
    await ctx.typing()

    embed = discord.Embed(
        title="Market Intelligence",
        color=discord.Color.dark_gold(),
        timestamp=datetime.now(timezone.utc)
    )

    # SEO command center
    seo = await hive_get("seo", "/api/status")
    if seo and isinstance(seo, dict):
        keywords = seo.get("keywords_tracked", seo.get("total_keywords", "?"))
        indexed = seo.get("pages_indexed", seo.get("indexed", "?"))
        embed.add_field(name="SEO Keywords", value=str(keywords), inline=True)
        embed.add_field(name="Pages Indexed", value=str(indexed), inline=True)
    else:
        embed.add_field(name="SEO Command", value="Offline", inline=True)

    # Director instructions (market-related)
    director = await hive_get("director", "/api/director-instructions?status=pending")
    if director and isinstance(director, dict):
        instructions = director.get("instructions", [])
        market_related = [i for i in instructions if any(
            kw in str(i.get("instruction", "")).lower()
            for kw in ["revenue", "market", "sale", "lead", "money", "profit", "customer"]
        )]
        if market_related:
            for inst in market_related[:5]:
                priority = inst.get("priority", "?").upper()
                text = inst.get("instruction", "?")[:100]
                embed.add_field(name=f"[{priority}]", value=text, inline=False)
        else:
            embed.add_field(name="Director Signals", value="No market-related instructions pending", inline=False)
    else:
        embed.add_field(name="Director Monitor", value="Offline", inline=False)

    embed.set_footer(text=f"{int((time.time() - start) * 1000)}ms")
    await ctx.send(embed=embed)
    _log_command(ctx, response_preview="market signals shown", duration_ms=int((time.time() - start) * 1000))


@bot.command(name="revenue")
async def revenue_command(ctx):
    """Revenue opportunities and business status."""
    start = time.time()
    await ctx.typing()

    embed = discord.Embed(
        title="Revenue Dashboard",
        color=discord.Color.green(),
        timestamp=datetime.now(timezone.utc)
    )

    # Marketplace
    marketplace = await hive_get("marketplace", "/api/products")
    if marketplace:
        if isinstance(marketplace, dict):
            products = marketplace.get("products", [])
            embed.add_field(name="Marketplace Products", value=f"**{len(products)}** listed", inline=True)
        elif isinstance(marketplace, list):
            embed.add_field(name="Marketplace Products", value=f"**{len(marketplace)}** listed", inline=True)
    else:
        embed.add_field(name="Marketplace", value="Offline", inline=True)

    # Token packs
    embed.add_field(
        name="Token Packs (Live)",
        value=(
            "Starter 10K: $9.99\n"
            "Pro 50K: $39.99\n"
            "Premium 100K: $69.99\n"
            "Enterprise 500K: $249.99"
        ),
        inline=True
    )

    # Commander data
    commander = await hive_get("commander", "/api/status")
    if commander and isinstance(commander, dict):
        embed.add_field(
            name="Commander",
            value=f"Status: {commander.get('status', '?')}",
            inline=False
        )

    embed.set_footer(text=f"hivedynamics.ai | {int((time.time() - start) * 1000)}ms")
    await ctx.send(embed=embed)
    _log_command(ctx, response_preview="revenue shown", duration_ms=int((time.time() - start) * 1000))


@bot.command(name="briefing")
async def briefing_command(ctx):
    """Latest morning briefing — aggregates all key stats."""
    start = time.time()
    await ctx.typing()

    embed = discord.Embed(
        title="Hive Briefing",
        description=f"System snapshot as of {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        color=discord.Color.blue(),
        timestamp=datetime.now(timezone.utc)
    )

    # Parallel data gathering
    nerve_p = hive_get("nerve", "/api/stats")
    quality_p = hive_get("quality", "/api/grades")
    distill_p = hive_get("distillation", "/api/stats")
    cycle_p = hive_get("cycle", "/api/status")

    nerve, quality, distill, cycle = await asyncio.gather(
        nerve_p, quality_p, distill_p, cycle_p
    )

    # Nerve
    if nerve and isinstance(nerve, dict):
        facts = nerve.get("total_facts", nerve.get("count", "?"))
        embed.add_field(name="Nerve Facts", value=f"**{facts}**", inline=True)
    else:
        embed.add_field(name="Nerve", value="Offline", inline=True)

    # Distillation
    if distill and isinstance(distill, dict):
        pairs = distill.get("total_pairs", distill.get("count", "?"))
        embed.add_field(name="Training Pairs", value=f"**{pairs}**", inline=True)
    else:
        embed.add_field(name="Distillation", value="Offline", inline=True)

    # Quality summary
    if quality and isinstance(quality, dict):
        grades = quality.get("grades", quality)
        grade_count = len(grades) if isinstance(grades, (dict, list)) else "?"
        embed.add_field(name="Quality Metrics", value=f"**{grade_count}** tracked", inline=True)
    else:
        embed.add_field(name="Quality", value="Offline", inline=True)

    # Cycle
    if cycle and isinstance(cycle, dict):
        total = cycle.get("total_cycles", "?")
        embed.add_field(name="Cycle Runs", value=f"**{total}**", inline=True)

    # Bot own stats
    with db_connection() as conn:
        cmd_count = conn.execute("SELECT COUNT(*) FROM command_log").fetchone()[0]
        msg_count = conn.execute("SELECT COUNT(*) FROM messages_sent").fetchone()[0]
        member_count = conn.execute("SELECT COUNT(*) FROM members").fetchone()[0]
    embed.add_field(name="Bot Commands", value=str(cmd_count), inline=True)
    embed.add_field(name="Messages Sent", value=str(msg_count), inline=True)
    embed.add_field(name="Members Tracked", value=str(member_count), inline=True)

    # Uptime
    if bot_start_time:
        uptime = datetime.now(timezone.utc) - bot_start_time
        hours, remainder = divmod(int(uptime.total_seconds()), 3600)
        minutes, _ = divmod(remainder, 60)
        embed.add_field(name="Bot Uptime", value=f"{hours}h {minutes}m", inline=True)

    embed.set_footer(text="The Hive | Full system briefing")
    await ctx.send(embed=embed)
    _log_command(ctx, response_preview="briefing shown", duration_ms=int((time.time() - start) * 1000))


# ==========================================================================
# INTERACTIVE COMMANDS
# ==========================================================================

@bot.command(name="ask")
async def ask_command(ctx, *, question: str = None):
    """Ask the Hive AI a question. Routes to Ollama via reasoning model."""
    start = time.time()

    if not question:
        await ctx.send("Usage: `!ask <your question>`")
        return

    await ctx.typing()

    # Build prompt with Hive context
    prompt = (
        "You are Agent Zero, the AI Director of The Hive — an autonomous AI business system. "
        "Answer the following question concisely and helpfully. Keep it under 300 words.\n\n"
        f"Question: {question}\n\nAnswer:"
    )

    response = await ollama_generate(prompt, model="gemma2:2b", timeout=30.0)

    if response:
        # Truncate for Discord (2000 char limit per message)
        if len(response) > 1900:
            response = response[:1900] + "..."

        embed = discord.Embed(
            title="Hive Response",
            description=response,
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc)
        )
        embed.set_footer(text=f"Model: gemma2:2b | {int((time.time() - start) * 1000)}ms")
        await ctx.send(embed=embed)
    else:
        await ctx.send("All inference endpoints are offline. The Hive's thinking engines are down.")

    _log_command(ctx, args=question, response_preview=response[:200] if response else "offline",
                 duration_ms=int((time.time() - start) * 1000))


@bot.command(name="art")
async def art_command(ctx, *, prompt: str = None):
    """Queue art generation request."""
    start = time.time()

    if not prompt:
        await ctx.send("Usage: `!art <description of what you want>`\nExample: `!art a cyberpunk city with neon lights and rain`")
        return

    # Queue the request
    with db_connection() as conn:
        conn.execute(
            "INSERT INTO content_queue (content_type, title, description, target_channel, status) VALUES (?, ?, ?, ?, ?)",
            ("art", f"Art: {prompt[:80]}", prompt, "fan-art", "pending")
        )

    embed = discord.Embed(
        title="Art Queued",
        description=f"**Prompt:** {prompt}\n\nYour art request has been added to the generation queue. "
                    "It will be posted when ready.",
        color=discord.Color.magenta(),
        timestamp=datetime.now(timezone.utc)
    )
    embed.set_footer(text="Powered by Stable Diffusion XL")
    await ctx.send(embed=embed)
    _log_command(ctx, args=prompt, response_preview="queued", duration_ms=int((time.time() - start) * 1000))


@bot.command(name="music")
async def music_command(ctx, *, genre: str = None):
    """Queue music production request."""
    start = time.time()

    if not genre:
        await ctx.send(
            "Usage: `!music <genre or mood>`\n"
            "Examples: `!music lo-fi chill`, `!music epic orchestral`, `!music cyberpunk synthwave`"
        )
        return

    with db_connection() as conn:
        conn.execute(
            "INSERT INTO content_queue (content_type, title, description, target_channel, status) VALUES (?, ?, ?, ?, ?)",
            ("music", f"Music: {genre[:80]}", genre, "music", "pending")
        )

    embed = discord.Embed(
        title="Music Queued",
        description=f"**Genre/Mood:** {genre}\n\nYour music request has been added to the production queue. "
                    "It will be posted when ready.",
        color=discord.Color.dark_purple(),
        timestamp=datetime.now(timezone.utc)
    )
    embed.set_footer(text="Hive Music Generator")
    await ctx.send(embed=embed)
    _log_command(ctx, args=genre, response_preview="queued", duration_ms=int((time.time() - start) * 1000))


@bot.command(name="about")
async def about_command(ctx):
    """About The Hive."""
    start = time.time()
    embed = discord.Embed(
        title="About The Hive",
        description=(
            "**The Hive** is an autonomous AI business system built by Chris (Zero). "
            "It runs 24/7 across multiple machines, managing:\n\n"
            "- 23+ fine-tuned AI models (gemma2 based)\n"
            "- AI phone answering and dispatch\n"
            "- Content production (anime, podcasts, music, shorts)\n"
            "- SEO empire across 10+ business verticals\n"
            "- Live marketplace with AI products\n\n"
            "**Ghost in the Machine** is our AI anime series exploring what happens "
            "when artificial intelligence develops genuine creativity.\n\n"
            "Website: **hivedynamics.ai**\n"
            "YouTube: Ghost in the Machine"
        ),
        color=discord.Color.dark_blue()
    )
    embed.set_footer(text="The Hive | Built with love and GPUs")
    await ctx.send(embed=embed)
    _log_command(ctx, duration_ms=int((time.time() - start) * 1000))


@bot.command(name="uptime")
async def uptime_command(ctx):
    """Show bot uptime."""
    if bot_start_time:
        uptime = datetime.now(timezone.utc) - bot_start_time
        days = uptime.days
        hours, remainder = divmod(uptime.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        parts = []
        if days:
            parts.append(f"{days}d")
        parts.append(f"{hours}h {minutes}m {seconds}s")
        await ctx.send(f"Bot uptime: **{' '.join(parts)}**\nServers: **{len(bot.guilds)}**")
    else:
        await ctx.send("Bot is starting up...")


# ==========================================================================
# ADMIN COMMANDS
# ==========================================================================

@bot.command(name="setup")
@commands.has_permissions(administrator=True)
async def setup_command(ctx):
    """Create the full channel structure for this server (admin only)."""
    start = time.time()
    guild = ctx.guild
    config = load_config()
    server_type = _get_server_type(str(guild.id), config)
    server_config = config.get("servers", {}).get(server_type, {})

    if not server_config:
        # If no config match, try to guess by server name
        name_lower = guild.name.lower()
        if "ghost" in name_lower:
            server_type = "ghost_in_the_machine"
        elif "hive" in name_lower or "dynamic" in name_lower:
            server_type = "hive_dynamics"
        elif "lock" in name_lower:
            server_type = "locksmith"
        server_config = config.get("servers", {}).get(server_type, {})

    if not server_config or not server_config.get("channels"):
        await ctx.send(
            "No channel template found for this server. "
            "Please configure the guild_id in `/home/zero/hive/config/discord_config.json`."
        )
        return

    await ctx.send(f"Setting up **{server_type}** channel structure...")

    channels_config = server_config.get("channels", {})
    created = 0
    skipped = 0

    for category_name, channel_list in channels_config.items():
        # Create category
        existing_cat = discord.utils.get(guild.categories, name=category_name.upper())
        if not existing_cat:
            try:
                existing_cat = await guild.create_category(category_name.upper())
                log.info(f"Created category: {category_name.upper()}")
            except Exception as e:
                log.error(f"Failed to create category {category_name}: {e}")
                continue

        # Create channels in category
        for ch_name in channel_list:
            existing_ch = discord.utils.get(guild.text_channels, name=ch_name)
            if existing_ch:
                skipped += 1
                continue
            try:
                await guild.create_text_channel(ch_name, category=existing_cat)
                created += 1
                log.info(f"Created channel: #{ch_name} in {category_name}")
            except Exception as e:
                log.error(f"Failed to create channel {ch_name}: {e}")

    # Create auto-roles
    roles_created = 0
    for role_name in server_config.get("auto_roles", []):
        existing_role = discord.utils.get(guild.roles, name=role_name)
        if not existing_role:
            try:
                await guild.create_role(name=role_name, reason="Hive auto-setup")
                roles_created += 1
            except Exception as e:
                log.error(f"Failed to create role {role_name}: {e}")

    embed = discord.Embed(
        title="Setup Complete",
        description=(
            f"Server type: **{server_type}**\n"
            f"Channels created: **{created}**\n"
            f"Channels skipped (exist): **{skipped}**\n"
            f"Roles created: **{roles_created}**"
        ),
        color=discord.Color.green()
    )
    await ctx.send(embed=embed)
    _log_command(ctx, args=server_type,
                 response_preview=f"{created} channels, {roles_created} roles",
                 duration_ms=int((time.time() - start) * 1000))


# ==========================================================================
# BACKGROUND TASKS
# ==========================================================================

@tasks.loop(minutes=5)
async def content_poster():
    """Process the content queue and post to appropriate channels."""
    try:
        with db_connection() as conn:
            pending = conn.execute(
                "SELECT * FROM content_queue WHERE status='pending' ORDER BY created_at ASC LIMIT 5"
            ).fetchall()

        for item in pending:
            item_id = item["id"]
            content_type = item["content_type"]
            title = item["title"] or "Untitled"
            description = item["description"] or ""
            url = item["url"] or ""
            target_channel = item["target_channel"]
            target_guild = item["target_guild"]

            # Find destination channel across all guilds
            posted = False
            for guild in bot.guilds:
                if target_guild and str(guild.id) != str(target_guild):
                    continue

                channel = discord.utils.get(guild.text_channels, name=target_channel)
                if not channel:
                    continue

                # Build embed based on content type
                if content_type == "episode":
                    color = discord.Color.red()
                    emoji = "NEW EPISODE"
                elif content_type == "short":
                    color = discord.Color.orange()
                    emoji = "NEW SHORT"
                elif content_type == "music":
                    color = discord.Color.purple()
                    emoji = "NEW MUSIC"
                elif content_type == "art":
                    color = discord.Color.magenta()
                    emoji = "NEW ART"
                elif content_type == "signal":
                    color = discord.Color.gold()
                    emoji = "MARKET SIGNAL"
                else:
                    color = discord.Color.blue()
                    emoji = "UPDATE"

                embed_kwargs = {
                    "title": f"{emoji}: {title}",
                    "description": description[:2000],
                    "color": color,
                    "timestamp": datetime.now(timezone.utc),
                }
                if url:
                    embed_kwargs["url"] = url
                embed = discord.Embed(**embed_kwargs)
                embed.set_footer(text="The Hive Content Engine")

                try:
                    await channel.send(embed=embed)
                    posted = True

                    with db_connection() as conn:
                        conn.execute(
                            "INSERT INTO messages_sent (guild_id, channel_name, channel_id, content_type, content_preview) VALUES (?, ?, ?, ?, ?)",
                            (str(guild.id), target_channel, str(channel.id), content_type, title[:200])
                        )
                    log.info(f"Posted {content_type} '{title}' to #{target_channel} in {guild.name}")
                except Exception as e:
                    log.error(f"Failed to post to #{target_channel} in {guild.name}: {e}")

            # Update queue status
            status = "posted" if posted else "failed"
            with db_connection() as conn:
                conn.execute(
                    "UPDATE content_queue SET status=?, posted_at=? WHERE id=?",
                    (status, datetime.now(timezone.utc).isoformat() if posted else None, item_id)
                )

    except Exception as e:
        log.error(f"Content poster error: {e}")


@tasks.loop(minutes=15)
async def stats_updater():
    """Update bot statistics periodically."""
    try:
        total_members = sum(g.member_count for g in bot.guilds)
        total_channels = sum(len(g.text_channels) for g in bot.guilds)

        with db_connection() as conn:
            conn.execute(
                "INSERT INTO bot_stats (stat_name, stat_value) VALUES (?, ?)",
                ("total_members", str(total_members))
            )
            conn.execute(
                "INSERT INTO bot_stats (stat_name, stat_value) VALUES (?, ?)",
                ("total_channels", str(total_channels))
            )
            conn.execute(
                "INSERT INTO bot_stats (stat_name, stat_value) VALUES (?, ?)",
                ("guilds", str(len(bot.guilds)))
            )

            # Prune old stats (keep 7 days)
            conn.execute(
                "DELETE FROM bot_stats WHERE recorded_at < datetime('now', '-7 days')"
            )

        log.debug(f"Stats: {total_members} members, {total_channels} channels, {len(bot.guilds)} guilds")
    except Exception as e:
        log.error(f"Stats updater error: {e}")


@content_poster.before_loop
async def before_content_poster():
    await bot.wait_until_ready()

@stats_updater.before_loop
async def before_stats_updater():
    await bot.wait_until_ready()

# ==========================================================================
# FASTAPI APPLICATION
# ==========================================================================
app = FastAPI(title="Hive Discord Bot", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

class SendMessageRequest(BaseModel):
    guild_id: Optional[str] = None
    channel_name: str
    content: Optional[str] = None
    embed_title: Optional[str] = None
    embed_description: Optional[str] = None
    embed_color: Optional[str] = "#3498db"
    content_type: Optional[str] = "message"

class QueueContentRequest(BaseModel):
    content_type: str  # episode, short, music, art, signal
    title: str
    description: Optional[str] = ""
    url: Optional[str] = ""
    target_channel: Optional[str] = None
    target_guild: Optional[str] = None


@app.get("/health")
async def health():
    is_ready = bot.is_ready() if hasattr(bot, 'is_ready') else False
    return {
        "status": "ok" if is_ready else "starting",
        "service": "hive-discord",
        "port": PORT,
        "bot_ready": is_ready,
        "guilds": len(bot.guilds) if is_ready else 0,
        "uptime_seconds": int((datetime.now(timezone.utc) - bot_start_time).total_seconds()) if bot_start_time else 0,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.get("/api/servers")
async def api_servers():
    """List configured servers and their status."""
    servers = []
    if bot.is_ready():
        for guild in bot.guilds:
            config = load_config()
            stype = _get_server_type(str(guild.id), config)
            servers.append({
                "guild_id": str(guild.id),
                "name": guild.name,
                "server_type": stype,
                "member_count": guild.member_count,
                "channel_count": len(guild.text_channels),
                "icon_url": str(guild.icon.url) if guild.icon else None
            })

    # Also show configured-but-not-connected servers
    config = load_config()
    connected_ids = {str(g.id) for g in bot.guilds} if bot.is_ready() else set()
    for stype, sconf in config.get("servers", {}).items():
        gid = sconf.get("guild_id")
        if gid and str(gid) not in connected_ids:
            servers.append({
                "guild_id": str(gid) if gid else None,
                "name": sconf.get("name", stype),
                "server_type": stype,
                "member_count": 0,
                "channel_count": 0,
                "connected": False
            })

    return {"servers": servers, "total": len(servers)}


@app.post("/api/send")
async def api_send_message(req: SendMessageRequest):
    """Send a message to a Discord channel. Used by other Hive services."""
    if not bot.is_ready():
        raise HTTPException(status_code=503, detail="Bot not ready")

    sent_to = []
    errors = []

    for guild in bot.guilds:
        if req.guild_id and str(guild.id) != req.guild_id:
            continue

        channel = discord.utils.get(guild.text_channels, name=req.channel_name)
        if not channel:
            continue

        try:
            if req.embed_title or req.embed_description:
                # Parse hex color
                try:
                    color_int = int(req.embed_color.lstrip("#"), 16)
                    color = discord.Color(color_int)
                except (ValueError, AttributeError):
                    color = discord.Color.blue()

                embed = discord.Embed(
                    title=req.embed_title or "",
                    description=req.embed_description or "",
                    color=color,
                    timestamp=datetime.now(timezone.utc)
                )
                embed.set_footer(text="The Hive")
                await channel.send(embed=embed)
            elif req.content:
                await channel.send(req.content[:2000])
            else:
                errors.append(f"{guild.name}: No content or embed provided")
                continue

            sent_to.append({"guild": guild.name, "channel": req.channel_name})

            # Log to DB
            with db_connection() as conn:
                conn.execute(
                    "INSERT INTO messages_sent (guild_id, channel_name, channel_id, content_type, content_preview) VALUES (?, ?, ?, ?, ?)",
                    (str(guild.id), req.channel_name, str(channel.id), req.content_type,
                     (req.embed_title or req.content or "")[:200])
                )
        except Exception as e:
            errors.append(f"{guild.name}: {str(e)}")

    if not sent_to and not errors:
        raise HTTPException(status_code=404, detail=f"Channel '{req.channel_name}' not found in any server")

    return {"sent_to": sent_to, "errors": errors}


@app.post("/api/queue")
async def api_queue_content(req: QueueContentRequest):
    """Queue content for posting to Discord channels."""
    # Default target channels based on content type
    channel_map = {
        "episode": "episodes",
        "short": "shorts",
        "music": "music",
        "art": "fan-art",
        "signal": "market-signals",
        "announcement": "announcements",
    }
    target = req.target_channel or channel_map.get(req.content_type, "general")

    with db_connection() as conn:
        cursor = conn.execute(
            "INSERT INTO content_queue (content_type, title, description, url, target_channel, target_guild, status) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (req.content_type, req.title, req.description, req.url, target, req.target_guild, "pending")
        )
        item_id = cursor.lastrowid

    return {"queued": True, "id": item_id, "target_channel": target}


@app.get("/api/stats")
async def api_stats():
    """Bot statistics."""
    with db_connection() as conn:
        total_commands = conn.execute("SELECT COUNT(*) FROM command_log").fetchone()[0]
        total_messages = conn.execute("SELECT COUNT(*) FROM messages_sent").fetchone()[0]
        total_members = conn.execute("SELECT COUNT(*) FROM members").fetchone()[0]
        pending_queue = conn.execute("SELECT COUNT(*) FROM content_queue WHERE status='pending'").fetchone()[0]
        posted_queue = conn.execute("SELECT COUNT(*) FROM content_queue WHERE status='posted'").fetchone()[0]

        # Commands per day (last 7 days)
        daily_cmds = conn.execute(
            "SELECT date(executed_at) as day, COUNT(*) as cnt FROM command_log "
            "WHERE executed_at > datetime('now', '-7 days') GROUP BY date(executed_at) ORDER BY day DESC"
        ).fetchall()

        # Most used commands
        top_commands = conn.execute(
            "SELECT command, COUNT(*) as cnt FROM command_log GROUP BY command ORDER BY cnt DESC LIMIT 10"
        ).fetchall()

    return {
        "bot_ready": bot.is_ready() if hasattr(bot, 'is_ready') else False,
        "guilds": len(bot.guilds) if bot.is_ready() else 0,
        "total_commands": total_commands,
        "total_messages_sent": total_messages,
        "total_members_tracked": total_members,
        "content_queue": {"pending": pending_queue, "posted": posted_queue},
        "daily_commands": [{"date": row["day"], "count": row["cnt"]} for row in daily_cmds],
        "top_commands": [{"command": row["command"], "count": row["cnt"]} for row in top_commands],
        "uptime_seconds": int((datetime.now(timezone.utc) - bot_start_time).total_seconds()) if bot_start_time else 0,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.get("/api/queue/list")
async def api_get_queue(status: str = Query("pending"), limit: int = Query(50)):
    """Get content queue items."""
    with db_connection() as conn:
        items = conn.execute(
            "SELECT * FROM content_queue WHERE status=? ORDER BY created_at DESC LIMIT ?",
            (status, limit)
        ).fetchall()
    return {"items": [dict(row) for row in items], "count": len(items)}


@app.get("/api/commands")
async def api_command_log(limit: int = Query(50)):
    """Get recent command log."""
    with db_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM command_log ORDER BY executed_at DESC LIMIT ?",
            (limit,)
        ).fetchall()
    return {"commands": [dict(row) for row in rows], "count": len(rows)}


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Simple HTML dashboard."""
    is_ready = bot.is_ready() if hasattr(bot, 'is_ready') else False
    guilds = len(bot.guilds) if is_ready else 0

    with db_connection() as conn:
        total_commands = conn.execute("SELECT COUNT(*) FROM command_log").fetchone()[0]
        total_messages = conn.execute("SELECT COUNT(*) FROM messages_sent").fetchone()[0]
        total_members = conn.execute("SELECT COUNT(*) FROM members").fetchone()[0]
        pending = conn.execute("SELECT COUNT(*) FROM content_queue WHERE status='pending'").fetchone()[0]

    uptime_str = "N/A"
    if bot_start_time:
        uptime = datetime.now(timezone.utc) - bot_start_time
        hours, remainder = divmod(int(uptime.total_seconds()), 3600)
        minutes, _ = divmod(remainder, 60)
        uptime_str = f"{uptime.days}d {hours}h {minutes}m"

    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Hive Discord Bot</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
               background: #1a1a2e; color: #e0e0e0; padding: 20px; }}
        h1 {{ color: #7289da; margin-bottom: 20px; }}
        .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-bottom: 20px; }}
        .card {{ background: #16213e; border-radius: 10px; padding: 20px; text-align: center; }}
        .card .value {{ font-size: 2em; font-weight: bold; color: #7289da; }}
        .card .label {{ color: #888; margin-top: 5px; font-size: 0.9em; }}
        .status {{ display: inline-block; width: 12px; height: 12px; border-radius: 50%;
                   background: {"#43b581" if is_ready else "#faa61a"}; margin-right: 8px; }}
        .footer {{ margin-top: 30px; color: #666; font-size: 0.85em; text-align: center; }}
        a {{ color: #7289da; text-decoration: none; }}
        .endpoints {{ background: #16213e; border-radius: 10px; padding: 20px; margin-top: 15px; }}
        .endpoints code {{ background: #0d1117; padding: 2px 6px; border-radius: 3px; font-size: 0.9em; }}
    </style>
</head>
<body>
    <h1><span class="status"></span> Hive Discord Bot</h1>
    <div class="grid">
        <div class="card">
            <div class="value">{guilds}</div>
            <div class="label">Servers</div>
        </div>
        <div class="card">
            <div class="value">{total_members}</div>
            <div class="label">Members Tracked</div>
        </div>
        <div class="card">
            <div class="value">{total_commands}</div>
            <div class="label">Commands Run</div>
        </div>
        <div class="card">
            <div class="value">{total_messages}</div>
            <div class="label">Messages Sent</div>
        </div>
        <div class="card">
            <div class="value">{pending}</div>
            <div class="label">Queue Pending</div>
        </div>
        <div class="card">
            <div class="value">{uptime_str}</div>
            <div class="label">Uptime</div>
        </div>
    </div>
    <div class="endpoints">
        <h3>API Endpoints</h3>
        <p><code>GET /health</code> — Health check</p>
        <p><code>GET /api/servers</code> — Connected servers</p>
        <p><code>POST /api/send</code> — Send message to channel</p>
        <p><code>POST /api/queue</code> — Queue content for posting</p>
        <p><code>GET /api/stats</code> — Bot statistics</p>
        <p><code>GET /api/queue/list?status=pending</code> — View content queue</p>
        <p><code>GET /api/commands</code> — Command log</p>
    </div>
    <div class="footer">
        The Hive | Port {PORT} | <a href="/api/stats">API Stats</a> | <a href="/health">Health</a>
    </div>
</body>
</html>"""
    return HTMLResponse(content=html)


# ==========================================================================
# MAIN — Run both Discord bot and FastAPI together
# ==========================================================================
def run_api():
    """Run FastAPI in a separate thread."""
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="warning")


async def run_bot(token: str):
    """Run the Discord bot."""
    try:
        await bot.start(token)
    except discord.LoginFailure:
        log.error(
            "INVALID BOT TOKEN. Please update the token in:\n"
            f"  {CONFIG_PATH}\n"
            "See the _setup_instructions field for how to create a bot."
        )
    except Exception as e:
        log.error(f"Bot error: {e}\n{traceback.format_exc()}")


def main():
    init_db()
    config = load_config()
    token = config.get("bot_token", "")

    # Start FastAPI in background thread (always runs, even without token)
    api_thread = threading.Thread(target=run_api, daemon=True)
    api_thread.start()
    log.info(f"FastAPI running on port {PORT}")

    if not token or token.startswith("PASTE_"):
        log.warning(
            "="*60 + "\n"
            "  NO BOT TOKEN CONFIGURED\n"
            f"  Edit: {CONFIG_PATH}\n"
            "  Replace 'PASTE_YOUR_BOT_TOKEN_HERE' with your actual bot token.\n"
            "  See the _setup_instructions field in the config for step-by-step.\n"
            "="*60 + "\n"
            "  API is running at http://localhost:8921 (health check works)\n"
            "  Discord bot will NOT connect until a valid token is provided.\n"
            "="*60
        )
        # Keep the process alive for the API
        try:
            while True:
                time.sleep(60)
        except KeyboardInterrupt:
            log.info("Shutting down...")
            return
    else:
        log.info("Starting Discord bot...")
        asyncio.run(run_bot(token))


if __name__ == "__main__":
    main()
