#!/usr/bin/env python3
"""
THE HIVE — Cyber Division Agent
Port 8914 | SQLite at /home/zero/hivecode_sandbox/cyber.db
MIT License

Cybersecurity and CTF competition division:
  - CTF Competition Tracker (CTFtime.org)
  - Bug Bounty Scanner (HackerOne, Bugcrowd public listings)
  - Security Tools Library (MIT/Apache only)
  - Training Module (skill tracking, practice challenges)
  - Security Audit Service (sellable website scanner)

RULES:
  - DEFENSIVE security, CTF competitions, and authorized testing ONLY
  - No attacking systems without explicit authorization
  - Bug bounties are LEGAL and ENCOURAGED
  - CTF competitions are educational
  - Security audits require documented client consent
"""

import json
import sqlite3
import time
import threading
import os
import re
import hashlib
import traceback
import ssl
import socket
import html as html_lib
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any
from contextlib import contextmanager
from urllib.parse import urlparse, quote_plus

import httpx
from fastapi import FastAPI, BackgroundTasks, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
import uvicorn

# ==========================================================================
# CONFIG
# ==========================================================================
PORT = 8914
DB_PATH = "/home/zero/hivecode_sandbox/cyber.db"

OLLAMA_URL = "http://localhost:11434"
OLLAMA_MODEL = "gemma2:2b"

SCAN_INTERVAL_MINUTES = 60  # how often background scanners run
CTFTIME_API = "https://ctftime.org/api/v1"
CTFTIME_WEB = "https://ctftime.org"

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
# CTF CATEGORIES WE TRACK
# ==========================================================================
CTF_CATEGORIES = ["web", "crypto", "reverse_engineering", "forensics", "pwn", "misc"]

# ==========================================================================
# SECURITY TOOLS LIBRARY — MIT/Apache-2.0 ONLY
# ==========================================================================
SECURITY_TOOLS = [
    {
        "name": "nmap",
        "description": "Network scanner and host discovery. The gold standard for port scanning.",
        "license": "custom-open (Nmap Public Source License)",
        "category": "reconnaissance",
        "install": "sudo apt install nmap",
        "check_cmd": "nmap --version",
        "url": "https://nmap.org",
    },
    {
        "name": "nuclei",
        "description": "Fast vulnerability scanner with community templates. Template-based scanning for known CVEs.",
        "license": "MIT",
        "category": "vulnerability_scanning",
        "install": "go install github.com/projectdiscovery/nuclei/v3/cmd/nuclei@latest",
        "check_cmd": "nuclei -version",
        "url": "https://github.com/projectdiscovery/nuclei",
    },
    {
        "name": "subfinder",
        "description": "Subdomain discovery tool using passive sources.",
        "license": "MIT",
        "category": "reconnaissance",
        "install": "go install github.com/projectdiscovery/subfinder/v2/cmd/subfinder@latest",
        "check_cmd": "subfinder -version",
        "url": "https://github.com/projectdiscovery/subfinder",
    },
    {
        "name": "httpx",
        "description": "Fast HTTP probing tool. Checks live hosts, status codes, titles, tech stack.",
        "license": "MIT",
        "category": "reconnaissance",
        "install": "go install github.com/projectdiscovery/httpx/cmd/httpx@latest",
        "check_cmd": "httpx -version",
        "url": "https://github.com/projectdiscovery/httpx",
    },
    {
        "name": "ffuf",
        "description": "Fast web fuzzer for directory/parameter brute-forcing (authorized targets only).",
        "license": "MIT",
        "category": "fuzzing",
        "install": "go install github.com/ffuf/ffuf/v2@latest",
        "check_cmd": "ffuf -V",
        "url": "https://github.com/ffuf/ffuf",
    },
    {
        "name": "nikto",
        "description": "Web server scanner that checks for dangerous files, outdated software, and misconfigurations.",
        "license": "GPL",
        "category": "vulnerability_scanning",
        "install": "sudo apt install nikto",
        "check_cmd": "nikto -Version",
        "url": "https://github.com/sullo/nikto",
        "note": "GPL license — use but do not redistribute modified versions",
    },
    {
        "name": "testssl.sh",
        "description": "Tests TLS/SSL ciphers, protocols, and cryptographic flaws on any port.",
        "license": "GPL-2.0",
        "category": "ssl_analysis",
        "install": "git clone https://github.com/drwetter/testssl.sh.git",
        "check_cmd": "testssl.sh --version",
        "url": "https://github.com/drwetter/testssl.sh",
    },
    {
        "name": "gobuster",
        "description": "Directory/file and DNS brute-force tool written in Go.",
        "license": "Apache-2.0",
        "category": "fuzzing",
        "install": "go install github.com/OJ/gobuster/v3@latest",
        "check_cmd": "gobuster version",
        "url": "https://github.com/OJ/gobuster",
    },
    {
        "name": "katana",
        "description": "Next-generation web crawling and spidering framework.",
        "license": "MIT",
        "category": "reconnaissance",
        "install": "go install github.com/projectdiscovery/katana/cmd/katana@latest",
        "check_cmd": "katana -version",
        "url": "https://github.com/projectdiscovery/katana",
    },
    {
        "name": "wappalyzer-cli",
        "description": "Technology profiler — identifies CMS, frameworks, JS libraries on websites.",
        "license": "MIT",
        "category": "reconnaissance",
        "install": "npm install -g wappalyzer",
        "check_cmd": "wappalyzer --version",
        "url": "https://github.com/wappalyzer/wappalyzer",
    },
    {
        "name": "trufflehog",
        "description": "Finds leaked credentials and secrets in git repos, S3 buckets, etc.",
        "license": "AGPL-3.0",
        "category": "secret_scanning",
        "install": "brew install trufflehog (or download binary)",
        "check_cmd": "trufflehog --version",
        "url": "https://github.com/trufflesecurity/trufflehog",
        "note": "AGPL — use as tool only, do not embed in our services",
    },
    {
        "name": "amass",
        "description": "In-depth attack surface mapping and external asset discovery.",
        "license": "Apache-2.0",
        "category": "reconnaissance",
        "install": "go install github.com/owasp-amass/amass/v4/...@master",
        "check_cmd": "amass -version",
        "url": "https://github.com/owasp-amass/amass",
    },
    {
        "name": "dnsx",
        "description": "Fast multi-purpose DNS toolkit for running DNS queries.",
        "license": "MIT",
        "category": "reconnaissance",
        "install": "go install github.com/projectdiscovery/dnsx/cmd/dnsx@latest",
        "check_cmd": "dnsx -version",
        "url": "https://github.com/projectdiscovery/dnsx",
    },
    {
        "name": "sqlmap",
        "description": "Automatic SQL injection detection and exploitation (authorized targets only).",
        "license": "GPL-2.0",
        "category": "exploitation",
        "install": "pip install sqlmap",
        "check_cmd": "sqlmap --version",
        "url": "https://github.com/sqlmapproject/sqlmap",
        "note": "GPL — use as standalone tool only, authorized targets only",
    },
    {
        "name": "feroxbuster",
        "description": "Fast content discovery tool written in Rust. Recursive directory brute-forcing.",
        "license": "MIT",
        "category": "fuzzing",
        "install": "cargo install feroxbuster",
        "check_cmd": "feroxbuster --version",
        "url": "https://github.com/epi052/feroxbuster",
    },
]

# ==========================================================================
# SELLABLE SECURITY SERVICES
# ==========================================================================
SECURITY_SERVICES = [
    {
        "id": "basic_audit",
        "name": "Basic Website Security Audit",
        "price": "$99",
        "description": "Automated scan of HTTPS, security headers, SSL/TLS config, common misconfigurations. PDF report.",
        "turnaround": "24 hours",
        "includes": [
            "HTTPS enforcement check",
            "Security headers analysis (CSP, HSTS, X-Frame, etc.)",
            "SSL/TLS certificate validation",
            "Open port scan (top 100)",
            "DNS configuration review",
            "Cookie security flags",
            "Professional PDF report with remediation steps",
        ],
    },
    {
        "id": "full_audit",
        "name": "Full AI Security Audit",
        "price": "$199",
        "description": "Comprehensive automated security assessment with AI-powered analysis and prioritized remediation.",
        "turnaround": "48 hours",
        "includes": [
            "Everything in Basic Audit",
            "Technology stack fingerprinting",
            "Subdomain enumeration",
            "Directory discovery",
            "Known CVE checks (nuclei templates)",
            "Information disclosure detection",
            "AI-powered risk assessment and prioritization",
            "Executive summary + technical detail PDF",
        ],
    },
    {
        "id": "continuous_monitoring",
        "name": "Continuous Security Monitoring",
        "price": "$49/month",
        "description": "Weekly automated scans with alerts for new vulnerabilities, certificate expiry, and config drift.",
        "turnaround": "Ongoing",
        "includes": [
            "Weekly full security scan",
            "SSL certificate expiry monitoring",
            "New CVE detection for your tech stack",
            "Uptime monitoring",
            "Monthly security posture report",
            "Email alerts for critical findings",
        ],
    },
    {
        "id": "pentest_lite",
        "name": "Penetration Test Lite",
        "price": "$499",
        "description": "Authorized manual + automated penetration test focused on web application security.",
        "turnaround": "1 week",
        "includes": [
            "Everything in Full Audit",
            "OWASP Top 10 testing",
            "Authentication/authorization testing",
            "API endpoint security review",
            "Business logic testing",
            "Detailed remediation guidance",
            "30-minute consultation call",
            "Requires signed authorization agreement",
        ],
    },
]

# ==========================================================================
# BUG BOUNTY PLATFORMS — PUBLIC LISTING URLS
# ==========================================================================
BOUNTY_PLATFORMS = {
    "hackerone": {
        "name": "HackerOne",
        "directory_url": "https://hackerone.com/bug-bounty-programs",
        "api_url": "https://hackerone.com/programs/search?query=type:hackerone&sort=published_at:descending&page=",
        "base_url": "https://hackerone.com",
    },
    "bugcrowd": {
        "name": "Bugcrowd",
        "directory_url": "https://bugcrowd.com/programs",
        "api_url": "https://bugcrowd.com/programs.json",
        "base_url": "https://bugcrowd.com",
    },
}

# Skills we match against bounty programs
OUR_SKILLS = ["web", "api", "automation", "xss", "sqli", "idor", "ssrf", "misconfiguration"]

# ==========================================================================
# DATABASE
# ==========================================================================


def get_db() -> sqlite3.Connection:
    """Get a database connection with WAL mode."""
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def db_session():
    """Context manager for database operations."""
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
    """Initialize all database tables."""
    with db_session() as conn:
        # CTF events
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ctf_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ctftime_id TEXT UNIQUE,
                name TEXT NOT NULL,
                url TEXT,
                start_time TEXT,
                end_time TEXT,
                format TEXT,
                location TEXT,
                weight REAL DEFAULT 0,
                categories TEXT DEFAULT '[]',
                description TEXT,
                status TEXT DEFAULT 'upcoming',
                our_score INTEGER,
                our_rank INTEGER,
                total_teams INTEGER,
                notes TEXT,
                scraped_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            )
        """)

        # CTF writeups and learnings
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ctf_learnings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER REFERENCES ctf_events(id),
                challenge_name TEXT,
                category TEXT,
                difficulty TEXT,
                solved INTEGER DEFAULT 0,
                points INTEGER,
                writeup TEXT,
                techniques TEXT DEFAULT '[]',
                tools_used TEXT DEFAULT '[]',
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)

        # Bug bounty programs
        conn.execute("""
            CREATE TABLE IF NOT EXISTS bounty_programs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform TEXT NOT NULL,
                program_name TEXT NOT NULL,
                program_url TEXT,
                scope TEXT DEFAULT '[]',
                bounty_min REAL DEFAULT 0,
                bounty_max REAL DEFAULT 0,
                response_time TEXT,
                difficulty TEXT DEFAULT 'medium',
                skill_match TEXT DEFAULT '[]',
                match_score REAL DEFAULT 0,
                status TEXT DEFAULT 'active',
                notes TEXT,
                scraped_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                UNIQUE(platform, program_name)
            )
        """)

        # Bug bounty submissions and payouts
        conn.execute("""
            CREATE TABLE IF NOT EXISTS bounty_submissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                program_id INTEGER REFERENCES bounty_programs(id),
                title TEXT NOT NULL,
                severity TEXT,
                status TEXT DEFAULT 'submitted',
                payout REAL DEFAULT 0,
                submitted_at TEXT DEFAULT (datetime('now')),
                resolved_at TEXT,
                notes TEXT
            )
        """)

        # Security tools inventory
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tools_inventory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                machine TEXT NOT NULL,
                installed INTEGER DEFAULT 0,
                version TEXT,
                last_checked TEXT DEFAULT (datetime('now')),
                UNIQUE(name, machine)
            )
        """)

        # Skill tracking
        conn.execute("""
            CREATE TABLE IF NOT EXISTS skill_levels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT NOT NULL UNIQUE,
                level INTEGER DEFAULT 1,
                xp INTEGER DEFAULT 0,
                challenges_attempted INTEGER DEFAULT 0,
                challenges_solved INTEGER DEFAULT 0,
                last_practiced TEXT,
                notes TEXT,
                updated_at TEXT DEFAULT (datetime('now'))
            )
        """)

        # Practice challenges
        conn.execute("""
            CREATE TABLE IF NOT EXISTS practice_challenges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                name TEXT NOT NULL,
                url TEXT,
                category TEXT,
                difficulty TEXT DEFAULT 'medium',
                solved INTEGER DEFAULT 0,
                solution_notes TEXT,
                time_spent_minutes INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                UNIQUE(source, name)
            )
        """)

        # Security scan results
        conn.execute("""
            CREATE TABLE IF NOT EXISTS scan_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                target_url TEXT NOT NULL,
                scan_type TEXT DEFAULT 'basic',
                findings TEXT DEFAULT '[]',
                score REAL DEFAULT 0,
                grade TEXT DEFAULT 'F',
                report_summary TEXT,
                raw_data TEXT,
                requested_by TEXT,
                scanned_at TEXT DEFAULT (datetime('now'))
            )
        """)

        # Scan events / audit log
        conn.execute("""
            CREATE TABLE IF NOT EXISTS scan_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                action TEXT NOT NULL,
                detail TEXT,
                timestamp TEXT DEFAULT (datetime('now'))
            )
        """)

        # Initialize skill levels for all categories
        for cat in CTF_CATEGORIES:
            conn.execute(
                "INSERT OR IGNORE INTO skill_levels (category) VALUES (?)",
                (cat,),
            )

    print(f"[CyberDiv] Database initialized at {DB_PATH}")


# ==========================================================================
# FASTAPI APP
# ==========================================================================
app = FastAPI(
    title="THE HIVE — Cyber Division",
    description="Cybersecurity, CTF, Bug Bounty, and Security Audit division",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==========================================================================
# PYDANTIC MODELS
# ==========================================================================


class ScanRequest(BaseModel):
    url: str
    scan_type: str = "basic"
    requested_by: str = "anonymous"


class LearningEntry(BaseModel):
    event_id: Optional[int] = None
    challenge_name: str
    category: str
    difficulty: str = "medium"
    solved: bool = False
    points: int = 0
    writeup: str = ""
    techniques: List[str] = []
    tools_used: List[str] = []


class BountySubmission(BaseModel):
    program_id: int
    title: str
    severity: str = "medium"
    notes: str = ""


class SkillUpdate(BaseModel):
    category: str
    xp_gained: int = 0
    challenges_attempted: int = 0
    challenges_solved: int = 0


# ==========================================================================
# CTF SCANNER — Scrapes CTFtime.org
# ==========================================================================


async def fetch_upcoming_ctfs() -> List[Dict[str, Any]]:
    """Fetch upcoming CTF events from CTFtime.org API."""
    events = []
    now = int(time.time())
    # Fetch events for the next 30 days
    end = now + (30 * 86400)

    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            # CTFtime API v1 — public, no auth needed
            resp = await client.get(
                f"{CTFTIME_API}/events/",
                params={"limit": 50, "start": now, "finish": end},
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "application/json",
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                for ev in data:
                    event = {
                        "ctftime_id": str(ev.get("id", "")),
                        "name": ev.get("title", "Unknown CTF"),
                        "url": ev.get("url", ev.get("ctftime_url", "")),
                        "start_time": ev.get("start", ""),
                        "end_time": ev.get("finish", ""),
                        "format": ev.get("format", ""),
                        "location": ev.get("location", "Online"),
                        "weight": ev.get("weight", 0),
                        "description": ev.get("description", "")[:500],
                        "categories": json.dumps(_detect_categories(ev)),
                    }
                    events.append(event)
                print(f"[CyberDiv] Fetched {len(events)} upcoming CTFs from CTFtime API")
            else:
                print(f"[CyberDiv] CTFtime API returned {resp.status_code}, trying web scrape...")
                events = await _scrape_ctftime_web(client)
    except Exception as e:
        print(f"[CyberDiv] CTFtime API error: {e}, trying web scrape...")
        try:
            async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
                events = await _scrape_ctftime_web(client)
        except Exception as e2:
            print(f"[CyberDiv] CTFtime web scrape also failed: {e2}")

    return events


async def _scrape_ctftime_web(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    """Fallback: scrape CTFtime.org event listing page."""
    events = []
    resp = await client.get(
        f"{CTFTIME_WEB}/event/list/upcoming",
        headers=HEADERS,
    )
    if resp.status_code != 200:
        return events

    text = resp.text

    # Parse table rows — CTFtime has a table with class "table"
    # Extract event data from HTML
    event_pattern = re.compile(
        r'<a\s+href="/event/(\d+)"[^>]*>\s*([^<]+)</a>',
        re.IGNORECASE,
    )
    date_pattern = re.compile(
        r'<td[^>]*class="[^"]*"[^>]*>(\d{2}\s+\w+\s+\d{4}[^<]*)</td>',
        re.IGNORECASE,
    )

    matches = event_pattern.findall(text)
    for ctf_id, name in matches[:50]:
        events.append({
            "ctftime_id": ctf_id.strip(),
            "name": html_lib.unescape(name.strip()),
            "url": f"{CTFTIME_WEB}/event/{ctf_id.strip()}",
            "start_time": "",
            "end_time": "",
            "format": "Jeopardy",
            "location": "Online",
            "weight": 0,
            "description": "",
            "categories": json.dumps(["misc"]),
        })

    print(f"[CyberDiv] Scraped {len(events)} CTFs from CTFtime web")
    return events


def _detect_categories(event: dict) -> List[str]:
    """Detect CTF categories from event description and title."""
    text = f"{event.get('title', '')} {event.get('description', '')}".lower()
    found = []
    category_keywords = {
        "web": ["web", "xss", "sqli", "injection", "webapp", "http"],
        "crypto": ["crypto", "cipher", "rsa", "aes", "encryption", "hash"],
        "reverse_engineering": ["reverse", "binary", "disassem", "ghidra", "ida", "radare"],
        "forensics": ["forensic", "memory", "disk", "pcap", "wireshark", "steganography"],
        "pwn": ["pwn", "exploit", "buffer overflow", "rop", "shellcode", "heap"],
        "misc": ["misc", "trivia", "osint", "scripting", "programming"],
    }
    for cat, keywords in category_keywords.items():
        if any(kw in text for kw in keywords):
            found.append(cat)
    return found if found else ["misc"]


def store_ctf_events(events: List[Dict[str, Any]]):
    """Store CTF events in database."""
    stored = 0
    with db_session() as conn:
        for ev in events:
            try:
                conn.execute("""
                    INSERT INTO ctf_events (ctftime_id, name, url, start_time, end_time,
                        format, location, weight, categories, description, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'upcoming')
                    ON CONFLICT(ctftime_id) DO UPDATE SET
                        name=excluded.name, url=excluded.url,
                        start_time=excluded.start_time, end_time=excluded.end_time,
                        weight=excluded.weight, updated_at=datetime('now')
                """, (
                    ev["ctftime_id"], ev["name"], ev["url"], ev["start_time"],
                    ev["end_time"], ev["format"], ev["location"], ev["weight"],
                    ev["categories"], ev["description"],
                ))
                stored += 1
            except Exception as e:
                print(f"[CyberDiv] Error storing CTF {ev.get('name')}: {e}")
    print(f"[CyberDiv] Stored/updated {stored} CTF events")
    return stored


# ==========================================================================
# BUG BOUNTY SCANNER
# ==========================================================================


async def fetch_bounty_programs() -> List[Dict[str, Any]]:
    """Fetch public bug bounty programs from HackerOne and Bugcrowd."""
    programs = []

    async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
        # --- HackerOne public directory ---
        try:
            resp = await client.get(
                "https://hackerone.com/opportunities/all/search",
                params={
                    "ordering": "started_accepting_at+desc",
                    "type[]": "hackerone",
                    "limit": 50,
                },
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "application/json",
                },
            )
            if resp.status_code == 200:
                try:
                    data = resp.json()
                    for prog in data.get("data", data) if isinstance(data, dict) else data:
                        if isinstance(prog, dict):
                            attrs = prog.get("attributes", prog)
                            name = attrs.get("name", attrs.get("handle", "Unknown"))
                            programs.append({
                                "platform": "hackerone",
                                "program_name": name,
                                "program_url": f"https://hackerone.com/{attrs.get('handle', name)}",
                                "scope": json.dumps(attrs.get("targets", {}).get("in_scope", [])),
                                "bounty_min": attrs.get("min_bounty", 0) or 0,
                                "bounty_max": attrs.get("max_bounty", 0) or 0,
                                "response_time": str(attrs.get("average_response_time", "N/A")),
                                "difficulty": _estimate_difficulty(attrs),
                                "skill_match": json.dumps(_match_skills(attrs)),
                                "match_score": _calc_match_score(attrs),
                            })
                except (json.JSONDecodeError, TypeError):
                    pass
            print(f"[CyberDiv] HackerOne: fetched {sum(1 for p in programs if p['platform'] == 'hackerone')} programs")
        except Exception as e:
            print(f"[CyberDiv] HackerOne fetch error: {e}")

        # --- HackerOne fallback: scrape the directory page ---
        if not any(p["platform"] == "hackerone" for p in programs):
            try:
                resp = await client.get(
                    "https://hackerone.com/bug-bounty-programs",
                    headers=HEADERS,
                )
                if resp.status_code == 200:
                    # Extract program handles from the page
                    handles = re.findall(
                        r'href="/([a-zA-Z0-9_-]+)"[^>]*class="[^"]*program[^"]*"',
                        resp.text,
                    )
                    for handle in handles[:30]:
                        if handle not in ("", "sign_up", "users", "hacktivity"):
                            programs.append({
                                "platform": "hackerone",
                                "program_name": handle,
                                "program_url": f"https://hackerone.com/{handle}",
                                "scope": "[]",
                                "bounty_min": 0,
                                "bounty_max": 0,
                                "response_time": "N/A",
                                "difficulty": "medium",
                                "skill_match": json.dumps(["web"]),
                                "match_score": 0.5,
                            })
                    print(f"[CyberDiv] HackerOne web scrape: {len(handles[:30])} programs")
            except Exception as e:
                print(f"[CyberDiv] HackerOne web scrape error: {e}")

        # --- Bugcrowd public programs ---
        try:
            resp = await client.get(
                "https://bugcrowd.com/programs.json",
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "application/json",
                },
            )
            if resp.status_code == 200:
                try:
                    data = resp.json()
                    program_list = data if isinstance(data, list) else data.get("programs", [])
                    for prog in program_list[:50]:
                        if isinstance(prog, dict):
                            name = prog.get("name", prog.get("program_url", "Unknown"))
                            programs.append({
                                "platform": "bugcrowd",
                                "program_name": name,
                                "program_url": f"https://bugcrowd.com{prog.get('program_url', '')}",
                                "scope": json.dumps(prog.get("target_groups", [])),
                                "bounty_min": prog.get("min_rewards", 0) or 0,
                                "bounty_max": prog.get("max_rewards", 0) or 0,
                                "response_time": "N/A",
                                "difficulty": "medium",
                                "skill_match": json.dumps(["web", "api"]),
                                "match_score": 0.5,
                            })
                except (json.JSONDecodeError, TypeError):
                    pass
            print(f"[CyberDiv] Bugcrowd: fetched {sum(1 for p in programs if p['platform'] == 'bugcrowd')} programs")
        except Exception as e:
            print(f"[CyberDiv] Bugcrowd fetch error: {e}")

        # --- Bugcrowd fallback: scrape listing page ---
        if not any(p["platform"] == "bugcrowd" for p in programs):
            try:
                resp = await client.get(
                    "https://bugcrowd.com/programs",
                    headers=HEADERS,
                )
                if resp.status_code == 200:
                    prog_links = re.findall(
                        r'href="(/[a-zA-Z0-9_-]+)"[^>]*>([^<]{3,60})</a>',
                        resp.text,
                    )
                    seen = set()
                    for link, name in prog_links[:30]:
                        clean_name = html_lib.unescape(name.strip())
                        if clean_name not in seen and link.startswith("/") and len(link) > 2:
                            seen.add(clean_name)
                            programs.append({
                                "platform": "bugcrowd",
                                "program_name": clean_name,
                                "program_url": f"https://bugcrowd.com{link}",
                                "scope": "[]",
                                "bounty_min": 0,
                                "bounty_max": 0,
                                "response_time": "N/A",
                                "difficulty": "medium",
                                "skill_match": json.dumps(["web"]),
                                "match_score": 0.5,
                            })
                    print(f"[CyberDiv] Bugcrowd web scrape: {len(seen)} programs")
            except Exception as e:
                print(f"[CyberDiv] Bugcrowd web scrape error: {e}")

    return programs


def _estimate_difficulty(attrs: dict) -> str:
    """Estimate program difficulty from attributes."""
    max_bounty = attrs.get("max_bounty", 0) or 0
    if max_bounty > 10000:
        return "hard"
    elif max_bounty > 1000:
        return "medium"
    return "easy"


def _match_skills(attrs: dict) -> List[str]:
    """Match program scope against our skills."""
    scope_text = json.dumps(attrs).lower()
    matched = []
    for skill in OUR_SKILLS:
        if skill in scope_text:
            matched.append(skill)
    return matched if matched else ["web"]


def _calc_match_score(attrs: dict) -> float:
    """Calculate how well a program matches our capabilities (0-1)."""
    score = 0.3  # base score
    skills = _match_skills(attrs)
    score += len(skills) * 0.1
    # Prefer programs with reasonable bounties
    max_bounty = attrs.get("max_bounty", 0) or 0
    if 100 <= max_bounty <= 5000:
        score += 0.2  # sweet spot for beginners
    return min(score, 1.0)


def store_bounty_programs(programs: List[Dict[str, Any]]):
    """Store bounty programs in database."""
    stored = 0
    with db_session() as conn:
        for prog in programs:
            try:
                conn.execute("""
                    INSERT INTO bounty_programs (platform, program_name, program_url,
                        scope, bounty_min, bounty_max, response_time, difficulty,
                        skill_match, match_score)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(platform, program_name) DO UPDATE SET
                        program_url=excluded.program_url,
                        bounty_min=excluded.bounty_min,
                        bounty_max=excluded.bounty_max,
                        match_score=excluded.match_score,
                        updated_at=datetime('now')
                """, (
                    prog["platform"], prog["program_name"], prog["program_url"],
                    prog["scope"], prog["bounty_min"], prog["bounty_max"],
                    prog["response_time"], prog["difficulty"],
                    prog["skill_match"], prog["match_score"],
                ))
                stored += 1
            except Exception as e:
                print(f"[CyberDiv] Error storing bounty program {prog.get('program_name')}: {e}")
    print(f"[CyberDiv] Stored/updated {stored} bounty programs")
    return stored


# ==========================================================================
# SECURITY SCANNER — Passive/Header-based checks (NO exploitation)
# ==========================================================================

SECURITY_HEADERS = {
    "Strict-Transport-Security": {
        "weight": 15,
        "description": "HSTS — forces HTTPS connections",
        "recommendation": "Add header: Strict-Transport-Security: max-age=31536000; includeSubDomains",
    },
    "Content-Security-Policy": {
        "weight": 15,
        "description": "CSP — prevents XSS and injection attacks",
        "recommendation": "Implement a Content-Security-Policy header appropriate for your application",
    },
    "X-Content-Type-Options": {
        "weight": 10,
        "description": "Prevents MIME-type sniffing attacks",
        "recommendation": "Add header: X-Content-Type-Options: nosniff",
    },
    "X-Frame-Options": {
        "weight": 10,
        "description": "Prevents clickjacking via iframes",
        "recommendation": "Add header: X-Frame-Options: DENY (or SAMEORIGIN)",
    },
    "X-XSS-Protection": {
        "weight": 5,
        "description": "Legacy XSS filter (deprecated but still useful)",
        "recommendation": "Add header: X-XSS-Protection: 1; mode=block",
    },
    "Referrer-Policy": {
        "weight": 5,
        "description": "Controls referrer information leakage",
        "recommendation": "Add header: Referrer-Policy: strict-origin-when-cross-origin",
    },
    "Permissions-Policy": {
        "weight": 5,
        "description": "Controls browser feature access (camera, microphone, etc.)",
        "recommendation": "Add header: Permissions-Policy: camera=(), microphone=(), geolocation=()",
    },
    "X-Permitted-Cross-Domain-Policies": {
        "weight": 3,
        "description": "Restricts Adobe Flash/PDF cross-domain data loading",
        "recommendation": "Add header: X-Permitted-Cross-Domain-Policies: none",
    },
}

DANGEROUS_HEADERS = [
    "Server",
    "X-Powered-By",
    "X-AspNet-Version",
    "X-AspNetMvc-Version",
    "X-Generator",
]


async def scan_website(url: str, scan_type: str = "basic") -> Dict[str, Any]:
    """
    Perform a PASSIVE security scan on a URL.
    Only checks publicly visible attributes — no exploitation.
    """
    findings = []
    score = 100  # start at 100, deduct for issues
    raw_data = {}

    # Normalize URL
    if not url.startswith("http"):
        url = f"https://{url}"

    parsed = urlparse(url)
    hostname = parsed.hostname or url

    try:
        async with httpx.AsyncClient(
            timeout=15,
            follow_redirects=True,
            verify=True,
        ) as client:
            # --- 1. HTTPS Check ---
            https_ok = url.startswith("https://")
            if not https_ok:
                findings.append({
                    "severity": "high",
                    "category": "transport",
                    "title": "No HTTPS",
                    "detail": "Site does not use HTTPS. All traffic is unencrypted.",
                    "recommendation": "Enable HTTPS with a valid TLS certificate (Let's Encrypt is free).",
                })
                score -= 20

            # --- 2. Fetch the page ---
            try:
                resp = await client.get(url, headers={"User-Agent": USER_AGENT})
                status_code = resp.status_code
                headers = dict(resp.headers)
                raw_data["status_code"] = status_code
                raw_data["headers"] = headers
                raw_data["final_url"] = str(resp.url)
            except httpx.ConnectError as e:
                return {
                    "target_url": url,
                    "scan_type": scan_type,
                    "findings": [{"severity": "critical", "category": "connectivity",
                                  "title": "Connection Failed",
                                  "detail": f"Could not connect to {url}: {e}",
                                  "recommendation": "Verify the URL is correct and the server is running."}],
                    "score": 0,
                    "grade": "F",
                    "report_summary": f"Could not connect to {url}",
                }
            except httpx.ConnectTimeout:
                return {
                    "target_url": url,
                    "scan_type": scan_type,
                    "findings": [{"severity": "critical", "category": "connectivity",
                                  "title": "Connection Timeout",
                                  "detail": f"Connection to {url} timed out after 15 seconds.",
                                  "recommendation": "Server may be down or blocking requests."}],
                    "score": 0,
                    "grade": "F",
                    "report_summary": f"Connection timeout for {url}",
                }

            # --- 3. HTTP -> HTTPS redirect ---
            if url.startswith("https://"):
                try:
                    http_url = url.replace("https://", "http://", 1)
                    http_resp = await client.get(http_url, follow_redirects=False)
                    if http_resp.status_code not in (301, 302, 307, 308):
                        findings.append({
                            "severity": "medium",
                            "category": "transport",
                            "title": "HTTP does not redirect to HTTPS",
                            "detail": "HTTP version of the site does not redirect to HTTPS.",
                            "recommendation": "Configure HTTP to 301 redirect to HTTPS.",
                        })
                        score -= 10
                except Exception:
                    pass  # if http port isn't open that's actually fine

            # --- 4. Security Headers ---
            for header_name, meta in SECURITY_HEADERS.items():
                header_lower = {k.lower(): v for k, v in headers.items()}
                if header_name.lower() not in header_lower:
                    findings.append({
                        "severity": "medium" if meta["weight"] >= 10 else "low",
                        "category": "headers",
                        "title": f"Missing {header_name}",
                        "detail": meta["description"],
                        "recommendation": meta["recommendation"],
                    })
                    score -= meta["weight"]

            # --- 5. Information Disclosure ---
            for hdr in DANGEROUS_HEADERS:
                hdr_lower = {k.lower(): v for k, v in headers.items()}
                val = hdr_lower.get(hdr.lower())
                if val:
                    findings.append({
                        "severity": "low",
                        "category": "information_disclosure",
                        "title": f"Server exposes {hdr} header",
                        "detail": f"{hdr}: {val} — reveals server technology.",
                        "recommendation": f"Remove or obfuscate the {hdr} header.",
                    })
                    score -= 3

            # --- 6. Cookie Security ---
            cookies = resp.headers.get_list("set-cookie") if hasattr(resp.headers, "get_list") else []
            if not cookies:
                # try alternate method
                cookies = [v for k, v in resp.headers.multi_items() if k.lower() == "set-cookie"]
            for cookie in cookies:
                cookie_name = cookie.split("=")[0].strip() if "=" in cookie else "unknown"
                cookie_lower = cookie.lower()
                if "secure" not in cookie_lower and url.startswith("https"):
                    findings.append({
                        "severity": "medium",
                        "category": "cookies",
                        "title": f"Cookie '{cookie_name}' missing Secure flag",
                        "detail": "Cookie can be sent over unencrypted HTTP.",
                        "recommendation": "Add the Secure flag to all cookies.",
                    })
                    score -= 5
                if "httponly" not in cookie_lower:
                    findings.append({
                        "severity": "medium",
                        "category": "cookies",
                        "title": f"Cookie '{cookie_name}' missing HttpOnly flag",
                        "detail": "Cookie accessible via JavaScript (XSS risk).",
                        "recommendation": "Add the HttpOnly flag to session cookies.",
                    })
                    score -= 5
                if "samesite" not in cookie_lower:
                    findings.append({
                        "severity": "low",
                        "category": "cookies",
                        "title": f"Cookie '{cookie_name}' missing SameSite attribute",
                        "detail": "Cookie may be vulnerable to CSRF attacks.",
                        "recommendation": "Add SameSite=Lax or SameSite=Strict to cookies.",
                    })
                    score -= 3

            # --- 7. SSL/TLS Certificate Check ---
            if parsed.scheme == "https":
                try:
                    cert_info = await _check_ssl_cert(hostname, parsed.port or 443)
                    raw_data["ssl"] = cert_info
                    if cert_info.get("days_until_expiry", 999) < 30:
                        findings.append({
                            "severity": "high",
                            "category": "ssl",
                            "title": "SSL certificate expiring soon",
                            "detail": f"Certificate expires in {cert_info['days_until_expiry']} days.",
                            "recommendation": "Renew SSL certificate immediately.",
                        })
                        score -= 15
                    if cert_info.get("error"):
                        findings.append({
                            "severity": "high",
                            "category": "ssl",
                            "title": "SSL certificate error",
                            "detail": cert_info["error"],
                            "recommendation": "Fix SSL certificate configuration.",
                        })
                        score -= 20
                except Exception as e:
                    findings.append({
                        "severity": "medium",
                        "category": "ssl",
                        "title": "Could not verify SSL certificate",
                        "detail": str(e),
                        "recommendation": "Ensure SSL certificate is properly configured.",
                    })
                    score -= 10

            # --- 8. DNS Checks ---
            try:
                dns_info = _check_dns(hostname)
                raw_data["dns"] = dns_info
                if not dns_info.get("has_ipv6"):
                    findings.append({
                        "severity": "info",
                        "category": "dns",
                        "title": "No IPv6 (AAAA) record",
                        "detail": "Site is not reachable via IPv6.",
                        "recommendation": "Consider adding AAAA DNS records for IPv6 support.",
                    })
            except Exception as e:
                raw_data["dns"] = {"error": str(e)}

    except Exception as e:
        findings.append({
            "severity": "critical",
            "category": "scan_error",
            "title": "Scan Error",
            "detail": str(e),
            "recommendation": "Check URL validity and retry.",
        })
        score = 0

    # Clamp score
    score = max(0, min(100, score))

    # Grade
    if score >= 90:
        grade = "A"
    elif score >= 80:
        grade = "B"
    elif score >= 65:
        grade = "C"
    elif score >= 50:
        grade = "D"
    else:
        grade = "F"

    # Summary
    critical = sum(1 for f in findings if f["severity"] == "critical")
    high = sum(1 for f in findings if f["severity"] == "high")
    medium = sum(1 for f in findings if f["severity"] == "medium")
    low = sum(1 for f in findings if f["severity"] in ("low", "info"))

    summary = (
        f"Security scan of {url}: Grade {grade} ({score}/100). "
        f"Found {len(findings)} issues: {critical} critical, {high} high, "
        f"{medium} medium, {low} low/info."
    )

    result = {
        "target_url": url,
        "scan_type": scan_type,
        "findings": findings,
        "score": score,
        "grade": grade,
        "report_summary": summary,
        "raw_data": raw_data,
    }

    # Store result
    try:
        with db_session() as conn:
            conn.execute("""
                INSERT INTO scan_results (target_url, scan_type, findings, score, grade,
                    report_summary, raw_data, requested_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                url, scan_type, json.dumps(findings), score, grade,
                summary, json.dumps(raw_data, default=str), "api",
            ))
            conn.execute(
                "INSERT INTO scan_log (action, detail) VALUES (?, ?)",
                ("scan_complete", f"{url} -> {grade} ({score}/100)"),
            )
    except Exception as e:
        print(f"[CyberDiv] Error storing scan result: {e}")

    return result


async def _check_ssl_cert(hostname: str, port: int = 443) -> Dict[str, Any]:
    """Check SSL certificate details."""
    info = {}
    try:
        ctx = ssl.create_default_context()
        with socket.create_connection((hostname, port), timeout=10) as sock:
            with ctx.wrap_socket(sock, server_hostname=hostname) as ssock:
                cert = ssock.getpeercert()
                info["subject"] = dict(x[0] for x in cert.get("subject", ()))
                info["issuer"] = dict(x[0] for x in cert.get("issuer", ()))
                info["serial_number"] = cert.get("serialNumber", "")
                info["not_before"] = cert.get("notBefore", "")
                info["not_after"] = cert.get("notAfter", "")
                info["version"] = cert.get("version", 0)
                info["protocol"] = ssock.version()

                # Parse expiry
                if cert.get("notAfter"):
                    from email.utils import parsedate_to_datetime
                    try:
                        expiry = parsedate_to_datetime(cert["notAfter"])
                        now = datetime.now(timezone.utc)
                        days_left = (expiry - now).days
                        info["days_until_expiry"] = days_left
                        info["expires_at"] = expiry.isoformat()
                    except Exception:
                        info["days_until_expiry"] = -1

                # SAN (Subject Alternative Names)
                san = cert.get("subjectAltName", ())
                info["san"] = [x[1] for x in san]
    except ssl.SSLError as e:
        info["error"] = f"SSL error: {e}"
    except socket.timeout:
        info["error"] = "Connection timeout"
    except Exception as e:
        info["error"] = str(e)

    return info


def _check_dns(hostname: str) -> Dict[str, Any]:
    """Basic DNS checks."""
    info = {"hostname": hostname, "has_ipv4": False, "has_ipv6": False}
    try:
        # IPv4
        addrs = socket.getaddrinfo(hostname, None, socket.AF_INET)
        if addrs:
            info["has_ipv4"] = True
            info["ipv4"] = list(set(a[4][0] for a in addrs))
    except socket.gaierror:
        pass

    try:
        # IPv6
        addrs6 = socket.getaddrinfo(hostname, None, socket.AF_INET6)
        if addrs6:
            info["has_ipv6"] = True
            info["ipv6"] = list(set(a[4][0] for a in addrs6))
    except socket.gaierror:
        pass

    return info


# ==========================================================================
# TRAINING MODULE
# ==========================================================================

# Public CTF practice resources
PRACTICE_SOURCES = [
    {
        "name": "PicoCTF",
        "url": "https://picoctf.org",
        "categories": ["web", "crypto", "forensics", "reverse_engineering", "pwn"],
        "difficulty": "beginner",
        "description": "Carnegie Mellon's free CTF platform. Perfect for beginners.",
    },
    {
        "name": "OverTheWire",
        "url": "https://overthewire.org/wargames/",
        "categories": ["pwn", "crypto", "misc"],
        "difficulty": "beginner-intermediate",
        "description": "Classic wargames. Start with Bandit, then Natas for web.",
    },
    {
        "name": "HackTheBox",
        "url": "https://www.hackthebox.com",
        "categories": ["web", "pwn", "crypto", "reverse_engineering", "forensics"],
        "difficulty": "intermediate-hard",
        "description": "Retired machines and challenges for practice.",
    },
    {
        "name": "TryHackMe",
        "url": "https://tryhackme.com",
        "categories": ["web", "forensics", "misc", "pwn"],
        "difficulty": "beginner-intermediate",
        "description": "Guided rooms and learning paths. Great for structured learning.",
    },
    {
        "name": "CryptoHack",
        "url": "https://cryptohack.org",
        "categories": ["crypto"],
        "difficulty": "beginner-advanced",
        "description": "The best platform for learning cryptography through challenges.",
    },
    {
        "name": "CTFlearn",
        "url": "https://ctflearn.com",
        "categories": ["web", "crypto", "forensics", "reverse_engineering", "misc"],
        "difficulty": "beginner",
        "description": "Community-driven CTF challenges sorted by difficulty.",
    },
    {
        "name": "Root Me",
        "url": "https://www.root-me.org",
        "categories": ["web", "crypto", "forensics", "reverse_engineering", "pwn"],
        "difficulty": "beginner-advanced",
        "description": "450+ challenges across all categories. Free.",
    },
    {
        "name": "pwnable.kr",
        "url": "https://pwnable.kr",
        "categories": ["pwn"],
        "difficulty": "intermediate-hard",
        "description": "Binary exploitation challenges. The classic pwn practice site.",
    },
    {
        "name": "Exploit Education",
        "url": "https://exploit.education",
        "categories": ["pwn", "reverse_engineering"],
        "difficulty": "beginner-intermediate",
        "description": "Phoenix, Protostar, Nebula — classic binary exploitation VMs.",
    },
    {
        "name": "OWASP WebGoat",
        "url": "https://owasp.org/www-project-webgoat/",
        "categories": ["web"],
        "difficulty": "beginner",
        "description": "Intentionally vulnerable web app for learning web security.",
    },
]


def get_skill_recommendations() -> List[Dict[str, Any]]:
    """Analyze skill levels and recommend what to practice."""
    recommendations = []
    with db_session() as conn:
        skills = conn.execute(
            "SELECT * FROM skill_levels ORDER BY level ASC, xp ASC"
        ).fetchall()

        for skill in skills:
            cat = skill["category"]
            level = skill["level"]
            xp = skill["xp"]
            solved = skill["challenges_solved"]

            # Find matching practice sources
            sources = [
                s for s in PRACTICE_SOURCES
                if cat in s["categories"]
            ]

            # Determine recommended difficulty
            if level <= 2:
                rec_diff = "beginner"
            elif level <= 5:
                rec_diff = "intermediate"
            else:
                rec_diff = "advanced"

            # Priority: lowest level skills first
            priority = "high" if level <= 2 else ("medium" if level <= 4 else "low")

            recommendations.append({
                "category": cat,
                "current_level": level,
                "xp": xp,
                "challenges_solved": solved,
                "recommended_difficulty": rec_diff,
                "priority": priority,
                "practice_resources": sources,
                "tip": _get_skill_tip(cat, level),
            })

    return recommendations


def _get_skill_tip(category: str, level: int) -> str:
    """Get a practice tip for a given category and level."""
    tips = {
        "web": {
            1: "Start with OWASP WebGoat and TryHackMe web fundamentals. Learn Burp Suite basics.",
            2: "Practice XSS and SQLi on PicoCTF. Read PortSwigger Web Security Academy.",
            3: "Move to HackTheBox web challenges. Learn SSRF, SSTI, and deserialization.",
            5: "Study real-world bug bounty reports on HackerOne hacktivity.",
        },
        "crypto": {
            1: "Start with CryptoHack introduction. Learn about Caesar, XOR, and base encodings.",
            2: "Study RSA, AES modes, and hash functions on CryptoHack.",
            3: "Practice real CTF crypto from past competitions. Study elliptic curves.",
            5: "Read academic papers on lattice attacks and side channels.",
        },
        "reverse_engineering": {
            1: "Install Ghidra. Start with CrackMe challenges on CTFlearn.",
            2: "Learn x86 assembly basics. Practice with Exploit Education Phoenix.",
            3: "Reverse stripped binaries. Learn anti-debugging techniques.",
            5: "Analyze real malware samples (in sandboxed VMs only).",
        },
        "forensics": {
            1: "Learn hex editors and file signatures. Start with PicoCTF forensics.",
            2: "Practice with Wireshark (pcap analysis) and Autopsy (disk forensics).",
            3: "Memory forensics with Volatility. Steganography challenges.",
            5: "Practice incident response scenarios. Study DFIR methodology.",
        },
        "pwn": {
            1: "Start with OverTheWire Bandit. Learn basic Linux/shell.",
            2: "Move to Protostar. Learn buffer overflows and stack canaries.",
            3: "Study ROP chains and heap exploitation on pwnable.kr.",
            5: "Kernel exploitation and advanced heap techniques.",
        },
        "misc": {
            1: "OSINT challenges are great starters. Try CTFlearn misc category.",
            2: "Learn scripting for automation. Practice programming challenges.",
            3: "Study blockchain, QR codes, and unconventional encodings.",
            5: "Contribute challenge writeups to the community.",
        },
    }
    cat_tips = tips.get(category, tips["misc"])
    # Find the highest level tip that applies
    best_tip = cat_tips.get(1, "Keep practicing!")
    for lvl in sorted(cat_tips.keys()):
        if level >= lvl:
            best_tip = cat_tips[lvl]
    return best_tip


# ==========================================================================
# BACKGROUND TASKS
# ==========================================================================

_scanner_running = False


async def background_scan():
    """Run periodic CTF + bounty scans."""
    global _scanner_running
    if _scanner_running:
        return
    _scanner_running = True

    print("[CyberDiv] Background scan starting...")
    try:
        # Fetch CTF events
        try:
            events = await fetch_upcoming_ctfs()
            if events:
                store_ctf_events(events)
        except Exception as e:
            print(f"[CyberDiv] CTF scan error: {e}")

        # Fetch bounty programs
        try:
            programs = await fetch_bounty_programs()
            if programs:
                store_bounty_programs(programs)
        except Exception as e:
            print(f"[CyberDiv] Bounty scan error: {e}")

        with db_session() as conn:
            conn.execute(
                "INSERT INTO scan_log (action, detail) VALUES (?, ?)",
                ("background_scan", "CTF + Bounty scan completed"),
            )
    except Exception as e:
        print(f"[CyberDiv] Background scan error: {e}")
    finally:
        _scanner_running = False
    print("[CyberDiv] Background scan complete.")


def start_periodic_scanner():
    """Start background scanner in a thread."""
    import asyncio

    def _run():
        while True:
            try:
                loop = asyncio.new_event_loop()
                loop.run_until_complete(background_scan())
                loop.close()
            except Exception as e:
                print(f"[CyberDiv] Periodic scanner error: {e}")
            time.sleep(SCAN_INTERVAL_MINUTES * 60)

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    print(f"[CyberDiv] Periodic scanner started (every {SCAN_INTERVAL_MINUTES} min)")


# ==========================================================================
# API ROUTES
# ==========================================================================


@app.on_event("startup")
async def startup():
    init_db()
    start_periodic_scanner()
    # Trigger initial scan in background
    import asyncio
    asyncio.create_task(background_scan())
    print(f"[CyberDiv] Cyber Division online on port {PORT}")


@app.get("/health")
async def health():
    """Health check."""
    with db_session() as conn:
        ctf_count = conn.execute("SELECT COUNT(*) FROM ctf_events").fetchone()[0]
        bounty_count = conn.execute("SELECT COUNT(*) FROM bounty_programs").fetchone()[0]
        scan_count = conn.execute("SELECT COUNT(*) FROM scan_results").fetchone()[0]
    return {
        "status": "healthy",
        "service": "cyber_division",
        "port": PORT,
        "ctf_events": ctf_count,
        "bounty_programs": bounty_count,
        "scans_completed": scan_count,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/ctf/upcoming")
async def get_upcoming_ctfs(
    limit: int = Query(default=25, ge=1, le=100),
    category: Optional[str] = Query(default=None),
    refresh: bool = Query(default=False),
):
    """Get upcoming CTF competitions."""
    if refresh:
        events = await fetch_upcoming_ctfs()
        if events:
            store_ctf_events(events)

    with db_session() as conn:
        query = "SELECT * FROM ctf_events WHERE status = 'upcoming'"
        params: list = []

        if category:
            query += " AND categories LIKE ?"
            params.append(f"%{category}%")

        query += " ORDER BY start_time ASC LIMIT ?"
        params.append(limit)

        rows = conn.execute(query, params).fetchall()
        events = []
        for r in rows:
            events.append({
                "id": r["id"],
                "ctftime_id": r["ctftime_id"],
                "name": r["name"],
                "url": r["url"],
                "start_time": r["start_time"],
                "end_time": r["end_time"],
                "format": r["format"],
                "location": r["location"],
                "weight": r["weight"],
                "categories": json.loads(r["categories"]) if r["categories"] else [],
                "description": r["description"],
                "status": r["status"],
            })

    return {
        "count": len(events),
        "events": events,
        "source": "ctftime.org",
        "last_refresh": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/ctf/past")
async def get_past_ctfs(limit: int = Query(default=25, ge=1, le=100)):
    """Get past CTF events with our results."""
    with db_session() as conn:
        rows = conn.execute("""
            SELECT e.*, COUNT(l.id) as learnings_count
            FROM ctf_events e
            LEFT JOIN ctf_learnings l ON l.event_id = e.id
            WHERE e.status IN ('completed', 'participated')
            GROUP BY e.id
            ORDER BY e.end_time DESC
            LIMIT ?
        """, (limit,)).fetchall()

        events = []
        for r in rows:
            events.append({
                "id": r["id"],
                "name": r["name"],
                "url": r["url"],
                "start_time": r["start_time"],
                "end_time": r["end_time"],
                "our_score": r["our_score"],
                "our_rank": r["our_rank"],
                "total_teams": r["total_teams"],
                "learnings_count": r["learnings_count"],
                "notes": r["notes"],
            })

    return {"count": len(events), "events": events}


@app.post("/api/ctf/learning")
async def add_ctf_learning(entry: LearningEntry):
    """Record a CTF learning/writeup."""
    with db_session() as conn:
        conn.execute("""
            INSERT INTO ctf_learnings (event_id, challenge_name, category, difficulty,
                solved, points, writeup, techniques, tools_used)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            entry.event_id, entry.challenge_name, entry.category,
            entry.difficulty, 1 if entry.solved else 0, entry.points,
            entry.writeup, json.dumps(entry.techniques),
            json.dumps(entry.tools_used),
        ))

        # Update skill XP
        xp_gain = entry.points if entry.solved else max(entry.points // 4, 5)
        conn.execute("""
            UPDATE skill_levels
            SET xp = xp + ?,
                challenges_attempted = challenges_attempted + 1,
                challenges_solved = challenges_solved + ?,
                last_practiced = datetime('now'),
                updated_at = datetime('now')
            WHERE category = ?
        """, (xp_gain, 1 if entry.solved else 0, entry.category))

        # Level up check (every 100 XP)
        skill = conn.execute(
            "SELECT xp, level FROM skill_levels WHERE category = ?",
            (entry.category,),
        ).fetchone()
        if skill:
            new_level = max(1, skill["xp"] // 100 + 1)
            if new_level > skill["level"]:
                conn.execute(
                    "UPDATE skill_levels SET level = ? WHERE category = ?",
                    (new_level, entry.category),
                )

    return {"status": "recorded", "xp_gained": xp_gain, "category": entry.category}


@app.get("/api/bounties")
async def get_bounty_programs(
    platform: Optional[str] = Query(default=None),
    difficulty: Optional[str] = Query(default=None),
    min_bounty: float = Query(default=0),
    limit: int = Query(default=50, ge=1, le=200),
    refresh: bool = Query(default=False),
):
    """Get active bug bounty programs."""
    if refresh:
        programs = await fetch_bounty_programs()
        if programs:
            store_bounty_programs(programs)

    with db_session() as conn:
        query = "SELECT * FROM bounty_programs WHERE status = 'active'"
        params: list = []

        if platform:
            query += " AND platform = ?"
            params.append(platform)
        if difficulty:
            query += " AND difficulty = ?"
            params.append(difficulty)
        if min_bounty > 0:
            query += " AND bounty_max >= ?"
            params.append(min_bounty)

        query += " ORDER BY match_score DESC, bounty_max DESC LIMIT ?"
        params.append(limit)

        rows = conn.execute(query, params).fetchall()
        programs = []
        for r in rows:
            programs.append({
                "id": r["id"],
                "platform": r["platform"],
                "program_name": r["program_name"],
                "program_url": r["program_url"],
                "bounty_min": r["bounty_min"],
                "bounty_max": r["bounty_max"],
                "difficulty": r["difficulty"],
                "skill_match": json.loads(r["skill_match"]) if r["skill_match"] else [],
                "match_score": r["match_score"],
                "response_time": r["response_time"],
                "updated_at": r["updated_at"],
            })

    return {
        "count": len(programs),
        "programs": programs,
        "our_skills": OUR_SKILLS,
    }


@app.post("/api/bounties/submission")
async def add_bounty_submission(sub: BountySubmission):
    """Record a bug bounty submission."""
    with db_session() as conn:
        conn.execute("""
            INSERT INTO bounty_submissions (program_id, title, severity, notes)
            VALUES (?, ?, ?, ?)
        """, (sub.program_id, sub.title, sub.severity, sub.notes))

        conn.execute(
            "INSERT INTO scan_log (action, detail) VALUES (?, ?)",
            ("bounty_submitted", f"{sub.title} ({sub.severity})"),
        )

    return {"status": "submitted", "title": sub.title}


@app.get("/api/bounties/submissions")
async def get_bounty_submissions():
    """Get all bounty submissions and payouts."""
    with db_session() as conn:
        rows = conn.execute("""
            SELECT s.*, p.program_name, p.platform
            FROM bounty_submissions s
            LEFT JOIN bounty_programs p ON p.id = s.program_id
            ORDER BY s.submitted_at DESC
        """).fetchall()

        subs = []
        total_payout = 0
        for r in rows:
            total_payout += r["payout"] or 0
            subs.append({
                "id": r["id"],
                "program": r["program_name"],
                "platform": r["platform"],
                "title": r["title"],
                "severity": r["severity"],
                "status": r["status"],
                "payout": r["payout"],
                "submitted_at": r["submitted_at"],
                "resolved_at": r["resolved_at"],
            })

    return {
        "count": len(subs),
        "total_payout": total_payout,
        "submissions": subs,
    }


@app.get("/api/tools")
async def get_security_tools(
    category: Optional[str] = Query(default=None),
    installed_only: bool = Query(default=False),
):
    """Get security tools inventory."""
    tools = []
    with db_session() as conn:
        for tool in SECURITY_TOOLS:
            if category and tool["category"] != category:
                continue

            # Check installation status from DB
            install_info = conn.execute(
                "SELECT * FROM tools_inventory WHERE name = ?",
                (tool["name"],),
            ).fetchall()

            machines = {}
            for row in install_info:
                machines[row["machine"]] = {
                    "installed": bool(row["installed"]),
                    "version": row["version"],
                    "last_checked": row["last_checked"],
                }

            is_installed_anywhere = any(m["installed"] for m in machines.values())
            if installed_only and not is_installed_anywhere:
                continue

            tools.append({
                **tool,
                "installed_on": machines,
                "is_installed": is_installed_anywhere,
            })

    categories = list(set(t["category"] for t in SECURITY_TOOLS))
    return {
        "count": len(tools),
        "tools": tools,
        "categories": categories,
    }


@app.post("/api/tools/check")
async def check_tool_installation(
    tool_name: str = Query(...),
    machine: str = Query(default="ZeroDESK"),
):
    """Check if a specific tool is installed on a machine."""
    tool = next((t for t in SECURITY_TOOLS if t["name"] == tool_name), None)
    if not tool:
        raise HTTPException(status_code=404, detail=f"Unknown tool: {tool_name}")

    # Try to run the check command locally (only works for ZeroDESK)
    installed = False
    version = "unknown"
    if machine == "ZeroDESK":
        import subprocess
        try:
            result = subprocess.run(
                tool["check_cmd"].split(),
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode == 0:
                installed = True
                version = result.stdout.strip()[:100] or result.stderr.strip()[:100]
        except (subprocess.TimeoutExpired, FileNotFoundError, Exception):
            pass

    with db_session() as conn:
        conn.execute("""
            INSERT INTO tools_inventory (name, machine, installed, version)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(name, machine) DO UPDATE SET
                installed=excluded.installed, version=excluded.version,
                last_checked=datetime('now')
        """, (tool_name, machine, 1 if installed else 0, version))

    return {
        "tool": tool_name,
        "machine": machine,
        "installed": installed,
        "version": version,
    }


@app.get("/api/skills")
async def get_skills():
    """Get skill assessment across all CTF categories."""
    with db_session() as conn:
        rows = conn.execute(
            "SELECT * FROM skill_levels ORDER BY level DESC, xp DESC"
        ).fetchall()

        skills = []
        for r in rows:
            skills.append({
                "category": r["category"],
                "level": r["level"],
                "xp": r["xp"],
                "xp_to_next": ((r["level"]) * 100) - r["xp"],
                "challenges_attempted": r["challenges_attempted"],
                "challenges_solved": r["challenges_solved"],
                "solve_rate": (
                    round(r["challenges_solved"] / r["challenges_attempted"] * 100, 1)
                    if r["challenges_attempted"] > 0 else 0
                ),
                "last_practiced": r["last_practiced"],
            })

    recommendations = get_skill_recommendations()

    return {
        "skills": skills,
        "recommendations": recommendations,
        "practice_sources": PRACTICE_SOURCES,
        "total_xp": sum(s["xp"] for s in skills),
        "average_level": round(sum(s["level"] for s in skills) / max(len(skills), 1), 1),
    }


@app.post("/api/skills/update")
async def update_skill(update: SkillUpdate):
    """Update skill level for a category."""
    if update.category not in CTF_CATEGORIES:
        raise HTTPException(status_code=400, detail=f"Invalid category. Choose from: {CTF_CATEGORIES}")

    with db_session() as conn:
        conn.execute("""
            UPDATE skill_levels
            SET xp = xp + ?,
                challenges_attempted = challenges_attempted + ?,
                challenges_solved = challenges_solved + ?,
                last_practiced = datetime('now'),
                updated_at = datetime('now')
            WHERE category = ?
        """, (
            update.xp_gained, update.challenges_attempted,
            update.challenges_solved, update.category,
        ))

        # Level up check
        skill = conn.execute(
            "SELECT xp, level FROM skill_levels WHERE category = ?",
            (update.category,),
        ).fetchone()
        if skill:
            new_level = max(1, skill["xp"] // 100 + 1)
            if new_level != skill["level"]:
                conn.execute(
                    "UPDATE skill_levels SET level = ? WHERE category = ?",
                    (new_level, update.category),
                )

    return {"status": "updated", "category": update.category}


@app.post("/api/scan-site")
async def scan_site(req: ScanRequest):
    """
    Perform a passive security scan on a website.
    Checks: HTTPS, security headers, SSL certificate, DNS, cookies.
    This is a NON-INTRUSIVE scan — no exploitation, no fuzzing.
    """
    # Basic URL validation
    url = req.url.strip()
    if not url:
        raise HTTPException(status_code=400, detail="URL is required")

    # Prevent scanning internal networks
    if not url.startswith("http"):
        url = f"https://{url}"

    parsed = urlparse(url)
    hostname = parsed.hostname or ""
    if not hostname:
        raise HTTPException(status_code=400, detail="Invalid URL")

    # Block internal/private IPs
    blocked_patterns = [
        "localhost", "127.0.0.1", "0.0.0.0",
        "10.", "172.16.", "172.17.", "172.18.", "172.19.",
        "172.20.", "172.21.", "172.22.", "172.23.", "172.24.",
        "172.25.", "172.26.", "172.27.", "172.28.", "172.29.",
        "172.30.", "172.31.", "192.168.", "100.",
    ]
    for pattern in blocked_patterns:
        if hostname.startswith(pattern) or hostname == pattern.rstrip("."):
            raise HTTPException(
                status_code=400,
                detail="Cannot scan private/internal addresses. Only public URLs allowed.",
            )

    with db_session() as conn:
        conn.execute(
            "INSERT INTO scan_log (action, detail) VALUES (?, ?)",
            ("scan_requested", f"{url} by {req.requested_by}"),
        )

    result = await scan_website(url, req.scan_type)
    return result


@app.get("/api/scan-history")
async def get_scan_history(limit: int = Query(default=25, ge=1, le=100)):
    """Get past security scan results."""
    with db_session() as conn:
        rows = conn.execute("""
            SELECT id, target_url, scan_type, score, grade, report_summary, scanned_at
            FROM scan_results
            ORDER BY scanned_at DESC
            LIMIT ?
        """, (limit,)).fetchall()

        scans = []
        for r in rows:
            scans.append({
                "id": r["id"],
                "target_url": r["target_url"],
                "scan_type": r["scan_type"],
                "score": r["score"],
                "grade": r["grade"],
                "report_summary": r["report_summary"],
                "scanned_at": r["scanned_at"],
            })

    return {"count": len(scans), "scans": scans}


@app.get("/api/scan/{scan_id}")
async def get_scan_detail(scan_id: int):
    """Get detailed scan result by ID."""
    with db_session() as conn:
        row = conn.execute(
            "SELECT * FROM scan_results WHERE id = ?", (scan_id,)
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Scan not found")

        return {
            "id": row["id"],
            "target_url": row["target_url"],
            "scan_type": row["scan_type"],
            "findings": json.loads(row["findings"]) if row["findings"] else [],
            "score": row["score"],
            "grade": row["grade"],
            "report_summary": row["report_summary"],
            "raw_data": json.loads(row["raw_data"]) if row["raw_data"] else {},
            "scanned_at": row["scanned_at"],
        }


@app.get("/api/services")
async def get_security_services():
    """Get sellable security services catalog."""
    # Calculate stats
    with db_session() as conn:
        total_scans = conn.execute("SELECT COUNT(*) FROM scan_results").fetchone()[0]
        avg_score = conn.execute(
            "SELECT AVG(score) FROM scan_results WHERE score > 0"
        ).fetchone()[0] or 0

    return {
        "services": SECURITY_SERVICES,
        "stats": {
            "total_scans_completed": total_scans,
            "average_security_score": round(avg_score, 1),
        },
        "note": "All security audits require documented client consent before scanning.",
    }


@app.get("/api/stats")
async def get_stats():
    """Get overall cyber division statistics."""
    with db_session() as conn:
        ctf_upcoming = conn.execute(
            "SELECT COUNT(*) FROM ctf_events WHERE status = 'upcoming'"
        ).fetchone()[0]
        ctf_participated = conn.execute(
            "SELECT COUNT(*) FROM ctf_events WHERE status = 'participated'"
        ).fetchone()[0]
        bounty_programs = conn.execute(
            "SELECT COUNT(*) FROM bounty_programs WHERE status = 'active'"
        ).fetchone()[0]
        bounty_subs = conn.execute(
            "SELECT COUNT(*) FROM bounty_submissions"
        ).fetchone()[0]
        total_payout = conn.execute(
            "SELECT COALESCE(SUM(payout), 0) FROM bounty_submissions WHERE payout > 0"
        ).fetchone()[0]
        total_scans = conn.execute(
            "SELECT COUNT(*) FROM scan_results"
        ).fetchone()[0]
        learnings = conn.execute(
            "SELECT COUNT(*) FROM ctf_learnings"
        ).fetchone()[0]
        total_xp = conn.execute(
            "SELECT COALESCE(SUM(xp), 0) FROM skill_levels"
        ).fetchone()[0]
        avg_level = conn.execute(
            "SELECT AVG(level) FROM skill_levels"
        ).fetchone()[0] or 1

    return {
        "ctf": {
            "upcoming_events": ctf_upcoming,
            "participated": ctf_participated,
            "learnings_recorded": learnings,
        },
        "bounties": {
            "active_programs": bounty_programs,
            "submissions": bounty_subs,
            "total_payout": total_payout,
        },
        "skills": {
            "total_xp": total_xp,
            "average_level": round(avg_level, 1),
        },
        "scans": {
            "total_completed": total_scans,
        },
        "tools_tracked": len(SECURITY_TOOLS),
    }


# ==========================================================================
# HTML DASHBOARD
# ==========================================================================


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Cyber Division HTML dashboard."""
    # Gather stats
    with db_session() as conn:
        ctf_upcoming = conn.execute(
            "SELECT COUNT(*) FROM ctf_events WHERE status = 'upcoming'"
        ).fetchone()[0]
        ctf_events = conn.execute("""
            SELECT name, url, start_time, format, weight, categories
            FROM ctf_events WHERE status = 'upcoming'
            ORDER BY start_time ASC LIMIT 10
        """).fetchall()

        bounty_count = conn.execute(
            "SELECT COUNT(*) FROM bounty_programs WHERE status = 'active'"
        ).fetchone()[0]
        top_bounties = conn.execute("""
            SELECT program_name, platform, bounty_max, match_score, program_url, difficulty
            FROM bounty_programs WHERE status = 'active'
            ORDER BY match_score DESC, bounty_max DESC LIMIT 10
        """).fetchall()

        total_scans = conn.execute(
            "SELECT COUNT(*) FROM scan_results"
        ).fetchone()[0]
        recent_scans = conn.execute("""
            SELECT target_url, grade, score, scanned_at
            FROM scan_results ORDER BY scanned_at DESC LIMIT 5
        """).fetchall()

        skills = conn.execute(
            "SELECT category, level, xp, challenges_solved FROM skill_levels ORDER BY level DESC"
        ).fetchall()

        total_xp = sum(s["xp"] for s in skills)
        total_payout = conn.execute(
            "SELECT COALESCE(SUM(payout), 0) FROM bounty_submissions WHERE payout > 0"
        ).fetchone()[0]

    # Build CTF rows
    ctf_rows = ""
    for ev in ctf_events:
        cats = json.loads(ev["categories"]) if ev["categories"] else ["misc"]
        cat_badges = " ".join(
            f'<span class="badge badge-{c}">{c}</span>' for c in cats[:3]
        )
        name_escaped = html_lib.escape(ev["name"])
        url = html_lib.escape(ev["url"] or "#")
        ctf_rows += f"""
        <tr>
            <td><a href="{url}" target="_blank">{name_escaped}</a></td>
            <td>{ev["start_time"][:10] if ev["start_time"] else "TBD"}</td>
            <td>{ev["format"] or "N/A"}</td>
            <td>{ev["weight"] or 0}</td>
            <td>{cat_badges}</td>
        </tr>"""

    # Build bounty rows
    bounty_rows = ""
    for b in top_bounties:
        name_escaped = html_lib.escape(b["program_name"])
        url = html_lib.escape(b["program_url"] or "#")
        bounty_rows += f"""
        <tr>
            <td><a href="{url}" target="_blank">{name_escaped}</a></td>
            <td>{b["platform"]}</td>
            <td>${b["bounty_max"]:,.0f}</td>
            <td>{b["difficulty"]}</td>
            <td>{b["match_score"]:.1%}</td>
        </tr>"""

    # Build skill bars
    skill_bars = ""
    for s in skills:
        pct = min(100, (s["xp"] % 100) if s["level"] > 1 else s["xp"])
        color = "#22c55e" if s["level"] >= 5 else "#3b82f6" if s["level"] >= 3 else "#eab308"
        skill_bars += f"""
        <div class="skill-row">
            <div class="skill-label">{s["category"].replace("_", " ").title()}</div>
            <div class="skill-bar-bg">
                <div class="skill-bar" style="width:{pct}%; background:{color}"></div>
            </div>
            <div class="skill-meta">Lv.{s["level"]} ({s["xp"]} XP) | {s["challenges_solved"]} solved</div>
        </div>"""

    # Build scan rows
    scan_rows = ""
    for sc in recent_scans:
        grade_color = {
            "A": "#22c55e", "B": "#84cc16", "C": "#eab308",
            "D": "#f97316", "F": "#ef4444",
        }.get(sc["grade"], "#6b7280")
        scan_rows += f"""
        <tr>
            <td>{html_lib.escape(sc["target_url"])}</td>
            <td style="color:{grade_color}; font-weight:bold">{sc["grade"]}</td>
            <td>{sc["score"]}/100</td>
            <td>{sc["scanned_at"]}</td>
        </tr>"""

    # Services section
    services_html = ""
    for svc in SECURITY_SERVICES:
        includes = "".join(f"<li>{html_lib.escape(i)}</li>" for i in svc["includes"])
        services_html += f"""
        <div class="service-card">
            <h3>{html_lib.escape(svc["name"])}</h3>
            <div class="price">{svc["price"]}</div>
            <p>{html_lib.escape(svc["description"])}</p>
            <ul>{includes}</ul>
            <div class="turnaround">Turnaround: {svc["turnaround"]}</div>
        </div>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Hive Cyber Division</title>
<style>
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{ font-family: 'Segoe UI', system-ui, sans-serif; background:#0a0a0a; color:#e5e5e5; }}
.container {{ max-width:1400px; margin:0 auto; padding:20px; }}
header {{ background:linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
    padding:30px; border-radius:12px; margin-bottom:24px; border:1px solid #1e3a5f; }}
header h1 {{ font-size:2.2em; color:#00ff88; text-shadow:0 0 20px rgba(0,255,136,0.3); }}
header .subtitle {{ color:#94a3b8; margin-top:4px; }}
.stats-grid {{ display:grid; grid-template-columns:repeat(auto-fit, minmax(200px, 1fr));
    gap:16px; margin-bottom:24px; }}
.stat-card {{ background:#111827; border:1px solid #1f2937; border-radius:10px;
    padding:20px; text-align:center; }}
.stat-card .value {{ font-size:2em; font-weight:bold; color:#00ff88; }}
.stat-card .label {{ color:#9ca3af; font-size:0.9em; margin-top:4px; }}
.section {{ background:#111827; border:1px solid #1f2937; border-radius:10px;
    padding:20px; margin-bottom:20px; }}
.section h2 {{ color:#60a5fa; margin-bottom:16px; font-size:1.3em;
    border-bottom:1px solid #1f2937; padding-bottom:8px; }}
table {{ width:100%; border-collapse:collapse; }}
th {{ background:#1e293b; color:#94a3b8; padding:10px 12px; text-align:left;
    font-size:0.85em; text-transform:uppercase; }}
td {{ padding:10px 12px; border-bottom:1px solid #1f2937; }}
tr:hover {{ background:#1a2332; }}
a {{ color:#60a5fa; text-decoration:none; }}
a:hover {{ text-decoration:underline; }}
.badge {{ display:inline-block; padding:2px 8px; border-radius:12px;
    font-size:0.75em; font-weight:600; margin-right:4px; }}
.badge-web {{ background:#1e40af; color:#93c5fd; }}
.badge-crypto {{ background:#7c2d12; color:#fdba74; }}
.badge-reverse_engineering {{ background:#581c87; color:#d8b4fe; }}
.badge-forensics {{ background:#064e3b; color:#6ee7b7; }}
.badge-pwn {{ background:#7f1d1d; color:#fca5a5; }}
.badge-misc {{ background:#374151; color:#d1d5db; }}
.skill-row {{ margin-bottom:12px; }}
.skill-label {{ font-weight:600; margin-bottom:4px; }}
.skill-bar-bg {{ background:#1f2937; border-radius:8px; height:20px; overflow:hidden; }}
.skill-bar {{ height:100%; border-radius:8px; transition:width 0.5s; }}
.skill-meta {{ font-size:0.8em; color:#9ca3af; margin-top:2px; }}
.services-grid {{ display:grid; grid-template-columns:repeat(auto-fit, minmax(300px, 1fr)); gap:16px; }}
.service-card {{ background:#1a2332; border:1px solid #1f2937; border-radius:10px;
    padding:20px; }}
.service-card h3 {{ color:#e5e5e5; margin-bottom:8px; }}
.service-card .price {{ font-size:1.5em; color:#00ff88; font-weight:bold; margin-bottom:8px; }}
.service-card p {{ color:#9ca3af; margin-bottom:12px; }}
.service-card ul {{ color:#d1d5db; padding-left:20px; font-size:0.9em; }}
.service-card li {{ margin-bottom:4px; }}
.service-card .turnaround {{ margin-top:12px; color:#60a5fa; font-size:0.85em; font-weight:600; }}
.disclaimer {{ background:#1c1917; border:1px solid #78350f; border-radius:8px;
    padding:16px; margin-top:20px; color:#fbbf24; font-size:0.9em; }}
.two-col {{ display:grid; grid-template-columns:1fr 1fr; gap:20px; }}
@media (max-width:768px) {{ .two-col {{ grid-template-columns:1fr; }} }}
.scan-form {{ display:flex; gap:8px; margin-bottom:16px; }}
.scan-form input {{ flex:1; padding:10px; border-radius:6px; border:1px solid #374151;
    background:#1e293b; color:#e5e5e5; font-size:1em; }}
.scan-form button {{ padding:10px 24px; border-radius:6px; border:none;
    background:#00ff88; color:#000; font-weight:bold; cursor:pointer; font-size:1em; }}
.scan-form button:hover {{ background:#00cc6a; }}
#scan-result {{ display:none; margin-top:16px; padding:16px; background:#1a2332;
    border-radius:8px; border:1px solid #1f2937; }}
</style>
</head>
<body>
<div class="container">
    <header>
        <h1>HIVE CYBER DIVISION</h1>
        <div class="subtitle">Defensive Security | CTF Competitions | Bug Bounties | Security Audits</div>
    </header>

    <div class="stats-grid">
        <div class="stat-card">
            <div class="value">{ctf_upcoming}</div>
            <div class="label">Upcoming CTFs</div>
        </div>
        <div class="stat-card">
            <div class="value">{bounty_count}</div>
            <div class="label">Bounty Programs</div>
        </div>
        <div class="stat-card">
            <div class="value">{total_scans}</div>
            <div class="label">Scans Completed</div>
        </div>
        <div class="stat-card">
            <div class="value">{total_xp}</div>
            <div class="label">Total XP</div>
        </div>
        <div class="stat-card">
            <div class="value">${total_payout:,.2f}</div>
            <div class="label">Bounty Earnings</div>
        </div>
        <div class="stat-card">
            <div class="value">{len(SECURITY_TOOLS)}</div>
            <div class="label">Tools Tracked</div>
        </div>
    </div>

    <div class="section">
        <h2>Quick Security Scan</h2>
        <div class="scan-form">
            <input type="text" id="scan-url" placeholder="Enter a URL to scan (e.g., example.com)" />
            <button onclick="runScan()">Scan</button>
        </div>
        <div id="scan-result"></div>
    </div>

    <div class="two-col">
        <div class="section">
            <h2>Upcoming CTF Competitions</h2>
            <table>
                <thead><tr><th>Name</th><th>Date</th><th>Format</th><th>Weight</th><th>Categories</th></tr></thead>
                <tbody>{ctf_rows if ctf_rows else '<tr><td colspan="5" style="text-align:center;color:#6b7280">No events loaded yet. Data refreshes hourly.</td></tr>'}</tbody>
            </table>
        </div>

        <div class="section">
            <h2>Top Bug Bounty Matches</h2>
            <table>
                <thead><tr><th>Program</th><th>Platform</th><th>Max Bounty</th><th>Difficulty</th><th>Match</th></tr></thead>
                <tbody>{bounty_rows if bounty_rows else '<tr><td colspan="5" style="text-align:center;color:#6b7280">No programs loaded yet. Data refreshes hourly.</td></tr>'}</tbody>
            </table>
        </div>
    </div>

    <div class="two-col">
        <div class="section">
            <h2>Skill Levels</h2>
            {skill_bars if skill_bars else '<p style="color:#6b7280">No skill data yet. Complete CTF challenges to gain XP.</p>'}
        </div>

        <div class="section">
            <h2>Recent Scans</h2>
            <table>
                <thead><tr><th>URL</th><th>Grade</th><th>Score</th><th>Date</th></tr></thead>
                <tbody>{scan_rows if scan_rows else '<tr><td colspan="4" style="text-align:center;color:#6b7280">No scans yet. Try the scanner above.</td></tr>'}</tbody>
            </table>
        </div>
    </div>

    <div class="section">
        <h2>Security Services (Sellable)</h2>
        <div class="services-grid">{services_html}</div>
    </div>

    <div class="disclaimer">
        <strong>NOTICE:</strong> The Hive Cyber Division operates strictly within legal and ethical boundaries.
        All scanning requires explicit authorization from the target owner.
        Bug bounty hunting is conducted only through official programs.
        CTF competitions are educational exercises.
        No systems are ever accessed without proper authorization.
    </div>
</div>

<script>
async function runScan() {{
    const url = document.getElementById('scan-url').value.trim();
    if (!url) return alert('Enter a URL');
    const box = document.getElementById('scan-result');
    box.style.display = 'block';
    box.innerHTML = '<p style="color:#60a5fa">Scanning... please wait.</p>';
    try {{
        const res = await fetch('/api/scan-site', {{
            method: 'POST',
            headers: {{'Content-Type': 'application/json'}},
            body: JSON.stringify({{url: url, scan_type: 'basic', requested_by: 'dashboard'}})
        }});
        const data = await res.json();
        if (!res.ok) {{
            box.innerHTML = '<p style="color:#ef4444">Error: ' + (data.detail || 'Scan failed') + '</p>';
            return;
        }}
        const gradeColor = {{'A':'#22c55e','B':'#84cc16','C':'#eab308','D':'#f97316','F':'#ef4444'}}[data.grade] || '#6b7280';
        let html = '<div style="display:flex;align-items:center;gap:20px;margin-bottom:16px">';
        html += '<div style="font-size:3em;font-weight:bold;color:' + gradeColor + '">' + data.grade + '</div>';
        html += '<div><div style="font-size:1.2em;font-weight:600">' + data.score + '/100</div>';
        html += '<div style="color:#9ca3af">' + data.target_url + '</div></div></div>';
        html += '<table style="width:100%"><thead><tr><th>Severity</th><th>Category</th><th>Finding</th><th>Recommendation</th></tr></thead><tbody>';
        for (const f of data.findings || []) {{
            const sevColor = {{'critical':'#ef4444','high':'#f97316','medium':'#eab308','low':'#6b7280','info':'#60a5fa'}}[f.severity] || '#fff';
            html += '<tr><td style="color:' + sevColor + ';font-weight:bold">' + f.severity.toUpperCase() + '</td>';
            html += '<td>' + f.category + '</td><td>' + f.title + '</td>';
            html += '<td style="font-size:0.85em;color:#9ca3af">' + (f.recommendation||'') + '</td></tr>';
        }}
        if (!data.findings || data.findings.length === 0) {{
            html += '<tr><td colspan="4" style="text-align:center;color:#22c55e">No issues found!</td></tr>';
        }}
        html += '</tbody></table>';
        box.innerHTML = html;
    }} catch(e) {{
        box.innerHTML = '<p style="color:#ef4444">Error: ' + e.message + '</p>';
    }}
}}
</script>
</body>
</html>"""

    return HTMLResponse(content=html)


# ==========================================================================
# MAIN
# ==========================================================================

if __name__ == "__main__":
    print(f"""
    ╔══════════════════════════════════════════════════╗
    ║       THE HIVE — CYBER DIVISION                  ║
    ║       Port: {PORT}                                ║
    ║       DB: {DB_PATH}            ║
    ║       Defensive Security | CTF | Bug Bounties    ║
    ╚══════════════════════════════════════════════════╝
    """)
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
