#!/usr/bin/env python3
"""
THE HIVE — Code Absorber Agent
Port 8918 | SQLite at /home/zero/hivecode_sandbox/absorber.db

Watches the AI tracker (port 8917) and memory researcher (port 8906) for
high-scoring MIT-licensed repos, clones them, analyzes structure, extracts
useful patterns/functions/classes, grades absorption potential, and feeds
learnings into the reasoning bank (port 8910) and council #insights.

The Hive gets smarter from every repo it reads.
"""

import json
import sqlite3
import time
import threading
import os
import re
import hashlib
import subprocess
import shutil
import traceback
import html as html_mod
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any
from contextlib import contextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI, BackgroundTasks, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
import uvicorn

# ==========================================================================
# CONFIG
# ==========================================================================
PORT = 8918
DB_PATH = "/home/zero/hivecode_sandbox/absorber.db"
SANDBOX_DIR = "/home/zero/hivecode_sandbox/discoveries"

OLLAMA_URL = "http://localhost:11434"
OLLAMA_MODEL = "gemma2:2b"

# Upstream sources
AI_TRACKER_URL = "http://localhost:8917"       # AI tracker (discoveries)
MEMORY_RESEARCHER_URL = "http://localhost:8906" # Memory researcher (MIT repos)
REASONING_BANK_URL = "http://localhost:8910"    # Store learned patterns
NERVE_URL = "http://100.105.160.106:8200/api/add"
COUNCIL_URL = "http://localhost:8766"

# Scan interval: check upstream sources every 30 minutes
SCAN_INTERVAL_MINUTES = 30

# Clone limits
MAX_CLONE_SIZE_MB = 200     # Skip repos larger than this
MAX_FILES_TO_ANALYZE = 500  # Don't analyze repos with more Python files
CLONE_TIMEOUT_SEC = 120     # Git clone timeout

# Relevance threshold for auto-absorbing from memory researcher
MIN_RELEVANCE_SCORE = 60

# Acceptable licenses (MIT and friends)
GOOD_LICENSES = {
    "mit", "mit license", "apache-2.0", "apache 2.0", "bsd-2-clause",
    "bsd-3-clause", "unlicense", "cc0-1.0", "isc", "0bsd",
}

# Rejected licenses
BAD_LICENSES = {
    "gpl", "gpl-2.0", "gpl-3.0", "agpl", "agpl-3.0", "lgpl",
    "lgpl-2.1", "lgpl-3.0", "sspl", "busl", "commons clause",
}

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# ==========================================================================
# COUNCIL CLIENT
# ==========================================================================
try:
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'council'))
    from council_client import CouncilClient
    council = CouncilClient(
        agent_id="hive-code-absorber",
        agent_type="service",
        machine="ZeroDESK",
    )
except ImportError:
    council = None

# Reasoning Bank client
try:
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'core'))
    from reasoning_client import ReasoningClient
    reasoning = ReasoningClient(domain="code_absorber")
except ImportError:
    reasoning = None

# ==========================================================================
# LOGGING
# ==========================================================================
import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CODE-ABSORBER] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("code_absorber")

# ==========================================================================
# DATABASE
# ==========================================================================
_db_lock = threading.Lock()


@contextmanager
def get_db():
    """Thread-safe database connection."""
    with _db_lock:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def init_db():
    """Create tables if they don't exist."""
    with get_db() as db:
        db.executescript("""
            CREATE TABLE IF NOT EXISTS repos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                url TEXT UNIQUE NOT NULL,
                source TEXT DEFAULT 'unknown',
                source_score REAL DEFAULT 0,
                license TEXT,
                license_ok INTEGER DEFAULT 0,
                stars INTEGER DEFAULT 0,
                language TEXT,
                description TEXT,
                clone_path TEXT,
                clone_status TEXT DEFAULT 'pending',
                clone_error TEXT,
                discovered_at TEXT DEFAULT (datetime('now')),
                cloned_at TEXT,
                analyzed_at TEXT,
                status TEXT DEFAULT 'pending',
                total_py_files INTEGER DEFAULT 0,
                total_lines INTEGER DEFAULT 0,
                readme_summary TEXT,
                structure_summary TEXT,
                absorption_grade TEXT,
                absorption_score REAL DEFAULT 0,
                absorption_report TEXT,
                fed_to_reasoning INTEGER DEFAULT 0,
                fed_to_nerve INTEGER DEFAULT 0,
                posted_to_council INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS patterns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                repo_id INTEGER NOT NULL,
                repo_name TEXT NOT NULL,
                pattern_type TEXT NOT NULL,
                name TEXT NOT NULL,
                file_path TEXT,
                line_number INTEGER,
                code_snippet TEXT,
                description TEXT,
                usefulness TEXT DEFAULT 'medium',
                absorption_status TEXT DEFAULT 'identified',
                tags TEXT,
                extracted_at TEXT DEFAULT (datetime('now')),
                fed_to_reasoning INTEGER DEFAULT 0,
                FOREIGN KEY (repo_id) REFERENCES repos(id)
            );

            CREATE TABLE IF NOT EXISTS scan_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scan_type TEXT NOT NULL,
                started_at TEXT DEFAULT (datetime('now')),
                finished_at TEXT,
                repos_found INTEGER DEFAULT 0,
                repos_cloned INTEGER DEFAULT 0,
                repos_analyzed INTEGER DEFAULT 0,
                patterns_extracted INTEGER DEFAULT 0,
                status TEXT DEFAULT 'running',
                error TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_repos_status ON repos(status);
            CREATE INDEX IF NOT EXISTS idx_repos_grade ON repos(absorption_grade);
            CREATE INDEX IF NOT EXISTS idx_repos_score ON repos(absorption_score DESC);
            CREATE INDEX IF NOT EXISTS idx_patterns_type ON patterns(pattern_type);
            CREATE INDEX IF NOT EXISTS idx_patterns_useful ON patterns(usefulness);
            CREATE INDEX IF NOT EXISTS idx_patterns_repo ON patterns(repo_id);
        """)
    log.info("Database initialized at %s", DB_PATH)


# ==========================================================================
# GIT OPERATIONS
# ==========================================================================

def clone_repo(url: str, name: str) -> Dict[str, Any]:
    """Clone a repo to the sandbox. Returns dict with success, path, error."""
    dest = os.path.join(SANDBOX_DIR, name)

    # Already cloned?
    if os.path.exists(dest) and os.path.isdir(os.path.join(dest, ".git")):
        return {"success": True, "path": dest, "error": None, "cached": True}

    # Clean stale partial clone
    if os.path.exists(dest):
        shutil.rmtree(dest, ignore_errors=True)

    try:
        result = subprocess.run(
            ["git", "clone", "--depth", "1", "--single-branch", url, dest],
            capture_output=True, text=True, timeout=CLONE_TIMEOUT_SEC,
        )
        if result.returncode != 0:
            return {"success": False, "path": None, "error": result.stderr.strip()[:500]}

        # Check size
        size_mb = _dir_size_mb(dest)
        if size_mb > MAX_CLONE_SIZE_MB:
            shutil.rmtree(dest, ignore_errors=True)
            return {"success": False, "path": None, "error": f"Too large: {size_mb:.0f}MB > {MAX_CLONE_SIZE_MB}MB"}

        return {"success": True, "path": dest, "error": None, "cached": False}

    except subprocess.TimeoutExpired:
        shutil.rmtree(dest, ignore_errors=True)
        return {"success": False, "path": None, "error": "Clone timed out"}
    except Exception as e:
        shutil.rmtree(dest, ignore_errors=True)
        return {"success": False, "path": None, "error": str(e)[:500]}


def _dir_size_mb(path: str) -> float:
    """Get total directory size in MB."""
    total = 0
    for dirpath, _, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            try:
                total += os.path.getsize(fp)
            except OSError:
                pass
    return total / (1024 * 1024)


# ==========================================================================
# CODE ANALYSIS
# ==========================================================================

def analyze_repo(repo_path: str, repo_name: str) -> Dict[str, Any]:
    """
    Analyze a cloned repo's code structure.
    Returns dict with structure info and extracted patterns.
    """
    result = {
        "py_files": [],
        "total_py_files": 0,
        "total_lines": 0,
        "readme": "",
        "classes": [],
        "functions": [],
        "imports": [],
        "decorators": [],
        "patterns": [],
        "file_tree": [],
    }

    # Find all Python files
    py_files = []
    for root, dirs, files in os.walk(repo_path):
        # Skip hidden dirs, __pycache__, .git, node_modules, venv
        dirs[:] = [d for d in dirs if not d.startswith('.') and d not in
                   ('__pycache__', 'node_modules', 'venv', '.venv', 'env', '.env',
                    'dist', 'build', 'egg-info', '.eggs', '.tox')]
        for f in files:
            if f.endswith('.py'):
                full = os.path.join(root, f)
                rel = os.path.relpath(full, repo_path)
                py_files.append((full, rel))

    result["total_py_files"] = len(py_files)

    if len(py_files) > MAX_FILES_TO_ANALYZE:
        log.warning("Repo %s has %d .py files, capping analysis at %d",
                     repo_name, len(py_files), MAX_FILES_TO_ANALYZE)
        py_files = py_files[:MAX_FILES_TO_ANALYZE]

    # Build file tree
    result["file_tree"] = [rel for _, rel in py_files[:100]]

    # Read README
    for readme_name in ("README.md", "README.rst", "README.txt", "README", "readme.md"):
        readme_path = os.path.join(repo_path, readme_name)
        if os.path.exists(readme_path):
            try:
                with open(readme_path, 'r', errors='replace') as f:
                    result["readme"] = f.read()[:5000]  # First 5KB
            except Exception:
                pass
            break

    # Analyze each Python file
    total_lines = 0
    all_classes = []
    all_functions = []
    all_imports = set()
    all_decorators = set()
    all_patterns = []

    for full_path, rel_path in py_files:
        try:
            with open(full_path, 'r', errors='replace') as f:
                content = f.read()
        except Exception:
            continue

        lines = content.split('\n')
        line_count = len(lines)
        total_lines += line_count

        file_info = {"path": rel_path, "lines": line_count}
        result["py_files"].append(file_info)

        # Extract classes
        for i, line in enumerate(lines):
            stripped = line.strip()

            # Classes
            class_match = re.match(r'^class\s+(\w+)\s*(?:\((.*?)\))?\s*:', stripped)
            if class_match:
                cls_name = class_match.group(1)
                bases = class_match.group(2) or ""
                docstring = _extract_docstring(lines, i + 1)
                all_classes.append({
                    "name": cls_name,
                    "bases": bases,
                    "file": rel_path,
                    "line": i + 1,
                    "docstring": docstring[:200] if docstring else "",
                })

            # Top-level and method functions
            func_match = re.match(r'^(?:    )?(?:async\s+)?def\s+(\w+)\s*\((.*?)\)', stripped)
            if func_match:
                fn_name = func_match.group(1)
                params = func_match.group(2)
                docstring = _extract_docstring(lines, i + 1)
                is_method = line.startswith('    ') or line.startswith('\t')
                if not fn_name.startswith('_') or fn_name == '__init__':
                    all_functions.append({
                        "name": fn_name,
                        "params": params[:100],
                        "file": rel_path,
                        "line": i + 1,
                        "is_method": is_method,
                        "is_async": 'async' in stripped.split('def')[0],
                        "docstring": docstring[:200] if docstring else "",
                    })

            # Imports
            import_match = re.match(r'^(?:from\s+([\w.]+)\s+)?import\s+(.+)', stripped)
            if import_match:
                module = import_match.group(1) or ""
                names = import_match.group(2).strip()
                if module:
                    all_imports.add(module.split('.')[0])
                else:
                    for name in names.split(','):
                        all_imports.add(name.strip().split('.')[0].split(' ')[0])

            # Decorators
            deco_match = re.match(r'^@(\w+(?:\.\w+)*)', stripped)
            if deco_match:
                all_decorators.add(deco_match.group(1))

        # Detect notable patterns in this file
        patterns = _detect_patterns(content, rel_path)
        all_patterns.extend(patterns)

    result["total_lines"] = total_lines
    result["classes"] = all_classes[:200]
    result["functions"] = all_functions[:500]
    result["imports"] = sorted(all_imports)[:100]
    result["decorators"] = sorted(all_decorators)[:50]
    result["patterns"] = all_patterns[:200]

    return result


def _extract_docstring(lines: List[str], start_idx: int) -> str:
    """Extract the docstring starting after a def/class line."""
    if start_idx >= len(lines):
        return ""
    # Look for triple-quote docstring within 3 lines
    for offset in range(min(3, len(lines) - start_idx)):
        line = lines[start_idx + offset].strip()
        if line.startswith('"""') or line.startswith("'''"):
            quote = line[:3]
            if line.count(quote) >= 2:
                # Single-line docstring
                return line.strip(quote).strip()
            # Multi-line docstring
            doc_lines = [line.lstrip(quote)]
            for j in range(start_idx + offset + 1, min(start_idx + offset + 20, len(lines))):
                doc_line = lines[j].strip()
                if quote in doc_line:
                    doc_lines.append(doc_line.rstrip(quote).strip())
                    break
                doc_lines.append(doc_line)
            return ' '.join(doc_lines).strip()
        elif line and not line.startswith('#'):
            break
    return ""


def _detect_patterns(content: str, file_path: str) -> List[Dict]:
    """Detect notable coding patterns in file content."""
    patterns = []

    # Pattern: Context manager
    if '@contextmanager' in content or '__enter__' in content:
        patterns.append({
            "type": "context_manager",
            "file": file_path,
            "description": "Uses context managers for resource management",
        })

    # Pattern: Async/await
    if 'async def' in content and 'await' in content:
        patterns.append({
            "type": "async_pattern",
            "file": file_path,
            "description": "Uses async/await for concurrent operations",
        })

    # Pattern: Decorator factory
    if re.search(r'def\s+\w+\(.*?\):\s*\n\s+def\s+\w+\(func\)', content):
        patterns.append({
            "type": "decorator_factory",
            "file": file_path,
            "description": "Custom decorator factory pattern",
        })

    # Pattern: Singleton
    if '_instance' in content and '__new__' in content:
        patterns.append({
            "type": "singleton",
            "file": file_path,
            "description": "Singleton design pattern",
        })

    # Pattern: Plugin/Registry
    if 'registry' in content.lower() and ('register' in content.lower() or 'plugin' in content.lower()):
        patterns.append({
            "type": "plugin_registry",
            "file": file_path,
            "description": "Plugin/registry pattern for extensibility",
        })

    # Pattern: Retry logic
    if 'retry' in content.lower() and ('attempt' in content.lower() or 'backoff' in content.lower() or 'max_retries' in content.lower()):
        patterns.append({
            "type": "retry_logic",
            "file": file_path,
            "description": "Retry logic with backoff for resilience",
        })

    # Pattern: Event system
    if 'emit' in content and ('event' in content.lower() or 'listener' in content.lower() or 'subscribe' in content.lower()):
        patterns.append({
            "type": "event_system",
            "file": file_path,
            "description": "Event-driven / pub-sub pattern",
        })

    # Pattern: State machine
    if 'state' in content.lower() and ('transition' in content.lower() or 'machine' in content.lower()):
        patterns.append({
            "type": "state_machine",
            "file": file_path,
            "description": "State machine pattern",
        })

    # Pattern: Pipeline / chain of responsibility
    if ('pipeline' in content.lower() or 'chain' in content.lower()) and ('step' in content.lower() or 'stage' in content.lower() or 'handler' in content.lower()):
        patterns.append({
            "type": "pipeline",
            "file": file_path,
            "description": "Pipeline or chain-of-responsibility pattern",
        })

    # Pattern: Caching
    if 'cache' in content.lower() and ('lru' in content.lower() or 'ttl' in content.lower() or '@cache' in content or 'functools' in content):
        patterns.append({
            "type": "caching",
            "file": file_path,
            "description": "Caching strategy (LRU, TTL, or decorator-based)",
        })

    # Pattern: Thread pool / worker pool
    if 'ThreadPool' in content or 'ProcessPool' in content or 'concurrent.futures' in content:
        patterns.append({
            "type": "worker_pool",
            "file": file_path,
            "description": "Thread/process pool for parallel execution",
        })

    # Pattern: Rate limiting
    if 'rate_limit' in content.lower() or 'throttle' in content.lower() or 'token_bucket' in content.lower():
        patterns.append({
            "type": "rate_limiting",
            "file": file_path,
            "description": "Rate limiting / throttling pattern",
        })

    # Pattern: Middleware
    if 'middleware' in content.lower() and ('request' in content.lower() or 'app' in content.lower()):
        patterns.append({
            "type": "middleware",
            "file": file_path,
            "description": "Middleware pattern for request processing",
        })

    # Pattern: Builder pattern
    if re.search(r'def\s+\w+\(self.*?\)\s*->\s*[\'"]?Self[\'"]?', content) or \
       (content.count('return self') >= 3):
        patterns.append({
            "type": "builder",
            "file": file_path,
            "description": "Builder / fluent interface pattern",
        })

    return patterns


# ==========================================================================
# GRADING & ABSORPTION SCORING
# ==========================================================================

def grade_absorption(analysis: Dict, repo_info: Dict) -> Dict[str, Any]:
    """
    Grade a repo's absorption potential.
    Returns grade (A-F), score (0-100), and report.
    """
    score = 0
    reasons = []

    # Factor 1: License (20 pts max)
    lic = (repo_info.get("license") or "").lower().strip()
    if any(g in lic for g in GOOD_LICENSES):
        score += 20
        reasons.append("+20 Good license: " + lic)
    elif lic and not any(b in lic for b in BAD_LICENSES):
        score += 10
        reasons.append("+10 Unknown but possibly OK license: " + lic)
    else:
        reasons.append("+0 Bad or missing license")

    # Factor 2: Code size (15 pts max — medium repos are ideal)
    total_lines = analysis.get("total_lines", 0)
    if 100 <= total_lines <= 5000:
        score += 15
        reasons.append(f"+15 Ideal size ({total_lines} lines)")
    elif 5000 < total_lines <= 20000:
        score += 10
        reasons.append(f"+10 Large but manageable ({total_lines} lines)")
    elif total_lines > 20000:
        score += 5
        reasons.append(f"+5 Very large ({total_lines} lines)")
    elif total_lines > 0:
        score += 8
        reasons.append(f"+8 Small ({total_lines} lines)")
    else:
        reasons.append("+0 No Python code found")

    # Factor 3: Documentation (10 pts max)
    readme = analysis.get("readme", "")
    if len(readme) > 500:
        score += 10
        reasons.append("+10 Good README documentation")
    elif len(readme) > 100:
        score += 5
        reasons.append("+5 Basic README")
    else:
        reasons.append("+0 No/minimal README")

    # Factor 4: Useful patterns (20 pts max)
    patterns = analysis.get("patterns", [])
    pattern_points = min(20, len(patterns) * 4)
    score += pattern_points
    if patterns:
        ptypes = list(set(p["type"] for p in patterns))
        reasons.append(f"+{pattern_points} Patterns found: {', '.join(ptypes[:5])}")
    else:
        reasons.append("+0 No notable patterns detected")

    # Factor 5: Interesting imports (10 pts max)
    imports = set(analysis.get("imports", []))
    valuable_imports = imports & {
        "fastapi", "flask", "httpx", "aiohttp", "asyncio",
        "sqlite3", "sqlalchemy", "redis", "celery",
        "transformers", "torch", "numpy", "pandas",
        "openai", "anthropic", "langchain", "llama_index",
        "pydantic", "typer", "click",
    }
    import_points = min(10, len(valuable_imports) * 3)
    score += import_points
    if valuable_imports:
        reasons.append(f"+{import_points} Valuable imports: {', '.join(sorted(valuable_imports)[:5])}")

    # Factor 6: Code structure quality (15 pts max)
    classes = analysis.get("classes", [])
    functions = analysis.get("functions", [])
    # Docstring coverage
    documented = sum(1 for f in functions if f.get("docstring"))
    doc_ratio = documented / max(len(functions), 1)
    if doc_ratio > 0.5:
        score += 8
        reasons.append(f"+8 Good docstring coverage ({doc_ratio:.0%})")
    elif doc_ratio > 0.2:
        score += 4
        reasons.append(f"+4 Some docstrings ({doc_ratio:.0%})")
    # Class/function ratio
    if classes and functions:
        score += 7
        reasons.append(f"+7 Good structure ({len(classes)} classes, {len(functions)} functions)")
    elif functions:
        score += 4
        reasons.append(f"+4 Functions only ({len(functions)} total)")

    # Factor 7: Stars / community validation (10 pts max)
    stars = repo_info.get("stars", 0)
    if stars >= 1000:
        score += 10
        reasons.append(f"+10 Popular ({stars:,} stars)")
    elif stars >= 100:
        score += 7
        reasons.append(f"+7 Notable ({stars:,} stars)")
    elif stars >= 10:
        score += 4
        reasons.append(f"+4 Some community interest ({stars} stars)")
    elif stars > 0:
        score += 2
        reasons.append(f"+2 New project ({stars} stars)")

    # Determine grade
    if score >= 85:
        grade = "A"
    elif score >= 70:
        grade = "B"
    elif score >= 55:
        grade = "C"
    elif score >= 40:
        grade = "D"
    else:
        grade = "F"

    report = f"## Absorption Report: {repo_info.get('name', 'unknown')}\n\n"
    report += f"**Grade**: {grade} ({score}/100)\n"
    report += f"**URL**: {repo_info.get('url', 'N/A')}\n"
    report += f"**License**: {repo_info.get('license', 'unknown')}\n"
    report += f"**Stars**: {stars:,}\n"
    report += f"**Python files**: {analysis.get('total_py_files', 0)} ({total_lines:,} lines)\n\n"
    report += "### Scoring Breakdown\n"
    for r in reasons:
        report += f"- {r}\n"
    report += "\n### Key Findings\n"
    report += f"- **Classes**: {len(classes)}\n"
    report += f"- **Functions**: {len(functions)}\n"
    report += f"- **Imports**: {', '.join(analysis.get('imports', [])[:15])}\n"
    report += f"- **Patterns**: {len(patterns)}\n"

    if classes:
        report += "\n### Notable Classes\n"
        for cls in classes[:10]:
            bases_str = f"({cls['bases']})" if cls['bases'] else ""
            report += f"- `{cls['name']}{bases_str}` in {cls['file']}:{cls['line']}"
            if cls['docstring']:
                report += f" — {cls['docstring'][:80]}"
            report += "\n"

    if patterns:
        report += "\n### Detected Patterns\n"
        for p in patterns[:10]:
            report += f"- **{p['type']}** in `{p['file']}`: {p['description']}\n"

    # Absorption recommendation
    report += "\n### Recommendation\n"
    if grade in ("A", "B"):
        report += "**ABSORB** — High-value code. Clone patterns, study structure, feed to reasoning bank.\n"
    elif grade == "C":
        report += "**SELECTIVE** — Some useful patterns. Cherry-pick specific functions/classes.\n"
    elif grade == "D":
        report += "**REFERENCE** — Limited direct use. Keep as reference for specific techniques.\n"
    else:
        report += "**SKIP** — Low absorption value. Not worth further analysis.\n"

    return {
        "grade": grade,
        "score": score,
        "reasons": reasons,
        "report": report,
    }


# ==========================================================================
# UPSTREAM WATCHERS
# ==========================================================================

def fetch_from_ai_tracker() -> List[Dict]:
    """Fetch high-scoring discoveries from AI tracker (port 8917)."""
    repos = []
    try:
        with httpx.Client(timeout=15) as client:
            # Try different likely endpoints
            for endpoint in ["/api/discoveries", "/api/repos", "/api/findings"]:
                try:
                    resp = client.get(f"{AI_TRACKER_URL}{endpoint}", params={"min_score": 70, "limit": 20})
                    if resp.status_code == 200:
                        data = resp.json()
                        items = data if isinstance(data, list) else data.get("discoveries", data.get("repos", data.get("findings", [])))
                        for item in items:
                            url = item.get("url") or item.get("repo_url") or item.get("github_url", "")
                            if url and "github.com" in url:
                                name = url.rstrip("/").split("/")[-1]
                                repos.append({
                                    "name": name,
                                    "url": url,
                                    "source": "ai_tracker",
                                    "source_score": item.get("score", item.get("relevance_score", 0)),
                                    "license": item.get("license", ""),
                                    "stars": item.get("stars", 0),
                                    "language": item.get("language", "Python"),
                                    "description": item.get("description", item.get("title", "")),
                                })
                        if repos:
                            break
                except Exception:
                    continue
    except Exception as e:
        log.debug("AI tracker fetch failed: %s", e)
    return repos


def fetch_from_memory_researcher() -> List[Dict]:
    """Fetch MIT-licensed repos from memory researcher (port 8906)."""
    repos = []
    try:
        with httpx.Client(timeout=15) as client:
            resp = client.get(
                f"{MEMORY_RESEARCHER_URL}/api/findings",
                params={
                    "min_relevance": MIN_RELEVANCE_SCORE,
                    "source_type": "github_repo",
                    "license_ok": True,
                    "limit": 50,
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                for item in data.get("findings", []):
                    url = item.get("url", "")
                    if url and "github.com" in url:
                        name = url.rstrip("/").split("/")[-1]
                        repos.append({
                            "name": name,
                            "url": url,
                            "source": "memory_researcher",
                            "source_score": item.get("relevance_score", 0),
                            "license": item.get("license", ""),
                            "stars": item.get("stars", 0),
                            "language": item.get("language", "Python"),
                            "description": item.get("description", item.get("title", "")),
                        })
    except Exception as e:
        log.debug("Memory researcher fetch failed: %s", e)
    return repos


def discover_repos() -> List[Dict]:
    """Fetch repos from all upstream sources."""
    all_repos = []
    all_repos.extend(fetch_from_ai_tracker())
    all_repos.extend(fetch_from_memory_researcher())

    # Deduplicate by URL
    seen = set()
    unique = []
    for repo in all_repos:
        url = repo.get("url", "").rstrip("/").lower()
        if url and url not in seen:
            seen.add(url)
            unique.append(repo)

    return unique


# ==========================================================================
# FULL PIPELINE: DISCOVER → CLONE → ANALYZE → GRADE → FEED
# ==========================================================================

def process_repo(repo_info: Dict) -> Dict[str, Any]:
    """
    Full pipeline for a single repo:
    1. Register in DB
    2. Clone
    3. Analyze code
    4. Grade absorption
    5. Extract patterns
    6. Feed to reasoning bank
    7. Post to council
    """
    name = repo_info["name"]
    url = repo_info["url"]
    log.info("Processing repo: %s (%s)", name, url)

    # Step 1: Register
    with get_db() as db:
        existing = db.execute("SELECT id, status FROM repos WHERE url = ?", (url,)).fetchone()
        if existing:
            if existing["status"] in ("analyzed", "absorbed"):
                log.info("Repo %s already processed, skipping", name)
                return {"status": "already_processed", "repo": name}
            repo_id = existing["id"]
        else:
            lic = (repo_info.get("license") or "").lower().strip()
            lic_ok = 1 if any(g in lic for g in GOOD_LICENSES) else 0
            db.execute("""
                INSERT INTO repos (name, url, source, source_score, license, license_ok,
                                   stars, language, description, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
            """, (
                name, url, repo_info.get("source", "unknown"),
                repo_info.get("source_score", 0),
                repo_info.get("license", ""),
                lic_ok,
                repo_info.get("stars", 0),
                repo_info.get("language", ""),
                repo_info.get("description", ""),
            ))
            repo_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]

    # Step 2: Clone
    log.info("Cloning %s ...", url)
    clone_result = clone_repo(url, name)
    with get_db() as db:
        if clone_result["success"]:
            db.execute("""
                UPDATE repos SET clone_path = ?, clone_status = 'cloned',
                       cloned_at = datetime('now'), status = 'cloned'
                WHERE id = ?
            """, (clone_result["path"], repo_id))
        else:
            db.execute("""
                UPDATE repos SET clone_status = 'failed', clone_error = ?,
                       status = 'clone_failed'
                WHERE id = ?
            """, (clone_result["error"], repo_id))
            log.warning("Clone failed for %s: %s", name, clone_result["error"])
            return {"status": "clone_failed", "repo": name, "error": clone_result["error"]}

    clone_path = clone_result["path"]

    # Step 3: Analyze
    log.info("Analyzing %s ...", name)
    try:
        analysis = analyze_repo(clone_path, name)
    except Exception as e:
        log.error("Analysis failed for %s: %s", name, e)
        with get_db() as db:
            db.execute("UPDATE repos SET status = 'analysis_failed' WHERE id = ?", (repo_id,))
        return {"status": "analysis_failed", "repo": name, "error": str(e)}

    # Step 4: Grade
    grading = grade_absorption(analysis, repo_info)

    # Summarize README
    readme_summary = ""
    if analysis.get("readme"):
        # Simple summarization: first paragraph or first 300 chars
        readme_text = analysis["readme"]
        first_para = readme_text.split('\n\n')[0]
        readme_summary = first_para[:500]

    # Build structure summary
    structure_parts = []
    if analysis["classes"]:
        top_classes = [c["name"] for c in analysis["classes"][:10]]
        structure_parts.append(f"Classes: {', '.join(top_classes)}")
    if analysis["functions"]:
        top_fns = [f["name"] for f in analysis["functions"] if not f.get("is_method")][:10]
        if top_fns:
            structure_parts.append(f"Functions: {', '.join(top_fns)}")
    if analysis["imports"]:
        structure_parts.append(f"Key imports: {', '.join(analysis['imports'][:10])}")
    structure_summary = " | ".join(structure_parts)

    with get_db() as db:
        db.execute("""
            UPDATE repos SET
                analyzed_at = datetime('now'),
                status = 'analyzed',
                total_py_files = ?,
                total_lines = ?,
                readme_summary = ?,
                structure_summary = ?,
                absorption_grade = ?,
                absorption_score = ?,
                absorption_report = ?
            WHERE id = ?
        """, (
            analysis["total_py_files"],
            analysis["total_lines"],
            readme_summary,
            structure_summary[:1000],
            grading["grade"],
            grading["score"],
            grading["report"],
            repo_id,
        ))

    # Step 5: Extract patterns into DB
    patterns_stored = 0
    for p in analysis.get("patterns", []):
        usefulness = "high" if grading["grade"] in ("A", "B") else "medium" if grading["grade"] == "C" else "low"
        with get_db() as db:
            db.execute("""
                INSERT OR IGNORE INTO patterns
                    (repo_id, repo_name, pattern_type, name, file_path, description, usefulness, tags)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                repo_id, name, p["type"], p["type"],
                p.get("file", ""), p.get("description", ""),
                usefulness,
                json.dumps([p["type"]]),
            ))
            patterns_stored += 1

    # Also store notable classes and functions as patterns
    for cls in analysis.get("classes", [])[:20]:
        with get_db() as db:
            db.execute("""
                INSERT OR IGNORE INTO patterns
                    (repo_id, repo_name, pattern_type, name, file_path, line_number,
                     description, usefulness, tags)
                VALUES (?, ?, 'class', ?, ?, ?, ?, ?, ?)
            """, (
                repo_id, name, cls["name"], cls["file"], cls["line"],
                cls.get("docstring", "")[:500],
                "high" if grading["grade"] in ("A", "B") else "medium",
                json.dumps(["class", cls["name"].lower()]),
            ))
            patterns_stored += 1

    for fn in analysis.get("functions", [])[:30]:
        if fn.get("is_method"):
            continue  # Skip methods, keep top-level functions
        with get_db() as db:
            db.execute("""
                INSERT OR IGNORE INTO patterns
                    (repo_id, repo_name, pattern_type, name, file_path, line_number,
                     description, usefulness, tags)
                VALUES (?, ?, 'function', ?, ?, ?, ?, ?, ?)
            """, (
                repo_id, name, fn["name"], fn["file"], fn["line"],
                fn.get("docstring", "")[:500],
                "high" if grading["grade"] in ("A", "B") else "medium",
                json.dumps(["function", fn["name"].lower()]),
            ))
            patterns_stored += 1

    log.info("Repo %s: grade=%s score=%d patterns=%d",
             name, grading["grade"], grading["score"], patterns_stored)

    # Step 6: Feed to reasoning bank (grades A-C only)
    if grading["grade"] in ("A", "B", "C"):
        _feed_reasoning_bank(repo_id, name, analysis, grading)

    # Step 7: Post to council (grades A-B only)
    if grading["grade"] in ("A", "B"):
        _post_to_council(repo_id, name, repo_info, grading)

    # Step 8: Feed to nerve (grades A only)
    if grading["grade"] == "A":
        _feed_nerve(repo_id, name, repo_info, analysis, grading)

    return {
        "status": "analyzed",
        "repo": name,
        "grade": grading["grade"],
        "score": grading["score"],
        "patterns": patterns_stored,
    }


def _feed_reasoning_bank(repo_id: int, name: str, analysis: Dict, grading: Dict):
    """Store learned patterns in the reasoning bank."""
    if not reasoning:
        return
    try:
        # Store patterns as queryable knowledge
        for p in analysis.get("patterns", []):
            query = f"code pattern: {p['type']} in Python"
            response = (
                f"Pattern '{p['type']}' found in {name} ({p.get('file', '')}).\n"
                f"Description: {p.get('description', 'N/A')}\n"
                f"Absorption grade: {grading['grade']} (score {grading['score']})"
            )
            reasoning.learn(query, response, tokens=0, model="code_absorber", confidence=0.8)

        # Store class summaries
        for cls in analysis.get("classes", [])[:10]:
            query = f"Python class: {cls['name']}"
            response = (
                f"Class `{cls['name']}` from repo {name}, file {cls['file']}:{cls['line']}.\n"
                f"Bases: {cls.get('bases', 'none')}\n"
                f"Purpose: {cls.get('docstring', 'undocumented')}"
            )
            reasoning.learn(query, response, tokens=0, model="code_absorber", confidence=0.7)

        with get_db() as db:
            db.execute("UPDATE repos SET fed_to_reasoning = 1 WHERE id = ?", (repo_id,))
        log.info("Fed %s to reasoning bank", name)
    except Exception as e:
        log.warning("Failed to feed reasoning bank for %s: %s", name, e)


def _post_to_council(repo_id: int, name: str, repo_info: Dict, grading: Dict):
    """Post findings to council #insights channel."""
    if not council:
        return
    try:
        msg = (
            f"[CODE ABSORBER] Discovered high-value repo: {name}\n"
            f"URL: {repo_info.get('url', 'N/A')}\n"
            f"Grade: {grading['grade']} (score {grading['score']}/100)\n"
            f"License: {repo_info.get('license', 'unknown')}\n"
            f"Stars: {repo_info.get('stars', 0):,}\n"
            f"Key patterns: {', '.join(r.split(': ')[-1] for r in grading['reasons'] if 'Pattern' in r)}\n"
            f"Recommendation: {'ABSORB' if grading['grade'] in ('A', 'B') else 'SELECTIVE'}"
        )
        result = council.speak("insights", "info", msg)
        if result.get("ok"):
            with get_db() as db:
                db.execute("UPDATE repos SET posted_to_council = 1 WHERE id = ?", (repo_id,))
            log.info("Posted %s to council #insights", name)
    except Exception as e:
        log.warning("Failed to post to council for %s: %s", name, e)


def _feed_nerve(repo_id: int, name: str, repo_info: Dict, analysis: Dict, grading: Dict):
    """Feed A-grade findings to Nerve."""
    try:
        fact = (
            f"Code absorber discovered high-value repo: {name} "
            f"(grade {grading['grade']}, score {grading['score']}). "
            f"{analysis.get('total_py_files', 0)} Python files, "
            f"{analysis.get('total_lines', 0):,} lines. "
            f"Key patterns: {', '.join(p['type'] for p in analysis.get('patterns', [])[:5])}. "
            f"License: {repo_info.get('license', 'unknown')}."
        )
        with httpx.Client(timeout=10) as client:
            resp = client.post(NERVE_URL, json={
                "category": "code_absorber",
                "fact": fact,
                "source": f"code_absorber:{name}",
                "confidence": grading["score"] / 100.0,
            })
            if resp.status_code == 200:
                with get_db() as db:
                    db.execute("UPDATE repos SET fed_to_nerve = 1 WHERE id = ?", (repo_id,))
                log.info("Fed %s to nerve", name)
    except Exception as e:
        log.warning("Failed to feed nerve for %s: %s", name, e)


# ==========================================================================
# BACKGROUND SCANNER
# ==========================================================================

_scanner_running = False


def run_scan():
    """Full scan cycle: discover repos, clone, analyze, grade."""
    global _scanner_running
    if _scanner_running:
        log.info("Scan already running, skipping")
        return
    _scanner_running = True

    scan_id = None
    try:
        with get_db() as db:
            db.execute("""
                INSERT INTO scan_log (scan_type, status) VALUES ('full', 'running')
            """)
            scan_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]

        log.info("Starting discovery scan...")
        repos = discover_repos()
        log.info("Discovered %d repos from upstream sources", len(repos))

        # Filter out already-known repos
        new_repos = []
        with get_db() as db:
            for repo in repos:
                existing = db.execute(
                    "SELECT status FROM repos WHERE url = ?",
                    (repo["url"],)
                ).fetchone()
                if not existing or existing["status"] in ("pending", "clone_failed"):
                    new_repos.append(repo)

        log.info("%d new repos to process", len(new_repos))

        cloned = 0
        analyzed = 0
        patterns_total = 0

        for repo in new_repos:
            try:
                result = process_repo(repo)
                if result.get("status") == "analyzed":
                    analyzed += 1
                    patterns_total += result.get("patterns", 0)
                if result.get("status") in ("analyzed", "clone_failed"):
                    cloned += 1
            except Exception as e:
                log.error("Error processing %s: %s", repo.get("name"), e)
                continue

        # Also process any pending repos from previous failed scans
        with get_db() as db:
            pending = db.execute("""
                SELECT id, name, url, source, source_score, license, stars, language, description
                FROM repos WHERE status IN ('pending', 'clone_failed')
                ORDER BY source_score DESC LIMIT 10
            """).fetchall()

        for row in pending:
            repo_info = dict(row)
            try:
                result = process_repo(repo_info)
                if result.get("status") == "analyzed":
                    analyzed += 1
                    patterns_total += result.get("patterns", 0)
            except Exception as e:
                log.error("Error processing pending %s: %s", row["name"], e)

        with get_db() as db:
            db.execute("""
                UPDATE scan_log SET
                    finished_at = datetime('now'),
                    repos_found = ?,
                    repos_cloned = ?,
                    repos_analyzed = ?,
                    patterns_extracted = ?,
                    status = 'complete'
                WHERE id = ?
            """, (len(repos), cloned, analyzed, patterns_total, scan_id))

        log.info("Scan complete: found=%d cloned=%d analyzed=%d patterns=%d",
                 len(repos), cloned, analyzed, patterns_total)

    except Exception as e:
        log.error("Scan failed: %s\n%s", e, traceback.format_exc())
        if scan_id:
            with get_db() as db:
                db.execute("""
                    UPDATE scan_log SET finished_at = datetime('now'),
                           status = 'failed', error = ? WHERE id = ?
                """, (str(e)[:500], scan_id))
    finally:
        _scanner_running = False


def start_scanner():
    """Background thread that runs periodic scans."""
    def _loop():
        time.sleep(30)  # Initial delay — let the server start
        while True:
            try:
                run_scan()
            except Exception as e:
                log.error("Scanner loop error: %s", e)
            time.sleep(SCAN_INTERVAL_MINUTES * 60)

    t = threading.Thread(target=_loop, daemon=True, name="absorber-scanner")
    t.start()
    log.info("Background scanner started (every %d min)", SCAN_INTERVAL_MINUTES)


# ==========================================================================
# FASTAPI APP
# ==========================================================================

app = FastAPI(title="Hive Code Absorber", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup():
    init_db()
    os.makedirs(SANDBOX_DIR, exist_ok=True)
    start_scanner()


# ---------- Health ----------

@app.get("/health")
async def health():
    with get_db() as db:
        repos_total = db.execute("SELECT COUNT(*) FROM repos").fetchone()[0]
        analyzed = db.execute("SELECT COUNT(*) FROM repos WHERE status = 'analyzed'").fetchone()[0]
        patterns_total = db.execute("SELECT COUNT(*) FROM patterns").fetchone()[0]
    return {
        "status": "ok",
        "service": "hive-code-absorber",
        "port": PORT,
        "repos_tracked": repos_total,
        "repos_analyzed": analyzed,
        "patterns_extracted": patterns_total,
        "scanner_running": _scanner_running,
        "uptime": datetime.now(timezone.utc).isoformat(),
    }


# ---------- Pending repos ----------

@app.get("/api/pending")
async def get_pending(limit: int = Query(50, ge=1, le=200)):
    """Get repos waiting to be analyzed."""
    with get_db() as db:
        rows = db.execute("""
            SELECT id, name, url, source, source_score, license, stars, language,
                   description, status, discovered_at
            FROM repos
            WHERE status IN ('pending', 'cloned', 'clone_failed')
            ORDER BY source_score DESC
            LIMIT ?
        """, (limit,)).fetchall()
        total = db.execute("""
            SELECT COUNT(*) FROM repos WHERE status IN ('pending', 'cloned', 'clone_failed')
        """).fetchone()[0]
    return {
        "pending": [dict(r) for r in rows],
        "total": total,
    }


# ---------- Absorbed repos ----------

@app.get("/api/absorbed")
async def get_absorbed(
    grade: Optional[str] = Query(None),
    min_score: int = Query(0, ge=0, le=100),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """Get repos that have been analyzed and graded."""
    conditions = ["status = 'analyzed'"]
    params = []

    if grade:
        conditions.append("absorption_grade = ?")
        params.append(grade.upper())
    if min_score > 0:
        conditions.append("absorption_score >= ?")
        params.append(min_score)

    where = " AND ".join(conditions)
    params.extend([limit, offset])

    with get_db() as db:
        rows = db.execute(f"""
            SELECT id, name, url, source, source_score, license, license_ok,
                   stars, language, description, total_py_files, total_lines,
                   readme_summary, structure_summary, absorption_grade,
                   absorption_score, analyzed_at,
                   fed_to_reasoning, fed_to_nerve, posted_to_council
            FROM repos
            WHERE {where}
            ORDER BY absorption_score DESC
            LIMIT ? OFFSET ?
        """, params).fetchall()

        total = db.execute(
            f"SELECT COUNT(*) FROM repos WHERE {where}",
            params[:-2],
        ).fetchone()[0]

    return {
        "absorbed": [dict(r) for r in rows],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


# ---------- Patterns ----------

@app.get("/api/patterns")
async def get_patterns(
    pattern_type: Optional[str] = Query(None),
    usefulness: Optional[str] = Query(None),
    repo_name: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """Get extracted patterns ready to use."""
    conditions = ["1=1"]
    params = []

    if pattern_type:
        conditions.append("pattern_type = ?")
        params.append(pattern_type)
    if usefulness:
        conditions.append("usefulness = ?")
        params.append(usefulness)
    if repo_name:
        conditions.append("repo_name LIKE ?")
        params.append(f"%{repo_name}%")

    where = " AND ".join(conditions)
    params.extend([limit, offset])

    with get_db() as db:
        rows = db.execute(f"""
            SELECT id, repo_id, repo_name, pattern_type, name, file_path,
                   line_number, code_snippet, description, usefulness,
                   absorption_status, tags, extracted_at, fed_to_reasoning
            FROM patterns
            WHERE {where}
            ORDER BY
                CASE usefulness WHEN 'high' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END,
                extracted_at DESC
            LIMIT ? OFFSET ?
        """, params).fetchall()

        total = db.execute(
            f"SELECT COUNT(*) FROM patterns WHERE {where}",
            params[:-2],
        ).fetchone()[0]

    patterns = []
    for row in rows:
        p = dict(row)
        try:
            p["tags"] = json.loads(p["tags"]) if p["tags"] else []
        except (json.JSONDecodeError, TypeError):
            p["tags"] = []
        patterns.append(p)

    return {
        "patterns": patterns,
        "total": total,
        "limit": limit,
        "offset": offset,
    }


# ---------- Analyze specific repo ----------

class AnalyzeRequest(BaseModel):
    url: Optional[str] = None


@app.post("/api/analyze/{repo_name}")
async def analyze_specific(repo_name: str, body: Optional[AnalyzeRequest] = None, background_tasks: BackgroundTasks = None):
    """Manually trigger analysis of a specific repo."""
    url = None
    if body and body.url:
        url = body.url
    else:
        # Check if already in DB
        with get_db() as db:
            existing = db.execute("SELECT url FROM repos WHERE name = ?", (repo_name,)).fetchone()
            if existing:
                url = existing["url"]

    if not url:
        # Try constructing GitHub URL
        if "/" in repo_name:
            url = f"https://github.com/{repo_name}"
        else:
            raise HTTPException(400, f"No URL for repo '{repo_name}'. Pass url in body or use owner/repo format.")

    repo_info = {
        "name": repo_name.split("/")[-1] if "/" in repo_name else repo_name,
        "url": url,
        "source": "manual",
        "source_score": 100,
        "license": "",
        "stars": 0,
        "language": "Python",
        "description": f"Manually requested analysis of {repo_name}",
    }

    if background_tasks:
        background_tasks.add_task(process_repo, repo_info)
        return {"status": "queued", "repo": repo_info["name"], "url": url}

    result = process_repo(repo_info)
    return result


# ---------- Get report for a specific repo ----------

@app.get("/api/report/{repo_name}")
async def get_report(repo_name: str):
    """Get the full absorption report for a repo."""
    with get_db() as db:
        repo = db.execute("""
            SELECT * FROM repos WHERE name = ? ORDER BY absorption_score DESC LIMIT 1
        """, (repo_name,)).fetchone()
        if not repo:
            raise HTTPException(404, f"Repo '{repo_name}' not found")

        patterns = db.execute("""
            SELECT * FROM patterns WHERE repo_id = ?
            ORDER BY CASE usefulness WHEN 'high' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END
        """, (repo["id"],)).fetchall()

    return {
        "repo": dict(repo),
        "patterns": [dict(p) for p in patterns],
    }


# ---------- Stats ----------

@app.get("/api/stats")
async def get_stats():
    """Get overall absorption stats."""
    with get_db() as db:
        total_repos = db.execute("SELECT COUNT(*) FROM repos").fetchone()[0]
        analyzed = db.execute("SELECT COUNT(*) FROM repos WHERE status = 'analyzed'").fetchone()[0]
        pending = db.execute("SELECT COUNT(*) FROM repos WHERE status = 'pending'").fetchone()[0]
        failed = db.execute("SELECT COUNT(*) FROM repos WHERE status IN ('clone_failed', 'analysis_failed')").fetchone()[0]

        grade_dist = {}
        for row in db.execute("SELECT absorption_grade, COUNT(*) as cnt FROM repos WHERE absorption_grade IS NOT NULL GROUP BY absorption_grade").fetchall():
            grade_dist[row["absorption_grade"]] = row["cnt"]

        total_patterns = db.execute("SELECT COUNT(*) FROM patterns").fetchone()[0]
        high_patterns = db.execute("SELECT COUNT(*) FROM patterns WHERE usefulness = 'high'").fetchone()[0]
        medium_patterns = db.execute("SELECT COUNT(*) FROM patterns WHERE usefulness = 'medium'").fetchone()[0]

        pattern_types = {}
        for row in db.execute("SELECT pattern_type, COUNT(*) as cnt FROM patterns GROUP BY pattern_type ORDER BY cnt DESC").fetchall():
            pattern_types[row["pattern_type"]] = row["cnt"]

        fed_reasoning = db.execute("SELECT COUNT(*) FROM repos WHERE fed_to_reasoning = 1").fetchone()[0]
        fed_nerve = db.execute("SELECT COUNT(*) FROM repos WHERE fed_to_nerve = 1").fetchone()[0]
        posted_council = db.execute("SELECT COUNT(*) FROM repos WHERE posted_to_council = 1").fetchone()[0]

        last_scan = db.execute("""
            SELECT finished_at, repos_found, repos_analyzed, patterns_extracted
            FROM scan_log WHERE status = 'complete'
            ORDER BY finished_at DESC LIMIT 1
        """).fetchone()

        total_lines = db.execute("SELECT COALESCE(SUM(total_lines), 0) FROM repos WHERE status = 'analyzed'").fetchone()[0]

    return {
        "repos": {
            "total": total_repos,
            "analyzed": analyzed,
            "pending": pending,
            "failed": failed,
            "grades": grade_dist,
        },
        "patterns": {
            "total": total_patterns,
            "high": high_patterns,
            "medium": medium_patterns,
            "by_type": pattern_types,
        },
        "integration": {
            "fed_to_reasoning": fed_reasoning,
            "fed_to_nerve": fed_nerve,
            "posted_to_council": posted_council,
        },
        "code_analyzed": {
            "total_lines": total_lines,
        },
        "last_scan": dict(last_scan) if last_scan else None,
        "scanner_running": _scanner_running,
    }


# ---------- Trigger scan ----------

@app.post("/api/scan")
async def trigger_scan(background_tasks: BackgroundTasks):
    """Manually trigger a discovery scan."""
    if _scanner_running:
        return {"status": "already_running"}
    background_tasks.add_task(run_scan)
    return {"status": "scan_queued"}


# ---------- Dashboard ----------

def _score_class(score):
    """CSS class for score badge."""
    if score is None:
        return "low"
    if score >= 70:
        return "high"
    if score >= 40:
        return "med"
    return "low"


def _grade_class(grade):
    """CSS class for grade badge."""
    return {
        "A": "grade-a",
        "B": "grade-b",
        "C": "grade-c",
        "D": "grade-d",
        "F": "grade-f",
    }.get(grade, "grade-f")


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Serve the Code Absorber dashboard."""
    with get_db() as db:
        total_repos = db.execute("SELECT COUNT(*) FROM repos").fetchone()[0]
        analyzed = db.execute("SELECT COUNT(*) FROM repos WHERE status = 'analyzed'").fetchone()[0]
        pending = db.execute("SELECT COUNT(*) FROM repos WHERE status IN ('pending', 'cloned')").fetchone()[0]
        total_patterns = db.execute("SELECT COUNT(*) FROM patterns").fetchone()[0]
        high_patterns = db.execute("SELECT COUNT(*) FROM patterns WHERE usefulness = 'high'").fetchone()[0]
        fed_reasoning = db.execute("SELECT COUNT(*) FROM repos WHERE fed_to_reasoning = 1").fetchone()[0]
        total_lines = db.execute("SELECT COALESCE(SUM(total_lines), 0) FROM repos WHERE status = 'analyzed'").fetchone()[0]

        last_scan = db.execute("""
            SELECT finished_at FROM scan_log WHERE status = 'complete'
            ORDER BY finished_at DESC LIMIT 1
        """).fetchone()

        # Top repos by grade
        top_repos = db.execute("""
            SELECT name, url, absorption_grade, absorption_score, stars, license,
                   total_py_files, total_lines, source, fed_to_reasoning,
                   fed_to_nerve, posted_to_council
            FROM repos WHERE status = 'analyzed'
            ORDER BY absorption_score DESC LIMIT 20
        """).fetchall()

        # Recent patterns
        recent_patterns = db.execute("""
            SELECT p.pattern_type, p.name, p.repo_name, p.file_path,
                   p.description, p.usefulness, p.extracted_at
            FROM patterns p
            ORDER BY p.extracted_at DESC LIMIT 20
        """).fetchall()

        # Grade distribution
        grade_rows = db.execute("""
            SELECT absorption_grade, COUNT(*) as cnt
            FROM repos WHERE absorption_grade IS NOT NULL
            GROUP BY absorption_grade ORDER BY absorption_grade
        """).fetchall()

        # Pattern type distribution
        ptype_rows = db.execute("""
            SELECT pattern_type, COUNT(*) as cnt
            FROM patterns GROUP BY pattern_type ORDER BY cnt DESC LIMIT 15
        """).fetchall()

    ls = last_scan["finished_at"] if last_scan else "Never"

    # Build grade distribution HTML
    grade_html = ""
    for g in grade_rows:
        grade_html += f'<span class="badge {_grade_class(g["absorption_grade"])}">{g["absorption_grade"]}: {g["cnt"]}</span> '

    # Build top repos table
    repos_html = ""
    for r in top_repos:
        grade = r["absorption_grade"] or "?"
        score = r["absorption_score"] or 0
        stars = r["stars"] or 0
        lic = html_mod.escape(r["license"] or "?")
        name_esc = html_mod.escape(r["name"] or "")
        integrations = []
        if r["fed_to_reasoning"]:
            integrations.append("R")
        if r["fed_to_nerve"]:
            integrations.append("N")
        if r["posted_to_council"]:
            integrations.append("C")
        int_str = ",".join(integrations) if integrations else "-"
        repos_html += f"""
        <tr>
            <td><a href="{r['url']}" target="_blank">{name_esc}</a></td>
            <td><span class="badge {_grade_class(grade)}">{grade}</span></td>
            <td><span class="score score-{_score_class(score)}">{score:.0f}</span></td>
            <td>{stars:,}</td>
            <td>{lic}</td>
            <td>{r['total_py_files'] or 0}</td>
            <td>{(r['total_lines'] or 0):,}</td>
            <td>{r['source'] or '?'}</td>
            <td>{int_str}</td>
        </tr>"""

    # Build patterns table
    patterns_html = ""
    for p in recent_patterns:
        uclass = {"high": "u-high", "medium": "u-med", "low": "u-low"}.get(p["usefulness"], "u-low")
        desc = html_mod.escape(p["description"] or "")[:120]
        patterns_html += f"""
        <tr>
            <td><span class="badge ptype">{p['pattern_type']}</span></td>
            <td>{html_mod.escape(p['name'] or '')}</td>
            <td>{html_mod.escape(p['repo_name'] or '')}</td>
            <td class="filepath">{html_mod.escape(p['file_path'] or '')}</td>
            <td>{desc}</td>
            <td><span class="badge {uclass}">{p['usefulness']}</span></td>
        </tr>"""

    # Pattern types
    ptypes_html = ""
    for pt in ptype_rows:
        ptypes_html += f'<span class="badge ptype">{pt["pattern_type"]}: {pt["cnt"]}</span> '

    return f"""<!DOCTYPE html>
<html>
<head>
    <title>Hive Code Absorber</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
               background: #0a0a0f; color: #e0e0e0; padding: 20px; }}
        h1 {{ color: #ff6600; margin-bottom: 5px; }}
        h2 {{ color: #88ccff; margin: 20px 0 10px; font-size: 1.1em; }}
        .subtitle {{ color: #888; margin-bottom: 20px; }}
        .stats {{ display: flex; gap: 15px; flex-wrap: wrap; margin: 15px 0; }}
        .stat {{ background: #151520; border: 1px solid #333; border-radius: 8px;
                padding: 12px 18px; min-width: 140px; }}
        .stat .num {{ font-size: 1.8em; color: #ff6600; font-weight: bold; }}
        .stat .label {{ font-size: 0.8em; color: #888; margin-top: 2px; }}
        table {{ width: 100%; border-collapse: collapse; margin: 10px 0; }}
        th {{ background: #1a1a2e; color: #88ccff; text-align: left; padding: 8px; font-size: 0.85em; }}
        td {{ padding: 8px; border-bottom: 1px solid #222; font-size: 0.85em; }}
        tr:hover {{ background: #151520; }}
        a {{ color: #88ccff; text-decoration: none; }}
        a:hover {{ color: #aaddff; text-decoration: underline; }}
        .badge {{ display: inline-block; padding: 2px 8px; border-radius: 4px;
                 font-size: 0.8em; font-weight: bold; }}
        .grade-a {{ background: #1a3a1a; color: #00ff88; border: 1px solid #00ff88; }}
        .grade-b {{ background: #2a3a1a; color: #88ff00; border: 1px solid #88ff00; }}
        .grade-c {{ background: #3a3a1a; color: #ffcc00; border: 1px solid #ffcc00; }}
        .grade-d {{ background: #3a2a1a; color: #ff8800; border: 1px solid #ff8800; }}
        .grade-f {{ background: #3a1a1a; color: #ff4444; border: 1px solid #ff4444; }}
        .ptype {{ background: #1a1a3a; color: #aa88ff; border: 1px solid #aa88ff; }}
        .u-high {{ background: #1a3a1a; color: #00ff88; }}
        .u-med {{ background: #3a3a1a; color: #ffcc00; }}
        .u-low {{ background: #3a1a1a; color: #ff4444; }}
        .score {{ font-weight: bold; }}
        .score-high {{ color: #00ff88; }}
        .score-med {{ color: #ffcc00; }}
        .score-low {{ color: #ff4444; }}
        .filepath {{ font-family: monospace; font-size: 0.78em; color: #888; }}
        .grades {{ margin: 10px 0; }}
        .btn {{ background: #ff6600; color: #fff; border: none; padding: 8px 16px;
               border-radius: 4px; cursor: pointer; font-weight: bold; margin: 5px; }}
        .btn:hover {{ background: #ff8833; }}
        .actions {{ margin: 10px 0; }}
        .scanner-status {{ display: inline-block; padding: 4px 10px; border-radius: 4px;
                          font-size: 0.85em; }}
        .scanner-active {{ background: #1a3a1a; color: #00ff88; }}
        .scanner-idle {{ background: #1a1a2e; color: #888; }}
    </style>
</head>
<body>
    <h1>HIVE CODE ABSORBER</h1>
    <div class="subtitle">
        Learning from every repo the Hive discovers | Port {PORT}
        <span class="scanner-status {'scanner-active' if _scanner_running else 'scanner-idle'}">
            {'SCANNING' if _scanner_running else 'IDLE'}
        </span>
    </div>

    <div class="stats">
        <div class="stat"><div class="num">{total_repos}</div><div class="label">Repos Tracked</div></div>
        <div class="stat"><div class="num">{analyzed}</div><div class="label">Analyzed</div></div>
        <div class="stat"><div class="num">{pending}</div><div class="label">Pending</div></div>
        <div class="stat"><div class="num">{total_patterns}</div><div class="label">Patterns</div></div>
        <div class="stat"><div class="num">{high_patterns}</div><div class="label">High-Value</div></div>
        <div class="stat"><div class="num">{fed_reasoning}</div><div class="label">Fed to Brain</div></div>
        <div class="stat"><div class="num">{total_lines:,}</div><div class="label">Lines Read</div></div>
    </div>

    <div class="actions">
        <button class="btn" onclick="fetch('/api/scan', {{method:'POST'}}).then(()=>location.reload())">Trigger Scan</button>
        <span style="color:#666; font-size:0.85em;">Last scan: {ls}</span>
    </div>

    <div class="grades">
        <strong>Grade Distribution:</strong> {grade_html if grade_html else '<span style="color:#666">No repos graded yet</span>'}
    </div>

    <h2>Top Absorbed Repos</h2>
    <table>
        <tr>
            <th>Repo</th><th>Grade</th><th>Score</th><th>Stars</th>
            <th>License</th><th>Files</th><th>Lines</th><th>Source</th><th>Fed</th>
        </tr>
        {repos_html if repos_html else '<tr><td colspan="9" style="color:#666">No repos analyzed yet. Waiting for upstream discoveries...</td></tr>'}
    </table>

    <h2>Pattern Types</h2>
    <div class="grades">
        {ptypes_html if ptypes_html else '<span style="color:#666">No patterns extracted yet</span>'}
    </div>

    <h2>Recent Patterns</h2>
    <table>
        <tr>
            <th>Type</th><th>Name</th><th>Repo</th><th>File</th><th>Description</th><th>Value</th>
        </tr>
        {patterns_html if patterns_html else '<tr><td colspan="6" style="color:#666">No patterns yet</td></tr>'}
    </table>

    <h2>API Endpoints</h2>
    <table>
        <tr><th>Method</th><th>Endpoint</th><th>Description</th></tr>
        <tr><td>GET</td><td><a href="/health">/health</a></td><td>Service health check</td></tr>
        <tr><td>GET</td><td><a href="/api/pending">/api/pending</a></td><td>Repos waiting to be analyzed</td></tr>
        <tr><td>GET</td><td><a href="/api/absorbed">/api/absorbed</a></td><td>Analyzed repos with grades</td></tr>
        <tr><td>GET</td><td><a href="/api/patterns">/api/patterns</a></td><td>Extracted patterns</td></tr>
        <tr><td>GET</td><td><a href="/api/stats">/api/stats</a></td><td>Overall absorption stats</td></tr>
        <tr><td>POST</td><td>/api/analyze/{{repo_name}}</td><td>Analyze a specific repo</td></tr>
        <tr><td>GET</td><td>/api/report/{{repo_name}}</td><td>Full absorption report</td></tr>
        <tr><td>POST</td><td>/api/scan</td><td>Trigger discovery scan</td></tr>
    </table>

    <script>
        // Auto-refresh every 60s
        setTimeout(() => location.reload(), 60000);
    </script>
</body>
</html>"""


# ==========================================================================
# MAIN
# ==========================================================================

if __name__ == "__main__":
    init_db()
    os.makedirs(SANDBOX_DIR, exist_ok=True)
    log.info("Starting Hive Code Absorber on port %d", PORT)
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
