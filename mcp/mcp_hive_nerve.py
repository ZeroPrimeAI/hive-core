#!/usr/bin/env python3
"""
MCP Server: hive-nerve
Access the Hive's central nervous system (nerve.db), quality tracker,
model router, and distillation pipeline on ZeroQ.

Tools:
  - nerve_stats: Count facts, categories, recent growth
  - nerve_search: Search nerve.db facts by keyword
  - nerve_add: Add a fact to nerve.db
  - quality_grades: Get current quality grades from quality-tracker
  - model_inventory: Get model router inventory
  - distillation_stats: Get training pair counts and domains
"""

import subprocess
import json
from mcp.server.fastmcp import FastMCP

mcp = FastMCP(
    name="hive-nerve",
    instructions="Access the Hive's knowledge base (nerve.db on ZeroQ), "
    "quality tracker (port 8879), model router (port 8878), and distillation pipeline (port 8870).",
)

ZEROQ = "zero@100.70.226.103"
NERVE_DB = "/THE_HIVE/memory/nerve.db"
SSH_TIMEOUT = 30


def _ssh_zeroq(command: str, timeout: int = SSH_TIMEOUT) -> tuple[str, str, int]:
    """Run a command on ZeroQ via SSH. Returns (stdout, stderr, returncode)."""
    try:
        result = subprocess.run(
            ["ssh", ZEROQ, command],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return result.stdout, result.stderr, result.returncode
    except subprocess.TimeoutExpired:
        return "", f"SSH command timed out after {timeout}s", 1
    except Exception as e:
        return "", f"SSH error: {str(e)}", 1


def _curl_zeroq(url: str, method: str = "GET", data: str | None = None) -> str:
    """Curl an API endpoint on ZeroQ via SSH."""
    if method == "GET":
        cmd = f"curl -s --connect-timeout 10 --max-time 20 '{url}'"
    else:
        if data:
            cmd = f"curl -s --connect-timeout 10 --max-time 20 -X {method} -H 'Content-Type: application/json' -d '{data}' '{url}'"
        else:
            cmd = f"curl -s --connect-timeout 10 --max-time 20 -X {method} '{url}'"

    stdout, stderr, rc = _ssh_zeroq(cmd)
    if rc != 0 and not stdout:
        return f"Error: {stderr}"
    return stdout


@mcp.tool(
    name="nerve_stats",
    description="Get stats from nerve.db: total facts, categories, recent growth rate, "
    "and newest facts added.",
)
def nerve_stats() -> str:
    """Get nerve.db statistics."""
    queries = [
        ("Total facts", f"sqlite3 {NERVE_DB} 'SELECT count(*) FROM knowledge'"),
        (
            "Categories",
            f"sqlite3 {NERVE_DB} \"SELECT category, count(*) as cnt FROM knowledge GROUP BY category ORDER BY cnt DESC LIMIT 20\"",
        ),
        (
            "Recent (last 24h)",
            f"sqlite3 {NERVE_DB} \"SELECT count(*) FROM knowledge WHERE timestamp > datetime('now', '-1 day')\"",
        ),
        (
            "Newest 10 facts",
            f"sqlite3 {NERVE_DB} \"SELECT category || ': ' || key || ' = ' || substr(value, 1, 80) FROM knowledge ORDER BY timestamp DESC LIMIT 10\"",
        ),
    ]

    results = []
    for label, query in queries:
        stdout, stderr, rc = _ssh_zeroq(query)
        if rc != 0:
            results.append(f"{label}: Error - {stderr.strip()}")
        else:
            results.append(f"--- {label} ---\n{stdout.strip()}")

    return "\n\n".join(results)


@mcp.tool(
    name="nerve_search",
    description="Search nerve.db facts by keyword. Searches across category, key, and value fields. "
    "Returns matching facts (up to 50 results).",
)
def nerve_search(query: str) -> str:
    """Search nerve.db by keyword."""
    # Escape single quotes for SQL safety
    safe_query = query.replace("'", "''")

    sql = (
        f"sqlite3 {NERVE_DB} "
        f"\"SELECT category || ' | ' || key || ' = ' || substr(value, 1, 120) "
        f"FROM knowledge "
        f"WHERE category LIKE '%{safe_query}%' OR key LIKE '%{safe_query}%' OR value LIKE '%{safe_query}%' "
        f"ORDER BY timestamp DESC LIMIT 50\""
    )

    stdout, stderr, rc = _ssh_zeroq(sql)
    if rc != 0:
        return f"Search error: {stderr}"
    if not stdout.strip():
        return f"No facts found matching '{query}'."

    lines = stdout.strip().split("\n")
    return f"=== Nerve Search: '{query}' ({len(lines)} results) ===\n{stdout.strip()}"


@mcp.tool(
    name="nerve_add",
    description="Add a new fact to nerve.db. Requires category (e.g., 'system', 'discovery', 'metric'), "
    "key (short identifier), and value (the fact content).",
)
def nerve_add(category: str, key: str, value: str) -> str:
    """Add a fact to nerve.db."""
    # Escape single quotes
    safe_cat = category.replace("'", "''")
    safe_key = key.replace("'", "''")
    safe_val = value.replace("'", "''")

    sql = (
        f"sqlite3 {NERVE_DB} "
        f"\"INSERT INTO knowledge (category, key, value, timestamp) "
        f"VALUES ('{safe_cat}', '{safe_key}', '{safe_val}', datetime('now'))\""
    )

    stdout, stderr, rc = _ssh_zeroq(sql)
    if rc != 0:
        return f"Failed to add fact: {stderr}"
    return f"Added to nerve.db: [{category}] {key} = {value}"


@mcp.tool(
    name="quality_grades",
    description="Get current quality grades (A-F) for all Hive processes from the quality tracker (port 8879). "
    "Shows grades, scores, and improvement trends.",
)
def quality_grades() -> str:
    """Get quality grades from the quality tracker."""
    raw = _curl_zeroq("http://localhost:8879/api/grades")
    if raw.startswith("Error:"):
        return raw

    try:
        data = json.loads(raw)
        # Format nicely
        lines = ["=== Hive Quality Grades ===\n"]
        if isinstance(data, dict):
            grades = data.get("grades", data)
            if isinstance(grades, dict):
                for process, info in sorted(grades.items()):
                    if isinstance(info, dict):
                        grade = info.get("grade", "?")
                        score = info.get("score", "?")
                        trend = info.get("trend", "")
                        lines.append(f"  {process:<30} Grade: {grade}  Score: {score}  {trend}")
                    else:
                        lines.append(f"  {process:<30} {info}")
            else:
                lines.append(str(grades))
        else:
            lines.append(str(data))
        return "\n".join(lines)
    except json.JSONDecodeError:
        return f"Quality tracker response (raw):\n{raw}"


@mcp.tool(
    name="model_inventory",
    description="Get the model router inventory (port 8878) showing all available models, "
    "their locations, and health status across all machines.",
)
def model_inventory() -> str:
    """Get model router inventory."""
    raw = _curl_zeroq("http://localhost:8878/api/inventory")
    if raw.startswith("Error:"):
        return raw

    try:
        data = json.loads(raw)
        lines = ["=== Model Router Inventory ===\n"]

        if isinstance(data, dict):
            for machine, info in sorted(data.items()):
                if machine == "cloud_brains":
                    continue
                if isinstance(info, dict):
                    models = info.get("models", [])
                    health = info.get("health", {})
                    status = health.get("status", "unknown") if isinstance(health, dict) else str(health)
                    lines.append(f"  {machine}: {len(models)} models, status={status}")
                    for m in models[:10]:
                        if isinstance(m, dict):
                            lines.append(f"    - {m.get('name', m)}")
                        else:
                            lines.append(f"    - {m}")
                    if len(models) > 10:
                        lines.append(f"    ... and {len(models) - 10} more")
                else:
                    lines.append(f"  {machine}: {info}")

            # Cloud brains separately
            if "cloud_brains" in data:
                lines.append("\n  Cloud Brains:")
                cb = data["cloud_brains"]
                if isinstance(cb, dict):
                    for brain, binfo in cb.items():
                        lines.append(f"    {brain}: {binfo}")
                elif isinstance(cb, list):
                    for b in cb:
                        lines.append(f"    {b}")

        return "\n".join(lines)
    except json.JSONDecodeError:
        return f"Model router response (raw):\n{raw}"


@mcp.tool(
    name="distillation_stats",
    description="Get distillation pipeline stats: training pair counts by domain, "
    "quality scores, and readiness for retraining.",
)
def distillation_stats() -> str:
    """Get distillation pipeline statistics."""
    # Try the API first
    raw = _curl_zeroq("http://localhost:8870/api/stats")

    # If API is down, query the DB directly
    if raw.startswith("Error:") or "Connection refused" in raw or not raw.strip():
        db_path = "/THE_HIVE/memory/distillation.db"
        queries = [
            (
                "Pairs by domain",
                f"sqlite3 {db_path} \"SELECT domain, count(*) as cnt, round(avg(quality_score), 2) as avg_q FROM training_pairs GROUP BY domain ORDER BY cnt DESC\"",
            ),
            (
                "Total pairs",
                f"sqlite3 {db_path} 'SELECT count(*) FROM training_pairs'",
            ),
            (
                "High quality (>=0.8)",
                f"sqlite3 {db_path} \"SELECT count(*) FROM training_pairs WHERE quality_score >= 0.8\"",
            ),
            (
                "Recent (24h)",
                f"sqlite3 {db_path} \"SELECT count(*) FROM training_pairs WHERE created_at > datetime('now', '-1 day')\"",
            ),
        ]

        results = ["=== Distillation Stats (from DB) ===\n"]
        for label, query in queries:
            stdout, stderr, rc = _ssh_zeroq(query)
            if rc != 0:
                results.append(f"{label}: Error - {stderr.strip()}")
            else:
                results.append(f"--- {label} ---\n{stdout.strip()}")

        return "\n\n".join(results)

    try:
        data = json.loads(raw)
        lines = ["=== Distillation Stats (from API) ===\n"]
        if isinstance(data, dict):
            for k, v in data.items():
                lines.append(f"  {k}: {v}")
        return "\n".join(lines)
    except json.JSONDecodeError:
        return f"Distillation response (raw):\n{raw}"


if __name__ == "__main__":
    mcp.run(transport="stdio")
