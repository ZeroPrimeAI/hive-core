#!/usr/bin/env python3
"""
MCP Server: hive-brain
Query the trained Hive Brain for context, memory, and decisions.
Claude Code's direct line to The Hive's collective intelligence.
"""
import json
import urllib.request
from mcp.server.fastmcp import FastMCP

mcp = FastMCP(
    name="hive-brain",
    instructions="Query The Hive's trained brain for context about Chris, past decisions, "
    "architecture, anime production, business strategy, and anything discussed in previous sessions.",
)

OLLAMA = "http://localhost:11434"
MODELS = ["gemma2-hive-v9", "gemma2-hive_ops-v6", "gemma2:2b"]

def _get_model():
    try:
        r = urllib.request.urlopen(f"{OLLAMA}/api/tags", timeout=5)
        tags = json.loads(r.read())
        available = [m["name"] for m in tags.get("models", [])]
        for m in MODELS:
            if m in available or f"{m}:latest" in available:
                return m
        for m in available:
            if "hive" in m:
                return m
        return available[0] if available else "gemma2:2b"
    except:
        return "gemma2:2b"

def _query(system, question, max_tokens=300):
    model = _get_model()
    try:
        data = json.dumps({
            "model": model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": question}
            ],
            "stream": False
        }).encode()
        req = urllib.request.Request(f"{OLLAMA}/api/chat", data=data,
              headers={"Content-Type": "application/json"})
        resp = json.loads(urllib.request.urlopen(req, timeout=30).read())
        return f"[{model}] {resp.get('message', {}).get('content', 'No response')}"
    except Exception as e:
        return f"Brain offline: {e}"

@mcp.tool()
def brain_query(question: str) -> str:
    """Ask the Hive Brain anything — past decisions, Chris's preferences, architecture, strategy."""
    return _query(
        "You are The Hive's memory. You know everything about Chris (Zero), "
        "the 5-machine mesh, Ghost in the Machine anime, locksmith business, "
        "YouTube channels, training pipeline, and all past decisions. Answer concisely.",
        question
    )

@mcp.tool()
def brain_recall(topic: str) -> str:
    """Recall what Chris said or decided about a specific topic."""
    return _query(
        "You are Kael, The Hive's memory keeper. Recall everything Chris has said "
        "about the given topic. Include his exact preferences, directives, and decisions.",
        f"What has Chris said about: {topic}"
    )

@mcp.tool()
def brain_context() -> str:
    """Get full current Hive context — what should Claude Code know right now?"""
    import os, glob
    # Read urgent file if it exists
    urgent = ""
    urgent_file = "/home/zero/.claude/projects/-home-zero/memory/URGENT_next_session.md"
    if os.path.exists(urgent_file):
        with open(urgent_file) as f:
            urgent = f.read()[:2000]

    # Read kael mission
    kael = ""
    kael_file = "/home/zero/.claude/projects/-home-zero/memory/kael_mission.md"
    if os.path.exists(kael_file):
        with open(kael_file) as f:
            kael = f.read()[:1500]

    # Count resources
    shorts = len(glob.glob("/tmp/youtube_shorts_v6/*.mp4")) + len(glob.glob("/tmp/ghost_shorts_quality/*.mp4"))
    episodes = len(glob.glob("/tmp/ghost_anime_sdxl/ep*_small.mp4"))
    training = sum(1 for f in glob.glob("/tmp/training_new/*.jsonl") for _ in open(f))

    brain_response = _query(
        "You are The Hive Brain. Give a 2-sentence status update.",
        "What is the current state of The Hive?"
    )

    return f"""HIVE CONTEXT:
{urgent[:1000]}

KAEL'S MISSION:
{kael[:500]}

RESOURCES: {shorts} shorts ready, {episodes} episodes, {training} training pairs
BRAIN: {brain_response}"""

if __name__ == "__main__":
    mcp.run()
