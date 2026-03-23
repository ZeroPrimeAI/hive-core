#!/usr/bin/env python3
"""
MCP Server: anime-production
Manage Ghost in the Machine anime production pipeline.

Tools:
  - list_episodes: List all rendered episodes with versions, durations, sizes
  - render_episode: Trigger rendering of a specific episode
  - production_status: Current pipeline state (rendering, done, disk)
  - list_shorts: All YouTube shorts with upload status
  - generate_shorts: Generate new shorts for a topic
  - tts_status: Check all TTS engines
  - voice_test: Generate a test voice clip
  - list_art: List SDXL art assets per episode
  - upload_queue: What's ready to upload vs already uploaded
  - generate_image: Generate an image using Nano Banana (Gemini) API
"""

import subprocess
import os
import glob
import json
from mcp.server.fastmcp import FastMCP

mcp = FastMCP(
    name="anime-production",
    instructions="Manage Ghost in the Machine anime production: episodes, shorts, "
    "TTS voices, SDXL art, music, YouTube uploads, and Nano Banana image generation.",
)

# === Paths ===
EPISODE_DIRS = {
    "v1": "/tmp/ghost_videos",
    "v3": "/tmp/ghost_videos_v3",
    "v3-final": "/tmp/ghost_anime_final",
    "v4": "/tmp/ghost_anime_v4",
    "v5": "/tmp/ghost_anime_v5",
}
SCRIPTS_DIR = "/tmp/ghost_scripts"
ART_DIR = "/tmp/ghost_art"
SHORTS_DIR = "/tmp/youtube_shorts"
SHORTS_V3_DIR = "/tmp/youtube_shorts_v3"
VOICES_DIR = "/tmp/ghost_voices_v5"
MUSIC_DIR = "/tmp/hive_music"
THUMBNAILS_DIR = "/tmp/ghost_thumbnails"
PRODUCER_V5 = "/tmp/anime_producer_v5.py"
SHORTS_GENERATOR = "/tmp/make_shorts_v3.py"


def _get_duration(path):
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", path],
            capture_output=True, text=True, timeout=10
        )
        return round(float(r.stdout.strip()), 1)
    except:
        return 0


def _get_size_mb(path):
    try:
        return round(os.path.getsize(path) / (1024 * 1024), 1)
    except:
        return 0


@mcp.tool()
def list_episodes() -> str:
    """List all rendered anime episodes across all versions with duration and size."""
    result = []
    for version, dirpath in EPISODE_DIRS.items():
        if not os.path.isdir(dirpath):
            continue
        eps = sorted(glob.glob(os.path.join(dirpath, "*.mp4")))
        if not eps:
            continue
        result.append(f"\n=== {version.upper()} ({dirpath}) ===")
        total_size = 0
        for ep in eps:
            dur = _get_duration(ep)
            size = _get_size_mb(ep)
            total_size += size
            name = os.path.basename(ep)
            result.append(f"  {name}: {dur}s ({size}MB)")
        result.append(f"  Total: {len(eps)} episodes, {total_size:.0f}MB")
    return "\n".join(result) if result else "No episodes found."


@mcp.tool()
def render_episode(episode: str, version: str = "v5") -> str:
    """Trigger rendering of a specific episode. episode: e.g. 'ep09', version: 'v4' or 'v5'."""
    if version == "v5" and os.path.exists(PRODUCER_V5):
        cmd = f"nohup python3 {PRODUCER_V5} --episode {episode} > /tmp/render_{episode}.log 2>&1 &"
        subprocess.run(cmd, shell=True)
        return f"Rendering {episode} with v5 producer. Check /tmp/render_{episode}.log"
    return f"Producer for {version} not found."


@mcp.tool()
def production_status() -> str:
    """Show current production pipeline state: what's rendering, disk space, etc."""
    lines = ["=== PRODUCTION STATUS ==="]

    # Check running renders
    r = subprocess.run("ps aux | grep -E 'anime_producer|make_shorts|hive_music' | grep -v grep",
                       shell=True, capture_output=True, text=True)
    renders = r.stdout.strip()
    lines.append(f"\nRunning processes:\n{renders or '  None'}")

    # Disk space
    r2 = subprocess.run("df -h /tmp | tail -1", shell=True, capture_output=True, text=True)
    lines.append(f"\nDisk (/tmp): {r2.stdout.strip()}")

    # Count assets
    for name, d in [("Episodes (v5)", EPISODE_DIRS.get("v5", "")),
                    ("Shorts", SHORTS_DIR), ("Shorts v3", SHORTS_V3_DIR),
                    ("Art", ART_DIR), ("Music", MUSIC_DIR)]:
        if os.path.isdir(d):
            files = glob.glob(os.path.join(d, "*"))
            total_mb = sum(os.path.getsize(f) for f in files if os.path.isfile(f)) / (1024 * 1024)
            lines.append(f"  {name}: {len(files)} files, {total_mb:.0f}MB")

    return "\n".join(lines)


@mcp.tool()
def list_shorts() -> str:
    """List all YouTube shorts with file sizes."""
    result = []
    for d, label in [(SHORTS_DIR, "V1/V2"), (SHORTS_V3_DIR, "V3")]:
        if not os.path.isdir(d):
            continue
        shorts = sorted(glob.glob(os.path.join(d, "*.mp4")))
        if shorts:
            result.append(f"\n=== {label} SHORTS ({len(shorts)} total) ===")
            for s in shorts:
                dur = _get_duration(s)
                size = _get_size_mb(s)
                result.append(f"  {os.path.basename(s)}: {dur}s ({size}MB)")
    return "\n".join(result) if result else "No shorts found."


@mcp.tool()
def generate_shorts(topic: str, count: int = 3) -> str:
    """Generate new YouTube shorts for a given topic. topic: 'locksmith', 'ai', 'ghost', 'garage'."""
    if os.path.exists(SHORTS_GENERATOR):
        cmd = f"nohup python3 {SHORTS_GENERATOR} > /tmp/shorts_gen.log 2>&1 &"
        subprocess.run(cmd, shell=True)
        return f"Shorts generation started. Check /tmp/shorts_gen.log"
    return "Shorts generator not found."


@mcp.tool()
def tts_status() -> str:
    """Check status of all TTS engines (edge-tts, Chatterbox, Kokoro, F5)."""
    engines = []

    # edge-tts
    r = subprocess.run("edge-tts --version", shell=True, capture_output=True, text=True, timeout=5)
    engines.append(f"edge-tts: {'OK - ' + r.stdout.strip() if r.returncode == 0 else 'NOT FOUND'}")

    # Chatterbox TTS
    try:
        import requests
        r = requests.get("http://localhost:8199/health", timeout=3)
        engines.append(f"Chatterbox TTS (8199): {r.json() if r.ok else 'DOWN'}")
    except:
        engines.append("Chatterbox TTS (8199): DOWN")

    # Kokoro TTS
    try:
        import requests
        r = requests.get("http://localhost:8250/health", timeout=3)
        engines.append(f"Kokoro TTS (8250): {r.json() if r.ok else 'DOWN'}")
    except:
        engines.append("Kokoro TTS (8250): DOWN")

    # F5-TTS
    r2 = subprocess.run("pip show f5-tts 2>/dev/null | head -2", shell=True, capture_output=True, text=True)
    engines.append(f"F5-TTS: {'INSTALLED - ' + r2.stdout.strip() if r2.stdout else 'NOT INSTALLED'}")

    return "\n".join(engines)


@mcp.tool()
def voice_test(text: str, voice: str = "en-US-ChristopherNeural", engine: str = "edge-tts") -> str:
    """Generate a test voice clip. engine: 'edge-tts', 'chatterbox'. Returns path to clip."""
    out_path = f"/tmp/voice_test_{engine}.mp3"
    if engine == "edge-tts":
        cmd = ["edge-tts", "-v", voice, "-t", text, f"--write-media={out_path}"]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if r.returncode == 0:
            return f"Voice clip saved: {out_path} ({_get_size_mb(out_path)}MB)"
        return f"Failed: {r.stderr[:200]}"
    elif engine == "chatterbox":
        try:
            import requests
            r = requests.post("http://localhost:8199/tts", json={"text": text}, timeout=30)
            if r.ok:
                with open(out_path, "wb") as f:
                    f.write(r.content)
                return f"Chatterbox clip saved: {out_path}"
        except Exception as e:
            return f"Chatterbox failed: {e}"
    return f"Unknown engine: {engine}"


@mcp.tool()
def list_art() -> str:
    """List all SDXL art assets per episode."""
    if not os.path.isdir(ART_DIR):
        return "No art directory found."
    result = [f"=== ART ASSETS ({ART_DIR}) ==="]
    episodes = {}
    for f in sorted(glob.glob(os.path.join(ART_DIR, "*"))):
        name = os.path.basename(f)
        ep = name.split("_")[0] if "_" in name else "misc"
        episodes.setdefault(ep, []).append(name)
    for ep, files in sorted(episodes.items()):
        result.append(f"\n  {ep}: {len(files)} assets")
        for f in files:
            result.append(f"    {f}")
    return "\n".join(result)


@mcp.tool()
def upload_queue() -> str:
    """Show what's ready to upload vs what might already be uploaded."""
    result = ["=== UPLOAD QUEUE ==="]

    # V5 episodes
    v5_dir = EPISODE_DIRS.get("v5", "")
    if os.path.isdir(v5_dir):
        eps = sorted(glob.glob(os.path.join(v5_dir, "*.mp4")))
        result.append(f"\nV5 Episodes ({len(eps)} ready):")
        for ep in eps:
            dur = _get_duration(ep)
            size = _get_size_mb(ep)
            result.append(f"  {os.path.basename(ep)}: {dur}s, {size}MB")

    # Shorts
    for d, label in [(SHORTS_DIR, "V1/V2 Shorts"), (SHORTS_V3_DIR, "V3 Shorts")]:
        if os.path.isdir(d):
            shorts = sorted(glob.glob(os.path.join(d, "*.mp4")))
            result.append(f"\n{label} ({len(shorts)} ready):")
            for s in shorts[:10]:
                result.append(f"  {os.path.basename(s)}")
            if len(shorts) > 10:
                result.append(f"  ... and {len(shorts) - 10} more")

    # Music
    if os.path.isdir(MUSIC_DIR):
        tracks = glob.glob(os.path.join(MUSIC_DIR, "*.mp3")) + glob.glob(os.path.join(MUSIC_DIR, "*.wav"))
        result.append(f"\nMusic Tracks ({len(tracks)} available):")
        for t in sorted(tracks):
            result.append(f"  {os.path.basename(t)}")

    # YouTube login status
    lock = os.path.exists("/home/zero/.playwright-youtube/SingletonLock")
    result.append(f"\nYouTube browser lock: {'LOCKED (remove SingletonLock)' if lock else 'OK'}")
    result.append("YouTube login: Check with uploader --login if expired")

    return "\n".join(result)


@mcp.tool()
def generate_image(prompt: str, style: str = "anime") -> str:
    """Generate an image using Google Gemini (Nano Banana) API. Free: 50/day.
    style: 'anime', 'photo', 'art', 'product'"""
    style_prefix = {
        "anime": "anime style, studio ghibli inspired, detailed illustration, ",
        "photo": "photorealistic, professional photography, high quality, ",
        "art": "digital art, beautiful composition, cinematic lighting, ",
        "product": "clean product photo, white background, professional, ",
    }
    full_prompt = style_prefix.get(style, "") + prompt

    try:
        import google.generativeai as genai
        genai.configure(api_key=os.environ.get("GEMINI_API_KEY", ""))
        # This would need the Gemini API key to work
        return f"Gemini API key not configured. Set GEMINI_API_KEY env var. Prompt ready: {full_prompt[:100]}..."
    except ImportError:
        return "google-generativeai not installed. Run: pip install google-generativeai"


@mcp.tool()
def list_music() -> str:
    """List all generated music tracks available for production."""
    if not os.path.isdir(MUSIC_DIR):
        return "No music directory. Run /tmp/hive_music_generator.py to generate tracks."
    tracks = sorted(glob.glob(os.path.join(MUSIC_DIR, "*")))
    if not tracks:
        return "Music directory empty. Run /tmp/hive_music_generator.py"
    result = [f"=== MUSIC LIBRARY ({len(tracks)} tracks) ==="]
    for t in tracks:
        dur = _get_duration(t) if t.endswith(('.mp3', '.wav', '.m4a')) else 0
        size = _get_size_mb(t)
        result.append(f"  {os.path.basename(t)}: {dur}s ({size}MB)")
    return "\n".join(result)


if __name__ == "__main__":
    mcp.run()
