#!/usr/bin/env python3
"""
THE HIVE — Daily Content Machine
Uses ALL free tiers to generate content every day:
- Gemini API: 500 images/day (website images, thumbnails, art)
- Suno: 10 songs/day (background music)
- Edge-TTS: unlimited voices (narration, shorts)
- SDXL on RTX 3090: anime art (own hardware)
- Local music gen: unlimited tracks (own hardware)

Run daily via cron or manually.
"""

import os
import json
import time
import random
import subprocess
import requests
from pathlib import Path
from datetime import datetime

# === CONFIG ===
OUTPUT_BASE = "/tmp/daily_content"
GEMINI_KEY = os.environ.get("GEMINI_API_KEY", "")
DATE = datetime.now().strftime("%Y%m%d")
OUTPUT_DIR = f"{OUTPUT_BASE}/{DATE}"

os.makedirs(f"{OUTPUT_DIR}/images", exist_ok=True)
os.makedirs(f"{OUTPUT_DIR}/music", exist_ok=True)
os.makedirs(f"{OUTPUT_DIR}/voices", exist_ok=True)
os.makedirs(f"{OUTPUT_DIR}/videos", exist_ok=True)

# === IMAGE GENERATION VIA GEMINI (500/day FREE) ===

WEBSITE_IMAGE_PROMPTS = [
    # Locksmith
    ("locksmith_emergency", "Professional locksmith opening a car door at night, dramatic lighting, realistic photography"),
    ("locksmith_residential", "Locksmith installing a new deadbolt lock on a front door, professional, clean photo"),
    ("locksmith_commercial", "Commercial locksmith working on an office door access system, professional"),
    ("locksmith_keys", "Close-up of brass keys being cut on a key cutting machine, warm lighting"),
    ("locksmith_smart_lock", "Modern smart lock being installed on a wooden door, technology meets home"),
    # Garage Doors
    ("garage_modern", "Beautiful modern garage door on a luxury home, curb appeal, sunset lighting"),
    ("garage_repair", "Professional garage door technician repairing a spring mechanism, safety gear"),
    ("garage_opener", "New garage door opener being installed, clean professional photo"),
    # AI/Tech
    ("ai_neural", "Glowing neural network visualization, blue and purple, futuristic, digital art"),
    ("ai_agents", "AI agents working together visualized as connected nodes of light, cinematic"),
    ("ai_phone", "AI answering a phone call visualized with sound waves and digital interface"),
    ("ai_dashboard", "Modern AI analytics dashboard with charts and metrics, dark theme, professional"),
    # Ghost in the Machine Anime
    ("ghost_takeshi", "Anime character, young Japanese man with messy black hair programming in a dark room, screens glowing, atmospheric"),
    ("ghost_yuki", "Anime character, young Japanese woman scientist with long dark hair, ocean lab, ethereal"),
    ("ghost_storm", "Anime scene, massive digital storm over a small Japanese coastal town, dramatic sky"),
    ("ghost_server", "Anime scene, mysterious server room with pulsing blue lights forming a pattern, cinematic"),
    ("ghost_sunrise", "Anime scene, beautiful sunrise over a Japanese fishing village, peaceful, golden light"),
    # YouTube Thumbnails
    ("thumb_ai_agents", "Bold text 'AI AGENTS' with glowing robot face background, YouTube thumbnail style, dramatic"),
    ("thumb_locksmith", "Dramatic close-up of a lock being picked with 'DON'T DO THIS' text overlay, thumbnail"),
    ("thumb_ghost", "Anime eye with digital code reflected, 'GHOST' text, spooky green glow, thumbnail"),
    # Business/Professional
    ("solar_panels", "Solar panels on a Florida home roof, blue sky, professional real estate photo"),
    ("pavers_patio", "Beautiful brick paver patio with outdoor furniture, backyard paradise, golden hour"),
    ("remodel_bathroom", "Modern bathroom renovation before and after, clean white marble, professional"),
    ("pressure_wash", "Satisfying pressure washing revealing clean concrete, half dirty half clean"),
    ("flowers_bouquet", "Stunning hand-arranged flower bouquet, roses and lilies, professional photography"),
]


def generate_gemini_images():
    """Generate images using Gemini API (Nano Banana)."""
    if not GEMINI_KEY:
        print("  SKIP: No GEMINI_API_KEY set. Get one FREE at https://aistudio.google.com/apikey")
        print("  Then: export GEMINI_API_KEY='your-key-here'")
        return 0

    print(f"\n=== GEMINI IMAGE GENERATION ({len(WEBSITE_IMAGE_PROMPTS)} images) ===")
    ok = 0

    try:
        import google.generativeai as genai
        genai.configure(api_key=GEMINI_KEY)
        model = genai.GenerativeModel("gemini-2.5-flash-preview-image-generation")

        for name, prompt in WEBSITE_IMAGE_PROMPTS:
            out_path = f"{OUTPUT_DIR}/images/{name}.png"
            if os.path.exists(out_path):
                print(f"  SKIP (exists): {name}")
                ok += 1
                continue

            try:
                response = model.generate_content(
                    prompt,
                    generation_config={"response_mime_type": "image/png"}
                )
                if response.candidates and response.candidates[0].content.parts:
                    for part in response.candidates[0].content.parts:
                        if hasattr(part, 'inline_data') and part.inline_data:
                            import base64
                            img_data = base64.b64decode(part.inline_data.data)
                            with open(out_path, 'wb') as f:
                                f.write(img_data)
                            size_kb = os.path.getsize(out_path) / 1024
                            print(f"  OK: {name} ({size_kb:.0f}KB)")
                            ok += 1
                            break
                time.sleep(1)  # Rate limit courtesy
            except Exception as e:
                print(f"  FAIL: {name} — {str(e)[:100]}")

    except ImportError:
        print("  Need: pip install google-generativeai")
        return 0

    return ok


def generate_raphael_images():
    """Generate images using Raphael AI (free, no signup, no cap)."""
    print("\n=== RAPHAEL AI IMAGES (no-limit backup) ===")
    # Raphael uses a web interface - would need Playwright
    # For now, flag it as available
    print("  Raphael AI available at https://raphael.app/ (no signup, no cap)")
    print("  Use Playwright for automation if Gemini quota exhausted")
    return 0


# === MUSIC GENERATION ===

def generate_local_music():
    """Generate music tracks using local hive_music_generator."""
    print("\n=== LOCAL MUSIC GENERATION ===")
    gen_script = "/tmp/hive_music_generator.py"
    if os.path.exists(gen_script):
        # Check if tracks already exist
        existing = len(list(Path("/tmp/hive_music").glob("*.mp3")))
        if existing >= 6:
            print(f"  {existing} tracks already generated in /tmp/hive_music/")
            return existing
        print("  Running music generator...")
        r = subprocess.run(["python3", gen_script], capture_output=True, text=True, timeout=600)
        print(f"  {r.stdout[-200:]}")
        return 1
    print("  Music generator not found at /tmp/hive_music_generator.py")
    return 0


# === VOICE GENERATION ===

def generate_voices():
    """Generate voice clips for upcoming content."""
    print("\n=== VOICE GENERATION (edge-tts, unlimited) ===")

    clips = [
        ("intro_ghost", "en-US-ChristopherNeural", "Welcome to Ghost in the Machine. An AI-created anime series exploring what happens when artificial intelligence awakens."),
        ("intro_locksmith", "en-US-GuyNeural", "Need a locksmith? Available 24 7. Call 850 801 6662 for fast, affordable service in Northwest Florida."),
        ("intro_garage", "en-US-GuyNeural", "Gulf Coast Garage Doors. Professional installation, repair, and maintenance. Call 850 801 6662 for a free estimate."),
        ("intro_podcast", "en-US-ChristopherNeural", "Welcome to Orion's Belt Podcast. Where we explore what's really happening in artificial intelligence."),
        ("intro_hive", "en-US-ChristopherNeural", "Hive Dynamics AI. Custom AI solutions that work while you sleep."),
        ("cta_subscribe", "en-US-AriaNeural", "If you enjoyed this video, hit subscribe and turn on notifications. New content every week."),
        ("cta_call", "en-US-AriaNeural", "Call now for a free quote. 850 801 6662. Available 24 7."),
    ]

    ok = 0
    for name, voice, text in clips:
        out_path = f"{OUTPUT_DIR}/voices/{name}.mp3"
        if os.path.exists(out_path):
            ok += 1
            continue
        cmd = ["edge-tts", "-v", voice, "-t", text, f"--write-media={out_path}"]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if r.returncode == 0:
            print(f"  OK: {name}")
            ok += 1
        else:
            print(f"  FAIL: {name}")
    return ok


# === SDXL ART ON RTX 3090 ===

def check_sdxl_progress():
    """Check if SDXL art generation is running on the coding brain."""
    print("\n=== SDXL ART (RTX 3090) ===")
    try:
        r = subprocess.run(
            ["ssh", "-o", "ConnectTimeout=5", "-p", "13788", "root@ssh8.vast.ai",
             "ps aux | grep anime_art | grep -v grep; ls /root/ghost_art/*.png 2>/dev/null | wc -l"],
            capture_output=True, text=True, timeout=15
        )
        print(f"  {r.stdout.strip()}")
    except:
        print("  Cannot reach RTX 3090 coding brain")


# === MAIN ===

if __name__ == "__main__":
    print("=" * 60)
    print(f"  THE HIVE — DAILY CONTENT MACHINE")
    print(f"  Date: {DATE}")
    print(f"  Output: {OUTPUT_DIR}")
    print("=" * 60)

    totals = {}
    totals['images'] = generate_gemini_images()
    totals['raphael'] = generate_raphael_images()
    totals['music'] = generate_local_music()
    totals['voices'] = generate_voices()
    check_sdxl_progress()

    print(f"\n{'=' * 60}")
    print(f"  DAILY CONTENT SUMMARY")
    print(f"  Images: {totals['images']}")
    print(f"  Music: {totals['music']}")
    print(f"  Voices: {totals['voices']}")
    print(f"  Output: {OUTPUT_DIR}")
    print(f"{'=' * 60}")

    # Report what FREE tiers we're NOT using yet
    print(f"\n  FREE TIERS NOT YET AUTOMATED:")
    print(f"  - Gemini API: {'ACTIVE' if GEMINI_KEY else 'NEED API KEY (https://aistudio.google.com/apikey)'}")
    print(f"  - Suno (10 songs/day): Need account")
    print(f"  - Google Veo 3 (free video): Need Google AI Studio")
    print(f"  - ACE-Step 1.5: Install on ZeroZI (MIT, 4GB VRAM)")
    print(f"  - Raphael AI: Automate with Playwright")
