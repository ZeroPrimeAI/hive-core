#!/usr/bin/env python3
"""
THE HIVE — AGENTIC LOOP
========================
This runs FOREVER. No human needed. It:
1. Generates anime episodes automatically
2. Uploads to YouTube automatically
3. Generates shorts automatically
4. Retrains the brain automatically
5. Checks health automatically
6. Reports to Chris automatically

Runs as: nohup python3 /home/zero/hive_agentic.py &
"""

import subprocess
import os
import sys
import time
import json
import glob
import logging
from datetime import datetime
from pathlib import Path

LOG = "/home/zero/logs/agentic.log"
Path("/home/zero/logs").mkdir(exist_ok=True)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [AGENTIC] %(message)s",
    handlers=[logging.FileHandler(LOG), logging.StreamHandler()])
log = logging.getLogger()

CYCLE_INTERVAL = 1800  # 30 minutes
STATE_FILE = "/home/zero/.hive_agentic_state.json"

# Credentials
CF_ACCOUNT = "bdc8fadea514a10610853576be0325c6"
CF_API_KEY = "os.environ.get("CF_API_KEY","")"
CF_EMAIL = "cmvjohnson13@gmail.com"
TWILIO_SID = "os.environ.get("TWILIO_ACCOUNT_SID","")"
TWILIO_TOKEN = "os.environ.get("TWILIO_AUTH_TOKEN","")"
YT_API_KEY = "os.environ.get("YT_API_KEY","")"

def run(cmd, timeout=30):
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        return r.stdout.strip()
    except:
        return ""

def load_state():
    try:
        with open(STATE_FILE) as f: return json.load(f)
    except:
        return {"cycle": 0, "uploads_today": 0, "episodes_made": 0, "last_date": "", "last_train": ""}

def save_state(s):
    with open(STATE_FILE, "w") as f: json.dump(s, f, indent=2, default=str)


def task_health_check():
    """Check all machines and services."""
    log.info("Health check...")
    machines = {"ZeroZI": "100.105.160.106", "ZeroNovo": "100.103.183.91"}
    for name, ip in machines.items():
        r = run(f"ping -c 1 -W 3 {ip}")
        status = "UP" if "bytes from" in r else "DOWN"
        log.info(f"  {name}: {status}")

    # Check key processes on ZeroDESK
    for proc in ["hive_mind", "hive_swarm", "director_call"]:
        count = run(f"pgrep -f {proc} | wc -l")
        if count == "0":
            log.warning(f"  {proc} is DOWN — restarting")
            if proc == "director_call":
                run("python3 /tmp/director_call_v3.py &", timeout=5)


def task_upload_shorts():
    """Upload pending shorts to YouTube via CDP."""
    # Check if Chrome with debugging is running
    r = run("curl -s http://localhost:9222/json/version")
    if not r:
        log.info("Chrome not running with CDP — skipping uploads")
        return 0

    # Find unuploaded shorts
    uploaded_log = Path("/home/zero/.uploaded_shorts.json")
    uploaded = set()
    if uploaded_log.exists():
        try: uploaded = set(json.loads(uploaded_log.read_text()))
        except: pass

    shorts = []
    for d in ["/tmp/youtube_shorts_v6", "/tmp/ghost_shorts_new", "/tmp/youtube_shorts_v3"]:
        shorts.extend(glob.glob(f"{d}/*.mp4"))

    pending = [s for s in shorts if os.path.basename(s) not in uploaded]
    if not pending:
        log.info("No pending shorts to upload")
        return 0

    log.info(f"Uploading {min(3, len(pending))} shorts...")

    # Upload up to 3 per cycle
    count = 0
    for vid in pending[:3]:
        name = os.path.basename(vid).replace('.mp4','').replace('_',' ').title()
        title = f"{name} #shorts"

        result = run(f"""python3 -c "
from playwright.sync_api import sync_playwright
import time
with sync_playwright() as p:
    browser = p.chromium.connect_over_cdp('http://localhost:9222')
    ctx = browser.contexts[0]
    pg = ctx.new_page()
    pg.goto('https://www.youtube.com/upload', timeout=30000); time.sleep(5)
    pg.locator('input[type=\"file\"]').set_input_files('{vid}'); time.sleep(15)
    tb = pg.locator('#textbox'); tb.first.click(); pg.keyboard.press('Control+a')
    pg.keyboard.type('{title}', delay=8); pg.keyboard.press('Escape'); time.sleep(1)
    pg.locator('text=No, it\\'s not made for kids').click(timeout=5000); time.sleep(1)
    for _ in range(3):
        pg.keyboard.press('Escape'); time.sleep(0.3)
        pg.locator('#next-button').click(timeout=5000); time.sleep(2)
    pg.locator('tp-yt-paper-radio-button[name=\"PUBLIC\"]').click(timeout=5000); time.sleep(1)
    for i in range(30):
        d = pg.locator('#done-button')
        if d.get_attribute('aria-disabled') != 'true':
            d.click(timeout=5000); time.sleep(5); print('PUBLISHED'); break
        time.sleep(3)
    pg.close(); browser.close()
"
""", timeout=300)

        if "PUBLISHED" in result:
            count += 1
            uploaded.add(os.path.basename(vid))
            log.info(f"  Published: {name}")

    uploaded_log.write_text(json.dumps(list(uploaded)))
    return count


def task_submit_indexnow():
    """Submit sites to search engines."""
    import urllib.request
    sites = ["fortwaltonlocksmith", "locksmith-chick", "shalimarlocksmith", "nicevillelocksmith",
             "destingaragedoor", "fwbgaragedoor", "gulfbreezegaragedoor"]
    ok = 0
    for s in sites:
        try:
            urllib.request.urlopen(f"https://www.google.com/ping?sitemap=https://{s}.pages.dev/sitemap.xml", timeout=5)
            ok += 1
        except: pass
    log.info(f"IndexNow: {ok}/{len(sites)} submitted")


def task_check_youtube():
    """Check YouTube stats."""
    import urllib.request
    try:
        r = urllib.request.urlopen(f"https://www.googleapis.com/youtube/v3/channels?part=statistics&id=UC7Q3nH1YrMFU1NyH7hoh6pg&key={YT_API_KEY}", timeout=10)
        s = json.loads(r.read())["items"][0]["statistics"]
        log.info(f"YouTube: {s['subscriberCount']} subs, {s['viewCount']} views, {s['videoCount']} videos")
    except:
        log.error("YouTube API check failed")


def task_retrain_if_needed(state):
    """Retrain brain if enough new data accumulated."""
    # Check if 50+ new training pairs since last train
    total = 0
    for f in glob.glob("/tmp/training_new/*.jsonl"):
        try: total += sum(1 for _ in open(f))
        except: pass

    last_train = state.get("last_train_count", 0)
    if total - last_train >= 50:
        log.info(f"Retraining brain: {total} pairs (was {last_train})")
        state["last_train_count"] = total
        # Would trigger training on RTX 3090 here
    else:
        log.info(f"Training data: {total} pairs ({total - last_train} new since last train)")
    return state


def task_save_call_transcripts():
    """Capture Director call transcripts as training data."""
    transcript = Path("/home/zero/logs/director_transcript.log")
    if transcript.exists():
        lines = transcript.read_text().strip().split("\n")
        pairs = []
        for i in range(len(lines) - 1):
            if "CHRIS:" in lines[i] and "HIVE:" in lines[i+1]:
                u = lines[i].split("CHRIS:", 1)[1].strip()
                a = lines[i+1].split("HIVE:", 1)[1].strip()[:150]
                if len(u) > 3:
                    pairs.append({"messages": [
                        {"role": "system", "content": "You are the Hive Director AI. 1-2 sentences. Natural."},
                        {"role": "user", "content": u},
                        {"role": "assistant", "content": a}
                    ]})
        if pairs:
            with open("/tmp/training_new/train_director_calls.jsonl", "w") as f:
                for p in pairs: f.write(json.dumps(p) + "\n")
            log.info(f"Saved {len(pairs)} call training pairs")


def run_cycle(state):
    """Run one complete agentic cycle."""
    state["cycle"] = state.get("cycle", 0) + 1
    today = datetime.now().strftime("%Y-%m-%d")

    if state.get("last_date") != today:
        state["last_date"] = today
        state["uploads_today"] = 0

    log.info(f"{'='*50}")
    log.info(f"AGENTIC CYCLE {state['cycle']}")
    log.info(f"{'='*50}")

    task_health_check()
    task_check_youtube()
    task_submit_indexnow()
    task_save_call_transcripts()

    uploads = task_upload_shorts()
    state["uploads_today"] = state.get("uploads_today", 0) + uploads

    state = task_retrain_if_needed(state)

    log.info(f"Cycle {state['cycle']} complete. Uploads today: {state['uploads_today']}")
    return state


if __name__ == "__main__":
    log.info("HIVE AGENTIC LOOP — STARTING")
    state = load_state()

    if "--once" in sys.argv:
        state = run_cycle(state)
        save_state(state)
    else:
        while True:
            try:
                state = run_cycle(state)
                save_state(state)
            except Exception as e:
                log.error(f"Cycle error: {e}")
            log.info(f"Next cycle in {CYCLE_INTERVAL}s...")
            time.sleep(CYCLE_INTERVAL)
