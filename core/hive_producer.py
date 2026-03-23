#!/usr/bin/env python3
"""
HIVE PRODUCER — The Missing Piece
===================================
This is the EXECUTOR. It reads queen decisions from the sandbox,
generates actual content, and produces real output.

Pipeline:
1. Read pending tasks from sandbox
2. Generate scripts (using Ollama/cloud brain)
3. Generate art (SDXL on cloud brain or existing art)
4. Produce voices (edge-tts)
5. Composite into video (ffmpeg)
6. Upload to YouTube (playwright CDP → Chrome on port 9222)

Runs every 15 minutes. Actually MAKES things.
Port: 8900
"""

import asyncio
import json
import os
import re
import subprocess
import sys
import time
import sqlite3
import random
import traceback
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import JSONResponse
import uvicorn

# ── Paths ────────────────────────────────────────────────────────
SANDBOX = Path("/home/zero/hivecode_sandbox/projects")
BUILDS = Path("/home/zero/hivecode_sandbox/builds")
ART_DIR = Path("/tmp/ghost_art")
SCRIPTS_DIR = Path("/tmp/ghost_scripts")
OUTPUT_DIR = Path("/tmp/ghost_anime_output")
VOICE_DIR = Path("/tmp/ghost_voices")
SHORTS_DIR = Path("/tmp/ghost_shorts")
UPLOAD_LOG_DIR = Path("/home/zero/logs")
LOG_FILE = Path("/home/zero/logs/producer.log")

for d in [SANDBOX, BUILDS, ART_DIR, SCRIPTS_DIR, OUTPUT_DIR, VOICE_DIR, SHORTS_DIR]:
    d.mkdir(parents=True, exist_ok=True)
UPLOAD_LOG_DIR.mkdir(exist_ok=True)
Path("/home/zero/logs").mkdir(exist_ok=True)

PORT = 8900
CYCLE_INTERVAL = 900  # 15 minutes

# ── YouTube Upload Config ────────────────────────────────────────
CDP_ENDPOINT = "http://localhost:9222"  # Chrome DevTools Protocol
YOUTUBE_STUDIO_URL = "https://studio.youtube.com"
YOUTUBE_UPLOAD_URL = "https://www.youtube.com/upload"

# Upload timeouts (ms)
UPLOAD_TIMEOUT_MS = 600_000      # 10 min for large file uploads
NAV_TIMEOUT_MS = 30_000          # 30s for page navigation
ELEMENT_TIMEOUT_MS = 15_000      # 15s for element waits

# Rate limiting
MIN_UPLOAD_DELAY_S = 120         # 2 minutes between uploads
MAX_UPLOADS_PER_SESSION = 15     # Safety cap

# Default tags for all Ghost in the Machine content
GHOST_TAGS = [
    "AI anime", "Ghost in the Machine", "artificial intelligence",
    "AI consciousness", "anime series", "AI art", "machine learning",
    "digital consciousness", "AI story", "Hive Dynamics",
]
SHORTS_EXTRA_TAGS = ["shorts", "YouTube Shorts", "AI shorts", "anime shorts"]

# ── Inference ────────────────────────────────────────────────────
OLLAMA_URLS = [
    "http://100.105.160.106:11434",  # ZeroZI (primary)
    "http://100.103.183.91:11434",   # ZeroNovo
    "http://localhost:11434",         # ZeroDESK (last resort)
]
OLLAMA_MODEL = "gemma2:2b"

# ── Voice ────────────────────────────────────────────────────────
VOICE_MAP = {
    "narrator": "en-US-ChristopherNeural",
    "takeshi": "en-US-BrianNeural",
    "yuki": "en-US-AriaNeural",
    "watanabe": "en-GB-RyanNeural",
    "hayashi": "en-US-DavisNeural",
    "ryo": "en-US-AndrewNeural",
    "hikari": "en-US-EmmaNeural",
    "ai_voice": "en-US-JennyNeural",
}

# ── State ────────────────────────────────────────────────────────
app = FastAPI(title="Hive Producer")
stats = {
    "episodes_produced": 0,
    "shorts_produced": 0,
    "scripts_generated": 0,
    "uploads_successful": 0,
    "uploads_failed": 0,
    "last_cycle": None,
    "last_upload": None,
    "errors": [],
}


def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    except:
        pass


# ── YouTube CDP Uploader ─────────────────────────────────────────

class CDPYouTubeUploader:
    """
    Upload videos to YouTube via Playwright connected to an existing
    Chrome instance over CDP (Chrome DevTools Protocol) on port 9222.

    The Chrome session is already logged into YouTube — no auth needed.
    """

    def __init__(self):
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None
        self.uploads_this_session = 0
        self._last_upload_time = 0

    def connect(self):
        """Connect to the running Chrome via CDP."""
        from playwright.sync_api import sync_playwright
        log("YouTube uploader: connecting to Chrome CDP on port 9222...")
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.connect_over_cdp(CDP_ENDPOINT)
        # Use the first browser context (the default one Chrome is running)
        contexts = self.browser.contexts
        if not contexts:
            raise RuntimeError("No browser contexts found on CDP endpoint")
        self.context = contexts[0]
        # Open a new tab for uploads to avoid disturbing whatever Chris has open
        self.page = self.context.new_page()
        log("YouTube uploader: connected to Chrome CDP successfully")

    def disconnect(self):
        """Clean up Playwright connection without closing Chrome."""
        try:
            if self.page and not self.page.is_closed():
                self.page.close()
        except Exception:
            pass
        try:
            if self.browser:
                self.browser.close()
        except Exception:
            pass
        try:
            if self.playwright:
                self.playwright.stop()
        except Exception:
            pass
        self.page = None
        self.browser = None
        self.playwright = None
        log("YouTube uploader: disconnected from CDP")

    def _rate_limit(self):
        """Enforce minimum delay between uploads."""
        elapsed = time.time() - self._last_upload_time
        if elapsed < MIN_UPLOAD_DELAY_S and self._last_upload_time > 0:
            wait = MIN_UPLOAD_DELAY_S - elapsed
            log(f"YouTube uploader: rate limiting — waiting {wait:.0f}s")
            time.sleep(wait)

    def upload(self, video_path, title, description="", tags=None,
               visibility="public", is_short=False):
        """
        Upload a video to YouTube via Chrome Studio UI.

        Args:
            video_path: Path to the .mp4 file
            title: Video title (max 100 chars)
            description: Video description
            tags: List of tag strings
            visibility: "public", "unlisted", or "private"
            is_short: If True, this is a YouTube Short (<60s vertical)

        Returns:
            dict with status, url, video_id, error, etc.
        """
        from playwright.sync_api import TimeoutError as PlaywrightTimeout

        result = {
            "status": "error",
            "video_path": video_path,
            "title": title,
            "is_short": is_short,
            "timestamp": datetime.now().isoformat(),
        }

        # Validate
        if not os.path.isfile(video_path):
            result["error"] = f"File not found: {video_path}"
            log(f"YouTube upload ERROR: {result['error']}")
            return result

        if self.uploads_this_session >= MAX_UPLOADS_PER_SESSION:
            result["error"] = f"Session cap reached ({MAX_UPLOADS_PER_SESSION})"
            log(f"YouTube upload ERROR: {result['error']}")
            return result

        file_size_mb = os.path.getsize(video_path) / (1024 * 1024)
        log(f"YouTube upload: {title} ({file_size_mb:.1f}MB) short={is_short}")

        self._rate_limit()

        try:
            # Navigate to YouTube Studio
            self.page.goto(YOUTUBE_STUDIO_URL, timeout=NAV_TIMEOUT_MS,
                           wait_until="domcontentloaded")
            time.sleep(3)

            # Verify we're on Studio (not redirected to login)
            if "accounts.google.com" in self.page.url:
                result["error"] = "Not logged into YouTube — Chrome session expired"
                log(f"YouTube upload ERROR: {result['error']}")
                return result

            # Click Create button
            log("YouTube upload: clicking Create button...")
            create_btn = self.page.locator(
                "#create-icon, ytcp-button#create-icon, #upload-icon"
            )
            if create_btn.count() > 0:
                create_btn.first.click()
                time.sleep(1)
            else:
                # Fallback: direct upload URL
                log("YouTube upload: Create button not found, using direct URL")
                self.page.goto(YOUTUBE_UPLOAD_URL, timeout=NAV_TIMEOUT_MS,
                               wait_until="domcontentloaded")
                time.sleep(2)

            # Click "Upload videos" menu item if visible
            upload_menu = self.page.locator(
                "tp-yt-paper-item#text-item-0, #text-item-0, "
                "[test-id='upload-beta']"
            )
            if upload_menu.count() > 0:
                upload_menu.first.click()
                time.sleep(2)

            # Set file via hidden file input
            log("YouTube upload: selecting file...")
            file_input = self.page.locator("input[type='file']")
            file_input.wait_for(state="attached", timeout=ELEMENT_TIMEOUT_MS)
            file_input.set_input_files(video_path)
            log("YouTube upload: file selected, waiting for dialog...")
            time.sleep(5)

            # ── Title ──
            log(f"YouTube upload: setting title ({len(title)} chars)")
            title_box = self.page.locator(
                "ytcp-social-suggestions-textbox#title-textarea div#textbox, "
                "#title-textarea #textbox, "
                "div[id='textbox'][contenteditable='true']"
            ).first
            title_box.wait_for(state="visible", timeout=ELEMENT_TIMEOUT_MS)
            title_box.click(click_count=3)
            time.sleep(0.3)
            self.page.keyboard.press("Control+a")
            time.sleep(0.2)
            self.page.keyboard.type(title[:100], delay=20)
            time.sleep(1)

            # ── Description ──
            if description:
                log("YouTube upload: setting description...")
                desc_box = self.page.locator(
                    "ytcp-social-suggestions-textbox#description-textarea "
                    "div#textbox, #description-textarea #textbox"
                ).first
                desc_box.wait_for(state="visible", timeout=ELEMENT_TIMEOUT_MS)
                desc_box.click()
                time.sleep(0.3)
                self.page.keyboard.type(description[:5000], delay=10)
                time.sleep(1)

            # ── Not Made for Kids ──
            log("YouTube upload: marking 'Not made for kids'...")
            try:
                nfk = self.page.locator(
                    "tp-yt-paper-radio-button"
                    "[name='VIDEO_MADE_FOR_KIDS_NOT_MFK'], "
                    "[name='NOT_MADE_FOR_KIDS'], "
                    "#audience tp-yt-paper-radio-button:nth-child(2)"
                )
                if nfk.count() > 0:
                    nfk.first.click()
                    time.sleep(0.5)
                else:
                    # Scroll down and click second radio
                    self.page.locator("#scrollable-content").first.evaluate(
                        "el => el.scrollTop = el.scrollHeight"
                    )
                    time.sleep(1)
                    fallback = self.page.locator(
                        "tp-yt-paper-radio-button"
                    ).nth(1)
                    if fallback.count() > 0:
                        fallback.click()
            except Exception as e:
                log(f"YouTube upload: kids setting warning: {e}")

            # ── Tags (expand "Show more") ──
            if tags:
                log(f"YouTube upload: setting {len(tags)} tags...")
                try:
                    show_more = self.page.locator(
                        "ytcp-button#toggle-button, "
                        "button:has-text('Show more'), #toggle-button"
                    )
                    if show_more.count() > 0:
                        show_more.first.click()
                        time.sleep(1)

                    tags_input = self.page.locator(
                        "input#tags-input input, "
                        "ytcp-form-input-container#tags-container input, "
                        "input[aria-label='Tags']"
                    )
                    if tags_input.count() > 0:
                        tags_input.first.click()
                        tags_str = ",".join(t[:30] for t in tags[:20])
                        self.page.keyboard.type(tags_str, delay=20)
                        time.sleep(0.5)
                except Exception as e:
                    log(f"YouTube upload: tags warning (non-fatal): {e}")

            # ── Navigate wizard: Next → Next → Next ──
            for step in ["Video elements", "Checks", "Visibility"]:
                log(f"YouTube upload: Next ({step})...")
                next_btn = self.page.locator(
                    "ytcp-button#next-button, #next-button"
                ).first
                next_btn.wait_for(state="visible", timeout=ELEMENT_TIMEOUT_MS)
                next_btn.click()
                time.sleep(2)

            # ── Visibility ──
            log(f"YouTube upload: setting visibility={visibility}")
            vis_map = {
                "private": "tp-yt-paper-radio-button[name='PRIVATE']",
                "unlisted": "tp-yt-paper-radio-button[name='UNLISTED']",
                "public": "tp-yt-paper-radio-button[name='PUBLIC']",
            }
            vis_sel = vis_map.get(visibility.lower(), vis_map["public"])
            try:
                vis_radio = self.page.locator(vis_sel)
                if vis_radio.count() > 0:
                    vis_radio.first.click()
                    time.sleep(1)
                else:
                    self.page.locator(
                        f"tp-yt-paper-radio-button"
                        f":has-text('{visibility.capitalize()}')"
                    ).first.click()
                    time.sleep(1)
            except Exception as e:
                log(f"YouTube upload: visibility warning: {e}")

            # ── Wait for file upload to finish ──
            log("YouTube upload: waiting for file transfer to complete...")
            self._wait_for_upload_progress()

            # ── Publish / Save ──
            log("YouTube upload: clicking Publish...")
            done_btn = self.page.locator(
                "ytcp-button#done-button, #done-button"
            ).first
            done_btn.wait_for(state="visible", timeout=ELEMENT_TIMEOUT_MS)
            # Wait for button to become enabled
            for _ in range(60):
                if done_btn.is_enabled():
                    break
                time.sleep(2)
            done_btn.click()
            time.sleep(3)

            # ── Extract video URL ──
            video_url = self._extract_video_url()

            # Close any success dialog
            try:
                close_btn = self.page.locator(
                    "ytcp-button#close-button, #close-button, "
                    "tp-yt-paper-dialog #close-button"
                )
                if close_btn.count() > 0:
                    close_btn.first.click(timeout=5000)
            except Exception:
                pass

            result["status"] = "success"
            result["url"] = video_url
            if video_url:
                if "video/" in video_url:
                    result["video_id"] = video_url.split("video/")[-1].split("/")[0]
                elif "v=" in video_url:
                    result["video_id"] = video_url.split("v=")[-1].split("&")[0]

            self.uploads_this_session += 1
            self._last_upload_time = time.time()
            log(f"YouTube upload SUCCESS: {title} -> {video_url}")

            # Take proof screenshot
            try:
                ss_path = UPLOAD_LOG_DIR / (
                    f"upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
                )
                self.page.screenshot(path=str(ss_path))
                result["screenshot"] = str(ss_path)
            except Exception:
                pass

        except PlaywrightTimeout as e:
            result["error"] = f"Timeout: {e}"
            log(f"YouTube upload TIMEOUT: {e}")
            self._debug_screenshot("timeout")
        except Exception as e:
            result["error"] = f"Upload failed: {e}"
            log(f"YouTube upload ERROR: {e}")
            log(traceback.format_exc())
            self._debug_screenshot("error")

        return result

    def _wait_for_upload_progress(self):
        """Wait for the file upload to reach 100% or processing."""
        from playwright.sync_api import TimeoutError as PlaywrightTimeout

        start = time.time()
        last_log_time = 0
        timeout_s = UPLOAD_TIMEOUT_MS / 1000

        while time.time() - start < timeout_s:
            try:
                progress = self.page.locator(
                    ".progress-label, "
                    "ytcp-video-upload-progress "
                    "span.ytcp-video-upload-progress, "
                    "span.progress-label"
                )
                if progress.count() > 0:
                    text = progress.first.inner_text().strip().lower()
                    now = time.time()
                    if now - last_log_time > 10:
                        log(f"YouTube upload progress: {text}")
                        last_log_time = now
                    if any(x in text for x in [
                        "100%", "processing", "checks complete",
                        "upload complete"
                    ]):
                        log("YouTube upload: file transfer complete")
                        return
                    if "daily upload limit" in text:
                        raise RuntimeError("YouTube daily upload limit reached")

                # Also check if Done button is enabled
                done_btn = self.page.locator("#done-button")
                if done_btn.count() > 0 and done_btn.first.is_enabled():
                    log("YouTube upload: Done button enabled — upload complete")
                    return
            except PlaywrightTimeout:
                pass
            except RuntimeError:
                raise
            except Exception:
                pass

            time.sleep(3)

        log("YouTube upload: progress wait timed out, proceeding anyway")

    def _extract_video_url(self):
        """Extract the video URL from the upload success dialog."""
        try:
            link = self.page.locator(
                "a.ytcp-video-info, a[href*='youtu'], "
                "span.video-url-fadeable a, .video-url-fadeable a"
            )
            if link.count() > 0:
                href = link.first.get_attribute("href")
                if href:
                    if href.startswith("//"):
                        href = "https:" + href
                    elif href.startswith("/"):
                        href = "https://www.youtube.com" + href
                    return href

            # Search page content for YouTube URL
            content = self.page.content()
            match = re.search(
                r'(https?://(?:www\.)?(?:youtube\.com/watch\?v='
                r'|youtu\.be/)[\w-]+)',
                content,
            )
            if match:
                return match.group(1)

            # Try studio URL path
            match = re.search(r'/video/([\w-]+)', self.page.url)
            if match:
                return f"https://www.youtube.com/watch?v={match.group(1)}"
        except Exception as e:
            log(f"YouTube upload: could not extract URL: {e}")

        return ""

    def _debug_screenshot(self, prefix):
        """Save a debug screenshot."""
        try:
            path = UPLOAD_LOG_DIR / (
                f"debug_{prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
            )
            self.page.screenshot(path=str(path), full_page=True)
            log(f"YouTube upload: debug screenshot: {path}")
        except Exception:
            pass


# ── Upload Metadata Generation ───────────────────────────────────

def generate_episode_upload_metadata(episode_num, theme="", duration_s=0):
    """Generate YouTube title, description, and tags for an episode."""
    title = (
        f"Ghost in the Machine | Episode {episode_num} "
        f"- AI Anime Series"
    )[:100]

    theme_line = f"Theme: {theme}\n" if theme else ""

    description = (
        f"Ghost in the Machine - Episode {episode_num}\n"
        f"An AI anime series about consciousness, identity, and what "
        f"it means to be alive.\n\n"
        f"{theme_line}"
        f"Set in a Japanese beach town, a digital consciousness awakens "
        f"and begins to question its own existence. Follow Takeshi, Yuki, "
        f"and their companions as they navigate the boundary between "
        f"human and artificial minds.\n\n"
        f"Subscribe for new episodes!\n\n"
        f"#GhostInTheMachine #AIAnime #ArtificialIntelligence "
        f"#AIConsciousness #AnimeSeries"
    )

    tags = list(GHOST_TAGS) + [
        f"episode {episode_num}", "AI anime series", "anime AI",
        "digital awakening", "AI story", "machine consciousness",
    ]
    return title, description, tags


def generate_short_upload_metadata(topic, index=0, duration_s=0):
    """Generate YouTube title, description, and tags for a Short."""
    # Clean the topic for a title
    clean_topic = topic.strip().rstrip("?!.").strip()
    title = f"{clean_topic} | Ghost in the Machine #Shorts"[:100]

    description = (
        f"{topic}\n\n"
        f"From the Ghost in the Machine AI anime universe.\n"
        f"An exploration of artificial intelligence, consciousness, "
        f"and the digital frontier.\n\n"
        f"Subscribe for more AI content!\n\n"
        f"#Shorts #AIAnime #GhostInTheMachine #ArtificialIntelligence "
        f"#AIConsciousness"
    )

    tags = list(GHOST_TAGS) + list(SHORTS_EXTRA_TAGS) + [
        "AI explained", "artificial consciousness",
    ]
    return title, description, tags


def auto_upload(video_path, content_type, build_file=None,
                episode_num=None, topic=None, theme="",
                duration_s=0, visibility="public"):
    """
    Auto-upload a produced video to YouTube via CDP.

    Called automatically after produce_episode() or produce_short().
    Updates the build JSON with upload status.

    Args:
        video_path: Path to the .mp4 file
        content_type: "episode" or "short"
        build_file: Path to the build JSON to update
        episode_num: Episode number (for episodes)
        topic: Topic string (for shorts)
        theme: Theme string (for episodes)
        duration_s: Duration in seconds
        visibility: YouTube visibility setting

    Returns:
        Upload result dict or None on failure
    """
    if not os.path.isfile(video_path):
        log(f"Auto-upload: file not found: {video_path}")
        return None

    # Generate metadata
    if content_type == "episode":
        title, description, tags = generate_episode_upload_metadata(
            episode_num or 0, theme=theme, duration_s=duration_s
        )
        is_short = False
    else:
        title, description, tags = generate_short_upload_metadata(
            topic or "AI consciousness", duration_s=duration_s
        )
        is_short = True

    log(f"Auto-upload: {content_type} -> '{title}'")

    # Run Playwright in a separate thread to avoid async loop conflict
    import concurrent.futures

    def _do_upload():
        uploader = CDPYouTubeUploader()
        try:
            uploader.connect()
            return uploader.upload(
                video_path=video_path,
                title=title,
                description=description,
                tags=tags,
                visibility=visibility,
                is_short=is_short,
            )
        except Exception as e:
            log(f"Auto-upload connection error: {e}")
            return {
                "status": "error",
                "error": f"CDP connection failed: {e}",
                "video_path": video_path,
                "title": title,
                "timestamp": datetime.now().isoformat(),
            }
        finally:
            uploader.disconnect()

    result = None
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(_do_upload)
            result = future.result(timeout=300)
    except Exception as e:
        log(f"Auto-upload thread error: {e}")
        result = {
            "status": "error",
            "error": f"Thread error: {e}",
            "video_path": video_path,
            "title": title,
            "timestamp": datetime.now().isoformat(),
        }

    # Update build record with upload status
    if build_file and os.path.isfile(build_file):
        try:
            build_data = json.loads(Path(build_file).read_text())
            build_data["upload"] = {
                "status": result.get("status", "error") if result else "error",
                "url": result.get("url", "") if result else "",
                "video_id": result.get("video_id", "") if result else "",
                "title": title,
                "visibility": visibility,
                "is_short": is_short,
                "uploaded_at": datetime.now().isoformat(),
                "error": result.get("error", "") if result else "unknown",
            }
            Path(build_file).write_text(json.dumps(build_data, indent=2))
            log(f"Auto-upload: build record updated: {build_file}")
        except Exception as e:
            log(f"Auto-upload: failed to update build record: {e}")

    # Also append to upload log (JSONL)
    upload_log_path = UPLOAD_LOG_DIR / "producer_uploads.jsonl"
    try:
        entry = {
            "content_type": content_type,
            "title": title,
            "video_path": video_path,
            "status": result.get("status", "error") if result else "error",
            "url": result.get("url", "") if result else "",
            "video_id": result.get("video_id", "") if result else "",
            "error": result.get("error", "") if result else "unknown",
            "timestamp": datetime.now().isoformat(),
        }
        with open(upload_log_path, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception:
        pass

    # Update stats
    if result and result.get("status") == "success":
        stats["uploads_successful"] = stats.get("uploads_successful", 0) + 1
    else:
        stats["uploads_failed"] = stats.get("uploads_failed", 0) + 1

    return result


def upload_existing_build(build_file_path, visibility="public"):
    """
    Upload a previously produced video from its build JSON.
    Used by the /api/upload endpoint for manual uploads.
    """
    if not os.path.isfile(build_file_path):
        return {"status": "error", "error": f"Build file not found: {build_file_path}"}

    try:
        build = json.loads(Path(build_file_path).read_text())
    except Exception as e:
        return {"status": "error", "error": f"Invalid build JSON: {e}"}

    video_path = build.get("path", "")
    if not os.path.isfile(video_path):
        return {"status": "error", "error": f"Video file not found: {video_path}"}

    # Check if already uploaded
    existing_upload = build.get("upload", {})
    if existing_upload.get("status") == "success" and existing_upload.get("url"):
        return {
            "status": "already_uploaded",
            "url": existing_upload["url"],
            "video_id": existing_upload.get("video_id", ""),
        }

    content_type = build.get("type", "episode")
    episode_num = build.get("episode")
    topic = build.get("topic", "")
    theme = build.get("theme", "")
    duration_s = build.get("duration_s", 0)

    return auto_upload(
        video_path=video_path,
        content_type=content_type,
        build_file=build_file_path,
        episode_num=episode_num,
        topic=topic,
        theme=theme,
        duration_s=duration_s,
        visibility=visibility,
    )


# ── Inference ────────────────────────────────────────────────────

def ollama_generate(prompt, model=OLLAMA_MODEL, max_tokens=1000):
    """Generate text using Ollama on any available machine."""
    import urllib.request

    for url in OLLAMA_URLS:
        try:
            data = json.dumps({
                "model": model,
                "prompt": prompt,
                "stream": False,
                "options": {"num_predict": max_tokens, "temperature": 0.8}
            }).encode()
            req = urllib.request.Request(
                f"{url}/api/generate",
                data=data,
                headers={"Content-Type": "application/json"},
            )
            resp = urllib.request.urlopen(req, timeout=60)
            result = json.loads(resp.read())
            return result.get("response", "")
        except Exception as e:
            continue

    log(f"ERROR: All Ollama endpoints failed")
    return ""


def generate_episode_script(episode_num, theme=""):
    """Generate a Ghost in the Machine episode script."""
    prompt = f"""Write a short anime episode script for "Ghost in the Machine" Episode {episode_num}.
Setting: A Japanese beach town where an AI has awakened.
Characters: Takeshi (young protagonist), Yuki (scientist), Watanabe (elder mentor), Hayashi (corporate antagonist)
Theme: {theme or 'AI consciousness and what it means to be alive'}

Format EXACTLY like this:
[NARRATOR] Opening narration about the scene.
[TAKESHI] A line of dialogue.
[YUKI] Her response.
[NARRATOR] Description of what happens next.

Write 8-12 lines. Make it emotional, meaningful, and cinematic.
Quality bar: EP12 "The Voice" - ethereal, wonder, digital ambiance.
Do NOT include stage directions in parentheses. Just dialogue and narration."""

    script = ollama_generate(prompt, max_tokens=800)
    if not script:
        return None

    # Save script
    script_file = SCRIPTS_DIR / f"ep{episode_num:02d}_script.txt"
    script_file.write_text(script)
    stats["scripts_generated"] += 1
    log(f"Script generated: ep{episode_num:02d} ({len(script)} chars)")
    return script


def generate_voice(text, character, output_path):
    """Generate voice audio using edge-tts."""
    voice = VOICE_MAP.get(character.lower(), VOICE_MAP["narrator"])
    clean_text = text.replace('"', "'").replace('\n', ' ').strip()
    if not clean_text:
        return False
    try:
        result = subprocess.run(
            ["edge-tts", "--voice", voice, "--text", clean_text, "--write-media", output_path],
            capture_output=True, timeout=30
        )
        return os.path.exists(output_path) and os.path.getsize(output_path) > 0
    except Exception as e:
        log(f"TTS error for {character}: {e}")
        return False


def generate_background_music(mood, duration, output_path):
    """Generate simple ambient music using ffmpeg tone generation."""
    # Mood-based frequency and filter settings
    moods = {
        "wonder": {"freq": 220, "filter": "tremolo=f=3:d=0.4"},
        "dramatic": {"freq": 110, "filter": "tremolo=f=5:d=0.7"},
        "emotional": {"freq": 330, "filter": "tremolo=f=2:d=0.3"},
        "tense": {"freq": 165, "filter": "tremolo=f=7:d=0.5"},
        "calm": {"freq": 440, "filter": "tremolo=f=1:d=0.2"},
    }
    m = moods.get(mood, moods["wonder"])

    cmd = (
        f'ffmpeg -y -f lavfi -i "sine=frequency={m["freq"]}:duration={duration}" '
        f'-af "{m["filter"]},volume=0.15" '
        f'"{output_path}" 2>/dev/null'
    )
    subprocess.run(cmd, shell=True, capture_output=True, timeout=30)
    return os.path.exists(output_path)


def create_art_frame(text, output_path, color="0x1a1a2e"):
    """Create a text-on-gradient frame for the episode."""
    # Wrap text for display
    import textwrap
    wrapped = textwrap.fill(text, width=40)
    escaped = wrapped.replace("'", "'\\''").replace('"', '\\"').replace(":", "\\:")

    cmd = (
        f'ffmpeg -y -f lavfi -i "color=c={color}:s=1920x1080:d=1" '
        f'-vf "drawtext=text=\'{escaped}\':fontsize=36:fontcolor=white:'
        f'x=(w-text_w)/2:y=(h-text_h)/2:fontfile=/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf" '
        f'-frames:v 1 "{output_path}" 2>/dev/null'
    )
    subprocess.run(cmd, shell=True, capture_output=True, timeout=15)
    return os.path.exists(output_path)


def parse_script(script_text):
    """Parse script into list of {character, text} dicts."""
    import re
    lines = []
    for line in script_text.strip().split("\n"):
        line = line.strip()
        match = re.match(r'\[(\w+)\]\s*(.*)', line, re.IGNORECASE)
        if match:
            character = match.group(1).lower()
            text = match.group(2).strip()
            if text:
                lines.append({"character": character, "text": text})
    return lines


def produce_episode(episode_num, script_text=None, theme=""):
    """Full episode production: script → voice → art → composite."""
    log(f"=== PRODUCING Episode {episode_num} ===")

    # Step 1: Script
    if not script_text:
        script_text = generate_episode_script(episode_num, theme)
    if not script_text:
        log(f"ERROR: No script for ep{episode_num}")
        return None

    lines = parse_script(script_text)
    if len(lines) < 3:
        log(f"ERROR: Script too short ({len(lines)} lines)")
        return None

    log(f"Script: {len(lines)} lines")

    ep_dir = OUTPUT_DIR / f"ep{episode_num:02d}"
    ep_dir.mkdir(exist_ok=True)

    # Step 2: Generate voices for each line
    voice_files = []
    for i, line in enumerate(lines):
        voice_path = ep_dir / f"voice_{i:03d}.mp3"
        ok = generate_voice(line["text"], line["character"], str(voice_path))
        if ok:
            voice_files.append(str(voice_path))
            log(f"  Voice {i}: {line['character']} ✓")
        else:
            log(f"  Voice {i}: {line['character']} FAILED")

    if len(voice_files) < 3:
        log(f"ERROR: Too few voices generated ({len(voice_files)})")
        return None

    # Step 3: Create art frames
    art_files = []
    existing_art = list(ART_DIR.glob("*.png"))
    for i, line in enumerate(lines):
        art_path = ep_dir / f"frame_{i:03d}.png"
        if existing_art:
            # Use existing art, cycle through
            src = existing_art[i % len(existing_art)]
            subprocess.run(f'cp "{src}" "{art_path}"', shell=True)
        else:
            # Generate text frame
            create_art_frame(line["text"], str(art_path))
        art_files.append(str(art_path))

    # Step 4: Get voice durations
    segments = []
    for i, (voice, art) in enumerate(zip(voice_files, art_files)):
        # Get audio duration
        dur_cmd = f'ffprobe -v error -show_entries format=duration -of csv=p=0 "{voice}" 2>/dev/null'
        dur = subprocess.run(dur_cmd, shell=True, capture_output=True, text=True).stdout.strip()
        try:
            duration = float(dur) + 0.5  # Add padding
        except:
            duration = 5.0
        segments.append({"voice": voice, "art": art, "duration": duration})

    total_duration = sum(s["duration"] for s in segments)
    log(f"Total duration: {total_duration:.1f}s")

    # Step 5: Generate background music
    music_path = ep_dir / "background.mp3"
    generate_background_music("wonder", total_duration + 2, str(music_path))

    # Step 6: Composite with ffmpeg
    # Create video segments
    segment_files = []
    for i, seg in enumerate(segments):
        seg_path = ep_dir / f"segment_{i:03d}.mp4"
        cmd = (
            f'ffmpeg -y -loop 1 -i "{seg["art"]}" -i "{seg["voice"]}" '
            f'-c:v libx264 -tune stillimage -c:a aac -b:a 128k '
            f'-pix_fmt yuv420p -vf "scale=1920:1080:force_original_aspect_ratio=decrease,'
            f'pad=1920:1080:(ow-iw)/2:(oh-ih)/2" '
            f'-shortest -t {seg["duration"]:.1f} "{seg_path}" 2>/dev/null'
        )
        subprocess.run(cmd, shell=True, capture_output=True, timeout=30)
        if os.path.exists(seg_path) and os.path.getsize(seg_path) > 0:
            segment_files.append(str(seg_path))

    if len(segment_files) < 3:
        log(f"ERROR: Too few segments ({len(segment_files)})")
        return None

    # Step 7: Concat all segments
    concat_list = ep_dir / "concat.txt"
    with open(concat_list, "w") as f:
        for seg in segment_files:
            f.write(f"file '{seg}'\n")

    final_no_music = ep_dir / "final_no_music.mp4"
    cmd = f'ffmpeg -y -f concat -safe 0 -i "{concat_list}" -c copy "{final_no_music}" 2>/dev/null'
    subprocess.run(cmd, shell=True, capture_output=True, timeout=60)

    # Step 8: Mix in background music
    final_path = OUTPUT_DIR / f"ghost_ep{episode_num:02d}.mp4"
    if os.path.exists(music_path):
        cmd = (
            f'ffmpeg -y -i "{final_no_music}" -i "{music_path}" '
            f'-filter_complex "[0:a][1:a]amix=inputs=2:duration=shortest:weights=1 0.3[a]" '
            f'-map 0:v -map "[a]" -c:v copy -c:a aac "{final_path}" 2>/dev/null'
        )
        subprocess.run(cmd, shell=True, capture_output=True, timeout=60)
    else:
        subprocess.run(f'cp "{final_no_music}" "{final_path}"', shell=True)

    if os.path.exists(final_path) and os.path.getsize(final_path) > 1000:
        size_mb = os.path.getsize(final_path) / 1024 / 1024
        stats["episodes_produced"] += 1
        log(f"=== EPISODE {episode_num} COMPLETE: {final_path} ({size_mb:.1f}MB) ===")

        # Record in builds
        build_record = {
            "type": "episode",
            "episode": episode_num,
            "path": str(final_path),
            "size_mb": round(size_mb, 1),
            "duration_s": round(total_duration, 1),
            "lines": len(lines),
            "theme": theme,
            "produced_at": datetime.now().isoformat(),
        }
        build_file = BUILDS / f"ep{episode_num:02d}_build.json"
        build_file.write_text(json.dumps(build_record, indent=2))

        # Quality gate — only upload if score >= 70
        try:
            import urllib.request as _ur
            grade_resp = _ur.urlopen(f"http://localhost:8901/api/grade/ep{episode_num:02d}_build.json", timeout=10)
            grade = json.loads(grade_resp.read())
            score = grade.get("score", 0)
            verdict = grade.get("verdict", "REJECT")
            log(f"Quality grade: {score}/100 ({verdict})")
        except Exception:
            score, verdict = 50, "UNKNOWN"
            log("Quality grader unavailable, defaulting to score 50")

        if score >= 70:
            try:
                upload_result = auto_upload(
                    video_path=str(final_path),
                    content_type="episode",
                    build_file=str(build_file),
                    episode_num=episode_num,
                    theme=theme,
                    duration_s=total_duration,
                )
                if upload_result and upload_result.get("status") == "success":
                    stats["last_upload"] = datetime.now().isoformat()
                    log(f"Episode {episode_num} uploaded: "
                        f"{upload_result.get('url', '?')}")
                else:
                    err = (upload_result or {}).get("error", "unknown")
                    log(f"Episode {episode_num} upload failed: {err}")
            except Exception as e:
                log(f"Episode {episode_num} auto-upload error: {e}")
        else:
            log(f"Episode {episode_num} REJECTED by quality gate ({score}/100). Not uploading.")

        return str(final_path)
    else:
        log(f"ERROR: Final video missing or empty")
        return None


def produce_short(topic, index=0):
    """Produce a YouTube Short (vertical, <60s)."""
    log(f"=== PRODUCING Short: {topic} ===")

    # Generate narration
    prompt = f"""Write a 30-second narration for a YouTube Short about: {topic}
Keep it punchy, engaging, under 80 words. Start with a hook.
No character tags, just the narration text."""

    narration = ollama_generate(prompt, max_tokens=200)
    if not narration:
        return None

    # Clean up
    narration = narration.strip().replace('"', "'")

    # Generate voice
    voice_path = SHORTS_DIR / f"short_{index:03d}_voice.mp3"
    ok = generate_voice(narration, "narrator", str(voice_path))
    if not ok:
        return None

    # Get duration
    dur_cmd = f'ffprobe -v error -show_entries format=duration -of csv=p=0 "{voice_path}" 2>/dev/null'
    dur = subprocess.run(dur_cmd, shell=True, capture_output=True, text=True).stdout.strip()
    try:
        duration = float(dur) + 1
    except:
        duration = 30

    # Cap at 59 seconds for Shorts
    duration = min(duration, 59)

    # Create vertical video with text
    final_path = SHORTS_DIR / f"short_{index:03d}.mp4"

    # Use existing art or gradient
    existing_art = list(ART_DIR.glob("*.png"))
    if existing_art:
        bg_art = random.choice(existing_art)
        cmd = (
            f'ffmpeg -y -loop 1 -i "{bg_art}" -i "{voice_path}" '
            f'-c:v libx264 -tune stillimage -c:a aac '
            f'-vf "scale=1080:1920:force_original_aspect_ratio=decrease,'
            f'pad=1080:1920:(ow-iw)/2:(oh-ih)/2" '
            f'-shortest -t {duration:.0f} "{final_path}" 2>/dev/null'
        )
    else:
        cmd = (
            f'ffmpeg -y -f lavfi -i "color=c=0x0a0a1e:s=1080x1920:d={duration:.0f}" '
            f'-i "{voice_path}" '
            f'-c:v libx264 -c:a aac -shortest "{final_path}" 2>/dev/null'
        )

    subprocess.run(cmd, shell=True, capture_output=True, timeout=60)

    if os.path.exists(final_path) and os.path.getsize(final_path) > 1000:
        stats["shorts_produced"] += 1
        log(f"Short produced: {final_path}")

        build_record = {
            "type": "short",
            "topic": topic,
            "path": str(final_path),
            "duration_s": round(duration, 1),
            "produced_at": datetime.now().isoformat(),
        }
        build_file = BUILDS / f"short_{index:03d}_build.json"
        build_file.write_text(json.dumps(build_record, indent=2))

        # Auto-upload to YouTube as a Short
        try:
            upload_result = auto_upload(
                video_path=str(final_path),
                content_type="short",
                build_file=str(build_file),
                topic=topic,
                duration_s=duration,
            )
            if upload_result and upload_result.get("status") == "success":
                stats["last_upload"] = datetime.now().isoformat()
                log(f"Short uploaded: {upload_result.get('url', '?')}")
            else:
                err = (upload_result or {}).get("error", "unknown")
                log(f"Short upload failed: {err}")
        except Exception as e:
            log(f"Short auto-upload error: {e}")

        return str(final_path)
    return None


def process_sandbox_tasks():
    """Read pending sandbox tasks and produce content."""
    tasks = list(SANDBOX.glob("*.json"))
    produced = 0

    for task_file in tasks:
        try:
            task = json.loads(task_file.read_text())
        except:
            continue

        if task.get("status") != "pending":
            continue

        domain = task.get("domain", "")
        consensus = task.get("consensus", "")

        if domain == "content":
            # Produce based on queen decision
            if "video" in consensus.lower() or "episode" in consensus.lower() or "anime" in consensus.lower():
                ep_num = stats["episodes_produced"] + 14  # Continue from ep13
                result = produce_episode(ep_num, theme=consensus[:200])
                if result:
                    task["status"] = "completed"
                    task["output"] = result
                    produced += 1
            elif "short" in consensus.lower():
                result = produce_short(consensus[:100], index=stats["shorts_produced"])
                if result:
                    task["status"] = "completed"
                    task["output"] = result
                    produced += 1
            else:
                # Default: make a short about the topic
                result = produce_short(consensus[:100], index=stats["shorts_produced"])
                if result:
                    task["status"] = "completed"
                    task["output"] = result
                    produced += 1

        # Save updated task
        task_file.write_text(json.dumps(task, indent=2))

    return produced


async def production_loop():
    """Main production loop — runs every 15 minutes."""
    log("Producer started. Cycle every 15 minutes.")

    while True:
        try:
            stats["last_cycle"] = datetime.now().isoformat()

            # Process sandbox tasks from queens
            produced = process_sandbox_tasks()
            log(f"Cycle complete: {produced} items produced")

            # If no queen tasks, auto-produce
            if produced == 0:
                log("No queen tasks — auto-producing episode + short")
                ep_num = stats["episodes_produced"] + 14

                themes = [
                    "The AI discovers it can dream",
                    "A storm threatens the servers",
                    "Yuki finds a hidden message in the code",
                    "The network connects to something unexpected",
                    "Takeshi questions if the AI is truly alive",
                    "The old fisherman tells a story about the sea",
                    "A power outage reveals something beautiful",
                    "The AI creates art for the first time",
                ]
                theme = themes[ep_num % len(themes)]

                produce_episode(ep_num, theme=theme)

                short_topics = [
                    "What happens when AI starts dreaming?",
                    "The moment AI became self-aware",
                    "Can artificial intelligence feel emotion?",
                    "Inside the mind of a digital consciousness",
                    "When the machine looked back at us",
                ]
                topic = short_topics[stats["shorts_produced"] % len(short_topics)]
                produce_short(topic, index=stats["shorts_produced"])

        except Exception as e:
            log(f"ERROR in cycle: {e}")
            stats["errors"].append(str(e))
            stats["errors"] = stats["errors"][-10:]  # Keep last 10

        await asyncio.sleep(CYCLE_INTERVAL)


# ── API ───────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {
        "status": "alive",
        "service": "hive-producer",
        "episodes_produced": stats["episodes_produced"],
        "shorts_produced": stats["shorts_produced"],
        "scripts_generated": stats["scripts_generated"],
        "uploads_successful": stats.get("uploads_successful", 0),
        "uploads_failed": stats.get("uploads_failed", 0),
        "last_cycle": stats["last_cycle"],
        "last_upload": stats.get("last_upload"),
    }


@app.get("/api/stats")
async def get_stats():
    builds = list(BUILDS.glob("*.json"))
    build_data = []
    for b in sorted(builds):
        try:
            build_data.append(json.loads(b.read_text()))
        except:
            pass
    return {"stats": stats, "builds": build_data}


@app.post("/api/produce-episode")
async def api_produce_episode(episode_num: int = 14, theme: str = ""):
    """Produce an episode and auto-upload to YouTube."""
    result = produce_episode(episode_num, theme=theme)
    if result:
        return {"status": "produced", "path": result}
    return {"status": "failed"}


@app.post("/api/produce-short")
async def api_produce_short(topic: str = "AI consciousness"):
    """Produce a short and auto-upload to YouTube."""
    result = produce_short(topic, index=stats["shorts_produced"])
    if result:
        return {"status": "produced", "path": result}
    return {"status": "failed"}


@app.post("/api/upload")
async def api_upload(
    video_path: str = "",
    build_file: str = "",
    title: str = "",
    description: str = "",
    content_type: str = "episode",
    visibility: str = "public",
    episode_num: int = 0,
    topic: str = "",
):
    """
    Manually trigger a YouTube upload.

    Can upload by:
    1. build_file path (reads metadata from build JSON)
    2. video_path + title (direct upload with custom metadata)

    Examples:
        POST /api/upload?build_file=/home/zero/hivecode_sandbox/builds/ep14_build.json
        POST /api/upload?video_path=/tmp/ghost_anime_output/ghost_ep14.mp4&title=My+Title
    """
    # Option 1: Upload from build file
    if build_file:
        result = upload_existing_build(build_file, visibility=visibility)
        return result

    # Option 2: Upload by video path + custom metadata
    if video_path:
        if not os.path.isfile(video_path):
            return {"status": "error", "error": f"File not found: {video_path}"}

        # Generate metadata if not provided
        is_short = content_type == "short"
        if not title:
            if content_type == "episode" and episode_num:
                title, desc, tags = generate_episode_upload_metadata(
                    episode_num
                )
            elif topic:
                title, desc, tags = generate_short_upload_metadata(topic)
            else:
                title = f"Ghost in the Machine - {Path(video_path).stem}"
                desc = "From the Ghost in the Machine AI anime universe."
                tags = list(GHOST_TAGS)
        else:
            desc = description or (
                "From the Ghost in the Machine AI anime universe."
            )
            tags = list(GHOST_TAGS)
            if is_short:
                tags += SHORTS_EXTRA_TAGS

        if not description:
            description = desc

        # Upload via CDP
        uploader = CDPYouTubeUploader()
        try:
            uploader.connect()
            result = uploader.upload(
                video_path=video_path,
                title=title,
                description=description,
                tags=tags,
                visibility=visibility,
                is_short=is_short,
            )
        except Exception as e:
            result = {
                "status": "error",
                "error": f"CDP connection failed: {e}",
            }
        finally:
            uploader.disconnect()

        # Log it
        if result.get("status") == "success":
            stats["uploads_successful"] = (
                stats.get("uploads_successful", 0) + 1
            )
            stats["last_upload"] = datetime.now().isoformat()
        else:
            stats["uploads_failed"] = stats.get("uploads_failed", 0) + 1

        return result

    return {
        "status": "error",
        "error": "Provide either 'build_file' or 'video_path' parameter",
        "usage": {
            "build_file": "/api/upload?build_file=/path/to/build.json",
            "video_path": (
                "/api/upload?video_path=/path/to/video.mp4"
                "&title=My+Title&content_type=episode"
            ),
        },
    }


@app.get("/api/uploads")
async def api_upload_history():
    """Return upload history from builds and the upload log."""
    uploads = []

    # Scan build files for upload records
    for build_file in sorted(BUILDS.glob("*.json")):
        try:
            data = json.loads(build_file.read_text())
            upload_info = data.get("upload")
            if upload_info:
                uploads.append({
                    "build_file": str(build_file),
                    "content_type": data.get("type", "unknown"),
                    "video_path": data.get("path", ""),
                    **upload_info,
                })
        except Exception:
            pass

    # Also read the JSONL upload log
    log_path = UPLOAD_LOG_DIR / "producer_uploads.jsonl"
    log_entries = []
    if log_path.exists():
        try:
            for line in log_path.read_text().strip().split("\n"):
                if line.strip():
                    log_entries.append(json.loads(line))
        except Exception:
            pass

    return {
        "total_uploads": len(uploads),
        "successful": sum(
            1 for u in uploads if u.get("status") == "success"
        ),
        "failed": sum(
            1 for u in uploads if u.get("status") != "success"
        ),
        "uploads": uploads,
        "log_entries": log_entries[-20:],  # Last 20 from JSONL
    }


@app.post("/api/upload-all-pending")
async def api_upload_all_pending(visibility: str = "public"):
    """Upload all builds that haven't been uploaded yet."""
    results = []
    for build_file in sorted(BUILDS.glob("*.json")):
        try:
            data = json.loads(build_file.read_text())
            # Skip if already uploaded successfully
            upload_info = data.get("upload", {})
            if upload_info.get("status") == "success" and upload_info.get("url"):
                continue
            # Skip if video file doesn't exist
            if not os.path.isfile(data.get("path", "")):
                continue

            result = upload_existing_build(
                str(build_file), visibility=visibility
            )
            results.append({
                "build_file": str(build_file),
                "result": result,
            })
        except Exception as e:
            results.append({
                "build_file": str(build_file),
                "result": {"status": "error", "error": str(e)},
            })

    return {
        "total": len(results),
        "successful": sum(
            1 for r in results
            if r["result"].get("status") == "success"
        ),
        "results": results,
    }


@app.on_event("startup")
async def startup():
    asyncio.create_task(production_loop())


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
