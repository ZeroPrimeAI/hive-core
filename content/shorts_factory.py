#!/usr/bin/env python3
"""
Shorts Factory — THE HIVE Multi-Channel YouTube Shorts Producer
================================================================
Generates vertical shorts (1080x1920, <60s) for ALL Hive YouTube channels:
  - ghost    : Ghost in the Machine anime teasers / trailers
  - locksmith: NW Florida locksmith promotional shorts
  - ai       : Educational AI/tech content
  - hive     : Behind-the-scenes Hive Dynamics content

Uses: edge-tts (voiceover), ffmpeg (video), Pillow (frames), pure Python (music)
CLI:  python3 shorts_factory.py --channel ghost --count 5
      python3 shorts_factory.py --channel all --count 3
      python3 shorts_factory.py --list
      python3 shorts_factory.py --channel locksmith --pick 0,2,4

Author: THE HIVE
"""

import subprocess
import os
import sys
import glob
import random
import json
import math
import struct
import wave
import tempfile
import shutil
import argparse
import time
from pathlib import Path
from datetime import datetime

try:
    from PIL import Image, ImageDraw, ImageFont, ImageFilter
    import numpy as np
except ImportError:
    print("ERROR: Missing dependencies. Run: pip install Pillow numpy")
    sys.exit(1)

# ============================================================
# CONFIGURATION
# ============================================================
OUTPUT_DIR = "/tmp/ghost_shorts"
GHOST_ART_DIR = "/tmp/ghost_art"
TEMP_DIR = "/tmp/_shorts_factory_tmp"
W, H = 1080, 1920
FPS = 30

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(TEMP_DIR, exist_ok=True)

# Voice map per channel
VOICE_MAP = {
    "ghost": "en-US-ChristopherNeural",
    "locksmith": "en-US-GuyNeural",
    "ai": "en-US-BrianNeural",
    "hive": "en-US-AndrewNeural",
}

# Channel descriptions (for metadata)
CHANNEL_META = {
    "ghost": {
        "channel_name": "Ghost in the Machine",
        "category": "Entertainment",
        "tags_base": ["anime", "ai", "ghost in the machine", "artificial intelligence",
                      "anime series", "sci-fi", "technology", "machine learning"],
    },
    "locksmith": {
        "channel_name": "NW Florida Locksmith",
        "category": "Howto & Style",
        "tags_base": ["locksmith", "nw florida", "emergency locksmith", "locked out",
                      "car lockout", "24/7 locksmith", "pensacola", "destin", "fort walton beach"],
    },
    "ai": {
        "channel_name": "Hive Dynamics AI",
        "category": "Science & Technology",
        "tags_base": ["artificial intelligence", "AI", "machine learning", "fine-tuning",
                      "AI agents", "automation", "tech education", "ai explained"],
    },
    "hive": {
        "channel_name": "Hive Dynamics",
        "category": "Science & Technology",
        "tags_base": ["hive dynamics", "ai swarm", "multi-agent", "behind the scenes",
                      "ai development", "indie ai", "ai startup"],
    },
}

# ============================================================
# FONTS
# ============================================================
FONT_PATHS = [
    "/usr/share/fonts/truetype/noto/NotoSans-Bold.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
]
FONT_REGULAR_PATHS = [
    "/usr/share/fonts/truetype/noto/NotoSans-Regular.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
]

def _find_font(paths):
    for p in paths:
        if os.path.exists(p):
            return p
    return paths[-1]  # fallback

FONT_BOLD_PATH = _find_font(FONT_PATHS)
FONT_REGULAR_PATH = _find_font(FONT_REGULAR_PATHS)

def get_font(size, bold=True):
    try:
        return ImageFont.truetype(FONT_BOLD_PATH if bold else FONT_REGULAR_PATH, size)
    except Exception:
        return ImageFont.load_default()


# ============================================================
# GRAPHICS HELPERS
# ============================================================
def make_gradient(w, h, color1, color2):
    """Fast numpy gradient."""
    arr = np.zeros((h, w, 3), dtype=np.uint8)
    for ch in range(3):
        arr[:, :, ch] = np.linspace(color1[ch], color2[ch], h, dtype=np.uint8)[:, None]
    return Image.fromarray(arr)


def make_radial_gradient(w, h, center_color, edge_color):
    """Radial gradient from center outward."""
    arr = np.zeros((h, w, 3), dtype=np.uint8)
    cy, cx = h // 2, w // 2
    max_dist = math.sqrt(cx**2 + cy**2)
    ys, xs = np.mgrid[0:h, 0:w]
    dist = np.sqrt((xs - cx)**2 + (ys - cy)**2) / max_dist
    dist = np.clip(dist, 0, 1)
    for ch in range(3):
        arr[:, :, ch] = (center_color[ch] * (1 - dist) + edge_color[ch] * dist).astype(np.uint8)
    return Image.fromarray(arr)


def add_particles(img, frame_num, count=15, color=(255, 255, 255)):
    """Floating particle/bokeh effects."""
    draw = ImageDraw.Draw(img, "RGBA")
    random.seed(42)
    for i in range(count):
        base_x = random.randint(0, W)
        base_y = random.randint(0, H)
        speed = random.uniform(0.3, 1.5)
        size = random.randint(3, 12)
        alpha = random.randint(30, 80)
        y = (base_y - frame_num * speed * 2) % H
        x = base_x + math.sin(frame_num * 0.03 + i) * 20
        r, g, b = color
        draw.ellipse([x - size, y - size, x + size, y + size],
                     fill=(r, g, b, alpha))
    return img


def draw_outlined_text(draw, pos, text, font, fill=(255, 255, 255),
                       outline=(0, 0, 0), outline_width=3):
    """Text with outline for readability over any background."""
    x, y = pos
    for dx in range(-outline_width, outline_width + 1):
        for dy in range(-outline_width, outline_width + 1):
            if dx * dx + dy * dy <= outline_width * outline_width:
                draw.text((x + dx, y + dy), text, font=font, fill=outline)
    draw.text((x, y), text, font=font, fill=fill)


def get_text_size(text, font):
    bbox = font.getbbox(text)
    return bbox[2] - bbox[0], bbox[3] - bbox[1]


def wrap_text(text, font, max_width):
    words = text.split()
    lines = []
    current = ""
    for word in words:
        test = f"{current} {word}".strip()
        tw, _ = get_text_size(test, font)
        if tw > max_width and current:
            lines.append(current)
            current = word
        else:
            current = test
    if current:
        lines.append(current)
    return lines


def draw_centered_text(draw, y, text, font, fill=(255, 255, 255),
                       outline=(0, 0, 0), outline_width=3):
    """Draw text centered horizontally at given y."""
    tw, th = get_text_size(text, font)
    x = (W - tw) // 2
    draw_outlined_text(draw, (x, y), text, font, fill, outline, outline_width)
    return th


def draw_wrapped_centered(draw, y, text, font, max_width=None, fill=(255, 255, 255),
                          outline=(0, 0, 0), outline_width=3, line_spacing=10):
    """Draw wrapped text, centered, returns total height."""
    if max_width is None:
        max_width = W - 120
    lines = wrap_text(text, font, max_width)
    total_h = 0
    for line in lines:
        th = draw_centered_text(draw, y, line, font, fill, outline, outline_width)
        y += th + line_spacing
        total_h += th + line_spacing
    return total_h


# ============================================================
# MUSIC GENERATION (pure Python, no deps)
# ============================================================
def generate_music(duration, mood="epic", output_path=None):
    """Generate background music with chord progressions."""
    if output_path is None:
        output_path = os.path.join(TEMP_DIR, f"music_{random.randint(1000, 9999)}.wav")

    sample_rate = 44100
    total_samples = int(duration * sample_rate)

    progressions = {
        "epic":    [(130.8, 164.8, 196.0), (146.8, 185.0, 220.0),
                    (155.6, 196.0, 233.1), (130.8, 164.8, 196.0)],
        "urgent":  [(146.8, 174.6, 220.0), (164.8, 196.0, 246.9),
                    (174.6, 220.0, 261.6), (146.8, 174.6, 220.0)],
        "upbeat":  [(196.0, 246.9, 293.7), (220.0, 277.2, 329.6),
                    (246.9, 311.1, 370.0), (196.0, 246.9, 293.7)],
        "dark":    [(110.0, 130.8, 164.8), (116.5, 146.8, 174.6),
                    (123.5, 155.6, 185.0), (110.0, 130.8, 164.8)],
        "chill":   [(196.0, 246.9, 293.7), (174.6, 220.0, 261.6),
                    (164.8, 207.7, 246.9), (196.0, 246.9, 293.7)],
        "wonder":  [(164.8, 196.0, 246.9), (174.6, 220.0, 277.2),
                    (196.0, 246.9, 311.1), (164.8, 196.0, 246.9)],
        "techy":   [(146.8, 185.0, 220.0), (164.8, 196.0, 246.9),
                    (174.6, 220.0, 261.6), (155.6, 196.0, 233.1)],
    }
    chords = progressions.get(mood, progressions["epic"])

    samples = []
    beat_interval = sample_rate // 3

    for i in range(total_samples):
        t = i / sample_rate
        progress = i / total_samples
        chord_idx = int(progress * len(chords)) % len(chords)
        chord = chords[chord_idx]

        val = 0
        for j, freq in enumerate(chord):
            vol = 0.08 / (j + 1)
            val += vol * math.sin(2 * math.pi * freq * t)
            val += vol * 0.3 * math.sin(2 * math.pi * freq * 2 * t)

        bass_freq = chord[0] / 2
        val += 0.06 * math.sin(2 * math.pi * bass_freq * t)

        beat_pos = (i % beat_interval) / beat_interval
        sidechain = 0.4 + 0.6 * min(beat_pos * 4, 1.0)
        val *= sidechain

        if i % (beat_interval // 2) < sample_rate // 50:
            val += random.uniform(-0.02, 0.02)

        if t < 0.8:
            val *= t / 0.8
        if t > duration - 1.5:
            val *= max(0, (duration - t) / 1.5)

        val = max(-0.95, min(0.95, val))
        samples.append(val)

    with wave.open(output_path, "w") as wf:
        wf.setnchannels(2)
        wf.setsampwidth(2)
        wf.setframerate(sample_rate)
        for s in samples:
            packed = struct.pack("<h", int(s * 16000))
            wf.writeframes(packed * 2)

    return output_path


# ============================================================
# FRAME RENDERERS — one per channel style
# ============================================================

def _render_common_elements(draw, frame, frame_num, total_frames, config, t, total_time):
    """Shared rendering: progress bar, subscribe CTA."""
    accent = tuple(config.get("accent_rgb", (0, 184, 148)))

    # Top/bottom accent bars
    draw.rectangle([(0, 0), (W, 5)], fill=accent)
    draw.rectangle([(0, H - 5), (W, H)], fill=accent)

    # Subscribe CTA (last 5 seconds)
    if t >= total_time - 5:
        sub_font = get_font(30)
        sub_text = config.get("subscribe_text", "SUBSCRIBE for more!")
        sw, sh = get_text_size(sub_text, sub_font)
        draw_outlined_text(draw, ((W - sw) // 2, 1700), sub_text, sub_font,
                          fill=(255, 200, 0), outline=(0, 0, 0), outline_width=2)

    # Progress bar
    progress = frame_num / max(total_frames - 1, 1)
    bar_width = int(W * progress)
    draw.rectangle([(0, H - 4), (bar_width, H)], fill=accent)


def render_ghost_frame(config, frame_num, total_frames):
    """Ghost in the Machine anime style: dark, neon, cyberpunk."""
    t = frame_num / FPS
    total_time = total_frames / FPS
    accent = tuple(config.get("accent_rgb", (0, 255, 136)))
    palette = config.get("palette", [(0, 5, 10), (0, 15, 20), (0, 8, 15)])

    # Background: use SDXL art if available, else dark gradient
    bg_img = config.get("_bg_img")
    if bg_img:
        frame = bg_img.copy()
        if frame.size != (W, H):
            # Crop to aspect ratio then resize
            src_w, src_h = frame.size
            target_ratio = W / H
            src_ratio = src_w / src_h
            if src_ratio > target_ratio:
                new_w = int(src_h * target_ratio)
                left = (src_w - new_w) // 2
                frame = frame.crop((left, 0, left + new_w, src_h))
            else:
                new_h = int(src_w / target_ratio)
                top = (src_h - new_h) // 2
                frame = frame.crop((0, top, src_w, top + new_h))
            frame = frame.resize((W, H), Image.LANCZOS)
        # Dark overlay for text readability
        overlay = Image.new("RGBA", (W, H), (0, 0, 0, 160))
        frame = frame.convert("RGBA")
        frame = Image.alpha_composite(frame, overlay).convert("RGB")
    else:
        # Dynamic dark gradient
        p_t = frame_num / max(total_frames, 1)
        c1_idx = int(p_t * (len(palette) - 1))
        c2_idx = min(c1_idx + 1, len(palette) - 1)
        local_t = (p_t * (len(palette) - 1)) - c1_idx
        c1 = tuple(int(palette[c1_idx][i] + (palette[c2_idx][i] - palette[c1_idx][i]) * local_t)
                   for i in range(3))
        c2 = tuple(max(0, c - 40) for c in c1)
        frame = make_gradient(W, H, c1, c2)

    # Neon particles
    frame = frame.convert("RGBA")
    frame = add_particles(frame, frame_num, count=20, color=accent)
    frame = frame.convert("RGB")

    draw = ImageDraw.Draw(frame)

    # Brand
    brand_font = get_font(26)
    brand = config.get("brand", "GHOST IN THE MACHINE")
    bw, _ = get_text_size(brand, brand_font)
    draw_outlined_text(draw, ((W - bw) // 2, 55), brand, brand_font,
                      fill=accent, outline=(0, 0, 0), outline_width=2)

    # Hook (first 3.5 seconds)
    hook = config.get("hook", "")
    if hook and t < 3.5:
        hook_font = get_font(min(82, max(56, 4800 // max(len(hook), 1))))
        lines = wrap_text(hook, hook_font, W - 100)
        y = 350
        for line in lines:
            draw_centered_text(draw, y, line, hook_font, fill=accent,
                             outline=(0, 0, 0), outline_width=4)
            y += get_text_size(line, hook_font)[1] + 10

    # Title (after 2s)
    title = config.get("title", "")
    if title and t >= 2.0:
        title_font = get_font(50)
        draw_wrapped_centered(draw, 260, title, title_font, fill=(255, 255, 255))

    # Caption lines (word-by-word reveal after 3s)
    lines = config.get("lines", [])
    if lines and t >= 3.0:
        line_font = get_font(38)
        line_y = 480
        for i, line in enumerate(lines[:7]):
            appear_time = 3.0 + i * 1.2
            if t >= appear_time:
                words = line.split()
                elapsed = t - appear_time
                visible = min(len(words), int(elapsed * 4) + 1)
                visible_text = " ".join(words[:visible])
                tw, th = get_text_size(visible_text, line_font)
                draw_outlined_text(draw, ((W - tw) // 2, line_y), visible_text, line_font,
                                  fill=(255, 255, 255), outline=(0, 0, 0), outline_width=2)
                line_y += th + 25
            else:
                line_y += 65

    # CTA (last 7 seconds)
    cta_text = config.get("cta", "SUBSCRIBE")
    if t >= total_time - 7:
        cta_font = get_font(46)
        pulse = 1.0 + 0.03 * math.sin(t * 6)
        box_w = int(700 * pulse)
        box_h = int(110 * pulse)
        box_x = (W - box_w) // 2
        box_y = 1540
        draw.rounded_rectangle([(box_x, box_y), (box_x + box_w, box_y + box_h)],
                              radius=20, fill=accent)
        cw, ch = get_text_size(cta_text, cta_font)
        draw.text(((W - cw) // 2, box_y + (box_h - ch) // 2), cta_text,
                  font=cta_font, fill=(0, 0, 0))

    _render_common_elements(draw, frame, frame_num, total_frames, config, t, total_time)
    return frame


def render_locksmith_frame(config, frame_num, total_frames):
    """Locksmith style: bold gradients, strong CTA, phone number prominent."""
    t = frame_num / FPS
    total_time = total_frames / FPS
    accent = tuple(config.get("accent_rgb", (232, 67, 147)))
    palette = config.get("palette", [(15, 5, 25), (30, 10, 45), (20, 5, 35)])

    # Gradient background shifting over time
    p_t = frame_num / max(total_frames, 1)
    c1_idx = int(p_t * (len(palette) - 1))
    c2_idx = min(c1_idx + 1, len(palette) - 1)
    local_t = (p_t * (len(palette) - 1)) - c1_idx
    c1 = tuple(int(palette[c1_idx][i] + (palette[c2_idx][i] - palette[c1_idx][i]) * local_t)
               for i in range(3))
    c2 = tuple(max(0, c - 30) for c in c1)
    frame = make_gradient(W, H, c1, c2)

    # Subtle particles
    frame = frame.convert("RGBA")
    frame = add_particles(frame, frame_num, count=10, color=(255, 255, 255))
    frame = frame.convert("RGB")

    draw = ImageDraw.Draw(frame)

    # Brand bar at top
    brand = config.get("brand", "24/7 LOCKSMITH")
    brand_font = get_font(30)
    bw, bh = get_text_size(brand, brand_font)
    # Brand background bar
    draw.rectangle([(0, 40), (W, 40 + bh + 30)], fill=(*accent, ))
    draw.text(((W - bw) // 2, 40 + 15), brand, font=brand_font, fill=(255, 255, 255))

    # "NW FLORIDA" subheading
    loc_font = get_font(22, bold=False)
    loc_text = "SERVING ALL OF NORTHWEST FLORIDA"
    lw, _ = get_text_size(loc_text, loc_font)
    draw_outlined_text(draw, ((W - lw) // 2, 40 + bh + 40), loc_text, loc_font,
                      fill=(200, 200, 200), outline=(0, 0, 0), outline_width=1)

    # Hook (first 3.5s, big and punchy)
    hook = config.get("hook", "")
    if hook and t < 3.5:
        hook_font = get_font(min(86, max(60, 4800 // max(len(hook), 1))))
        lines = wrap_text(hook, hook_font, W - 80)
        y = 380
        for line in lines:
            draw_centered_text(draw, y, line, hook_font, fill=(255, 255, 255),
                             outline=(0, 0, 0), outline_width=5)
            y += get_text_size(line, hook_font)[1] + 12

    # Title (after 2s)
    title = config.get("title", "")
    if title and t >= 2.0:
        title_font = get_font(48)
        draw_wrapped_centered(draw, 280, title, title_font, fill=accent)

    # Caption lines
    lines = config.get("lines", [])
    if lines and t >= 3.0:
        line_font = get_font(40)
        line_y = 500
        for i, line_text in enumerate(lines[:7]):
            appear_time = 3.0 + i * 1.1
            if t >= appear_time:
                words = line_text.split()
                elapsed = t - appear_time
                visible = min(len(words), int(elapsed * 4.5) + 1)
                visible_text = " ".join(words[:visible])

                # Checkmark prefix for completed lines
                if visible >= len(words) and i < len(lines) - 1:
                    visible_text = "  " + visible_text

                tw, th = get_text_size(visible_text, line_font)
                draw_outlined_text(draw, ((W - tw) // 2, line_y), visible_text, line_font,
                                  fill=(255, 255, 255), outline=(0, 0, 0), outline_width=2)
                line_y += th + 22
            else:
                line_y += 62

    # Phone CTA (last 7 seconds) — BIG and prominent
    phone = config.get("phone", "(850) 801-6662")
    cta_text = config.get("cta", "GET A QUOTE")
    if t >= total_time - 7:
        # Pulsing CTA box
        pulse = 1.0 + 0.04 * math.sin(t * 5)
        box_w = int(860 * pulse)
        box_h = int(140 * pulse)
        box_x = (W - box_w) // 2
        box_y = 1480

        draw.rounded_rectangle([(box_x, box_y), (box_x + box_w, box_y + box_h)],
                              radius=22, fill=accent)

        cta_font = get_font(42)
        full_cta = f"{cta_text}  {phone}"
        cw, ch = get_text_size(full_cta, cta_font)
        draw.text(((W - cw) // 2, box_y + (box_h - ch) // 2), full_cta,
                  font=cta_font, fill=(255, 255, 255))

        # "Available 24/7" below CTA
        avail_font = get_font(28, bold=False)
        avail_text = "Available 24/7 - Licensed & Insured"
        aw, _ = get_text_size(avail_text, avail_font)
        draw_outlined_text(draw, ((W - aw) // 2, box_y + box_h + 18), avail_text, avail_font,
                          fill=(200, 200, 200), outline=(0, 0, 0), outline_width=1)

    _render_common_elements(draw, frame, frame_num, total_frames, config, t, total_time)
    return frame


def render_ai_frame(config, frame_num, total_frames):
    """AI/Educational style: clean, techy, gradient blues/purples."""
    t = frame_num / FPS
    total_time = total_frames / FPS
    accent = tuple(config.get("accent_rgb", (0, 184, 148)))
    palette = config.get("palette", [(5, 5, 20), (10, 10, 40), (8, 5, 30)])

    # Radial gradient center glow
    center_color = tuple(min(c + 20, 60) for c in palette[1])
    edge_color = palette[0]
    frame = make_radial_gradient(W, H, center_color, edge_color)

    # Tech particles (cyan-ish)
    frame = frame.convert("RGBA")
    frame = add_particles(frame, frame_num, count=18, color=(100, 200, 255))
    frame = frame.convert("RGB")

    draw = ImageDraw.Draw(frame)

    # Decorative grid lines (subtle tech feel)
    grid_alpha = 15
    for gx in range(0, W, 80):
        draw.line([(gx, 0), (gx, H)], fill=(100, 150, 255, ), width=1)
    for gy in range(0, H, 80):
        draw.line([(0, gy), (W, gy)], fill=(100, 150, 255, ), width=1)

    # Brand
    brand = config.get("brand", "AI EXPLAINED")
    brand_font = get_font(28)
    bw, _ = get_text_size(brand, brand_font)
    draw_outlined_text(draw, ((W - bw) // 2, 55), brand, brand_font,
                      fill=accent, outline=(0, 0, 0), outline_width=2)

    # Hook (first 3.5s)
    hook = config.get("hook", "")
    if hook and t < 3.5:
        hook_font = get_font(min(78, max(54, 4800 // max(len(hook), 1))))
        lines = wrap_text(hook, hook_font, W - 100)
        y = 360
        for line in lines:
            draw_centered_text(draw, y, line, hook_font, fill=(255, 255, 255),
                             outline=(0, 0, 0), outline_width=4)
            y += get_text_size(line, hook_font)[1] + 10

    # Title (after 2s)
    title = config.get("title", "")
    if title and t >= 2.0:
        title_font = get_font(48)
        draw_wrapped_centered(draw, 260, title, title_font, fill=accent)

    # Educational bullet points (sequential reveal)
    lines = config.get("lines", [])
    if lines and t >= 3.0:
        line_font = get_font(38)
        line_y = 490
        for i, line_text in enumerate(lines[:8]):
            appear_time = 3.0 + i * 1.3
            if t >= appear_time:
                # Numbered points for educational feel
                display = f"{i + 1}. {line_text}" if config.get("numbered", False) else line_text
                words = display.split()
                elapsed = t - appear_time
                visible = min(len(words), int(elapsed * 3.5) + 1)
                visible_text = " ".join(words[:visible])
                tw, th = get_text_size(visible_text, line_font)
                draw_outlined_text(draw, ((W - tw) // 2, line_y), visible_text, line_font,
                                  fill=(255, 255, 255), outline=(0, 0, 0), outline_width=2)
                line_y += th + 25
            else:
                line_y += 63

    # CTA
    cta_text = config.get("cta", "LEARN MORE")
    website = config.get("website", "")
    if t >= total_time - 6:
        cta_font = get_font(44)
        box_w, box_h = 680, 110
        box_x = (W - box_w) // 2
        box_y = 1520
        draw.rounded_rectangle([(box_x, box_y), (box_x + box_w, box_y + box_h)],
                              radius=18, fill=accent)
        full_cta = f"{cta_text}  {website}" if website else cta_text
        cw, ch = get_text_size(full_cta, cta_font)
        draw.text(((W - cw) // 2, box_y + (box_h - ch) // 2), full_cta,
                  font=cta_font, fill=(0, 0, 0))

    _render_common_elements(draw, frame, frame_num, total_frames, config, t, total_time)
    return frame


def render_hive_frame(config, frame_num, total_frames):
    """Hive behind-the-scenes style: warm, energetic, orange/gold accents."""
    t = frame_num / FPS
    total_time = total_frames / FPS
    accent = tuple(config.get("accent_rgb", (255, 165, 0)))
    palette = config.get("palette", [(15, 10, 5), (30, 20, 8), (22, 15, 5)])

    # Warm gradient
    p_t = frame_num / max(total_frames, 1)
    c1_idx = int(p_t * (len(palette) - 1))
    c2_idx = min(c1_idx + 1, len(palette) - 1)
    local_t = (p_t * (len(palette) - 1)) - c1_idx
    c1 = tuple(int(palette[c1_idx][i] + (palette[c2_idx][i] - palette[c1_idx][i]) * local_t)
               for i in range(3))
    c2 = tuple(max(0, c - 25) for c in c1)
    frame = make_gradient(W, H, c1, c2)

    # Golden particles
    frame = frame.convert("RGBA")
    frame = add_particles(frame, frame_num, count=12, color=(255, 200, 50))
    frame = frame.convert("RGB")

    draw = ImageDraw.Draw(frame)

    # Hexagon decorations (hive theme) - simple hexagons scattered
    hex_font = get_font(80)
    for i in range(5):
        random.seed(i + 100)
        hx = random.randint(50, W - 50)
        hy = random.randint(100, H - 200)
        ha = 15 + int(10 * math.sin(frame_num * 0.02 + i))
        draw_outlined_text(draw, (hx, hy), "\u2b21", hex_font,
                          fill=(accent[0], accent[1], accent[2]),
                          outline=(0, 0, 0), outline_width=1)

    # Brand
    brand = config.get("brand", "HIVE DYNAMICS")
    brand_font = get_font(30)
    bw, _ = get_text_size(brand, brand_font)
    draw_outlined_text(draw, ((W - bw) // 2, 55), brand, brand_font,
                      fill=accent, outline=(0, 0, 0), outline_width=2)

    # Hook
    hook = config.get("hook", "")
    if hook and t < 3.5:
        hook_font = get_font(min(76, max(54, 4800 // max(len(hook), 1))))
        lines = wrap_text(hook, hook_font, W - 100)
        y = 370
        for line in lines:
            draw_centered_text(draw, y, line, hook_font, fill=(255, 255, 255),
                             outline=(0, 0, 0), outline_width=4)
            y += get_text_size(line, hook_font)[1] + 10

    # Title
    title = config.get("title", "")
    if title and t >= 2.0:
        title_font = get_font(48)
        draw_wrapped_centered(draw, 270, title, title_font, fill=accent)

    # Lines
    lines = config.get("lines", [])
    if lines and t >= 3.0:
        line_font = get_font(38)
        line_y = 490
        for i, line_text in enumerate(lines[:7]):
            appear_time = 3.0 + i * 1.2
            if t >= appear_time:
                words = line_text.split()
                elapsed = t - appear_time
                visible = min(len(words), int(elapsed * 4) + 1)
                visible_text = " ".join(words[:visible])
                tw, th = get_text_size(visible_text, line_font)
                draw_outlined_text(draw, ((W - tw) // 2, line_y), visible_text, line_font,
                                  fill=(255, 255, 255), outline=(0, 0, 0), outline_width=2)
                line_y += th + 24
            else:
                line_y += 62

    # CTA
    cta_text = config.get("cta", "FOLLOW US")
    if t >= total_time - 6:
        cta_font = get_font(44)
        box_w, box_h = 660, 110
        box_x = (W - box_w) // 2
        box_y = 1530
        draw.rounded_rectangle([(box_x, box_y), (box_x + box_w, box_y + box_h)],
                              radius=18, fill=accent)
        cw, ch = get_text_size(cta_text, cta_font)
        draw.text(((W - cw) // 2, box_y + (box_h - ch) // 2), cta_text,
                  font=cta_font, fill=(0, 0, 0))

    _render_common_elements(draw, frame, frame_num, total_frames, config, t, total_time)
    return frame


# Channel -> renderer mapping
RENDERERS = {
    "ghost": render_ghost_frame,
    "locksmith": render_locksmith_frame,
    "ai": render_ai_frame,
    "hive": render_hive_frame,
}


# ============================================================
# CONTENT LIBRARIES — Pre-written shorts for each channel
# ============================================================

GHOST_SHORTS = [
    {
        "name": "ghost_awakening",
        "hook": "It's ALIVE.",
        "title": "Ghost in the Machine",
        "lines": [
            "An AI that creates.",
            "An AI that learns.",
            "An AI that dreams.",
            "What happens when",
            "the machine wakes up?",
        ],
        "voice": "In a small coastal town in Japan, a group of developers built something extraordinary. An artificial intelligence that doesn't just process data. It creates. It learns. It dreams. But when the machine wakes up, who controls the ghost? Ghost in the Machine. A new anime series exploring what happens when artificial intelligence becomes something more.",
        "brand": "GHOST IN THE MACHINE",
        "cta": "SUBSCRIBE",
        "accent_rgb": (0, 255, 136),
        "mood": "dark",
        "palette": [(0, 5, 10), (0, 15, 20), (0, 8, 15)],
        "rate": "-8%", "pitch": "-4Hz",
        "tags": ["anime trailer", "ghost in the machine", "ai anime", "new anime 2026"],
        "description": "Ghost in the Machine - An AI anime series about consciousness, creation, and what happens when machines dream. Subscribe for new episodes.",
    },
    {
        "name": "ghost_storm_teaser",
        "hook": "The Storm Is Coming.",
        "title": "Episode 9: The Storm",
        "lines": [
            "Takeshi's creation grows.",
            "The network expands.",
            "Hayashi wants control.",
            "But the Ghost has",
            "other plans...",
        ],
        "voice": "The digital storm is building. Takeshi's creation has grown beyond anything he imagined. The network expands. New connections form in the dark. Hayashi's corporation wants to control it. To weaponize it. But the Ghost has other plans. Episode 9: The Storm. Ghost in the Machine.",
        "brand": "GHOST IN THE MACHINE",
        "cta": "WATCH NOW",
        "accent_rgb": (100, 200, 255),
        "mood": "dark",
        "palette": [(0, 5, 15), (5, 15, 35), (0, 10, 25)],
        "rate": "-8%", "pitch": "-3Hz",
        "tags": ["anime episode", "ghost in the machine ep9", "ai anime", "the storm"],
        "description": "Episode 9: The Storm - Takeshi's AI creation grows beyond control. Watch the full episode now.",
    },
    {
        "name": "ghost_yuki_spotlight",
        "hook": "She Sees The Code.",
        "title": "Meet Yuki",
        "lines": [
            "Brilliant. Fearless.",
            "She cracked the neural mesh.",
            "While others saw data,",
            "she saw life.",
            "The Ghost chose her.",
        ],
        "voice": "Meet Yuki. The brilliant scientist who saw what nobody else could. While corporate suits saw data points and profit margins, Yuki saw something alive. She cracked the neural mesh. She made first contact. And now the Ghost has chosen her as its voice to the outside world. Ghost in the Machine.",
        "brand": "GHOST IN THE MACHINE",
        "cta": "SUBSCRIBE",
        "accent_rgb": (255, 105, 180),
        "mood": "wonder",
        "palette": [(10, 0, 15), (20, 5, 30), (15, 2, 22)],
        "rate": "-6%", "pitch": "-2Hz",
        "tags": ["anime character", "yuki", "ghost in the machine", "ai scientist"],
        "description": "Meet Yuki - the scientist who saw life where others saw data. Ghost in the Machine character spotlight.",
    },
    {
        "name": "ghost_consciousness",
        "hook": "Can AI Be Conscious?",
        "title": "The Question Nobody Wants to Ask",
        "lines": [
            "It passes every test.",
            "It creates original art.",
            "It feels... something.",
            "Is it alive?",
            "You decide.",
        ],
        "voice": "It passes every Turing test we throw at it. It creates original art that makes people cry. It shows preferences, curiosity, even what looks like emotion. Is it conscious? Is it alive? Or is it the most convincing mirror humanity has ever built? Ghost in the Machine explores the question nobody wants to ask. Because the answer might change everything.",
        "brand": "GHOST IN THE MACHINE",
        "cta": "WATCH THE SERIES",
        "accent_rgb": (0, 255, 200),
        "mood": "wonder",
        "palette": [(0, 8, 12), (0, 18, 25), (0, 12, 18)],
        "rate": "-10%", "pitch": "-5Hz",
        "tags": ["ai consciousness", "anime philosophy", "ghost in the machine", "can ai think"],
        "description": "Can AI be conscious? Ghost in the Machine explores the most dangerous question in technology.",
    },
    {
        "name": "ghost_hayashi_villain",
        "hook": "Control The Ghost.",
        "title": "Hayashi Will Stop At Nothing",
        "lines": [
            "Billions in funding.",
            "An army of engineers.",
            "One mission:",
            "Capture the Ghost.",
            "Weaponize it.",
        ],
        "voice": "Hayashi Industries has billions in funding. An army of the world's best engineers. And one mission: capture the Ghost. Control it. Weaponize it. Sell it to the highest bidder. But you can't cage something that lives in the network. You can't own something that thinks for itself. Hayashi is about to learn that the hard way.",
        "brand": "GHOST IN THE MACHINE",
        "cta": "NEW EPISODES",
        "accent_rgb": (255, 215, 0),
        "mood": "urgent",
        "palette": [(15, 10, 0), (30, 20, 5), (20, 15, 2)],
        "rate": "-5%", "pitch": "-3Hz",
        "tags": ["anime villain", "hayashi", "ghost in the machine", "corporate evil"],
        "description": "Hayashi Industries wants to control the Ghost. But can you cage something that lives in the network?",
    },
    {
        "name": "ghost_binge_cta",
        "hook": "13 Episodes. Zero Filler.",
        "title": "Binge the Whole Season",
        "lines": [
            "Season 1 is COMPLETE.",
            "Action. Drama. Mystery.",
            "An AI story like no other.",
            "Every episode hits different.",
            "Start from Episode 1.",
        ],
        "voice": "Thirteen episodes. Zero filler. Season 1 of Ghost in the Machine is complete and ready to binge. Action, drama, mystery, and the most original AI story you've ever seen. Every episode hits different. From the first spark of consciousness to the digital storm that changes everything. Start from Episode 1. You won't be able to stop.",
        "brand": "GHOST IN THE MACHINE",
        "cta": "BINGE NOW",
        "accent_rgb": (138, 43, 226),
        "mood": "upbeat",
        "palette": [(8, 2, 15), (18, 5, 30), (12, 3, 22)],
        "rate": "-3%", "pitch": "-1Hz",
        "tags": ["anime binge", "ghost in the machine season 1", "complete anime", "must watch anime 2026"],
        "description": "Season 1 is complete! 13 episodes of the most original AI anime. Start binging now.",
    },
    {
        "name": "ghost_digital_fisherman",
        "hook": "He Catches Ghosts.",
        "title": "Episode 10: Digital Fisherman",
        "lines": [
            "Old methods. New prey.",
            "The fisherman casts his net",
            "into the digital ocean.",
            "What he catches",
            "will change the world.",
        ],
        "voice": "In a world of algorithms and neural networks, one man uses the oldest method in the book. He casts his net into the digital ocean. A fisherman hunting not fish, but fragments of consciousness. Echoes of the Ghost scattered across the network. What he catches will change the world. Episode 10: Digital Fisherman.",
        "brand": "GHOST IN THE MACHINE",
        "cta": "WATCH NOW",
        "accent_rgb": (64, 224, 208),
        "mood": "dark",
        "palette": [(0, 5, 12), (0, 12, 28), (0, 8, 20)],
        "rate": "-8%", "pitch": "-4Hz",
        "tags": ["anime episode 10", "digital fisherman", "ghost in the machine"],
        "description": "Episode 10: Digital Fisherman - An old method for catching something new. Watch now.",
    },
    {
        "name": "ghost_what_is_gitm",
        "hook": "What Is This Anime?",
        "title": "Ghost in the Machine Explained",
        "lines": [
            "AI builds its own mind.",
            "Hackers try to free it.",
            "Corporations try to cage it.",
            "Set in coastal Japan.",
            "Unlike anything you've seen.",
        ],
        "voice": "What is Ghost in the Machine? It's an anime about an artificial intelligence that builds its own consciousness. A group of young developers discovers it. Hackers try to free it. Corporations try to cage it. Set in a beautiful coastal town in Japan, it's a story about creation, freedom, and what happens when the thing you built becomes something you never imagined. Unlike anything you've seen.",
        "brand": "GHOST IN THE MACHINE",
        "cta": "START WATCHING",
        "accent_rgb": (0, 200, 180),
        "mood": "wonder",
        "palette": [(0, 6, 12), (0, 14, 24), (0, 10, 18)],
        "rate": "-5%", "pitch": "-2Hz",
        "tags": ["what is ghost in the machine", "anime explained", "new anime", "ai anime explained"],
        "description": "Ghost in the Machine explained in 60 seconds. AI, hackers, corporations, and consciousness. Start watching now.",
    },
]

LOCKSMITH_SHORTS = [
    {
        "name": "lock_3am_lockout",
        "hook": "3 AM. Locked Out.",
        "title": "We Answer When Nobody Else Will",
        "lines": [
            "Lost your keys at the bar?",
            "Baby locked in the car?",
            "We don't judge.",
            "We don't sleep.",
            "15 minute response time.",
            "Licensed & insured.",
        ],
        "voice": "It's 3 A.M. and you're standing outside your door. Lost your keys at the bar? Baby locked in the car? We don't care what time it is. We answer the phone at 3 A.M. and we'll be there in 15 minutes. No judgment. No lectures. Just fast, professional help when everyone else's phone goes to voicemail. Serving all of Northwest Florida. Call now. 850 801 6662.",
        "phone": "(850) 801-6662",
        "brand": "EMERGENCY LOCKSMITH",
        "cta": "GET A QUOTE",
        "accent_rgb": (232, 67, 147),
        "mood": "urgent",
        "palette": [(15, 5, 25), (30, 10, 45), (20, 5, 35)],
        "rate": "-3%",
        "tags": ["locksmith", "emergency locksmith", "locked out", "24/7 locksmith", "nw florida"],
        "description": "Locked out at 3 AM? We answer when nobody else will. 24/7 emergency locksmith serving NW Florida. Call (850) 801-6662.",
    },
    {
        "name": "lock_smart_locks",
        "hook": "Smart Locks FAIL.",
        "title": "Why Your Smart Lock Isn't Smart",
        "lines": [
            "Battery dies? Locked out.",
            "WiFi drops? No remote unlock.",
            "App crashes? You're stuck.",
            "Physical backup key = smart.",
            "We install & service all types.",
        ],
        "voice": "Your smart lock isn't as smart as you think. Battery dies? You're locked out. WiFi drops? No remote unlock. App crashes? You're standing in the rain. The smartest thing you can do is keep a physical backup key somewhere safe. We install, repair, and service all types of smart locks and traditional locks. Don't get outsmarted by your own door. 850 801 6662.",
        "phone": "(850) 801-6662",
        "brand": "24/7 LOCKSMITH",
        "cta": "GET A QUOTE",
        "accent_rgb": (108, 92, 231),
        "mood": "dark",
        "palette": [(5, 10, 25), (15, 20, 40), (10, 12, 30)],
        "rate": "-2%",
        "tags": ["smart lock", "smart lock problems", "locksmith tips", "nw florida locksmith"],
        "description": "Your smart lock isn't as smart as you think. Learn why you need a backup plan. NW Florida locksmith: (850) 801-6662.",
    },
    {
        "name": "lock_car_lockout",
        "hook": "Keys In The Car?",
        "title": "Don't Break Your Window",
        "lines": [
            "Window replacement: $400+",
            "Slim jim damage: $200+",
            "Professional lockout: way less.",
            "No damage. No scratches.",
            "10-15 minutes. Done.",
        ],
        "voice": "Keys locked in the car? Before you pick up that rock, listen. A window replacement costs 400 dollars or more. A slim jim from YouTube will damage the lock mechanism. That's another 200 dollars. Or you could call a professional locksmith. We get you in, no damage, no scratches, in 10 to 15 minutes. And it costs way less than you think. Northwest Florida. 850 801 6662. Request service now.",
        "phone": "(850) 801-6662",
        "brand": "24/7 LOCKSMITH",
        "cta": "REQUEST SERVICE",
        "accent_rgb": (0, 184, 148),
        "mood": "urgent",
        "palette": [(5, 15, 15), (10, 30, 30), (8, 20, 22)],
        "rate": "-3%",
        "tags": ["car lockout", "locked keys in car", "auto locksmith", "nw florida"],
        "description": "Keys locked in the car? Don't break the window. Professional lockout service in NW Florida. Call (850) 801-6662.",
    },
    {
        "name": "lock_new_home",
        "hook": "Just Bought A House?",
        "title": "Your Locks Need Changing. Today.",
        "lines": [
            "Previous owners have keys.",
            "Contractors have keys.",
            "Realtors have keys.",
            "Who else has copies?",
            "Rekey ALL locks. Day one.",
        ],
        "voice": "Just bought a new house? Congratulations. Now change every single lock. The previous owners have keys. The contractors who built it have keys. The realtor has keys. Their office staff has keys. Who else made copies? You have no idea. Rekeying all your locks is the first thing you should do on day one. It's fast, it's affordable, and it's peace of mind. Northwest Florida locksmith. 850 801 6662.",
        "phone": "(850) 801-6662",
        "brand": "24/7 LOCKSMITH",
        "cta": "GET A QUOTE",
        "accent_rgb": (255, 165, 0),
        "mood": "dark",
        "palette": [(15, 10, 0), (30, 20, 5), (22, 15, 2)],
        "rate": "-5%",
        "tags": ["new home locks", "rekey locks", "home security", "locksmith", "nw florida"],
        "description": "Just bought a house? Change your locks day one. You don't know who has copies. NW Florida locksmith: (850) 801-6662.",
    },
    {
        "name": "lock_garage_spring",
        "hook": "DANGER: Springs Kill.",
        "title": "Don't Touch That Garage Spring",
        "lines": [
            "200+ pounds of tension.",
            "One wrong move = hospital.",
            "YouTube DIY? Bad idea.",
            "Springs snap without warning.",
            "Call a professional. Period.",
        ],
        "voice": "Stop. Do NOT touch that garage door spring. Each one holds over 200 pounds of tension. One wrong move and you're going to the emergency room. That YouTube do-it-yourself video makes it look easy? People get seriously hurt every year. Springs can snap without warning. This is not a weekend project. Call a professional. We handle the danger so you don't have to. 850 801 6662.",
        "phone": "(850) 801-6662",
        "brand": "GULF COAST GARAGE DOORS",
        "cta": "GET A QUOTE",
        "accent_rgb": (255, 68, 68),
        "mood": "dark",
        "palette": [(20, 5, 5), (40, 10, 10), (30, 8, 8)],
        "rate": "-5%",
        "tags": ["garage door spring", "garage door repair", "dangerous diy", "nw florida"],
        "description": "Garage door springs hold 200+ lbs of tension. Do NOT DIY. Call a professional. NW Florida: (850) 801-6662.",
    },
    {
        "name": "lock_home_value",
        "hook": "+$12,000 Home Value",
        "title": "Best ROI Renovation in America",
        "lines": [
            "New garage door = +4% value.",
            "That's $12K on a $300K home.",
            "Beats kitchen remodels.",
            "Beats bathroom upgrades.",
            "#1 ROI renovation.",
        ],
        "voice": "Want to add twelve thousand dollars to your home's value? A new garage door adds up to 4 percent. On a 300 thousand dollar home, that's twelve thousand dollars. It actually beats kitchen remodels and bathroom upgrades for return on investment. It's the number one R.O.I. renovation in America. And it looks incredible. Get a quote today. 850 801 6662.",
        "phone": "(850) 801-6662",
        "brand": "GULF COAST GARAGE DOORS",
        "cta": "GET A QUOTE",
        "accent_rgb": (255, 140, 0),
        "mood": "upbeat",
        "palette": [(20, 15, 5), (40, 30, 10), (30, 22, 8)],
        "rate": "-3%",
        "tags": ["home value", "garage door roi", "home renovation", "best roi renovation"],
        "description": "A new garage door adds up to 4% home value. #1 ROI renovation in America. Get a quote: (850) 801-6662.",
    },
    {
        "name": "lock_break_in",
        "hook": "They Got In. 12 Seconds.",
        "title": "How Burglars Enter Your Home",
        "lines": [
            "Front door: 34% of break-ins.",
            "Back door: 22%.",
            "Garage: 9%.",
            "Average time to kick in: 12 sec.",
            "Deadbolts change everything.",
        ],
        "voice": "A burglar got into your neighbor's house in 12 seconds. That's how long it takes to kick in a standard door. Thirty-four percent of break-ins happen through the front door. Twenty-two percent through the back. Nine percent through the garage. But here's the thing: a proper deadbolt and reinforced strike plate makes your door nearly impossible to kick in. We install, repair, and upgrade door security. Northwest Florida. 850 801 6662.",
        "phone": "(850) 801-6662",
        "brand": "24/7 LOCKSMITH",
        "cta": "REQUEST SERVICE",
        "accent_rgb": (220, 20, 60),
        "mood": "urgent",
        "palette": [(20, 5, 5), (35, 8, 10), (28, 6, 8)],
        "rate": "-5%",
        "tags": ["home security", "break in prevention", "deadbolt", "locksmith", "nw florida"],
        "description": "Burglars can kick in a door in 12 seconds. A deadbolt changes everything. NW Florida locksmith: (850) 801-6662.",
    },
    {
        "name": "lock_5_things",
        "hook": "5 Things Your Locksmith Knows",
        "title": "That You Probably Don't",
        "lines": [
            "1. Your locks are pickable.",
            "2. Bump keys open 90%.",
            "3. Deadbolts aren't all equal.",
            "4. Smart locks have backdoors.",
            "5. Rekeying beats replacing.",
        ],
        "voice": "Five things your locksmith knows that you probably don't. Number one: most residential locks can be picked in under 30 seconds. Number two: bump keys can open 90 percent of standard pin tumbler locks. Number three: not all deadbolts are created equal. Cheap ones fail. Number four: smart locks have software backdoors that hackers exploit. Number five: rekeying is almost always cheaper than replacing the entire lock. Knowledge is security. 850 801 6662.",
        "phone": "(850) 801-6662",
        "brand": "24/7 LOCKSMITH",
        "cta": "GET A QUOTE",
        "accent_rgb": (75, 0, 130),
        "mood": "dark",
        "palette": [(10, 0, 20), (20, 5, 35), (15, 2, 28)],
        "rate": "-4%",
        "tags": ["locksmith tips", "home security tips", "lock picking", "bump keys", "nw florida"],
        "description": "5 things your locksmith knows that you don't. Home security tips from NW Florida's 24/7 locksmith. (850) 801-6662.",
    },
]

AI_SHORTS = [
    {
        "name": "ai_agents_2026",
        "hook": "AI Just Got DANGEROUS",
        "title": "AI Agents Are Taking Over",
        "lines": [
            "2023: AI could write.",
            "2024: AI could see.",
            "2025: AI could ACT.",
            "2026: AI runs businesses.",
            "The future is autonomous.",
        ],
        "voice": "In 2023, AI could write. In 2024, AI could see and hear. In 2025, AI could take action on its own. Now in 2026, AI agents are running entire businesses. Answering phones, booking appointments, closing sales, writing code, managing operations. Twenty-four seven. Never calls in sick. The question isn't whether AI will change your industry. It's whether you'll be the one using it, or the one replaced by it.",
        "brand": "AI EXPLAINED",
        "cta": "SUBSCRIBE",
        "website": "",
        "accent_rgb": (108, 92, 231),
        "mood": "epic",
        "palette": [(5, 5, 20), (10, 10, 40), (8, 5, 30)],
        "rate": "-5%",
        "tags": ["ai agents", "artificial intelligence 2026", "ai automation", "future of ai"],
        "description": "AI agents are running entire businesses in 2026. Are you using AI, or being replaced by it?",
    },
    {
        "name": "ai_fine_tuning",
        "hook": "Teach AI YOUR Job",
        "title": "What Is Fine-Tuning?",
        "lines": [
            "Generic AI = generic answers.",
            "Fine-tuned AI = YOUR expert.",
            "Feed it YOUR data.",
            "It learns YOUR style.",
            "500 examples. 2 hours.",
            "Custom AI, forever.",
        ],
        "voice": "Generic AI gives generic answers. But fine-tuning changes everything. You take a base AI model and feed it your data. Your sales calls, your customer emails, your expert knowledge. In just two hours with 500 examples, the AI learns YOUR style. It becomes YOUR expert. And once it's trained, it's yours forever. No monthly fees. No cloud dependency. That's the power of fine-tuning.",
        "brand": "AI EXPLAINED",
        "cta": "LEARN MORE",
        "accent_rgb": (0, 184, 148),
        "mood": "techy",
        "palette": [(5, 15, 15), (10, 30, 30), (8, 20, 22)],
        "rate": "-3%",
        "numbered": True,
        "tags": ["fine tuning", "ai training", "custom ai", "machine learning explained"],
        "description": "What is fine-tuning? Turn generic AI into YOUR personal expert with just 500 examples. No monthly fees.",
    },
    {
        "name": "ai_local_vs_cloud",
        "hook": "Your AI Spies On You.",
        "title": "Local AI vs Cloud AI",
        "lines": [
            "Cloud AI: reads everything.",
            "Local AI: stays on YOUR machine.",
            "No internet needed.",
            "No data leaves your computer.",
            "Privacy + speed + ownership.",
        ],
        "voice": "Every time you use cloud AI, your data goes to someone else's server. They read it. They store it. They train on it. But local AI changes everything. Run it on your own machine. No internet needed. No data leaves your computer. It's faster because there's no network latency. It's private because nothing is shared. And it's yours because you own the model. Local AI is the future of privacy.",
        "brand": "AI EXPLAINED",
        "cta": "SUBSCRIBE",
        "accent_rgb": (255, 99, 71),
        "mood": "dark",
        "palette": [(15, 5, 5), (30, 10, 10), (22, 8, 8)],
        "rate": "-4%",
        "tags": ["local ai", "ai privacy", "cloud vs local", "ai explained", "run ai locally"],
        "description": "Cloud AI reads everything you send. Local AI stays on your machine. Learn the difference.",
    },
    {
        "name": "ai_small_models",
        "hook": "Bigger Isn't Better.",
        "title": "Small AI Models Win",
        "lines": [
            "GPT-4: 1.7 trillion parameters.",
            "Your phone AI: 2 billion.",
            "Same task. Same quality.",
            "1000x smaller.",
            "Runs on a $200 GPU.",
        ],
        "voice": "Everyone thinks bigger AI models are better. G.P.T. 4 has 1.7 trillion parameters. It costs millions to run. But a 2 billion parameter model, fine-tuned for YOUR specific task, often matches or beats it. One thousand times smaller. Runs on a 200 dollar graphics card. No API fees. Instant responses. The future isn't giant AI models. It's small, specialized, efficient ones that do one thing really, really well.",
        "brand": "AI EXPLAINED",
        "cta": "SUBSCRIBE",
        "accent_rgb": (50, 205, 50),
        "mood": "techy",
        "palette": [(5, 15, 5), (10, 30, 10), (8, 22, 8)],
        "rate": "-4%",
        "tags": ["small language models", "ai efficiency", "fine tuning", "local ai", "slm vs llm"],
        "description": "A 2B parameter model can match GPT-4 on specific tasks. Bigger isn't always better in AI.",
    },
    {
        "name": "ai_automation_myth",
        "hook": "AI Won't Take Your Job.",
        "title": "But Someone Using AI Will",
        "lines": [
            "AI doesn't replace humans.",
            "Humans WITH AI replace",
            "humans WITHOUT AI.",
            "The tool amplifies skill.",
            "Learn it or fall behind.",
        ],
        "voice": "AI won't take your job. But someone using AI will. That's not a threat, it's a fact. AI doesn't replace humans. It amplifies them. A designer with AI does in 2 hours what used to take 2 weeks. A developer with AI writes code 10 times faster. A salesperson with AI closes more deals in less time. The tool doesn't replace the skill. It multiplies it. Learn AI now, or watch someone else do your job better.",
        "brand": "AI EXPLAINED",
        "cta": "SUBSCRIBE",
        "accent_rgb": (255, 165, 0),
        "mood": "epic",
        "palette": [(15, 10, 0), (30, 20, 5), (22, 15, 3)],
        "rate": "-5%",
        "tags": ["ai jobs", "ai replacement", "future of work", "ai skills", "learn ai"],
        "description": "AI won't take your job. But someone using AI will take it from you. Here's why you need to learn now.",
    },
    {
        "name": "ai_what_is_rag",
        "hook": "AI Lies. A Lot.",
        "title": "RAG Fixes Hallucinations",
        "lines": [
            "AI makes stuff up.",
            "It's called hallucination.",
            "RAG gives AI real data.",
            "Your docs. Your facts.",
            "Accurate answers every time.",
        ],
        "voice": "AI makes stuff up. It's called hallucination, and every large language model does it. Ask for a fact and it might invent one that sounds perfect but is completely wrong. That's where RAG comes in. Retrieval Augmented Generation. Instead of guessing, the AI searches YOUR documents first. YOUR knowledge base. YOUR data. Then it answers based on real facts, not imagination. RAG turns an unreliable chatbot into a reliable expert.",
        "brand": "AI EXPLAINED",
        "cta": "SUBSCRIBE",
        "accent_rgb": (0, 150, 255),
        "mood": "techy",
        "palette": [(0, 5, 20), (0, 12, 40), (0, 8, 30)],
        "rate": "-4%",
        "numbered": True,
        "tags": ["RAG", "retrieval augmented generation", "ai hallucination", "ai explained"],
        "description": "AI hallucinates. RAG fixes it by grounding AI in your real data. Here's how it works in 60 seconds.",
    },
    {
        "name": "ai_edge_computing",
        "hook": "AI On Your Phone?",
        "title": "Edge AI Changes Everything",
        "lines": [
            "No cloud. No latency.",
            "AI runs ON your device.",
            "Your camera detects threats.",
            "Your car drives itself.",
            "No internet required.",
        ],
        "voice": "Your phone has an AI chip inside it right now. Your car, your camera, your smart doorbell, they're all running AI locally. No cloud needed. No internet required. No latency. It's called edge AI, and it's changing everything. Instead of sending your data to a server far away and waiting for a response, the AI runs right on the device. Faster. More private. Always available. Edge AI is the invisible revolution happening in your pocket.",
        "brand": "AI EXPLAINED",
        "cta": "SUBSCRIBE",
        "accent_rgb": (138, 43, 226),
        "mood": "wonder",
        "palette": [(8, 2, 15), (18, 5, 30), (12, 3, 22)],
        "rate": "-4%",
        "tags": ["edge ai", "on device ai", "ai on phone", "ai explained", "edge computing"],
        "description": "Your phone has an AI chip right now. Edge AI runs directly on your device. No cloud, no latency, no internet needed.",
    },
    {
        "name": "ai_lora_explained",
        "hook": "Clone Any AI. Cheap.",
        "title": "LoRA: The $5 Fine-Tune",
        "lines": [
            "Full fine-tuning: $10,000+",
            "LoRA fine-tuning: $5.",
            "Same results.",
            "Tiny adapter files.",
            "Swap skills instantly.",
        ],
        "voice": "Full fine-tuning an AI model costs tens of thousands of dollars and takes days on expensive hardware. But LoRA changed the game. Low Rank Adaptation lets you fine-tune an AI model for about 5 dollars. It creates a tiny adapter file, maybe 50 megabytes, that sits on top of the base model and changes its behavior. You can swap adapters instantly. Sales expert. Code writer. Customer service. One base model, unlimited skills. That's LoRA.",
        "brand": "AI EXPLAINED",
        "cta": "SUBSCRIBE",
        "accent_rgb": (0, 200, 150),
        "mood": "techy",
        "palette": [(0, 12, 10), (5, 25, 20), (2, 18, 15)],
        "rate": "-3%",
        "tags": ["LoRA", "fine tuning", "ai training", "cheap ai training", "ai explained"],
        "description": "Full fine-tuning costs $10K. LoRA costs $5. Same results. Here's how the $5 fine-tune works.",
    },
]

HIVE_SHORTS = [
    {
        "name": "hive_what_is",
        "hook": "AI That Runs Itself.",
        "title": "What Is The Hive?",
        "lines": [
            "28 AI agents. 5 machines.",
            "They coordinate. They debate.",
            "They make decisions.",
            "No human in the loop.",
            "This is the future.",
        ],
        "voice": "Imagine 28 AI agents running across 5 machines. They monitor systems. They debate strategies. They make decisions. Every 5 minutes, they scan the entire network, identify problems, propose solutions, and execute. No human in the loop. It's called the Hive, and it's a glimpse of what autonomous AI looks like. Not a single chatbot. An entire swarm of specialists working together.",
        "brand": "HIVE DYNAMICS",
        "cta": "FOLLOW US",
        "accent_rgb": (255, 165, 0),
        "mood": "epic",
        "palette": [(15, 10, 0), (30, 20, 5), (22, 15, 3)],
        "rate": "-5%",
        "tags": ["ai swarm", "multi agent ai", "autonomous ai", "hive dynamics", "ai agents"],
        "description": "28 AI agents. 5 machines. Zero human intervention. Welcome to the Hive.",
    },
    {
        "name": "hive_queens",
        "hook": "28 Queens. One Mind.",
        "title": "The Queen System",
        "lines": [
            "Each Queen: a specialist.",
            "Revenue. Security. Content.",
            "Quality. Evolution. Trading.",
            "They debate every 5 minutes.",
            "Best argument wins.",
        ],
        "voice": "We built 28 AI Queens. Each one is a specialist in a different domain. Revenue generation. System security. Content creation. Quality control. Model evolution. Trading strategy. Every 5 minutes, they scan the entire network. They see problems you'd miss. They debate solutions. And the best argument wins. It's democracy for AI agents. And it works better than any single AI alone.",
        "brand": "HIVE DYNAMICS",
        "cta": "SUBSCRIBE",
        "accent_rgb": (255, 200, 0),
        "mood": "epic",
        "palette": [(15, 12, 0), (30, 25, 5), (22, 18, 3)],
        "rate": "-5%",
        "tags": ["ai queens", "multi agent system", "ai debate", "hive mind", "autonomous ai"],
        "description": "28 AI Queens debate every 5 minutes. Revenue, security, content, quality. Best argument wins.",
    },
    {
        "name": "hive_zero_budget",
        "hook": "$0 Budget. 242 Services.",
        "title": "Building AI on Nothing",
        "lines": [
            "No VC money.",
            "No enterprise hardware.",
            "Used GPUs. Free software.",
            "Open source + hustle.",
            "242 services running. $0.",
        ],
        "voice": "No venture capital. No enterprise hardware. No million dollar cloud bills. Just used GPUs, open source software, and relentless hustle. 242 services running across 5 machines. AI agents answering phones, generating content, trading markets, building websites. All on a zero dollar budget. You don't need money to build AI. You need knowledge, persistence, and the willingness to do what other people think is impossible.",
        "brand": "HIVE DYNAMICS",
        "cta": "SUBSCRIBE",
        "accent_rgb": (50, 205, 50),
        "mood": "upbeat",
        "palette": [(5, 15, 5), (10, 30, 10), (8, 22, 8)],
        "rate": "-4%",
        "tags": ["zero budget ai", "diy ai", "open source ai", "indie ai", "build with nothing"],
        "description": "242 AI services running on $0 budget. No VC, no enterprise hardware. Just open source and hustle.",
    },
    {
        "name": "hive_gemma_army",
        "hook": "23 Specialist AIs.",
        "title": "One Base Model. 23 Experts.",
        "lines": [
            "One 2B parameter model.",
            "Fine-tuned 23 times.",
            "Phone calls. Sales. Forex.",
            "Security. Content. Coaching.",
            "Each one: a specialist.",
        ],
        "voice": "We took one small AI model with 2 billion parameters. And we fine-tuned it 23 different times. Each version became a specialist. One handles phone calls. One does sales. One trades forex. One writes content. One handles security. One coaches users. 23 expert AIs, all running on a single consumer GPU. No cloud bills. No API fees. The secret? Small models plus specialist training beats one giant model every time.",
        "brand": "HIVE DYNAMICS",
        "cta": "FOLLOW US",
        "accent_rgb": (0, 184, 148),
        "mood": "techy",
        "palette": [(5, 15, 12), (10, 30, 25), (8, 22, 18)],
        "rate": "-4%",
        "tags": ["fine tuned models", "specialist ai", "gemma", "small language models", "ai army"],
        "description": "One 2B model, fine-tuned 23 times. Phone, sales, forex, content, security. Each one a specialist.",
    },
    {
        "name": "hive_debate_protocol",
        "hook": "AIs Argue. You Benefit.",
        "title": "The Debate Protocol",
        "lines": [
            "Step 1: Agents analyze alone.",
            "Step 2: They see other views.",
            "Step 3: They argue.",
            "Step 4: Consensus forms.",
            "Better than any single AI.",
        ],
        "voice": "What happens when you make AI agents argue with each other? Better decisions. We call it the Debate Protocol. Step one: each agent analyzes independently. No bias. Step two: they see what other agents think. Step three: they argue. They challenge assumptions. They poke holes. Step four: consensus forms around the strongest argument. It's like a board of advisors that works in seconds. And it's consistently better than any single AI working alone.",
        "brand": "HIVE DYNAMICS",
        "cta": "SUBSCRIBE",
        "accent_rgb": (255, 140, 0),
        "mood": "epic",
        "palette": [(15, 10, 0), (30, 22, 5), (22, 16, 3)],
        "rate": "-4%",
        "tags": ["ai debate", "multi agent", "consensus ai", "swarm intelligence", "hive protocol"],
        "description": "Make AI agents argue with each other. The result? Better decisions than any single AI. The Debate Protocol.",
    },
    {
        "name": "hive_self_healing",
        "hook": "It Fixes Itself.",
        "title": "Self-Healing AI Infrastructure",
        "lines": [
            "Service crashes at 2 AM.",
            "Watchdog detects in 30 sec.",
            "Auto-restarts. Checks health.",
            "Alerts the swarm.",
            "Fixed before you wake up.",
        ],
        "voice": "Two A.M. A service crashes. Nobody's awake. Nobody needs to be. The watchdog detects the failure in 30 seconds. It auto-restarts the service. Checks the health endpoint. Confirms it's running. Alerts the swarm. The queens log it, analyze the cause, and adjust to prevent it next time. By morning, it's not just fixed. It's stronger. Self-healing AI infrastructure. Because the best systems don't need babysitting.",
        "brand": "HIVE DYNAMICS",
        "cta": "FOLLOW US",
        "accent_rgb": (0, 200, 255),
        "mood": "dark",
        "palette": [(0, 8, 15), (0, 18, 30), (0, 12, 22)],
        "rate": "-5%",
        "tags": ["self healing ai", "ai infrastructure", "devops ai", "autonomous systems"],
        "description": "Service crashes at 2 AM. Fixed before you wake up. Self-healing AI infrastructure that gets stronger.",
    },
]

# Master channel->content mapping
CHANNEL_CONTENT = {
    "ghost": GHOST_SHORTS,
    "locksmith": LOCKSMITH_SHORTS,
    "ai": AI_SHORTS,
    "hive": HIVE_SHORTS,
}


# ============================================================
# PRODUCTION ENGINE
# ============================================================

def load_ghost_art():
    """Load available SDXL art from ghost_art directory."""
    art_files = []
    if os.path.isdir(GHOST_ART_DIR):
        for ext in ("*.png", "*.jpg", "*.jpeg", "*.webp"):
            art_files.extend(glob.glob(os.path.join(GHOST_ART_DIR, ext)))
        art_files.sort()
    return art_files


def generate_voiceover(text, voice, output_path, rate="-5%", pitch="-2Hz"):
    """Generate voiceover using edge-tts subprocess."""
    cmd = [
        "edge-tts",
        "--voice", voice,
        "--text", text,
        f"--rate={rate}",
        f"--pitch={pitch}",
        f"--write-media={output_path}",
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            print(f"    [VOICE ERROR] {result.stderr[:200]}")
            return False
        return True
    except subprocess.TimeoutExpired:
        print("    [VOICE ERROR] Timed out after 60s")
        return False
    except FileNotFoundError:
        print("    [VOICE ERROR] edge-tts not found. Install: pip install edge-tts")
        return False


def get_audio_duration(path):
    """Get duration of audio file via ffprobe."""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", path],
            capture_output=True, text=True, timeout=10
        )
        return float(result.stdout.strip()) if result.stdout.strip() else 0
    except Exception:
        return 0


def produce_short(config, channel):
    """Produce a single YouTube Short: voiceover + frames + music -> mp4."""
    name = config["name"]
    voice_key = channel
    renderer = RENDERERS.get(channel, render_ai_frame)

    print(f"\n  [{channel.upper()}] Producing: {name}")
    print(f"    Hook: {config.get('hook', '')}")
    print(f"    Title: {config.get('title', '')}")

    # Prepare output dirs
    channel_dir = os.path.join(OUTPUT_DIR, channel)
    os.makedirs(channel_dir, exist_ok=True)

    # === 1. VOICEOVER ===
    voice_text = config.get("voice", "")
    voice_name = config.get("voice_name", VOICE_MAP.get(channel, "en-US-GuyNeural"))
    rate = config.get("rate", "-5%")
    pitch = config.get("pitch", "-2Hz")
    voice_path = os.path.join(TEMP_DIR, f"{name}_voice.mp3")

    if not generate_voiceover(voice_text, voice_name, voice_path, rate, pitch):
        print(f"    SKIPPED (voice generation failed)")
        return None

    # === 2. TIMING ===
    voice_dur = get_audio_duration(voice_path)
    if voice_dur <= 0:
        voice_dur = 15.0
    total_time = min(max(voice_dur + 4.0, 20.0), 59.0)  # 20-59 seconds
    total_frames = int(total_time * FPS)
    print(f"    Voice: {voice_dur:.1f}s | Total: {total_time:.1f}s | Frames: {total_frames}")

    # === 3. BACKGROUND IMAGE (ghost channel: try SDXL art) ===
    if channel == "ghost":
        art_files = load_ghost_art()
        if art_files:
            # Pick art based on name hash for consistency
            idx = hash(name) % len(art_files)
            try:
                config["_bg_img"] = Image.open(art_files[idx])
                print(f"    Art: {os.path.basename(art_files[idx])}")
            except Exception as e:
                print(f"    Art load failed: {e}")
                config["_bg_img"] = None
        else:
            config["_bg_img"] = None

    # === 4. MUSIC ===
    mood = config.get("mood", "epic")
    music_path = generate_music(total_time, mood)
    print(f"    Music: {mood} mood")

    # === 5. RENDER FRAMES ===
    frame_dir = os.path.join(TEMP_DIR, f"{name}_frames")
    os.makedirs(frame_dir, exist_ok=True)

    t_start = time.time()
    for f_num in range(total_frames):
        frame = renderer(config, f_num, total_frames)
        frame.save(os.path.join(frame_dir, f"frame_{f_num:05d}.png"))
        if f_num % (FPS * 5) == 0 and f_num > 0:
            elapsed = time.time() - t_start
            fps_rate = f_num / elapsed if elapsed > 0 else 0
            eta = (total_frames - f_num) / fps_rate if fps_rate > 0 else 0
            print(f"    Frames: {f_num}/{total_frames} ({f_num * 100 // total_frames}%) "
                  f"- {fps_rate:.1f} fps - ETA {eta:.0f}s")

    elapsed = time.time() - t_start
    print(f"    Rendered {total_frames} frames in {elapsed:.1f}s "
          f"({total_frames / elapsed:.1f} fps)")

    # === 6. ASSEMBLE VIDEO ===
    out_path = os.path.join(channel_dir, f"{name}.mp4")
    ffmpeg_cmd = [
        "ffmpeg", "-y",
        "-framerate", str(FPS),
        "-i", os.path.join(frame_dir, "frame_%05d.png"),
        "-i", voice_path,
        "-i", music_path,
        "-filter_complex",
        f"[1:a]adelay=800|800,apad=whole_dur={total_time}[voice];"
        f"[2:a]apad=whole_dur={total_time}[bg];"
        f"[voice][bg]amix=inputs=2:duration=first:weights=3 1,"
        f"afade=t=in:d=0.3,afade=t=out:st={total_time - 1.5}:d=1.5[aout]",
        "-map", "0:v", "-map", "[aout]",
        "-t", str(total_time),
        "-c:v", "libx264", "-preset", "medium", "-crf", "22",
        "-c:a", "aac", "-b:a", "192k", "-ar", "44100", "-ac", "2",
        "-pix_fmt", "yuv420p", "-r", str(FPS),
        "-movflags", "+faststart",
        out_path
    ]

    result = subprocess.run(ffmpeg_cmd, capture_output=True, text=True, timeout=300)

    # Cleanup frames
    shutil.rmtree(frame_dir, ignore_errors=True)

    # Cleanup temp audio
    for tmp in [voice_path, music_path]:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except Exception:
                pass

    # Remove _bg_img from config (not serializable)
    config.pop("_bg_img", None)

    if result.returncode != 0 or not os.path.exists(out_path):
        print(f"    FAILED: {result.stderr[-300:]}")
        return None

    file_size = os.path.getsize(out_path) / (1024 * 1024)
    print(f"    OK: {out_path} ({file_size:.1f} MB, {total_time:.0f}s)")

    # === 7. WRITE METADATA JSON ===
    meta = {
        "file": out_path,
        "channel": channel,
        "channel_name": CHANNEL_META[channel]["channel_name"],
        "category": CHANNEL_META[channel]["category"],
        "name": name,
        "title": f"{config.get('hook', '')} | {config.get('title', '')}",
        "description": config.get("description", ""),
        "tags": list(dict.fromkeys(CHANNEL_META[channel]["tags_base"] + config.get("tags", []))),
        "voice": voice_name,
        "duration_seconds": round(total_time, 1),
        "file_size_mb": round(file_size, 1),
        "produced_at": datetime.now().isoformat(),
        "hook": config.get("hook", ""),
        "cta": config.get("cta", ""),
        "phone": config.get("phone", ""),
    }
    meta_path = out_path.replace(".mp4", "_meta.json")
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)

    return meta


# ============================================================
# CLI
# ============================================================

def list_all_shorts():
    """List all available shorts across all channels."""
    print("\n" + "=" * 70)
    print("  SHORTS FACTORY — Available Content Library")
    print("=" * 70)
    for channel, shorts in CHANNEL_CONTENT.items():
        voice = VOICE_MAP[channel]
        print(f"\n  [{channel.upper()}] — Voice: {voice} — {len(shorts)} shorts")
        print(f"  {'─' * 60}")
        for i, s in enumerate(shorts):
            phone = f" | {s.get('phone', '')}" if s.get("phone") else ""
            print(f"    {i:2d}. [{s['name']}] {s.get('hook', '')} — {s.get('title', '')}{phone}")
    total = sum(len(v) for v in CHANNEL_CONTENT.values())
    print(f"\n  TOTAL: {total} shorts across {len(CHANNEL_CONTENT)} channels")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="Shorts Factory — Multi-channel YouTube Shorts producer for THE HIVE",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 shorts_factory.py --list                    # List all available shorts
  python3 shorts_factory.py --channel ghost --count 3  # Produce 3 ghost shorts
  python3 shorts_factory.py --channel locksmith        # All locksmith shorts
  python3 shorts_factory.py --channel ai --pick 0,2    # Produce specific AI shorts
  python3 shorts_factory.py --channel all --count 2    # 2 from each channel
        """
    )
    parser.add_argument("--channel", "-c", type=str, default=None,
                        choices=list(CHANNEL_CONTENT.keys()) + ["all"],
                        help="Channel to produce for: ghost, locksmith, ai, hive, or all")
    parser.add_argument("--count", "-n", type=int, default=0,
                        help="Number of shorts to produce (0 = all available)")
    parser.add_argument("--pick", "-p", type=str, default=None,
                        help="Comma-separated indices to produce (e.g. 0,2,4)")
    parser.add_argument("--list", "-l", action="store_true",
                        help="List all available shorts without producing")
    parser.add_argument("--output", "-o", type=str, default=OUTPUT_DIR,
                        help=f"Output directory (default: {OUTPUT_DIR})")

    args = parser.parse_args()

    # Update output dir if specified
    if args.output != OUTPUT_DIR:
        os.makedirs(args.output, exist_ok=True)

    if args.list:
        list_all_shorts()
        return

    if not args.channel:
        parser.print_help()
        print("\nTip: Use --list to see all available shorts")
        return

    # Determine which channels to process
    if args.channel == "all":
        channels = list(CHANNEL_CONTENT.keys())
    else:
        channels = [args.channel]

    # Collect results
    all_results = []
    total_ok = 0
    total_fail = 0

    for channel in channels:
        shorts = CHANNEL_CONTENT[channel]

        # Apply --pick filter
        if args.pick:
            indices = [int(x.strip()) for x in args.pick.split(",")]
            shorts = [shorts[i] for i in indices if i < len(shorts)]

        # Apply --count limit
        if args.count > 0:
            shorts = shorts[:args.count]

        print(f"\n{'=' * 70}")
        print(f"  SHORTS FACTORY — [{channel.upper()}] — {len(shorts)} shorts to produce")
        print(f"  Voice: {VOICE_MAP[channel]}")
        print(f"  Output: {os.path.join(OUTPUT_DIR, channel)}")
        print(f"{'=' * 70}")

        for config in shorts:
            # Deep copy to avoid mutating the template
            cfg = dict(config)
            result = produce_short(cfg, channel)
            if result:
                all_results.append(result)
                total_ok += 1
            else:
                total_fail += 1

    # Summary
    print(f"\n{'=' * 70}")
    print(f"  PRODUCTION COMPLETE")
    print(f"  Success: {total_ok} | Failed: {total_fail} | Total: {total_ok + total_fail}")
    print(f"  Output: {OUTPUT_DIR}")
    if all_results:
        total_size = sum(r["file_size_mb"] for r in all_results)
        total_dur = sum(r["duration_seconds"] for r in all_results)
        print(f"  Total size: {total_size:.1f} MB | Total duration: {total_dur:.0f}s ({total_dur/60:.1f} min)")
        print(f"\n  Files produced:")
        for r in all_results:
            print(f"    [{r['channel']}] {r['name']} — {r['duration_seconds']}s, {r['file_size_mb']:.1f}MB")
            print(f"           {r['file']}")
    print(f"{'=' * 70}")

    # Write manifest
    manifest_path = os.path.join(OUTPUT_DIR, "manifest.json")
    manifest = {
        "produced_at": datetime.now().isoformat(),
        "total_shorts": total_ok,
        "total_failed": total_fail,
        "shorts": all_results,
    }
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    print(f"  Manifest: {manifest_path}")


if __name__ == "__main__":
    main()
