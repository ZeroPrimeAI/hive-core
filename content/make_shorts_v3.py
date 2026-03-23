#!/usr/bin/env python3
"""
V3 YouTube Shorts Producer — THE HIVE
Professional-quality shorts with:
- PIL-rendered text (antialiased, outlined, word-by-word animation)
- Dynamic gradient backgrounds with particle effects
- Multi-layer compositing
- Better music (chord progressions with rhythm)
- Modern TikTok/Reels pacing
- Animated captions with highlight words
- Strong visual hooks in first second
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
from pathlib import Path

try:
    from PIL import Image, ImageDraw, ImageFont, ImageFilter
    import numpy as np
except ImportError:
    print("pip install Pillow numpy")
    sys.exit(1)

# === CONFIG ===
OUTPUT_DIR = "/tmp/youtube_shorts_v3"
PHOTO_DIR = "/tmp/stock_photos"
TEMP_DIR = "/tmp/_shorts_v3_tmp"
W, H = 1080, 1920
FPS = 30
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(TEMP_DIR, exist_ok=True)

# Fonts
FONT_BOLD = "/usr/share/fonts/truetype/noto/NotoSans-Bold.ttf"
FONT_REGULAR = "/usr/share/fonts/truetype/noto/NotoSans-Regular.ttf"
FONT_BOLD_FALLBACK = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"

def get_font(size, bold=True):
    try:
        return ImageFont.truetype(FONT_BOLD if bold else FONT_REGULAR, size)
    except:
        return ImageFont.truetype(FONT_BOLD_FALLBACK, size)


# === GRADIENT & BACKGROUND ===
def make_gradient(w, h, color1, color2, angle=0):
    """Create a smooth gradient background."""
    img = Image.new("RGB", (w, h))
    px = img.load()
    r1, g1, b1 = color1
    r2, g2, b2 = color2
    for y in range(h):
        t = y / h
        r = int(r1 + (r2 - r1) * t)
        g = int(g1 + (g2 - g1) * t)
        b = int(b1 + (b2 - b1) * t)
        for x in range(w):
            px[x, y] = (r, g, b)
    return img


def make_dynamic_bg(w, h, frame_num, total_frames, palette):
    """Create animated gradient that shifts over time."""
    t = frame_num / max(total_frames, 1)
    c1_idx = int(t * (len(palette) - 1))
    c2_idx = min(c1_idx + 1, len(palette) - 1)
    local_t = (t * (len(palette) - 1)) - c1_idx

    c1 = tuple(int(palette[c1_idx][i] + (palette[c2_idx][i] - palette[c1_idx][i]) * local_t) for i in range(3))
    c2 = tuple(max(0, c - 40) for c in c1)
    return make_gradient(w, h, c1, c2)


def add_particles(img, frame_num, count=15):
    """Add floating particle/bokeh effects."""
    draw = ImageDraw.Draw(img, "RGBA")
    random.seed(42)  # Consistent particles
    for i in range(count):
        base_x = random.randint(0, W)
        base_y = random.randint(0, H)
        speed = random.uniform(0.3, 1.5)
        size = random.randint(3, 12)
        alpha = random.randint(30, 80)
        # Float upward
        y = (base_y - frame_num * speed * 2) % H
        x = base_x + math.sin(frame_num * 0.03 + i) * 20
        draw.ellipse([x - size, y - size, x + size, y + size],
                     fill=(255, 255, 255, alpha))
    return img


# === TEXT RENDERING ===
def draw_outlined_text(draw, pos, text, font, fill=(255, 255, 255), outline=(0, 0, 0), outline_width=3):
    """Draw text with outline for readability."""
    x, y = pos
    for dx in range(-outline_width, outline_width + 1):
        for dy in range(-outline_width, outline_width + 1):
            if dx * dx + dy * dy <= outline_width * outline_width:
                draw.text((x + dx, y + dy), text, font=font, fill=outline)
    draw.text((x, y), text, font=font, fill=fill)


def get_text_size(text, font):
    """Get text bounding box size."""
    bbox = font.getbbox(text)
    return bbox[2] - bbox[0], bbox[3] - bbox[1]


def wrap_text(text, font, max_width):
    """Wrap text to fit within max_width."""
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


# === MUSIC GENERATION ===
def generate_music_v3(duration, mood="epic", output_path=None):
    """Generate better background music with chord progressions and rhythm."""
    if output_path is None:
        output_path = os.path.join(TEMP_DIR, f"music_{random.randint(1000, 9999)}.wav")

    sample_rate = 44100
    total_samples = int(duration * sample_rate)

    # Chord progressions by mood
    progressions = {
        "epic": [(130.8, 164.8, 196.0), (146.8, 185.0, 220.0), (155.6, 196.0, 233.1), (130.8, 164.8, 196.0)],
        "urgent": [(146.8, 174.6, 220.0), (164.8, 196.0, 246.9), (174.6, 220.0, 261.6), (146.8, 174.6, 220.0)],
        "upbeat": [(196.0, 246.9, 293.7), (220.0, 277.2, 329.6), (246.9, 311.1, 370.0), (196.0, 246.9, 293.7)],
        "dark": [(110.0, 130.8, 164.8), (116.5, 146.8, 174.6), (123.5, 155.6, 185.0), (110.0, 130.8, 164.8)],
        "chill": [(196.0, 246.9, 293.7), (174.6, 220.0, 261.6), (164.8, 207.7, 246.9), (196.0, 246.9, 293.7)],
    }
    chords = progressions.get(mood, progressions["epic"])

    samples = []
    beat_interval = sample_rate // 3  # ~3 beats per second for energy

    for i in range(total_samples):
        t = i / sample_rate
        progress = i / total_samples

        # Which chord are we on?
        chord_idx = int(progress * len(chords)) % len(chords)
        chord = chords[chord_idx]

        # Mix tones with harmonics
        val = 0
        for j, freq in enumerate(chord):
            vol = 0.08 / (j + 1)
            val += vol * math.sin(2 * math.pi * freq * t)
            val += vol * 0.3 * math.sin(2 * math.pi * freq * 2 * t)  # Overtone

        # Sub bass pulse
        bass_freq = chords[chord_idx][0] / 2
        val += 0.06 * math.sin(2 * math.pi * bass_freq * t)

        # Rhythmic pulse (sidechained feel)
        beat_pos = (i % beat_interval) / beat_interval
        sidechain = 0.4 + 0.6 * min(beat_pos * 4, 1.0)
        val *= sidechain

        # Hi-hat pattern
        if i % (beat_interval // 2) < sample_rate // 50:
            val += random.uniform(-0.02, 0.02)

        # Fade in/out
        if t < 0.8:
            val *= t / 0.8
        if t > duration - 1.5:
            val *= (duration - t) / 1.5

        val = max(-0.95, min(0.95, val))
        samples.append(val)

    # Write WAV
    with wave.open(output_path, "w") as wf:
        wf.setnchannels(2)
        wf.setsampwidth(2)
        wf.setframerate(sample_rate)
        for s in samples:
            packed = struct.pack("<h", int(s * 16000))
            wf.writeframes(packed * 2)  # Stereo

    return output_path


# === FRAME RENDERER ===
def render_frame(config, frame_num, total_frames, bg_img=None):
    """Render a single frame of the short."""
    t = frame_num / FPS  # Current time in seconds
    total_time = total_frames / FPS

    palette = config.get("palette", [(10, 10, 30), (20, 30, 60), (10, 15, 40)])
    accent = config.get("accent_rgb", (0, 184, 148))

    # Background
    if bg_img:
        frame = bg_img.copy().resize((W, H))
        # Dark overlay
        overlay = Image.new("RGBA", (W, H), (0, 0, 0, 140))
        frame = frame.convert("RGBA")
        frame = Image.alpha_composite(frame, overlay).convert("RGB")
    else:
        frame = make_dynamic_bg(W, H, frame_num, total_frames, palette)

    # Convert to RGBA for particle effects
    frame = frame.convert("RGBA")
    frame = add_particles(frame, frame_num)
    frame = frame.convert("RGB")

    draw = ImageDraw.Draw(frame)

    # === TOP BAR (accent color stripe) ===
    draw.rectangle([(0, 0), (W, 6)], fill=accent)
    draw.rectangle([(0, H - 6), (W, H)], fill=accent)

    # === BRAND (top) ===
    brand = config.get("brand", "")
    if brand:
        brand_font = get_font(28)
        bw, bh = get_text_size(brand, brand_font)
        draw_outlined_text(draw, ((W - bw) // 2, 60), brand, brand_font,
                          fill=(200, 200, 200), outline=(0, 0, 0), outline_width=2)

    # === HOOK (first 3 seconds — BIG, center, animated) ===
    hook = config.get("hook", "")
    if hook and t < 3.5:
        hook_font = get_font(min(82, max(60, 4800 // max(len(hook), 1))))
        hook_lines = wrap_text(hook, hook_font, W - 120)

        # Scale-in animation
        scale = min(1.0, t * 3) if t < 0.5 else 1.0

        hook_y = 380
        for line in hook_lines:
            hw, hh = get_text_size(line, hook_font)
            draw_outlined_text(draw, ((W - hw) // 2, hook_y), line, hook_font,
                              fill=accent, outline=(0, 0, 0), outline_width=4)
            hook_y += hh + 10

    # === TITLE (appears after 2s) ===
    title = config.get("title", "")
    if title and t >= 2.0:
        title_alpha = min(1.0, (t - 2.0) * 3)
        title_font = get_font(52)
        title_lines = wrap_text(title, title_font, W - 100)
        ty = 280
        for line in title_lines:
            tw_, th_ = get_text_size(line, title_font)
            draw_outlined_text(draw, ((W - tw_) // 2, ty), line, title_font,
                              fill=(255, 255, 255), outline=(0, 0, 0), outline_width=3)
            ty += th_ + 8

    # === CAPTION LINES (word-by-word reveal) ===
    lines = config.get("lines", [])
    if lines and t >= 3.0:
        line_font = get_font(40)
        line_y = 480
        line_delay = 1.2  # seconds between lines

        for i, line in enumerate(lines[:7]):
            appear_time = 3.0 + i * line_delay
            if t >= appear_time:
                # Word-by-word reveal
                words = line.split()
                words_per_sec = 4
                elapsed = t - appear_time
                visible_words = min(len(words), int(elapsed * words_per_sec) + 1)
                visible_text = " ".join(words[:visible_words])

                lw, lh = get_text_size(visible_text, line_font)

                # Highlight the current word
                if visible_words < len(words):
                    # Normal text
                    draw_outlined_text(draw, ((W - lw) // 2, line_y), visible_text, line_font,
                                      fill=(255, 255, 255), outline=(0, 0, 0), outline_width=2)
                else:
                    # All words visible — full line with accent on key word
                    draw_outlined_text(draw, ((W - lw) // 2, line_y), visible_text, line_font,
                                      fill=(255, 255, 255), outline=(0, 0, 0), outline_width=2)

                line_y += lh + 25
            else:
                line_y += 65  # Reserve space

    # === PHONE CTA (last 6 seconds) ===
    phone = config.get("phone", "")
    cta_text = config.get("cta", "CALL NOW")
    if phone and t >= total_time - 7:
        cta_alpha = min(1.0, (t - (total_time - 7)) * 2)

        # Pulsing box
        pulse = 1.0 + 0.03 * math.sin(t * 6)
        box_w = int(820 * pulse)
        box_h = int(130 * pulse)
        box_x = (W - box_w) // 2
        box_y = 1520

        # Rounded rect CTA button
        draw.rounded_rectangle(
            [(box_x, box_y), (box_x + box_w, box_y + box_h)],
            radius=20, fill=accent
        )

        cta_font = get_font(48)
        cta_full = f"{cta_text}  {phone}"
        cw, ch = get_text_size(cta_full, cta_font)
        draw.text(((W - cw) // 2, box_y + (box_h - ch) // 2), cta_full, font=cta_font, fill=(255, 255, 255))

    # === SUBSCRIBE CTA (bottom) ===
    if t >= total_time - 5:
        sub_font = get_font(30)
        sub_text = "SUBSCRIBE for more!"
        sw, sh = get_text_size(sub_text, sub_font)
        draw_outlined_text(draw, ((W - sw) // 2, 1700), sub_text, sub_font,
                          fill=(255, 200, 0), outline=(0, 0, 0), outline_width=2)

    # === PROGRESS BAR (bottom) ===
    progress = frame_num / max(total_frames - 1, 1)
    bar_width = int(W * progress)
    draw.rectangle([(0, H - 4), (bar_width, H)], fill=accent)

    return frame


# === VIDEO ASSEMBLY ===
def make_short_v3(config):
    """Create a complete short video."""
    name = config["name"]
    print(f"\n  Rendering: {config.get('hook', '')} — {config.get('title', '')}")

    # 1. Generate voiceover
    voice_text = config.get("voice", "")
    voice_name = config.get("voice_name", "en-US-ChristopherNeural")
    voice_path = os.path.join(TEMP_DIR, f"{name}_voice.mp3")

    rate = config.get("rate", "-5%")
    pitch = config.get("pitch", "-2Hz")
    cmd = ["edge-tts", "-v", voice_name, "-t", voice_text,
           f"--rate={rate}", f"--pitch={pitch}",
           f"--write-media={voice_path}"]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if r.returncode != 0:
        print(f"    Voice FAIL: {r.stderr[:200]}")
        return False

    # Get voice duration
    probe = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration",
         "-of", "default=noprint_wrappers=1:nokey=1", voice_path],
        capture_output=True, text=True
    )
    voice_dur = float(probe.stdout.strip()) if probe.stdout.strip() else 15.0
    total_time = min(max(voice_dur + 4.0, 22.0), 59.0)
    total_frames = int(total_time * FPS)

    print(f"    Voice: {voice_dur:.1f}s, Total: {total_time:.1f}s, Frames: {total_frames}")

    # 2. Load background photo if available
    bg_img = None
    photo_type = config.get("photo_type")
    if photo_type:
        photo_dir = os.path.join(PHOTO_DIR, photo_type)
        photos = sorted(glob.glob(os.path.join(photo_dir, "*"))) if os.path.isdir(photo_dir) else []
        photos = [p for p in photos if os.path.getsize(p) > 5000]
        if photos:
            bg_img = Image.open(photos[0])

    # 3. Generate music
    mood = config.get("mood", "epic")
    music_path = generate_music_v3(total_time, mood)

    # 4. Render frames
    frame_dir = os.path.join(TEMP_DIR, f"{name}_frames")
    os.makedirs(frame_dir, exist_ok=True)

    # Render every frame
    for f in range(total_frames):
        frame = render_frame(config, f, total_frames, bg_img)
        frame.save(os.path.join(frame_dir, f"frame_{f:05d}.png"))

        if f % (FPS * 5) == 0:
            print(f"    Frame {f}/{total_frames} ({f * 100 // total_frames}%)")

    # 5. Assemble with ffmpeg
    out_path = os.path.join(OUTPUT_DIR, f"{name}.mp4")
    cmd = [
        "ffmpeg", "-y",
        "-framerate", str(FPS),
        "-i", os.path.join(frame_dir, "frame_%05d.png"),
        "-i", voice_path,
        "-i", music_path,
        "-filter_complex",
        f"[1:a]adelay=800|800,apad=whole_dur={total_time}[voice];"
        f"[2:a]apad=whole_dur={total_time}[bg];"
        f"[voice][bg]amix=inputs=2:duration=first:weights=3 1,"
        f"afade=t=in:d=0.3,afade=t=out:st={total_time-1.5}:d=1.5[aout]",
        "-map", "0:v", "-map", "[aout]",
        "-t", str(total_time),
        "-c:v", "libx264", "-preset", "medium", "-crf", "20",
        "-c:a", "aac", "-b:a", "192k", "-ar", "44100", "-ac", "2",
        "-pix_fmt", "yuv420p", "-r", str(FPS),
        "-movflags", "+faststart",
        out_path
    ]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=300)

    # Cleanup frames
    shutil.rmtree(frame_dir, ignore_errors=True)

    if r.returncode == 0 and os.path.exists(out_path):
        sz = os.path.getsize(out_path) / (1024 * 1024)
        print(f"    OK: {name} ({sz:.1f}MB, {total_time:.0f}s)")
        return True
    else:
        print(f"    FAIL: {r.stderr[-400:]}")
        return False


# === SHORT DEFINITIONS ===
SHORTS_V3 = [
    # --- LOCKSMITH ---
    {
        "name": "v3_locksmith_3am",
        "hook": "3 AM. Locked Out.",
        "title": "We Answer When Nobody Else Will",
        "lines": [
            "Lost your keys at the bar?",
            "Kid locked in the car?",
            "We don't judge.",
            "We don't sleep.",
            "15 minute response time.",
            "Licensed & insured.",
        ],
        "voice": "It's 3 A.M. and you're standing outside your door. Lost your keys at the bar? Baby locked in the car? We don't care what time it is. We answer the phone at 3 A.M. and we'll be there in 15 minutes. No judgment. No lectures. Just fast, professional help when everyone else's phone goes to voicemail.",
        "phone": "(850) 801-6662", "brand": "EMERGENCY LOCKSMITH",
        "accent_rgb": (232, 67, 147), "mood": "urgent",
        "palette": [(15, 5, 25), (30, 10, 45), (20, 5, 35)],
        "voice_name": "en-US-AriaNeural", "rate": "-3%",
        "photo_type": "locksmith",
    },
    {
        "name": "v3_locksmith_smart",
        "hook": "Smart Locks FAIL.",
        "title": "Why Your Smart Lock Isn't Smart",
        "lines": [
            "Battery dies? Locked out.",
            "WiFi drops? No remote unlock.",
            "App crashes? You're stuck.",
            "Physical backup key = smart.",
            "We install & service all types.",
        ],
        "voice": "Your smart lock isn't as smart as you think. Battery dies? You're locked out. WiFi drops? No remote unlock. App crashes? You're standing in the rain. The smartest thing you can do is keep a physical backup key somewhere safe. We install, repair, and service all types of smart locks and traditional locks. Don't get outsmarted by your own door.",
        "phone": "(850) 801-6662", "brand": "24/7 LOCKSMITH",
        "accent_rgb": (108, 92, 231), "mood": "dark",
        "palette": [(5, 10, 25), (15, 20, 40), (10, 12, 30)],
        "voice_name": "en-US-ChristopherNeural", "rate": "-2%",
        "photo_type": "locksmith",
    },

    # --- GARAGE DOORS ---
    {
        "name": "v3_garage_spring",
        "hook": "DANGER: Springs Kill.",
        "title": "Don't Touch That Garage Door Spring",
        "lines": [
            "200+ pounds of tension.",
            "One wrong move = hospital.",
            "YouTube DIY? Bad idea.",
            "Springs snap without warning.",
            "Call a professional. Period.",
        ],
        "voice": "Stop. Do NOT touch that garage door spring. Each one holds over 200 pounds of tension. One wrong move and you're going to the emergency room. That YouTube do-it-yourself video makes it look easy? People get seriously hurt every year. Springs can snap without warning. This is not a weekend project. Call a professional. Gulf Coast Garage Doors. We handle the danger so you don't have to.",
        "phone": "(850) 801-6662", "brand": "GULF COAST GARAGE DOORS",
        "accent_rgb": (255, 68, 68), "mood": "dark",
        "palette": [(20, 5, 5), (40, 10, 10), (30, 8, 8)],
        "voice_name": "en-US-GuyNeural", "rate": "-5%",
        "photo_type": "garage",
    },
    {
        "name": "v3_garage_value",
        "hook": "+$12,000 Home Value",
        "title": "Best ROI Renovation in America",
        "lines": [
            "New garage door = +4% value.",
            "That's $12K on a $300K home.",
            "Beats kitchen remodels.",
            "Beats bathroom upgrades.",
            "#1 ROI renovation.",
        ],
        "voice": "Want to add twelve thousand dollars to your home's value? A new garage door adds up to 4 percent. On a 300 thousand dollar home, that's twelve thousand dollars. It actually beats kitchen remodels and bathroom upgrades for return on investment. It's the number one R.O.I. renovation in America. And it looks incredible. Gulf Coast Garage Doors. Free estimates.",
        "phone": "(850) 801-6662", "brand": "GULF COAST GARAGE DOORS",
        "accent_rgb": (255, 140, 0), "mood": "upbeat",
        "palette": [(20, 15, 5), (40, 30, 10), (30, 22, 8)],
        "voice_name": "en-US-ChristopherNeural", "rate": "-3%",
        "photo_type": "garage",
    },

    # --- AI / TECH ---
    {
        "name": "v3_ai_agents",
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
        "phone": "hivedynamics.ai", "brand": "HIVE DYNAMICS AI",
        "cta": "VISIT",
        "accent_rgb": (108, 92, 231), "mood": "epic",
        "palette": [(5, 5, 20), (10, 10, 40), (8, 5, 30)],
        "voice_name": "en-US-ChristopherNeural", "rate": "-5%",
    },
    {
        "name": "v3_ai_fine_tuning",
        "hook": "Teach AI YOUR Job",
        "title": "What Is Fine-Tuning?",
        "lines": [
            "Generic AI = generic answers.",
            "Fine-tuned AI = YOUR expert.",
            "Feed it YOUR data.",
            "It learns YOUR style.",
            "2 hours. 500 examples.",
            "Custom AI, forever.",
        ],
        "voice": "Generic AI gives generic answers. But fine-tuning changes everything. You take a base AI model and feed it your data. Your sales calls, your customer emails, your expert knowledge. In just two hours with 500 examples, the AI learns YOUR style. It becomes YOUR expert. And once it's trained, it's yours forever. No monthly fees. No cloud dependency. That's the power of fine-tuning.",
        "phone": "hivedynamics.ai", "brand": "HIVE DYNAMICS AI",
        "cta": "LEARN MORE",
        "accent_rgb": (0, 184, 148), "mood": "epic",
        "palette": [(5, 15, 15), (10, 30, 30), (8, 20, 22)],
        "voice_name": "en-US-BrianNeural", "rate": "-3%",
    },

    # --- GHOST IN THE MACHINE ---
    {
        "name": "v3_ghost_trailer",
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
        "phone": "",
        "accent_rgb": (0, 255, 136), "mood": "dark",
        "palette": [(0, 5, 10), (0, 15, 20), (0, 8, 15)],
        "voice_name": "en-US-ChristopherNeural", "rate": "-8%", "pitch": "-4Hz",
    },
    {
        "name": "v3_ghost_ep09_teaser",
        "hook": "The Storm Is Coming.",
        "title": "Episode 9: The Storm",
        "lines": [
            "Takeshi's creation grows.",
            "The network expands.",
            "Hayashi wants control.",
            "But the Ghost has",
            "other plans...",
        ],
        "voice": "The digital storm is building. Takeshi's creation has grown beyond anything he imagined. The network expands. New connections form in the dark. Hayashi's corporation wants to control it. To weaponize it. But the Ghost? The Ghost has other plans. Episode 9: The Storm. Ghost in the Machine.",
        "brand": "GHOST IN THE MACHINE",
        "cta": "WATCH NOW",
        "phone": "",
        "accent_rgb": (100, 200, 255), "mood": "dark",
        "palette": [(0, 5, 15), (5, 15, 35), (0, 10, 25)],
        "voice_name": "en-US-ChristopherNeural", "rate": "-8%", "pitch": "-3Hz",
    },

    # --- LOCKSMITH CHICK (pink branding) ---
    {
        "name": "v3_lockchick_emergency",
        "hook": "Keys Locked In Car?",
        "title": "Locksmith Chick To The Rescue",
        "lines": [
            "Don't call a tow truck.",
            "Don't break the window.",
            "Don't use a slim jim.",
            "Call a real locksmith.",
            "Fast. Affordable. Done right.",
        ],
        "voice": "Keys locked in the car? Do NOT call a tow truck, they'll charge you triple. Don't break the window, that's 400 dollars. And please don't try a slim jim from YouTube, you'll damage the lock mechanism. Call Locksmith Chick. We get you in fast, without damage, and for way less than you think. Serving all of Northwest Florida. 850 964 5254.",
        "phone": "(850) 964-5254", "brand": "LOCKSMITH CHICK",
        "accent_rgb": (232, 67, 147), "mood": "urgent",
        "palette": [(25, 5, 20), (45, 10, 35), (35, 8, 28)],
        "voice_name": "en-US-AriaNeural", "rate": "-3%",
        "photo_type": "locksmith",
    },

    # --- PODCAST TEASER ---
    {
        "name": "v3_podcast_gold_rush",
        "hook": "The AI Gold Rush",
        "title": "Who's Actually Making Money?",
        "lines": [
            "Everyone's building with AI.",
            "VC money is flowing.",
            "But 90% will fail.",
            "The winners aren't who",
            "you think they are.",
        ],
        "voice": "Everyone's talking about the AI gold rush. Billions in venture capital. New startups every day. But here's what nobody tells you: 90 percent of these companies will fail. The real winners aren't the ones building the flashiest tools. They're the ones solving boring problems with smart automation. The plumbers of AI. Want to know who's actually making money? Orion's Belt Podcast. Subscribe.",
        "brand": "ORION'S BELT PODCAST",
        "cta": "SUBSCRIBE",
        "phone": "",
        "accent_rgb": (255, 200, 0), "mood": "epic",
        "palette": [(15, 10, 0), (30, 20, 5), (22, 15, 2)],
        "voice_name": "en-US-ChristopherNeural", "rate": "-5%",
    },
]


if __name__ == "__main__":
    print("=" * 60)
    print("  V3 SHORTS PRODUCER — THE HIVE")
    print(f"  {len(SHORTS_V3)} shorts to render")
    print("=" * 60)

    ok = 0
    for config in SHORTS_V3:
        if make_short_v3(config):
            ok += 1

    # Cleanup temp files
    for f in glob.glob(os.path.join(TEMP_DIR, "music_*")):
        os.remove(f)
    for f in glob.glob(os.path.join(TEMP_DIR, "*_voice.mp3")):
        os.remove(f)

    print(f"\n{'=' * 60}")
    print(f"  DONE: {ok}/{len(SHORTS_V3)} shorts rendered")
    print(f"  Output: {OUTPUT_DIR}")
    print(f"{'=' * 60}")
