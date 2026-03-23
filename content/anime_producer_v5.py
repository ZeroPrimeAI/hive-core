#!/usr/bin/env python3
"""
Ghost in the Machine — Full Production Pipeline v5
MAJOR UPGRADE: Emotional voice acting, mood-reactive music, sound effects, scene transitions
"""

import subprocess
import os
import re
import textwrap
import random
import math

ART_DIR = "/tmp/ghost_art"
SCRIPT_DIR = "/tmp/ghost_scripts"
OUTPUT_DIR = "/tmp/ghost_anime_v5"
VOICE_DIR = "/tmp/ghost_voices_v5"
MUSIC_DIR = "/tmp/ghost_music_v5"
SFX_DIR = "/tmp/ghost_sfx"
TEMP_DIR = "/tmp/ghost_anime_temp_v5"

for d in [OUTPUT_DIR, VOICE_DIR, MUSIC_DIR, SFX_DIR, TEMP_DIR]:
    os.makedirs(d, exist_ok=True)

# ── UPGRADED VOICE CASTING ──────────────────────────────────────
# Picked for expressiveness and personality, not just gender
VOICE_MAP = {
    "narrator": "en-US-ChristopherNeural",  # Authoritative, clear, passionate
    "takeshi": "en-US-BrianNeural",          # Approachable young protagonist
    "yuki": "en-US-AriaNeural",              # Confident, expressive scientist
    "watanabe": "en-GB-RyanNeural",          # Distinguished elder (British = wise)
    "hayashi": "en-US-DavisNeural",          # Corporate antagonist
    "ryo": "en-US-AndrewNeural",             # Warm, friendly
    "hikari": "en-US-EmmaNeural",            # Cheerful, bright
    "ai_voice": "en-US-JennyNeural",         # The AI character — calm, precise
}

CHAR_COLORS = {
    "takeshi": "0x4a9eff", "yuki": "0xff69b4", "hayashi": "0xffd700",
    "watanabe": "0xdaa520", "ryo": "0x66ff66", "hikari": "0xff8c00",
    "ai_voice": "0x00ffcc",
}

# ── MOOD DETECTION ──────────────────────────────────────────────
MOOD_KEYWORDS = {
    "dramatic": ["storm", "typhoon", "thunder", "lightning", "flood", "danger",
                 "death", "destroy", "crash", "collapse", "tear", "scream",
                 "darkness", "fear", "dread", "horrif", "catastroph"],
    "tense": ["silence", "stare", "pause", "wait", "hover", "hesitat",
              "slowly", "careful", "quiet", "whisper", "shadow", "threat",
              "watch", "creep", "lurk", "sense", "realiz"],
    "emotional": ["love", "tears", "heart", "beautiful", "remember",
                  "goodbye", "hope", "dream", "prayer", "miss",
                  "together", "promise", "believe", "trust"],
    "action": ["run", "race", "sprint", "grab", "pull", "push", "fight",
               "chase", "explode", "surge", "rush", "leap", "throw",
               "crash", "slam", "dive", "fast", "quick", "hurry"],
    "wonder": ["glow", "light", "shimmer", "pulse", "bloom", "awaken",
               "emerge", "evolve", "pattern", "connect", "network",
               "constellation", "alive", "conscious", "aware", "born"],
    "calm": ["morning", "sunrise", "ocean", "gentle", "peace", "still",
             "drift", "float", "breeze", "warm", "quiet", "seren"],
}


def detect_mood(text, context=""):
    """Detect the emotional mood of text for prosody control."""
    combined = (text + " " + context).lower()
    scores = {}
    for mood, keywords in MOOD_KEYWORDS.items():
        scores[mood] = sum(1 for kw in keywords if kw in combined)
    best = max(scores, key=scores.get) if max(scores.values()) > 0 else "neutral"
    return best


# ── PROSODY SETTINGS PER MOOD ───────────────────────────────────
# rate: speech speed (negative = slower), pitch: voice pitch
MOOD_PROSODY = {
    "dramatic": {"rate": "-18%", "pitch": "-4Hz"},   # Slow, deep, gravitas
    "tense":    {"rate": "-12%", "pitch": "-6Hz"},    # Slow, lower, suspenseful
    "emotional": {"rate": "-15%", "pitch": "+2Hz"},   # Slow, slightly higher, feeling
    "action":   {"rate": "+12%", "pitch": "+3Hz"},    # Fast, energetic
    "wonder":   {"rate": "-8%",  "pitch": "+5Hz"},    # Slightly slow, awe
    "calm":     {"rate": "-5%",  "pitch": "+0Hz"},    # Gentle, natural
    "neutral":  {"rate": "+0%",  "pitch": "+0Hz"},    # Default
}

EPISODES = {
    "ep09_the_storm": {
        "title": "THE STORM",
        "subtitle": "Episode 9",
        "number": 9,
        "scenes": ["title", "scene01", "scene02", "scene03", "scene04",
                    "scene05", "scene06", "scene07", "scene08", "endcard"],
        "next": "Next: Episode 10 - Digital Fisherman",
        "mood_override": "dramatic",  # Overall episode mood
    },
    "ep10_digital_fisherman": {
        "title": "DIGITAL FISHERMAN",
        "subtitle": "Episode 10",
        "number": 10,
        "scenes": ["title", "scene01", "scene02", "scene03", "scene04",
                    "scene05", "scene06", "endcard"],
        "next": "Next: Episode 11 - Ghost Protocol",
        "mood_override": "calm",
    },
    "ep11_ghost_protocol": {
        "title": "GHOST PROTOCOL",
        "subtitle": "Episode 11",
        "number": 11,
        "scenes": ["title", "scene01", "scene02", "scene03", "scene04", "endcard"],
        "next": "Next: Episode 12 - The Voice",
        "mood_override": "tense",
    },
    "ep12_the_voice": {
        "title": "THE VOICE",
        "subtitle": "Episode 12",
        "number": 12,
        "scenes": ["title", "scene01", "scene02", "scene03", "endcard"],
        "next": "Next: Episode 13 - Convergence",
        "mood_override": "wonder",
    },
    "ep13_convergence": {
        "title": "CONVERGENCE",
        "subtitle": "Episode 13 - Season Finale",
        "number": 13,
        "scenes": ["title", "scene01", "scene02", "scene03", "scene04", "endcard"],
        "next": "Season 2 Coming Soon",
        "mood_override": "wonder",
    },
}


# ── SOUND EFFECTS GENERATOR ─────────────────────────────────────
def generate_sfx(sfx_type, output_path, duration=10):
    """Generate atmospheric sound effects with FFmpeg."""
    if os.path.exists(output_path) and os.path.getsize(output_path) > 500:
        return True

    filters = {
        "rain": (
            f"anoisesrc=d={duration}:c=pink:r=44100:a=0.3,"
            f"bandpass=f=3000:w=2000,"
            f"tremolo=f=8:d=0.4,"
            f"volume=0.5,"
            f"afade=t=in:d=2,afade=t=out:st={max(0,duration-2)}:d=2"
        ),
        "thunder": (
            f"sine=f=40:d=3[s1];"
            f"anoisesrc=d=3:c=brown:a=0.8[s2];"
            f"[s1][s2]amix=inputs=2,"
            f"afade=t=in:d=0.1,afade=t=out:st=0.5:d=2.5,"
            f"volume=0.7"
        ),
        "ocean": (
            f"anoisesrc=d={duration}:c=brown:r=44100:a=0.4,"
            f"bandpass=f=400:w=300,"
            f"tremolo=f=0.15:d=0.8,"
            f"volume=0.35,"
            f"afade=t=in:d=3,afade=t=out:st={max(0,duration-3)}:d=3"
        ),
        "wind": (
            f"anoisesrc=d={duration}:c=pink:r=44100:a=0.5,"
            f"bandpass=f=600:w=400,"
            f"tremolo=f=0.3:d=0.6,"
            f"volume=0.3,"
            f"afade=t=in:d=2,afade=t=out:st={max(0,duration-2)}:d=2"
        ),
        "lab_hum": (
            f"sine=f=60:d={duration}[h1];"
            f"sine=f=120:d={duration}[h2];"
            f"anoisesrc=d={duration}:c=white:a=0.02[n];"
            f"[h1]volume=0.08[v1];[h2]volume=0.04[v2];[n]lowpass=f=200[v3];"
            f"[v1][v2][v3]amix=inputs=3:duration=first,"
            f"volume=0.25,"
            f"afade=t=in:d=2,afade=t=out:st={max(0,duration-2)}:d=2"
        ),
        "digital": (
            f"sine=f=800:d={duration}[d1];"
            f"sine=f=1200:d={duration}[d2];"
            f"sine=f=400:d={duration}[d3];"
            f"[d1]volume=0.03,tremolo=f=4:d=0.9[v1];"
            f"[d2]volume=0.02,tremolo=f=6:d=0.8[v2];"
            f"[d3]volume=0.04,tremolo=f=2:d=0.5[v3];"
            f"[v1][v2][v3]amix=inputs=3:duration=first,"
            f"aecho=0.8:0.88:60:0.4,"
            f"volume=0.3,"
            f"afade=t=in:d=1,afade=t=out:st={max(0,duration-1)}:d=1"
        ),
    }

    if sfx_type not in filters:
        return False

    cmd = [
        "ffmpeg", "-y", "-filter_complex", filters[sfx_type],
        "-c:a", "aac", "-b:a", "128k", "-ar", "44100", "-ac", "2",
        "-t", str(duration), output_path
    ]
    return subprocess.run(cmd, capture_output=True, text=True).returncode == 0


# ── MOOD-REACTIVE MUSIC GENERATOR ───────────────────────────────
def generate_mood_music(mood, output_path, duration=120):
    """Generate mood-specific background music."""
    if os.path.exists(output_path) and os.path.getsize(output_path) > 1000:
        return True

    # Different chord voicings per mood
    configs = {
        "dramatic": {  # C minor, low, heavy
            "freqs": [55.0, 65.4, 130.8, 155.6, 196.0],
            "vols": [0.18, 0.15, 0.08, 0.06, 0.04],
            "echo": "aecho=0.8:0.88:800|1200:0.35|0.25",
            "tremolo": "tremolo=f=0.08:d=0.4",
            "lowpass": 600,
        },
        "tense": {  # Diminished, dissonant
            "freqs": [55.0, 77.8, 116.5, 155.6, 233.1],
            "vols": [0.15, 0.10, 0.08, 0.06, 0.04],
            "echo": "aecho=0.8:0.9:600|900:0.4|0.3",
            "tremolo": "tremolo=f=0.15:d=0.5",
            "lowpass": 500,
        },
        "emotional": {  # Major 7th, warm
            "freqs": [65.4, 82.4, 130.8, 164.8, 246.9],
            "vols": [0.14, 0.10, 0.08, 0.06, 0.04],
            "echo": "aecho=0.8:0.9:700|1100:0.3|0.2",
            "tremolo": "tremolo=f=0.06:d=0.3",
            "lowpass": 900,
        },
        "action": {  # Power chord, driving
            "freqs": [55.0, 82.4, 110.0, 165.0, 220.0],
            "vols": [0.18, 0.12, 0.10, 0.06, 0.04],
            "echo": "aecho=0.6:0.7:300|500:0.2|0.15",
            "tremolo": "tremolo=f=0.5:d=0.3",
            "lowpass": 1200,
        },
        "wonder": {  # Major with shimmer
            "freqs": [65.4, 98.0, 130.8, 196.0, 329.6],
            "vols": [0.12, 0.08, 0.08, 0.06, 0.05],
            "echo": "aecho=0.8:0.92:900|1400:0.35|0.25",
            "tremolo": "tremolo=f=0.05:d=0.25",
            "lowpass": 1100,
        },
        "calm": {  # Pentatonic, gentle
            "freqs": [65.4, 73.4, 98.0, 130.8, 146.8],
            "vols": [0.10, 0.07, 0.06, 0.05, 0.04],
            "echo": "aecho=0.8:0.92:1000|1500:0.3|0.2",
            "tremolo": "tremolo=f=0.04:d=0.2",
            "lowpass": 800,
        },
    }
    cfg = configs.get(mood, configs["calm"])

    parts = []
    for i, (freq, vol) in enumerate(zip(cfg["freqs"], cfg["vols"])):
        parts.append(f"sine=f={freq}:d={duration}[s{i}]")
        parts.append(f"[s{i}]volume={vol}[v{i}]")

    n = len(cfg["freqs"])
    mix_inputs = "".join(f"[v{i}]" for i in range(n))
    fade_out = max(0, duration - 5)

    filt = (
        ";".join(parts) + ";"
        f"{mix_inputs}amix=inputs={n}:duration=first,"
        f"{cfg['echo']},"
        f"lowpass=f={cfg['lowpass']},"
        f"{cfg['tremolo']},"
        f"volume=0.4,"
        f"afade=t=in:ss=0:d=5,afade=t=out:st={fade_out}:d=5"
        f"[music]"
    )

    cmd = [
        "ffmpeg", "-y", "-filter_complex", filt,
        "-map", "[music]", "-c:a", "aac", "-b:a", "128k",
        "-ar", "44100", "-ac", "2", "-t", str(duration), output_path
    ]
    return subprocess.run(cmd, capture_output=True, text=True).returncode == 0


# ── SCRIPT PARSER (enhanced with mood context) ──────────────────
def parse_script(path):
    """Extract narration, dialogue, and stage directions with mood context."""
    if not os.path.exists(path):
        return []
    with open(path) as f:
        content = f.read()

    blocks = []
    lines = content.split("\n")
    last_visual = ""  # Track visual descriptions for mood context
    i = 0

    while i < len(lines):
        line = lines[i].strip()
        if not line or line.startswith("=") or line.startswith("GHOST IN") or \
           line.startswith("Runtime") or line.startswith("FULL NARR"):
            i += 1
            continue

        # Capture visual descriptions for mood context
        if line.startswith("[VISUAL:"):
            visual_text = line
            while i < len(lines) and "]" not in visual_text:
                i += 1
                if i < len(lines):
                    visual_text += " " + lines[i].strip()
            last_visual = visual_text
            i += 1
            continue

        # Skip section headers but note them
        if line.startswith("COLD OPEN") or line.startswith("ACT ") or \
           line.startswith("["):
            i += 1
            continue

        # NARRATOR blocks
        if "NARRATOR" in line:
            i += 1
            text = ""
            while i < len(lines):
                l = lines[i].strip()
                if not l or l.startswith("[") or l.startswith("="):
                    break
                if re.match(r'^[A-Z]{2,}', l) and ":" in l and "NARRATOR" not in l:
                    break
                text += " " + l.strip('"') if text else l.strip('"')
                i += 1
            if text.strip():
                # Split into smaller chunks for better pacing
                sentences = re.split(r'(?<=[.!?])\s+', text.strip())
                for j in range(0, len(sentences), 2):
                    chunk = " ".join(sentences[j:j+2]).strip()
                    if chunk and len(chunk) > 15:
                        mood = detect_mood(chunk, last_visual)
                        blocks.append({
                            "type": "narration",
                            "text": chunk,
                            "speaker": "narrator",
                            "mood": mood,
                            "context": last_visual[:200],
                        })
            continue

        # Character dialogue (multi-line)
        match = re.match(r'^([A-Z][A-Z\s]+?)(?:\s*\(.*?\))?\s*:\s*$', line)
        if match:
            speaker = match.group(1).strip().lower()
            i += 1
            text = ""
            while i < len(lines):
                l = lines[i].strip()
                if not l or l.startswith("[") or l.startswith("="):
                    break
                if re.match(r'^[A-Z]{2,}', l) and ":" in l:
                    break
                text += " " + l.strip('"') if text else l.strip('"')
                i += 1
            if text.strip() and len(text.strip()) > 5:
                mood = detect_mood(text, last_visual)
                blocks.append({
                    "type": "dialogue",
                    "text": text.strip(),
                    "speaker": speaker,
                    "mood": mood,
                    "context": last_visual[:200],
                })
            continue

        # Inline dialogue (SPEAKER: "text")
        inline = re.match(r'^([A-Z]{2,}[A-Z\s]*?)(?:\s*\(.*?\))?\s*:\s*"?(.+)"?\s*$', line)
        if inline:
            speaker = inline.group(1).strip().lower()
            text = inline.group(2).strip().strip('"')
            if text and len(text) > 5:
                mood = detect_mood(text, last_visual)
                blocks.append({
                    "type": "dialogue",
                    "text": text,
                    "speaker": speaker,
                    "mood": mood,
                    "context": last_visual[:200],
                })
            i += 1
            continue

        i += 1

    return blocks


# ── EMOTIONAL VOICE GENERATION ──────────────────────────────────
def generate_voice(text, speaker, mood, output_path):
    """Generate voice with emotional prosody control."""
    if os.path.exists(output_path) and os.path.getsize(output_path) > 100:
        return True

    voice = VOICE_MAP.get(speaker, "en-US-ChristopherNeural")
    prosody = MOOD_PROSODY.get(mood, MOOD_PROSODY["neutral"])

    # Clean text for TTS
    clean = text.replace("\\:", ":").replace("%%", "%")
    clean = re.sub(r'[^\w\s.,!?;:\'-]', '', clean)
    if not clean.strip():
        return False

    try:
        cmd = [
            "edge-tts",
            "-v", voice,
            "-t", clean,
            f"--rate={prosody['rate']}",
            f"--pitch={prosody['pitch']}",
            "--write-media", output_path,
        ]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        return r.returncode == 0 and os.path.exists(output_path) and os.path.getsize(output_path) > 100
    except Exception as e:
        print(f"    Voice error: {e}")
        return False


def get_audio_duration(path):
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", path],
            capture_output=True, text=True
        )
        return float(r.stdout.strip())
    except:
        return 5.0


def esc(text):
    t = text.replace("'", "\u2019")
    t = t.replace(":", "\\:")
    t = t.replace("%", "%%")
    t = t.replace('"', '\\"')
    return t


# ── CLIP BUILDERS ───────────────────────────────────────────────
def make_voiced_clip(art_path, voice_path, text, speaker, mood, output_path, sfx_path=None):
    """Create clip with art + emotional voice + subtitle + Ken Burns + optional SFX."""
    voice_dur = get_audio_duration(voice_path)
    # More padding for dramatic scenes
    pre_pad = 0.8 if mood in ("dramatic", "tense", "emotional") else 0.5
    post_pad = 1.5 if mood in ("dramatic", "emotional") else 1.0
    total_dur = voice_dur + pre_pad + post_pad

    wrapped = textwrap.wrap(text, width=50)
    if len(wrapped) > 3:
        wrapped = wrapped[:3]
        wrapped[-1] = wrapped[-1][:47] + "..."

    num_lines = len(wrapped)
    line_height = 44
    box_height = 30 + num_lines * line_height + 30
    box_y = 1080 - box_height - 30

    # Ken Burns — mood affects zoom speed and direction
    if mood in ("action",):
        zoom_start, zoom_end = 1.0, 1.12  # Faster zoom for action
    elif mood in ("dramatic", "tense"):
        zoom_start, zoom_end = 1.02, 1.10  # Start slightly zoomed, slow push
    elif mood in ("wonder",):
        zoom_start, zoom_end = 1.08, 1.0  # Zoom OUT for wonder/reveal
    else:
        zoom_start, zoom_end = 1.0, 1.06  # Gentle default

    h = hash(text) % 6
    pan_exprs = [
        ("'iw/2-(iw/zoom/2)'", "'ih/2-(ih/zoom/2)'"),   # center
        ("'0'", "'0'"),                                     # top-left
        ("'iw-iw/zoom'", "'0'"),                           # top-right
        ("'iw/2-(iw/zoom/2)'", "'ih-ih/zoom'"),           # bottom-center
        ("'0'", "'ih-ih/zoom'"),                            # bottom-left
        ("'iw-iw/zoom'", "'ih-ih/zoom'"),                  # bottom-right
    ]
    x_expr, y_expr = pan_exprs[h]

    # Build video filter
    frames = int(total_dur * 30)
    filters = (
        f"[0:v]scale=2100:1200,zoompan=z='min({zoom_start}+{zoom_end-zoom_start}*on/({frames}),{zoom_end})':"
        f"x={x_expr}:y={y_expr}:"
        f"d={frames}:s=1920x1080:fps=30,"
        # Subtle vignette effect for mood
        f"vignette=PI/4,"
        f"drawbox=x=140:y={box_y}:w=1640:h={box_height}:color=black@0.72:t=fill"
    )

    # Speaker label for dialogue
    is_dialogue = speaker and speaker != "narrator"
    if is_dialogue:
        display_name = speaker.replace("_", " ").title()
        color = CHAR_COLORS.get(speaker, "0xeeeeee")
        name_y = box_y + 12
        filters += (
            f",drawtext=text='{esc(display_name)}':"
            f"fontsize=28:fontcolor={color}:"
            f"x=180:y={name_y}:"
            f"shadowcolor=black:shadowx=2:shadowy=2"
        )
        text_start_y = name_y + 38
    else:
        text_start_y = box_y + 20

    # Subtitle lines with fade-in
    for li, line_text in enumerate(wrapped):
        y = text_start_y + li * line_height
        x_pos = "180" if is_dialogue else "(w-text_w)/2"
        fade_start = pre_pad + li * 0.15  # Stagger line appearance
        filters += (
            f",drawtext=text='{esc(line_text)}':"
            f"fontsize=34:fontcolor=0xeeeeee:"
            f"x={x_pos}:y={y}:"
            f"shadowcolor=black:shadowx=2:shadowy=2:"
            f"enable='gte(t,{fade_start:.1f})'"
        )

    filters += "[vout]"

    # Audio: voice delayed by pre_pad, padded to total_dur
    delay_ms = int(pre_pad * 1000)
    audio_filter = f"[1:a]adelay={delay_ms}|{delay_ms},volume=1.0,apad=whole_dur={total_dur}[voice]"

    if sfx_path and os.path.exists(sfx_path):
        # Mix voice + SFX
        audio_filter += (
            f";[2:a]volume=0.25,atrim=0:{total_dur},apad=whole_dur={total_dur}[sfx];"
            f"[voice][sfx]amix=inputs=2:duration=first:dropout_transition=1[aout]"
        )
        cmd = [
            "ffmpeg", "-y",
            "-loop", "1", "-i", art_path,
            "-i", voice_path,
            "-i", sfx_path,
            "-filter_complex", f"{filters};{audio_filter}",
            "-map", "[vout]", "-map", "[aout]",
            "-t", str(total_dur),
            "-c:v", "libx264", "-preset", "fast", "-crf", "22",
            "-c:a", "aac", "-b:a", "192k", "-ar", "44100", "-ac", "2",
            "-pix_fmt", "yuv420p", "-r", "30", "-shortest",
            output_path
        ]
    else:
        audio_filter += ";[voice]acopy[aout]"
        cmd = [
            "ffmpeg", "-y",
            "-loop", "1", "-i", art_path,
            "-i", voice_path,
            "-filter_complex", f"{filters};{audio_filter}",
            "-map", "[vout]", "-map", "[aout]",
            "-t", str(total_dur),
            "-c:v", "libx264", "-preset", "fast", "-crf", "22",
            "-c:a", "aac", "-b:a", "192k", "-ar", "44100", "-ac", "2",
            "-pix_fmt", "yuv420p", "-r", "30", "-shortest",
            output_path
        ]

    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        for e in r.stderr.split("\n")[-5:]:
            if e.strip():
                print(f"    ERR: {e.strip()[:120]}")
    return r.returncode == 0


def make_title_clip(art_path, title, subtitle, output_path, dur=10):
    """Title card with cinematic fade-in and atmosphere."""
    filters = (
        f"[0:v]scale=2100:1200,zoompan=z='min(1.0+0.04*on/({dur}*30),1.04)':"
        f"x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':"
        f"d={int(dur*30)}:s=1920x1080:fps=30,"
        f"vignette=PI/3.5,"
        # Fade from black
        f"fade=t=in:st=0:d=2,"
        f"drawbox=x=0:y=0:w=iw:h=ih:color=black@0.55:t=fill,"
        f"drawtext=text='GHOST IN THE MACHINE':"
        f"fontsize=60:fontcolor=0x4a9eff:"
        f"x=(w-text_w)/2:y=(h/2)-110:"
        f"shadowcolor=black:shadowx=3:shadowy=3:"
        f"enable='gte(t,1)',"
        f"drawtext=text='{esc(title)}':"
        f"fontsize=50:fontcolor=0xffffff:"
        f"x=(w-text_w)/2:y=(h/2)-20:"
        f"shadowcolor=black:shadowx=3:shadowy=3:"
        f"enable='gte(t,1.5)',"
        f"drawtext=text='{esc(subtitle)}':"
        f"fontsize=24:fontcolor=0x888888:"
        f"x=(w-text_w)/2:y=(h/2)+50:"
        f"shadowcolor=black:shadowx=1:shadowy=1:"
        f"enable='gte(t,2)'"
        f"[out]"
    )
    cmd = [
        "ffmpeg", "-y", "-loop", "1", "-i", art_path,
        "-f", "lavfi", "-i", "anullsrc=r=44100:cl=stereo",
        "-filter_complex", filters, "-map", "[out]", "-map", "1:a",
        "-t", str(dur), "-c:v", "libx264", "-preset", "fast", "-crf", "22",
        "-c:a", "aac", "-b:a", "192k", "-pix_fmt", "yuv420p", "-r", "30",
        "-ar", "44100", "-ac", "2",
        output_path
    ]
    return subprocess.run(cmd, capture_output=True, text=True).returncode == 0


def make_endcard_clip(art_path, next_text, output_path, dur=10):
    """End card with fade-out."""
    clean_next = esc(next_text)
    filters = (
        f"[0:v]scale=2100:1200,zoompan=z='min(1.0+0.03*on/({dur}*30),1.03)':"
        f"x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':"
        f"d={int(dur*30)}:s=1920x1080:fps=30,"
        f"vignette=PI/4,"
        f"drawbox=x=0:y=0:w=iw:h=ih:color=black@0.6:t=fill,"
        f"drawtext=text='GHOST IN THE MACHINE':"
        f"fontsize=48:fontcolor=0x4a9eff:"
        f"x=(w-text_w)/2:y=(h/2)-90:"
        f"shadowcolor=black:shadowx=2:shadowy=2:"
        f"enable='gte(t,0.5)',"
        f"drawtext=text='{clean_next}':"
        f"fontsize=36:fontcolor=0xffffff:"
        f"x=(w-text_w)/2:y=(h/2)-10:"
        f"shadowcolor=black:shadowx=2:shadowy=2:"
        f"enable='gte(t,1.5)',"
        f"drawtext=text='SUBSCRIBE for Season 2':"
        f"fontsize=26:fontcolor=0xff4444:"
        f"x=(w-text_w)/2:y=(h/2)+50:"
        f"shadowcolor=black:shadowx=1:shadowy=1:"
        f"enable='gte(t,2.5)',"
        # Fade to black at end
        f"fade=t=out:st={dur-2}:d=2"
        f"[out]"
    )
    cmd = [
        "ffmpeg", "-y", "-loop", "1", "-i", art_path,
        "-f", "lavfi", "-i", "anullsrc=r=44100:cl=stereo",
        "-filter_complex", filters, "-map", "[out]", "-map", "1:a",
        "-t", str(dur), "-c:v", "libx264", "-preset", "fast", "-crf", "22",
        "-c:a", "aac", "-b:a", "192k", "-pix_fmt", "yuv420p", "-r", "30",
        "-ar", "44100", "-ac", "2",
        output_path
    ]
    return subprocess.run(cmd, capture_output=True, text=True).returncode == 0


def make_transition_clip(output_path, dur=1.5):
    """Black transition between scenes."""
    cmd = [
        "ffmpeg", "-y",
        "-f", "lavfi", "-i", f"color=c=black:s=1920x1080:d={dur}:r=30",
        "-f", "lavfi", "-i", f"anullsrc=r=44100:cl=stereo",
        "-t", str(dur),
        "-c:v", "libx264", "-preset", "fast", "-crf", "22",
        "-c:a", "aac", "-b:a", "192k", "-pix_fmt", "yuv420p",
        "-ar", "44100", "-ac", "2",
        output_path
    ]
    return subprocess.run(cmd, capture_output=True, text=True).returncode == 0


# ── SFX SELECTION PER MOOD/CONTEXT ──────────────────────────────
def select_sfx(mood, context):
    """Pick the right sound effect based on mood and scene context."""
    ctx = context.lower()
    if any(w in ctx for w in ["storm", "rain", "typhoon", "thunder", "lightning"]):
        return "rain"
    if any(w in ctx for w in ["ocean", "sea", "wave", "shore", "dock", "boat", "fish"]):
        return "ocean"
    if any(w in ctx for w in ["wind", "breeze", "gust"]):
        return "wind"
    if any(w in ctx for w in ["lab", "server", "machine", "basement", "computer", "monitor"]):
        return "lab_hum"
    if any(w in ctx for w in ["digital", "neural", "network", "data", "pulse", "glow", "ai"]):
        return "digital"
    # Mood-based fallback
    if mood in ("dramatic", "tense"):
        return "wind"
    if mood == "wonder":
        return "digital"
    return None


# ── NORMALIZE + CONCAT ──────────────────────────────────────────
def normalize_clip(input_path, output_path):
    """Normalize audio to 44100Hz stereo AAC."""
    cmd = [
        "ffmpeg", "-y", "-i", input_path,
        "-c:v", "copy",
        "-c:a", "aac", "-ar", "44100", "-ac", "2", "-b:a", "192k",
        output_path
    ]
    r = subprocess.run(cmd, capture_output=True, text=True)
    return r.returncode == 0


# ── EPISODE BUILDER ─────────────────────────────────────────────
def build_episode(ep_key, ep_info):
    ep_art_dir = os.path.join(ART_DIR, ep_key)
    ep_temp = os.path.join(TEMP_DIR, ep_key)
    ep_voice = os.path.join(VOICE_DIR, ep_key)
    norm_dir = os.path.join(ep_temp, "normalized")
    os.makedirs(ep_temp, exist_ok=True)
    os.makedirs(ep_voice, exist_ok=True)
    os.makedirs(norm_dir, exist_ok=True)
    output = os.path.join(OUTPUT_DIR, f"{ep_key}_full.mp4")

    ep_mood = ep_info.get("mood_override", "neutral")

    print(f"\n{'='*60}")
    print(f"  {ep_info['title']} ({ep_info['subtitle']}) — Mood: {ep_mood.upper()}")
    print(f"{'='*60}")

    # Gather art
    art = {}
    for s in ep_info["scenes"]:
        p = os.path.join(ep_art_dir, f"{s}.png")
        if os.path.exists(p):
            art[s] = p
    print(f"  Art: {len(art)}/{len(ep_info['scenes'])}")
    if not art:
        return False

    # Parse script with mood detection
    script = os.path.join(SCRIPT_DIR, f"{ep_key}.txt")
    blocks = parse_script(script)
    narr = sum(1 for b in blocks if b["type"] == "narration")
    dial = sum(1 for b in blocks if b["type"] == "dialogue")
    moods = {}
    for b in blocks:
        m = b.get("mood", "neutral")
        moods[m] = moods.get(m, 0) + 1
    mood_str = ", ".join(f"{k}:{v}" for k, v in sorted(moods.items(), key=lambda x: -x[1]))
    print(f"  Script: {len(blocks)} blocks ({narr} narr, {dial} dial)")
    print(f"  Moods: {mood_str}")

    # Pre-generate sound effects
    print(f"  Generating sound effects...")
    sfx_cache = {}
    for sfx_type in ["rain", "thunder", "ocean", "wind", "lab_hum", "digital"]:
        sfx_path = os.path.join(SFX_DIR, f"{sfx_type}.m4a")
        if generate_sfx(sfx_type, sfx_path, 30):
            sfx_cache[sfx_type] = sfx_path

    # Generate mood music for this episode
    print(f"  Generating {ep_mood} music...")
    music_path = os.path.join(MUSIC_DIR, f"music_{ep_mood}.m4a")
    generate_mood_music(ep_mood, music_path, 700)

    # Step 1: Generate ALL voice audio with emotional prosody
    print(f"  Generating emotional voices...")
    voice_files = []
    for bi, block in enumerate(blocks):
        voice_path = os.path.join(ep_voice, f"v{bi:03d}.mp3")
        speaker = block.get("speaker", "narrator")
        mood = block.get("mood", ep_mood)

        if generate_voice(block["text"], speaker, mood, voice_path):
            voice_files.append(voice_path)
            dur = get_audio_duration(voice_path)
            print(f"    [{bi:02d}] {speaker[:8]:8s} {mood[:6]:6s} {dur:.1f}s | {block['text'][:45]}...")
        else:
            voice_files.append(None)
            print(f"    [{bi:02d}] {speaker[:8]:8s} FAIL  | {block['text'][:45]}...")

    ok_voices = sum(1 for v in voice_files if v)
    print(f"  Voices: {ok_voices}/{len(blocks)}")

    # Step 2: Build clips
    clips = []
    idx = 0

    # Title card
    if "title" in art:
        clip = os.path.join(ep_temp, f"c{idx:03d}_title.mp4")
        if make_title_clip(art["title"], ep_info["title"], ep_info["subtitle"], clip, 10):
            clips.append(clip)
            idx += 1
            print(f"  [title] 10s cinematic intro")

    # Brief transition after title
    trans = os.path.join(ep_temp, f"c{idx:03d}_trans.mp4")
    if make_transition_clip(trans, 1.0):
        clips.append(trans)
        idx += 1

    # Scene clips with voice + SFX
    scene_keys = [k for k in ep_info["scenes"] if k not in ("title", "endcard") and k in art]

    if blocks and scene_keys:
        per = max(1, len(blocks) // len(scene_keys))
        prev_scene = None

        for si, sk in enumerate(scene_keys):
            start = si * per
            end = start + per if si < len(scene_keys) - 1 else len(blocks)
            chunk_indices = list(range(start, min(end, len(blocks))))

            # Add scene transition between different scenes
            if prev_scene and prev_scene != sk:
                trans = os.path.join(ep_temp, f"c{idx:03d}_trans.mp4")
                if make_transition_clip(trans, 0.8):
                    clips.append(trans)
                    idx += 1

            ok_count = 0
            for bi in chunk_indices:
                block = blocks[bi]
                t = block["text"].strip()
                if not t or len(t) < 10:
                    continue

                clip = os.path.join(ep_temp, f"c{idx:03d}.mp4")
                speaker = block.get("speaker", "narrator")
                mood = block.get("mood", ep_mood)
                context = block.get("context", "")

                # Select SFX for this clip
                sfx_type = select_sfx(mood, context)
                sfx_path = sfx_cache.get(sfx_type) if sfx_type else None

                if voice_files[bi]:
                    sub_speaker = speaker if block["type"] == "dialogue" else None
                    if make_voiced_clip(art[sk], voice_files[bi], t, sub_speaker, mood, clip, sfx_path):
                        clips.append(clip)
                        idx += 1
                        ok_count += 1

            prev_scene = sk
            print(f"  [{sk}] {ok_count} clips")

    # Endcard
    if "endcard" in art:
        # Transition before endcard
        trans = os.path.join(ep_temp, f"c{idx:03d}_trans.mp4")
        if make_transition_clip(trans, 1.5):
            clips.append(trans)
            idx += 1

        clip = os.path.join(ep_temp, f"c{idx:03d}_end.mp4")
        if make_endcard_clip(art["endcard"], ep_info.get("next", "More coming"), clip, 10):
            clips.append(clip)
            idx += 1
            print(f"  [endcard] 10s cinematic outro")

    if not clips:
        return False

    # Step 3: Normalize all clips to consistent audio format
    print(f"  Normalizing {len(clips)} clips...")
    norm_clips = []
    for c in clips:
        nc = os.path.join(norm_dir, os.path.basename(c))
        if normalize_clip(c, nc):
            norm_clips.append(nc)
        else:
            print(f"    WARN: failed to normalize {os.path.basename(c)}")

    print(f"  Normalized: {len(norm_clips)}/{len(clips)}")

    # Step 4: Concat
    cf = os.path.join(norm_dir, "concat.txt")
    with open(cf, "w") as f:
        for c in norm_clips:
            f.write(f"file '{c}'\n")

    concat_raw = os.path.join(ep_temp, "concat_raw.mp4")
    cmd = [
        "ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", cf,
        "-c", "copy",
        concat_raw
    ]
    print(f"  Joining {len(norm_clips)} clips...")
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        print(f"  Concat FAIL: {r.stderr[-200:]}")
        return False

    video_dur = get_audio_duration(concat_raw)
    print(f"  Raw video: {video_dur:.0f}s ({video_dur/60:.1f}min)")

    # Step 5: Mix in mood-reactive background music
    if os.path.exists(music_path):
        print(f"  Mixing voice + {ep_mood} music...")
        fade_out_start = max(0, video_dur - 5)
        cmd = [
            "ffmpeg", "-y",
            "-i", concat_raw,
            "-i", music_path,
            "-filter_complex",
            f"[1:a]volume=0.15,aresample=44100,atrim=0:{video_dur},"
            f"afade=t=in:d=3,afade=t=out:st={fade_out_start}:d=5[music];"
            f"[0:a]aresample=44100[voice];"
            f"[voice][music]amix=inputs=2:duration=first:dropout_transition=2[aout]",
            "-map", "0:v", "-map", "[aout]",
            "-c:v", "copy", "-c:a", "aac", "-b:a", "192k", "-ar", "44100",
            output
        ]
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode != 0:
            print(f"  Music mix failed, using raw")
            import shutil
            shutil.copy2(concat_raw, output)
    else:
        import shutil
        shutil.copy2(concat_raw, output)

    # Stats
    if os.path.exists(output):
        sz = os.path.getsize(output) / (1024*1024)
        dur = get_audio_duration(output)
        print(f"  DONE: {sz:.1f}MB, {dur/60:.1f}min")
        return True
    return False


def main():
    print("=" * 60)
    print("  GHOST IN THE MACHINE — FULL PRODUCTION v5")
    print("  Emotional Voices + Mood Music + Sound Effects")
    print("  Scene Transitions + Cinematic Title/Endcards")
    print("=" * 60)

    results = {}
    for ek, ei in EPISODES.items():
        results[ek] = build_episode(ek, ei)

    print(f"\n{'='*60}")
    print("  PRODUCTION COMPLETE — v5")
    print(f"{'='*60}")
    done = sum(1 for v in results.values() if v)
    total_dur = 0
    total_sz = 0
    for ek, ok in results.items():
        p = os.path.join(OUTPUT_DIR, f"{ek}_full.mp4")
        if ok and os.path.exists(p):
            sz = os.path.getsize(p) / (1024*1024)
            dur = get_audio_duration(p)
            total_dur += dur
            total_sz += sz
            print(f"  {EPISODES[ek]['title']}: {sz:.1f}MB, {dur/60:.1f}min")
        else:
            print(f"  {EPISODES[ek]['title']}: FAIL")
    print(f"\n  {done}/5 episodes | {total_sz:.0f}MB | {total_dur/60:.1f}min total")
    print(f"  Output: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
