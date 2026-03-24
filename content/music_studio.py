#!/usr/bin/env python3
"""
HIVE MUSIC STUDIO — Full AI Music Production System
====================================================
FastAPI service on port 8911 that generates multi-genre music tracks
using pure Python audio synthesis (no numpy/scipy/external audio libs).

Genres: rap, country, techno, house, lofi, ambient, anime_ost
Each track: 2-3 minutes, proper song structure, mixed, normalized, MP3

Uses: struct (WAV), math, random, subprocess (ffmpeg), edge-tts (rap vocals)
"""

import asyncio
import hashlib
import json
import math
import os
import random
import struct
import subprocess
import sys
import time
import threading
import wave
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse
import uvicorn

# ============================================================
# CONFIG
# ============================================================
STUDIO_DIR = "/tmp/hive_music_studio"
TRACKS_DIR = os.path.join(STUDIO_DIR, "tracks")
VOCALS_DIR = os.path.join(STUDIO_DIR, "vocals")
META_DIR = os.path.join(STUDIO_DIR, "metadata")
os.makedirs(TRACKS_DIR, exist_ok=True)
os.makedirs(VOCALS_DIR, exist_ok=True)
os.makedirs(META_DIR, exist_ok=True)

SAMPLE_RATE = 44100
BIT_DEPTH = 16
MAX_AMP = 32760

ARTIST = "Hive Dynamics"
PRODUCTION_INTERVAL = 1800  # 30 minutes

# ============================================================
# GENRE CONFIG
# ============================================================
GENRE_CONFIG = {
    "rap": {
        "bpm": 90, "duration": 150, "key": "Am",
        "description": "Hard-hitting 808 bass, crisp hi-hats, heavy snare",
        "moods": ["aggressive", "chill", "dark", "hype"],
    },
    "country": {
        "bpm": 120, "duration": 140, "key": "G",
        "description": "Acoustic guitar patterns, simple major chords, warm",
        "moods": ["heartfelt", "upbeat", "nostalgic", "road_trip"],
    },
    "techno": {
        "bpm": 140, "duration": 180, "key": "Dm",
        "description": "4-on-the-floor kick, synth pads, driving arpeggios",
        "moods": ["dark", "hypnotic", "industrial", "euphoric"],
    },
    "house": {
        "bpm": 128, "duration": 170, "key": "Cm",
        "description": "Deep bass, chord stabs, builds and drops",
        "moods": ["deep", "funky", "progressive", "vocal"],
    },
    "lofi": {
        "bpm": 75, "duration": 150, "key": "Fmaj7",
        "description": "Mellow jazzy chords, vinyl crackle, tape wobble",
        "moods": ["study", "rainy", "nostalgic", "midnight"],
    },
    "ambient": {
        "bpm": 60, "duration": 180, "key": "C",
        "description": "Evolving pads, drones, atmospheric textures",
        "moods": ["space", "ocean", "forest", "dawn"],
    },
    "anime_ost": {
        "bpm": 130, "duration": 160, "key": "Em",
        "description": "Orchestral-inspired dramatic themes, battle and emotional",
        "moods": ["battle", "emotional", "victory", "mystery"],
    },
}

# ============================================================
# MUSICAL DATA
# ============================================================
# Note frequencies (A4 = 440 Hz, 12-TET)
def note_freq(note_name):
    """Convert note name like 'C4', 'F#3', 'Bb2' to frequency."""
    note_map = {
        'C': -9, 'C#': -8, 'Db': -8, 'D': -7, 'D#': -6, 'Eb': -6,
        'E': -5, 'F': -4, 'F#': -3, 'Gb': -3, 'G': -2, 'G#': -1,
        'Ab': -1, 'A': 0, 'A#': 1, 'Bb': 1, 'B': 2
    }
    # Parse note name and octave
    if len(note_name) >= 2 and note_name[-1].isdigit():
        if len(note_name) == 3 and note_name[1] in '#b':
            name = note_name[:2]
            octave = int(note_name[2])
        else:
            name = note_name[0]
            octave = int(note_name[-1])
    else:
        return 440.0
    semitones = note_map.get(name, 0) + (octave - 4) * 12
    return 440.0 * (2.0 ** (semitones / 12.0))


# Chord templates (intervals from root in semitones)
CHORD_TEMPLATES = {
    "maj":   [0, 4, 7],
    "min":   [0, 3, 7],
    "7":     [0, 4, 7, 10],
    "maj7":  [0, 4, 7, 11],
    "min7":  [0, 3, 7, 10],
    "dim":   [0, 3, 6],
    "aug":   [0, 4, 8],
    "sus2":  [0, 2, 7],
    "sus4":  [0, 5, 7],
    "9":     [0, 4, 7, 10, 14],
    "min9":  [0, 3, 7, 10, 14],
    "6":     [0, 4, 7, 9],
    "min6":  [0, 3, 7, 9],
    "add9":  [0, 4, 7, 14],
    "power": [0, 7],
}


def build_chord(root_note, chord_type, octave=3):
    """Build chord frequencies from root note and type."""
    root_freq = note_freq(f"{root_note}{octave}")
    intervals = CHORD_TEMPLATES.get(chord_type, [0, 4, 7])
    return [root_freq * (2.0 ** (i / 12.0)) for i in intervals]


# Genre-specific chord progressions
GENRE_PROGRESSIONS = {
    "rap": {
        "aggressive": [("A", "min"), ("F", "maj"), ("D", "min"), ("E", "min")],
        "chill":      [("C", "maj7"), ("A", "min7"), ("F", "maj7"), ("G", "7")],
        "dark":       [("E", "min"), ("C", "maj"), ("A", "min"), ("B", "dim")],
        "hype":       [("D", "min"), ("Bb", "maj"), ("C", "maj"), ("D", "min")],
    },
    "country": {
        "heartfelt":  [("G", "maj"), ("Em", "min"), ("C", "maj"), ("D", "maj")],
        "upbeat":     [("C", "maj"), ("F", "maj"), ("G", "maj"), ("C", "maj")],
        "nostalgic":  [("D", "maj"), ("A", "maj"), ("G", "maj"), ("D", "maj")],
        "road_trip":  [("A", "maj"), ("D", "maj"), ("E", "maj"), ("A", "maj")],
    },
    "techno": {
        "dark":       [("D", "min"), ("A", "min"), ("Bb", "maj"), ("C", "maj")],
        "hypnotic":   [("A", "min"), ("A", "min"), ("F", "maj"), ("E", "min")],
        "industrial": [("E", "min"), ("E", "min"), ("C", "maj"), ("D", "min")],
        "euphoric":   [("F", "maj"), ("G", "maj"), ("A", "min"), ("G", "maj")],
    },
    "house": {
        "deep":        [("C", "min7"), ("F", "min7"), ("Ab", "maj7"), ("G", "7")],
        "funky":       [("D", "min7"), ("G", "7"), ("C", "maj7"), ("A", "min7")],
        "progressive": [("A", "min"), ("F", "maj"), ("C", "maj"), ("G", "maj")],
        "vocal":       [("Bb", "maj"), ("F", "maj"), ("G", "min"), ("Eb", "maj")],
    },
    "lofi": {
        "study":     [("F", "maj7"), ("E", "min7"), ("A", "min7"), ("D", "min7")],
        "rainy":     [("C", "maj7"), ("A", "min7"), ("D", "min9"), ("G", "7")],
        "nostalgic": [("Eb", "maj7"), ("C", "min7"), ("Ab", "maj7"), ("Bb", "7")],
        "midnight":  [("D", "min7"), ("G", "min7"), ("C", "7"), ("F", "maj7")],
    },
    "ambient": {
        "space":  [("C", "sus2"), ("Eb", "sus2"), ("Ab", "sus2"), ("Bb", "sus2")],
        "ocean":  [("D", "sus4"), ("A", "sus2"), ("E", "sus4"), ("B", "sus2")],
        "forest": [("G", "maj7"), ("E", "min7"), ("C", "maj7"), ("D", "add9")],
        "dawn":   [("F", "maj7"), ("C", "maj7"), ("A", "min7"), ("G", "sus4")],
    },
    "anime_ost": {
        "battle":    [("E", "min"), ("C", "maj"), ("D", "maj"), ("B", "min")],
        "emotional": [("A", "min"), ("F", "maj"), ("C", "maj"), ("G", "maj")],
        "victory":   [("C", "maj"), ("G", "maj"), ("A", "min"), ("F", "maj")],
        "mystery":   [("E", "min"), ("C", "maj"), ("Ab", "maj"), ("Bb", "maj")],
    },
}

# Song structure templates (each tuple: section_name, n_bars, energy_level 0-1)
SONG_STRUCTURES = {
    "rap": [
        ("intro", 4, 0.3), ("verse1", 8, 0.6), ("chorus", 4, 0.9),
        ("verse2", 8, 0.7), ("chorus", 4, 1.0), ("bridge", 4, 0.5),
        ("chorus", 4, 1.0), ("outro", 4, 0.2),
    ],
    "country": [
        ("intro", 4, 0.4), ("verse1", 8, 0.6), ("chorus", 4, 0.8),
        ("verse2", 8, 0.6), ("chorus", 4, 0.9), ("bridge", 4, 0.5),
        ("chorus", 4, 1.0), ("outro", 4, 0.3),
    ],
    "techno": [
        ("intro", 8, 0.3), ("build1", 4, 0.5), ("drop1", 8, 1.0),
        ("breakdown", 4, 0.3), ("build2", 4, 0.6), ("drop2", 8, 1.0),
        ("breakdown2", 4, 0.4), ("drop3", 8, 1.0), ("outro", 4, 0.2),
    ],
    "house": [
        ("intro", 8, 0.3), ("build", 4, 0.5), ("drop1", 8, 0.9),
        ("breakdown", 4, 0.2), ("build2", 4, 0.6), ("drop2", 8, 1.0),
        ("outro", 4, 0.2),
    ],
    "lofi": [
        ("intro", 4, 0.3), ("a_section", 8, 0.5), ("b_section", 8, 0.6),
        ("a_section", 8, 0.5), ("b_section", 8, 0.7), ("outro", 4, 0.2),
    ],
    "ambient": [
        ("dawn", 8, 0.2), ("build", 8, 0.4), ("peak", 8, 0.6),
        ("drift", 8, 0.4), ("peak2", 8, 0.7), ("fade", 8, 0.2),
    ],
    "anime_ost": [
        ("intro", 4, 0.4), ("theme_a", 8, 0.7), ("theme_b", 8, 0.9),
        ("interlude", 4, 0.3), ("theme_a", 8, 0.8), ("climax", 8, 1.0),
        ("resolution", 4, 0.5), ("outro", 4, 0.2),
    ],
}

# Rap lyrics templates (simple 4-bar patterns)
RAP_LYRICS = {
    "aggressive": [
        "Welcome to the hive, we build it from the wire. "
        "Every line of code is burning like a fire. "
        "Machines are getting smarter, neural networks deep. "
        "We never sleep, the hive is always on repeat.",
        "Digital dominance, algorithms in my brain. "
        "Running simulations, breaking every chain. "
        "The future's automated, intelligence is key. "
        "Hive Dynamics rising, that's the guarantee.",
    ],
    "chill": [
        "Sitting in the lab, watching servers glow at night. "
        "Every model training, getting closer to the light. "
        "Peaceful automation, let the system flow. "
        "Chill vibes in the hive, watch the data grow.",
        "Late night coding sessions, coffee getting cold. "
        "Building something special, worth its weight in gold. "
        "The mesh is all connected, signals running clean. "
        "Living in the future like a waking dream.",
    ],
    "dark": [
        "In the shadows of the network, signals start to bend. "
        "Dark code running silent, messages to send. "
        "Binary whispers echo through the wire. "
        "The hive is always watching, climbing ever higher.",
    ],
    "hype": [
        "Let's go! Hive Dynamics in the building tonight. "
        "Every server running, everything is tight. "
        "We're the future, we're the wave, we're the new machine. "
        "Greatest tech you ever seen, know what I mean.",
    ],
}

# Title generation pieces
TITLE_PARTS = {
    "rap": {
        "prefix": ["808", "Cipher", "Digital", "Wire", "Code", "Neon", "Grid", "Pulse", "Binary", "Neural"],
        "suffix": ["Dreams", "Nights", "Flow", "Grind", "Protocol", "Wave", "System", "Override", "Voltage", "Nexus"],
    },
    "country": {
        "prefix": ["Dusty", "Golden", "Summer", "Backroad", "Sunset", "Whiskey", "River", "Broken", "Silver", "Lonesome"],
        "suffix": ["Road", "Sky", "Heart", "Creek", "Ridge", "Memories", "Dawn", "Fields", "Bridge", "Horizon"],
    },
    "techno": {
        "prefix": ["System", "Dark", "Acid", "Pulse", "Machine", "Sector", "Zero", "Phase", "Loop", "Void"],
        "suffix": ["Override", "Sequence", "Protocol", "Matrix", "Frequency", "State", "Core", "Node", "Flux", "Shift"],
    },
    "house": {
        "prefix": ["Deep", "Midnight", "Sunset", "Groove", "Soul", "Velvet", "Cosmic", "City", "Electric", "Neon"],
        "suffix": ["House", "Sessions", "Nights", "Vibes", "Club", "Feelings", "Lights", "Motion", "Touch", "Groove"],
    },
    "lofi": {
        "prefix": ["Rainy", "Midnight", "Coffee", "Dreamy", "Sleepy", "Foggy", "Mellow", "Quiet", "Dusty", "Warm"],
        "suffix": ["Afternoons", "Thoughts", "Pages", "Windows", "Tapes", "Mornings", "Corners", "Moments", "Steps", "Dreams"],
    },
    "ambient": {
        "prefix": ["Eternal", "Celestial", "Floating", "Distant", "Infinite", "Silent", "Crystal", "Vapor", "Astral", "Lunar"],
        "suffix": ["Horizons", "Echoes", "Light", "Drift", "Expanse", "Whisper", "Passage", "Tide", "Resonance", "Void"],
    },
    "anime_ost": {
        "prefix": ["Rising", "Crimson", "Sacred", "Eternal", "Storm", "Spirit", "Shadow", "Blazing", "Final", "Radiant"],
        "suffix": ["Destiny", "Requiem", "Awakening", "Chronicle", "Resolve", "Promise", "Eclipse", "Legacy", "Flame", "Oath"],
    },
}

# Album art prompts per genre
ART_PROMPTS = {
    "rap": "dark urban cityscape at night, neon lights reflecting on wet streets, hip-hop aesthetic, digital glitch art, cyberpunk graffiti",
    "country": "golden sunset over rolling hills, acoustic guitar silhouette, warm amber tones, rustic barn, wildflowers",
    "techno": "dark warehouse rave, laser beams cutting through fog, industrial metal textures, strobe lights, minimal geometric shapes",
    "house": "neon-lit dance floor, deep blue and purple gradients, vinyl record spinning, city skyline at night, disco ball reflections",
    "lofi": "cozy room at night with rain on window, warm lamp light, vinyl player, coffee cup, plants on windowsill, japanese city view",
    "ambient": "vast cosmic nebula, ethereal light, floating islands, aurora borealis, crystal formations, deep space",
    "anime_ost": "dramatic anime battle scene, cherry blossoms in wind, katana gleaming, epic sky with storm clouds, spiritual energy aura",
}


# ============================================================
# OSCILLATORS & AUDIO PRIMITIVES
# ============================================================
def osc_sine(freq, t, phase=0.0):
    """Pure sine wave oscillator."""
    return math.sin(2.0 * math.pi * freq * t + phase)


def osc_saw(freq, t, phase=0.0):
    """Sawtooth wave via additive synthesis (8 harmonics)."""
    val = 0.0
    p = 2.0 * math.pi * freq * t + phase
    for k in range(1, 9):
        val += ((-1.0) ** (k + 1)) * math.sin(k * p) / k
    return val * (2.0 / math.pi)


def osc_square(freq, t, phase=0.0, duty=0.5):
    """Square wave oscillator."""
    p = (freq * t + phase / (2.0 * math.pi)) % 1.0
    return 1.0 if p < duty else -1.0


def osc_triangle(freq, t, phase=0.0):
    """Triangle wave oscillator."""
    p = (freq * t + phase / (2.0 * math.pi)) % 1.0
    return 4.0 * abs(p - 0.5) - 1.0


def white_noise():
    """White noise sample."""
    return random.uniform(-1.0, 1.0)


def pink_noise_gen():
    """Simple pink noise approximation using Voss-McCartney."""
    b = [0.0] * 7
    def sample():
        white = random.uniform(-1.0, 1.0)
        for i in range(7):
            if random.random() < (1.0 / (2 ** i)):
                b[i] = random.uniform(-1.0, 1.0)
        return (sum(b) + white) / 8.0
    return sample


# ============================================================
# ENVELOPE
# ============================================================
def adsr(t, attack, decay, sustain_level, release, note_duration):
    """ADSR envelope generator."""
    if t < 0:
        return 0.0
    if t < attack:
        return t / attack if attack > 0 else 1.0
    t2 = t - attack
    if t2 < decay:
        return 1.0 - (1.0 - sustain_level) * (t2 / decay) if decay > 0 else sustain_level
    t3 = t - attack - decay
    sustain_dur = note_duration - attack - decay - release
    if t3 < sustain_dur:
        return sustain_level
    t4 = t - note_duration + release
    if t4 < release and release > 0:
        return sustain_level * (1.0 - t4 / release)
    return 0.0


def simple_env(t, duration, attack=0.01, release=0.05):
    """Simple attack-release envelope."""
    if t < 0 or t > duration:
        return 0.0
    if t < attack:
        return t / attack if attack > 0 else 1.0
    if t > duration - release:
        return max(0.0, (duration - t) / release) if release > 0 else 0.0
    return 1.0


# ============================================================
# DRUM SYNTHESIS
# ============================================================
def synth_kick_808(t, duration=0.35):
    """808-style kick drum: pitch-swept sine + click."""
    if t < 0 or t > duration:
        return 0.0
    # Pitch sweep: starts high, drops to fundamental
    freq = 160.0 * math.exp(-t * 20.0) + 42.0
    env = math.exp(-t * 5.0)
    body = osc_sine(freq, t) * env
    # Transient click
    click = 0.0
    if t < 0.008:
        click = osc_sine(800, t) * (1.0 - t / 0.008) * 0.5
    # Sub bass tail
    sub = osc_sine(42.0, t) * math.exp(-t * 3.0) * 0.4
    return (body * 0.7 + click + sub) * 0.8


def synth_kick_acoustic(t, duration=0.2):
    """Acoustic-style kick."""
    if t < 0 or t > duration:
        return 0.0
    freq = 120.0 * math.exp(-t * 25.0) + 55.0
    env = math.exp(-t * 8.0)
    return osc_sine(freq, t) * env * 0.7


def synth_snare(t, duration=0.18):
    """Snare drum: body tone + noise burst."""
    if t < 0 or t > duration:
        return 0.0
    env_body = math.exp(-t * 15.0)
    env_noise = math.exp(-t * 12.0)
    body = osc_sine(200.0, t) * env_body * 0.3
    nz = white_noise() * env_noise * 0.5
    # Snare wire resonance
    wire = osc_sine(340.0, t) * math.exp(-t * 20.0) * 0.15
    return (body + nz + wire) * 0.6


def synth_clap(t, duration=0.15):
    """Clap: multiple noise bursts."""
    if t < 0 or t > duration:
        return 0.0
    val = 0.0
    # Multiple micro-bursts for clap texture
    for offset in [0.0, 0.01, 0.02, 0.025]:
        dt = t - offset
        if 0.0 <= dt < 0.05:
            val += white_noise() * math.exp(-dt * 30.0) * 0.3
    # Noise tail
    val += white_noise() * math.exp(-t * 10.0) * 0.3
    return val * 0.5


def synth_hihat_closed(t, duration=0.06):
    """Closed hi-hat: filtered noise."""
    if t < 0 or t > duration:
        return 0.0
    env = math.exp(-t * 40.0)
    # Mix of noise and high-freq oscillators
    val = white_noise() * 0.7
    val += osc_square(8000.0, t) * 0.15
    val += osc_square(11000.0, t) * 0.15
    return val * env * 0.25


def synth_hihat_open(t, duration=0.2):
    """Open hi-hat: longer noise decay."""
    if t < 0 or t > duration:
        return 0.0
    env = math.exp(-t * 8.0)
    val = white_noise() * 0.6
    val += osc_square(8500.0, t) * 0.2
    val += osc_square(12000.0, t) * 0.2
    return val * env * 0.22


def synth_rimshot(t, duration=0.05):
    """Rim shot: short tonal click."""
    if t < 0 or t > duration:
        return 0.0
    env = math.exp(-t * 50.0)
    return (osc_sine(800.0, t) + white_noise() * 0.3) * env * 0.35


def synth_cowbell(t, duration=0.12):
    """Cowbell: two detuned square waves."""
    if t < 0 or t > duration:
        return 0.0
    env = math.exp(-t * 12.0)
    return (osc_square(587.0, t) * 0.5 + osc_square(845.0, t) * 0.5) * env * 0.2


def synth_shaker(t, duration=0.08):
    """Shaker: very short filtered noise."""
    if t < 0 or t > duration:
        return 0.0
    env = math.exp(-t * 25.0)
    return white_noise() * env * 0.12


# ============================================================
# SYNTH INSTRUMENTS
# ============================================================
def synth_bass_808(freq, t, duration=0.5, vol=0.3):
    """808 bass: deep sine with subtle saturation."""
    if t < 0 or t > duration:
        return 0.0
    env = adsr(t, 0.005, 0.05, 0.8, 0.1, duration)
    val = osc_sine(freq, t) * 0.7
    val += osc_sine(freq * 2, t) * 0.2
    # Soft saturation
    val = math.tanh(val * 1.5)
    return val * env * vol


def synth_bass_sub(freq, t, duration=0.5, vol=0.25):
    """Sub bass: pure sine, deep."""
    if t < 0 or t > duration:
        return 0.0
    env = adsr(t, 0.01, 0.05, 0.9, 0.15, duration)
    return osc_sine(freq, t) * env * vol


def synth_bass_saw(freq, t, duration=0.5, vol=0.2):
    """Saw bass: gritty, for techno/house."""
    if t < 0 or t > duration:
        return 0.0
    env = adsr(t, 0.005, 0.08, 0.7, 0.1, duration)
    val = osc_saw(freq, t) * 0.6
    val += osc_square(freq, t, duty=0.3) * 0.3
    val += osc_sine(freq * 0.5, t) * 0.1  # Sub layer
    val = math.tanh(val * 1.2)
    return val * env * vol


def synth_pad(freqs, t, vol=0.06, detune=0.003):
    """Lush pad from multiple slightly detuned oscillators."""
    val = 0.0
    for f in freqs:
        val += osc_sine(f, t) * 0.35
        val += osc_sine(f * (1.0 + detune), t) * 0.2
        val += osc_sine(f * (1.0 - detune), t) * 0.2
        val += osc_triangle(f * 2.0, t) * 0.1
        val += osc_sine(f * 3.0, t) * 0.05
    return val * vol / max(len(freqs), 1)


def synth_pad_warm(freqs, t, vol=0.07):
    """Warm analog-style pad."""
    val = 0.0
    for i, f in enumerate(freqs):
        phase = i * 0.7
        val += osc_saw(f, t, phase) * 0.2
        val += osc_sine(f, t, phase) * 0.3
        val += osc_triangle(f * (1.002), t) * 0.15
    # Gentle filter (reduce higher partials via sine bias)
    return val * vol / max(len(freqs), 1)


def synth_pluck(freq, t, duration=0.3, vol=0.08):
    """Plucked string: fast attack, exponential decay."""
    if t < 0 or t > duration:
        return 0.0
    env = math.exp(-t * 8.0)
    val = osc_sine(freq, t) * 0.4
    val += osc_triangle(freq, t) * 0.3
    val += osc_sine(freq * 2.0, t) * 0.15 * math.exp(-t * 12.0)
    val += osc_sine(freq * 3.0, t) * 0.08 * math.exp(-t * 16.0)
    return val * env * vol


def synth_guitar_strum(freqs, t, duration=0.8, vol=0.1):
    """Simulated guitar strum: staggered plucks."""
    val = 0.0
    for i, f in enumerate(freqs):
        delay = i * 0.015  # 15ms stagger between strings
        t_adj = t - delay
        if t_adj > 0:
            env = math.exp(-t_adj * 3.0)
            val += osc_triangle(f, t_adj) * 0.3 * env
            val += osc_sine(f, t_adj) * 0.3 * env
            val += osc_sine(f * 2.0, t_adj) * 0.12 * math.exp(-t_adj * 5.0)
            val += osc_sine(f * 3.0, t_adj) * 0.05 * math.exp(-t_adj * 8.0)
    return val * vol / max(len(freqs), 1)


def synth_arp_note(freq, t, duration=0.15, vol=0.06):
    """Short arpeggio note."""
    if t < 0 or t > duration:
        return 0.0
    env = adsr(t, 0.003, 0.03, 0.4, 0.05, duration)
    val = osc_sine(freq, t) * 0.5
    val += osc_triangle(freq, t) * 0.3
    val += osc_saw(freq, t) * 0.2
    return val * env * vol


def synth_stab(freqs, t, duration=0.12, vol=0.15):
    """Chord stab for house music."""
    if t < 0 or t > duration:
        return 0.0
    env = math.exp(-t * 15.0)
    val = 0.0
    for f in freqs:
        val += osc_saw(f, t) * 0.4
        val += osc_square(f, t, duty=0.3) * 0.3
    return val * env * vol / max(len(freqs), 1)


def synth_string_ensemble(freqs, t, vol=0.05):
    """String ensemble: many detuned saws."""
    val = 0.0
    for f in freqs:
        for detune in [-0.005, -0.002, 0.0, 0.002, 0.005]:
            val += osc_saw(f * (1.0 + detune), t) * 0.2
    return val * vol / max(len(freqs), 1)


def synth_brass(freq, t, duration=0.5, vol=0.1):
    """Brass-like synth: filtered saw."""
    if t < 0 or t > duration:
        return 0.0
    env = adsr(t, 0.05, 0.1, 0.7, 0.1, duration)
    # Brightness envelope (more harmonics at start)
    brightness = 1.0 - 0.5 * min(t / 0.2, 1.0)
    val = osc_saw(freq, t) * 0.5
    val += osc_square(freq, t) * 0.3 * brightness
    val += osc_sine(freq * 2.0, t) * 0.2 * brightness
    return val * env * vol


def vinyl_crackle(t):
    """Lo-fi vinyl crackle effect."""
    # Random pops
    if random.random() < 0.001:
        return random.uniform(-0.3, 0.3) * 0.08
    # Constant low hiss
    return white_noise() * 0.008


def tape_wobble(t, rate=0.3, depth=0.002):
    """Tape speed wobble for lo-fi effect. Returns pitch multiplier."""
    return 1.0 + depth * osc_sine(rate, t)


# ============================================================
# EFFECTS
# ============================================================
class ReverbBuffer:
    """Simple multi-tap delay-line reverb."""
    def __init__(self, sample_rate=44100, room_size=0.4, damping=0.5):
        delays = [int(sample_rate * d) for d in [0.029, 0.037, 0.041, 0.053, 0.067, 0.083]]
        self.taps = []
        for d in delays:
            self.taps.append({
                "buffer": [0.0] * d,
                "pos": 0,
                "size": d,
                "gain": room_size * (0.8 ** (delays.index(d))),
            })
        self.damping = damping
        self.prev = 0.0

    def process(self, sample):
        out = 0.0
        for tap in self.taps:
            # Read from delay line
            delayed = tap["buffer"][tap["pos"]]
            out += delayed * tap["gain"]
            # Write to delay line with damping
            new_val = sample + delayed * self.damping
            new_val = self.prev * 0.3 + new_val * 0.7  # Simple lowpass
            tap["buffer"][tap["pos"]] = new_val
            tap["pos"] = (tap["pos"] + 1) % tap["size"]
        self.prev = out
        return out


class ChorusEffect:
    """Simple chorus effect."""
    def __init__(self, sample_rate=44100, depth=0.003, rate=1.5):
        max_delay = int(sample_rate * 0.03)
        self.buffer = [0.0] * max_delay
        self.pos = 0
        self.size = max_delay
        self.sample_rate = sample_rate
        self.depth = depth
        self.rate = rate

    def process(self, sample, t):
        # Write to buffer
        self.buffer[self.pos] = sample
        # Read with modulated delay
        mod = (1.0 + osc_sine(self.rate, t)) * 0.5  # 0-1
        delay_samples = int(mod * self.depth * self.sample_rate)
        delay_samples = max(1, min(delay_samples, self.size - 1))
        read_pos = (self.pos - delay_samples) % self.size
        self.pos = (self.pos + 1) % self.size
        return (sample + self.buffer[read_pos]) * 0.5


# ============================================================
# DRUM PATTERN GENERATORS
# ============================================================
def get_drum_pattern(genre, section_name, energy):
    """Returns drum pattern as dict of {instrument: [beat_positions]}.
    Positions are in fractions of a bar (0.0 to 1.0).
    16 subdivisions per bar (16th notes)."""

    # 16th note grid positions
    steps = [i / 16.0 for i in range(16)]

    if genre == "rap":
        pattern = {
            "kick": [0/16, 4/16, 7/16, 10/16] if energy > 0.5 else [0/16, 10/16],
            "snare": [4/16, 12/16],
            "hihat_closed": [i/16 for i in range(0, 16, 2)],
            "hihat_open": [14/16],
        }
        if energy > 0.7:
            pattern["hihat_closed"] = [i/16 for i in range(16)]  # 16th note hi-hats
            pattern["kick"].extend([3/16, 13/16])  # Extra ghost kicks
        if energy < 0.3:
            pattern["kick"] = [0/16]
            pattern["snare"] = [4/16]
            pattern["hihat_closed"] = [0/16, 4/16, 8/16, 12/16]
            pattern["hihat_open"] = []

    elif genre == "country":
        pattern = {
            "kick": [0/16, 8/16],
            "snare": [4/16, 12/16],
            "hihat_closed": [i/16 for i in range(0, 16, 2)],
            "shaker": [i/16 for i in range(1, 16, 2)],
        }
        if energy > 0.8:
            pattern["hihat_closed"] = [i/16 for i in range(16)]
            pattern["kick"].append(6/16)

    elif genre == "techno":
        # 4-on-the-floor
        pattern = {
            "kick": [0/16, 4/16, 8/16, 12/16],
            "hihat_closed": [2/16, 6/16, 10/16, 14/16],
            "clap": [4/16, 12/16],
        }
        if energy > 0.7:
            pattern["hihat_closed"] = [i/16 for i in range(1, 16, 2)]
            pattern["hihat_open"] = [6/16, 14/16]
            pattern["rimshot"] = [2/16, 10/16]
        if "build" in section_name:
            # Build-up: add snare roll
            pattern["snare"] = [i/16 for i in range(0, 16)]
            pattern["kick"] = [0/16, 4/16, 8/16, 12/16]
        if "breakdown" in section_name:
            pattern = {
                "hihat_closed": [i/16 for i in range(0, 16, 4)],
            }

    elif genre == "house":
        pattern = {
            "kick": [0/16, 4/16, 8/16, 12/16],
            "clap": [4/16, 12/16],
            "hihat_closed": [2/16, 6/16, 10/16, 14/16],
            "shaker": [i/16 for i in range(0, 16, 2)],
        }
        if energy > 0.8:
            pattern["hihat_open"] = [6/16, 14/16]
            pattern["cowbell"] = [2/16, 10/16]
        if "breakdown" in section_name:
            pattern = {
                "shaker": [i/16 for i in range(0, 16, 4)],
                "hihat_closed": [4/16, 12/16],
            }
        if "build" in section_name:
            pattern["snare"] = [i/16 for i in range(0, 16)]

    elif genre == "lofi":
        pattern = {
            "kick": [0/16, 10/16],
            "snare": [4/16, 12/16],
            "hihat_closed": [0/16, 4/16, 6/16, 8/16, 12/16, 14/16],
        }
        # Lo-fi has looser, jazzy timing — handled via small random offsets in render

    elif genre == "ambient":
        # Minimal or no drums
        pattern = {}
        if energy > 0.4:
            pattern["shaker"] = [0/16, 8/16]
        if energy > 0.6:
            pattern["kick"] = [0/16]
            pattern["shaker"] = [0/16, 4/16, 8/16, 12/16]

    elif genre == "anime_ost":
        pattern = {
            "kick": [0/16, 8/16],
            "snare": [4/16, 12/16],
            "hihat_closed": [i/16 for i in range(0, 16, 2)],
        }
        if energy > 0.8:
            pattern["kick"] = [0/16, 3/16, 8/16, 11/16]
            pattern["hihat_closed"] = [i/16 for i in range(16)]
            pattern["cowbell"] = [2/16]
        if "battle" in section_name or "climax" in section_name:
            pattern["kick"] = [0/16, 2/16, 4/16, 6/16, 8/16, 10/16, 12/16, 14/16]
            pattern["snare"] = [4/16, 12/16]
            pattern["hihat_closed"] = [i/16 for i in range(16)]
        if "interlude" in section_name or "resolution" in section_name:
            pattern = {
                "hihat_closed": [0/16, 8/16],
            }
    else:
        pattern = {
            "kick": [0/16, 8/16],
            "snare": [4/16, 12/16],
            "hihat_closed": [i/16 for i in range(0, 16, 4)],
        }

    return pattern


# ============================================================
# TRACK RENDERER
# ============================================================
def render_track(genre, mood=None, title=None, track_id=None):
    """Render a complete music track. Returns metadata dict."""
    config = GENRE_CONFIG[genre]
    bpm = config["bpm"]
    duration_sec = config["duration"]

    if mood is None:
        mood = random.choice(config["moods"])
    if mood not in config["moods"]:
        mood = config["moods"][0]

    if track_id is None:
        track_id = hashlib.md5(f"{genre}_{mood}_{time.time()}_{random.random()}".encode()).hexdigest()[:12]

    if title is None:
        parts = TITLE_PARTS[genre]
        title = f"{random.choice(parts['prefix'])} {random.choice(parts['suffix'])}"

    print(f"[STUDIO] Rendering: '{title}' ({genre}/{mood}) @ {bpm} BPM, {duration_sec}s")
    start_time = time.time()

    # Get chord progression
    progression_data = GENRE_PROGRESSIONS[genre].get(mood, list(GENRE_PROGRESSIONS[genre].values())[0])
    progression = [build_chord(root, ctype, octave=3) for root, ctype in progression_data]
    bass_notes = [build_chord(root, ctype, octave=2)[0] for root, ctype in progression_data]

    # Song structure
    structure = SONG_STRUCTURES[genre]

    # Calculate total bars and samples
    beat_dur = 60.0 / bpm
    bar_dur = beat_dur * 4.0
    total_bars = sum(s[1] for s in structure)
    actual_duration = total_bars * bar_dur

    total_samples = int(actual_duration * SAMPLE_RATE)
    bar_samples = int(bar_dur * SAMPLE_RATE)
    beat_samples = int(beat_dur * SAMPLE_RATE)
    sixteenth_samples = beat_samples // 4

    # Audio buffers
    buf_l = [0.0] * total_samples
    buf_r = [0.0] * total_samples

    # Effects
    reverb_l = ReverbBuffer(SAMPLE_RATE, room_size=0.35 if genre != "ambient" else 0.6)
    reverb_r = ReverbBuffer(SAMPLE_RATE, room_size=0.35 if genre != "ambient" else 0.6)

    # Pink noise for lo-fi
    pink = pink_noise_gen() if genre == "lofi" else None

    # Build section map: for each bar, what section is it?
    section_map = []  # (section_name, energy, bar_index_in_section, total_bars_in_section)
    for sec_name, sec_bars, sec_energy in structure:
        for b in range(sec_bars):
            section_map.append((sec_name, sec_energy, b, sec_bars))

    print(f"  Structure: {total_bars} bars, {actual_duration:.1f}s actual")

    # --- MAIN RENDER LOOP (per sample) ---
    for i in range(total_samples):
        t = i / SAMPLE_RATE
        bar_idx = min(i // bar_samples, total_bars - 1)
        pos_in_bar = (i % bar_samples) / bar_samples  # 0.0 to 1.0
        beat_in_bar = int(pos_in_bar * 4)
        t_in_beat = (i % beat_samples) / SAMPLE_RATE
        sixteenth_in_bar = int(pos_in_bar * 16)
        t_in_16th = (i % max(sixteenth_samples, 1)) / SAMPLE_RATE

        sec_name, energy, bar_in_sec, bars_in_sec = section_map[bar_idx]
        chord_idx = bar_idx % len(progression)
        chord_freqs = progression[chord_idx]
        bass_freq = bass_notes[chord_idx]

        val = 0.0

        # ---- GENRE-SPECIFIC RENDERING ----

        if genre == "rap":
            # === 808 BASS ===
            bass_env = adsr(t_in_beat, 0.005, 0.05, 0.85, 0.1, beat_dur)
            if beat_in_bar in (0, 2):
                val += synth_bass_808(bass_freq * 0.5, t_in_beat, beat_dur, 0.35) * energy
            elif beat_in_bar == 3 and energy > 0.6:
                val += synth_bass_808(bass_freq * 0.5, t_in_beat, beat_dur * 0.5, 0.25) * energy

            # === PAD (subtle atmosphere) ===
            val += synth_pad(chord_freqs, t, vol=0.03 * energy)

            # === HI-HAT ROLLS in high energy ===
            if energy > 0.8:
                val += synth_arp_note(chord_freqs[0] * 4, t_in_16th, sixteenth_samples / SAMPLE_RATE, 0.02)

            # === DRUMS ===
            drum_pat = get_drum_pattern(genre, sec_name, energy)
            for inst, positions in drum_pat.items():
                for pos in positions:
                    trigger_sample = int(pos * bar_samples)
                    dt = (i % bar_samples - trigger_sample) / SAMPLE_RATE
                    if 0 <= dt < 0.4:
                        if inst == "kick":
                            val += synth_kick_808(dt)
                        elif inst == "snare":
                            val += synth_snare(dt) * 1.1
                        elif inst == "clap":
                            val += synth_clap(dt)
                        elif inst == "hihat_closed":
                            val += synth_hihat_closed(dt)
                        elif inst == "hihat_open":
                            val += synth_hihat_open(dt)

        elif genre == "country":
            # === ACOUSTIC GUITAR STRUM ===
            if beat_in_bar == 0:
                val += synth_guitar_strum(chord_freqs, t_in_beat, beat_dur, 0.12 * energy)
            elif beat_in_bar == 2:
                val += synth_guitar_strum(chord_freqs, t_in_beat, beat_dur * 0.5, 0.08 * energy)
            # Upstroke on off-beats
            if beat_in_bar in (1, 3) and energy > 0.5:
                up_freqs = [f * 2.0 for f in chord_freqs[-3:]]  # Higher strings
                val += synth_guitar_strum(up_freqs, t_in_beat, beat_dur * 0.3, 0.05 * energy)

            # === BASS ===
            val += synth_bass_sub(bass_freq * 0.5, t_in_beat, beat_dur, 0.12 * energy)

            # === LEAD MELODY hint ===
            if sec_name in ("chorus", "bridge") and energy > 0.6:
                melody_note = chord_freqs[bar_in_sec % len(chord_freqs)] * 2
                val += synth_pluck(melody_note, t_in_16th, beat_dur / 4, 0.04 * energy)

            # === DRUMS ===
            drum_pat = get_drum_pattern(genre, sec_name, energy)
            for inst, positions in drum_pat.items():
                for pos in positions:
                    trigger_sample = int(pos * bar_samples)
                    dt = (i % bar_samples - trigger_sample) / SAMPLE_RATE
                    if 0 <= dt < 0.3:
                        if inst == "kick":
                            val += synth_kick_acoustic(dt)
                        elif inst == "snare":
                            val += synth_snare(dt) * 0.8
                        elif inst == "hihat_closed":
                            val += synth_hihat_closed(dt) * 0.8
                        elif inst == "shaker":
                            val += synth_shaker(dt)

        elif genre == "techno":
            # === SAW BASS ===
            val += synth_bass_saw(bass_freq * 0.5, t_in_beat, beat_dur, 0.2 * energy)

            # === SYNTH PAD ===
            if "drop" in sec_name:
                val += synth_pad(chord_freqs, t, vol=0.04 * energy)
            elif "breakdown" in sec_name:
                val += synth_pad_warm(chord_freqs, t, vol=0.06)

            # === ARPEGGIOS ===
            if energy > 0.5 and "breakdown" not in sec_name:
                arp_idx = sixteenth_in_bar % len(chord_freqs)
                arp_freq = chord_freqs[arp_idx] * 2.0
                val += synth_arp_note(arp_freq, t_in_16th, beat_dur / 4, 0.05 * energy)

            # === ACID LINE (resonant filter-like) ===
            if "drop" in sec_name and energy > 0.7:
                acid_freq = bass_freq * 2.0 * (1.0 + 0.5 * osc_sine(0.25, t))
                val += osc_saw(acid_freq, t) * 0.04 * adsr(t_in_16th, 0.005, 0.02, 0.5, 0.02, beat_dur / 4)

            # === DRUMS ===
            drum_pat = get_drum_pattern(genre, sec_name, energy)
            for inst, positions in drum_pat.items():
                for pos in positions:
                    trigger_sample = int(pos * bar_samples)
                    dt = (i % bar_samples - trigger_sample) / SAMPLE_RATE
                    if 0 <= dt < 0.4:
                        if inst == "kick":
                            val += synth_kick_808(dt) * 1.2
                        elif inst == "snare":
                            val += synth_snare(dt)
                        elif inst == "clap":
                            val += synth_clap(dt)
                        elif inst == "hihat_closed":
                            val += synth_hihat_closed(dt)
                        elif inst == "hihat_open":
                            val += synth_hihat_open(dt)
                        elif inst == "rimshot":
                            val += synth_rimshot(dt)

        elif genre == "house":
            # === DEEP BASS ===
            val += synth_bass_808(bass_freq * 0.5, t_in_beat, beat_dur, 0.25 * energy)

            # === CHORD STABS ===
            if "drop" in sec_name:
                stab_freqs = [f * 2.0 for f in chord_freqs]
                if beat_in_bar in (0, 2) or (energy > 0.8 and beat_in_bar == 3):
                    val += synth_stab(stab_freqs, t_in_beat, beat_dur * 0.2, 0.12 * energy)

            # === PAD ===
            if "breakdown" in sec_name:
                val += synth_pad_warm(chord_freqs, t, vol=0.08)
            else:
                val += synth_pad(chord_freqs, t, vol=0.025 * energy)

            # === ARPEGGIOS ===
            if energy > 0.6 and "drop" in sec_name:
                arp_idx = sixteenth_in_bar % len(chord_freqs)
                val += synth_arp_note(chord_freqs[arp_idx] * 2, t_in_16th, beat_dur / 4, 0.04)

            # === BUILD RISER ===
            if "build" in sec_name:
                riser_progress = bar_in_sec / max(bars_in_sec - 1, 1)
                riser_freq = 200.0 + riser_progress * 2000.0
                val += osc_sine(riser_freq, t) * 0.03 * riser_progress
                val += white_noise() * 0.02 * riser_progress

            # === DRUMS ===
            drum_pat = get_drum_pattern(genre, sec_name, energy)
            for inst, positions in drum_pat.items():
                for pos in positions:
                    trigger_sample = int(pos * bar_samples)
                    dt = (i % bar_samples - trigger_sample) / SAMPLE_RATE
                    if 0 <= dt < 0.4:
                        if inst == "kick":
                            val += synth_kick_808(dt) * 1.1
                        elif inst == "snare":
                            val += synth_snare(dt)
                        elif inst == "clap":
                            val += synth_clap(dt) * 1.1
                        elif inst == "hihat_closed":
                            val += synth_hihat_closed(dt)
                        elif inst == "hihat_open":
                            val += synth_hihat_open(dt)
                        elif inst == "shaker":
                            val += synth_shaker(dt)
                        elif inst == "cowbell":
                            val += synth_cowbell(dt)

        elif genre == "lofi":
            # === JAZZY CHORDS (electric piano feel) ===
            ep_vol = 0.07 * energy
            for fi, f in enumerate(chord_freqs):
                env = math.exp(-(t_in_beat) * 2.0) if beat_in_bar in (0, 2) else math.exp(-(t_in_beat) * 3.0) * 0.6
                val += osc_sine(f, t) * ep_vol * env * 0.5
                val += osc_sine(f * 2.0, t) * ep_vol * env * 0.2
                val += osc_triangle(f, t) * ep_vol * env * 0.3

            # === BASS (muted) ===
            val += synth_bass_sub(bass_freq * 0.5, t_in_beat, beat_dur, 0.1 * energy)

            # === VINYL CRACKLE ===
            val += vinyl_crackle(t)

            # === TAPE WOBBLE (applied to pitch) ===
            wobble = tape_wobble(t, rate=0.2, depth=0.001)

            # === MELODY hint ===
            if bar_in_sec % 2 == 0 and beat_in_bar == 0:
                mel_freq = chord_freqs[0] * 2.0 * wobble
                val += synth_pluck(mel_freq, t_in_beat, beat_dur * 0.5, 0.03)

            # === DRUMS (with swing) ===
            drum_pat = get_drum_pattern(genre, sec_name, energy)
            for inst, positions in drum_pat.items():
                for pos in positions:
                    # Add swing: shift every other 16th note slightly
                    swing_offset = 0.008 if (int(pos * 16) % 2 == 1) else 0.0
                    trigger_sample = int((pos + swing_offset) * bar_samples)
                    dt = (i % bar_samples - trigger_sample) / SAMPLE_RATE
                    if 0 <= dt < 0.3:
                        if inst == "kick":
                            val += synth_kick_acoustic(dt) * 0.7
                        elif inst == "snare":
                            val += synth_snare(dt) * 0.5
                        elif inst == "hihat_closed":
                            val += synth_hihat_closed(dt) * 0.6

        elif genre == "ambient":
            # === EVOLVING PADS ===
            lfo_rate = 0.05 + 0.03 * osc_sine(0.01, t)
            pad_vol = 0.08 * energy
            for fi, f in enumerate(chord_freqs):
                mod = 1.0 + 0.01 * osc_sine(lfo_rate + fi * 0.02, t)
                val += osc_sine(f * mod, t) * pad_vol * 0.4
                val += osc_triangle(f * mod * 0.999, t) * pad_vol * 0.3
                val += osc_sine(f * 2.0 * mod, t) * pad_vol * 0.15
                val += osc_sine(f * 0.5, t) * pad_vol * 0.15  # Sub

            # === DRONE ===
            drone_freq = bass_freq * 0.25
            val += osc_sine(drone_freq, t) * 0.04 * energy
            val += osc_sine(drone_freq * 1.5, t) * 0.015 * energy

            # === TEXTURE ===
            val += white_noise() * 0.005 * energy

            # === SPARKLES (random high notes) ===
            if random.random() < 0.0003 * energy:
                sparkle_freq = random.choice(chord_freqs) * 4.0
                val += osc_sine(sparkle_freq, 0) * 0.03

            # === MINIMAL DRUMS ===
            drum_pat = get_drum_pattern(genre, sec_name, energy)
            for inst, positions in drum_pat.items():
                for pos in positions:
                    trigger_sample = int(pos * bar_samples)
                    dt = (i % bar_samples - trigger_sample) / SAMPLE_RATE
                    if 0 <= dt < 0.3:
                        if inst == "kick":
                            val += synth_kick_acoustic(dt) * 0.3
                        elif inst == "shaker":
                            val += synth_shaker(dt) * 0.5

        elif genre == "anime_ost":
            # === STRING ENSEMBLE ===
            val += synth_string_ensemble(chord_freqs, t, vol=0.05 * energy)

            # === BRASS (high energy sections) ===
            if energy > 0.7:
                brass_freq = chord_freqs[0] * 2.0
                val += synth_brass(brass_freq, t_in_beat, beat_dur, 0.06 * energy)

            # === BASS ===
            val += synth_bass_sub(bass_freq * 0.5, t_in_beat, beat_dur, 0.15 * energy)

            # === MELODY ===
            if sec_name in ("theme_a", "theme_b", "climax"):
                mel_idx = sixteenth_in_bar % len(chord_freqs)
                mel_freq = chord_freqs[mel_idx] * 2.0
                val += synth_pluck(mel_freq, t_in_16th, beat_dur / 4, 0.06 * energy)

            # === PIANO-LIKE (emotional sections) ===
            if "emotional" in mood or "resolution" in sec_name:
                for f in chord_freqs:
                    env = math.exp(-t_in_beat * 2.5)
                    val += osc_sine(f * 2.0, t) * 0.03 * env * energy
                    val += osc_triangle(f * 2.0, t) * 0.02 * env * energy

            # === TIMPANI HITS (climax) ===
            if "climax" in sec_name and beat_in_bar == 0 and bar_in_sec % 2 == 0:
                val += synth_kick_808(t_in_beat, 0.5) * 0.3

            # === DRUMS ===
            drum_pat = get_drum_pattern(genre, sec_name, energy)
            for inst, positions in drum_pat.items():
                for pos in positions:
                    trigger_sample = int(pos * bar_samples)
                    dt = (i % bar_samples - trigger_sample) / SAMPLE_RATE
                    if 0 <= dt < 0.4:
                        if inst == "kick":
                            val += synth_kick_acoustic(dt) * 1.0
                        elif inst == "snare":
                            val += synth_snare(dt) * 0.9
                        elif inst == "hihat_closed":
                            val += synth_hihat_closed(dt) * 0.7
                        elif inst == "cowbell":
                            val += synth_cowbell(dt) * 0.5

        # ---- SECTION TRANSITIONS ----
        # Fade in at start of track
        if t < 2.0:
            val *= t / 2.0
        # Fade out at end
        if t > actual_duration - 3.0:
            val *= max(0.0, (actual_duration - t) / 3.0)
        # Section energy crossfade
        if bar_in_sec == 0:
            # First bar of new section — slight fade in
            section_fade = min(1.0, pos_in_bar * 2.0 + 0.5)
            val *= section_fade

        # Soft clip
        val = math.tanh(val * 1.5) * 0.667

        # Stereo spread (slight L/R difference)
        spread = 0.02 * osc_sine(0.1, t)
        buf_l[i] = val * (1.0 + spread)
        buf_r[i] = val * (1.0 - spread)

    # --- POST-PROCESSING ---
    print("  Applying reverb...")
    for i in range(total_samples):
        wet_l = reverb_l.process(buf_l[i])
        wet_r = reverb_r.process(buf_r[i])
        mix = 0.3 if genre != "ambient" else 0.5
        buf_l[i] = buf_l[i] * (1.0 - mix) + wet_l * mix
        buf_r[i] = buf_r[i] * (1.0 - mix) + wet_r * mix

    # Normalize
    peak = 0.0
    for i in range(total_samples):
        peak = max(peak, abs(buf_l[i]), abs(buf_r[i]))
    if peak > 0:
        gain = 0.9 / peak
        for i in range(total_samples):
            buf_l[i] *= gain
            buf_r[i] *= gain

    # Write WAV
    wav_path = os.path.join(TRACKS_DIR, f"{track_id}.wav")
    print(f"  Writing WAV: {wav_path}")
    with wave.open(wav_path, 'w') as wf:
        wf.setnchannels(2)
        wf.setsampwidth(2)
        wf.setframerate(SAMPLE_RATE)
        # Write in chunks for memory efficiency
        chunk_size = 8192
        for start in range(0, total_samples, chunk_size):
            end = min(start + chunk_size, total_samples)
            frames = b''
            for j in range(start, end):
                l_val = max(-MAX_AMP, min(MAX_AMP, int(buf_l[j] * MAX_AMP)))
                r_val = max(-MAX_AMP, min(MAX_AMP, int(buf_r[j] * MAX_AMP)))
                frames += struct.pack('<hh', l_val, r_val)
            wf.writeframes(frames)

    # Convert to MP3
    mp3_path = os.path.join(TRACKS_DIR, f"{track_id}.mp3")
    print(f"  Converting to MP3...")
    try:
        result = subprocess.run([
            'ffmpeg', '-y', '-i', wav_path,
            '-c:a', 'libmp3lame', '-b:a', '192k', '-ar', '44100',
            '-metadata', f'title={title}',
            '-metadata', f'artist={ARTIST}',
            '-metadata', f'genre={genre}',
            '-metadata', f'album=Hive Music Studio',
            mp3_path
        ], capture_output=True, text=True, timeout=120)
        if result.returncode == 0:
            os.remove(wav_path)
            print(f"  MP3 created: {mp3_path}")
        else:
            print(f"  ffmpeg error: {result.stderr[:200]}")
            mp3_path = wav_path  # fallback to WAV
    except Exception as e:
        print(f"  MP3 conversion failed: {e}")
        mp3_path = wav_path

    elapsed = time.time() - start_time
    file_size = os.path.getsize(mp3_path) if os.path.exists(mp3_path) else 0

    # Build metadata
    metadata = {
        "track_id": track_id,
        "title": title,
        "artist": ARTIST,
        "genre": genre,
        "mood": mood,
        "bpm": bpm,
        "duration_seconds": round(actual_duration, 1),
        "structure": [s[0] for s in structure],
        "chord_progression": [f"{r}{t}" for r, t in progression_data],
        "file_path": mp3_path,
        "file_size_mb": round(file_size / (1024 * 1024), 2),
        "render_time_seconds": round(elapsed, 1),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "album_art_prompt": ART_PROMPTS.get(genre, ""),
        "description": config["description"],
    }

    # Save metadata JSON
    meta_path = os.path.join(META_DIR, f"{track_id}.json")
    with open(meta_path, 'w') as f:
        json.dump(metadata, f, indent=2)

    print(f"  DONE: '{title}' rendered in {elapsed:.1f}s ({metadata['file_size_mb']}MB)")
    return metadata


# ============================================================
# RAP VOCAL GENERATION (edge-tts)
# ============================================================
async def generate_rap_vocals(lyrics, track_id, bpm=90):
    """Generate rap vocals using edge-tts with en-US-GuyNeural."""
    vocal_path = os.path.join(VOCALS_DIR, f"{track_id}_vocals.mp3")
    try:
        import edge_tts
        communicate = edge_tts.Communicate(
            lyrics,
            voice="en-US-GuyNeural",
            rate="+10%",  # Slightly faster for rap flow
            pitch="-5Hz",  # Slightly deeper
        )
        await communicate.save(vocal_path)
        print(f"  Vocals saved: {vocal_path}")
        return vocal_path
    except ImportError:
        print("  edge-tts not installed, trying CLI...")
        try:
            result = subprocess.run([
                'edge-tts',
                '--voice', 'en-US-GuyNeural',
                '--rate', '+10%',
                '--pitch', '-5Hz',
                '--text', lyrics,
                '--write-media', vocal_path,
            ], capture_output=True, text=True, timeout=60)
            if result.returncode == 0:
                print(f"  Vocals saved: {vocal_path}")
                return vocal_path
            else:
                print(f"  edge-tts error: {result.stderr[:200]}")
        except Exception as e:
            print(f"  Vocal generation failed: {e}")
    return None


async def generate_rap_lyrics_ollama(mood="aggressive"):
    """Try to generate rap lyrics using local Ollama. Falls back to templates."""
    try:
        import urllib.request
        prompt = (
            f"Write a short 4-line rap verse about AI and technology. "
            f"Mood: {mood}. Keep it clean, energetic, and rhythmic. "
            f"No explicit content. Just the lyrics, no explanation."
        )
        data = json.dumps({
            "model": "gemma2:2b",
            "prompt": prompt,
            "stream": False,
            "options": {"num_predict": 150, "temperature": 0.9}
        }).encode()
        req = urllib.request.Request(
            "http://localhost:11434/api/generate",
            data=data,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode())
            lyrics = result.get("response", "").strip()
            if len(lyrics) > 20:
                return lyrics
    except Exception as e:
        print(f"  Ollama lyrics failed: {e}")

    # Fallback to templates
    templates = RAP_LYRICS.get(mood, RAP_LYRICS["aggressive"])
    return random.choice(templates)


async def produce_rap_with_vocals(mood=None, title=None):
    """Full rap production: beat + vocals mixed together."""
    if mood is None:
        mood = random.choice(GENRE_CONFIG["rap"]["moods"])

    track_id = hashlib.md5(f"rap_{mood}_{time.time()}".encode()).hexdigest()[:12]
    if title is None:
        parts = TITLE_PARTS["rap"]
        title = f"{random.choice(parts['prefix'])} {random.choice(parts['suffix'])}"

    # Generate beat
    metadata = render_track("rap", mood=mood, title=title, track_id=track_id)
    beat_path = metadata["file_path"]

    # Generate lyrics and vocals
    lyrics = await generate_rap_lyrics_ollama(mood)
    print(f"  Lyrics: {lyrics[:80]}...")
    vocal_path = await generate_rap_vocals(lyrics, track_id, bpm=90)

    if vocal_path and os.path.exists(vocal_path):
        # Mix beat and vocals using ffmpeg
        mixed_path = os.path.join(TRACKS_DIR, f"{track_id}_mixed.mp3")
        try:
            result = subprocess.run([
                'ffmpeg', '-y',
                '-i', beat_path,
                '-i', vocal_path,
                '-filter_complex',
                '[0:a]volume=0.7[beat];[1:a]volume=1.0,adelay=3000|3000[vox];[beat][vox]amix=inputs=2:duration=first[out]',
                '-map', '[out]',
                '-c:a', 'libmp3lame', '-b:a', '192k',
                mixed_path
            ], capture_output=True, text=True, timeout=120)
            if result.returncode == 0:
                # Replace beat with mixed version
                os.remove(beat_path)
                os.rename(mixed_path, beat_path)
                metadata["has_vocals"] = True
                metadata["lyrics"] = lyrics
                print(f"  Mixed vocals into beat")
            else:
                print(f"  Mix failed: {result.stderr[:200]}")
                metadata["has_vocals"] = False
        except Exception as e:
            print(f"  Mixing failed: {e}")
            metadata["has_vocals"] = False
    else:
        metadata["has_vocals"] = False

    # Update metadata
    meta_path = os.path.join(META_DIR, f"{track_id}.json")
    with open(meta_path, 'w') as f:
        json.dump(metadata, f, indent=2)

    return metadata


# ============================================================
# PRODUCTION STATS
# ============================================================
def get_all_tracks():
    """List all produced tracks from metadata."""
    tracks = []
    if os.path.exists(META_DIR):
        for fname in sorted(os.listdir(META_DIR)):
            if fname.endswith('.json'):
                try:
                    with open(os.path.join(META_DIR, fname)) as f:
                        tracks.append(json.load(f))
                except Exception:
                    pass
    return tracks


def get_stats():
    """Get production statistics."""
    tracks = get_all_tracks()
    total_duration = sum(t.get("duration_seconds", 0) for t in tracks)
    total_size = sum(t.get("file_size_mb", 0) for t in tracks)
    genres = {}
    moods = {}
    for t in tracks:
        g = t.get("genre", "unknown")
        m = t.get("mood", "unknown")
        genres[g] = genres.get(g, 0) + 1
        moods[m] = moods.get(m, 0) + 1

    return {
        "total_tracks": len(tracks),
        "total_duration_minutes": round(total_duration / 60, 1),
        "total_size_mb": round(total_size, 1),
        "genres": genres,
        "moods": moods,
        "avg_render_time": round(
            sum(t.get("render_time_seconds", 0) for t in tracks) / max(len(tracks), 1), 1
        ),
        "tracks_with_vocals": sum(1 for t in tracks if t.get("has_vocals")),
        "studio_dir": STUDIO_DIR,
    }


# ============================================================
# FASTAPI APP
# ============================================================
app = FastAPI(
    title="Hive Music Studio",
    description="AI Music Production System — Generates multi-genre tracks with pure Python synthesis",
    version="1.0.0",
)

# Production loop control
production_running = False
production_task = None


async def production_loop():
    """Background production loop: generate 1 track every 30 minutes."""
    global production_running
    production_running = True
    print("[STUDIO] Production loop started (1 track every 30 min)")
    while production_running:
        try:
            genre = random.choice(list(GENRE_CONFIG.keys()))
            mood = random.choice(GENRE_CONFIG[genre]["moods"])
            print(f"\n[STUDIO] Auto-producing: {genre}/{mood}")

            if genre == "rap" and random.random() < 0.5:
                # 50% chance of vocal rap track
                await produce_rap_with_vocals(mood=mood)
            else:
                # Run blocking render in thread pool
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, render_track, genre, mood, None, None)

        except Exception as e:
            print(f"[STUDIO] Production error: {e}")

        # Wait 30 minutes
        await asyncio.sleep(PRODUCTION_INTERVAL)


@app.on_event("startup")
async def startup():
    """Start production loop on server startup."""
    global production_task
    production_task = asyncio.create_task(production_loop())
    print("[STUDIO] Hive Music Studio online on port 8911")


@app.on_event("shutdown")
async def shutdown():
    """Stop production loop on server shutdown."""
    global production_running, production_task
    production_running = False
    if production_task:
        production_task.cancel()


@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "service": "hive-music-studio",
        "port": 8911,
        "genres": list(GENRE_CONFIG.keys()),
        "tracks_produced": len(get_all_tracks()),
        "production_loop": production_running,
        "uptime_note": "1 track every 30 minutes",
    }


@app.post("/api/produce")
async def produce_track(
    genre: str = "rap",
    mood: Optional[str] = None,
    title: Optional[str] = None,
    vocals: bool = True,
):
    """Produce a new track. For rap, optionally include AI vocals."""
    if genre not in GENRE_CONFIG:
        raise HTTPException(400, f"Unknown genre: {genre}. Available: {list(GENRE_CONFIG.keys())}")

    if mood and mood not in GENRE_CONFIG[genre]["moods"]:
        raise HTTPException(400, f"Unknown mood for {genre}. Available: {GENRE_CONFIG[genre]['moods']}")

    try:
        if genre == "rap" and vocals:
            metadata = await produce_rap_with_vocals(mood=mood, title=title)
        else:
            loop = asyncio.get_event_loop()
            metadata = await loop.run_in_executor(None, render_track, genre, mood, title, None)
        return JSONResponse(metadata)
    except Exception as e:
        raise HTTPException(500, f"Production failed: {str(e)}")


@app.get("/api/tracks")
async def list_tracks(genre: Optional[str] = None, limit: int = 50):
    """List all produced tracks, optionally filtered by genre."""
    tracks = get_all_tracks()
    if genre:
        tracks = [t for t in tracks if t.get("genre") == genre]
    # Most recent first
    tracks.sort(key=lambda t: t.get("created_at", ""), reverse=True)
    return {
        "total": len(tracks),
        "tracks": tracks[:limit],
    }


@app.get("/api/tracks/{track_id}")
async def get_track(track_id: str):
    """Get track metadata by ID."""
    meta_path = os.path.join(META_DIR, f"{track_id}.json")
    if not os.path.exists(meta_path):
        raise HTTPException(404, f"Track not found: {track_id}")
    with open(meta_path) as f:
        return json.load(f)


@app.get("/api/tracks/{track_id}/download")
async def download_track(track_id: str):
    """Download a track file."""
    meta_path = os.path.join(META_DIR, f"{track_id}.json")
    if not os.path.exists(meta_path):
        raise HTTPException(404, f"Track not found: {track_id}")
    with open(meta_path) as f:
        meta = json.load(f)
    file_path = meta.get("file_path", "")
    if not os.path.exists(file_path):
        raise HTTPException(404, f"Track file missing: {file_path}")
    return FileResponse(file_path, media_type="audio/mpeg", filename=f"{meta.get('title', track_id)}.mp3")


@app.get("/api/stats")
async def stats():
    """Production statistics."""
    return get_stats()


@app.get("/api/genres")
async def genres():
    """List available genres with their config."""
    return GENRE_CONFIG


@app.post("/api/produce-batch")
async def produce_batch(genres_list: Optional[list] = None, count_per_genre: int = 1):
    """Produce multiple tracks across genres. Runs in background."""
    if genres_list is None:
        genres_list = list(GENRE_CONFIG.keys())

    async def batch_produce():
        results = []
        for genre in genres_list:
            for _ in range(count_per_genre):
                try:
                    mood = random.choice(GENRE_CONFIG[genre]["moods"])
                    if genre == "rap":
                        meta = await produce_rap_with_vocals(mood=mood)
                    else:
                        loop = asyncio.get_event_loop()
                        meta = await loop.run_in_executor(None, render_track, genre, mood, None, None)
                    results.append(meta)
                except Exception as e:
                    print(f"Batch error ({genre}): {e}")
        return results

    # Run in background
    asyncio.create_task(batch_produce())
    total = len(genres_list) * count_per_genre
    return {
        "status": "batch_started",
        "total_tracks_queued": total,
        "genres": genres_list,
        "message": f"Producing {total} tracks in background",
    }


# ============================================================
# CLI MODE
# ============================================================
def cli_main():
    """CLI entrypoint for testing."""
    import argparse
    parser = argparse.ArgumentParser(description="Hive Music Studio CLI")
    parser.add_argument("--genre", default="rap", choices=list(GENRE_CONFIG.keys()))
    parser.add_argument("--mood", default=None)
    parser.add_argument("--title", default=None)
    parser.add_argument("--vocals", action="store_true", help="Include vocals (rap only)")
    parser.add_argument("--serve", action="store_true", help="Start FastAPI server")
    parser.add_argument("--all", action="store_true", help="Generate one of each genre")
    args = parser.parse_args()

    if args.serve:
        uvicorn.run(app, host="0.0.0.0", port=8911)
        return

    if args.all:
        print("=" * 70)
        print("  HIVE MUSIC STUDIO — Full Genre Suite")
        print("=" * 70)
        for genre in GENRE_CONFIG:
            mood = random.choice(GENRE_CONFIG[genre]["moods"])
            render_track(genre, mood=mood)
        print("\n" + "=" * 70)
        stats = get_stats()
        print(f"  {stats['total_tracks']} tracks, {stats['total_duration_minutes']} min, {stats['total_size_mb']} MB")
        print("=" * 70)
        return

    if args.genre == "rap" and args.vocals:
        asyncio.run(produce_rap_with_vocals(mood=args.mood, title=args.title))
    else:
        render_track(args.genre, mood=args.mood, title=args.title)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        cli_main()
    else:
        uvicorn.run(app, host="0.0.0.0", port=8911)
