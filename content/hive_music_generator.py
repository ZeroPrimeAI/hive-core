#!/usr/bin/env python3
"""
Hive Music Generator — Professional Background Music
Generates mood-specific background tracks with:
- Proper chord progressions (I-V-vi-IV, ii-V-I, etc.)
- Drum patterns (kick, snare, hi-hat from noise synthesis)
- Bass lines following chord roots
- Arpeggiated melodies
- Reverb effect (delay-based)
- Multiple moods: epic, calm, tense, upbeat, mysterious, action
"""

import math
import random
import struct
import wave
import os
import sys

OUTPUT_DIR = "/tmp/hive_music"
os.makedirs(OUTPUT_DIR, exist_ok=True)

SAMPLE_RATE = 44100

# === Note frequencies ===
NOTES = {
    'C2': 65.41, 'D2': 73.42, 'E2': 82.41, 'F2': 87.31, 'G2': 98.00, 'A2': 110.0, 'B2': 123.5,
    'C3': 130.8, 'D3': 146.8, 'E3': 164.8, 'F3': 174.6, 'G3': 196.0, 'A3': 220.0, 'B3': 246.9,
    'C4': 261.6, 'D4': 293.7, 'E4': 329.6, 'F4': 349.2, 'G4': 392.0, 'A4': 440.0, 'B4': 493.9,
    'C5': 523.3, 'D5': 587.3, 'E5': 659.3, 'F5': 698.5, 'G5': 784.0, 'A5': 880.0,
    'Bb2': 116.5, 'Eb3': 155.6, 'Ab3': 207.7, 'Bb3': 233.1, 'Eb4': 311.1, 'Ab4': 415.3,
    'F#3': 185.0, 'F#4': 370.0, 'C#3': 138.6, 'C#4': 277.2,
}

# Chord definitions (root, third, fifth, optional seventh)
CHORD_LIBRARY = {
    'Cmaj': ['C3', 'E3', 'G3'], 'Cmaj7': ['C3', 'E3', 'G3', 'B3'],
    'Dm': ['D3', 'F3', 'A3'], 'Dm7': ['D3', 'F3', 'A3', 'C4'],
    'Em': ['E3', 'G3', 'B3'], 'Em7': ['E3', 'G3', 'B3', 'D4'],
    'Fmaj': ['F3', 'A3', 'C4'], 'Fmaj7': ['F3', 'A3', 'C4', 'E4'],
    'Gmaj': ['G3', 'B3', 'D4'], 'G7': ['G3', 'B3', 'D4', 'F4'],
    'Am': ['A3', 'C4', 'E4'], 'Am7': ['A3', 'C4', 'E4', 'G4'],
    'Bdim': ['B3', 'D4', 'F4'],
    'Bbmaj': ['Bb3', 'D4', 'F4'],
    'Ebmaj': ['Eb3', 'G3', 'Bb3'],
    'Abmaj': ['Ab3', 'C4', 'Eb4'],
}

# Chord progressions by mood
PROGRESSIONS = {
    'epic': [
        ['Am', 'Fmaj', 'Cmaj', 'Gmaj'],  # i-VI-III-VII (epic cinematic)
        ['Cmaj', 'Gmaj', 'Am', 'Fmaj'],   # I-V-vi-IV (anthemic)
    ],
    'calm': [
        ['Cmaj7', 'Am7', 'Fmaj7', 'Gmaj'],  # Smooth jazz feel
        ['Fmaj7', 'Em7', 'Dm7', 'Cmaj7'],    # Descending
    ],
    'tense': [
        ['Am', 'Bdim', 'Em', 'Am'],        # Minor tension
        ['Em', 'Cmaj', 'Am', 'Bdim'],      # Dark buildup
    ],
    'upbeat': [
        ['Cmaj', 'Fmaj', 'Gmaj', 'Cmaj'],  # Happy major
        ['Gmaj', 'Em', 'Cmaj', 'Fmaj'],     # Pop progression
    ],
    'mysterious': [
        ['Am', 'Ebmaj', 'Bbmaj', 'Fmaj'],  # Chromatic movement
        ['Em', 'Cmaj', 'Abmaj', 'Bbmaj'],   # Unexpected shifts
    ],
    'action': [
        ['Em', 'Cmaj', 'Gmaj', 'Bdim'],    # Driving minor
        ['Am', 'Gmaj', 'Fmaj', 'Em'],       # Power descent
    ],
}

# Tempo by mood (BPM)
TEMPOS = {
    'epic': 80, 'calm': 65, 'tense': 100,
    'upbeat': 120, 'mysterious': 75, 'action': 140,
}


def sine(freq, t, phase=0):
    return math.sin(2 * math.pi * freq * t + phase)


def noise():
    return random.uniform(-1, 1)


def envelope(t, attack, decay, sustain, release, duration):
    """ADSR envelope."""
    if t < attack:
        return t / attack
    elif t < attack + decay:
        return 1.0 - (1.0 - sustain) * ((t - attack) / decay)
    elif t < duration - release:
        return sustain
    elif t < duration:
        return sustain * (1.0 - (t - (duration - release)) / release)
    return 0.0


def kick(t, duration=0.15):
    """Synthesize kick drum."""
    if t > duration:
        return 0
    env = max(0, 1.0 - t / duration)
    freq = 150 * math.exp(-t * 30) + 40
    return env * env * sine(freq, t) * 0.7


def snare(t, duration=0.12):
    """Synthesize snare drum."""
    if t > duration:
        return 0
    env = max(0, 1.0 - t / duration)
    tone = sine(180, t) * 0.3
    nz = noise() * 0.7
    return env * env * (tone + nz) * 0.5


def hihat(t, duration=0.05):
    """Synthesize hi-hat."""
    if t > duration:
        return 0
    env = max(0, 1.0 - t / duration)
    return env * noise() * 0.2


def pad_tone(freq, t, vol=0.06):
    """Soft pad synth tone with harmonics."""
    val = sine(freq, t) * vol
    val += sine(freq * 2, t) * vol * 0.3
    val += sine(freq * 3, t) * vol * 0.1
    return val


def bass_tone(freq, t, vol=0.12):
    """Bass synth with slight distortion."""
    val = sine(freq, t) * vol
    val += sine(freq * 2, t) * vol * 0.4  # Overtone
    # Soft clip
    val = max(-vol, min(vol, val * 1.3))
    return val


def arpeggio_note(freq, t, duration=0.15, vol=0.04):
    """Short pluck-like arpeggio note."""
    if t > duration:
        return 0
    env = envelope(t, 0.005, 0.05, 0.3, 0.08, duration)
    return env * sine(freq, t) * vol


def generate_track(mood, duration_sec, output_path):
    """Generate a full music track."""
    print(f"  Generating {mood} track ({duration_sec}s)...")

    total_samples = int(duration_sec * SAMPLE_RATE)
    bpm = TEMPOS[mood]
    beat_samples = int(SAMPLE_RATE * 60 / bpm)
    bar_samples = beat_samples * 4  # 4/4 time

    # Pick progression
    prog_options = PROGRESSIONS[mood]
    progression = prog_options[0]  # Use first option

    # Pre-compute chord frequencies for each bar
    samples_l = [0.0] * total_samples
    samples_r = [0.0] * total_samples

    # Reverb buffer (simple delay)
    reverb_delay = int(SAMPLE_RATE * 0.15)  # 150ms
    reverb_amount = 0.25

    for i in range(total_samples):
        t_global = i / SAMPLE_RATE
        bar_idx = i // bar_samples
        beat_in_bar = (i % bar_samples) // beat_samples
        pos_in_beat = (i % beat_samples) / beat_samples
        t_in_beat = (i % beat_samples) / SAMPLE_RATE

        # Current chord
        chord_name = progression[bar_idx % len(progression)]
        chord_notes = CHORD_LIBRARY.get(chord_name, ['C3', 'E3', 'G3'])
        chord_freqs = [NOTES[n] for n in chord_notes]
        bass_freq = chord_freqs[0] / 2  # Bass is one octave below root

        val = 0.0

        # === PAD (sustained chords) ===
        pad_vol = 0.04 if mood in ('calm', 'mysterious') else 0.06
        for freq in chord_freqs:
            val += pad_tone(freq, t_global, pad_vol)
            # Subtle detuning for width
            val += pad_tone(freq * 1.003, t_global, pad_vol * 0.3)

        # === BASS LINE ===
        # Bass plays on beats 1 and 3, with octave on beat 3
        bass_vol = 0.10 if mood in ('action', 'epic', 'upbeat') else 0.07
        if beat_in_bar in (0, 2):
            bass_env = envelope(t_in_beat, 0.01, 0.1, 0.6, 0.1, 60.0 / bpm)
            bf = bass_freq if beat_in_bar == 0 else bass_freq * 2
            val += bass_tone(bf, t_global, bass_vol) * bass_env

        # === DRUMS ===
        if mood not in ('calm',):
            # Kick on 1 and 3
            if beat_in_bar in (0, 2):
                val += kick(t_in_beat)
            # Snare on 2 and 4
            if beat_in_bar in (1, 3):
                val += snare(t_in_beat)
            # Hi-hat on every 8th note
            eighth_samples = beat_samples // 2
            t_in_eighth = (i % eighth_samples) / SAMPLE_RATE
            val += hihat(t_in_eighth)

            # Extra kick hits for action/upbeat
            if mood in ('action', 'upbeat') and beat_in_bar == 0:
                # 16th note kick at end of bar
                sixteenth_samples = beat_samples // 4
                if (i % bar_samples) > bar_samples - sixteenth_samples * 2:
                    val += kick(t_in_eighth) * 0.5

        # === ARPEGGIO (melody hints) ===
        if mood in ('epic', 'mysterious', 'upbeat', 'action'):
            # Arpeggiate through chord notes on 16th notes
            sixteenth_samples = beat_samples // 4
            arp_idx = ((i % bar_samples) // sixteenth_samples) % len(chord_freqs)
            t_in_16th = (i % sixteenth_samples) / SAMPLE_RATE
            arp_freq = chord_freqs[arp_idx] * 2  # One octave up
            arp_vol = 0.03 if mood == 'mysterious' else 0.05
            val += arpeggio_note(arp_freq, t_in_16th, 60.0 / bpm / 4, arp_vol)

        # === CALM MOOD: gentle arpeggios instead of drums ===
        if mood == 'calm':
            eighth_samples = beat_samples // 2
            arp_idx = ((i % bar_samples) // eighth_samples) % len(chord_freqs)
            t_in_8th = (i % eighth_samples) / SAMPLE_RATE
            arp_freq = chord_freqs[arp_idx] * 2
            val += arpeggio_note(arp_freq, t_in_8th, 60.0 / bpm / 2, 0.06)

        # === FADE IN/OUT ===
        if t_global < 2.0:
            val *= t_global / 2.0
        if t_global > duration_sec - 3.0:
            val *= (duration_sec - t_global) / 3.0

        # Soft limit
        val = max(-0.95, min(0.95, val))

        # Store with slight stereo spread
        samples_l[i] = val
        samples_r[i] = val

    # Apply reverb
    for i in range(reverb_delay, total_samples):
        samples_l[i] += samples_l[i - reverb_delay] * reverb_amount
        samples_r[i] += samples_r[i - reverb_delay] * reverb_amount * 0.8  # Slightly different for stereo

    # Normalize
    peak = max(max(abs(s) for s in samples_l), max(abs(s) for s in samples_r))
    if peak > 0:
        norm = 0.85 / peak
        samples_l = [s * norm for s in samples_l]
        samples_r = [s * norm for s in samples_r]

    # Write WAV
    with wave.open(output_path, 'w') as wf:
        wf.setnchannels(2)
        wf.setsampwidth(2)
        wf.setframerate(SAMPLE_RATE)
        for i in range(total_samples):
            l = int(samples_l[i] * 32000)
            r = int(samples_r[i] * 32000)
            wf.writeframes(struct.pack('<hh', l, r))

    size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"    OK: {os.path.basename(output_path)} ({size_mb:.1f}MB)")
    return output_path


def convert_to_mp3(wav_path):
    """Convert WAV to MP3 using ffmpeg."""
    mp3_path = wav_path.replace('.wav', '.mp3')
    import subprocess
    r = subprocess.run([
        'ffmpeg', '-y', '-i', wav_path,
        '-c:a', 'libmp3lame', '-b:a', '192k', '-ar', '44100',
        mp3_path
    ], capture_output=True, text=True, timeout=60)
    if r.returncode == 0:
        os.remove(wav_path)  # Remove WAV to save space
        size_mb = os.path.getsize(mp3_path) / (1024 * 1024)
        print(f"    MP3: {os.path.basename(mp3_path)} ({size_mb:.1f}MB)")
        return mp3_path
    return wav_path


if __name__ == '__main__':
    moods = ['epic', 'calm', 'tense', 'upbeat', 'mysterious', 'action']
    durations = {
        'short': 30,    # For YouTube Shorts
        'medium': 60,   # For longer shorts / intros
        'long': 180,    # For full episodes
    }

    print("=" * 60)
    print("  HIVE MUSIC GENERATOR")
    print(f"  {len(moods)} moods × {len(durations)} lengths = {len(moods) * len(durations)} tracks")
    print("=" * 60)

    # Generate 60-second versions of each mood first (most useful)
    ok = 0
    for mood in moods:
        for length_name, dur in [('medium', 60)]:  # Start with 60s
            out_path = os.path.join(OUTPUT_DIR, f"music_{mood}_{length_name}.wav")
            try:
                generate_track(mood, dur, out_path)
                convert_to_mp3(out_path)
                ok += 1
            except Exception as e:
                print(f"    FAIL: {e}")

    # Also generate 30-second versions for shorts
    for mood in moods:
        out_path = os.path.join(OUTPUT_DIR, f"music_{mood}_short.wav")
        try:
            generate_track(mood, 30, out_path)
            convert_to_mp3(out_path)
            ok += 1
        except Exception as e:
            print(f"    FAIL: {e}")

    print(f"\n{'=' * 60}")
    print(f"  DONE: {ok} tracks generated")
    print(f"  Output: {OUTPUT_DIR}")
    print(f"{'=' * 60}")
