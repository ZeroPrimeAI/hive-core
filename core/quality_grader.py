#!/usr/bin/env python3
"""
Quality Grader — Content quality gate for The Hive.

Grades produced content (episodes, shorts) BEFORE upload.
Uses ffprobe to inspect video properties and scores 0-100.

Port: 8901
DB: /home/zero/hivecode_sandbox/quality.db
Builds dir: /home/zero/hivecode_sandbox/builds/

Score thresholds:
  0-49  = REJECT (do not upload)
  50-69 = NEEDS_IMPROVEMENT (fix issues first)
  70+   = UPLOAD_READY

Scoring rules:
  Base score: 60
  Episode < 30s duration:       -20
  Episode < 1MB file size:      -30
  No audio track:               -50
  Resolution < 1080p:           -10
  Has SDXL art (>2MB):          +20
  Episode duration 60-300s:     +10
  Short > 60s (YT Shorts cap):  -30
"""

import json
import os
import sqlite3
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException
import uvicorn

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PORT = 8901
BUILDS_DIR = Path("/home/zero/hivecode_sandbox/builds/")
DB_PATH = Path("/home/zero/hivecode_sandbox/quality.db")
FFPROBE_BIN = "ffprobe"

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS grades (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            build_file  TEXT NOT NULL,
            video_path  TEXT,
            content_type TEXT,           -- 'episode' or 'short'
            score       INTEGER NOT NULL,
            verdict     TEXT NOT NULL,    -- REJECT / NEEDS_IMPROVEMENT / UPLOAD_READY
            details     TEXT,            -- JSON blob with per-rule breakdown
            duration_s  REAL,
            size_mb     REAL,
            has_audio   INTEGER,
            width       INTEGER,
            height      INTEGER,
            bitrate_kbps REAL,
            graded_at   TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_grades_build ON grades(build_file)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_grades_verdict ON grades(verdict)
    """)
    conn.commit()
    return conn


def _save_grade(conn: sqlite3.Connection, row: dict) -> int:
    cur = conn.execute("""
        INSERT INTO grades
            (build_file, video_path, content_type, score, verdict, details,
             duration_s, size_mb, has_audio, width, height, bitrate_kbps, graded_at)
        VALUES
            (:build_file, :video_path, :content_type, :score, :verdict, :details,
             :duration_s, :size_mb, :has_audio, :width, :height, :bitrate_kbps, :graded_at)
    """, row)
    conn.commit()
    return cur.lastrowid


# ---------------------------------------------------------------------------
# ffprobe helper
# ---------------------------------------------------------------------------

def ffprobe_inspect(video_path: str) -> dict:
    """Run ffprobe on a video file and return structured metadata.

    Returns dict with keys:
        duration_s, size_mb, has_audio, width, height, bitrate_kbps, error
    """
    result = {
        "duration_s": 0.0,
        "size_mb": 0.0,
        "has_audio": False,
        "width": 0,
        "height": 0,
        "bitrate_kbps": 0.0,
        "error": None,
    }

    if not os.path.isfile(video_path):
        result["error"] = f"File not found: {video_path}"
        return result

    # Get file size from OS (more reliable than ffprobe for this)
    try:
        result["size_mb"] = round(os.path.getsize(video_path) / (1024 * 1024), 2)
    except OSError:
        pass

    try:
        proc = subprocess.run(
            [
                FFPROBE_BIN,
                "-v", "quiet",
                "-print_format", "json",
                "-show_format",
                "-show_streams",
                video_path,
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if proc.returncode != 0:
            result["error"] = f"ffprobe exited with code {proc.returncode}: {proc.stderr[:300]}"
            return result

        data = json.loads(proc.stdout)
    except subprocess.TimeoutExpired:
        result["error"] = "ffprobe timed out after 30s"
        return result
    except json.JSONDecodeError as exc:
        result["error"] = f"ffprobe output not valid JSON: {exc}"
        return result
    except FileNotFoundError:
        result["error"] = "ffprobe binary not found on this system"
        return result

    # Duration from format block
    fmt = data.get("format", {})
    try:
        result["duration_s"] = round(float(fmt.get("duration", 0)), 2)
    except (ValueError, TypeError):
        pass

    # Bitrate from format block
    try:
        result["bitrate_kbps"] = round(float(fmt.get("bit_rate", 0)) / 1000, 1)
    except (ValueError, TypeError):
        pass

    # Walk streams for video and audio info
    for stream in data.get("streams", []):
        codec_type = stream.get("codec_type", "")
        if codec_type == "video":
            result["width"] = int(stream.get("width", 0))
            result["height"] = int(stream.get("height", 0))
            # Fallback duration from video stream if format didn't provide it
            if result["duration_s"] == 0.0:
                try:
                    result["duration_s"] = round(float(stream.get("duration", 0)), 2)
                except (ValueError, TypeError):
                    pass
        elif codec_type == "audio":
            result["has_audio"] = True

    return result


# ---------------------------------------------------------------------------
# Grading engine
# ---------------------------------------------------------------------------

def compute_grade(build_meta: dict, probe: dict) -> dict:
    """Score content 0-100 based on quality rules.

    Args:
        build_meta: parsed build JSON (type, episode, path, size_mb, duration_s, ...)
        probe: ffprobe_inspect() result dict

    Returns:
        dict with score, verdict, breakdown (list of rule results)
    """
    BASE_SCORE = 60
    score = BASE_SCORE
    breakdown = []
    content_type = build_meta.get("type", "episode")  # 'episode' or 'short'

    # Use ffprobe values first, fall back to build JSON metadata
    duration = probe.get("duration_s") or build_meta.get("duration_s", 0) or 0
    size_mb = probe.get("size_mb") or build_meta.get("size_mb", 0) or 0
    has_audio = probe.get("has_audio", False)
    width = probe.get("width", 0)
    height = probe.get("height", 0)

    # If ffprobe failed entirely but we have build metadata, use that
    if probe.get("error") and not probe.get("duration_s"):
        duration = build_meta.get("duration_s", 0) or 0
        size_mb = build_meta.get("size_mb", 0) or 0
        breakdown.append({
            "rule": "ffprobe_error",
            "delta": -5,
            "reason": f"ffprobe failed: {probe['error']} — using build metadata as fallback",
        })
        score -= 5

    # ---- Rule: Episode too short (< 30s) ----
    if content_type == "episode" and duration > 0 and duration < 30:
        breakdown.append({
            "rule": "episode_too_short",
            "delta": -20,
            "reason": f"Episode is only {duration:.1f}s (minimum 30s)",
        })
        score -= 20

    # ---- Rule: File too small (< 1MB — probably just text frames) ----
    if content_type == "episode" and 0 < size_mb < 1.0:
        breakdown.append({
            "rule": "file_too_small",
            "delta": -30,
            "reason": f"File is only {size_mb:.2f}MB — likely just text frames, no real video",
        })
        score -= 30

    # ---- Rule: No audio track ----
    if not has_audio:
        breakdown.append({
            "rule": "no_audio",
            "delta": -50,
            "reason": "No audio track detected — viewers will skip immediately",
        })
        score -= 50

    # ---- Rule: Resolution below 1080p ----
    if height > 0 and height < 1080:
        breakdown.append({
            "rule": "low_resolution",
            "delta": -10,
            "reason": f"Resolution {width}x{height} is below 1080p",
        })
        score -= 10

    # ---- Rule: Has SDXL art (file > 2MB indicates real rendered frames) ----
    if size_mb >= 2.0:
        breakdown.append({
            "rule": "has_sdxl_art",
            "delta": 20,
            "reason": f"File is {size_mb:.1f}MB — likely contains rendered art/frames",
        })
        score += 20

    # ---- Rule: Ideal episode duration (60-300s) ----
    if content_type == "episode" and 60 <= duration <= 300:
        breakdown.append({
            "rule": "ideal_duration",
            "delta": 10,
            "reason": f"Duration {duration:.0f}s is in the ideal 1-5 minute range",
        })
        score += 10

    # ---- Rule: Short exceeds 60s (YouTube Shorts limit) ----
    if content_type == "short" and duration > 60:
        breakdown.append({
            "rule": "short_too_long",
            "delta": -30,
            "reason": f"Short is {duration:.1f}s — YouTube Shorts must be 60s or less",
        })
        score -= 30

    # Clamp to 0-100
    score = max(0, min(100, score))

    # Determine verdict
    if score >= 70:
        verdict = "UPLOAD_READY"
    elif score >= 50:
        verdict = "NEEDS_IMPROVEMENT"
    else:
        verdict = "REJECT"

    return {
        "score": score,
        "verdict": verdict,
        "base_score": BASE_SCORE,
        "breakdown": breakdown,
    }


# ---------------------------------------------------------------------------
# Grade a single build file
# ---------------------------------------------------------------------------

def grade_build(build_filename: str) -> dict:
    """Grade a single build file. Returns the full grade record."""
    build_path = BUILDS_DIR / build_filename
    if not build_path.exists():
        raise FileNotFoundError(f"Build file not found: {build_path}")

    with open(build_path, "r") as f:
        build_meta = json.load(f)

    video_path = build_meta.get("path", "")
    content_type = build_meta.get("type", "episode")

    # Run ffprobe on the actual video file
    probe = ffprobe_inspect(video_path)

    # Compute grade
    grade = compute_grade(build_meta, probe)

    now = datetime.now(timezone.utc).isoformat()

    record = {
        "build_file": build_filename,
        "video_path": video_path,
        "content_type": content_type,
        "score": grade["score"],
        "verdict": grade["verdict"],
        "details": json.dumps({
            "base_score": grade["base_score"],
            "breakdown": grade["breakdown"],
            "ffprobe_error": probe.get("error"),
        }),
        "duration_s": probe.get("duration_s") or build_meta.get("duration_s"),
        "size_mb": probe.get("size_mb") or build_meta.get("size_mb"),
        "has_audio": 1 if probe.get("has_audio") else 0,
        "width": probe.get("width", 0),
        "height": probe.get("height", 0),
        "bitrate_kbps": probe.get("bitrate_kbps", 0),
        "graded_at": now,
    }

    # Save to DB
    conn = _db()
    try:
        row_id = _save_grade(conn, record)
        record["id"] = row_id
    finally:
        conn.close()

    # Parse details back to dict for JSON response
    record["details"] = json.loads(record["details"])
    record["has_audio"] = bool(record["has_audio"])

    return record


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Hive Quality Grader",
    description="Content quality gate — grades videos before upload",
    version="1.0.0",
)


@app.get("/health")
def health():
    """Health check endpoint."""
    builds_count = len(list(BUILDS_DIR.glob("*_build.json"))) if BUILDS_DIR.exists() else 0
    conn = _db()
    try:
        total_grades = conn.execute("SELECT COUNT(*) FROM grades").fetchone()[0]
    finally:
        conn.close()

    return {
        "status": "ok",
        "service": "hive-quality-grader",
        "port": PORT,
        "builds_dir": str(BUILDS_DIR),
        "db_path": str(DB_PATH),
        "builds_found": builds_count,
        "total_grades": total_grades,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/grade/{build_file}")
def api_grade_one(build_file: str):
    """Grade a specific build file.

    Example: GET /api/grade/ep14_build.json
    """
    try:
        record = grade_build(build_file)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid build JSON: {exc}")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Grading failed: {exc}")

    return record


@app.get("/api/grades")
def api_list_grades(
    verdict: Optional[str] = None,
    content_type: Optional[str] = None,
    limit: int = 100,
):
    """List all recorded grades.

    Optional filters:
      ?verdict=REJECT|NEEDS_IMPROVEMENT|UPLOAD_READY
      ?content_type=episode|short
      ?limit=N (default 100)
    """
    conn = _db()
    try:
        query = "SELECT * FROM grades WHERE 1=1"
        params = []

        if verdict:
            query += " AND verdict = ?"
            params.append(verdict.upper())
        if content_type:
            query += " AND content_type = ?"
            params.append(content_type.lower())

        query += " ORDER BY graded_at DESC LIMIT ?"
        params.append(limit)

        rows = conn.execute(query, params).fetchall()

        grades = []
        for row in rows:
            d = dict(row)
            # Parse details JSON
            if d.get("details"):
                try:
                    d["details"] = json.loads(d["details"])
                except (json.JSONDecodeError, TypeError):
                    pass
            d["has_audio"] = bool(d.get("has_audio"))
            grades.append(d)

        # Summary stats
        total = conn.execute("SELECT COUNT(*) FROM grades").fetchone()[0]
        ready = conn.execute(
            "SELECT COUNT(*) FROM grades WHERE verdict='UPLOAD_READY'"
        ).fetchone()[0]
        rejected = conn.execute(
            "SELECT COUNT(*) FROM grades WHERE verdict='REJECT'"
        ).fetchone()[0]
        needs_work = conn.execute(
            "SELECT COUNT(*) FROM grades WHERE verdict='NEEDS_IMPROVEMENT'"
        ).fetchone()[0]
        avg_score = conn.execute(
            "SELECT ROUND(AVG(score), 1) FROM grades"
        ).fetchone()[0]

    finally:
        conn.close()

    return {
        "grades": grades,
        "summary": {
            "total": total,
            "upload_ready": ready,
            "needs_improvement": needs_work,
            "rejected": rejected,
            "average_score": avg_score or 0,
        },
    }


@app.post("/api/grade-all")
def api_grade_all():
    """Grade all build files in the builds directory.

    Skips files that have already been graded (by build_file name)
    unless ?force=true is passed.
    """
    if not BUILDS_DIR.exists():
        raise HTTPException(status_code=404, detail=f"Builds directory not found: {BUILDS_DIR}")

    build_files = sorted(BUILDS_DIR.glob("*_build.json"))
    if not build_files:
        return {"message": "No build files found", "graded": [], "skipped": [], "errors": []}

    # Check which have already been graded
    conn = _db()
    try:
        already_graded = set()
        rows = conn.execute("SELECT DISTINCT build_file FROM grades").fetchall()
        for row in rows:
            already_graded.add(row[0])
    finally:
        conn.close()

    graded = []
    skipped = []
    errors = []

    for bf in build_files:
        fname = bf.name
        if fname in already_graded:
            skipped.append(fname)
            continue

        try:
            record = grade_build(fname)
            graded.append({
                "build_file": fname,
                "score": record["score"],
                "verdict": record["verdict"],
            })
        except Exception as exc:
            errors.append({"build_file": fname, "error": str(exc)})

    return {
        "message": f"Graded {len(graded)} builds, skipped {len(skipped)} already-graded, {len(errors)} errors",
        "graded": graded,
        "skipped": skipped,
        "errors": errors,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Ensure builds dir exists
    BUILDS_DIR.mkdir(parents=True, exist_ok=True)

    # Initialize DB on startup
    conn = _db()
    conn.close()
    print(f"[quality_grader] DB at {DB_PATH}")
    print(f"[quality_grader] Watching builds in {BUILDS_DIR}")
    print(f"[quality_grader] Starting on port {PORT}")

    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
