#!/usr/bin/env python3
"""
THE HIVE — Autonomous Operations Center
=========================================
Runs 24/7. Reports to Chris. Never crashes. Never runs out of space.

Features:
- Morning + Evening email reports (status, improvements, failures, actions needed)
- Disk space monitoring + auto-cleanup
- Mesh health checks (all machines)
- Training pipeline monitoring
- YouTube content pipeline tracking
- Revenue monitoring (when Stripe connected)
- Auto-restart dead services
- Crash prevention (disk, memory, GPU)

Runs as systemd service: hive-operations
"""

import json
import logging
import os
import smtplib
import subprocess
import sys
import time
import traceback
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path

# === CONFIG ===
CHRIS_EMAIL = os.environ.get("CHRIS_EMAIL", "")  # Set this
CHRIS_PHONE = "+18509648866"
REPORT_TIMES = [7, 21]  # 7 AM and 9 PM
HEALTH_CHECK_INTERVAL = 300  # 5 minutes
DISK_WARNING_PCT = 85
DISK_CRITICAL_PCT = 95
LOG_FILE = "/home/zero/logs/hive_operations.log"

# Machines
MACHINES = {
    "ZeroDESK": {"ip": "100.77.113.48", "local": True},
    "ZeroZI": {"ip": "100.105.160.106", "ssh": "ssh -o ConnectTimeout=5 zero@100.105.160.106"},
    "ZeroNovo": {"ip": "100.103.183.91", "ssh": "ssh -o ConnectTimeout=5 zero@100.103.183.91"},
    "ZeroQ": {"ip": "100.70.226.103", "ssh": "ssh -o ConnectTimeout=5 zero@100.70.226.103"},
}

# State file
STATE_FILE = "/home/zero/.hive_ops_state.json"

os.makedirs("/home/zero/logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [OPS] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ]
)
log = logging.getLogger("hive-ops")


def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except:
        return {"last_report_hour": -1, "reports_sent": 0, "errors": [], "achievements": []}


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2, default=str)


def run_cmd(cmd, timeout=15):
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        return r.stdout.strip()
    except:
        return "TIMEOUT"


# === HEALTH CHECKS ===

def check_disk(machine_name, ssh_prefix=""):
    """Check disk space, auto-clean if critical."""
    if ssh_prefix:
        result = run_cmd(f'{ssh_prefix} "df -h / | tail -1"')
    else:
        result = run_cmd("df -h / | tail -1")

    if not result or result == "TIMEOUT":
        return {"status": "UNREACHABLE", "usage": "?"}

    parts = result.split()
    usage_pct = int(parts[4].replace("%", "")) if len(parts) >= 5 else 0

    status = "OK"
    if usage_pct >= DISK_CRITICAL_PCT:
        status = "CRITICAL"
        # Auto-clean on local machine
        if not ssh_prefix:
            log.warning(f"Disk CRITICAL at {usage_pct}% — auto-cleaning")
            run_cmd("find /tmp -name '*.log' -mtime +3 -delete 2>/dev/null")
            run_cmd("find /tmp -name '_temp_*' -delete 2>/dev/null")
            run_cmd("find /tmp -name '*.pyc' -delete 2>/dev/null")
    elif usage_pct >= DISK_WARNING_PCT:
        status = "WARNING"

    return {"status": status, "usage": f"{usage_pct}%", "raw": result}


def check_machine(name, config):
    """Full health check for a machine."""
    result = {"name": name, "status": "UNKNOWN"}

    if config.get("local"):
        result["status"] = "UP"
        result["disk"] = check_disk(name)
        result["gpu"] = run_cmd("nvidia-smi --query-gpu=utilization.gpu,memory.used,memory.total --format=csv,noheader 2>/dev/null")
        result["load"] = run_cmd("uptime | awk -F'load average:' '{print $2}'")
        result["ram"] = run_cmd("free -m | awk 'NR==2{printf \"%dMB/%dMB (%.0f%%)\", $3,$2,$3/$2*100}'")
    else:
        ssh = config.get("ssh", "")
        # Ping first
        ping = run_cmd(f"ping -c 1 -W 3 {config['ip']} 2>/dev/null | grep -c 'bytes from'")
        if ping == "1":
            result["status"] = "UP"
            result["disk"] = check_disk(name, ssh)
            result["gpu"] = run_cmd(f'{ssh} "nvidia-smi --query-gpu=utilization.gpu,memory.used,memory.total --format=csv,noheader 2>/dev/null"')
            result["load"] = run_cmd(f'{ssh} "uptime 2>/dev/null | tail -1"')
        else:
            result["status"] = "DOWN"

    return result


def check_all_machines():
    """Check all machines in the mesh."""
    results = {}
    for name, config in MACHINES.items():
        results[name] = check_machine(name, config)
        log.info(f"  {name}: {results[name]['status']}")
    return results


def check_services():
    """Check key processes on ZeroDESK."""
    services = {}
    checks = {
        "HiveMind (Queens)": "hive_mind.py",
        "HiveSwarm": "hive_swarm.py",
        "Bridge (Telegram)": "hive_code_bridge.py",
        "Training Harvester": "hive_training_harvester.py",
        "Content Empire": "hive_content_empire.py",
        "Anime Dashboard": "hive_anime_dashboard.py",
        "Mesh Agent": "hive_mesh_agent",
    }
    for name, proc in checks.items():
        result = run_cmd(f"pgrep -f '{proc}' | wc -l")
        services[name] = "RUNNING" if result and int(result) > 0 else "DOWN"
    return services


def check_training():
    """Check training status on ZeroZI."""
    log_content = run_cmd('ssh -o ConnectTimeout=5 zero@100.105.160.106 "tail -3 /home/zero/training_v2.log 2>/dev/null || tail -3 /home/zero/training.log 2>/dev/null"')
    vllm = run_cmd('ssh -o ConnectTimeout=5 zero@100.105.160.106 "systemctl is-active hive-vllm 2>/dev/null"')
    return {"log": log_content, "vllm": vllm or "unknown"}


def check_content():
    """Count content assets."""
    return {
        "shorts_v3": len(list(Path("/tmp/youtube_shorts_v3").glob("*.mp4"))) if Path("/tmp/youtube_shorts_v3").exists() else 0,
        "shorts_v2": len(list(Path("/tmp/youtube_shorts").glob("*.mp4"))) if Path("/tmp/youtube_shorts").exists() else 0,
        "music_tracks": len(list(Path("/tmp/hive_music").glob("*.mp3"))) if Path("/tmp/hive_music").exists() else 0,
        "anime_episodes": len(list(Path("/tmp/ghost_anime_v5").glob("*.mp4"))) if Path("/tmp/ghost_anime_v5").exists() else 0,
        "anime_art": len(list(Path("/tmp/new_anime_art").glob("*.png"))) if Path("/tmp/new_anime_art").exists() else 0,
        "scripts_v2": len(list(Path("/tmp/ghost_scripts_v2").glob("*.txt"))) if Path("/tmp/ghost_scripts_v2").exists() else 0,
    }


# === REPORT GENERATION ===

def generate_report(report_type="evening"):
    """Generate a full status report."""
    now = datetime.now()
    log.info(f"Generating {report_type} report...")

    machines = check_all_machines()
    services = check_services()
    training = check_training()
    content = check_content()

    lines = []
    lines.append(f"{'='*50}")
    lines.append(f"THE HIVE — {report_type.upper()} REPORT")
    lines.append(f"{now.strftime('%Y-%m-%d %H:%M')}")
    lines.append(f"{'='*50}")

    # Mesh Status
    lines.append(f"\n--- MESH STATUS ---")
    for name, info in machines.items():
        status = info.get("status", "?")
        icon = "OK" if status == "UP" else "DOWN" if status == "DOWN" else "??"
        disk = info.get("disk", {}).get("usage", "?")
        gpu = info.get("gpu", "N/A")[:40] if info.get("gpu") else "N/A"
        lines.append(f"  [{icon}] {name}: disk {disk}, GPU: {gpu}")

    # Services
    lines.append(f"\n--- SERVICES ---")
    running = sum(1 for v in services.values() if v == "RUNNING")
    lines.append(f"  {running}/{len(services)} running")
    for name, status in services.items():
        if status != "RUNNING":
            lines.append(f"  [DOWN] {name}")

    # Training
    lines.append(f"\n--- TRAINING ---")
    lines.append(f"  vLLM: {training.get('vllm', '?')}")
    if training.get("log"):
        lines.append(f"  Latest: {training['log'][:200]}")

    # Content
    lines.append(f"\n--- CONTENT READY ---")
    for k, v in content.items():
        lines.append(f"  {k}: {v}")

    # Warnings
    lines.append(f"\n--- WARNINGS ---")
    warnings = []
    for name, info in machines.items():
        if info.get("status") == "DOWN":
            warnings.append(f"{name} is DOWN")
        disk = info.get("disk", {})
        if disk.get("status") in ("WARNING", "CRITICAL"):
            warnings.append(f"{name} disk {disk.get('status')}: {disk.get('usage')}")
    for name, status in services.items():
        if status != "RUNNING":
            warnings.append(f"Service DOWN: {name}")
    if warnings:
        for w in warnings:
            lines.append(f"  !! {w}")
    else:
        lines.append("  None — all systems healthy")

    # Actions needed
    lines.append(f"\n--- CHRIS ACTION NEEDED ---")
    lines.append("  1. YouTube: verify identity + upload shorts from /tmp/youtube_shorts_v3/")
    lines.append("  2. Gemini API key: aistudio.google.com/apikey (500 free images/day)")
    lines.append("  3. Affiliate signups: Amazon Associates, Impact.com, CJ Affiliate")
    lines.append("  4. Twilio: update webhook URL for phone system")
    lines.append("  5. TikTok: create account for content posting")

    lines.append(f"\n{'='*50}")
    lines.append("The Hive never sleeps.")
    lines.append(f"{'='*50}")

    return "\n".join(lines)


def send_email_report(report, subject="Hive Report"):
    """Send report via email. Uses Gmail SMTP or falls back to file."""
    if not CHRIS_EMAIL:
        # Save to file instead
        report_file = f"/home/zero/logs/report_{datetime.now().strftime('%Y%m%d_%H%M')}.txt"
        with open(report_file, "w") as f:
            f.write(report)
        log.info(f"No email configured. Report saved to {report_file}")
        log.info("Set CHRIS_EMAIL env var and configure SMTP to enable email reports")
        return False

    try:
        msg = MIMEMultipart()
        msg["From"] = "hive@hivedynamics.ai"
        msg["To"] = CHRIS_EMAIL
        msg["Subject"] = f"[HIVE] {subject} — {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        msg.attach(MIMEText(report, "plain"))

        # Try Gmail SMTP (needs app password)
        gmail_user = os.environ.get("GMAIL_USER", "")
        gmail_pass = os.environ.get("GMAIL_PASS", "")
        if gmail_user and gmail_pass:
            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
                server.login(gmail_user, gmail_pass)
                server.send_message(msg)
            log.info(f"Email sent to {CHRIS_EMAIL}")
            return True
    except Exception as e:
        log.error(f"Email failed: {e}")

    # Fallback: save to file
    report_file = f"/home/zero/logs/report_{datetime.now().strftime('%Y%m%d_%H%M')}.txt"
    with open(report_file, "w") as f:
        f.write(report)
    log.info(f"Email failed, report saved to {report_file}")
    return False


def send_telegram_report(report):
    """Send report via Telegram bridge."""
    try:
        import requests
        # Use the bridge to send to Chris
        requests.post("http://localhost:8200/send", json={
            "message": report[:4000],  # Telegram limit
            "target": "chris"
        }, timeout=10)
        log.info("Report sent via Telegram")
        return True
    except:
        return False


# === AUTO-CLEANUP ===

def auto_cleanup():
    """Prevent disk from filling up."""
    disk = check_disk("ZeroDESK")
    usage = int(disk.get("usage", "0%").replace("%", ""))

    if usage > 90:
        log.warning(f"Disk at {usage}% — cleaning up...")
        # Remove old temp files
        run_cmd("find /tmp -name '*.log' -mtime +7 -delete 2>/dev/null")
        run_cmd("find /tmp -name '_temp_*' -mtime +1 -delete 2>/dev/null")
        run_cmd("find /tmp -name 'frame_*.png' -delete 2>/dev/null")
        run_cmd("find /tmp/_shorts_v3_tmp -delete 2>/dev/null")
        # Remove old training outputs (keep latest)
        run_cmd("find /tmp -name '*.gguf' -mtime +14 -delete 2>/dev/null")
        log.info("Cleanup complete")
    return usage


# === MAIN LOOP ===

def main():
    log.info("=" * 50)
    log.info("HIVE OPERATIONS CENTER — STARTING")
    log.info("=" * 50)

    state = load_state()
    last_health_check = 0
    last_cleanup = 0

    while True:
        try:
            now = datetime.now()
            current_hour = now.hour

            # === SCHEDULED REPORTS (7 AM and 9 PM) ===
            if current_hour in REPORT_TIMES and state.get("last_report_hour") != current_hour:
                report_type = "morning" if current_hour < 12 else "evening"
                report = generate_report(report_type)
                send_email_report(report, f"{report_type.title()} Status Report")
                send_telegram_report(report)
                state["last_report_hour"] = current_hour
                state["reports_sent"] = state.get("reports_sent", 0) + 1
                save_state(state)

            # === HEALTH CHECKS (every 5 min) ===
            if time.time() - last_health_check > HEALTH_CHECK_INTERVAL:
                log.info("Health check...")
                machines = check_all_machines()
                services = check_services()

                # Auto-restart dead services on ZeroDESK
                for name, status in services.items():
                    if status != "RUNNING":
                        log.warning(f"Service DOWN: {name} — attempting restart")
                        # Map service names to scripts
                        restarts = {
                            "HiveMind (Queens)": "python3 /tmp/hive_mind.py",
                            "HiveSwarm": "python3 /tmp/hive_swarm.py",
                        }
                        if name in restarts:
                            run_cmd(f"nohup {restarts[name]} > /dev/null 2>&1 &")
                            log.info(f"  Restarted: {name}")

                last_health_check = time.time()

            # === AUTO-CLEANUP (every hour) ===
            if time.time() - last_cleanup > 3600:
                auto_cleanup()
                last_cleanup = time.time()

            time.sleep(60)  # Check every minute

        except KeyboardInterrupt:
            log.info("Shutting down operations center")
            break
        except Exception as e:
            log.error(f"Error in main loop: {e}")
            traceback.print_exc()
            time.sleep(60)


if __name__ == "__main__":
    if "--report" in sys.argv:
        print(generate_report("manual"))
    elif "--once" in sys.argv:
        report = generate_report("status")
        print(report)
        send_email_report(report, "Manual Status Report")
    else:
        main()
