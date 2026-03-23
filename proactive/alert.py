#!/usr/bin/env python3
"""
HIVE ALERT MODULE — Multi-channel alerting for the Proactive Brain
==================================================================
Sends alerts via:
  1. Council API (http://localhost:8766/council/post)
  2. Telegram Bot
  3. Local log file
"""

import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

COUNCIL_URL = "http://localhost:8766/council/post"
TELEGRAM_BOT_TOKEN = "8574794345:AAH9VfvTCbzO-Xr2dIhsTmtMcGw1JgTI2Ow"
TELEGRAM_CHAT_ID = "6934187950"
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

LOG_DIR = Path("/home/zero/logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "alerts.log"

AGENT_ID = "proactive-brain"
MACHINE = "ZeroDESK"

# ---------------------------------------------------------------------------
# Logger setup
# ---------------------------------------------------------------------------

logger = logging.getLogger("hive-alerts")
logger.setLevel(logging.INFO)

if not logger.handlers:
    fh = logging.FileHandler(LOG_FILE)
    fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.addHandler(fh)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def send_alert(message: str, channel: str = "alerts", message_type: str = "alert") -> dict:
    """
    Send an alert through all available channels.

    Args:
        message: The alert text.
        channel: Council channel (alerts, general, ops, revenue, etc.).
        message_type: Council message type (alert, info, status, decision, etc.).

    Returns:
        dict with delivery results per channel.
    """
    results = {}
    ts = datetime.now(timezone.utc).isoformat()

    # 1. Log locally (always succeeds)
    logger.info(f"[{channel}] {message}")
    results["log"] = True

    # 2. Post to Council
    results["council"] = _post_council(message, channel, message_type)

    # 3. Send Telegram
    results["telegram"] = _send_telegram(message)

    return results


def send_info(message: str, channel: str = "general") -> dict:
    """Convenience wrapper for non-critical informational posts."""
    return send_alert(message, channel=channel, message_type="info")


def send_status(message: str, channel: str = "ops") -> dict:
    """Convenience wrapper for status updates."""
    return send_alert(message, channel=channel, message_type="status")


def send_decision(message: str, channel: str = "strategy") -> dict:
    """Convenience wrapper for decisions."""
    return send_alert(message, channel=channel, message_type="decision")


# ---------------------------------------------------------------------------
# Internal delivery methods
# ---------------------------------------------------------------------------

def _post_council(message: str, channel: str, message_type: str) -> bool:
    """Post a message to the Agent Council."""
    payload = {
        "agent_id": AGENT_ID,
        "agent_type": "daemon",
        "machine": MACHINE,
        "channel": channel,
        "message_type": message_type,
        "message": message,
    }
    try:
        with httpx.Client(timeout=10) as client:
            resp = client.post(COUNCIL_URL, json=payload)
            if resp.status_code == 200:
                logger.debug(f"Council OK: {resp.json()}")
                return True
            else:
                logger.warning(f"Council HTTP {resp.status_code}: {resp.text}")
                return False
    except Exception as e:
        logger.warning(f"Council unreachable: {e}")
        return False


def _send_telegram(message: str) -> bool:
    """Send a message via Telegram Bot API."""
    # Truncate to Telegram's 4096 char limit
    if len(message) > 4000:
        message = message[:3997] + "..."

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": f"[HIVE BRAIN] {message}",
        "parse_mode": "HTML",
    }
    try:
        with httpx.Client(timeout=15) as client:
            resp = client.post(TELEGRAM_API, json=payload)
            if resp.status_code == 200:
                logger.debug("Telegram sent OK")
                return True
            else:
                logger.warning(f"Telegram HTTP {resp.status_code}: {resp.text}")
                return False
    except Exception as e:
        logger.warning(f"Telegram unreachable: {e}")
        return False


# ---------------------------------------------------------------------------
# CLI test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    msg = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "Test alert from Proactive Brain"
    result = send_alert(msg)
    print(f"Alert sent: {result}")
