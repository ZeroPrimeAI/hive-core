"""Simple Telegram alert sender — any agent can import this."""
import urllib.request
import json

BOT_TOKEN = "8574794345:AAH9VfvTCbzO-Xr2dIhsTmtMcGw1JgTI2Ow"
CHAT_ID = "6934187950"

def send(message: str, parse_mode: str = "HTML") -> bool:
    """Send a message to Chris on Telegram."""
    try:
        data = json.dumps({
            "chat_id": CHAT_ID,
            "text": message[:4000],
            "parse_mode": parse_mode,
        }).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data=data,
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=10)
        return True
    except Exception:
        return False

def send_alert(message: str) -> bool:
    return send(f"🚨 <b>HIVE ALERT</b>\n{message}")

def send_report(message: str) -> bool:
    return send(f"📊 <b>HIVE REPORT</b>\n{message}")

if __name__ == "__main__":
    import sys
    msg = " ".join(sys.argv[1:]) or "Test alert from the Hive"
    send_alert(msg)
    print("Sent!")
