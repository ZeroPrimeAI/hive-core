#!/usr/bin/env python3
"""Locksmith Call Webhook — answers calls + notifies Chris via SMS"""
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse
import urllib.request, urllib.parse, base64, json, time, os

PORT = 8110
TWILIO_SID = os.environ.get("TWILIO_ACCOUNT_SID", "")
TWILIO_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN", "")
CHRIS_PHONE = "+18509648866"

def notify_chris(caller, business="Locksmith"):
    """Send SMS to Chris immediately when a call comes in"""
    try:
        creds = base64.b64encode(f"{TWILIO_SID}:{TWILIO_TOKEN}".encode()).decode()
        data = urllib.parse.urlencode({
            "To": CHRIS_PHONE,
            "From": "+18508016662",
            "Body": f"NEW CALL from {caller} on {business} line! Call back ASAP."
        }).encode()
        req = urllib.request.Request(
            f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Messages.json",
            data=data, headers={"Authorization": f"Basic {creds}"})
        urllib.request.urlopen(req, timeout=10)
    except: pass

class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode()
        params = parse_qs(body)
        caller = params.get("From", ["unknown"])[0]
        called = params.get("To", ["unknown"])[0]
        
        # Log the call
        with open("/home/zero/logs/locksmith_calls.log", "a") as f:
            f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} CALL from {caller} to {called}\n")
        
        # NOTIFY CHRIS IMMEDIATELY
        notify_chris(caller, f"Line {called}")
        
        # Answer with professional locksmith greeting
        twiml = f"""<?xml version="1.0"?>
<Response>
  <Say voice="Polly.Matthew-Neural">Thank you for calling. We are available 24 7 for all your locksmith needs. Please leave a detailed message with your name, number, and what you need help with. We will call you right back.</Say>
  <Record maxLength="120" action="/recording-complete" transcribe="true" transcribeCallback="/transcription"/>
  <Say voice="Polly.Matthew-Neural">Thank you. We will call you back shortly.</Say>
</Response>"""
        
        self.send_response(200)
        self.send_header("Content-Type", "text/xml")
        self.end_headers()
        self.wfile.write(twiml.encode())
    
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status":"locksmith_webhook","notifications":"sms_to_chris"}')
    
    def log_message(self, *a): pass

print(f"Locksmith webhook on {PORT} — SMS notifications to Chris")
HTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
