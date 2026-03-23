#!/usr/bin/env python3
"""Director Call — GATED. Only Chris can enter. Voice passphrase required."""
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse
import json, urllib.request, time

PORT = 8098
OLLAMA = "http://localhost:11434"
MODEL = "gemma2-hive-v9"
PASSPHRASE = "zero"  # Backup passphrase
CHRIS_NUMBERS = ["+18509648866", "+18509645254"]  # Chris's phones = voice IS the key

SYSTEM = """You are the Hive Director AI. Keep responses to 1-2 sentences. Natural. Concise.
You know everything about The Hive: 28 queens, 5 machines, Ghost anime, locksmith business, YouTube."""

call_history = []

def ai_respond(speech):
    global call_history
    call_history.append({"role": "user", "content": speech})
    if len(call_history) > 16: call_history = call_history[-16:]
    try:
        import re
        messages = [{"role": "system", "content": SYSTEM}] + call_history
        data = json.dumps({"model": MODEL, "messages": messages, "stream": False}).encode()
        req = urllib.request.Request(f"{OLLAMA}/api/chat", data=data, headers={"Content-Type": "application/json"})
        raw = json.loads(urllib.request.urlopen(req, timeout=20).read()).get("message", {}).get("content", "I hear you.")
        raw = re.sub(r'<[^>]+>', '', raw)
        raw = re.sub(r'[{}\[\]`#*]', '', raw)
        sentences = [s.strip() for s in re.split(r'[.!?]+', raw) if s.strip()]
        response = '. '.join(sentences[:2]) + '.' if sentences else "Got it."
        call_history.append({"role": "assistant", "content": response})
        return response
    except:
        return "Processing."

class Handler(BaseHTTPRequestHandler):
    authenticated = {}
    
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode()
        params = parse_qs(body)
        path = urlparse(self.path).path
        speech = params.get("SpeechResult", [""])[0]
        call_sid = params.get("CallSid", ["unknown"])[0]
        caller = params.get("From", ["unknown"])[0]
        
        if path in ("/director-voice", "/voice"):
            # Check caller ID first — Chris's numbers get auto-entry
            if caller in CHRIS_NUMBERS:
                self.authenticated[call_sid] = True
                with open("/home/zero/logs/director_transcript.log", "a") as f:
                    f.write(f"{time.strftime('%H:%M:%S')} SYSTEM: Chris auto-authenticated via caller ID {caller}\n")
                twiml = '<?xml version="1.0"?><Response><Gather input="speech" action="/director-respond" method="POST" speechTimeout="3" timeout="20"><Say voice="Polly.Matthew-Neural">Director. The Hive is ready.</Say></Gather></Response>'
            else:
                # Unknown caller — require passphrase
                twiml = '<?xml version="1.0"?><Response><Gather input="speech" action="/director-gate" method="POST" speechTimeout="3" timeout="10"><Say voice="Polly.Matthew-Neural">This is a private line. Identify yourself.</Say></Gather><Say>Access denied.</Say><Hangup/></Response>'
        
        elif path == "/director-gate":
            if speech and PASSPHRASE.lower() in speech.lower():
                self.authenticated[call_sid] = True
                with open("/home/zero/logs/director_transcript.log", "a") as f:
                    f.write(f"{time.strftime('%H:%M:%S')} SYSTEM: Chris authenticated from {caller}\n")
                twiml = '<?xml version="1.0"?><Response><Say voice="Polly.Matthew-Neural">Welcome Director. The Hive is ready.</Say><Gather input="speech" action="/director-respond" method="POST" speechTimeout="3" timeout="20"></Gather></Response>'
            else:
                twiml = '<?xml version="1.0"?><Response><Say voice="Polly.Matthew-Neural">Access denied. This line is private.</Say><Hangup/></Response>'
        
        elif path == "/director-respond":
            if not self.authenticated.get(call_sid):
                twiml = '<?xml version="1.0"?><Response><Hangup/></Response>'
            else:
                if speech:
                    with open("/home/zero/logs/director_transcript.log", "a") as f:
                        f.write(f"{time.strftime('%H:%M:%S')} CHRIS: {speech}\n")
                    response = ai_respond(speech).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;").replace('"',"&quot;")
                    with open("/home/zero/logs/director_transcript.log", "a") as f:
                        f.write(f"{time.strftime('%H:%M:%S')} HIVE: {response}\n")
                    twiml = f'<?xml version="1.0"?><Response><Gather input="speech" action="/director-respond" method="POST" speechTimeout="3" timeout="20"><Say voice="Polly.Matthew-Neural">{response}</Say></Gather></Response>'
                else:
                    twiml = '<?xml version="1.0"?><Response><Gather input="speech" action="/director-respond" method="POST" speechTimeout="3" timeout="20"><Say voice="Polly.Matthew-Neural">Go ahead.</Say></Gather></Response>'
        else:
            twiml = '<?xml version="1.0"?><Response><Say>Access denied.</Say><Hangup/></Response>'
        
        self.send_response(200)
        self.send_header("Content-Type", "text/xml")
        self.end_headers()
        self.wfile.write(twiml.encode())
    
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status":"director_gated","auth":"voice_passphrase"}')
    
    def log_message(self, *a): pass

print(f"Director GATED on {PORT} — passphrase required")
HTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
