#!/usr/bin/env python3
"""
HIVE FAILOVER — Automatic Service Redistribution
=================================================
Every machine runs this. It monitors all other machines.
If a machine dies, this picks up its critical services locally.

How it works:
1. Every 30s, ping all machines
2. If a machine is unreachable for 3 consecutive checks (90s), declare it dead
3. Check the service manifest — which services from the dead machine should we take over?
4. Start those services locally
5. When the dead machine comes back, hand services back

Port: 8899
"""

import asyncio
import json
import os
import signal
import socket
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import JSONResponse
import uvicorn
import httpx

# ── Identity ──────────────────────────────────────────────────────
HOSTNAME = socket.gethostname()
MY_IP = {
    "ZeroDESK": "100.77.113.48",
    "ZeroZI": "100.105.160.106",
    "ZeroNovo": "100.103.183.91",
}.get(HOSTNAME, "127.0.0.1")

# Also handle hostname variations
if "zero-desk" in HOSTNAME.lower() or "zerodesk" in HOSTNAME.lower():
    MY_IP = "100.77.113.48"
    HOSTNAME = "ZeroDESK"
elif "zi" in HOSTNAME.lower():
    MY_IP = "100.105.160.106"
    HOSTNAME = "ZeroZI"
elif "novo" in HOSTNAME.lower():
    MY_IP = "100.103.183.91"
    HOSTNAME = "ZeroNovo"

PORT = 8899
CHECK_INTERVAL = 30  # seconds between health checks
DEAD_THRESHOLD = 3   # consecutive failures before declaring dead
LOG_FILE = Path.home() / "failover.log"

# ── Machine Registry ──────────────────────────────────────────────
MACHINES = {
    "ZeroDESK": {
        "ip": "100.77.113.48",
        "check_port": 11434,  # Ollama
        "role": "claude_hub",
    },
    "ZeroZI": {
        "ip": "100.105.160.106",
        "check_port": 8200,  # Nerve
        "role": "primary_inference",
    },
    "ZeroNovo": {
        "ip": "100.103.183.91",
        "check_port": 8200,  # Nerve backup
        "role": "cpu_worker",
    },
}

# ── Service Manifest ──────────────────────────────────────────────
# Defines where services run and their failover targets
# priority: lower = try first
SERVICES = {
    "nerve": {
        "script": "hive_nerve_v2.py",
        "port": 8200,
        "primary": "ZeroZI",
        "failover": ["ZeroNovo", "ZeroDESK"],
        "critical": True,
    },
    "phone_webhook": {
        "script": "phone_webhook_v2.py",
        "port": 8110,
        "primary": "ZeroZI",
        "failover": ["ZeroNovo"],
        "critical": True,
    },
    "director": {
        "script": "hive_agents/interactive_call.py",
        "args": ["--serve", "--port", "8098"],
        "port": 8098,
        "primary": "ZeroZI",
        "failover": ["ZeroDESK"],
        "critical": True,
        "env": {
        },
    },
    "model_router": {
        "script": "hive_model_router.py",
        "port": 8878,
        "primary": "ZeroZI",
        "failover": ["ZeroDESK"],
        "critical": True,
    },
    "dispatch": {
        "script": "dispatch_admin.py",
        "port": 8141,
        "primary": "ZeroNovo",
        "failover": ["ZeroZI"],
        "critical": False,
    },
}

# ── State ─────────────────────────────────────────────────────────
machine_status = {}  # machine -> {"alive": bool, "fail_count": int, "last_seen": float}
takeover_services = {}  # service_name -> {"pid": int, "started": float}
app = FastAPI(title=f"Hive Failover ({HOSTNAME})")


def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
        # Trim log
        if LOG_FILE.stat().st_size > 1_000_000:
            lines = LOG_FILE.read_text().splitlines()[-500:]
            LOG_FILE.write_text("\n".join(lines) + "\n")
    except Exception:
        pass


async def check_machine(name, info):
    """Check if a machine is reachable."""
    ip = info["ip"]
    port = info["check_port"]
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(f"http://{ip}:{port}/health")
            return resp.status_code == 200
    except Exception:
        # Fallback: TCP connect
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(3)
            result = sock.connect_ex((ip, port))
            sock.close()
            return result == 0
        except Exception:
            return False


def start_service(name, svc):
    """Start a service locally as failover."""
    script = svc["script"]
    port = svc["port"]

    # Check if port is already in use locally
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex(("127.0.0.1", port))
        sock.close()
        if result == 0:
            log(f"  Port {port} already in use, {name} may already be running")
            return None
    except Exception:
        pass

    # Find the script
    search_paths = [
        Path.home() / script,
        Path.home() / "hive" / "core" / script,
        Path.home() / "hive" / "services" / script,
        Path.home() / "hive" / "agents" / script,
        Path("/tmp") / script,
    ]
    script_path = None
    for p in search_paths:
        if p.exists():
            script_path = p
            break

    if not script_path:
        log(f"  ERROR: Cannot find {script} for {name}")
        return None

    env = os.environ.copy()
    if "env" in svc:
        env.update(svc["env"])

    args = ["python3", str(script_path)] + svc.get("args", [])
    log_path = Path.home() / f"failover_{name}.log"

    try:
        with open(log_path, "a") as lf:
            proc = subprocess.Popen(
                args,
                stdout=lf,
                stderr=lf,
                env=env,
                start_new_session=True,
            )
        log(f"  STARTED {name} (PID {proc.pid}) on port {port}")
        return proc.pid
    except Exception as e:
        log(f"  ERROR starting {name}: {e}")
        return None


def stop_service(name):
    """Stop a failover service we started."""
    if name in takeover_services:
        pid = takeover_services[name]["pid"]
        try:
            os.kill(pid, signal.SIGTERM)
            log(f"  STOPPED failover {name} (PID {pid})")
        except ProcessLookupError:
            pass
        del takeover_services[name]


async def failover_loop():
    """Main monitoring loop."""
    log(f"Failover started on {HOSTNAME} ({MY_IP})")

    # Initialize status for all OTHER machines
    for name, info in MACHINES.items():
        if info["ip"] != MY_IP:
            machine_status[name] = {
                "alive": True,
                "fail_count": 0,
                "last_seen": time.time(),
            }

    while True:
        try:
            for name, info in MACHINES.items():
                if info["ip"] == MY_IP:
                    continue  # Don't check ourselves

                alive = await check_machine(name, info)
                status = machine_status[name]

                if alive:
                    was_dead = not status["alive"]
                    status["alive"] = True
                    status["fail_count"] = 0
                    status["last_seen"] = time.time()

                    if was_dead:
                        log(f"RECOVERED: {name} is back online!")
                        # Hand back services
                        for svc_name, svc in SERVICES.items():
                            if svc["primary"] == name and svc_name in takeover_services:
                                log(f"  Handing back {svc_name} to {name}")
                                stop_service(svc_name)
                else:
                    status["fail_count"] += 1
                    if status["fail_count"] >= DEAD_THRESHOLD and status["alive"]:
                        status["alive"] = False
                        log(f"DEAD: {name} unreachable for {status['fail_count']} checks")

                        # Take over its critical services if we're in the failover list
                        for svc_name, svc in SERVICES.items():
                            if svc["primary"] == name and HOSTNAME in svc.get("failover", []):
                                # Am I the highest priority failover?
                                failover_list = svc["failover"]
                                my_priority = failover_list.index(HOSTNAME) if HOSTNAME in failover_list else 999

                                # Only take over if I'm first in the failover list
                                # OR if higher-priority machines are also dead
                                should_take = my_priority == 0
                                if not should_take:
                                    higher = failover_list[:my_priority]
                                    all_higher_dead = all(
                                        not machine_status.get(h, {}).get("alive", True)
                                        for h in higher
                                    )
                                    should_take = all_higher_dead

                                if should_take and svc_name not in takeover_services:
                                    log(f"  TAKING OVER {svc_name} from {name}")
                                    pid = start_service(svc_name, svc)
                                    if pid:
                                        takeover_services[svc_name] = {
                                            "pid": pid,
                                            "started": time.time(),
                                            "from": name,
                                        }
        except Exception as e:
            log(f"ERROR in failover loop: {e}")

        await asyncio.sleep(CHECK_INTERVAL)


# ── API ───────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {
        "status": "alive",
        "service": "hive-failover",
        "hostname": HOSTNAME,
        "ip": MY_IP,
        "monitoring": len(machine_status),
        "takeovers": len(takeover_services),
    }


@app.get("/api/status")
async def status():
    return {
        "hostname": HOSTNAME,
        "machines": {
            name: {
                "alive": s["alive"],
                "fail_count": s["fail_count"],
                "last_seen": datetime.fromtimestamp(s["last_seen"]).isoformat(),
                "age_seconds": int(time.time() - s["last_seen"]),
            }
            for name, s in machine_status.items()
        },
        "takeover_services": {
            name: {
                "pid": t["pid"],
                "from": t["from"],
                "started": datetime.fromtimestamp(t["started"]).isoformat(),
                "uptime_seconds": int(time.time() - t["started"]),
            }
            for name, t in takeover_services.items()
        },
        "service_manifest": {
            name: {
                "primary": s["primary"],
                "failover": s["failover"],
                "port": s["port"],
                "critical": s["critical"],
            }
            for name, s in SERVICES.items()
        },
    }


@app.on_event("startup")
async def startup():
    asyncio.create_task(failover_loop())


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
