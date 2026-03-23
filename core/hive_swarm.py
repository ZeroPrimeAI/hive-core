#!/usr/bin/env python3
"""
HiveSwarm — Multi-Agent Prediction & Simulation Engine
PROPRIETARY — Hive Dynamics AI | All Rights Reserved
CONFIDENTIAL — Do not distribute

Better than MiroFish because:
- Uses our 23+ REAL specialist models as agent personalities
- Integrates with nerve.db (37K+ facts) as knowledge graph
- Tailored prediction domains: forex, leads, SEO, customer behavior
- Debate protocol with consensus detection
- Runs on our hardware (Ollama/vLLM on ZeroZI, ZeroDESK, cloud brains)

Architecture:
1. Scenario → spawn N agents with specialist personas
2. Agents analyze independently (parallel inference)
3. Debate rounds: agents see others' views, argue, adjust
4. Consensus detection → prediction report
5. Store results, track accuracy over time

Port: 8750
"""

import asyncio
import json
import time
import sqlite3
import hashlib
import os
import re
import random
from datetime import datetime, timedelta
from typing import Optional
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

# ============================================================
# CONFIGURATION
# ============================================================
PORT = 8750
DB_PATH = "/tmp/hive_swarm.db"

# Inference endpoints (try in order)
INFERENCE_ENDPOINTS = [
    {"name": "ZeroZI-vLLM", "url": "http://100.105.160.106:8000/v1/chat/completions", "type": "openai"},
    {"name": "ZeroZI-Ollama", "url": "http://100.105.160.106:11434/api/chat", "type": "ollama"},
    {"name": "ZeroDESK-Ollama", "url": "http://localhost:11434/api/chat", "type": "ollama"},
    {"name": "Cloud-Reasoning", "url": "http://100.70.226.103:11437/api/chat", "type": "ollama"},
]

# Nerve API (knowledge graph)
NERVE_URL = "http://100.70.226.103:8200"

# ============================================================
# AGENT PERSONAS — Each backed by a specialist model
# ============================================================
AGENT_PERSONAS = {
    "analyst": {
        "name": "The Analyst",
        "model": "gemma2:2b",
        "specialty": "data analysis, pattern recognition, statistical reasoning",
        "personality": "Methodical, evidence-driven, skeptical of claims without data. Looks for hidden correlations.",
        "bias": "conservative",
    },
    "trader": {
        "name": "The Trader",
        "model": "gemma2-forex",
        "specialty": "forex trading, market psychology, technical analysis, risk assessment",
        "personality": "Aggressive, reads market sentiment, trusts charts over fundamentals. Quick decisions.",
        "bias": "action-oriented",
    },
    "strategist": {
        "name": "The Strategist",
        "model": "gemma2-hive",
        "specialty": "long-term planning, system architecture, resource optimization",
        "personality": "Thinks in systems and dependencies. Sees second-order effects others miss.",
        "bias": "long-term",
    },
    "sales": {
        "name": "The Closer",
        "model": "gemma2-sales",
        "specialty": "sales psychology, customer behavior, conversion optimization, revenue strategy",
        "personality": "Energetic, persuasive, always thinking about revenue. Sees opportunity everywhere.",
        "bias": "optimistic",
    },
    "skeptic": {
        "name": "The Skeptic",
        "model": "gemma2:2b",
        "specialty": "risk analysis, failure modes, contrarian thinking, stress testing",
        "personality": "Devil's advocate. Finds the flaw in every plan. Assumes things will go wrong.",
        "bias": "pessimistic",
    },
    "engineer": {
        "name": "The Engineer",
        "model": "gemma2-coding",
        "specialty": "technical feasibility, implementation details, system design, debugging",
        "personality": "Practical, pragmatic. Cares about what actually works, not what sounds good.",
        "bias": "pragmatic",
    },
    "marketer": {
        "name": "The Marketer",
        "model": "gemma2-seo",
        "specialty": "SEO, content strategy, brand positioning, audience psychology",
        "personality": "Creative, trend-aware, obsessed with attention and engagement metrics.",
        "bias": "growth-focused",
    },
    "historian": {
        "name": "The Historian",
        "model": "gemma2:2b",
        "specialty": "pattern matching with past events, institutional memory, precedent analysis",
        "personality": "References past successes and failures. Warns when patterns repeat.",
        "bias": "experiential",
    },
    "innovator": {
        "name": "The Innovator",
        "model": "gemma2:2b",
        "specialty": "creative solutions, unconventional approaches, blue-sky thinking",
        "personality": "Wild ideas, connects dots others don't see. Sometimes brilliant, sometimes crazy.",
        "bias": "disruptive",
    },
    "operator": {
        "name": "The Operator",
        "model": "gemma2-dispatcher",
        "specialty": "operations, logistics, scheduling, resource allocation, process optimization",
        "personality": "Detail-oriented, execution-focused. Makes things actually happen on time.",
        "bias": "efficiency",
    },
}

# Prediction domains and their relevant agents
DOMAIN_AGENTS = {
    "forex": ["trader", "analyst", "skeptic", "strategist", "historian"],
    "leads": ["sales", "marketer", "analyst", "skeptic", "operator"],
    "seo": ["marketer", "engineer", "analyst", "strategist", "innovator"],
    "revenue": ["sales", "trader", "strategist", "skeptic", "marketer"],
    "technical": ["engineer", "analyst", "strategist", "skeptic", "innovator"],
    "general": ["analyst", "strategist", "skeptic", "sales", "engineer"],
    "customer": ["sales", "marketer", "operator", "analyst", "historian"],
    "growth": ["strategist", "innovator", "sales", "marketer", "engineer"],
}

# ============================================================
# DATABASE
# ============================================================
def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS simulations (
            id TEXT PRIMARY KEY,
            scenario TEXT NOT NULL,
            domain TEXT DEFAULT 'general',
            status TEXT DEFAULT 'pending',
            agent_count INTEGER DEFAULT 5,
            debate_rounds INTEGER DEFAULT 3,
            predictions TEXT,
            consensus_score REAL,
            final_report TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            completed_at TEXT,
            actual_outcome TEXT,
            accuracy_score REAL
        );
        CREATE TABLE IF NOT EXISTS agent_responses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            simulation_id TEXT NOT NULL,
            agent_id TEXT NOT NULL,
            agent_name TEXT NOT NULL,
            round INTEGER NOT NULL,
            response TEXT NOT NULL,
            prediction TEXT,
            confidence REAL,
            changed_mind INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (simulation_id) REFERENCES simulations(id)
        );
        CREATE TABLE IF NOT EXISTS prediction_accuracy (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            simulation_id TEXT NOT NULL,
            domain TEXT,
            predicted TEXT,
            actual TEXT,
            accuracy REAL,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_sim_status ON simulations(status);
        CREATE INDEX IF NOT EXISTS idx_responses_sim ON agent_responses(simulation_id);
    """)
    conn.close()

# ============================================================
# INFERENCE ENGINE
# ============================================================
async def infer(prompt: str, system: str = "", model: str = "gemma2:2b", max_tokens: int = 500) -> str:
    """Try each inference endpoint until one works."""
    async with httpx.AsyncClient(timeout=60.0) as client:
        for endpoint in INFERENCE_ENDPOINTS:
            try:
                if endpoint["type"] == "openai":
                    # vLLM OpenAI-compatible
                    messages = []
                    if system:
                        messages.append({"role": "system", "content": system})
                    messages.append({"role": "user", "content": prompt})

                    r = await client.post(endpoint["url"], json={
                        "model": model,
                        "messages": messages,
                        "max_tokens": max_tokens,
                        "temperature": 0.7,
                    })
                    if r.status_code == 200:
                        data = r.json()
                        return data["choices"][0]["message"]["content"]

                elif endpoint["type"] == "ollama":
                    messages = []
                    if system:
                        messages.append({"role": "system", "content": system})
                    messages.append({"role": "user", "content": prompt})

                    r = await client.post(endpoint["url"], json={
                        "model": model,
                        "messages": messages,
                        "stream": False,
                        "options": {"num_predict": max_tokens, "temperature": 0.7},
                    })
                    if r.status_code == 200:
                        data = r.json()
                        return data.get("message", {}).get("content", "")

            except Exception:
                continue

    return "[Inference unavailable — all endpoints failed]"


async def get_nerve_context(query: str) -> str:
    """Pull relevant knowledge from nerve.db."""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(f"{NERVE_URL}/api/search", params={"q": query, "limit": 5})
            if r.status_code == 200:
                facts = r.json()
                if facts:
                    return "KNOWN FACTS:\n" + "\n".join(f"- {f.get('fact', f.get('content', str(f)))}" for f in facts[:5])
    except Exception:
        pass
    return ""


# ============================================================
# SIMULATION ENGINE
# ============================================================
async def run_agent_round(sim_id: str, agent_id: str, persona: dict, scenario: str,
                          round_num: int, context: str, previous_responses: list) -> dict:
    """Run one agent's analysis for one round."""

    system_prompt = (
        f"You are {persona['name']}, a specialist in {persona['specialty']}. "
        f"Personality: {persona['personality']}. Bias: {persona['bias']}. "
        f"You are part of a prediction swarm analyzing a scenario. "
        f"Be concise (3-5 sentences). End with a clear PREDICTION and CONFIDENCE (0-100%)."
    )

    if round_num == 0:
        # First round: independent analysis
        prompt = f"SCENARIO: {scenario}\n\n{context}\n\nAnalyze this scenario from your perspective. What will happen? Give a clear prediction with confidence level."
    else:
        # Debate rounds: see others' views
        others = "\n".join(f"- {r['name']} ({r['bias']}): {r['response']}" for r in previous_responses if r['agent_id'] != agent_id)
        prompt = (
            f"SCENARIO: {scenario}\n\n{context}\n\n"
            f"OTHER AGENTS' VIEWS (Round {round_num}):\n{others}\n\n"
            f"Consider their arguments. Do you agree or disagree? Update your prediction if warranted. "
            f"If you changed your mind, explain why. Give your updated PREDICTION and CONFIDENCE (0-100%)."
        )

    response = await infer(prompt, system_prompt, persona["model"])

    # Extract confidence
    confidence = 50.0
    conf_match = re.search(r'(\d+)\s*%', response)
    if conf_match:
        confidence = min(100, max(0, float(conf_match.group(1))))

    # Detect if mind changed
    changed = 0
    if round_num > 0:
        change_words = ["changed", "revised", "updated", "reconsidered", "agree with", "convinced"]
        if any(w in response.lower() for w in change_words):
            changed = 1

    # Store response
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT INTO agent_responses (simulation_id, agent_id, agent_name, round, response, confidence, changed_mind) VALUES (?,?,?,?,?,?,?)",
        (sim_id, agent_id, persona["name"], round_num, response, confidence, changed)
    )
    conn.commit()
    conn.close()

    return {
        "agent_id": agent_id,
        "name": persona["name"],
        "bias": persona["bias"],
        "round": round_num,
        "response": response,
        "confidence": confidence,
        "changed_mind": changed,
    }


async def run_simulation(sim_id: str, scenario: str, domain: str, agent_count: int, debate_rounds: int):
    """Run a full multi-agent simulation."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("UPDATE simulations SET status='running' WHERE id=?", (sim_id,))
    conn.commit()
    conn.close()

    try:
        # Select agents for this domain
        agent_ids = DOMAIN_AGENTS.get(domain, DOMAIN_AGENTS["general"])[:agent_count]
        agents = {aid: AGENT_PERSONAS[aid] for aid in agent_ids if aid in AGENT_PERSONAS}

        # Get knowledge context
        nerve_context = await get_nerve_context(scenario)

        all_responses = []

        # Run debate rounds
        for round_num in range(debate_rounds):
            round_responses = []

            # Run all agents in parallel for this round
            tasks = []
            for agent_id, persona in agents.items():
                tasks.append(run_agent_round(
                    sim_id, agent_id, persona, scenario,
                    round_num, nerve_context, all_responses
                ))

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for r in results:
                if isinstance(r, dict):
                    round_responses.append(r)
                    all_responses.append(r)

        # Generate consensus report
        final_responses = [r for r in all_responses if r["round"] == debate_rounds - 1]

        # Calculate consensus
        confidences = [r["confidence"] for r in final_responses]
        avg_confidence = sum(confidences) / len(confidences) if confidences else 50

        # Count mind changes
        total_changes = sum(1 for r in all_responses if r["changed_mind"])

        # Generate final report
        report_prompt = (
            f"SCENARIO: {scenario}\n\n"
            f"DOMAIN: {domain}\n\n"
            f"AGENT FINAL POSITIONS ({len(final_responses)} agents, {debate_rounds} debate rounds):\n"
        )
        for r in final_responses:
            report_prompt += f"\n{r['name']} ({r['bias']}, {r['confidence']}% confident):\n{r['response']}\n"

        report_prompt += (
            f"\nSTATISTICS:\n"
            f"- Average confidence: {avg_confidence:.1f}%\n"
            f"- Mind changes during debate: {total_changes}\n"
            f"- Agents who agreed: {sum(1 for c in confidences if c > 60)}/{len(confidences)}\n\n"
            f"Synthesize all agent perspectives into a clear, actionable prediction report. "
            f"Include: 1) Consensus prediction, 2) Key agreements, 3) Key disagreements, "
            f"4) Confidence level, 5) Recommended actions, 6) Risk factors."
        )

        report = await infer(
            report_prompt,
            "You are the HiveSwarm Report Generator. Synthesize multi-agent debate into clear predictions.",
            "gemma2:2b",
            800
        )

        # Store results
        predictions = json.dumps([{
            "agent": r["name"],
            "prediction": r["response"][:200],
            "confidence": r["confidence"]
        } for r in final_responses])

        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            "UPDATE simulations SET status='completed', predictions=?, consensus_score=?, final_report=?, completed_at=datetime('now') WHERE id=?",
            (predictions, avg_confidence, report, sim_id)
        )
        conn.commit()
        conn.close()

    except Exception as e:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("UPDATE simulations SET status='error', final_report=? WHERE id=?", (str(e), sim_id))
        conn.commit()
        conn.close()


# ============================================================
# API
# ============================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield

app = FastAPI(title="HiveSwarm", version="1.0.0", lifespan=lifespan)


class SimulationRequest(BaseModel):
    scenario: str
    domain: str = "general"
    agent_count: int = 5
    debate_rounds: int = 3


class OutcomeReport(BaseModel):
    simulation_id: str
    actual_outcome: str


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    sims = conn.execute("SELECT * FROM simulations ORDER BY created_at DESC LIMIT 20").fetchall()

    stats = conn.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) as completed,
            AVG(consensus_score) as avg_confidence,
            COUNT(DISTINCT domain) as domains
        FROM simulations
    """).fetchone()
    conn.close()

    sim_rows = ""
    for s in sims:
        status_color = {"completed": "#27ae60", "running": "#f39c12", "error": "#e74c3c", "pending": "#95a5a6"}.get(s["status"], "#95a5a6")
        sim_rows += f"""<tr>
<td><code>{s['id'][:8]}</code></td>
<td>{s['scenario'][:80]}...</td>
<td><span style="color:{status_color};font-weight:700;">{s['status']}</span></td>
<td>{s['domain']}</td>
<td>{s['consensus_score']:.0f}%</td>
<td>{s['created_at']}</td>
<td><a href="/simulation/{s['id']}">View</a></td>
</tr>""" if s["consensus_score"] else f"""<tr>
<td><code>{s['id'][:8]}</code></td>
<td>{s['scenario'][:80]}...</td>
<td><span style="color:{status_color};font-weight:700;">{s['status']}</span></td>
<td>{s['domain']}</td>
<td>—</td>
<td>{s['created_at']}</td>
<td><a href="/simulation/{s['id']}">View</a></td>
</tr>"""

    return f"""<!DOCTYPE html><html><head>
<title>HiveSwarm — Multi-Agent Prediction Engine</title>
<style>
body {{ font-family: system-ui; background: #0a0a1a; color: #e8e8e8; margin: 0; padding: 20px; }}
h1 {{ color: #6c5ce7; }} h2 {{ color: #a29bfe; }}
.stats {{ display: flex; gap: 20px; margin: 20px 0; }}
.stat {{ background: rgba(255,255,255,0.05); padding: 20px; border-radius: 12px; flex: 1; text-align: center; }}
.stat h3 {{ color: #6c5ce7; font-size: 2rem; margin: 0; }} .stat p {{ color: #aaa; margin: 5px 0 0; }}
table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
th, td {{ padding: 10px; text-align: left; border-bottom: 1px solid rgba(255,255,255,0.1); }}
th {{ color: #6c5ce7; }} a {{ color: #a29bfe; }}
form {{ background: rgba(255,255,255,0.05); padding: 20px; border-radius: 12px; margin: 20px 0; }}
textarea, select, input {{ background: rgba(255,255,255,0.1); color: white; border: 1px solid rgba(255,255,255,0.2); padding: 10px; border-radius: 6px; width: 100%; margin: 5px 0 15px; }}
button {{ background: #6c5ce7; color: white; padding: 12px 24px; border: none; border-radius: 8px; font-weight: 700; cursor: pointer; font-size: 1rem; }}
button:hover {{ background: #a29bfe; }}
.agents {{ display: flex; flex-wrap: wrap; gap: 10px; margin: 15px 0; }}
.agent {{ background: rgba(108,92,231,0.15); padding: 8px 14px; border-radius: 8px; font-size: 0.85rem; }}
</style></head><body>
<h1>🐝 HiveSwarm — Multi-Agent Prediction Engine</h1>
<p>Spawn AI agents backed by specialist models. They debate, argue, and predict.</p>

<div class="stats">
<div class="stat"><h3>{stats['total'] or 0}</h3><p>Simulations</p></div>
<div class="stat"><h3>{stats['completed'] or 0}</h3><p>Completed</p></div>
<div class="stat"><h3>{stats['avg_confidence'] or 0:.0f}%</h3><p>Avg Confidence</p></div>
<div class="stat"><h3>{stats['domains'] or 0}</h3><p>Domains</p></div>
</div>

<h2>New Simulation</h2>
<form action="/api/simulate" method="POST">
<label>Scenario / Question:</label>
<textarea name="scenario" rows="3" placeholder="What will happen to USD/JPY if the Fed raises rates by 25bp next week?"></textarea>
<label>Domain:</label>
<select name="domain">
<option value="general">General</option>
<option value="forex">Forex / Trading</option>
<option value="leads">Lead Generation</option>
<option value="seo">SEO / Marketing</option>
<option value="revenue">Revenue / Sales</option>
<option value="technical">Technical / Engineering</option>
<option value="customer">Customer Behavior</option>
<option value="growth">Growth Strategy</option>
</select>
<div style="display:flex;gap:15px;">
<div style="flex:1;"><label>Agents:</label><input type="number" name="agent_count" value="5" min="2" max="10"></div>
<div style="flex:1;"><label>Debate Rounds:</label><input type="number" name="debate_rounds" value="3" min="1" max="5"></div>
</div>
<button type="submit">🚀 Launch Simulation</button>
</form>

<h2>Available Agents</h2>
<div class="agents">
{"".join(f'<div class="agent"><strong>{p["name"]}</strong> ({aid}) — {p["specialty"][:50]}</div>' for aid, p in AGENT_PERSONAS.items())}
</div>

<h2>Recent Simulations</h2>
<table>
<tr><th>ID</th><th>Scenario</th><th>Status</th><th>Domain</th><th>Confidence</th><th>Created</th><th>Details</th></tr>
{sim_rows}
</table>

<h2>API</h2>
<pre style="background:rgba(255,255,255,0.05);padding:15px;border-radius:8px;overflow-x:auto;">
POST /api/simulate  — Start a simulation
GET  /api/status/ID — Check simulation status
GET  /simulation/ID — View full report
POST /api/outcome   — Report actual outcome (for accuracy tracking)
GET  /api/agents    — List available agents
GET  /api/accuracy  — Prediction accuracy stats
GET  /health        — Health check
</pre>
</body></html>"""


@app.post("/api/simulate")
async def start_simulation(req: SimulationRequest, background_tasks: BackgroundTasks):
    sim_id = hashlib.sha256(f"{req.scenario}{time.time()}".encode()).hexdigest()[:16]

    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT INTO simulations (id, scenario, domain, agent_count, debate_rounds) VALUES (?,?,?,?,?)",
        (sim_id, req.scenario, req.domain, req.agent_count, req.debate_rounds)
    )
    conn.commit()
    conn.close()

    background_tasks.add_task(run_simulation, sim_id, req.scenario, req.domain, req.agent_count, req.debate_rounds)

    return {"simulation_id": sim_id, "status": "started", "agents": req.agent_count, "rounds": req.debate_rounds}


@app.get("/api/status/{sim_id}")
async def get_status(sim_id: str):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    sim = conn.execute("SELECT * FROM simulations WHERE id=?", (sim_id,)).fetchone()
    if not sim:
        raise HTTPException(404, "Simulation not found")

    responses = conn.execute(
        "SELECT agent_name, round, confidence, changed_mind FROM agent_responses WHERE simulation_id=? ORDER BY round, agent_name",
        (sim_id,)
    ).fetchall()
    conn.close()

    return {
        "id": sim["id"],
        "status": sim["status"],
        "domain": sim["domain"],
        "consensus_score": sim["consensus_score"],
        "response_count": len(responses),
        "rounds_completed": max((r["round"] for r in responses), default=-1) + 1,
        "final_report": sim["final_report"],
    }


@app.get("/simulation/{sim_id}", response_class=HTMLResponse)
async def view_simulation(sim_id: str):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    sim = conn.execute("SELECT * FROM simulations WHERE id=?", (sim_id,)).fetchone()
    if not sim:
        return HTMLResponse("<h1>Simulation not found</h1>", 404)

    responses = conn.execute(
        "SELECT * FROM agent_responses WHERE simulation_id=? ORDER BY round, agent_name",
        (sim_id,)
    ).fetchall()
    conn.close()

    rounds_html = ""
    current_round = -1
    for r in responses:
        if r["round"] != current_round:
            current_round = r["round"]
            rounds_html += f"<h3>Round {current_round + 1}</h3>"

        changed = " 🔄 CHANGED MIND" if r["changed_mind"] else ""
        rounds_html += f"""<div style="background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.08);border-radius:8px;padding:15px;margin:10px 0;">
<strong style="color:#6c5ce7;">{r['agent_name']}</strong> — Confidence: {r['confidence']:.0f}%{changed}
<p style="color:#ccc;margin:8px 0 0;">{r['response']}</p></div>"""

    report = sim["final_report"] or "Simulation still running..."

    return f"""<!DOCTYPE html><html><head><title>Simulation {sim_id[:8]}</title>
<style>body{{font-family:system-ui;background:#0a0a1a;color:#e8e8e8;margin:0;padding:20px;max-width:900px;margin:0 auto;}}
h1{{color:#6c5ce7;}}h2{{color:#a29bfe;}}h3{{color:#fd79a8;margin-top:30px;}}
a{{color:#a29bfe;}}.report{{background:rgba(108,92,231,0.1);padding:20px;border-radius:12px;border:1px solid rgba(108,92,231,0.3);margin:20px 0;line-height:1.8;}}</style>
</head><body>
<a href="/">← Back to Dashboard</a>
<h1>Simulation: {sim_id[:8]}</h1>
<p><strong>Scenario:</strong> {sim['scenario']}</p>
<p><strong>Domain:</strong> {sim['domain']} | <strong>Status:</strong> {sim['status']} | <strong>Consensus:</strong> {sim['consensus_score'] or 0:.0f}%</p>

<h2>Final Report</h2>
<div class="report">{report}</div>

<h2>Agent Debate</h2>
{rounds_html}
</body></html>"""


@app.post("/api/outcome")
async def report_outcome(req: OutcomeReport):
    """Report actual outcome for accuracy tracking."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("UPDATE simulations SET actual_outcome=? WHERE id=?", (req.actual_outcome, req.simulation_id))
    conn.commit()
    conn.close()
    return {"status": "recorded"}


@app.get("/api/agents")
async def list_agents():
    return {aid: {"name": p["name"], "model": p["model"], "specialty": p["specialty"], "bias": p["bias"]}
            for aid, p in AGENT_PERSONAS.items()}


@app.get("/api/accuracy")
async def accuracy_stats():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    sims_with_outcomes = conn.execute(
        "SELECT * FROM simulations WHERE actual_outcome IS NOT NULL"
    ).fetchall()
    conn.close()
    return {
        "simulations_with_outcomes": len(sims_with_outcomes),
        "results": [{"id": s["id"], "domain": s["domain"], "consensus": s["consensus_score"],
                     "accuracy": s["accuracy_score"]} for s in sims_with_outcomes]
    }


@app.get("/health")
async def health():
    conn = sqlite3.connect(DB_PATH)
    total = conn.execute("SELECT COUNT(*) FROM simulations").fetchone()[0]
    conn.close()
    return {"status": "healthy", "service": "hive-swarm", "simulations": total, "agents": len(AGENT_PERSONAS)}


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    import uvicorn
    print(f"🐝 HiveSwarm — Multi-Agent Prediction Engine")
    print(f"   Port: {PORT}")
    print(f"   Agents: {len(AGENT_PERSONAS)}")
    print(f"   Domains: {list(DOMAIN_AGENTS.keys())}")
    print(f"   Dashboard: http://localhost:{PORT}")
    uvicorn.run(app, host="0.0.0.0", port=PORT)
