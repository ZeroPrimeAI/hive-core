#!/usr/bin/env python3
"""
HIVE REASONING CLIENT — Universal cache-first inference wrapper
================================================================
Import this from ANY agent to get free cached answers before
hitting Ollama. Saves tokens, saves GPU, saves time.

Usage:
    from reasoning_client import ReasoningClient
    rc = ReasoningClient(domain="content")

    # Check cache before calling Ollama
    cached = rc.ask("Write a script about AI awakening")
    if cached["hit"]:
        response = cached["response"]   # FREE, instant
    else:
        response = ollama_generate(prompt)  # Costs tokens
        rc.learn(prompt, response, tokens=len(response)//4)

    # Get routing recommendation
    route = rc.route("complex analysis task")
    # route["tier"] => 1 (cache), 2 (local gemma), 3 (cloud brain)
"""

import json
import logging
from urllib.request import Request, urlopen
from urllib.error import URLError

logger = logging.getLogger("reasoning_client")

BANK_URL = "http://localhost:8910"
TIMEOUT = 5  # seconds — fast fail so inference isn't blocked


class ReasoningClient:
    """Lightweight client for the Hive Reasoning Bank (port 8910)."""

    def __init__(self, domain: str = "general", bank_url: str = BANK_URL):
        self.domain = domain
        self.bank_url = bank_url.rstrip("/")

    # ── Cache lookup ──────────────────────────────────────────

    def ask(self, query: str) -> dict:
        """
        Check cache first. Returns dict with:
            hit: bool — True if cache had an answer
            response: str — The cached response (empty string on miss)
            tier: int — 1=cache, 2=local gemma, 3=cloud brain
            confidence: float — How reliable the cached answer is
            source: str — "exact", "similar", or "miss"
        """
        try:
            params = f"query={_urlencode(query)}&domain={_urlencode(self.domain)}"
            url = f"{self.bank_url}/api/retrieve?{params}"
            data = _get_json(url)
            if data and data.get("hit"):
                return {
                    "hit": True,
                    "response": data.get("response", ""),
                    "tier": 1,
                    "confidence": data.get("confidence", 1.0),
                    "source": data.get("source", "exact"),
                    "tokens_saved": data.get("tokens_saved", 0),
                }
        except Exception as e:
            logger.debug(f"Reasoning bank lookup failed: {e}")

        # Cache miss — return routing recommendation
        return {
            "hit": False,
            "response": "",
            "tier": 2,  # Default: use local gemma
            "confidence": 0.0,
            "source": "miss",
            "tokens_saved": 0,
        }

    # ── Store successful responses ────────────────────────────

    def learn(self, query: str, response: str, tokens: int = 0,
              model: str = "gemma2:2b", confidence: float = 1.0):
        """
        Store a query/response pair so future similar queries get instant answers.
        Call this AFTER a successful Ollama inference.
        """
        try:
            payload = json.dumps({
                "query": query,
                "response": response,
                "domain": self.domain,
                "model": model,
                "confidence": confidence,
                "tokens": tokens,
            }).encode()
            # Build URL with query params (FastAPI expects query params for this endpoint)
            params = (
                f"query={_urlencode(query)}"
                f"&response={_urlencode(response)}"
                f"&domain={_urlencode(self.domain)}"
                f"&model={_urlencode(model)}"
                f"&confidence={confidence}"
                f"&tokens={tokens}"
            )
            url = f"{self.bank_url}/api/store?{params}"
            req = Request(url, method="POST", data=b"",
                          headers={"Content-Type": "application/json"})
            urlopen(req, timeout=TIMEOUT)
        except Exception as e:
            logger.debug(f"Reasoning bank store failed: {e}")

    # ── Task routing ──────────────────────────────────────────

    def route(self, task: str) -> dict:
        """
        Get routing recommendation for a task. Returns:
            tier: 1 (cache), 2 (local gemma), 3 (cloud brain)
            handler: str — which backend to use
            response: str — cached response if tier 1
        """
        try:
            params = f"task={_urlencode(task)}&domain={_urlencode(self.domain)}"
            url = f"{self.bank_url}/api/route?{params}"
            req = Request(url, method="POST", data=b"",
                          headers={"Content-Type": "application/json"})
            resp = urlopen(req, timeout=TIMEOUT)
            return json.loads(resp.read())
        except Exception as e:
            logger.debug(f"Reasoning bank route failed: {e}")
            return {"tier": 2, "handler": "gemma_local_fallback"}

    # ── Feedback ──────────────────────────────────────────────

    def feedback(self, query: str, success: bool):
        """Report whether a cached response was useful (improves future confidence)."""
        try:
            import hashlib
            normalized = query.strip().lower()
            qhash = hashlib.sha256(f"{self.domain}:{normalized}".encode()).hexdigest()[:32]
            params = f"query_hash={_urlencode(qhash)}&success={'true' if success else 'false'}"
            url = f"{self.bank_url}/api/feedback?{params}"
            req = Request(url, method="POST", data=b"",
                          headers={"Content-Type": "application/json"})
            urlopen(req, timeout=TIMEOUT)
        except Exception as e:
            logger.debug(f"Reasoning bank feedback failed: {e}")


# ── Helpers ───────────────────────────────────────────────────

def _urlencode(s: str) -> str:
    """URL-encode a string."""
    from urllib.parse import quote
    return quote(str(s), safe="")


def _get_json(url: str) -> dict:
    """GET a URL and parse JSON response."""
    req = Request(url)
    resp = urlopen(req, timeout=TIMEOUT)
    return json.loads(resp.read())
