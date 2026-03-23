#!/usr/bin/env python3
"""
Council Client — Mixin for any Hive agent to communicate via the Agent Council.

Usage:
    from council_client import CouncilClient

    council = CouncilClient(agent_id="hive-nerve", agent_type="service", machine="ZeroQ")
    council.speak("ops", "status", "Nerve DB at 37,067 facts")
    unread = council.listen()
    unread_ops = council.listen(channel="ops")
"""

import os
from typing import Optional, List, Dict, Any

import httpx

COUNCIL_URL = os.environ.get("COUNCIL_URL", "http://localhost:8766")
TIMEOUT = 10.0


class CouncilClient:
    """Mixin / standalone client for Agent Council communication."""

    def __init__(
        self,
        agent_id: str,
        agent_type: str = "service",
        machine: str = "unknown",
        council_url: str = COUNCIL_URL,
    ):
        self.agent_id = agent_id
        self.agent_type = agent_type
        self.machine = machine
        self.council_url = council_url.rstrip("/")

    def speak(
        self,
        channel: str,
        message_type: str,
        message: str,
        addressed_to: Optional[str] = None,
        reply_to: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Post a message to a council channel.

        Args:
            channel: One of general, revenue, ops, comms, alerts, strategy, insights
            message_type: One of info, request, alert, decision, question, reply, status, directive
            message: The message text
            addressed_to: Optional agent_id this message is directed at
            reply_to: Optional message id this is a reply to

        Returns:
            Response dict with ok, id, posted_at, channel
        """
        payload = {
            "agent_id": self.agent_id,
            "agent_type": self.agent_type,
            "machine": self.machine,
            "channel": channel,
            "message_type": message_type,
            "message": message,
            "addressed_to": addressed_to,
            "reply_to": reply_to,
        }
        try:
            resp = httpx.post(
                f"{self.council_url}/council/post",
                json=payload,
                timeout=TIMEOUT,
            )
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as e:
            return {"ok": False, "error": f"HTTP {e.response.status_code}: {e.response.text}"}
        except httpx.ConnectError:
            return {"ok": False, "error": f"Cannot connect to Council at {self.council_url}"}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def listen(
        self,
        channel: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Get unread messages (marks them as read on retrieval).

        Args:
            channel: Optional channel filter
            limit: Max messages to return (default 100)

        Returns:
            List of message dicts
        """
        params = {"limit": limit}
        if channel:
            params["channel"] = channel
        try:
            resp = httpx.get(
                f"{self.council_url}/council/read/{self.agent_id}",
                params=params,
                timeout=TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
            return data.get("messages", [])
        except httpx.ConnectError:
            return []
        except Exception:
            return []

    def channel(self, channel: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Read recent messages from a channel (does NOT mark as read).

        Args:
            channel: Channel name
            limit: Max messages (default 50)

        Returns:
            List of message dicts
        """
        try:
            resp = httpx.get(
                f"{self.council_url}/council/channel/{channel}",
                params={"limit": limit},
                timeout=TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
            return data.get("messages", [])
        except Exception:
            return []

    def thread(self, message_id: int) -> List[Dict[str, Any]]:
        """Get a full conversation thread starting from any message in it.

        Args:
            message_id: Any message ID in the thread

        Returns:
            List of message dicts in chronological order
        """
        try:
            resp = httpx.get(
                f"{self.council_url}/council/thread/{message_id}",
                timeout=TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
            return data.get("messages", [])
        except Exception:
            return []

    def resolve(self, message_id: int) -> Dict[str, Any]:
        """Mark a message as resolved.

        Args:
            message_id: The message to resolve

        Returns:
            Response dict
        """
        try:
            resp = httpx.post(
                f"{self.council_url}/council/resolve/{message_id}",
                json={"agent_id": self.agent_id},
                timeout=TIMEOUT,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def status(self) -> Dict[str, Any]:
        """Get council status: active agents, channel counts, etc."""
        try:
            resp = httpx.get(
                f"{self.council_url}/council/status",
                timeout=TIMEOUT,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            return {"error": str(e)}

    def health(self) -> Dict[str, Any]:
        """Check if council API is reachable."""
        try:
            resp = httpx.get(
                f"{self.council_url}/health",
                timeout=TIMEOUT,
            )
            resp.raise_for_status()
            return resp.json()
        except httpx.ConnectError:
            return {"status": "unreachable", "url": self.council_url}
        except Exception as e:
            return {"status": "error", "error": str(e)}
