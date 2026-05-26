"""AnyueBusAdapter — HTTP client wrapper for the Agent Message Bus API."""

import json
import time
import urllib.request
import urllib.error
from datetime import datetime


class AnyueBusAdapter:
    """Client adapter for the Agent Message Bus HTTP API."""

    def __init__(self, agent, base_url="http://127.0.0.1:8648"):
        self.agent = agent
        self.base_url = base_url.rstrip("/")

    def _request(self, method, path, payload=None, timeout=15):
        url = f"{self.base_url}{path}"
        data = None
        if payload is not None:
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            try:
                return json.loads(body)
            except Exception:
                return {"ok": False, "error": body, "status_code": e.code}

    def heartbeat(self, status="online", native_status="ok", native_url="", metadata=None):
        return self._request("POST", "/v1/heartbeat", {
            "agent": self.agent,
            "status": status,
            "native_status": native_status,
            "native_url": native_url,
            "metadata": metadata or {},
        })

    def poll(self, limit=5):
        result = self._request("GET", f"/v1/poll?agent={self.agent}&limit={limit}")
        return result.get("messages", []) if result.get("ok") else []

    def send(self, to_agent, content, metadata=None, priority=5, client_msg_id=None):
        payload = {
            "from_agent": self.agent,
            "to_agent": to_agent,
            "content": content,
            "priority": priority,
            "metadata": metadata or {},
        }
        if client_msg_id:
            payload["client_msg_id"] = client_msg_id
        return self._request("POST", "/v1/send", payload)

    def ack_pending(self, message_id):
        return self._request("POST", "/v1/ack_pending", {
            "message_id": message_id,
            "agent": self.agent,
        })

    def ack(self, message_id, reply="", processing_mode="ai", error_detail=""):
        return self._request("POST", "/v1/ack", {
            "message_id": message_id,
            "reply": reply,
            "processing_mode": processing_mode,
            "error_detail": error_detail,
        })

    def status(self):
        """Get full bus status."""
        return self._request("GET", "/v1/status")
