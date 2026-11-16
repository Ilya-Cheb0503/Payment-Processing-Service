from __future__ import annotations

import json
from collections import deque
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock
from typing import Any
from urllib.parse import urlparse

_MAX_EVENTS = 1000
_events: deque[dict[str, Any]] = deque(maxlen=_MAX_EVENTS)
_lock = Lock()


class WebhookMockHandler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:  # noqa: A003
        print(f"[webhook-mock] {self.address_string()} {format % args}", flush=True)

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw.decode("utf-8") or "{}")
        except json.JSONDecodeError:
            body = {"raw": raw.decode("utf-8", errors="replace")}
        event = {
            "received_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "path": self.path,
            "body": body,
        }
        with _lock:
            _events.appendleft(event)
        self._json(202, {"ok": True})

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path in {"/health", "/"}:
            self._json(200, {"status": "ok", "count": len(_events)})
            return
        if parsed.path == "/events":
            with _lock:
                events = list(_events)
            self._json(200, {"events": events})
            return
        self._json(404, {"error": "not found"})

    def _json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main() -> None:
    server = ThreadingHTTPServer(("0.0.0.0", 8080), WebhookMockHandler)
    print("webhook mock listening on :8080", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
