from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import logging
from collections.abc import Callable
from typing import Any
from urllib.parse import parse_qs, urlsplit

log = logging.getLogger("tare_api")

# Backward-compatible globals.
tare_request = False
filter_alpha = 0.05

_tare_callback: Callable[[], Any] | None = None
_filter_callback: Callable[[float], Any] | None = None
_status_callback: Callable[[], dict[str, Any]] | None = None


def set_handlers(
    tare_callback: Callable[[], Any] | None = None,
    filter_callback: Callable[[float], Any] | None = None,
    status_callback: Callable[[], dict[str, Any]] | None = None,
) -> None:
    global _tare_callback, _filter_callback, _status_callback
    _tare_callback = tare_callback
    _filter_callback = filter_callback
    _status_callback = status_callback


class TareHandler(BaseHTTPRequestHandler):
    def add_cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def do_OPTIONS(self):
        self.send_response(200)
        self.add_cors_headers()
        self.end_headers()

    def do_POST(self):
        global tare_request, filter_alpha
        path = urlsplit(self.path).path.rstrip("/") or "/"

        if path == "/tare":
            tare_request = True
            if not _tare_callback:
                self._json_response(
                    {"status": "unavailable", "error": "tare handler unavailable"},
                    status=503,
                )
                return
            mode = parse_qs(urlsplit(self.path).query).get("mode", ["toggle"])[0]
            started = _tare_callback(mode == "ensure")
            if started is False:
                self._json_response(
                    {"status": "rejected", "error": "tare busy or ADS1220 unavailable"},
                    status=409,
                )
                return
            self._json_response({"status": "started"})
            return

        if path == "/filter":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length)
                data = json.loads(body or b"{}")
                alpha = float(data["alpha"])
                if not 0.0 < alpha <= 1.0:
                    raise ValueError("alpha must be in range 0..1")
                filter_alpha = alpha
                if _filter_callback:
                    _filter_callback(alpha)
                self._json_response({"alpha": filter_alpha})
            except Exception as exc:
                self._json_response({"error": str(exc)}, status=400)
            return

        self.send_response(404)
        self.end_headers()

    def do_GET(self):
        path = urlsplit(self.path).path.rstrip("/") or "/"

        if path == "/filter":
            self._json_response({"alpha": filter_alpha})
            return

        if path == "/status":
            status = _status_callback() if _status_callback else {}
            self._json_response(status)
            return

        self.send_response(404)
        self.end_headers()

    def _json_response(self, payload: dict[str, Any], status: int = 200):
        self.send_response(status)
        self.add_cors_headers()
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(payload, default=str).encode("utf-8"))

    def log_message(self, format, *args):
        return


def start_server(host: str = "0.0.0.0", port: int = 8080):
    try:
        server = ThreadingHTTPServer((host, int(port)), TareHandler)
        log.info("Tare API running on %s:%s", host, port)
        server.serve_forever()
    except Exception:
        log.exception("Tare API error")
