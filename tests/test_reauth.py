"""
Client re-auth handling: a mid-session server nudge triggers an HTTP token refresh.

The server's watchdog can send ``re_auth_required`` (or ``auth_requested``) over a
live socket once the access token lapses. The client must POST /auth/refresh to
rotate the token — the server keeps the same socket open — and then keep reading
the in-flight query's result instead of choking on the nudge.
"""

import json
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from websockets.exceptions import ConnectionClosed
from websockets.sync.server import serve

from tngri.client import Client
from tngri.config import Config

VALID_REFRESH = "valid-refresh"


class _RefreshHandler(BaseHTTPRequestHandler):
    """POST /auth/refresh double: rotates the token pair for a valid refresh cookie."""

    def do_POST(self):  # noqa: N802
        self.server.hits += 1  # type: ignore[attr-defined]
        cookie = self.headers.get("Cookie", "")
        if f"refresh_token={VALID_REFRESH}" in cookie:
            self.send_response(200)
            self.send_header("Set-Cookie", "auth_token=rotated-access; Path=/")
            self.send_header("Set-Cookie", "refresh_token=rotated-refresh; Path=/auth/refresh")
            self.end_headers()
            self.wfile.write(b'{"user":"u","access_expires_at":0}')
        else:
            self.send_response(401)
            self.end_headers()

    def log_message(self, *args):  # silence per-request stderr logging
        pass


def _nudging_ws_handler(ws) -> None:
    """auth → auth_success; query → a re_auth_required nudge, then the real result."""
    try:
        for message in ws:
            msg = json.loads(message)
            if msg["_type"] == "auth":
                ws.send(json.dumps({"_type": "auth_success"}))
            elif msg["_type"] == "query":
                ws.send(json.dumps({"_type": "re_auth_required"}))
                ws.send(
                    json.dumps(
                        {
                            "_type": "query_finished",
                            "id": msg["id"],
                            "result": [[["n", "BIGINT"]], [1]],
                        }
                    )
                )
    except ConnectionClosed:
        pass


@pytest.fixture
def reauth_env() -> Iterator[tuple[Client, HTTPServer]]:
    """A Client whose ws server nudges mid-query and whose /auth/refresh is a live HTTP double.

    Production derives the refresh URL from ws_addr, so the endpoint shares the ws
    host:port. The websockets test server only speaks GET/upgrade, so the POST double
    runs on its own port and the client's ``_auth_refresh_url`` is pointed at it.
    """
    ws_server = serve(_nudging_ws_handler, "localhost", 0)
    ws_port = ws_server.socket.getsockname()[1]
    threading.Thread(target=ws_server.serve_forever, daemon=True).start()

    http_server = HTTPServer(("localhost", 0), _RefreshHandler)
    http_server.hits = 0  # type: ignore[attr-defined]
    http_port = http_server.socket.getsockname()[1]
    threading.Thread(target=http_server.serve_forever, daemon=True).start()

    client = Client(
        Config(
            ws_addr=f"ws://localhost:{ws_port}",
            ws_token="stale-access",
            ws_refresh_token=VALID_REFRESH,
        )
    )
    client._auth_refresh_url = lambda: f"http://localhost:{http_port}/auth/refresh"  # type: ignore[method-assign]

    yield client, http_server

    ws_server.shutdown()
    http_server.shutdown()


def test_reauth_refreshes_and_continues(reauth_env):
    client, http_server = reauth_env
    out = client.sql("SELECT 1 AS n")

    assert out["n"].iloc[0] == 1  # the query still completed past the nudge
    assert http_server.hits == 1  # the refresh was performed exactly once
    # the rotated tokens were adopted for the next open / next refresh
    assert client._config.ws_token == "rotated-access"
    assert client._config.ws_refresh_token == "rotated-refresh"


def test_reauth_without_refresh_token_raises(reauth_env):
    client, _ = reauth_env
    client._config.ws_refresh_token = None
    with pytest.raises(RuntimeError, match="access-token-only session"):
        client.sql("SELECT 1 AS n")


def test_reauth_rejected_401_raises(reauth_env):
    client, _ = reauth_env
    client._config.ws_refresh_token = "wrong-refresh"
    with pytest.raises(RuntimeError, match="token refresh rejected"):
        client.sql("SELECT 1 AS n")


def test_refresh_url_computes_correctly():
    # Single-port deployment: derive from ws_addr (wss -> https).
    client = Client(Config(ws_addr="wss://app.example:3001/ws"))
    assert client._auth_refresh_url() == "https://app.example:3001/auth/refresh"
