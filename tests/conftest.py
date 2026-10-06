"""
Fixtures backing the client unit tests with real doubles for its two backends.

- ``s3_client``: a Client whose S3 calls hit an in-process moto server (a real S3
  endpoint over HTTP), so the boto3 + endpoint_url path is exercised for real.
- ``sql_env``: a Client whose ws SQL calls hit an in-process websocket server that
  runs each query against an in-memory DuckDB and replies in the wire shape the
  client decodes, so ``Client.sql`` is exercised end to end without a live stack.
"""

import json
import threading
import uuid
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import ClassVar

import duckdb
import pytest
from moto.server import ThreadedMotoServer
from websockets.exceptions import ConnectionClosed
from websockets.sync.server import serve

from tngri.client import Client
from tngri.config import Config


@pytest.fixture(scope="session")
def moto_server() -> Iterator[str]:
    """A session-wide moto S3 server; yields its endpoint URL."""
    server = ThreadedMotoServer(port=0)
    server.start()
    _, port = server.get_host_and_port()
    yield f"http://127.0.0.1:{port}"
    server.stop()


class _CredentialsRoute(BaseHTTPRequestHandler):
    """The server's GET /v1/storage/credentials, answering for alice with moto's address."""

    reply: ClassVar[dict] = {}
    authorizations: ClassVar[list[str | None]] = []

    def do_GET(self) -> None:
        type(self).authorizations.append(self.headers.get("Authorization"))
        if self.path != "/v1/storage/credentials":
            self.send_error(404)
            return
        body = json.dumps(type(self).reply).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args) -> None:
        pass


@pytest.fixture
def credentials_route(moto_server) -> Iterator[type[_CredentialsRoute]]:
    """The credentials route on its own port, with a fresh bucket named in its reply."""
    route = type("Route", (_CredentialsRoute,), {"authorizations": []})
    route.reply = {
        "endpoint": moto_server,
        "region": "us-east-1",
        "bucket": f"regress-{uuid.uuid4().hex}",
        "access_key": "test",
        "secret_key": "test",
        "staging_prefix": "Stage",
        "own_prefix": "Stage/home/alice",
        "public_prefix": "Stage/public",
    }
    server = HTTPServer(("127.0.0.1", 0), route)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    route.port = server.server_address[1]
    yield route
    server.shutdown()


@pytest.fixture
def s3_client(credentials_route) -> Client:
    """A Client that learns its storage from the credentials route, as alice."""
    client = Client(Config(ws_addr=f"ws://127.0.0.1:{credentials_route.port}", ws_token="t"))
    client._s3_client().create_bucket(Bucket=credentials_route.reply["bucket"])
    return client


@pytest.fixture
def sql_env() -> Iterator[tuple[Client, duckdb.DuckDBPyConnection]]:
    """
    A Client wired to a DuckDB-backed ws server; yields (client, connection).

    The connection is the same DuckDB the server queries, so a test can seed
    tables through it and read them back through the client.
    """
    con = duckdb.connect()
    lock = threading.Lock()

    def handler(ws) -> None:
        # The client closes the socket abruptly after each call; that surfaces as
        # ConnectionClosed on the next recv, which just ends this handler.
        try:
            for message in ws:
                msg = json.loads(message)
                if msg["_type"] == "auth":
                    ws.send(json.dumps({"_type": "auth_success"}))
                elif msg["_type"] == "query":
                    try:
                        with lock:
                            cur = con.execute(msg["query"])
                            schema = [[d[0], str(d[1])] for d in cur.description]
                            rows = [list(r) for r in cur.fetchall()]
                        payload = {
                            "_type": "query_finished",
                            "id": msg["id"],
                            "result": [schema, *rows],
                        }
                    except Exception as e:
                        payload = {"_type": "query_finished", "id": msg["id"], "error": str(e)}
                    ws.send(json.dumps(payload, default=str))
        except ConnectionClosed:
            pass

    server = serve(handler, "localhost", 0)
    port = server.socket.getsockname()[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    client = Client(Config(ws_addr=f"ws://localhost:{port}", ws_token="x"))
    yield client, con
    server.shutdown()
    con.close()
