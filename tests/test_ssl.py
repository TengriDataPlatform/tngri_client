"""
ws_ca_cert lets the client trust a self-signed wss:// endpoint.

A throwaway TLS websocket server with a self-signed cert answers the minimal
auth + query handshake, so the real Client exercises the SSL path end-to-end:
without ws_ca_cert the handshake is rejected, with it the query round-trips.
"""

import json
import ssl
import subprocess
import threading
from collections.abc import Iterator
from pathlib import Path

import pytest
from websockets.sync.server import serve

from tngri.client import Client
from tngri.config import Config

HOST, PORT = "localhost", 8765


def _handler(ws) -> None:
    for message in ws:
        msg = json.loads(message)
        if msg["_type"] == "auth":
            ws.send(json.dumps({"_type": "auth_success"}))
        elif msg["_type"] == "query":
            # Result shape expected by Client._rows_to_df: [schema, *rows],
            # where schema is a list of (name, type) pairs.
            result = [[["n", "BIGINT"]], [1]]
            ws.send(json.dumps({"_type": "query_finished", "id": msg["id"], "result": result}))


@pytest.fixture(scope="module")
def selfsigned_server(tmp_path_factory) -> Iterator[Path]:
    """A self-signed wss:// server; yields the path to its cert."""
    tmp = tmp_path_factory.mktemp("ssl")
    cert, key = tmp / "cert.pem", tmp / "key.pem"
    subprocess.run(
        [
            "openssl", "req", "-x509", "-newkey", "rsa:2048",
            "-keyout", str(key), "-out", str(cert),
            "-days", "1", "-nodes", "-subj", f"/CN={HOST}",
            "-addext", f"subjectAltName=DNS:{HOST},IP:127.0.0.1",
        ],
        check=True,
        capture_output=True,
    )
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(certfile=str(cert), keyfile=str(key))
    server = serve(_handler, HOST, PORT, ssl=ctx)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield cert
    server.shutdown()


def _client(ws_ca_cert: str | None) -> Client:
    return Client(Config(ws_addr=f"wss://{HOST}:{PORT}", ws_token="x", ws_ca_cert=ws_ca_cert))


def test_selfsigned_rejected_without_ca(selfsigned_server):
    with pytest.raises(ssl.SSLCertVerificationError):
        _client(None).sql("select 1 as n")


def test_selfsigned_trusted_with_ca(selfsigned_server):
    df = _client(str(selfsigned_server)).sql("select 1 as n")
    assert df.iloc[0, 0] == 1
