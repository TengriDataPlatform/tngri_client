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


@pytest.fixture
def s3_client(moto_server) -> Client:
    """A Client pointed at moto, with a fresh empty bucket per test."""
    bucket = f"regress-{uuid.uuid4().hex}"
    client = Client(
        Config(
            ws_addr="ws://unused",
            s3_endpoint_url=moto_server,
            s3_access_key_id="test",
            s3_secret_access_key="test",
            s3_region="us-east-1",
            s3_bucket_name=bucket,
        )
    )
    client._s3_client().create_bucket(Bucket=bucket)
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
                            "_type": "query_finished", "id": msg["id"], "result": [schema, *rows]
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
