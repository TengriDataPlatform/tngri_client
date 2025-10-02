import io
import json
import os
import pathlib
import random
import uuid
from copy import deepcopy
from dataclasses import dataclass
from string import ascii_lowercase
from typing import Generator

import boto3
import polars

from .config import Config


def _randstr(length: int = 12):
    return "".join(random.choice(ascii_lowercase) for i in range(length))


from contextlib import contextmanager


@contextmanager
def environ(**env):
    original_env = deepcopy(os.environ)

    for key, value in env.items():
        if value is None:
            del os.environ[key]
        else:
            os.environ[key] = value
    yield

    os.environ = original_env


@dataclass
class UploadedFile:
    s3_path: str

    _client: "Client" = None

    def __str__(self):
        return pathlib.Path(self.s3_path).name


class Client:
    def __init__(self, config: Config | None = None):
        self._config = config

    @classmethod
    def from_env(cls):
        return cls(Config.from_env())

    def upload_file(self, filepath: str, filename: str | None = None) -> UploadedFile:
        filepath = pathlib.Path(filepath)
        if not filepath.exists():
            raise ValueError(f"File {filepath} does not exist")

        if not filename:
            filename = _randstr()

        s3_client = boto3.client(
            "s3",
            endpoint_url=self._config.s3_endpoint_url,
            aws_access_key_id=self._config.s3_access_key_id,
            aws_secret_access_key=self._config.s3_secret_access_key,
            region_name=self._config.s3_region,
        )

        suffix = filepath.suffix
        filename = f"{filename}{suffix}"

        s3_client.upload_file(
            filepath, self._config.s3_bucket_name, f"Stage/{filename}"
        )

        return UploadedFile(f"s3://{self._config.s3_bucket_name}/Stage/{filename}")

    def upload_df(self, df: object, filename: str | None = None) -> UploadedFile:
        if not hasattr(df, "write_parquet"):
            df = polars.DataFrame(df)

        if not filename:
            filename = f"{_randstr()}.parquet"

        s3_client = boto3.client(
            "s3",
            endpoint_url=self._config.s3_endpoint_url,
            aws_access_key_id=self._config.s3_access_key_id,
            aws_secret_access_key=self._config.s3_secret_access_key,
            region_name=self._config.s3_region,
        )
        buffer = io.BytesIO()
        df.write_parquet(buffer)
        parquet = buffer.getvalue()
        s3_client.put_object(
            Body=parquet, Bucket=self._config.s3_bucket_name, Key=f"Stage/{filename}"
        )

        return UploadedFile(f"s3://{self._config.s3_bucket_name}/Stage/{filename}")

    def upload_s3(
        self,
        object: str,
        access_key: str,
        secret_key: str,
        *,
        bucket: str | None = None,
        endpoint: str | None = None,
        region: str | None = None,
        filename: str | None = None,
    ):
        if object.startswith("s3://"):
            bucket, object = object.replace("s3://", "").split("/", 1)

        if not bucket:
            raise ValueError(
                "bucket is required or object shall be in full form (i.e. s3://bucket/path/to/object)"
            )

        # if "*" in object:
        #     prefix = object.partition("*")[0]
        #     objects =

        with environ(
            AWS_ENDPOINT_URL=endpoint or None,
            AWS_ACCESS_KEY_ID=access_key,
            AWS_SECRET_ACCESS_KEY=secret_key,
            AWS_DEFAULT_REGION=region,
        ):
            source_client = boto3.client("s3")
            obj = source_client.get_object(Bucket=bucket, Key=object)["Body"]

        if not filename:
            filename = f"{_randstr()}.{pathlib.Path(object).suffix[1:]}"

        s3_client = boto3.client(
            "s3",
            endpoint_url=self._config.s3_endpoint_url,
            aws_access_key_id=self._config.s3_access_key_id,
            aws_secret_access_key=self._config.s3_secret_access_key,
            region_name=self._config.s3_region,
        )
        s3_client.upload_fileobj(
            obj, Bucket=self._config.s3_bucket_name, Key=f"Stage/{filename}"
        )

        return UploadedFile(f"s3://{self._config.s3_bucket_name}/Stage/{filename}")

    @staticmethod
    def _rows_to_df(rows):
        try:
            return polars.DataFrame(rows[1:], orient='row', schema=[c for (c, t) in rows[0]])
        except Exception as e:
            raise RuntimeError(f"Error while executing: {e}") from e

    @contextmanager
    def _socket(self) -> Generator["WebSocket"]:
        from websocket._core import create_connection

        ws = create_connection(self._config.ws_addr)

        # authenticate
        ws.send(json.dumps({"_type": "auth", "token": self._config.ws_token}))
        msg = json.loads(ws.recv())

        if msg["_type"] != "auth_success":
            ws.close()
            raise RuntimeError(f"Failed to authenticate in {self._config.ws_addr}")

        # use socket
        yield ws

        # close socket
        ws.close()

    def sql(self, sql: str):
        with self._socket() as ws:
            req_id = str(uuid.uuid4())
            ws.send(json.dumps({"_type": "query", "query": sql, "id": req_id}))

            while msg := ws.recv():
                msg = json.loads(msg)
                if msg.get("id") != req_id:
                    continue
                elif msg["_type"] == "query_finished" and msg.get("error"):
                    raise RuntimeError(f"Error while executing: {msg["error"]}")
                elif msg["_type"] == "query_finished" and msg.get("result"):
                    return self._rows_to_df(msg["result"])

    def run_notebook(self, notebook_id: str, env_name: str | None = None):
        with self._socket() as ws:
            req_id = str(uuid.uuid4())
            output = []
            ws.send(json.dumps({"_type": "notebook", "notebook_id": notebook_id, "env_name": env_name, "id": req_id}))

            while msg := ws.recv():
                msg = json.loads(msg)
                if msg.get("id") != req_id:
                    continue
                elif msg["_type"] == "notebook_finished" and msg.get("error"):
                    raise RuntimeError(f"Error while executing: {msg["error"]}")
                elif msg["_type"] == "notebook_finished" and msg.get("result"):
                    return '\n'.join([*map(str.rstrip, output), msg["result"]])

                if error := msg.get("error"):
                    output.append(error)

                res = msg.get("result")
                if isinstance(res, dict) and res.get("output_type") == "error":
                    output.append(': '.join(filter(None, [res.get("ename"), res.get("evalue")])))
                elif isinstance(res, dict) and res.get("output_type") == "stream":
                    text = res.get('text')
                    output.append(''.join(text) if isinstance(text, list) else text)
