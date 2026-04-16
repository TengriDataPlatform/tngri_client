import datetime
import io
import json
import os
import pathlib
import random
import uuid
from collections.abc import Generator
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass
from string import ascii_lowercase

import boto3
import botocore.config
import pandas as pd
import polars

from .config import Config


def _randstr(length: int = 12):
    return "".join(random.choice(ascii_lowercase) for i in range(length))


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
        return self.s3_path


@dataclass
class StagedFile:
    path: str
    size: int
    modified: datetime.datetime

    def __repr__(self):
        return f"{self.__class__.__name__}({' '.join([f'{k}={v}' for k, v in self.__dict__.items()])})"


@dataclass
class RunStatus:
    ok: bool
    output: str
    errors: str


def join_lines(lines: list[str]) -> str:
    return "\n".join(map(str.rstrip, lines))


class Client:
    def __init__(self, config: Config | None = None):
        self._config = config

    @classmethod
    def from_env(cls):
        return cls(Config.from_env())

    def _s3_client(self):
        client_config = botocore.config.Config(request_checksum_calculation="WHEN_REQUIRED")  # type: ignore
        return boto3.client(
            "s3",
            endpoint_url=self._config.s3_endpoint_url,
            aws_access_key_id=self._config.s3_access_key_id,
            aws_secret_access_key=self._config.s3_secret_access_key,
            region_name=self._config.s3_region,
            config=client_config,
        )

    def upload_file(self, filepath: str, filename: str | None = None) -> UploadedFile:
        filepath = pathlib.Path(filepath)
        if not filepath.exists():
            raise ValueError(f"File {filepath} does not exist")

        if not filename:
            filename = f"{_randstr()}{filepath.suffix}"

        s3_client = self._s3_client()

        s3_client.upload_file(filepath, self._config.s3_bucket_name, f"Stage/{filename}")

        return UploadedFile(f"s3://{self._config.s3_bucket_name}/Stage/{filename}")

    def upload_df(self, df: object, filename: str | None = None) -> UploadedFile:
        if not hasattr(df, "write_parquet"):
            df = polars.DataFrame(df)

        if not filename:
            filename = f"{_randstr()}.parquet"

        s3_client = self._s3_client()

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

        s3_client = self._s3_client()
        s3_client.upload_fileobj(obj, Bucket=self._config.s3_bucket_name, Key=f"Stage/{filename}")

        return UploadedFile(f"s3://{self._config.s3_bucket_name}/Stage/{filename}")

    def list_files(self, filepath: str = "") -> list[StagedFile]:
        s3_client = self._s3_client()

        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self._config.s3_bucket_name, Prefix=f"Stage/{filepath}")
        return [
            StagedFile(
                obj["Key"].removeprefix("Stage/"), obj.get("Size", 0), obj.get("LastModified")
            )
            for page in pages
            for obj in page.get("Contents", [])
            if obj.get("Size", 1) > 0
        ]

    def delete_file(self, file: str | StagedFile | UploadedFile):
        s3_client = self._s3_client()

        def normalize_stage_path(path: str):
            path = path.removeprefix(f"s3://{self._config.s3_bucket_name}/")
            return path if path.startswith("Stage/") else f"Stage/{path}"

        if isinstance(file, StagedFile):
            path = normalize_stage_path(file.path)
            s3_client.delete_object(Bucket=self._config.s3_bucket_name, Key=path)
        elif isinstance(file, UploadedFile):
            path = normalize_stage_path(file.s3_path)
            s3_client.delete_object(Bucket=self._config.s3_bucket_name, Key=path)
        else:
            path = normalize_stage_path(file)
            s3_client.delete_object(Bucket=self._config.s3_bucket_name, Key=path)

    @staticmethod
    def _rows_to_df(rows):
        try:
            return polars.DataFrame(
                rows[1:], orient="row", schema=[c for (c, t) in rows[0]], infer_schema_length=None
            ).to_pandas()
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
                    raise RuntimeError(f"Error while executing: {msg['error']}")
                elif msg["_type"] == "query_finished" and msg.get("result"):
                    return self._rows_to_df(msg["result"])

    def run_notebook(
        self, notebook_id: str, env_name: str | None = None, parent_job_id: str | None = None
    ) -> RunStatus:
        if not parent_job_id:
            parent_job_id = self._config.default_parent_job_id

        with self._socket() as ws:
            req_id = str(uuid.uuid4())
            output: list[str] = []
            errors: list[str] = []
            ws.send(
                json.dumps(
                    {
                        "_type": "notebook",
                        "notebook_id": notebook_id,
                        "env_name": env_name,
                        "id": req_id,
                        "job_id": parent_job_id,
                    }
                )
            )

            while msg := ws.recv():
                msg = json.loads(msg)
                if msg.get("id") != req_id:
                    continue
                elif msg["_type"] == "notebook_finished" and msg.get("error"):
                    errors.append(msg["error"])
                    return RunStatus(False, join_lines(output), join_lines(errors))
                elif msg["_type"] == "notebook_finished" and msg.get("result"):
                    output.append(msg["result"])
                    return RunStatus(True, join_lines(output), join_lines(errors))

                if error := msg.get("error"):
                    errors.append(error)

                res = msg.get("result")
                if isinstance(res, dict) and res.get("output_type") == "error":
                    errors.append(": ".join(filter(None, [res.get("ename"), res.get("evalue")])))
                elif isinstance(res, dict) and res.get("output_type") == "stream":
                    text = res.get("text")
                    output.append("".join(text) if isinstance(text, list) else text)

    def create_table(
        self,
        data: pd.DataFrame | polars.DataFrame,
        table_name: str,
        replace: bool = False,
    ):
        data = pd.DataFrame(data)
        file = self.upload_df(data)
        self.sql(f"""
            CREATE {"OR REPLACE" if replace else ""} TABLE {table_name}
            AS SELECT * FROM read_parquet('{file.s3_path}')
        """)
