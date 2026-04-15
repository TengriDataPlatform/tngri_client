import pathlib

import pandas as pd
import polars

from .client import Client, StagedFile, UploadedFile

__all__ = ["Client", "upload_df", "upload_file", "upload_s3"]

_DEFAULT_CLIENT = None


def _default_client() -> Client | None:
    return _DEFAULT_CLIENT or Client.from_env()


def _client_or_raise(c: Client | None = None) -> Client:
    _c = c or _default_client()
    if not _c:
        raise ValueError(
            "No default client is set to tngri module. Use tngri.set_default_client() or set proper environment variables"
        )
    return _c


def set_default_client(c: Client | None = None):
    _DEFAULT_CLIENT = c


def upload_file(
    file: pathlib.Path,
    filename: str | None = None,
    *,
    client: Client | None = None,
):
    _c = _client_or_raise(client)

    return _c.upload_file(file, filename)


def upload_df(
    df: object,
    filename: str | None = None,
    *,
    client: Client | None = None,
):
    _c = _client_or_raise(client)

    return _c.upload_df(df, filename)


def upload_s3(
    object: str,
    access_key: str,
    secret_key: str,
    *,
    bucket: str | None = None,
    endpoint: str | None = None,
    region: str | None = None,
    filename: str | None = None,
    client: Client | None = None,
):
    _c = _client_or_raise(client)

    return _c.upload_s3(
        object,
        access_key,
        secret_key,
        bucket=bucket,
        endpoint=endpoint,
        region=region,
        filename=filename,
    )


def list_files(
    filepath: str = "",
    *,
    client: Client | None = None,
):
    _c = _client_or_raise(client)

    return _c.list_files(filepath)


def delete_file(
    file: str | StagedFile | UploadedFile,
    *,
    client: Client | None = None,
):
    _c = _client_or_raise(client)

    _c.delete_file(file)


def sql(sql: str, *, client: Client | None = None):
    _c = _client_or_raise(client)
    return _c.sql(sql)


def run_notebook(notebook_id: str, env_name: str | None = None, client: Client | None = None):
    _c = _client_or_raise(client)
    return _c.run_notebook(notebook_id, env_name)


def create_table(data: pd.DataFrame | polars.DataFrame, table_name: str, replace: bool = False):
    _c = _client_or_raise()
    return _c.create_table(data, table_name, replace=replace)


def update():
    import os

    os.system(
        "PATH=/usr/bin:$PATH python -m pip install --upgrade --force-reinstall git+https://github.com/TengriDataPlatform/tngri_client.git"
    )
