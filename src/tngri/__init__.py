import pathlib

from .client import Client

__all__ = ["Client", "upload_df", "upload_file", "upload_s3"]


_DEFAULT_CLIENT = None


def _default_client() -> Client | None:
    return _DEFAULT_CLIENT or Client.from_env()


def set_default_client(c: Client | None = None):
    _DEFAULT_CLIENT = c


def upload_file(
    file: pathlib.Path,
    filename: str | None = None,
    *,
    client: Client | None = None,
):
    _c = client or _default_client()
    if _c is None:
        raise ValueError("No client provided")

    return _c.upload_file(file, filename)


def upload_df(
    df: object,
    filename: str | None = None,
    *,
    client: Client | None = None,
):
    _c = client or _default_client()
    if _c is None:
        raise ValueError("No client provided")

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

    _c = client or _default_client()
    if _c is None:
        raise ValueError("No client provided")

    return _c.upload_s3(
        object,
        access_key,
        secret_key,
        bucket=bucket,
        endpoint=endpoint,
        region=region,
        filename=filename,
    )


def update():
    import os

    os.system("pip install --upgrade git+ssh://git@github.com/naorlov/tngri_client.git")
