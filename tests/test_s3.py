"""Client staging operations against a moto S3 server."""

import io

import pandas as pd
import pytest

from tngri.client import Client, StagedFile, UploadedFile
from tngri.config import Config


@pytest.fixture
def df() -> pd.DataFrame:
    return pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})


def _get_bytes(client, key: str) -> bytes:
    return (
        client._s3_client()
        .get_object(Bucket=client._storage_access().bucket, Key=key)["Body"]
        .read()
    )


def test_upload_df_roundtrip(s3_client, df):
    uploaded = s3_client.upload_df(df)
    assert uploaded.s3_path.startswith(
        f"s3://{s3_client._storage_access().bucket}/Stage/home/alice/"
    )
    assert uploaded.s3_path.endswith(".parquet")

    key = uploaded.s3_path.split(f"{s3_client._storage_access().bucket}/", 1)[1]
    back = pd.read_parquet(io.BytesIO(_get_bytes(s3_client, key)))
    assert back.equals(df)


def test_upload_df_custom_filename(s3_client, df):
    uploaded = s3_client.upload_df(df, filename="named.parquet")
    assert uploaded.s3_path.endswith("/Stage/home/alice/named.parquet")


def test_upload_df_relative_path_is_under_home(s3_client, df):
    uploaded = s3_client.upload_df(df, filename="sub/named.parquet")
    assert uploaded.s3_path.endswith("/Stage/home/alice/sub/named.parquet")


def test_upload_df_absolute_path_resolves_from_stage_root(s3_client, df):
    uploaded = s3_client.upload_df(df, filename="/public/named.parquet")
    assert uploaded.s3_path.endswith("/Stage/public/named.parquet")


def test_upload_file_roundtrip(s3_client, df, tmp_path):
    local = tmp_path / "sample.parquet"
    df.to_parquet(local)

    uploaded = s3_client.upload_file(str(local))
    key = uploaded.s3_path.split(f"{s3_client._storage_access().bucket}/", 1)[1]
    back = pd.read_parquet(io.BytesIO(_get_bytes(s3_client, key)))
    assert back.equals(df)


def test_upload_file_missing_raises(s3_client):
    with pytest.raises(ValueError, match="does not exist"):
        s3_client.upload_file("/no/such/file.parquet")


def test_list_files_shows_upload_and_filters_empty(s3_client, df):
    s3_client.upload_df(df, filename="one.parquet")
    # A zero-byte object must not show up as a staged file.
    s3_client._s3_client().put_object(
        Bucket=s3_client._storage_access().bucket, Key="Stage/home/alice/empty", Body=b""
    )

    files = s3_client.list_files()
    assert [f.path for f in files] == ["/home/alice/one.parquet"]
    assert isinstance(files[0], StagedFile)
    assert files[0].size > 0


def test_list_files_prefix(s3_client, df):
    s3_client.upload_df(df, filename="keep/one.parquet")
    s3_client.upload_df(df, filename="other.parquet")
    assert [f.path for f in s3_client.list_files("keep/")] == ["/home/alice/keep/one.parquet"]


def test_list_files_defaults_to_home_and_slash_lists_stage_root(s3_client, df):
    s3_client.upload_df(df, filename="mine.parquet")
    s3_client.upload_df(df, filename="/public/shared.parquet")
    assert [f.path for f in s3_client.list_files()] == ["/home/alice/mine.parquet"]
    assert [f.path for f in s3_client.list_files("/")] == [
        "/home/alice/mine.parquet",
        "/public/shared.parquet",
    ]


@pytest.mark.parametrize("as_type", ["uploaded", "staged", "relative_str", "absolute_str"])
def test_delete_file(s3_client, df, as_type):
    uploaded = s3_client.upload_df(df, filename="gone.parquet")
    assert s3_client.list_files()

    target: UploadedFile | StagedFile | str
    if as_type == "uploaded":
        target = uploaded
    elif as_type == "staged":
        target = s3_client.list_files()[0]
    elif as_type == "relative_str":
        target = "gone.parquet"
    else:
        target = "/home/alice/gone.parquet"

    s3_client.delete_file(target)
    assert s3_client.list_files() == []


def test_storage_is_read_once_from_the_credentials_route_as_the_caller(
    s3_client, credentials_route, df
):
    s3_client.upload_df(df, filename="one.parquet")
    s3_client.list_files()
    assert credentials_route.authorizations == ["Bearer t"]


def test_staging_root_is_taken_from_the_credentials_reply(credentials_route, df):
    credentials_route.reply.update(staging_prefix="staging", own_prefix="staging/home/alice")
    client = Client(Config(ws_addr=f"ws://127.0.0.1:{credentials_route.port}", ws_token="t"))
    client._s3_client().create_bucket(Bucket=credentials_route.reply["bucket"])

    assert client.upload_df(df, filename="/public/x.parquet").s3_path.endswith(
        "/staging/public/x.parquet"
    )
    assert [f.path for f in client.list_files("/")] == ["/public/x.parquet"]


def test_staging_fails_when_server_has_no_s3_proxy(credentials_route):
    credentials_route.reply["endpoint"] = None
    client = Client(Config(ws_addr=f"ws://127.0.0.1:{credentials_route.port}", ws_token="t"))
    with pytest.raises(RuntimeError, match="no S3 access proxy"):
        client.list_files()


def test_s3_settings_skip_the_credentials_route(credentials_route, df):
    reply = credentials_route.reply
    client = Client(
        Config(
            ws_addr=f"ws://127.0.0.1:{credentials_route.port}",
            ws_token="t",
            s3_endpoint_url=reply["endpoint"],
            s3_region=reply["region"],
            s3_bucket_name=reply["bucket"],
            s3_access_key_id="test",
            s3_secret_access_key="test",
        )
    )
    client._s3_client().create_bucket(Bucket=reply["bucket"])

    assert client.upload_df(df, filename="x.parquet").s3_path.endswith(
        "/Stage/x.parquet"
    )
    assert [f.path for f in client.list_files("/")] == ["/x.parquet"]
    assert credentials_route.authorizations == []


def test_s3_settings_are_read_from_the_environment(monkeypatch):
    monkeypatch.setenv("TNGRI_S3_ACCESS_KEY_ID", "key")
    assert Config.from_env().s3_access_key_id == "key"
