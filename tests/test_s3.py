"""Client staging operations against a moto S3 server."""

import io

import pandas as pd
import pytest

from tngri.client import StagedFile, UploadedFile


@pytest.fixture
def df() -> pd.DataFrame:
    return pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})


def _get_bytes(client, key: str) -> bytes:
    return client._s3_client().get_object(Bucket=client._config.s3_bucket_name, Key=key)["Body"].read()


def test_upload_df_roundtrip(s3_client, df):
    uploaded = s3_client.upload_df(df)
    assert uploaded.s3_path.startswith(f"s3://{s3_client._config.s3_bucket_name}/Stage/")
    assert uploaded.s3_path.endswith(".parquet")

    key = uploaded.s3_path.split(f"{s3_client._config.s3_bucket_name}/", 1)[1]
    back = pd.read_parquet(io.BytesIO(_get_bytes(s3_client, key)))
    assert back.equals(df)


def test_upload_df_custom_filename(s3_client, df):
    uploaded = s3_client.upload_df(df, filename="named.parquet")
    assert uploaded.s3_path.endswith("/Stage/named.parquet")


def test_upload_file_roundtrip(s3_client, df, tmp_path):
    local = tmp_path / "sample.parquet"
    df.to_parquet(local)

    uploaded = s3_client.upload_file(str(local))
    key = uploaded.s3_path.split(f"{s3_client._config.s3_bucket_name}/", 1)[1]
    back = pd.read_parquet(io.BytesIO(_get_bytes(s3_client, key)))
    assert back.equals(df)


def test_upload_file_missing_raises(s3_client):
    with pytest.raises(ValueError, match="does not exist"):
        s3_client.upload_file("/no/such/file.parquet")


def test_list_files_shows_upload_and_filters_empty(s3_client, df):
    s3_client.upload_df(df, filename="one.parquet")
    # A zero-byte object must not show up as a staged file.
    s3_client._s3_client().put_object(
        Bucket=s3_client._config.s3_bucket_name, Key="Stage/empty", Body=b""
    )

    files = s3_client.list_files()
    assert [f.path for f in files] == ["one.parquet"]
    assert isinstance(files[0], StagedFile)
    assert files[0].size > 0


def test_list_files_prefix(s3_client, df):
    s3_client.upload_df(df, filename="keep/one.parquet")
    s3_client.upload_df(df, filename="other.parquet")
    assert [f.path for f in s3_client.list_files("keep/")] == ["keep/one.parquet"]


@pytest.mark.parametrize("as_type", ["uploaded", "staged", "str"])
def test_delete_file(s3_client, df, as_type):
    uploaded = s3_client.upload_df(df, filename="gone.parquet")
    assert s3_client.list_files()

    target: UploadedFile | StagedFile | str
    if as_type == "uploaded":
        target = uploaded
    elif as_type == "staged":
        target = s3_client.list_files()[0]
    else:
        target = "gone.parquet"

    s3_client.delete_file(target)
    assert s3_client.list_files() == []
