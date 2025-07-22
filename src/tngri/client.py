import io
import pathlib
import random
from dataclasses import dataclass
from string import ascii_lowercase

import boto3
import polars

from src.tngri.config import Config


def _randstr(length: int = 12):
    return "".join(random.choice(ascii_lowercase) for i in range(length))


@dataclass
class UploadedFile:
    s3_path: str

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

        s3_client.upload_file(filepath, self._config.s3_bucket_name, f"Stage/{filename}")

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

        return filename
