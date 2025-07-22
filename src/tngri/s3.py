@dataclass
class S3Object:
    s3_path: str = None
    s3_access_key_id: str = None  # type: ignore
    s3_secret_access_key: str = None  # type: ignore
    s3_endpoint_url: str = None  # type: ignore
    s3_region: str = "eu-central-1"
    s3_default_region: str = "eu-central-1"
