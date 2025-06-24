import logging
import traceback

import boto3
import botocore.exceptions
from botocore.config import Config

from address_etl.settings import Settings

logger = logging.getLogger(__name__)


def upload_file(
    bucket_name: str,
    key: str,
    file_path: str,
    s3: "S3",
    presigned_url_expiry_seconds: int = 3600,
) -> str:
    logger.info(f"Uploading file {file_path} to {bucket_name}/{key}")
    s3.client.upload_file(file_path, bucket_name, key)

    # Create presigned URL for the uploaded file
    presigned_url = s3.client.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket_name, "Key": key},
        ExpiresIn=presigned_url_expiry_seconds,
    )
    logger.info(f"Created presigned URL for {bucket_name}/{key}: {presigned_url}")
    return presigned_url


def download_file(bucket_name: str, key: str, file_path: str, s3: "S3") -> None:
    s3.download_object_to_file(bucket_name, key, file_path)


def get_latest_file(bucket_name: str, s3: "S3", prefix: str = "") -> str | None:
    logger.info(f"Getting latest file from {bucket_name}")
    objects = s3.list_objects(bucket_name)
    for obj in objects:
        if obj["Key"].startswith(prefix):
            logger.info(f"Latest file: {obj['Key']}")
            return obj["Key"]

    logger.info("No files found")
    return None


def _get_s3_client(settings: Settings):
    logger.info("Getting S3 client")

    if settings.use_minio:
        logger.info("Using Minio with S3 client")
        return boto3.client(
            "s3",
            endpoint_url=settings.minio_endpoint,
            aws_access_key_id=settings.minio_access_key,
            aws_secret_access_key=settings.minio_secret_key,
            region_name=settings.minio_region,
            config=Config(signature_version="s3v4"),
        )

    logger.info("Using AWS S3 client")
    return boto3.client("s3")


class S3:
    def __init__(self, settings: Settings):
        self.client = _get_s3_client(settings)

    def list_buckets(self) -> list:
        try:
            return self.client.list_buckets()["Buckets"]
        except boto3.exceptions.Boto3Error as e:
            logger.error(f"Failed to list S3 buckets: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error listing S3 buckets: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def bucket_exists(self, bucket_name: str) -> bool:
        try:
            self.client.head_bucket(Bucket=bucket_name)
            return True
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise

    def create_bucket(self, bucket_name: str) -> None:
        try:
            self.client.create_bucket(Bucket=bucket_name)
        except boto3.exceptions.Boto3Error as e:
            logger.error(f"Failed to create S3 bucket: {str(e)}")
            raise
        except Exception:
            logger.error(traceback.format_exc())
            raise

    def delete_bucket(self, bucket_name: str) -> None:
        try:
            self.client.delete_bucket(Bucket=bucket_name)
        except boto3.exceptions.Boto3Error as e:
            logger.error(f"Failed to delete S3 bucket: {str(e)}")
            raise
        except Exception:
            logger.error(traceback.format_exc())
            raise

    def list_objects(self, bucket_name: str) -> list:
        try:
            result = self.client.list_objects(Bucket=bucket_name)
            result = result["Contents"] if "Contents" in result else []
            return sorted(result, key=lambda x: x["Key"], reverse=True)
        except boto3.exceptions.Boto3Error as e:
            logger.error(f"Failed to list S3 objects: {str(e)}")
            raise
        except Exception:
            logger.error(traceback.format_exc())
            raise

    def create_object(self, bucket_name: str, key: str, body: bytes) -> None:
        try:
            self.client.put_object(Bucket=bucket_name, Key=key, Body=body)
        except boto3.exceptions.Boto3Error as e:
            logger.error(f"Failed to create S3 object: {str(e)}")
            raise
        except Exception:
            logger.error(traceback.format_exc())
            raise

    def delete_object(self, bucket_name: str, key: str) -> None:
        try:
            self.client.delete_object(Bucket=bucket_name, Key=key)
        except boto3.exceptions.Boto3Error as e:
            logger.error(f"Failed to delete S3 object: {str(e)}")
            raise
        except Exception:
            logger.error(traceback.format_exc())
            raise

    def get_object(self, bucket_name: str, key: str) -> bytes:
        try:
            result = self.client.get_object(Bucket=bucket_name, Key=key)
            return result["Body"].read()
        except boto3.exceptions.Boto3Error as e:
            logger.error(f"Failed to get S3 object: {str(e)}")
            raise
        except Exception:
            logger.error(traceback.format_exc())
            raise

    def download_object_to_file(
        self, bucket_name: str, key: str, file_path: str
    ) -> None:
        """
        Download an S3 object directly to a file on disk without loading it into memory.
        This is more memory-efficient for large files.
        """
        try:
            logger.info(f"Downloading {bucket_name}/{key} to {file_path}")
            self.client.download_file(bucket_name, key, file_path)
        except boto3.exceptions.Boto3Error as e:
            logger.error(f"Failed to download S3 object: {str(e)}")
            raise
        except Exception:
            logger.error(traceback.format_exc())
            raise

    def download_object_to_file_streaming(
        self, bucket_name: str, key: str, file_path: str, chunk_size: int = 8192
    ) -> None:
        """
        Download an S3 object to a file using streaming with custom chunk size.
        This gives you more control over the download process and memory usage.
        """
        try:
            logger.info(
                f"Downloading {bucket_name}/{key} to {file_path} with streaming"
            )
            result = self.client.get_object(Bucket=bucket_name, Key=key)

            with open(file_path, "wb") as file:
                for chunk in result["Body"].iter_chunks(chunk_size=chunk_size):
                    file.write(chunk)
        except boto3.exceptions.Boto3Error as e:
            logger.error(f"Failed to download S3 object: {str(e)}")
            raise
        except Exception:
            logger.error(traceback.format_exc())
            raise
