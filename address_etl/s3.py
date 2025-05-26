import logging
import traceback

import boto3
from botocore.config import Config

from address_etl.settings import Settings

logger = logging.getLogger(__name__)


def get_s3_client(settings: Settings):
    if settings.use_minio:
        return boto3.client(
            "s3",
            endpoint_url=settings.minio_endpoint,
            aws_access_key_id=settings.minio_access_key,
            aws_secret_access_key=settings.minio_secret_key,
            region_name=settings.minio_region,
            config=Config(signature_version="s3v4"),
        )

    return boto3.client("s3")


class S3:
    def __init__(self, settings: Settings):
        self.client = get_s3_client(settings)

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
