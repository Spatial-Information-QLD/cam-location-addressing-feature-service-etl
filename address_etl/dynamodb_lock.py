import boto3.dynamodb
from dynamodblock import DynamoDBLock
from mypy_boto3_dynamodb.service_resource import Table

from address_etl.settings import settings


def get_lock(lock_id: str, table: Table) -> DynamoDBLock:
    lock = DynamoDBLock(
        lock_id=lock_id,
        dynamodb_table_resource=table,
        lock_ttl=86_400,  # 24 hours
        retry_timeout=600,  # 10 minutes
        retry_interval=60,  # 1 minute
        verbose=True,
        debug=True,
    )
    return lock


def get_lock_table(lock_table_name: str) -> Table:
    if settings.use_minio:
        dynamodb = boto3.resource(
            "dynamodb",
            endpoint_url="http://localhost:4566",
            region_name=settings.minio_region,
            aws_access_key_id=settings.minio_access_key,
            aws_secret_access_key=settings.minio_secret_key,
        )
    else:
        dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(lock_table_name)
    return table
