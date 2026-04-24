from typing import TYPE_CHECKING

from dynamodblock import DynamoDBLock

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.service_resource import Table


def get_lock(lock_id: str, table: "Table") -> DynamoDBLock:
    return DynamoDBLock(
        lock_id=lock_id,
        dynamodb_table_resource=table,
        lock_ttl=86_400,  # 24 hours
        retry_timeout=600,  # 10 minutes
        retry_interval=60,  # 1 minute
        verbose=True,
        debug=True,
    )
