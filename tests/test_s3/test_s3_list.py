from address_etl.s3 import S3
from address_etl.settings import settings


def test_list_buckets():
    # No buckets
    s3 = S3(settings)
    assert s3.list_buckets() == []

    # Create bucket
    s3.create_bucket("test-bucket")

    # List bucket
    buckets = s3.list_buckets()
    assert buckets[0]["Name"] == "test-bucket"

    # Delete bucket
    s3.delete_bucket("test-bucket")

    # No buckets
    assert s3.list_buckets() == []


def test_list_objects():
    s3 = S3(settings)
    s3.create_bucket("location-addressing-sqlite")

    assert s3.list_objects("location-addressing-sqlite") == []

    FILE_NAME = "etl/2025-05-26T12:00:00-test.txt"
    FILE_NAME_2 = "etl/2025-05-26T12:00:01-test.txt"

    s3.create_object(
        "location-addressing-sqlite",
        FILE_NAME,
        b"Hello, world!",
    )

    result = s3.list_objects("location-addressing-sqlite")
    assert result[0]["Key"] == FILE_NAME

    # API should return in desc lexigraphical order
    s3.create_object(
        "location-addressing-sqlite",
        FILE_NAME_2,
        b"Hello, world!",
    )

    result = s3.list_objects("location-addressing-sqlite")
    assert result[0]["Key"] == FILE_NAME_2 and result[1]["Key"] == FILE_NAME
