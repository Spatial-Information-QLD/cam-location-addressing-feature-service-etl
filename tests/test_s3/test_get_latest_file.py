from address_etl.s3 import S3, get_latest_file


def test_get_latest_file(s3: S3):
    s3.create_bucket("test-bucket")
    s3.create_object(
        "test-bucket", "etl/2025-05-28T00:00:00+1000/address.db", b"Hello, world!"
    )
    s3.create_object(
        "test-bucket", "etl/2025-05-27T00:00:00+1000/address.db", b"Hello, world!"
    )

    # Create a file that doesn't start with the etl/ prefix
    s3.create_object(
        "test-bucket", "z/2025-05-28T00:00:00+1000/address.db", b"Hello, world!"
    )

    result = get_latest_file("test-bucket", s3, "etl/")
    assert result == "etl/2025-05-28T00:00:00+1000/address.db"

    # Get latest file retrieves the objects sorted in desc lexigraphical order
    # So here, the z/ will be sorted first and match on the empty prefix
    result = get_latest_file("test-bucket", s3, "")
    assert result == "z/2025-05-28T00:00:00+1000/address.db"
