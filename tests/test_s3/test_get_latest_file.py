from address_etl.s3 import S3, get_latest_file


class FakeS3:
    def __init__(self):
        self.calls = []

    def list_objects(self, bucket_name: str, prefix: str = ""):
        self.calls.append((bucket_name, prefix))
        return [{"Key": f"{prefix}2025-05-28T00:00:00+1000/pls.db"}]


def test_get_latest_file(s3: S3):
    s3.create_bucket("test-bucket")
    s3.create_object(
        "test-bucket", "pls-etl/2025-05-28T00:00:00+1000/pls.db", b"Hello, world!"
    )
    s3.create_object(
        "test-bucket", "pls-etl/2025-05-27T00:00:00+1000/pls.db", b"Hello, world!"
    )

    # Create a file that doesn't start with the pls-etl/ prefix
    s3.create_object(
        "test-bucket", "z/2025-05-28T00:00:00+1000/pls.db", b"Hello, world!"
    )

    result = get_latest_file("test-bucket", s3, "pls-etl/")
    assert result == "pls-etl/2025-05-28T00:00:00+1000/pls.db"

    # Get latest file retrieves the objects sorted in desc lexigraphical order
    # So here, the z/ will be sorted first and match on the empty prefix
    result = get_latest_file("test-bucket", s3, "")
    assert result == "z/2025-05-28T00:00:00+1000/pls.db"


def test_get_latest_file_passes_prefix_to_s3():
    s3 = FakeS3()

    result = get_latest_file("test-bucket", s3, "pls-etl/")

    assert result == "pls-etl/2025-05-28T00:00:00+1000/pls.db"
    assert s3.calls == [("test-bucket", "pls-etl/")]
