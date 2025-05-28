import sqlite3
import tempfile

from address_etl.s3 import S3, download_file, upload_file


def test_sqlite_to_s3(s3: S3):
    bucket_name = "test-bucket"
    s3.create_bucket(bucket_name)

    with tempfile.NamedTemporaryFile(suffix=".sqlite") as temp_file:
        conn = sqlite3.connect(temp_file.name)
        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("INSERT INTO test (name) VALUES (?)", ("test",))
        conn.commit()

        upload_file(bucket_name, "test.sqlite", temp_file.name, s3)

        with tempfile.NamedTemporaryFile(suffix=".sqlite") as temp_file:
            download_file(bucket_name, "test.sqlite", temp_file.name, s3)

            conn = sqlite3.connect(temp_file.name)
            result = conn.execute("SELECT name FROM test")
            assert result.fetchone() == ("test",)
