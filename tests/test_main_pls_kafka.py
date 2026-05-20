import shutil
import sqlite3
from contextlib import contextmanager
from datetime import datetime

import pytz

import main_pls
from address_etl.pls.tables import create_tables


class FakeDatetime:
    values = iter(())

    @classmethod
    def now(cls, _tz):
        return next(cls.values)

    @classmethod
    def fromisoformat(cls, value):
        return datetime.fromisoformat(value)


class FakeDynamoResource:
    def Table(self, _name):
        return object()


class FakeLock:
    @contextmanager
    def acquire(self):
        yield


class FakeS3:
    def __init__(self, _settings):
        pass

    def bucket_exists(self, _bucket_name: str) -> bool:
        return True


def test_main_publishes_uploaded_presigned_url_to_kafka(
    monkeypatch,
    tmp_path,
):
    events = []
    recorded = {
        "metadata_start": None,
        "metadata_end": None,
        "uploads": [],
        "publish": None,
    }

    start_time = datetime(2026, 4, 23, 2, 0, 0, tzinfo=pytz.UTC)
    finish_time = datetime(2026, 4, 23, 2, 2, 30, tzinfo=pytz.UTC)
    upload_time = datetime(2026, 4, 23, 2, 2, 45, tzinfo=pytz.UTC)
    FakeDatetime.values = iter((start_time, finish_time, upload_time))

    def fake_metadata_write_start_time(cursor, start_time_str):
        recorded["metadata_start"] = start_time_str

    def fake_metadata_write_end_time(cursor, end_time_str):
        events.append("metadata_end")
        recorded["metadata_end"] = end_time_str

    def fake_compact_sqlite_database(file_path):
        events.append("compact")
        assert file_path == str(tmp_path / "pls.db")

    def fake_upload_file(bucket_name, key, file_path, s3, presigned_url_expiry_seconds):
        events.append("upload")
        recorded["uploads"].append(
            {
                "bucket_name": bucket_name,
                "key": key,
                "file_path": file_path,
                "presigned_url_expiry_seconds": presigned_url_expiry_seconds,
            }
        )
        return f"https://example.com/presigned/{key.rsplit('/', 1)[-1]}"

    def fake_export_pls_csv_zip(sqlite_db_path, zip_path):
        events.append("export")
        assert sqlite_db_path == str(tmp_path / "pls.db")
        zip_path.write_bytes(b"zip")

    def fake_publish_presigned_url(presigned_url, headers):
        recorded["publish"] = {
            "presigned_url": presigned_url,
            "headers": headers,
        }

    monkeypatch.setattr(main_pls, "datetime", FakeDatetime)
    monkeypatch.setattr(main_pls, "utc_to_brisbane_time", lambda dt: dt)
    monkeypatch.setattr(
        main_pls, "metadata_write_start_time", fake_metadata_write_start_time
    )
    monkeypatch.setattr(
        main_pls, "metadata_write_end_time", fake_metadata_write_end_time
    )
    monkeypatch.setattr(
        main_pls, "compact_sqlite_database", fake_compact_sqlite_database
    )
    monkeypatch.setattr(main_pls, "export_pls_csv_zip", fake_export_pls_csv_zip)
    monkeypatch.setattr(main_pls, "upload_file", fake_upload_file)
    monkeypatch.setattr(main_pls, "publish_presigned_url", fake_publish_presigned_url)
    monkeypatch.setattr(main_pls, "get_latest_file", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_pls, "create_tables", lambda cursor: None)
    monkeypatch.setattr(
        main_pls, "import_address_pid_mappings", lambda cursor, previous: None
    )
    monkeypatch.setattr(main_pls, "import_geocodes", lambda cursor, previous: None)
    monkeypatch.setattr(main_pls, "populate_tables", lambda cursor: None)
    monkeypatch.setattr(main_pls, "S3", FakeS3)
    monkeypatch.setattr(main_pls, "get_lock", lambda lock_id, table: FakeLock())
    monkeypatch.setattr(
        main_pls.boto3, "resource", lambda *args, **kwargs: FakeDynamoResource()
    )

    monkeypatch.setattr(main_pls.settings, "use_minio", False)
    monkeypatch.setattr(main_pls.settings, "lock_table_name", "address-etl-lock")
    monkeypatch.setattr(
        main_pls.settings, "pls_s3_bucket_name", "pls-feature-service-etl"
    )
    monkeypatch.setattr(
        main_pls.settings, "pls_sqlite_conn_str", str(tmp_path / "pls.db")
    )
    monkeypatch.setattr(main_pls.settings, "s3_presigned_url_expiry_seconds", 3600)
    monkeypatch.setattr(main_pls.settings, "kafka_enabled", True)

    main_pls.main()

    assert recorded["metadata_start"] == "2026-04-23T02:00:00+0000"
    assert recorded["metadata_end"] == "2026-04-23T02:02:30+0000"
    assert events == ["metadata_end", "compact", "upload", "export", "upload"]
    assert recorded["uploads"][0] == {
        "bucket_name": "pls-feature-service-etl",
        "key": "pls-etl/2026-04-23T02:02:30+0000/pls.db",
        "file_path": str(tmp_path / "pls.db"),
        "presigned_url_expiry_seconds": 3600,
    }
    assert recorded["uploads"][1]["bucket_name"] == "pls-feature-service-etl"
    assert recorded["uploads"][1]["key"] == "pls-etl/2026-04-23T02:02:30+0000/pls.zip"
    assert recorded["uploads"][1]["file_path"].endswith("/pls.zip")
    assert recorded["uploads"][1]["presigned_url_expiry_seconds"] == 3600
    assert recorded["publish"] == {
        "presigned_url": "https://example.com/presigned/pls.zip",
        "headers": {
            "etl-name": "pls",
            "etl-started-at": "2026-04-23T02:00:00+00:00",
            "etl-finished-at": "2026-04-23T02:02:30+00:00",
            "artifact-uploaded-at": "2026-04-23T02:02:45+00:00",
            "etl-duration-seconds": "150.000",
            "s3-bucket": "pls-feature-service-etl",
            "s3-key": "pls-etl/2026-04-23T02:02:30+0000/pls.zip",
            "presigned-url-expiry-seconds": "3600",
        },
    }


def test_main_skips_kafka_publish_when_disabled(
    monkeypatch,
    tmp_path,
):
    recorded = {
        "uploads": [],
        "publish_called": False,
    }

    start_time = datetime(2026, 4, 23, 2, 0, 0, tzinfo=pytz.UTC)
    finish_time = datetime(2026, 4, 23, 2, 2, 30, tzinfo=pytz.UTC)
    upload_time = datetime(2026, 4, 23, 2, 2, 45, tzinfo=pytz.UTC)
    FakeDatetime.values = iter((start_time, finish_time, upload_time))

    def fake_upload_file(bucket_name, key, file_path, s3, presigned_url_expiry_seconds):
        recorded["uploads"].append(
            {
                "bucket_name": bucket_name,
                "key": key,
                "file_path": file_path,
                "presigned_url_expiry_seconds": presigned_url_expiry_seconds,
            }
        )
        return "https://example.com/presigned"

    def fake_publish_presigned_url(_presigned_url, _headers):
        recorded["publish_called"] = True

    monkeypatch.setattr(main_pls, "datetime", FakeDatetime)
    monkeypatch.setattr(main_pls, "utc_to_brisbane_time", lambda dt: dt)
    monkeypatch.setattr(main_pls, "metadata_write_start_time", lambda *args: None)
    monkeypatch.setattr(main_pls, "metadata_write_end_time", lambda *args: None)
    monkeypatch.setattr(main_pls, "compact_sqlite_database", lambda *args: None)
    monkeypatch.setattr(
        main_pls,
        "export_pls_csv_zip",
        lambda _db_path, zip_path: zip_path.write_bytes(b"zip"),
    )
    monkeypatch.setattr(main_pls, "upload_file", fake_upload_file)
    monkeypatch.setattr(main_pls, "publish_presigned_url", fake_publish_presigned_url)
    monkeypatch.setattr(main_pls, "get_latest_file", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_pls, "create_tables", lambda cursor: None)
    monkeypatch.setattr(
        main_pls, "import_address_pid_mappings", lambda cursor, previous: None
    )
    monkeypatch.setattr(main_pls, "import_geocodes", lambda cursor, previous: None)
    monkeypatch.setattr(main_pls, "populate_tables", lambda cursor: None)
    monkeypatch.setattr(main_pls, "S3", FakeS3)
    monkeypatch.setattr(main_pls, "get_lock", lambda lock_id, table: FakeLock())
    monkeypatch.setattr(
        main_pls.boto3, "resource", lambda *args, **kwargs: FakeDynamoResource()
    )

    monkeypatch.setattr(main_pls.settings, "use_minio", False)
    monkeypatch.setattr(main_pls.settings, "lock_table_name", "address-etl-lock")
    monkeypatch.setattr(
        main_pls.settings, "pls_s3_bucket_name", "pls-feature-service-etl"
    )
    monkeypatch.setattr(
        main_pls.settings, "pls_sqlite_conn_str", str(tmp_path / "pls.db")
    )
    monkeypatch.setattr(main_pls.settings, "s3_presigned_url_expiry_seconds", 3600)
    monkeypatch.setattr(main_pls.settings, "kafka_enabled", False)

    main_pls.main()

    assert recorded["uploads"][0] == {
        "bucket_name": "pls-feature-service-etl",
        "key": "pls-etl/2026-04-23T02:02:30+0000/pls.db",
        "file_path": str(tmp_path / "pls.db"),
        "presigned_url_expiry_seconds": 3600,
    }
    assert recorded["uploads"][1]["bucket_name"] == "pls-feature-service-etl"
    assert recorded["uploads"][1]["key"] == "pls-etl/2026-04-23T02:02:30+0000/pls.zip"
    assert recorded["uploads"][1]["file_path"].endswith("/pls.zip")
    assert recorded["uploads"][1]["presigned_url_expiry_seconds"] == 3600
    assert recorded["publish_called"] is False


def test_main_closes_sqlite_before_uploading_wal_database(monkeypatch, tmp_path):
    recorded = {
        "uploaded_count": None,
        "wal_exists_at_upload": None,
    }

    start_time = datetime(2026, 4, 23, 2, 0, 0, tzinfo=pytz.UTC)
    finish_time = datetime(2026, 4, 23, 2, 2, 30, tzinfo=pytz.UTC)
    upload_time = datetime(2026, 4, 23, 2, 2, 45, tzinfo=pytz.UTC)
    FakeDatetime.values = iter((start_time, finish_time, upload_time))

    def fake_populate_tables(cursor):
        cursor.execute("PRAGMA journal_mode = WAL")
        cursor.execute("PRAGMA wal_autocheckpoint = 0")
        cursor.execute("CREATE TABLE records (value TEXT)")
        cursor.executemany(
            "INSERT INTO records (value) VALUES (?)",
            [("x" * 1000,) for _ in range(5000)],
        )
        cursor.connection.commit()
        assert (tmp_path / "pls.db-wal").stat().st_size > 0

    def fake_upload_file(bucket_name, key, file_path, s3, presigned_url_expiry_seconds):
        if key.endswith("/pls.zip"):
            return "https://example.com/presigned"

        wal_path = tmp_path / "pls.db-wal"
        recorded["wal_exists_at_upload"] = wal_path.exists()
        uploaded_connection = sqlite3.connect(file_path)
        try:
            recorded["uploaded_count"] = uploaded_connection.execute(
                "SELECT COUNT(*) FROM records"
            ).fetchone()[0]
        finally:
            uploaded_connection.close()
        return "https://example.com/presigned"

    monkeypatch.setattr(main_pls, "datetime", FakeDatetime)
    monkeypatch.setattr(main_pls, "utc_to_brisbane_time", lambda dt: dt)
    monkeypatch.setattr(main_pls, "metadata_write_start_time", lambda *args: None)
    monkeypatch.setattr(main_pls, "metadata_write_end_time", lambda *args: None)
    monkeypatch.setattr(
        main_pls, "compact_sqlite_database", main_pls.compact_sqlite_database
    )
    monkeypatch.setattr(
        main_pls,
        "export_pls_csv_zip",
        lambda _db_path, zip_path: zip_path.write_bytes(b"zip"),
    )
    monkeypatch.setattr(main_pls, "upload_file", fake_upload_file)
    monkeypatch.setattr(main_pls, "publish_presigned_url", lambda *args: None)
    monkeypatch.setattr(main_pls, "get_latest_file", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_pls, "create_tables", lambda cursor: None)
    monkeypatch.setattr(
        main_pls, "import_address_pid_mappings", lambda cursor, previous: None
    )
    monkeypatch.setattr(main_pls, "import_geocodes", lambda cursor, previous: None)
    monkeypatch.setattr(main_pls, "populate_tables", fake_populate_tables)
    monkeypatch.setattr(main_pls, "S3", FakeS3)
    monkeypatch.setattr(main_pls, "get_lock", lambda lock_id, table: FakeLock())
    monkeypatch.setattr(
        main_pls.boto3, "resource", lambda *args, **kwargs: FakeDynamoResource()
    )

    monkeypatch.setattr(main_pls.settings, "use_minio", False)
    monkeypatch.setattr(main_pls.settings, "lock_table_name", "address-etl-lock")
    monkeypatch.setattr(
        main_pls.settings, "pls_s3_bucket_name", "pls-feature-service-etl"
    )
    monkeypatch.setattr(
        main_pls.settings, "pls_sqlite_conn_str", str(tmp_path / "pls.db")
    )
    monkeypatch.setattr(main_pls.settings, "s3_presigned_url_expiry_seconds", 3600)
    monkeypatch.setattr(main_pls.settings, "kafka_enabled", False)

    main_pls.main()

    assert recorded == {
        "uploaded_count": 5000,
        "wal_exists_at_upload": False,
    }


def test_compact_sqlite_database_vacuums_in_place(tmp_path):
    db_path = tmp_path / "pls.db"
    connection = sqlite3.connect(db_path)
    try:
        connection.execute("CREATE TABLE records (value TEXT)")
        connection.executemany(
            "INSERT INTO records (value) VALUES (?)",
            [("x" * 1000,) for _ in range(5000)],
        )
        connection.commit()
        connection.execute("DELETE FROM records")
        connection.commit()
    finally:
        connection.close()

    size_before = db_path.stat().st_size

    main_pls.compact_sqlite_database(str(db_path))

    size_after = db_path.stat().st_size
    compacted_connection = sqlite3.connect(db_path)
    try:
        assert (
            compacted_connection.execute("PRAGMA quick_check(1)").fetchone()[0] == "ok"
        )
        assert (
            compacted_connection.execute("SELECT COUNT(*) FROM records").fetchone()[0]
            == 0
        )
    finally:
        compacted_connection.close()
    assert size_after < size_before


def test_load_previous_esri_geocodes_skips_legacy_pls_geocode_table(tmp_path):
    previous_path = tmp_path / "previous.db"
    previous_connection = sqlite3.connect(previous_path)
    previous_cursor = previous_connection.cursor()
    previous_cursor.execute(
        """
        CREATE TABLE lf_geocode_sp_survey_point (
            geocode_id TEXT PRIMARY KEY,
            geocode_type TEXT,
            address_pid TEXT,
            site_id TEXT,
            centoid_lat REAL,
            centoid_lon REAL
        )
        """
    )
    previous_cursor.execute(
        """
        INSERT INTO lf_geocode_sp_survey_point (
            geocode_id,
            geocode_type,
            address_pid,
            site_id,
            centoid_lat,
            centoid_lon
        )
        VALUES ('geo-1', 'PC', '100', 'site-1', -27.1, 153.1)
        """
    )
    previous_connection.commit()
    previous_connection.close()

    connection = sqlite3.connect(":memory:")
    cursor = connection.cursor()
    create_tables(cursor)
    cursor.execute("ATTACH DATABASE ? AS previous", (str(previous_path),))

    loaded = main_pls.load_previous_esri_geocodes(cursor)

    assert loaded is False
    assert cursor.execute("SELECT COUNT(*) FROM esri_geocodes").fetchone()[0] == 0

    cursor.execute("DETACH DATABASE previous")
    connection.close()


def test_load_previous_esri_geocodes_copies_raw_cache(tmp_path):
    previous_path = tmp_path / "previous.db"
    previous_connection = sqlite3.connect(previous_path)
    previous_cursor = previous_connection.cursor()
    previous_cursor.execute(
        """
        CREATE TABLE esri_geocodes (
            geocode_id TEXT PRIMARY KEY,
            geocode_type TEXT,
            address_pid TEXT,
            centoid_lat REAL,
            centoid_lon REAL
        )
        """
    )
    previous_cursor.execute(
        """
        INSERT INTO esri_geocodes (
            geocode_id,
            geocode_type,
            address_pid,
            centoid_lat,
            centoid_lon
        )
        VALUES ('geo-1', 'PC', '100', -27.1, 153.1)
        """
    )
    previous_connection.commit()
    previous_connection.close()

    connection = sqlite3.connect(":memory:")
    cursor = connection.cursor()
    create_tables(cursor)
    cursor.execute("ATTACH DATABASE ? AS previous", (str(previous_path),))

    loaded = main_pls.load_previous_esri_geocodes(cursor)

    assert loaded is True
    assert cursor.execute(
        """
        SELECT geocode_id, geocode_type, address_pid, centoid_lat, centoid_lon
        FROM esri_geocodes
        """
    ).fetchone() == ("geo-1", "PC", "100", -27.1, 153.1)

    cursor.execute("DETACH DATABASE previous")
    connection.close()


def test_main_full_pulls_geocodes_when_previous_has_no_raw_cache(
    monkeypatch,
    tmp_path,
):
    recorded = {
        "address_pid_previous": None,
        "geocode_previous": "unset",
    }

    previous_path = tmp_path / "previous.db"
    previous_connection = sqlite3.connect(previous_path)
    previous_connection.row_factory = main_pls.dict_row_factory
    previous_cursor = previous_connection.cursor()
    create_tables(previous_cursor)
    previous_cursor.execute(
        "INSERT INTO metadata (start_time) VALUES ('2026-04-22T12:00:00+1000')"
    )
    previous_cursor.execute("DROP TABLE esri_geocodes")
    previous_connection.commit()
    previous_connection.close()

    downloaded_previous_path = tmp_path / "downloaded_previous.db"

    start_time = datetime(2026, 4, 23, 2, 0, 0, tzinfo=pytz.UTC)
    finish_time = datetime(2026, 4, 23, 2, 2, 30, tzinfo=pytz.UTC)
    upload_time = datetime(2026, 4, 23, 2, 2, 45, tzinfo=pytz.UTC)
    FakeDatetime.values = iter((start_time, finish_time, upload_time))

    def fake_download_file(_bucket_name, _key, file_path, _s3):
        shutil.copyfile(previous_path, file_path)

    def fake_import_address_pid_mappings(_cursor, previous):
        recorded["address_pid_previous"] = previous

    def fake_import_geocodes(_cursor, previous):
        recorded["geocode_previous"] = previous

    def fake_get_latest_file(_bucket_name, _s3, prefix="", suffix=None):
        assert prefix == "pls-etl/"
        assert suffix == "/pls.db"
        return "old.db"

    monkeypatch.setattr(main_pls, "datetime", FakeDatetime)
    monkeypatch.setattr(main_pls, "utc_to_brisbane_time", lambda dt: dt)
    monkeypatch.setattr(main_pls, "download_file", fake_download_file)
    monkeypatch.setattr(main_pls, "get_latest_file", fake_get_latest_file)
    monkeypatch.setattr(
        main_pls, "import_address_pid_mappings", fake_import_address_pid_mappings
    )
    monkeypatch.setattr(main_pls, "import_geocodes", fake_import_geocodes)
    monkeypatch.setattr(main_pls, "populate_tables", lambda cursor: None)
    monkeypatch.setattr(
        main_pls,
        "export_pls_csv_zip",
        lambda _db_path, zip_path: zip_path.write_bytes(b"zip"),
    )
    monkeypatch.setattr(
        main_pls, "upload_file", lambda *args, **kwargs: "https://example.com/presigned"
    )
    monkeypatch.setattr(main_pls, "publish_presigned_url", lambda *args: None)
    monkeypatch.setattr(main_pls, "S3", FakeS3)
    monkeypatch.setattr(main_pls, "get_lock", lambda lock_id, table: FakeLock())
    monkeypatch.setattr(
        main_pls.boto3, "resource", lambda *args, **kwargs: FakeDynamoResource()
    )
    monkeypatch.setattr(main_pls, "PREVIOUS_DB_PATH", str(downloaded_previous_path))

    monkeypatch.setattr(main_pls.settings, "use_minio", False)
    monkeypatch.setattr(main_pls.settings, "lock_table_name", "address-etl-lock")
    monkeypatch.setattr(
        main_pls.settings, "pls_s3_bucket_name", "pls-feature-service-etl"
    )
    monkeypatch.setattr(
        main_pls.settings, "pls_sqlite_conn_str", str(tmp_path / "pls.db")
    )
    monkeypatch.setattr(main_pls.settings, "s3_presigned_url_expiry_seconds", 3600)
    monkeypatch.setattr(main_pls.settings, "kafka_enabled", False)

    main_pls.main()

    assert recorded["address_pid_previous"] == datetime.fromisoformat(
        "2026-04-22T12:00:00+1000"
    )
    assert recorded["geocode_previous"] is None
