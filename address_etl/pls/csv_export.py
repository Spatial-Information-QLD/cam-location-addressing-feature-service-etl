import csv
import logging
import sqlite3
import tempfile
import zipfile
from pathlib import Path

logger = logging.getLogger(__name__)

CSV_EXPORTS: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    (
        "lf_address_with_parcel.csv",
        "lf_address",
        (
            "parcel_id",
            "addr_id",
            "addr_status_code",
            "unit_type",
            "unit_no",
            "unit_suffix",
            "level_type",
            "level_no",
            "level_suffix",
            "street_no_first",
            "street_no_first_suffix",
            "street_no_last",
            "street_no_last_suffix",
            "road_id",
            "site_id",
            "location_desc",
            "address_standard",
        ),
    ),
    (
        "lf_geocode_sp_survey_point.csv",
        "lf_geocode_sp_survey_point",
        ("geocode_id", "geocode_type", "site_id", "centoid_lat", "centoid_lon"),
    ),
    ("lf_parcel.csv", "lf_parcel", ("parcel_id", "plan_no", "lot_no")),
    (
        "lf_place_name.csv",
        "lf_place_name",
        (
            "place_name_id",
            "pl_name_status_code",
            "pl_name_type_code",
            "pl_name",
            "site_id",
        ),
    ),
    (
        "lf_road.csv",
        "lf_road",
        (
            "road_id",
            "road_cat",
            "road_name",
            "road_name_suffix",
            "road_name_type",
            "locality_code",
            "road_cat_desc",
        ),
    ),
    ("lf_site.csv", "lf_site", ("site_id", "parent_site_id", "site_type", "parcel_id")),
    (
        "locality.csv",
        "locality",
        (
            "locality_code",
            "locality_name",
            "locality_type",
            "la_code",
            "state",
            "status",
        ),
    ),
    ("local_auth.csv", "local_auth", ("la_code", "la_name")),
    ("metadata.csv", "metadata", ("id", "start_time", "end_time")),
)


def export_pls_csv_zip(sqlite_db_path: str | Path, zip_path: str | Path) -> None:
    sqlite_db_path = Path(sqlite_db_path)
    zip_path = Path(zip_path)
    logger.info("Exporting PLS CSV ZIP from %s to %s", sqlite_db_path, zip_path)

    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as csv_dir_name:
        csv_dir = Path(csv_dir_name)
        connection = sqlite3.connect(sqlite_db_path)
        try:
            cursor = connection.cursor()
            for csv_name, table_name, columns in CSV_EXPORTS:
                csv_path = csv_dir / csv_name
                _export_csv(cursor, table_name, columns, csv_path)

            with zipfile.ZipFile(
                zip_path, "w", compression=zipfile.ZIP_DEFLATED
            ) as zip_file:
                for csv_name, _, _ in CSV_EXPORTS:
                    zip_file.write(csv_dir / csv_name, arcname=csv_name)
        finally:
            connection.close()

    logger.info("Exported PLS CSV ZIP to %s", zip_path)


def _export_csv(
    cursor: sqlite3.Cursor, table_name: str, columns: tuple[str, ...], csv_path: Path
) -> None:
    column_sql = ", ".join(columns)
    cursor.execute(f"SELECT {column_sql} FROM {table_name}")

    with csv_path.open("w", newline="") as csv_file:
        writer = csv.writer(csv_file, lineterminator="\n")
        writer.writerows(cursor)
