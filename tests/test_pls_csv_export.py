import csv
import sqlite3
import zipfile
from io import StringIO

from address_etl.pls.csv_export import CSV_EXPORTS, export_pls_csv_zip
from address_etl.pls.tables import create_tables, generate_pls_geocodes
from address_etl.sqlite_dict_factory import dict_row_factory


def test_export_pls_csv_zip_contains_required_no_header_csvs(tmp_path):
    db_path = tmp_path / "pls.db"
    zip_path = tmp_path / "pls.zip"
    connection = sqlite3.connect(db_path)
    connection.row_factory = dict_row_factory
    try:
        cursor = connection.cursor()
        create_tables(cursor)
        cursor.execute("PRAGMA foreign_keys = OFF")
        cursor.execute(
            "INSERT INTO metadata (id, start_time, end_time) VALUES (?, ?, ?)",
            (1, "2026-04-23T12:00:00+1000", "2026-04-23T12:02:30+1000"),
        )
        cursor.execute(
            "INSERT INTO local_auth (la_code, la_name) VALUES (?, ?)", (1, "LGA")
        )
        cursor.execute(
            """
            INSERT INTO locality (
                locality_code,
                locality_name,
                locality_type,
                la_code,
                state,
                status
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("LOC", "LOCALITY", "LOC", 1, "QLD", "C"),
        )
        cursor.execute(
            """
            INSERT INTO lf_road (
                road_id,
                road_cat,
                road_name,
                road_name_suffix,
                road_name_type,
                locality_code,
                road_cat_desc
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("road-1", None, "ROAD", None, "ST", "LOC", "R"),
        )
        cursor.execute(
            "INSERT INTO lf_parcel (parcel_id, plan_no, lot_no) VALUES (?, ?, ?)",
            ("parcel-1", "PLAN", "1"),
        )
        cursor.execute(
            """
            INSERT INTO lf_site (
                site_id,
                parent_site_id,
                site_type,
                parcel_id
            ) VALUES (?, ?, ?, ?)
            """,
            ("site-1", None, "P", "parcel-1"),
        )
        cursor.execute(
            """
            INSERT INTO lf_place_name (
                place_name_id,
                pl_name_status_code,
                pl_name_type_code,
                pl_name,
                site_id
            ) VALUES (?, ?, ?, ?, ?)
            """,
            ("place-1", "C", "SUB", "PLACE", "site-1"),
        )
        cursor.execute(
            """
            INSERT INTO lf_address (
                addr_id,
                address_pid,
                parcel_id,
                addr_status_code,
                unit_type,
                unit_no,
                unit_suffix,
                level_type,
                level_no,
                level_suffix,
                street_no_first,
                street_no_first_suffix,
                street_no_last,
                street_no_last_suffix,
                road_id,
                site_id,
                location_desc,
                address_standard
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "addr-1",
                "pid-1",
                "parcel-1",
                "C",
                None,
                "12",
                None,
                None,
                None,
                None,
                "1",
                None,
                None,
                None,
                "road-1",
                "site-1",
                None,
                "STD",
            ),
        )
        cursor.execute(
            """
            INSERT INTO esri_geocodes (
                geocode_id,
                geocode_type,
                address_pid,
                centoid_lat,
                centoid_lon
            ) VALUES (?, ?, ?, ?, ?)
            """,
            ("geo-1", "PC", "pid-1", -27.123456789, 153.987654321),
        )
        connection.commit()

        generate_pls_geocodes(cursor)
    finally:
        connection.close()

    export_pls_csv_zip(db_path, zip_path)

    with zipfile.ZipFile(zip_path) as zip_file:
        assert sorted(zip_file.namelist()) == sorted(
            csv_name for csv_name, _, _ in CSV_EXPORTS
        )

        address_rows = _read_csv_rows(zip_file, "lf_address_with_parcel.csv")
        assert address_rows == [
            [
                "parcel-1",
                "addr-1",
                "C",
                "",
                "12",
                "",
                "",
                "",
                "",
                "1",
                "",
                "",
                "",
                "road-1",
                "site-1",
                "",
                "STD",
            ]
        ]

        geocode_rows = _read_csv_rows(zip_file, "lf_geocode_sp_survey_point.csv")
        assert geocode_rows == [
            ["geo-1", "PC", "site-1", "-27.12345679", "153.98765432"]
        ]
        assert _read_csv_rows(zip_file, "metadata.csv") == [
            ["1", "2026-04-23T12:00:00+1000", "2026-04-23T12:02:30+1000"]
        ]

        local_auth_text = zip_file.read("local_auth.csv").decode()
        assert "la_code" not in local_auth_text
        assert local_auth_text == "1,LGA\n"


def _read_csv_rows(zip_file: zipfile.ZipFile, csv_name: str) -> list[list[str]]:
    csv_text = zip_file.read(csv_name).decode()
    return list(csv.reader(StringIO(csv_text)))
