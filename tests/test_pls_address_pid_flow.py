import sqlite3

import pytest

from address_etl.id_map import text_to_id_for_pk
from address_etl.pls.tables import (
    build_address_insert_data,
    create_tables,
    generate_pls_geocodes,
    prune_addresses_without_pid_mapping,
    validate_foreign_keys,
)
from address_etl.sqlite_dict_factory import dict_row_factory


def connection():
    db = sqlite3.connect(":memory:")
    db.row_factory = dict_row_factory
    cursor = db.cursor()
    create_tables(cursor)
    cursor.execute("PRAGMA foreign_keys = OFF")
    db.commit()
    return db


def connection_with_foreign_keys():
    db = sqlite3.connect(":memory:")
    db.row_factory = dict_row_factory
    create_tables(db.cursor())
    return db


def test_validate_foreign_keys_logs_existing_violations_after_reenable(caplog):
    db = connection()
    try:
        cursor = db.cursor()
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
                "missing-parcel",
                "C",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                "missing-road",
                "missing-site",
                None,
                "STD",
            ),
        )
        db.commit()

        validate_foreign_keys(cursor)

        assert cursor.execute("PRAGMA foreign_keys").fetchone()["foreign_keys"] == 1
        assert "SQLite foreign key check found violations before upload" in caplog.text
        assert "table=lf_address" in caplog.text
    finally:
        db.close()


def test_build_address_insert_data_skips_unmapped_addresses():
    rows = [
        {
            "addr_iri": {"value": "https://example.com/address/1"},
            "addr_id": {"value": "addr-1"},
            "parcel_id": {"value": "parcel-1"},
            "addr_status_code": {"value": "C"},
            "road_id": {"value": "road-1"},
            "site_id": {"value": "site-1"},
            "address_standard": {"value": "STD"},
        },
        {
            "addr_iri": {"value": "https://example.com/address/2"},
            "addr_id": {"value": "addr-2"},
            "parcel_id": {"value": "parcel-2"},
            "addr_status_code": {"value": "C"},
            "road_id": {"value": "road-2"},
            "site_id": {"value": "site-2"},
            "address_standard": {"value": "STD"},
        },
    ]

    insert_data, missing_iris = build_address_insert_data(
        rows,
        {"https://example.com/address/1": "100"},
    )

    assert insert_data == [
        (
            "addr-1",
            "100",
            "parcel-1",
            "C",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            "road-1",
            "site-1",
            None,
            "STD",
        )
    ]
    assert missing_iris == ["https://example.com/address/2"]


def test_prune_addresses_without_pid_mapping():
    db = connection()
    try:
        cursor = db.cursor()
        cursor.execute(
            "INSERT INTO address_iri_pid_map (address_iri, address_pid) VALUES (?, ?)",
            ("https://example.com/address/1", "100"),
        )
        cursor.executemany(
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
            [
                (
                    "addr-1",
                    "100",
                    "parcel-1",
                    "C",
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    "road-1",
                    "site-1",
                    None,
                    "STD",
                ),
                (
                    "addr-2",
                    "200",
                    "parcel-2",
                    "C",
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    "road-2",
                    "site-2",
                    None,
                    "STD",
                ),
            ],
        )
        db.commit()

        prune_addresses_without_pid_mapping(cursor)

        assert cursor.execute(
            "SELECT addr_id, address_pid FROM lf_address ORDER BY addr_id"
        ).fetchall() == [{"addr_id": "addr-1", "address_pid": "100"}]
    finally:
        db.close()


def test_id_mapping_cascades_after_geocode_site_update():
    db = connection_with_foreign_keys()
    try:
        cursor = db.cursor()
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
                road_name,
                locality_code,
                road_cat_desc
            ) VALUES (?, ?, ?, ?)
            """,
            ("road-iri", "ROAD", "LOC", "R"),
        )
        cursor.execute(
            "INSERT INTO lf_parcel (parcel_id, plan_no, lot_no) VALUES (?, ?, ?)",
            ("parcel-iri", "PLAN", "1"),
        )
        cursor.execute(
            "INSERT INTO lf_site (site_id, site_type, parcel_id) VALUES (?, ?, ?)",
            ("site-iri", "P", "parcel-iri"),
        )
        cursor.execute(
            """
            INSERT INTO lf_address (
                addr_id,
                address_pid,
                parcel_id,
                addr_status_code,
                road_id,
                site_id,
                address_standard
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "addr-iri",
                "pid-1",
                "parcel-iri",
                "C",
                "road-iri",
                "site-iri",
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
            ("geo-1", "PC", "pid-1", -27.0, 153.0),
        )
        db.commit()

        generate_pls_geocodes(cursor)
        assert cursor.execute("PRAGMA foreign_keys").fetchone()["foreign_keys"] == 1

        text_to_id_for_pk("lf_road_id_map", "lf_road", "road_id", cursor)
        text_to_id_for_pk("lf_parcel_id_map", "lf_parcel", "parcel_id", cursor)
        text_to_id_for_pk("lf_site_id_map", "lf_site", "site_id", cursor)
        text_to_id_for_pk("lf_address_id_map", "lf_address", "addr_id", cursor)

        address_row = cursor.execute(
            "SELECT addr_id, parcel_id, road_id, site_id FROM lf_address"
        ).fetchone()
        geocode_row = cursor.execute(
            "SELECT geocode_id, site_id FROM lf_geocode_sp_survey_point"
        ).fetchone()

        assert address_row == {
            "addr_id": "1",
            "parcel_id": "1",
            "road_id": "1",
            "site_id": "1",
        }
        assert geocode_row == {"geocode_id": "geo-1", "site_id": "1"}
        assert cursor.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        db.close()


def test_generate_pls_geocodes_keeps_unmatched_raw_geocodes():
    db = connection()
    try:
        cursor = db.cursor()
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
                "100",
                "parcel-1",
                "C",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                "road-1",
                "site-1",
                None,
                "STD",
            ),
        )
        cursor.executemany(
            """
            INSERT INTO esri_geocodes (
                geocode_id,
                geocode_type,
                address_pid,
                centoid_lat,
                centoid_lon
            ) VALUES (?, ?, ?, ?, ?)
            """,
            [
                ("geo-1", "PC", "100", -27.0, 153.0),
                ("geo-2", "PC", "999", -28.0, 152.0),
            ],
        )
        db.commit()

        generate_pls_geocodes(cursor)

        assert cursor.execute(
            """
            SELECT geocode_id, address_pid, site_id
            FROM lf_geocode_sp_survey_point
            ORDER BY geocode_id
            """
        ).fetchall() == [
            {"geocode_id": "geo-1", "address_pid": "100", "site_id": "site-1"}
        ]
        assert cursor.execute(
            "SELECT geocode_id FROM esri_geocodes ORDER BY geocode_id"
        ).fetchall() == [{"geocode_id": "geo-1"}, {"geocode_id": "geo-2"}]
    finally:
        db.close()


def test_generate_pls_geocodes_emits_previously_unmatched_raw_geocode():
    db = connection()
    try:
        cursor = db.cursor()
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
            ("geo-1", "PC", "100", -27.0, 153.0),
        )
        db.commit()

        generate_pls_geocodes(cursor)
        assert (
            cursor.execute("SELECT COUNT(*) AS count FROM esri_geocodes").fetchone()[
                "count"
            ]
            == 1
        )
        assert (
            cursor.execute(
                "SELECT COUNT(*) AS count FROM lf_geocode_sp_survey_point"
            ).fetchone()["count"]
            == 0
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
                road_name,
                locality_code,
                road_cat_desc
            ) VALUES (?, ?, ?, ?)
            """,
            ("road-1", "ROAD", "LOC", "R"),
        )
        cursor.execute(
            "INSERT INTO lf_parcel (parcel_id, plan_no, lot_no) VALUES (?, ?, ?)",
            ("parcel-1", "PLAN", "1"),
        )
        cursor.execute(
            "INSERT INTO lf_site (site_id, site_type, parcel_id) VALUES (?, ?, ?)",
            ("site-1", "P", "parcel-1"),
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
                "100",
                "parcel-1",
                "C",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                "road-1",
                "site-1",
                None,
                "STD",
            ),
        )
        db.commit()

        generate_pls_geocodes(cursor)
        assert cursor.execute(
            """
            SELECT geocode_id, address_pid, site_id
            FROM lf_geocode_sp_survey_point
            """
        ).fetchall() == [
            {"geocode_id": "geo-1", "address_pid": "100", "site_id": "site-1"}
        ]
    finally:
        db.close()
