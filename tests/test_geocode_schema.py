from datetime import datetime, timezone
import sqlite3

import address_etl.geocode as geocode_module
from address_etl.geocode import (
    GeocodeLayerSchema,
    build_geocode_type_code_query,
    build_geocode_where_clause,
    get_geocode_layer_schema,
    get_layer_url,
    import_geocodes,
    insert_geocodes,
    load_geocode_type_codes,
    normalize_geocode_feature,
    normalize_geocode_type,
    parse_geocode_type_code_bindings,
    save_geocode_type_codes,
)
from address_etl.sqlite_dict_factory import dict_row_factory
from address_etl.tables import create_geocode_type_code_table
from address_etl.pls.tables import create_tables


def test_normalize_geocode_type_keeps_legacy_code():
    assert normalize_geocode_type("PC") == "PC"


def test_normalize_geocode_type_maps_uri_to_legacy_code():
    assert (
        normalize_geocode_type(
            "https://linked.data.gov.au/def/geocode-types/property-centroid",
            {"https://linked.data.gov.au/def/geocode-types/property-centroid": "PC"},
        )
        == "PC"
    )


def test_normalize_geocode_type_falls_back_to_initialism():
    assert (
        normalize_geocode_type(
            "https://linked.data.gov.au/def/geocode-types/example-new-geocode"
        )
        == "ENG"
    )


def test_build_geocode_type_code_query_includes_values_clause():
    query = build_geocode_type_code_query(
        [
            "https://linked.data.gov.au/def/geocode-types/property-centroid",
            "https://linked.data.gov.au/def/geocode-types/driveway-frontage",
        ]
    )

    assert "VALUES ?type" in query
    assert "<https://linked.data.gov.au/def/geocode-types/property-centroid>" in query
    assert "<https://linked.data.gov.au/def/geocode-types/driveway-frontage>" in query
    assert "skos:notation ?code" in query


def test_parse_geocode_type_code_bindings():
    assert parse_geocode_type_code_bindings(
        {
            "results": {
                "bindings": [
                    {
                        "type": {
                            "value": "https://linked.data.gov.au/def/geocode-types/property-centroid"
                        },
                        "code": {"value": "PC"},
                    },
                    {
                        "type": {
                            "value": "https://linked.data.gov.au/def/geocode-types/driveway-frontage"
                        },
                        "code": {"value": "DF"},
                    },
                ]
            }
        }
    ) == {
        "https://linked.data.gov.au/def/geocode-types/property-centroid": "PC",
        "https://linked.data.gov.au/def/geocode-types/driveway-frontage": "DF",
    }


def test_get_geocode_layer_schema_supports_new_field_names():
    schema = get_geocode_layer_schema(
        {
            "objectIdField": "objectid",
            "fields": [
                {"name": "objectid"},
                {"name": "pid"},
                {"name": "type"},
                {"name": "source"},
            ],
        }
    )

    assert schema == GeocodeLayerSchema(
        object_id_field="objectid",
        address_pid_field="pid",
        geocode_type_field="type",
        geocode_source_field="source",
        geocode_status_field=None,
        last_edited_field=None,
    )


def test_build_geocode_where_clause_for_new_schema_without_incremental_field():
    schema = GeocodeLayerSchema(
        object_id_field="objectid",
        address_pid_field="pid",
        geocode_type_field="type",
        geocode_source_field="source",
        geocode_status_field=None,
        last_edited_field=None,
    )

    assert build_geocode_where_clause(schema, "2026-04-14 00:00:00") == "1=1"


def test_build_geocode_where_clause_for_incremental_schema():
    schema = GeocodeLayerSchema(
        object_id_field="objectid",
        address_pid_field="pid",
        geocode_type_field="type",
        geocode_source_field="source",
        geocode_status_field=None,
        last_edited_field="last_edited_date",
    )

    assert (
        build_geocode_where_clause(schema, "2026-04-14 00:00:00")
        == "last_edited_date >= DATE '2026-04-14 00:00:00'"
    )


def test_build_geocode_where_clause_for_full_load_with_incremental_schema():
    schema = GeocodeLayerSchema(
        object_id_field="objectid",
        address_pid_field="pid",
        geocode_type_field="type",
        geocode_source_field="source",
        geocode_status_field=None,
        last_edited_field="last_edited_date",
    )

    assert build_geocode_where_clause(schema, None) == "1=1"


def test_normalize_geocode_feature_for_new_schema():
    schema = GeocodeLayerSchema(
        object_id_field="objectid",
        address_pid_field="pid",
        geocode_type_field="type",
        geocode_source_field="source",
        geocode_status_field=None,
        last_edited_field=None,
    )

    assert normalize_geocode_feature(
        {
            "attributes": {
                "objectid": 1,
                "pid": 444541,
                "type": "https://linked.data.gov.au/def/geocode-types/property-centroid",
            },
            "geometry": {"x": 153.1, "y": -27.6},
        },
        schema,
        {"https://linked.data.gov.au/def/geocode-types/property-centroid": "PC"},
    ) == {
        "attributes": {
            "objectid": "1",
            "address_pid": "444541",
            "geocode_type": "PC",
        },
        "geometry": {"x": 153.1, "y": -27.6},
    }


def test_get_layer_url_removes_query_suffix():
    assert (
        get_layer_url(
            "https://qportal.information.qld.gov.au/arcgis/rest/services/LOC/Address_Geocodes_UAT/FeatureServer/0/query"
        )
        == "https://qportal.information.qld.gov.au/arcgis/rest/services/LOC/Address_Geocodes_UAT/FeatureServer/0"
    )


def test_geocode_type_code_round_trip():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = dict_row_factory
    try:
        cursor = connection.cursor()
        create_geocode_type_code_table(cursor)
        save_geocode_type_codes(
            cursor,
            {
                "https://linked.data.gov.au/def/geocode-types/property-centroid": "PC",
                "https://linked.data.gov.au/def/geocode-types/driveway-frontage": "DF",
            },
        )

        assert load_geocode_type_codes(cursor) == {
            "https://linked.data.gov.au/def/geocode-types/property-centroid": "PC",
            "https://linked.data.gov.au/def/geocode-types/driveway-frontage": "DF",
        }
    finally:
        connection.close()


def test_save_geocode_type_codes_updates_existing_rows():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = dict_row_factory
    try:
        cursor = connection.cursor()
        create_geocode_type_code_table(cursor)
        save_geocode_type_codes(
            cursor,
            {"https://linked.data.gov.au/def/geocode-types/property-centroid": "PC"},
        )
        save_geocode_type_codes(
            cursor,
            {"https://linked.data.gov.au/def/geocode-types/property-centroid": "PX"},
        )

        assert load_geocode_type_codes(cursor) == {
            "https://linked.data.gov.au/def/geocode-types/property-centroid": "PX"
        }
    finally:
        connection.close()


def test_insert_geocodes_upserts_into_esri_geocode_cache():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = dict_row_factory
    try:
        cursor = connection.cursor()
        create_tables(cursor)
        insert_geocodes(
            cursor,
            [
                {
                    "attributes": {
                        "objectid": "geo-1",
                        "geocode_type": "PC",
                        "address_pid": "100",
                    },
                    "geometry": {"y": -27.0, "x": 153.0},
                }
            ],
        )
        insert_geocodes(
            cursor,
            [
                {
                    "attributes": {
                        "objectid": "geo-1",
                        "geocode_type": "DF",
                        "address_pid": "200",
                    },
                    "geometry": {"y": -28.0, "x": 152.0},
                }
            ],
        )

        assert cursor.execute(
            """
            SELECT geocode_id, geocode_type, address_pid, centoid_lat, centoid_lon
            FROM esri_geocodes
            """
        ).fetchall() == [
            {
                "geocode_id": "geo-1",
                "geocode_type": "DF",
                "address_pid": "200",
                "centoid_lat": -28.0,
                "centoid_lon": 152.0,
            }
        ]
        assert (
            cursor.execute(
                "SELECT COUNT(*) AS count FROM lf_geocode_sp_survey_point"
            ).fetchone()["count"]
            == 0
        )
    finally:
        connection.close()


def test_import_geocodes_full_refreshes_when_incremental_cache_count_mismatches(
    monkeypatch,
):
    importer_dates = []

    class FakeClient:
        def __init__(self, timeout):
            self.timeout = timeout

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return False

    class FakeGeocodeImporter:
        def __init__(self, cursor, _client, esri_date):
            self.cursor = cursor
            self.esri_date = esri_date
            self.requires_full_refresh = False
            self.geocode_count = 2
            importer_dates.append(esri_date)

        def import_geocodes(self):
            if self.esri_date is not None:
                return

            insert_geocodes(
                self.cursor,
                [
                    {
                        "attributes": {
                            "objectid": "geo-1",
                            "geocode_type": "PC",
                            "address_pid": "100",
                        },
                        "geometry": {"y": -27.0, "x": 153.0},
                    },
                    {
                        "attributes": {
                            "objectid": "geo-2",
                            "geocode_type": "DF",
                            "address_pid": "200",
                        },
                        "geometry": {"y": -28.0, "x": 152.0},
                    },
                ],
            )
            self.cursor.connection.commit()

    monkeypatch.setattr(geocode_module.httpx, "Client", FakeClient)
    monkeypatch.setattr(geocode_module, "GeocodeImporter", FakeGeocodeImporter)

    connection = sqlite3.connect(":memory:")
    connection.row_factory = dict_row_factory
    try:
        cursor = connection.cursor()
        create_tables(cursor)
        insert_geocodes(
            cursor,
            [
                {
                    "attributes": {
                        "objectid": "stale-geocode",
                        "geocode_type": "PC",
                        "address_pid": "999",
                    },
                    "geometry": {"y": -29.0, "x": 151.0},
                }
            ],
        )
        connection.commit()

        import_geocodes(cursor, datetime(2026, 4, 23, tzinfo=timezone.utc))

        assert importer_dates == ["2026-04-23 00:00:00", None]
        assert cursor.execute(
            """
            SELECT geocode_id, geocode_type, address_pid
            FROM esri_geocodes
            ORDER BY geocode_id
            """
        ).fetchall() == [
            {"geocode_id": "geo-1", "geocode_type": "PC", "address_pid": "100"},
            {"geocode_id": "geo-2", "geocode_type": "DF", "address_pid": "200"},
        ]
    finally:
        connection.close()
