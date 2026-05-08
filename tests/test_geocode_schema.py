import sqlite3

from address_etl.geocode import (
    GeocodeLayerSchema,
    build_geocode_type_code_query,
    build_geocode_where_clause,
    get_geocode_layer_schema,
    get_layer_url,
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
