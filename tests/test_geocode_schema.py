import sqlite3

from address_etl.geocode import (
    GeocodeLayerSchema,
    build_geocode_type_code_query,
    build_geocode_where_clause,
    get_geocode_layer_schema,
    get_layer_url,
    load_geocode_type_codes,
    normalize_geocode_feature,
    normalize_geocode_type,
    parse_geocode_type_code_bindings,
    save_geocode_type_codes,
)
from address_etl.sqlite_dict_factory import dict_row_factory
from address_etl.tables import create_geocode_type_code_table


def test_normalize_geocode_type_keeps_legacy_code():
    assert normalize_geocode_type("PC") == "PC"


def test_normalize_geocode_type_maps_uri_to_legacy_code():
    assert (
        normalize_geocode_type(
            "https://linked.data.gov.au/def/geocode-types/property-centroid",
            {
                "https://linked.data.gov.au/def/geocode-types/property-centroid": "PC"
            },
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
