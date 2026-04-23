import sqlite3

from address_etl.address_iri_pid_map import (
    AddressIriPidLayerSchema,
    build_address_iri_pid_where_clause,
    get_address_iri_pid_layer_schema,
    load_address_pid_mappings,
    normalize_address_iri_pid_feature,
    save_address_pid_mappings,
)
from address_etl.sqlite_dict_factory import dict_row_factory
from address_etl.tables import create_address_iri_pid_map_table


def test_get_address_iri_pid_layer_schema_supports_live_field_names():
    schema = get_address_iri_pid_layer_schema(
        {
            "objectIdField": "objectid",
            "fields": [
                {"name": "objectid"},
                {"name": "iri"},
                {"name": "pid"},
                {"name": "last_edited_date"},
            ],
        }
    )

    assert schema == AddressIriPidLayerSchema(
        object_id_field="objectid",
        address_iri_field="iri",
        address_pid_field="pid",
        last_edited_field="last_edited_date",
    )


def test_build_address_iri_pid_where_clause():
    schema = AddressIriPidLayerSchema(
        object_id_field="objectid",
        address_iri_field="iri",
        address_pid_field="pid",
        last_edited_field="last_edited_date",
    )

    assert build_address_iri_pid_where_clause(schema, None) == "1=1"
    assert (
        build_address_iri_pid_where_clause(schema, "2026-04-14 00:00:00")
        == "last_edited_date >= DATE '2026-04-14 00:00:00'"
    )


def test_normalize_address_iri_pid_feature():
    schema = AddressIriPidLayerSchema(
        object_id_field="objectid",
        address_iri_field="iri",
        address_pid_field="pid",
        last_edited_field="last_edited_date",
    )

    assert normalize_address_iri_pid_feature(
        {
            "attributes": {
                "objectid": 7,
                "iri": "https://example.com/address/1",
                "pid": 444541,
            }
        },
        schema,
    ) == {
        "objectid": "7",
        "address_iri": "https://example.com/address/1",
        "address_pid": "444541",
    }


def test_save_address_pid_mappings_updates_existing_rows():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = dict_row_factory
    try:
        cursor = connection.cursor()
        create_address_iri_pid_map_table(cursor)
        save_address_pid_mappings(
            cursor,
            [
                {
                    "objectid": "1",
                    "address_iri": "https://example.com/address/1",
                    "address_pid": "100",
                },
                {
                    "objectid": "2",
                    "address_iri": "https://example.com/address/2",
                    "address_pid": "200",
                },
            ],
        )
        save_address_pid_mappings(
            cursor,
            [
                {
                    "objectid": "1",
                    "address_iri": "https://example.com/address/1",
                    "address_pid": "101",
                }
            ],
        )

        assert load_address_pid_mappings(cursor) == {
            "https://example.com/address/1": "101",
            "https://example.com/address/2": "200",
        }
    finally:
        connection.close()
