import sqlite3

from address_etl.pls.debug_parcels import DEBUG_PARCEL_IRIS
from address_etl.pls.queries import (
    address,
    local_auth,
    locality,
    parcel,
    place_name,
    site,
)
from address_etl.pls.tables import filter_address_iris_to_loaded_parcels
from address_etl.sqlite_dict_factory import dict_row_factory

VALID_PARCEL_FILTERS = (
    "FILTER(STRLEN(STR(?plan_no)) <= 10)",
    "FILTER(STRLEN(STR(?_lot_no)) <= 5)",
)


def assert_valid_parcel_identifier_filters(query: str):
    for expected_filter in VALID_PARCEL_FILTERS:
        assert expected_filter in query


def assert_no_valid_parcel_identifier_filters(query: str):
    for expected_filter in VALID_PARCEL_FILTERS:
        assert expected_filter not in query


def test_get_query_iris_only_filters_to_current_non_private_addresses():
    query = address.get_query_iris_only()

    assert "PREFIX lc: <https://linked.data.gov.au/def/lifecycle/>" in query
    assert "PREFIX time: <http://www.w3.org/2006/time#>" in query
    assert "SELECT ?addr_iri (MAX(?_start_time) AS ?latest_start_time)" in query
    assert "lc:hasLifecycleStage ?latest_lifecycle_stage" in query
    assert (
        "sdo:additionalType <https://linked.data.gov.au/def/lifecycle-stage-types/current> ;"
        in query
    )
    assert "FILTER NOT EXISTS {" in query
    assert "?latest_lifecycle_stage time:hasEnd ?end_time" in query
    assert "GRAPH <urn:qali:graph:tags>" in query
    assert "<urn:qali:tag-collection:private> skos:member ?private_tag ." in query
    assert_no_valid_parcel_identifier_filters(query)


def test_get_query_iris_only_debug_filters_lifecycle_subquery_to_debug_parcels():
    query = address.get_query_iris_only(debug=True)

    lifecycle_subquery = query[
        query.index(
            "SELECT ?addr_iri (MAX(?_start_time) AS ?latest_start_time)"
        ) : query.index("GROUP BY ?addr_iri")
    ]

    assert "VALUES ?parcel_id {" in lifecycle_subquery
    assert "cn:hasName ?addr_iri" in lifecycle_subquery
    assert f"<{DEBUG_PARCEL_IRIS[0]}>" in lifecycle_subquery


def test_get_query_filters_to_current_non_private_addresses():
    query = address.get_query(
        iris=[
            {
                "addr_iri": "https://example.com/address/1",
                "parcel_id": "https://example.com/parcel/1",
                "road": "https://example.com/road/1",
                "locality_code": "12345",
                "_road_name": "Example",
            }
        ]
    )

    assert "VALUES (?addr_iri ?parcel_id ?road ?locality_code ?_road_name)" in query
    assert "VALUES ?addr_iri {" in query
    assert "<https://example.com/address/1>" in query
    assert "SELECT ?addr_iri (MAX(?_start_time) AS ?latest_start_time)" in query
    assert "lc:hasLifecycleStage ?latest_lifecycle_stage" in query
    assert "GRAPH <urn:qali:graph:tags>" in query
    assert "<urn:qali:tag-collection:private> skos:member ?private_tag ." in query
    assert "?address_pid" not in query
    assert_no_valid_parcel_identifier_filters(query)


def test_filter_address_iris_to_loaded_parcels_uses_existing_parcel_filter():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = dict_row_factory
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE lf_parcel (parcel_id TEXT PRIMARY KEY)")
    cursor.execute("INSERT INTO lf_parcel (parcel_id) VALUES (?)", ("parcel-1",))

    filtered = filter_address_iris_to_loaded_parcels(
        [
            {"addr_iri": "address-1", "parcel_id": "parcel-1"},
            {"addr_iri": "address-2", "parcel_id": "parcel-2"},
        ],
        cursor,
    )

    assert filtered == [{"addr_iri": "address-1", "parcel_id": "parcel-1"}]


def test_parcel_queries_filter_to_valid_parcel_identifiers():
    assert_valid_parcel_identifier_filters(parcel.get_query_iris_only())
    assert_valid_parcel_identifier_filters(
        parcel.get_query(iris=["https://example.com/parcel/1"])
    )


def test_site_queries_filter_to_valid_parcel_identifiers():
    assert_valid_parcel_identifier_filters(site.get_query_iris_only())
    assert_valid_parcel_identifier_filters(
        site.get_query(
            iris=[
                {
                    "parcel_id": "https://example.com/parcel/1",
                    "address": "https://example.com/address/1",
                }
            ]
        )
    )


def test_place_name_queries_filter_to_valid_parcel_identifiers():
    assert_valid_parcel_identifier_filters(place_name.get_query_iris_only())
    assert_valid_parcel_identifier_filters(
        place_name.get_query(
            iris=[
                {
                    "parcel_id": "https://example.com/parcel/1",
                    "addr_iri": "https://example.com/address/1",
                }
            ]
        )
    )


def test_local_auth_query_excludes_empty_lga_names():
    query = local_auth.get_query()

    assert "sdo:value ?_lga_name" in query
    assert "BIND(UCASE(STR(?_lga_name)) AS ?lga_name)" in query
    assert "FILTER(STRLEN(STR(?lga_name)) > 0)" in query


def test_locality_query_maps_empty_statuses_to_current():
    query = locality.get_query()

    assert "sdo:value ?_status" in query
    assert 'BIND(IF(STR(?_status) = "", "Y", ?_status) AS ?status)' in query
    assert "FILTER(STRLEN(STR(?status)) = 1)" not in query


def test_debug_parcel_iris_list_has_expected_size():
    assert len(DEBUG_PARCEL_IRIS) == 100
