from address_etl.pls.queries import address


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
