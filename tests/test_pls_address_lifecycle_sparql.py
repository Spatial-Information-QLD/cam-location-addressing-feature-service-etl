from __future__ import annotations

import time
from collections.abc import Iterator
from dataclasses import dataclass

import httpx
import pytest
from testcontainers.core.container import DockerContainer

from address_etl.pls.queries import address

FUSEKI_IMAGE = "ghcr.io/kurrawong/fuseki:5.6.0-0"
FUSEKI_PORT = 3030
CURRENT_ADDRESS_IRI = (
    "https://linked.data.gov.au/dataset/qld-addr/addr/"
    "55a3b5e1-145b-4592-b682-20daf9711cbb"
)
RETIRED_ADDRESS_IRI = (
    "https://linked.data.gov.au/dataset/qld-addr/addr/"
    "dd1654e2-aa5e-57a6-9972-34beb0ec53aa"
)
PARCEL_IRI = "https://linked.data.gov.au/dataset/qld-addr/parcel/2CP903860"
MARTIN_STREET_ROAD_IRI = (
    "https://linked.data.gov.au/dataset/qld-addr/road-name/"
    "7602d0bd-24c0-5596-bbc7-39d499a7db9e"
)

FUSEKI_CONFIG = """
PREFIX :      <http://base/#>
PREFIX fuseki: <http://jena.apache.org/fuseki#>
PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX tdb2:  <http://jena.apache.org/2016/tdb#>

:service rdf:type fuseki:Service ;
    rdfs:label "DS" ;
    fuseki:name "ds" ;
    fuseki:dataset :dataset ;
    fuseki:endpoint [ fuseki:name "query" ; fuseki:operation fuseki:query ] ;
    fuseki:endpoint [ fuseki:name "update" ; fuseki:operation fuseki:update ] ;
    fuseki:endpoint [ fuseki:operation fuseki:query ] ;
    fuseki:endpoint [ fuseki:operation fuseki:update ] .

:dataset rdf:type tdb2:DatasetTDB2 ;
    tdb2:location "/fuseki/databases/ds" ;
    tdb2:unionDefaultGraph true .
"""

EXACT_ADDRESS_FIXTURE = f"""
PREFIX addr:         <https://linked.data.gov.au/def/addr/>
PREFIX apt:          <https://linked.data.gov.au/def/addr-part-types/>
PREFIX cn:           <https://linked.data.gov.au/def/cn/>
PREFIX datatype:     <https://linked.data.gov.au/dataset/qld-addr/datatype/>
PREFIX lc:           <https://linked.data.gov.au/def/lifecycle/>
PREFIX rnpt:         <https://linked.data.gov.au/def/road-name-part-types/>
PREFIX schema:       <https://schema.org/>
PREFIX sdo:          <https://schema.org/>
PREFIX skos:         <http://www.w3.org/2004/02/skos/core#>
PREFIX time:         <http://www.w3.org/2006/time#>
PREFIX xsd:          <http://www.w3.org/2001/XMLSchema#>

INSERT DATA {{
  GRAPH <urn:qali:graph:addresses> {{
    <{PARCEL_IRI}>
      a addr:AddressableObject ;
      cn:hasName <{CURRENT_ADDRESS_IRI}> , <{RETIRED_ADDRESS_IRI}> ;
      schema:additionalProperty [
        schema:propertyID "parcel_id" ;
        schema:value "344982"
      ] ;
      schema:additionalProperty [
        schema:propertyID "parcel_status_code" ;
        schema:value "C"
      ] ;
      schema:additionalProperty [
        schema:propertyID "parcel_create_date" ;
        schema:value "1996-08-20T00:00:00"
      ] ;
      schema:additionalProperty [
        schema:propertyID "parcel_data_source_date" ;
        schema:value "1996-08-20T00:00:00"
      ] ;
      schema:additionalType <https://linked.data.gov.au/def/go-categories/parcel> ;
      schema:identifier
        "CP903860"^^datatype:plan ,
        "2CP903860"^^datatype:lotplan ,
        "2"^^datatype:lot .

    <{CURRENT_ADDRESS_IRI}>
      a cn:CompoundName , addr:Address ;
      addr:hasStatus <https://linked.data.gov.au/def/addr-status-type/primary> ;
      cn:isNameFor <{PARCEL_IRI}> ;
      lc:hasLifecycleStage [
        time:hasBeginning [
          __CURRENT_LIFECYCLE_BEGINNING__
        ] ;
        schema:additionalType
          <https://linked.data.gov.au/def/lifecycle-stage-types/current>
      ] ;
      schema:additionalType <https://linked.data.gov.au/def/addr-classes/unknown> ;
      schema:hasPart [
        schema:additionalType apt:addressNumberFirst ;
        schema:value "3"
      ] ;
      schema:hasPart [
        schema:additionalType apt:countryName ;
        schema:value <https://sws.geonames.org/2077456/>
      ] ;
      schema:hasPart [
        schema:additionalType apt:stateOrTerritory ;
        schema:value
          <https://linked.data.gov.au/dataset/qld-addr/geographic-name/3136e196-b37f-4a19-b9f2-1fa4c83d1c62>
      ] ;
      schema:hasPart [
        schema:additionalType apt:locality ;
        schema:value
          <https://linked.data.gov.au/dataset/qld-addr/geographic-name/0fe48fdf-0b1a-4f4c-8e6f-9917167f395d>
      ] ;
      schema:hasPart [
        schema:additionalType apt:road ;
        schema:value <{MARTIN_STREET_ROAD_IRI}>
      ] ;
      schema:name "3 Martin Street, Ingham, Queensland, Australia" .

    <{RETIRED_ADDRESS_IRI}>
      a cn:CompoundName , addr:Address ;
      addr:hasStatus <https://linked.data.gov.au/def/addr-status-type/primary> ;
      cn:isNameFor <{PARCEL_IRI}> ;
      lc:hasLifecycleStage [
        time:hasBeginning [
          time:inXSDDate "2026-07-01"^^xsd:date
        ] ;
        schema:additionalType
          <https://linked.data.gov.au/def/lifecycle-stage-types/retired>
      ] ;
      lc:hasLifecycleStage [
        time:hasBeginning [
          time:inXSDDateTime "2009-09-30T08:57:08"^^xsd:dateTime
        ] ;
        time:hasEnd [
          time:inXSDDate "2026-07-01"^^xsd:date
        ] ;
        schema:additionalType
          <https://linked.data.gov.au/def/lifecycle-stage-types/current>
      ] ;
      schema:additionalType <https://linked.data.gov.au/def/addr-classes/unknown> ;
      schema:hasPart [
        schema:additionalType apt:countryName ;
        schema:value <https://sws.geonames.org/2077456/>
      ] ;
      schema:hasPart [
        schema:additionalType apt:stateOrTerritory ;
        schema:value
          <https://linked.data.gov.au/dataset/qld-addr/geographic-name/3136e196-b37f-4a19-b9f2-1fa4c83d1c62>
      ] ;
      schema:hasPart [
        schema:additionalType apt:locality ;
        schema:value
          <https://linked.data.gov.au/dataset/qld-addr/geographic-name/0fe48fdf-0b1a-4f4c-8e6f-9917167f395d>
      ] ;
      schema:hasPart [
        schema:additionalType apt:road ;
        schema:value
          <https://linked.data.gov.au/dataset/qld-addr/road-name/c75760eb-f9f6-5f1a-8f8b-20ef08b8d5d9>
      ] ;
      schema:name "Bruce Highway, Ingham, Queensland, Australia" .
  }}

  GRAPH <urn:qali:graph:geographical-names> {{
    <https://linked.data.gov.au/dataset/qld-addr/geographic-name/0fe48fdf-0b1a-4f4c-8e6f-9917167f395d>
      schema:additionalProperty [
        schema:propertyID "lalf.locality_code" ;
        schema:value "LGA_0556"
      ] .
  }}

  GRAPH <urn:qali:graph:roads> {{
    <{MARTIN_STREET_ROAD_IRI}>
      schema:hasPart [
        schema:additionalType rnpt:roadGivenName ;
        schema:value "Martin"
      ] .

    <https://linked.data.gov.au/dataset/qld-addr/road-name/c75760eb-f9f6-5f1a-8f8b-20ef08b8d5d9>
      schema:hasPart [
        schema:additionalType rnpt:roadGivenName ;
        schema:value "Bruce"
      ] .
  }}

  GRAPH <urn:qali:graph:vocabs> {{
    <https://linked.data.gov.au/def/addr-status-type/primary>
      skos:notation "P"^^datatype:sir-pub .

    <https://linked.data.gov.au/def/addr-classes/unknown>
      skos:notation "UNK"^^datatype:sir-pub ;
      skos:inScheme <https://linked.data.gov.au/def/addr-classes> .
  }}
}}
"""

CURRENT_DATE_BEGINNING = 'time:inXSDDate "2026-05-21"^^xsd:date'
CURRENT_DATETIME_BEGINNING = 'time:inXSDDateTime "2026-05-21T00:00:00"^^xsd:dateTime'


@dataclass(frozen=True)
class FusekiEndpoints:
    query: str
    update: str


@pytest.fixture
def fuseki_endpoints(tmp_path) -> Iterator[FusekiEndpoints]:
    config_path = tmp_path / "ds.ttl"
    config_path.write_text(FUSEKI_CONFIG)

    container = DockerContainer(FUSEKI_IMAGE)
    container.with_volume_mapping(str(config_path), "/opt/fuseki/configuration/ds.ttl")
    container.with_exposed_ports(FUSEKI_PORT)
    container.start()
    try:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(FUSEKI_PORT)
        base_url = f"http://{host}:{port}/ds"
        _wait_for_http_ready(f"{base_url}/query")
        yield FusekiEndpoints(query=f"{base_url}/query", update=f"{base_url}/update")
    finally:
        container.stop()


@pytest.mark.parametrize(
    "current_lifecycle_beginning",
    [CURRENT_DATE_BEGINNING, CURRENT_DATETIME_BEGINNING],
    ids=["date", "datetime"],
)
def test_address_queries_select_exact_current_address(
    fuseki_endpoints: FusekiEndpoints,
    current_lifecycle_beginning: str,
) -> None:
    with httpx.Client(timeout=httpx.Timeout(30.0, connect=5.0)) as client:
        _post_update(client, fuseki_endpoints.update, "CLEAR ALL")
        _post_update(
            client,
            fuseki_endpoints.update,
            EXACT_ADDRESS_FIXTURE.replace(
                "__CURRENT_LIFECYCLE_BEGINNING__",
                current_lifecycle_beginning,
            ),
        )

        iri_rows = _select(
            client, fuseki_endpoints.query, address.get_query_iris_only()
        )

        assert [row["addr_iri"]["value"] for row in iri_rows] == [CURRENT_ADDRESS_IRI]
        assert iri_rows[0]["parcel_id"]["value"] == PARCEL_IRI
        assert iri_rows[0]["road"]["value"] == MARTIN_STREET_ROAD_IRI
        assert iri_rows[0]["locality_code"]["value"] == "LGA_0556"
        assert iri_rows[0]["_road_name"]["value"] == "Martin"

        address_rows = _select(
            client,
            fuseki_endpoints.query,
            address.get_query(
                [
                    {
                        "addr_iri": row["addr_iri"]["value"],
                        "parcel_id": row["parcel_id"]["value"],
                        "road": row["road"]["value"],
                        "locality_code": row["locality_code"]["value"],
                        "_road_name": row["_road_name"]["value"],
                    }
                    for row in iri_rows
                ]
            ),
        )

    assert [row["addr_iri"]["value"] for row in address_rows] == [CURRENT_ADDRESS_IRI]
    assert address_rows[0]["parcel_id"]["value"] == PARCEL_IRI
    assert address_rows[0]["street_no_first"]["value"] == "3"
    assert address_rows[0]["addr_status_code"]["value"] == "P"
    assert address_rows[0]["address_standard"]["value"] == "UNK"
    assert MARTIN_STREET_ROAD_IRI in address_rows[0]["road_id"]["value"]


def _post_update(client: httpx.Client, endpoint: str, update: str) -> None:
    response = client.post(endpoint, data={"update": update})
    assert response.status_code in {200, 204}, response.text


def _select(
    client: httpx.Client, endpoint: str, query: str
) -> list[dict[str, dict[str, str]]]:
    response = client.post(
        endpoint,
        content=query,
        headers={
            "Content-Type": "application/sparql-query",
            "Accept": "application/sparql-results+json",
        },
    )
    assert response.status_code == 200, response.text
    return response.json()["results"]["bindings"]


def _wait_for_http_ready(query_url: str) -> None:
    deadline = time.monotonic() + 30.0
    last_error: Exception | None = None
    with httpx.Client(timeout=httpx.Timeout(5.0, connect=1.0)) as client:
        while time.monotonic() < deadline:
            try:
                response = client.get(
                    query_url,
                    params={"query": "ASK {}"},
                    headers={"Accept": "application/sparql-results+json"},
                )
                if response.status_code == 200:
                    return
            except httpx.HTTPError as error:
                last_error = error
            time.sleep(0.2)
    raise AssertionError(
        f"Fuseki server was not ready at {query_url!r}"
    ) from last_error
