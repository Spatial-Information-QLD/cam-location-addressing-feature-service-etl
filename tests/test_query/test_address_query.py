import httpx
from testcontainers.core.container import DockerContainer

from address_etl.address_iris import address_iris_query


def test(fuseki_container: DockerContainer):
    port = fuseki_container.get_exposed_port(3030)

    data = """
    PREFIX ns1: <https://linked.data.gov.au/def/cn/>
    PREFIX ns2: <https://linked.data.gov.au/def/addr/>
    PREFIX ns3: <https://linked.data.gov.au/def/lifecycle/>
    PREFIX schema: <https://schema.org/>
    PREFIX time: <http://www.w3.org/2006/time#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    <urn:qali:graph:addresses> {
        <https://linked.data.gov.au/dataset/qld-addr/address/044e60b4-f18d-5a48-90ea-a4463ec7d616>
            a
                ns2:Address ,
                ns1:CompoundName ;
            ns2:hasStatus <https://linked.data.gov.au/def/addr-status-type/primary> ;
            ns1:isNameFor <https://linked.data.gov.au/dataset/qld-addr/parcel/235RP33643> ;
            ns3:hasLifecycleStage
                [
                    time:hasBeginning
                        [
                            time:inXSDDateTime "2006-08-30T14:30:23"^^xsd:dateTime ;
                        ] ;
                    schema:additionalType <https://linked.data.gov.au/def/lifecycle-stage-types/current> ;
                ] ;
            schema:additionalType <https://linked.data.gov.au/def/addr-classes/unknown> ;
            schema:hasPart 
                [
                    schema:additionalType <https://linked.data.gov.au/def/addr-part-types/road> ;
                    schema:value <https://linked.data.gov.au/dataset/qld-addr/road-name/QLDRNUD1530748227408711000> ;
                ] ,
                [
                    schema:additionalType <https://linked.data.gov.au/def/addr-part-types/locality> ;
                    schema:value <https://linked.data.gov.au/dataset/qld-addr/gn/52381> ;
                ] ,
                [
                    schema:additionalType <https://linked.data.gov.au/def/addr-part-types/addressNumberFirst> ;
                    schema:value "96" ;
                ] ,
                [
                    schema:additionalType <https://linked.data.gov.au/def/addr-part-types/countryName> ;
                    schema:value <https://sws.geonames.org/2077456/> ;
                ] ,
                [
                    schema:additionalType <https://linked.data.gov.au/def/addr-part-types/stateOrTerritory> ;
                    schema:value <https://sws.geonames.org/2152274/> ;
                ] ;
            schema:identifier "1017055"^^<https://linked.data.gov.au/dataset/qld-addr/datatype/address-pid> ;
            schema:name "96 Nudgee Road, Hamilton, Queensland, Australia" ;
        .

        <https://linked.data.gov.au/dataset/qld-addr/parcel/235RP33643>
            ns1:hasName <https://linked.data.gov.au/dataset/qld-addr/address/044e60b4-f18d-5a48-90ea-a4463ec7d616> ;
        .
    }
    """

    with httpx.Client(base_url=f"http://localhost:{port}") as client:
        headers = {
            "Content-Type": "application/trig",
        }
        client.post("/ds", data=data, headers=headers)

        response = client.post(
            "/ds",
            headers={
                "Accept": "application/sparql-results+json",
                "Content-Type": "application/sparql-query",
            },
            data=address_iris_query(),
        )
        response_json = response.json()
        assert response_json["results"]["bindings"] == [
            {
                "iri": {
                    "type": "uri",
                    "value": "https://linked.data.gov.au/dataset/qld-addr/address/044e60b4-f18d-5a48-90ea-a4463ec7d616",
                },
                "start_time": {
                    "type": "literal",
                    "value": "2006-08-30T14:30:23",
                    "datatype": "http://www.w3.org/2001/XMLSchema#dateTime",
                },
            }
        ]


def test_query_address_lifecycle_stage_status_proposed(
    fuseki_container: DockerContainer,
):
    port = fuseki_container.get_exposed_port(3030)

    data = """
    PREFIX ns1: <https://linked.data.gov.au/def/cn/>
    PREFIX ns2: <https://linked.data.gov.au/def/addr/>
    PREFIX ns3: <https://linked.data.gov.au/def/lifecycle/>
    PREFIX schema: <https://schema.org/>
    PREFIX time: <http://www.w3.org/2006/time#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    <urn:qali:graph:addresses> {
        <https://linked.data.gov.au/dataset/qld-addr/address/044e60b4-f18d-5a48-90ea-a4463ec7d616>
            a
                ns2:Address ,
                ns1:CompoundName ;
            ns2:hasStatus <https://linked.data.gov.au/def/addr-status-type/primary> ;
            ns1:isNameFor <https://linked.data.gov.au/dataset/qld-addr/parcel/235RP33643> ;
            ns3:hasLifecycleStage
                [
                    time:hasBeginning
                        [
                            time:inXSDDateTime "2006-08-30T14:30:23"^^xsd:dateTime ;
                        ] ;
                    schema:additionalType <https://linked.data.gov.au/def/lifecycle-stage-types/proposed> ;
                ] ;
            schema:additionalType <https://linked.data.gov.au/def/addr-classes/unknown> ;
            schema:hasPart 
                [
                    schema:additionalType <https://linked.data.gov.au/def/addr-part-types/road> ;
                    schema:value <https://linked.data.gov.au/dataset/qld-addr/road-name/QLDRNUD1530748227408711000> ;
                ] ,
                [
                    schema:additionalType <https://linked.data.gov.au/def/addr-part-types/locality> ;
                    schema:value <https://linked.data.gov.au/dataset/qld-addr/gn/52381> ;
                ] ,
                [
                    schema:additionalType <https://linked.data.gov.au/def/addr-part-types/addressNumberFirst> ;
                    schema:value "96" ;
                ] ,
                [
                    schema:additionalType <https://linked.data.gov.au/def/addr-part-types/countryName> ;
                    schema:value <https://sws.geonames.org/2077456/> ;
                ] ,
                [
                    schema:additionalType <https://linked.data.gov.au/def/addr-part-types/stateOrTerritory> ;
                    schema:value <https://sws.geonames.org/2152274/> ;
                ] ;
            schema:identifier "1017055"^^<https://linked.data.gov.au/dataset/qld-addr/datatype/address-pid> ;
            schema:name "96 Nudgee Road, Hamilton, Queensland, Australia" ;
        .

        <https://linked.data.gov.au/dataset/qld-addr/parcel/235RP33643>
            ns1:hasName <https://linked.data.gov.au/dataset/qld-addr/address/044e60b4-f18d-5a48-90ea-a4463ec7d616> ;
        .
    }
    """

    with httpx.Client(base_url=f"http://localhost:{port}") as client:
        headers = {
            "Content-Type": "application/trig",
        }
        client.post("/ds", data=data, headers=headers)

        response = client.post(
            "/ds",
            headers={
                "Accept": "application/sparql-results+json",
                "Content-Type": "application/sparql-query",
            },
            data=address_iris_query(),
        )
        response_json = response.json()
        assert response_json["results"]["bindings"] == []


def test_with_multiple_lc_stages_with_no_end_date(fuseki_container: DockerContainer):
    port = fuseki_container.get_exposed_port(3030)

    data = """
    PREFIX ns1: <https://linked.data.gov.au/def/cn/>
    PREFIX ns2: <https://linked.data.gov.au/def/addr/>
    PREFIX ns3: <https://linked.data.gov.au/def/lifecycle/>
    PREFIX schema: <https://schema.org/>
    PREFIX time: <http://www.w3.org/2006/time#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    <urn:qali:graph:addresses> {
        <https://linked.data.gov.au/dataset/qld-addr/address/044e60b4-f18d-5a48-90ea-a4463ec7d616>
            a
                ns2:Address ,
                ns1:CompoundName ;
            ns2:hasStatus <https://linked.data.gov.au/def/addr-status-type/primary> ;
            ns1:isNameFor <https://linked.data.gov.au/dataset/qld-addr/parcel/235RP33643> ;
            ns3:hasLifecycleStage
                [
                    time:hasBeginning
                        [
                            time:inXSDDateTime "2006-08-30T14:30:23"^^xsd:dateTime ;
                        ] ;
                    schema:additionalType <https://linked.data.gov.au/def/lifecycle-stage-types/current> ;
                ],
                [
                    time:hasBeginning
                        [
                            time:inXSDDateTime "2006-09-30T14:30:23"^^xsd:dateTime ;
                        ] ;
                    schema:additionalType <https://linked.data.gov.au/def/lifecycle-stage-types/current> ;
                ] ;
            schema:additionalType <https://linked.data.gov.au/def/addr-classes/unknown> ;
            schema:hasPart 
                [
                    schema:additionalType <https://linked.data.gov.au/def/addr-part-types/road> ;
                    schema:value <https://linked.data.gov.au/dataset/qld-addr/road-name/QLDRNUD1530748227408711000> ;
                ] ,
                [
                    schema:additionalType <https://linked.data.gov.au/def/addr-part-types/locality> ;
                    schema:value <https://linked.data.gov.au/dataset/qld-addr/gn/52381> ;
                ] ,
                [
                    schema:additionalType <https://linked.data.gov.au/def/addr-part-types/addressNumberFirst> ;
                    schema:value "96" ;
                ] ,
                [
                    schema:additionalType <https://linked.data.gov.au/def/addr-part-types/countryName> ;
                    schema:value <https://sws.geonames.org/2077456/> ;
                ] ,
                [
                    schema:additionalType <https://linked.data.gov.au/def/addr-part-types/stateOrTerritory> ;
                    schema:value <https://sws.geonames.org/2152274/> ;
                ] ;
            schema:identifier "1017055"^^<https://linked.data.gov.au/dataset/qld-addr/datatype/address-pid> ;
            schema:name "96 Nudgee Road, Hamilton, Queensland, Australia" ;
        .

        <https://linked.data.gov.au/dataset/qld-addr/parcel/235RP33643>
            ns1:hasName <https://linked.data.gov.au/dataset/qld-addr/address/044e60b4-f18d-5a48-90ea-a4463ec7d616> ;
        .
    }
    """

    with httpx.Client(base_url=f"http://localhost:{port}") as client:
        headers = {
            "Content-Type": "application/trig",
        }
        client.post("/ds", data=data, headers=headers)

        response = client.post(
            "/ds",
            headers={
                "Accept": "application/sparql-results+json",
                "Content-Type": "application/sparql-query",
            },
            data=address_iris_query(),
        )
        response_json = response.json()
        assert response_json["results"]["bindings"] == [
            {
                "iri": {
                    "type": "uri",
                    "value": "https://linked.data.gov.au/dataset/qld-addr/address/044e60b4-f18d-5a48-90ea-a4463ec7d616",
                },
                "start_time": {
                    "type": "literal",
                    "value": "2006-09-30T14:30:23",
                    "datatype": "http://www.w3.org/2001/XMLSchema#dateTime",
                },
            },
        ]
