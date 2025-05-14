import logging
import os
import sqlite3
import time

import httpx
from jinja2 import Template

from address_etl.create_tables import create_tables
from address_etl.get_address_iris import get_address_iris


def get_rows(address_iris: list[str], sparql_endpoint: str, client: httpx.Client):
    query = Template(
        """
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
        PREFIX addr: <https://linked.data.gov.au/def/addr/>
        PREFIX apt: <https://linked.data.gov.au/def/addr-part-types/>
        PREFIX cn: <https://linked.data.gov.au/def/cn/>
        PREFIX sdo: <https://schema.org/>

        SELECT 
            ?iri
            ?name
            ?lot
            ?plan
            ?unit_number
            ?unit_type
            ?street_number
            (?road_name AS ?street_name)
            (?road_type AS ?street_type)
            ?state
            (?road_suffix AS ?street_suffix)
            ?unit_suffix
            ?floor_type
            ?floor_number
            ?floor_suffix
            ?property_name
            ?street_no_1
            ?street_no_1_suffix
            ?street_no_2
            ?street_no_2_suffix
            ?street_full
            ?locality
            ?local_authority
            ?address_status
            ?address_standard
            ?lotplan_status
            ?address_pid
        FROM <http://www.ontotext.com/explicit>
        WHERE {
            VALUES ?iri {
                {% for address_iri in address_iris %}
                <{{ address_iri }}>
                {% endfor %}
            }
            
            GRAPH <urn:qali:graph:addresses> {
                ?iri a addr:Address ;
                sdo:identifier ?address_pid ;
                cn:isNameFor ?parcel ;
                sdo:name ?name ;
                
                # unit number
                OPTIONAL {
                    ?iri sdo:hasPart [
                            sdo:additionalType apt:subaddressNumber ;
                        sdo:value ?unit_number
                    ]
                }
                
                # unit type
                OPTIONAL {
                    ?iri sdo:hasPart [
                            sdo:additionalType apt:subaddressType ;
                        sdo:value ?unit_type_concept
                    ] .
                    
                    GRAPH ?unit_type_graph {
                        ?unit_type_concept skos:notation ?unit_type ;
                                        skos:inScheme <https://linked.data.gov.au/def/subaddress-types>
                        FILTER(DATATYPE(?unit_type) = <https://linked.data.gov.au/dataset/qld-addr/datatype/sir-pub>)
                    }
                }
                
                # street number
                OPTIONAL {
                    ?iri sdo:hasPart [
                            sdo:additionalType apt:addressNumberFirst ;
                        sdo:value ?street_number
                    ]
                }
                
                # road
                ?iri sdo:hasPart [
                    sdo:additionalType apt:road ;
                    sdo:value ?road
                ] .
                
                # street name
                GRAPH <urn:qali:graph:roads> {
                    ?road sdo:hasPart [
                        sdo:additionalType <https://linked.data.gov.au/def/road-name-part-types/roadGivenName> ;
                        sdo:value ?road_name
                    ] .
                }
                
                # street type
                OPTIONAL {
                    GRAPH <urn:qali:graph:roads> {
                        ?road sdo:hasPart [
                            sdo:additionalType <https://linked.data.gov.au/def/road-name-part-types/roadType> ;
                            sdo:value ?road_type_concept
                        ] .
                        
                        GRAPH ?road_type_graph {
                            ?road_type_concept skos:prefLabel ?road_type ;
                                            skos:inScheme <https://linked.data.gov.au/def/road-types>
                        }
                    }
                }
                
                # state
                BIND("QLD" AS ?state)
                
                # lot and plan
                ?parcel sdo:identifier ?lot ;
                        sdo:identifier ?plan .
                FILTER(DATATYPE(?lot) = <https://linked.data.gov.au/dataset/qld-addr/datatype/lot> && datatype(?plan) = <https://linked.data.gov.au/dataset/qld-addr/datatype/plan>)
                
                # street suffix
                OPTIONAL {
                    GRAPH <urn:qali:graph:roads> {
                        ?road sdo:hasPart [
                            sdo:additionalType <https://linked.data.gov.au/def/road-name-part-types/roadSuffix> ;
                            sdo:value ?road_suffix_concept
                        ] .
                        
                        GRAPH ?road_suffix_graph {
                            ?road_suffix_concept skos:prefLabel ?road_suffix ;
                                                skos:inScheme <https://linked.data.gov.au/def/gn-affix>
                        }
                    }
                }
                
                # geocode type
                # TODO: post-process
                
                # address
                # TODO: post-process
                
                # unit suffix
                OPTIONAL {
                    ?iri sdo:hasPart [
                            sdo:additionalType apt:subaddressNumberSuffix ;
                        sdo:value ?unit_suffix
                    ]
                }
                
                # floor type
                OPTIONAL {
                    ?iri sdo:hasPart [
                            sdo:additionalType apt:buildingLevelType ;
                        sdo:value ?floor_type_concept
                    ] .
                    
                    graph ?floor_type_graph {
                            ?floor_type_concept skos:prefLabel ?floor_type ;
                                                skos:inScheme <https://linked.data.gov.au/def/building-level-types>
                        }
                }
                
                # floor number
                OPTIONAL {
                    ?iri sdo:hasPart [
                            sdo:additionalType apt:buildingLevelNumber ;
                        sdo:value ?floor_number
                    ] .
                }
                
                # floor suffix
                OPTIONAL {
                    ?iri sdo:hasPart [
                            sdo:additionalType apt:buildingLevelSuffix ;
                        sdo:value ?floor_suffix
                    ] .
                }
                
                # property name
        #        optional {
        #            ?iri sdo:hasPart [
        #                    sdo:additionalType apt:propertyName ;
        #                sdo:value ?property_name_object
        #            ] .
        #            
        #            graph <urn:qali:graph:geographical-names> {
        #            	?property_name_object sdo:name ?property_name    
        #            }
        #        }
                
                # street no 1
                OPTIONAL {
                    ?iri sdo:hasPart [
                            sdo:additionalType apt:addressNumberFirst ;
                        sdo:value ?street_no_1
                    ]
                }
                
                # street no 1 suffix
                OPTIONAL {
                    ?iri sdo:hasPart [
                            sdo:additionalType apt:addressNumberFirstSuffix ;
                        sdo:value ?street_no_1_suffix
                    ]
                }
                
                # street no 2
                OPTIONAL {
                    ?iri sdo:hasPart [
                            sdo:additionalType apt:addressNumberLast ;
                        sdo:value ?street_no_2
                    ]
                }
                
                # street no 2 suffix
                OPTIONAL {
                    ?iri sdo:hasPart [
                            sdo:additionalType apt:addressNumberLastSuffix ;
                        sdo:value ?street_no_2_suffix
                    ]
                }
                
                # street full
                GRAPH <urn:qali:graph:roads> {
                    ?road sdo:name ?street_full    
                }
                
                # locality
                ?iri sdo:hasPart [
                    sdo:additionalType apt:locality ;
                    sdo:value ?locality_object
                ] .
                GRAPH <urn:qali:graph:geographical-names> {
                    ?locality_object sdo:name ?locality
                }
                
                # local authority
                OPTIONAL {
                    GRAPH <urn:qali:graph:geographical-names> {
                        ?locality_object sdo:additionalProperty [
                            sdo:propertyID "local_authority" ;
                            sdo:value ?local_authority
                        ] .
                    }
                }
                
                # address status
                ?iri addr:hasStatus ?address_status_concept .
                GRAPH ?address_status_vocab_graph {
                    ?address_status_concept skos:notation ?address_status ;
                                            skos:inScheme <https://linked.data.gov.au/def/address-status-type> .
                    FILTER(DATATYPE(?address_status) = <https://linked.data.gov.au/dataset/qld-addr/datatype/sir-pub>)
                }
                
                # address standard
                ?iri sdo:additionalType ?address_standard_concept .
                GRAPH ?address_standard_vocab_graph {
                    ?address_standard_concept skos:notation ?address_standard ;
                                            skos:inScheme <https://linked.data.gov.au/def/address-classes> .
                    FILTER(DATATYPE(?address_standard) = <https://linked.data.gov.au/dataset/qld-addr/datatype/sir-pub>)
                }
                
                # lotplan status
                ?parcel sdo:additionalProperty [
                        sdo:propertyID "parcel_status_code" ;
                        sdo:value ?lotplan_status
                ]
            }
        }
    """
    ).render(address_iris=address_iris)
    response = client.post(
        sparql_endpoint,
        headers={
            "Content-Type": "application/sparql-query",
            "Accept": "application/sparql-results+json",
        },
        data=query,
    )
    if response.status_code != 200:
        raise Exception(f"Address rows query failed: {response.text}")
    return response.json()["results"]["bindings"]


def write_address_rows(data: list[dict], cursor: sqlite3.Cursor):
    query = """
        INSERT INTO address_current_staging VALUES(
            :id,
            :lot,
            :plan,
            :unit_type,
            :unit_number,
            :unit_suffix,
            :floor_type,
            :floor_number,
            :floor_suffix,
            :property_name,
            :street_no_1,
            :street_no_1_suffix,
            :street_no_2,
            :street_no_2_suffix,
            :street_number,
            :street_name,
            :street_type,
            :street_suffix,
            :street_full,
            :locality,
            :local_authority,
            :state,
            :address,
            :address_status,
            :address_standard,
            :lotplan_status,
            :address_pid,
            :geocode_type,
            :latitude,
            :longitude
        )
    """
    cursor.executemany(query, data)
    cursor.connection.commit()


def get_address_concatenation(row: dict) -> str:
    def get_value(key: str) -> str:
        return row.get(key, {}).get("value", "")

    street_no_2 = get_value("street_no_2")
    unit_number = get_value("unit_number")
    return f"{get_value('unit_type')}{unit_number}{get_value('unit_suffix')}{'/' if unit_number else ''}{get_value('street_no_1')}{get_value('street_no_1_suffix')}{'-' if street_no_2 else ''}{street_no_2}{get_value('street_no_2_suffix')} {get_value('street_name')} {get_value('street_type')} {get_value('street_suffix')} {get_value('locality')} {get_value('state')}"


def populate_address_current_staging_table(
    sparql_endpoint: str, cursor: sqlite3.Cursor, logger: logging.Logger
):
    with httpx.Client() as client:
        address_iris = get_address_iris(sparql_endpoint, client)
        logger.info(f"Retrieved {len(address_iris)} address IRIs to process")

        # Create chunks of 1000 IRIs
        address_iri_chunks = [
            address_iris[i : i + 1000] for i in range(0, len(address_iris), 1000)
        ]
        logger.info(f"Split into {len(address_iri_chunks)} chunks for processing")

        for address_iri_chunk in address_iri_chunks:
            rows = get_rows(address_iri_chunk, sparql_endpoint, client)
            modified_rows = []
            for row in rows:
                data = {}
                data["id"] = None
                data["lot"] = row.get("lot", {}).get("value")
                data["plan"] = row.get("plan", {}).get("value")
                data["unit_type"] = row.get("unit_type", {}).get("value")
                data["unit_number"] = row.get("unit_number", {}).get("value")
                data["unit_suffix"] = row.get("unit_suffix", {}).get("value")
                data["floor_type"] = row.get("floor_type", {}).get("value")
                data["floor_number"] = row.get("floor_number", {}).get("value")
                data["floor_suffix"] = row.get("floor_suffix", {}).get("value")
                data["property_name"] = row.get("property_name", {}).get("value")
                data["street_no_1"] = row.get("street_no_1", {}).get("value")
                data["street_no_1_suffix"] = row.get("street_no_1_suffix", {}).get(
                    "value"
                )
                data["street_no_2"] = row.get("street_no_2", {}).get("value")
                data["street_no_2_suffix"] = row.get("street_no_2_suffix", {}).get(
                    "value"
                )
                data["street_number"] = row.get("street_number", {}).get("value")
                data["street_name"] = row.get("street_name", {}).get("value")
                data["street_type"] = row.get("street_type", {}).get("value")
                data["street_suffix"] = row.get("street_suffix", {}).get("value")
                data["street_full"] = row.get("street_full", {}).get("value")
                data["locality"] = row.get("locality", {}).get("value")
                data["local_authority"] = row.get("local_authority", {}).get("value")
                data["state"] = row.get("state", {}).get("value")
                data["address"] = get_address_concatenation(row)
                data["address_status"] = row.get("address_status", {}).get("value")
                data["address_standard"] = row.get("address_standard", {}).get("value")
                data["lotplan_status"] = row.get("lotplan_status", {}).get("value")
                data["address_pid"] = row.get("address_pid", {}).get("value")
                data["geocode_type"] = None  # TODO: geocodes
                data["latitude"] = None  # TODO: geocodes
                data["longitude"] = None  # TODO: geocodes
                modified_rows.append(data)

            write_address_rows(modified_rows, cursor)


def populate_geocode_table(cursor: sqlite3.Cursor):
    cursor.executemany(
        """
        INSERT INTO geocode VALUES(
            :address_pid,
            :geocode_type,
            :longitude,
            :latitude
        )
    """,
        [
            {
                "address_pid": "196483",
                "geocode_type": "PC",
                "longitude": "153.11051065",
                "latitude": "-27.24430653",
            }
        ],
    )
    cursor.connection.commit()


def populate_address_current_table(cursor: sqlite3.Cursor):
    cursor.execute(
        """
        INSERT INTO address_current
        SELECT
            a.id,
            a.lot,
            a.plan,
            a.unit_type,
            a.unit_number,
            a.unit_suffix,
            a.floor_type,
            a.floor_number,
            a.floor_suffix,
            a.property_name,
            a.street_no_1,
            a.street_no_1_suffix,
            a.street_no_2,
            a.street_no_2_suffix,
            a.street_number,
            a.street_name,
            a.street_type,
            a.street_suffix,
            a.street_full,
            a.locality,
            a.local_authority,
            a.state,
            a.address,
            a.address_status,
            a.address_standard,
            a.lotplan_status,
            a.address_pid,
            g.geocode_type,
            g.latitude,
            g.longitude
        FROM address_current_staging a
        JOIN geocode g ON a.address_pid = g.address_pid
    """
    )
    cursor.connection.commit()


def main():
    logger = logging.getLogger(__name__)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)

    start_time = time.time()
    logger.info("Starting ETL process")
    sparql_endpoint = os.environ["SPARQL_ENDPOINT"]
    sqlite_conn_str = os.environ["SQLITE_CONN_STR"]

    connection = sqlite3.connect(sqlite_conn_str)
    try:
        cursor = connection.cursor()
        create_tables(cursor, logger)
        populate_address_current_staging_table(sparql_endpoint, cursor, logger)
        populate_geocode_table(cursor)
        populate_address_current_table(cursor)
    finally:
        logger.info("Closing connection to SQLite database")
        connection.close()

    logger.info("ETL process completed successfully")
    logger.info(f"Total time taken: {time.time() - start_time:.2f} seconds")


if __name__ == "__main__":
    main()
