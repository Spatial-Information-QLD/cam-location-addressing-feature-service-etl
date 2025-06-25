import httpx
from jinja2 import Template

from address_etl.crud import sparql_query


def get_address_rows(
    address_iris: list[str], sparql_endpoint: str, client: httpx.Client
):
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
                        ?floor_type_concept skos:notation ?floor_type ;
                        skos:inScheme <https://linked.data.gov.au/def/building-level-types>
                        FILTER(DATATYPE(?floor_type) = <https://linked.data.gov.au/dataset/qld-addr/datatype/sir-pub>)
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
                optional {
                    ?iri sdo:hasPart [
                            sdo:additionalType apt:propertyName ;
                        sdo:value ?property_name_object
                    ] .
                    
                    graph <urn:qali:graph:geographical-names> {
                    	?property_name_object sdo:name ?property_name    
                    }
                }
                
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
                            sdo:propertyID "pndb.lga_name" ;
                            sdo:value ?local_authority
                        ] .
                    }
                }
                
                # address status
                ?iri addr:hasStatus ?address_status_concept .
                GRAPH ?address_status_vocab_graph {
                    ?address_status_concept skos:notation ?address_status ;
                                            skos:inScheme <https://linked.data.gov.au/def/addr-status-type> .
                    FILTER(DATATYPE(?address_status) = <https://linked.data.gov.au/dataset/qld-addr/datatype/sir-pub>)
                }
                
                # address standard
                ?iri sdo:additionalType ?address_standard_concept .
                GRAPH ?address_standard_vocab_graph {
                    ?address_standard_concept skos:notation ?address_standard ;
                                            skos:inScheme <https://linked.data.gov.au/def/addr-classes> .
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
    response = sparql_query(sparql_endpoint, query, client)
    return response.json()["results"]["bindings"]
