from textwrap import dedent

from jinja2 import Template


def get_query_iris_only(debug: bool = False):
    return Template(
        dedent(
            """
        PREFIX addr: <https://linked.data.gov.au/def/addr/>
        PREFIX apt: <https://linked.data.gov.au/def/addr-part-types/>
        PREFIX cn: <https://linked.data.gov.au/def/cn/>
        PREFIX rnpt: <https://linked.data.gov.au/def/road-name-part-types/>
        PREFIX sdo: <https://schema.org/>
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

        SELECT DISTINCT ?addr_iri ?parcel_id ?road ?locality_code ?_road_name
        WHERE {
            {% if debug %}
            VALUES ?parcel_id {
                <https://linked.data.gov.au/dataset/qld-addr/parcel/59SP217152>
                <https://linked.data.gov.au/dataset/qld-addr/parcel/58SP217152>
                <https://linked.data.gov.au/dataset/qld-addr/parcel/57SP217152>
                <https://linked.data.gov.au/dataset/qld-addr/parcel/2SP217150>
                <https://linked.data.gov.au/dataset/qld-addr/parcel/1SP217150>
                <https://linked.data.gov.au/dataset/qld-addr/parcel/0SP217149>
                <https://linked.data.gov.au/dataset/qld-addr/parcel/2SP217149>
                <https://linked.data.gov.au/dataset/qld-addr/parcel/1SP217149>
                <https://linked.data.gov.au/dataset/qld-addr/parcel/17SP217147>
                <https://linked.data.gov.au/dataset/qld-addr/parcel/16SP217147>
                <https://linked.data.gov.au/dataset/qld-addr/parcel/235RP33643>
                <https://linked.data.gov.au/dataset/qld-addr/parcel/1SP101578>
                <https://linked.data.gov.au/dataset/qld-addr/parcel/2RP141728>
                <https://linked.data.gov.au/dataset/qld-addr/parcel/41SP317569>
            }
            {% endif %}

            GRAPH <urn:qali:graph:addresses> {
                ?parcel_id a addr:AddressableObject ;
                    cn:hasName ?addr_iri .
                
                ?addr_iri a addr:Address .

                # Road
                ?addr_iri sdo:hasPart [
                        sdo:additionalType apt:road ;
                    sdo:value ?road
                ],
                        [
                        sdo:additionalType apt:locality ;
                    sdo:value ?locality
                ] .

                # Locality
                GRAPH <urn:qali:graph:geographical-names> {
                    ?locality sdo:additionalProperty [
                            sdo:propertyID "lalf.locality_code" ;
                        sdo:value ?locality_code
                    ]
                }

                GRAPH <urn:qali:graph:roads> {
                    # Road Name
                    ?road sdo:hasPart [
                            sdo:additionalType rnpt:roadGivenName ;
                        sdo:value ?_road_name
                    ] .
                    BIND(UCASE(?_road_name) as ?road_name)
                }
            }
        }
        """
        )
    ).render(debug=debug)


def get_query(debug: bool = False, iris: list = None):
    return Template(
        dedent(
            """
        PREFIX addr: <https://linked.data.gov.au/def/addr/>
        PREFIX apt: <https://linked.data.gov.au/def/addr-part-types/>
        PREFIX cn: <https://linked.data.gov.au/def/cn/>
        PREFIX rnpt: <https://linked.data.gov.au/def/road-name-part-types/>
        PREFIX sdo: <https://schema.org/>
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

        SELECT
            ?parcel_id
            ?addr_id
            ?address_pid
            ?addr_status_code
            ?unit_type
            ?unit_no
            ?unit_suffix
            ?level_type
            ?level_no
            ?level_suffix
            ?street_no_first
            ?street_no_first_suffix
            ?street_no_last
            ?street_no_last_suffix
            ?road_id
            ?site_id
            ?location_desc
            ?address_standard
        WHERE {
            {% if iris %}
            VALUES (?addr_iri ?parcel_id ?road ?locality_code ?_road_name) {
                {% for iri in iris %}
                (<{{ iri["addr_iri"] }}> <{{ iri["parcel_id"] }}> <{{ iri["road"] }}> "{{ iri["locality_code"] }}" "{{ iri["_road_name"] }}")
                {% endfor %}
            }
            {% endif %}

            GRAPH <urn:qali:graph:addresses> {
                ?parcel_id a addr:AddressableObject ;
                    cn:hasName ?addr_iri .

                ?addr_iri a addr:Address ;
                    sdo:identifier ?address_pid ;
                    addr:hasStatus ?addr_status .
                FILTER(DATATYPE(?address_pid) = <https://linked.data.gov.au/dataset/qld-addr/datatype/address-pid>)
                
                # addr id
                BIND(CONCAT(STR(?addr_iri), "/", ?road_id, "/", STR(?parcel_id)) AS ?addr_id)

                # addr status code
                GRAPH ?addr_status_vocab_graph {
                    ?addr_status skos:notation ?addr_status_code .
                    FILTER(DATATYPE(?addr_status_code) = <https://linked.data.gov.au/dataset/qld-addr/datatype/sir-pub>)
                }
                
                # unit type
                OPTIONAL {
                    ?addr_iri sdo:hasPart [
                            sdo:additionalType apt:subaddressType ;
                        sdo:value ?unit_type_concept
                    ] .

                    GRAPH ?unit_type_graph {
                        ?unit_type_concept skos:notation ?unit_type ;
                        skos:inScheme <https://linked.data.gov.au/def/subaddress-types>
                        FILTER(DATATYPE(?unit_type) = <https://linked.data.gov.au/dataset/qld-addr/datatype/sir-pub>)
                    }
                }
                
                # unit no
                OPTIONAL {
                    ?addr_iri sdo:hasPart [
                            sdo:additionalType apt:subaddressNumber ;
                        sdo:value ?unit_no
                    ]
                }
                
                # unit suffix
                OPTIONAL {
                    ?addr_iri sdo:hasPart [
                            sdo:additionalType apt:subaddressNumberSuffix ;
                        sdo:value ?unit_suffix
                    ]
                }
                
                # level type
                OPTIONAL {
                    ?addr_iri sdo:hasPart [
                            sdo:additionalType apt:buildingLevelType ;
                        sdo:value ?level_type_concept
                    ] .

                    graph ?level_type_graph {
                        ?level_type_concept skos:prefLabel ?level_type ;
                        skos:inScheme <https://linked.data.gov.au/def/building-level-types>
                    }
                }
                
                # level no
                OPTIONAL {
                    ?addr_iri sdo:hasPart [
                            sdo:additionalType apt:buildingLevelNumber ;
                        sdo:value ?level_no
                    ] .
                }
                
                # level suffix
                OPTIONAL {
                    ?addr_iri sdo:hasPart [
                            sdo:additionalType apt:buildingLevelSuffix ;
                        sdo:value ?level_suffix
                    ] .
                }
                
                # street no first
                OPTIONAL {
                    ?addr_iri sdo:hasPart [
                            sdo:additionalType apt:addressNumberFirst ;
                        sdo:value ?street_no_first
                    ]
                }

                # street no first suffix
                OPTIONAL {
                    ?addr_iri sdo:hasPart [
                            sdo:additionalType apt:addressNumberFirstSuffix ;
                        sdo:value ?street_no_first_suffix
                    ]
                }
                
                # street no last
                OPTIONAL {
                    ?addr_iri sdo:hasPart [
                            sdo:additionalType apt:addressNumberLast ;
                        sdo:value ?street_no_last
                    ]
                }

                # street no last suffix
                OPTIONAL {
                    ?addr_iri sdo:hasPart [
                            sdo:additionalType apt:addressNumberLastSuffix ;
                        sdo:value ?street_no_last_suffix
                    ]
                }
                
                # road
                ?addr_iri sdo:hasPart [
                        sdo:additionalType apt:road ;
                    sdo:value ?road
                ],
                        [
                        sdo:additionalType apt:locality ;
                    sdo:value ?locality
                ] .

                # Locality
                GRAPH <urn:qali:graph:geographical-names> {
                    ?locality sdo:additionalProperty [
                            sdo:propertyID "lalf.locality_code" ;
                        sdo:value ?locality_code
                    ]
                }

                GRAPH <urn:qali:graph:roads> {
                    # Road Name
                    ?road sdo:hasPart [
                            sdo:additionalType rnpt:roadGivenName ;
                        sdo:value ?_road_name
                    ] .
                    BIND(UCASE(?_road_name) as ?road_name)
                }
                
                # road id
                BIND(CONCAT(STR(?road), "/", ?locality_code, "/", ?road_name) AS ?road_id)
                
                # site
                BIND(CONCAT(STR(?parcel_id), "|", STR(?addr_iri)) AS ?site_id)
                
                # location_desc
                # TODO: don't think this is relevant to the PLS service
                
                # address standard
                ?addr_iri sdo:additionalType ?address_standard_concept .
                GRAPH ?address_standard_vocab_graph {
                    ?address_standard_concept skos:notation ?address_standard ;
                    skos:inScheme <https://linked.data.gov.au/def/addr-classes> .
                    FILTER(DATATYPE(?address_standard) = <https://linked.data.gov.au/dataset/qld-addr/datatype/sir-pub>)
                }
            }
        }
        """
        )
    ).render(iris=iris)
