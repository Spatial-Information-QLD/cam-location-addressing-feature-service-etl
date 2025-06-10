from textwrap import dedent


def get_query():
    return dedent(
        """
        PREFIX addr: <https://linked.data.gov.au/def/addr/>
        PREFIX apt: <https://linked.data.gov.au/def/addr-part-types/>
        PREFIX rnpt: <https://linked.data.gov.au/def/road-name-part-types/>
        PREFIX sdo: <https://schema.org/>
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

        SELECT DISTINCT (CONCAT(STR(?road), "/", ?locality_code, "/", ?road_name) AS ?road_id) ?road_name ?road_name_suffix ?road_name_type ?locality_code ?road_cat_desc
        WHERE {
            GRAPH <urn:qali:graph:addresses> {
                ?iri a addr:Address ;
                sdo:hasPart [
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

                    # Road Suffix
                    OPTIONAL {
                        ?road sdo:hasPart [
                                sdo:additionalType rnpt:roadSuffix ;
                            sdo:value ?road_name_suffix_iri
                        ] .

                        GRAPH ?vocab_graph {
                            ?road_name_suffix_iri skos:notation ?road_name_suffix .
                            FILTER(DATATYPE(?road_name_suffix) = <https://linked.data.gov.au/dataset/qld-addr/datatype/sir-pub>)
                        }
                    }

                    # Road Type
                    OPTIONAL {
                        ?road sdo:hasPart [
                                sdo:additionalType rnpt:roadType ;
                            sdo:value ?road_name_type_iri
                        ] .

                        GRAPH ?vocab_graph {
                            ?road_name_type_iri skos:notation ?road_name_type
                            FILTER(DATATYPE(?road_name_type) = <https://linked.data.gov.au/dataset/qld-addr/datatype/sir-pub>)
                        }
                    }
                }
            }

            BIND("P" as ?road_cat_desc)
        }
        """
    )
