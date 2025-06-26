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

        SELECT DISTINCT ?road ?locality_code ?_road_name
        WHERE {
            GRAPH <urn:qali:graph:addresses> {
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

                ?parcel_id a addr:AddressableObject ;
                    cn:hasName ?iri .
                {% endif %}
            
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
                }
            }
        }
        """
        )
    ).render(debug=debug)


def get_query(iris: list = None):
    return Template(
        dedent(
            """
        PREFIX addr: <https://linked.data.gov.au/def/addr/>
        PREFIX apt: <https://linked.data.gov.au/def/addr-part-types/>
        PREFIX cn: <https://linked.data.gov.au/def/cn/>
        PREFIX roads: <https://linked.data.gov.au/def/roads/>
        PREFIX rnpt: <https://linked.data.gov.au/def/road-name-part-types/>
        PREFIX sdo: <https://schema.org/>
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

        SELECT (CONCAT(STR(?road), "/", ?locality_code, "/", UCASE(?_road_name)) AS ?road_id) (UCASE(?_road_name) as ?road_name) ?road_name_suffix ?road_name_type ?locality_code ?road_cat_desc
        WHERE {
            
            {% if iris %}
            VALUES (?road ?locality_code ?_road_name) {
                {% for iri in iris %}
                (<{{ iri["road"] }}> "{{ iri["locality_code"] }}" "{{ iri["_road_name"] }}")
                {% endfor %}
            }
            {% endif %}

            GRAPH <urn:qali:graph:roads> {
                ?road a roads:RoadName .

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

            BIND("P" as ?road_cat_desc)
        }
        """
        )
    ).render(iris=iris)
