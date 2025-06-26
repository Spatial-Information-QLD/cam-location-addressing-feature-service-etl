from textwrap import dedent
from jinja2 import Template


def get_query_iris_only(debug: bool = False):
    return Template(
        dedent(
            """
        PREFIX addr: <https://linked.data.gov.au/def/addr/>
        PREFIX apt: <https://linked.data.gov.au/def/addr-part-types/>
        PREFIX cn: <https://linked.data.gov.au/def/cn/>
        PREFIX sdo: <https://schema.org/>

        SELECT ?parcel_id ?addr_iri
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

                ?addr_iri a addr:Address
            }
        }
        """
        )
    ).render(debug=debug)


def get_query(iris: list):
    return Template(
        dedent(
            """
        PREFIX addr: <https://linked.data.gov.au/def/addr/>
        PREFIX apt: <https://linked.data.gov.au/def/addr-part-types/>
        PREFIX cn: <https://linked.data.gov.au/def/cn/>
        PREFIX sdo: <https://schema.org/>

        SELECT
            (CONCAT(STR(?_place_name_id), "|", STR(?parcel_id), "|", STR(?addr_iri)) AS ?place_name_id)
            ("P" AS ?pl_name_status_code)
            ("PROP" AS ?pl_name_type_code)
            ?pl_name
            (CONCAT(STR(?parcel_id), "|", STR(?addr_iri)) AS ?site_id)
        WHERE {            
            GRAPH <urn:qali:graph:addresses> {
                VALUES (?parcel_id ?addr_iri) {
                    {% for iri in iris %}
                    (<{{ iri["parcel_id"] }}> <{{ iri["addr_iri"] }}>)
                    {% endfor %}
                }

                # property name
                ?addr_iri sdo:hasPart [
                        sdo:additionalType apt:propertyName ;
                    sdo:value ?_place_name_id
                ]

                graph <urn:qali:graph:geographical-names> {
                    ?_place_name_id sdo:name ?pl_name
                }
            }
        }
        """
        )
    ).render(iris=iris)
