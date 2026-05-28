from textwrap import dedent

from jinja2 import Template

from address_etl.pls.debug_parcels import DEBUG_PARCEL_IRIS
from address_etl.pls.queries.parcel_constraints import valid_parcel_identifier_filters


def get_query_iris_only(debug: bool = False):
    return Template(
        dedent(
            """
        PREFIX addr: <https://linked.data.gov.au/def/addr/>
        PREFIX cn: <https://linked.data.gov.au/def/cn/>
        PREFIX sdo: <https://schema.org/>

        SELECT ?parcel_id ?address
        WHERE {
            {% if debug %}
            VALUES ?parcel_id {
                {% for parcel_iri in DEBUG_PARCEL_IRIS %}
                <{{ parcel_iri }}>
                {% endfor %}
            }
            {% endif %}

            GRAPH <urn:qali:graph:addresses> {
                ?parcel_id a addr:AddressableObject ;
                           sdo:identifier ?plan_no, ?_lot_no ;
                           cn:hasName ?address .

                {{ valid_parcel_identifier_filters }}
                
                ?address a addr:Address .
            }
        }
        """
        )
    ).render(
        debug=debug,
        DEBUG_PARCEL_IRIS=DEBUG_PARCEL_IRIS,
        valid_parcel_identifier_filters=valid_parcel_identifier_filters(),
    )


def get_query(iris: list = None):
    return Template(
        dedent(
            """
        PREFIX addr: <https://linked.data.gov.au/def/addr/>
        PREFIX cn: <https://linked.data.gov.au/def/cn/>
        PREFIX sdo: <https://schema.org/>

        SELECT (CONCAT(STR(?parcel_id), "|", STR(?address)) AS ?site_id) ?parent_site_id ?site_type ?parcel_id
        WHERE {
            {% if iris %}
            VALUES (?parcel_id ?address) {
                {% for iri in iris %}
                (<{{ iri["parcel_id"] }}> <{{ iri["address"] }}>)
                {% endfor %}
            }
            {% endif %}

            GRAPH <urn:qali:graph:addresses> {
                ?parcel_id a addr:AddressableObject ;
                sdo:identifier ?plan_no, ?_lot_no .

                {{ valid_parcel_identifier_filters }}

                ?parcel_id cn:hasName ?address .
                ?address a addr:Address .

                # Commented out as we can't determine the parent site from the data as there exists some 9999 lotplans with multiple primary addresses.
                # OPTIONAL {
                #     ?parent_parcel_id sdo:identifier ?plan_no, "0"^^<https://linked.data.gov.au/dataset/qld-addr/datatype/lot> .

                #     ?parent_parcel_id cn:hasName ?parent_address .
                #     ?parent_address a addr:Address .

                #     BIND(
                #         IF(
                #             STR(?_lot_no) != "0",
                #             CONCAT(STR(?parent_parcel_id), "|", STR(?parent_address)),
                #             1/0
                #         )
                #         AS ?parent_site_id
                #     )
                # }

                BIND("P" AS ?site_type)
            }
        }
        """
        )
    ).render(
        iris=iris, valid_parcel_identifier_filters=valid_parcel_identifier_filters()
    )
