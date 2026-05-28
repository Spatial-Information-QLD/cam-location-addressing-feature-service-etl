from textwrap import dedent

from jinja2 import Template

from address_etl.pls.debug_parcels import DEBUG_PARCEL_IRIS
from address_etl.pls.queries.parcel_constraints import valid_parcel_identifier_filters


def get_query_iris_only(debug: bool = False):
    return Template(
        dedent(
            """
        PREFIX addr: <https://linked.data.gov.au/def/addr/>
        PREFIX sdo: <https://schema.org/>

        SELECT ?parcel_id
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
                    sdo:identifier ?plan_no, ?_lot_no .

                {{ valid_parcel_identifier_filters }}
            }
        }
        """
        )
    ).render(
        debug=debug,
        DEBUG_PARCEL_IRIS=DEBUG_PARCEL_IRIS,
        valid_parcel_identifier_filters=valid_parcel_identifier_filters(),
    )


def get_query(iris: list[str] = None):
    return Template(
        dedent(
            """
        PREFIX addr: <https://linked.data.gov.au/def/addr/>
        PREFIX sdo: <https://schema.org/>

        SELECT ?parcel_id ?plan_no ?lot_no
        WHERE {
            {% if iris %}
            VALUES ?parcel_id {
                {% for iri in iris %}
                <{{ iri }}>     
                {% endfor %}
            }
            {% endif %}

            GRAPH <urn:qali:graph:addresses> {
                ?parcel_id a addr:AddressableObject ;
                sdo:identifier ?plan_no, ?_lot_no .

                {{ valid_parcel_identifier_filters }}

                # If it's a "0" with datatype of lot, then bind it as "9999"
                BIND(
                    COALESCE(
                        IF(
                            ?_lot_no = "0"^^<https://linked.data.gov.au/dataset/qld-addr/datatype/lot>,
                            "9999"^^<https://linked.data.gov.au/dataset/qld-addr/datatype/lot>,
                            1/0 # let it error to accept the default coalesce value
                        ),
                        ?_lot_no
                    )
                    AS ?lot_no
                )
            }
        }
        """
        )
    ).render(
        iris=iris, valid_parcel_identifier_filters=valid_parcel_identifier_filters()
    )
