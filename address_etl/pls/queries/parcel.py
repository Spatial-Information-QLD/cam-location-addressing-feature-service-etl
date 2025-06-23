from textwrap import dedent

from jinja2 import Template


def get_query_iris_only(debug: bool = False):
    return Template(
        dedent(
            """
        PREFIX addr: <https://linked.data.gov.au/def/addr/>

        SELECT ?parcel_id
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
                ?parcel_id a addr:AddressableObject .
            }
        }
        """
        )
    ).render(debug=debug)


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

                FILTER(DATATYPE(?plan_no) = <https://linked.data.gov.au/dataset/qld-addr/datatype/plan>)
                FILTER(DATATYPE(?_lot_no) = <https://linked.data.gov.au/dataset/qld-addr/datatype/lot>)

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
    ).render(iris=iris)
