from textwrap import dedent
from jinja2 import Template


def get_query(debug: bool = False):
    return Template(
        dedent(
            """
        PREFIX addr: <https://linked.data.gov.au/def/addr/>
        PREFIX cn: <https://linked.data.gov.au/def/cn/>
        PREFIX sdo: <https://schema.org/>

        SELECT (CONCAT(STR(?parcel_id), "|", STR(?address)) AS ?site_id) ?parent_site_id ?site_type ?parcel_id
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
                sdo:identifier ?plan_no, ?_lot_no .

                FILTER(DATATYPE(?plan_no) = <https://linked.data.gov.au/dataset/qld-addr/datatype/plan>)
                FILTER(DATATYPE(?_lot_no) = <https://linked.data.gov.au/dataset/qld-addr/datatype/lot>).

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
        ORDER BY ?parcel_id ?parent_site_id
        """
        )
    ).render(debug=debug)
