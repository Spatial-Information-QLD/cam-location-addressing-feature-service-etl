from textwrap import dedent


def get_query():
    return dedent(
        """
        PREFIX addr: <https://linked.data.gov.au/def/addr/>
        PREFIX sdo: <https://schema.org/>

        SELECT ?parcel_id ?plan_no ?lot_no
        WHERE {
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
