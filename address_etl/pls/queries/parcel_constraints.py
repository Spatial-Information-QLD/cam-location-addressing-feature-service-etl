from textwrap import dedent

PLAN_DATATYPE = "https://linked.data.gov.au/dataset/qld-addr/datatype/plan"
LOT_DATATYPE = "https://linked.data.gov.au/dataset/qld-addr/datatype/lot"
PLAN_NO_MAX_LENGTH = 10
LOT_NO_MAX_LENGTH = 5


def valid_parcel_identifier_filters(
    plan_variable: str = "?plan_no", lot_variable: str = "?_lot_no"
) -> str:
    return dedent(
        f"""
        FILTER(DATATYPE({plan_variable}) = <{PLAN_DATATYPE}>)
        FILTER(DATATYPE({lot_variable}) = <{LOT_DATATYPE}>)
        FILTER(STRLEN(STR({plan_variable})) <= {PLAN_NO_MAX_LENGTH})
        FILTER(STRLEN(STR({lot_variable})) <= {LOT_NO_MAX_LENGTH})
        """
    ).strip()
