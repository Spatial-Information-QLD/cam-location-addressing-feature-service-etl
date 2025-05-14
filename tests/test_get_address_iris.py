from textwrap import dedent

from address_etl.get_address_iris import get_address_iris_query


def test_get_address_iris_query_limit_10():
    query = get_address_iris_query(10)
    query_limit_10 = dedent(
        """
        PREFIX addr: <https://linked.data.gov.au/def/addr/>
        SELECT ?iri
        WHERE {
            ?iri a addr:Address
        }
        LIMIT 10
        """
    )
    assert query == query_limit_10


def test_get_address_iris_query_no_limit():
    query = get_address_iris_query()
    query_no_limit = dedent(
        """
        PREFIX addr: <https://linked.data.gov.au/def/addr/>
        SELECT ?iri
        WHERE {
            ?iri a addr:Address
        }
    """
    )
    assert query == query_no_limit
