from textwrap import dedent

from address_etl.address_iris import address_iris_query


def test_get_address_iris_query_limit_10():
    query = address_iris_query(10)
    query_limit_10 = dedent(
        """
        PREFIX addr: <https://linked.data.gov.au/def/addr/>
        SELECT ?iri
        WHERE {
            ?iri a addr:Address
        }

        LIMIT 10
        """
    ).strip()
    assert query == query_limit_10


def test_get_address_iris_query_no_limit():
    query = address_iris_query()
    query_no_limit = dedent(
        """
        PREFIX addr: <https://linked.data.gov.au/def/addr/>
        SELECT ?iri
        WHERE {
            ?iri a addr:Address
        }
    """
    ).strip()
    assert query == query_no_limit
