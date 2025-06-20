from textwrap import dedent

from address_etl.address_iris import address_iris_query


def test_get_address_iris_query_limit_10():
    query = address_iris_query(10)
    query_limit_10 = dedent(
        """
        PREFIX addr: <https://linked.data.gov.au/def/addr/>
        PREFIX lc: <https://linked.data.gov.au/def/lifecycle/>
        PREFIX sdo: <https://schema.org/>
        PREFIX time: <http://www.w3.org/2006/time#>

        SELECT ?iri (MAX(?_start_time) AS ?start_time)
        WHERE {
            GRAPH <urn:qali:graph:addresses> {
                ?iri a addr:Address ;
                    lc:hasLifecycleStage ?lifecycle_stage .

                ?lifecycle_stage sdo:additionalType <https://linked.data.gov.au/def/lifecycle-stage-types/current> ;
                    time:hasBeginning/time:inXSDDateTime ?_start_time

                FILTER NOT EXISTS {
                    ?lifecycle_stage time:hasEnd ?end_time
                }
            }
        }
        GROUP BY ?iri

        LIMIT 10
        """
    ).strip()
    assert query == query_limit_10


def test_get_address_iris_query_no_limit():
    query = address_iris_query()
    query_no_limit = dedent(
        """
        PREFIX addr: <https://linked.data.gov.au/def/addr/>
        PREFIX lc: <https://linked.data.gov.au/def/lifecycle/>
        PREFIX sdo: <https://schema.org/>
        PREFIX time: <http://www.w3.org/2006/time#>

        SELECT ?iri (MAX(?_start_time) AS ?start_time)
        WHERE {
            GRAPH <urn:qali:graph:addresses> {
                ?iri a addr:Address ;
                    lc:hasLifecycleStage ?lifecycle_stage .

                ?lifecycle_stage sdo:additionalType <https://linked.data.gov.au/def/lifecycle-stage-types/current> ;
                    time:hasBeginning/time:inXSDDateTime ?_start_time

                FILTER NOT EXISTS {
                    ?lifecycle_stage time:hasEnd ?end_time
                }
            }
        }
        GROUP BY ?iri
    """
    ).strip()
    assert query == query_no_limit
