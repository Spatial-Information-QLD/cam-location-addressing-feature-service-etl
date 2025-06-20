from textwrap import dedent

import httpx
from jinja2 import Template


def address_iris_query(limit: int | None = None):
    """
    Get the query to get the address IRIs.
    """
    return (
        Template(
            dedent(
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
                {% if limit %}
                LIMIT {{ limit }}
                {% endif %}
                """
            )
        )
        .render(limit=limit)
        .strip()
    )


def get_address_iris(
    sparql_endpoint: str, client: httpx.Client, limit: int | None = None
):
    """
    Get the address IRIs from the SPARQL endpoint.

    The limit parameter is optional and only used for testing.
    """
    query = address_iris_query(limit)
    response = client.post(
        sparql_endpoint,
        headers={
            "Content-Type": "application/sparql-query",
            "Accept": "application/sparql-results+json",
        },
        data=query,
    )
    if response.status_code != 200:
        raise Exception(f"Failed to get address IRIs: {response.text}")
    return [row["iri"]["value"] for row in response.json()["results"]["bindings"]]
