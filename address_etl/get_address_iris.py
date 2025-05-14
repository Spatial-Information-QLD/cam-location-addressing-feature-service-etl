from textwrap import dedent

import httpx
from jinja2 import Template


def get_address_iris_query(limit: int | None = None):
    return (
        Template(
            dedent(
                """
                PREFIX addr: <https://linked.data.gov.au/def/addr/>
                SELECT ?iri
                WHERE {
                    ?iri a addr:Address
                }\
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
    query = get_address_iris_query(limit)
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
