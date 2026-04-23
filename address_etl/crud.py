import logging

import backoff
import httpx

from address_etl.settings import settings

logger = logging.getLogger(__name__)


def on_backoff_handler(details):
    logger.warning(
        f"Backing off {details['wait']} seconds after {details['tries']} tries"
        f" calling function {details['target']}"
        f"with exception: {details['exception']}"
    )


@backoff.on_exception(
    backoff.expo,
    (httpx.HTTPError,),
    max_time=settings.http_retry_max_time_in_seconds,
    on_backoff=on_backoff_handler,
)
def sparql_query(
    sparql_endpoint: str, query: str, client: httpx.Client
) -> httpx.Response:
    response = client.post(
        sparql_endpoint,
        headers={
            "Content-Type": "application/sparql-query",
            "Accept": "application/sparql-results+json",
        },
        data=query,
    )
    try:
        response.raise_for_status()
        return response
    except Exception as error:
        if hasattr(error, "response") and error.response is not None:
            logger.error(
                "Error querying SPARQL endpoint (%s): %s",
                error.response.status_code,
                error.response.text,
            )
        else:
            logger.error("Error querying SPARQL endpoint: %s", error)
        raise error
