import logging

import httpx

logger = logging.getLogger(__name__)


def _raise_for_missing_key(
    response: httpx.Response,
    payload: dict,
    missing_key: str,
    context: str,
) -> None:
    error = payload.get("error")
    if error:
        details = "; ".join(error.get("details", []))
        raise RuntimeError(
            f"{context}: ESRI error {error.get('code')}: {error.get('message')}"
            + (f" ({details})" if details else "")
        )

    raise RuntimeError(
        f"{context}: expected '{missing_key}' in ESRI response, got: {response.text[:1000]}"
    )


def get_esri_token(
    esri_auth_url: str,
    referer: str,
    esri_username: str,
    esri_password: str,
    client: httpx.Client,
    expiration_in_minutes: int = 15,
) -> str:
    """Get an ESRI token for the given username and password"""
    params = {"f": "json", "referer": referer, "expiration": expiration_in_minutes}
    data = {
        "username": esri_username,
        "password": esri_password,
    }

    response = client.post(esri_auth_url, params=params, data=data)
    try:
        response.raise_for_status()
        payload = response.json()
        token = payload.get("token")
        if token is None:
            _raise_for_missing_key(
                response, payload, "token", "Error getting ESRI token"
            )
        return token
    except Exception as e:
        logger.error(f"Error getting ESRI token: {response.text}")
        raise e


def get_total_count(
    esri_url: str, client: httpx.Client, access_token: str, params: dict | None = None
) -> int:
    """Get the total number of records from the service"""
    params = params or {
        "where": "1=1",
        "returnCountOnly": "true",
        "f": "json",
    }
    params["token"] = access_token

    response = client.get(esri_url, params=params)
    try:
        response.raise_for_status()
        payload = response.json()
        count = payload.get("count")
        if count is None:
            _raise_for_missing_key(
                response, payload, "count", "Error getting total count"
            )
        return int(count)
    except Exception as e:
        logger.error(f"Error getting total count: {response.text}")
        raise e


def get_count(
    where_clause: str,
    esri_url: str,
    client: httpx.Client,
    access_token: str,
    params: dict | None = None,
) -> int:
    """Get the number of records from the service that match the where clause"""
    params = params or {
        "where": where_clause,
        "returnCountOnly": "true",
        "f": "json",
    }
    params["token"] = access_token

    response = client.post(esri_url, data=params)
    try:
        response.raise_for_status()
        payload = response.json()
        count = payload.get("count")
        if count is None:
            _raise_for_missing_key(
                response, payload, "count", "Error getting records count"
            )
        return int(count)
    except Exception as e:
        logger.error(
            f"Error getting records count ({response.status_code}): {response.text[:1000]}"
        )
        logger.info(f"Where clause: {where_clause[:500]}")
        raise e
