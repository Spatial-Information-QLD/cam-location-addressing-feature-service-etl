import logging

import httpx

logger = logging.getLogger(__name__)


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
        return response.json()["token"]
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
        return int(response.json()["count"])
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

    response = client.get(esri_url, params=params)
    try:
        response.raise_for_status()
        return int(response.json()["count"])
    except Exception as e:
        logger.error(f"Error getting records count: {response.text}")
        raise e
