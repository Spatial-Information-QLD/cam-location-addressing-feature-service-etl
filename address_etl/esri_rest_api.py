import httpx


def get_esri_token(
    esri_auth_url: str,
    referer: str,
    esri_username: str,
    esri_password: str,
    client: httpx.Client,
    expiration_in_minutes: int = 10,
) -> str:
    """Get an ESRI token for the given username and password"""
    params = {"f": "json", "referer": referer, "expiration": expiration_in_minutes}
    data = {
        "username": esri_username,
        "password": esri_password,
    }

    response = client.post(esri_auth_url, params=params, data=data)
    response.raise_for_status()
    return response.json()["token"]
