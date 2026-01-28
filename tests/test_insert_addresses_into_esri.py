import json
import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from address_etl.address_tables import create_tables
from address_etl.crud import insert_addresses_into_esri
from address_etl.sqlite_dict_factory import dict_row_factory


@pytest.fixture
def db_cursor():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = dict_row_factory
    cursor = connection.cursor()
    create_tables(cursor)
    yield cursor
    connection.close()


def test_insert_addresses_includes_floor_fields(db_cursor):
    """
    Test that the insert_addresses_into_esri function includes unit_suffix and floor fields
    in the payload sent to ESRI.
    """
    # Insert dummy data into address_current
    db_cursor.execute(
        """
        INSERT INTO address_current (
            address_pid,
            floor_type,
            floor_number,
            floor_suffix,
            unit_suffix,
            address,
            longitude,
            latitude
        ) VALUES (
            'test_pid_1',
            'FL',
            '1',
            'A',
            'B',
            'Flat 1A, Level 1, 123 Test St',
            153.0,
            -27.0
        )
        """
    )
    db_cursor.connection.commit()

    # Mock httpx.Client and get_esri_token
    with patch("address_etl.crud.httpx.Client") as mock_client_cls, \
         patch("address_etl.crud.get_esri_token") as mock_get_token:

        mock_client = mock_client_cls.return_value
        mock_client.__enter__.return_value = mock_client
        mock_get_token.return_value = "fake_token"

        # Mock the response for _insert_addresses_into_esri
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"success": true}'
        mock_client.post.return_value = mock_response

        # Call the function
        insert_addresses_into_esri(['test_pid_1'], "http://esri-fake-url", db_cursor)

        # Verify the call to client.post
        assert mock_client.post.called

        # Find the call with the payload
        found_payload = False
        for args, kwargs in mock_client.post.call_args_list:
            if "data" in kwargs:
                data = kwargs["data"]
                if "adds" in data:
                    adds = json.loads(data["adds"])
                    attributes = adds[0]["attributes"]

                    assert attributes["floor_type"] == "FL"
                    assert attributes["floor_number"] == "1"
                    assert attributes["floor_suffix"] == "A"
                    assert attributes["unit_suffix"] == "B"
                    assert attributes["address_pid"] == "test_pid_1"
                    found_payload = True

        assert found_payload, "Did not find a POST call with 'adds' data containing floor fields"
