from address_etl.get_address_concatenation import get_address_concatenation


def test_get_address_concatenation():
    row = {
        "lot": {"value": "36"},
        "plan": {"value": "SP185408"},
        "unit_type": {"value": "U"},
        "unit_number": {"value": "36"},
        "floor_type": {"value": "Level"},
        "floor_number": {"value": "14"},
        "street_no_1": {"value": "148"},
        "street_no_1_suffix": {"value": "C"},
        "street_number": {"value": "148"},
        "street_name": {"value": "Walker"},
        "street_type": {"value": "Street"},
        "street_full": {"value": "Walker Street"},
        "locality": {"value": "Townsville City"},
        "local_authority": {"value": "Townsville City"},
        "state": {"value": "QLD"},
        "address": {"value": "U36/148C Walker Street  Townsville City QLD"},
        "address_status": {"value": "P"},
        "address_standard": {"value": "UK"},
        "lotplan_status": {"value": "C"},
        "address_pid": {"value": 2077263},
    }

    address = get_address_concatenation(row)
    assert address == "U36/148C Walker Street Townsville City QLD"
