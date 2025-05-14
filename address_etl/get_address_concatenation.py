def get_address_concatenation(row: dict) -> str:
    def get_value(key: str) -> str:
        return row.get(key, {}).get("value", "")

    street_no_2 = get_value("street_no_2")
    unit_number = get_value("unit_number")
    return f"{get_value('unit_type')}{unit_number}{get_value('unit_suffix')}{'/' if unit_number else ''}{get_value('street_no_1')}{get_value('street_no_1_suffix')}{'-' if street_no_2 else ''}{street_no_2}{get_value('street_no_2_suffix')} {get_value('street_full')} {get_value('locality')} {get_value('state')}"
