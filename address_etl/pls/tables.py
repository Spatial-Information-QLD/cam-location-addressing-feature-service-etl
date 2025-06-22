import csv
import logging
import sqlite3
import time

import httpx

from address_etl.settings import settings
from address_etl.pls.queries import (
    local_auth,
    locality,
    road,
    parcel,
    site,
    address,
    place_name,
)
from address_etl.id_map import text_to_id_for_pk
from address_etl.tables import create_metadata_table

logger = logging.getLogger(__name__)


def create_id_map_table(table_name: str, cursor: sqlite3.Cursor):
    logger.info(f"Creating {table_name} table")
    cursor.execute(
        f"""
        CREATE TABLE {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            iri TEXT UNIQUE
        )
    """
    )
    cursor.execute(f"CREATE INDEX idx_{table_name}_iri ON {table_name} (iri)")


def create_locality_tables(cursor: sqlite3.Cursor):
    logger.info("Creating local_auth table")
    cursor.execute(
        """
        CREATE TABLE local_auth (
            la_code INTEGER PRIMARY KEY,
            la_name TEXT CHECK (length(la_name) <= 40) NOT NULL,
            hash TEXT
        )
    """
    )

    logger.info("Creating local_auth_loaded table")
    cursor.execute(
        """
        CREATE TABLE local_auth_loaded (
            la_code INTEGER,
            loaded BOOLEAN DEFAULT FALSE
        )
        """
    )

    logger.info("Creating locality table")
    cursor.execute(
        """
        CREATE TABLE locality (
            locality_code TEXT PRIMARY KEY CHECK (length(locality_code) <= 20),
            locality_name TEXT CHECK (length(locality_name) <= 40) NOT NULL,
            locality_type TEXT CHECK (length(locality_type) <= 5) NOT NULL,
            la_code INTEGER NOT NULL,
            state TEXT CHECK (state = 'QLD') NOT NULL,
            status TEXT CHECK (length(status) = 1) NOT NULL,
            hash TEXT,
            FOREIGN KEY (la_code) REFERENCES local_auth(la_code) ON UPDATE CASCADE
        )
    """
    )

    logger.info("Creating locality_loaded table")
    cursor.execute(
        """
        CREATE TABLE locality_loaded (
            locality_code TEXT,
            loaded BOOLEAN DEFAULT FALSE
        )
        """
    )

    cursor.execute("CREATE INDEX idx_locality_la_code ON locality (la_code)")


def create_road_tables(cursor: sqlite3.Cursor):
    logger.info("Creating lf_road table")
    cursor.execute(
        """
        CREATE TABLE lf_road (
            road_id TEXT PRIMARY KEY,
            road_cat TEXT CHECK (length(road_cat) <=20),
            road_name TEXT CHECK (length(road_name) <=50) NOT NULL,
            road_name_suffix TEXT CHECK (length(road_name_suffix) <= 30),
            road_name_type TEXT CHECK (length(road_name_type) <= 20),
            locality_code TEXT NOT NULL,
            road_cat_desc TEXT CHECK (length(road_cat_desc) = 1) NOT NULL,
            hash TEXT,
            FOREIGN KEY (locality_code) REFERENCES locality(locality_code) ON UPDATE CASCADE
        )
    """
    )

    logger.info("Creating lf_road_loaded table")
    cursor.execute(
        """
        CREATE TABLE lf_road_loaded (
            road_id TEXT,
            loaded BOOLEAN DEFAULT FALSE
        )
    """
    )

    create_id_map_table("lf_road_id_map", cursor)
    cursor.execute("CREATE INDEX idx_lf_road_locality_code ON lf_road (locality_code)")


def create_parcel_tables(cursor: sqlite3.Cursor):
    logger.info("Creating lf_parcel table")
    cursor.execute(
        """
        CREATE TABLE lf_parcel (
            parcel_id TEXT PRIMARY KEY,
            plan_no TEXT CHECK (length(plan_no) <= 10),
            lot_no TEXT CHECK (length(lot_no) <= 5),
            hash TEXT
        )
    """
    )

    logger.info("Creating lf_parcel_loaded table")
    cursor.execute(
        """
        CREATE TABLE lf_parcel_loaded (
            parcel_id TEXT,
            loaded BOOLEAN DEFAULT FALSE
        )
        """
    )

    create_id_map_table("lf_parcel_id_map", cursor)
    cursor.execute("CREATE INDEX idx_lf_parcel_plan_lot ON lf_parcel(plan_no, lot_no)")


def create_site_tables(cursor: sqlite3.Cursor):
    logger.info("Creating lf_site table")
    cursor.execute(
        """
        CREATE TABLE lf_site (
            site_id TEXT PRIMARY KEY,
            parent_site_id TEXT,
            site_type TEXT CHECK (length(site_type) <= 50) NOT NULL,
            parcel_id TEXT NOT NULL,
            hash TEXT,
            FOREIGN KEY (parent_site_id) REFERENCES lf_site(site_id) ON UPDATE CASCADE,
            FOREIGN KEY (parcel_id) REFERENCES lf_parcel(parcel_id) ON UPDATE CASCADE
        )
    """
    )

    logger.info("Creating lf_site_loaded table")
    cursor.execute(
        """
        CREATE TABLE lf_site_loaded (
            site_id TEXT,
            loaded BOOLEAN DEFAULT FALSE
        )
        """
    )

    create_id_map_table("lf_site_id_map", cursor)
    cursor.execute("CREATE INDEX idx_lf_site_parcel_id ON lf_site (parcel_id)")
    cursor.execute(
        "CREATE INDEX idx_lf_site_parent_site_id ON lf_site (parent_site_id)"
    )


def create_place_name_tables(cursor: sqlite3.Cursor):
    logger.info("Creating lf_place_name table")
    cursor.execute(
        """
        CREATE TABLE lf_place_name (
            place_name_id TEXT PRIMARY KEY,
            pl_name_status_code TEXT CHECK (length(pl_name_status_code) = 1) NOT NULL,
            pl_name_type_code TEXT CHECK (length(pl_name_type_code) <= 4) NOT NULL,
            pl_name TEXT CHECK (length(pl_name) <= 60) NOT NULL,
            site_id TEXT NOT NULL,
            hash TEXT,
            FOREIGN KEY (site_id) REFERENCES lf_site(site_id) ON UPDATE CASCADE
        )
    """
    )

    logger.info("Creating lf_place_name_loaded table")
    cursor.execute(
        """
        CREATE TABLE lf_place_name_loaded (
            place_name_id TEXT,
            loaded BOOLEAN DEFAULT FALSE
        )
    """
    )

    create_id_map_table("lf_place_name_id_map", cursor)
    cursor.execute("CREATE INDEX idx_lf_place_name_site_id ON lf_place_name (site_id)")


def create_geocode_tables(cursor: sqlite3.Cursor):
    logger.info("Creating lf_geocode_sp_survey_point table")
    cursor.execute(
        """
        CREATE TABLE lf_geocode_sp_survey_point (
            geocode_id TEXT PRIMARY KEY,
            geocode_type TEXT CHECK (length(geocode_type) <= 4) NOT NULL,
            address_pid TEXT NOT NULL,
            site_id TEXT,
            centoid_lat REAL NOT NULL,
            centoid_lon REAL NOT NULL,
            hash TEXT
        )
    """
    )

    cursor.execute(
        "CREATE INDEX idx_lf_geocode_sp_survey_point_address_pid ON lf_geocode_sp_survey_point (address_pid)"
    )
    cursor.execute(
        "CREATE INDEX idx_lf_geocode_sp_survey_point_site_id ON lf_geocode_sp_survey_point (site_id)"
    )


def create_address_tables(cursor: sqlite3.Cursor):
    logger.info("Creating address table")
    cursor.execute(
        """
        CREATE TABLE lf_address (
            addr_id TEXT PRIMARY KEY,
            address_pid TEXT NOT NULL,
            parcel_id TEXT NOT NULL,
            addr_status_code TEXT CHECK (length(addr_status_code) = 1) NOT NULL,
            unit_type TEXT CHECK (length(unit_type) <= 50),
            unit_no TEXT CHECK (length(unit_no) <= 5),
            unit_suffix TEXT CHECK (length(unit_suffix) <= 1),
            level_type TEXT CHECK (length(level_type) <= 20),
            level_no TEXT CHECK (length(level_no) <= 20),
            level_suffix TEXT CHECK (length(level_suffix) <= 5),
            street_no_first TEXT CHECK (length(street_no_first) <= 10),
            street_no_first_suffix TEXT CHECK (length(street_no_first_suffix) <= 10),
            street_no_last TEXT CHECK (length(street_no_last) <= 10),
            street_no_last_suffix TEXT CHECK (length(street_no_last_suffix) <= 10),
            road_id TEXT NOT NULL,
            site_id TEXT NOT NULL,
            location_desc TEXT CHECK (length(location_desc) <= 50),
            address_standard TEXT CHECK (length(address_standard) <= 10) NOT NULL,
            hash TEXT,
            FOREIGN KEY (parcel_id) REFERENCES lf_parcel(parcel_id) ON UPDATE CASCADE,
            FOREIGN KEY (road_id) REFERENCES lf_road(road_id) ON UPDATE CASCADE,
            FOREIGN KEY (site_id) REFERENCES lf_site(site_id) ON UPDATE CASCADE
        )
    """
    )

    logger.info("Creating lf_address_loaded table")
    cursor.execute(
        """
        CREATE TABLE lf_address_loaded (
            addr_id TEXT,
            loaded BOOLEAN DEFAULT FALSE
        )
        """
    )

    cursor.execute(
        "CREATE INDEX idx_lf_address_address_pid ON lf_address (address_pid)"
    )
    cursor.execute("CREATE INDEX idx_lf_address_parcel_id ON lf_address (parcel_id)")
    cursor.execute("CREATE INDEX idx_lf_address_road_id ON lf_address (road_id)")
    cursor.execute("CREATE INDEX idx_lf_address_site_id ON lf_address (site_id)")

    create_id_map_table("lf_address_id_map", cursor)


def create_tables(cursor: sqlite3.Cursor):
    cursor.execute("PRAGMA foreign_keys = ON")
    create_metadata_table(cursor)
    create_locality_tables(cursor)
    create_road_tables(cursor)
    create_parcel_tables(cursor)
    create_site_tables(cursor)
    create_place_name_tables(cursor)
    create_geocode_tables(cursor)
    create_address_tables(cursor)
    cursor.connection.commit()


def populate_locality_tables(client: httpx.Client, cursor: sqlite3.Cursor):
    start_time = time.time()
    logger.info("Fetching locality data")

    # local_auth table
    query = local_auth.get_query()
    response = client.post(
        settings.sparql_endpoint,
        headers={
            "Content-Type": "application/sparql-query",
            "Accept": "application/sparql-results+json",
        },
        data=query,
    )
    if response.status_code != 200:
        raise Exception(f"Locality query failed: {response.text}")

    rows = response.json()["results"]["bindings"]
    logger.info(f"Found {len(rows)} local_auth rows")
    for row in rows:
        cursor.execute(
            "INSERT INTO local_auth (la_code, la_name) VALUES (?, ?)",
            (row["la_code"]["value"], row["lga_name"]["value"]),
        )

    cursor.connection.commit()

    # locality table
    query = locality.get_query()
    response = client.post(
        settings.sparql_endpoint,
        headers={
            "Content-Type": "application/sparql-query",
            "Accept": "application/sparql-results+json",
        },
        data=query,
    )
    if response.status_code != 200:
        raise Exception(f"Locality query failed: {response.text}")

    rows = response.json()["results"]["bindings"]
    logger.info(f"Found {len(rows)} locality rows")
    for row in rows:
        cursor.execute(
            "INSERT INTO locality (locality_code, locality_name, locality_type, la_code, state, status) VALUES (?, ?, ?, ?, ?, ?)",
            (
                row["locality_code"]["value"],
                row["locality_name"]["value"],
                row["locality_type"]["value"],
                row["la_code"]["value"],
                row["state"]["value"],
                row["status"]["value"],
            ),
        )

    cursor.connection.commit()
    logger.info(f"Time taken: {time.time() - start_time:.2f} seconds")


def populate_road_tables(client: httpx.Client, cursor: sqlite3.Cursor):
    start_time = time.time()
    logger.info("Fetching road data")
    query = road.get_query(settings.debug)
    response = client.post(
        settings.sparql_endpoint,
        headers={
            "Content-Type": "application/sparql-query",
            "Accept": "application/sparql-results+json",
        },
        data=query,
    )
    if response.status_code != 200:
        raise Exception(f"Road query failed: {response.text}")

    rows = response.json()["results"]["bindings"]
    logger.info(f"Found {len(rows)} road rows")
    for row in rows:
        cursor.execute(
            "INSERT INTO lf_road (road_id, road_name, road_name_suffix, road_name_type, locality_code, road_cat_desc) VALUES (?, ?, ?, ?, ?, ?)",
            (
                row["road_id"]["value"],
                row["road_name"]["value"],
                row.get("road_name_suffix", {}).get("value"),
                row.get("road_name_type", {}).get("value"),
                row["locality_code"]["value"],
                row["road_cat_desc"]["value"],
            ),
        )

    cursor.connection.commit()
    logger.info(f"Time taken: {time.time() - start_time:.2f} seconds")


def populate_parcel_tables(client: httpx.Client, cursor: sqlite3.Cursor):
    start_time = time.time()
    logger.info("Fetching parcel data")
    query = parcel.get_query(settings.debug)
    response = client.post(
        settings.sparql_endpoint,
        headers={
            "Content-Type": "application/sparql-query",
            "Accept": "application/sparql-results+json",
        },
        data=query,
    )
    if response.status_code != 200:
        raise Exception(f"Parcel query failed: {response.text}")

    rows = response.json()["results"]["bindings"]
    logger.info(f"Found {len(rows)} parcel rows")
    for row in rows:
        cursor.execute(
            "INSERT INTO lf_parcel (parcel_id, plan_no, lot_no) VALUES (?, ?, ?)",
            (
                row["parcel_id"]["value"],
                row["plan_no"]["value"],
                row["lot_no"]["value"],
            ),
        )

    cursor.connection.commit()
    logger.info(f"Time taken: {time.time() - start_time:.2f} seconds")


def populate_site_tables(client: httpx.Client, cursor: sqlite3.Cursor):
    start_time = time.time()
    logger.info("Fetching site data")
    query = site.get_query(settings.debug)
    response = client.post(
        settings.sparql_endpoint,
        headers={
            "Content-Type": "application/sparql-query",
            "Accept": "application/sparql-results+json",
        },
        data=query,
    )
    if response.status_code != 200:
        raise Exception(f"Site query failed: {response.text}")

    rows = response.json()["results"]["bindings"]
    logger.info(f"Found {len(rows)} site rows")
    for row in rows:
        cursor.execute(
            "INSERT INTO lf_site (site_id, parent_site_id, site_type, parcel_id) VALUES (?, ?, ?, ?)",
            (
                row["site_id"]["value"],
                row.get("parent_site_id", {}).get("value"),
                row["site_type"]["value"],
                row["parcel_id"]["value"],
            ),
        )

    cursor.connection.commit()
    logger.info(f"Time taken: {time.time() - start_time:.2f} seconds")


def populate_place_name_tables(client: httpx.Client, cursor: sqlite3.Cursor):
    start_time = time.time()
    logger.info("Fetching place name data")
    query = place_name.get_query(settings.debug)
    response = client.post(
        settings.sparql_endpoint,
        headers={
            "Content-Type": "application/sparql-query",
            "Accept": "application/sparql-results+json",
        },
        data=query,
    )
    if response.status_code != 200:
        raise Exception(f"Place name query failed: {response.text}")

    rows = response.json()["results"]["bindings"]
    logger.info(f"Found {len(rows)} place name rows")
    for row in rows:
        cursor.execute(
            "INSERT INTO lf_place_name (place_name_id, pl_name_status_code, pl_name_type_code, pl_name, site_id) VALUES (?, ?, ?, ?, ?)",
            (
                row["place_name_id"]["value"],
                row["pl_name_status_code"]["value"],
                row["pl_name_type_code"]["value"],
                row["pl_name"]["value"],
                row["site_id"]["value"],
            ),
        )

    cursor.connection.commit()
    logger.info(f"Time taken: {time.time() - start_time:.2f} seconds")


def populate_address_tables(client: httpx.Client, cursor: sqlite3.Cursor):
    start_time = time.time()
    logger.info("Populating address table")
    query = address.get_query(settings.debug)
    response = client.post(
        settings.sparql_endpoint,
        headers={
            "Content-Type": "application/sparql-query",
            "Accept": "application/sparql-results+json",
        },
        data=query,
    )
    if response.status_code != 200:
        raise Exception(f"Address query failed: {response.text}")

    rows = response.json()["results"]["bindings"]
    logger.info(f"Found {len(rows)} address rows")
    for row in rows:
        cursor.execute(
            "INSERT INTO lf_address (addr_id, address_pid, parcel_id, addr_status_code, unit_type, unit_no, unit_suffix, level_type, level_no, level_suffix, street_no_first, street_no_first_suffix, street_no_last, street_no_last_suffix, road_id, site_id, location_desc, address_standard) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                row["addr_id"]["value"],
                row["address_pid"]["value"],
                row["parcel_id"]["value"],
                row["addr_status_code"]["value"],
                row.get("unit_type", {}).get("value"),
                row.get("unit_no", {}).get("value"),
                row.get("unit_suffix", {}).get("value"),
                row.get("level_type", {}).get("value"),
                row.get("level_no", {}).get("value"),
                row.get("level_suffix", {}).get("value"),
                row.get("street_no_first", {}).get("value"),
                row.get("street_no_first_suffix", {}).get("value"),
                row.get("street_no_last", {}).get("value"),
                row.get("street_no_last_suffix", {}).get("value"),
                row["road_id"]["value"],
                row["site_id"]["value"],
                row.get("location_desc", {}).get("value"),
                row["address_standard"]["value"],
            ),
        )

    cursor.connection.commit()
    logger.info(f"Time taken: {time.time() - start_time:.2f} seconds")


def update_geocode_site_id(cursor: sqlite3.Cursor):
    start_time = time.time()
    logger.info("Updating geocode table with site_id")
    cursor.execute(
        "UPDATE lf_geocode_sp_survey_point SET site_id = (SELECT site_id FROM lf_address WHERE lf_address.address_pid = lf_geocode_sp_survey_point.address_pid)"
    )

    # Create a new table with the foreign key constraint
    cursor.execute(
        """
        CREATE TABLE lf_geocode_sp_survey_point_new (
            geocode_id TEXT PRIMARY KEY,
            geocode_type TEXT CHECK (length(geocode_type) <= 4) NOT NULL,
            address_pid TEXT NOT NULL,
            site_id TEXT,
            centoid_lat REAL NOT NULL,
            centoid_lon REAL NOT NULL,
            hash TEXT,
            FOREIGN KEY (site_id) REFERENCES lf_site(site_id) ON UPDATE CASCADE
        )
    """
    )

    # Copy data from old table to new table, excluding NULL site_ids
    cursor.execute(
        """
        INSERT INTO lf_geocode_sp_survey_point_new 
        SELECT * FROM lf_geocode_sp_survey_point
    """
    )

    # Drop the old table and rename the new one
    cursor.execute("DROP TABLE lf_geocode_sp_survey_point")
    cursor.execute(
        "ALTER TABLE lf_geocode_sp_survey_point_new RENAME TO lf_geocode_sp_survey_point"
    )

    # Recreate the indexes
    cursor.execute(
        "CREATE INDEX idx_lf_geocode_sp_survey_point_address_pid ON lf_geocode_sp_survey_point (address_pid)"
    )
    cursor.execute(
        "CREATE INDEX idx_lf_geocode_sp_survey_point_site_id ON lf_geocode_sp_survey_point (site_id)"
    )

    cursor.connection.commit()
    logger.info(f"Time taken: {time.time() - start_time:.2f} seconds")


def populate_tables(cursor: sqlite3.Cursor):
    with httpx.Client(timeout=settings.http_timeout_in_seconds) as client:
        populate_locality_tables(client, cursor)
        populate_road_tables(client, cursor)
        populate_parcel_tables(client, cursor)
        populate_site_tables(client, cursor)
        populate_place_name_tables(client, cursor)
        populate_address_tables(client, cursor)

        update_geocode_site_id(cursor)

    text_to_id_for_pk("lf_road_id_map", "lf_road", "road_id", cursor)
    text_to_id_for_pk("lf_parcel_id_map", "lf_parcel", "parcel_id", cursor)
    text_to_id_for_pk("lf_site_id_map", "lf_site", "site_id", cursor)
    text_to_id_for_pk("lf_place_name_id_map", "lf_place_name", "place_name_id", cursor)
    text_to_id_for_pk("lf_address_id_map", "lf_address", "addr_id", cursor)
