import csv
import logging
import sqlite3
import time

import httpx

from address_etl.settings import settings
from address_etl.pls.queries import local_auth, locality, road, parcel, site
from address_etl.id_map import text_to_id_for_pk

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
            la_name TEXT CHECK (length(la_name) <= 40) NOT NULL
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
            FOREIGN KEY (la_code) REFERENCES local_auth(la_code) ON UPDATE CASCADE
        )
    """
    )


def create_road_tables(cursor: sqlite3.Cursor):
    logger.info("Creating road table")
    cursor.execute(
        """
        CREATE TABLE road (
            road_id TEXT PRIMARY KEY,
            road_cat TEXT CHECK (length(road_cat) <=20),
            road_name TEXT CHECK (length(road_name) <=50) NOT NULL,
            road_name_suffix TEXT CHECK (length(road_name_suffix) <= 30),
            road_name_type TEXT CHECK (length(road_name_type) <= 20),
            locality_code TEXT NOT NULL,
            road_cat_desc TEXT CHECK (length(road_cat_desc) = 1) NOT NULL,
            FOREIGN KEY (locality_code) REFERENCES locality(locality_code) ON UPDATE CASCADE
        )
    """
    )

    create_id_map_table("road_id_map", cursor)


def create_parcel_tables(cursor: sqlite3.Cursor):
    logger.info("Creating parcel table")
    cursor.execute(
        """
        CREATE TABLE parcel (
            parcel_id TEXT PRIMARY KEY,
            plan_no TEXT CHECK (length(plan_no) <= 10),
            lot_no TEXT CHECK (length(lot_no) <= 5)
        )
    """
    )

    create_id_map_table("parcel_id_map", cursor)


def create_site_tables(cursor: sqlite3.Cursor):
    logger.info("Creating site table")
    cursor.execute(
        """
        CREATE TABLE site (
            site_id TEXT PRIMARY KEY,
            parent_site_id TEXT,
            site_type TEXT CHECK (length(site_type) <= 50) NOT NULL,
            parcel_id TEXT NOT NULL,
            FOREIGN KEY (parent_site_id) REFERENCES site(site_id) ON UPDATE CASCADE,
            FOREIGN KEY (parcel_id) REFERENCES parcel(parcel_id) ON UPDATE CASCADE
        )
    """
    )

    create_id_map_table("site_id_map", cursor)


def create_geocode_tables(cursor: sqlite3.Cursor):
    logger.info("Creating geocode table")
    cursor.execute(
        """
        CREATE TABLE geocode (
            geocode_id TEXT PRIMARY KEY,
            geocode_type TEXT CHECK (length(geocode_type) <= 4) NOT NULL,
            site_id TEXT NOT NULL,
            centoid_lat REAL NOT NULL,
            centoid_lon REAL NOT NULL,
            FOREIGN KEY (site_id) REFERENCES site(site_id) ON UPDATE CASCADE
        )
    """
    )


def create_tables(cursor: sqlite3.Cursor):
    cursor.execute("PRAGMA foreign_keys = ON")
    create_locality_tables(cursor)
    create_road_tables(cursor)
    create_parcel_tables(cursor)
    create_site_tables(cursor)
    create_geocode_tables(cursor)
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
    query = road.get_query()
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
            "INSERT INTO road (road_id, road_name, road_name_suffix, road_name_type, locality_code, road_cat_desc) VALUES (?, ?, ?, ?, ?, ?)",
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
    query = parcel.get_query()
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
            "INSERT INTO parcel (parcel_id, plan_no, lot_no) VALUES (?, ?, ?)",
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
    query = site.get_query()
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
            "INSERT INTO site (site_id, parent_site_id, site_type, parcel_id) VALUES (?, ?, ?, ?)",
            (
                row["site_id"]["value"],
                row.get("parent_site_id", {}).get("value"),
                row["site_type"]["value"],
                row["parcel_id"]["value"],
            ),
        )

    cursor.connection.commit()
    logger.info(f"Time taken: {time.time() - start_time:.2f} seconds")


def populate_geocode_tables(
    http_client: httpx.Client, cursor: sqlite3.Cursor, pls_geocode_csv_path: str | None
):
    start_time = time.time()
    logger.info("Populating geocode table")
    if pls_geocode_csv_path:
        logger.info(f"Reading geocode data from {pls_geocode_csv_path}")
        with open(pls_geocode_csv_path, "r") as f:
            reader = csv.reader(f)
            for i, row in enumerate(reader):
                cursor.execute(
                    "INSERT INTO geocode (geocode_id, geocode_type, site_id, centoid_lat, centoid_lon) VALUES (?, ?, ?, ?, ?)",
                    (i, row[0], row[1], row[2], row[3]),
                )
    else:
        logger.info(
            "Populating geocode table from SIRRTE not yet implemented. Skipping"
        )

    cursor.connection.commit()
    logger.info(f"Time taken: {time.time() - start_time:.2f} seconds")


def populate_tables(cursor: sqlite3.Cursor):
    with httpx.Client(timeout=settings.http_timeout_in_seconds) as client:
        populate_locality_tables(client, cursor)
        populate_road_tables(client, cursor)
        populate_parcel_tables(client, cursor)
        populate_site_tables(client, cursor)
        populate_geocode_tables(client, cursor, settings.pls_geocode_csv_path)

    text_to_id_for_pk("road_id_map", "road", "road_id", cursor)
    text_to_id_for_pk("parcel_id_map", "parcel", "parcel_id", cursor)
    text_to_id_for_pk("site_id_map", "site", "site_id", cursor)
