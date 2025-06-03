"""
Load geocodes to SIRRTE REST API.

Reads in a CSV dump of the geocodes from LALF
and loads them in to SIRRTE.
"""

import concurrent.futures
import logging
import csv
from pathlib import Path
import time

from address_etl.geocode_load import load_geocodes

# File dumped from cam-etl code.
CSV_FILE = Path("geocodes_for_esri.csv")

# 10 minutes
HTTP_TIMEOUT = 600
MAX_WORKERS = 4

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def main():
    if not CSV_FILE.exists():
        raise FileNotFoundError(f"File {CSV_FILE} does not exist.")

    with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []

        with open(CSV_FILE, "r") as f:
            reader = csv.DictReader(f)
            batch = []
            job_id = 1
            for row in reader:
                batch.append(row)
                if len(batch) == 10000:
                    futures.append(
                        executor.submit(load_geocodes, job_id, batch, HTTP_TIMEOUT)
                    )
                    batch = []
                    job_id += 1

            if batch:
                futures.append(
                    executor.submit(load_geocodes, job_id, batch, HTTP_TIMEOUT)
                )

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"A worker process failed with error: {e}")
                for f in futures:
                    f.cancel()
                raise


if __name__ == "__main__":
    start_time = time.time()
    main()
    logger.info(f"Total time taken: {time.time() - start_time:.2f} seconds")
