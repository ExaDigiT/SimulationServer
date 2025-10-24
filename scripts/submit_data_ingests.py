#!/usr/bin/env python3
"""
Submits the replay data ingests to druid.
"""
from pathlib import Path
import urllib.parse
from typing import Any
import time, os
import pyjson5
import requests
import getpass, argparse
from loguru import logger


class DruidApi:
    def __init__(self, url: str, user: str | None = None, password: str | None = None) -> None:
        self.url = url.removesuffix("/")
        self.user = user
        self.password = password

    def request(self, method, url, **kwargs) -> Any:
        url = urllib.parse.urljoin(self.url, url)
        if self.user and self.password:
            auth = (self.user, self.password)
        else:
            auth = None

        response = requests.request(method, url, timeout = 5 * 60, auth = auth, **kwargs)
        if not response.ok:
            raise Exception(f"Request {url} failed with {response.status_code}: {response.text}")

        if response.text.strip():
            return response.json()
        else: # Some druid endpoints return empty response
            return None



def submit_ingest(druid: DruidApi, file):
    logger.info(f"Submitting ingest for {file}...")
    ingest = pyjson5.loads(Path(file).read_text()) # using yaml as hack to allow comments
    ingest_type = ingest['type']

    if ingest_type == "kafka":
        response = druid.request("POST", "/druid/indexer/v1/supervisor", json = ingest)
        logger.info(f"Supervisor for {file} submitted")
        logger.info(f"See {druid.url}/unified-console.html to view the streaming ingest.")
    else:
        response = druid.request("POST", "/druid/indexer/v1/task", json = ingest)
        task_id = response['task']

        logger.info(f"See {druid.url}/unified-console.html#tasks/task_id~{task_id} to view ingest progress.")
        logger.info(f"Waiting for ingest{task_id} to complete...")

        status = "RUNNING"
        while status == "RUNNING":
            time.sleep(5)
            response = druid.request("GET", f"/druid/indexer/v1/task/{task_id}/status")
            status = response['status']['statusCode']
        if status != "SUCCESS":
            raise ValueError(f"Ingest for {file} failed!")
        else:
            logger.info(f"Ingest for {file} finished.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description = __doc__.strip(),
        formatter_class = argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("ingests", type = Path, nargs = "*", help = "List of druid ingests")
    args = parser.parse_args()

    if not args.ingests:
        ingests = sorted(Path("./druid_ingests").resolve().glob("data-*.json"))
    else:
        ingests = [Path(p).resolve() for p in args.ingests]

    druid_url = os.environ.get("DRUID_URL")
    if not druid_url:
        druid_url = input("Druid URL (http://localhost:8888): ")
        druid_url = druid_url.strip() or "http://localhost:8888"

    druid_username = os.environ.get("DRUID_USERNAME")
    if not druid_username:
        druid_username = input("Druid Username: ").strip() or None
    
    druid_password = os.environ.get("DRUID_PASSWORD")
    if not druid_password:
        druid_password = getpass.getpass("Druid Password: ").strip() or None

    druid = DruidApi(druid_url, druid_username, druid_password)

    for ingest in ingests:
        submit_ingest(druid, ingest)

    logger.info("Done!")
