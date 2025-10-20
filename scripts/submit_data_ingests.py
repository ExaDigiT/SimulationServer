#!/usr/bin/env python3
from pathlib import Path
import urllib.parse
from typing import Any
import time, os
import pyjson5
import requests
from loguru import logger
import orjson


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
    ingest = pyjson5.loads(Path(file).read_text()) # using yaml as hack to allow comments
    logger.info(f"Submitting ingest for {file}...")
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
    DRUID_URL = os.environ.get("DRUID_URL", "http://localhost:8888")
    DRUID_USER = os.environ.get("DRUID_USER") or None # Convert "" to None
    DRUID_PASSWORD = os.environ.get("DRUID_PASSWORD") or None

    druid = DruidApi(DRUID_URL, DRUID_USER, DRUID_PASSWORD)

    submit_ingest(druid, "./druid_ingests/data-marconi100.json")
    submit_ingest(druid, "./druid_ingests/data-lassen-allocation-history.json")
    submit_ingest(druid, "./druid_ingests/data-lassen-node-history.json")
    submit_ingest(druid, "./druid_ingests/data-lassen-step-history.json")
    submit_ingest(druid, "./druid_ingests/data-fugaku.json")

    logger.info("Done!")

