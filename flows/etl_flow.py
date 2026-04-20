"""A small extract/transform/load flow demonstrating retries, logging, and task return values.

Run locally:
    python -m flows.etl_flow
"""

from __future__ import annotations

from typing import Any

import httpx
from prefect import flow, get_run_logger, task

EXTRACT_URL = "https://httpbin.org/uuid"


@task(retries=2, retry_delay_seconds=5)
def extract(url: str = EXTRACT_URL) -> dict[str, Any]:
    logger = get_run_logger()
    logger.info("Extracting payload from %s", url)
    response = httpx.get(url, timeout=10.0)
    response.raise_for_status()
    return response.json()


@task
def transform(payload: dict[str, Any]) -> dict[str, Any]:
    logger = get_run_logger()
    raw_uuid = payload.get("uuid", "")
    transformed = {
        "id": raw_uuid,
        "id_no_dashes": raw_uuid.replace("-", ""),
        "length": len(raw_uuid),
    }
    logger.info("Transformed record: %s", transformed)
    return transformed


@task
def load(record: dict[str, Any]) -> None:
    logger = get_run_logger()
    # For a real pipeline this would write to a warehouse/DB/object store;
    # logging here keeps the example free of external dependencies.
    logger.info("Loaded record: %s", record)


@flow(name="etl-flow")
def etl_flow(url: str = EXTRACT_URL) -> dict[str, Any]:
    raw = extract(url)
    record = transform(raw)
    load(record)
    return record


if __name__ == "__main__":
    etl_flow()
