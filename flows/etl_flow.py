"""A small extract/transform/load flow demonstrating retries, logging, and task return values.

Run locally:
    python -m flows.etl_flow
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

import httpx
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient, ContentSettings
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret
from pymongo import MongoClient

EXTRACT_URL = "https://httpbin.org/uuid"
AZURE_CONTAINER = "ptest"


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


def _credentials_from_ptest_block() -> dict[str, str]:
    """Read Mongo connection settings from the Prefect Secret block named ``ptest``.

    The secret value must be JSON: ``{"uri": "<mongo connection string>", "ptest": "<database name>"}``.
    """
    raw = Secret.load("ptest").get()
    if isinstance(raw, dict):
        data = raw
    elif isinstance(raw, str):
        data = json.loads(raw)
    else:
        raise TypeError("Block ptest must resolve to a JSON object (dict or JSON string).")
    uri = data.get("uri")
    db_name = data.get("ptest")
    if not uri or not db_name:
        raise ValueError('Secret ptest JSON must include non-empty "uri" and "ptest" keys.')
    return {"uri": str(uri), "db": str(db_name)}


@task
def load(record: dict[str, Any]) -> str:
    logger = get_run_logger()
    creds = _credentials_from_ptest_block()
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    collection_name = f"UUID_{ts}"

    with MongoClient(creds["uri"], serverSelectionTimeoutMS=10_000) as client:
        coll = client[creds["db"]][collection_name]
        coll.insert_one(dict(record))
    logger.info(
        "Inserted transformed row into %s.%s (document keys: %s)",
        creds["db"],
        collection_name,
        list(record),
    )
    return collection_name


def _azure_blob_service_url_from_secret() -> str:
    """Build blob service ``account_url`` for ``BlobServiceClient`` from Secret ``azure-ptest``.

    Accepts either:

    - Full service URL including SAS, e.g. ``https://acct.blob.core.windows.net?sv=...``
    - SAS token only: set ``AZURE_STORAGE_ACCOUNT`` to the storage account name; the secret
      may be ``?sv=...`` or ``sv=...`` and is appended to that account's blob endpoint.
    """
    raw = Secret.load("azure-ptest").get()
    if not isinstance(raw, str):
        raw = str(raw)
    s = raw.strip()
    if not s:
        raise ValueError("Secret azure-ptest is empty.")
    if s.startswith("https://"):
        return s
    account = os.environ.get("AZURE_STORAGE_ACCOUNT", "").strip()
    if not account:
        raise ValueError(
            "When azure-ptest is not a full https:// URL, set AZURE_STORAGE_ACCOUNT to the "
            "storage account name (the secret is then treated as the SAS query string)."
        )
    base = f"https://{account}.blob.core.windows.net"
    if s.startswith("?"):
        return f"{base}{s}"
    return f"{base}?{s}"


@task
def upload_transformed_json_to_azure(record: dict[str, Any], blob_stem: str) -> str:
    """Write ``record`` as JSON to container ``ptest``; blob name ``{blob_stem}.json``."""
    logger = get_run_logger()
    account_url = _azure_blob_service_url_from_secret()
    service = BlobServiceClient(account_url=account_url)
    blob_name = f"{blob_stem}.json"
    container = service.get_container_client(AZURE_CONTAINER)
    try:
        container.create_container()
    except ResourceExistsError:
        pass
    blob = container.get_blob_client(blob_name)
    body = json.dumps(record, indent=2).encode("utf-8")
    blob.upload_blob(
        body,
        overwrite=True,
        content_settings=ContentSettings(content_type="application/json"),
    )
    logger.info("Uploaded JSON to container %s blob %s", AZURE_CONTAINER, blob_name)
    return blob_name


@flow(name="etl-flow")
def etl_flow(url: str = EXTRACT_URL) -> dict[str, Any]:
    raw = extract(url)
    record = transform(raw)
    collection_name = load(record)
    upload_transformed_json_to_azure(record, collection_name)
    return record


if __name__ == "__main__":
    etl_flow()
