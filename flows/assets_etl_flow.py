"""Multi-layer ETL flow designed to exercise Prefect Cloud assets (materializations, lineage).

Writes bronze → silver → gold JSON artifacts under ``asset-etl-demo/<flow_run_id>/`` in the
``ptest`` Azure container (same credentials as ``etl_flow`` — Secret block ``azure-ptest``).

Run locally:
    python -m flows.assets_etl_flow

See: https://docs.prefect.io/v3/concepts/assets
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

import httpx
from azure.storage.blob import BlobServiceClient, ContainerClient, ContentSettings
from prefect import flow, get_run_logger, task
from prefect.assets import Asset, AssetProperties, add_asset_metadata, materialize
from prefect.blocks.system import Secret
from prefect.context import get_run_context

AZURE_STORAGE_ACCOUNT = "jmdtestingkyg"
AZURE_CONTAINER = "ptest"
JSON_CONTENT_TYPE = "application/json"

# Upstream HTTP sources as referenced assets (no query strings in keys — Prefect convention).
HTTPBIN_JSON = "prefect://external/httpbin.org/json"
HTTPBIN_UUID = "prefect://external/httpbin.org/uuid"
HTTPBIN_HEADERS = "prefect://external/httpbin.org/headers"

# Placeholder keys for ``@materialize``; real blob URIs are set via ``with_options(assets=[...])``.
_PLACEHOLDER_BRONZE = "prefect://asset-etl-demo/materializations/bronze-raw-bundle"
_PLACEHOLDER_SILVER = "prefect://asset-etl-demo/materializations/silver-normalized"
_PLACEHOLDER_GOLD = "prefect://asset-etl-demo/materializations/gold-summary"

PIPELINE_RUN_ASSET = Asset(
    key="prefect://asset-etl-demo/pipeline/medallion-etl-run",
    properties=AssetProperties(
        name="Asset ETL — Medallion pipeline run",
        description=(
            "End-to-end demo: parallel HTTP extracts, bronze bundle, silver normalization, "
            "gold aggregates; all blobs in Azure container ``ptest``."
        ),
    ),
)


def _flow_run_slug() -> str:
    try:
        return str(get_run_context().flow_run.id)
    except Exception:
        return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def _azure_container_client_from_secret() -> ContainerClient:
    raw = Secret.load("azure-ptest").get()
    if not isinstance(raw, str):
        raw = str(raw)
    s = raw.strip()
    if not s:
        raise ValueError("Secret azure-ptest is empty.")

    if s.startswith("https://"):
        path_first = (urlparse(s).path.strip("/").split("/") or [""])[0]
        if path_first == AZURE_CONTAINER:
            return ContainerClient.from_container_url(s)
        return BlobServiceClient(account_url=s).get_container_client(AZURE_CONTAINER)

    qs = s[1:] if s.startswith("?") else s
    container_url = (
        f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net/{AZURE_CONTAINER}?{qs}"
    )
    return ContainerClient.from_container_url(container_url)


def _blob_stem(run_id: str, layer: str, name: str) -> str:
    return f"asset-etl-demo/{run_id}/{layer}/{name}"


def _azure_blob_asset_uri(stem: str) -> str:
    """HTTPS URI for a blob ``{stem}.json`` in ``ptest`` (used as Prefect asset key)."""
    return (
        f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net/"
        f"{AZURE_CONTAINER}/{stem}.json"
    )


@task(retries=2, retry_delay_seconds=5)
def extract_parallel_sources() -> dict[str, Any]:
    """Fetch several httpbin endpoints and merge into one bronze-shaped payload."""
    logger = get_run_logger()
    out: dict[str, Any] = {}
    specs = [
        ("uuid", "https://httpbin.org/uuid", HTTPBIN_UUID),
        ("sample_json", "https://httpbin.org/json", HTTPBIN_JSON),
        ("headers_echo", "https://httpbin.org/headers", HTTPBIN_HEADERS),
    ]
    with httpx.Client(timeout=15.0) as client:
        for key, url, _dep in specs:
            r = client.get(url)
            r.raise_for_status()
            try:
                out[key] = r.json()
            except Exception:
                out[key] = {"raw_text": r.text[:2000]}
            logger.info("Extracted %s from %s", key, url)
    out["_extracted_at"] = datetime.now(timezone.utc).isoformat()
    return out


@task
def transform_to_silver(bronze: dict[str, Any]) -> dict[str, Any]:
    """Normalize keys, add deterministic ids and simple quality flags (silver layer)."""
    logger = get_run_logger()
    uuid_val = (
        bronze.get("uuid", {})
        if isinstance(bronze.get("uuid"), dict)
        else {}
    )
    raw_id = str(uuid_val.get("uuid", "")).replace("-", "")
    normalized = {
        "silver_schema_version": 1,
        "source_uuid_compact": raw_id,
        "slideshow": bronze.get("sample_json", {}).get("slideshow", {}),
        "header_keys": list(
            (bronze.get("headers_echo") or {}).get("headers", {}).keys(),
        ),
        "row_quality_ok": bool(raw_id) and len(bronze.get("sample_json", {})) > 0,
    }
    payload = json.dumps(normalized, sort_keys=True).encode()
    normalized["content_sha256"] = hashlib.sha256(payload).hexdigest()
    logger.info("Silver record keys: %s", list(normalized))
    return normalized


@task
def aggregate_for_gold(silver: dict[str, Any]) -> dict[str, Any]:
    """Derive summary metrics for the gold layer."""
    logger = get_run_logger()
    slides = silver.get("slideshow") or {}
    author = slides.get("author") if isinstance(slides, dict) else None
    gold = {
        "gold_schema_version": 1,
        "author": author,
        "header_count": len(silver.get("header_keys") or []),
        "quality_flag": silver.get("row_quality_ok"),
        "checksum": silver.get("content_sha256"),
    }
    logger.info("Gold summary: %s", gold)
    return gold


@materialize(
    _PLACEHOLDER_BRONZE,
    asset_deps=[HTTPBIN_JSON, HTTPBIN_UUID, HTTPBIN_HEADERS],
)
def upload_bronze_bundle(bronze: dict[str, Any], stem: str) -> str:
    logger = get_run_logger()
    container = _azure_container_client_from_secret()
    blob_name = f"{stem}.json"
    blob = container.get_blob_client(blob_name)
    body = json.dumps(bronze, indent=2).encode("utf-8")
    blob.upload_blob(
        body,
        overwrite=True,
        content_settings=ContentSettings(content_type=JSON_CONTENT_TYPE),
    )
    key = _azure_blob_asset_uri(stem)
    logger.info("Bronze uploaded: %s", blob_name)
    add_asset_metadata(
        key,
        {
            "layer": "bronze",
            "blob_name": blob_name,
            "container": AZURE_CONTAINER,
            "bytes": len(body),
            "top_level_keys": list(bronze.keys()),
        },
    )
    return blob_name


@materialize(
    _PLACEHOLDER_SILVER,
    asset_deps=[],
)
def upload_silver_normalized(silver: dict[str, Any], stem: str) -> str:
    logger = get_run_logger()
    container = _azure_container_client_from_secret()
    blob_name = f"{stem}.json"
    blob = container.get_blob_client(blob_name)
    body = json.dumps(silver, indent=2).encode("utf-8")
    blob.upload_blob(
        body,
        overwrite=True,
        content_settings=ContentSettings(content_type=JSON_CONTENT_TYPE),
    )
    key = _azure_blob_asset_uri(stem)
    logger.info("Silver uploaded: %s", blob_name)
    add_asset_metadata(
        key,
        {
            "layer": "silver",
            "blob_name": blob_name,
            "bytes": len(body),
            "quality_ok": silver.get("row_quality_ok"),
        },
    )
    return blob_name


@materialize(
    _PLACEHOLDER_GOLD,
    asset_deps=[],
)
def upload_gold_summary(gold: dict[str, Any], stem: str) -> str:
    logger = get_run_logger()
    container = _azure_container_client_from_secret()
    blob_name = f"{stem}.json"
    blob = container.get_blob_client(blob_name)
    body = json.dumps(gold, indent=2).encode("utf-8")
    blob.upload_blob(
        body,
        overwrite=True,
        content_settings=ContentSettings(content_type=JSON_CONTENT_TYPE),
    )
    key = _azure_blob_asset_uri(stem)
    logger.info("Gold uploaded: %s", blob_name)
    add_asset_metadata(
        key,
        {
            "layer": "gold",
            "blob_name": blob_name,
            "bytes": len(body),
            "author": gold.get("author"),
        },
    )
    return blob_name


@materialize(PIPELINE_RUN_ASSET, asset_deps=[])
def record_pipeline_completion(
    bronze_key: str,
    silver_key: str,
    gold_key: str,
    run_id: str,
) -> None:
    PIPELINE_RUN_ASSET.add_metadata(
        {
            "run_id": run_id,
            "bronze_asset": bronze_key,
            "silver_asset": silver_key,
            "gold_asset": gold_key,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }
    )


@flow(name="assets-etl-flow")
def assets_etl_flow() -> dict[str, Any]:
    """Bronze → silver → gold with explicit Azure asset keys and pipeline-level asset."""
    run_id = _flow_run_slug()
    stem_bronze = _blob_stem(run_id, "bronze", "raw_bundle")
    stem_silver = _blob_stem(run_id, "silver", "normalized")
    stem_gold = _blob_stem(run_id, "gold", "summary")

    bronze_key = _azure_blob_asset_uri(stem_bronze)
    silver_key = _azure_blob_asset_uri(stem_silver)
    gold_key = _azure_blob_asset_uri(stem_gold)

    raw = extract_parallel_sources()
    upload_bronze_bundle.with_options(
        assets=[bronze_key],
        asset_deps=[HTTPBIN_JSON, HTTPBIN_UUID, HTTPBIN_HEADERS],
    )(raw, stem_bronze)

    silver = transform_to_silver(raw)
    upload_silver_normalized.with_options(
        assets=[silver_key],
        asset_deps=[bronze_key],
    )(silver, stem_silver)

    gold = aggregate_for_gold(silver)
    upload_gold_summary.with_options(
        assets=[gold_key],
        asset_deps=[silver_key],
    )(gold, stem_gold)

    record_pipeline_completion.with_options(
        asset_deps=[bronze_key, silver_key, gold_key],
    )(bronze_key, silver_key, gold_key, run_id)

    return {
        "run_id": run_id,
        "assets": {
            "bronze": bronze_key,
            "silver": silver_key,
            "gold": gold_key,
            "pipeline": PIPELINE_RUN_ASSET.key,
        },
    }


if __name__ == "__main__":
    assets_etl_flow()
