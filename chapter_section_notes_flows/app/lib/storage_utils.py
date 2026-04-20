"""Storage helpers used by chapter/section notes flows (subset of main dataflows repo)."""

from __future__ import annotations

import io
import os
from dataclasses import asdict
from pathlib import Path
from typing import Literal
from urllib.parse import urlparse

from adlfs import AzureBlobFileSystem
from prefect.blocks.system import Secret

from app import config as app_config
from app.lib.import_service_observability.import_observability_config import (
    resolve_import_service_observability_env_key,
)
from app.type.data_source import ABSCreds, DataSource

AZURE_CREDS_REQUIRED_MSG = (
    "Azure Blob Storage requires credentials (ABSCreds) in the DataSource object."
)


def _abs_creds_from_azure_ptest_secret() -> ABSCreds:
    """Build ``ABSCreds`` from the Prefect Secret block used by ``flows/etl_flow.py`` (``azure-ptest``).

    Accepts the same secret shapes as ``_azure_container_client_from_secret`` there:

    - **JSON dict** with ``account_name`` and ``sas_token`` (optional ``account_name`` if omitted uses config).
    - **HTTPS URL** to the account or container (SAS in query string).
    - **SAS query string** only (``?sv=...`` or ``sv=...``), combined with ``AZURE_STORAGE_ACCOUNT``.
    """
    raw = Secret.load(app_config.AZURE_PT_SECRET_BLOCK).get()
    if isinstance(raw, dict):
        account = (raw.get("account_name") or app_config.AZURE_STORAGE_ACCOUNT or "").strip()
        sas = raw.get("sas_token") or raw.get("credential") or ""
        if not account or not sas:
            raise ValueError(
                f"Secret {app_config.AZURE_PT_SECRET_BLOCK!r} dict must include "
                "account_name (or rely on CCT_AZURE_STORAGE_ACCOUNT) and sas_token."
            )
        return ABSCreds(
            account_name=account,
            sas_token=_normalize_sas_query(str(sas)),
        )

    s = raw.strip() if isinstance(raw, str) else str(raw).strip()
    if not s:
        raise ValueError(f"Secret {app_config.AZURE_PT_SECRET_BLOCK!r} is empty.")

    if s.startswith("https://"):
        parsed = urlparse(s)
        host = parsed.hostname or ""
        if not host.endswith(".blob.core.windows.net"):
            raise ValueError(
                "Azure secret URL must be an https://*.blob.core.windows.net/... URL."
            )
        account_name = host.split(".", 1)[0]
        qs = parsed.query
        if not qs:
            raise ValueError("Azure secret URL must include a SAS query string (?sv=...).")
        return ABSCreds(
            account_name=account_name,
            sas_token=_normalize_sas_query(qs),
        )

    qs = s[1:] if s.startswith("?") else s
    return ABSCreds(
        account_name=app_config.AZURE_STORAGE_ACCOUNT,
        sas_token=_normalize_sas_query(qs),
    )


def _normalize_sas_query(qs: str) -> str:
    return qs[1:] if qs.startswith("?") else qs


def _classification_codes_project_root() -> Path:
    """
    Repo root (directory containing ``flows.py`` and ``app/``).

    Set ``CLASSIFICATION_CODES_PROJECT_ROOT`` to this folder if auto-detection fails
    (e.g. unconventional working directory).
    """
    env = (os.environ.get("CLASSIFICATION_CODES_PROJECT_ROOT") or "").strip()
    if env:
        return Path(env).expanduser().resolve()

    def _looks_like_bundle_root(p: Path) -> bool:
        return (p / "flows.py").is_file() and (p / "app").is_dir()

    here = Path(__file__).resolve()
    for base in (here, *here.parents):
        if _looks_like_bundle_root(base):
            return base

    cwd = Path.cwd().resolve()
    for base in (cwd, *cwd.parents):
        if _looks_like_bundle_root(base):
            return base

    return here.parents[2]


def load_data_to_storage(
    file: str | io.BytesIO,
    destination_path: str,
    destination_type: Literal["LOCAL_FILE_SYSTEM", "AZURE_BLOB_STORAGE"],
    creds: ABSCreds | None = None,
) -> None:
    if destination_type == "AZURE_BLOB_STORAGE":
        abfs = AzureBlobFileSystem(**asdict(creds))

        with abfs.open(destination_path, "wb") as dest:
            if hasattr(file, "read"):
                file.seek(0)
                dest.write(file.read())
            elif isinstance(file, str):
                with open(file, "rb") as src:
                    dest.write(src.read())
            elif isinstance(file, bytes):
                dest.write(file)
            else:
                raise TypeError("Unsupported file type for upload")
        print(f"File uploaded to: {destination_path}")

    elif destination_type == "LOCAL_FILE_SYSTEM":
        local_path = Path(destination_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)

        with open(local_path, "wb") as dest:
            if hasattr(file, "read"):
                file.seek(0)
                dest.write(file.read())
            elif isinstance(file, str):
                with open(file, "rb") as src:
                    dest.write(src.read())
            elif isinstance(file, bytes):
                dest.write(file)
            else:
                raise TypeError("Unsupported file type for upload")
        print(f"File saved locally to: {destination_path}")

    else:
        raise ValueError(f"Unsupported datasource type: {destination_type}")


def get_bronze_datasource_for_env(
    env: str,
    _service_type: Literal["sage", "import"] = "sage",
    *,
    _import_blob_cred_env: str | None = None,
) -> DataSource:
    key = resolve_import_service_observability_env_key(env)
    if key == "local":
        datasource_type = "LOCAL_FILE_SYSTEM"
        datasource_path = str(_classification_codes_project_root() / "bronze")
        datasource = DataSource(type=datasource_type, path=datasource_path)
    else:
        datasource_type = "AZURE_BLOB_STORAGE"
        datasource_path = app_config.AZURE_BLOB_CONTAINER
        datasource = DataSource(
            type=datasource_type,
            path=datasource_path,
            creds=_abs_creds_from_azure_ptest_secret(),
        )

    return datasource


def get_silver_datasource_for_env(
    env: str,
    _service_type: Literal["sage", "import"] = "sage",
    *,
    _import_blob_cred_env: str | None = None,
) -> DataSource:
    key = resolve_import_service_observability_env_key(env)
    if key == "local":
        datasource_type = "LOCAL_FILE_SYSTEM"
        datasource_path = str(_classification_codes_project_root() / "silver")
        datasource = DataSource(type=datasource_type, path=datasource_path)
    else:
        datasource_type = "AZURE_BLOB_STORAGE"
        datasource_path = app_config.AZURE_BLOB_CONTAINER
        datasource = DataSource(
            type=datasource_type,
            path=datasource_path,
            creds=_abs_creds_from_azure_ptest_secret(),
        )

    return datasource


def read_file_content_from_storage(
    data_source: DataSource,
    file_path: str,
) -> bytes:
    if data_source.type == "AZURE_BLOB_STORAGE":
        if not data_source.creds:
            raise TypeError(AZURE_CREDS_REQUIRED_MSG)
        abfs = AzureBlobFileSystem(**asdict(data_source.creds))
        try:
            with abfs.open(file_path, "rb") as f:
                return f.read()
        except FileNotFoundError as e:
            raise FileNotFoundError(
                f"File not found in Azure Blob Storage: {file_path}"
            ) from e
    elif data_source.type == "LOCAL_FILE_SYSTEM":
        local_path = Path(file_path)
        if not local_path.exists():
            raise FileNotFoundError(f"File not found locally: {file_path}")
        with open(local_path, "rb") as f:
            return f.read()
    else:
        raise ValueError(f"Unsupported datasource type: {data_source.type}")
