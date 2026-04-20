"""Subset of import observability config: env keys for Sage bronze/silver blob credentials."""

from __future__ import annotations


def normalize_logical_import_service_env(raw: str) -> str:
    s = raw.strip()
    if not s:
        return s
    lower = s.lower()
    if lower == "local":
        return "local"
    return lower


def resolve_import_service_observability_env_key(raw: str) -> str:
    key = normalize_logical_import_service_env(raw.strip())
    if not key:
        raise ValueError("Import observability environment key must be non-empty.")
    return key


def map_import_service_env_to_import_blob_env_key(raw: str) -> str:
    s = raw.strip()
    if not s:
        return s
    lower = s.lower()
    if lower == "local":
        return "local"
    if lower == "dev":
        return "dev"
    if lower == "qa":
        return "qa"
    return "prod"
