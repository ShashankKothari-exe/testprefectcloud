"""Stable URI keys for Prefect Assets (HS chapter/section notes flows)."""

from __future__ import annotations

from pathlib import Path

from app import config
from app.type.data_source import DataSource

# Upstream REST APIs (referenced assets / external deps)
USITC_REST_BASE = "https://hts.usitc.gov/reststop/"


def usitc_chapter_notes_dependency(chapter_num: int) -> str:
    """Lineage key for the USITC chapter-notes endpoint (no ``?`` — Prefect Asset keys forbid it)."""
    return f"prefect://external/hts.usitc.gov/reststop/getChapterNotes/doc/{int(chapter_num)}"


def usitc_section_notes_dependency(first_chapter_num: int) -> str:
    """Lineage key for the USITC section-notes endpoint (no query string)."""
    return f"prefect://external/hts.usitc.gov/reststop/getSectionNotes/doc/{int(first_chapter_num)}"


def _azure_blob_uri(blob_path: str) -> str:
    """``blob_path`` is ``container/path/to/blob`` (no leading slash)."""
    return (
        f"https://{config.AZURE_STORAGE_ACCOUNT}.blob.core.windows.net/"
        f"{blob_path.lstrip('/')}"
    )


def _local_file_uri(absolute_path: Path) -> str:
    return absolute_path.resolve().as_uri()


def storage_blob_or_file_uri(datasource: DataSource, relative_under_root: str) -> str:
    """URI for an object under bronze/silver root (matches ``load_data_to_storage`` paths)."""
    full = f"{datasource.path}/{relative_under_root}"
    if datasource.type == "AZURE_BLOB_STORAGE":
        return _azure_blob_uri(full)
    local_path = Path(full)
    if not local_path.is_absolute():
        local_path = Path.cwd() / local_path
    return _local_file_uri(local_path)


def bronze_chapter_html_uri(datasource: DataSource, chapter_num: int) -> str:
    rel = f"chapter_section_wco_notes/html/chapter_notes/chapter_{chapter_num:02d}.html"
    return storage_blob_or_file_uri(datasource, rel)


def bronze_section_html_uri(datasource: DataSource, section_index: int) -> str:
    rel = f"chapter_section_wco_notes/html/section_notes/section_{section_index:02d}.html"
    return storage_blob_or_file_uri(datasource, rel)


def bronze_html_prefix_uri(datasource: DataSource) -> str:
    """Logical folder for all bronze HTML (for silver flow lineage)."""
    rel = "chapter_section_wco_notes/html/"
    return storage_blob_or_file_uri(datasource, rel)


def silver_json_prefix_uri(datasource: DataSource) -> str:
    """Logical folder for silver JSON outputs."""
    rel = "chapter_section_wco_notes/json/"
    return storage_blob_or_file_uri(datasource, rel)


# Placeholder for @materialize default when real key is set via ``with_options``
MATERIALIZE_PLACEHOLDER_BRONZE_CHAPTER = (
    "prefect://hs-notes/materializations/bronze-chapter-html"
)
MATERIALIZE_PLACEHOLDER_BRONZE_SECTION = (
    "prefect://hs-notes/materializations/bronze-section-html"
)
