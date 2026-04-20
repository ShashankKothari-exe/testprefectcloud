"""
Prefect flows: USITC chapter/section notes bronze download and bronze→silver HTML→JSON.

Portable copy of ``hs_data_chapter_section_notes_2b_from_usitc`` and
``hs_data_chapter_section_notes_b2s_from_html_to_json`` from kyg-classification-codes-dataflows.
"""

from __future__ import annotations

import os

from prefect import flow
from prefect.artifacts import create_markdown_artifact
from prefect.assets import Asset, AssetProperties, materialize
from prefect.task_runners import ThreadPoolTaskRunner

from app import config
from app.lib.hs_notes_asset_keys import (
    USITC_REST_BASE,
    bronze_chapter_html_uri,
    bronze_html_prefix_uri,
    bronze_section_html_uri,
    silver_json_prefix_uri,
    usitc_chapter_notes_dependency,
    usitc_section_notes_dependency,
)
from app.lib.notes_utils import get_chapter_numbers_grouped_by_section
from app.lib.storage_utils import (
    get_bronze_datasource_for_env,
    get_silver_datasource_for_env,
)
from app.tasks.notes.chapter_notes import (
    fetch_and_store_raw_chapter_notes_bronze,
    transform_and_load_chapters_from_bronze_to_silver,
)
from app.tasks.notes.section_notes import (
    fetch_and_store_raw_section_notes_bronze,
    transform_and_load_sections_from_bronze_to_silver,
)
from app.type.data_source import DataSource

# Flow-level assets: summarize each pipeline in the Prefect UI asset graph.
BRONZE_PIPELINE_ASSET = Asset(
    key="prefect://hs-notes/pipeline/bronze-usitc-download",
    properties=AssetProperties(
        name="HS Notes — Bronze (USITC HTML)",
        description=(
            "End-to-end bronze load: 99 chapter HTML files + 21 section HTML files "
            "from USITC REST APIs into bronze storage."
        ),
    ),
)

SILVER_PIPELINE_ASSET = Asset(
    key="prefect://hs-notes/pipeline/silver-json-from-html",
    properties=AssetProperties(
        name="HS Notes — Silver (JSON)",
        description=(
            "Bronze→silver transforms: parsed JSON, HTS reference maps, and section notes."
        ),
    ),
)


@materialize(BRONZE_PIPELINE_ASSET, asset_deps=[USITC_REST_BASE])
def record_bronze_pipeline_completion(
    successful_chapters: int,
    failed_chapters: int,
    successful_sections: int,
    failed_sections: int,
) -> None:
    BRONZE_PIPELINE_ASSET.add_metadata(
        {
            "successful_chapters": successful_chapters,
            "failed_chapters": failed_chapters,
            "successful_sections": successful_sections,
            "failed_sections": failed_sections,
        }
    )


@materialize(SILVER_PIPELINE_ASSET, asset_deps=[])
def record_silver_pipeline_completion(
    successful_chapter_transforms: int,
    failed_chapter_transforms: int,
    successful_section_transforms: int,
    failed_section_transforms: int,
) -> None:
    SILVER_PIPELINE_ASSET.add_metadata(
        {
            "successful_chapter_transforms": successful_chapter_transforms,
            "failed_chapter_transforms": failed_chapter_transforms,
            "successful_section_transforms": successful_section_transforms,
            "failed_section_transforms": failed_section_transforms,
        }
    )


@flow(
    task_runner=ThreadPoolTaskRunner(max_workers=config.CONCURRENT_EXECUTION_COUNT),
    log_prints=True,
)
def hs_data_chapter_section_notes_2b_from_usitc(
    bronze_datasource: DataSource | None = None,
):
    """Download raw chapter and section notes to bronze layer."""
    env = os.environ.get("EXECUTION_ENVIRONMENT") or "local"
    print(
        f"STARTING FLOW FOR CHAPTER SECTION NOTES BRONZE DOWNLOAD in environment: {env}"
    )

    bronze_datasource = bronze_datasource or get_bronze_datasource_for_env(env, "sage")

    create_markdown_artifact(
        markdown=f"""
            # Environment Variables

            ## Processing Details
            - **Execution Environment**: {env}
            - **Bronze Datasource**: {bronze_datasource}
            
            """,
        key="chapter-section-fetch-environment-summary",
    )

    all_sections = get_chapter_numbers_grouped_by_section()

    print("Starting bronze download for 99 chapters and 21 sections...")
    chapter_futures = [
        fetch_and_store_raw_chapter_notes_bronze.with_options(
            assets=[bronze_chapter_html_uri(bronze_datasource, n)],
            asset_deps=[usitc_chapter_notes_dependency(n)],
        ).submit(n, bronze_datasource)
        for n in range(1, 100)
    ]

    section_futures = []
    for section_index, chapter_nums in enumerate(all_sections, start=1):
        future = fetch_and_store_raw_section_notes_bronze.with_options(
            assets=[bronze_section_html_uri(bronze_datasource, section_index)],
            asset_deps=[usitc_section_notes_dependency(chapter_nums[0])],
        ).submit(section_index, chapter_nums, bronze_datasource)
        section_futures.append(future)

    chapter_results = [future.result() for future in chapter_futures]
    section_results = [future.result() for future in section_futures]

    successful_chapters = sum(1 for result in chapter_results if result)
    failed_chapters = len(chapter_results) - successful_chapters
    successful_sections = sum(1 for result in section_results if result)
    failed_sections = len(section_results) - successful_sections

    record_bronze_pipeline_completion(
        successful_chapters,
        failed_chapters,
        successful_sections,
        failed_sections,
    )

    create_markdown_artifact(
        markdown=f"""
            # Chapter and Section Notes Bronze Download Summary

            ## Download Results
            - **Chapters**: {successful_chapters} successful, {failed_chapters} failed (out of 99 total)
            - **Sections**: {successful_sections} successful, {failed_sections} failed (out of 21 total)
            """,
        key="chapter-section-bronze-download-summary",
    )

    print("Completed bronze download of 99 chapters and 21 sections.")


@flow(
    task_runner=ThreadPoolTaskRunner(max_workers=config.CONCURRENT_EXECUTION_COUNT),
    log_prints=True,
)
def hs_data_chapter_section_notes_b2s_from_html_to_json(
    bronze_datasource: DataSource | None = None,
    silver_datasource: DataSource | None = None,
):
    """Process chapter and section notes from bronze layer to silver layer."""
    env = os.environ.get("EXECUTION_ENVIRONMENT") or "local"
    print(
        f"STARTING FLOW FOR CHAPTER SECTION NOTES SILVER PROCESS in environment: {env}"
    )

    bronze_datasource = bronze_datasource or get_bronze_datasource_for_env(env, "sage")
    silver_datasource = silver_datasource or get_silver_datasource_for_env(env, "sage")

    create_markdown_artifact(
        markdown=f"""
            # Environment Variables

            ## Processing Details
            - **Execution Environment**: {env}
            - **Bronze Datasource**: {bronze_datasource}
            - **Silver Datasource**: {silver_datasource}
            
            """,
        key="chapter-section-html-to-json-environment-summary",
    )

    all_sections = get_chapter_numbers_grouped_by_section()

    print(
        "Starting silver processing for 99 chapters and 21 sections (including subchapters for 98/99)..."
    )

    chapter_transform_results = transform_and_load_chapters_from_bronze_to_silver(
        bronze_datasource, silver_datasource
    )
    section_transform_results = transform_and_load_sections_from_bronze_to_silver(
        bronze_datasource, silver_datasource, all_sections
    )
    successful_chapter_transforms = sum(
        1 for result in chapter_transform_results if result
    )
    failed_chapter_transforms = (
        len(chapter_transform_results) - successful_chapter_transforms
    )
    successful_section_transforms = sum(
        1 for result in section_transform_results if result
    )
    failed_section_transforms = (
        len(section_transform_results) - successful_section_transforms
    )

    record_silver_pipeline_completion.with_options(
        asset_deps=[
            bronze_html_prefix_uri(bronze_datasource),
            silver_json_prefix_uri(silver_datasource),
            BRONZE_PIPELINE_ASSET,
        ],
    )(
        successful_chapter_transforms,
        failed_chapter_transforms,
        successful_section_transforms,
        failed_section_transforms,
    )

    create_markdown_artifact(
        markdown=f"""
            # Chapter and Section Notes Bronze to Silver Transform Summary

            ## Transform Results
            - **Chapters**: {successful_chapter_transforms} successful, {failed_chapter_transforms} failed (out of 99 total)
            - **Sections**: {successful_section_transforms} successful, {failed_section_transforms} failed (out of 21 total)
            """,
        key="chapter-section-bronze-to-silver-transform-summary",
    )

    print("Completed silver processing of 99 chapters and 21 sections.")

    print("Silver processing completed.")
