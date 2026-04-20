"""
Prefect flows: USITC chapter/section notes bronze download and bronze→silver HTML→JSON.

Portable copy of ``hs_data_chapter_section_notes_2b_from_usitc`` and
``hs_data_chapter_section_notes_b2s_from_html_to_json`` from kyg-classification-codes-dataflows.
"""

from __future__ import annotations

import os

from prefect import flow
from prefect.artifacts import create_markdown_artifact
from prefect.task_runners import ThreadPoolTaskRunner

from app import config
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
    chapter_futures = fetch_and_store_raw_chapter_notes_bronze.map(
        list(range(1, 100)),
        [bronze_datasource] * 99,
    )

    section_futures = []
    for section_index, chapter_nums in enumerate(all_sections, start=1):
        future = fetch_and_store_raw_section_notes_bronze.submit(
            section_index, chapter_nums, bronze_datasource
        )
        section_futures.append(future)

    chapter_results = [future.result() for future in chapter_futures]
    section_results = [future.result() for future in section_futures]

    successful_chapters = sum(1 for result in chapter_results if result)
    failed_chapters = len(chapter_results) - successful_chapters
    successful_sections = sum(1 for result in section_results if result)
    failed_sections = len(section_results) - successful_sections

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
