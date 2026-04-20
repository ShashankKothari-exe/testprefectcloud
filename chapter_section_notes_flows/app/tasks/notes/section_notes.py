import json
import os
from datetime import datetime
from io import BytesIO

from bs4 import BeautifulSoup
from prefect import task
from prefect.artifacts import create_markdown_artifact

from app.lib.notes_utils import (
    BS_PARSER,
    extract_referenced_data_from_chapter_and_section_notes,
    fetch_html,
    get_chapter_numbers_grouped_by_section,
    parse_section_notes,
    read_raw_notes,
)
from app.lib.storage_utils import load_data_to_storage
from app.type.data_source import DataSource


@task
def fetch_and_store_raw_section_notes_bronze(
    section_index, chapter_nums, bronze_datasource: DataSource
):
    """Fetch section notes HTML from USITC API and store to bronze layer.

    Raises:
        requests.HTTPError: If the API returns a non-200 status code.
        ValueError: If the API returns an empty response.
    """
    html = fetch_html(
        f"https://hts.usitc.gov/reststop/getSectionNotes?doc={chapter_nums[0]}"
    )

    file_path = f"{bronze_datasource.path}/chapter_section_wco_notes/html/section_notes/section_{section_index:02d}.html"
    html_bytes = BytesIO(html.encode("utf-8"))
    load_data_to_storage(
        html_bytes, file_path, bronze_datasource.type, bronze_datasource.creds
    )

    # Create markdown artifact
    chapter_range = (
        f"{chapter_nums[0]}-{chapter_nums[-1]}"
        if len(chapter_nums) > 1
        else str(chapter_nums[0])
    )
    create_markdown_artifact(
        markdown=f"""
            # Section {section_index} HTML Fetch & Store Summary

            ## Processing Details
            - **Section Number**: {section_index}
            - **Chapter Range**: {chapter_range}
            - **Storage Status**: ✅ Successfully stored
            """,
        key=f"section-{section_index}-fetch-summary",
    )
    return html


@task
def transform_and_load_sections_from_bronze_to_silver(
    bronze_datasource: DataSource, silver_datasource: DataSource, all_sections: list
) -> list[bool]:
    """Process sections from bronze layer to silver layer."""
    results = []
    hts_to_references = {}
    for section_index, chapter_nums in enumerate(all_sections, start=1):
        print(f"Processing section {section_index} of 21")
        html = read_raw_notes("section", section_index, bronze_datasource, "bronze")
        if not html:
            results.append(False)
            continue
        try:
            section_data = parse_section_notes(html)
            section_code = section_data.get("section_title", "").split()[-1]
            section_data["section_code"] = section_code
            section_data["section_number"] = section_index
            section_data.pop("chapters", None)

            # Extract references and build mapping
            soup = BeautifulSoup(html, BS_PARSER)
            for div in soup.find_all("div", class_="misc_title"):
                div.decompose()
            references = extract_referenced_data_from_chapter_and_section_notes(
                str(soup),
                get_chapter_numbers_grouped_by_section()[section_index - 1][0],
            )
            for code in references["hts_references"]["hts_codes_references"]:
                if code not in hts_to_references:
                    hts_to_references[code.replace(".", "")] = {
                        "section_references": []
                    }
                hts_to_references[code.replace(".", "")]["section_references"].append(
                    section_index
                )
            section_data.update(references)
            # Store JSON to silver blob
            file_path = f"{silver_datasource.path}/chapter_section_wco_notes/json/section_notes/section_{section_index:02d}.json"
            json_bytes = json.dumps(section_data, indent=2).encode("utf-8")
            load_data_to_storage(
                json_bytes, file_path, silver_datasource.type, silver_datasource.creds
            )

            # Create markdown artifact
            env = os.environ.get("EXECUTION_ENVIRONMENT", "unknown")
            chapter_range = (
                f"{chapter_nums[0]}-{chapter_nums[-1]}"
                if len(chapter_nums) > 1
                else str(chapter_nums[0])
            )

            create_markdown_artifact(
                markdown=f"""
                    # Section {section_index} Bronze to Silver Transform Summary

                    ## Processing Details
                    - **Section Number**: {section_index}
                    - **Section Title**: {section_data.get("section_title", "Unknown")}
                    - **Chapter Range**: {chapter_range}
                    - **Chapters Covered**: {len(chapter_nums)} chapters ({", ".join(map(str, chapter_nums))})
                    - **Source File**: section_{section_index:02d}.html
                    - **Target File**: {file_path}
                    - **JSON Size**: {len(json_bytes)} bytes
                    - **Processing Status**: ✅ Successfully transformed and stored
                    - **Environment**: {env}
                    - **Timestamp**: {datetime.now().isoformat()}

                    ## Data Sources
                    - **Bronze Source**: {bronze_datasource.type} - {bronze_datasource.path}
                    - **Silver Target**: {silver_datasource.type} - {silver_datasource.path}
                    """,
                key=f"section-{section_index}-transform-summary",
            )
            results.append(True)
        except Exception as e:
            print(f"Error processing section {section_index}: {e}")
            results.append(False)
            raise e

    # Store consolidated HTS to references mapping
    ref_file_path = f"{silver_datasource.path}/chapter_section_wco_notes/json/hts_to_section_notes_references.json"
    ref_json_bytes = json.dumps(hts_to_references, indent=2).encode("utf-8")
    load_data_to_storage(
        ref_json_bytes, ref_file_path, silver_datasource.type, silver_datasource.creds
    )

    return results
