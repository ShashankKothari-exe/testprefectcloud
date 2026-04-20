import json
import os
import re
from datetime import datetime
from io import BytesIO

from bs4 import BeautifulSoup, NavigableString
from prefect import task
from prefect.artifacts import create_markdown_artifact

from app.lib.notes_utils import (
    BS_PARSER,
    extract_referenced_data_from_chapter_and_section_notes,
    fetch_html,
    int_to_roman,
    parse_chapter_notes,
    read_raw_notes,
)
from app.lib.storage_utils import load_data_to_storage, read_file_content_from_storage
from app.type.data_source import DataSource


@task
def fetch_and_store_raw_chapter_notes_bronze(
    chapter_num, bronze_datasource: DataSource
):
    """Fetch chapter notes HTML from USITC API and store to bronze layer.

    Raises:
        requests.HTTPError: If the API returns a non-200 status code.
        ValueError: If the API returns an empty response.
    """
    html = fetch_html(
        f"https://hts.usitc.gov/reststop/getChapterNotes?doc={chapter_num}"
    )

    file_path = f"{bronze_datasource.path}/chapter_section_wco_notes/html/chapter_notes/chapter_{chapter_num:02d}.html"
    html_bytes = BytesIO(html.encode("utf-8"))
    load_data_to_storage(
        html_bytes, file_path, bronze_datasource.type, bronze_datasource.creds
    )

    # Create markdown artifact
    create_markdown_artifact(
        markdown=f"""
            # Chapter {chapter_num} HTML Fetch & Store

            ## Processing Details
            - **Chapter Number**: {chapter_num}
            - **File Path**: {file_path}
            - **Storage Status**: ✅ Successfully stored
            """,
        key=f"chapter-{chapter_num}-fetch-summary",
    )
    return html


@task
def transform_and_load_chapters_from_bronze_to_silver(
    bronze_datasource: DataSource, silver_datasource: DataSource
) -> list[bool]:
    """Process chapters from bronze layer to silver layer."""
    results = []
    hts_to_chapter_references = {}
    # Process regular chapters 1-97
    for chapter_num in range(1, 98):
        print(f"Processing chapter {chapter_num} of 97")
        html = read_raw_notes("chapter", chapter_num, bronze_datasource, "bronze")
        if not html:
            results.append(False)
            continue
        try:
            chapter_data = parse_chapter_notes(html)
            chapter_title = chapter_data.get("chapter_title") or ""
            soup = BeautifulSoup(html, BS_PARSER)
            for div in soup.find_all("div", class_="misc_title"):
                div.decompose()
            references = extract_referenced_data_from_chapter_and_section_notes(
                str(soup), chapter_num
            )
            _update_hts_to_chapter_references(
                hts_to_chapter_references, references, chapter_num
            )
            chapter_data.update(references)

            # Store chapter notes JSON to silver blob
            file_path = f"{silver_datasource.path}/chapter_section_wco_notes/json/chapter_notes/chapter_{chapter_num:02d}.json"
            json_bytes = json.dumps(chapter_data, indent=2).encode("utf-8")
            load_data_to_storage(
                json_bytes, file_path, silver_datasource.type, silver_datasource.creds
            )

            # store hts to chapter notes references mapping
            ref_file_path = f"{silver_datasource.path}/chapter_section_wco_notes/json/hts_to_chapter_notes_references.json"
            _store_json_to_storage_with_formatting(
                hts_to_chapter_references, ref_file_path, silver_datasource
            )

            # Create markdown artifact
            _create_chapter_transform_artifact(
                chapter_num, chapter_title, file_path, json_bytes, silver_datasource
            )
            results.append(True)
        except Exception as e:
            print(f"Error processing chapter {chapter_num}: {e}")
            results.append(False)
            raise e

    # Process subchapters for chapters 98 and 99
    for chapter_num in [98, 99]:
        print(f"Processing subchapters for chapter {chapter_num}")
        subchapter_success = transform_and_load_subchapters_from_bronze_to_silver(
            chapter_num,
            bronze_datasource,
            silver_datasource,
            hts_to_chapter_references,
        )
        results.append(subchapter_success)
    return results


def transform_and_load_subchapters_from_bronze_to_silver(
    chapter_num: int,
    bronze_datasource: DataSource,
    silver_datasource: DataSource,
    hts_to_chapter_references: dict,
) -> bool:
    """Process subchapters for chapters 98 and 99 from bronze to silver layer by parsing full HTML directly."""

    # Get the full chapter HTML
    html = read_raw_notes("chapter", chapter_num, bronze_datasource, "bronze")
    if not html:
        return False

    # Split HTML by <hr> tags using string split
    parts = html.split("<hr>")
    document_sections = [
        part.strip() for part in parts[1:]
    ]  # Skip first part, take from after first <hr>

    # Check if we have any sections after splitting
    if not document_sections:
        print(
            f"Warning: No <hr> tags found in chapter {chapter_num} HTML, skipping subchapter processing"
        )
        return False

    # Process main chapter
    main_data = _extract_and_process_98_99_main_chapter_notes(
        document_sections[0], hts_to_chapter_references, silver_datasource, chapter_num
    )

    # Process subchapters
    subchapters = _extract_and_process_98_99_subchapter_notes(
        document_sections, hts_to_chapter_references, chapter_num, main_data
    )

    # Create final data structure
    final_data = {
        "chapter_title": main_data.get("chapter_title", ""),
        "chapter_desc": main_data.get("chapter_desc", ""),
        "section_number": main_data.get("section_number", ""),
        "chapter_numbering": main_data.get("chapter_numbering", ""),
        "notes": main_data.get("notes", []),
        "additional_notes": main_data.get("additional_notes", []),
        "statistical_notes": main_data.get("statistical_notes", []),
        "subheading_notes": main_data.get("subheading_notes", []),
        "original_html": main_data.get("original_html", ""),
        "hts_references": main_data.get("hts_references", {}),
        "sub_chapters": subchapters,
    }

    # Store chapter 98 and 99 notes JSON
    file_path = f"{silver_datasource.path}/chapter_section_wco_notes/json/chapter_notes/chapter_{chapter_num:02d}.json"
    json_bytes = json.dumps(final_data, indent=2).encode("utf-8")
    load_data_to_storage(
        json_bytes, file_path, silver_datasource.type, silver_datasource.creds
    )

    # store hts to subchapter notes references mapping
    ref_file_path = f"{silver_datasource.path}/chapter_section_wco_notes/json/hts_to_chapter_notes_references.json"
    _store_json_to_storage_with_formatting(
        hts_to_chapter_references, ref_file_path, silver_datasource
    )

    # Create markdown artifact for main chapter
    create_markdown_artifact(
        markdown=f"""
            # Chapter {chapter_num} with Subchapters Bronze to Silver Transform Summary

            ## Processing Details
            - **Chapter Number**: {chapter_num}
            - **Subchapters Embedded**: {len(subchapters)}
            - **Parsing Method**: Direct HTML splitting by <hr>
            - **Target File**: {file_path}
            - **JSON Size**: {len(json_bytes)} bytes
            - **Processing Status**: ✅ Successfully stored chapter with embedded subchapters
            """,
        key=f"chapter-{chapter_num}-with-subchapters-transform-summary",
    )

    return True


def _update_hts_to_chapter_references(
    hts_to_chapter_references: dict, references: dict, chapter_num: int, idx=None
):
    subchapter_roman = []
    if idx is not None:
        subchapter_roman.append(int_to_roman(idx))

    for code in references["hts_references"]["hts_codes_references"]:
        clean_code = code.replace(".", "")
        if clean_code not in hts_to_chapter_references:
            # Initialize with first chapter reference
            hts_to_chapter_references[clean_code] = {
                "chapter_references": [
                    {"chapter": chapter_num, "subchapters": subchapter_roman.copy()}
                ]
            }
        else:
            # Look for existing chapter reference
            chapter_refs = hts_to_chapter_references[clean_code]["chapter_references"]
            existing_ref = next(
                (ref for ref in chapter_refs if ref["chapter"] == chapter_num), None
            )

            if existing_ref:
                if (
                    idx is not None
                    and int_to_roman(idx) not in existing_ref["subchapters"]
                ):
                    existing_ref["subchapters"].append(int_to_roman(idx))
            else:
                chapter_refs.append(
                    {"chapter": chapter_num, "subchapters": subchapter_roman.copy()}
                )


def _create_subchapter_artifact(chapter_num: int, idx: int, sub_data: dict):
    """Create markdown artifact for subchapter."""
    env = os.environ.get("EXECUTION_ENVIRONMENT", "unknown")
    create_markdown_artifact(
        markdown=f"""
            # Chapter {chapter_num} Subchapter {idx} Bronze to Silver Transform Summary

            ## Processing Details
            - **Chapter Number**: {chapter_num}
            - **Subchapter Index**: {idx}
            - **Chapter Title**: {sub_data.get("chapter_title", "")}
            - **Processing Status**: ✅ Successfully parsed subchapter from full HTML
            - **Environment**: {env}
            - **Timestamp**: {datetime.now().isoformat()}

            ## Data Sources
            """,
        key=f"chapter-{chapter_num}-subchapter-{idx}-transform-summary",
    )


def _store_json_to_storage_with_formatting(
    data: dict, file_path: str, datasource: DataSource
):
    """Store JSON data to storage with consistent formatting."""
    json_bytes = json.dumps(data, indent=2).encode("utf-8")
    load_data_to_storage(json_bytes, file_path, datasource.type, datasource.creds)


def _create_chapter_transform_artifact(
    chapter_num: int,
    chapter_title: str,
    file_path: str,
    json_bytes: bytes,
    datasource: DataSource,
):
    """Create markdown artifact for chapter transform processing."""
    env = os.environ.get("EXECUTION_ENVIRONMENT", "unknown")
    create_markdown_artifact(
        markdown=f"""
            # Chapter {chapter_num} Bronze to Silver Transform Summary

            ## Processing Details
            - **Chapter Number**: {chapter_num}
            - **Chapter Title**: {chapter_title}
            - **Source File**: chapter_{chapter_num:02d}.html
            - **Target File**: {file_path}
            - **JSON Size**: {len(json_bytes)} bytes
            - **Processing Status**: ✅ Successfully transformed and stored
            - **Environment**: {env}
            - **Timestamp**: {datetime.now().isoformat()}

            ## Data Sources
            - **Bronze Source**: {datasource.type} - {datasource.path}
            - **Silver Target**: {datasource.type} - {datasource.path}
            """,
        key=f"chapter-{chapter_num}-transform-summary",
    )


def _extract_and_process_98_99_main_chapter_notes(
    html: str,
    hts_to_chapter_references: dict,
    silver_datasource: DataSource,
    chapter_num: int,
) -> dict:
    """Process the main chapter section."""
    main_data = parse_chapter_notes(html)
    soup = BeautifulSoup(html, BS_PARSER)
    for div in soup.find_all("div", class_="misc_title"):
        div.decompose()
    references = extract_referenced_data_from_chapter_and_section_notes(
        str(soup), chapter_num
    )
    _update_hts_to_chapter_references(
        hts_to_chapter_references, references, chapter_num
    )
    main_data.update(references)

    # store hts to chapter notes references mapping
    ref_file_path = f"{silver_datasource.path}/chapter_section_wco_notes/json/hts_to_chapter_notes_references.json"
    _store_json_to_storage_with_formatting(
        hts_to_chapter_references, ref_file_path, silver_datasource
    )

    return main_data


def _is_valid_subchapter(doc_section_html: str, pattern: str) -> bool:
    """Check if the document section is a valid subchapter."""
    return bool(re.search(pattern, doc_section_html))


def _process_table_cell(cell) -> str:
    """Process a table cell: clean nl and u tags, extract text."""
    # Replace <nl/> tags with space
    for nl in cell.find_all("nl"):
        nl.replace_with(" ")
    # Remove <u> tags but keep text
    for u in cell.find_all("u"):
        u.unwrap()
    return cell.get_text(strip=True)


def _process_table_row(row) -> list[str] | None:
    """Process a table row: collect cell data, return if not empty."""
    cells = row.find_all("td")
    if not cells:
        return None

    row_data = [_process_table_cell(cell) for cell in cells]

    # Only return non-empty rows
    if any(cell.strip() for cell in row_data):
        return row_data
    return None


def _process_standalone_table(content, child):
    """Process standalone table elements not associated with ul/li."""
    from app.lib.notes_utils import parse_html_table_element

    table_data = parse_html_table_element(child)
    content.append({"type": "table", "value": table_data})


def _process_ul_element(content, child):
    """Process ul elements with no_liststyle class, including tables between items."""
    from app.lib.notes_utils import extract_ordered_content, parse_html_table_element

    ul_content = []

    # Process children of the ul element
    ul_children = list(child.children)

    for ul_child in ul_children:
        # Skip whitespace-only text nodes
        if not ul_child.name and not str(ul_child).strip():
            continue

        if ul_child.name == "li":
            # Process the li element
            li_content = extract_ordered_content(ul_child)
            ul_content.extend(li_content)

        elif ul_child.name == "table" and "misc_table" in ul_child.get("class", []):
            table_data = parse_html_table_element(ul_child)
            ul_content.append({"type": "table", "value": table_data})

        elif ul_child.name and ul_child.name not in ["div"]:
            # Other elements - currently not handled
            pass

    content.extend(ul_content)


def _process_text_content(content, child):
    """Process text content from elements and text nodes."""
    if isinstance(child, str):
        # Additional text nodes
        text = child.strip()
    else:
        # Text content outside ul/table
        text = child.get_text(separator=" ", strip=True)

    if text:
        content.append({"type": "text", "value": text})


def extract_ordered_content_from_section(soup):
    """
    Extract sequential content from an entire section, including tables outside lists.
    Tables within ul elements are associated with the preceding li element.
    """
    content = []

    # Remove title divs that we've already processed for the identifier
    for title_div in soup.find_all("div", class_="misc_title"):
        title_div.decompose()

    # Process all children
    children = list(soup.children)

    for child in children:
        if child.name == "table" and "misc_table" in child.get("class", []):
            # Standalone table (not associated with ul/li)
            _process_standalone_table(content, child)

        elif child.name == "ul" and "no_liststyle" in child.get("class", []):
            # Handle lists with potential tables between items
            _process_ul_element(content, child)

        elif child.name and child.name not in ["div"]:
            # Text content outside ul/table
            _process_text_content(content, child)

        elif isinstance(child, str):
            # Additional text nodes
            _process_text_content(content, child)

    return content


def _process_additional_notes_not_part_of_any_subchapter(
    doc_section_html: str, chapter_data: dict, idx: int, chapter_num: int
) -> None:
    """Process sections that are not valid subchapters as additional notes."""
    print(f"Processing additional notes section {idx} in chapter {chapter_num}.")
    special_notes = parse_chapter_notes(doc_section_html)
    soup = BeautifulSoup(doc_section_html, BS_PARSER)

    # Extract identifier from chapter title
    identifier = special_notes.get("chapter_title", "Unknown Title")
    if not identifier or identifier == "Unknown Title":
        # Fallback: try to extract from HTML
        title_div = soup.find("div", class_="misc_title")
        if title_div:
            identifier = title_div.get_text(strip=True)

    # Extract note content from the entire section
    note_content = extract_ordered_content_from_section(soup)

    special_notes_entry = {
        "identifier": identifier,
        "content": note_content,
        "sub_notes": [],
    }

    if "additional_notes" not in chapter_data:
        chapter_data["additional_notes"] = []
    chapter_data["additional_notes"].append(special_notes_entry)


def _process_valid_subchapter_section(
    doc_section_html: str,
    hts_to_chapter_references: dict,
    chapter_num: int,
    idx: int,
) -> dict:
    """Process a valid subchapter section."""
    # Fix corrupted subchapter 21 and 22 in chapter 99
    if chapter_num == 99 and (
        "SUBCHAPTER XXI" in doc_section_html or "SUBCHAPTER XXII" in doc_section_html
    ):
        doc_section_html = _fix_broken_subchapter_html(doc_section_html)

    sub_data = parse_chapter_notes(doc_section_html)
    soup = BeautifulSoup(doc_section_html, BS_PARSER)
    for div in soup.find_all("div", class_="misc_title"):
        div.decompose()
    references = extract_referenced_data_from_chapter_and_section_notes(
        str(soup), chapter_num
    )
    _update_hts_to_chapter_references(
        hts_to_chapter_references, references, chapter_num, idx
    )
    sub_data.update(references)

    _create_subchapter_artifact(chapter_num, idx, sub_data)
    return sub_data


def _extract_and_process_98_99_subchapter_notes(
    document_sections: list[str],
    hts_to_chapter_references: dict,
    chapter_num: int,
    chapter_data: dict,
) -> list[dict]:
    """Process all subchapter document sections."""
    subchapters = []
    pattern = r"class='misc_title'>\s*\[?(?:CHAPTER|SUBCHAPTER)\s+[IVXLCDM]+"
    for idx, doc_section_html in enumerate(document_sections[1:], 1):
        if not _is_valid_subchapter(doc_section_html, pattern):
            _process_additional_notes_not_part_of_any_subchapter(
                doc_section_html, chapter_data, idx, chapter_num
            )
            continue

        sub_data = _process_valid_subchapter_section(
            doc_section_html, hts_to_chapter_references, chapter_num, idx
        )
        subchapters.append(sub_data)

    return subchapters


def extract_direct_text(element):
    """Return only direct text from element, ignoring nested tags."""
    texts = []
    for child in element.children:
        if isinstance(child, NavigableString):
            text = child.strip()
            if text:
                texts.append(text)
        elif getattr(child, "name", None) in {"div", "ul"}:
            # stop before nested structures
            break
    return " ".join(texts).strip()


def build_div(div_class: str, text: str) -> str:
    """Helper to format <div> with index='null'."""
    return f"<div index='null' class='{div_class}'>{text}</div>"


def _extract_nested_titles(outer_div):
    """Extract all nested <div class='misc_title'> as proper siblings."""
    fragments = []
    nested_titles = outer_div.find_all("div", class_="misc_title", recursive=True)
    for i, nested in enumerate(nested_titles):
        text = nested.get_text(" ", strip=True)
        if not text:
            continue
        class_name = "misc_title_desc" if i == 0 else "misc_title"
        fragments.append(build_div(class_name, text))
    return fragments


def _extract_list_section(outer_div):
    """Extract <ul> lists wrapped with a 'Note' title."""
    lists = outer_div.find_all("ul", class_="no_liststyle", recursive=True)
    if not lists:
        return []
    fragments = [build_div("misc_note_title", "<u>Note</u>")]
    fragments.extend(str(ul) for ul in lists)
    return fragments


def _fix_broken_subchapter_html(corrupted_html: str) -> str:
    """
    Fixes malformed HTS HTML:
     - Separates nested <div> into misc_title & misc_title_desc
     - Preserves <ul>/<li> content
     - Wraps lists in <div class='misc_note_title'><u>Note</u></div>
    """
    soup = BeautifulSoup(corrupted_html, BS_PARSER)
    outer_div = soup.find("div", class_="misc_title")
    if not outer_div:
        return corrupted_html.strip()
    fragments = []
    title_text = extract_direct_text(outer_div)
    if title_text:
        fragments.append(build_div("misc_title", title_text))
    fragments.extend(_extract_nested_titles(outer_div))
    fragments.extend(_extract_list_section(outer_div))
    cleaned = "".join(fragments)
    return " ".join(cleaned.split())


def read_raw_notes_from_path(
    file_path: str, datasource: DataSource, folder: str = "html/chapter_notes"
) -> str | None:
    """Read raw notes from a custom path in the bronze layer."""

    full_path = f"{datasource.path}/chapter_section_wco_notes/{folder}/{file_path}"
    data = read_file_content_from_storage(datasource, full_path)
    if data:
        return data.decode("utf-8")
    return None
