import json
import re
from typing import Literal

import requests
from bs4 import BeautifulSoup

from app.lib.storage_utils import read_file_content_from_storage
from app.type.data_source import DataSource

BS_PARSER = "html.parser"


def get_notes_full_file_path_in_storage(
    data_source: DataSource,
    note_type: Literal["chapter", "section"],
    note_id: int,
    # layer: Literal["bronze", "silver"],
    file_format: Literal["html", "json"],
) -> str:
    """
    Constructs the full file path for chapter or section notes in storage.

    Args:
        data_source: The DataSource object for the storage.
        note_type: The type of note, either "chapter" or "section".
        note_id: The chapter number or section index.
        layer: The data layer, "bronze" or "silver".
        file_format: The file format, "html" or "json".

    Returns:
        The full file path as a string.
    """
    base_path = f"{data_source.path}/chapter_section_wco_notes/{file_format}"
    if note_type == "chapter":
        return f"{base_path}/chapter_notes/chapter_{note_id:02d}.{file_format}"
    elif note_type == "section":
        return f"{base_path}/section_notes/section_{note_id:02d}.{file_format}"
    else:
        raise ValueError(f"Invalid note_type: {note_type}")


def get_chapter_numbers_grouped_by_section() -> list:
    """Returns a list grouping chapter numbers by their respective HS sections.

    Returns:
        list: A list where each index represents the section number (1-21) and contains
            a list of chapter numbers belonging to that section.
    """
    return [
        list(range(1, 6)),
        list(range(6, 15)),
        list(range(15, 16)),
        list(range(16, 25)),
        list(range(25, 28)),
        list(range(28, 39)),
        list(range(39, 41)),
        list(range(41, 44)),
        list(range(44, 47)),
        list(range(47, 50)),
        list(range(50, 64)),
        list(range(64, 68)),
        list(range(68, 71)),
        list(range(71, 72)),
        list(range(72, 84)),
        list(range(84, 86)),
        list(range(86, 90)),
        list(range(90, 93)),
        list(range(93, 94)),
        list(range(94, 97)),
        list(range(97, 98)),
        list(range(98, 100)),
    ]


def get_section_number_for_chapter_number(chapter_num: int) -> int:
    """Returns the section number corresponding to a given chapter number.

    Args:
        chapter_num (int): The chapter number for which the section number is to be determined.

    Returns:
        int: The section number corresponding to the given chapter number.
    """
    section_groups = get_chapter_numbers_grouped_by_section()
    for section_number, chapters in enumerate(section_groups, start=1):
        if chapter_num in chapters:
            return section_number
    raise ValueError(f"Invalid chapter number: {chapter_num}")


def fetch_html(url) -> str:
    """Fetches HTML content from a given URL.

    Args:
        url (str): The URL to fetch HTML content from.

    Returns:
        str: The HTML content as a string if successful.

    Raises:
        requests.HTTPError: If the response status code is not 200.
        ValueError: If the response body is empty.
    """
    r = requests.get(url, timeout=30)
    r.raise_for_status()  # Raises HTTPError for 4xx/5xx responses

    if not r.text:
        raise ValueError(f"Empty response body from URL: {url}")

    return r.text


def _is_title_stop_condition(current_elem) -> bool:
    """Check if current element should stop notes collection."""
    if current_elem.name != "div":
        return False

    classes = current_elem.get("class", [])
    if classes not in [["misc_note_title"], ["misc_paragraph"]]:
        return False

    title_text = current_elem.get_text(strip=True)
    title_keywords = ["Notes", "Additional", "Statistical", "Subheading", "U.S."]
    return any(
        keyword.lower() in title_text.lower() for keyword in title_keywords
    ) and not title_text.startswith("[")


def _process_list_element(current_elem, notes):
    """Process ul element and extract note items."""
    if "no_liststyle" not in current_elem.get("class", []):
        return

    for li in current_elem.find_all("li", recursive=False):
        note_item = process_note_html_list_item(li)
        notes.append(note_item)


def _process_paragraph_element(current_elem, notes):
    """Process paragraph div element and extract content."""
    if current_elem.get("class") != ["misc_paragraph"]:
        return

    text = current_elem.get_text(separator=" ", strip=True)
    if text:  # Only add non-empty paragraphs
        content = [{"type": "text", "value": text}]
        notes.append({"identifier": "", "content": content, "sub_notes": []})


def extract_notes_by_title(soup, title) -> list:
    """Extracts notes from HTML soup based on section title.

    Args:
        soup: BeautifulSoup object containing the parsed HTML.
        title (str): The title of the notes section to extract.

    Returns:
        list: List of processed note items, each as a dictionary.

    Notes:
        Finds the title element, locates all notes lists and paragraphs that follow
        until the next title element is encountered, and processes each list item
        and paragraph into structured note data.
    """
    title_elem = find_title_element(soup, title)
    if not title_elem:
        return []

    notes = []
    current_elem = title_elem

    while current_elem:
        current_elem = current_elem.find_next_sibling()
        if not current_elem:
            break

        if _is_title_stop_condition(current_elem):
            break

        if current_elem.name == "ul":
            _process_list_element(current_elem, notes)
        elif current_elem.name == "div":
            _process_paragraph_element(current_elem, notes)

    return notes


def find_title_element(soup, target_title) -> str | None:
    """Finds HTML element containing target title.

    Args:
        soup: BeautifulSoup object containing the parsed HTML.
        target_title (str): The title text to search for.

    Returns:
        BeautifulSoup element or None: The div element with class "misc_note_title" or "misc_paragraph"
        that contains the target title text, or None if not found.

    Notes:
        Performs case-insensitive matching by normalizing both the target title
        and div text (lowercase, strip whitespace, remove trailing colons).
    """

    def normalize(text):
        return text.lower().strip().rstrip(":")

    def has_title_class(class_attr):
        if isinstance(class_attr, list):
            return any(c in class_attr for c in ["misc_note_title", "misc_paragraph"])
        return class_attr in ["misc_note_title", "misc_paragraph"]

    for div in soup.find_all("div"):
        if has_title_class(div.get("class")):
            div_text = div.get_text(strip=True)
            if normalize(div_text) == normalize(target_title):
                return div
    return None


def extract_paragraph_notes(title_elem) -> list:
    """Extracts notes from paragraph elements following a title element.

    Args:
        title_elem: BeautifulSoup element representing the title.

    Returns:
        list: List of note dictionaries with identifier, text, and sub_notes.

    Notes:
        Finds all misc_paragraph divs following the title element until
        encountering another title element or specific stop criteria.
    """
    notes = []
    current_elem = title_elem

    while current_elem:
        current_elem = current_elem.find_next_sibling()
        if not current_elem:
            break

        # Stop if we encounter another title element (stop collecting notes)
        if current_elem.name == "div" and current_elem.get("class") == [
            "misc_note_title"
        ]:
            break

        # If it's a paragraph, extract its content as a note
        if current_elem.name == "div" and current_elem.get("class") == [
            "misc_paragraph"
        ]:
            text = current_elem.get_text(separator=" ", strip=True)
            if text:  # Only add non-empty paragraphs
                content = [{"type": "text", "value": text}]
                notes.append(
                    {
                        "identifier": "",
                        "content": content,
                        "sub_notes": [],
                    }
                )

        # Stop at other stopping criteria if needed (e.g., another section start)
        if current_elem.name == "div" and "misc_title" in current_elem.get("class", []):
            break

    return notes


def extract_numbering_key(text: str) -> tuple[str, str]:
    """
    Extract numbering key from text using regex patterns.
    Returns tuple of (key, cleaned_text).
    If no numbering found, returns ("", original_text).
    """
    # Pattern 1: Hindu numerals with period (1., 2., 3., etc.) or multi-level (1.1., 1.2., etc.)
    hindu_pattern = r"^(\(?\d+(?:\.\d+)*\.?\)?)\s*"
    match = re.match(hindu_pattern, text)
    if match:
        key = match.group(1)
        norm_key = key
        # Add period if missing and not in parentheses
        if not (
            norm_key.startswith("(") and norm_key.endswith(")")
        ) and not norm_key.endswith("."):
            norm_key = norm_key + "."
        return norm_key, text[match.end() :].strip()

    # Pattern 2: Roman numerals in parentheses ((i), (ii), (iii), etc.)
    roman_pattern = r"^\(([ivxlcm]+)\)\s*"
    match = re.match(roman_pattern, text, re.IGNORECASE)
    if match:
        key = f"({match.group(1).lower()})"
        return key, text[match.end() :].strip()

    # Pattern 3: Lettered in parentheses ((a), (b), (c), etc.)
    letter_pattern = r"^\(([a-z])\)\s*"
    match = re.match(letter_pattern, text, re.IGNORECASE)
    if match:
        key = f"({match.group(1).lower()})"
        return key, text[match.end() :].strip()

    # Pattern 5: Roman numerals without parentheses (i , ii , etc.)
    roman_no_paren_pattern = (
        r"^(i{1,3}|iv|vi{0,3}|ix|x{0,3}|l{0,3}|c{0,3}|d{0,3}|m{0,3})(\s+)"
    )
    match = re.match(roman_no_paren_pattern, text, re.IGNORECASE)
    if match:
        key = f"({match.group(1).lower()})"
        return key, text[match.end() :].strip()

    # Pattern 6: Lettered without parentheses (a , b , etc.)
    letter_no_paren_pattern = r"^([a-z])(\s+)"
    match = re.match(letter_no_paren_pattern, text, re.IGNORECASE)
    if match:
        key = f"({match.group(1).lower()})"
        return key, text[match.end() :].strip()

    # No numbering found
    return "", text


def process_note_html_list_item(li):
    """
    Processes individual note list item, extracting numbering, sequential content (text/tables), and nested notes.
    Also checks for tables that appear immediately after this li element (between li elements).
    """
    value = li.get("value", "").strip()
    text = clean_note_text(li)

    key, cleaned_text = extract_numbering_key(text)

    if not cleaned_text.strip():
        # If only numbering present, preserve it in content and clear identifier
        key = ""

    if not key:
        key, _ = fallback_key_and_text(value, text)

    # Extract ordered content (text and tables in sequence) - this handles content within the li
    note_content = extract_ordered_content(li, key)

    # Also check for tables that appear immediately after this li element
    # Only attach the table if it is the next non-whitespace sibling (i.e., not another li).
    next_table = None
    sibling = li.next_sibling
    while sibling and isinstance(sibling, str) and not sibling.strip():
        sibling = sibling.next_sibling
    if sibling and sibling.name == "table" and "misc_table" in sibling.get("class", []):
        next_table = sibling

    if next_table:
        from app.lib.notes_utils import parse_html_table_element

        table_data = parse_html_table_element(next_table)
        note_content.append({"type": "table", "value": table_data})

    note_entry = {
        "identifier": key or "",
        "content": note_content,
        "sub_notes": extract_nested_notes_html_list(li),
    }

    return note_entry


def fallback_key_and_text(value, text):
    """Handles key/text extraction when no key is found initially."""
    if not value:
        return "", text

    def strip_prefix(s, prefix):
        return s[len(prefix) :].strip()

    if value.endswith("."):
        if text.startswith(value + " "):
            return value, strip_prefix(text, value + " ")
        return value, text

    dot_value = value + "."
    if text.startswith(dot_value + " "):
        return dot_value, strip_prefix(text, dot_value + " ")
    if text.startswith(value + " "):
        return dot_value, strip_prefix(text, value + " ")
    return dot_value, text


def clean_note_text(li) -> str:
    """Cleans note text by extracting from list item and removing nested content.

    Args:
        li: BeautifulSoup list item element containing the note text.

    Returns:
        str: Cleaned text string with nested list and table content removed.

    Notes:
        Extracts all text from the list item, then removes text from any nested
        unordered lists and tables to avoid duplication in the main note text.
    """
    text = li.get_text(separator=" ", strip=True)
    for nested_ul in li.find_all("ul", recursive=False):
        nested_text = nested_ul.get_text(separator=" ", strip=True)
        text = text.replace(nested_text, "").strip()
    for table in li.find_all("table", class_="misc_table", recursive=False):
        table_text = table.get_text(separator=" ", strip=True)
        text = text.replace(table_text, "").strip()
    return text


def extract_nested_notes_html_list(li) -> list:
    """Extracts nested notes from list item with numbering extraction.

    Args:
        li: BeautifulSoup parent list item element that may contain nested notes.

    Returns:
        list: List of nested note dictionaries with identifier, text, and sub_notes.

    Notes:
        Finds nested unordered list within the parent list item, processes each
        nested list item with full recursive processing, and returns structured data.
        This enables multiple levels of nesting like a tree structure.
    """
    nested_ul = li.find("ul", class_="no_liststyle")
    if not nested_ul:
        return []

    nested_notes = []
    for nested_li in nested_ul.find_all("li", recursive=False):
        # Use the same processing function for full recursion
        nested_note = process_note_html_list_item(nested_li)
        nested_notes.append(nested_note)

    return nested_notes


def _is_nested_ul(child):
    """Checks if child is a nested ul that should be skipped (handled separately for subnotes)."""
    return child.name == "ul" and "no_liststyle" in child.get("class", [])


def _is_misc_table(child):
    """Checks if child is a misc_table element."""
    return child.name == "table" and "misc_table" in child.get("class", [])


def _flush_accumulated_text(content, current_text):
    """Flushes accumulated text content to the main content list."""
    if current_text:
        text_value = "".join(current_text).strip()
        if text_value:
            content.append({"type": "text", "value": text_value})
        current_text.clear()


def _handle_table_child(child, content, current_text):
    """Handles table child by flushing text first, then adding the parsed table."""
    _flush_accumulated_text(content, current_text)
    table_data = parse_html_table_element(child)
    content.append({"type": "table", "value": table_data})


def _extract_element_text(child, current_text, key, is_first_text=None):
    """Extracts text from named elements and appends to current text accumulation."""
    text = child.get_text(separator=" ", strip=True)
    if text:
        if is_first_text and key:
            # More robust stripping: use regex to strip the numbering at the start
            # This handles cases where numbering was normalized or mismatched.
            text = re.sub(
                r"^\s*(\(?\d+[\d.]*\)?|\([a-z]+\)|[a-z]+)\.?\s*",
                "",
                text,
                count=1,
                flags=re.IGNORECASE,
            )
        current_text.append(text + " ")


def _append_text_node(child, current_text, key, is_first_text=None):
    """Appends text node content to current text accumulation."""
    if is_first_text and key:
        child = re.sub(
            r"^\s*(\(?\d+[\d.]*\)?|\([a-z]+\)|[a-z]+)\.?\s*",
            "",
            child,
            count=1,
            flags=re.IGNORECASE,
        )
    current_text.append(child)


def extract_ordered_content(li, key=""):
    """
    Extracts sequential content from a list item, preserving order of text and tables.
    Strips nested ul content to avoid duplication.

    Args:
        li: BeautifulSoup list item element.
        key: The extracted identifier key, used to strip numbering from first text node.

    Returns:
        list: List of content items, each a dict with "type" ("text" or "table") and "value".
    """
    content = []
    current_text = []

    is_first_text = True
    for child in li.children:
        if _is_nested_ul(child):
            # Skip nested ul - subnotes are handled separately
            pass
        elif _is_misc_table(child):
            _handle_table_child(child, content, current_text)
        elif child.name:
            # For other elements, extract text recursively
            _extract_element_text(child, current_text, key, is_first_text)
            is_first_text = False
        else:
            # Text node
            _append_text_node(child, current_text, key, is_first_text)
            is_first_text = False

    # Flush any remaining text
    _flush_accumulated_text(content, current_text)

    return content


def extract_table_data_from_html_list(li):
    """Extracts table data from note list item if present using advanced parser.

    Args:
        li: BeautifulSoup list item element that may contain a table.

    Returns:
        dict | None: Parsed table data using advanced format, or None if no table found.

    Notes:
        Looks for table with class "misc_table" within the list item and parses it
        using the advanced parser function if found. Also checks for tables immediately
        following the li element (between li elements) due to HTML structure issues.
        Returns the full advanced table structure without conversion.
    """
    # First, try the normal approach - table inside li
    table = li.find("table", class_="misc_table")
    if table:
        return parse_html_table_element(table)

    # Second, check for table immediately following this li (HTML structure issue)
    next_table = li.find_next_sibling("table", class_="misc_table")
    if next_table:
        # Assume any immediately following table belongs to this li
        # This handles malformed HTML where tables are placed between li elements
        return parse_html_table_element(next_table)

    return None


def parse_misc_table_in_html_list(table) -> list:
    """Parses HTML table with miscellaneous data and returns structured rows.

    Args:
        table: BeautifulSoup table element to parse.

    Returns:
        list: List of dictionaries representing table rows, with headers as keys.

    Notes:
        Handles various table structures by detecting headers and data rows.
        Skips empty rows and handles tables with different column structures.
    """
    rows = []
    trs = table.find_all("tr")
    headers = []

    for tr in trs:
        cells = tr.find_all(["th", "td"])
        row = [cell.get_text(strip=True) for cell in cells]
        if not any(row):
            continue
        # Set headers from this row if it has th elements or if no th and multiple rows
        if not headers and (
            any(cell.name == "th" for cell in cells)
            or (
                len(table.find_all("tr")) > 1
                and not any(cell.name == "th" for cell in cells)
            )
        ):
            headers = list(row)
            continue
        # Process data rows
        if headers and len(row) == len(headers):
            rows.append(dict(zip(headers, row, strict=True)))
        elif len(row) == 2:
            rows.append({"Col1": row[0], "Col2": row[1]})
    return rows


def _extract_content_after_chapter_title(soup: BeautifulSoup) -> list:
    """Extract note content that follows the first misc_title when there are no
    standard section headers (Notes, Additional U.S. Notes, etc.).

    Collects siblings after the first div.misc_title that are div.misc_note_title,
    div.misc_paragraph, or table.misc_table, and returns them as note entries.
    Stops at the next misc_title. Used for blocks like "[SUBCHAPTER XIV deleted]"
    that only have a compiler's note and/or table.
    """
    title_elem = soup.find("div", class_="misc_title")
    if not title_elem:
        return []

    notes = []
    current_elem = title_elem

    while current_elem:
        current_elem = current_elem.find_next_sibling()
        if not current_elem:
            break

        if current_elem.name == "div" and "misc_title" in current_elem.get("class", []):
            break

        if current_elem.name == "div":
            classes = current_elem.get("class", [])
            if "misc_note_title" in classes or "misc_paragraph" in classes:
                text = current_elem.get_text(separator=" ", strip=True)
                if text:
                    content = [{"type": "text", "value": text}]
                    notes.append(
                        {"identifier": "", "content": content, "sub_notes": []}
                    )
        elif current_elem.name == "table" and "misc_table" in current_elem.get(
            "class", []
        ):
            table_data = parse_html_table_element(current_elem)
            content = [{"type": "table", "value": table_data}]
            notes.append({"identifier": "", "content": content, "sub_notes": []})

    return notes


def parse_chapter_notes(html):
    """Parses HTML content of chapter notes into structured dictionary.

    Args:
        html (str): Raw HTML string containing chapter notes content.

    Returns:
        dict: Dictionary containing parsed chapter data with keys:
            - chapter_title: Title of the chapter
            - chapter_desc: Description text
            - chapter_numbering: List of numbering elements
            - notes: Main chapter notes
            - additional_notes: Additional U.S. notes
            - statistical_notes: Statistical notes
            - subheading Notes: Subheading notes
            - original_html: Original HTML content for reference

    Notes:
        Uses BeautifulSoup to parse HTML and extract various note types
        by searching for specific title elements and their associated content.
        Preserves the original HTML content alongside parsed data.
    """
    soup = BeautifulSoup(html, BS_PARSER)

    title = soup.find("div", class_="misc_title")
    desc = soup.find_all("div", class_="misc_title_desc")

    section_number, chapter_numbering, chapter_number, code = (
        _extract_chapter_notes_metadata(soup, title)
    )

    notes = extract_notes_by_title(soup, "Notes") or extract_notes_by_title(
        soup, "Note"
    )
    additional_notes = (
        extract_notes_by_title(soup, "Additional U.S. Notes")
        or extract_notes_by_title(soup, "Additional U.S. Note")
        or extract_notes_by_title(soup, "U.S. Note")
        or extract_notes_by_title(soup, "U.S. Notes")
    )
    statistical_notes = extract_notes_by_title(
        soup, "Statistical Notes"
    ) or extract_notes_by_title(soup, "Statistical Note")
    subheading_notes = extract_notes_by_title(
        soup, "Subheading Notes"
    ) or extract_notes_by_title(soup, "Subheading Note")
    # Fallback: when there are no standard section headers (e.g. "[SUBCHAPTER XIV deleted]"
    # with only misc_note_title + table), collect content following the chapter title
    if (
        not notes
        and not additional_notes
        and not statistical_notes
        and not subheading_notes
    ):
        notes = _extract_content_after_chapter_title(soup)

    return {
        "chapter_title": title.text.strip() if title else None,
        "chapter_desc": " ".join(d.text.strip() for d in desc),
        "section_number": section_number,
        "chapter_numbering": chapter_numbering,
        "chapter_number": chapter_number,
        "code": code,
        "notes": notes,
        "additional_notes": additional_notes,
        "statistical_notes": statistical_notes,
        "subheading_notes": subheading_notes,
        "original_html": html,  # Preserve original HTML content
    }


def parse_section_notes(html) -> dict:
    """Parse section notes.
    Args:
        html (str): HTML of section notes

    Returns:
        dict: Parsed section notes with original HTML preserved
    """
    soup = BeautifulSoup(html, BS_PARSER)

    # Find section title, skipping note-related titles
    title = None
    for div in soup.find_all("div", class_="misc_title"):
        title_text = div.get_text(strip=True).upper()
        # Skip note-related titles
        if not any(
            keyword in title_text
            for keyword in ["NOTES", "ADDITIONAL", "STATISTICAL", "SUBHEADING"]
        ):
            title = div
            break

    return {
        "section_title": title.text.strip() if title else None,
        "section_desc": " ".join(
            desc.text.strip() for desc in soup.find_all("div", class_="misc_title_desc")
        ),
        "section_numbering": soup.find("div", class_="misc_numbering").text.strip()
        if soup.find("div", class_="misc_numbering")
        else "",
        "notes": extract_notes_by_title(soup, "Notes")
        or extract_notes_by_title(soup, "Note"),
        "additional_notes": extract_notes_by_title(soup, "Additional U.S. Notes")
        or extract_notes_by_title(soup, "Additional U.S. Note")
        or extract_notes_by_title(soup, "U.S. Note")
        or extract_notes_by_title(soup, "U.S. Notes"),
        "statistical_notes": extract_notes_by_title(soup, "Statistical Notes")
        or extract_notes_by_title(soup, "Statistical Note"),
        "subheading_notes": extract_notes_by_title(soup, "Subheading Notes")
        or extract_notes_by_title(soup, "Subheading Note"),
        "original_html": html,  # Preserve original HTML content
    }


def read_raw_notes(
    note_type: Literal["chapter", "section"],
    note_id: int,
    datasource: DataSource,
    layer: Literal["bronze", "silver"],
) -> str | None:
    """Read raw notes from storage."""
    file_format = "html" if layer == "bronze" else "json"
    file_path = get_notes_full_file_path_in_storage(
        datasource, note_type, note_id, file_format
    )
    content = read_file_content_from_storage(datasource, file_path)
    return content.decode("utf-8")


def read_notes_json(
    note_type: Literal["chapter", "section"],
    note_id: int,
    silver_datasource: DataSource,
) -> dict | None:
    """Read notes JSON from silver layer."""
    file_path = get_notes_full_file_path_in_storage(
        silver_datasource, note_type, note_id, "json"
    )
    content = read_file_content_from_storage(silver_datasource, file_path)
    return json.loads(content)


def int_to_roman(num: int) -> str:
    """
    Converts an integer to Roman numeral string (1-99 range for HTS use).
    This supports the necessary range for HTS Chapters (1-99) and Sections (1-22).
    """
    if not (1 <= num <= 99):
        return ""

    roman_numerals = [
        ("XC", 90),
        ("LXXX", 80),
        ("LXX", 70),
        ("LX", 60),
        ("L", 50),
        ("XL", 40),
        ("XXX", 30),
        ("XX", 20),
        ("X", 10),
        ("IX", 9),
        ("VIII", 8),
        ("VII", 7),
        ("VI", 6),
        ("V", 5),
        ("IV", 4),
        ("III", 3),
        ("II", 2),
        ("I", 1),
    ]

    result = ""
    for roman, value in roman_numerals:
        while num >= value:
            result += roman
            num -= value
    return result


def roman_to_int(roman_numeral: str) -> int | None:
    """
    Converts a Roman numeral string (up to C, as in 100) to an integer.
    This supports the necessary range for HTS Chapters (1-99) and Sections (1-22).
    """
    if not roman_numeral:
        return None

    # Map for conversion up to 100 (C)
    roman_map = {"I": 1, "V": 5, "X": 10, "L": 50, "C": 100}
    integer_value = 0
    prev_value = 0

    # Convert to uppercase for robustness
    roman_numeral = roman_numeral.upper().strip()

    for char in reversed(roman_numeral):
        value = roman_map.get(char)
        if value is None:
            # Character is not a valid Roman numeral for this range
            return None
        # Subtractive principle: if current value is less than the previous, subtract.
        if value < prev_value:
            integer_value -= value
        else:
            integer_value += value
        prev_value = value
    return integer_value


def _extract_roman_numerals(string: str) -> str:
    """Extracts Roman numerals from a string using regex.

    Args:
        string: The input string to search for Roman numerals.

    Returns:
        The first Roman numeral found, or empty string if none found.
    """
    # Regular expression pattern for Roman numerals
    roman_pattern = r"\b[IVXLCDM]+\b"
    found_numerals = re.findall(roman_pattern, string)
    return found_numerals[0] if found_numerals else ""


def _extract_numbering_fields(soup: BeautifulSoup) -> tuple[str, str]:
    """Extracts section number and chapter numbering from soup.

    Args:
        soup: BeautifulSoup object containing the parsed HTML.

    Returns:
        A tuple containing (section_number, chapter_numbering).
    """
    numbering = soup.find_all("div", class_="misc_numbering")
    section_number = numbering[0].text.strip() if len(numbering) > 1 else ""
    chapter_numbering = (
        numbering[len(numbering) - 1].text.strip() if len(numbering) > 0 else ""
    )
    return section_number, chapter_numbering


def _parse_alphanumeric_numbering(
    chapter_numbering: str,
) -> tuple[int | None, str | None]:
    """Parses chapter numbering that contains alphabetic characters.

    Args:
        chapter_numbering: The chapter numbering string (e.g., "1-X-A").

    Returns:
        A tuple containing (chapter_number, code).
    """
    code = chapter_numbering.split("-")[1]
    parts = chapter_numbering.split("-")
    chapter_number = roman_to_int(code) if len(parts) == 3 else None
    return chapter_number, code


def _parse_numeric_numbering(chapter_numbering: str) -> tuple[int | None, str | None]:
    """Parses chapter numbering that is numeric.

    Args:
        chapter_numbering: The chapter numbering string (e.g., "01").

    Returns:
        A tuple containing (chapter_number, code).
    """
    try:
        chapter_number = int(chapter_numbering.split("-")[0])
        return chapter_number, None
    except (ValueError, IndexError):
        return None, None


def _parse_fallback_from_title(title_elem) -> tuple[int | None, str | None]:
    """Parses chapter details from title element as fallback.

    Args:
        title_elem: The title element for error logging and parsing.

    Returns:
        A tuple containing (chapter_number, code).
    """
    if not title_elem:
        return None, None

    code = _extract_roman_numerals(title_elem.text.strip())
    chapter_number = roman_to_int(code) if code else None
    return chapter_number, code


def _extract_chapter_notes_metadata(
    soup: BeautifulSoup, title_elem
) -> tuple[str, str, int | None, str | None]:
    """Extracts chapter numbering details from BeautifulSoup object.

    Args:
        soup: BeautifulSoup object containing the parsed HTML.
        title_elem: The title element for error logging if needed.

    Returns:
        A tuple containing (section_number, chapter_numbering, chapter_number, code).
    """
    section_number, chapter_numbering = _extract_numbering_fields(soup)

    if re.search(r"[A-Za-z]", chapter_numbering.strip()):
        chapter_number, code = _parse_alphanumeric_numbering(chapter_numbering.strip())
    else:
        chapter_number, code = _parse_numeric_numbering(chapter_numbering.strip())
        if chapter_number is None:
            chapter_number, code = _parse_fallback_from_title(title_elem)

    return section_number, chapter_numbering, chapter_number, code


def extract_hts_codes(row_text, hts_code_pattern):
    """Extracts unique HTS codes from a row's text."""
    return set(hts_code_pattern.findall(row_text))


def extract_sections(row_text, section_explicit_pattern, section_implied_pattern):
    """Extracts unique section numbers (as integers) from a row's text."""
    section_references = section_explicit_pattern.findall(row_text)
    section_references.extend(section_implied_pattern.findall(row_text))
    unique_section_refs = set(section_references)
    extracted_sections = set()
    for section_num_str in unique_section_refs:
        section_int = None
        try:
            section_int = int(section_num_str)
        except ValueError:
            section_int = roman_to_int(section_num_str)
        if section_int is not None and 1 <= section_int <= 22:
            extracted_sections.add(section_int)
    return extracted_sections


def _parse_chapter_tokens_from_row(row_text):
    """Helper to extract all chapter tokens from a row."""
    pattern = re.compile(r"\bchapters?\s+([^.]*(?:\.\s)?)", re.IGNORECASE)
    captures = pattern.findall(row_text)
    all_tokens = []
    for token in captures:
        # Extract all digits from the captured token
        numbers = re.findall(r"\d+", token)
        all_tokens.extend(numbers)
    return all_tokens


def _parse_subchapter_tokens_from_row(row_text):
    """Helper to extract all subchapter tokens from a row."""
    subchapter_list_continuation_pattern = re.compile(
        r"\bsubchapter?\s((?:[IVXLCDM]+(?:,\s*| or | and )?)+)\b", re.IGNORECASE
    )
    return subchapter_list_continuation_pattern.findall(row_text)


def extract_chapters(row_text):
    """Extracts unique chapter numbers (as integers) from a row's text."""
    all_chapter_tokens_in_row = _parse_chapter_tokens_from_row(row_text)
    extracted_chapters = set()
    for chapter_num_str in all_chapter_tokens_in_row:
        chapter_int = None
        try:
            chapter_int = int(chapter_num_str)
        except ValueError:
            chapter_int = roman_to_int(chapter_num_str)
        if chapter_int is not None and 1 <= chapter_int <= 99:
            extracted_chapters.add(chapter_int)
    return extracted_chapters


def _extract_subchapters_referenced_in_chapter_98_99(row_text):
    """Extracts unique subchapter numbers as Roman numerals from a row's text."""
    all_subchapter_tokens_in_row = _parse_subchapter_tokens_from_row(row_text)
    extracted_subchapters = set()
    for chapter_num_str in all_subchapter_tokens_in_row:
        # Split by common separators and extract romans
        parts = re.split(r"\s*(?:\s*,\s*| and | or )\s*", chapter_num_str)
        for part in parts:
            roman = part.strip().upper()
            if roman and all(c in "IVXLCDM" for c in roman):
                extracted_subchapters.add(roman)
    return extracted_subchapters


def _parse_current_subchapter(soup: BeautifulSoup) -> int | None:
    """Extracts the current subchapter number from the page title."""
    title_div = soup.find("div", class_="misc_title")
    if not title_div:
        return None

    title_text = title_div.get_text(strip=True).upper()
    if not title_text.startswith("SUBCHAPTER "):
        return None

    subchapter_part = title_text[len("SUBCHAPTER ") :].strip()
    try:
        return int(subchapter_part)
    except ValueError:
        return roman_to_int(subchapter_part)


def _create_extraction_patterns() -> tuple:
    """Creates and returns all regex patterns used for extraction."""
    hts_code_pattern = re.compile(r"\b\d{4}(?:\.\d{2,4}){1,3}\b")
    hts_heading_pattern = re.compile(
        r"\b\w*headings?\s((?:\d+(?:\.\d+)*(?:,\s*| or | and )?)+)",
        re.IGNORECASE,
    )
    section_explicit_pattern = re.compile(r"section\s+(\d+|[IVXLCDM]+)", re.IGNORECASE)
    section_implied_pattern = re.compile(
        r"section\s+(?:\d+|[IVXLCDM]+)(?:\s+and\s+|,\s+)(\d+|[IVXLCDM]+)",
        re.IGNORECASE,
    )
    enumerator_pattern = re.compile(r"^\s*\((\d+|[ivxlcdm]+)\)\s*", re.IGNORECASE)

    return (
        hts_code_pattern,
        hts_heading_pattern,
        section_explicit_pattern,
        section_implied_pattern,
        enumerator_pattern,
    )


def _process_table_rows(
    soup: BeautifulSoup,
    hts_code_pattern,
    hts_heading_pattern,
    section_explicit_pattern,
    section_implied_pattern,
    enumerator_pattern,
) -> tuple[set, set, set, set]:
    """Processes all table rows and extracts references."""
    extracted_hts_codes = set()
    extracted_chapters = set()
    extracted_subchapters = set()
    extracted_sections = set()

    note_rows = soup.find_all("tr")
    for row in note_rows:
        row_text_raw = row.get_text(strip=True)
        row_text = enumerator_pattern.sub("", row_text_raw, count=1).strip()

        hts_codes = extract_hts_codes(row_text, hts_code_pattern)
        hts_headings = extract_hts_codes(row_text, hts_heading_pattern)
        hts_codes.update(hts_headings)

        for hts_code in hts_codes:
            extracted_hts_codes.update(
                part.strip().replace(".", "")
                for part in re.split(r"\s*(?:,|or|and)\s*", hts_code)
                if part.strip()
            )

        sections = extract_sections(
            row_text, section_explicit_pattern, section_implied_pattern
        )
        extracted_sections.update(sections)

        chapters = extract_chapters(row_text)
        extracted_chapters.update(chapters)

        subchapters = _extract_subchapters_referenced_in_chapter_98_99(row_text)
        extracted_subchapters.update(subchapters)

    return (
        extracted_hts_codes,
        extracted_chapters,
        extracted_subchapters,
        extracted_sections,
    )


def _apply_reference_exclusions(
    extracted_refs: dict, chapter_num: int, current_subchapter: int | None
) -> dict:
    """Applies exclusions for current section and subchapter references."""
    extracted_refs = {
        key: value.copy()
        if isinstance(value, set)
        else value.copy()
        if isinstance(value, dict)
        else value
        for key, value in extracted_refs.items()
    }
    extracted_refs["sections"].discard(
        get_section_number_for_chapter_number(chapter_num)
    )

    if current_subchapter is not None:
        extracted_refs["subchapters"].discard(current_subchapter)

    return extracted_refs


def _build_chapters_map(
    extracted_chapters: set, chapter_num: int, extracted_subchapters: set
) -> list:
    """Builds the chapters reference map with subchapters."""
    extracted_chapters_list = list(extracted_chapters)

    if chapter_num not in extracted_chapters_list:
        extracted_chapters_list.append(chapter_num)

    extracted_chapters_map = []
    for chapter in extracted_chapters_list:
        if chapter == chapter_num:
            if extracted_subchapters:
                extracted_chapters_map.append(
                    {
                        "chapter": chapter,
                        "subchapters": list(extracted_subchapters),
                    }
                )
        else:
            extracted_chapters_map.append({"chapter": chapter, "subchapters": []})

    return extracted_chapters_map


def extract_referenced_data_from_chapter_and_section_notes(
    html_string: str, chapter_num: int
) -> dict:
    """
    Parses a raw HTML string of a special classification subchapter note (Chapter 98/99)
    to extract unique HTS codes, associated chapters, and explicitly referenced
    sections within the note text, returning the results as sets.
    """
    soup = BeautifulSoup(html_string, BS_PARSER)
    current_subchapter = _parse_current_subchapter(soup)

    # Get all extraction patterns
    patterns = _create_extraction_patterns()
    (
        hts_code_pattern,
        hts_heading_pattern,
        section_explicit_pattern,
        section_implied_pattern,
        enumerator_pattern,
    ) = patterns

    # Process table rows
    table_refs = _process_table_rows(
        soup,
        hts_code_pattern,
        hts_heading_pattern,
        section_explicit_pattern,
        section_implied_pattern,
        enumerator_pattern,
    )
    (
        extracted_hts_codes,
        extracted_chapters,
        extracted_subchapters,
        extracted_sections,
    ) = table_refs

    # Extract from full text
    full_text = soup.get_text(separator=" ", strip=True)
    text_refs = _extract_all_references_from_text(
        full_text,
        hts_code_pattern,
        hts_heading_pattern,
        section_explicit_pattern,
        section_implied_pattern,
    )

    # Combine references
    extracted_refs = {
        "hts_codes": extracted_hts_codes,
        "chapters": extracted_chapters,
        "subchapters": extracted_subchapters,
        "sections": extracted_sections,
    }

    # Update with text references
    for key in extracted_refs:
        extracted_refs[key].update(text_refs[key])

    # Apply exclusions
    extracted_refs = _apply_reference_exclusions(
        extracted_refs, chapter_num, current_subchapter
    )

    # Build chapters map
    extracted_chapters_map = _build_chapters_map(
        extracted_refs["chapters"], chapter_num, extracted_refs["subchapters"]
    )

    return {
        "hts_references": {
            "hts_codes_references": list(extracted_refs["hts_codes"]),
            "chapters_references": extracted_chapters_map,
            "sections_references": list(extracted_refs["sections"]),
        }
    }


def _extract_all_references_from_text(
    text: str,
    hts_code_pattern,
    hts_heading_pattern,
    section_explicit_pattern,
    section_implied_pattern,
) -> dict:
    """Extract all references from plain text."""
    hts_codes = extract_hts_codes(text, hts_code_pattern)
    hts_headings = extract_hts_codes(text, hts_heading_pattern)
    hts_codes.update(hts_headings)
    flattened_hts_codes = set()
    for hts_code in hts_codes:
        flattened_hts_codes.update(
            part.strip().replace(".", "")
            for part in re.split(r"\s*(?:,|or|and)\s*", hts_code)
            if part.strip()
        )
    dirty_codes = list(flattened_hts_codes)
    for code in dirty_codes:
        if code == "":
            flattened_hts_codes.remove(code)
    sections = extract_sections(text, section_explicit_pattern, section_implied_pattern)
    chapters = extract_chapters(text)
    subchapters = _extract_subchapters_referenced_in_chapter_98_99(text)
    return {
        "hts_codes": flattened_hts_codes,
        "sections": sections,
        "chapters": chapters,
        "subchapters": subchapters,
    }


def get_cell_text(cell):
    # Preserve spaces inside the cell text
    return "".join(cell.strings)


def get_attributes(tag, attrs_to_keep=None):
    # Collect all attributes (or selected ones)
    if attrs_to_keep is None:
        return dict(tag.attrs)
    return {attr: tag.get(attr) for attr in attrs_to_keep if tag.get(attr) is not None}


def parse_html_table_element(table):
    """
    Parses a single BeautifulSoup table element into structured format.

    Args:
        table: BeautifulSoup table element to parse.

    Returns:
        dict: Parsed table data with attributes, cols, and rows.
    """
    table_data = {
        "attributes": dict(table.attrs),
        "cols": [dict(col.attrs) for col in table.find_all("col")],
        "rows": [],
    }

    for row in table.find_all("tr"):
        row_data = {"attributes": dict(row.attrs), "cells": []}
        for cell in row.find_all(["td", "th"]):
            cell_info = {
                "text": "".join(cell.strings),
                "colspan": int(cell.get("colspan", 1)),
                "rowspan": int(cell.get("rowspan", 1)),
                "is_header": cell.name == "th",
                "attributes": dict(cell.attrs),
            }
            row_data["cells"].append(cell_info)
        table_data["rows"].append(row_data)

    return table_data
