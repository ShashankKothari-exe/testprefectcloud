#!/usr/bin/env python3
"""CLI entrypoint for the portable chapter/section notes flows bundle."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

_BUNDLE_ROOT = Path(__file__).resolve().parent
if str(_BUNDLE_ROOT) not in sys.path:
    sys.path.insert(0, str(_BUNDLE_ROOT))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run USITC chapter/section notes flows: bronze download, then silver HTML→JSON."
        )
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--bronze-only",
        action="store_true",
        help="Only run hs_data_chapter_section_notes_2b_from_usitc.",
    )
    group.add_argument(
        "--silver-only",
        action="store_true",
        help="Only run hs_data_chapter_section_notes_b2s_from_html_to_json.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable DEBUG logging.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
    )

    from dotenv import load_dotenv

    load_dotenv(_BUNDLE_ROOT / ".env")

    from flows import (
        hs_data_chapter_section_notes_2b_from_usitc,
        hs_data_chapter_section_notes_b2s_from_html_to_json,
    )

    if not args.silver_only:
        logging.info("Running hs_data_chapter_section_notes_2b_from_usitc")
        hs_data_chapter_section_notes_2b_from_usitc()

    if not args.bronze_only:
        logging.info("Running hs_data_chapter_section_notes_b2s_from_html_to_json")
        hs_data_chapter_section_notes_b2s_from_html_to_json()

    logging.info("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
