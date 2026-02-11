"""Bank auto-detection from file content and structure."""

import logging
from typing import Optional, Tuple, Type

from .base_parser import BaseParser
from .file_reader import SheetData
from .parsers import PARSER_REGISTRY

logger = logging.getLogger('bank_parser')


def detect_parser(sheets: list, file_info: dict) -> Optional[Type[BaseParser]]:
    """Detect the best parser for the given file.

    Args:
        sheets: List of SheetData from the file
        file_info: Dict with keys: filename, extension, folder_name, filepath

    Returns:
        Parser class with highest confidence, or None
    """
    if not sheets:
        return None

    best_parser = None
    best_score = 0.0

    # Try all sheets, not just the first one (some HTML-xls files have garbled first sheets)
    for sheet in sheets:
        for parser_cls in PARSER_REGISTRY:
            try:
                score = parser_cls.can_parse(sheet, file_info)
                if score > best_score:
                    best_score = score
                    best_parser = parser_cls
            except Exception as e:
                logger.warning(f"Error in {parser_cls.__name__}.can_parse(): {e}")
        # If we got a strong match on this sheet, no need to check more
        if best_score >= 0.9:
            break

    if best_score >= 0.3:
        logger.info(f"Detected {best_parser.__name__} (score={best_score:.2f}) for {file_info['filename']}")
        return best_parser

    logger.warning(f"No parser detected for {file_info['filename']} (best score={best_score:.2f})")
    return None
