"""Abstract base parser class for all bank statement parsers."""

from abc import ABC, abstractmethod
from typing import List, Tuple, Optional
import logging

from .models import Transaction, ParseResult
from .file_reader import SheetData
from .normalizer import clean_string

logger = logging.getLogger('bank_parser')


class BaseParser(ABC):
    """Abstract base class for all bank statement parsers."""

    BANK_NAME: str = ""  # Human-readable bank name for statement_bank field

    @classmethod
    @abstractmethod
    def can_parse(cls, sheet: SheetData, file_info: dict) -> float:
        """Return confidence score 0.0-1.0 that this parser can handle the file.

        Args:
            sheet: First sheet data from the file
            file_info: Dict with keys: filename, extension, folder_name, filepath
        """
        pass

    @abstractmethod
    def parse_sheet(self, sheet: SheetData, file_info: dict) -> Tuple[List[Transaction], dict]:
        """Parse a single worksheet into Transaction objects.

        Args:
            sheet: Worksheet data
            file_info: File metadata dict

        Returns:
            Tuple of (transactions_list, metadata_dict)
            metadata_dict may contain: account_number, warnings, errors
        """
        pass

    def parse(self, sheets: List[SheetData], file_info: dict) -> ParseResult:
        """Parse all sheets of a file. Override if bank uses multiple sheets."""
        result = ParseResult(
            filepath=file_info['filepath'],
            source_file=file_info['filename'],
            bank_detected=self.BANK_NAME,
            parser_used=self.__class__.__name__,
        )

        all_transactions = []
        for sheet in sheets:
            try:
                transactions, metadata = self.parse_sheet(sheet, file_info)
                all_transactions.extend(transactions)
                if metadata.get('account_number'):
                    result.account_number = metadata['account_number']
                result.warnings.extend(metadata.get('warnings', []))
                result.errors.extend(metadata.get('errors', []))
            except Exception as e:
                result.errors.append(f"Error parsing sheet '{sheet.name}': {e}")
                logger.error(f"Error parsing sheet '{sheet.name}' in {file_info['filename']}: {e}")

        result.transactions = all_transactions
        result.total_transactions = len(all_transactions)
        result.parse_status = 'success' if all_transactions else ('failed' if result.errors else 'skipped')
        if all_transactions and result.errors:
            result.parse_status = 'partial'

        return result

    # --- Utility methods ---

    @staticmethod
    def find_header_row(rows: list, marker_columns: list, max_rows: int = 30) -> Optional[int]:
        """Find the row index containing header columns.

        Args:
            rows: List of row lists
            marker_columns: List of column name substrings to look for
            max_rows: Maximum rows to scan

        Returns:
            Row index or None
        """
        for i, row in enumerate(rows[:max_rows]):
            row_str = [clean_string(c) or '' for c in row]
            row_lower = [s.lower() for s in row_str]
            matches = sum(
                1 for marker in marker_columns
                if any(marker.lower() in cell for cell in row_lower)
            )
            # Require at least 60% of markers to match
            if matches >= len(marker_columns) * 0.6:
                return i

        return None

    @staticmethod
    def extract_cell_value(rows: list, search_text: str, max_rows: int = 30) -> Optional[str]:
        """Search first N rows for a cell containing search_text, return value from next cell."""
        search_lower = search_text.lower()
        for row in rows[:max_rows]:
            for i, cell in enumerate(row):
                if cell and search_lower in str(cell).lower():
                    # Return the cell itself or next cell depending on context
                    text = str(cell)
                    # If the cell contains "Key: Value", extract value
                    if ':' in text:
                        parts = text.split(':', 1)
                        return parts[1].strip()
                    # Otherwise return next cell
                    if i + 1 < len(row) and row[i + 1]:
                        return str(row[i + 1]).strip()
                    return text
        return None

    @staticmethod
    def get_account_from_filename(filename: str) -> Optional[str]:
        """Try to extract IBAN account number from filename."""
        import re
        # KZ + 2 digits + 4 alphanumeric + 12 digits pattern
        match = re.search(r'(KZ\d{2}[A-Za-z0-9]{4}\d{12})', filename)
        if match:
            return match.group(1)
        return None
