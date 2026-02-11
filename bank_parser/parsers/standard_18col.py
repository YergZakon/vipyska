"""Parser for standard 18-column format.

Used by: Шинхан, Home Credit, ВТБ, Фридом Финанс, Фридом Банк.
These all share the same column structure with minor variations.

Shinhan/HomeCredit/Freedom: header at row 0 or 1, 18 cols, row after header has numbers 1-18
VTB: header at row 0, 18 cols, dates "YYYY.MM.DD HH:MM:SS", currency "KZT - Тенге"
Freedom Finance/Bank: header at row 1 (row 0 is empty), 17 cols (no Назначение платежа)
"""

from typing import List, Tuple, Optional
import re

from ..base_parser import BaseParser
from ..models import Transaction
from ..file_reader import SheetData
from ..normalizer import (
    normalize_date, normalize_iin_bin, normalize_amount,
    normalize_currency, clean_string
)
from . import register_parser


# Standard 18-column headers (Shinhan/Home Credit)
STANDARD_MARKERS = [
    'дата и время операции',
    'валюта операции',
    'сумма в валюте',
    'сумма в тенге',
    'плательщик',
    'получател',
]

# VTB-specific: "Вид операции (КД)" instead of "Виды операции (категория документа)"
VTB_MARKERS = ['вид операции (кд)', 'резиденство']  # Note: VTB has typo "резиденство"


def _is_standard_header(row: list) -> bool:
    """Check if row looks like the standard 18-col header."""
    if not row or len([c for c in row if c]) < 10:
        return False
    row_text = ' '.join(str(c).lower() for c in row if c)
    matches = sum(1 for m in STANDARD_MARKERS if m in row_text)
    return matches >= 4


def _find_header_idx(rows: list) -> Optional[int]:
    """Find header row in first 10 rows."""
    for i, row in enumerate(rows[:10]):
        if _is_standard_header(row):
            return i
    return None


def _find_data_start(rows: list, header_idx: int) -> int:
    """Find where actual data starts (skip number row like 1,2,3...)."""
    next_row = header_idx + 1
    if next_row < len(rows):
        row = rows[next_row]
        # Check if it's a number row (1, 2, 3, ...)
        non_none = [c for c in row if c is not None]
        if non_none and all(isinstance(c, (int, float)) and c == int(c) for c in non_none):
            return next_row + 1
    return next_row


@register_parser
class Standard18ColParser(BaseParser):
    """Parser for standard 18-column bank statement format."""

    BANK_NAME = ""  # Will be set dynamically based on detection

    @classmethod
    def can_parse(cls, sheet: SheetData, file_info: dict) -> float:
        """Detect standard 18-col format."""
        header_idx = _find_header_idx(sheet.rows)
        if header_idx is None:
            return 0.0

        row = sheet.rows[header_idx]
        non_none_count = len([c for c in row if c])

        # Must have 17-18 non-None columns
        if non_none_count < 15:
            return 0.0

        # Check for standard markers
        row_text = ' '.join(str(c).lower() for c in row if c)

        # VTB specific
        if any(m in row_text for m in VTB_MARKERS):
            return 0.9

        # Standard format (Shinhan, Home Credit, Freedom)
        if 'виды операции' in row_text or 'категория документа' in row_text:
            return 0.9

        if 'дата и время операции' in row_text and 'сумма' in row_text:
            return 0.7

        return 0.0

    def _detect_bank_name(self, sheet: SheetData, file_info: dict) -> str:
        """Detect specific bank from data content, with folder as fallback."""
        # SWIFT code map for 18-col banks
        swift_to_bank = {
            'VTBAKZKA': 'ДО АО Банк ВТБ (Казахстан)',
            'SHBKKZKA': 'АО Шинхан Банк Казахстан',
        }

        # Step 1: Scan data for SWIFT codes and bank name mentions
        header_idx = _find_header_idx(sheet.rows)
        scan_end = min(len(sheet.rows), (header_idx or 0) + 10)
        for row in sheet.rows[:scan_end]:
            for cell in row:
                if cell:
                    cs = str(cell)
                    cl = cs.lower()
                    for swift, bank_name in swift_to_bank.items():
                        if swift in cs:
                            return bank_name
                    if 'втб' in cl:
                        return 'ДО АО Банк ВТБ (Казахстан)'
                    if 'shinhan' in cl or 'шинхан' in cl:
                        return 'АО Шинхан Банк Казахстан'
                    if 'home credit' in cl or 'хоум кредит' in cl:
                        return 'АО Home Credit Bank'
                    if 'фридом финанс' in cl:
                        return 'АО Банк Фридом Финанс Казахстан'
                    if 'фридом' in cl and 'банк' in cl:
                        return 'АО Фридом Банк Казахстан'

        # Step 2: Folder fallback
        folder = file_info.get('folder_name', '').lower()
        if 'втб' in folder or 'vtb' in folder:
            return 'ДО АО Банк ВТБ (Казахстан)'
        if 'шинхан' in folder or 'shinhan' in folder:
            return 'АО Шинхан Банк Казахстан'
        if 'home credit' in folder or 'хоум' in folder:
            return 'АО Home Credit Bank'
        if 'фридом финанс' in folder:
            return 'АО Банк Фридом Финанс Казахстан'
        if 'фридом банк' in folder or 'фридом' in folder:
            return 'АО Фридом Банк Казахстан'

        return file_info.get('folder_name', '') or 'Неизвестный банк'

    def parse_sheet(self, sheet: SheetData, file_info: dict) -> Tuple[List[Transaction], dict]:
        rows = sheet.rows
        warnings = []
        transactions = []

        header_idx = _find_header_idx(rows)
        if header_idx is None:
            return [], {'warnings': [], 'errors': ['Header row not found']}

        header = rows[header_idx]
        data_start = _find_data_start(rows, header_idx)

        # Detect bank name
        bank_name = self._detect_bank_name(sheet, file_info)
        self.BANK_NAME = bank_name

        # Build column index map
        col_map = {}
        header_lower = [str(c).lower().strip() if c else '' for c in header]

        for i, h in enumerate(header_lower):
            if 'дата и время' in h or h == 'дата операции':
                col_map['date'] = i
            elif 'валюта операции' in h or (h == 'валюта' and 'date' in col_map):
                col_map['currency'] = i
            elif 'виды операции' in h or 'вид операции' in h or 'категория' in h:
                col_map['operation_type'] = i
            elif 'наименование сдп' in h:
                col_map['sdp'] = i
            elif 'сумма в валюте' in h or h == 'сумма (вал.)':
                col_map['amount'] = i
            elif 'сумма в тенге' in h or h == 'сумма (тенге)':
                col_map['amount_tenge'] = i
            elif ('плательщик' in h and ('наименование' in h or 'фио' in h)) or h == 'наименование/фио плательщика>':
                col_map['payer'] = i
            elif 'иин' in h and 'плательщик' in h:
                col_map['payer_iin'] = i
            elif 'резиден' in h and 'плательщик' in h:
                col_map['payer_residency'] = i
            elif 'банк плательщик' in h:
                col_map['payer_bank'] = i
            elif 'счет' in h and 'плательщик' in h:
                col_map['payer_account'] = i
            elif ('получател' in h and ('наименование' in h or 'фио' in h)):
                col_map['recipient'] = i
            elif 'иин' in h and 'получател' in h:
                col_map['recipient_iin'] = i
            elif 'резиден' in h and 'получател' in h:
                col_map['recipient_residency'] = i
            elif 'банк получател' in h:
                col_map['recipient_bank'] = i
            elif 'счет' in h and 'получател' in h:
                col_map['recipient_account'] = i
            elif 'код назначен' in h or 'код назначение' in h or h == 'кнп':
                col_map['knp'] = i
            elif 'назначение платежа' in h:
                col_map['payment_purpose'] = i

        # Extract account number from sheet name or filename
        account = self._extract_account(sheet.name, file_info['filename'])

        # Parse data rows
        for row_idx in range(data_start, len(rows)):
            row = rows[row_idx]
            if not row or all(c is None for c in row):
                continue

            # Skip summary/total rows
            date_val = self._get(row, col_map.get('date'))
            if date_val is None:
                continue

            # Determine direction from operation type for VTB
            op_type = clean_string(self._get(row, col_map.get('operation_type')))
            direction = self._determine_direction_from_op(op_type)

            amount_val = normalize_amount(self._get(row, col_map.get('amount')))
            amount_tenge_val = normalize_amount(self._get(row, col_map.get('amount_tenge')))

            # For VTB, negative amounts mean expense
            if amount_val is not None and amount_val < 0:
                direction = direction or 'Расход'
                amount_val = abs(amount_val)
            if amount_tenge_val is not None and amount_tenge_val < 0:
                direction = direction or 'Расход'
                amount_tenge_val = abs(amount_tenge_val)

            t = Transaction(
                transaction_date=normalize_date(date_val),
                amount=amount_val,
                currency=normalize_currency(self._get(row, col_map.get('currency'))),
                amount_tenge=amount_tenge_val,
                direction=direction,
                payer=clean_string(self._get(row, col_map.get('payer'))),
                payer_iin_bin=normalize_iin_bin(self._get(row, col_map.get('payer_iin'))),
                payer_bank=clean_string(self._get(row, col_map.get('payer_bank'))),
                payer_account=clean_string(self._get(row, col_map.get('payer_account'))),
                recipient=clean_string(self._get(row, col_map.get('recipient'))),
                recipient_iin_bin=normalize_iin_bin(self._get(row, col_map.get('recipient_iin'))),
                recipient_bank=clean_string(self._get(row, col_map.get('recipient_bank'))),
                recipient_account=clean_string(self._get(row, col_map.get('recipient_account'))),
                operation_type=op_type,
                knp=clean_string(self._get(row, col_map.get('knp'))),
                payment_purpose=clean_string(self._get(row, col_map.get('payment_purpose'))),
                document_number=None,
                statement_bank=bank_name,
                account_number=account,
                source_file=file_info['filename'],
            )
            transactions.append(t)

        return transactions, {
            'account_number': account,
            'warnings': warnings,
            'errors': [],
        }

    def _determine_direction_from_op(self, op_type: str) -> Optional[str]:
        """Determine direction from VTB-style operation type codes."""
        if not op_type:
            return None
        op_lower = op_type.lower()
        # VTB: "1 - Внешние входящие", "3 - Внутренние входящие"
        if 'входящ' in op_lower:
            return 'Приход'
        if 'исходящ' in op_lower:
            return 'Расход'
        # General patterns
        if 'зачисление' in op_lower or 'пополнение' in op_lower:
            return 'Приход'
        if 'списание' in op_lower or 'снятие' in op_lower:
            return 'Расход'
        return None

    def _extract_account(self, sheet_name: str, filename: str) -> Optional[str]:
        """Extract IBAN from sheet name or filename."""
        # Try sheet name first (e.g., "KZ72551N129228750KZT")
        match = re.search(r'(KZ\w{16,20})', sheet_name)
        if match:
            return match.group(1)
        # Try filename
        match = re.search(r'(KZ\w{16,20})', filename)
        if match:
            return match.group(1)
        return None

    @staticmethod
    def _get(row: list, idx: Optional[int]):
        """Safely get value from row by index."""
        if idx is None or idx >= len(row):
            return None
        return row[idx]
