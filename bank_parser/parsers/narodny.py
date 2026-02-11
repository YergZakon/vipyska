"""Parser for Народный сберегательный банк Казахстана (Halyk Bank).

Format: Metadata header (rows 0-8) + 19-column table with separate credit/debit columns.
Row 0: "HSBKKZKX АО \"НАРОДНЫЙ БАНК КАЗАХСТАНА\""
Row 1: "ИИН/БИН ..."
Rows 2-7: More metadata
Row 8: Header row with 19 columns
Data starts at row 9.

Key difference from standard: separate debit/credit amount columns instead of one amount column.
"""

from typing import List, Tuple, Optional

from ..base_parser import BaseParser
from ..models import Transaction
from ..file_reader import SheetData
from ..normalizer import (
    normalize_date, normalize_iin_bin, normalize_amount,
    normalize_currency, determine_direction, clean_string
)
from . import register_parser


HEADER_MARKERS = [
    'дата и время операции',
    'сумма в валюте',
    'по кредиту',
    'по дебету',
    'плательщик',
]


@register_parser
class NarodnyBankParser(BaseParser):
    """Parser for Narodny Bank (Halyk Bank) statements."""

    BANK_NAME = 'АО Народный сберегательный банк Казахстана'

    @classmethod
    def can_parse(cls, sheet: SheetData, file_info: dict) -> float:
        # Check for HSBKKZKX marker in first rows
        for row in sheet.rows[:5]:
            for cell in row:
                if cell and 'HSBKKZKX' in str(cell):
                    return 0.95

        # Check folder name
        folder = file_info.get('folder_name', '').lower()
        if 'народный сберегательный' in folder:
            return 0.8

        return 0.0

    def parse_sheet(self, sheet: SheetData, file_info: dict) -> Tuple[List[Transaction], dict]:
        rows = sheet.rows
        warnings = []
        transactions = []

        # Extract metadata from header rows
        account_number = None
        client_name = None

        for row in rows[:10]:
            for cell in row:
                if cell is None:
                    continue
                s = str(cell)
                if 'ИИН/БИН' in s:
                    # "ИИН/БИН 210540025224" — extract the number
                    parts = s.split()
                    for p in parts:
                        if p.isdigit() and len(p) >= 10:
                            client_name = None  # Will get from next cell
                            break
                if s.startswith('KZ') and len(s) >= 18:
                    account_number = s.strip()

        # Find header row
        header_idx = self.find_header_row(rows, HEADER_MARKERS)
        if header_idx is None:
            return [], {'warnings': warnings, 'errors': ['Header row not found']}

        header = rows[header_idx]
        header_lower = [str(c).lower().strip() if c else '' for c in header]

        # Build column map
        col_map = {}
        for i, h in enumerate(header_lower):
            if 'дата и время' in h:
                col_map['date'] = i
            elif 'валюта' in h and 'операции' in h:
                col_map['currency'] = i
            elif 'виды операции' in h or 'вид операции' in h or 'категория' in h:
                col_map['operation_type'] = i
            elif 'наименование сдп' in h:
                col_map['sdp'] = i
            elif 'по кредиту' in h and 'сумма' in h and 'валюте' in h:
                col_map['credit_amount'] = i
            elif 'по дебету' in h and 'сумма' in h and 'валюте' in h:
                col_map['debit_amount'] = i
            elif 'по кредиту' in h and 'тенге' in h:
                col_map['credit_tenge'] = i
            elif 'по дебету' in h and 'тенге' in h:
                col_map['debit_tenge'] = i
            elif 'плательщик' in h and ('наименование' in h or 'фио' in h):
                col_map['payer'] = i
            elif 'иин' in h and 'плательщик' in h:
                col_map['payer_iin'] = i
            elif 'банк плательщик' in h:
                col_map['payer_bank'] = i
            elif 'счет' in h and 'плательщик' in h:
                col_map['payer_account'] = i
            elif 'получател' in h and ('наименование' in h or 'фио' in h):
                col_map['recipient'] = i
            elif 'иин' in h and 'получател' in h:
                col_map['recipient_iin'] = i
            elif 'банк получател' in h:
                col_map['recipient_bank'] = i
            elif 'счет' in h and 'получател' in h:
                col_map['recipient_account'] = i
            elif 'код назначен' in h:
                col_map['knp'] = i
            elif 'назначение платежа' in h:
                col_map['payment_purpose'] = i

        # Also try to find account from row data (rows before header often have it)
        if not account_number:
            for row in rows[:header_idx]:
                for cell in row:
                    if cell and str(cell).startswith('KZ') and len(str(cell)) >= 18:
                        account_number = str(cell).strip()
                        break

        # Parse data rows
        data_start = header_idx + 1
        # Skip number row if present
        if data_start < len(rows):
            next_row = rows[data_start]
            non_none = [c for c in next_row if c is not None]
            if non_none and all(isinstance(c, (int, float)) for c in non_none):
                data_start += 1

        for row_idx in range(data_start, len(rows)):
            row = rows[row_idx]
            if not row or all(c is None for c in row):
                continue

            date_val = self._get(row, col_map.get('date'))
            if date_val is None:
                continue

            # Skip summary rows
            date_str = str(date_val).lower()
            if any(w in date_str for w in ['итого', 'входящий', 'исходящий', 'остаток', 'всего']):
                continue

            credit_amt = normalize_amount(self._get(row, col_map.get('credit_amount')))
            debit_amt = normalize_amount(self._get(row, col_map.get('debit_amount')))
            credit_tenge = normalize_amount(self._get(row, col_map.get('credit_tenge')))
            debit_tenge = normalize_amount(self._get(row, col_map.get('debit_tenge')))

            direction = determine_direction(debit_amount=debit_amt, credit_amount=credit_amt)
            amount = credit_amt or debit_amt
            amount_tenge = credit_tenge or debit_tenge

            t = Transaction(
                transaction_date=normalize_date(date_val),
                amount=amount,
                currency=normalize_currency(self._get(row, col_map.get('currency'))),
                amount_tenge=amount_tenge,
                direction=direction,
                payer=clean_string(self._get(row, col_map.get('payer'))),
                payer_iin_bin=normalize_iin_bin(self._get(row, col_map.get('payer_iin'))),
                payer_bank=clean_string(self._get(row, col_map.get('payer_bank'))),
                payer_account=clean_string(self._get(row, col_map.get('payer_account'))),
                recipient=clean_string(self._get(row, col_map.get('recipient'))),
                recipient_iin_bin=normalize_iin_bin(self._get(row, col_map.get('recipient_iin'))),
                recipient_bank=clean_string(self._get(row, col_map.get('recipient_bank'))),
                recipient_account=clean_string(self._get(row, col_map.get('recipient_account'))),
                operation_type=clean_string(self._get(row, col_map.get('operation_type'))),
                knp=clean_string(self._get(row, col_map.get('knp'))),
                payment_purpose=clean_string(self._get(row, col_map.get('payment_purpose'))),
                document_number=None,
                statement_bank=self.BANK_NAME,
                account_number=account_number,
                source_file=file_info['filename'],
            )
            transactions.append(t)

        return transactions, {
            'account_number': account_number,
            'warnings': warnings,
            'errors': [],
        }

    @staticmethod
    def _get(row: list, idx: Optional[int]):
        if idx is None or idx >= len(row):
            return None
        return row[idx]
