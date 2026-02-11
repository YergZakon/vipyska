"""Parser for АО ForteBank.

Format: SDP transfers — 14 columns with metadata header.
Row 0-4: metadata (date, code)
Row 5: "Инфорация по переводам" (typo preserved)
Row 8/9: Header row
Columns include: №, Отделение, Вид перевода, Состояние, Дата, Номер, Валюта, Сумма,
                 ФИО отправителя, ИИН отправителя, Документ, ФИО получателя, Направление, Страна

Also has registry files (Prilozhenie) — skip those.
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


@register_parser
class ForteBankSDPParser(BaseParser):
    BANK_NAME = 'АО ForteBank'

    @classmethod
    def can_parse(cls, sheet: SheetData, file_info: dict) -> float:
        for row in sheet.rows[:10]:
            for cell in row:
                if cell and 'инфорация по переводам' in str(cell).lower():
                    return 0.95
                if cell and 'информация по переводам' in str(cell).lower():
                    return 0.95

        # Check for SDP header structure
        for row in sheet.rows[:15]:
            row_text = ' '.join(str(c).lower() for c in row if c)
            if 'вид перевода' in row_text and 'состояние' in row_text:
                return 0.9
            if 'золотая корона' in row_text:
                return 0.85

        folder = file_info.get('folder_name', '').lower()
        if 'forte' in folder:
            # Check if it's a registry file (skip)
            fn = file_info.get('filename', '').lower()
            if 'prilozhenie' in fn or 'pril' in fn:
                return 0.0  # Skip registry files
            if 'sdp' in fn.lower():
                return 0.8
        return 0.0

    def parse_sheet(self, sheet: SheetData, file_info: dict) -> Tuple[List[Transaction], dict]:
        rows = sheet.rows
        transactions = []

        # Find header row
        header_idx = None
        for i, row in enumerate(rows[:15]):
            row_text = ' '.join(str(c).lower() for c in row if c)
            if ('отделение' in row_text or 'вид перевода' in row_text) and 'дата' in row_text:
                header_idx = i
                break
            if '№' in row_text and 'сумма' in row_text and 'фио' in row_text:
                header_idx = i
                break

        if header_idx is None:
            return [], {'warnings': [], 'errors': ['Header not found'], 'account_number': None}

        header = rows[header_idx]
        header_lower = [str(c).lower().strip() if c else '' for c in header]

        col_map = {}
        for i, h in enumerate(header_lower):
            if h == 'дата' or 'дата' in h and 'операц' not in h:
                col_map['date'] = i
            elif 'отделение' in h:
                col_map['branch'] = i
            elif 'вид перевода' in h:
                col_map['transfer_type'] = i
            elif 'состояние' in h:
                col_map['status'] = i
            elif 'валюта' in h:
                col_map['currency'] = i
            elif 'сумма' in h:
                col_map['amount'] = i
            elif 'отправител' in h and 'фио' in h:
                col_map['sender'] = i
            elif 'иин' in h and 'отправител' in h:
                col_map['sender_iin'] = i
            elif 'получател' in h and 'фио' in h:
                col_map['recipient'] = i
            elif 'направлен' in h:
                col_map['direction'] = i
            elif 'страна' in h:
                col_map['country'] = i
            elif 'иин' in h:
                col_map.setdefault('sender_iin', i)

        for row_idx in range(header_idx + 1, len(rows)):
            row = rows[row_idx]
            if not row or all(c is None for c in row):
                continue

            date_val = self._get(row, col_map.get('date'))
            if date_val is None:
                continue

            raw_dir = clean_string(self._get(row, col_map.get('direction')))
            direction = determine_direction(raw_direction=raw_dir)

            amount_raw = self._get(row, col_map.get('amount'))
            # ForteBank amounts may have leading spaces
            amount = normalize_amount(amount_raw)

            t = Transaction(
                transaction_date=normalize_date(date_val),
                amount=amount,
                currency=normalize_currency(self._get(row, col_map.get('currency'))),
                amount_tenge=amount,  # Most transfers in KZT
                direction=direction,
                payer=clean_string(self._get(row, col_map.get('sender'))),
                payer_iin_bin=normalize_iin_bin(self._get(row, col_map.get('sender_iin'))),
                payer_bank=self.BANK_NAME,
                payer_account=None,
                recipient=clean_string(self._get(row, col_map.get('recipient'))),
                recipient_iin_bin=None, recipient_bank=None, recipient_account=None,
                operation_type=clean_string(self._get(row, col_map.get('transfer_type'))),
                knp=None,
                payment_purpose=clean_string(self._get(row, col_map.get('transfer_type'))),
                document_number=None,
                statement_bank=self.BANK_NAME,
                account_number=None,
                source_file=file_info['filename'],
            )
            transactions.append(t)

        return transactions, {'account_number': None, 'warnings': [], 'errors': []}

    @staticmethod
    def _get(row, idx):
        if idx is None or idx >= len(row):
            return None
        return row[idx]


@register_parser
class ForteBankRegistryParser(BaseParser):
    """ForteBank registry files (Prilozhenie) — skip, not transaction data."""
    BANK_NAME = 'АО ForteBank'

    @classmethod
    def can_parse(cls, sheet: SheetData, file_info: dict) -> float:
        fn = file_info.get('filename', '').lower()
        folder = file_info.get('folder_name', '').lower()
        if 'forte' in folder and ('prilozhenie' in fn or 'pril_' in fn):
            return 0.95
        for row in sheet.rows[:5]:
            row_text = ' '.join(str(c).lower() for c in row if c)
            if 'наименование организации' in row_text and 'код гк' in row_text:
                return 0.9
        return 0.0

    def parse_sheet(self, sheet: SheetData, file_info: dict) -> Tuple[List[Transaction], dict]:
        return [], {'warnings': ['Registry file — no transactions'], 'errors': [], 'account_number': None}
