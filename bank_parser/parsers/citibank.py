"""Parser for АО Ситибанк Казахстан.

Format: Certificate/report format "Справка по движению денег/товара".
14 columns with header-heavy metadata.
"""

import re
from typing import List, Tuple, Optional

from ..base_parser import BaseParser
from ..models import Transaction
from ..file_reader import SheetData
from ..normalizer import (
    normalize_date, normalize_iin_bin, normalize_amount,
    normalize_currency, clean_string
)
from . import register_parser


@register_parser
class CitibankParser(BaseParser):
    BANK_NAME = 'АО Ситибанк Казахстан'

    @classmethod
    def can_parse(cls, sheet: SheetData, file_info: dict) -> float:
        for row in sheet.rows[:5]:
            for cell in row:
                if cell and 'справка по движению' in str(cell).lower():
                    return 0.9
        folder = file_info.get('folder_name', '').lower()
        if 'ситибанк' in folder or 'citibank' in folder.lower():
            return 0.8
        fn = file_info.get('filename', '').lower()
        if 'справка' in fn and 'spsd' in fn:
            return 0.85
        return 0.0

    def parse_sheet(self, sheet: SheetData, file_info: dict) -> Tuple[List[Transaction], dict]:
        rows = sheet.rows
        transactions = []

        # Find table header
        header_idx = None
        for i, row in enumerate(rows[:20]):
            row_text = ' '.join(str(c).lower() for c in row if c)
            if 'дата' in row_text and ('сумма' in row_text or 'получатель' in row_text or 'отправитель' in row_text):
                header_idx = i
                break

        if header_idx is None:
            return [], {'warnings': ['Certificate format — limited transaction data'], 'errors': [], 'account_number': None}

        header = rows[header_idx]
        header_lower = [str(c).lower().strip() if c else '' for c in header]

        col_map = {}
        for i, h in enumerate(header_lower):
            if 'дата' in h:
                col_map['date'] = i
            elif 'сумма' in h:
                col_map['amount'] = i
            elif 'валюта' in h:
                col_map['currency'] = i
            elif 'отправитель' in h or 'плательщик' in h:
                col_map['sender'] = i
            elif 'получатель' in h:
                col_map['recipient'] = i
            elif 'назначение' in h:
                col_map['purpose'] = i
            elif 'иин' in h or 'бин' in h:
                col_map.setdefault('iin', i)

        for row_idx in range(header_idx + 1, len(rows)):
            row = rows[row_idx]
            if not row or all(c is None for c in row):
                continue

            date_val = self._get(row, col_map.get('date'))
            if date_val is None:
                continue

            t = Transaction(
                transaction_date=normalize_date(date_val),
                amount=normalize_amount(self._get(row, col_map.get('amount'))),
                currency=normalize_currency(self._get(row, col_map.get('currency'))),
                amount_tenge=None,
                direction=None,
                payer=clean_string(self._get(row, col_map.get('sender'))),
                payer_iin_bin=None, payer_bank=None, payer_account=None,
                recipient=clean_string(self._get(row, col_map.get('recipient'))),
                recipient_iin_bin=None, recipient_bank=None, recipient_account=None,
                operation_type=None, knp=None,
                payment_purpose=clean_string(self._get(row, col_map.get('purpose'))),
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
