"""Parser for Акционерное общество Исламский банк Заман-Банк.

Format: .xls files with metadata header + 12 columns.
Row 0: "Акционерное общество \"Исламский банк \"Заман-Банк\" БИК ZAJSKZ22"
"""

import re
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
class ZamanBankParser(BaseParser):
    BANK_NAME = 'АО Исламский банк Заман-Банк'

    @classmethod
    def can_parse(cls, sheet: SheetData, file_info: dict) -> float:
        for row in sheet.rows[:5]:
            for cell in row:
                if cell:
                    s = str(cell)
                    if 'Заман-Банк' in s or 'ZAJSKZ22' in s:
                        return 0.95
        folder = file_info.get('folder_name', '').lower()
        if 'заман' in folder:
            return 0.8
        return 0.0

    def parse_sheet(self, sheet: SheetData, file_info: dict) -> Tuple[List[Transaction], dict]:
        rows = sheet.rows
        transactions = []
        account_number = None

        for row in rows[:10]:
            for cell in row:
                if cell:
                    match = re.search(r'(KZ\w{16,22})', str(cell))
                    if match:
                        account_number = match.group(1)

        # Find header
        header_idx = None
        for i, row in enumerate(rows[:15]):
            row_text = ' '.join(str(c).lower() for c in row if c)
            if 'дата' in row_text and ('сумма' in row_text or 'дебет' in row_text or 'кредит' in row_text):
                header_idx = i
                break

        if header_idx is None:
            return [], {'warnings': [], 'errors': ['Header not found'], 'account_number': account_number}

        header = rows[header_idx]
        header_lower = [str(c).lower().strip() if c else '' for c in header]

        col_map = {}
        for i, h in enumerate(header_lower):
            if 'дата' in h and 'date' not in col_map:
                col_map['date'] = i
            elif 'дебет' in h:
                col_map['debit'] = i
            elif 'кредит' in h:
                col_map['credit'] = i
            elif 'сумма' in h:
                col_map['amount'] = i
            elif 'валюта' in h:
                col_map['currency'] = i
            elif 'назначение' in h or 'описание' in h:
                col_map['purpose'] = i
            elif 'плательщик' in h or 'отправитель' in h:
                col_map['payer'] = i
            elif 'получатель' in h:
                col_map['recipient'] = i
            elif 'иин' in h or 'бин' in h:
                col_map.setdefault('iin', i)

        for row_idx in range(header_idx + 1, len(rows)):
            row = rows[row_idx]
            if not row or all(c is None for c in row):
                continue

            date_val = self._get(row, col_map.get('date'))
            if date_val is None:
                continue
            if isinstance(date_val, str) and any(w in date_val.lower() for w in ['итого', 'остаток']):
                continue

            debit = normalize_amount(self._get(row, col_map.get('debit')))
            credit = normalize_amount(self._get(row, col_map.get('credit')))
            amount = normalize_amount(self._get(row, col_map.get('amount'))) or credit or debit
            direction = determine_direction(debit_amount=debit, credit_amount=credit)

            t = Transaction(
                transaction_date=normalize_date(date_val),
                amount=amount,
                currency=normalize_currency(self._get(row, col_map.get('currency'))) or 'KZT',
                amount_tenge=amount,
                direction=direction,
                payer=clean_string(self._get(row, col_map.get('payer'))),
                payer_iin_bin=normalize_iin_bin(self._get(row, col_map.get('iin'))),
                payer_bank=None, payer_account=None,
                recipient=clean_string(self._get(row, col_map.get('recipient'))),
                recipient_iin_bin=None, recipient_bank=None, recipient_account=None,
                operation_type=None, knp=None,
                payment_purpose=clean_string(self._get(row, col_map.get('purpose'))),
                document_number=None,
                statement_bank=self.BANK_NAME,
                account_number=account_number,
                source_file=file_info['filename'],
            )
            transactions.append(t)

        return transactions, {'account_number': account_number, 'warnings': [], 'errors': []}

    @staticmethod
    def _get(row, idx):
        if idx is None or idx >= len(row):
            return None
        return row[idx]
