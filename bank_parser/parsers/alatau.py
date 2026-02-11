"""Parser for АО Alatau City Bank.

Format: Metadata header + 16-column table with separate debit/credit oborot.
File naming: Statement_standard_KZxxx.xlsx
Some files may have only 1 row (empty statements).
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
class AlatauCityParser(BaseParser):
    BANK_NAME = 'АО Alatau City Bank'

    @classmethod
    def can_parse(cls, sheet: SheetData, file_info: dict) -> float:
        fn = file_info.get('filename', '').lower()
        if 'statement_standard' in fn:
            return 0.95

        folder = file_info.get('folder_name', '').lower()
        if 'alatau' in folder:
            return 0.8

        for row in sheet.rows[:10]:
            for cell in row:
                if cell and 'alatau city' in str(cell).lower():
                    return 0.85
        return 0.0

    def parse_sheet(self, sheet: SheetData, file_info: dict) -> Tuple[List[Transaction], dict]:
        rows = sheet.rows
        transactions = []
        account_number = None

        # Extract account from filename
        match = re.search(r'(KZ\w{16,22})', file_info.get('filename', ''))
        if match:
            account_number = match.group(1)

        # Find header row
        header_idx = None
        for i, row in enumerate(rows[:20]):
            row_text = ' '.join(str(c).lower() for c in row if c)
            if 'дата' in row_text and ('дебет' in row_text or 'кредит' in row_text or 'оборот' in row_text):
                header_idx = i
                break
            if 'плательщик' in row_text or 'получатель' in row_text:
                header_idx = i
                break

        if header_idx is None:
            # Empty statement
            return [], {'warnings': ['No data rows found'], 'errors': [], 'account_number': account_number}

        header = rows[header_idx]
        header_lower = [str(c).lower().strip() if c else '' for c in header]

        col_map = {}
        for i, h in enumerate(header_lower):
            if 'дата' in h:
                col_map['date'] = i
            elif 'дебетовый оборот' in h or ('дебет' in h and 'оборот' in h):
                col_map['debit'] = i
            elif 'кредитовый оборот' in h or ('кредит' in h and 'оборот' in h):
                col_map['credit'] = i
            elif 'валюта' in h:
                col_map['currency'] = i
            elif 'плательщик' in h:
                col_map['payer'] = i
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

            if isinstance(date_val, str) and any(w in date_val.lower() for w in ['итого', 'остаток']):
                continue

            debit = normalize_amount(self._get(row, col_map.get('debit')))
            credit = normalize_amount(self._get(row, col_map.get('credit')))
            direction = determine_direction(debit_amount=debit, credit_amount=credit)
            amount = credit or debit

            t = Transaction(
                transaction_date=normalize_date(date_val),
                amount=amount,
                currency=normalize_currency(self._get(row, col_map.get('currency'))),
                amount_tenge=amount,
                direction=direction,
                payer=clean_string(self._get(row, col_map.get('payer'))),
                payer_iin_bin=None, payer_bank=None, payer_account=None,
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
