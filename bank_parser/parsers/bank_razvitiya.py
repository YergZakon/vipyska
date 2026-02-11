"""Parser for АО Банк Развития Казахстана.

Format: System code format starting with "PC01_515_S".
10 columns with metadata. SWIFT: DVKAKZKA.
Columns include: Референс, Дата, Банк корресп., Счет корресп., etc.
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
class BankRazvitiyaParser(BaseParser):
    BANK_NAME = 'АО Банк Развития Казахстана'

    @classmethod
    def can_parse(cls, sheet: SheetData, file_info: dict) -> float:
        for row in sheet.rows[:5]:
            for cell in row:
                if cell:
                    s = str(cell)
                    if 'DVKAKZKA' in s or 'PC01_515' in s:
                        return 0.95
                    if 'Банк Развития' in s:
                        return 0.9
        folder = file_info.get('folder_name', '').lower()
        if 'банк развития' in folder:
            return 0.8
        return 0.0

    def parse_sheet(self, sheet: SheetData, file_info: dict) -> Tuple[List[Transaction], dict]:
        rows = sheet.rows
        transactions = []
        account_number = None

        # Extract account from filename
        match = re.search(r'(KZ\w{16,22})', file_info.get('filename', ''))
        if match:
            account_number = match.group(1)

        # Find header — can be deep in the file (row 23+)
        header_idx = None
        for i, row in enumerate(rows[:40]):
            row_text = ' '.join(str(c).lower() for c in row if c)
            if 'дата' in row_text and ('референс' in row_text or 'корресп' in row_text):
                header_idx = i
                break

        if header_idx is None:
            return [], {'warnings': ['System code format'], 'errors': ['Header not found'], 'account_number': account_number}

        header = rows[header_idx]
        header_lower = [str(c).lower().strip() if c else '' for c in header]

        col_map = {}
        for i, h in enumerate(header_lower):
            if 'дата' in h:
                col_map['date'] = i
            elif 'референс' in h:
                col_map['ref'] = i
            elif 'сумма' in h and 'тенге' not in h:
                col_map['amount'] = i
            elif 'тенге' in h:
                col_map['amount_tenge'] = i
            elif 'валюта' in h:
                col_map['currency'] = i
            elif 'корресп' in h and 'банк' in h:
                col_map['corr_bank'] = i
            elif 'корресп' in h and 'счет' in h:
                col_map['corr_account'] = i
            elif 'назначение' in h:
                col_map['purpose'] = i
            elif 'дебет' in h:
                col_map['debit'] = i
            elif 'кредит' in h:
                col_map['credit'] = i

        # Skip sub-header row if present (e.g., "док.", "корресп.", "корресп.")
        data_start = header_idx + 1
        if data_start < len(rows):
            sub = rows[data_start]
            sub_text = ' '.join(str(c).lower() for c in sub if c)
            if 'док' in sub_text or 'корресп' in sub_text:
                data_start += 1

        for row_idx in range(data_start, len(rows)):
            row = rows[row_idx]
            if not row or all(c is None for c in row):
                continue

            date_val = self._get(row, col_map.get('date'))
            if date_val is None:
                continue

            if isinstance(date_val, str) and any(w in date_val.lower() for w in ['итого', 'остаток']):
                continue

            amount = normalize_amount(self._get(row, col_map.get('amount')))
            debit = normalize_amount(self._get(row, col_map.get('debit')))
            credit = normalize_amount(self._get(row, col_map.get('credit')))

            from ..normalizer import determine_direction
            direction = determine_direction(debit_amount=debit, credit_amount=credit)
            if not amount:
                amount = credit or debit

            t = Transaction(
                transaction_date=normalize_date(date_val),
                amount=amount,
                currency=normalize_currency(self._get(row, col_map.get('currency'))) or 'KZT',
                amount_tenge=normalize_amount(self._get(row, col_map.get('amount_tenge'))),
                direction=direction,
                payer=None, payer_iin_bin=None, payer_bank=None, payer_account=None,
                recipient=None, recipient_iin_bin=None,
                recipient_bank=clean_string(self._get(row, col_map.get('corr_bank'))),
                recipient_account=clean_string(self._get(row, col_map.get('corr_account'))),
                operation_type=None, knp=None,
                payment_purpose=clean_string(self._get(row, col_map.get('purpose'))),
                document_number=clean_string(self._get(row, col_map.get('ref'))),
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
