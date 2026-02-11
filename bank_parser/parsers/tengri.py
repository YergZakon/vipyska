"""Parser for АО Tengri Bank.

Format: Metadata header + 13-column table with separate debit/credit columns.
Row 0: "АО \"Tengri Bank\""
Row 1: "Дата формирования: 05/09/2023"
...more metadata...
Header row has: Дата | ИИН/БИН | Счет-корреспондент | СГК | Описание транз. | etc
Separate Дебет/Кредит (валюта) and (нац.покрытие) columns.
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
class TengriBankParser(BaseParser):
    BANK_NAME = 'АО Tengri Bank'

    @classmethod
    def can_parse(cls, sheet: SheetData, file_info: dict) -> float:
        for row in sheet.rows[:5]:
            for cell in row:
                if cell and 'Tengri Bank' in str(cell):
                    return 0.95
        folder = file_info.get('folder_name', '').lower()
        if 'tengri' in folder:
            return 0.8
        return 0.0

    def parse_sheet(self, sheet: SheetData, file_info: dict) -> Tuple[List[Transaction], dict]:
        rows = sheet.rows
        warnings = []
        transactions = []
        account_number = None
        currency = None

        # Extract metadata
        for row in rows[:15]:
            for cell in row:
                if cell is None:
                    continue
                s = str(cell)
                match = re.search(r'(KZ\w{16,22})', s)
                if match:
                    account_number = match.group(1)
                if 'валюта:' in s.lower():
                    currency = s.split(':')[-1].strip()

        # Find header row
        header_idx = None
        for i, row in enumerate(rows[:20]):
            row_text = ' '.join(str(c).lower() for c in row if c)
            if 'дата' in row_text and ('дебет' in row_text or 'кредит' in row_text):
                header_idx = i
                break

        if header_idx is None:
            return [], {'warnings': warnings, 'errors': ['Header not found'], 'account_number': account_number}

        header = rows[header_idx]
        header_lower = [str(c).lower().strip() if c else '' for c in header]

        col_map = {}
        for i, h in enumerate(header_lower):
            if h == 'дата' or 'дата опер' in h:
                col_map['date'] = i
            elif 'иин' in h or 'бин' in h:
                col_map['iin'] = i
            elif 'счет-корреспондент' in h or 'корресп' in h:
                col_map['corr_account'] = i
            elif 'описание' in h:
                col_map['description'] = i
            elif 'дебет' in h and 'валют' in h:
                col_map['debit_amount'] = i
            elif 'кредит' in h and 'валют' in h:
                col_map['credit_amount'] = i
            elif 'дебет' in h and 'покрыт' in h:
                col_map['debit_tenge'] = i
            elif 'кредит' in h and 'покрыт' in h:
                col_map['credit_tenge'] = i
            elif h == 'дебет':
                col_map['debit_amount'] = i
            elif h == 'кредит':
                col_map['credit_amount'] = i

        for row_idx in range(header_idx + 1, len(rows)):
            row = rows[row_idx]
            if not row or all(c is None for c in row):
                continue

            date_val = self._get(row, col_map.get('date'))
            if date_val is None:
                continue

            if isinstance(date_val, str) and any(w in date_val.lower() for w in ['итого', 'остаток', 'входящий']):
                continue

            debit = normalize_amount(self._get(row, col_map.get('debit_amount')))
            credit = normalize_amount(self._get(row, col_map.get('credit_amount')))
            debit_t = normalize_amount(self._get(row, col_map.get('debit_tenge')))
            credit_t = normalize_amount(self._get(row, col_map.get('credit_tenge')))

            direction = determine_direction(debit_amount=debit, credit_amount=credit)
            amount = credit or debit
            amount_tenge = credit_t or debit_t

            t = Transaction(
                transaction_date=normalize_date(date_val),
                amount=amount,
                currency=normalize_currency(currency),
                amount_tenge=amount_tenge,
                direction=direction,
                payer=None,
                payer_iin_bin=normalize_iin_bin(self._get(row, col_map.get('iin'))),
                payer_bank=None,
                payer_account=clean_string(self._get(row, col_map.get('corr_account'))),
                recipient=None,
                recipient_iin_bin=None,
                recipient_bank=None,
                recipient_account=None,
                operation_type=None,
                knp=None,
                payment_purpose=clean_string(self._get(row, col_map.get('description'))),
                document_number=None,
                statement_bank=self.BANK_NAME,
                account_number=account_number,
                source_file=file_info['filename'],
            )
            transactions.append(t)

        return transactions, {'account_number': account_number, 'warnings': warnings, 'errors': []}

    @staticmethod
    def _get(row, idx):
        if idx is None or idx >= len(row):
            return None
        return row[idx]
