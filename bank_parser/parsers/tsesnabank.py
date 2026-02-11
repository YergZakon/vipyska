"""Parser for АО Цеснабанк.

Format: Metadata header + 10-column table. Separate sheets for debit/credit.
Sheet names: "дебет", "кредит"
Row 0-5: metadata (date, bank name ЦЕСНАБАНК, SWIFT, account info)
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
class TsesnabankParser(BaseParser):
    BANK_NAME = 'АО Цеснабанк'

    @classmethod
    def can_parse(cls, sheet: SheetData, file_info: dict) -> float:
        for row in sheet.rows[:10]:
            for cell in row:
                if cell and 'ЦЕСНАБАНК' in str(cell).upper():
                    return 0.95
                if cell and 'TSESKZKA' in str(cell).upper():
                    return 0.95
        folder = file_info.get('folder_name', '').lower()
        if 'цеснабанк' in folder:
            return 0.8
        return 0.0

    def parse(self, sheets, file_info):
        """Override to handle separate debit/credit sheets."""
        from ..models import ParseResult
        result = ParseResult(
            filepath=file_info['filepath'],
            source_file=file_info['filename'],
            bank_detected=self.BANK_NAME,
            parser_used=self.__class__.__name__,
        )

        all_transactions = []
        for sheet in sheets:
            txns, meta = self.parse_sheet(sheet, file_info)
            all_transactions.extend(txns)
            if meta.get('account_number'):
                result.account_number = meta['account_number']

        result.transactions = all_transactions
        result.total_transactions = len(all_transactions)
        result.parse_status = 'success' if all_transactions else 'failed'
        return result

    def parse_sheet(self, sheet: SheetData, file_info: dict) -> Tuple[List[Transaction], dict]:
        rows = sheet.rows
        transactions = []
        account_number = None

        # Determine direction from sheet name
        direction = None
        sn = sheet.name.lower()
        if 'кредит' in sn:
            direction = 'Приход'
        elif 'дебет' in sn:
            direction = 'Расход'

        # Extract metadata
        for row in rows[:15]:
            for cell in row:
                if cell:
                    s = str(cell)
                    match = re.search(r'(KZ\w{16,22})', s)
                    if match:
                        account_number = match.group(1)

        # Find header
        header_idx = None
        for i, row in enumerate(rows[:20]):
            row_text = ' '.join(str(c).lower() for c in row if c)
            if ('дата' in row_text and ('сумма' in row_text or 'назначение' in row_text)):
                header_idx = i
                break
            if 'контрагент' in row_text or 'корреспондент' in row_text:
                header_idx = i
                break

        if header_idx is None:
            return [], {'warnings': [], 'errors': ['Header not found'], 'account_number': account_number}

        header = rows[header_idx]
        header_lower = [str(c).lower().strip() if c else '' for c in header]

        col_map = {}
        for i, h in enumerate(header_lower):
            if 'дата' in h and 'операц' in h:
                col_map['date'] = i
            elif h == 'дата':
                col_map['date'] = i
            elif 'сумма' in h:
                col_map['amount'] = i
            elif 'валюта' in h:
                col_map['currency'] = i
            elif 'контрагент' in h or 'корреспондент' in h or 'наименование' in h:
                col_map['counterparty'] = i
            elif 'иин' in h or 'бин' in h:
                col_map['iin'] = i
            elif 'назначение' in h:
                col_map['purpose'] = i
            elif 'счет' in h and 'корресп' in h:
                col_map['corr_account'] = i

        for row_idx in range(header_idx + 1, len(rows)):
            row = rows[row_idx]
            if not row or all(c is None for c in row):
                continue

            date_val = self._get(row, col_map.get('date'))
            if date_val is None:
                continue

            if isinstance(date_val, str) and any(w in date_val.lower() for w in ['итого', 'остаток']):
                continue

            counterparty = clean_string(self._get(row, col_map.get('counterparty')))

            t = Transaction(
                transaction_date=normalize_date(date_val),
                amount=normalize_amount(self._get(row, col_map.get('amount'))),
                currency=normalize_currency(self._get(row, col_map.get('currency'))) or 'KZT',
                amount_tenge=normalize_amount(self._get(row, col_map.get('amount'))),
                direction=direction,
                payer=counterparty if direction == 'Приход' else None,
                payer_iin_bin=normalize_iin_bin(self._get(row, col_map.get('iin'))) if direction == 'Приход' else None,
                payer_bank=None, payer_account=None,
                recipient=counterparty if direction == 'Расход' else None,
                recipient_iin_bin=normalize_iin_bin(self._get(row, col_map.get('iin'))) if direction == 'Расход' else None,
                recipient_bank=None, recipient_account=None,
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
