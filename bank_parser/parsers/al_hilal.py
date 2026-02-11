"""Parser for АО Исламский Банк Al Hilal.

Two formats:
1. .xlsx: 6-col — Дата транзакции | Дата валют. | Детали транзакции | Кредит | Дебет | Баланс
2. .xls: 20-col — КОд | Отправитель (Счет) | Отправитель (РНН) | ... | Получатель | ... | Сумма | ...
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
class AlHilalParser(BaseParser):
    """Al Hilal 6-column .xlsx format."""
    BANK_NAME = 'АО Исламский Банк Al Hilal'

    @classmethod
    def can_parse(cls, sheet: SheetData, file_info: dict) -> float:
        for row in sheet.rows[:5]:
            for cell in row:
                if cell and ('HLALKZKZ' in str(cell) or 'Al Hilal' in str(cell)):
                    # Check if this is the simple 6-col format (few columns)
                    if sheet.num_cols <= 10:
                        return 0.95
                    return 0.5  # Let the full parser take priority
        folder = file_info.get('folder_name', '').lower()
        if 'al hilal' in folder and sheet.num_cols <= 10:
            return 0.8
        return 0.0

    def parse_sheet(self, sheet: SheetData, file_info: dict) -> Tuple[List[Transaction], dict]:
        rows = sheet.rows
        transactions = []
        account_number = None
        currency = None

        # Extract metadata
        for row in rows[:10]:
            for cell in row:
                if cell is None:
                    continue
                s = str(cell)
                match = re.search(r'(KZ\w{16,22})', s)
                if match:
                    account_number = match.group(1)
                if 'валюта:' in s.lower():
                    currency = s.split(':')[-1].strip()

        # Detect currency from sheet name or filename
        fn_lower = (file_info.get('filename', '') + ' ' + sheet.name).lower()
        if 'доллар' in fn_lower or 'usd' in fn_lower:
            currency = currency or 'USD'
        elif 'тенге' in fn_lower or 'kzt' in fn_lower:
            currency = currency or 'KZT'

        # Find header
        header_idx = None
        for i, row in enumerate(rows[:15]):
            row_text = ' '.join(str(c).lower() for c in row if c)
            if 'дата транзакции' in row_text or ('дата' in row_text and ('кредит' in row_text or 'дебет' in row_text)):
                header_idx = i
                break

        if header_idx is None:
            return [], {'warnings': [], 'errors': ['Header not found'], 'account_number': account_number}

        header = rows[header_idx]
        header_lower = [str(c).lower().strip() if c else '' for c in header]

        col_map = {}
        for i, h in enumerate(header_lower):
            if 'дата транзакции' in h or (h == 'дата' and 'date' not in col_map):
                col_map['date'] = i
            elif 'дата валют' in h:
                col_map['value_date'] = i
            elif 'детали' in h or 'описание' in h:
                col_map['details'] = i
            elif h == 'кредит':
                col_map['credit'] = i
            elif h == 'дебет':
                col_map['debit'] = i
            elif h == 'баланс':
                col_map['balance'] = i

        for row_idx in range(header_idx + 1, len(rows)):
            row = rows[row_idx]
            if not row or all(c is None for c in row):
                continue

            date_val = self._get(row, col_map.get('date'))
            if date_val is None:
                continue

            if isinstance(date_val, str) and any(w in date_val.lower() for w in ['итого', 'остаток', 'входящий']):
                continue

            credit = normalize_amount(self._get(row, col_map.get('credit')))
            debit = normalize_amount(self._get(row, col_map.get('debit')))
            direction = determine_direction(debit_amount=debit, credit_amount=credit)
            amount = credit or debit

            t = Transaction(
                transaction_date=normalize_date(date_val),
                amount=amount,
                currency=normalize_currency(currency),
                amount_tenge=amount if currency == 'KZT' else None,
                direction=direction,
                payer=None, payer_iin_bin=None, payer_bank=None, payer_account=None,
                recipient=None, recipient_iin_bin=None, recipient_bank=None, recipient_account=None,
                operation_type=None, knp=None,
                payment_purpose=clean_string(self._get(row, col_map.get('details'))),
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


@register_parser
class AlHilalFullParser(BaseParser):
    """Al Hilal 20-col .xls format (outgoing transfers)."""
    BANK_NAME = 'АО Исламский Банк Al Hilal'

    @classmethod
    def can_parse(cls, sheet: SheetData, file_info: dict) -> float:
        # Check for Al Hilal folder + many columns
        folder = file_info.get('folder_name', '').lower()
        if 'al hilal' not in folder:
            return 0.0
        # Header at row 0 or 1 with full column set
        for row in sheet.rows[:3]:
            row_text = ' '.join(str(c).lower() for c in row if c)
            if 'отправитель' in row_text and 'получатель' in row_text:
                return 0.96  # Higher than Tsesnabank's 0.95
            if 'код' in row_text and 'сумма' in row_text and sheet.num_cols >= 15:
                return 0.96
        if sheet.num_cols >= 15:
            return 0.85
        return 0.0

    def parse_sheet(self, sheet: SheetData, file_info: dict) -> Tuple[List[Transaction], dict]:
        rows = sheet.rows
        transactions = []

        # Detect currency from filename/sheetname
        fn_lower = (file_info.get('filename', '') + ' ' + sheet.name).lower()
        currency = 'KZT'
        if 'доллар' in fn_lower or 'usd' in fn_lower:
            currency = 'USD'

        # Detect direction from filename
        direction = None
        if 'входящ' in fn_lower:
            direction = 'Приход'
        elif 'исход' in fn_lower:
            direction = 'Расход'

        # Find header — check rows 0-5 for column names
        header_idx = None
        for i, row in enumerate(rows[:5]):
            row_text = ' '.join(str(c).lower() for c in row if c)
            if 'отправитель' in row_text or 'получатель' in row_text or 'сумма' in row_text:
                header_idx = i
                break

        # Could be 2-row header (row 0 = group headers, row 1 = column headers)
        if header_idx is not None and header_idx + 1 < len(rows):
            next_text = ' '.join(str(c).lower() for c in rows[header_idx + 1] if c)
            if 'счет' in next_text or 'рнн' in next_text or 'код' in next_text:
                header_idx = header_idx + 1

        if header_idx is None:
            # Fallback: try first row with data
            header_idx = 0

        header = rows[header_idx]
        header_lower = [str(c).lower().strip() if c else '' for c in header]

        col_map = {}
        for i, h in enumerate(header_lower):
            if 'код' in h and 'date' not in col_map:
                col_map['code'] = i
            elif 'счет' in h and 'отправитель' not in col_map.get('_context', ''):
                if 'payer_account' not in col_map:
                    col_map['payer_account'] = i
                elif 'recipient_account' not in col_map:
                    col_map['recipient_account'] = i
            elif 'рнн' in h or 'иин' in h:
                if 'payer_iin' not in col_map:
                    col_map['payer_iin'] = i
                elif 'recipient_iin' not in col_map:
                    col_map['recipient_iin'] = i
            elif 'отправитель' in h:
                col_map['payer'] = i
            elif 'получатель' in h:
                col_map['recipient'] = i
            elif 'сумма' in h:
                col_map['amount'] = i
            elif 'дата' in h:
                if 'date' not in col_map:
                    col_map['date'] = i
                elif 'value_date' not in col_map:
                    col_map['value_date'] = i
            elif 'кнп' in h:
                col_map['knp'] = i
            elif 'назначение' in h:
                col_map['purpose'] = i

        # Skip filter/summary rows (rows 2-3 may have date ranges etc.)
        data_start = header_idx + 1
        # Skip rows that look like filters or indices
        while data_start < len(rows) and data_start < header_idx + 4:
            row = rows[data_start]
            if row and any(c is not None for c in row):
                # Check if it's a numeric index row (all small numbers)
                vals = [c for c in row if c is not None]
                text = ' '.join(str(v) for v in vals)
                if all(str(v).strip().isdigit() and int(str(v).strip()) < 30 for v in vals if str(v).strip()):
                    data_start += 1
                    continue
            break

        for row_idx in range(data_start, len(rows)):
            row = rows[row_idx]
            if not row or all(c is None for c in row):
                continue

            date_val = self._get(row, col_map.get('date'))
            if date_val is None:
                # Try value_date
                date_val = self._get(row, col_map.get('value_date'))
            if date_val is None:
                continue

            if isinstance(date_val, str) and any(w in date_val.lower() for w in ['итого', 'всего']):
                continue

            amount = normalize_amount(self._get(row, col_map.get('amount')))

            t = Transaction(
                transaction_date=normalize_date(date_val),
                amount=amount,
                currency=normalize_currency(currency),
                amount_tenge=amount if currency == 'KZT' else None,
                direction=direction,
                payer=clean_string(self._get(row, col_map.get('payer'))),
                payer_iin_bin=normalize_iin_bin(self._get(row, col_map.get('payer_iin'))),
                payer_bank=None,
                payer_account=clean_string(self._get(row, col_map.get('payer_account'))),
                recipient=clean_string(self._get(row, col_map.get('recipient'))),
                recipient_iin_bin=normalize_iin_bin(self._get(row, col_map.get('recipient_iin'))),
                recipient_bank=None,
                recipient_account=clean_string(self._get(row, col_map.get('recipient_account'))),
                operation_type=None,
                knp=clean_string(self._get(row, col_map.get('knp'))),
                payment_purpose=clean_string(self._get(row, col_map.get('purpose'))),
                document_number=clean_string(self._get(row, col_map.get('code'))),
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
