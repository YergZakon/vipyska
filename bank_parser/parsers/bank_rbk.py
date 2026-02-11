"""Parser for АО Bank RBK.

Two formats:
1. Card transactions: 14 English columns (POSTING_DATE, TRANS_AMOUNT, etc.)
2. Simple format: 8 columns (Дата, ИИН, Клиент, Номер карты, Сумма, etc.)
"""

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
class BankRBKCardParser(BaseParser):
    """Bank RBK card transaction format (English headers)."""
    BANK_NAME = 'АО Bank RBK'

    @classmethod
    def can_parse(cls, sheet: SheetData, file_info: dict) -> float:
        for row in sheet.rows[:3]:
            row_text = ' '.join(str(c).lower() for c in row if c)
            if 'posting_date' in row_text and 'trans_amount' in row_text:
                return 0.95
        folder = file_info.get('folder_name', '').lower()
        if 'bank rbk' in folder or 'банк рбк' in folder:
            for row in sheet.rows[:3]:
                row_text = ' '.join(str(c) for c in row if c)
                if 'POSTING_DATE' in row_text:
                    return 0.9
        return 0.0

    def parse_sheet(self, sheet: SheetData, file_info: dict) -> Tuple[List[Transaction], dict]:
        rows = sheet.rows
        warnings = []
        transactions = []

        # Header is row 0
        header_idx = 0
        for i, row in enumerate(rows[:3]):
            if any(c and 'POSTING_DATE' in str(c) for c in row):
                header_idx = i
                break

        header = rows[header_idx]
        header_upper = [str(c).upper().strip() if c else '' for c in header]

        col_map = {}
        for i, h in enumerate(header_upper):
            if h == 'POSTING_DATE':
                col_map['date'] = i
            elif h == 'TRANS_AMOUNT':
                col_map['amount'] = i
            elif h == 'FEE_AMOUNT':
                col_map['fee'] = i
            elif h == 'TRANS_CURR':
                col_map['currency'] = i
            elif h == 'TRANS_TYPE':
                col_map['type'] = i
            elif h == 'ADDITIONAL_DESC':
                col_map['description'] = i
            elif h == 'AUTH_CODE':
                col_map['auth_code'] = i
            elif h == 'RET_REF_NUMBER':
                col_map['ref'] = i
            elif h == 'CPID':
                col_map['cpid'] = i
            elif h == 'TRANS_DATE':
                col_map['trans_date'] = i
            elif h == 'CONTRACT_FOR':
                col_map['card'] = i
            elif h == 'CLIENT':
                col_map['client'] = i
            elif h == 'ITN':
                col_map['itn'] = i

        account = sheet.name if sheet.name.startswith('KZ') else None

        for row_idx in range(header_idx + 1, len(rows)):
            row = rows[row_idx]
            if not row or all(c is None for c in row):
                continue

            date_val = self._get(row, col_map.get('date'))
            if date_val is None:
                continue

            raw_amount = normalize_amount(self._get(row, col_map.get('amount')))
            direction = None
            amount = None
            if raw_amount is not None:
                direction = 'Расход' if raw_amount < 0 else 'Приход'
                amount = abs(raw_amount)

            currency = normalize_currency(self._get(row, col_map.get('currency')))
            amount_tenge = amount if currency == 'KZT' else None

            t = Transaction(
                transaction_date=normalize_date(date_val),
                amount=amount,
                currency=currency,
                amount_tenge=amount_tenge,
                direction=direction,
                payer=clean_string(self._get(row, col_map.get('client'))),
                payer_iin_bin=normalize_iin_bin(self._get(row, col_map.get('itn'))),
                payer_bank=self.BANK_NAME,
                payer_account=account,
                recipient=clean_string(self._get(row, col_map.get('cpid'))),
                recipient_iin_bin=None,
                recipient_bank=None,
                recipient_account=None,
                operation_type=clean_string(self._get(row, col_map.get('type'))),
                knp=None,
                payment_purpose=clean_string(self._get(row, col_map.get('description'))),
                document_number=clean_string(self._get(row, col_map.get('ref'))),
                statement_bank=self.BANK_NAME,
                account_number=account,
                source_file=file_info['filename'],
            )
            transactions.append(t)

        return transactions, {'account_number': account, 'warnings': warnings, 'errors': []}

    @staticmethod
    def _get(row, idx):
        if idx is None or idx >= len(row):
            return None
        return row[idx]


@register_parser
class BankRBKSimpleParser(BaseParser):
    """Bank RBK simple 8-column format."""
    BANK_NAME = 'АО Bank RBK'

    @classmethod
    def can_parse(cls, sheet: SheetData, file_info: dict) -> float:
        for row in sheet.rows[:5]:
            row_text = ' '.join(str(c).lower() for c in row if c)
            if 'номер карты' in row_text and 'назначение платежа' in row_text:
                folder = file_info.get('folder_name', '').lower()
                if 'rbk' in folder:
                    return 0.85
        return 0.0

    def parse_sheet(self, sheet: SheetData, file_info: dict) -> Tuple[List[Transaction], dict]:
        rows = sheet.rows
        transactions = []

        header_idx = None
        for i, row in enumerate(rows[:10]):
            row_text = ' '.join(str(c).lower() for c in row if c)
            if 'дата' in row_text and 'сумма' in row_text:
                header_idx = i
                break

        if header_idx is None:
            return [], {'warnings': [], 'errors': ['Header not found'], 'account_number': None}

        header = rows[header_idx]
        header_lower = [str(c).lower().strip() if c else '' for c in header]

        col_map = {}
        for i, h in enumerate(header_lower):
            if 'дата' in h:
                col_map['date'] = i
            elif 'иин' in h:
                col_map['iin'] = i
            elif 'клиент' in h:
                col_map['client'] = i
            elif 'номер карт' in h:
                col_map['card'] = i
            elif 'сумма в валюте' in h:
                col_map['amount'] = i
            elif 'сумма в тенге' in h:
                col_map['amount_tenge'] = i
            elif 'валюта' in h:
                col_map['currency'] = i
            elif 'назначение' in h:
                col_map['purpose'] = i

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
                amount_tenge=normalize_amount(self._get(row, col_map.get('amount_tenge'))),
                direction=None,
                payer=clean_string(self._get(row, col_map.get('client'))),
                payer_iin_bin=normalize_iin_bin(self._get(row, col_map.get('iin'))),
                payer_bank=self.BANK_NAME,
                payer_account=None,
                recipient=None, recipient_iin_bin=None, recipient_bank=None, recipient_account=None,
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
