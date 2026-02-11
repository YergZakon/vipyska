"""Parser for АО Евразийский Банк.

Two formats:
1. Card operations (7-col): ИИН | Тип операции | Номер счета | Дата | Сумма | Валюта | Детали операции
2. Full statement (15-col): Дата проводки | Вид операции | Номер документа | Наименование |
   ИИН/БИН | ИИК | Наименование банка | БИК | Назначение | Дебет | Кредит | Остаток
   (metadata header with EURIKZKA)
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
class EurasianCardParser(BaseParser):
    BANK_NAME = 'АО Евразийский Банк'

    @classmethod
    def can_parse(cls, sheet: SheetData, file_info: dict) -> float:
        for row in sheet.rows[:5]:
            row_text = ' '.join(str(c).lower() for c in row if c)
            if 'тип операции' in row_text and 'детали операции' in row_text:
                return 0.9
        folder = file_info.get('folder_name', '').lower()
        if 'евразийский' in folder:
            for row in sheet.rows[:5]:
                if len([c for c in row if c]) == 7:
                    return 0.7
        return 0.0

    def parse_sheet(self, sheet: SheetData, file_info: dict) -> Tuple[List[Transaction], dict]:
        rows = sheet.rows
        transactions = []

        header_idx = None
        for i, row in enumerate(rows[:5]):
            row_text = ' '.join(str(c).lower() for c in row if c)
            if 'иин' in row_text and 'тип операции' in row_text:
                header_idx = i
                break

        if header_idx is None:
            return [], {'warnings': [], 'errors': ['Header not found'], 'account_number': None}

        header = rows[header_idx]
        header_lower = [str(c).lower().strip() if c else '' for c in header]

        col_map = {}
        for i, h in enumerate(header_lower):
            if h == 'иин':
                col_map['iin'] = i
            elif 'тип операции' in h:
                col_map['type'] = i
            elif 'номер счета' in h:
                col_map['account'] = i
            elif h == 'дата':
                col_map['date'] = i
            elif h == 'сумма':
                col_map['amount'] = i
            elif h == 'валюта':
                col_map['currency'] = i
            elif 'детали' in h:
                col_map['details'] = i

        account = None
        for row_idx in range(header_idx + 1, len(rows)):
            row = rows[row_idx]
            if not row or all(c is None for c in row):
                continue

            date_val = self._get(row, col_map.get('date'))
            if date_val is None:
                continue

            op_type = clean_string(self._get(row, col_map.get('type')))
            direction = determine_direction(operation_type=op_type)

            acct = clean_string(self._get(row, col_map.get('account')))
            if acct and not account:
                account = acct

            currency = normalize_currency(self._get(row, col_map.get('currency')))
            amount = normalize_amount(self._get(row, col_map.get('amount')))
            amount_tenge = amount if currency == 'KZT' else None

            t = Transaction(
                transaction_date=normalize_date(date_val),
                amount=amount,
                currency=currency,
                amount_tenge=amount_tenge,
                direction=direction,
                payer=None,
                payer_iin_bin=normalize_iin_bin(self._get(row, col_map.get('iin'))),
                payer_bank=self.BANK_NAME,
                payer_account=acct,
                recipient=None, recipient_iin_bin=None, recipient_bank=None, recipient_account=None,
                operation_type=op_type, knp=None,
                payment_purpose=clean_string(self._get(row, col_map.get('details'))),
                document_number=None,
                statement_bank=self.BANK_NAME,
                account_number=account,
                source_file=file_info['filename'],
            )
            transactions.append(t)

        return transactions, {'account_number': account, 'warnings': [], 'errors': []}

    @staticmethod
    def _get(row, idx):
        if idx is None or idx >= len(row):
            return None
        return row[idx]


@register_parser
class EurasianStatementParser(BaseParser):
    """Eurasian Bank full statement format (15-col with metadata header)."""
    BANK_NAME = 'АО Евразийский Банк'

    @classmethod
    def can_parse(cls, sheet: SheetData, file_info: dict) -> float:
        for row in sheet.rows[:10]:
            for cell in row:
                if cell and 'EURIKZKA' in str(cell):
                    return 0.95
        # Check for "Дата проводки" header deeper in file
        for row in sheet.rows[:25]:
            row_text = ' '.join(str(c).lower() for c in row if c)
            if 'дата проводки' in row_text and 'вид операции' in row_text:
                return 0.9
        folder = file_info.get('folder_name', '').lower()
        if 'евразийский' in folder:
            # Check for metadata pattern
            for row in sheet.rows[:10]:
                for cell in row:
                    if cell and 'отделение' in str(cell).lower():
                        return 0.6
        return 0.0

    def parse_sheet(self, sheet: SheetData, file_info: dict) -> Tuple[List[Transaction], dict]:
        import re
        rows = sheet.rows
        transactions = []
        account_number = None

        # Extract metadata
        for row in rows[:20]:
            for cell in row:
                if cell:
                    s = str(cell)
                    match = re.search(r'(KZ\w{16,22})', s)
                    if match:
                        account_number = match.group(1)

        # Find header — can be deep (row 16+)
        header_idx = None
        for i, row in enumerate(rows[:30]):
            row_text = ' '.join(str(c).lower() for c in row if c)
            if 'дата проводки' in row_text and 'вид операции' in row_text:
                header_idx = i
                break

        if header_idx is None:
            return [], {'warnings': [], 'errors': ['Header not found'], 'account_number': account_number}

        header = rows[header_idx]
        header_lower = [str(c).lower().strip() if c else '' for c in header]

        col_map = {}
        for i, h in enumerate(header_lower):
            if 'дата проводки' in h:
                col_map['date'] = i
            elif 'вид операции' in h:
                col_map['type'] = i
            elif 'номер документа' in h:
                col_map['doc_number'] = i
            elif 'наименование' in h and 'банк' not in h:
                col_map['counterparty'] = i
            elif 'иин' in h or 'бин' in h:
                col_map['iin'] = i
            elif 'иик' in h:
                col_map['account'] = i
            elif 'наименование банка' in h:
                col_map['bank'] = i
            elif 'назначение' in h:
                col_map['purpose'] = i
            elif 'дебет' in h:
                col_map['debit'] = i
            elif 'кредит' in h:
                col_map['credit'] = i

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

            op_type = clean_string(self._get(row, col_map.get('type')))
            if not direction and op_type:
                direction = determine_direction(operation_type=op_type)

            t = Transaction(
                transaction_date=normalize_date(date_val),
                amount=amount,
                currency='KZT',
                amount_tenge=amount,
                direction=direction,
                payer=clean_string(self._get(row, col_map.get('counterparty'))) if direction == 'Приход' else None,
                payer_iin_bin=normalize_iin_bin(self._get(row, col_map.get('iin'))) if direction == 'Приход' else None,
                payer_bank=clean_string(self._get(row, col_map.get('bank'))) if direction == 'Приход' else None,
                payer_account=clean_string(self._get(row, col_map.get('account'))) if direction == 'Приход' else None,
                recipient=clean_string(self._get(row, col_map.get('counterparty'))) if direction == 'Расход' else None,
                recipient_iin_bin=normalize_iin_bin(self._get(row, col_map.get('iin'))) if direction == 'Расход' else None,
                recipient_bank=clean_string(self._get(row, col_map.get('bank'))) if direction == 'Расход' else None,
                recipient_account=clean_string(self._get(row, col_map.get('account'))) if direction == 'Расход' else None,
                operation_type=op_type,
                knp=None,
                payment_purpose=clean_string(self._get(row, col_map.get('purpose'))),
                document_number=clean_string(self._get(row, col_map.get('doc_number'))),
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
