"""Parser for Акционерное общество Delta Bank.

Format: 6 columns with direction indicated by sheet name.
Row 0: "Клиент, Маханов ..., ИИН..."
Row 1: "Входящие/Исходящие платежи"
Row 2: Header: № п/п | Наименование компании/ФИО | БИН/ИИН | Дата операции | Суммы | Назначение платежа
Multiple sheets per file (incoming, outgoing, by currency).
"""

import re
from typing import List, Tuple, Optional

from ..base_parser import BaseParser
from ..models import Transaction
from ..file_reader import SheetData
from ..normalizer import (
    normalize_date, normalize_iin_bin, normalize_amount, clean_string
)
from . import register_parser


@register_parser
class DeltaBankParser(BaseParser):
    BANK_NAME = 'АО Delta Bank'

    @classmethod
    def can_parse(cls, sheet: SheetData, file_info: dict) -> float:
        folder = file_info.get('folder_name', '').lower()
        if 'delta bank' in folder:
            return 0.9

        for row in sheet.rows[:5]:
            for cell in row:
                if cell and 'delta bank' in str(cell).lower():
                    return 0.85
            row_text = ' '.join(str(c).lower() for c in row if c)
            if '№ п/п' in row_text and 'наименование компании' in row_text:
                return 0.7
        return 0.0

    def parse(self, sheets, file_info):
        """Override to handle multiple sheets (incoming/outgoing)."""
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

        # Determine direction from sheet name or content
        direction = None
        sheet_lower = sheet.name.lower()
        if 'входящ' in sheet_lower:
            direction = 'Приход'
        elif 'исходящ' in sheet_lower:
            direction = 'Расход'

        # Also check first rows for direction
        for row in rows[:5]:
            for cell in row:
                if cell:
                    s = str(cell).lower()
                    if 'входящие' in s:
                        direction = 'Приход'
                    elif 'исходящие' in s:
                        direction = 'Расход'

        # Extract client info from row 0
        client_iin = None
        for row in rows[:3]:
            for cell in row:
                if cell and 'ИИН' in str(cell):
                    match = re.search(r'ИИН\s*(\d{12})', str(cell))
                    if match:
                        client_iin = match.group(1)

        # Detect currency from sheet name
        currency = 'KZT'
        if 'валюта' in sheet_lower or 'usd' in sheet_lower:
            currency = 'USD'
        elif 'eur' in sheet_lower:
            currency = 'EUR'

        # Find header
        header_idx = None
        for i, row in enumerate(rows[:10]):
            row_text = ' '.join(str(c).lower() for c in row if c)
            if '№' in row_text and ('наименование' in row_text or 'дата' in row_text):
                header_idx = i
                break

        if header_idx is None:
            return [], {'warnings': [], 'errors': [], 'account_number': None}

        header = rows[header_idx]
        header_lower = [str(c).lower().strip() if c else '' for c in header]

        col_map = {}
        for i, h in enumerate(header_lower):
            if 'наименование' in h or 'фио' in h:
                col_map['name'] = i
            elif 'бин' in h or 'иин' in h:
                col_map['iin'] = i
            elif 'дата' in h:
                col_map['date'] = i
            elif 'сумм' in h:
                col_map['amount'] = i
            elif 'назначение' in h:
                col_map['purpose'] = i

        for row_idx in range(header_idx + 1, len(rows)):
            row = rows[row_idx]
            if not row or all(c is None for c in row):
                continue

            date_val = self._get(row, col_map.get('date'))
            if date_val is None:
                continue

            amount = normalize_amount(self._get(row, col_map.get('amount')))
            if amount is None:
                continue

            counterparty = clean_string(self._get(row, col_map.get('name')))
            counterparty_iin = normalize_iin_bin(self._get(row, col_map.get('iin')))

            t = Transaction(
                transaction_date=normalize_date(date_val),
                amount=amount,
                currency=currency,
                amount_tenge=amount if currency == 'KZT' else None,
                direction=direction,
                payer=counterparty if direction == 'Приход' else None,
                payer_iin_bin=counterparty_iin if direction == 'Приход' else client_iin,
                payer_bank=None, payer_account=None,
                recipient=counterparty if direction == 'Расход' else None,
                recipient_iin_bin=counterparty_iin if direction == 'Расход' else client_iin,
                recipient_bank=None, recipient_account=None,
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
