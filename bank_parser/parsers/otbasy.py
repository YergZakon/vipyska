"""Parser for АО Отбасы банк.

Format: Metadata header (bank name, SWIFT, BIN, client info) + standard 18-col table.
Row 0: "Наименование Банка: АО \"Жилищный строительный сберегательный банк \"Отбасы банк\""
Row 1: "SWIFT Банка: HCSKKZKA"
...metadata rows...
Then 18-col header similar to standard format.
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
class OtbasyParser(BaseParser):
    BANK_NAME = 'АО Отбасы банк'

    @classmethod
    def can_parse(cls, sheet: SheetData, file_info: dict) -> float:
        for row in sheet.rows[:5]:
            for cell in row:
                if cell and ('HCSKKZKA' in str(cell) or 'Отбасы' in str(cell) or
                             'Жилищный строительный' in str(cell)):
                    return 0.95
        folder = file_info.get('folder_name', '').lower()
        if 'отбасы' in folder:
            return 0.8
        return 0.0

    def parse_sheet(self, sheet: SheetData, file_info: dict) -> Tuple[List[Transaction], dict]:
        rows = sheet.rows
        warnings = []
        transactions = []
        account_number = None

        # Extract metadata
        for row in rows[:15]:
            for cell in row:
                if cell is None:
                    continue
                s = str(cell)
                match = re.search(r'(KZ\w{16,22})', s)
                if match:
                    account_number = match.group(1)

        # Find header row (standard 18-col markers)
        header_idx = self.find_header_row(rows, [
            'дата и время операции', 'валюта', 'сумма', 'плательщик'
        ])

        if header_idx is None:
            return [], {'warnings': warnings, 'errors': ['Header not found'], 'account_number': account_number}

        header = rows[header_idx]
        header_lower = [str(c).lower().strip() if c else '' for c in header]

        col_map = {}
        for i, h in enumerate(header_lower):
            if 'дата и время' in h:
                col_map['date'] = i
            elif 'валюта операции' in h or (h == 'валюта' and 'date' in col_map):
                col_map['currency'] = i
            elif 'виды операции' in h or 'вид операции' in h:
                col_map['operation_type'] = i
            elif 'сдп' in h:
                col_map['sdp'] = i
            elif 'сумма в валюте' in h or h == 'сумма (вал.)':
                col_map['amount'] = i
            elif 'сумма в тенге' in h:
                col_map['amount_tenge'] = i
            elif 'плательщик' in h and ('наименование' in h or 'фио' in h or 'банк' not in h):
                if 'банк' in h:
                    col_map['payer_bank'] = i
                elif 'иин' in h or 'бин' in h:
                    col_map['payer_iin'] = i
                elif 'счет' in h or 'счёт' in h:
                    col_map['payer_account'] = i
                else:
                    col_map['payer'] = i
            elif 'иин' in h and 'плательщик' in h:
                col_map['payer_iin'] = i
            elif 'банк' in h and 'плательщик' in h:
                col_map['payer_bank'] = i
            elif 'счет' in h and 'плательщик' in h:
                col_map['payer_account'] = i
            elif 'получател' in h and ('наименование' in h or 'фио' in h or 'банк' not in h):
                if 'банк' in h:
                    col_map['recipient_bank'] = i
                elif 'иин' in h or 'бин' in h:
                    col_map['recipient_iin'] = i
                elif 'счет' in h or 'счёт' in h:
                    col_map['recipient_account'] = i
                else:
                    col_map['recipient'] = i
            elif 'иин' in h and 'получател' in h:
                col_map['recipient_iin'] = i
            elif 'банк' in h and 'получател' in h:
                col_map['recipient_bank'] = i
            elif 'счет' in h and 'получател' in h:
                col_map['recipient_account'] = i
            elif 'код назначен' in h:
                col_map['knp'] = i
            elif 'назначение платежа' in h:
                col_map['payment_purpose'] = i

        # Data starts after header (skip number row if present)
        data_start = header_idx + 1
        if data_start < len(rows):
            nr = rows[data_start]
            non_none = [c for c in nr if c is not None]
            if non_none and all(isinstance(c, (int, float)) for c in non_none):
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

            op_type = clean_string(self._get(row, col_map.get('operation_type')))
            direction = None
            if op_type:
                op_lower = op_type.lower()
                if 'входящ' in op_lower or 'зачислен' in op_lower or 'пополнен' in op_lower:
                    direction = 'Приход'
                elif 'исходящ' in op_lower or 'списан' in op_lower or 'снят' in op_lower:
                    direction = 'Расход'

            t = Transaction(
                transaction_date=normalize_date(date_val),
                amount=normalize_amount(self._get(row, col_map.get('amount'))),
                currency=normalize_currency(self._get(row, col_map.get('currency'))),
                amount_tenge=normalize_amount(self._get(row, col_map.get('amount_tenge'))),
                direction=direction,
                payer=clean_string(self._get(row, col_map.get('payer'))),
                payer_iin_bin=normalize_iin_bin(self._get(row, col_map.get('payer_iin'))),
                payer_bank=clean_string(self._get(row, col_map.get('payer_bank'))),
                payer_account=clean_string(self._get(row, col_map.get('payer_account'))),
                recipient=clean_string(self._get(row, col_map.get('recipient'))),
                recipient_iin_bin=normalize_iin_bin(self._get(row, col_map.get('recipient_iin'))),
                recipient_bank=clean_string(self._get(row, col_map.get('recipient_bank'))),
                recipient_account=clean_string(self._get(row, col_map.get('recipient_account'))),
                operation_type=op_type,
                knp=clean_string(self._get(row, col_map.get('knp'))),
                payment_purpose=clean_string(self._get(row, col_map.get('payment_purpose'))),
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
