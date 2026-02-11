"""Parser for Altyn Bank (ДБ China CITIC Bank).

Format: 17-column with explicit Направление (direction) column.
Row 0: title row
Row 1: header
Data starts at row 2.

Columns: Дата и время операции | Валюта | Направление | Сумма операции | Сумма в тенге |
         Плательщик | ИИН/БИН плательщика | Резидентство | Банк плательщика | Счёт плательщика |
         Получатель | ИИН/БИН получателя | Резидентство | Банк получателя | Счёт получателя |
         Код назначения платежа | Описание
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
class AltynBankParser(BaseParser):
    BANK_NAME = 'АО Altyn Bank'

    @classmethod
    def can_parse(cls, sheet: SheetData, file_info: dict) -> float:
        folder = file_info.get('folder_name', '').lower()
        if 'altyn bank' in folder:
            return 0.85

        for row in sheet.rows[:5]:
            for cell in row:
                if cell and 'altyn bank' in str(cell).lower():
                    return 0.85

        # Check for 17-col header with Направление
        for row in sheet.rows[:5]:
            row_text = ' '.join(str(c).lower() for c in row if c)
            if 'направление' in row_text and 'сумма операции' in row_text and 'описание' in row_text:
                return 0.8
        return 0.0

    def parse_sheet(self, sheet: SheetData, file_info: dict) -> Tuple[List[Transaction], dict]:
        rows = sheet.rows
        warnings = []
        transactions = []

        # Find header row
        header_idx = None
        for i, row in enumerate(rows[:10]):
            row_text = ' '.join(str(c).lower() for c in row if c)
            if 'дата и время операции' in row_text and 'направление' in row_text:
                header_idx = i
                break

        if header_idx is None:
            return [], {'warnings': [], 'errors': ['Header not found']}

        header = rows[header_idx]
        header_lower = [str(c).lower().strip() if c else '' for c in header]

        # Map columns
        col_map = {}
        for i, h in enumerate(header_lower):
            if 'дата и время' in h:
                col_map['date'] = i
            elif h == 'валюта':
                col_map['currency'] = i
            elif h == 'направление':
                col_map['direction'] = i
            elif 'сумма операции' in h:
                col_map['amount'] = i
            elif 'сумма в тенге' in h:
                col_map['amount_tenge'] = i
            elif 'плательщик' in h and ('наименование' in h or 'фио' in h):
                col_map['payer'] = i
            elif 'иин' in h and 'плательщик' in h:
                col_map['payer_iin'] = i
            elif 'банк плательщик' in h:
                col_map['payer_bank'] = i
            elif 'счет' in h and 'плательщик' in h:
                col_map['payer_account'] = i
            elif 'получател' in h and ('наименование' in h or 'фио' in h):
                col_map['recipient'] = i
            elif 'иин' in h and 'получател' in h:
                col_map['recipient_iin'] = i
            elif 'банк получател' in h:
                col_map['recipient_bank'] = i
            elif 'счет' in h and 'получател' in h:
                col_map['recipient_account'] = i
            elif 'код назначен' in h:
                col_map['knp'] = i
            elif 'описание' in h or 'назначение' in h:
                col_map['payment_purpose'] = i
            # Residency columns — just skip, they contain "Казахстан" etc
            elif 'резидентство' in h and 'плательщик' in h:
                col_map['payer_residency'] = i
            elif 'резидентство' in h and 'получател' in h:
                col_map['recipient_residency'] = i

        # Handle residency columns by position if header names don't contain party name
        if 'payer_residency' not in col_map and 'recipient_residency' not in col_map:
            res_indices = [i for i, h in enumerate(header_lower) if h == 'резидентство']
            # First residency after payer IIN = payer residency, second = recipient
            # Skip — not needed for output

        for row_idx in range(header_idx + 1, len(rows)):
            row = rows[row_idx]
            if not row or all(c is None for c in row):
                continue

            date_val = self._get(row, col_map.get('date'))
            if date_val is None:
                continue

            raw_direction = clean_string(self._get(row, col_map.get('direction')))
            direction = determine_direction(raw_direction=raw_direction)

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
                operation_type=None,
                knp=clean_string(self._get(row, col_map.get('knp'))),
                payment_purpose=clean_string(self._get(row, col_map.get('payment_purpose'))),
                document_number=None,
                statement_bank=self.BANK_NAME,
                account_number=None,
                source_file=file_info['filename'],
            )
            transactions.append(t)

        return transactions, {'account_number': None, 'warnings': warnings, 'errors': []}

    @staticmethod
    def _get(row, idx):
        if idx is None or idx >= len(row):
            return None
        return row[idx]
