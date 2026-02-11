"""Parser for Kaspi Bank.

Multiple formats:
1. Statement format — metadata header + 2-row merged header + transaction data
2. Statistics format — "Статистика по успешным операциям", merchant data
3. Terminal/partner list — just BIN+Terminal_id (skip)
"""

import re
from typing import List, Tuple, Optional
from datetime import datetime

from ..base_parser import BaseParser
from ..models import Transaction
from ..file_reader import SheetData
from ..normalizer import (
    normalize_date, normalize_iin_bin, normalize_amount,
    normalize_currency, determine_direction, clean_string
)
from . import register_parser


@register_parser
class KaspiStatementParser(BaseParser):
    """Kaspi Bank statement format with metadata header."""

    BANK_NAME = 'АО Kaspi Bank'

    @classmethod
    def can_parse(cls, sheet: SheetData, file_info: dict) -> float:
        folder = file_info.get('folder_name', '').lower()

        # Check for Kaspi-specific metadata
        for row in sheet.rows[:15]:
            for cell in row:
                if cell is None:
                    continue
                s = str(cell)
                if 'Kaspi Bank' in s or 'КАСПИ' in s.upper():
                    # Check it's a statement (not statistics or partner list)
                    for row2 in sheet.rows[:20]:
                        for cell2 in row2:
                            if cell2 and 'входящий остаток' in str(cell2).lower():
                                return 0.95
                            if cell2 and 'плательщик' in str(cell2).lower():
                                return 0.9

        if 'kaspi' in folder:
            # Check if it has statement structure
            for row in sheet.rows[:20]:
                row_text = ' '.join(str(c).lower() for c in row if c)
                if 'плательщик' in row_text and 'получател' in row_text:
                    return 0.85
                if 'входящий остаток' in row_text:
                    return 0.85

        return 0.0

    def parse_sheet(self, sheet: SheetData, file_info: dict) -> Tuple[List[Transaction], dict]:
        rows = sheet.rows
        warnings = []
        transactions = []
        account_number = None

        # Extract metadata from first rows
        for row in rows[:15]:
            for cell in row:
                if cell is None:
                    continue
                s = str(cell)
                # Look for account number
                match = re.search(r'(KZ\w{16,20})', s)
                if match:
                    account_number = match.group(1)

        # Find header row — look for row with "Плательщик" or "Дата"
        header_idx = None
        for i, row in enumerate(rows[:25]):
            row_text = ' '.join(str(c).lower() for c in row if c)
            if ('дата' in row_text and ('плательщик' in row_text or 'получател' in row_text)):
                header_idx = i
                break
            if 'дата' in row_text and 'валюта' in row_text and 'сумма' in row_text:
                header_idx = i
                break

        if header_idx is None:
            return [], {'warnings': warnings, 'errors': ['Header not found'], 'account_number': account_number}

        header = rows[header_idx]
        # Check for 2-row header (sub-columns)
        sub_header = rows[header_idx + 1] if header_idx + 1 < len(rows) else None

        # Build column map based on actual header content
        header_lower = [str(c).lower().strip() if c else '' for c in header]
        col_map = {}

        for i, h in enumerate(header_lower):
            if 'дата' in h and 'операц' in h:
                col_map['date'] = i
            elif h == 'дата' or 'дата опер' in h:
                col_map['date'] = i
            elif 'валюта' in h:
                col_map['currency'] = i
            elif 'сумма' in h and 'тенге' not in h and 'нб' not in h:
                col_map['amount'] = i
            elif 'сумма' in h and ('тенге' in h or 'нб' in h):
                col_map['amount_tenge'] = i
            elif 'направлен' in h:
                col_map['direction'] = i

        # For Kaspi, sub-header might define Плательщик/Получатель sub-columns
        if sub_header:
            sub_lower = [str(c).lower().strip() if c else '' for c in sub_header]
            # Map sub-header columns
            for i, h in enumerate(sub_lower):
                if 'наименование' in h and i < len(header_lower):
                    parent = header_lower[i] if header_lower[i] else ''
                    # Inherit from parent merged cell - check leftward
                    for j in range(i, -1, -1):
                        if header_lower[j]:
                            parent = header_lower[j]
                            break
                    if 'плательщик' in parent:
                        col_map['payer'] = i
                    elif 'получател' in parent:
                        col_map['recipient'] = i
                elif 'иин' in h or 'бин' in h:
                    # Check parent for context
                    parent = ''
                    for j in range(i, -1, -1):
                        if header_lower[j]:
                            parent = header_lower[j]
                            break
                    if 'плательщик' in parent:
                        col_map['payer_iin'] = i
                    elif 'получател' in parent:
                        col_map['recipient_iin'] = i
                elif 'назначение' in h:
                    col_map['payment_purpose'] = i
                elif 'кнп' in h or 'код' in h:
                    col_map['knp'] = i

        # Determine data start (skip header + sub-header)
        data_start = header_idx + 1
        if sub_header and any(c for c in sub_header if c is not None):
            data_start = header_idx + 2

        for row_idx in range(data_start, len(rows)):
            row = rows[row_idx]
            if not row or all(c is None for c in row):
                continue

            date_val = self._get(row, col_map.get('date'))
            if date_val is None:
                continue

            # Skip summary rows
            if isinstance(date_val, str):
                d_lower = date_val.lower()
                if any(w in d_lower for w in ['итого', 'остаток', 'входящий', 'исходящий']):
                    continue

            raw_dir = clean_string(self._get(row, col_map.get('direction')))
            direction = determine_direction(raw_direction=raw_dir) if raw_dir else None

            t = Transaction(
                transaction_date=normalize_date(date_val),
                amount=normalize_amount(self._get(row, col_map.get('amount'))),
                currency=normalize_currency(self._get(row, col_map.get('currency'))),
                amount_tenge=normalize_amount(self._get(row, col_map.get('amount_tenge'))),
                direction=direction,
                payer=clean_string(self._get(row, col_map.get('payer'))),
                payer_iin_bin=normalize_iin_bin(self._get(row, col_map.get('payer_iin'))),
                payer_bank=None,
                payer_account=None,
                recipient=clean_string(self._get(row, col_map.get('recipient'))),
                recipient_iin_bin=normalize_iin_bin(self._get(row, col_map.get('recipient_iin'))),
                recipient_bank=None,
                recipient_account=None,
                operation_type=None,
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


@register_parser
class KaspiStatisticsParser(BaseParser):
    """Kaspi Bank statistics format (merchant/terminal data)."""

    BANK_NAME = 'АО Kaspi Bank'

    @classmethod
    def can_parse(cls, sheet: SheetData, file_info: dict) -> float:
        for row in sheet.rows[:10]:
            for cell in row:
                if cell and 'статистика' in str(cell).lower():
                    return 0.9
                if cell and 'терминал_id' in str(cell).lower():
                    return 0.3  # Partner list, low priority
        return 0.0

    def parse_sheet(self, sheet: SheetData, file_info: dict) -> Tuple[List[Transaction], dict]:
        """Statistics files often don't have standard transaction format — skip or parse minimally."""
        rows = sheet.rows
        warnings = ['Kaspi statistics format — limited field mapping']
        transactions = []

        # Find header
        header_idx = None
        for i, row in enumerate(rows[:10]):
            row_text = ' '.join(str(c).lower() for c in row if c)
            if 'дата' in row_text and ('сумма' in row_text or 'бин' in row_text):
                header_idx = i
                break

        if header_idx is None:
            return [], {'warnings': warnings, 'errors': ['No parseable header'], 'account_number': None}

        header = rows[header_idx]
        header_lower = [str(c).lower().strip() if c else '' for c in header]

        col_map = {}
        for i, h in enumerate(header_lower):
            if 'дата' in h:
                col_map['date'] = i
            elif 'сумма' in h:
                col_map['amount'] = i
            elif 'бин' in h and 'банк' not in h and 'эквайер' not in h:
                col_map['bin'] = i
            elif 'наименование' in h:
                col_map['name'] = i
            elif 'тип операции' in h:
                col_map['type'] = i
            elif 'валюта' in h:
                col_map['currency'] = i

        for row_idx in range(header_idx + 1, len(rows)):
            row = rows[row_idx]
            if not row or all(c is None for c in row):
                continue
            date_val = self._get(row, col_map.get('date'))
            if date_val is None:
                continue

            op_type = clean_string(self._get(row, col_map.get('type')))
            direction = determine_direction(raw_direction=op_type)

            t = Transaction(
                transaction_date=normalize_date(date_val),
                amount=normalize_amount(self._get(row, col_map.get('amount'))),
                currency=normalize_currency(self._get(row, col_map.get('currency'))) or 'KZT',
                amount_tenge=normalize_amount(self._get(row, col_map.get('amount'))),
                direction=direction,
                payer=None,
                payer_iin_bin=None,
                payer_bank=None,
                payer_account=None,
                recipient=clean_string(self._get(row, col_map.get('name'))),
                recipient_iin_bin=normalize_iin_bin(self._get(row, col_map.get('bin'))),
                recipient_bank=None,
                recipient_account=None,
                operation_type=op_type,
                knp=None,
                payment_purpose=None,
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
