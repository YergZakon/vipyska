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

        has_kaspi_mention = False
        has_balance = False
        has_payer_recipient = False
        has_vidy_operacii = False

        for row in sheet.rows[:25]:
            for cell in row:
                if cell is None:
                    continue
                s = str(cell)
                s_low = s.lower()
                if 'Kaspi Bank' in s or 'КАСПИ' in s.upper() or 'KASPI' in s.upper():
                    has_kaspi_mention = True
                if 'входящий остаток' in s_low:
                    has_balance = True
                if 'плательщик' in s_low:
                    has_payer_recipient = True
                if 'получател' in s_low:
                    has_payer_recipient = True
                if 'виды операции' in s_low or 'категория документа' in s_low:
                    has_vidy_operacii = True

        # Kaspi Bank explicitly mentioned + statement structure
        if has_kaspi_mention and has_balance:
            return 0.95
        if has_kaspi_mention and has_payer_recipient:
            return 0.9

        # No explicit Kaspi mention, but strong structural match
        # (Kaspi has unique combo: "Входящий остаток" + "Виды операции" + "Плательщик"/"Получатель")
        if has_balance and has_vidy_operacii and has_payer_recipient:
            return 0.88

        # Folder hint
        if 'kaspi' in folder or 'каспи' in folder:
            if has_payer_recipient or has_balance:
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
                col_map.setdefault('date', i)
            elif 'валюта' in h:
                col_map['currency'] = i
            elif 'виды операции' in h or 'категория документа' in h:
                col_map['operation_type'] = i
            elif 'сумма' in h and ('валют' in h or ('тенге' not in h and 'нб' not in h)):
                col_map['amount'] = i
            elif 'сумма' in h and ('тенге' in h or 'нб' in h):
                col_map['amount_tenge'] = i
            elif 'направлен' in h:
                col_map['direction'] = i
            elif 'назначение' in h and 'код' not in h:
                col_map['payment_purpose'] = i
            elif 'код назначен' in h or 'кнп' in h:
                col_map['knp'] = i

        # For Kaspi, sub-header might define Плательщик/Получатель sub-columns
        # Build parent map: for merged cells, propagate parent rightward
        parent_map = {}  # col_index -> parent header text
        if sub_header:
            current_parent = ''
            for i, h in enumerate(header_lower):
                if h:
                    current_parent = h
                parent_map[i] = current_parent

            sub_lower = [str(c).lower().strip() if c else '' for c in sub_header]
            for i, h in enumerate(sub_lower):
                parent = parent_map.get(i, '')
                if 'наименование' in h or h == 'фио':
                    if 'плательщик' in parent:
                        col_map['payer'] = i
                    elif 'получател' in parent:
                        col_map['recipient'] = i
                elif 'иин' in h or 'бин' in h:
                    if 'плательщик' in parent:
                        col_map['payer_iin'] = i
                    elif 'получател' in parent:
                        col_map['recipient_iin'] = i
                elif 'банк' in h:
                    if 'плательщик' in parent:
                        col_map['payer_bank'] = i
                    elif 'получател' in parent:
                        col_map['recipient_bank'] = i
                elif 'счет' in h or 'номер счета' in h:
                    if 'плательщик' in parent:
                        col_map['payer_account'] = i
                    elif 'получател' in parent:
                        col_map['recipient_account'] = i
                elif 'назначение' in h:
                    col_map.setdefault('payment_purpose', i)
                elif 'кнп' in h or 'код' in h:
                    col_map.setdefault('knp', i)

        # Determine data start (skip header + sub-header + optional numbering row)
        data_start = header_idx + 1
        if sub_header and any(c for c in sub_header if c is not None):
            data_start = header_idx + 2

        # Skip numbering row (1, 2, 3, ...)
        if data_start < len(rows):
            numbering_row = rows[data_start]
            if numbering_row and all(
                str(c).strip().isdigit() or c is None
                for c in numbering_row
            ):
                data_start += 1

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
            op_type = clean_string(self._get(row, col_map.get('operation_type')))
            direction = determine_direction(raw_direction=raw_dir) if raw_dir else None

            # Determine direction from operation type if not explicit
            if not direction and op_type:
                op_low = op_type.lower()
                if 'дебет' in op_low or 'исх' in op_low:
                    direction = 'Расход'
                elif 'кредит' in op_low or 'вх' in op_low:
                    direction = 'Приход'

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
