"""Parser for АО Нурбанк.

Two formats:
1. 23-col .xlsx: "Операции, проведенные в АБИС" — full report
2. 13-col .xls: Bilingual (Kazakh/Russian) — Дата, № Документа, Счет ГК, Дебет, Кредит, etc.
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
class NurbankParser(BaseParser):
    """Nurbank 23-col or 16-col .xlsx format."""
    BANK_NAME = 'АО Нурбанк'

    @classmethod
    def can_parse(cls, sheet: SheetData, file_info: dict) -> float:
        for row in sheet.rows[:10]:
            for cell in row:
                if cell and 'операции, проведенные в абис' in str(cell).lower():
                    return 0.95
        # 16-col format with header at row 13
        for row in sheet.rows[:15]:
            row_text = ' '.join(str(c).lower() for c in row if c)
            if '№ п/п' in row_text and ('дата операции' in row_text or 'категория' in row_text):
                folder = file_info.get('folder_name', '').lower()
                if 'нурбанк' in folder:
                    return 0.9
                return 0.7
            if 'плательщик' in row_text and 'получатель' in row_text:
                folder = file_info.get('folder_name', '').lower()
                if 'нурбанк' in folder:
                    return 0.9
                return 0.7
        return 0.0

    def parse_sheet(self, sheet: SheetData, file_info: dict) -> Tuple[List[Transaction], dict]:
        rows = sheet.rows
        transactions = []

        # Find header row — scan deeper for some formats
        header_idx = None
        for i, row in enumerate(rows[:20]):
            row_text = ' '.join(str(c).lower() for c in row if c)
            if '№ п/п' in row_text and ('дата операции' in row_text or 'категория' in row_text):
                header_idx = i
                break
            if 'плательщик' in row_text and 'получатель' in row_text:
                header_idx = i
                break

        if header_idx is None:
            return [], {'warnings': [], 'errors': ['Header not found'], 'account_number': None}

        header = rows[header_idx]
        header_lower = [str(c).lower().strip() if c else '' for c in header]

        col_map = {}
        for i, h in enumerate(header_lower):
            if 'дата операции' in h:
                col_map['date'] = i
            elif h == 'валюта':
                col_map['currency'] = i
            elif 'категория' in h:
                col_map['category'] = i
            elif 'сумма' in h and 'вал' in h:
                col_map['amount'] = i
            elif 'сумма' in h and 'тенге' in h:
                col_map['amount_tenge'] = i
            elif h == 'кнп' or 'код назначен' in h:
                col_map['knp'] = i
            elif 'плательщик' in h and 'иин' not in h and 'банк' not in h and 'счет' not in h:
                col_map['payer'] = i
            elif 'иин' in h and 'плательщик' in h:
                col_map['payer_iin'] = i
            elif 'банк' in h and 'плательщик' in h:
                col_map['payer_bank'] = i
            elif 'счет' in h and 'плательщик' in h:
                col_map['payer_account'] = i
            elif 'получатель' in h and 'иин' not in h and 'банк' not in h and 'счет' not in h:
                col_map['recipient'] = i
            elif 'иин' in h and 'получатель' in h:
                col_map['recipient_iin'] = i
            elif 'банк' in h and 'получатель' in h:
                col_map['recipient_bank'] = i
            elif 'счет' in h and 'получатель' in h:
                col_map['recipient_account'] = i
            elif 'назначение' in h:
                col_map['purpose'] = i
            elif '№ операции' in h or 'номер документа' in h:
                col_map['doc_number'] = i

        # Skip numeric index row if present (row with 1, 2, 3...)
        data_start = header_idx + 1
        if data_start < len(rows):
            row = rows[data_start]
            vals = [c for c in row if c is not None]
            if vals and all(isinstance(v, (int, float)) and v < 50 for v in vals):
                data_start += 1

        for row_idx in range(data_start, len(rows)):
            row = rows[row_idx]
            if not row or all(c is None for c in row):
                continue

            date_val = self._get(row, col_map.get('date'))
            if date_val is None:
                continue

            if isinstance(date_val, str) and any(w in date_val.lower() for w in ['итого', 'всего']):
                continue

            t = Transaction(
                transaction_date=normalize_date(date_val),
                amount=normalize_amount(self._get(row, col_map.get('amount'))),
                currency=normalize_currency(self._get(row, col_map.get('currency'))),
                amount_tenge=normalize_amount(self._get(row, col_map.get('amount_tenge'))),
                direction=None,
                payer=clean_string(self._get(row, col_map.get('payer'))),
                payer_iin_bin=normalize_iin_bin(self._get(row, col_map.get('payer_iin'))),
                payer_bank=clean_string(self._get(row, col_map.get('payer_bank'))),
                payer_account=clean_string(self._get(row, col_map.get('payer_account'))),
                recipient=clean_string(self._get(row, col_map.get('recipient'))),
                recipient_iin_bin=normalize_iin_bin(self._get(row, col_map.get('recipient_iin'))),
                recipient_bank=clean_string(self._get(row, col_map.get('recipient_bank'))),
                recipient_account=clean_string(self._get(row, col_map.get('recipient_account'))),
                operation_type=clean_string(self._get(row, col_map.get('category'))),
                knp=clean_string(self._get(row, col_map.get('knp'))),
                payment_purpose=clean_string(self._get(row, col_map.get('purpose'))),
                document_number=clean_string(self._get(row, col_map.get('doc_number'))),
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


@register_parser
class NurbankXlsParser(BaseParser):
    """Nurbank 13-col bilingual .xls format."""
    BANK_NAME = 'АО Нурбанк'

    @classmethod
    def can_parse(cls, sheet: SheetData, file_info: dict) -> float:
        folder = file_info.get('folder_name', '').lower()
        if 'нурбанк' not in folder:
            return 0.0
        for row in sheet.rows[:15]:
            row_text = ' '.join(str(c).lower() for c in row if c)
            if ('дата' in row_text and 'дебет' in row_text and 'кредит' in row_text and
                    ('корреспондент' in row_text or 'назначение' in row_text)):
                return 0.92
        return 0.0

    def parse_sheet(self, sheet: SheetData, file_info: dict) -> Tuple[List[Transaction], dict]:
        rows = sheet.rows
        transactions = []
        account_number = None

        # Extract account from metadata
        for row in rows[:10]:
            for cell in row:
                if cell:
                    m = re.search(r'(KZ\w{16,22})', str(cell))
                    if m:
                        account_number = m.group(1)

        # Find header
        header_idx = None
        for i, row in enumerate(rows[:15]):
            row_text = ' '.join(str(c).lower() for c in row if c)
            if 'дата' in row_text and 'дебет' in row_text and 'кредит' in row_text:
                header_idx = i
                break

        if header_idx is None:
            return [], {'warnings': [], 'errors': ['Header not found'], 'account_number': account_number}

        header = rows[header_idx]
        header_lower = [str(c).lower().strip() if c else '' for c in header]

        col_map = {}
        for i, h in enumerate(header_lower):
            if 'дата' in h:
                col_map.setdefault('date', i)
            elif '№ документа' in h or 'құжат' in h:
                col_map['doc_number'] = i
            elif 'счет корреспондент' in h or 'корресп' in h and 'счет' in h:
                col_map['corr_account'] = i
            elif 'наименование корреспондент' in h or ('наименование' in h and 'корресп' in h):
                col_map['counterparty'] = i
            elif 'иин' in h or 'бин' in h:
                col_map['iin'] = i
            elif 'бик' in h:
                col_map['bik'] = i
            elif 'банк' in h and 'корресп' in h:
                col_map['corr_bank'] = i
            elif 'дебет' in h and 'эквивалент' not in h:
                col_map.setdefault('debit', i)
            elif 'дебет' in h and 'эквивалент' in h:
                col_map['debit_equiv'] = i
            elif 'кредит' in h and 'эквивалент' not in h:
                col_map.setdefault('credit', i)
            elif 'кредит' in h and 'эквивалент' in h:
                col_map['credit_equiv'] = i
            elif 'назначение' in h:
                col_map['purpose'] = i

        for row_idx in range(header_idx + 1, len(rows)):
            row = rows[row_idx]
            if not row or all(c is None for c in row):
                continue

            date_val = self._get(row, col_map.get('date'))
            if date_val is None:
                continue
            if isinstance(date_val, str) and any(w in date_val.lower() for w in ['итого', 'всего', 'остаток', 'входящий']):
                continue

            debit = normalize_amount(self._get(row, col_map.get('debit')))
            credit = normalize_amount(self._get(row, col_map.get('credit')))
            direction = determine_direction(debit_amount=debit, credit_amount=credit)
            amount = credit or debit

            debit_equiv = normalize_amount(self._get(row, col_map.get('debit_equiv')))
            credit_equiv = normalize_amount(self._get(row, col_map.get('credit_equiv')))
            amount_tenge = credit_equiv or debit_equiv or amount

            counterparty = clean_string(self._get(row, col_map.get('counterparty')))
            iin = normalize_iin_bin(self._get(row, col_map.get('iin')))
            corr_bank = clean_string(self._get(row, col_map.get('corr_bank')))
            corr_account = clean_string(self._get(row, col_map.get('corr_account')))

            t = Transaction(
                transaction_date=normalize_date(date_val),
                amount=amount,
                currency='KZT',
                amount_tenge=amount_tenge,
                direction=direction,
                payer=counterparty if direction == 'Приход' else None,
                payer_iin_bin=iin if direction == 'Приход' else None,
                payer_bank=corr_bank if direction == 'Приход' else None,
                payer_account=corr_account if direction == 'Приход' else None,
                recipient=counterparty if direction == 'Расход' else None,
                recipient_iin_bin=iin if direction == 'Расход' else None,
                recipient_bank=corr_bank if direction == 'Расход' else None,
                recipient_account=corr_account if direction == 'Расход' else None,
                operation_type=None, knp=None,
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
