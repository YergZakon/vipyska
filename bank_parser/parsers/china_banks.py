"""Parser for Chinese banks in Kazakhstan.

1. АО ДБ «Банк Китая в Казахстане» — .xls, 21 columns
2. АО Торгово-промышленный банк Китая в Алматы — .xls, 7 columns
   Format: "Выписка со счета" header, bilingual (Kazakh/Russian)
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
class BankKitayaParser(BaseParser):
    """АО ДБ Банк Китая в Казахстане."""
    BANK_NAME = 'АО ДБ Банк Китая в Казахстане'

    @classmethod
    def can_parse(cls, sheet: SheetData, file_info: dict) -> float:
        for row in sheet.rows[:15]:
            for cell in row:
                if cell:
                    s = str(cell).lower()
                    if 'банк китая в казахстане' in s and 'торгово' not in s:
                        return 0.95
        folder = file_info.get('folder_name', '').lower()
        if 'банк китая' in folder and 'торгово' not in folder:
            return 0.85
        return 0.0

    def parse_sheet(self, sheet: SheetData, file_info: dict) -> Tuple[List[Transaction], dict]:
        rows = sheet.rows
        transactions = []
        account_number = None

        # Find header — scan up to 35 rows
        header_idx = None
        for i, row in enumerate(rows[:35]):
            row_text = ' '.join(str(c).lower() for c in row if c)
            if 'дата' in row_text and ('сумма' in row_text or 'получатель' in row_text or 'плательщик' in row_text):
                header_idx = i
                break
            if 'дата' in row_text and ('дебет' in row_text or 'кредит' in row_text):
                header_idx = i
                break

        if header_idx is None:
            return [], {'warnings': [], 'errors': ['Header not found'], 'account_number': None}

        header = rows[header_idx]
        header_lower = [str(c).lower().strip() if c else '' for c in header]

        col_map = {}
        for i, h in enumerate(header_lower):
            if 'дата' in h and 'date' not in col_map:
                col_map['date'] = i
            elif 'сумма' in h and 'тенге' not in h:
                col_map['amount'] = i
            elif 'тенге' in h or 'эквивалент' in h:
                col_map['amount_tenge'] = i
            elif 'валюта' in h:
                col_map['currency'] = i
            elif 'плательщик' in h and 'банк' not in h and 'иин' not in h:
                col_map['payer'] = i
            elif 'получатель' in h and 'банк' not in h and 'иин' not in h:
                col_map['recipient'] = i
            elif 'иин' in h and 'плательщик' in h:
                col_map['payer_iin'] = i
            elif 'иин' in h and 'получатель' in h:
                col_map['recipient_iin'] = i
            elif 'назначение' in h:
                col_map['purpose'] = i
            elif 'дебет' in h:
                col_map['debit'] = i
            elif 'кредит' in h:
                col_map['credit'] = i

        # Check for sub-header row (e.g. Дебет / Кредит on next row)
        data_start = header_idx + 1
        if data_start < len(rows):
            sub = rows[data_start]
            sub_text = ' '.join(str(c).lower() for c in sub if c)
            if 'дебет' in sub_text and 'кредит' in sub_text and 'дата' not in sub_text:
                sub_lower = [str(c).lower().strip() if c else '' for c in sub]
                for i, h in enumerate(sub_lower):
                    if 'дебет' in h and 'debit' not in col_map:
                        col_map['debit'] = i
                    elif 'кредит' in h and 'credit' not in col_map:
                        col_map['credit'] = i
                data_start = header_idx + 2

        for row_idx in range(data_start, len(rows)):
            row = rows[row_idx]
            if not row or all(c is None for c in row):
                continue

            date_val = self._get(row, col_map.get('date'))
            if date_val is None:
                continue

            if isinstance(date_val, str) and any(w in date_val.lower() for w in ['итого', 'остаток', 'барлығы']):
                continue

            debit = normalize_amount(self._get(row, col_map.get('debit')))
            credit = normalize_amount(self._get(row, col_map.get('credit')))
            amount = normalize_amount(self._get(row, col_map.get('amount'))) or credit or debit
            direction = determine_direction(debit_amount=debit, credit_amount=credit)

            t = Transaction(
                transaction_date=normalize_date(date_val),
                amount=amount,
                currency=normalize_currency(self._get(row, col_map.get('currency'))),
                amount_tenge=normalize_amount(self._get(row, col_map.get('amount_tenge'))),
                direction=direction,
                payer=clean_string(self._get(row, col_map.get('payer'))),
                payer_iin_bin=normalize_iin_bin(self._get(row, col_map.get('payer_iin'))),
                payer_bank=None, payer_account=None,
                recipient=clean_string(self._get(row, col_map.get('recipient'))),
                recipient_iin_bin=normalize_iin_bin(self._get(row, col_map.get('recipient_iin'))),
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


@register_parser
class TPBKitayaParser(BaseParser):
    """АО Торгово-промышленный банк Китая в Алматы."""
    BANK_NAME = 'АО Торгово-промышленный банк Китая в Алматы'

    def parse(self, sheets, file_info):
        """Override to skip metadata-only and garbled sheets."""
        relevant = []
        for s in sheets:
            # Skip garbled Chinese-only sheets (e.g. '页面1-1') and sheets without data headers
            if s.num_cols < 3:
                continue
            has_data_header = False
            for row in s.rows[:5]:
                row_text = ' '.join(str(c).lower() for c in row if c)
                if 'дата' in row_text or 'күн' in row_text or 'дебет' in row_text or 'референс' in row_text:
                    has_data_header = True
                    break
            if has_data_header:
                relevant.append(s)
        if not relevant:
            relevant = sheets  # fallback
        return super().parse(relevant, file_info)

    @classmethod
    def can_parse(cls, sheet: SheetData, file_info: dict) -> float:
        for row in sheet.rows[:5]:
            for cell in row:
                if cell:
                    s = str(cell).lower()
                    if 'шоттан үзінді' in s or 'тпбк' in s:
                        return 0.95
                    if 'выписка со счета' in s:
                        folder = file_info.get('folder_name', '').lower()
                        if 'торгово-промышленный' in folder or 'тпб' in folder:
                            return 0.95
                        return 0.5
        for row in sheet.rows[:10]:
            for cell in row:
                if cell and 'торгово-промышленный' in str(cell).lower():
                    return 0.93
        folder = file_info.get('folder_name', '').lower()
        if 'торгово-промышленный' in folder:
            return 0.85
        return 0.0

    def parse_sheet(self, sheet: SheetData, file_info: dict) -> Tuple[List[Transaction], dict]:
        rows = sheet.rows
        transactions = []
        account_number = sheet.name if sheet.name.startswith('KZ') else None

        # Extract account number and currency from metadata rows
        currency = None
        for row in rows[:20]:
            for cell in row:
                if cell:
                    s = str(cell)
                    # Account number (KZ...)
                    m = re.search(r'(KZ\w{16,22})', s)
                    if m and not account_number:
                        account_number = m.group(1)
                    # Currency from metadata (e.g. row 14 col 4)
                    if s.strip() in ('KZT', 'USD', 'EUR', 'CNY', 'RUB', 'GBP'):
                        currency = s.strip()

        # Find header — scan up to row 35
        header_idx = None
        for i, row in enumerate(rows[:35]):
            row_text = ' '.join(str(c).lower() for c in row if c)
            if ('дата операции' in row_text or 'операция жасалатын күн' in row_text):
                header_idx = i
                break
            if 'дата' in row_text and ('дебет' in row_text or 'кредит' in row_text or 'сумма' in row_text):
                header_idx = i
                break

        if header_idx is None:
            return [], {'warnings': [], 'errors': ['Header not found'], 'account_number': account_number}

        header = rows[header_idx]
        header_lower = [str(c).lower().strip() if c else '' for c in header]

        col_map = {}
        for i, h in enumerate(header_lower):
            if 'дата' in h or 'күн' in h:
                col_map.setdefault('date', i)
            elif 'референс' in h or 'назначение' in h:
                col_map['purpose'] = i
            elif 'дебет' in h:
                col_map['debit'] = i
            elif 'кредит' in h or 'несие' in h:
                col_map['credit'] = i
            elif 'эквивалент' in h or 'тенге' in h:
                col_map['amount_tenge'] = i
            elif 'бенефициар' in h and 'банк' not in h:
                col_map['beneficiary'] = i
            elif 'банк' in h and 'бенефициар' in h:
                col_map['beneficiary_bank'] = i
            elif 'корреспондент' in h or 'контрагент' in h:
                col_map['counterparty'] = i
            elif 'описание' in h:
                col_map.setdefault('purpose', i)

        # Check for sub-header row with Дебет/Кредит
        data_start = header_idx + 1
        if data_start < len(rows):
            sub = rows[data_start]
            sub_text = ' '.join(str(c).lower() for c in sub if c)
            if ('дебет' in sub_text or 'несие' in sub_text) and 'дата' not in sub_text:
                sub_lower = [str(c).lower().strip() if c else '' for c in sub]
                for i, h in enumerate(sub_lower):
                    if 'дебет' in h and 'debit' not in col_map:
                        col_map['debit'] = i
                    elif ('кредит' in h or 'несие' in h) and 'credit' not in col_map:
                        col_map['credit'] = i
                data_start = header_idx + 2

        for row_idx in range(data_start, len(rows)):
            row = rows[row_idx]
            if not row or all(c is None for c in row):
                continue

            date_val = self._get(row, col_map.get('date'))
            if date_val is None:
                continue
            if isinstance(date_val, str) and any(w in date_val.lower() for w in ['итого', 'остаток', 'входящий', 'барлығы', 'оборот']):
                continue

            debit = normalize_amount(self._get(row, col_map.get('debit')))
            credit = normalize_amount(self._get(row, col_map.get('credit')))
            direction = determine_direction(debit_amount=debit, credit_amount=credit)
            amount = credit or debit

            amount_tenge = normalize_amount(self._get(row, col_map.get('amount_tenge')))

            # Beneficiary info
            beneficiary = clean_string(self._get(row, col_map.get('beneficiary')))
            beneficiary_bank = clean_string(self._get(row, col_map.get('beneficiary_bank')))
            counterparty = clean_string(self._get(row, col_map.get('counterparty')))
            party = beneficiary or counterparty

            # Extract IIN/BIN from beneficiary string (e.g. "ТОО Ромат\nИИК: KZ...\nБИН: 123456789012")
            party_iin = None
            party_account = None
            if party:
                iin_m = re.search(r'(?:БИН|ИИН|BIN|IIN)[:\s]*(\d{12})', party)
                if iin_m:
                    party_iin = iin_m.group(1)
                acc_m = re.search(r'(?:ИИК|IIK|Счет)[:\s]*(KZ\w{16,22})', party)
                if acc_m:
                    party_account = acc_m.group(1)

            t = Transaction(
                transaction_date=normalize_date(date_val),
                amount=amount,
                currency=currency,
                amount_tenge=amount_tenge or amount,
                direction=direction,
                payer=party if direction == 'Приход' else None,
                payer_iin_bin=party_iin if direction == 'Приход' else None,
                payer_bank=beneficiary_bank if direction == 'Приход' else None,
                payer_account=party_account if direction == 'Приход' else None,
                recipient=party if direction == 'Расход' else None,
                recipient_iin_bin=party_iin if direction == 'Расход' else None,
                recipient_bank=beneficiary_bank if direction == 'Расход' else None,
                recipient_account=party_account if direction == 'Расход' else None,
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
