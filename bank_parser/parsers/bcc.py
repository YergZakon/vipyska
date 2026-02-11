"""Parser for АО Банк ЦентрКредит (BCC).

Three formats:
1. Simple 3-col deposit: "Движение денежных средств по депозитному счету" — Дата | Сумма в тг. | Примечание
2. Full 8-col: № | Дата операции | Валюта | Сумма операции | Сумма по курсу НБ | Отправитель | Получатель | Назначение платежа
3. Multi-sheet "Движение по счету клиента": each sheet = direction, 7-col (Dos Group format)
Also .xls files with 15-col format (handled by BCCFullParser).
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
class BCCSimpleParser(BaseParser):
    """BCC deposit movement (3-column format)."""
    BANK_NAME = 'АО Банк ЦентрКредит'

    @classmethod
    def can_parse(cls, sheet: SheetData, file_info: dict) -> float:
        for row in sheet.rows[:5]:
            for cell in row:
                if cell and 'движение денежных средств по депозитному' in str(cell).lower():
                    return 0.9
        return 0.0

    def parse_sheet(self, sheet: SheetData, file_info: dict) -> Tuple[List[Transaction], dict]:
        rows = sheet.rows
        transactions = []
        account_number = None

        # Extract account from title row
        for row in rows[:3]:
            for cell in row:
                if cell:
                    match = re.search(r'(KZ\w{16,22})', str(cell))
                    if match:
                        account_number = match.group(1)

        # Find header
        header_idx = None
        for i, row in enumerate(rows[:10]):
            row_text = ' '.join(str(c).lower() for c in row if c)
            if 'дата' in row_text and 'сумма' in row_text:
                header_idx = i
                break

        if header_idx is None:
            return [], {'warnings': [], 'errors': ['Header not found'], 'account_number': account_number}

        header = rows[header_idx]
        header_lower = [str(c).lower().strip() if c else '' for c in header]

        col_map = {}
        for i, h in enumerate(header_lower):
            if 'дата' in h:
                col_map['date'] = i
            elif 'сумма' in h:
                col_map['amount'] = i
            elif 'примечание' in h or 'описание' in h:
                col_map['note'] = i

        for row_idx in range(header_idx + 1, len(rows)):
            row = rows[row_idx]
            if not row or all(c is None for c in row):
                continue

            date_val = self._get(row, col_map.get('date'))
            if date_val is None:
                continue

            note = clean_string(self._get(row, col_map.get('note')))
            direction = None
            if note:
                direction = determine_direction(raw_direction=note)

            t = Transaction(
                transaction_date=normalize_date(date_val),
                amount=normalize_amount(self._get(row, col_map.get('amount'))),
                currency='KZT',
                amount_tenge=normalize_amount(self._get(row, col_map.get('amount'))),
                direction=direction,
                payer=None, payer_iin_bin=None, payer_bank=None, payer_account=None,
                recipient=None, recipient_iin_bin=None, recipient_bank=None, recipient_account=None,
                operation_type=None, knp=None,
                payment_purpose=note,
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
class BCCFullParser(BaseParser):
    """BCC full statement (8 or 15-column format, including bilingual .xls)."""
    BANK_NAME = 'АО Банк ЦентрКредит'

    @classmethod
    def can_parse(cls, sheet: SheetData, file_info: dict) -> float:
        for row in sheet.rows[:20]:
            row_text = ' '.join(str(c).lower() for c in row if c)
            if 'отправитель' in row_text and 'получатель' in row_text and 'назначение' in row_text:
                return 0.9
            if 'движение денежных средств по счету клиента' in row_text:
                return 0.85
            # Bilingual BCC .xls format
            if 'күні / дата' in row_text or ('дебетовый оборот' in row_text and 'кредитовый оборот' in row_text):
                return 0.92
            if 'выписка по лицевому счету' in row_text:
                folder = file_info.get('folder_name', '').lower()
                if 'центркредит' in folder:
                    return 0.88
        folder = file_info.get('folder_name', '').lower()
        if 'центркредит' in folder and sheet.num_cols >= 7:
            return 0.5
        return 0.0

    def parse_sheet(self, sheet: SheetData, file_info: dict) -> Tuple[List[Transaction], dict]:
        rows = sheet.rows
        transactions = []
        account_number = None

        # Find ALL header rows (file may contain multiple account blocks)
        header_indices = []
        for i, row in enumerate(rows):
            row_text = ' '.join(str(c).lower() for c in row if c)
            if 'дата операции' in row_text and ('отправитель' in row_text or 'получатель' in row_text):
                header_indices.append(i)
            elif 'күні / дата' in row_text or ('дата' in row_text and 'дебетовый оборот' in row_text):
                header_indices.append(i)

        if not header_indices:
            return [], {'warnings': [], 'errors': ['Header not found'], 'account_number': None}

        for block_idx, header_idx in enumerate(header_indices):
            # Extract account number from rows before this header
            block_account = None
            search_start = header_indices[block_idx - 1] if block_idx > 0 else 0
            for row in rows[search_start:header_idx]:
                for cell in row:
                    if cell:
                        match = re.search(r'(KZ\w{16,22})', str(cell))
                        if match:
                            block_account = match.group(1)
            if not account_number:
                account_number = block_account

            header = rows[header_idx]
            header_lower = [str(c).lower().strip() if c else '' for c in header]

            col_map = {}
            for i, h in enumerate(header_lower):
                if 'дата' in h:
                    col_map.setdefault('date', i)
                elif 'валюта' in h:
                    col_map['currency'] = i
                elif 'сумма операции' in h:
                    col_map['amount'] = i
                elif 'сумма по курсу' in h or 'курс нб' in h:
                    col_map['amount_tenge'] = i
                elif 'отправитель' in h:
                    col_map['sender'] = i
                elif 'получатель' in h or 'наименование контрагента' in h:
                    col_map['recipient'] = i
                elif 'назначение' in h or 'төлем мақсаты' in h:
                    col_map['purpose'] = i
                elif 'дебетовый оборот' in h or ('дебет' in h and 'кредит' not in h):
                    col_map['debit'] = i
                elif 'кредитовый оборот' in h or ('кредит' in h and 'дебет' not in h):
                    col_map['credit'] = i
                elif 'иин' in h and 'бин' in h:
                    col_map['iin'] = i
                elif '№ документа' in h or 'құжат' in h:
                    col_map['doc_number'] = i
                elif 'банк корресп' in h or 'корресп. банк' in h:
                    col_map['corr_bank'] = i
                elif 'счет-корреспондент' in h or 'корресп. есепшоты' in h:
                    col_map['corr_account'] = i
                elif 'кнп' in h or 'тмк' in h:
                    col_map['knp'] = i

            # Data ends at next header or end of file
            end_idx = header_indices[block_idx + 1] if block_idx + 1 < len(header_indices) else len(rows)

            for row_idx in range(header_idx + 1, end_idx):
                row = rows[row_idx]
                if not row or all(c is None for c in row):
                    continue

                date_val = self._get(row, col_map.get('date'))
                if date_val is None:
                    continue
                if isinstance(date_val, str) and not date_val.strip():
                    continue
                # Skip summary/total rows
                if isinstance(date_val, str) and any(w in date_val.lower() for w in ['итого', 'выписка', 'барлығы']):
                    continue

                amount = normalize_amount(self._get(row, col_map.get('amount')))
                debit = normalize_amount(self._get(row, col_map.get('debit')))
                credit = normalize_amount(self._get(row, col_map.get('credit')))
                direction = determine_direction(debit_amount=debit, credit_amount=credit) if (debit or credit) else None
                if not amount:
                    amount = credit or debit

                t = Transaction(
                    transaction_date=normalize_date(date_val),
                    amount=amount,
                    currency=normalize_currency(self._get(row, col_map.get('currency'))),
                    amount_tenge=normalize_amount(self._get(row, col_map.get('amount_tenge'))) or amount,
                    direction=direction,
                    payer=clean_string(self._get(row, col_map.get('sender'))),
                    payer_iin_bin=normalize_iin_bin(self._get(row, col_map.get('iin'))) if direction == 'Приход' else None,
                    payer_bank=clean_string(self._get(row, col_map.get('corr_bank'))) if direction == 'Приход' else None,
                    payer_account=clean_string(self._get(row, col_map.get('corr_account'))) if direction == 'Приход' else None,
                    recipient=clean_string(self._get(row, col_map.get('recipient'))),
                    recipient_iin_bin=normalize_iin_bin(self._get(row, col_map.get('iin'))) if direction == 'Расход' else None,
                    recipient_bank=clean_string(self._get(row, col_map.get('corr_bank'))) if direction == 'Расход' else None,
                    recipient_account=clean_string(self._get(row, col_map.get('corr_account'))) if direction == 'Расход' else None,
                    operation_type=None,
                    knp=clean_string(self._get(row, col_map.get('knp'))),
                    payment_purpose=clean_string(self._get(row, col_map.get('purpose'))),
                    document_number=clean_string(self._get(row, col_map.get('doc_number'))),
                    statement_bank=self.BANK_NAME,
                    account_number=block_account or account_number,
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
class BCCClientMovementParser(BaseParser):
    """BCC multi-sheet 'Движение по счету клиента' format (e.g. Dos Group)."""
    BANK_NAME = 'АО Банк ЦентрКредит'

    @classmethod
    def can_parse(cls, sheet: SheetData, file_info: dict) -> float:
        for row in sheet.rows[:3]:
            for cell in row:
                if cell and 'движение денежных средств по счету клиента' in str(cell).lower():
                    return 0.93
        # Check sheet names for direction-based multi-sheet
        sn = sheet.name.lower()
        if ('входящие' in sn or 'исходящие' in sn or 'снятие' in sn):
            folder = file_info.get('folder_name', '').lower()
            if 'центркредит' in folder:
                return 0.88
        return 0.0

    def parse_sheet(self, sheet: SheetData, file_info: dict) -> Tuple[List[Transaction], dict]:
        rows = sheet.rows
        transactions = []
        account_number = None

        # Determine direction from title or sheet name
        direction = None
        sn = sheet.name.lower()
        title_text = ''
        for row in rows[:3]:
            for cell in row:
                if cell:
                    title_text += ' ' + str(cell).lower()

        if 'входящ' in sn or 'входящ' in title_text:
            direction = 'Приход'
        elif 'исходящ' in sn or 'исходящ' in title_text or 'снятие' in sn or 'снятие' in title_text:
            direction = 'Расход'

        # Extract BIN/client from title
        client_bin = None
        client_name = None
        for row in rows[:3]:
            for cell in row:
                if cell:
                    m = re.search(r'БИН\s*(\d{12})', str(cell))
                    if m:
                        client_bin = m.group(1)
                    # Extract client name between quotes
                    m2 = re.search(r'[«"](.+?)[»"]', str(cell))
                    if m2:
                        client_name = m2.group(1)

        # Find header
        header_idx = None
        for i, row in enumerate(rows[:5]):
            row_text = ' '.join(str(c).lower() for c in row if c)
            if 'дата операции' in row_text or ('дата' in row_text and ('сумма' in row_text or 'наименование' in row_text)):
                header_idx = i
                break

        if header_idx is None:
            return [], {'warnings': [], 'errors': ['Header not found'], 'account_number': None}

        header = rows[header_idx]
        header_lower = [str(c).lower().strip() if c else '' for c in header]

        col_map = {}
        for i, h in enumerate(header_lower):
            if 'дата' in h:
                col_map.setdefault('date', i)
            elif 'сумма' in h:
                col_map['amount'] = i
            elif 'наименование дебет' in h or 'дебет' in h:
                col_map['debit_name'] = i
            elif 'наименование кредит' in h or 'кредит' in h:
                col_map['credit_name'] = i
            elif h == 'бин' or 'бин' in h:
                col_map['bin'] = i
            elif 'основание' in h or 'назначение' in h:
                col_map['purpose'] = i
            elif 'подразделение' in h:
                col_map['branch'] = i

        for row_idx in range(header_idx + 1, len(rows)):
            row = rows[row_idx]
            if not row or all(c is None for c in row):
                continue

            date_val = self._get(row, col_map.get('date'))
            if date_val is None:
                continue
            if isinstance(date_val, str) and any(w in date_val.lower() for w in ['итого', 'всего']):
                continue

            amount = normalize_amount(self._get(row, col_map.get('amount')))
            payer = clean_string(self._get(row, col_map.get('debit_name')))
            recipient = clean_string(self._get(row, col_map.get('credit_name')))
            bin_val = normalize_iin_bin(self._get(row, col_map.get('bin')))

            t = Transaction(
                transaction_date=normalize_date(date_val),
                amount=amount,
                currency='KZT',
                amount_tenge=amount,
                direction=direction,
                payer=payer,
                payer_iin_bin=bin_val if direction == 'Приход' else client_bin,
                payer_bank=None, payer_account=None,
                recipient=recipient,
                recipient_iin_bin=client_bin if direction == 'Приход' else bin_val,
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
