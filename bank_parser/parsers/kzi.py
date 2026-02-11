"""Parser for АО ДБ КЗИ БАНК.

Format: 12 columns.
Row 0: empty
Row 1: Header: № | Дата транзакции | ИИН | Номер счета | Держатель карты ФИО | Отправитель |
       Получатель | наименование | назначение платежа | сумма (вход.) | сумма (исход.) | Вид операции
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
class KZIBankParser(BaseParser):
    BANK_NAME = 'АО ДБ КЗИ БАНК'

    @classmethod
    def can_parse(cls, sheet: SheetData, file_info: dict) -> float:
        found_kzi_header = False
        found_sdp_header = False

        for row in sheet.rows[:10]:
            row_text = ' '.join(str(c).lower() for c in row if c)
            if 'дата транзакции' in row_text and 'держатель карты' in row_text:
                return 0.95
            if 'вход. оборот' in row_text or 'исход. оборот' in row_text:
                return 0.9
            # СДП format: 8 cols — Наименование + Дата + Сумма + Валюта + ИИН/БИН + Описание
            if ('наименование' in row_text and 'сумма' in row_text
                    and 'валюта' in row_text and 'описание' in row_text
                    and 'иин/бин' in row_text):
                found_sdp_header = True

        # Check metadata for KZI-specific patterns (e.g. "Транзакций с ... по ...")
        for row in sheet.rows[:10]:
            for cell in row:
                if cell:
                    cl = str(cell).lower()
                    if 'транзакций с' in cl:
                        found_kzi_header = True

        if found_sdp_header and found_kzi_header:
            return 0.92  # СДП format with "Транзакций с..." marker
        if found_sdp_header:
            folder = file_info.get('folder_name', '').lower()
            if 'кзи' in folder:
                return 0.90
            return 0.82  # Generic but unique combo of cols

        folder = file_info.get('folder_name', '').lower()
        if 'кзи банк' in folder or 'кзи' in folder:
            for row in sheet.rows[:15]:
                row_text = ' '.join(str(c).lower() for c in row if c)
                if ('дата' in row_text and 'сумма' in row_text) or 'наименование' in row_text:
                    return 0.8
            return 0.7
        return 0.0

    def parse_sheet(self, sheet: SheetData, file_info: dict) -> Tuple[List[Transaction], dict]:
        rows = sheet.rows
        transactions = []

        header_idx = None
        for i, row in enumerate(rows[:15]):
            row_text = ' '.join(str(c).lower() for c in row if c)
            if 'дата транзакции' in row_text:
                header_idx = i
                break
            if 'дата' in row_text and 'сумма' in row_text and 'наименование' in row_text:
                header_idx = i
                break
            # СДП 8-col format: № п/п | Наименование | Дата | Сумма | Валюта | ИИН/БИН | Наименование клиента | Описание
            if '№ п/п' in row_text and 'наименование' in row_text and 'описание' in row_text:
                header_idx = i
                break

        if header_idx is None:
            return [], {'warnings': [], 'errors': ['Header not found'], 'account_number': None}

        header = rows[header_idx]
        header_lower = [str(c).lower().strip() if c else '' for c in header]

        col_map = {}
        for i, h in enumerate(header_lower):
            if 'дата транзакции' in h or ('дата' in h and 'date' not in col_map):
                col_map['date'] = i
            elif h == 'иин' or 'иин/бин' in h:
                col_map['iin'] = i
            elif 'номер счета' in h:
                col_map['account'] = i
            elif 'держатель' in h:
                col_map['holder'] = i
            elif 'отправитель' in h:
                col_map['sender'] = i
            elif 'получатель' in h:
                col_map['recipient'] = i
            elif 'наименование клиента' in h:
                col_map['client_name'] = i
            elif 'наименование' in h and 'клиент' not in h:
                col_map['name'] = i
            elif 'назначение' in h or 'описание' in h:
                col_map['purpose'] = i
            elif 'вход' in h:
                col_map['credit'] = i
            elif 'исход' in h:
                col_map['debit'] = i
            elif 'сумма' in h:
                col_map['amount'] = i
            elif 'валюта' in h:
                col_map['currency'] = i
            elif 'вид операции' in h:
                col_map['type'] = i

        # Skip numeric index row if present
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

            credit = normalize_amount(self._get(row, col_map.get('credit')))
            debit = normalize_amount(self._get(row, col_map.get('debit')))
            direction = determine_direction(debit_amount=debit, credit_amount=credit)
            amount = credit or debit
            # Fallback to 'amount' column for simple format
            if not amount:
                amount = normalize_amount(self._get(row, col_map.get('amount')))

            currency_val = clean_string(self._get(row, col_map.get('currency')))

            t = Transaction(
                transaction_date=normalize_date(date_val),
                amount=amount,
                currency=normalize_currency(currency_val) if currency_val else 'KZT',
                amount_tenge=amount,
                direction=direction,
                payer=clean_string(self._get(row, col_map.get('sender'))) or clean_string(self._get(row, col_map.get('client_name'))),
                payer_iin_bin=normalize_iin_bin(self._get(row, col_map.get('iin'))),
                payer_bank=None,
                payer_account=clean_string(self._get(row, col_map.get('account'))),
                recipient=clean_string(self._get(row, col_map.get('recipient'))),
                recipient_iin_bin=None, recipient_bank=None, recipient_account=None,
                operation_type=clean_string(self._get(row, col_map.get('type'))) or clean_string(self._get(row, col_map.get('name'))),
                knp=None,
                payment_purpose=clean_string(self._get(row, col_map.get('purpose'))),
                document_number=None,
                statement_bank=self.BANK_NAME,
                account_number=clean_string(self._get(row, col_map.get('account'))),
                source_file=file_info['filename'],
            )
            transactions.append(t)

        return transactions, {'account_number': None, 'warnings': [], 'errors': []}

    @staticmethod
    def _get(row, idx):
        if idx is None or idx >= len(row):
            return None
        return row[idx]
