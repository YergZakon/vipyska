"""Parser for АО Halyk Finance (securities/investment operations).

Format: 14 columns.
Columns: Клиент | Счет расхода | Контрагент | Сумма/Количество ЦБ | Код валюты |
         Валюта/Инструмент | Комментарий | Дата | Режим сделки | Сорт д-та | Тикер |
         Cчет прихода | № л/с | Цена
"""

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
class HalykFinanceParser(BaseParser):
    BANK_NAME = 'АО Halyk Finance'

    @classmethod
    def can_parse(cls, sheet: SheetData, file_info: dict) -> float:
        for row in sheet.rows[:3]:
            row_text = ' '.join(str(c).lower() for c in row if c)
            if 'режим сделки' in row_text or 'тикер' in row_text:
                return 0.9
            if 'сорт д-та' in row_text or 'сорта д-та' in row_text:
                return 0.9
            # Halyk Finance unique combo: "Код инстр-та" + "Счет расхода" + "Контрагент"
            if 'инстр-та' in row_text and 'счет расхода' in row_text:
                return 0.9
            if 'код валюты' in row_text and 'контрагент' in row_text and 'счет расхода' in row_text:
                return 0.88
        folder = file_info.get('folder_name', '').lower()
        if 'halyk finance' in folder:
            return 0.8
        return 0.0

    def parse_sheet(self, sheet: SheetData, file_info: dict) -> Tuple[List[Transaction], dict]:
        rows = sheet.rows
        transactions = []

        header_idx = 0
        for i, row in enumerate(rows[:5]):
            row_text = ' '.join(str(c).lower() for c in row if c)
            if 'клиент' in row_text and 'дата' in row_text:
                header_idx = i
                break

        header = rows[header_idx]
        header_lower = [str(c).lower().strip() if c else '' for c in header]

        col_map = {}
        for i, h in enumerate(header_lower):
            if h == 'клиент':
                col_map['client'] = i
            elif 'счет расхода' in h:
                col_map['debit_account'] = i
            elif 'контрагент' in h:
                col_map['counterparty'] = i
            elif 'сумма' in h:
                col_map['amount'] = i
            elif 'код валюты' in h:
                col_map['currency_code'] = i
            elif 'валюта' in h or 'инструмент' in h:
                col_map['instrument'] = i
            elif 'комментарий' in h:
                col_map['comment'] = i
            elif h == 'дата':
                col_map['date'] = i
            elif 'режим' in h:
                col_map['mode'] = i
            elif 'сорт' in h or 'сорта' in h:
                col_map['doc_type'] = i
            elif 'тикер' in h:
                col_map['ticker'] = i
            elif 'счет прихода' in h:
                col_map['credit_account'] = i

        for row_idx in range(header_idx + 1, len(rows)):
            row = rows[row_idx]
            if not row or all(c is None for c in row):
                continue

            date_val = self._get(row, col_map.get('date'))
            if date_val is None:
                continue

            doc_type = clean_string(self._get(row, col_map.get('doc_type')))
            direction = None
            if doc_type:
                dt_lower = doc_type.lower()
                if 'пополнение' in dt_lower or 'приход' in dt_lower:
                    direction = 'Приход'
                elif 'вывод' in dt_lower or 'расход' in dt_lower or 'списание' in dt_lower:
                    direction = 'Расход'

            t = Transaction(
                transaction_date=normalize_date(date_val),
                amount=normalize_amount(self._get(row, col_map.get('amount'))),
                currency=normalize_currency(self._get(row, col_map.get('currency_code'))),
                amount_tenge=None,
                direction=direction,
                payer=clean_string(self._get(row, col_map.get('client'))),
                payer_iin_bin=None, payer_bank=None,
                payer_account=clean_string(self._get(row, col_map.get('debit_account'))),
                recipient=clean_string(self._get(row, col_map.get('counterparty'))),
                recipient_iin_bin=None, recipient_bank=None,
                recipient_account=clean_string(self._get(row, col_map.get('credit_account'))),
                operation_type=doc_type,
                knp=None,
                payment_purpose=clean_string(self._get(row, col_map.get('comment'))),
                document_number=None,
                statement_bank=self.BANK_NAME,
                account_number=None,
                source_file=file_info['filename'],
            )
            transactions.append(t)

        return transactions, {'account_number': None, 'warnings': ['Securities format'], 'errors': []}

    @staticmethod
    def _get(row, idx):
        if idx is None or idx >= len(row):
            return None
        return row[idx]
