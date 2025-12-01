"""
Парсер выписок Казкоммерцбанк (сейчас Halyk Bank)
Формат: xls с метаданными в заголовке и транзакциями
"""
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import List, Tuple, Union, Optional
import re
import pandas as pd

from .base_parser import BaseParser
from ..models import UnifiedTransaction, StatementMetadata


class KazkomParser(BaseParser):
    """Парсер выписок Казкоммерцбанк"""

    BANK_NAME = "АО Казкоммерцбанк"
    BANK_ALIASES = ['казкоммерцбанк', 'kazkommertsbank', 'kazkom', 'kkb']

    def __init__(self, file_path: Union[str, Path]):
        super().__init__(file_path)

    @classmethod
    def can_parse(cls, file_path: Union[str, Path]) -> bool:
        """Проверка, подходит ли файл для парсера Казкоммерцбанк"""
        try:
            path = Path(file_path)
            if path.suffix.lower() not in ('.xlsx', '.xls'):
                return False

            try:
                df = pd.read_excel(file_path, header=None, nrows=15, engine='xlrd')
            except:
                df = pd.read_excel(file_path, header=None, nrows=15)

            for idx in range(min(12, len(df))):
                row = df.iloc[idx]
                row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
                # Характерная структура Казкоммерцбанка
                if 'имя клиента' in row_text and 'номер документа' in row_text:
                    return True
                if 'период формирования выписки' in row_text:
                    return True
                if 'bta debit account' in row_text.lower():
                    return True

            return False
        except Exception:
            return False

    def parse(self) -> Tuple[StatementMetadata, List[UnifiedTransaction]]:
        """Парсинг файла выписки Казкоммерцбанк"""
        try:
            df_raw = pd.read_excel(self.file_path, header=None, engine='xlrd')
        except:
            df_raw = pd.read_excel(self.file_path, header=None)

        metadata = StatementMetadata()
        metadata.bank_name = self.BANK_NAME
        metadata.source_file = self.file_path.name

        # Извлекаем метаданные
        header_row = None
        for idx in range(min(15, len(df_raw))):
            row = df_raw.iloc[idx]
            row_text = ' '.join(str(v) for v in row if pd.notna(v))
            row_lower = row_text.lower()

            # Имя клиента
            if 'имя клиента' in row_lower:
                parts = row_text.split('|')
                if len(parts) > 1:
                    metadata.client_name = parts[1].strip()
                else:
                    # Ищем значение в следующей ячейке
                    for i, val in enumerate(row):
                        if pd.notna(val) and 'имя клиента' in str(val).lower():
                            if i + 1 < len(row) and pd.notna(row.iloc[i + 1]):
                                metadata.client_name = str(row.iloc[i + 1]).strip()

            # ИИН
            if row_lower.startswith('иин') or 'иин' in row_lower:
                match = re.search(r'ИИН\s*(\d{12})', row_text, re.IGNORECASE)
                if match:
                    metadata.client_bin_iin = match.group(1)

            # Счёт
            if 'счет' in row_lower:
                match = re.search(r'(KZ\w+)', row_text)
                if match:
                    metadata.account_number = match.group(1)

            # Период
            if 'период формирования' in row_lower:
                dates = re.findall(r'(\d{2}\.\d{2}\.\d{4})', row_text)
                if len(dates) >= 2:
                    metadata.period_start = self._parse_date(dates[0], ['%d.%m.%Y'])
                    metadata.period_end = self._parse_date(dates[1], ['%d.%m.%Y'])

            # Ищем строку заголовков
            if 'дата транзакции' in row_lower or 'дата постирования' in row_lower:
                header_row = idx
                break

        if header_row is None:
            self.metadata = metadata
            self.transactions = []
            return metadata, []

        # Читаем данные с заголовками
        try:
            df = pd.read_excel(self.file_path, header=header_row, engine='xlrd')
        except:
            df = pd.read_excel(self.file_path, header=header_row)

        # Маппинг колонок
        col_map = {}
        for col in df.columns:
            col_lower = str(col).lower().strip()
            if 'дата транзакции' in col_lower:
                col_map['date'] = col
            elif 'дата постирования' in col_lower:
                col_map['posting_date'] = col
            elif 'карта' in col_lower or 'счет' in col_lower:
                col_map['card'] = col
            elif 'описание' in col_lower:
                col_map['description'] = col
            elif 'тип транзакции' in col_lower:
                col_map['transaction_type'] = col
            elif 'сумма операции кредит' in col_lower or 'кредит' in col_lower:
                col_map['credit'] = col
            elif 'сумма операции дебет' in col_lower or 'дебет' in col_lower:
                col_map['debit'] = col

        transactions = []

        for idx, row in df.iterrows():
            try:
                date_val = row.get(col_map.get('date')) or row.get(col_map.get('posting_date'))
                if pd.isna(date_val):
                    continue

                date = self._parse_date(date_val, ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d.%m.%Y'])
                if date is None:
                    continue

                # Определяем сумму и направление
                credit = self._parse_decimal(row.get(col_map.get('credit')))
                debit = self._parse_decimal(row.get(col_map.get('debit')))

                if credit and credit > 0:
                    direction = 'income'
                    amount = credit
                elif debit and debit > 0:
                    direction = 'expense'
                    amount = debit
                else:
                    continue

                if amount == 0:
                    continue

                description = self._clean_string(row.get(col_map.get('description'), ''))
                transaction_type = self._clean_string(row.get(col_map.get('transaction_type'), ''))

                # Определяем валюту из последней колонки (обычно KZT)
                currency = 'KZT'
                for val in row:
                    if pd.notna(val) and str(val).strip() in ['KZT', 'USD', 'EUR', 'RUB']:
                        currency = str(val).strip()

                transaction = UnifiedTransaction(
                    date=date,
                    amount=abs(amount),
                    currency=currency,
                    amount_kzt=abs(amount) if currency == 'KZT' else None,
                    direction=direction,
                    payer_name=metadata.client_name if direction == 'expense' else '',
                    payer_bin_iin=metadata.client_bin_iin if direction == 'expense' else '',
                    recipient_name='' if direction == 'expense' else metadata.client_name,
                    recipient_bin_iin='' if direction == 'expense' else metadata.client_bin_iin,
                    operation_type=transaction_type,
                    description=description,
                    source_bank=self.BANK_NAME,
                    source_file=self.file_path.name,
                    account_number=metadata.account_number or '',
                )

                transactions.append(transaction)

            except Exception as e:
                print(f"Ошибка парсинга строки {idx}: {e}")
                continue

        self.metadata = metadata
        self.transactions = transactions
        return metadata, transactions
