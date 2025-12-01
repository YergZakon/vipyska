"""
Парсер выписок Евразийского Банка
Формат: xlsx с заголовками в строке 1, данные со строки 3
Колонки: ИИН, Тип операции, Номер счета, Дата, Сумма, Валюта, Детали операции
"""
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import List, Tuple, Union, Optional
import re
import pandas as pd

from .base_parser import BaseParser
from ..models import UnifiedTransaction, StatementMetadata


class EurasianParser(BaseParser):
    """Парсер выписок Евразийского Банка"""

    BANK_NAME = "АО Евразийский Банк"
    BANK_ALIASES = ['евразийский', 'eurasian', 'евразийский банк']

    def __init__(self, file_path: Union[str, Path]):
        super().__init__(file_path)

    @classmethod
    def can_parse(cls, file_path: Union[str, Path]) -> bool:
        """Проверка, подходит ли файл для парсера Евразийского банка"""
        try:
            path = Path(file_path)
            if path.suffix.lower() not in ('.xlsx', '.xls'):
                return False

            df = pd.read_excel(file_path, header=None, nrows=5)

            # Проверяем характерные заголовки Евразийского банка
            # Уникальная комбинация: ИИН + Тип операции + Детали операции (без "Дата и время")
            for idx in range(min(3, len(df))):
                row = df.iloc[idx]
                row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))

                # Евразийский имеет "Тип операции" и "Детали операции" одновременно
                if 'тип операции' in row_text and 'детали операции' in row_text:
                    # Исключаем форматы других банков
                    if 'дата и время операции' not in row_text:  # Это формат Фридом/Халык
                        return True

                # Также: ИИН как первая колонка + "Номер счета" + формат даты DD/MM/YYYY
                headers = [str(v).lower() for v in row if pd.notna(v)]
                if headers and headers[0] == 'иин' and 'номер счета' in row_text:
                    return True

            return False
        except Exception:
            return False

    def _parse_amount_with_spaces(self, value: any) -> Optional[Decimal]:
        """Парсинг суммы с неразрывными пробелами"""
        if pd.isna(value) or value is None:
            return None

        str_value = str(value).strip()
        # Удаляем неразрывные пробелы и обычные пробелы
        str_value = str_value.replace('\xa0', '').replace(' ', '')
        # Заменяем запятую на точку
        str_value = str_value.replace(',', '.')

        try:
            return Decimal(str_value)
        except:
            return None

    def parse(self) -> Tuple[StatementMetadata, List[UnifiedTransaction]]:
        """Парсинг файла выписки Евразийского банка"""
        df_raw = pd.read_excel(self.file_path, header=None)

        metadata = StatementMetadata()
        metadata.bank_name = self.BANK_NAME
        metadata.source_file = self.file_path.name

        # Ищем строку с заголовками
        header_row = 1
        for idx in range(min(5, len(df_raw))):
            row = df_raw.iloc[idx]
            row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
            if 'иин' in row_text and 'тип операции' in row_text:
                header_row = idx
                break

        df = pd.read_excel(self.file_path, header=header_row)

        # Маппинг колонок
        col_map = {}
        for col in df.columns:
            col_lower = str(col).lower()
            if 'иин' in col_lower and 'бин' not in col_lower:
                col_map['iin'] = col
            elif 'тип операции' in col_lower:
                col_map['operation_type'] = col
            elif 'номер счета' in col_lower:
                col_map['account'] = col
            elif col_lower == 'дата':
                col_map['date'] = col
            elif col_lower == 'сумма':
                col_map['amount'] = col
            elif col_lower == 'валюта':
                col_map['currency'] = col
            elif 'детали' in col_lower:
                col_map['description'] = col

        transactions = []

        for idx, row in df.iterrows():
            try:
                # Пропускаем пустые строки
                date_val = row.get(col_map.get('date'))
                if pd.isna(date_val) or str(date_val).strip() == '':
                    continue

                # Специальный формат даты Евразийского банка: DD/MM/YYYY HH:MM:SS
                date = self._parse_date(date_val, formats=[
                    '%d/%m/%Y %H:%M:%S',
                    '%d/%m/%Y',
                    '%Y-%m-%d %H:%M:%S',
                    '%Y-%m-%d',
                    '%d.%m.%Y %H:%M:%S',
                    '%d.%m.%Y',
                ])
                if date is None:
                    continue

                amount = self._parse_amount_with_spaces(row.get(col_map.get('amount')))
                if amount is None or amount == 0:
                    continue

                # Извлекаем ИИН клиента
                iin = self._extract_bin_iin(row.get(col_map.get('iin'), ''))
                if not metadata.client_bin_iin and iin:
                    metadata.client_bin_iin = iin

                account = self._clean_string(row.get(col_map.get('account'), ''))
                if not metadata.account_number and account:
                    metadata.account_number = account

                currency = self._clean_string(row.get(col_map.get('currency'), 'KZT'))
                operation_type = self._clean_string(row.get(col_map.get('operation_type'), ''))
                description = self._clean_string(row.get(col_map.get('description'), ''))

                # Определяем направление по типу операции
                op_lower = operation_type.lower()
                if 'payment to client' in op_lower or 'зачисление' in op_lower or 'пополн' in op_lower:
                    direction = 'income'
                elif 'снятие' in op_lower or 'списание' in op_lower or 'cash' in op_lower:
                    direction = 'expense'
                else:
                    direction = 'income'  # По умолчанию

                transaction = UnifiedTransaction(
                    date=date,
                    amount=abs(amount),
                    currency=currency,
                    amount_kzt=abs(amount) if currency == 'KZT' else None,
                    direction=direction,
                    payer_name='',
                    payer_bin_iin=iin if direction == 'expense' else '',
                    recipient_name='',
                    recipient_bin_iin=iin if direction == 'income' else '',
                    operation_type=operation_type,
                    description=description,
                    source_bank=self.BANK_NAME,
                    source_file=self.file_path.name,
                    account_number=account,
                )

                transactions.append(transaction)

            except Exception as e:
                print(f"Ошибка парсинга строки {idx}: {e}")
                continue

        self.metadata = metadata
        self.transactions = transactions
        return metadata, transactions
