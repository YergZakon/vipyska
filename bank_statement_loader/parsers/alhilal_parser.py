"""
Парсер выписок Исламский Банк Al Hilal
Формат: xls с платёжными документами
"""
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import List, Tuple, Union, Optional
import re
import pandas as pd

from .base_parser import BaseParser
from ..models import UnifiedTransaction, StatementMetadata


class AlHilalParser(BaseParser):
    """Парсер выписок Исламский Банк Al Hilal"""

    BANK_NAME = "АО Исламский Банк Al Hilal"
    BANK_ALIASES = ['al hilal', 'аль хилал', 'hlalkzkz']

    def __init__(self, file_path: Union[str, Path]):
        super().__init__(file_path)

    @classmethod
    def can_parse(cls, file_path: Union[str, Path]) -> bool:
        """Проверка, подходит ли файл для парсера Al Hilal"""
        try:
            path = Path(file_path)
            if path.suffix.lower() not in ('.xlsx', '.xls'):
                return False

            try:
                df = pd.read_excel(file_path, header=None, nrows=10, engine='xlrd')
            except:
                df = pd.read_excel(file_path, header=None, nrows=10)

            for idx in range(min(5, len(df))):
                row = df.iloc[idx]
                row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
                # Характерная структура Al Hilal
                if 'инициатор документа' in row_text and 'бенефециар' in row_text:
                    return True
                if 'hlalkzkz' in row_text:
                    return True
                if 'исходящие платежи' in row_text and 'подтверждение дебета' in row_text:
                    return True

            return False
        except Exception:
            return False

    def parse(self) -> Tuple[StatementMetadata, List[UnifiedTransaction]]:
        """Парсинг файла выписки Al Hilal"""
        try:
            df_raw = pd.read_excel(self.file_path, header=None, engine='xlrd')
        except:
            df_raw = pd.read_excel(self.file_path, header=None)

        metadata = StatementMetadata()
        metadata.bank_name = self.BANK_NAME
        metadata.source_file = self.file_path.name

        # Определяем направление из заголовка
        default_direction = 'expense'
        for idx in range(min(5, len(df_raw))):
            row = df_raw.iloc[idx]
            row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
            if 'исходящие' in row_text:
                default_direction = 'expense'
                break
            if 'входящие' in row_text:
                default_direction = 'income'
                break

        # Ищем строку заголовков - она имеет две строки
        # Строка 0: Инициатор документа | Бенефециар документа | Платежный документ | Свойства
        # Строка 1: КОд | Отправитель (Счет) | Отправитель (РНН) | ...

        header_row = None
        for idx in range(min(5, len(df_raw))):
            row = df_raw.iloc[idx]
            row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
            if 'отправитель' in row_text and 'счет' in row_text:
                header_row = idx
                break

        if header_row is None:
            # Пробуем читать с заголовком 1
            header_row = 1

        # Читаем данные с заголовками
        try:
            df = pd.read_excel(self.file_path, header=header_row, engine='xlrd')
        except:
            df = pd.read_excel(self.file_path, header=header_row)

        # Маппинг колонок - колонки могут иметь сложные названия
        col_map = {}
        for i, col in enumerate(df.columns):
            col_lower = str(col).lower().strip()
            if 'отправитель' in col_lower and 'счет' in col_lower:
                col_map['sender_account'] = col
            elif 'отправитель' in col_lower and 'рнн' in col_lower:
                col_map['sender_rnn'] = col
            elif 'отправитель' in col_lower and 'наименование' in col_lower:
                col_map['sender_name'] = col
            elif 'отправитель' in col_lower and 'бик' in col_lower:
                col_map['sender_bik'] = col
            elif 'получатель' in col_lower and 'счет' in col_lower:
                col_map['recipient_account'] = col
            elif 'получатель' in col_lower and 'рнн' in col_lower:
                col_map['recipient_rnn'] = col
            elif 'получатель' in col_lower and 'наименование' in col_lower:
                col_map['recipient_name'] = col
            elif 'получатель' in col_lower and 'бик' in col_lower:
                col_map['recipient_bik'] = col
            elif 'сумма' in col_lower:
                col_map['amount'] = col
            elif 'валюта' in col_lower:
                col_map['currency'] = col
            elif 'дата' in col_lower:
                col_map['date'] = col
            elif 'назначение' in col_lower:
                col_map['description'] = col
            elif col_lower == 'код' or col_lower == 'кбе':
                col_map['kbe'] = col

        transactions = []

        for idx, row in df.iterrows():
            try:
                # Пропускаем строки с итогами или заголовками подразделов
                first_val = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ''
                if first_val.startswith(' - ') or 'итого' in first_val.lower():
                    continue

                # Получаем дату
                date_val = row.get(col_map.get('date'))
                date = None
                if pd.notna(date_val):
                    date = self._parse_date(date_val, ['%Y-%m-%d', '%d.%m.%Y'])

                # Если даты нет в колонке, пробуем найти в других местах
                if date is None:
                    for val in row:
                        if pd.notna(val):
                            date = self._parse_date(val, ['%Y-%m-%d', '%d.%m.%Y'])
                            if date:
                                break

                if date is None:
                    continue

                # Получаем сумму
                amount = self._parse_decimal(row.get(col_map.get('amount')))
                if amount is None or amount == 0:
                    # Ищем сумму в других колонках
                    for val in row:
                        if pd.notna(val):
                            amt = self._parse_decimal(val)
                            if amt and amt > 100:  # Предполагаем что сумма > 100
                                amount = amt
                                break

                if amount is None or amount == 0:
                    continue

                sender_name = self._clean_string(row.get(col_map.get('sender_name'), ''))
                sender_account = self._clean_string(row.get(col_map.get('sender_account'), ''))
                sender_rnn = self._extract_bin_iin(row.get(col_map.get('sender_rnn'), ''))

                recipient_name = self._clean_string(row.get(col_map.get('recipient_name'), ''))
                recipient_account = self._clean_string(row.get(col_map.get('recipient_account'), ''))
                recipient_rnn = self._extract_bin_iin(row.get(col_map.get('recipient_rnn'), ''))

                description = self._clean_string(row.get(col_map.get('description'), ''))

                # Сохраняем первого отправителя в метаданные
                if not metadata.client_name and sender_name:
                    metadata.client_name = sender_name
                if not metadata.client_bin_iin and sender_rnn:
                    metadata.client_bin_iin = sender_rnn
                if not metadata.account_number and sender_account.startswith('KZ'):
                    metadata.account_number = sender_account

                direction = default_direction

                if direction == 'expense':
                    payer_name = sender_name
                    payer_bin = sender_rnn
                    payer_account = sender_account
                    rec_name = recipient_name
                    rec_bin = recipient_rnn
                    rec_account = recipient_account
                else:
                    payer_name = recipient_name
                    payer_bin = recipient_rnn
                    payer_account = recipient_account
                    rec_name = sender_name
                    rec_bin = sender_rnn
                    rec_account = sender_account

                transaction = UnifiedTransaction(
                    date=date,
                    amount=abs(amount),
                    currency='KZT',
                    amount_kzt=abs(amount),
                    direction=direction,
                    payer_name=payer_name,
                    payer_bin_iin=payer_bin,
                    payer_account=payer_account,
                    recipient_name=rec_name,
                    recipient_bin_iin=rec_bin,
                    recipient_account=rec_account,
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
