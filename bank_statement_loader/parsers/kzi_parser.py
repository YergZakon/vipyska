"""
Парсер выписок КЗИ Банк (ДБ КЗИ БАНК)
Формат: xlsx/xls с различными форматами запросов
"""
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import List, Tuple, Union, Optional
import re
import pandas as pd

from .base_parser import BaseParser
from ..models import UnifiedTransaction, StatementMetadata


class KZIParser(BaseParser):
    """Парсер выписок КЗИ Банк"""

    BANK_NAME = "АО ДБ КЗИ БАНК"
    BANK_ALIASES = ['кзи банк', 'kzi bank', 'кзи']

    def __init__(self, file_path: Union[str, Path]):
        super().__init__(file_path)

    @classmethod
    def can_parse(cls, file_path: Union[str, Path]) -> bool:
        """Проверка, подходит ли файл для парсера КЗИ Банк"""
        try:
            path = Path(file_path)
            if path.suffix.lower() not in ('.xlsx', '.xls'):
                return False

            # Проверяем по папке или имени файла
            if 'кзи банк' in str(path).lower():
                return True

            try:
                df = pd.read_excel(file_path, header=None, nrows=10)
            except:
                try:
                    df = pd.read_excel(file_path, header=None, nrows=10, engine='xlrd')
                except:
                    return False

            for idx in range(min(5, len(df))):
                row = df.iloc[idx]
                row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
                if 'кзи банк' in row_text:
                    return True

            return False
        except Exception:
            return False

    def parse(self) -> Tuple[StatementMetadata, List[UnifiedTransaction]]:
        """Парсинг файла КЗИ Банк"""
        try:
            df_raw = pd.read_excel(self.file_path, header=None)
        except:
            df_raw = pd.read_excel(self.file_path, header=None, engine='xlrd')

        metadata = StatementMetadata()
        metadata.bank_name = self.BANK_NAME
        metadata.source_file = self.file_path.name

        # Ищем строку заголовков
        header_row = None
        for idx in range(min(10, len(df_raw))):
            row = df_raw.iloc[idx]
            row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))

            # Разные форматы заголовков
            if 'дата транзакции' in row_text and 'иин' in row_text:
                header_row = idx
                break
            if '№' in row_text and 'дата' in row_text and 'сумма' in row_text:
                header_row = idx
                break

        if header_row is None:
            # Пробуем читать с первой строки
            header_row = 0
            for idx in range(min(5, len(df_raw))):
                row = df_raw.iloc[idx]
                # Проверяем, есть ли это заголовок с номером
                first_val = str(row.iloc[0]).strip().lower() if pd.notna(row.iloc[0]) else ''
                if first_val == '№' or first_val == 'номер':
                    header_row = idx
                    break

        # Читаем данные с заголовками
        try:
            df = pd.read_excel(self.file_path, header=header_row)
        except:
            df = pd.read_excel(self.file_path, header=header_row, engine='xlrd')

        # Маппинг колонок
        col_map = {}
        for col in df.columns:
            col_lower = str(col).lower().strip()
            if col_lower == '№' or 'номер' in col_lower:
                col_map['number'] = col
            elif 'дата транзакции' in col_lower or col_lower == 'дата':
                col_map['date'] = col
            elif col_lower == 'иин' or 'иин клиента' in col_lower:
                col_map['iin'] = col
            elif 'номер счета' in col_lower or 'счет' in col_lower:
                col_map['account'] = col
            elif 'держатель' in col_lower or 'фио' in col_lower:
                col_map['holder_name'] = col
            elif 'отправитель' in col_lower:
                col_map['sender'] = col
            elif 'получатель' in col_lower:
                col_map['recipient'] = col
            elif 'сумма' in col_lower and 'тенге' in col_lower:
                col_map['amount_kzt'] = col
            elif 'сумма' in col_lower:
                col_map['amount'] = col
            elif 'валюта' in col_lower:
                col_map['currency'] = col
            elif 'назначение' in col_lower or 'описание' in col_lower:
                col_map['description'] = col

        transactions = []

        for idx, row in df.iterrows():
            try:
                # Пропускаем строки с нумерацией заголовков (1, 2, 3...)
                first_val = row.iloc[0] if len(row) > 0 else None
                if pd.notna(first_val):
                    first_str = str(first_val).strip()
                    # Пропускаем если это номер колонки (1, 2, 3...) в заголовке
                    if first_str.isdigit() and int(first_str) <= 20:
                        # Проверяем, не является ли вторая колонка тоже числом
                        second_val = row.iloc[1] if len(row) > 1 else None
                        if pd.notna(second_val) and str(second_val).strip().isdigit():
                            continue

                date_val = row.get(col_map.get('date'))
                if pd.isna(date_val):
                    continue

                # Парсим дату в формате ISO
                date = self._parse_date(date_val, ['%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%d', '%d.%m.%Y'])
                if date is None:
                    continue

                # Получаем сумму
                amount = self._parse_decimal(row.get(col_map.get('amount')))
                amount_kzt = self._parse_decimal(row.get(col_map.get('amount_kzt')))

                if amount is None and amount_kzt:
                    amount = amount_kzt
                if amount is None or amount == 0:
                    continue

                # Определяем направление по отправителю/получателю
                sender = self._clean_string(row.get(col_map.get('sender'), ''))
                recipient = self._clean_string(row.get(col_map.get('recipient'), ''))
                holder_name = self._clean_string(row.get(col_map.get('holder_name'), ''))
                iin = self._extract_bin_iin(row.get(col_map.get('iin'), ''))
                account = self._clean_string(row.get(col_map.get('account'), ''))

                # Сохраняем метаданные
                if not metadata.client_name and holder_name:
                    metadata.client_name = holder_name
                if not metadata.client_bin_iin and iin:
                    metadata.client_bin_iin = iin
                if not metadata.account_number and account.startswith('KZ'):
                    metadata.account_number = account

                # Определяем направление
                # Если отправитель = держатель карты, то это расход
                if sender and holder_name and sender.lower() in holder_name.lower():
                    direction = 'expense'
                elif recipient and holder_name and recipient.lower() in holder_name.lower():
                    direction = 'income'
                else:
                    direction = 'expense'  # По умолчанию

                description = self._clean_string(row.get(col_map.get('description'), ''))
                if not description:
                    description = f"{sender} -> {recipient}"

                currency = self._clean_string(row.get(col_map.get('currency'), 'KZT'))

                transaction = UnifiedTransaction(
                    date=date,
                    amount=abs(amount),
                    currency=currency,
                    amount_kzt=abs(amount_kzt) if amount_kzt else (abs(amount) if currency == 'KZT' else None),
                    direction=direction,
                    payer_name=sender if sender else holder_name,
                    payer_bin_iin=iin if direction == 'expense' else '',
                    recipient_name=recipient,
                    recipient_bin_iin='' if direction == 'expense' else iin,
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
