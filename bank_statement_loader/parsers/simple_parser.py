"""
Универсальный парсер для простых форматов выписок
Формат: xlsx с простой табличной структурой (дата, сумма, контрагент и т.д.)
"""
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import List, Tuple, Union, Optional
import re
import pandas as pd

from .base_parser import BaseParser
from ..models import UnifiedTransaction, StatementMetadata


class SimpleParser(BaseParser):
    """Универсальный парсер для простых табличных форматов"""

    BANK_NAME = "Неизвестный банк"

    def __init__(self, file_path: Union[str, Path]):
        super().__init__(file_path)

    @classmethod
    def can_parse(cls, file_path: Union[str, Path]) -> bool:
        """Проверка, подходит ли файл для универсального парсера"""
        try:
            path = Path(file_path)
            if path.suffix.lower() not in ('.xlsx', '.xls'):
                return False

            # Этот парсер используется как последний вариант
            # Проверяем, что файл содержит хотя бы колонки с датой и суммой
            try:
                df = pd.read_excel(file_path, header=None, nrows=10)
            except:
                try:
                    df = pd.read_excel(file_path, header=None, nrows=10, engine='xlrd')
                except:
                    return False

            # Ищем строку заголовков
            for idx in range(min(8, len(df))):
                row = df.iloc[idx]
                row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))

                # Должны быть колонки с датой и суммой
                has_date = 'дата' in row_text or 'date' in row_text
                has_amount = 'сумма' in row_text or 'amount' in row_text or 'sum' in row_text

                if has_date and has_amount:
                    return True

            return False
        except Exception:
            return False

    def _detect_bank(self, df_raw: pd.DataFrame) -> str:
        """Попытка определить банк по содержимому"""
        for idx in range(min(10, len(df_raw))):
            row = df_raw.iloc[idx]
            row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))

            # Проверяем известные банки
            if 'halyk' in row_text or 'народный' in row_text or 'hsbkkzkx' in row_text:
                return "АО Народный Банк Казахстана"
            if 'kaspi' in row_text or 'каспи' in row_text:
                return "АО Kaspi Bank"
            if 'forte' in row_text or 'форте' in row_text:
                return "АО ForteBank"
            if 'rbk' in row_text or 'рбк' in row_text:
                return "АО Bank RBK"
            if 'centercredit' in row_text or 'центркредит' in row_text:
                return "АО Банк ЦентрКредит"
            if 'eurasian' in row_text or 'евразийский' in row_text:
                return "АО Евразийский Банк"
            if 'freedom' in row_text or 'фридом' in row_text:
                return "АО Freedom Finance Bank"

        return self.BANK_NAME

    def parse(self) -> Tuple[StatementMetadata, List[UnifiedTransaction]]:
        """Парсинг файла"""
        try:
            df_raw = pd.read_excel(self.file_path, header=None)
        except:
            df_raw = pd.read_excel(self.file_path, header=None, engine='xlrd')

        metadata = StatementMetadata()
        metadata.bank_name = self._detect_bank(df_raw)
        metadata.source_file = self.file_path.name

        # Ищем строку заголовков
        header_row = 0
        for idx in range(min(10, len(df_raw))):
            row = df_raw.iloc[idx]
            row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))

            if 'дата' in row_text and ('сумма' in row_text or 'amount' in row_text):
                header_row = idx
                break

        # Читаем данные
        try:
            df = pd.read_excel(self.file_path, header=header_row)
        except:
            df = pd.read_excel(self.file_path, header=header_row, engine='xlrd')

        # Автоматический маппинг колонок
        col_map = {}
        for col in df.columns:
            col_lower = str(col).lower().strip()

            if any(x in col_lower for x in ['дата', 'date']):
                if 'date' not in col_map:
                    col_map['date'] = col

            if any(x in col_lower for x in ['сумма', 'amount', 'sum']):
                if 'тенге' in col_lower or 'kzt' in col_lower:
                    col_map['amount_kzt'] = col
                elif 'дебет' in col_lower:
                    col_map['debit'] = col
                elif 'кредит' in col_lower:
                    col_map['credit'] = col
                elif 'amount' not in col_map:
                    col_map['amount'] = col

            if any(x in col_lower for x in ['отправитель', 'sender', 'плательщик', 'payer']):
                col_map['sender'] = col

            if any(x in col_lower for x in ['получатель', 'recipient', 'beneficiary', 'бенефициар']):
                col_map['recipient'] = col

            if any(x in col_lower for x in ['бин', 'иин', 'bin', 'iin']):
                col_map['bin_iin'] = col

            if any(x in col_lower for x in ['назначение', 'описание', 'description', 'purpose']):
                col_map['description'] = col

            if any(x in col_lower for x in ['валюта', 'currency']):
                col_map['currency'] = col

            if any(x in col_lower for x in ['клиент', 'client', 'фио', 'наименование']):
                col_map['client'] = col

            if any(x in col_lower for x in ['счет', 'account', 'iban']):
                col_map['account'] = col

        transactions = []

        for idx, row in df.iterrows():
            try:
                # Получаем дату
                date_val = row.get(col_map.get('date'))
                if pd.isna(date_val):
                    continue

                date = self._parse_date(date_val)
                if date is None:
                    continue

                # Получаем сумму
                debit = self._parse_decimal(row.get(col_map.get('debit')))
                credit = self._parse_decimal(row.get(col_map.get('credit')))
                amount = self._parse_decimal(row.get(col_map.get('amount')))
                amount_kzt = self._parse_decimal(row.get(col_map.get('amount_kzt')))

                if credit and credit > 0:
                    direction = 'income'
                    final_amount = credit
                elif debit and debit > 0:
                    direction = 'expense'
                    final_amount = debit
                elif amount and amount > 0:
                    direction = 'expense'  # По умолчанию
                    final_amount = amount
                else:
                    continue

                if final_amount == 0:
                    continue

                sender = self._clean_string(row.get(col_map.get('sender'), ''))
                recipient = self._clean_string(row.get(col_map.get('recipient'), ''))
                bin_iin = self._extract_bin_iin(row.get(col_map.get('bin_iin'), ''))
                description = self._clean_string(row.get(col_map.get('description'), ''))
                currency = self._clean_string(row.get(col_map.get('currency'), 'KZT'))
                client = self._clean_string(row.get(col_map.get('client'), ''))
                account = self._clean_string(row.get(col_map.get('account'), ''))

                # Сохраняем метаданные
                if not metadata.client_name and client:
                    metadata.client_name = client
                if not metadata.client_bin_iin and bin_iin:
                    metadata.client_bin_iin = bin_iin
                if not metadata.account_number and account.startswith('KZ'):
                    metadata.account_number = account

                transaction = UnifiedTransaction(
                    date=date,
                    amount=abs(final_amount),
                    currency=currency if currency else 'KZT',
                    amount_kzt=abs(amount_kzt) if amount_kzt else (abs(final_amount) if currency == 'KZT' else None),
                    direction=direction,
                    payer_name=sender if sender else client,
                    payer_bin_iin=bin_iin if direction == 'expense' else '',
                    recipient_name=recipient,
                    recipient_bin_iin=bin_iin if direction == 'income' else '',
                    description=description,
                    source_bank=metadata.bank_name,
                    source_file=self.file_path.name,
                    account_number=account,
                )

                transactions.append(transaction)

            except Exception as e:
                continue

        self.metadata = metadata
        self.transactions = transactions
        return metadata, transactions
