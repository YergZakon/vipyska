"""
Парсер выписок Банк Kassa Nova
Формат: xlsx с "Входящие платежи" в строке 1, заголовки в строке 3, данные с строки 4
"""
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import List, Tuple, Union, Optional
import re
import pandas as pd

from .base_parser import BaseParser
from ..models import UnifiedTransaction, StatementMetadata


class KassaNovaParser(BaseParser):
    """Парсер выписок Банк Kassa Nova"""

    BANK_NAME = "АО Банк Kassa Nova"
    BANK_ALIASES = ['kassa nova', 'касса нова', 'kassanova']

    def __init__(self, file_path: Union[str, Path]):
        super().__init__(file_path)

    @classmethod
    def can_parse(cls, file_path: Union[str, Path]) -> bool:
        """Проверка, подходит ли файл для парсера Kassa Nova"""
        try:
            path = Path(file_path)
            if path.suffix.lower() not in ('.xlsx', '.xls'):
                return False

            df = pd.read_excel(file_path, header=None, nrows=10)

            for idx in range(min(5, len(df))):
                row = df.iloc[idx]
                row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
                # Характерная структура: "Входящие платежи" или "Исходящие платежи" как заголовок секции
                if 'входящие платежи' in row_text or 'исходящие платежи' in row_text:
                    # Проверяем что есть колонка "Наименование бенефициара/отправителя"
                    for idx2 in range(idx, min(idx + 5, len(df))):
                        row2 = df.iloc[idx2]
                        row_text2 = ' '.join(str(v).lower() for v in row2 if pd.notna(v))
                        if 'бенефициара' in row_text2 or 'отправителя' in row_text2:
                            return True

            return False
        except Exception:
            return False

    def parse(self) -> Tuple[StatementMetadata, List[UnifiedTransaction]]:
        """Парсинг файла выписки Kassa Nova"""
        df_raw = pd.read_excel(self.file_path, header=None)

        metadata = StatementMetadata()
        metadata.bank_name = self.BANK_NAME
        metadata.source_file = self.file_path.name

        # Находим секции "Входящие платежи" и "Исходящие платежи"
        incoming_start = None
        outgoing_start = None

        for idx in range(len(df_raw)):
            row = df_raw.iloc[idx]
            row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
            if 'входящие платежи' in row_text and incoming_start is None:
                incoming_start = idx
            elif 'исходящие платежи' in row_text:
                outgoing_start = idx

        transactions = []

        # Парсим входящие платежи
        if incoming_start is not None:
            end_row = outgoing_start if outgoing_start else len(df_raw)
            incoming_txns = self._parse_section(df_raw, incoming_start, end_row, 'income')
            transactions.extend(incoming_txns)

        # Парсим исходящие платежи
        if outgoing_start is not None:
            outgoing_txns = self._parse_section(df_raw, outgoing_start, len(df_raw), 'expense')
            transactions.extend(outgoing_txns)

        self.metadata = metadata
        self.transactions = transactions
        return metadata, transactions

    def _parse_section(self, df: pd.DataFrame, start_row: int, end_row: int, direction: str) -> List[UnifiedTransaction]:
        """Парсинг секции (входящие или исходящие)"""
        transactions = []

        # Находим строку с заголовками
        header_row = start_row + 2
        for idx in range(start_row, min(start_row + 5, end_row)):
            row = df.iloc[idx]
            row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
            if 'дата операции' in row_text:
                header_row = idx
                break

        # Маппинг колонок
        col_map = {}
        header = df.iloc[header_row]
        for i, col in enumerate(header):
            if pd.isna(col):
                continue
            col_lower = str(col).lower()
            if 'дата операции' in col_lower:
                col_map['date'] = i
            elif 'наименование' in col_lower and ('бенефициара' in col_lower or 'отправителя' in col_lower):
                col_map['counterparty'] = i
            elif 'бин' in col_lower or 'иин' in col_lower:
                col_map['bin_iin'] = i
            elif 'сумма' in col_lower:
                col_map['amount'] = i
            elif 'назначение' in col_lower:
                col_map['description'] = i

        # Парсим данные
        for idx in range(header_row + 1, end_row):
            row = df.iloc[idx]

            try:
                # Проверяем что это не пустая строка или итоговая строка
                date_val = row.iloc[col_map.get('date', 0)] if 'date' in col_map else None
                if pd.isna(date_val):
                    continue

                # Пропускаем итоговые строки
                if isinstance(date_val, str) and ('итого' in date_val.lower() or 'всего' in date_val.lower()):
                    continue

                date = self._parse_date(date_val, formats=['%d.%m.%y', '%d.%m.%Y', '%Y-%m-%d'])
                if date is None:
                    continue

                amount = self._parse_decimal(row.iloc[col_map.get('amount', 3)] if 'amount' in col_map else None)
                if amount is None or amount == 0:
                    continue

                counterparty = self._clean_string(row.iloc[col_map.get('counterparty', 1)] if 'counterparty' in col_map else '')
                counterparty_bin = self._extract_bin_iin(row.iloc[col_map.get('bin_iin', 2)] if 'bin_iin' in col_map else '')
                description = self._clean_string(row.iloc[col_map.get('description', 4)] if 'description' in col_map else '')

                # Сохраняем первого контрагента в метаданные как клиента
                if not self.metadata.client_name and direction == 'income':
                    # Для входящих - получатель это клиент
                    pass
                elif not self.metadata.client_name and direction == 'expense':
                    # Для исходящих - отправитель это клиент
                    pass

                if direction == 'income':
                    payer_name = counterparty
                    payer_bin = counterparty_bin
                    recipient_name = ''
                    recipient_bin = ''
                else:
                    payer_name = ''
                    payer_bin = ''
                    recipient_name = counterparty
                    recipient_bin = counterparty_bin

                transaction = UnifiedTransaction(
                    date=date,
                    amount=abs(amount),
                    currency='KZT',
                    amount_kzt=abs(amount),
                    direction=direction,
                    payer_name=payer_name,
                    payer_bin_iin=payer_bin,
                    recipient_name=recipient_name,
                    recipient_bin_iin=recipient_bin,
                    description=description,
                    source_bank=self.BANK_NAME,
                    source_file=self.file_path.name,
                )

                transactions.append(transaction)

            except Exception as e:
                print(f"Ошибка парсинга строки {idx}: {e}")
                continue

        return transactions
