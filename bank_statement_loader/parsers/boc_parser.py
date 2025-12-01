"""
Парсер выписок ДБ Банк Китая в Казахстане (Bank of China)
Формат: xls/xlsx с заголовком "ВЫПИСКА СО СЧЕТА"
"""
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import List, Tuple, Union, Optional
import re
import pandas as pd

from .base_parser import BaseParser
from ..models import UnifiedTransaction, StatementMetadata


class BOCParser(BaseParser):
    """Парсер выписок ДБ Банк Китая в Казахстане"""

    BANK_NAME = "АО ДБ Банк Китая в Казахстане"
    BANK_ALIASES = ['банк китая', 'bank of china', 'bkchkzka']

    def __init__(self, file_path: Union[str, Path]):
        super().__init__(file_path)

    @classmethod
    def can_parse(cls, file_path: Union[str, Path]) -> bool:
        """Проверка, подходит ли файл для парсера Банка Китая"""
        try:
            path = Path(file_path)
            if path.suffix.lower() not in ('.xlsx', '.xls'):
                return False

            df = pd.read_excel(file_path, header=None, nrows=15)

            for idx in range(min(10, len(df))):
                row = df.iloc[idx]
                row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
                if 'банк китая' in row_text:
                    return True
                if 'bkchkzka' in row_text:
                    return True
                if 'bank of china' in row_text:
                    return True

            return False
        except Exception:
            return False

    def parse(self) -> Tuple[StatementMetadata, List[UnifiedTransaction]]:
        """Парсинг файла выписки Банка Китая"""
        df_raw = pd.read_excel(self.file_path, header=None)

        metadata = StatementMetadata()
        metadata.bank_name = self.BANK_NAME
        metadata.source_file = self.file_path.name

        # Извлекаем метаданные из заголовка
        for idx in range(min(15, len(df_raw))):
            row = df_raw.iloc[idx]
            row_text = ' '.join(str(v) for v in row if pd.notna(v))

            # Ищем БИН банка и SWIFT
            if 'бин:' in row_text.lower():
                match = re.search(r'БИН:\s*(\d+)', row_text, re.IGNORECASE)
                if match:
                    pass  # Это БИН банка, не клиента

            # Ищем период
            if 'дата с' in row_text.lower():
                dates = re.findall(r'(\d{4}[/.-]\d{2}[/.-]\d{2})', row_text)
                if len(dates) >= 1:
                    metadata.period_start = self._parse_date(dates[0], ['%Y/%m/%d', '%Y-%m-%d', '%Y.%m.%d'])
                if len(dates) >= 2:
                    metadata.period_end = self._parse_date(dates[1], ['%Y/%m/%d', '%Y-%m-%d', '%Y.%m.%d'])

        # Находим строку с заголовками данных
        header_row = None
        for idx in range(10, min(30, len(df_raw))):
            row = df_raw.iloc[idx]
            row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
            if 'дата' in row_text and 'сумма' in row_text:
                header_row = idx
                break

        if header_row is None:
            # Файл без транзакций или нестандартный формат
            self.metadata = metadata
            self.transactions = []
            return metadata, []

        # Читаем данные с заголовками
        df = pd.read_excel(self.file_path, header=header_row)

        # Маппинг колонок
        col_map = {}
        for col in df.columns:
            col_lower = str(col).lower()
            if 'дата' in col_lower and 'операци' in col_lower:
                col_map['date'] = col
            elif 'дата' in col_lower and 'валют' in col_lower:
                col_map['value_date'] = col
            elif 'сумма' in col_lower and 'дебет' in col_lower:
                col_map['debit'] = col
            elif 'сумма' in col_lower and 'кредит' in col_lower:
                col_map['credit'] = col
            elif 'валюта' in col_lower:
                col_map['currency'] = col
            elif 'назначение' in col_lower or 'описание' in col_lower:
                col_map['description'] = col
            elif 'контрагент' in col_lower:
                col_map['counterparty'] = col
            elif 'бин' in col_lower or 'иин' in col_lower:
                col_map['bin_iin'] = col
            elif 'номер документа' in col_lower:
                col_map['doc_number'] = col

        transactions = []

        for idx, row in df.iterrows():
            try:
                date_val = row.get(col_map.get('date'))
                if pd.isna(date_val):
                    continue

                date = self._parse_date(date_val, formats=['%Y.%m.%d', '%Y-%m-%d', '%d.%m.%Y'])
                if date is None:
                    continue

                # Определяем сумму и направление
                debit = self._parse_decimal(row.get(col_map.get('debit')))
                credit = self._parse_decimal(row.get(col_map.get('credit')))

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

                currency = self._clean_string(row.get(col_map.get('currency'), 'KZT'))
                description = self._clean_string(row.get(col_map.get('description'), ''))
                counterparty = self._clean_string(row.get(col_map.get('counterparty'), ''))
                counterparty_bin = self._extract_bin_iin(row.get(col_map.get('bin_iin'), ''))

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
                    currency=currency,
                    amount_kzt=abs(amount) if currency == 'KZT' else None,
                    direction=direction,
                    payer_name=payer_name,
                    payer_bin_iin=payer_bin,
                    recipient_name=recipient_name,
                    recipient_bin_iin=recipient_bin,
                    description=description,
                    document_number=self._clean_string(row.get(col_map.get('doc_number'), '')),
                    source_bank=self.BANK_NAME,
                    source_file=self.file_path.name,
                )

                transactions.append(transaction)

            except Exception as e:
                print(f"Ошибка парсинга строки {idx}: {e}")
                continue

        self.metadata = metadata
        self.transactions = transactions
        return metadata, transactions
