"""
Парсер выписок Отбасы банка (Жилстройсбербанк)
Формат: xlsx с метаданными в строках 0-11, заголовками в строке 12, данные со строки 14
"""
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import List, Tuple, Union, Optional
import re
import pandas as pd

from .base_parser import BaseParser
from ..models import UnifiedTransaction, StatementMetadata


class OtbasyParser(BaseParser):
    """Парсер выписок Отбасы банка"""

    BANK_NAME = "АО Отбасы банк"
    BANK_ALIASES = ['отбасы', 'жилстрой', 'жилищный строительный', 'hcskkzka']

    def __init__(self, file_path: Union[str, Path]):
        super().__init__(file_path)

    @classmethod
    def can_parse(cls, file_path: Union[str, Path]) -> bool:
        """Проверка, подходит ли файл для парсера Отбасы банка"""
        try:
            path = Path(file_path)
            if path.suffix.lower() not in ('.xlsx', '.xls'):
                return False

            df = pd.read_excel(file_path, header=None, nrows=10)

            for idx in range(min(10, len(df))):
                row = df.iloc[idx]
                row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
                if any(alias in row_text for alias in cls.BANK_ALIASES):
                    return True
                if 'hcskkzka' in row_text:
                    return True

            return False
        except Exception:
            return False

    def _parse_metadata(self, df: pd.DataFrame) -> StatementMetadata:
        """Извлечение метаданных"""
        metadata = StatementMetadata()
        metadata.bank_name = self.BANK_NAME
        metadata.source_file = self.file_path.name

        for idx in range(min(15, len(df))):
            row = df.iloc[idx]
            row_text = ' '.join(str(v) for v in row if pd.notna(v))

            # Имя клиента
            if 'фио клиента' in row_text.lower():
                match = re.search(r'фио клиента[:\s]+(.+)', row_text, re.IGNORECASE)
                if match:
                    metadata.client_name = match.group(1).strip()

            # ИИН
            if 'иин:' in row_text.lower():
                match = re.search(r'\d{12}', row_text)
                if match:
                    metadata.client_bin_iin = match.group()

            # Номер счёта
            if '№ счета' in row_text.lower() or 'номер счета' in row_text.lower():
                match = re.search(r'KZ\w{18,22}', row_text)
                if match:
                    metadata.account_number = match.group()

            # Период
            if 'за период' in row_text.lower():
                dates = re.findall(r'\d{2}\.\d{2}\.\d{4}', row_text)
                if len(dates) >= 2:
                    metadata.period_start = self._parse_date(dates[0])
                    metadata.period_end = self._parse_date(dates[1])

        return metadata

    def parse(self) -> Tuple[StatementMetadata, List[UnifiedTransaction]]:
        """Парсинг файла выписки Отбасы банка"""
        df_raw = pd.read_excel(self.file_path, header=None)

        self.metadata = self._parse_metadata(df_raw)

        # Ищем строку с заголовками
        header_row = 12
        for idx in range(min(20, len(df_raw))):
            row = df_raw.iloc[idx]
            row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
            if 'дата и время операции' in row_text and 'валюта операции' in row_text:
                header_row = idx
                break

        df = pd.read_excel(self.file_path, header=header_row)

        # Маппинг колонок (стандартный формат)
        col_map = {}
        for col in df.columns:
            col_lower = str(col).lower()
            if 'дата' in col_lower and 'операци' in col_lower:
                col_map['date'] = col
            elif 'валюта операции' in col_lower:
                col_map['currency'] = col
            elif 'виды операции' in col_lower or 'категория' in col_lower:
                col_map['operation_type'] = col
            elif 'сумма в валюте' in col_lower:
                col_map['amount'] = col
            elif 'сумма в тенге' in col_lower:
                col_map['amount_kzt'] = col
            elif 'наименование' in col_lower and 'плательщик' in col_lower:
                col_map['payer_name'] = col
            elif 'иин' in col_lower and 'плательщик' in col_lower:
                col_map['payer_bin'] = col
            elif 'банк' in col_lower and 'плательщик' in col_lower:
                col_map['payer_bank'] = col
            elif 'наименование' in col_lower and 'получатель' in col_lower:
                col_map['recipient_name'] = col
            elif 'иин' in col_lower and 'получатель' in col_lower:
                col_map['recipient_bin'] = col
            elif 'банк' in col_lower and 'получатель' in col_lower:
                col_map['recipient_bank'] = col
            elif 'назначение' in col_lower:
                col_map['description'] = col

        transactions = []

        for idx, row in df.iterrows():
            try:
                # Пропускаем строки с номерами колонок
                first_val = str(row.iloc[0]).strip() if len(row) > 0 else ''
                if first_val.isdigit() and int(first_val) < 20:
                    continue

                date_val = row.get(col_map.get('date'))
                if pd.isna(date_val):
                    continue

                date = self._parse_date(date_val)
                if date is None:
                    continue

                amount = self._parse_decimal(row.get(col_map.get('amount')))
                if amount is None:
                    continue

                amount_kzt = self._parse_decimal(row.get(col_map.get('amount_kzt')))
                currency = self._clean_string(row.get(col_map.get('currency'), 'KZT'))

                payer_name = self._clean_string(row.get(col_map.get('payer_name'), ''))
                recipient_name = self._clean_string(row.get(col_map.get('recipient_name'), ''))

                # Определяем направление
                if self.metadata.client_name and self.metadata.client_name.lower() in recipient_name.lower():
                    direction = 'income'
                elif self.metadata.client_name and self.metadata.client_name.lower() in payer_name.lower():
                    direction = 'expense'
                else:
                    direction = 'income'

                transaction = UnifiedTransaction(
                    date=date,
                    amount=abs(amount),
                    currency=currency,
                    amount_kzt=abs(amount_kzt) if amount_kzt else None,
                    direction=direction,
                    payer_name=payer_name,
                    payer_bin_iin=self._extract_bin_iin(row.get(col_map.get('payer_bin'), '')),
                    payer_bank=self._clean_string(row.get(col_map.get('payer_bank'), '')),
                    recipient_name=recipient_name,
                    recipient_bin_iin=self._extract_bin_iin(row.get(col_map.get('recipient_bin'), '')),
                    recipient_bank=self._clean_string(row.get(col_map.get('recipient_bank'), '')),
                    operation_type=self._clean_string(row.get(col_map.get('operation_type'), '')),
                    description=self._clean_string(row.get(col_map.get('description'), '')),
                    source_bank=self.BANK_NAME,
                    source_file=self.file_path.name,
                    account_number=self.metadata.account_number,
                )

                transactions.append(transaction)

            except Exception as e:
                print(f"Ошибка парсинга строки {idx}: {e}")
                continue

        self.transactions = transactions
        return self.metadata, transactions
