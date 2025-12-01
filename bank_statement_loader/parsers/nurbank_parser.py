"""
Парсер выписок Нурбанка
Формат: xlsx с метаданными в строках 0-7, заголовками в строке 8, данные со строки 10
23 колонки с полной информацией
"""
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import List, Tuple, Union, Optional
import re
import pandas as pd

from .base_parser import BaseParser
from ..models import UnifiedTransaction, StatementMetadata


class NurbankParser(BaseParser):
    """Парсер выписок Нурбанка"""

    BANK_NAME = "АО Нурбанк"
    BANK_ALIASES = ['нурбанк', 'nurbank']

    def __init__(self, file_path: Union[str, Path]):
        super().__init__(file_path)

    @classmethod
    def can_parse(cls, file_path: Union[str, Path]) -> bool:
        """Проверка, подходит ли файл для парсера Нурбанка"""
        try:
            path = Path(file_path)
            if path.suffix.lower() not in ('.xlsx', '.xls'):
                return False

            df = pd.read_excel(file_path, header=None, nrows=12)

            for idx in range(min(10, len(df))):
                row = df.iloc[idx]
                row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
                if 'операции, проведенные в абис' in row_text:
                    return True
                if 'нурбанк' in row_text or 'nurbank' in row_text:
                    return True
                # Проверяем характерные заголовки
                if '№ п/п' in row_text and '№ операции' in row_text and 'категория документа' in row_text:
                    return True

            return False
        except Exception:
            return False

    def _parse_metadata(self, df: pd.DataFrame) -> StatementMetadata:
        """Извлечение метаданных"""
        metadata = StatementMetadata()
        metadata.bank_name = self.BANK_NAME
        metadata.source_file = self.file_path.name

        for idx in range(min(10, len(df))):
            row = df.iloc[idx]
            row_values = [str(v).strip() for v in row if pd.notna(v) and str(v).strip()]

            for i, val in enumerate(row_values):
                val_lower = val.lower()

                if 'начало периода' in val_lower and i + 1 < len(row_values):
                    metadata.period_start = self._parse_date(row_values[i + 1])
                elif 'конец периода' in val_lower and i + 1 < len(row_values):
                    metadata.period_end = self._parse_date(row_values[i + 1])
                elif 'иин' in val_lower and 'бин' in val_lower and i + 1 < len(row_values):
                    metadata.client_bin_iin = self._extract_bin_iin(row_values[i + 1])

        return metadata

    def parse(self) -> Tuple[StatementMetadata, List[UnifiedTransaction]]:
        """Парсинг файла выписки Нурбанка"""
        df_raw = pd.read_excel(self.file_path, header=None)

        self.metadata = self._parse_metadata(df_raw)

        # Ищем строку с заголовками
        header_row = 8
        for idx in range(min(15, len(df_raw))):
            row = df_raw.iloc[idx]
            row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
            if '№ п/п' in row_text and 'дата операции' in row_text:
                header_row = idx
                break

        df = pd.read_excel(self.file_path, header=header_row)

        # Маппинг колонок
        col_map = {}
        for col in df.columns:
            col_lower = str(col).lower()
            if '№ п/п' in col_lower:
                col_map['row_num'] = col
            elif '№ операции' in col_lower:
                col_map['operation_num'] = col
            elif 'дата операции' in col_lower:
                col_map['date'] = col
            elif col_lower == 'валюта':
                col_map['currency'] = col
            elif 'категория документа' in col_lower:
                col_map['operation_type'] = col
            elif 'сумма (вал' in col_lower:
                col_map['amount'] = col
            elif 'сумма (тенге' in col_lower:
                col_map['amount_kzt'] = col
            elif col_lower == 'кнп':
                col_map['knp'] = col
            elif col_lower == 'плательщик':
                col_map['payer_name'] = col
            elif 'резидент' in col_lower and 'плательщик' in col_lower:
                col_map['payer_residency'] = col
            elif 'бин плательщика' in col_lower:
                col_map['payer_bin'] = col
            elif 'банк плательщика' in col_lower:
                col_map['payer_bank'] = col
            elif col_lower == 'получатель':
                col_map['recipient_name'] = col
            elif 'резидент' in col_lower and 'получатель' in col_lower:
                col_map['recipient_residency'] = col
            elif 'бин получателя' in col_lower:
                col_map['recipient_bin'] = col
            elif 'банк получателя' in col_lower:
                col_map['recipient_bank'] = col
            elif 'назначение' in col_lower:
                col_map['description'] = col

        transactions = []

        for idx, row in df.iterrows():
            try:
                # Пропускаем строки с номерами колонок
                row_num = row.get(col_map.get('row_num'))
                if pd.isna(row_num) or str(row_num).strip() in ['', '1', '2', '3'] and idx < header_row + 3:
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

                # Определяем направление по КНП или типу операции
                operation_type = self._clean_string(row.get(col_map.get('operation_type'), ''))
                knp = self._clean_string(row.get(col_map.get('knp'), ''))

                # Если КНП начинается с 3 - это исходящий платёж
                if knp.startswith('3') or 'исход' in operation_type.lower():
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
                    payer_residency=self._clean_string(row.get(col_map.get('payer_residency'), '')),
                    recipient_name=recipient_name,
                    recipient_bin_iin=self._extract_bin_iin(row.get(col_map.get('recipient_bin'), '')),
                    recipient_bank=self._clean_string(row.get(col_map.get('recipient_bank'), '')),
                    recipient_residency=self._clean_string(row.get(col_map.get('recipient_residency'), '')),
                    operation_type=operation_type,
                    knp_code=knp,
                    description=self._clean_string(row.get(col_map.get('description'), '')),
                    document_number=self._clean_string(row.get(col_map.get('operation_num'), '')),
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
