"""
Парсер выписок Altyn Bank (ДБ China CITIC Bank)
Формат: xlsx со стандартным форматом (18 колонок), заголовки в строке 1, данные с строки 3
"""
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import List, Tuple, Union, Optional
import re
import pandas as pd

from .base_parser import BaseParser
from ..models import UnifiedTransaction, StatementMetadata


class AltynParser(BaseParser):
    """Парсер выписок Altyn Bank"""

    BANK_NAME = "АО Altyn Bank"
    BANK_ALIASES = ['altyn', 'алтын', 'altyn bank', 'china citic', 'citic']

    COLUMN_MAPPING = {
        'Дата и время операции': 'date',
        'Валюта': 'currency',
        'Направление': 'direction',
        'Сумма операции': 'amount',
        'Сумма в тенге': 'amount_kzt',
        'Наименование/ФИО плательщика': 'payer_name',
        'ИИН/БИН плательщика': 'payer_bin_iin',
        'Резидентство плательщика': 'payer_residency',
        'Банк плательщика': 'payer_bank',
        'Номер счета плательщика': 'payer_account',
        'Наименование/ФИО получателя': 'recipient_name',
        'ИИН/БИН получателя': 'recipient_bin_iin',
        'Резидентство получателя': 'recipient_residency',
        'Банк получателя': 'recipient_bank',
        'Номер счета получателя': 'recipient_account',
        'Код назначения платежа': 'knp_code',
        'Описание': 'description',
    }

    def __init__(self, file_path: Union[str, Path]):
        super().__init__(file_path)

    @classmethod
    def can_parse(cls, file_path: Union[str, Path]) -> bool:
        """Проверка, подходит ли файл для парсера Altyn Bank"""
        try:
            path = Path(file_path)
            if path.suffix.lower() not in ('.xlsx', '.xls'):
                return False

            df = pd.read_excel(file_path, header=None, nrows=10)

            for idx in range(min(5, len(df))):
                row = df.iloc[idx]
                row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
                if 'altyn bank' in row_text or 'altyn' in row_text:
                    return True
                if 'china citic' in row_text or 'citic' in row_text:
                    return True
                # Характерный БИК
                if 'atynkzka' in row_text:
                    return True

            return False
        except Exception:
            return False

    def parse(self) -> Tuple[StatementMetadata, List[UnifiedTransaction]]:
        """Парсинг файла выписки Altyn Bank"""
        df_raw = pd.read_excel(self.file_path, header=None)

        metadata = StatementMetadata()
        metadata.bank_name = self.BANK_NAME
        metadata.source_file = self.file_path.name

        # Находим строку с заголовками
        header_row = 1
        for idx in range(min(5, len(df_raw))):
            row = df_raw.iloc[idx]
            row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
            if 'дата и время' in row_text or 'дата операции' in row_text:
                header_row = idx
                break

        # Читаем данные с заголовками
        df = pd.read_excel(self.file_path, header=header_row)

        # Маппинг колонок
        col_map = {}
        for col in df.columns:
            col_str = str(col).strip()
            col_lower = col_str.lower()

            for expected, key in self.COLUMN_MAPPING.items():
                if expected.lower() in col_lower:
                    col_map[key] = col
                    break

            # Дополнительные проверки
            if 'дата' in col_lower and 'время' in col_lower:
                col_map['date'] = col
            elif col_lower == 'валюта':
                col_map['currency'] = col
            elif col_lower == 'направление':
                col_map['direction'] = col
            elif 'сумма операции' in col_lower:
                col_map['amount'] = col
            elif 'сумма в тенге' in col_lower:
                col_map['amount_kzt'] = col
            elif 'плательщик' in col_lower and 'наименование' in col_lower:
                col_map['payer_name'] = col
            elif 'плательщик' in col_lower and ('иин' in col_lower or 'бин' in col_lower):
                col_map['payer_bin_iin'] = col
            elif 'плательщик' in col_lower and 'банк' in col_lower:
                col_map['payer_bank'] = col
            elif 'плательщик' in col_lower and 'счет' in col_lower:
                col_map['payer_account'] = col
            elif 'получател' in col_lower and 'наименование' in col_lower:
                col_map['recipient_name'] = col
            elif 'получател' in col_lower and ('иин' in col_lower or 'бин' in col_lower):
                col_map['recipient_bin_iin'] = col
            elif 'получател' in col_lower and 'банк' in col_lower:
                col_map['recipient_bank'] = col
            elif 'получател' in col_lower and 'счет' in col_lower:
                col_map['recipient_account'] = col
            elif 'код назначения' in col_lower or col_lower == 'кнп':
                col_map['knp_code'] = col
            elif 'описание' in col_lower or 'назначение' in col_lower:
                col_map['description'] = col

        transactions = []

        # Пропускаем строку с номерами колонок (обычно 1, 2, 3...)
        start_row = 0
        for idx, row in df.iterrows():
            first_val = row.iloc[0] if len(row) > 0 else None
            if pd.notna(first_val) and str(first_val).strip() in ['1', '2']:
                start_row = idx + 1
                break

        for idx, row in df.iterrows():
            if idx < start_row:
                continue

            try:
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

                # Определяем направление
                direction_val = self._clean_string(row.get(col_map.get('direction'), ''))
                if 'входящ' in direction_val.lower():
                    direction = 'income'
                elif 'исходящ' in direction_val.lower():
                    direction = 'expense'
                else:
                    direction = ''

                payer_name = self._clean_string(row.get(col_map.get('payer_name'), ''))
                payer_bin = self._extract_bin_iin(row.get(col_map.get('payer_bin_iin'), ''))
                recipient_name = self._clean_string(row.get(col_map.get('recipient_name'), ''))
                recipient_bin = self._extract_bin_iin(row.get(col_map.get('recipient_bin_iin'), ''))

                # Сохраняем клиента в метаданные
                if not metadata.client_name:
                    if direction == 'income' and recipient_name:
                        metadata.client_name = recipient_name
                        metadata.client_bin_iin = recipient_bin
                    elif direction == 'expense' and payer_name:
                        metadata.client_name = payer_name
                        metadata.client_bin_iin = payer_bin

                transaction = UnifiedTransaction(
                    date=date,
                    amount=abs(amount),
                    currency=currency,
                    amount_kzt=abs(amount_kzt) if amount_kzt else None,
                    direction=direction,
                    payer_name=payer_name,
                    payer_bin_iin=payer_bin,
                    payer_bank=self._clean_string(row.get(col_map.get('payer_bank'), '')),
                    payer_account=self._clean_string(row.get(col_map.get('payer_account'), '')),
                    payer_residency=self._clean_string(row.get(col_map.get('payer_residency'), '')),
                    recipient_name=recipient_name,
                    recipient_bin_iin=recipient_bin,
                    recipient_bank=self._clean_string(row.get(col_map.get('recipient_bank'), '')),
                    recipient_account=self._clean_string(row.get(col_map.get('recipient_account'), '')),
                    recipient_residency=self._clean_string(row.get(col_map.get('recipient_residency'), '')),
                    knp_code=self._clean_string(row.get(col_map.get('knp_code'), '')),
                    description=self._clean_string(row.get(col_map.get('description'), '')),
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
