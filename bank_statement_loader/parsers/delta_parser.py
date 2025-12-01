"""
Парсер выписок Delta Bank
Формат: xlsx с несколькими листами (Входящие платежи, Исходящие платежи)
Простая структура: № п/п, Наименование, БИН/ИИН, Дата, Сумма, Назначение
"""
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import List, Tuple, Union, Optional
import re
import pandas as pd

from .base_parser import BaseParser
from ..models import UnifiedTransaction, StatementMetadata


class DeltaParser(BaseParser):
    """Парсер выписок Delta Bank"""

    BANK_NAME = "АО Delta Bank"
    BANK_ALIASES = ['delta', 'дельта', 'delta bank']

    def __init__(self, file_path: Union[str, Path]):
        super().__init__(file_path)

    @classmethod
    def can_parse(cls, file_path: Union[str, Path]) -> bool:
        """Проверка, подходит ли файл для парсера Delta Bank"""
        try:
            path = Path(file_path)
            if path.suffix.lower() not in ('.xlsx', '.xls'):
                return False

            xl = pd.ExcelFile(file_path)
            sheet_names_lower = [s.lower() for s in xl.sheet_names]

            # Характерные листы Delta Bank - именно "платеж в тенге"
            if any('входящие платеж' in s and 'тенге' in s for s in sheet_names_lower):
                return True
            if any('исходящие платеж' in s and 'тенге' in s for s in sheet_names_lower):
                return True
            # Новый формат: "Исходящий платеж, валюта", "Входящий платеж, тенге"
            if any('исходящий платеж' in s for s in sheet_names_lower):
                return True
            if any('входящий платеж' in s for s in sheet_names_lower):
                return True

            # Проверяем содержимое - Delta Bank имеет специфичную структуру
            df = pd.read_excel(file_path, header=None, nrows=5)
            for idx in range(min(5, len(df))):
                row = df.iloc[idx]
                row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
                # Delta Bank имеет "1. Входящие платежи" в тексте
                if '1. входящие платежи' in row_text or '2. исходящие платежи' in row_text:
                    return True
                # Новый формат с "Клиент, ФИО, ИИН"
                if 'клиент,' in row_text and 'иин' in row_text:
                    return True

            return False
        except Exception:
            return False

    def _parse_amount_with_spaces(self, value: any) -> Optional[Decimal]:
        """Парсинг суммы с неразрывными пробелами"""
        if pd.isna(value) or value is None:
            return None

        str_value = str(value).strip()
        str_value = str_value.replace('\xa0', '').replace(' ', '')
        str_value = str_value.replace(',', '.')

        try:
            return Decimal(str_value)
        except:
            return None

    def parse(self) -> Tuple[StatementMetadata, List[UnifiedTransaction]]:
        """Парсинг файла выписки Delta Bank"""
        xl = pd.ExcelFile(self.file_path)

        metadata = StatementMetadata()
        metadata.bank_name = self.BANK_NAME
        metadata.source_file = self.file_path.name

        # Извлекаем название клиента из первой строки первого листа
        df_first = pd.read_excel(self.file_path, sheet_name=0, header=None, nrows=3)
        if len(df_first) > 0:
            first_row = ' '.join(str(v) for v in df_first.iloc[0] if pd.notna(v))
            # Убираем номер пункта если есть
            first_row = re.sub(r'^\d+\.\s*', '', first_row).strip()
            if first_row and 'платеж' not in first_row.lower():
                # Формат "Клиент, ФИО, ИИН..."
                if 'клиент,' in first_row.lower():
                    match = re.search(r'Клиент,\s*([^,]+),\s*ИИН(\d+)', first_row, re.IGNORECASE)
                    if match:
                        metadata.client_name = match.group(1).strip()
                        metadata.client_bin_iin = match.group(2)
                else:
                    metadata.client_name = first_row

        transactions = []

        for sheet_name in xl.sheet_names:
            sheet_lower = sheet_name.lower()

            # Определяем направление по названию листа
            if 'входящ' in sheet_lower:
                direction = 'income'
            elif 'исходящ' in sheet_lower:
                direction = 'expense'
            else:
                continue  # Пропускаем неизвестные листы

            # Определяем валюту из названия листа
            if 'usd' in sheet_lower or 'валюта' in sheet_lower or 'доллар' in sheet_lower:
                sheet_currency = 'USD'
            elif 'eur' in sheet_lower or 'евро' in sheet_lower:
                sheet_currency = 'EUR'
            else:
                sheet_currency = 'KZT'

            df_raw = pd.read_excel(self.file_path, sheet_name=sheet_name, header=None)

            # Ищем строку с заголовками
            header_row = 2
            for idx in range(min(5, len(df_raw))):
                row = df_raw.iloc[idx]
                row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
                if '№ п/п' in row_text or 'наименование' in row_text:
                    header_row = idx
                    break

            df = pd.read_excel(self.file_path, sheet_name=sheet_name, header=header_row)

            # Маппинг колонок
            col_map = {}
            for col in df.columns:
                col_lower = str(col).lower()
                if '№' in col_lower and 'п/п' in col_lower:
                    col_map['row_num'] = col
                elif 'наименование' in col_lower or 'фио' in col_lower:
                    col_map['name'] = col
                elif 'бин' in col_lower or 'иин' in col_lower:
                    col_map['bin_iin'] = col
                elif 'дата' in col_lower:
                    col_map['date'] = col
                elif 'сумма' in col_lower:
                    col_map['amount'] = col
                elif 'назначение' in col_lower:
                    col_map['description'] = col

            for idx, row in df.iterrows():
                try:
                    date_val = row.get(col_map.get('date'))
                    if pd.isna(date_val):
                        continue

                    date = self._parse_date(date_val)
                    if date is None:
                        continue

                    amount = self._parse_amount_with_spaces(row.get(col_map.get('amount')))
                    if amount is None or amount == 0:
                        continue

                    counterparty_name = self._clean_string(row.get(col_map.get('name'), ''))
                    counterparty_bin = self._extract_bin_iin(row.get(col_map.get('bin_iin'), ''))

                    if direction == 'income':
                        payer_name = counterparty_name
                        payer_bin = counterparty_bin
                        recipient_name = metadata.client_name
                        recipient_bin = ''
                    else:
                        payer_name = metadata.client_name
                        payer_bin = ''
                        recipient_name = counterparty_name
                        recipient_bin = counterparty_bin

                    transaction = UnifiedTransaction(
                        date=date,
                        amount=abs(amount),
                        currency=sheet_currency,
                        amount_kzt=abs(amount) if sheet_currency == 'KZT' else None,
                        direction=direction,
                        payer_name=payer_name,
                        payer_bin_iin=payer_bin,
                        recipient_name=recipient_name,
                        recipient_bin_iin=recipient_bin,
                        description=self._clean_string(row.get(col_map.get('description'), '')),
                        operation_type=sheet_name,
                        source_bank=self.BANK_NAME,
                        source_file=self.file_path.name,
                        account_number='',
                    )

                    transactions.append(transaction)

                except Exception as e:
                    print(f"Ошибка парсинга строки {idx} в листе {sheet_name}: {e}")
                    continue

        self.metadata = metadata
        self.transactions = transactions
        return metadata, transactions
