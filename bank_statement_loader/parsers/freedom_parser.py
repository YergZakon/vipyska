"""
Парсер выписок Фридом Банка (Freedom Finance Bank)
Формат: xls с несколькими листами по валютам (KZT, USD, RUB и т.д.)
Заголовки в строке 1, данные со строки 2
"""
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import List, Tuple, Union, Optional
import re
import pandas as pd

from .base_parser import BaseParser
from ..models import UnifiedTransaction, StatementMetadata


class FreedomParser(BaseParser):
    """Парсер выписок Фридом Банка"""

    BANK_NAME = "АО Фридом Банк Казахстан"
    BANK_ALIASES = ['фридом', 'freedom', 'фридом финанс', 'freedom finance', 'freedom bank']

    # Маппинг колонок Фридом банка
    COLUMN_MAPPING = {
        'Дата и время операции': 'date',
        'Валюта операции': 'currency',
        'Виды операции (категория документа)': 'operation_type',
        'Наименование СДП (при наличии)': 'sdp_name',
        'Сумма в валюте ее проведения': 'amount',
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
        'Код назначения платежа': 'description',  # В Freedom это поле содержит описание
    }

    def __init__(self, file_path: Union[str, Path]):
        super().__init__(file_path)

    @classmethod
    def can_parse(cls, file_path: Union[str, Path]) -> bool:
        """Проверка, подходит ли файл для парсера Фридом"""
        try:
            path = Path(file_path)
            if path.suffix.lower() not in ('.xlsx', '.xls'):
                return False

            xl = pd.ExcelFile(file_path)

            # Проверяем названия листов - у Фридом они содержат номера счетов KZ...KZT/USD/RUB
            for sheet in xl.sheet_names:
                if re.match(r'KZ\w+[A-Z]{3}$', sheet):  # Формат: KZ...KZT или KZ...USD
                    return True

            # Проверяем содержимое
            df = pd.read_excel(file_path, header=None, nrows=5)
            for idx in range(min(5, len(df))):
                row = df.iloc[idx]
                row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
                if any(alias in row_text for alias in cls.BANK_ALIASES):
                    return True
                # Проверяем характерные заголовки
                if 'дата и время операции' in row_text and 'валюта операции' in row_text:
                    # Дополнительная проверка на наличие колонки "Сумма в валюте ее проведения" (без разделения на дебет/кредит)
                    if 'сумма в валюте ее проведения' in row_text and 'кредит' not in row_text:
                        return True

            return False
        except Exception:
            return False

    def _extract_account_from_sheet(self, sheet_name: str) -> Tuple[str, str]:
        """Извлечение номера счёта и валюты из названия листа"""
        # Формат: KZ95551B529526835KZT
        match = re.match(r'(KZ\w+?)([A-Z]{3})$', sheet_name)
        if match:
            return match.group(1) + match.group(2), match.group(2)
        return sheet_name, 'KZT'

    def _parse_metadata(self, df: pd.DataFrame, sheet_name: str) -> StatementMetadata:
        """Извлечение метаданных"""
        metadata = StatementMetadata()
        metadata.bank_name = self.BANK_NAME
        metadata.source_file = self.file_path.name

        account, currency = self._extract_account_from_sheet(sheet_name)
        metadata.account_number = account
        metadata.currency = currency

        # Пытаемся извлечь информацию о клиенте из первых транзакций
        if len(df) > 2:
            # Читаем данные с заголовками
            df_data = pd.read_excel(self.file_path, sheet_name=sheet_name, header=1)
            if len(df_data) > 0:
                # Берём информацию из первой транзакции
                for col in df_data.columns:
                    col_lower = str(col).lower()
                    if 'иин/бин плательщика' in col_lower or 'иин/бин получателя' in col_lower:
                        first_val = df_data[col].iloc[0] if len(df_data) > 0 else None
                        if pd.notna(first_val):
                            metadata.client_bin_iin = self._extract_bin_iin(first_val)
                    if 'наименование/фио плательщика' in col_lower or 'наименование/фио получателя' in col_lower:
                        first_val = df_data[col].iloc[0] if len(df_data) > 0 else None
                        if pd.notna(first_val):
                            metadata.client_name = str(first_val).strip()

        return metadata

    def parse(self) -> Tuple[StatementMetadata, List[UnifiedTransaction]]:
        """Парсинг файла выписки Фридом банка"""
        xl = pd.ExcelFile(self.file_path)
        all_transactions = []
        combined_metadata = StatementMetadata()
        combined_metadata.bank_name = self.BANK_NAME
        combined_metadata.source_file = self.file_path.name

        for sheet_name in xl.sheet_names:
            # Читаем лист
            df_raw = pd.read_excel(self.file_path, sheet_name=sheet_name, header=None)

            if len(df_raw) < 2:
                continue

            # Ищем строку с заголовками
            header_row = 0
            for idx in range(min(5, len(df_raw))):
                row = df_raw.iloc[idx]
                row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
                if 'дата' in row_text and 'операци' in row_text:
                    header_row = idx
                    break

            # Если первая строка пустая, заголовки в строке 1
            if pd.isna(df_raw.iloc[0, 0]) or str(df_raw.iloc[0, 0]).strip() == '':
                header_row = 1

            # Читаем с заголовками
            df = pd.read_excel(self.file_path, sheet_name=sheet_name, header=header_row)

            # Извлекаем метаданные
            sheet_metadata = self._parse_metadata(df_raw, sheet_name)
            if not combined_metadata.client_name and sheet_metadata.client_name:
                combined_metadata.client_name = sheet_metadata.client_name
                combined_metadata.client_bin_iin = sheet_metadata.client_bin_iin

            account_number, sheet_currency = self._extract_account_from_sheet(sheet_name)

            # Маппинг колонок
            col_map = {}
            for col in df.columns:
                col_str = str(col).strip()
                for expected, key in self.COLUMN_MAPPING.items():
                    if expected.lower() in col_str.lower() or col_str.lower() in expected.lower():
                        col_map[key] = col
                        break

            # Парсим транзакции
            for idx, row in df.iterrows():
                try:
                    date_val = row.get(col_map.get('date'))
                    if pd.isna(date_val) or str(date_val).strip() == '':
                        continue

                    date = self._parse_date(date_val)
                    if date is None:
                        continue

                    amount = self._parse_decimal(row.get(col_map.get('amount')))
                    if amount is None:
                        continue

                    amount_kzt = self._parse_decimal(row.get(col_map.get('amount_kzt')))

                    # Определяем направление
                    # В Фридом банке направление определяется по тому, кто плательщик/получатель
                    payer_account = self._clean_string(row.get(col_map.get('payer_account'), ''))
                    recipient_account = self._clean_string(row.get(col_map.get('recipient_account'), ''))

                    # Если счёт выписки совпадает со счётом получателя - это приход
                    if account_number in recipient_account:
                        direction = 'income'
                    elif account_number in payer_account:
                        direction = 'expense'
                    else:
                        # Определяем по знаку или другим признакам
                        direction = 'income' if amount > 0 else 'expense'

                    currency = self._clean_string(row.get(col_map.get('currency'), sheet_currency))
                    if not currency:
                        currency = sheet_currency

                    transaction = UnifiedTransaction(
                        date=date,
                        amount=abs(amount),
                        currency=currency,
                        amount_kzt=abs(amount_kzt) if amount_kzt else None,
                        direction=direction,
                        payer_name=self._clean_string(row.get(col_map.get('payer_name'), '')),
                        payer_bin_iin=self._extract_bin_iin(row.get(col_map.get('payer_bin_iin'), '')),
                        payer_bank=self._clean_string(row.get(col_map.get('payer_bank'), '')),
                        payer_account=payer_account,
                        payer_residency=self._clean_string(row.get(col_map.get('payer_residency'), '')),
                        recipient_name=self._clean_string(row.get(col_map.get('recipient_name'), '')),
                        recipient_bin_iin=self._extract_bin_iin(row.get(col_map.get('recipient_bin_iin'), '')),
                        recipient_bank=self._clean_string(row.get(col_map.get('recipient_bank'), '')),
                        recipient_account=recipient_account,
                        recipient_residency=self._clean_string(row.get(col_map.get('recipient_residency'), '')),
                        operation_type=self._clean_string(row.get(col_map.get('operation_type'), '')),
                        description=self._clean_string(row.get(col_map.get('description'), '')),
                        source_bank=self.BANK_NAME,
                        source_file=self.file_path.name,
                        account_number=account_number,
                    )

                    all_transactions.append(transaction)

                except Exception as e:
                    print(f"Ошибка парсинга строки {idx} в листе {sheet_name}: {e}")
                    continue

        self.metadata = combined_metadata
        self.transactions = all_transactions
        return self.metadata, self.transactions
