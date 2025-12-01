"""
Парсер выписок Home Credit Bank
Формат: xlsx с заголовками в строке 0, данные со строки 1
Стандартный формат с 18 колонками, похож на Халык/Фридом
"""
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import List, Tuple, Union, Optional
import re
import pandas as pd

from .base_parser import BaseParser
from ..models import UnifiedTransaction, StatementMetadata


class HomeCreditParser(BaseParser):
    """Парсер выписок Home Credit Bank"""

    BANK_NAME = "АО Home Credit Bank"
    BANK_ALIASES = ['home credit', 'хоум кредит', 'homecredit']

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
        'Код назначения платежа': 'knp_code',
        'Назначение платежа': 'description',
    }

    def __init__(self, file_path: Union[str, Path]):
        super().__init__(file_path)

    @classmethod
    def can_parse(cls, file_path: Union[str, Path]) -> bool:
        """Проверка, подходит ли файл для парсера Home Credit"""
        try:
            path = Path(file_path)
            if path.suffix.lower() not in ('.xlsx', '.xls'):
                return False

            xl = pd.ExcelFile(file_path)

            # Проверяем название листа - Home Credit имеет код 886 в номере счёта
            for sheet in xl.sheet_names:
                if sheet.startswith('KZ') and '886' in sheet:
                    return True

            # Проверяем содержимое на наличие "Home Credit"
            df = pd.read_excel(file_path, header=None, nrows=5)
            for idx in range(min(5, len(df))):
                row = df.iloc[idx]
                row_text = ' '.join(str(v) for v in row if pd.notna(v))
                if 'home credit' in row_text.lower():
                    return True

            # Проверяем данные - ищем Home Credit в колонке банка
            df_data = pd.read_excel(file_path, header=0, nrows=10)
            for col in df_data.columns:
                col_str = str(col).lower()
                if 'банк' in col_str:
                    for val in df_data[col]:
                        if pd.notna(val) and 'home credit' in str(val).lower():
                            return True

            return False
        except Exception:
            return False

    def _extract_account_from_sheet(self, sheet_name: str) -> str:
        """Извлечение номера счёта из названия листа"""
        if sheet_name.startswith('KZ'):
            return sheet_name
        return ''

    def parse(self) -> Tuple[StatementMetadata, List[UnifiedTransaction]]:
        """Парсинг файла выписки Home Credit"""
        xl = pd.ExcelFile(self.file_path)

        metadata = StatementMetadata()
        metadata.bank_name = self.BANK_NAME
        metadata.source_file = self.file_path.name

        all_transactions = []

        for sheet_name in xl.sheet_names:
            account_number = self._extract_account_from_sheet(sheet_name)
            if not metadata.account_number and account_number:
                metadata.account_number = account_number

            df = pd.read_excel(self.file_path, sheet_name=sheet_name, header=0)

            # Маппинг колонок
            col_map = {}
            for col in df.columns:
                col_str = str(col).strip()
                for expected, key in self.COLUMN_MAPPING.items():
                    if expected.lower() in col_str.lower():
                        col_map[key] = col
                        break

            for idx, row in df.iterrows():
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

                    payer_account = self._clean_string(row.get(col_map.get('payer_account'), ''))
                    recipient_account = self._clean_string(row.get(col_map.get('recipient_account'), ''))

                    # Определяем направление
                    if account_number and account_number in recipient_account:
                        direction = 'income'
                    elif account_number and account_number in payer_account:
                        direction = 'expense'
                    else:
                        direction = 'income'

                    # Извлекаем данные клиента
                    payer_name = self._clean_string(row.get(col_map.get('payer_name'), ''))
                    if not metadata.client_name and payer_name and 'home credit' not in payer_name.lower():
                        metadata.client_name = payer_name
                    payer_bin = self._extract_bin_iin(row.get(col_map.get('payer_bin_iin'), ''))
                    if not metadata.client_bin_iin and payer_bin and payer_bin != '930540000147':
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
                        payer_account=payer_account,
                        payer_residency=self._clean_string(row.get(col_map.get('payer_residency'), '')),
                        recipient_name=self._clean_string(row.get(col_map.get('recipient_name'), '')),
                        recipient_bin_iin=self._extract_bin_iin(row.get(col_map.get('recipient_bin_iin'), '')),
                        recipient_bank=self._clean_string(row.get(col_map.get('recipient_bank'), '')),
                        recipient_account=recipient_account,
                        recipient_residency=self._clean_string(row.get(col_map.get('recipient_residency'), '')),
                        operation_type=self._clean_string(row.get(col_map.get('operation_type'), '')),
                        knp_code=self._clean_string(row.get(col_map.get('knp_code'), '')),
                        description=self._clean_string(row.get(col_map.get('description'), '')),
                        source_bank=self.BANK_NAME,
                        source_file=self.file_path.name,
                        account_number=account_number,
                    )

                    all_transactions.append(transaction)

                except Exception as e:
                    print(f"Ошибка парсинга строки {idx}: {e}")
                    continue

        self.metadata = metadata
        self.transactions = all_transactions
        return metadata, all_transactions
