"""
Парсер выписок Kaspi Bank
Формат: xlsx с метаданными в строках 0-9, заголовки в строках 10-12, данные с строки 13
"""
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import List, Tuple, Union, Optional
import re
import pandas as pd

from .base_parser import BaseParser
from ..models import UnifiedTransaction, StatementMetadata


class KaspiParser(BaseParser):
    """Парсер выписок Kaspi Bank"""

    BANK_NAME = "АО Kaspi Bank"
    BANK_ALIASES = ['kaspi', 'каспи', 'kaspi bank']

    # Маппинг колонок Kaspi
    COLUMN_MAPPING = {
        'Дата и время операции': 'date',
        'Валюта операции': 'currency',
        'Виды операции (категория документа)': 'operation_type',
        'Сумма в валюте ее проведения': 'amount',
        'Сумма в тенге': 'amount_kzt',
        'Наименование/ФИО': 'name',  # Используется для плательщика и получателя
        'ИИН/БИН': 'bin_iin',
        'Резидентство': 'residency',
        'Банк': 'bank',
        'Номер счета': 'account',
        'Код назначения платежа': 'knp_code',
        'Назначение платежа': 'description',
    }

    def __init__(self, file_path: Union[str, Path]):
        super().__init__(file_path)

    @classmethod
    def can_parse(cls, file_path: Union[str, Path]) -> bool:
        """Проверка, подходит ли файл для парсера Kaspi"""
        try:
            path = Path(file_path)
            if path.suffix.lower() not in ('.xlsx', '.xls'):
                return False

            df = pd.read_excel(file_path, header=None, nrows=15)

            # Проверяем характерные признаки Kaspi
            for idx in range(min(12, len(df))):
                row = df.iloc[idx]
                row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
                if any(alias in row_text for alias in cls.BANK_ALIASES):
                    return True

            # Проверяем название листа
            xl = pd.ExcelFile(file_path)
            for sheet in xl.sheet_names:
                if 'kas' in sheet.lower() or 'kaspi' in sheet.lower():
                    return True

            return False
        except Exception:
            return False

    def _parse_metadata(self, df: pd.DataFrame) -> StatementMetadata:
        """Извлечение метаданных из заголовочной части"""
        metadata = StatementMetadata()
        metadata.bank_name = self.BANK_NAME
        metadata.source_file = self.file_path.name

        for idx in range(min(12, len(df))):
            row = df.iloc[idx]
            row_values = [str(v).strip() for v in row if pd.notna(v) and str(v).strip()]

            for i, val in enumerate(row_values):
                val_lower = val.lower()

                if 'клиент:' in val_lower or val_lower == 'клиент:':
                    # Имя клиента в следующей ячейке
                    if i + 1 < len(row_values):
                        metadata.client_name = row_values[i + 1]
                    elif len(row) > 2:
                        for v in row[2:]:
                            if pd.notna(v) and str(v).strip():
                                metadata.client_name = str(v).strip()
                                break

                elif 'иин/бин:' in val_lower or val_lower == 'иин/бин:':
                    if i + 1 < len(row_values):
                        metadata.client_bin_iin = self._extract_bin_iin(row_values[i + 1])
                    elif len(row) > 2:
                        for v in row[2:]:
                            if pd.notna(v):
                                metadata.client_bin_iin = self._extract_bin_iin(v)
                                break

                elif 'счет:' in val_lower or val_lower == 'счет:':
                    if i + 1 < len(row_values):
                        account = row_values[i + 1]
                        if account.startswith('KZ'):
                            metadata.account_number = account
                    elif len(row) > 2:
                        for v in row[2:]:
                            if pd.notna(v) and str(v).startswith('KZ'):
                                metadata.account_number = str(v).strip()
                                break

                elif 'валюта счета:' in val_lower:
                    if i + 1 < len(row_values):
                        metadata.currency = row_values[i + 1]
                    elif len(row) > 2:
                        for v in row[2:]:
                            if pd.notna(v) and str(v).strip() in ['KZT', 'USD', 'EUR', 'RUB']:
                                metadata.currency = str(v).strip()
                                break

                elif 'период:' in val_lower:
                    # Формат: c DD/MM/YYYY по DD/MM/YYYY
                    if i + 1 < len(row_values):
                        period_str = row_values[i + 1]
                        dates = re.findall(r'\d{2}/\d{2}/\d{4}', period_str)
                        if len(dates) >= 2:
                            metadata.period_start = self._parse_date(dates[0], ['%d/%m/%Y'])
                            metadata.period_end = self._parse_date(dates[1], ['%d/%m/%Y'])

                elif 'входящий остаток:' in val_lower:
                    if i + 1 < len(row_values):
                        # Парсим формат "300.27 вал 126386.65 нац"
                        balance_str = row_values[i + 1]
                        match = re.match(r'([\d.,]+)', balance_str)
                        if match:
                            metadata.opening_balance = self._parse_decimal(match.group(1))

        return metadata

    def _find_data_start(self, df: pd.DataFrame) -> int:
        """Поиск строки начала данных (после заголовков)"""
        for idx in range(min(20, len(df))):
            row = df.iloc[idx]
            # Ищем строку с номерами колонок (1, 2, 3, ...)
            first_vals = [str(v).strip() for v in row[:5] if pd.notna(v)]
            if first_vals and first_vals[0] == '1' and len(first_vals) > 1 and first_vals[1] == '2':
                return idx + 1  # Данные начинаются со следующей строки

            # Или ищем строку с датой
            first_val = row.iloc[0] if len(row) > 0 else None
            if pd.notna(first_val):
                date = self._parse_date(first_val)
                if date and date.year > 2000:
                    return idx

        return 13  # По умолчанию

    def parse(self) -> Tuple[StatementMetadata, List[UnifiedTransaction]]:
        """Парсинг файла выписки Kaspi"""
        # Читаем весь файл
        df_raw = self._read_excel(header=None)

        # Извлекаем метаданные
        self.metadata = self._parse_metadata(df_raw)

        # Ищем строку с заголовками (обычно 10)
        header_row = 10
        for idx in range(8, min(15, len(df_raw))):
            row = df_raw.iloc[idx]
            row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
            if 'дата' in row_text and 'операци' in row_text:
                header_row = idx
                break

        # Строка заголовков для Kaspi состоит из двух строк (10 и 11)
        # Основные заголовки в строке 10, подзаголовки (Плательщик/Получатель) в 11

        # Читаем данные
        df = pd.read_excel(self.file_path, header=None)
        data_start = self._find_data_start(df)

        # Определяем структуру колонок по заголовкам
        # Kaspi имеет специфичную структуру с объединёнными ячейками
        # Колонки: 0-Дата, 1-Валюта, 2-Вид операции, 3-Сумма вал, 4-Сумма тенге,
        # 5-Наим. плательщика, 6-ИИН плат, 7-Резидент плат, 8-Банк плат, 9-Счёт плат
        # 10-Наим. получателя, 11-ИИН получ, 12-Резидент получ, 13-Банк получ, 14-Счёт получ
        # 15-КНП, 16-Назначение

        col_indices = {
            'date': 0,
            'currency': 1,
            'operation_type': 2,
            'amount': 3,
            'amount_kzt': 4,
            'payer_name': 5,
            'payer_bin_iin': 6,
            'payer_residency': 7,
            'payer_bank': 8,
            'payer_account': 9,
            'recipient_name': 10,
            'recipient_bin_iin': 11,
            'recipient_residency': 12,
            'recipient_bank': 13,
            'recipient_account': 14,
            'knp_code': 15,
            'description': 16,
        }

        # Парсим транзакции
        transactions = []
        for idx in range(data_start, len(df)):
            row = df.iloc[idx]

            try:
                # Получаем значение даты
                date_val = row.iloc[col_indices['date']] if len(row) > col_indices['date'] else None
                if pd.isna(date_val) or str(date_val).strip() in ['', 'nan']:
                    continue

                # Парсим дату
                date = self._parse_date(date_val)
                if date is None:
                    continue

                # Парсим сумму
                amount = self._parse_decimal(row.iloc[col_indices['amount']] if len(row) > col_indices['amount'] else None)
                amount_kzt = self._parse_decimal(row.iloc[col_indices['amount_kzt']] if len(row) > col_indices['amount_kzt'] else None)

                if amount is None:
                    continue

                # Определяем направление по типу операции
                operation_type = self._clean_string(row.iloc[col_indices['operation_type']] if len(row) > col_indices['operation_type'] else '')
                direction = 'expense' if 'дебет' in operation_type.lower() or 'исх' in operation_type.lower() else 'income'

                # Создаём транзакцию
                transaction = UnifiedTransaction(
                    date=date,
                    amount=abs(amount),
                    currency=self._clean_string(row.iloc[col_indices['currency']] if len(row) > col_indices['currency'] else 'KZT'),
                    amount_kzt=abs(amount_kzt) if amount_kzt else None,
                    direction=direction,
                    payer_name=self._clean_string(row.iloc[col_indices['payer_name']] if len(row) > col_indices['payer_name'] else ''),
                    payer_bin_iin=self._extract_bin_iin(row.iloc[col_indices['payer_bin_iin']] if len(row) > col_indices['payer_bin_iin'] else ''),
                    payer_bank=self._clean_string(row.iloc[col_indices['payer_bank']] if len(row) > col_indices['payer_bank'] else ''),
                    payer_account=self._clean_string(row.iloc[col_indices['payer_account']] if len(row) > col_indices['payer_account'] else ''),
                    payer_residency=self._clean_string(row.iloc[col_indices['payer_residency']] if len(row) > col_indices['payer_residency'] else ''),
                    recipient_name=self._clean_string(row.iloc[col_indices['recipient_name']] if len(row) > col_indices['recipient_name'] else ''),
                    recipient_bin_iin=self._extract_bin_iin(row.iloc[col_indices['recipient_bin_iin']] if len(row) > col_indices['recipient_bin_iin'] else ''),
                    recipient_bank=self._clean_string(row.iloc[col_indices['recipient_bank']] if len(row) > col_indices['recipient_bank'] else ''),
                    recipient_account=self._clean_string(row.iloc[col_indices['recipient_account']] if len(row) > col_indices['recipient_account'] else ''),
                    recipient_residency=self._clean_string(row.iloc[col_indices['recipient_residency']] if len(row) > col_indices['recipient_residency'] else ''),
                    operation_type=operation_type,
                    knp_code=self._clean_string(row.iloc[col_indices['knp_code']] if len(row) > col_indices['knp_code'] else ''),
                    description=self._clean_string(row.iloc[col_indices['description']] if len(row) > col_indices['description'] else ''),
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
