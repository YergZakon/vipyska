"""
Парсер выписок Народного банка Казахстана (Halyk Bank)
Формат: xlsx с заголовками в строках 0-8, данные начинаются с строки 9
"""
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import List, Tuple, Union, Optional
import re
import pandas as pd

from .base_parser import BaseParser
from ..models import UnifiedTransaction, StatementMetadata


class HalykParser(BaseParser):
    """Парсер выписок Народного банка Казахстана"""

    BANK_NAME = "АО Народный Банк Казахстана"
    BANK_ALIASES = ['народный', 'halyk', 'hsbkkzkx', 'народный банк']

    # Маппинг колонок Халык банка
    COLUMN_MAPPING = {
        'Дата и время операции': 'date',
        'Валюта операции': 'currency',
        'Виды операции (категория документа)': 'operation_type',
        'Наименование СДП (при наличии)': 'sdp_name',
        'Сумма в валюте ее проведения по кредиту': 'credit_amount',
        'Сумма в валюте ее проведения по дебету': 'debit_amount',
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
        self._header_row = 8  # Строка с заголовками (0-indexed)

    @classmethod
    def can_parse(cls, file_path: Union[str, Path]) -> bool:
        """Проверка, подходит ли файл для парсера Халык"""
        try:
            path = Path(file_path)
            if path.suffix.lower() not in ('.xlsx', '.xls'):
                return False

            df = pd.read_excel(file_path, header=None, nrows=10)

            # Проверяем характерные признаки Халык банка
            first_rows_text = ' '.join(str(v) for row in df.values[:5] for v in row if pd.notna(v)).lower()

            if any(alias in first_rows_text for alias in cls.BANK_ALIASES):
                return True

            # Проверяем структуру заголовков
            if df.shape[1] >= 15:
                row_8 = df.iloc[8] if len(df) > 8 else None
                if row_8 is not None:
                    headers = [str(v).lower() for v in row_8 if pd.notna(v)]
                    if any('дата' in h and 'операци' in h for h in headers):
                        if any('кредит' in h for h in headers) and any('дебет' in h for h in headers):
                            return True

            return False
        except Exception:
            return False

    def _parse_metadata(self, df: pd.DataFrame) -> StatementMetadata:
        """Извлечение метаданных выписки из заголовочной части"""
        metadata = StatementMetadata()
        metadata.bank_name = self.BANK_NAME
        metadata.source_file = self.file_path.name

        for idx in range(min(8, len(df))):
            row = df.iloc[idx]
            row_text = ' '.join(str(v) for v in row if pd.notna(v))

            # ИИН/БИН
            if 'ИИН/БИН' in row_text or 'иин' in row_text.lower():
                match = re.search(r'\d{12}', row_text)
                if match:
                    metadata.client_bin_iin = match.group()
                # Имя клиента обычно в следующей ячейке
                for v in row:
                    if pd.notna(v) and str(v).strip() and not re.match(r'\d{12}', str(v)):
                        name = str(v).strip()
                        if name and 'ИИН' not in name and 'БИН' not in name:
                            metadata.client_name = name
                            break

            # Номер контракта/счёта
            if 'Contract' in row_text or 'contract' in row_text.lower():
                match = re.search(r'KZ\w{18,20}', row_text)
                if match:
                    metadata.account_number = match.group()

            # Валюта
            if 'Валюта контракта' in row_text or 'валюта' in row_text.lower():
                for currency in ['KZT', 'USD', 'EUR', 'RUB']:
                    if currency in row_text:
                        metadata.currency = currency
                        break

            # Период выписки
            if 'Период выписки' in row_text or 'период' in row_text.lower():
                dates = re.findall(r'\d{2}\.\d{2}\.\d{4}', row_text)
                if len(dates) >= 2:
                    metadata.period_start = self._parse_date(dates[0])
                    metadata.period_end = self._parse_date(dates[1])

            # Балансы
            if 'Начальный баланс' in row_text or 'начальн' in row_text.lower():
                match = re.search(r'[-\d\s,.]+', row_text.split(':')[-1] if ':' in row_text else row_text)
                if match:
                    metadata.opening_balance = self._parse_decimal(match.group())

            if 'Конечный баланс' in row_text or 'конечн' in row_text.lower():
                match = re.search(r'[-\d\s,.]+', row_text.split(':')[-1] if ':' in row_text else row_text)
                if match:
                    metadata.closing_balance = self._parse_decimal(match.group())

        return metadata

    def _find_header_row(self, df: pd.DataFrame) -> int:
        """Поиск строки с заголовками таблицы"""
        for idx in range(min(15, len(df))):
            row = df.iloc[idx]
            row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
            if 'дата' in row_text and 'операци' in row_text and 'сумма' in row_text:
                return idx
        return 8  # По умолчанию

    def _map_columns(self, df: pd.DataFrame) -> dict:
        """Создание маппинга индексов колонок"""
        column_indices = {}
        headers = df.iloc[0].tolist() if len(df) > 0 else []

        for idx, header in enumerate(headers):
            if pd.isna(header):
                continue
            header_str = str(header).strip()
            for expected, key in self.COLUMN_MAPPING.items():
                if expected.lower() in header_str.lower() or header_str.lower() in expected.lower():
                    column_indices[key] = idx
                    break

        return column_indices

    def parse(self) -> Tuple[StatementMetadata, List[UnifiedTransaction]]:
        """Парсинг файла выписки Халык банка"""
        # Читаем весь файл
        df_raw = self._read_excel(header=None)

        # Извлекаем метаданные
        self.metadata = self._parse_metadata(df_raw)

        # Находим строку с заголовками
        header_row = self._find_header_row(df_raw)

        # Читаем данные с заголовками
        df = pd.read_excel(self.file_path, header=header_row)

        # Маппинг колонок
        col_map = {}
        for col in df.columns:
            col_str = str(col).strip()
            for expected, key in self.COLUMN_MAPPING.items():
                if expected.lower() in col_str.lower() or col_str.lower() in expected.lower():
                    col_map[key] = col
                    break

        # Парсинг транзакций
        transactions = []
        for idx, row in df.iterrows():
            try:
                # Пропускаем пустые строки и строки с номерами колонок
                date_val = row.get(col_map.get('date'))
                if pd.isna(date_val) or str(date_val).strip() in ['', 'nan', '1', '2', '3']:
                    continue

                # Парсим дату
                date = self._parse_date(date_val)
                if date is None:
                    continue

                # Парсим суммы
                credit = self._parse_decimal(row.get(col_map.get('credit_amount')))
                debit = self._parse_decimal(row.get(col_map.get('debit_amount')))
                amount_kzt = self._parse_decimal(row.get(col_map.get('amount_kzt')))

                # Определяем направление и сумму
                if credit and credit > 0:
                    amount = credit
                    direction = 'income'
                elif debit and debit > 0:
                    amount = debit
                    direction = 'expense'
                else:
                    continue

                # Создаём транзакцию
                transaction = UnifiedTransaction(
                    date=date,
                    amount=amount,
                    currency=self._clean_string(row.get(col_map.get('currency'), 'KZT')),
                    amount_kzt=amount_kzt,
                    direction=direction,
                    payer_name=self._clean_string(row.get(col_map.get('payer_name'))),
                    payer_bin_iin=self._extract_bin_iin(row.get(col_map.get('payer_bin_iin'))),
                    payer_bank=self._clean_string(row.get(col_map.get('payer_bank'))),
                    payer_account=self._clean_string(row.get(col_map.get('payer_account'))),
                    payer_residency=self._clean_string(row.get(col_map.get('payer_residency'))),
                    recipient_name=self._clean_string(row.get(col_map.get('recipient_name'))),
                    recipient_bin_iin=self._extract_bin_iin(row.get(col_map.get('recipient_bin_iin'))),
                    recipient_bank=self._clean_string(row.get(col_map.get('recipient_bank'))),
                    recipient_account=self._clean_string(row.get(col_map.get('recipient_account'))),
                    recipient_residency=self._clean_string(row.get(col_map.get('recipient_residency'))),
                    operation_type=self._clean_string(row.get(col_map.get('operation_type'))),
                    knp_code=self._clean_string(row.get(col_map.get('knp_code'))),
                    description=self._clean_string(row.get(col_map.get('description'))),
                    source_bank=self.BANK_NAME,
                    source_file=self.file_path.name,
                    account_number=self.metadata.account_number,
                )

                transactions.append(transaction)

            except Exception as e:
                # Логирование ошибки без прерывания обработки
                print(f"Ошибка парсинга строки {idx}: {e}")
                continue

        self.transactions = transactions
        return self.metadata, transactions
