"""
Парсер статистики операций Kaspi Bank
Формат: xlsx с отчётом по операциям (Статистика по успешным операциям)
Заголовки в строке 5, данные со строки 6
"""
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import List, Tuple, Union, Optional
import re
import pandas as pd

from .base_parser import BaseParser
from ..models import UnifiedTransaction, StatementMetadata


class KaspiStatsParser(BaseParser):
    """Парсер статистики операций Kaspi Bank"""

    BANK_NAME = "АО Kaspi Bank"
    BANK_ALIASES = ['kaspi', 'каспи', 'kaspi bank']

    def __init__(self, file_path: Union[str, Path]):
        super().__init__(file_path)

    @classmethod
    def can_parse(cls, file_path: Union[str, Path]) -> bool:
        """Проверка, подходит ли файл для парсера статистики Kaspi"""
        try:
            path = Path(file_path)
            if path.suffix.lower() not in ('.xlsx', '.xls'):
                return False

            df = pd.read_excel(file_path, header=None, nrows=10)

            # Ищем характерный заголовок "Статистика по успешным операциям"
            for idx in range(min(8, len(df))):
                row = df.iloc[idx]
                row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
                if 'статистика по успешным операциям' in row_text:
                    return True
                # Или характерные заголовки колонок
                if 'бин банка эквайера' in row_text and 'id мерчанта' in row_text:
                    return True
                if 'наименование мерчанта' in row_text and 'мсс' in row_text:
                    return True

            return False
        except Exception:
            return False

    def _find_header_row(self, df: pd.DataFrame) -> int:
        """Поиск строки с заголовками"""
        for idx in range(min(10, len(df))):
            row = df.iloc[idx]
            row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
            if 'бин банка эквайера' in row_text or 'id мерчанта' in row_text:
                return idx
            if 'дата операции' in row_text and 'сумма' in row_text:
                return idx
        return 5  # По умолчанию

    def parse(self) -> Tuple[StatementMetadata, List[UnifiedTransaction]]:
        """Парсинг файла статистики Kaspi"""
        df_raw = pd.read_excel(self.file_path, header=None)

        metadata = StatementMetadata()
        metadata.bank_name = self.BANK_NAME
        metadata.source_file = self.file_path.name

        # Извлекаем информацию о периоде из заголовка
        for idx in range(min(8, len(df_raw))):
            row = df_raw.iloc[idx]
            row_text = ' '.join(str(v) for v in row if pd.notna(v))
            if 'статистика' in row_text.lower():
                # Ищем даты в формате DD.MM.YYYY
                dates = re.findall(r'\d{2}\.\d{2}\.\d{4}', row_text)
                if len(dates) >= 2:
                    metadata.period_start = self._parse_date(dates[0], ['%d.%m.%Y'])
                    metadata.period_end = self._parse_date(dates[1], ['%d.%m.%Y'])
                break

        # Находим строку с заголовками
        header_row = self._find_header_row(df_raw)

        # Читаем данные с заголовками
        df = pd.read_excel(self.file_path, header=header_row)

        # Маппинг колонок
        col_map = {}
        for col in df.columns:
            col_lower = str(col).lower().strip()
            if 'бин банка' in col_lower or 'бин эквайера' in col_lower:
                col_map['acquirer_bin'] = col
            elif 'id мерчанта' in col_lower:
                col_map['merchant_id'] = col
            elif 'наименование мерчанта' in col_lower:
                col_map['merchant_name'] = col
            elif col_lower == 'мсс' or 'mcc' in col_lower:
                col_map['mcc'] = col
            elif 'дата операции' in col_lower or col_lower == 'дата':
                col_map['date'] = col
            elif 'сумма' in col_lower and 'kzt' in col_lower:
                col_map['amount'] = col
            elif col_lower == 'сумма kzt' or col_lower == 'сумма':
                col_map['amount'] = col
            elif 'номер счета' in col_lower or 'счет клиент' in col_lower:
                col_map['account'] = col
            elif 'иин клиента' in col_lower or col_lower == 'иин':
                col_map['client_iin'] = col
            elif 'фио клиента' in col_lower or 'фио' in col_lower:
                col_map['client_name'] = col
            elif 'тип операции' in col_lower:
                col_map['operation_type'] = col

        transactions = []

        for idx, row in df.iterrows():
            try:
                # Получаем дату
                date_val = row.get(col_map.get('date'))
                if pd.isna(date_val):
                    continue

                date = self._parse_date(date_val)
                if date is None:
                    continue

                # Получаем сумму
                amount = self._parse_decimal(row.get(col_map.get('amount')))
                if amount is None or amount == 0:
                    continue

                # Определяем направление
                op_type = self._clean_string(row.get(col_map.get('operation_type'), ''))
                if op_type.upper() == 'ИСХ' or 'исход' in op_type.lower():
                    direction = 'expense'
                elif op_type.upper() == 'ВХ' or 'входящ' in op_type.lower():
                    direction = 'income'
                else:
                    direction = 'expense'  # По умолчанию для операций с мерчантами

                # Данные клиента
                client_iin = self._extract_bin_iin(row.get(col_map.get('client_iin'), ''))
                client_name = self._clean_string(row.get(col_map.get('client_name'), ''))
                client_account = self._clean_string(row.get(col_map.get('account'), ''))

                # Сохраняем первого клиента в метаданные
                if not metadata.client_bin_iin and client_iin:
                    metadata.client_bin_iin = client_iin
                if not metadata.client_name and client_name:
                    metadata.client_name = client_name
                if not metadata.account_number and client_account:
                    metadata.account_number = client_account

                # Данные мерчанта
                merchant_name = self._clean_string(row.get(col_map.get('merchant_name'), ''))
                merchant_id = self._clean_string(row.get(col_map.get('merchant_id'), ''))
                mcc = self._clean_string(row.get(col_map.get('mcc'), ''))
                acquirer_bin = self._clean_string(row.get(col_map.get('acquirer_bin'), ''))

                # Формируем описание
                description = f"{merchant_name}"
                if mcc:
                    description += f" (MCC: {mcc})"
                if merchant_id:
                    description += f" [ID: {merchant_id}]"

                # Для расходных операций - клиент платит мерчанту
                if direction == 'expense':
                    payer_name = client_name
                    payer_iin = client_iin
                    payer_account = client_account
                    recipient_name = merchant_name
                    recipient_iin = acquirer_bin
                    recipient_account = ''
                else:
                    payer_name = merchant_name
                    payer_iin = acquirer_bin
                    payer_account = ''
                    recipient_name = client_name
                    recipient_iin = client_iin
                    recipient_account = client_account

                transaction = UnifiedTransaction(
                    date=date,
                    amount=abs(amount),
                    currency='KZT',
                    amount_kzt=abs(amount),
                    direction=direction,
                    payer_name=payer_name,
                    payer_bin_iin=payer_iin,
                    payer_account=payer_account,
                    recipient_name=recipient_name,
                    recipient_bin_iin=recipient_iin,
                    recipient_account=recipient_account,
                    operation_type=op_type if op_type else 'Карточная операция',
                    description=description,
                    source_bank=self.BANK_NAME,
                    source_file=self.file_path.name,
                    account_number=client_account,
                )

                transactions.append(transaction)

            except Exception as e:
                print(f"Ошибка парсинга строки {idx}: {e}")
                continue

        self.metadata = metadata
        self.transactions = transactions
        return metadata, transactions
