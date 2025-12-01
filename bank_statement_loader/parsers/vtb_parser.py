"""
Парсер выписок ВТБ Казахстан
Формат: xlsx с заголовками в строке 0, данные со строки 1
Колонки: Дата и время операции, Валюта операции, Вид операции (КД), etc.
"""
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import List, Tuple, Union, Optional
import re
import pandas as pd

from .base_parser import BaseParser
from ..models import UnifiedTransaction, StatementMetadata


class VTBParser(BaseParser):
    """Парсер выписок ВТБ Казахстан"""

    BANK_NAME = "ДО АО Банк ВТБ (Казахстан)"
    BANK_ALIASES = ['втб', 'vtb', 'банк втб']

    def __init__(self, file_path: Union[str, Path]):
        super().__init__(file_path)

    @classmethod
    def can_parse(cls, file_path: Union[str, Path]) -> bool:
        """Проверка, подходит ли файл для парсера ВТБ"""
        try:
            path = Path(file_path)
            if path.suffix.lower() not in ('.xlsx', '.xls'):
                return False

            df = pd.read_excel(file_path, header=None, nrows=5)

            # Проверяем характерные заголовки ВТБ
            for idx in range(min(3, len(df))):
                row = df.iloc[idx]
                row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
                if 'вид операции (кд)' in row_text:
                    return True
                if 'втб' in row_text:
                    return True
                # Характерный формат валюты "KZT - Тенге"
                for v in row:
                    if pd.notna(v) and 'kzt - тенге' in str(v).lower():
                        return True

            return False
        except Exception:
            return False

    def _parse_vtb_currency(self, value: any) -> str:
        """Парсинг валюты в формате ВТБ (KZT - Тенге)"""
        if pd.isna(value):
            return 'KZT'
        str_value = str(value).strip()
        match = re.match(r'^([A-Z]{3})', str_value)
        if match:
            return match.group(1)
        return 'KZT'

    def _parse_vtb_date(self, value: any) -> Optional[datetime]:
        """Парсинг даты в формате ВТБ (2021.04.22 17:02:02)"""
        if pd.isna(value):
            return None

        str_value = str(value).strip()

        formats = [
            '%Y.%m.%d %H:%M:%S',
            '%Y.%m.%d',
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d',
            '%d.%m.%Y %H:%M:%S',
            '%d.%m.%Y',
        ]

        for fmt in formats:
            try:
                return datetime.strptime(str_value, fmt)
            except ValueError:
                continue

        return self._parse_date(value)

    def parse(self) -> Tuple[StatementMetadata, List[UnifiedTransaction]]:
        """Парсинг файла выписки ВТБ"""
        df = pd.read_excel(self.file_path, header=0)

        metadata = StatementMetadata()
        metadata.bank_name = self.BANK_NAME
        metadata.source_file = self.file_path.name

        # Маппинг колонок
        col_map = {}
        for col in df.columns:
            col_lower = str(col).lower()
            if 'дата' in col_lower and 'операци' in col_lower:
                col_map['date'] = col
            elif 'валюта операции' in col_lower:
                col_map['currency'] = col
            elif 'вид операции' in col_lower or 'кд' in col_lower:
                col_map['operation_type'] = col
            elif 'сумма (вал' in col_lower:
                col_map['amount'] = col
            elif 'сумма (тенге' in col_lower:
                col_map['amount_kzt'] = col
            elif 'наименование' in col_lower and 'плательщик' in col_lower:
                col_map['payer_name'] = col
            elif 'иин' in col_lower and 'плательщик' in col_lower:
                col_map['payer_bin'] = col
            elif 'резиден' in col_lower and 'плательщик' in col_lower:
                col_map['payer_residency'] = col
            elif 'банк плательщика' in col_lower:
                col_map['payer_bank'] = col
            elif 'счет плательщика' in col_lower:
                col_map['payer_account'] = col
            elif 'наименование' in col_lower and 'получатель' in col_lower:
                col_map['recipient_name'] = col
            elif 'иин' in col_lower and 'получатель' in col_lower:
                col_map['recipient_bin'] = col
            elif 'банк получателя' in col_lower:
                col_map['recipient_bank'] = col
            elif 'счет получателя' in col_lower:
                col_map['recipient_account'] = col
            elif 'назначение' in col_lower:
                col_map['description'] = col

        transactions = []

        for idx, row in df.iterrows():
            try:
                date_val = row.get(col_map.get('date'))
                if pd.isna(date_val):
                    continue

                date = self._parse_vtb_date(date_val)
                if date is None:
                    continue

                amount = self._parse_decimal(row.get(col_map.get('amount')))
                if amount is None:
                    continue

                amount_kzt = self._parse_decimal(row.get(col_map.get('amount_kzt')))
                currency = self._parse_vtb_currency(row.get(col_map.get('currency')))

                operation_type = self._clean_string(row.get(col_map.get('operation_type'), ''))

                # Определяем направление по типу операции
                op_lower = operation_type.lower()
                if 'исходящ' in op_lower or 'внешние исходящие' in op_lower:
                    direction = 'expense'
                elif 'входящ' in op_lower or 'пополнение' in op_lower or 'кассовые' in op_lower:
                    direction = 'income'
                elif 'внутренние' in op_lower:
                    # Для внутренних переводов смотрим на контекст
                    payer_name = self._clean_string(row.get(col_map.get('payer_name'), ''))
                    if payer_name and 'втб' not in payer_name.lower():
                        direction = 'income'
                    else:
                        direction = 'expense'
                else:
                    direction = 'income'

                payer_name = self._clean_string(row.get(col_map.get('payer_name'), ''))
                payer_bin = self._extract_bin_iin(row.get(col_map.get('payer_bin'), ''))

                # Извлекаем данные клиента
                if not metadata.client_name and payer_name and 'втб' not in payer_name.lower():
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
                    recipient_name=self._clean_string(row.get(col_map.get('recipient_name'), '')),
                    recipient_bin_iin=self._extract_bin_iin(row.get(col_map.get('recipient_bin'), '')),
                    recipient_bank=self._clean_string(row.get(col_map.get('recipient_bank'), '')),
                    recipient_account=self._clean_string(row.get(col_map.get('recipient_account'), '')),
                    operation_type=operation_type,
                    description=self._clean_string(row.get(col_map.get('description'), '')),
                    source_bank=self.BANK_NAME,
                    source_file=self.file_path.name,
                    account_number=metadata.account_number,
                )

                transactions.append(transaction)

            except Exception as e:
                print(f"Ошибка парсинга строки {idx}: {e}")
                continue

        self.metadata = metadata
        self.transactions = transactions
        return metadata, transactions
