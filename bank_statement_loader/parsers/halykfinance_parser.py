"""
Парсер выписок Halyk Finance (Дочерняя организация Народного Банка Казахстана)
Формат: xlsx с операциями по ценным бумагам
"""
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import List, Tuple, Union, Optional
import re
import pandas as pd

from .base_parser import BaseParser
from ..models import UnifiedTransaction, StatementMetadata


class HalykFinanceParser(BaseParser):
    """Парсер выписок Halyk Finance"""

    BANK_NAME = "АО Halyk Finance"
    BANK_ALIASES = ['halyk finance', 'халык финанс', 'народного банка казахстана']

    def __init__(self, file_path: Union[str, Path]):
        super().__init__(file_path)

    @classmethod
    def can_parse(cls, file_path: Union[str, Path]) -> bool:
        """Проверка, подходит ли файл для парсера Halyk Finance"""
        try:
            path = Path(file_path)
            if path.suffix.lower() not in ('.xlsx', '.xls'):
                return False

            df = pd.read_excel(file_path, header=None, nrows=5)

            # Проверяем характерную структуру: первая строка с заголовками
            if len(df) > 0:
                first_row = ' '.join(str(v).lower() for v in df.iloc[0] if pd.notna(v))
                # Halyk Finance имеет специфичные колонки
                if 'счет расхода' in first_row and 'контрагент' in first_row:
                    return True
                if 'валюта/инструмент' in first_row:
                    return True

            return False
        except Exception:
            return False

    def parse(self) -> Tuple[StatementMetadata, List[UnifiedTransaction]]:
        """Парсинг файла выписки Halyk Finance"""
        df = pd.read_excel(self.file_path, header=0)

        metadata = StatementMetadata()
        metadata.bank_name = self.BANK_NAME
        metadata.source_file = self.file_path.name

        # Маппинг колонок
        col_map = {}
        for col in df.columns:
            col_lower = str(col).lower().strip()
            if col_lower == 'клиент':
                col_map['client'] = col
            elif 'счет расхода' in col_lower or 'счет' in col_lower:
                col_map['account'] = col
            elif col_lower == 'контрагент':
                col_map['counterparty'] = col
            elif 'сумма' in col_lower or 'количество' in col_lower:
                col_map['amount'] = col
            elif 'код валюты' in col_lower:
                col_map['currency_code'] = col
            elif 'валюта' in col_lower or 'инструмент' in col_lower:
                col_map['currency'] = col
            elif 'дата' in col_lower:
                col_map['date'] = col

        transactions = []

        for idx, row in df.iterrows():
            try:
                # Пропускаем строки с "Входящие остатки"
                counterparty = self._clean_string(row.get(col_map.get('counterparty'), ''))
                if 'входящие остатки' in counterparty.lower():
                    continue

                # Получаем данные клиента
                client_name = self._clean_string(row.get(col_map.get('client'), ''))
                if not metadata.client_name and client_name:
                    metadata.client_name = client_name

                # Парсим сумму
                amount = self._parse_decimal(row.get(col_map.get('amount')))
                if amount is None or amount == 0:
                    continue

                # Валюта
                currency = self._clean_string(row.get(col_map.get('currency'), ''))
                currency_code = self._clean_string(row.get(col_map.get('currency_code'), ''))
                if currency_code in ['KZT', 'USD', 'EUR', 'RUB']:
                    currency = currency_code
                elif 'тенге' in currency.lower():
                    currency = 'KZT'
                elif 'доллар' in currency.lower():
                    currency = 'USD'
                elif 'евро' in currency.lower():
                    currency = 'EUR'
                else:
                    # Это может быть ценная бумага
                    currency = currency_code if currency_code else 'SECURITY'

                # Счёт
                account = self._clean_string(row.get(col_map.get('account'), ''))
                if not metadata.account_number and account.startswith('KZ'):
                    metadata.account_number = account

                # Дата - если нет колонки даты, используем текущую
                date_val = row.get(col_map.get('date'))
                if pd.notna(date_val):
                    date = self._parse_date(date_val)
                else:
                    date = datetime.now().date()

                if date is None:
                    date = datetime.now().date()

                # Определяем направление
                # Если контрагент = клиент, это исходящая операция
                if counterparty.lower() == client_name.lower():
                    direction = 'expense'
                else:
                    direction = 'income'

                transaction = UnifiedTransaction(
                    date=date,
                    amount=abs(amount),
                    currency=currency,
                    amount_kzt=abs(amount) if currency == 'KZT' else None,
                    direction=direction,
                    payer_name=client_name if direction == 'expense' else counterparty,
                    recipient_name=counterparty if direction == 'expense' else client_name,
                    description=f"Операция по счёту {account}",
                    source_bank=self.BANK_NAME,
                    source_file=self.file_path.name,
                    account_number=account if account.startswith('KZ') else '',
                )

                transactions.append(transaction)

            except Exception as e:
                print(f"Ошибка парсинга строки {idx}: {e}")
                continue

        self.metadata = metadata
        self.transactions = transactions
        return metadata, transactions
