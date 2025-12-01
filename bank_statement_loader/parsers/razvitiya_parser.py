"""
Парсер выписок Банк Развития Казахстана
Формат: xlsx с текстовым представлением выписки (ВЫПИСКИ ИЗ ЛИЦЕВЫХ СЧЕТОВ)
"""
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import List, Tuple, Union, Optional
import re
import pandas as pd

from .base_parser import BaseParser
from ..models import UnifiedTransaction, StatementMetadata


class RazvitiyaParser(BaseParser):
    """Парсер выписок Банк Развития Казахстана"""

    BANK_NAME = "АО Банк Развития Казахстана"
    BANK_ALIASES = ['банк развития', 'development bank', 'dvkakzka']

    def __init__(self, file_path: Union[str, Path]):
        super().__init__(file_path)

    @classmethod
    def can_parse(cls, file_path: Union[str, Path]) -> bool:
        """Проверка, подходит ли файл для парсера Банка Развития"""
        try:
            path = Path(file_path)
            if path.suffix.lower() not in ('.xlsx', '.xls'):
                return False

            df = pd.read_excel(file_path, header=None, nrows=20)

            for idx in range(min(15, len(df))):
                row = df.iloc[idx]
                row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
                if 'банк развития казахстана' in row_text:
                    return True
                if 'dvkakzka' in row_text:
                    return True
                if 'выписки из лицевых счетов' in row_text:
                    return True

            return False
        except Exception:
            return False

    def parse(self) -> Tuple[StatementMetadata, List[UnifiedTransaction]]:
        """Парсинг файла выписки Банка Развития"""
        df_raw = pd.read_excel(self.file_path, header=None)

        metadata = StatementMetadata()
        metadata.bank_name = self.BANK_NAME
        metadata.source_file = self.file_path.name

        # Выписки Банка Развития имеют текстовый формат
        # Нужно парсить построчно
        transactions = []
        current_section = None

        for idx in range(len(df_raw)):
            row = df_raw.iloc[idx]
            row_text = ' '.join(str(v) for v in row if pd.notna(v))

            if not row_text.strip():
                continue

            row_lower = row_text.lower()

            # Извлекаем метаданные
            if 'cчет n' in row_lower or 'счет n' in row_lower:
                match = re.search(r'(KZ\w+)', row_text)
                if match:
                    metadata.account_number = match.group(1)

            if 'бин:' in row_lower:
                match = re.search(r'БИН:\s*(\d{12})', row_text, re.IGNORECASE)
                if match:
                    metadata.client_bin_iin = match.group(1)

            # Название клиента (обычно после номера счёта)
            if metadata.account_number and not metadata.client_name:
                # Следующая значимая строка может быть именем
                first_val = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ''
                if first_val and not first_val.startswith('ЖЖЖЖ') and 'период' not in first_val.lower():
                    if not re.match(r'^[\d\s\.\-\/]+$', first_val):
                        metadata.client_name = first_val.strip()

            # Период
            if 'период с' in row_lower:
                dates = re.findall(r'(\d{2}\.\d{2}\.\d{2,4})', row_text)
                if len(dates) >= 2:
                    metadata.period_start = self._parse_date(dates[0], ['%d.%m.%y', '%d.%m.%Y'])
                    metadata.period_end = self._parse_date(dates[1], ['%d.%m.%y', '%d.%m.%Y'])

            # Ищем строки с транзакциями
            # Формат: номер | дата | документ | БИК | контрагент | дебет | кредит | описание
            # Проверяем, начинается ли строка с номера и даты
            first_val = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ''

            # Пробуем найти дату в первых колонках
            date_found = None
            for i in range(min(3, len(row))):
                val = row.iloc[i] if pd.notna(row.iloc[i]) else None
                if val:
                    date_found = self._parse_date(val, ['%d.%m.%Y', '%d.%m.%y', '%Y-%m-%d'])
                    if date_found:
                        break

            if date_found:
                # Это может быть строка транзакции
                try:
                    # Ищем суммы (дебет/кредит)
                    amounts = []
                    for i, val in enumerate(row):
                        if pd.notna(val):
                            amount = self._parse_decimal(val)
                            if amount and amount > 0:
                                amounts.append((i, amount))

                    if len(amounts) >= 1:
                        # Берём последние две суммы как дебет и кредит
                        if len(amounts) >= 2:
                            debit_idx, debit = amounts[-2]
                            credit_idx, credit = amounts[-1]
                        else:
                            # Только одна сумма
                            debit = amounts[-1][1]
                            credit = Decimal('0')

                        if credit > 0:
                            direction = 'income'
                            amount = credit
                        else:
                            direction = 'expense'
                            amount = debit

                        if amount > 0:
                            # Собираем описание из текстовых колонок
                            description_parts = []
                            for val in row:
                                if pd.notna(val):
                                    val_str = str(val).strip()
                                    # Исключаем числа и даты
                                    if not re.match(r'^[\d\s\.\,\-]+$', val_str):
                                        if len(val_str) > 3:
                                            description_parts.append(val_str)

                            description = ' '.join(description_parts[:3])

                            transaction = UnifiedTransaction(
                                date=date_found,
                                amount=abs(amount),
                                currency='KZT',
                                amount_kzt=abs(amount),
                                direction=direction,
                                description=description,
                                source_bank=self.BANK_NAME,
                                source_file=self.file_path.name,
                                account_number=metadata.account_number or '',
                            )

                            transactions.append(transaction)

                except Exception as e:
                    continue

        self.metadata = metadata
        self.transactions = transactions
        return metadata, transactions
