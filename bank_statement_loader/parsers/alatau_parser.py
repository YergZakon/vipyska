"""
Парсер выписок Alatau City Bank
Формат: xlsx с "Statement", данные в нестандартном формате
"""
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import List, Tuple, Union, Optional
import re
import pandas as pd

from .base_parser import BaseParser
from ..models import UnifiedTransaction, StatementMetadata


class AlatauParser(BaseParser):
    """Парсер выписок Alatau City Bank"""

    BANK_NAME = "АО Alatau City Bank"
    BANK_ALIASES = ['alatau', 'алатау', 'alatau city', 'tseskzka']

    def __init__(self, file_path: Union[str, Path]):
        super().__init__(file_path)

    @classmethod
    def can_parse(cls, file_path: Union[str, Path]) -> bool:
        """Проверка, подходит ли файл для парсера Alatau City Bank"""
        try:
            path = Path(file_path)
            if path.suffix.lower() not in ('.xlsx', '.xls'):
                return False

            # Проверяем имя листа
            xl = pd.ExcelFile(file_path)
            if 'Statement' in xl.sheet_names:
                df = pd.read_excel(file_path, sheet_name='Statement', header=None, nrows=10)
                for idx in range(min(5, len(df))):
                    row = df.iloc[idx]
                    row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
                    if 'alatau city bank' in row_text or 'alatau' in row_text:
                        return True
                    if 'tseskzka' in row_text:
                        return True

            # Проверяем файл по имени
            if 'statement_standard' in path.name.lower():
                df = pd.read_excel(file_path, header=None, nrows=10)
                for idx in range(min(5, len(df))):
                    row = df.iloc[idx]
                    row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
                    if 'alatau' in row_text or 'tseskzka' in row_text:
                        return True

            return False
        except Exception:
            return False

    def parse(self) -> Tuple[StatementMetadata, List[UnifiedTransaction]]:
        """Парсинг файла выписки Alatau City Bank"""
        # Читаем лист Statement если есть
        xl = pd.ExcelFile(self.file_path)
        sheet_name = 'Statement' if 'Statement' in xl.sheet_names else xl.sheet_names[0]

        df_raw = pd.read_excel(self.file_path, sheet_name=sheet_name, header=None)

        metadata = StatementMetadata()
        metadata.bank_name = self.BANK_NAME
        metadata.source_file = self.file_path.name

        # Извлекаем метаданные
        for idx in range(min(15, len(df_raw))):
            row = df_raw.iloc[idx]
            row_text = ' '.join(str(v) for v in row if pd.notna(v))

            # Лицевой счёт
            if 'лицевой счет:' in row_text.lower():
                match = re.search(r'(KZ\w+)', row_text)
                if match:
                    metadata.account_number = match.group(1)

            # Наименование клиента (обычно в следующей ячейке после "Наименование:")
            if 'наименование:' in row_text.lower():
                for i, val in enumerate(row):
                    if pd.notna(val) and 'наименование' in str(val).lower():
                        # Ищем ИИН и ФИО в следующих ячейках
                        for j in range(i + 1, len(row)):
                            v = row.iloc[j]
                            if pd.notna(v):
                                v_str = str(v).strip()
                                if re.match(r'^\d{12}$', v_str):
                                    metadata.client_bin_iin = v_str
                                elif len(v_str) > 3 and not v_str.isdigit():
                                    metadata.client_name = v_str

        # Находим строку с заголовками данных
        header_row = None
        for idx in range(10, min(30, len(df_raw))):
            row = df_raw.iloc[idx]
            row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
            if 'дата' in row_text and ('сумма' in row_text or 'дебет' in row_text or 'кредит' in row_text):
                header_row = idx
                break

        if header_row is None:
            # Файл без транзакций
            self.metadata = metadata
            self.transactions = []
            return metadata, []

        # Читаем данные с заголовками
        df = pd.read_excel(self.file_path, sheet_name=sheet_name, header=header_row)

        # Маппинг колонок
        col_map = {}
        for col in df.columns:
            col_lower = str(col).lower()
            if 'дата' in col_lower:
                col_map['date'] = col
            elif 'дебет' in col_lower:
                col_map['debit'] = col
            elif 'кредит' in col_lower:
                col_map['credit'] = col
            elif 'сумма' in col_lower:
                col_map['amount'] = col
            elif 'назначение' in col_lower or 'описание' in col_lower:
                col_map['description'] = col
            elif 'контрагент' in col_lower or 'наименование' in col_lower:
                col_map['counterparty'] = col
            elif 'иин' in col_lower or 'бин' in col_lower:
                col_map['bin_iin'] = col

        transactions = []

        for idx, row in df.iterrows():
            try:
                date_val = row.get(col_map.get('date'))
                if pd.isna(date_val):
                    continue

                date = self._parse_date(date_val)
                if date is None:
                    continue

                # Определяем сумму и направление
                if 'debit' in col_map and 'credit' in col_map:
                    debit = self._parse_decimal(row.get(col_map.get('debit')))
                    credit = self._parse_decimal(row.get(col_map.get('credit')))

                    if credit and credit > 0:
                        direction = 'income'
                        amount = credit
                    elif debit and debit > 0:
                        direction = 'expense'
                        amount = debit
                    else:
                        continue
                else:
                    amount = self._parse_decimal(row.get(col_map.get('amount')))
                    if amount is None:
                        continue
                    direction = 'income' if amount > 0 else 'expense'
                    amount = abs(amount)

                if amount == 0:
                    continue

                description = self._clean_string(row.get(col_map.get('description'), ''))
                counterparty = self._clean_string(row.get(col_map.get('counterparty'), ''))
                counterparty_bin = self._extract_bin_iin(row.get(col_map.get('bin_iin'), ''))

                if direction == 'income':
                    payer_name = counterparty
                    payer_bin = counterparty_bin
                    recipient_name = metadata.client_name or ''
                    recipient_bin = metadata.client_bin_iin or ''
                else:
                    payer_name = metadata.client_name or ''
                    payer_bin = metadata.client_bin_iin or ''
                    recipient_name = counterparty
                    recipient_bin = counterparty_bin

                transaction = UnifiedTransaction(
                    date=date,
                    amount=abs(amount),
                    currency='KZT',
                    amount_kzt=abs(amount),
                    direction=direction,
                    payer_name=payer_name,
                    payer_bin_iin=payer_bin,
                    recipient_name=recipient_name,
                    recipient_bin_iin=recipient_bin,
                    description=description,
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
