"""
Парсер выписок Цеснабанк
Формат: xlsx с информацией по дебетовым/кредитовым оборотам
"""
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import List, Tuple, Union, Optional
import re
import pandas as pd

from .base_parser import BaseParser
from ..models import UnifiedTransaction, StatementMetadata


class CesnaParser(BaseParser):
    """Парсер выписок Цеснабанк"""

    BANK_NAME = "АО Цеснабанк"
    BANK_ALIASES = ['цеснабанк', 'cesna', 'tseskzka']

    def __init__(self, file_path: Union[str, Path]):
        super().__init__(file_path)

    @classmethod
    def can_parse(cls, file_path: Union[str, Path]) -> bool:
        """Проверка, подходит ли файл для парсера Цеснабанк"""
        try:
            path = Path(file_path)
            if path.suffix.lower() not in ('.xlsx', '.xls'):
                return False

            df = pd.read_excel(file_path, header=None, nrows=15)

            for idx in range(min(10, len(df))):
                row = df.iloc[idx]
                row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
                if 'цеснабанк' in row_text:
                    return True
                if 'tseskzka' in row_text:
                    return True

            return False
        except Exception:
            return False

    def parse(self) -> Tuple[StatementMetadata, List[UnifiedTransaction]]:
        """Парсинг файла выписки Цеснабанк"""
        df_raw = pd.read_excel(self.file_path, header=None)

        metadata = StatementMetadata()
        metadata.bank_name = self.BANK_NAME
        metadata.source_file = self.file_path.name

        # Извлекаем метаданные
        header_row = None
        for idx in range(min(20, len(df_raw))):
            row = df_raw.iloc[idx]
            row_text = ' '.join(str(v) for v in row if pd.notna(v))
            row_lower = row_text.lower()

            # Период
            if 'за период с' in row_lower:
                dates = re.findall(r'(\d{2}\.\d{2}\.\d{4})', row_text)
                if len(dates) >= 2:
                    metadata.period_start = self._parse_date(dates[0], ['%d.%m.%Y'])
                    metadata.period_end = self._parse_date(dates[1], ['%d.%m.%Y'])

            # Лицевой счет
            if 'лицевой счет' in row_lower:
                match = re.search(r'(KZ\w+)', row_text)
                if match:
                    metadata.account_number = match.group(1)

            # Наименование
            if 'наименование:' in row_lower:
                parts = row_text.split(':')
                if len(parts) > 1:
                    metadata.client_name = parts[1].strip()

            # БИН
            if row_lower.startswith('бин ') or 'бин ' in row_lower:
                match = re.search(r'БИН\s*(\d{12})', row_text, re.IGNORECASE)
                if match:
                    metadata.client_bin_iin = match.group(1)

            # Ищем строку заголовков
            if 'дата' in row_lower and ('документа' in row_lower or 'вид операции' in row_lower):
                header_row = idx
                break

        if header_row is None:
            self.metadata = metadata
            self.transactions = []
            return metadata, []

        # Читаем данные с заголовками
        df = pd.read_excel(self.file_path, header=header_row)

        # Маппинг колонок
        col_map = {}
        for col in df.columns:
            col_lower = str(col).lower().strip()
            if col_lower == 'дата' or 'дата проводки' in col_lower:
                col_map['date'] = col
            elif 'дата документа' in col_lower:
                col_map['doc_date'] = col
            elif '№ документа' in col_lower or 'номер документа' in col_lower:
                col_map['doc_number'] = col
            elif 'вид операции' in col_lower:
                col_map['operation_type'] = col
            elif 'бик' in col_lower or 'swift' in col_lower:
                col_map['bik'] = col
            elif 'счет-корреспондент' in col_lower or 'счет корреспондента' in col_lower:
                col_map['corr_account'] = col
            elif 'бин' in col_lower or 'иин' in col_lower:
                col_map['bin_iin'] = col
            elif 'наименование' in col_lower and 'корреспондент' in col_lower:
                col_map['counterparty'] = col
            elif 'сумма' in col_lower and 'дебет' in col_lower:
                col_map['debit'] = col
            elif 'сумма' in col_lower and 'кредит' in col_lower:
                col_map['credit'] = col
            elif col_lower == 'сумма':
                col_map['amount'] = col
            elif 'назначение' in col_lower:
                col_map['description'] = col

        transactions = []

        for idx, row in df.iterrows():
            try:
                date_val = row.get(col_map.get('date')) or row.get(col_map.get('doc_date'))
                if pd.isna(date_val):
                    continue

                date = self._parse_date(date_val, ['%d.%m.%y', '%d.%m.%Y', '%Y-%m-%d'])
                if date is None:
                    continue

                # Определяем сумму и направление
                debit = self._parse_decimal(row.get(col_map.get('debit')))
                credit = self._parse_decimal(row.get(col_map.get('credit')))
                amount_single = self._parse_decimal(row.get(col_map.get('amount')))

                if credit and credit > 0:
                    direction = 'income'
                    amount = credit
                elif debit and debit > 0:
                    direction = 'expense'
                    amount = debit
                elif amount_single and amount_single > 0:
                    # Если только одна колонка суммы - это дебетовые обороты (расход)
                    direction = 'expense'
                    amount = amount_single
                else:
                    continue

                if amount == 0:
                    continue

                counterparty = self._clean_string(row.get(col_map.get('counterparty'), ''))
                counterparty_bin = self._extract_bin_iin(row.get(col_map.get('bin_iin'), ''))
                description = self._clean_string(row.get(col_map.get('description'), ''))

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
                    document_number=self._clean_string(row.get(col_map.get('doc_number'), '')),
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
