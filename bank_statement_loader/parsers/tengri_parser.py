"""
Парсер выписок Tengri Bank
Формат: xlsx с метаданными в строках 0-10, заголовки в строке 11, данные с строки 13
"""
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import List, Tuple, Union, Optional
import re
import pandas as pd

from .base_parser import BaseParser
from ..models import UnifiedTransaction, StatementMetadata


class TengriParser(BaseParser):
    """Парсер выписок Tengri Bank"""

    BANK_NAME = "АО Tengri Bank"
    BANK_ALIASES = ['tengri', 'тенгри', 'tengri bank']

    def __init__(self, file_path: Union[str, Path]):
        super().__init__(file_path)

    @classmethod
    def can_parse(cls, file_path: Union[str, Path]) -> bool:
        """Проверка, подходит ли файл для парсера Tengri Bank"""
        try:
            path = Path(file_path)
            if path.suffix.lower() not in ('.xlsx', '.xls'):
                return False

            df = pd.read_excel(file_path, header=None, nrows=10)

            for idx in range(min(5, len(df))):
                row = df.iloc[idx]
                row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
                if 'tengri bank' in row_text or 'tengri' in row_text:
                    return True

            return False
        except Exception:
            return False

    def parse(self) -> Tuple[StatementMetadata, List[UnifiedTransaction]]:
        """Парсинг файла выписки Tengri Bank"""
        df_raw = pd.read_excel(self.file_path, header=None)

        metadata = StatementMetadata()
        metadata.bank_name = self.BANK_NAME
        metadata.source_file = self.file_path.name

        # Извлекаем метаданные
        for idx in range(min(12, len(df_raw))):
            row = df_raw.iloc[idx]
            row_text = ' '.join(str(v) for v in row if pd.notna(v))

            # Клиент и БИН/ИИН
            if 'клиент:' in row_text.lower():
                match = re.search(r'Клиент:\s*(.+?)\s+ИИН/БИН\s+(\d{12})', row_text, re.IGNORECASE)
                if match:
                    metadata.client_name = match.group(1).strip()
                    metadata.client_bin_iin = match.group(2)

            # Лицевой счёт
            if 'лицевой счет:' in row_text.lower():
                match = re.search(r'(KZ\w+)', row_text)
                if match:
                    metadata.account_number = match.group(1)

            # Валюта
            if 'валюта:' in row_text.lower():
                match = re.search(r'Валюта:\s*(\w+)', row_text, re.IGNORECASE)
                if match:
                    metadata.currency = match.group(1)

        # Находим строку с заголовками
        header_row = 11
        for idx in range(8, min(15, len(df_raw))):
            row = df_raw.iloc[idx]
            row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
            if 'дата' in row_text and 'документа' in row_text and 'дебет' in row_text:
                header_row = idx
                break

        # Маппинг колонок (по индексам, т.к. заголовки сложные)
        col_indices = {
            'date': 0,
            'doc_number': 1,
            'doc_type': 2,
            'bik': 3,
            'correspondent_account': 4,
            'bin_iin': 5,
            'sgk': 6,
            'counterparty': 7,
            'debit': 8,
            'debit_kzt': 9,
            'credit': 10,
            'credit_kzt': 11,
            'description': 12,
        }

        transactions = []
        data_start = header_row + 2  # Пропускаем строку с номерами

        for idx in range(data_start, len(df_raw)):
            row = df_raw.iloc[idx]

            try:
                date_val = row.iloc[col_indices['date']] if len(row) > col_indices['date'] else None
                if pd.isna(date_val):
                    continue

                date = self._parse_date(date_val, formats=['%d/%m/%Y', '%d.%m.%Y', '%Y-%m-%d'])
                if date is None:
                    continue

                # Определяем дебет и кредит
                debit = self._parse_decimal(row.iloc[col_indices['debit']] if len(row) > col_indices['debit'] else None)
                credit = self._parse_decimal(row.iloc[col_indices['credit']] if len(row) > col_indices['credit'] else None)

                if debit is None and credit is None:
                    continue
                if (debit is None or debit == 0) and (credit is None or credit == 0):
                    continue

                # Направление и сумма
                if credit and credit > 0:
                    direction = 'income'
                    amount = credit
                    amount_kzt = self._parse_decimal(row.iloc[col_indices['credit_kzt']] if len(row) > col_indices['credit_kzt'] else None)
                else:
                    direction = 'expense'
                    amount = debit if debit else Decimal(0)
                    amount_kzt = self._parse_decimal(row.iloc[col_indices['debit_kzt']] if len(row) > col_indices['debit_kzt'] else None)

                counterparty = self._clean_string(row.iloc[col_indices['counterparty']] if len(row) > col_indices['counterparty'] else '')
                counterparty_bin = self._extract_bin_iin(row.iloc[col_indices['bin_iin']] if len(row) > col_indices['bin_iin'] else '')
                description = self._clean_string(row.iloc[col_indices['description']] if len(row) > col_indices['description'] else '')

                # Разделяем контрагента (формат: "Банк / Наименование")
                if ' / ' in counterparty:
                    parts = counterparty.split(' / ', 1)
                    counterparty_bank = parts[0].strip()
                    counterparty_name = parts[1].strip()
                else:
                    counterparty_bank = ''
                    counterparty_name = counterparty

                if direction == 'income':
                    payer_name = counterparty_name
                    payer_bin = counterparty_bin
                    payer_bank = counterparty_bank
                    recipient_name = metadata.client_name or ''
                    recipient_bin = metadata.client_bin_iin or ''
                    recipient_bank = self.BANK_NAME
                else:
                    payer_name = metadata.client_name or ''
                    payer_bin = metadata.client_bin_iin or ''
                    payer_bank = self.BANK_NAME
                    recipient_name = counterparty_name
                    recipient_bin = counterparty_bin
                    recipient_bank = counterparty_bank

                transaction = UnifiedTransaction(
                    date=date,
                    amount=abs(amount),
                    currency=metadata.currency or 'KZT',
                    amount_kzt=abs(amount_kzt) if amount_kzt else None,
                    direction=direction,
                    payer_name=payer_name,
                    payer_bin_iin=payer_bin,
                    payer_bank=payer_bank,
                    recipient_name=recipient_name,
                    recipient_bin_iin=recipient_bin,
                    recipient_bank=recipient_bank,
                    description=description,
                    document_number=self._clean_string(row.iloc[col_indices['doc_number']] if len(row) > col_indices['doc_number'] else ''),
                    operation_type=self._clean_string(row.iloc[col_indices['doc_type']] if len(row) > col_indices['doc_type'] else ''),
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
