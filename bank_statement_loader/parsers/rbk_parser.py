"""
Парсер выписок Bank RBK
Формат: xlsx с чистыми заголовками в строке 0, данные со строки 1
Колонки на английском: POSTING_DATE, TRANS_AMOUNT, FEE_AMOUNT, etc.
"""
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import List, Tuple, Union, Optional
import re
import pandas as pd

from .base_parser import BaseParser
from ..models import UnifiedTransaction, StatementMetadata


class RBKParser(BaseParser):
    """Парсер выписок Bank RBK"""

    BANK_NAME = "АО Bank RBK"
    BANK_ALIASES = ['rbk', 'bank rbk', 'рбк']

    COLUMN_MAPPING = {
        'POSTING_DATE': 'date',
        'TRANS_AMOUNT': 'amount',
        'FEE_AMOUNT': 'fee',
        'TRANS_CURR': 'currency',
        'TRANS_TYPE': 'operation_type',
        'ADDITIONAL_DESC': 'description',
        'AUTH_CODE': 'auth_code',
        'RET_REF_NUMBER': 'ref_number',
        'MEMBER_ID': 'member_id',
        'CPID': 'cpid',
        'TRANS_DATE': 'trans_date',
        'CONTRACT_FOR': 'contract',
        'CLIENT': 'client_name',
        'ITN': 'client_iin',
    }

    def __init__(self, file_path: Union[str, Path]):
        super().__init__(file_path)

    @classmethod
    def can_parse(cls, file_path: Union[str, Path]) -> bool:
        """Проверка, подходит ли файл для парсера RBK"""
        try:
            path = Path(file_path)
            if path.suffix.lower() not in ('.xlsx', '.xls'):
                return False

            df = pd.read_excel(file_path, header=None, nrows=3)
            if len(df) < 1:
                return False

            # Проверяем характерные заголовки RBK
            first_row = [str(v).upper() for v in df.iloc[0] if pd.notna(v)]
            rbk_headers = ['POSTING_DATE', 'TRANS_AMOUNT', 'TRANS_CURR', 'TRANS_TYPE']
            return all(h in first_row for h in rbk_headers)

        except Exception:
            return False

    def _extract_account_from_sheet(self, sheet_name: str) -> str:
        """Извлечение номера счёта из названия листа"""
        if sheet_name.startswith('KZ'):
            return sheet_name
        return ''

    def parse(self) -> Tuple[StatementMetadata, List[UnifiedTransaction]]:
        """Парсинг файла выписки RBK"""
        xl = pd.ExcelFile(self.file_path)

        metadata = StatementMetadata()
        metadata.bank_name = self.BANK_NAME
        metadata.source_file = self.file_path.name

        all_transactions = []

        for sheet_name in xl.sheet_names:
            df = pd.read_excel(self.file_path, sheet_name=sheet_name, header=0)

            account_number = self._extract_account_from_sheet(sheet_name)
            if not metadata.account_number and account_number:
                metadata.account_number = account_number

            # Извлекаем данные клиента из первой строки если есть
            if 'CLIENT' in df.columns and len(df) > 0:
                first_client = df['CLIENT'].iloc[0]
                if pd.notna(first_client):
                    metadata.client_name = str(first_client).strip()
            if 'ITN' in df.columns and len(df) > 0:
                first_iin = df['ITN'].iloc[0]
                if pd.notna(first_iin):
                    metadata.client_bin_iin = self._extract_bin_iin(first_iin)

            for idx, row in df.iterrows():
                try:
                    date_val = row.get('POSTING_DATE')
                    if pd.isna(date_val):
                        continue

                    date = self._parse_date(date_val)
                    if date is None:
                        continue

                    amount = self._parse_decimal(row.get('TRANS_AMOUNT'))
                    if amount is None:
                        continue

                    fee = self._parse_decimal(row.get('FEE_AMOUNT')) or Decimal('0')

                    # Направление определяется по знаку суммы
                    if amount < 0:
                        direction = 'expense'
                        amount = abs(amount)
                    elif amount > 0:
                        direction = 'income'
                    else:
                        # Если amount = 0, смотрим на fee
                        if fee < 0:
                            direction = 'expense'
                            amount = abs(fee)
                        else:
                            continue

                    currency = self._clean_string(row.get('TRANS_CURR', 'KZT'))

                    transaction = UnifiedTransaction(
                        date=date,
                        amount=amount,
                        currency=currency,
                        amount_kzt=amount if currency == 'KZT' else None,
                        direction=direction,
                        payer_name=metadata.client_name if direction == 'expense' else '',
                        payer_bin_iin=metadata.client_bin_iin if direction == 'expense' else '',
                        recipient_name=metadata.client_name if direction == 'income' else '',
                        recipient_bin_iin=metadata.client_bin_iin if direction == 'income' else '',
                        operation_type=self._clean_string(row.get('TRANS_TYPE', '')),
                        description=self._clean_string(row.get('ADDITIONAL_DESC', '')),
                        document_number=self._clean_string(row.get('AUTH_CODE', '')),
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
