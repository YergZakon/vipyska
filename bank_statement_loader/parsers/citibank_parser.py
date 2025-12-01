"""
Парсер справок Ситибанк Казахстан
Формат: xlsx - справки по движению денег/товаров (не транзакционные выписки)
"""
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import List, Tuple, Union, Optional
import re
import pandas as pd

from .base_parser import BaseParser
from ..models import UnifiedTransaction, StatementMetadata


class CitibankParser(BaseParser):
    """Парсер справок Ситибанк Казахстан"""

    BANK_NAME = "АО Ситибанк Казахстан"
    BANK_ALIASES = ['ситибанк', 'citibank', 'citi']

    def __init__(self, file_path: Union[str, Path]):
        super().__init__(file_path)

    @classmethod
    def can_parse(cls, file_path: Union[str, Path]) -> bool:
        """Проверка, подходит ли файл для парсера Ситибанк"""
        try:
            path = Path(file_path)
            if path.suffix.lower() not in ('.xlsx', '.xls'):
                return False

            # Проверяем по названию файла (справки)
            if 'справка' in path.name.lower() or 'spsd' in path.name.lower():
                df = pd.read_excel(file_path, header=None, nrows=10)
                for idx in range(min(8, len(df))):
                    row = df.iloc[idx]
                    row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
                    if 'справка по движению' in row_text:
                        return True
                    if 'наименование клиента' in row_text and 'номер договора' in row_text:
                        return True

            return False
        except Exception:
            return False

    def parse(self) -> Tuple[StatementMetadata, List[UnifiedTransaction]]:
        """Парсинг справки Ситибанк"""
        df_raw = pd.read_excel(self.file_path, header=None)

        metadata = StatementMetadata()
        metadata.bank_name = self.BANK_NAME
        metadata.source_file = self.file_path.name

        # Справки Ситибанка - это не транзакционные выписки,
        # а справочные документы о договорах
        # Извлекаем основную информацию

        for idx in range(min(20, len(df_raw))):
            row = df_raw.iloc[idx]
            for i, val in enumerate(row):
                if pd.isna(val):
                    continue
                val_str = str(val).lower()

                if 'наименование клиента' in val_str:
                    # Следующая колонка содержит значение
                    if i + 1 < len(row) and pd.notna(row.iloc[i + 1]):
                        metadata.client_name = str(row.iloc[i + 1]).strip()

                if 'номер договора' in val_str:
                    if i + 1 < len(row) and pd.notna(row.iloc[i + 1]):
                        metadata.account_number = str(row.iloc[i + 1]).strip()

                if 'сумма договора' in val_str:
                    if i + 1 < len(row) and pd.notna(row.iloc[i + 1]):
                        pass  # Сумма договора

                if 'валюта договора' in val_str:
                    if i + 1 < len(row) and pd.notna(row.iloc[i + 1]):
                        metadata.currency = str(row.iloc[i + 1]).strip()

        # Справки не содержат транзакций в обычном понимании
        # Это документы о договорах с инопартнёрами
        # Возвращаем пустой список транзакций

        self.metadata = metadata
        self.transactions = []
        return metadata, []
