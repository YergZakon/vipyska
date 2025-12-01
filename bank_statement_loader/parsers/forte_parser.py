"""
Парсер выписок ForteBank
Формат: xlsx с отчётом о переводах (SDP.xlsx)
Заголовки в строке 8, данные начинаются с строки 9
"""
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import List, Tuple, Union, Optional
import re
import pandas as pd

from .base_parser import BaseParser
from ..models import UnifiedTransaction, StatementMetadata


class ForteParser(BaseParser):
    """Парсер выписок ForteBank"""

    BANK_NAME = "АО ForteBank"
    BANK_ALIASES = ['forte', 'форте', 'fortebank', 'форте банк']

    # Маппинг колонок ForteBank (файл SDP.xlsx)
    COLUMN_MAPPING = {
        '№ п/п': 'row_number',
        'Наименование филиала': 'branch',
        'Назначение платежа': 'description',
        'Состояние перевода': 'status',
        'Дата': 'date',
        'Номер документа': 'document_number',
        'Валюта': 'currency',
        'Сумма': 'amount',
        'ФИО отправителя/получателя': 'sender_name',
        'ИИН': 'sender_iin',
        'Данные документа уд.личности': 'id_document',
        'ФИО получателя / отправителя': 'recipient_name',
        'Направление': 'direction',
        'Страна получателя/отправителя': 'country',
    }

    def __init__(self, file_path: Union[str, Path]):
        super().__init__(file_path)

    @classmethod
    def can_parse(cls, file_path: Union[str, Path]) -> bool:
        """Проверка, подходит ли файл для парсера ForteBank"""
        try:
            path = Path(file_path)
            if path.suffix.lower() not in ('.xlsx', '.xls'):
                return False

            df = pd.read_excel(file_path, header=None, nrows=15)

            # Проверяем характерные признаки ForteBank
            for idx in range(min(12, len(df))):
                row = df.iloc[idx]
                row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
                if any(alias in row_text for alias in cls.BANK_ALIASES):
                    return True
                # Проверяем характерные заголовки SDP
                if 'направление' in row_text and 'состояние перевода' in row_text:
                    return True
                if 'инфорация по переводам' in row_text or 'информация по переводам' in row_text:
                    return True

            return False
        except Exception:
            return False

    def _parse_metadata(self, df: pd.DataFrame) -> StatementMetadata:
        """Извлечение метаданных из заголовочной части"""
        metadata = StatementMetadata()
        metadata.bank_name = self.BANK_NAME
        metadata.source_file = self.file_path.name

        for idx in range(min(10, len(df))):
            row = df.iloc[idx]
            row_values = [str(v).strip() for v in row if pd.notna(v) and str(v).strip()]

            for i, val in enumerate(row_values):
                val_lower = val.lower()

                # Наименование клиента
                if 'по наименованию клиента' in val_lower:
                    if i + 1 < len(row_values):
                        metadata.client_name = row_values[i + 1]

                # БИН/ИИН клиента
                if 'по бин/иин клиента' in val_lower:
                    if i + 1 < len(row_values):
                        metadata.client_bin_iin = self._extract_bin_iin(row_values[i + 1])

        return metadata

    def _find_header_row(self, df: pd.DataFrame) -> int:
        """Поиск строки с заголовками"""
        for idx in range(min(15, len(df))):
            row = df.iloc[idx]
            row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
            if '№ п/п' in row_text and 'дата' in row_text and 'сумма' in row_text:
                return idx
            if 'направление' in row_text and 'валюта' in row_text:
                return idx
        return 8  # По умолчанию

    def _parse_amount(self, value: any) -> Optional[Decimal]:
        """Парсинг суммы с учётом форматирования ForteBank"""
        if pd.isna(value) or value is None:
            return None

        str_value = str(value).strip()
        # Удаляем пробелы и неразрывные пробелы (разделители тысяч)
        str_value = re.sub(r'[\s\xa0]', '', str_value)
        # Заменяем запятую на точку
        str_value = str_value.replace(',', '.')

        try:
            return Decimal(str_value)
        except:
            return None

    def parse(self) -> Tuple[StatementMetadata, List[UnifiedTransaction]]:
        """Парсинг файла выписки ForteBank"""
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
            col_lower = col_str.lower()

            if '№' in col_lower and 'п/п' in col_lower:
                col_map['row_number'] = col
            elif col_lower == 'дата':
                col_map['date'] = col
            elif 'сумма' in col_lower:
                col_map['amount'] = col
            elif 'валюта' in col_lower:
                col_map['currency'] = col
            elif 'направление' in col_lower:
                col_map['direction'] = col
            elif 'назначение' in col_lower:
                col_map['description'] = col
            elif 'состояние' in col_lower:
                col_map['status'] = col
            elif 'номер документа' in col_lower:
                col_map['document_number'] = col
            elif 'фио отправителя' in col_lower:
                col_map['sender_name'] = col
            elif 'иин' in col_lower and 'бин' not in col_lower:
                col_map['sender_iin'] = col
            elif 'фио получателя' in col_lower:
                col_map['recipient_name'] = col
            elif 'страна' in col_lower:
                col_map['country'] = col
            elif 'филиал' in col_lower:
                col_map['branch'] = col
            elif 'документ' in col_lower and 'уд' in col_lower:
                col_map['id_document'] = col

        # Парсим транзакции
        transactions = []
        for idx, row in df.iterrows():
            try:
                # Проверяем номер строки или дату
                row_num = row.get(col_map.get('row_number'))
                date_val = row.get(col_map.get('date'))

                if pd.isna(date_val) and pd.isna(row_num):
                    continue

                # Парсим дату
                date = self._parse_date(date_val)
                if date is None:
                    continue

                # Парсим сумму
                amount = self._parse_amount(row.get(col_map.get('amount')))
                if amount is None:
                    continue

                # Определяем направление
                direction_val = self._clean_string(row.get(col_map.get('direction'), ''))
                if 'входящ' in direction_val.lower():
                    direction = 'income'
                elif 'исходящ' in direction_val.lower():
                    direction = 'expense'
                else:
                    direction = ''

                currency = self._clean_string(row.get(col_map.get('currency'), 'KZT'))
                if not currency:
                    currency = 'KZT'

                # Определяем плательщика и получателя в зависимости от направления
                sender_name = self._clean_string(row.get(col_map.get('sender_name'), ''))
                recipient_name = self._clean_string(row.get(col_map.get('recipient_name'), ''))
                sender_iin = self._extract_bin_iin(row.get(col_map.get('sender_iin'), ''))

                if direction == 'income':
                    payer_name = sender_name
                    payer_bin_iin = sender_iin
                    rec_name = recipient_name if recipient_name else self.metadata.client_name
                    rec_bin_iin = self.metadata.client_bin_iin
                else:
                    payer_name = self.metadata.client_name
                    payer_bin_iin = self.metadata.client_bin_iin
                    rec_name = recipient_name if recipient_name else sender_name
                    rec_bin_iin = sender_iin

                transaction = UnifiedTransaction(
                    date=date,
                    amount=abs(amount),
                    currency=currency,
                    amount_kzt=abs(amount) if currency == 'KZT' else None,
                    direction=direction,
                    payer_name=payer_name,
                    payer_bin_iin=payer_bin_iin,
                    recipient_name=rec_name,
                    recipient_bin_iin=rec_bin_iin,
                    description=self._clean_string(row.get(col_map.get('description'), '')),
                    document_number=self._clean_string(row.get(col_map.get('document_number'), '')),
                    operation_type=self._clean_string(row.get(col_map.get('status'), '')),
                    source_bank=self.BANK_NAME,
                    source_file=self.file_path.name,
                    account_number='',
                )

                transactions.append(transaction)

            except Exception as e:
                print(f"Ошибка парсинга строки {idx}: {e}")
                continue

        self.transactions = transactions
        return self.metadata, self.transactions
