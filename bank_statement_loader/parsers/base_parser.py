"""
Базовый класс парсера банковских выписок
"""
from abc import ABC, abstractmethod
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import List, Tuple, Optional, Union
import re
import pandas as pd

from ..models import UnifiedTransaction, StatementMetadata


class BaseParser(ABC):
    """Абстрактный базовый класс для парсеров банковских выписок"""

    BANK_NAME: str = "Unknown Bank"
    SUPPORTED_EXTENSIONS: Tuple[str, ...] = ('.xlsx', '.xls')

    def __init__(self, file_path: Union[str, Path]):
        self.file_path = Path(file_path)
        self.metadata = StatementMetadata()
        self.transactions: List[UnifiedTransaction] = []

    @abstractmethod
    def parse(self) -> Tuple[StatementMetadata, List[UnifiedTransaction]]:
        """
        Парсинг файла выписки
        Returns:
            Tuple[StatementMetadata, List[UnifiedTransaction]]: Метаданные и список транзакций
        """
        pass

    @classmethod
    @abstractmethod
    def can_parse(cls, file_path: Union[str, Path]) -> bool:
        """
        Проверка, может ли данный парсер обработать файл
        Returns:
            bool: True если парсер подходит для этого файла
        """
        pass

    def _read_excel(self, sheet_name: Union[str, int] = 0, header: Optional[int] = None) -> pd.DataFrame:
        """Чтение Excel файла"""
        return pd.read_excel(self.file_path, sheet_name=sheet_name, header=header)

    def _get_excel_sheets(self) -> List[str]:
        """Получение списка листов Excel"""
        xl = pd.ExcelFile(self.file_path)
        return xl.sheet_names

    @staticmethod
    def _parse_date(value: any, formats: List[str] = None) -> Optional[datetime]:
        """
        Парсинг даты из различных форматов
        """
        if pd.isna(value) or value is None or value == '':
            return None

        if isinstance(value, datetime):
            return value

        if isinstance(value, pd.Timestamp):
            return value.to_pydatetime()

        # Стандартные форматы дат
        if formats is None:
            formats = [
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d',
                '%d.%m.%Y %H:%M:%S',
                '%d.%m.%Y',
                '%d.%m.%y',
                '%d/%m/%Y',
                '%Y-%m-%dT%H:%M:%S',
            ]

        str_value = str(value).strip()

        for fmt in formats:
            try:
                return datetime.strptime(str_value, fmt)
            except ValueError:
                continue

        # Попытка парсинга Excel serial date
        try:
            serial = float(str_value)
            if 1 < serial < 100000:  # Разумный диапазон для Excel дат
                return datetime(1899, 12, 30) + pd.Timedelta(days=serial)
        except (ValueError, TypeError):
            pass

        return None

    @staticmethod
    def _parse_decimal(value: any) -> Optional[Decimal]:
        """
        Парсинг числа в Decimal
        """
        if pd.isna(value) or value is None or value == '':
            return None

        if isinstance(value, (int, float)):
            return Decimal(str(value))

        str_value = str(value).strip()

        # Удаляем пробелы, неразрывные пробелы и разделители тысяч
        str_value = re.sub(r'[\s\xa0]', '', str_value)

        # Заменяем запятую на точку для десятичного разделителя
        # Но сначала проверяем формат (1 234,56 vs 1,234.56)
        if ',' in str_value and '.' in str_value:
            # Если есть оба разделителя, определяем какой из них десятичный
            if str_value.rfind(',') > str_value.rfind('.'):
                # Формат: 1.234,56 (европейский)
                str_value = str_value.replace('.', '').replace(',', '.')
            else:
                # Формат: 1,234.56 (американский)
                str_value = str_value.replace(',', '')
        elif ',' in str_value:
            str_value = str_value.replace(',', '.')

        try:
            return Decimal(str_value)
        except InvalidOperation:
            return None

    @staticmethod
    def _clean_string(value: any) -> str:
        """Очистка строкового значения"""
        if pd.isna(value) or value is None:
            return ""
        return str(value).strip()

    @staticmethod
    def _extract_bin_iin(value: any) -> str:
        """Извлечение БИН/ИИН (12 цифр)"""
        if pd.isna(value) or value is None:
            return ""

        str_value = str(value).strip()
        # Удаляем апострофы и другие символы
        str_value = re.sub(r"['\"\s]", "", str_value)

        # Ищем 12 цифр подряд
        match = re.search(r'\d{12}', str_value)
        if match:
            return match.group()

        return str_value

    def _determine_direction(self, credit: Optional[Decimal], debit: Optional[Decimal]) -> str:
        """Определение направления операции"""
        if credit and credit > 0:
            return "income"
        if debit and debit > 0:
            return "expense"
        return ""

    def _determine_direction_by_amount(self, amount: Optional[Decimal]) -> str:
        """Определение направления по знаку суммы"""
        if amount is None:
            return ""
        if amount > 0:
            return "income"
        if amount < 0:
            return "expense"
        return ""
