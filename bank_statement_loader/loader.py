"""
Главный загрузчик банковских выписок
Автоматически определяет банк и парсит файл в унифицированный формат
"""
from pathlib import Path
from typing import List, Tuple, Union, Optional, Type
import pandas as pd

from .models import UnifiedTransaction, StatementMetadata
from .parsers import PARSERS, BaseParser


class BankStatementLoader:
    """
    Загрузчик банковских выписок

    Автоматически определяет формат файла и использует соответствующий парсер.
    Поддерживает экспорт в унифицированный Excel формат.
    """

    def __init__(self):
        self.parsers = PARSERS
        self.last_parser: Optional[BaseParser] = None
        self.last_metadata: Optional[StatementMetadata] = None
        self.last_transactions: List[UnifiedTransaction] = []

    def detect_bank(self, file_path: Union[str, Path]) -> Optional[Type[BaseParser]]:
        """
        Определение банка по содержимому файла

        Args:
            file_path: Путь к файлу выписки

        Returns:
            Класс парсера или None, если банк не определён
        """
        path = Path(file_path)

        if not path.exists():
            raise FileNotFoundError(f"Файл не найден: {file_path}")

        if path.suffix.lower() not in ('.xlsx', '.xls'):
            raise ValueError(f"Неподдерживаемый формат файла: {path.suffix}")

        for parser_class in self.parsers:
            try:
                if parser_class.can_parse(file_path):
                    return parser_class
            except Exception:
                continue

        return None

    def load(self, file_path: Union[str, Path]) -> Tuple[StatementMetadata, List[UnifiedTransaction]]:
        """
        Загрузка и парсинг файла выписки

        Args:
            file_path: Путь к файлу выписки

        Returns:
            Tuple[StatementMetadata, List[UnifiedTransaction]]: Метаданные и список транзакций
        """
        parser_class = self.detect_bank(file_path)

        if parser_class is None:
            raise ValueError(f"Не удалось определить банк для файла: {file_path}")

        parser = parser_class(file_path)
        metadata, transactions = parser.parse()

        self.last_parser = parser
        self.last_metadata = metadata
        self.last_transactions = transactions

        return metadata, transactions

    def load_multiple(self, file_paths: List[Union[str, Path]]) -> Tuple[List[StatementMetadata], List[UnifiedTransaction]]:
        """
        Загрузка нескольких файлов выписок

        Args:
            file_paths: Список путей к файлам

        Returns:
            Tuple[List[StatementMetadata], List[UnifiedTransaction]]: Списки метаданных и транзакций
        """
        all_metadata = []
        all_transactions = []

        for file_path in file_paths:
            try:
                metadata, transactions = self.load(file_path)
                all_metadata.append(metadata)
                all_transactions.extend(transactions)
            except Exception as e:
                print(f"Ошибка обработки файла {file_path}: {e}")
                continue

        return all_metadata, all_transactions

    def load_directory(self, directory: Union[str, Path], recursive: bool = True) -> Tuple[List[StatementMetadata], List[UnifiedTransaction]]:
        """
        Загрузка всех выписок из директории

        Args:
            directory: Путь к директории
            recursive: Рекурсивный поиск в поддиректориях

        Returns:
            Tuple[List[StatementMetadata], List[UnifiedTransaction]]: Списки метаданных и транзакций
        """
        dir_path = Path(directory)

        if not dir_path.is_dir():
            raise NotADirectoryError(f"Не является директорией: {directory}")

        pattern = '**/*.xls*' if recursive else '*.xls*'
        files = list(dir_path.glob(pattern))

        return self.load_multiple(files)

    def to_dataframe(self, transactions: List[UnifiedTransaction] = None) -> pd.DataFrame:
        """
        Преобразование транзакций в pandas DataFrame

        Args:
            transactions: Список транзакций (по умолчанию - последние загруженные)

        Returns:
            pd.DataFrame: DataFrame с унифицированными транзакциями
        """
        if transactions is None:
            transactions = self.last_transactions

        if not transactions:
            return pd.DataFrame()

        data = [t.to_flat_dict() for t in transactions]
        return pd.DataFrame(data)

    def export_to_excel(
        self,
        output_path: Union[str, Path],
        transactions: List[UnifiedTransaction] = None,
        include_metadata: bool = True
    ) -> str:
        """
        Экспорт транзакций в Excel файл

        Args:
            output_path: Путь для сохранения файла
            transactions: Список транзакций (по умолчанию - последние загруженные)
            include_metadata: Включить лист с метаданными

        Returns:
            str: Путь к сохранённому файлу
        """
        if transactions is None:
            transactions = self.last_transactions

        output_path = Path(output_path)

        # Создаём DataFrame
        df = self.to_dataframe(transactions)

        if df.empty:
            raise ValueError("Нет данных для экспорта")

        # Сохраняем в Excel
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Основной лист с транзакциями
            df.to_excel(writer, sheet_name='Транзакции', index=False)

            # Форматирование
            worksheet = writer.sheets['Транзакции']

            # Автоширина колонок
            for idx, col in enumerate(df.columns):
                max_length = max(
                    df[col].astype(str).map(len).max(),
                    len(col)
                ) + 2
                worksheet.column_dimensions[chr(65 + idx) if idx < 26 else chr(64 + idx // 26) + chr(65 + idx % 26)].width = min(max_length, 50)

            # Лист с метаданными
            if include_metadata and self.last_metadata:
                meta_data = self.last_metadata.to_dict()
                meta_df = pd.DataFrame([
                    {'Параметр': k, 'Значение': v}
                    for k, v in meta_data.items()
                ])
                meta_df.to_excel(writer, sheet_name='Метаданные', index=False)

        return str(output_path)

    def get_summary(self, transactions: List[UnifiedTransaction] = None) -> dict:
        """
        Получение сводной информации по транзакциям

        Args:
            transactions: Список транзакций

        Returns:
            dict: Сводная информация
        """
        if transactions is None:
            transactions = self.last_transactions

        if not transactions:
            return {}

        df = self.to_dataframe(transactions)

        income = df[df['Направление'] == 'Приход']['Сумма'].sum()
        expense = df[df['Направление'] == 'Расход']['Сумма'].sum()

        return {
            'total_transactions': len(transactions),
            'total_income': float(income),
            'total_expense': float(expense),
            'balance': float(income - expense),
            'currencies': df['Валюта'].unique().tolist(),
            'banks': df['Банк выписки'].unique().tolist(),
            'date_range': {
                'start': df['Дата операции'].min(),
                'end': df['Дата операции'].max(),
            }
        }


def load_statement(file_path: Union[str, Path]) -> Tuple[StatementMetadata, List[UnifiedTransaction]]:
    """
    Удобная функция для загрузки одного файла выписки

    Args:
        file_path: Путь к файлу выписки

    Returns:
        Tuple[StatementMetadata, List[UnifiedTransaction]]: Метаданные и список транзакций
    """
    loader = BankStatementLoader()
    return loader.load(file_path)


def load_and_export(
    input_path: Union[str, Path],
    output_path: Union[str, Path]
) -> str:
    """
    Загрузка выписки и экспорт в унифицированный Excel

    Args:
        input_path: Путь к исходному файлу или директории
        output_path: Путь для сохранения результата

    Returns:
        str: Путь к сохранённому файлу
    """
    loader = BankStatementLoader()

    input_path = Path(input_path)

    if input_path.is_dir():
        loader.load_directory(input_path)
    else:
        loader.load(input_path)

    return loader.export_to_excel(output_path)
