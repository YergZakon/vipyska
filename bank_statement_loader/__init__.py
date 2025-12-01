"""
Bank Statement Loader - Загрузчик банковских выписок Казахстана

Поддерживаемые банки:
- АО Народный Банк Казахстана (Halyk Bank)
- АО Kaspi Bank
- АО Банк ЦентрКредит
- АО Фридом Банк Казахстан (Freedom Finance Bank)
- АО ForteBank

Использование:
    from bank_statement_loader import BankStatementLoader, load_statement

    # Загрузка одного файла
    loader = BankStatementLoader()
    metadata, transactions = loader.load('path/to/statement.xlsx')

    # Экспорт в унифицированный Excel
    loader.export_to_excel('output.xlsx')

    # Или короткая функция
    from bank_statement_loader import load_and_export
    load_and_export('input.xlsx', 'output.xlsx')
"""

from .models import UnifiedTransaction, StatementMetadata
from .loader import BankStatementLoader, load_statement, load_and_export
from .parsers import (
    BaseParser,
    HalykParser,
    KaspiParser,
    CenterCreditParser,
    FreedomParser,
    ForteParser,
    PARSERS,
)

__version__ = '1.0.0'
__all__ = [
    # Основные классы
    'BankStatementLoader',
    'UnifiedTransaction',
    'StatementMetadata',

    # Удобные функции
    'load_statement',
    'load_and_export',

    # Парсеры
    'BaseParser',
    'HalykParser',
    'KaspiParser',
    'CenterCreditParser',
    'FreedomParser',
    'ForteParser',
    'PARSERS',
]
