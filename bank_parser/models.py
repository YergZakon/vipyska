"""Unified transaction model — 20 fields matching check.xlsx target format."""

from dataclasses import dataclass, asdict, fields
from typing import Optional


@dataclass
class Transaction:
    transaction_date: Optional[str] = None        # Дата операции
    amount: Optional[float] = None                # Сумма
    currency: Optional[str] = None                # Валюта
    amount_tenge: Optional[float] = None          # Сумма в тенге
    direction: Optional[str] = None               # Направление (Приход/Расход)
    payer: Optional[str] = None                   # Плательщик
    payer_iin_bin: Optional[str] = None           # ИИН/БИН плательщика
    payer_bank: Optional[str] = None              # Банк плательщика
    payer_account: Optional[str] = None           # Счёт плательщика
    recipient: Optional[str] = None               # Получатель
    recipient_iin_bin: Optional[str] = None       # ИИН/БИН получателя
    recipient_bank: Optional[str] = None          # Банк получателя
    recipient_account: Optional[str] = None       # Счёт получателя
    operation_type: Optional[str] = None          # Тип операции
    knp: Optional[str] = None                     # КНП
    payment_purpose: Optional[str] = None         # Назначение платежа
    document_number: Optional[str] = None         # Номер документа
    statement_bank: Optional[str] = None          # Банк выписки
    account_number: Optional[str] = None          # Номер счёта
    source_file: Optional[str] = None             # Исходный файл

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def field_names() -> list:
        return [f.name for f in fields(Transaction)]

    @staticmethod
    def russian_headers() -> list:
        return [
            'Дата операции', 'Сумма', 'Валюта', 'Сумма в тенге',
            'Направление', 'Плательщик', 'ИИН/БИН плательщика',
            'Банк плательщика', 'Счёт плательщика', 'Получатель',
            'ИИН/БИН получателя', 'Банк получателя', 'Счёт получателя',
            'Тип операции', 'КНП', 'Назначение платежа',
            'Номер документа', 'Банк выписки', 'Номер счёта', 'Исходный файл',
        ]


@dataclass
class ParseResult:
    """Result of parsing a single file."""
    filepath: str
    source_file: str = ''
    bank_detected: Optional[str] = None
    parser_used: Optional[str] = None
    account_number: Optional[str] = None
    parse_status: str = 'pending'  # pending | success | partial | failed | skipped
    total_transactions: int = 0
    errors: list = None
    warnings: list = None
    transactions: list = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []
        if self.warnings is None:
            self.warnings = []
        if self.transactions is None:
            self.transactions = []

    def to_dict(self) -> dict:
        return {
            'source_file': self.source_file,
            'bank_detected': self.bank_detected,
            'parser_used': self.parser_used,
            'account_number': self.account_number,
            'parse_status': self.parse_status,
            'total_transactions': self.total_transactions,
            'errors': self.errors,
            'warnings': self.warnings,
            'transactions': [t.to_dict() for t in self.transactions],
        }
