"""
Унифицированная модель транзакции для всех банков Казахстана
"""
from dataclasses import dataclass, field, asdict
from datetime import datetime
from decimal import Decimal
from typing import Optional, Dict, Any
import json


@dataclass
class UnifiedTransaction:
    """Унифицированная модель банковской транзакции"""

    # Основные данные операции
    date: datetime                          # Дата и время операции
    amount: Decimal                         # Сумма в валюте операции
    currency: str                           # Валюта операции (KZT, USD, EUR, RUB)
    amount_kzt: Optional[Decimal] = None    # Сумма в тенге (эквивалент)

    # Направление операции
    direction: str = ""                     # "income" (кредит) / "expense" (дебет)

    # Данные плательщика
    payer_name: str = ""                    # ФИО/Наименование плательщика
    payer_bin_iin: str = ""                 # БИН/ИИН плательщика
    payer_bank: str = ""                    # Банк плательщика
    payer_account: str = ""                 # Счёт плательщика
    payer_residency: str = ""               # Резидентство плательщика

    # Данные получателя
    recipient_name: str = ""                # ФИО/Наименование получателя
    recipient_bin_iin: str = ""             # БИН/ИИН получателя
    recipient_bank: str = ""                # Банк получателя
    recipient_account: str = ""             # Счёт получателя
    recipient_residency: str = ""           # Резидентство получателя

    # Детали операции
    operation_type: str = ""                # Тип/категория операции
    knp_code: str = ""                      # Код назначения платежа (КНП)
    description: str = ""                   # Назначение платежа
    document_number: str = ""               # Номер документа

    # Метаданные
    source_bank: str = ""                   # Банк-источник выписки
    source_file: str = ""                   # Имя исходного файла
    account_number: str = ""                # Номер счёта выписки
    raw_data: Dict[str, Any] = field(default_factory=dict)  # Оригинальные данные

    def to_dict(self) -> Dict[str, Any]:
        """Преобразование в словарь для экспорта"""
        result = asdict(self)
        result['date'] = self.date.isoformat() if self.date else None
        result['amount'] = str(self.amount) if self.amount else None
        result['amount_kzt'] = str(self.amount_kzt) if self.amount_kzt else None
        return result

    def to_flat_dict(self) -> Dict[str, Any]:
        """Плоский словарь для Excel (без вложенных структур)"""
        return {
            'Дата операции': self.date.strftime('%Y-%m-%d %H:%M:%S') if self.date else '',
            'Сумма': float(self.amount) if self.amount else 0,
            'Валюта': self.currency,
            'Сумма в тенге': float(self.amount_kzt) if self.amount_kzt else 0,
            'Направление': 'Приход' if self.direction == 'income' else 'Расход',
            'Плательщик': self.payer_name,
            'ИИН/БИН плательщика': self.payer_bin_iin,
            'Банк плательщика': self.payer_bank,
            'Счёт плательщика': self.payer_account,
            'Получатель': self.recipient_name,
            'ИИН/БИН получателя': self.recipient_bin_iin,
            'Банк получателя': self.recipient_bank,
            'Счёт получателя': self.recipient_account,
            'Тип операции': self.operation_type,
            'КНП': self.knp_code,
            'Назначение платежа': self.description,
            'Номер документа': self.document_number,
            'Банк выписки': self.source_bank,
            'Номер счёта': self.account_number,
            'Исходный файл': self.source_file,
        }


@dataclass
class StatementMetadata:
    """Метаданные банковской выписки"""

    bank_name: str = ""                     # Название банка
    client_name: str = ""                   # ФИО/Наименование клиента
    client_bin_iin: str = ""                # БИН/ИИН клиента
    account_number: str = ""                # Номер счёта
    currency: str = "KZT"                   # Валюта счёта
    period_start: Optional[datetime] = None # Начало периода
    period_end: Optional[datetime] = None   # Конец периода
    opening_balance: Optional[Decimal] = None  # Входящий остаток
    closing_balance: Optional[Decimal] = None  # Исходящий остаток
    source_file: str = ""                   # Имя файла

    def to_dict(self) -> Dict[str, Any]:
        return {
            'bank_name': self.bank_name,
            'client_name': self.client_name,
            'client_bin_iin': self.client_bin_iin,
            'account_number': self.account_number,
            'currency': self.currency,
            'period_start': self.period_start.isoformat() if self.period_start else None,
            'period_end': self.period_end.isoformat() if self.period_end else None,
            'opening_balance': str(self.opening_balance) if self.opening_balance else None,
            'closing_balance': str(self.closing_balance) if self.closing_balance else None,
            'source_file': self.source_file,
        }
