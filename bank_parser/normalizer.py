"""Data normalization utilities for dates, BIN/IIN, amounts, currency, direction."""

from datetime import datetime
from typing import Optional
import re


# Date formats observed across all banks (ordered by frequency)
DATE_FORMATS = [
    '%Y-%m-%d %H:%M:%S',          # 2023-12-20 17:19:00
    '%Y-%m-%dT%H:%M:%S.%f',       # 2024-10-16T17:00:23.000
    '%Y-%m-%dT%H:%M:%S',          # 2024-10-16T17:00:23
    '%Y.%m.%d %H:%M:%S',          # 2024.11.22 15:49:14
    '%d.%m.%Y %H:%M:%S',          # 07.02.2020 00:00:00
    '%d/%m/%Y %H:%M:%S',          # 06/02/2020 09:18:48
    '%d.%m.%Y',                    # 07.02.2020
    '%d/%m/%Y',                    # 14/06/2017
    '%Y-%m-%d',                    # 2023-12-20
    '%d.%m.%y',                    # 06.08.15 (2-digit year)
]


def normalize_date(value) -> Optional[str]:
    """Normalize any date format to ISO 8601 string."""
    if value is None:
        return None

    if isinstance(value, datetime):
        if value.hour == 0 and value.minute == 0 and value.second == 0:
            return value.strftime('%Y-%m-%d')
        return value.strftime('%Y-%m-%d %H:%M:%S')

    s = str(value).strip()
    if not s:
        return None

    for fmt in DATE_FORMATS:
        try:
            dt = datetime.strptime(s, fmt)
            if dt.hour == 0 and dt.minute == 0 and dt.second == 0:
                return dt.strftime('%Y-%m-%d')
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            continue

    # Return raw value if nothing matched
    return s


def normalize_iin_bin(value) -> Optional[str]:
    """Normalize IIN/BIN to 12-digit string, preserving leading zeros."""
    if value is None:
        return None

    s = str(value).strip()
    if not s or s.lower() == 'none':
        return None

    # Remove leading apostrophe (some banks use it)
    s = s.lstrip("'")
    # Handle float representation (030740001404.0 -> 030740001404)
    if '.' in s and s.replace('.', '').replace('0', '') != '':
        s = s.split('.')[0]
    # Remove non-breaking spaces
    s = s.replace('\xa0', '').replace(' ', '')
    # Remove any non-digit characters
    digits = re.sub(r'\D', '', s)

    if not digits:
        return s if s else None

    # Pad to 12 digits if shorter (lost leading zeros from Excel)
    if len(digits) < 12:
        digits = digits.zfill(12)

    return digits


def normalize_amount(value) -> Optional[float]:
    """Normalize amount to float."""
    if value is None:
        return None

    if isinstance(value, (int, float)):
        return round(float(value), 2)

    s = str(value).strip()
    if not s:
        return None

    # Remove thousand separators (non-breaking space, regular space)
    s = s.replace('\xa0', '').replace(' ', '')
    # Remove currency symbols
    s = s.replace('₸', '').replace('$', '').replace('€', '')
    # Replace comma decimal separator with dot
    # But only if there's no dot already (to handle "1,234.56" vs "1234,56")
    if ',' in s and '.' not in s:
        s = s.replace(',', '.')
    elif ',' in s and '.' in s:
        # "1,234.56" format — remove comma as thousand separator
        s = s.replace(',', '')

    try:
        return round(float(s), 2)
    except ValueError:
        return None


def normalize_amount_abs(value) -> Optional[float]:
    """Normalize amount and return absolute value."""
    result = normalize_amount(value)
    if result is not None:
        return abs(result)
    return None


def normalize_currency(value) -> Optional[str]:
    """Normalize currency to ISO code."""
    if value is None:
        return None

    s = str(value).strip()
    if not s:
        return None

    # Extract ISO code before dash ("KZT - Тенге" -> "KZT")
    if ' - ' in s:
        s = s.split(' - ')[0].strip()

    # Common mappings
    mapping = {
        'ТЕНГЕ': 'KZT', 'ТГ': 'KZT', 'ТЕНГЕ (KZT)': 'KZT',
        'ДОЛЛАР': 'USD', 'ДОЛЛАР США': 'USD',
        'ЕВРО': 'EUR',
        'ЮАНЬ': 'CNY', 'КИТАЙСКИЙ ЮАНЬ': 'CNY',
        'РУБЛЬ': 'RUB', 'РОССИЙСКИЙ РУБЛЬ': 'RUB',
    }

    upper = s.upper()
    if upper in mapping:
        return mapping[upper]

    # Already an ISO code?
    if len(s) == 3 and s.isalpha():
        return s.upper()

    # Numeric currency codes
    numeric_map = {'398': 'KZT', '840': 'USD', '978': 'EUR', '156': 'CNY', '643': 'RUB'}
    if s in numeric_map:
        return numeric_map[s]

    return s.upper() if s else None


def determine_direction(debit_amount=None, credit_amount=None,
                        operation_type=None, raw_direction=None) -> Optional[str]:
    """Determine transaction direction (Приход/Расход)."""
    # 1. Explicit direction string
    if raw_direction:
        d = str(raw_direction).lower().strip()
        income_markers = ['входящ', 'приход', 'кредит', 'income', 'cr', 'вход']
        expense_markers = ['исход', 'расход', 'дебет', 'expense', 'dr', 'исх', 'выход']

        for m in income_markers:
            if m in d:
                return 'Приход'
        for m in expense_markers:
            if m in d:
                return 'Расход'

    # 2. Separate debit/credit amounts
    credit_val = normalize_amount(credit_amount)
    debit_val = normalize_amount(debit_amount)

    if credit_val and credit_val > 0 and (not debit_val or debit_val == 0):
        return 'Приход'
    if debit_val and debit_val > 0 and (not credit_val or credit_val == 0):
        return 'Расход'

    # 3. Operation type text
    if operation_type:
        op = str(operation_type).lower()
        income_ops = ['входящ', 'пополн', 'зачисление', 'возврат', 'incoming']
        expense_ops = ['исходящ', 'списан', 'выдач', 'перевод', 'outgoing', 'снятие']

        for m in income_ops:
            if m in op:
                return 'Приход'
        for m in expense_ops:
            if m in op:
                return 'Расход'

    return None


def clean_string(value) -> Optional[str]:
    """Clean a string value — strip whitespace, normalize spaces."""
    if value is None:
        return None
    s = str(value).strip()
    if not s or s.lower() == 'none':
        return None
    # Normalize multiple spaces to single
    s = re.sub(r'\s+', ' ', s)
    return s
