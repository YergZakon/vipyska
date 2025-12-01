"""
Парсер выписок Банка ЦентрКредит
Два формата:
1. XLSX - структура с листами "Входящие операции", "Снятие наличных по счету" и др.
2. XLS - формат с метаданными в шапке и таблицей дебет/кредит
"""
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import List, Tuple, Union, Optional
import re
import pandas as pd

from .base_parser import BaseParser
from ..models import UnifiedTransaction, StatementMetadata


class CenterCreditParser(BaseParser):
    """Парсер выписок Банка ЦентрКредит"""

    BANK_NAME = "АО Банк ЦентрКредит"
    BANK_ALIASES = ['центркредит', 'centercredit', 'bcc', 'банк центркредит']

    def __init__(self, file_path: Union[str, Path]):
        super().__init__(file_path)

    @classmethod
    def can_parse(cls, file_path: Union[str, Path]) -> bool:
        """Проверка, подходит ли файл для парсера ЦентрКредит"""
        try:
            path = Path(file_path)
            if path.suffix.lower() not in ('.xlsx', '.xls'):
                return False

            xl = pd.ExcelFile(file_path)

            # Проверяем характерные листы для xlsx формата
            sheet_names_lower = [s.lower() for s in xl.sheet_names]
            if any('входящие' in s for s in sheet_names_lower) or any('снятие' in s for s in sheet_names_lower):
                return True

            # Проверяем содержимое для xls формата
            df = pd.read_excel(file_path, header=None, nrows=20)
            for idx in range(min(15, len(df))):
                row = df.iloc[idx]
                row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
                if any(alias in row_text for alias in cls.BANK_ALIASES):
                    return True
                if 'выписка по лицевому счету' in row_text:
                    return True

            return False
        except Exception:
            return False

    def _is_xlsx_format(self) -> bool:
        """Определяем формат файла по структуре листов"""
        xl = pd.ExcelFile(self.file_path)
        sheet_names_lower = [s.lower() for s in xl.sheet_names]
        return any('входящие' in s or 'исходящие' in s or 'снятие' in s for s in sheet_names_lower)

    def _parse_xlsx_format(self) -> Tuple[StatementMetadata, List[UnifiedTransaction]]:
        """Парсинг XLSX формата с несколькими листами"""
        metadata = StatementMetadata()
        metadata.bank_name = self.BANK_NAME
        metadata.source_file = self.file_path.name

        xl = pd.ExcelFile(self.file_path)
        transactions = []

        for sheet_name in xl.sheet_names:
            df = pd.read_excel(self.file_path, sheet_name=sheet_name, header=None)

            if len(df) < 2:
                continue

            # Извлекаем метаданные из первой строки
            first_row = ' '.join(str(v) for v in df.iloc[0] if pd.notna(v))
            if 'клиента' in first_row.lower():
                # Извлекаем имя клиента из заголовка
                match = re.search(r'клиента\s+([^"]+)', first_row, re.IGNORECASE)
                if match:
                    metadata.client_name = match.group(1).strip().strip('"')

            # Определяем направление операций по названию листа
            sheet_lower = sheet_name.lower()
            if 'входящ' in sheet_lower:
                direction = 'income'
            elif 'исходящ' in sheet_lower or 'снятие' in sheet_lower:
                direction = 'expense'
            else:
                direction = ''

            # Находим строку с заголовками
            header_row = None
            for idx in range(min(5, len(df))):
                row = df.iloc[idx]
                row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
                if 'дата' in row_text and ('сумма' in row_text or 'операц' in row_text):
                    header_row = idx
                    break

            if header_row is None:
                continue

            # Читаем данные с заголовками
            df_data = pd.read_excel(self.file_path, sheet_name=sheet_name, header=header_row)

            # Маппинг колонок
            col_map = {}
            for col in df_data.columns:
                col_lower = str(col).lower()
                if 'дата' in col_lower and 'операц' in col_lower:
                    col_map['date'] = col
                elif col_lower == 'дата операции':
                    col_map['date'] = col
                elif 'сумма' in col_lower:
                    if 'операц' in col_lower or col_map.get('amount') is None:
                        col_map['amount'] = col
                elif 'наименование' in col_lower and 'дебет' in col_lower:
                    col_map['payer_name'] = col
                elif 'наименование' in col_lower and 'кредит' in col_lower:
                    col_map['recipient_name'] = col
                elif 'бин' in col_lower or 'иин' in col_lower:
                    col_map['bin_iin'] = col
                elif 'основание' in col_lower or 'назначение' in col_lower:
                    col_map['description'] = col
                elif 'подразделение' in col_lower:
                    col_map['branch'] = col

            # Парсим транзакции
            for idx, row in df_data.iterrows():
                try:
                    date_val = row.get(col_map.get('date'))
                    if pd.isna(date_val):
                        continue

                    date = self._parse_date(date_val)
                    if date is None:
                        continue

                    amount = self._parse_decimal(row.get(col_map.get('amount')))
                    if amount is None:
                        continue

                    transaction = UnifiedTransaction(
                        date=date,
                        amount=abs(amount),
                        currency='KZT',
                        amount_kzt=abs(amount),
                        direction=direction,
                        payer_name=self._clean_string(row.get(col_map.get('payer_name'), '')),
                        payer_bin_iin=self._extract_bin_iin(row.get(col_map.get('bin_iin'), '')),
                        recipient_name=self._clean_string(row.get(col_map.get('recipient_name'), '')),
                        description=self._clean_string(row.get(col_map.get('description'), '')),
                        operation_type=sheet_name,
                        source_bank=self.BANK_NAME,
                        source_file=self.file_path.name,
                        account_number=metadata.account_number,
                    )

                    transactions.append(transaction)

                except Exception as e:
                    print(f"Ошибка парсинга строки {idx} в листе {sheet_name}: {e}")
                    continue

        return metadata, transactions

    def _parse_xls_format(self) -> Tuple[StatementMetadata, List[UnifiedTransaction]]:
        """Парсинг XLS формата с дебет/кредит структурой"""
        metadata = StatementMetadata()
        metadata.bank_name = self.BANK_NAME
        metadata.source_file = self.file_path.name

        df = pd.read_excel(self.file_path, header=None)

        # Извлекаем метаданные из шапки
        for idx in range(min(20, len(df))):
            row = df.iloc[idx]
            row_text = ' '.join(str(v) for v in row if pd.notna(v))

            # Номер счёта
            if 'выписка по лицевому счету' in row_text.lower():
                match = re.search(r'KZ\w{18,20}', row_text)
                if match:
                    metadata.account_number = match.group()

            # Период
            if 'период' in row_text.lower() or 'за период' in row_text.lower():
                dates = re.findall(r'\d{2}\.\d{2}\.\d{4}', row_text)
                if len(dates) >= 2:
                    metadata.period_start = self._parse_date(dates[0])
                    metadata.period_end = self._parse_date(dates[1])

            # Клиент
            if 'клиент' in row_text.lower():
                # Имя обычно после двоеточия
                for v in row:
                    if pd.notna(v) and ':' not in str(v) and len(str(v)) > 3:
                        name = str(v).strip()
                        if not name.startswith('KZ') and not re.match(r'^\d+$', name):
                            metadata.client_name = name
                            break

            # ИИН/БИН
            if 'сн/ин' in row_text.lower() or 'иин' in row_text.lower():
                match = re.search(r'\d{12}', row_text)
                if match:
                    metadata.client_bin_iin = match.group()

            # Балансы
            if 'входящий остаток' in row_text.lower() or 'кіріс қалдық' in row_text.lower():
                match = re.search(r'([\d\s,.-]+)', row_text.split(':')[-1] if ':' in row_text else row_text)
                if match:
                    metadata.opening_balance = self._parse_decimal(match.group(1))

            if 'исходящий остаток' in row_text.lower() or 'шығыс қалдық' in row_text.lower():
                match = re.search(r'([\d\s,.-]+)', row_text.split(':')[-1] if ':' in row_text else row_text)
                if match:
                    metadata.closing_balance = self._parse_decimal(match.group(1))

        # Ищем строку с заголовками таблицы
        header_row = None
        for idx in range(min(25, len(df))):
            row = df.iloc[idx]
            row_text = ' '.join(str(v).lower() for v in row if pd.notna(v))
            if ('дата' in row_text or 'күні' in row_text) and ('дебет' in row_text or 'кредит' in row_text):
                header_row = idx
                break

        if header_row is None:
            return metadata, []

        # Читаем с заголовками
        df_data = pd.read_excel(self.file_path, header=header_row)

        # Маппинг колонок
        col_map = {}
        for col in df_data.columns:
            col_lower = str(col).lower()
            if 'дата' in col_lower or 'күні' in col_lower:
                if 'открыт' not in col_lower and 'закрыт' not in col_lower:
                    col_map['date'] = col
            elif 'дебет' in col_lower:
                col_map['debit'] = col
            elif 'кредит' in col_lower:
                col_map['credit'] = col
            elif 'валюта' in col_lower:
                col_map['currency'] = col
            elif 'контрагент' in col_lower or 'наименование контрагента' in col_lower:
                col_map['counterparty'] = col
            elif 'иин' in col_lower or 'бин' in col_lower:
                col_map['bin_iin'] = col
            elif 'назначение' in col_lower or 'төлем' in col_lower:
                col_map['description'] = col
            elif 'документ' in col_lower or '№' in col_lower:
                col_map['doc_number'] = col
            elif 'кнп' in col_lower or 'тмк' in col_lower:
                col_map['knp'] = col
            elif 'банк' in col_lower and 'корресп' in col_lower:
                col_map['bank'] = col
            elif 'счет' in col_lower and col_map.get('account') is None:
                col_map['account'] = col

        # Парсим транзакции
        transactions = []
        for idx, row in df_data.iterrows():
            try:
                date_val = row.get(col_map.get('date'))
                if pd.isna(date_val):
                    continue

                date = self._parse_date(date_val)
                if date is None:
                    continue

                debit = self._parse_decimal(row.get(col_map.get('debit')))
                credit = self._parse_decimal(row.get(col_map.get('credit')))

                # Определяем сумму и направление
                if credit and credit > 0:
                    amount = credit
                    direction = 'income'
                elif debit and debit > 0:
                    amount = debit
                    direction = 'expense'
                else:
                    continue

                currency = self._clean_string(row.get(col_map.get('currency'), 'KZT'))
                if not currency:
                    currency = 'KZT'

                transaction = UnifiedTransaction(
                    date=date,
                    amount=amount,
                    currency=currency,
                    amount_kzt=amount if currency == 'KZT' else None,
                    direction=direction,
                    payer_name=self._clean_string(row.get(col_map.get('counterparty'), '')) if direction == 'income' else metadata.client_name,
                    payer_bin_iin=self._extract_bin_iin(row.get(col_map.get('bin_iin'), '')) if direction == 'income' else metadata.client_bin_iin,
                    recipient_name=metadata.client_name if direction == 'income' else self._clean_string(row.get(col_map.get('counterparty'), '')),
                    recipient_bin_iin=metadata.client_bin_iin if direction == 'income' else self._extract_bin_iin(row.get(col_map.get('bin_iin'), '')),
                    recipient_bank=self._clean_string(row.get(col_map.get('bank'), '')),
                    recipient_account=self._clean_string(row.get(col_map.get('account'), '')),
                    description=self._clean_string(row.get(col_map.get('description'), '')),
                    document_number=self._clean_string(row.get(col_map.get('doc_number'), '')),
                    knp_code=self._clean_string(row.get(col_map.get('knp'), '')),
                    source_bank=self.BANK_NAME,
                    source_file=self.file_path.name,
                    account_number=metadata.account_number,
                )

                transactions.append(transaction)

            except Exception as e:
                print(f"Ошибка парсинга строки {idx}: {e}")
                continue

        return metadata, transactions

    def parse(self) -> Tuple[StatementMetadata, List[UnifiedTransaction]]:
        """Парсинг файла выписки ЦентрКредит"""
        if self._is_xlsx_format():
            self.metadata, self.transactions = self._parse_xlsx_format()
        else:
            self.metadata, self.transactions = self._parse_xls_format()

        return self.metadata, self.transactions
