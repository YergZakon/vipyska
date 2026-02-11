"""Parser for АО Банк Kassa Nova.

Format: 5 columns with section markers "Входящие платежи" / "Исходящие платежи".
Columns: Дата операции | Наименование бенефициара/отправителя | БИН/ИИН | Сумма | Назначение платежа
"""

from typing import List, Tuple, Optional

from ..base_parser import BaseParser
from ..models import Transaction
from ..file_reader import SheetData
from ..normalizer import (
    normalize_date, normalize_iin_bin, normalize_amount, clean_string
)
from . import register_parser


@register_parser
class KassaNovaParser(BaseParser):
    BANK_NAME = 'АО Банк Kassa Nova'

    @classmethod
    def can_parse(cls, sheet: SheetData, file_info: dict) -> float:
        found_section_marker = False
        found_beneficiary = False
        found_company_header = False  # Delta Bank pattern

        for row in sheet.rows[:15]:
            for cell in row:
                if cell:
                    cl = str(cell).lower().strip()
                    if cl in ('входящие платежи', 'исходящие платежи'):
                        found_section_marker = True
                    elif 'поступления на текущий счет' in cl:
                        found_section_marker = True
            row_text = ' '.join(str(c).lower() for c in row if c)
            if 'бенефициар' in row_text:
                found_beneficiary = True
            # Delta Bank uses "Наименование компании" — never Kassa Nova
            if 'наименование компании' in row_text:
                found_company_header = True

        # If "Наименование компании" found — this is Delta Bank, not Kassa Nova
        if found_company_header:
            return 0.0

        folder = file_info.get('folder_name', '').lower()

        if found_section_marker and found_beneficiary:
            return 0.95  # Unique combo
        if found_section_marker:
            if 'kassa nova' in folder:
                return 0.95
            if sheet.num_cols <= 6:
                return 0.85  # 5-col format typical of Kassa Nova
            return 0.6
        if found_beneficiary:
            return 0.85  # бенефициара/отправителя is unique to Kassa Nova
        if 'kassa nova' in folder:
            return 0.7
        return 0.0

    def parse_sheet(self, sheet: SheetData, file_info: dict) -> Tuple[List[Transaction], dict]:
        rows = sheet.rows
        transactions = []
        current_direction = None

        for row_idx, row in enumerate(rows):
            if not row or all(c is None for c in row):
                continue

            # Detect section markers
            for cell in row:
                if cell:
                    s = str(cell).lower()
                    if 'входящие' in s:
                        current_direction = 'Приход'
                    elif 'исходящие' in s:
                        current_direction = 'Расход'

            # Skip header-like rows
            first_cell = row[0] if row else None
            if first_cell and isinstance(first_cell, str):
                fl = first_cell.lower()
                if 'дата' in fl or 'входящ' in fl or 'исходящ' in fl or fl.strip() == '':
                    continue

            # Try to parse as data row
            if len(row) >= 4:
                date_val = row[0]
                if date_val is None:
                    continue

                # Check if it looks like a date
                date_str = normalize_date(date_val)
                if date_str is None or (isinstance(date_val, str) and not any(c.isdigit() for c in date_val)):
                    continue

                name = clean_string(row[1] if len(row) > 1 else None)
                iin = normalize_iin_bin(row[2] if len(row) > 2 else None)
                amount = normalize_amount(row[3] if len(row) > 3 else None)
                purpose = clean_string(row[4] if len(row) > 4 else None)

                if amount is None:
                    continue

                t = Transaction(
                    transaction_date=date_str,
                    amount=amount,
                    currency='KZT',
                    amount_tenge=amount,
                    direction=current_direction,
                    payer=name if current_direction == 'Приход' else None,
                    payer_iin_bin=iin if current_direction == 'Приход' else None,
                    payer_bank=None, payer_account=None,
                    recipient=name if current_direction == 'Расход' else None,
                    recipient_iin_bin=iin if current_direction == 'Расход' else None,
                    recipient_bank=None, recipient_account=None,
                    operation_type=None, knp=None,
                    payment_purpose=purpose,
                    document_number=None,
                    statement_bank=self.BANK_NAME,
                    account_number=None,
                    source_file=file_info['filename'],
                )
                transactions.append(t)

        return transactions, {'account_number': None, 'warnings': [], 'errors': []}
