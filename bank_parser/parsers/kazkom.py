"""Parser for АО Казкоммерцбанк.

Format: Text-block format in 3-4 columns. Very non-standard.
Row 0: "ВЫПИСКА ПО СЧЕТУ"
Row 1: "Дата печати: ... Период: ..."
Rows 2-10: metadata (bank details, client info)
Then: 3-column layout (Дата | Дебет | Кредит) with multi-line transaction blocks.
Transactions separated by dashes.
"""

import re
from typing import List, Tuple, Optional

from ..base_parser import BaseParser
from ..models import Transaction
from ..file_reader import SheetData
from ..normalizer import (
    normalize_date, normalize_iin_bin, normalize_amount, clean_string
)
from . import register_parser


@register_parser
class KazkomParser(BaseParser):
    BANK_NAME = 'АО Казкоммерцбанк'

    @classmethod
    def can_parse(cls, sheet: SheetData, file_info: dict) -> float:
        found_kazkom_id = False
        found_statement_title = False
        found_dot_pattern = False

        for i, row in enumerate(sheet.rows[:15]):
            for cell in row:
                if cell:
                    cs = str(cell)
                    cl = cs.lower()
                    # SWIFT code is definitive anywhere
                    if 'KZKOKZKX' in cs:
                        found_kazkom_id = True
                    # Bank name only in metadata rows (first 10), not in data
                    if i < 10 and ('казкоммерцбанк' in cl and 'облигации' not in cl):
                        found_kazkom_id = True
                    if 'дата постирования' in cl:
                        return 0.95  # Unique misspelling in Kazkom card format
                    if 'выписка по счету' in cl:
                        found_statement_title = True
                    if '. . . :' in cs:
                        found_dot_pattern = True

        folder = file_info.get('folder_name', '').lower()

        if found_kazkom_id and found_statement_title:
            return 0.95
        if found_kazkom_id:
            return 0.90
        if found_statement_title:
            if found_dot_pattern:
                return 0.85  # Unique Kazkom text-block format
            if 'казкоммерц' in folder:
                return 0.95
            return 0.6
        if 'казкоммерц' in folder:
            return 0.7
        return 0.0

    def parse(self, sheets, file_info):
        """Override to handle multiple sheets."""
        from ..models import ParseResult
        result = ParseResult(
            filepath=file_info['filepath'],
            source_file=file_info['filename'],
            bank_detected=self.BANK_NAME,
            parser_used=self.__class__.__name__,
        )

        all_transactions = []
        for sheet in sheets:
            txns, meta = self.parse_sheet(sheet, file_info)
            all_transactions.extend(txns)
            if meta.get('account_number'):
                result.account_number = meta['account_number']

        result.transactions = all_transactions
        result.total_transactions = len(all_transactions)
        result.parse_status = 'success' if all_transactions else 'failed'
        return result

    def parse_sheet(self, sheet: SheetData, file_info: dict) -> Tuple[List[Transaction], dict]:
        rows = sheet.rows
        transactions = []
        account_number = None

        # Determine direction from sheet name
        direction = None
        sn = sheet.name.lower()
        if 'входящ' in sn or 'кредит' in sn:
            direction = 'Приход'
        elif 'исходящ' in sn or 'дебет' in sn or 'обнал' in sn:
            direction = 'Расход'

        # Extract metadata
        for row in rows[:15]:
            for cell in row:
                if cell:
                    s = str(cell)
                    match = re.search(r'(KZ\w{16,22})', s)
                    if match:
                        account_number = match.group(1)

        # Find header row (Дата | Дебет | Кредит or similar)
        header_idx = None
        for i, row in enumerate(rows[:20]):
            row_text = ' '.join(str(c).lower() for c in row if c)
            if 'дата' in row_text and ('дебет' in row_text or 'кредит' in row_text or 'сумма' in row_text):
                header_idx = i
                break

        if header_idx is None:
            # Try parsing without header — some sheets are just text blocks
            return self._parse_text_blocks(rows, direction, account_number, file_info)

        header = rows[header_idx]
        header_lower = [str(c).lower().strip() if c else '' for c in header]

        col_map = {}
        for i, h in enumerate(header_lower):
            if 'дата' in h:
                col_map['date'] = i
            elif 'дебет' in h:
                col_map['debit'] = i
            elif 'кредит' in h:
                col_map['credit'] = i

        current_date = None
        current_purpose = []

        for row_idx in range(header_idx + 1, len(rows)):
            row = rows[row_idx]
            if not row or all(c is None for c in row):
                continue

            # Check for separator (dashes)
            first_cell = str(row[0] or '')
            if '---' in first_cell:
                continue

            date_val = self._get(row, col_map.get('date'))
            debit = normalize_amount(self._get(row, col_map.get('debit')))
            credit = normalize_amount(self._get(row, col_map.get('credit')))

            if date_val and (debit or credit):
                # This is a transaction row
                from ..normalizer import determine_direction
                d = determine_direction(debit_amount=debit, credit_amount=credit) or direction
                amount = credit or debit

                # Collect purpose from subsequent rows
                purpose_parts = []
                for next_idx in range(row_idx + 1, min(row_idx + 5, len(rows))):
                    nr = rows[next_idx]
                    if not nr:
                        break
                    first = str(nr[0] or '').strip()
                    if first and '---' not in first and not normalize_date(first):
                        purpose_parts.append(first)
                    else:
                        break

                t = Transaction(
                    transaction_date=normalize_date(date_val),
                    amount=amount,
                    currency='KZT',
                    amount_tenge=amount,
                    direction=d,
                    payer=None, payer_iin_bin=None, payer_bank=None, payer_account=None,
                    recipient=None, recipient_iin_bin=None, recipient_bank=None, recipient_account=None,
                    operation_type=None, knp=None,
                    payment_purpose=clean_string(' '.join(purpose_parts)) if purpose_parts else None,
                    document_number=None,
                    statement_bank=self.BANK_NAME,
                    account_number=account_number,
                    source_file=file_info['filename'],
                )
                transactions.append(t)

        return transactions, {'account_number': account_number, 'warnings': [], 'errors': []}

    def _parse_text_blocks(self, rows, direction, account_number, file_info):
        """Parse unstructured text blocks (like the обнал sheet)."""
        transactions = []
        # Simple approach: look for date patterns and amounts
        for row in rows:
            if not row:
                continue
            text = ' '.join(str(c) for c in row if c)
            # Look for date + amount pattern
            date_match = re.search(r'(\d{2}\.\d{2}\.\d{2,4})', text)
            amount_match = re.search(r'(\d[\d\s]*\d)\s*(тг|тенге)', text.lower())
            if date_match and amount_match:
                amt_str = amount_match.group(1).replace(' ', '')
                t = Transaction(
                    transaction_date=normalize_date(date_match.group(1)),
                    amount=normalize_amount(amt_str),
                    currency='KZT',
                    amount_tenge=normalize_amount(amt_str),
                    direction=direction,
                    payer=None, payer_iin_bin=None, payer_bank=None, payer_account=None,
                    recipient=None, recipient_iin_bin=None, recipient_bank=None, recipient_account=None,
                    operation_type=None, knp=None,
                    payment_purpose=clean_string(text),
                    document_number=None,
                    statement_bank=self.BANK_NAME,
                    account_number=account_number,
                    source_file=file_info['filename'],
                )
                transactions.append(t)

        return transactions, {'account_number': account_number, 'warnings': ['Unstructured text format'], 'errors': []}

    @staticmethod
    def _get(row, idx):
        if idx is None or idx >= len(row):
            return None
        return row[idx]
