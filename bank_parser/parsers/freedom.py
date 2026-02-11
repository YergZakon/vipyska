"""Parser for Freedom Bank / Freedom Finance.

These use standard 17-column format (same as Shinhan but without Назначение платежа column)
in .xls files. The standard_18col parser handles them via the shared detection logic.

This parser exists as a fallback specifically for Freedom files that might not be caught.
"""

from . import register_parser
from .standard_18col import Standard18ColParser


# Freedom Bank/Finance use the same 17-18 col format detected by Standard18ColParser.
# No separate parser needed — they are handled by Standard18ColParser
# which detects via header signature and sets bank name from folder_name.
