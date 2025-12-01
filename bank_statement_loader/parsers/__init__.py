from .base_parser import BaseParser
from .halyk_parser import HalykParser
from .kaspi_parser import KaspiParser
from .kaspi_stats_parser import KaspiStatsParser
from .centercredit_parser import CenterCreditParser
from .freedom_parser import FreedomParser
from .forte_parser import ForteParser
from .rbk_parser import RBKParser
from .eurasian_parser import EurasianParser
from .nurbank_parser import NurbankParser
from .delta_parser import DeltaParser
from .homecredit_parser import HomeCreditParser
from .otbasy_parser import OtbasyParser
from .vtb_parser import VTBParser
from .tengri_parser import TengriParser
from .kassanova_parser import KassaNovaParser
from .altyn_parser import AltynParser
from .alatau_parser import AlatauParser
from .boc_parser import BOCParser

__all__ = [
    'BaseParser',
    'HalykParser',
    'KaspiParser',
    'KaspiStatsParser',
    'CenterCreditParser',
    'FreedomParser',
    'ForteParser',
    'RBKParser',
    'EurasianParser',
    'NurbankParser',
    'DeltaParser',
    'HomeCreditParser',
    'OtbasyParser',
    'VTBParser',
    'TengriParser',
    'KassaNovaParser',
    'AltynParser',
    'AlatauParser',
    'BOCParser',
]

# Список всех доступных парсеров
# Порядок важен - более специфичные парсеры должны быть раньше
PARSERS = [
    # Банки с уникальными форматами (высокая специфичность)
    RBKParser,           # Английские заголовки POSTING_DATE
    DeltaParser,         # Листы "Входящие платеж в тенге"
    NurbankParser,       # "Операции, проведенные в АБИС"
    ForteParser,         # "Инфорация по переводам"
    OtbasyParser,        # "Жилстройсбербанк", HCSKKZKA
    VTBParser,           # "Вид операции (КД)", формат даты YYYY.MM.DD
    KaspiStatsParser,    # "Статистика по успешным операциям" (до обычного Kaspi!)
    TengriParser,        # "Tengri Bank" в заголовке
    KassaNovaParser,     # "Входящие платежи" с "бенефициара/отправителя"
    AlatauParser,        # "Alatau City Bank", лист Statement
    BOCParser,           # "Банк Китая", BKCHKZKA
    AltynParser,         # "Altyn Bank", ATYNKZKA

    # Банки со стандартным форматом (средняя специфичность)
    HomeCreditParser,    # Код 886 в номере счёта
    KaspiParser,         # Название листа KAS_*
    HalykParser,         # HSBKKZKX
    EurasianParser,      # "Тип операции", "Детали операции"
    CenterCreditParser,  # Листы "Входящие операции"
    FreedomParser,       # Листы KZ...KZT/USD (проверяется последним)
]
