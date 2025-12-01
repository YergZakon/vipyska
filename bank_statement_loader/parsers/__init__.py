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
from .halykfinance_parser import HalykFinanceParser
from .zaman_parser import ZamanParser
from .citibank_parser import CitibankParser
from .razvitiya_parser import RazvitiyaParser
from .cesna_parser import CesnaParser
from .kazkom_parser import KazkomParser
from .alhilal_parser import AlHilalParser
from .kzi_parser import KZIParser
from .simple_parser import SimpleParser

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
    'HalykFinanceParser',
    'ZamanParser',
    'CitibankParser',
    'RazvitiyaParser',
    'CesnaParser',
    'KazkomParser',
    'AlHilalParser',
    'KZIParser',
    'SimpleParser',
]

# Список всех доступных парсеров
# Порядок важен - более специфичные парсеры должны быть раньше
PARSERS = [
    # Банки с уникальными форматами (высокая специфичность)
    HalykFinanceParser,  # Halyk Finance - ценные бумаги
    ZamanParser,         # Заман-Банк - ZAJSKZ22
    CitibankParser,      # Ситибанк справки
    RazvitiyaParser,     # Банк Развития - DVKAKZKA
    CesnaParser,         # Цеснабанк - TSESKZKA
    KazkomParser,        # Казкоммерцбанк - старый формат
    AlHilalParser,       # Al Hilal - HLALKZKZ
    KZIParser,           # КЗИ Банк

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

    # Универсальный парсер (используется как последний вариант)
    SimpleParser,        # Простые табличные форматы
]
