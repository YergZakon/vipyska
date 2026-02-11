"""
Streamlit-приложение для загрузки и обработки банковских выписок Казахстана.
Поддержка 30+ форматов от 30 банков. Автоопределение банка и формата.
"""
import streamlit as st
import pandas as pd
import tempfile
import os
import gc
import json
from pathlib import Path
from datetime import datetime
from io import BytesIO

from bank_parser.file_reader import read_excel_file
from bank_parser.detector import detect_parser
from bank_parser.models import Transaction, ParseResult

# Import all parsers so they register themselves
from bank_parser.parsers import (
    standard_18col, narodny, kaspi, otbasy, tengri,
    alatau, tsesnabank, al_hilal, kazkom,
    forte, bank_rbk, eurasian, kassa_nova, delta,
    bcc, kzi, nurbank, freedom, altyn,
    halyk_finance, citibank, bank_razvitiya,
    china_banks, zaman,
)

# --- Page config ---
st.set_page_config(
    page_title="Bank Statement Parser",
    page_icon="🏦",
    layout="wide",
)

# --- Supported banks ---
SUPPORTED_BANKS = [
    "Kaspi Bank",
    "Народный Банк (Halyk)",
    "Банк ЦентрКредит (BCC)",
    "ForteBank",
    "Freedom Bank / Finance",
    "Bank RBK",
    "Евразийский Банк",
    "Нурбанк",
    "Delta Bank",
    "Home Credit Bank",
    "Отбасы банк",
    "ВТБ Банк (Казахстан)",
    "Алтын Банк",
    "Тенгри Банк",
    "Цеснабанк",
    "Al Hilal Islamic Bank",
    "Казкоммерцбанк",
    "Halyk Finance",
    "Citibank Kazakhstan",
    "Банк Развития Казахстана",
    "Банк Китая в Казахстане",
    "ТПБ Китая в Алматы",
    "Kassa Nova",
    "КЗИ Банк",
    "Заман-Банк",
    "Shinhan Bank",
    "Alatau City",
]

RUSSIAN_HEADERS = Transaction.russian_headers()
FIELD_NAMES = Transaction.field_names()
HEADER_MAP = dict(zip(FIELD_NAMES, RUSSIAN_HEADERS))


def process_uploaded_file(uploaded_file, folder_hint: str = "") -> ParseResult:
    """Process a single uploaded file through our parser pipeline."""
    filename = uploaded_file.name
    ext = Path(filename).suffix.lower()

    result = ParseResult(filepath=filename, source_file=filename)

    if ext not in ('.xlsx', '.xls'):
        result.parse_status = 'skipped'
        result.errors.append(f'Unsupported format: {ext}')
        return result

    # Save to temp file
    with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
        tmp.write(uploaded_file.getvalue())
        tmp_path = tmp.name

    try:
        # Read
        sheets = read_excel_file(tmp_path)

        if not sheets or all(s.num_rows == 0 for s in sheets):
            result.parse_status = 'skipped'
            result.warnings.append('Empty file')
            return result

        file_info = {
            'filepath': tmp_path,
            'filename': filename,
            'extension': ext,
            'folder_name': folder_hint,
        }

        # Detect
        parser_cls = detect_parser(sheets, file_info)
        if parser_cls is None:
            result.parse_status = 'failed'
            result.errors.append('No parser detected')
            return result

        # Parse
        parser = parser_cls()
        result = parser.parse(sheets, file_info)
    except Exception as e:
        result.parse_status = 'failed'
        result.errors.append(f'Error: {e}')
    finally:
        gc.collect()
        try:
            os.unlink(tmp_path)
        except (PermissionError, OSError):
            pass

    return result


def transactions_to_df(transactions: list) -> pd.DataFrame:
    """Convert list of Transaction objects to a DataFrame with Russian headers."""
    if not transactions:
        return pd.DataFrame(columns=RUSSIAN_HEADERS)

    rows = [t.to_dict() for t in transactions]
    df = pd.DataFrame(rows)
    df.rename(columns=HEADER_MAP, inplace=True)

    # Reorder to match standard column order
    cols = [c for c in RUSSIAN_HEADERS if c in df.columns]
    df = df[cols]

    return df


# ============================================================
# UI
# ============================================================
st.title("🏦 Парсер банковских выписок")
st.markdown("Загрузите файлы выписок казахстанских банков — система автоматически определит банк и формат")

# --- Sidebar ---
with st.sidebar:
    st.header("Поддерживаемые банки")
    st.caption(f"Всего: {len(SUPPORTED_BANKS)} банков, 30+ форматов")
    for bank in SUPPORTED_BANKS:
        st.write(f"- {bank}")
    st.divider()
    st.markdown("**Форматы:** `.xlsx`, `.xls`")
    st.markdown("**Выходные данные:** 20 полей в унифицированном формате")

# --- Session state ---
if 'all_transactions' not in st.session_state:
    st.session_state.all_transactions = []
if 'processed_files' not in st.session_state:
    st.session_state.processed_files = []
if 'parse_results' not in st.session_state:
    st.session_state.parse_results = []

# --- Upload ---
st.header("📤 Загрузка файлов")

col_upload, col_hint = st.columns([3, 1])
with col_upload:
    uploaded_files = st.file_uploader(
        "Выберите файлы выписок",
        type=['xlsx', 'xls'],
        accept_multiple_files=True,
        help="Можно загрузить несколько файлов одновременно",
    )
with col_hint:
    folder_hint = st.text_input(
        "Подсказка банка (необязательно)",
        placeholder="Напр. Kaspi Bank",
        help="Если автоопределение не сработает, укажите название банка/папки",
    )

if uploaded_files:
    if st.button("🔄 Обработать файлы", type="primary", use_container_width=True):
        all_transactions = []
        processed = []
        results = []

        progress = st.progress(0, text="Подготовка...")

        for i, uf in enumerate(uploaded_files):
            progress.progress(
                (i) / len(uploaded_files),
                text=f"Обработка: {uf.name} ({i+1}/{len(uploaded_files)})",
            )

            result = process_uploaded_file(uf, folder_hint)
            results.append(result)
            all_transactions.extend(result.transactions)

            status_icon = {
                'success': '✅', 'partial': '⚠️',
                'failed': '❌', 'skipped': '⏭️',
            }.get(result.parse_status, '❓')

            processed.append({
                'Файл': uf.name,
                'Банк': result.bank_detected or '—',
                'Парсер': result.parser_used or '—',
                'Транзакций': result.total_transactions,
                'Статус': f"{status_icon} {result.parse_status}",
                'Ошибки': '; '.join(result.errors) if result.errors else '',
            })

        progress.progress(1.0, text="Готово!")

        st.session_state.all_transactions = all_transactions
        st.session_state.processed_files = processed
        st.session_state.parse_results = results

        success_count = sum(1 for r in results if r.parse_status in ('success', 'partial'))
        st.success(
            f"Обработано: {success_count}/{len(uploaded_files)} файлов, "
            f"извлечено {len(all_transactions)} транзакций"
        )

# --- Results table ---
if st.session_state.processed_files:
    st.header("📊 Результаты обработки")
    df_results = pd.DataFrame(st.session_state.processed_files)
    st.dataframe(df_results, use_container_width=True, hide_index=True)

# --- Stats ---
if st.session_state.all_transactions:
    st.header("📈 Статистика")

    df = transactions_to_df(st.session_state.all_transactions)

    # Metrics row
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Всего транзакций", f"{len(df):,}")
    with c2:
        if 'Сумма' in df.columns:
            income = pd.to_numeric(
                df.loc[df.get('Направление', pd.Series()) == 'Приход', 'Сумма'],
                errors='coerce'
            ).sum()
            st.metric("Приход", f"{income:,.0f} ₸")
    with c3:
        if 'Сумма' in df.columns:
            expense = pd.to_numeric(
                df.loc[df.get('Направление', pd.Series()) == 'Расход', 'Сумма'],
                errors='coerce'
            ).sum()
            st.metric("Расход", f"{expense:,.0f} ₸")
    with c4:
        banks_count = df['Банк выписки'].nunique() if 'Банк выписки' in df.columns else 0
        st.metric("Банков", banks_count)

    # Stats by bank
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("По банкам")
        if 'Банк выписки' in df.columns and 'Сумма' in df.columns:
            df['_amount'] = pd.to_numeric(df['Сумма'], errors='coerce')
            bank_stats = df.groupby('Банк выписки').agg(
                Транзакций=('_amount', 'count'),
                Сумма=('_amount', 'sum'),
            ).sort_values('Транзакций', ascending=False).reset_index()
            bank_stats.columns = ['Банк', 'Транзакций', 'Общая сумма']
            bank_stats['Общая сумма'] = bank_stats['Общая сумма'].apply(lambda x: f"{x:,.0f}")
            st.dataframe(bank_stats, use_container_width=True, hide_index=True)
            df.drop(columns=['_amount'], inplace=True, errors='ignore')

    with col_right:
        st.subheader("По направлениям")
        if 'Направление' in df.columns and 'Сумма' in df.columns:
            df['_amount'] = pd.to_numeric(df['Сумма'], errors='coerce')
            dir_stats = df.groupby('Направление').agg(
                Транзакций=('_amount', 'count'),
                Сумма=('_amount', 'sum'),
            ).reset_index()
            dir_stats.columns = ['Направление', 'Транзакций', 'Сумма']
            dir_stats['Сумма'] = dir_stats['Сумма'].apply(lambda x: f"{x:,.0f}")
            st.dataframe(dir_stats, use_container_width=True, hide_index=True)
            df.drop(columns=['_amount'], inplace=True, errors='ignore')

    # Date range
    if 'Дата операции' in df.columns:
        dates = pd.to_datetime(df['Дата операции'], errors='coerce').dropna()
        if not dates.empty:
            st.subheader("Период данных")
            st.write(
                f"С **{dates.min().strftime('%d.%m.%Y')}** "
                f"по **{dates.max().strftime('%d.%m.%Y')}**"
            )

# --- Data preview ---
if st.session_state.all_transactions:
    st.header("👁️ Предпросмотр данных")

    df = transactions_to_df(st.session_state.all_transactions)

    # Filters
    col_f1, col_f2, col_f3 = st.columns(3)

    with col_f1:
        if 'Банк выписки' in df.columns:
            banks_list = ['Все'] + sorted(df['Банк выписки'].dropna().unique().tolist())
            selected_bank = st.selectbox("Банк", banks_list)
            if selected_bank != 'Все':
                df = df[df['Банк выписки'] == selected_bank]

    with col_f2:
        if 'Направление' in df.columns:
            dirs_list = ['Все'] + sorted(df['Направление'].dropna().unique().tolist())
            selected_dir = st.selectbox("Направление", dirs_list)
            if selected_dir != 'Все':
                df = df[df['Направление'] == selected_dir]

    with col_f3:
        num_rows = st.selectbox("Строк", [25, 50, 100, 500, "Все"], index=0)

    if num_rows == "Все":
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.dataframe(df.head(num_rows), use_container_width=True, hide_index=True)

    st.caption(
        f"Показано {min(num_rows if num_rows != 'Все' else len(df), len(df))} "
        f"из {len(df)} записей"
    )

# --- Export ---
if st.session_state.all_transactions:
    st.header("💾 Экспорт данных")

    df_export = transactions_to_df(st.session_state.all_transactions)

    col_e1, col_e2, col_e3 = st.columns(3)

    with col_e1:
        # Excel
        output_xlsx = BytesIO()
        with pd.ExcelWriter(output_xlsx, engine='openpyxl') as writer:
            df_export.to_excel(writer, sheet_name='Транзакции', index=False)
        st.download_button(
            "📥 Скачать Excel",
            data=output_xlsx.getvalue(),
            file_name=f"transactions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

    with col_e2:
        # CSV
        csv_data = df_export.to_csv(index=False, encoding='utf-8-sig')
        st.download_button(
            "📥 Скачать CSV",
            data=csv_data,
            file_name=f"transactions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            use_container_width=True,
        )

    with col_e3:
        # JSON
        json_data = json.dumps(
            [t.to_dict() for t in st.session_state.all_transactions],
            ensure_ascii=False, indent=2,
        )
        st.download_button(
            "📥 Скачать JSON",
            data=json_data,
            file_name=f"transactions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            mime="application/json",
            use_container_width=True,
        )

# --- Clear ---
if st.session_state.all_transactions:
    st.divider()
    if st.button("🗑️ Очистить данные"):
        st.session_state.all_transactions = []
        st.session_state.processed_files = []
        st.session_state.parse_results = []
        st.rerun()

# --- Footer ---
st.divider()
st.caption(
    f"Bank Statement Parser v2.0 | "
    f"{len(SUPPORTED_BANKS)} банков | 30+ форматов | "
    f"80/87 файлов (92% покрытие)"
)
