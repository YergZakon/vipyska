"""
Streamlit приложение для загрузки и обработки банковских выписок
"""
import streamlit as st
import pandas as pd
import tempfile
import os
import gc
from pathlib import Path
from datetime import datetime

from bank_statement_loader import BankStatementLoader, UnifiedTransaction, StatementMetadata
from bank_statement_loader.parsers import PARSERS

# Настройка страницы
st.set_page_config(
    page_title="Загрузчик банковских выписок",
    page_icon="🏦",
    layout="wide"
)

# Заголовок
st.title("🏦 Загрузчик банковских выписок")
st.markdown("Загрузите файлы выписок казахстанских банков для обработки в унифицированном формате")

# Список поддерживаемых банков
supported_banks = [
    "Народный Банк (Halyk)",
    "Kaspi Bank",
    "Банк ЦентрКредит",
    "Фридом Банк",
    "ForteBank",
    "Bank RBK",
    "Евразийский Банк",
    "Нурбанк",
    "Delta Bank",
    "Home Credit Bank",
    "Отбасы банк",
    "ВТБ Банк"
]

# Боковая панель
with st.sidebar:
    st.header("📋 Поддерживаемые банки")
    for bank in supported_banks:
        st.write(f"• {bank}")

    st.divider()
    st.markdown("**Форматы файлов:** `.xlsx`, `.xls`")

# Инициализация состояния
if 'transactions' not in st.session_state:
    st.session_state.transactions = []
if 'metadata_list' not in st.session_state:
    st.session_state.metadata_list = []
if 'processed_files' not in st.session_state:
    st.session_state.processed_files = []

# Загрузка файлов
st.header("📤 Загрузка файлов")

uploaded_files = st.file_uploader(
    "Выберите файлы выписок",
    type=['xlsx', 'xls'],
    accept_multiple_files=True,
    help="Можно загрузить один или несколько файлов"
)

if uploaded_files:
    if st.button("🔄 Обработать файлы", type="primary"):
        loader = BankStatementLoader()

        all_transactions = []
        all_metadata = []
        processed = []
        errors = []

        progress_bar = st.progress(0)
        status_text = st.empty()

        for i, uploaded_file in enumerate(uploaded_files):
            status_text.text(f"Обработка: {uploaded_file.name}")

            # Сохраняем временный файл
            with tempfile.NamedTemporaryFile(delete=False, suffix=Path(uploaded_file.name).suffix) as tmp:
                tmp.write(uploaded_file.getvalue())
                tmp_path = tmp.name

            try:
                # Определяем банк
                parser_class = loader.detect_bank(tmp_path)

                if parser_class:
                    # Парсим файл
                    metadata, transactions = loader.load(tmp_path)

                    all_transactions.extend(transactions)
                    all_metadata.append(metadata)
                    processed.append({
                        'file': uploaded_file.name,
                        'bank': metadata.bank_name,
                        'transactions': len(transactions),
                        'status': '✅ Успешно'
                    })
                else:
                    errors.append({
                        'file': uploaded_file.name,
                        'error': 'Не удалось определить банк'
                    })
                    processed.append({
                        'file': uploaded_file.name,
                        'bank': 'Не определён',
                        'transactions': 0,
                        'status': '❌ Ошибка'
                    })
            except Exception as e:
                errors.append({
                    'file': uploaded_file.name,
                    'error': str(e)
                })
                processed.append({
                    'file': uploaded_file.name,
                    'bank': 'Ошибка',
                    'transactions': 0,
                    'status': '❌ Ошибка'
                })
            finally:
                # Закрываем все ресурсы и удаляем временный файл
                gc.collect()
                try:
                    os.unlink(tmp_path)
                except PermissionError:
                    pass  # Файл будет удалён позже системой

            progress_bar.progress((i + 1) / len(uploaded_files))

        status_text.text("Обработка завершена!")

        # Сохраняем результаты в сессию
        st.session_state.transactions = all_transactions
        st.session_state.metadata_list = all_metadata
        st.session_state.processed_files = processed

        st.success(f"Обработано файлов: {len(processed)}, транзакций: {len(all_transactions)}")

# Отображение результатов обработки
if st.session_state.processed_files:
    st.header("📊 Результаты обработки")

    results_df = pd.DataFrame(st.session_state.processed_files)
    st.dataframe(results_df, use_container_width=True, hide_index=True)

# Статистика
if st.session_state.transactions:
    st.header("📈 Статистика")

    loader = BankStatementLoader()
    loader.last_transactions = st.session_state.transactions
    df = loader.to_dataframe()

    # Метрики в колонках
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Всего транзакций", len(df))

    with col2:
        income = df[df['Направление'] == 'Приход']['Сумма'].sum() if 'Направление' in df.columns else 0
        st.metric("Приход (KZT)", f"{income:,.0f}")

    with col3:
        expense = df[df['Направление'] == 'Расход']['Сумма'].sum() if 'Направление' in df.columns else 0
        st.metric("Расход (KZT)", f"{expense:,.0f}")

    with col4:
        balance = income - expense
        st.metric("Баланс", f"{balance:,.0f}")

    # Статистика по банкам
    st.subheader("По банкам")
    if 'Банк выписки' in df.columns:
        bank_stats = df.groupby('Банк выписки').agg({
            'Сумма': ['count', 'sum']
        }).reset_index()
        bank_stats.columns = ['Банк', 'Кол-во транзакций', 'Общая сумма']
        st.dataframe(bank_stats, use_container_width=True, hide_index=True)

    # Статистика по направлениям
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("По направлениям")
        if 'Направление' in df.columns:
            direction_stats = df.groupby('Направление').agg({
                'Сумма': ['count', 'sum']
            }).reset_index()
            direction_stats.columns = ['Направление', 'Кол-во', 'Сумма']
            st.dataframe(direction_stats, use_container_width=True, hide_index=True)

    with col2:
        st.subheader("По валютам")
        if 'Валюта' in df.columns:
            currency_stats = df.groupby('Валюта').agg({
                'Сумма': ['count', 'sum']
            }).reset_index()
            currency_stats.columns = ['Валюта', 'Кол-во', 'Сумма']
            st.dataframe(currency_stats, use_container_width=True, hide_index=True)

    # Период данных
    if 'Дата операции' in df.columns:
        st.subheader("Период данных")
        min_date = pd.to_datetime(df['Дата операции']).min()
        max_date = pd.to_datetime(df['Дата операции']).max()
        st.write(f"С **{min_date.strftime('%d.%m.%Y')}** по **{max_date.strftime('%d.%m.%Y')}**")

# Предпросмотр данных
if st.session_state.transactions:
    st.header("👁️ Предпросмотр данных")

    loader = BankStatementLoader()
    loader.last_transactions = st.session_state.transactions
    df = loader.to_dataframe()

    # Выбор количества строк
    num_rows = st.selectbox(
        "Количество строк для отображения",
        options=[10, 25, 50, 100, "Все"],
        index=0
    )

    if num_rows == "Все":
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.dataframe(df.head(num_rows), use_container_width=True, hide_index=True)

    st.caption(f"Показано {min(num_rows if num_rows != 'Все' else len(df), len(df))} из {len(df)} записей")

# Экспорт данных
if st.session_state.transactions:
    st.header("💾 Экспорт данных")

    col1, col2 = st.columns(2)

    with col1:
        # Экспорт в Excel
        loader = BankStatementLoader()
        loader.last_transactions = st.session_state.transactions
        if st.session_state.metadata_list:
            loader.last_metadata = st.session_state.metadata_list[0]

        df = loader.to_dataframe()

        # Создаём Excel в памяти
        from io import BytesIO
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Транзакции', index=False)

        excel_data = output.getvalue()

        st.download_button(
            label="📥 Скачать Excel",
            data=excel_data,
            file_name=f"transactions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    with col2:
        # Экспорт в CSV
        csv_data = df.to_csv(index=False, encoding='utf-8-sig')

        st.download_button(
            label="📥 Скачать CSV",
            data=csv_data,
            file_name=f"transactions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )

# Очистка данных
if st.session_state.transactions:
    st.divider()
    if st.button("🗑️ Очистить данные"):
        st.session_state.transactions = []
        st.session_state.metadata_list = []
        st.session_state.processed_files = []
        st.rerun()

# Футер
st.divider()
st.caption("Bank Statement Loader v1.0.0 | Поддержка 12 казахстанских банков")
