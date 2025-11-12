import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px
import os
import json
from datetime import datetime
from typing import Tuple

# --- Funciones de tu código (simplificado para la web) ---
def load_file(file) -> pd.DataFrame:
    ext = os.path.splitext(file.name)[1].lower()
    if ext == '.csv':
        df = pd.read_csv(file)
    elif ext in ['.xls', '.xlsx']:
        df = pd.read_excel(file)
    elif ext == '.json':
        data = json.load(file)
        df = pd.json_normalize(data)
    else:
        raise ValueError(f'Extension {ext} no soportada')
    return df

def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns
        .astype(str)
        .str.strip()
        .str.lower()
        .str.replace(' ', '_')
        .str.replace('-', '_')
        .str.replace(r'[^\w]', '', regex=True)
    )
    return df

def clean_table(df: pd.DataFrame, table_type: str = None) -> Tuple[pd.DataFrame, dict]:
    df = standardize_columns(df)
    before_rows = len(df)
    df = df.drop_duplicates()
    removed_rows = 0
    obj_cols = df.select_dtypes(include=['object', 'string']).columns
    for c in obj_cols:
        df[c] = df[c].fillna('Desconocido')
    after_rows = len(df)
    report = {
        'rows_before': before_rows,
        'rows_after': after_rows,
        'deduplicated': before_rows - after_rows
    }
    return df, report

# --- Streamlit UI ---
st.title("🚀 Generador de Dashboards Interactivos")
st.write("Sube tus archivos Excel, CSV o JSON para generar dashboards automáticamente.")

uploaded_files = st.file_uploader("Sube tus archivos", type=['csv','xlsx','xls','json'], accept_multiple_files=True)

if uploaded_files:
    datasets = {}
    for file in uploaded_files:
        st.subheader(f"Procesando: {file.name}")
        try:
            df = load_file(file)
            st.dataframe(df.head(5))
        except Exception as e:
            st.error(f"Error al leer {file.name}: {e}")
            continue
        
        # Selección de área
        area = st.selectbox(f"Selecciona el área de {file.name}", ['ventas', 'clientes', 'productos', 'otro'])
        df_clean, report = clean_table(df, table_type=area)
        st.write("Reporte limpieza:", report)

        if area not in datasets:
            datasets[area] = []
        datasets[area].append({'filename': file.name, 'df': df_clean})

    # --- Combinar datos y generar dashboard ---
    if 'ventas' in datasets:
        df_ventas = pd.concat([item['df'] for item in datasets['ventas']], ignore_index=True)
        con = duckdb.connect(database=':memory:')
        con.register('ventas', df_ventas)
        df_master = df_ventas.copy()

        # Dashboard simple
        if 'total_venta' in df_master.columns and 'fecha_venta' in df_master.columns:
            df_master['fecha_venta'] = pd.to_datetime(df_master['fecha_venta'], errors='coerce')
            ingreso_total = df_master['total_venta'].sum()
            st.metric("Ingreso total", f"${ingreso_total:,.0f}")

            df_trend = df_master.set_index('fecha_venta').resample('M')['total_venta'].sum().reset_index()
            fig_trend = px.line(df_trend, x='fecha_venta', y='total_venta', title='Ingreso Mensual')
            st.plotly_chart(fig_trend)
