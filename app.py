import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px
import os
import json
from datetime import datetime
from typing import Tuple

# --- Funciones ---
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
        raise ValueError(f'Extensión {ext} no soportada')
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

        st.subheader("📊 Dashboard Automático")

        # 🔍 Detección automática de columnas de fecha y monto
        col_monto = next((c for c in df_master.columns if any(k in c for k in ['venta', 'monto', 'total', 'ingreso'])), None)
        col_fecha = next((c for c in df_master.columns if any(k in c for k in ['fecha', 'date'])), None)

        if not col_monto:
            col_monto = st.selectbox("Selecciona la columna de monto o venta", df_master.columns)
        if not col_fecha:
            col_fecha = st.selectbox("Selecciona la columna de fecha", df_master.columns)

        if col_monto and col_fecha:
            df_master[col_fecha] = pd.to_datetime(df_master[col_fecha], errors='coerce')
            df_master = df_master.dropna(subset=[col_fecha])

            ingreso_total = df_master[col_monto].sum()
            st.metric("Ingreso total", f"${ingreso_total:,.0f}")

            try:
                df_trend = df_master.set_index(col_fecha).resample('M')[col_monto].sum().reset_index()
                fig_trend = px.line(df_trend, x=col_fecha, y=col_monto, title='📈 Ingreso Mensual')
                st.plotly_chart(fig_trend)
            except Exception as e:
                st.warning(f"No se pudo generar la serie temporal: {e}")
        else:
            st.warning("⚠️ No se encontraron columnas adecuadas de fecha o monto para generar el gráfico.")

        # --- 🔥 NUEVA SECCIÓN: Gráficos Personalizados ---
        st.subheader("🎨 Crea tus propios gráficos")
        st.write("Selecciona qué columnas quieres graficar y el tipo de gráfico.")

        numeric_cols = df_master.select_dtypes(include=['number']).columns.tolist()
        all_cols = df_master.columns.tolist()

        col_x = st.selectbox("Eje X (categoría o fecha)", all_cols)
        col_y = st.selectbox("Eje Y (valor numérico)", numeric_cols)
        chart_type = st.radio("Tipo de gráfico", ["Barras", "Líneas", "Pastel"], horizontal=True)

        if col_x and col_y:
            if chart_type == "Barras":
                fig = px.bar(df_master, x=col_x, y=col_y, title=f"{col_y} por {col_x}")
            elif chart_type == "Líneas":
                fig = px.line(df_master, x=col_x, y=col_y, title=f"{col_y} en el tiempo ({col_x})")
            elif chart_type == "Pastel":
                df_grouped = df_master.groupby(col_x)[col_y].sum().reset_index()
                fig = px.pie(df_grouped, names=col_x, values=col_y, title=f"Distribución de {col_y} por {col_x}")
            
            st.plotly_chart(fig, use_container_width=True)
