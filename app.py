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

        # 🔍 Detección automática de columnas clave
        col_monto = next((c for c in df_master.columns if any(k in c for k in ['venta', 'monto', 'total', 'ingreso'])), None)
        col_fecha = next((c for c in df_master.columns if any(k in c for k in ['fecha', 'date'])), None)

        if not col_monto:
            col_monto = st.selectbox("Selecciona la columna de monto o venta", df_master.columns)
        if not col_fecha:
            col_fecha = st.selectbox("Selecciona la columna de fecha", df_master.columns)

        # ------------------------------
        # 🔥 🔥 🔥 NUEVA SECCIÓN: SEGMENTADORES 🔥 🔥 🔥
        # ------------------------------
        if col_monto and col_fecha:
            # --- 🔍 Detectar posibles columnas para segmentar ---
            posibles_productos = [c for c in df_master.columns if any(k in c for k in ['producto', 'item', 'articulo', 'sku'])]
            posibles_locales = [c for c in df_master.columns if any(k in c for k in ['local', 'tienda', 'sucursal'])]
            posibles_regiones = [c for c in df_master.columns if any(k in c for k in ['region', 'ciudad', 'zona', 'pais'])]

            st.subheader("🎯 Segmentadores de Datos")

            # Si existe columna de fecha, permitir filtrar por año
            df_master[col_fecha] = pd.to_datetime(df_master[col_fecha], errors='coerce')
            df_master = df_master.dropna(subset=[col_fecha])
            df_master['año'] = df_master[col_fecha].dt.year

            años_disponibles = sorted(df_master['año'].dropna().unique())
            año_sel = st.radio("Selecciona el año", ["Todos"] + list(map(str, años_disponibles)), horizontal=True)
            if año_sel != "Todos":
                df_master = df_master[df_master['año'] == int(año_sel)]

            # Filtros por producto, local y región
            if posibles_productos:
                productos_sel = st.multiselect("Filtrar por producto", sorted(df_master[posibles_productos[0]].unique()))
                if productos_sel:
                    df_master = df_master[df_master[posibles_productos[0]].isin(productos_sel)]

            if posibles_locales:
                locales_sel = st.multiselect("Filtrar por local o tienda", sorted(df_master[posibles_locales[0]].unique()))
                if locales_sel:
                    df_master = df_master[df_master[posibles_locales[0]].isin(locales_sel)]

            if posibles_regiones:
                regiones_sel = st.multiselect("Filtrar por región o ciudad", sorted(df_master[posibles_regiones[0]].unique()))
                if regiones_sel:
                    df_master = df_master[df_master[posibles_regiones[0]].isin(regiones_sel)]
        # ------------------------------
        # 🔥 FIN NUEVA SECCIÓN 🔥
        # ------------------------------

        if col_monto and col_fecha:
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

        # --- 🔥 NUEVA SECCIÓN: Gráficos Automáticos Inteligentes ---
        st.subheader("🤖 Análisis Automático")
        posibles_productos = [c for c in df_master.columns if any(k in c for k in ['producto', 'item', 'articulo', 'sku'])]
        posibles_locales = [c for c in df_master.columns if any(k in c for k in ['local', 'tienda', 'sucursal'])]
        posibles_regiones = [c for c in df_master.columns if any(k in c for k in ['region', 'ciudad', 'zona', 'pais'])]

        # Top 10 productos más vendidos
        if posibles_productos and col_monto:
            col_prod = posibles_productos[0]
            df_top = df_master.groupby(col_prod)[col_monto].sum().reset_index().sort_values(col_monto, ascending=False).head(10)
            fig_top = px.bar(df_top, x=col_prod, y=col_monto, title="🏆 Top 10 productos más vendidos")
            st.plotly_chart(fig_top, use_container_width=True)

        # Locales con mayores ventas
        if posibles_locales and col_monto:
            col_loc = posibles_locales[0]
            df_loc = df_master.groupby(col_loc)[col_monto].sum().reset_index().sort_values(col_monto, ascending=False).head(10)
            fig_loc = px.bar(df_loc, x=col_loc, y=col_monto, title="🏪 Locales con mayores ventas")
            st.plotly_chart(fig_loc, use_container_width=True)

        # Ventas por región o ciudad
        if posibles_regiones and col_monto:
            col_reg = posibles_regiones[0]
            df_reg = df_master.groupby(col_reg)[col_monto].sum().reset_index().sort_values(col_monto, ascending=False)
            fig_reg = px.pie(df_reg, names=col_reg, values=col_monto, title="🌎 Ventas por región o ciudad")
            st.plotly_chart(fig_reg, use_container_width=True)

        # --- 🎨 Gráficos Personalizados ---
        st.subheader("🎨 Crea tus propios gráficos")
        st.write("Selecciona qué columnas quieres graficar y el tipo de gráfico.")

        numeric_cols = df_master.select_dtypes(include=['number']).columns.tolist()
        all_cols = df_master.columns.tolist()

        col_x = st.selectbox("Eje X (categoría o_
