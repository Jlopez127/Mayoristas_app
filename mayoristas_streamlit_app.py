
import streamlit as st
import pandas as pd
import os
import requests
import io
import glob
from datetime import datetime

st.set_page_config(page_title="Mayoristas Encargomio", layout="wide")
st.title("📦 Procesamiento de Mayoristas - Encargomio")

# Diccionarios globales
ingresos_por_casillero = {}
egresos_por_casillero = {}
ingresos_extra_por_casillero = {}
conciliaciones = {}

casilleros = ["9444", "14856", "11591", "1444", "1633"]

# ========== Funciones ==========

def cargar_trm(fecha_str):
    try:
        url = f"https://www.datos.gov.co/resource/mcec-87by.json?vigenciadesde={fecha_str}T00:00:00.000"
        response = requests.get(url)
        data = response.json()
        if data and "valor" in data[0]:
            return float(data[0]["valor"])
    except:
        pass
    return None

def procesar_egresos(df_compras):
    df_compras["Fecha Compra"] = pd.to_datetime(df_compras["Fecha Compra"], errors="coerce", utc=True).dt.tz_convert(None)
    df_compras["Fecha Compra"] = df_compras["Fecha Compra"].dt.strftime('%Y-%m-%d')
    df_compras["Casillero"] = df_compras["Casillero"].astype(str)
    df_compras["Orden"] = pd.to_numeric(df_compras["Orden"], errors="coerce").astype("Int64")
    df_compras["Total de Pago COP"] = pd.to_numeric(df_compras["Total de Pago COP"], errors='coerce')
    df_compras["Valor de compra COP"] = pd.to_numeric(df_compras["Valor de compra COP"], errors='coerce')
    df_compras["Orden"] = df_compras["Orden"].astype(str)

    df_filtrado = df_compras[df_compras["Casillero"].isin(casilleros)].copy()
    df_filtrado["Tipo"] = "Egreso"
    condicion = (df_filtrado["Estado de Orden"] == "Cancelada") & (df_filtrado["Total de Pago COP"].isna())
    df_filtrado.loc[condicion, "Total de Pago COP"] = df_filtrado.loc[condicion, "Valor de compra COP"]
    df_filtrado.rename(columns={"Fecha Compra": "Fecha", "Valor de compra COP": "Monto"}, inplace=True)

    for casillero in casilleros:
        df_cas = df_filtrado[df_filtrado["Casillero"] == casillero]
        if not df_cas.empty:
            nombre_df = f"egresos_{casillero}"
            egresos_por_casillero[nombre_df] = df_cas.copy()
            st.success(f"Egresos cargados: {nombre_df} ({len(df_cas)} filas)")

def procesar_ingresos_extra(hojas):
    for nombre_hoja, df in hojas.items():
        casillero = str(nombre_hoja).split("-")[0].strip()
        if casillero.isdigit():
            nombre_df = f"Movimientos_extra_{casillero}"
            df["Casillero"] = df.get("Casillero", casillero)
            df["Casillero"] = df["Casillero"].astype(str)
            trm_valor = cargar_trm(pd.to_datetime(df["Fecha"]).max().strftime("%Y-%m-%d"))
            df["TRM"] = trm_valor
            ingresos_extra_por_casillero[nombre_df] = df.copy()
            st.success(f"Ingreso extra cargado: {nombre_df} ({len(df)} filas)")

def procesar_ingresos_archivos_multiples(files, usuario, casillero):
    dfs = []
    for archivo in files:
        df = pd.read_csv(archivo, sep='	', encoding='latin-1', engine='python')
        dfs.append(df)
    if not dfs:
        return
    df_final = pd.concat(dfs, ignore_index=True)
    df_final["REFERENCIA"] = df_final["REFERENCIA"].fillna(df_final["DESCRIPCIÓN"])
    df_final = df_final.dropna(how='all', axis=1)
    df_final["FECHA"] = pd.to_datetime(df_final["FECHA"], format="%Y/%m/%d", errors='coerce')
    df_final["VALOR"] = df_final["VALOR"].replace({",": ""}, regex=True).astype(float)
    df_final["Tipo"] = "Ingreso"
    df_final["Orden"] = ""
    df_final["Usuario"] = usuario
    df_final["Casillero"] = casillero
    df_final["Estado de Orden"] = ""
    df_final = df_final.rename(columns={"FECHA": "Fecha", "VALOR": "Monto", "REFERENCIA": "Nombre del producto"})
    df_final = df_final[df_final["Nombre del producto"] != "ABONO INTERESES AHORROS"]
    df_final = df_final[df_final["Monto"] > 0]
    trm_valor = cargar_trm(df_final["Fecha"].max().strftime("%Y-%m-%d"))
    df_final["TRM"] = trm_valor
    df_final = df_final[["Fecha", "Tipo", "Monto", "Orden", "TRM", "Usuario", "Casillero", "Estado de Orden", "Nombre del producto"]]
    ingresos_por_casillero[f"ingresos_{casillero}"] = df_final.copy()
    st.success(f"Ingresos cargados para {usuario} ({casillero}): {len(df_final)} filas.")

# ========== Interfaz ==========

st.header("1️⃣ Cargar COMPRAS")
archivo_compras = st.file_uploader("Archivo de COMPRAS (.xlsx)", type=["xlsx"], key="compras")
if archivo_compras:
    df_compras = pd.read_excel(archivo_compras)
    procesar_egresos(df_compras)

st.header("2️⃣ Cargar INGRESOS EXTRA")
archivo_extra = st.file_uploader("Archivo INGRESOS EXTRA (.xlsx con varias hojas)", type=["xlsx"], key="extra")
if archivo_extra:
    hojas = pd.read_excel(archivo_extra, sheet_name=None)
    procesar_ingresos_extra(hojas)

st.header("3️⃣ Cargar archivos Nathalia (.xls múltiples)")
archivos_nathalia = st.file_uploader("Archivos de Nathalia", type=["xls"], accept_multiple_files=True)
if archivos_nathalia:
    procesar_ingresos_archivos_multiples(archivos_nathalia, usuario="Nathalia Ospina", casillero="1633")

st.header("4️⃣ Cargar archivos Elvis (.xls múltiples)")
archivos_elvis = st.file_uploader("Archivos de Elvis", type=["xls"], accept_multiple_files=True, key="elvis")
if archivos_elvis:
    procesar_ingresos_archivos_multiples(archivos_elvis, usuario="Paula Herrera", casillero="11591")

st.header("5️⃣ Conciliación Final")
if st.button("Ejecutar conciliación"):
    for casillero in casilleros:
        ingresos_df = ingresos_por_casillero.get(f"ingresos_{casillero}")
        egresos_df = egresos_por_casillero.get(f"egresos_{casillero}")
        extra_df = ingresos_extra_por_casillero.get(f"Movimientos_extra_{casillero}")

        frames = [df for df in [ingresos_df, egresos_df, extra_df] if df is not None and not df.empty]
        if frames:
            df_final = pd.concat(frames, ignore_index=True)
            conciliaciones[f"conciliacion_{casillero}"] = df_final
            st.success(f"✔️ Conciliación generada para casillero {casillero} ({len(frames)} fuentes)")
        else:
            conciliaciones[f"conciliacion_{casillero}"] = pd.DataFrame()
            st.warning(f"⛔ Sin movimientos para casillero {casillero}, conciliación vacía creada")

    st.balloons()
