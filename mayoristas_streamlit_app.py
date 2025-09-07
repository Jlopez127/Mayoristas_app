# -*- coding: utf-8 -*-
"""
Created on Tue May 20 17:35:02 2025

@author: User
"""

import streamlit as st
import pandas as pd
import requests
import io
from datetime import datetime
from datetime import timedelta
import dropbox
import numpy as np
import smtplib, ssl
from email.message import EmailMessage


st.set_page_config(page_title="Conciliaciones Mayoristas", layout="wide")
# Crea un cliente de Dropbox usando tu token de Secrets
cfg_dbx = st.secrets["dropbox"]
dbx = dropbox.Dropbox(
    app_key=cfg_dbx["app_key"],
    app_secret=cfg_dbx["app_secret"],
    oauth2_refresh_token=cfg_dbx["refresh_token"],
)
def upload_to_dropbox(data: bytes):
    """Sube (o sobrescribe) un archivo a Dropbox."""
    cfg = st.secrets["dropbox"]
    try:
        dbx.files_upload(
            data,
            cfg["remote_path"],
            mode=dropbox.files.WriteMode.overwrite
        )
        st.success("✅ Histórico subido a Dropbox")
    except Exception as e:
        st.error(f"❌ Error subiendo a Dropbox: {e}")



# — 1) Egresos (Compras) —
@st.cache_data
def procesar_egresos(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    casilleros = ["9444", "14856", "11591", "1444", "1633", "13608"]
    df = df.copy()
    df["Fecha Compra"] = pd.to_datetime(df["Fecha Compra"], errors="coerce", utc=True)
    df["Fecha Compra"] = df["Fecha Compra"].dt.tz_convert(None)
    df["Casillero"] = df["Casillero"].astype(str)
    df = df[df["Casillero"].isin(casilleros)]
    cutoff = pd.Timestamp("2025-08-25")
    df = df[(df["Casillero"] != "13608") | (df["Fecha Compra"] >= cutoff)]
    df["Fecha Compra"] = df["Fecha Compra"].dt.strftime("%Y-%m-%d")
    df["Tipo"] = "Egreso"
    df["Total de Pago COP"] = pd.to_numeric(df["Total de Pago COP"], errors="coerce")
    df["Valor de compra COP"] = pd.to_numeric(df["Valor de compra COP"], errors="coerce")
    mask = (df["Estado de Orden"] == "Cancelada") & df["Total de Pago COP"].isna()
    df.loc[mask, "Total de Pago COP"] = df.loc[mask, "Valor de compra COP"]
    df["Orden"] = pd.to_numeric(df["Orden"], errors="coerce").astype("Int64")
    df = df.sort_values("Orden")
    df["Orden"] = df["Orden"].astype(str)
    df["Monto"] = df.apply(
    lambda row: row["Valor de compra USD"] if row["Casillero"] in ["1444", "14856"] else row["Valor de compra COP"],
    axis=1
    )
    df = df.rename(columns={
        "Fecha Compra": "Fecha"
    })[["Fecha","Tipo","Monto","Orden","TRM","Usuario","Casillero","Estado de Orden","Nombre del producto"]]
    df.loc[df["Casillero"]=="9444","Usuario"] = "Maira Alejandra Paez"
    salida = {}
    for cas in casilleros:
        salida[f"egresos_{cas}"] = df[df["Casillero"]==cas].reset_index(drop=True)
    return salida

# — 2) Ingresos Extra —
@st.cache_data
def procesar_ingresos_extra(hojas: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    resultado = {}
    for hoja, df in (hojas or {}).items():
        cas = hoja.split("-")[0].strip()
        if not cas.isdigit(): continue
        df2 = df.copy()
        if "Casillero" in df2.columns:
            df2["Casillero"] = df2["Casillero"].astype(str)
        else:
            df2["Casillero"] = cas
        if "Fecha" in df2.columns:
            try:
                fmax = pd.to_datetime(df2["Fecha"]).max().strftime("%Y-%m-%d")
                url = f"https://www.datos.gov.co/resource/mcec-87by.json?vigenciadesde={fmax}T00:00:00.000"
                data = requests.get(url).json()
                trm = float(data[0]["valor"]) if data and "valor" in data[0] else None
            except:
                trm = None
            df2["TRM"] = trm
        resultado[f"extra_{cas}"] = df2.reset_index(drop=True)
    return resultado

# — 3+4) Ingresos Cliente Genérico —
@st.cache_data



# 1) Tu función original para los .xls (TSV renombrado)
def procesar_ingresos_clientes_xls(files: list[bytes], usuario: str, casillero: str) -> pd.DataFrame:
      dfs = []
      for up in files:
          df = pd.read_csv(up, sep="\t", encoding="latin-1", engine="python")
          df["Archivo_Origen"] = up.name
          dfs.append(df)
      if not dfs:
          return pd.DataFrame()
      df = pd.concat(dfs, ignore_index=True)
      df["REFERENCIA"] = df["REFERENCIA"].fillna(df.get("DESCRIPCIÓN",""))
      df = df.dropna(how="all", axis=1)
      df["FECHA"] = pd.to_datetime(df["FECHA"], format="%Y/%m/%d", errors="coerce")
      df["VALOR"] = df["VALOR"].astype(str).str.replace(",","").astype(float)
      df["Tipo"] = "Ingreso"
      df["Orden"] = ""
      df["Usuario"] = usuario
      df["Casillero"] = casillero
      df["Estado de Orden"] = ""
      out = df.rename(columns={
          "FECHA":"Fecha",
          "VALOR":"Monto",
          "REFERENCIA":"Nombre del producto"
      })[["Fecha","Tipo","Monto","Orden","Usuario","Casillero","Estado de Orden","Nombre del producto"]]
      out = out[out["Nombre del producto"]!="ABONO INTERESES AHORROS"]
      out = out[out["Monto"]>0]
      try:
          fmax = out["Fecha"].max().strftime("%Y-%m-%d")
          url = f"https://www.datos.gov.co/resource/mcec-87by.json?vigenciadesde={fmax}T00:00:00.000"
          data = requests.get(url).json()
          trm = float(data[0]["valor"]) if data and "valor" in data[0] else None
      except:
          trm = None
      out["TRM"] = trm
      return out.reset_index(drop=True)

# 2) Nueva función para CSV “reales”
def procesar_ingresos_clientes_csv(files: list[bytes], usuario: str, casillero: str) -> pd.DataFrame:
    dfs = []
    for up in files:
        contenido = up.read() if hasattr(up, "read") else up
        fname = getattr(up, "name", "archivo_sin_nombre")
        
        texto = None
        for codec in ("utf-8", "utf-8-sig", "latin-1", "cp1252"):
            try:
                texto = contenido.decode(codec)
                break
            except UnicodeDecodeError:
                continue
        
        if texto is None:
            import streamlit as st
            st.warning(f"⚠️ No se pudo decodificar '{fname}' con utf-8 / utf-8-sig / latin-1 / cp1252. Se omite.")
            continue
        
        buf = io.StringIO(texto)
        df = pd.read_csv(buf, header=None, sep=",")  # <- ya no pases 'encoding'

        # Normalizar a 10 columnas (acepta 9 o 10)
        if df.shape[1] == 9:
            df["DESCONOCIDA_6"] = None  # agrega columna vacía al final
        elif df.shape[1] != 10:
            import streamlit as st
            fname = getattr(up, "name", "archivo_sin_nombre")
            st.warning(f"⚠️ '{fname}' tiene {df.shape[1]} columnas (esperaba 9 o 10). Se omite.")
            continue

        # Esquema fijo de 10 columnas
        df.columns = [
            "DESCRIPCIÓN", "DESCONOCIDA_1", "DESCONOCIDA_2", "FECHA",
            "DESCONOCIDA_3", "VALOR", "DESCONOCIDA_4", "REFERENCIA",
            "DESCONOCIDA_5", "DESCONOCIDA_6"
        ]
        df["Archivo_Origen"] = getattr(up, "name", "archivo_sin_nombre")
        dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)

    # Completar REFERENCIA con DESCRIPCIÓN si viene vacía
    df["REFERENCIA"] = df["REFERENCIA"].fillna(df.get("DESCRIPCIÓN", ""))

    # Eliminar columnas completamente vacías
    df = df.dropna(how="all", axis=1)

    # ---- Fecha con fallback por fila ----
    fechas_raw = df["FECHA"].astype(str).str.strip().str.zfill(8)
    f1 = pd.to_datetime(fechas_raw, format="%Y%m%d", errors="coerce")   # nuevo formato
    f2 = pd.to_datetime(fechas_raw, format="%d%m%Y", errors="coerce")   # formato viejo
    df["FECHA"] = f1.fillna(f2)
    # ------------------------------------

    # LIMPIEZA DE VALOR
    df["VALOR"] = (
        df["VALOR"]
        .astype(str)
        .str.replace(",", "", regex=False)  # elimina separador de miles si aparece
        .str.strip()
        .astype(float)
    )

    # Enriquecimiento
    df["Tipo"] = "Ingreso"
    df["Orden"] = ""
    df["Usuario"] = usuario
    df["Casillero"] = casillero
    df["Estado de Orden"] = ""

    # Renombrar y seleccionar columnas finales
    out = df.rename(columns={
        "FECHA": "Fecha",
        "VALOR": "Monto",
        "REFERENCIA": "Nombre del producto"
    })[["Fecha", "Tipo", "Monto", "Orden", "Usuario", "Casillero", "Estado de Orden", "Nombre del producto"]]

    # Filtros de negocio
    out = out[out["Nombre del producto"] != "ABONO INTERESES AHORROS"]
    out = out[out["Monto"] > 0]

    # TRM desde datos.gov.co (si posible)
    try:
        fmax = out["Fecha"].max().strftime("%Y-%m-%d")
        url = f"https://www.datos.gov.co/resource/mcec-87by.json?vigenciadesde={fmax}T00:00:00.000"
        data = requests.get(url).json()
        trm = float(data[0]["valor"]) if data and "valor" in data[0] else None
    except Exception:
        trm = None

    out["TRM"] = trm
    return out.reset_index(drop=True)

# 3) Pipeline común (extrae tu lógica de post-procesamiento)

def procesar_ingresos_clientes_csv_casillero1444(files: list[bytes], usuario: str, casillero: str) -> pd.DataFrame:
    import io
    import pandas as pd
    import requests
    try:
        import streamlit as st
    except Exception:
        # Si no se ejecuta en Streamlit, definimos un stub mínimo para evitar errores.
        class _Stub:
            def warning(self, *a, **k): pass
        st = _Stub()

    dfs = []
    for up in files:
        # --- Lectura robusta del archivo (bytes -> str) ---
        contenido = up.read() if hasattr(up, "read") else up
        fname = getattr(up, "name", "archivo_sin_nombre")

        texto = None
        for codec in ("utf-8", "utf-8-sig", "latin-1"):
            try:
                texto = contenido.decode(codec)
                break
            except UnicodeDecodeError:
                continue
        if texto is None:
            st.warning(f"⚠️ No se pudo decodificar '{fname}' con utf-8 / utf-8-sig / latin-1. Se omite.")
            continue

        buf = io.StringIO(texto)
        df = pd.read_csv(buf, header=None, sep=",")

        # --- Normalizar a 10 columnas (acepta 9 o 10) ---
        if df.shape[1] == 9:
            df["DESCONOCIDA_6"] = None  # agrega columna vacía al final
        elif df.shape[1] != 10:
            st.warning(f"⚠️ '{fname}' tiene {df.shape[1]} columnas (esperaba 9 o 10). Se omite.")
            continue

        # Esquema fijo de 10 columnas
        df.columns = [
            "DESCRIPCIÓN", "DESCONOCIDA_1", "DESCONOCIDA_2", "FECHA",
            "DESCONOCIDA_3", "VALOR", "DESCONOCIDA_4", "REFERENCIA",
            "DESCONOCIDA_5", "DESCONOCIDA_6"
        ]
        df["Archivo_Origen"] = fname
        dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    # Concatenamos todos los DataFrames válidos
    df = pd.concat(dfs, ignore_index=True)

    # REFERENCIA fallback desde DESCRIPCIÓN
    df["REFERENCIA"] = df["REFERENCIA"].fillna(df.get("DESCRIPCIÓN", ""))
    # Eliminar columnas completamente vacías
    df = df.dropna(how="all", axis=1)

    # ---- FECHA con fallback por fila ----
    # Limpia todo lo que no sea dígito, rellena a 8 y prueba YYYYMMDD; si NaT, prueba DDMMYYYY
    fechas_raw = (
        df["FECHA"].astype(str).str.strip()
        .str.replace(r"[^\d]", "", regex=True)
        .str.zfill(8)
    )
    f1 = pd.to_datetime(fechas_raw, format="%Y%m%d", errors="coerce")  # nuevo
    f2 = pd.to_datetime(fechas_raw, format="%d%m%Y", errors="coerce")  # viejo
    df["FECHA"] = f1.fillna(f2)

    # ---- VALOR a float ----
    df["VALOR"] = (
        df["VALOR"].astype(str)
        .str.replace(",", "", regex=False)  # si viniera separador de miles
        .str.strip()
        .replace({"": None})
        .astype(float)
    )

    # ===================== TRM por rango =====================
    try:
        # 1) Fecha máxima de transacción
        fecha_max_dt = df["FECHA"].max().date()

        # 2) Traemos TRM publicadas hasta esa fecha (orden descendente)
        punto_corte = f"{fecha_max_dt:%Y-%m-%d}T00:00:00.000"
        url = (
            "https://www.datos.gov.co/resource/mcec-87by.json?"
            f"$where=vigenciadesde <= '{punto_corte}'&$order=vigenciadesde DESC"
        )
        respuesta = requests.get(url)
        respuesta.raise_for_status()
        lista_trm = respuesta.json()

        if not lista_trm:
            st.warning("No se encontraron registros de TRM anteriores o iguales a la fecha máxima.")
            df_trm_para_joins = pd.DataFrame(columns=["Fecha", "TRM"])
        else:
            trm_df = pd.DataFrame(lista_trm)
            if "vigenciadesde" not in trm_df.columns or "valor" not in trm_df.columns:
                st.warning("La API de TRM no devolvió los campos esperados.")
                df_trm_para_joins = pd.DataFrame(columns=["Fecha", "TRM"])
            else:
                trm_df["vigenciadesde"] = pd.to_datetime(trm_df["vigenciadesde"]).dt.date
                trm_df["valor"] = trm_df["valor"].astype(float)
                trm_df = trm_df.rename(columns={"vigenciadesde": "Fecha", "valor": "TRM"})
                trm_df = trm_df.drop_duplicates(subset="Fecha", keep="first")
                trm_df = trm_df.sort_values("Fecha", ascending=False).set_index("Fecha")

                # 3) Rango de fechas para ffill
                fecha_min_trm = trm_df.index.min()
                fecha_min_tx = df["FECHA"].dt.date.min()
                fecha_inicio_rango = min(fecha_min_trm, fecha_min_tx)
                fecha_fin_rango = fecha_max_dt

                rango_fechas = pd.date_range(start=fecha_inicio_rango, end=fecha_fin_rango, freq="D").date
                df_trm_full = pd.DataFrame(index=rango_fechas, columns=["TRM"])

                # 7) Colocar TRM conocida en días disponibles
                for dia in trm_df.index:
                    df_trm_full.loc[dia, "TRM"] = trm_df.loc[dia, "TRM"]

                # 8) Rellenar hacia adelante
                df_trm_full["TRM"] = df_trm_full["TRM"].ffill()

                # 9) Preparar para merge
                df_trm_para_joins = df_trm_full.reset_index().rename(columns={"index": "Fecha"})
    except Exception as e:
        st.warning(f"No se pudo obtener TRM: {e}")
        df_trm_para_joins = pd.DataFrame(columns=["Fecha", "TRM"])
    # =========================================================

    # Preparar columna Fecha (date) y merge con TRM
    df["Fecha"] = df["FECHA"].dt.date
    df = df.merge(df_trm_para_joins, on="Fecha", how="left")

    # Calcular VALOR en USD (como en tu ejemplo, usando TRM + 100) y evitar división por nulo
    # Si TRM es NaN, el resultado será NaN; puedes decidir si llenar con 0 u otro valor.
    df["VALOR_USD"] = df["TRM"]
    df.loc[df["TRM"].notna(), "VALOR_USD"] = df.loc[df["TRM"].notna(), "VALOR"] / (df.loc[df["TRM"].notna(), "TRM"] + 100)

    # Asignar columnas estándar
    df["Tipo"] = "Ingreso"
    df["Orden"] = ""
    df["Usuario"] = usuario
    df["Casillero"] = casillero
    df["Estado de Orden"] = ""

    # Construcción final
    out = df.rename(columns={
        "VALOR_USD": "Monto",
        "REFERENCIA": "Nombre del producto"
    })[["Fecha", "Tipo", "Monto", "Orden", "Usuario", "Casillero", "Estado de Orden", "Nombre del producto", "TRM"]]

    # Filtros de negocio
    out = out[out["Nombre del producto"] != "ABONO INTERESES AHORROS"]
    out = out[out["Monto"] > 0]

    return out.reset_index(drop=True)


def procesar_ingresos_davivienda(files: list, usuario: str, casillero: str) -> pd.DataFrame:
    """
    Procesa archivos Excel de Davivienda (UploadedFile de Streamlit).
    Toma “Descripción motivo” (columna REFERENCIA) como Nombre del producto, 
    pero si está vacía o nula, utiliza “Referencia 1” como fallback.
    Devuelve un DataFrame con la misma estructura que la función de Bancolombia.
    """
    dfs = []
    for up in files:
        try:
            # 1) Leer Excel directamente
            df_excel = pd.read_excel(up)
        except Exception as e:
            st.warning(f"❌ No se pudo leer '{up.name}' como Excel: {e}")
            continue

        # 2) Renombrar columnas clave
        mapeo = {
            "Fecha de Sistema":   "FECHA",
            "Valor Total":         "VALOR",
            "Descripción motivo":  "REFERENCIA"
        }
        df_excel = df_excel.rename(columns=mapeo)

        # 3) Verificar columnas mínimas
        faltantes = [
            c for c in ["FECHA", "VALOR", "REFERENCIA", "Transacción", "Referencia 1"]
            if c not in df_excel.columns
        ]
        if faltantes:
            st.warning(f"⚠️ En '{up.name}' faltan columnas: {faltantes}. Se omitirá este archivo.")
            continue

        # 4) Convertir FECHA a datetime
        df_excel["FECHA"] = pd.to_datetime(
            df_excel["FECHA"],
            format="%d/%m/%Y",
            errors="coerce"
        )
        n_nat = int(df_excel["FECHA"].isna().sum())
        if n_nat > 0:
            st.warning(f"⚠️ En '{up.name}', {n_nat} filas tienen FECHA inválida y serán NaT.")

        # 5) Limpiar VALOR: quitar "$", espacios, separador de miles y coma decimal → punto
        df_excel["VALOR"] = (
            df_excel["VALOR"]
            .astype(str)
            .str.replace("$", "",     regex=False)
            .str.replace(" ", "",     regex=False)
            .str.replace(".", "",     regex=False)
            .str.replace(",", ".",    regex=False)
        )
        df_excel["VALOR"] = pd.to_numeric(df_excel["VALOR"], errors="coerce")
        n_nanval = int(df_excel["VALOR"].isna().sum())
        if n_nanval > 0:
            st.warning(f"⚠️ En '{up.name}', {n_nanval} filas tienen VALOR inválido (NaN).")

        # 6) Ajustar signo: “Nota Débito” → valor negativo
        mask_debito = (
            df_excel["Transacción"]
            .astype(str)
            .str.strip()
            .str.upper() == "NOTA DÉBITO"
        )
        df_excel.loc[mask_debito, "VALOR"] *= -1

        # 7) Construir “Nombre del producto”:
        #    - Si REFERENCIA no está vacía (ni NaN), usarla.
        #    - Si REFERENCIA está vacía/NaN, usar “Referencia 1”.
        def elegir_nombre(row):
            ref = str(row["REFERENCIA"]).strip()
            if ref and ref.upper() != "NAN":
                return ref
            # Si REF vacío o “nan”, caemos en Referencia 1:
            return str(row["Referencia 1"]).strip()

        df_excel["Nombre del producto"] = df_excel.apply(elegir_nombre, axis=1)

        # 8) Crear DataFrame temporal para merge_asof
        df_sel = pd.DataFrame({
            "Fecha": df_excel["FECHA"],
            "ValorCOP": df_excel["VALOR"],
            "Nombre del producto": df_excel["Nombre del producto"],
            "Archivo_Origen": up.name
        }).dropna(subset=["Fecha", "ValorCOP"])

        dfs.append(df_sel)

    # 9) Si no hay nada válido, retornar vacío
    if not dfs:
        return pd.DataFrame()

    # 10) Concatenar transacciones Davivienda limpias
    df_trans = pd.concat(dfs, ignore_index=True)

    # 11) Obtener TRM histórica (misma lógica que Bancolombia)
    try:
        fecha_max_tx = df_trans["Fecha"].max().date()
        punto_corte = fecha_max_tx.strftime("%Y-%m-%dT00:00:00.000")
        url = (
            "https://www.datos.gov.co/resource/mcec-87by.json?"
            f"$where=vigenciadesde <= '{punto_corte}'"
            "&$order=vigenciadesde DESC"
        )
        respuesta = requests.get(url)
        respuesta.raise_for_status()
        lista_trm = respuesta.json()

        if not lista_trm:
            st.warning("No se encontraron registros de TRM anteriores o iguales a la fecha máxima.")
            trm_df = pd.DataFrame(columns=["Fecha", "TRM"])
        else:
            trm_df = pd.DataFrame(lista_trm)
            trm_df["Fecha"] = pd.to_datetime(trm_df["vigenciadesde"], errors="coerce")
            trm_df["TRM"]   = trm_df["valor"].astype(float)
            trm_df = trm_df[["Fecha", "TRM"]]
            trm_df = trm_df.drop_duplicates(subset="Fecha", keep="first")
            trm_df = trm_df.sort_values("Fecha").reset_index(drop=True)
    except Exception as e:
        st.warning(f"No se pudo obtener TRM: {e}")
        trm_df = pd.DataFrame(columns=["Fecha", "TRM"])

    # 12) Merge_asof para asignar TRM a cada transacción
    df_trans = df_trans.sort_values("Fecha").reset_index(drop=True)
    trm_df   = trm_df.sort_values("Fecha").reset_index(drop=True)

    df_merged = pd.merge_asof(
        df_trans,
        trm_df,
        on="Fecha",
        direction="backward"
    )

    # 13) Calcular Monto (en USD)
    df_merged["Monto"] = df_merged["ValorCOP"] / (df_merged["TRM"] + 100)

    # 14) Añadir columnas fijas para homogeneizar con Bancolombia
    df_merged["Tipo"] = "Ingreso"
    df_merged["Orden"] = ""
    df_merged["Usuario"] = usuario
    df_merged["Casillero"] = casillero
    df_merged["Estado de Orden"] = ""

    # 15) DataFrame final con mismas columnas que Bancolombia
    out = df_merged[[
        "Fecha",
        "Tipo",
        "Monto",
        "Orden",
        "Usuario",
        "Casillero",
        "Estado de Orden",
        "Nombre del producto",
        "TRM"
    ]]

    # 16) Filtrar movimientos no deseados: “ABONO INTERESES AHORROS” y montos ≤ 0
    out = out[out["Nombre del producto"] != "ABONO INTERESES AHORROS"]
    out = out[out["Monto"] > 0]

    return out.reset_index(drop=True)





# === Config de cobros mensuales por casillero (fácil de cambiar) ===
COBROS_MENSUALES_CONF = {
    # casillero : {"inicio": "YYYY-MM-01", "monto": int}
    "1633": {"inicio": "2024-02-01", "monto": 711_750}
    #,
   # "1444": {"inicio": "2024-10-01", "monto": 100},
}

def aplicar_cobro_contabilidad_mensual(historico, hoja, casillero, usuario, fecha_carga, inicio_yyyymm, monto, etiqueta_base="cobro contabilidad"):
    """
    Agrega un Egreso mensual fijo con Fecha = último día de cada mes, desde 'inicio_yyyymm'
    hasta el MES ANTERIOR a 'fecha_carga'. Idempotente (no duplica por Orden/Nombre del producto).
    Luego descuenta la suma agregada del último 'Total'.
    """
    import calendar
    from datetime import date

    if hoja not in historico:
        return historico

    dfh = historico[hoja].copy()
    # Normalizaciones
    dfh["Fecha_dt"] = pd.to_datetime(dfh["Fecha"], errors="coerce").dt.date
    dfh["Monto"]    = pd.to_numeric(dfh["Monto"], errors="coerce")

    # Día de ejecución
    fc_date = pd.to_datetime(fecha_carga, errors="coerce").date()
    # Mes anterior al de la ejecución (límites del backfill)
    last_of_prev_month = fc_date.replace(day=1) - timedelta(days=1)
    end_y, end_m = last_of_prev_month.year, last_of_prev_month.month

    # Inicio parametrizable (YYYY-MM-01)
    start_date = pd.to_datetime(inicio_yyyymm, errors="coerce").date()
    start_y, start_m = start_date.year, start_date.month

    # Si el inicio es posterior al fin (todavía no hay nada que cargar), salir
    if (start_y, start_m) > (end_y, end_m):
        dfh = dfh.drop(columns=["Fecha_dt"], errors="ignore")
        historico[hoja] = dfh
        return historico

    meses = {
        1:"enero",2:"febrero",3:"marzo",4:"abril",5:"mayo",6:"junio",
        7:"julio",8:"agosto",9:"septiembre",10:"octubre",11:"noviembre",12:"diciembre"
    }

    agregado_total = 0.0
    y, m = start_y, start_m

    while (y, m) <= (end_y, end_m):
        # último día del mes (y, m)
        last_day = calendar.monthrange(y, m)[1]
        fecha_mes = date(y, m, last_day)
        orden_nombre = f"{etiqueta_base} ({meses[m]} {y})"

        # idempotencia: no agregues si ya existe ese Orden o Nombre del producto
        existe = False
        if "Orden" in dfh.columns:
            existe = existe or dfh["Orden"].astype(str).str.lower().eq(orden_nombre.lower()).any()
        if "Nombre del producto" in dfh.columns:
            existe = existe or dfh["Nombre del producto"].astype(str).str.lower().eq(orden_nombre.lower()).any()

        if not existe:
            nueva = pd.DataFrame([{
                "Fecha": fecha_mes,
                "Tipo": "Egreso",
                "Orden": orden_nombre,
                "Monto": float(monto),
                "Motivo": "contabilidad",
                "TRM": "",
                "Usuario": usuario,
                "Casillero": str(casillero),
                "Estado de Orden": "",
                "Nombre del producto": orden_nombre,
                "Fecha de Carga": fecha_carga
            }])
            dfh = pd.concat([dfh, nueva], ignore_index=True)
            agregado_total += float(monto)

        # siguiente mes
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1

    # Si agregamos algo, ajusta el último Total
    if agregado_total > 0:
        mask_tot = dfh["Tipo"].astype(str).str.upper() == "TOTAL"
        if mask_tot.any():
            ult_fecha_total = dfh.loc[mask_tot, "Fecha_dt"].max()
            mask_ult = mask_tot & (dfh["Fecha_dt"] == ult_fecha_total)
            if mask_ult.any():
                val = pd.to_numeric(dfh.loc[mask_ult, "Monto"], errors="coerce") - agregado_total
                dfh.loc[mask_ult, "Monto"] = val
                dfh.loc[mask_ult, "Orden"] = val.apply(lambda x: "Al día" if x >= 0 else "Alerta")

    dfh = dfh.drop(columns=["Fecha_dt"], errors="ignore")
    historico[hoja] = dfh
    return historico



def send_mail_gmail(subject: str, body: str, to_addrs) -> bool:
    """SMTP Gmail con App Password. Sin adjuntos."""
    try:
        cfg = st.secrets["gmail"]
        sender = cfg["address"]
        app_pw = cfg["app_password"]
        smtp_server = cfg.get("smtp_server", "smtp.gmail.com")
        smtp_port = int(cfg.get("smtp_port", 465))
    except Exception as e:
        st.error("❌ Falta configuración gmail en st.secrets['gmail']: " + str(e))
        return False

    if isinstance(to_addrs, str):
        to_addrs = [to_addrs]

    msg = EmailMessage()
    msg["From"] = sender
    msg["To"] = ", ".join(to_addrs)
    msg["Subject"] = subject
    msg.set_content(body)

    try:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(smtp_server, smtp_port, context=context) as server:
            server.login(sender, app_pw)
            server.send_message(msg)
        return True
    except Exception as e:
        st.error(f"❌ Error enviando email: {e}")
        return False


def obtener_y_enviar_alerta_saldo(historico: dict, casillero: str, fecha_carga: str) -> None:
    """
    Toma el último 'Total' del casillero en 'historico' y envía un correo SOLO
    al destinatario configurado para ese casillero. No usa default_to.
    """
    # 1) hallar la hoja del casillero
    hoja = next((h for h in historico if h.startswith(str(casillero))), None)
    if not hoja:
        return

    dfh = historico[hoja].copy()
    if dfh.empty:
        return

    # 2) filtrar Totales y tomar el más reciente por Fecha
    dfh["Tipo"] = dfh["Tipo"].astype(str)
    df_tot = dfh[dfh["Tipo"].str.upper() == "TOTAL"].copy()
    if df_tot.empty:
        return

    df_tot["Fecha"] = pd.to_datetime(df_tot["Fecha"], errors="coerce")
    df_tot = df_tot.dropna(subset=["Fecha"])
    if df_tot.empty:
        return

    df_tot = df_tot.sort_values("Fecha")
    fila = df_tot.iloc[-1]
    saldo = pd.to_numeric(fila["Monto"], errors="coerce")
    fecha_saldo = fila["Fecha"].date()

    if pd.isna(saldo):
        return

    # 3) destinatario SOLO si está mapeado
    recipients_map = st.secrets.get("gmail", {}).get("recipients", {})
    destino = recipients_map.get(str(casillero))
    if not destino:
        return  # no enviar si no hay correo configurado para ese casillero

    # 4) construir y enviar
    subject = f"[Encargomio] Saldo actual casillero {casillero} - {fecha_carga}"
    body = (
        "Hola,\n\n"
        f"Te informamos que tu saldo actual con Encargomio al {fecha_saldo:%Y-%m-%d} es:\n"
        f"    ${saldo:,.0f}\n\n"
        "Este mensaje es informativo. Si detectas alguna inconsistencia, por favor responde a este correo.\n\n"
        "Saludos,\nEncargomio"
    )

    ok = send_mail_gmail(subject, body, destino)
    if ok:
        st.success(f"📧 Alerta enviada a {destino} (casillero {casillero})")





def main():
    st.title("📊 Conciliaciones Mayoristas")

    # 1) Egresos
    st.header("1) Egresos (Compras)")
    compras = st.file_uploader("Sube archivos de COMPRAS", type=["xls","xlsx"], accept_multiple_files=True)
    egresos = {}
    if compras:
        dfc = pd.concat([pd.read_excel(f) for f in compras], ignore_index=True)
        egresos = procesar_egresos(dfc)
        tabs = st.tabs(list(egresos.keys()))
        for tab, key in zip(tabs, egresos):
            with tab:
                df = egresos[key]
                if df.empty:
                    st.info("Sin egresos")
                else:
                    st.dataframe(df, use_container_width=True)
    else:
        st.info("📂 Aún no subes Compras")

    st.markdown("---")

    # 2) Ingresos Extra
    st.header("2) Ingresos Extra")
    extra = st.file_uploader("Sube archivo de INGRESOS EXTRA", type=["xls","xlsx"])
    ingresos_extra = {}
    if extra:
        hojas = pd.read_excel(extra, sheet_name=None)
        ingresos_extra = procesar_ingresos_extra(hojas)
        tabs2 = st.tabs(list(ingresos_extra.keys()))
        for tab, key in zip(tabs2, ingresos_extra):
            with tab:
                df = ingresos_extra[key]
                if df.empty:
                    st.info("Sin datos")
                else:
                    st.dataframe(df, use_container_width=True)
    else:
        st.info("📂 Aún no subes Ingresos Extra")

    st.markdown("---")

   # 3) Ingresos Nathalia Ospina (CA1633)
    st.header("3) Ingresos Nathalia Ospina (CA1633)")
    nat_files = st.file_uploader(
        "Sube archivos .xls y .csv de Nathalia", 
        type=["xls", "xlsx", "csv"], 
        accept_multiple_files=True
    )
    ingresos_nath = {}
    
    if nat_files:
        # Separar por extensiones
        xls_files = [f for f in nat_files if f.name.lower().endswith((".xls", ".xlsx"))]
        csv_files = [f for f in nat_files if f.name.lower().endswith(".csv")]
    
        dfs = []
        if xls_files:
            df_xls = procesar_ingresos_clientes_xls(xls_files, "Nathalia Ospina", "1633")
            dfs.append(df_xls)
        if csv_files:
            df_csv = procesar_ingresos_clientes_csv(csv_files, "Nathalia Ospina", "1633")
            dfs.append(df_csv)
    
        # Concatenar resultados o crear DataFrame vacío
        if dfs:
            df_nat = pd.concat(dfs, ignore_index=True)
        else:
            df_nat = pd.DataFrame()
    
        ingresos_nath["ingresos_1633"] = df_nat
    
        # Mostrar en la app
        if df_nat.empty:
            st.info("Sin movimientos válidos")
        else:
            st.dataframe(df_nat, use_container_width=True)
    else:
        st.info("📂 No subes archivos de Nathalia")
    
    st.markdown("---")

    # 4) Ingresos Elvis (CA11591)
    st.header("4) Ingresos Elvis (CA11591)")
    elv_files = st.file_uploader(
        "Sube archivos .xls y .csv de Elvis",
        type=["xls", "xlsx", "csv"],
        accept_multiple_files=True
    )
    ingresos_elv = {}
    
    if elv_files:
        # Separar por extensión
        xls_files = [f for f in elv_files if f.name.lower().endswith((".xls", ".xlsx"))]
        csv_files = [f for f in elv_files if f.name.lower().endswith(".csv")]
    
        dfs = []
        if xls_files:
            df_xls = procesar_ingresos_clientes_xls(xls_files, "Elvis", "11591")
            dfs.append(df_xls)
        if csv_files:
            df_csv = procesar_ingresos_clientes_csv(csv_files, "Elvis", "11591")
            dfs.append(df_csv)
    
        # Concatenar resultados o crear DataFrame vacío
        if dfs:
            df_elv = pd.concat(dfs, ignore_index=True)
        else:
            df_elv = pd.DataFrame()
    
        ingresos_elv["ingresos_11591"] = df_elv
    
        # Mostrar en la app
        if df_elv.empty:
            st.info("Sin movimientos válidos")
        else:
            st.dataframe(df_elv, use_container_width=True)
    else:
        st.info("📂 No subes archivos de Elvis")
        
        
        
    st.markdown("---")
        
        # 3) Ingresos Julian Sanchez (CA13608)
    st.header("5) Ingresos Julian Sanchez (CA13608)")
    jul_files = st.file_uploader(
        "Sube archivos .xls y .csv de Julian",
        type=["xls", "xlsx", "csv"],
        accept_multiple_files=True
    )
    ingresos_jul = {}
    
    if jul_files:
        # Separar por extensiones
        xls_files = [f for f in jul_files if f.name.lower().endswith((".xls", ".xlsx"))]
        csv_files = [f for f in jul_files if f.name.lower().endswith(".csv")]
    
        dfs = []
        if xls_files:
            df_xls = procesar_ingresos_clientes_xls(xls_files, "Julian Sanchez", "13608")
            dfs.append(df_xls)
        if csv_files:
            df_csv = procesar_ingresos_clientes_csv(csv_files, "Julian Sanchez", "13608")
            dfs.append(df_csv)
    
        # Concatenar resultados o crear DataFrame vacío
        if dfs:
            df_jul = pd.concat(dfs, ignore_index=True)
        else:
            df_jul = pd.DataFrame()
    
        ingresos_jul["ingresos_13608"] = df_jul
    
        # Mostrar en la app
        if df_jul.empty:
            st.info("Sin movimientos válidos")
        else:
            st.dataframe(df_jul, use_container_width=True)
    else:
        st.info("📂 No subes archivos de Julian")
        
    st.markdown("---")
    
    st.header("6) Ingresos Maria Moises (CA1444)")
    moises_files = st.file_uploader(
        "Sube archivos .csv de Maria Moises (Bancolombia o Davivienda)", 
        type=["xls", "xlsx", "csv"], 
        accept_multiple_files=True
    )
    

    
    ingresos_moises = {}

    if moises_files:
        csv_banco   = []
        excel_davi  = []  # antes se llamaba csv_davi

        for file in moises_files:
          nombre = file.name.lower()
          # Si es un CSV lo metemos en Bancolombia:
          if nombre.endswith(".csv"):
              csv_banco.append(file)

          # Si es un Excel (.xlsx o .xls), lo metemos en Davivienda:
          elif nombre.endswith(".xlsx") or nombre.endswith(".xls"):
              excel_davi.append(file)

          else:
              st.warning(f"⚠️ '{file.name}' no es ni CSV ni Excel válido, se omitirá.")

        dfs = []
        if csv_banco:
          df_banco = procesar_ingresos_clientes_csv_casillero1444(
              csv_banco, "Maria Moises", "1444"
          )
          dfs.append(df_banco)

        if excel_davi:
          # Llamamos ahora a la función procesar_ingresos_davivienda para los Excel
          df_davi = procesar_ingresos_davivienda(
              excel_davi, "Maria Moises", "1444"
          )
          dfs.append(df_davi)

        if dfs:
          df_moises = pd.concat(dfs, ignore_index=True)
        else:
          df_moises = pd.DataFrame()

        ingresos_moises["ingresos_1444"] = df_moises

        if df_moises.empty:
          st.info("Sin movimientos válidos")
        else:
          st.dataframe(df_moises, use_container_width=True)
    else:
      st.info("📂 No subes archivos de Maria Moises")

    st.markdown("---")

    
    
    
    
    # 5) Conciliaciones
    # 5) Conciliaciones Finales
    st.header("7) Conciliaciones Finales")
    
    casilleros = ["9444", "14856", "11591", "1444", "1633", "13608"]
    conciliaciones = {}
    
    for cas in casilleros:
        key_ing = f"ingresos_{cas}"
    
        # 2) Obtener cada fuente de ingresos en orden de prioridad
        ing_j = ingresos_jul.get(key_ing)
        ing_n = ingresos_nath.get(key_ing)
        ing_e = ingresos_elv.get(key_ing)
        ing_m = ingresos_moises.get(key_ing)
    
        if ing_j is not None and not ing_j.empty:
            inc = ing_j
        elif ing_n is not None and not ing_n.empty:
            inc = ing_n
        elif ing_e is not None and not ing_e.empty:
            inc = ing_e
        elif ing_m is not None and not ing_m.empty:
            inc = ing_m
        else:
            inc = None
    
        # EGRESOS
        egr = egresos.get(f"egresos_{cas}")
    
        # EXTRA (ingresos extra)
        ext = ingresos_extra.get(f"extra_{cas}")
    
        # 3) Armar la lista de DataFrames válidos
        frames = []
        for df in (inc, egr, ext):
            if df is not None and not df.empty:
                frames.append(df)
    
        # 4) Guardar la conciliación (si no hay nada, vacío)
        if frames:
            conciliaciones[f"conciliacion_{cas}"] = pd.concat(frames, ignore_index=True)
        else:
            conciliaciones[f"conciliacion_{cas}"] = pd.DataFrame()
    
    # 5) Mostrar en pestañas
    tabs5 = st.tabs(list(conciliaciones.keys()))
    for tab, key in zip(tabs5, conciliaciones.keys()):
        with tab:
            dfc = conciliaciones[key]
            if dfc.empty:
                st.info("⛔ Sin movimientos para este casillero")
            else:
                st.dataframe(dfc, use_container_width=True)

    st.markdown("---")



    



    st.markdown("---")

    # 6) Histórico: carga y actualización
    # 6) Histórico: carga y actualización
    st.header("8) Actualizar Histórico") 
    hist_file = st.file_uploader("Sube tu archivo HISTÓRICO EXISTENTE", type=["xls","xlsx"])
    if hist_file:
        historico = pd.read_excel(hist_file, sheet_name=None)
        fecha_carga = pd.Timestamp.today().strftime("%Y-%m-%d")
    
        # actualizar cada conciliación
        for clave, df_nuevo in conciliaciones.items():
            cas = clave.replace("conciliacion_", "")
            dfn = df_nuevo.copy()
            dfn["Fecha de Carga"] = fecha_carga
            if dfn.empty:
                continue
    
            usuario = dfn["Usuario"].iloc[0]
            cnum    = dfn["Casillero"].iloc[0]
    
            # 1) Detectar hoja histórica existente
            hoja = next((h for h in historico if h.startswith(cas)), None)
            if hoja:
                hist_df  = historico[hoja].copy()
                combinado = pd.concat([hist_df, dfn], ignore_index=True)
            else:
                hist_df  = pd.DataFrame()
                combinado = dfn
                hoja = f"{cas} - sin_nombre"
    
            # 2) Dedups y limpiezas
            combinado["Orden"] = combinado["Orden"].astype(str)
    
            # eliminar duplicados egresos
            mask_e = combinado["Tipo"] == "Egreso"
            egrs   = combinado[mask_e].drop_duplicates(subset=["Orden", "Tipo"])
            otros  = combinado[~mask_e]
            combinado = pd.concat([otros, egrs], ignore_index=True)
    
            # normalizar Monto
            combinado["Monto"] = pd.to_numeric(combinado["Monto"], errors="coerce")
    
            # eliminar duplicados extra (si aplica)
            if "Motivo" in combinado.columns:
                mask_x = combinado["Motivo"] == "Ingreso_extra"
                iex    = combinado[mask_x].drop_duplicates(subset=["Orden", "Motivo"])
                ot     = combinado[~mask_x]
                combinado = pd.concat([ot, iex], ignore_index=True)
    
            # completar ingresos nulos desde egresos por Orden (cuando aplique)
            mask_n = (combinado["Tipo"] == "Ingreso") & combinado["Monto"].isna()
            for i, row in combinado[mask_n].iterrows():
                o = row["Orden"]
                match = combinado[(combinado["Tipo"] == "Egreso") & (combinado["Orden"] == o)]
                if not match.empty:
                    combinado.at[i, "Monto"] = match["Monto"].iloc[0]
    
            # ==================== Totales acumulados diarios (global) ====================
            # A) Fecha para comenzar a ACTUALIZAR totales: última 'Fecha de Carga' ya guardada
            if not hist_df.empty and "Fecha de Carga" in hist_df.columns:
                ult_fc_series = pd.to_datetime(hist_df["Fecha de Carga"], errors="coerce").dt.date.dropna()
                start_update_date = ult_fc_series.max() if not ult_fc_series.empty else None
            else:
                start_update_date = None
    
            # B) Preparar transacciones (excluir filas 'Total' para el cálculo)
            combinado["Fecha"] = pd.to_datetime(combinado["Fecha"], errors="coerce").dt.date
            combinado_tx = combinado[combinado["Tipo"].astype(str).str.upper() != "TOTAL"].copy()
            combinado_tx = combinado_tx.dropna(subset=["Fecha"])
    
            # Si no hay fechas válidas, guarda tal cual y sigue
            if combinado_tx.empty:
                historico[hoja] = combinado
                continue
    
            # C) Rango completo global (desde fecha mínima de transacción hasta hoy)
            min_date = combinado_tx["Fecha"].min()
            today    = pd.Timestamp.today().date()
            days_full = pd.date_range(start=min_date, end=today, freq="D").date
    
            # D) Agregados por día sobre TODO el histórico (global)
            ingresos_d = (combinado_tx[combinado_tx["Tipo"] == "Ingreso"]
                          .groupby("Fecha")["Monto"].sum())
            egresos_d  = (combinado_tx[combinado_tx["Tipo"] == "Egreso"]
                          .groupby("Fecha")["Monto"].sum())
    
            ingresos_full = pd.Series(index=days_full, data=[ingresos_d.get(d, 0.0) for d in days_full])
            egresos_full  = pd.Series(index=days_full, data=[egresos_d.get(d, 0.0) for d in days_full])
    
            # E) Saldo ACUMULADO GLOBAL (hasta cada día)
            saldo_acum_full = ingresos_full.cumsum() - egresos_full.cumsum()
    
            # F) Determinar el rango a ACTUALIZAR:
            if start_update_date is None:
                update_days = days_full
            else:
                update_days = [d for d in days_full if d >= start_update_date]
    
            # G) Eliminar 'Total' existentes SOLO en el rango de actualización para no duplicar
            es_total = combinado["Tipo"].astype(str).str.upper() == "TOTAL"
            en_rango = combinado["Fecha"].isin(update_days)
            combinado_sin_totales_rango = combinado[~(es_total & en_rango)].copy()
    
            # H) Construir nuevas filas 'Total' usando el ACUMULADO GLOBAL para ese día
            tot_rows = pd.DataFrame({
                "Fecha": list(update_days),
                "Tipo": "Total",
                "Monto": [float(saldo_acum_full.get(d, 0.0)) for d in update_days],
                "Orden": ["Al día" if float(saldo_acum_full.get(d, 0.0)) >= 0 else "Alerta" for d in update_days],
                "TRM": "",
                "Usuario": usuario,
                "Casillero": cnum,
                "Fecha de Carga": fecha_carga,  # marca de recálculo
                "Estado de Orden": "",
                "Nombre del producto": ""
            })
    
            # I) Reconstruir la hoja
            historico[hoja] = pd.concat([combinado_sin_totales_rango, tot_rows], ignore_index=True)
            # ================== /Totales acumulados diarios (global) =====================
    
            # ---------- COMISIÓN QUINCENAL POR TOTALES (SOLO CA1444) ----------
            if cas == "1444":
                import calendar
    
                dfh = historico[hoja].copy()
                # Normaliza
                dfh["Fecha_dt"] = pd.to_datetime(dfh["Fecha"], errors="coerce").dt.date
                dfh["Monto"] = pd.to_numeric(dfh["Monto"], errors="coerce")
    
                fc_date = pd.to_datetime(fecha_carga, errors="coerce").date()
                y, m, d = fc_date.year, fc_date.month, fc_date.day
    
                meses = {
                    1:"enero",2:"febrero",3:"marzo",4:"abril",5:"mayo",6:"junio",
                    7:"julio",8:"agosto",9:"septiembre",10:"octubre",11:"noviembre",12:"diciembre"
                }
    
                def agregar_comision_rango(ini_date, fin_date, etiqueta):
                    orden_nombre = f"Comision de ({etiqueta})"
    
                    # NO duplicar si ya existe ese Orden/Nombre
                    existe = False
                    if "Orden" in dfh.columns:
                        existe = existe or dfh["Orden"].astype(str).str.lower().eq(orden_nombre.lower()).any()
                    if "Nombre del producto" in dfh.columns:
                        existe = existe or dfh["Nombre del producto"].astype(str).str.lower().eq(orden_nombre.lower()).any()
                    if existe:
                        return 0.0, dfh
    
                    # Solo Totales en el ciclo
                    mask = (dfh["Tipo"].astype(str).str.upper() == "TOTAL") & \
                           (dfh["Fecha_dt"] >= ini_date) & (dfh["Fecha_dt"] <= fin_date)
                    serie = pd.to_numeric(dfh.loc[mask, "Monto"], errors="coerce")
                    serie = serie[serie < 0]
                    if serie.empty:
                        return 0.0, dfh
    
                    comision = float(abs(serie.min()) * 0.015)  # 1.5%
    
                    nueva = pd.DataFrame([{
                        "Fecha": fc_date,
                        "Tipo": "Egreso",
                        "Orden": orden_nombre,
                        "Monto": comision,
                        "Motivo": "comision",
                        "TRM": "",
                        "Usuario": "Maria Moises",
                        "Casillero": "1444",
                        "Estado de Orden": "",
                        "Nombre del producto": orden_nombre,
                        "Fecha de Carga": fecha_carga
                    }])
    
                    return comision, pd.concat([dfh, nueva], ignore_index=True)
    
                total_comision_hoy = 0.0
    
                # Día 1..15 → H2 del mes anterior (16–fin)
                if 1 <= d <= 15:
                    prev_y  = y if m > 1 else y - 1
                    prev_m  = m - 1 if m > 1 else 12
                    last_prev = calendar.monthrange(prev_y, prev_m)[1]
                    ini = pd.Timestamp(prev_y, prev_m, 16).date()
                    fin = pd.Timestamp(prev_y, prev_m, last_prev).date()
                    etiqueta = f"16-fin {meses[prev_m]} {prev_y}"
                    comi, dfh = agregar_comision_rango(ini, fin, etiqueta)
                    total_comision_hoy += comi
    
                # Día ≥16 → H1 del mes actual (1–15)
                if d >= 16:
                    ini = pd.Timestamp(y, m, 1).date()
                    fin = pd.Timestamp(y, m, 15).date()
                    etiqueta = f"1-15 {meses[m]} {y}"
                    comi, dfh = agregar_comision_rango(ini, fin, etiqueta)
                    total_comision_hoy += comi
    
                # Restar al último Total si hubo comisión hoy
                if total_comision_hoy > 0:
                    mask_tot = dfh["Tipo"].astype(str).str.upper() == "TOTAL"
                    if mask_tot.any():
                        ult_fecha_total = dfh.loc[mask_tot, "Fecha_dt"].max()
                        mask_ult = mask_tot & (dfh["Fecha_dt"] == ult_fecha_total)
                        if mask_ult.any():
                            val = pd.to_numeric(dfh.loc[mask_ult, "Monto"], errors="coerce") - total_comision_hoy
                            dfh.loc[mask_ult, "Monto"] = val
                            dfh.loc[mask_ult, "Orden"] = val.apply(lambda x: "Al día" if x >= 0 else "Alerta")
    
                dfh = dfh.drop(columns=["Fecha_dt"], errors="ignore")
                historico[hoja] = dfh
            # ---------- /COMISIÓN QUINCENAL ----------
            # ---- Cobros mensuales de contabilidad (parametrizados por casillero) ----
            if cas in COBROS_MENSUALES_CONF:
                cfg = COBROS_MENSUALES_CONF[cas]
                historico = aplicar_cobro_contabilidad_mensual(
                    historico, hoja, cas, usuario, fecha_carga,
                    inicio_yyyymm=cfg["inicio"], monto=cfg["monto"], etiqueta_base="cobro contabilidad"
                )
            # -------------------------------------------------------------------------


        # generar excel en memoria
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            for h, dfh in historico.items():
                w.book.create_sheet(h[:31])
                dfh.to_excel(w, sheet_name=h[:31], index=False)
        buf.seek(0)
        data_bytes = buf.read()
        
        # ⬅️ Envía correos por casillero (solo a los configurados)
        # 👉 envío de alerta SOLO para este casillero (sin adjuntos)
        for cas in st.secrets.get("gmail", {}).get("recipients", {}).keys():
            obtener_y_enviar_alerta_saldo(historico, str(cas), fecha_carga)

        # 1) Botón de descarga local
        st.download_button(
            "⬇️ Descargar Histórico Actualizado",
            data=data_bytes,
            file_name=f"{pd.Timestamp.today().strftime('%Y%m%d')}_Historico_mayoristas.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    
        # 2) Subida automática a Dropbox (DESACTIVADA mientras probamos)
        upload_to_dropbox(data_bytes)
    else:
        st.info("📂 Aún no subes tu histórico")


    st.caption("Desarrollado con ❤️ y Streamlit")

if __name__=="__main__":
    main()
