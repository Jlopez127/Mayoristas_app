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

st.set_page_config(page_title="Conciliaciones Mayoristas", layout="wide")

# — 1) Egresos (Compras) —
@st.cache_data
def procesar_egresos(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    casilleros = ["9444", "14856", "11591", "1444", "1633"]
    df = df.copy()
    df["Fecha Compra"] = pd.to_datetime(df["Fecha Compra"], errors="coerce", utc=True)
    df["Fecha Compra"] = df["Fecha Compra"].dt.tz_convert(None).dt.strftime("%Y-%m-%d")
    df["Casillero"] = df["Casillero"].astype(str)
    df = df[df["Casillero"].isin(casilleros)]
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
        buf = io.StringIO(contenido.decode("utf-8"))
        df = pd.read_csv(buf, header=None, sep=",", encoding="utf-8")
        if df.shape[1] != 9:
            import streamlit as st
            st.warning(f"⚠️ '{up.name}' tiene {df.shape[1]} columnas (esperaba 9). Se omite.")
            continue
        df.columns = [
            "DESCRIPCIÓN", "DESCONOCIDA_1", "DESCONOCIDA_2", "FECHA",
            "DESCONOCIDA_3", "VALOR", "DESCONOCIDA_4", "REFERENCIA", "DESCONOCIDA_5"
        ]
        df["Archivo_Origen"] = up.name
        dfs.append(df)
    if not dfs:
       return pd.DataFrame()
    df = pd.concat(dfs, ignore_index=True)
    df["REFERENCIA"] = df["REFERENCIA"].fillna(df.get("DESCRIPCIÓN",""))
    df = df.dropna(how="all", axis=1)
    # Aseguramos que sea string de 8 dígitos y parseamos como YYYYMMDD
    df["FECHA"] = (
       df["FECHA"]
       .astype(str)
       .str.zfill(8)              # asegurar 8 dígitos
       .pipe(pd.to_datetime,     # parsear
             format="%d%m%Y",     # <– ahora día-mes-año
             errors="coerce")
       )        
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
# 3) Pipeline común (extrae tu lógica de post-procesamiento)


def procesar_ingresos_clientes_csv_casillero1444(files: list[bytes], usuario: str, casillero: str) -> pd.DataFrame:
    dfs = []
    for up in files:
        # Leemos el contenido en bytes
        contenido = up.read() if hasattr(up, "read") else up

        # Intentamos primero decodificar en UTF-8; si da UnicodeDecodeError, usamos Latin-1
        try:
            texto = contenido.decode("utf-8")
        except UnicodeDecodeError:
            texto = contenido.decode("latin-1")

        # Convertimos ese texto a StringIO para que pandas lo lea sin problemas
        buf = io.StringIO(texto)

        # Ahora pd.read_csv funcionará porque 'texto' es un string válido.
        df = pd.read_csv(buf, header=None, sep=",", encoding="latin-1")
        if df.shape[1] != 9:
            st.warning(f"⚠️ '{up.name}' tiene {df.shape[1]} columnas (esperaba 9). Se omite.")
            continue

        df.columns = [
            "DESCRIPCIÓN", "DESCONOCIDA_1", "DESCONOCIDA_2", "FECHA",
            "DESCONOCIDA_3", "VALOR", "DESCONOCIDA_4", "REFERENCIA", "DESCONOCIDA_5"
        ]
        df["Archivo_Origen"] = up.name
        dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    # Concatenamos todos los DataFrames válidos
    df = pd.concat(dfs, ignore_index=True)

    # Usar DESCRIPCIÓN como respaldo de REFERENCIA
    df["REFERENCIA"] = df["REFERENCIA"].fillna(df.get("DESCRIPCIÓN", ""))
    df = df.dropna(how="all", axis=1)

    # Convertir FECHA a datetime
    df["FECHA"] = (
        df["FECHA"]
        .astype(str)
        .str.zfill(8)
        .pipe(pd.to_datetime, format="%d%m%Y", errors="coerce")
    )

    # Convertir VALOR a float
    df["VALOR"] = df["VALOR"].astype(str).str.replace(",", "").astype(float)

    # Obtener TRM por fecha del rango
    try:
       # 1) Definimos fecha_max en base a las transacciones:
       fecha_max_dt = df["FECHA"].max().date()
       
       # 2) Pedimos todas las TRM publicadas hasta esa fecha máxima:
       punto_corte = fecha_max_dt.strftime("%Y-%m-%dT00:00:00.000")
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
           df_trm = pd.DataFrame(columns=["Fecha", "TRM"])
       else:
           trm_df = pd.DataFrame(lista_trm)
           if "vigenciadesde" not in trm_df.columns or "valor" not in trm_df.columns:
               st.warning("La API de TRM no devolvió los campos esperados.")
               df_trm = pd.DataFrame(columns=["Fecha", "TRM"])
           else:
               # Convertimos vigenciadesde a date y el valor a float
               trm_df["vigenciadesde"] = pd.to_datetime(trm_df["vigenciadesde"]).dt.date
               trm_df["valor"] = trm_df["valor"].astype(float)
               trm_df = trm_df.rename(columns={"vigenciadesde": "Fecha", "valor": "TRM"})
               
               # Dejamos sólo la primera fila de cada fecha (en caso de duplicados)
               trm_df = trm_df.drop_duplicates(subset="Fecha", keep="first")
               
               # Ordenamos de forma descendente por Fecha
               trm_df = trm_df.sort_values("Fecha", ascending=False).set_index("Fecha")
   
               # 3) AHORA tomamos la fecha mínima de TRM (p.ej. 2025-05-23):
               fecha_min_trm = trm_df.index.min()  # ej. date(2025,5,23)

               # 4) Y la fecha mínima de transacción:
               fecha_min_tx = df["FECHA"].dt.date.min()  # ej. date(2025,5,25)

               # 5) Queremos que nuestro rango empiece en el menor de ambos:
               #    (si fecha_min_trm < fecha_min_tx, arranquemos desde la TRM más antigua)
               fecha_inicio_rango = min(fecha_min_trm, fecha_min_tx)
               fecha_fin_rango    = fecha_max_dt

               rango_fechas = pd.date_range(
                   start = fecha_inicio_rango,
                   end   = fecha_fin_rango,
                   freq  = "D"
               ).date

               # 6) Creamos un DataFrame vacío para todo ese rango:
               df_trm_full = pd.DataFrame(index=rango_fechas, columns=["TRM"])

               # 7) Colocamos en cada día la TRM que exista:
               for dia in trm_df.index:
                   df_trm_full.loc[dia, "TRM"] = trm_df.loc[dia, "TRM"]

               # 8) Hacemos ffill para copiar la última TRM hacia adelante
               df_trm_full["TRM"] = df_trm_full["TRM"].ffill()

               # 9) Preparamos el DataFrame para el merge final:
               df_trm_para_joins = (
                   df_trm_full
                   .reset_index()
                   .rename(columns={"index": "Fecha"})
               )
    except Exception as e:
       st.warning(f"No se pudo obtener TRM: {e}")
       df_trm_para_joins = pd.DataFrame(columns=["Fecha", "TRM"])

    # Preparar columna Fecha para merge
    df["Fecha"] = df["FECHA"].dt.date

    # Merge con TRM
    df = df.merge(df_trm_para_joins, on="Fecha", how="left")

    # Calcular VALOR en COP
    df["VALOR_USD"] = df["VALOR"] / (df["TRM"] + 100)

    # Asignar columnas estándar
    df["Tipo"] = "Ingreso"
    df["Orden"] = ""
    df["Usuario"] = usuario
    df["Casillero"] = casillero
    df["Estado de Orden"] = ""

    # Construcción final del DataFrame
    out = df.rename(columns={
        "VALOR_USD": "Monto",
        "REFERENCIA": "Nombre del producto"
    })[["Fecha", "Tipo", "Monto", "Orden", "Usuario", "Casillero", "Estado de Orden", "Nombre del producto", "TRM"]]

    # Filtrar datos válidos
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
    
    st.header("5) Ingresos Maria Moises (CA1444)")
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
    st.header("6) Conciliaciones Finales")
    
    casilleros = ["9444", "14856", "11591", "1444", "1633"]
    conciliaciones = {}
    
    for cas in casilleros:
        key_ing = f"ingresos_{cas}"
    
        # 2) Obtener cada fuente de ingresos en orden de prioridad
        ing_n = ingresos_nath.get(key_ing)
        ing_e = ingresos_elv.get(key_ing)
        ing_m = ingresos_moises.get(key_ing)
    
        if ing_n is not None and not ing_n.empty:
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
    st.header("7) Actualizar Histórico")
    hist_file = st.file_uploader("Sube tu archivo HISTÓRICO EXISTENTE", type=["xls","xlsx"])
    if hist_file:
        historico = pd.read_excel(hist_file, sheet_name=None)
        fecha_carga = pd.Timestamp.today().strftime("%Y-%m-%d")
        # actualizar cada conciliación
        for clave, df_nuevo in conciliaciones.items():
            cas = clave.replace("conciliacion_","")
            dfn = df_nuevo.copy()
            dfn["Fecha de Carga"] = fecha_carga
            if dfn.empty: continue
            usuario = dfn["Usuario"].iloc[0]
            cnum = dfn["Casillero"].iloc[0]
            hoja = next((h for h in historico if h.startswith(cas)), None)
            if hoja:
                combinado = pd.concat([historico[hoja], dfn], ignore_index=True)
            else:
                combinado = dfn
                hoja = f"{cas} - sin_nombre"
            # eliminar duplicados egresos
            mask_e = combinado["Tipo"]=="Egreso"
            egrs = combinado[mask_e].drop_duplicates(subset=["Orden","Tipo"])
            otros = combinado[~mask_e]
            combinado = pd.concat([otros,egrs], ignore_index=True)
            combinado["Monto"] = pd.to_numeric(combinado["Monto"], errors="coerce")
            # eliminar duplicados extra
            if "Motivo" in combinado.columns:
                mask_x = combinado["Motivo"]=="Ingreso_extra"
                iex = combinado[mask_x].drop_duplicates(subset=["Orden","Motivo"])
                ot = combinado[~mask_x]
                combinado = pd.concat([ot,iex], ignore_index=True)
            # completar ingresos nulos
            mask_n = (combinado["Tipo"]=="Ingreso") & combinado["Monto"].isna()
            for i,row in combinado[mask_n].iterrows():
                o = row["Orden"]
                match = combinado[(combinado["Tipo"]=="Egreso") & (combinado["Orden"]==o)]
                if not match.empty:
                    combinado.at[i,"Monto"] = match["Monto"].iloc[0]
            # balance y fila total
            tot_i = combinado[combinado["Tipo"]=="Ingreso"]["Monto"].sum()
            tot_e = combinado[combinado["Tipo"]=="Egreso"]["Monto"].sum()
            bal = tot_i - tot_e
            estado = "Al día" if bal>=0 else "Alerta"
            fila = pd.DataFrame([{
                "Fecha": fecha_carga,
                "Tipo":"Total",
                "Monto":bal,
                "Orden":estado,
                "TRM":"",
                "Usuario":usuario,
                "Casillero":cnum,
                "Fecha de Carga":fecha_carga,
                "Estado de Orden":"",
                "Nombre del producto":""
            }])
            historico[hoja] = pd.concat([combinado,fila], ignore_index=True)
        # generar excel en memoria
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            for h,dfh in historico.items():
                w.book.create_sheet(h[:31])
                dfh.to_excel(w, sheet_name=h[:31], index=False)
        buf.seek(0)
        st.download_button(
            "⬇️ Descargar Histórico Actualizado",
            data=buf,
            file_name=f"{pd.Timestamp.today().strftime('%Y%m%d')}_Historico_mayoristas.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.info("📂 Aún no subes tu histórico")

    st.caption("Desarrollado con ❤️ y Streamlit")

if __name__=="__main__":
    main()
