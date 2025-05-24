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
import xlrd

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
    df = df.rename(columns={
        "Fecha Compra": "Fecha",
        "Valor de compra COP": "Monto"
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
    
    # 5) Conciliaciones
    # 5) Conciliaciones Finales
    st.header("5) Conciliaciones Finales")
    
    casilleros = ["9444", "14856", "11591", "1444", "1633"]
    conciliaciones = {}
    
    for cas in casilleros:
            # --- INGRESOS: primero de Nathalia, si existe y no está vacío ---
            key_ing = f"ingresos_{cas}"
            ing_n = ingresos_nath.get(key_ing)
            ing_e = ingresos_elv.get(key_ing)
        
            if ing_n is not None and not ing_n.empty:
                inc = ing_n
            elif ing_e is not None and not ing_e.empty:
                inc = ing_e
            else:
                inc = None
        
            # --- EGRESOS ---
            egr = egresos.get(f"egresos_{cas}")
        
            # --- EXTRA (usa la misma clave que definiste en tu dict de ingresos extra) ---
            ext = ingresos_extra.get(f"extra_{cas}")
        
            # --- Armar lista de DataFrames válidos ---
            frames = []
            for df in (inc, egr, ext):
                if df is not None and not df.empty:
                    frames.append(df)
        
            # --- Guardar conciliación (aunque sea vacía) ---
            if frames:
                conciliaciones[f"conciliacion_{cas}"] = pd.concat(frames, ignore_index=True)
            else:
                conciliaciones[f"conciliacion_{cas}"] = pd.DataFrame()
        
        # --- Mostrar resultados en pestañas ---
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
    st.header("6) Actualizar Histórico")
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
            file_name="Historico_movimientos_actualizado.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.info("📂 Aún no subes tu histórico")

    st.caption("Desarrollado con ❤️ y Streamlit")

if __name__=="__main__":
    main()
