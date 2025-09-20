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
    cutoff = pd.Timestamp("2025-09-18")
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



@st.cache_data

def procesar_devoluciones(hojas: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """
    Lee un Excel con múltiples hojas (una por casillero).
    Espera la estructura:
    Tipo, Fecha, Orden, Monto, Usuario, Casillero, Motivo, Nombre del producto
    (Tipo='Ingreso' y Motivo='Devolucion' pueden venir o se completan).
    """
    resultado = {}
    for hoja, df in (hojas or {}).items():
        cas = hoja.split("-")[0].strip()
        if not cas.isdigit():
            continue

        df2 = df.copy()
        # 1) Limpia posibles espacios y normaliza headers
        df2.columns = [str(c).strip() for c in df2.columns]

        # 2) Validaciones mínimas
        if "Fecha" not in df2.columns:
            st.warning(f"Hoja '{hoja}': falta columna 'Fecha'. Se omite.")
            continue
        if "Orden" not in df2.columns:
            st.warning(f"Hoja '{hoja}': falta columna 'Orden'. Se omite.")
            continue
        if "Monto" not in df2.columns:
            st.warning(f"Hoja '{hoja}': falta columna 'Monto'. Se omite.")
            continue

        # 3) Normalizaciones de tipo
        df2["Fecha"] = pd.to_datetime(df2["Fecha"], errors="coerce").dt.date
        df2["Orden"] = df2["Orden"].astype("string").str.strip()   # conserva ceros a la izquierda
        df2["Monto"] = pd.to_numeric(df2["Monto"], errors="coerce")

        # Opcionales / defaults
        if "Usuario" not in df2.columns:
            df2["Usuario"] = ""
        else:
            df2["Usuario"] = df2["Usuario"].astype(str).str.strip()

        # Casillero: si no viene en el archivo, usamos el de la hoja
        if "Casillero" not in df2.columns:
            df2["Casillero"] = str(cas)
        else:
            df2["Casillero"] = df2["Casillero"].astype(str).str.strip()

        # Motivo (marcador para validación)
        if "Motivo" not in df2.columns:
            df2["Motivo"] = "Devolucion"
        else:
            df2["Motivo"] = df2["Motivo"].astype(str).str.strip()
            df2.loc[df2["Motivo"] == "", "Motivo"] = "Devolucion"

        # Nombre del producto
        if "Nombre del producto" not in df2.columns:
            df2["Nombre del producto"] = "Devolución"
        else:
            df2["Nombre del producto"] = df2["Nombre del producto"].astype(str).str.strip()

        # Tipo (siempre Ingreso para devoluciones)
        if "Tipo" not in df2.columns:
            df2["Tipo"] = "Ingreso"
        else:
            df2["Tipo"] = df2["Tipo"].astype(str).str.strip()
            df2.loc[df2["Tipo"] == "", "Tipo"] = "Ingreso"

        # 4) Filtra filas válidas
        df2 = df2.dropna(subset=["Fecha", "Orden", "Monto"])

        # 5) Salida EXACTA en el orden requerido (sin TRM)
        out = df2[[
            "Tipo",
            "Fecha",
            "Orden",
            "Monto",
            "Usuario",
            "Casillero",
            "Motivo",
            "Nombre del producto",
        ]].copy()

        resultado[f"devoluciones_{cas}"] = out.reset_index(drop=True)

    return resultado











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
    """
    1444 (María Moises)
    - Lee CSVs bancarios.
    - (Opcional) Netea egresos extra en COP por fecha (cargados por el usuario).
    - Aplica GMF 4x1000 diario (0.4%) sobre los COP del día ANTES de convertir a USD.
    - AVISOS:
        * Total: 'Saldo disponible en COP (esta carga): $X'
        * Detalle diario: '📅 <día> de <mes>: $Y COP disponibles' (si hay varios días).
    - Convierte SOLO el neto a USD con TRM diaria (TRM+100).
    - Si falta COP para cubrir egresos extra del día, muestra advertencia.
    - Guarda un log paralelo en st.session_state["1444_movimientos_cop"].
    """
    import io
    import pandas as pd
    import requests

    # ---- Streamlit (stub si no hay) ----
    try:
        import streamlit as st
    except Exception:
        class _Stub:
            def warning(self, *a, **k): pass
            def info(self, *a, **k): pass
            def checkbox(self, *a, **k): return False
            def radio(self, *a, **k): return "Escribir manualmente"
            def file_uploader(self, *a, **k): return None
            def data_editor(self, *a, **k): return pd.DataFrame()
            def expander(self, *a, **k):
                class _E:
                    def __enter__(selfi): return selfi
                    def __exit__(selfi, *x): return False
                return _E()
            def dataframe(self, *a, **k): pass
            def caption(self, *a, **k): pass
        st = _Stub()

    # ------------------ Lectura robusta CSV (Bancolombia) ------------------
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
            st.warning(f"⚠️ No se pudo decodificar '{fname}' (utf-8/utf-8-sig/latin-1/cp1252). Se omite.")
            continue

        buf = io.StringIO(texto)
        df = pd.read_csv(buf, header=None, sep=",")

        if df.shape[1] == 9:
            df["DESCONOCIDA_6"] = None
        elif df.shape[1] != 10:
            st.warning(f"⚠️ '{fname}' tiene {df.shape[1]} columnas (esperaba 9 o 10). Se omite.")
            continue

        df.columns = [
            "DESCRIPCIÓN", "DESCONOCIDA_1", "DESCONOCIDA_2", "FECHA",
            "DESCONOCIDA_3", "VALOR", "DESCONOCIDA_4", "REFERENCIA",
            "DESCONOCIDA_5", "DESCONOCIDA_6"
        ]
        df["Archivo_Origen"] = fname
        dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)
    df["REFERENCIA"] = df["REFERENCIA"].fillna(df.get("DESCRIPCIÓN", ""))
    df = df.dropna(how="all", axis=1)

    # ---- Fecha ----
    fechas_raw = (
        df["FECHA"].astype(str).str.strip()
        .str.replace(r"[^\d]", "", regex=True)
        .str.zfill(8)
    )
    f1 = pd.to_datetime(fechas_raw, format="%Y%m%d", errors="coerce")
    f2 = pd.to_datetime(fechas_raw, format="%d%m%Y", errors="coerce")
    df["FECHA"] = f1.fillna(f2)

    # ---- Valor COP ----
    df["VALOR"] = (
        df["VALOR"].astype(str)
        .str.replace(",", "", regex=False)
        .str.strip()
        .replace({"": None})
        .astype(float)
    )
    # ===== Filtro INICIAL (ANTES de netear/GMF) =====
    mask_interes = df["REFERENCIA"].astype(str).str.strip().str.upper().eq("ABONO INTERESES AHORROS")
    mask_pos     = pd.to_numeric(df["VALOR"], errors="coerce").fillna(0) > 0
    df_base = df.loc[~mask_interes & mask_pos].copy()

    # ========= Egresos extra (opcionales) =========
    egresos_extra = pd.DataFrame(columns=["Fecha", "Monto_COP", "Descripcion"])
    usar_extras = False
    try:
        usar_extras = st.checkbox("¿Tienes egresos extra en COP para netear (1444)?", value=False, key="eg_extra_1444_toggle")
    except Exception:
        pass

    if usar_extras:
        modo = st.radio("Cómo cargar egresos extra", ["Escribir manualmente", "Subir CSV"], index=0, horizontal=True, key="eg_extra_1444_modo")
        if modo == "Subir CSV":
            up_ex = st.file_uploader("CSV con columnas: Fecha, Monto (o Valor), Descripcion (opcional)", type=["csv"], key="eg_extras_1444_csv")
            if up_ex is not None:
                try:
                    tmp = pd.read_csv(up_ex)
                    cols = {c.lower().strip(): c for c in tmp.columns}
                    col_fecha = cols.get("fecha")
                    col_monto = cols.get("monto", cols.get("valor"))
                    col_desc  = cols.get("descripcion")
                    if not col_fecha or not col_monto:
                        st.warning("⚠️ El CSV debe traer 'Fecha' y 'Monto' (o 'Valor').")
                    else:
                        egresos_extra = pd.DataFrame({
                            "Fecha": pd.to_datetime(tmp[col_fecha], errors="coerce").dt.date,
                            "Monto_COP": pd.to_numeric(tmp[col_monto], errors="coerce").abs(),
                            "Descripcion": tmp[col_desc] if col_desc else ""
                        }).dropna(subset=["Fecha", "Monto_COP"])
                except Exception as e:
                    st.warning(f"⚠️ No se pudo leer el CSV de egresos extra: {e}")
        else:
            with st.expander("➕ Egresos extra manuales (COP)", expanded=True):
                plantilla = pd.DataFrame({"Fecha": [pd.Timestamp.today().date()], "Monto_COP": [0.0], "Descripcion": [""]})
                egresos_extra = st.data_editor(plantilla, num_rows="dynamic", use_container_width=True, key="eg_extra_1444_editor")
                if not egresos_extra.empty:
                    egresos_extra["Fecha"] = pd.to_datetime(egresos_extra["Fecha"], errors="coerce").dt.date
                    egresos_extra["Monto_COP"] = pd.to_numeric(egresos_extra["Monto_COP"], errors="coerce").abs()
                    egresos_extra["Descripcion"] = egresos_extra.get("Descripcion", "").astype(str)
                    egresos_extra = egresos_extra.dropna(subset=["Fecha", "Monto_COP"])

    # ========= Neteo diario en COP =========
    df_neteo = df_base.copy()

    df_neteo["FECHA_DATE"] = df_neteo["FECHA"].dt.date

    ingresos_por_dia = df_neteo.groupby("FECHA_DATE")["VALOR"].sum(min_count=1).fillna(0.0)
    extras_por_dia = egresos_extra.groupby("Fecha")["Monto_COP"].sum().astype(float) if not egresos_extra.empty else pd.Series(dtype="float64")

    log_rows = []
    if not extras_por_dia.empty:
        for fecha, egreso_cop in extras_por_dia.items():
            ingreso_dia = float(ingresos_por_dia.get(fecha, 0.0))
            neteo_aplicado = float(min(egreso_cop, ingreso_dia))
            cop_convertible = float(max(0.0, ingreso_dia - egreso_cop))
            egreso_pendiente = float(max(0.0, egreso_cop - ingreso_dia))

            # Netear filas del día
            if ingreso_dia > 0.0:
                idxs = df_neteo.index[df_neteo["FECHA_DATE"] == fecha].tolist()
                restante = egreso_cop
                for idx in idxs:
                    if restante <= 0: break
                    val = float(df_neteo.at[idx, "VALOR"] or 0.0)
                    if val <= 0: continue
                    if val <= restante:
                        restante -= val
                        df_neteo.at[idx, "VALOR"] = 0.0
                    else:
                        df_neteo.at[idx, "VALOR"] = val - restante
                        restante = 0.0

            if egreso_pendiente > 0:
                st.warning(f"AVISO (1444) {fecha}: Debes ${egreso_pendiente:,.0f} COP por netear. Cárgalo mañana.")

            log_rows.append({
                "Fecha": fecha,
                "Egreso_extra_COP": egreso_cop,
                "Ingresos_COP_del_dia": ingreso_dia,
                "Neteo_aplicado_COP": neteo_aplicado,
                "COP_convertible_a_USD": cop_convertible,
                "Egreso_pendiente_COP": egreso_pendiente,
                "TRM_del_dia": None,
                "USD_generado": None,
                "Aviso": ("DEBE ${:,.0f} COP — Cargar mañana".format(egreso_pendiente) if egreso_pendiente > 0 else "")
            })

        # Eliminar filas que quedaron en 0
        df_neteo = df_neteo[df_neteo["VALOR"].fillna(0.0) > 0.0].copy()

    # ========= GMF 4x1000 (COP) por día (aplicar ANTES de convertir a USD) =========
    GMF_RATE = 0.004

    gmf_por_dia = {}
    ingresos_pre_gmf = pd.Series(dtype="float64")
    if not df_neteo.empty:
        # Total del día ANTES de GMF (para log)
        ingresos_pre_gmf = (
            pd.to_numeric(df_neteo["VALOR"], errors="coerce")
            .groupby(df_neteo["FECHA_DATE"])
            .sum(min_count=1)
            .fillna(0.0)
        )

        # Restar GMF del total del día, repartiendo sobre las filas del día
        for fecha, total_cop in ingresos_pre_gmf.items():
            total_cop = float(total_cop or 0.0)
            if total_cop <= 0:
                continue

            gmf = round(total_cop * GMF_RATE, 2)  # 0.4%
            restante = gmf
            idxs = df_neteo.index[df_neteo["FECHA_DATE"] == fecha].tolist()
            for idx in idxs:
                if restante <= 0:
                    break
                val = float(df_neteo.at[idx, "VALOR"] or 0.0)
                if val <= 0:
                    continue
                if val <= restante:
                    restante -= val
                    df_neteo.at[idx, "VALOR"] = 0.0
                else:
                    df_neteo.at[idx, "VALOR"] = val - restante
                    restante = 0.0

            gmf_por_dia[fecha] = gmf

        # Quita filas que quedaron en 0 por el GMF
        df_neteo = df_neteo[pd.to_numeric(df_neteo["VALOR"], errors="coerce").fillna(0.0) > 0.0].copy()

    # Para el log: total del día DESPUÉS de GMF (lo que sí se convierte a USD)
    ingresos_post_gmf = (
        pd.to_numeric(df_neteo["VALOR"], errors="coerce")
        .groupby(df_neteo["FECHA_DATE"])
        .sum(min_count=1)
        .fillna(0.0)
        if not df_neteo.empty else pd.Series(dtype="float64")
    )

    # Asegura fila de log por cada fecha con ingresos (aunque no haya egresos extra)
    if not ingresos_pre_gmf.empty:
        fechas_log = {r["Fecha"] for r in log_rows} if log_rows else set()
        for fecha in ingresos_pre_gmf.index:
            if fecha not in fechas_log:
                log_rows.append({
                    "Fecha": fecha,
                    "Egreso_extra_COP": 0.0,
                    "Ingresos_COP_del_dia": float(ingresos_pre_gmf.get(fecha, 0.0)),
                    "Neteo_aplicado_COP": 0.0,
                    "COP_convertible_a_USD": float(ingresos_post_gmf.get(fecha, 0.0)),
                    "Egreso_pendiente_COP": 0.0,
                    "TRM_del_dia": None,
                    "USD_generado": None,
                    "Aviso": ""
                })
            else:
                # Actualiza si ya existía
                for r in log_rows:
                    if r["Fecha"] == fecha:
                        r["Ingresos_COP_del_dia"] = float(ingresos_pre_gmf.get(fecha, 0.0))
                        r["COP_convertible_a_USD"] = float(ingresos_post_gmf.get(fecha, 0.0))
                        break

    # Añade el GMF al log (como egreso en COP)
    for r in log_rows:
        f = r["Fecha"]
        r["GMF_4x1000_COP"] = float(gmf_por_dia.get(f, 0.0))

    # ---------- Base neteada (extras + GMF) que SÍ se convierte ----------
    df_convertible = df_neteo.copy()

    # ======= AVISOS: Total y detalle por día =======
    disponible_total_cop = float(pd.to_numeric(df_convertible["VALOR"], errors="coerce").sum()) if not df_convertible.empty else 0.0
    st.info(f"💡 Saldo disponible en COP (esta carga): ${disponible_total_cop:,.0f}")

    if not df_convertible.empty:
        disp_por_dia = df_convertible.groupby("FECHA_DATE")["VALOR"].sum()
        if len(disp_por_dia.index) >= 1:
            meses = {1:"enero",2:"febrero",3:"marzo",4:"abril",5:"mayo",6:"junio",7:"julio",8:"agosto",9:"septiembre",10:"octubre",11:"noviembre",12:"diciembre"}
            for fecha, valor in sorted(disp_por_dia.items()):
                if valor > 0:
                    st.caption(f"📅 {fecha.day} de {meses[fecha.month]}: ${valor:,.0f} COP disponibles")

    # ===================== TRM por rango (serie diaria) =====================
    try:
        fecha_max_dt = (df_convertible["FECHA"].max() if not df_convertible.empty else df_base["FECHA"].max())
        fecha_max_dt = None if pd.isna(fecha_max_dt) else fecha_max_dt.date()

        if fecha_max_dt is None:
            df_trm_para_joins = pd.DataFrame(columns=["Fecha", "TRM"])
        else:
            punto_corte = f"{fecha_max_dt:%Y-%m-%d}T00:00:00.000"
            url = (
                "https://www.datos.gov.co/resource/mcec-87by.json?"
                f"$where=vigenciadesde <= '{punto_corte}'&$order=vigenciadesde DESC"
            )
            respuesta = requests.get(url)
            respuesta.raise_for_status()
            lista_trm = respuesta.json()

            if not lista_trm:
                df_trm_para_joins = pd.DataFrame(columns=["Fecha", "TRM"])
            else:
                trm_df = pd.DataFrame(lista_trm)
                if "vigenciadesde" not in trm_df.columns or "valor" not in trm_df.columns:
                    df_trm_para_joins = pd.DataFrame(columns=["Fecha", "TRM"])
                else:
                    trm_df["vigenciadesde"] = pd.to_datetime(trm_df["vigenciadesde"]).dt.date
                    trm_df["valor"] = trm_df["valor"].astype(float)
                    trm_df = trm_df.rename(columns={"vigenciadesde": "Fecha", "valor": "TRM"})
                    trm_df = trm_df.drop_duplicates(subset="Fecha", keep="first")
                    trm_df = trm_df.sort_values("Fecha", ascending=False).set_index("Fecha")

                    fecha_min_trm = trm_df.index.min()
                    fecha_min_tx = (df_convertible["FECHA"].dt.date.min()
                                    if not df_convertible.empty else fecha_max_dt)
                    fecha_inicio_rango = min(fecha_min_trm, fecha_min_tx)
                    fecha_fin_rango = fecha_max_dt

                    rango_fechas = pd.date_range(start=fecha_inicio_rango, end=fecha_fin_rango, freq="D").date
                    df_trm_full = pd.DataFrame(index=rango_fechas, columns=["TRM"])
                    for dia in trm_df.index:
                        df_trm_full.loc[dia, "TRM"] = trm_df.loc[dia, "TRM"]
                    df_trm_full["TRM"] = df_trm_full["TRM"].ffill()
                    df_trm_para_joins = df_trm_full.reset_index().rename(columns={"index": "Fecha"})
    except Exception:
        df_trm_para_joins = pd.DataFrame(columns=["Fecha", "TRM"])

    # ===================== Conversión a USD =====================
    if df_convertible.empty:
        # Guardar log (si hay) y devolver vacío
        if log_rows:
            log_df = pd.DataFrame(log_rows).sort_values("Fecha").reset_index(drop=True)
            if not df_trm_para_joins.empty:
                log_df = log_df.merge(df_trm_para_joins, on="Fecha", how="left")
                log_df["TRM_del_dia"] = log_df["TRM"]; log_df.drop(columns=["TRM"], inplace=True, errors="ignore")
            st.session_state["1444_movimientos_cop"] = log_df
            with st.expander("📄 1444 movimientos en COP", expanded=False):
                st.dataframe(log_df, use_container_width=True)
        cols = ["Fecha","Tipo","Monto","Orden","Usuario","Casillero","Estado de Orden","Nombre del producto","TRM"]
        return pd.DataFrame(columns=cols)

    df_convertible["Fecha"] = df_convertible["FECHA"].dt.date
    df_convertible = df_convertible.merge(df_trm_para_joins, on="Fecha", how="left")

    df_convertible["VALOR_USD"] = None
    mask_trm = df_convertible["TRM"].notna()
    df_convertible.loc[mask_trm, "VALOR_USD"] = (
        df_convertible.loc[mask_trm, "VALOR"] / (df_convertible.loc[mask_trm, "TRM"] + 100.0)
    )

    # Armar OUT estándar
    df_convertible["Tipo"] = "Ingreso"
    df_convertible["Orden"] = ""
    df_convertible["Usuario"] = usuario
    df_convertible["Casillero"] = casillero
    df_convertible["Estado de Orden"] = ""

    out = df_convertible.rename(columns={
        "VALOR_USD": "Monto",
        "REFERENCIA": "Nombre del producto"
    })[["Fecha","Tipo","Monto","Orden","Usuario","Casillero","Estado de Orden","Nombre del producto","TRM"]]



    # Completar log (opcional)
    if log_rows:
        log_df = pd.DataFrame(log_rows).sort_values("Fecha").reset_index(drop=True)
        if not df_trm_para_joins.empty:
            log_df = log_df.merge(df_trm_para_joins, on="Fecha", how="left")
            log_df["TRM_del_dia"] = log_df["TRM"]; log_df.drop(columns=["TRM"], inplace=True, errors="ignore")
        usd_por_fecha = out.groupby("Fecha")["Monto"].sum().rename("USD_generado")
        log_df = log_df.merge(usd_por_fecha, on="Fecha", how="left")
        st.session_state["1444_movimientos_cop"] = log_df
        with st.expander("📄 1444 movimientos en COP", expanded=False):
            st.dataframe(log_df, use_container_width=True)

    return out.reset_index(drop=True)





def procesar_ingresos_davivienda(files: list, usuario: str, casillero: str) -> pd.DataFrame:
    """
    Procesa archivos Excel de Davivienda (UploadedFile de Streamlit).
    Toma “Descripción motivo” (columna REFERENCIA) como Nombre del producto, 
    pero si está vacía o nula, utiliza “Referencia 1” como fallback.
    Devuelve un DataFrame con la misma estructura que la función de Bancolombia.

    EXTRA (solo casillero 1444):
    - Calcula GMF 4x1000 diario sobre los COP positivos (excluye 'ABONO INTERESES AHORROS')
      y lo agrega al log compartido: st.session_state["1444_movimientos_cop"].
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
            "Valor Total":        "VALOR",
            "Descripción motivo": "REFERENCIA"
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

        # 7) Construir “Nombre del producto”
        def elegir_nombre(row):
            ref = str(row["REFERENCIA"]).strip()
            if ref and ref.upper() != "NAN":
                return ref
            return str(row["Referencia 1"]).strip()

        df_excel["Nombre del producto"] = df_excel.apply(elegir_nombre, axis=1)

        # 8) Selección base (COP y fecha)
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

    # === GMF 4x1000 -> agregar al MISMO log que 1444 (solo si casillero == "1444") ===
 # === APORTAR INGRESOS COP DE DAVIVIENDA AL MISMO LOG (solo 1444) ===
    # === DAVIVIENDA → aportar al log 1444_movimientos_cop (solo casillero 1444) ===
    if str(casillero) == "1444":
        try:
            # 1) COP positivos por fecha (ignora intereses)
            tmp = df_trans.copy()
            tmp["Fecha"] = pd.to_datetime(tmp["Fecha"], errors="coerce").dt.date
            tmp["ValorCOP"] = pd.to_numeric(tmp["ValorCOP"], errors="coerce")
            mask_valid = (
                (tmp["ValorCOP"] > 0) &
                (tmp["Nombre del producto"].astype(str).str.upper() != "ABONO INTERESES AHORROS")
            )
            ing_por_fecha = (
                tmp[mask_valid]
                .groupby("Fecha")["ValorCOP"]
                .sum(min_count=1)
            )

            if not ing_por_fecha.empty:
                # 2) Armar bloque con el MISMO esquema del log
                dav = ing_por_fecha.reset_index().rename(columns={"ValorCOP": "Ingresos_COP_del_dia"})

                # TRM del día (si existe)
                trm_map = trm_df.copy()
                trm_map["Fecha"] = pd.to_datetime(trm_map["Fecha"], errors="coerce").dt.date
                dav = dav.merge(trm_map[["Fecha", "TRM"]], on="Fecha", how="left").rename(columns={"TRM": "TRM_del_dia"})

                # GMF 4x1000 y neto convertible
                dav["GMF_4x1000_COP"]   = (dav["Ingresos_COP_del_dia"] * 0.004).round(2)
                dav["Egreso_extra_COP"] = 0.0
                dav["Neteo_aplicado_COP"] = 0.0
                dav["Egreso_pendiente_COP"] = 0.0
                dav["Aviso"] = ""
                dav["COP_convertible_a_USD"] = (dav["Ingresos_COP_del_dia"] - dav["GMF_4x1000_COP"]).clip(lower=0)

                # USD estimado
                dav["USD_generado"] = dav.apply(
                    lambda r: (r["COP_convertible_a_USD"] / (r["TRM_del_dia"] + 100.0))
                              if pd.notna(r["TRM_del_dia"]) else None,
                    axis=1
                )

                # Orden definitivo de columnas
                cols_log = [
                    "Fecha","Egreso_extra_COP","Ingresos_COP_del_dia","Neteo_aplicado_COP",
                    "COP_convertible_a_USD","Egreso_pendiente_COP","TRM_del_dia",
                    "USD_generado","Aviso","GMF_4x1000_COP"
                ]
                dav = dav[cols_log]

                # 3) Unir con el log existente sin crear _x/_y
                log_df = st.session_state.get("1444_movimientos_cop", None)
                if isinstance(log_df, pd.DataFrame) and not log_df.empty:
                    log_df = log_df.copy()
                    log_df["Fecha"] = pd.to_datetime(log_df["Fecha"], errors="coerce").dt.date
                    # Alinear columnas en ambos lados
                    for c in cols_log:
                        if c not in log_df.columns: log_df[c] = pd.NA
                    for c in log_df.columns:
                        if c not in dav.columns: dav[c] = pd.NA
                    comb = pd.concat([log_df[cols_log], dav[cols_log]], ignore_index=True)

                    # Agregar por día (sumar numéricos; concatenar avisos)
                    num_cols = ["Egreso_extra_COP","Ingresos_COP_del_dia","Neteo_aplicado_COP",
                                "COP_convertible_a_USD","Egreso_pendiente_COP","TRM_del_dia",
                                "USD_generado","GMF_4x1000_COP"]
                    for c in num_cols:
                        comb[c] = pd.to_numeric(comb[c], errors="coerce")
                    agg = comb.groupby("Fecha", as_index=False).agg(
                        {**{c: "sum" for c in num_cols}, **{"Aviso": lambda s: " | ".join([x for x in s.astype(str) if x and x != "nan"])}}
                    )
                    st.session_state["1444_movimientos_cop"] = agg.sort_values("Fecha").reset_index(drop=True)
                else:
                    st.session_state["1444_movimientos_cop"] = dav.sort_values("Fecha").reset_index(drop=True)
        except Exception as e:
            st.warning(f"No se pudieron registrar los ingresos de Davivienda en el log: {e}")
    # === /fin aporte Davivienda al log ===

    return out.reset_index(drop=True)






# === Config de cobros mensuales por casillero (fácil de cambiar) ===
COBROS_MENSUALES_CONF = {
    # casillero : {"inicio": "YYYY-MM-01", "monto": int}
    "1633": {"inicio": "2024-02-01", "monto": 711_750}
    #,
   # "13608": {"inicio": "2025-11-01", "monto": 500000},
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

def send_mail_zoho(subject: str, body: str, to_addrs) -> bool:
    """SMTP Zoho Mail con App Password. Sin adjuntos."""
    try:
        cfg = st.secrets["zoho"]
        sender = cfg["address"]
        app_pw = cfg["app_password"]
        smtp_server = cfg.get("smtp_server", "smtp.zoho.com")   # o "smtppro.zoho.com" según tu plan
        smtp_port = int(cfg.get("smtp_port", 465))              # 465 SSL ó 587 STARTTLS
        security = str(cfg.get("security", "SSL")).upper()      # "SSL" o "STARTTLS"
    except Exception as e:
        st.error("❌ Falta configuración zoho en st.secrets['zoho']: " + str(e))
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
        if security == "STARTTLS":
            with smtplib.SMTP(smtp_server, 587) as server:
                server.ehlo()
                server.starttls(context=context)
                server.login(sender, app_pw)
                server.send_message(msg)
        else:
            with smtplib.SMTP_SSL(smtp_server, smtp_port, context=context) as server:
                server.login(sender, app_pw)
                server.send_message(msg)
        return True
    except Exception as e:
        st.error(f"❌ Error enviando email (Zoho): {e}")
        return False


def obtener_y_enviar_alerta_saldo(historico: dict, casillero: str, fecha_carga: str) -> None:
    """
    Toma el último 'Total' del casillero en 'historico' y envía un correo SOLO
    al destinatario configurado para ese casillero (Zoho).
    """
    # 1) hallar la hoja del casillero
    hoja = next((h for h in historico if h.startswith(str(casillero))), None)
    if not hoja:
        return

    dfh = historico[hoja].copy()
    if dfh.empty:
        return

    # 2) último Total por fecha
    dfh["Tipo"] = dfh["Tipo"].astype(str)
    df_tot = dfh[dfh["Tipo"].str.upper() == "TOTAL"].copy()
    if df_tot.empty:
        return

    df_tot["Fecha"] = pd.to_datetime(df_tot["Fecha"], errors="coerce")
    df_tot = df_tot.dropna(subset=["Fecha"])
    if df_tot.empty:
        return

    fila = df_tot.sort_values("Fecha").iloc[-1]
    saldo = pd.to_numeric(fila["Monto"], errors="coerce")
    fecha_saldo = fila["Fecha"].date()
    if pd.isna(saldo):
        return

    # 3) destinatario SOLO si está mapeado (Zoho)
    recipients_map = st.secrets.get("zoho", {}).get("recipients", {})
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

    ok = send_mail_zoho(subject, body, destino)
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
    
    
    st.markdown("---")
    st.header("3) Devoluciones")
    dev_file = st.file_uploader("Sube archivo de DEVOLUCIONES", type=["xls","xlsx"])
    devoluciones = {}
    if dev_file:
        hojas_dev = pd.read_excel(dev_file, sheet_name=None)
        devoluciones = procesar_devoluciones(hojas_dev)
        tabs_dev = st.tabs(list(devoluciones.keys()))
        for tab, key in zip(tabs_dev, devoluciones):
            with tab:
                df = devoluciones[key]
                if df.empty:
                    st.info("Sin devoluciones")
                else:
                    st.dataframe(df, use_container_width=True)
    else:
        st.info("📂 Aún no subes Devoluciones")

    
    
    

    # 3) Ingresos Nathalia Ospina (CA1633)
    st.header("4) Ingresos Nathalia Ospina (CA1633)")
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
    st.header("5) Ingresos Elvis (CA11591)")
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
    st.header("6) Ingresos Julian Sanchez (CA13608)")
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
    
    st.header("7) Ingresos Maria Moises (CA1444)")
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
    st.header("8) Conciliaciones Finales")

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
    
        # ------------------ NUEVO: GMF 4x1000 SOLO PARA 1633 ------------------
        gmf_df = None
        if cas in ("1633", "13608"):
            # Elegir de qué DF calcular el GMF (preferimos el ingreso real que se usó)
            base_ing = inc if (inc is not None and not inc.empty) else ing_n  # fallback a Nath si inc viene de otra persona
            if base_ing is not None and not base_ing.empty:
                tmp = base_ing.copy()
    
                # Asegurar numérico
                tmp["Monto"] = pd.to_numeric(tmp["Monto"], errors="coerce").fillna(0)
    
                # Tomar SOLO movimientos de tipo Ingreso (por si en 'inc' vienen también Egresos)
                if "Tipo" in tmp.columns:
                    tmp = tmp[tmp["Tipo"].astype(str).str.strip().str.lower() == "ingreso"]
    
                # Evitar doble conteo si ya agregaste una fila GMF en otro paso
                if "Nombre del producto" in tmp.columns:
                    tmp = tmp[~tmp["Nombre del producto"].astype(str).str.contains("4x1000", case=False, na=False)]
    
                gmf_total = round(0.004 * tmp["Monto"].sum(), 2)  # 4x1000 = 0.4%
                usuario = "Nathalia Ospina" if cas == "1633" else "Julian Sanchez"
                casillero_val = cas  #
                
                # calcular la fecha a usar
                fecha_base = pd.to_datetime(base_ing.get("Fecha", pd.NaT), errors="coerce")
                fecha_gmf = fecha_base.max()
                if pd.isna(fecha_gmf):
                    fecha_gmf = pd.Timestamp.today().normalize()
                orden_gmf = f"GMF-4x1000-ACUM-{fecha_gmf.strftime('%Y%m%d')}"
                
                # Si quieres que sea EGRESO POSITIVO (recomendado para no caer en filtros de monto>0)
                if gmf_total != 0:
                    # Construir la fila con las columnas que maneja tu pipeline
                    cols = list(base_ing.columns)
                    fila = {c: None for c in cols}
    
                    fila.update({
                        "Fecha": pd.Timestamp.today().normalize(),     # o pd.to_datetime(base_ing["Fecha"]).max()
                        "Tipo": "Egreso",                              # <<< CLAVE
                        "Monto": gmf_total,                            # <<< POSITIVO
                        "Orden": orden_gmf,
                        "Usuario":usuario,
                        "Casillero": casillero_val,
                        "Estado de Orden": "",
                        "Nombre del producto": "GMF 4x1000 acumulado",
                    })
    
                    # Si existe TRM, puedes heredar el último
                    if "TRM" in cols:
                        try:
                            fila["TRM"] = pd.to_numeric(base_ing["TRM"], errors="coerce").dropna().iloc[-1]
                        except Exception:
                            fila["TRM"] = None
    
                    gmf_df = pd.DataFrame([fila])
        # ----------------------------------------------------------------------
    
        # EGRESOS
        egr = egresos.get(f"egresos_{cas}")
    
        # EXTRA (ingresos extra)
        ext = ingresos_extra.get(f"extra_{cas}")
    
        # <<< NUEVO: DEVOLUCIONES (ingresos por devolución)
        key_dev = f"devoluciones_{cas}"
        dev = devoluciones.get(key_dev) if 'devoluciones' in locals() else None  # guard contra que no exista el dict
    
        # 3) Armar la lista de DataFrames válidos
        frames = []
        for df in (inc, egr, ext):
            if df is not None and not df.empty:
                frames.append(df)
    
        if gmf_df is not None and not gmf_df.empty:
            frames.append(gmf_df)
    
        # <<< NUEVO: agregar devoluciones si hay
        if dev is not None and not dev.empty:
            frames.append(dev)
    
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
    st.header("9) Actualizar Histórico") 
    hist_file = st.file_uploader("Sube tu archivo HISTÓRICO EXISTENTE", type=["xls","xlsx"])
    if hist_file:
        historico = pd.read_excel(hist_file, sheet_name=None)
        fecha_carga = pd.Timestamp.today().strftime("%Y-%m-%d")
    
    # <<< NUEVO: acumulador de errores de validación
        errores_validacion = []
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


            # ---------- VALIDACIÓN DE DEVOLUCIONES vs EGRESOS (por Orden) ----------
            df_valid = historico[hoja].copy()

            df_valid["Tipo"] = df_valid["Tipo"].astype(str).str.upper()
            df_valid["Orden"] = df_valid["Orden"].astype(str).str.strip()
            df_valid["Monto"] = pd.to_numeric(df_valid["Monto"], errors="coerce")

            egresos_por_orden = (
                df_valid[df_valid["Tipo"] == "EGRESO"]
                .groupby("Orden")["Monto"].sum(min_count=1)
            )

            if "Motivo" in df_valid.columns:
                mask_dev = (df_valid["Tipo"] == "INGRESO") & (df_valid["Motivo"].astype(str).str.lower() == "devolucion")
            else:
                mask_dev = (df_valid["Tipo"] == "INGRESO") & (df_valid["Nombre del producto"].astype(str).str.lower().str.contains("devoluc"))

            devoluciones_por_orden = (
                df_valid[mask_dev]
                .groupby("Orden")["Monto"].sum(min_count=1)
            )

            ordenes = sorted(set(devoluciones_por_orden.index) | set(egresos_por_orden.index))
            for o in ordenes:
                eg = float(egresos_por_orden.get(o, 0.0) or 0.0)
                dv = float(devoluciones_por_orden.get(o, 0.0) or 0.0)
                if dv > eg and eg > 0:
                    exceso = dv - eg
                    msg = f"Devolución excedida en casillero {cas} — Orden {o}: devuelto ${dv:,.2f} > egresado ${eg:,.2f}. Exceso ${exceso:,.2f}."
                    st.error(f"🚨 {msg}")
                    errores_validacion.append(msg)   # <<< acumula para bloquear exportación
                if eg == 0 and dv > 0:
                    msg = f"Devolución sin egreso asociado en casillero {cas} — Orden {o}: devuelto ${dv:,.2f}. Revisa la Orden."
                    st.warning(f"⚠️ {msg}")
                    errores_validacion.append(msg)   # <<< también bloquea
            # ---------- /VALIDACIÓN ----------


        # <<< NUEVO: si hubo errores, no generar archivo ni enviar correos
        if errores_validacion:
            st.error("⛔ No se generó el histórico porque hay devoluciones inválidas. Corrige y vuelve a ejecutar.")
            with st.expander("Ver detalles"):
                for m in errores_validacion:
                    st.write("•", m)
            st.stop()  # <<< BLOQUEA exportación y resto del flujo



        # --- Anexar hoja con log COP de 1444 (crear o concatenar) ---
        sheet_name_cop = "1444 - Maria Moises COP"
        
        # Recuperar el log desde la sesión (si existe)
        try:
            log_df = st.session_state.get("1444_movimientos_cop", None)
        except Exception:
            log_df = None
        
        if isinstance(log_df, pd.DataFrame) and not log_df.empty:
            df_log = log_df.copy()
        
            # Normalizar Fecha a date (evita tz/datetime raros en Excel)
            if "Fecha" in df_log.columns:
                df_log["Fecha"] = pd.to_datetime(df_log["Fecha"], errors="coerce").dt.date
        
            if sheet_name_cop in historico:
                # Concatenar al final sin deduplicar
                old_df = historico[sheet_name_cop].copy()
        
                # Alinear columnas: mantener primero las existentes y luego cualquier columna nueva del log
                cols_old = list(old_df.columns)
                cols_log = list(df_log.columns)
                cols_extra = [c for c in cols_log if c not in cols_old]
                cols_final = cols_old + cols_extra
        
                # Asegurar que ambos DFs tengan todas las columnas del set final
                for c in cols_final:
                    if c not in old_df.columns:
                        old_df[c] = pd.NA
                    if c not in df_log.columns:
                        df_log[c] = pd.NA
        
                historico[sheet_name_cop] = pd.concat(
                    [old_df[cols_final], df_log[cols_final]],
                    ignore_index=True
                )
            else:
                # Crear la hoja por primera vez
                historico[sheet_name_cop] = df_log
        # --- /fin anexar hoja COP 1444 ---
        
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
        # 📧 ¿Estás haciendo una prueba?
        modo_prueba = st.radio(
            "¿Te encuentras haciendo una prueba?",
            ["Sí", "No"],
            index=0,            # por defecto: Sí (no envía)
            horizontal=True
        )
        
        if modo_prueba == "No":
            # Enviar correos por casillero (solo a los configurados)
            for cas in st.secrets["zoho"]["recipients"].keys():
                obtener_y_enviar_alerta_saldo(historico, str(cas), fecha_carga)
        else:
            st.info("Modo prueba activo: no se enviaron correos.")


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
