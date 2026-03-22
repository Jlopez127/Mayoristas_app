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
from pathlib import Path, PurePosixPath

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
    casilleros = ["9444", "14856", "11591", "1444", "1633", "13608", "9680", "14825"]
    df = df.copy()

    # Fechas y tipos
    df["Fecha Compra"] = pd.to_datetime(df["Fecha Compra"], errors="coerce", utc=True).dt.tz_convert(None)
    df["Casillero"] = df["Casillero"].astype(str)

    # Filtrar casilleros manejados
    df = df[df["Casillero"].isin(casilleros)]

    # Cortes por casillero
    cutoff_13608 = pd.Timestamp("2025-09-18")
    cutoff_9680  = pd.Timestamp("2025-11-11")
    cutoff_14825 = pd.Timestamp("2026-02-11")


    # Mantener 13608 solo desde 2025-09-18 y 9680 solo desde 2025-11-11
    df = df[
        ((df["Casillero"] != "13608") | (df["Fecha Compra"] >= cutoff_13608)) &
        ((df["Casillero"] != "9680")  | (df["Fecha Compra"] >= cutoff_9680)) &
        ((df["Casillero"] != "14825") | (df["Fecha Compra"] >= cutoff_14825))
    ]


    # Formatos y normalizaciones
    df["Fecha Compra"] = df["Fecha Compra"].dt.strftime("%Y-%m-%d")
    df["Tipo"] = "Egreso"
    df["Total de Pago COP"] = pd.to_numeric(df["Total de Pago COP"], errors="coerce")
    df["Valor de compra COP"] = pd.to_numeric(df["Valor de compra COP"], errors="coerce")

    # Si está cancelada y sin Total de Pago COP, usar Valor de compra COP
    mask = (df["Estado de Orden"] == "Cancelada") & df["Total de Pago COP"].isna()
    df.loc[mask, "Total de Pago COP"] = df.loc[mask, "Valor de compra COP"]

    # Orden como entero estable y luego string
    df["Orden"] = pd.to_numeric(df["Orden"], errors="coerce").astype("Int64")
    df = df.sort_values("Orden")
    df["Orden"] = df["Orden"].astype(str)

    # Monto: USD solo para 1444 y 14856; demás (incluye 9680) en COP
    df["Monto"] = df.apply(
        lambda row: row.get("Valor de compra USD", None) if row["Casillero"] in [ "14856"]
        else row["Valor de compra COP"],
        axis=1
    )

    # Renombrar y seleccionar columnas finales
    df = df.rename(columns={"Fecha Compra": "Fecha"})[
        ["Fecha","Tipo","Monto","Orden","TRM","Usuario","Casillero","Estado de Orden","Nombre del producto"]
    ]

    # Alias de usuario conocido
    df.loc[df["Casillero"] == "9444", "Usuario"] = "Maira Alejandra Paez"
    df.loc[df["Casillero"] == "9680", "Usuario"] = "Juan Felipe Laverde"
    df.loc[df["Casillero"] == "14825", "Usuario"] = "Cristian Javier Castro"
    # Salida por casillero
    salida = {}
    for cas in casilleros:
        salida[f"egresos_{cas}"] = df[df["Casillero"] == cas].reset_index(drop=True)

    return salida


# — 2) Ingresos Extra —
@st.cache_data
def procesar_ingresos_extra(hojas: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    resultado = {}
    for hoja, df in (hojas or {}).items():
        cas = hoja.split("-")[0].strip()
        if not cas.isdigit():
            continue

        # Copia y elimina/omite la columna 'Revision' si existe
        df2 = df.copy()
        df2.drop(columns=["Revision"], errors="ignore", inplace=True)

        # Casillero
        if "Casillero" in df2.columns:
            df2["Casillero"] = df2["Casillero"].astype(str)
        else:
            df2["Casillero"] = cas

        # TRM según fecha máxima (si existe 'Fecha')
        trm = None
        if "Fecha" in df2.columns:
            try:
                fmax = pd.to_datetime(df2["Fecha"], errors="coerce").max()
                if pd.notna(fmax):
                    fmax_str = fmax.strftime("%Y-%m-%d")
                    url = f"https://www.datos.gov.co/resource/mcec-87by.json?vigenciadesde={fmax_str}T00:00:00.000"
                    resp = requests.get(url, timeout=10)
                    resp.raise_for_status()
                    data = resp.json()
                    if data and isinstance(data, list) and "valor" in data[0]:
                        trm = float(data[0]["valor"])
            except Exception:
                trm = None

        df2["TRM"] = trm
        resultado[f"extra_{cas}"] = df2.reset_index(drop=True)

    return resultado






@st.cache_data
def procesar_envios_mayoristas(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Lee la hoja 'Mayoristas' (Envios mayoristas) y devuelve un dict con un DF por casillero.
    Normaliza Fecha de dd-mm-YYYY -> YYYY-MM-DD para que sea consistente con el resto.
    """
    casilleros_validos = {"9444", "14856", "11591", "1444", "1633", "13608", "9680", "14825"}

    df2 = df.copy()
    df2.columns = [str(c).strip() for c in df2.columns]

    # Asegurar columnas mínimas
    for c in ["Tipo","Fecha","Orden","Monto","Usuario","Casillero","Motivo","Nombre del producto"]:
        if c not in df2.columns:
            df2[c] = ""

    # Normalizaciones
    df2["Tipo"] = df2["Tipo"].astype(str).str.strip().replace({"": "Egreso"})
    df2["Orden"] = df2["Orden"].astype(str).str.strip()
    df2["Usuario"] = df2["Usuario"].astype(str).str.strip()
    df2["Casillero"] = df2["Casillero"].astype(str).str.strip()
    df2["Motivo"] = df2["Motivo"].astype(str).str.strip().replace({"": "Envio"})
    df2["Nombre del producto"] = df2["Nombre del producto"].astype(str).str.strip()

    # Fecha dd-mm-YYYY -> YYYY-MM-DD
    df2["Fecha"] = pd.to_datetime(
        df2["Fecha"].astype(str).str.strip(),
        format="%d-%m-%Y",
        errors="coerce"
    ).dt.strftime("%Y-%m-%d")

    # 🚩 AQUÍ el cambio: monto viene limpio → solo convertir a entero
    df2["Monto"] = pd.to_numeric(df2["Monto"], errors="coerce").astype(int)

    # Filtrar filas válidas y casilleros conocidos
    df2 = df2.dropna(subset=["Fecha", "Monto"])
    df2 = df2[df2["Casillero"].isin(casilleros_validos)].copy()

    # Orden de columnas
    cols = ["Fecha","Tipo","Monto","Orden","Usuario","Casillero","Motivo","Nombre del producto"]
    df2 = df2[cols]

    # Dict por casillero
    salida = {}
    for cas in sorted(df2["Casillero"].unique()):
        salida[f"envios_{cas}"] = df2[df2["Casillero"] == cas].reset_index(drop=True)

    return salida









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











from pathlib import Path
import pandas as pd
import requests
import hashlib

def leer_ingresos_archivo(up) -> pd.DataFrame:
    """Lee el archivo subido (tsv renombrado) y aplica el filtro por fecha en el nombre si existe."""
    df = pd.read_csv(up, sep="\t", encoding="latin-1", engine="python")

    # nombre del archivo
    nombre_archivo = up.name if hasattr(up, "name") else "desconocido"
    stem = Path(nombre_archivo).stem
    partes = stem.split()

    # 1) fecha del nombre
    fecha_archivo = None
    if partes:
        posible_fecha = partes[0]   # '20251030'
        try:
            fecha_archivo = pd.to_datetime(posible_fecha, format="%Y%m%d").date()
        except Exception:
            fecha_archivo = None

    # 2) banco (última palabra)
    banco = partes[-1] if len(partes) >= 2 else "desconocido"

    # parsear fecha de la columna
    df["FECHA"] = pd.to_datetime(df["FECHA"], format="%Y/%m/%d", errors="coerce").dt.date

    # FILTRO: si el nombre traía fecha → solo esas filas
    if fecha_archivo is not None:
        df = df[df["FECHA"] == fecha_archivo].copy()

    # guardar origen
    df["Archivo_Origen"] = nombre_archivo
    df["Banco_Origen"] = banco

    # opcional: número de línea para hacer ID más estable
    df["Linea_Origen"] = df.reset_index().index

    return df


def normalizar_ingresos(df: pd.DataFrame, usuario: str, casillero: str) -> pd.DataFrame:
    """Lleva el df leído al formato estándar tuyo."""
    # completar referencia
    df["REFERENCIA"] = df["REFERENCIA"].fillna(df.get("DESCRIPCIÓN", ""))

    # quitar columnas vacías
    df = df.dropna(how="all", axis=1)

    # volver a datetime normal
    df["Fecha"] = pd.to_datetime(df["FECHA"], errors="coerce")

    # monto
    df["Monto"] = (
        df["VALOR"].astype(str).str.replace(",", "", regex=False).astype(float)
    )

    df["Tipo"] = "Ingreso"
    df["Orden"] = ""   # lo llenamos luego si quieres
    df["Usuario"] = usuario
    df["Casillero"] = casillero
    df["Estado de Orden"] = ""

    out = df[[
        "Fecha",
        "Tipo",
        "Monto",
        "Orden",
        "Usuario",
        "Casillero",
        "Estado de Orden",
        "REFERENCIA",
        "Archivo_Origen",
        "Banco_Origen",
        "Linea_Origen",
    ]].rename(columns={
        "REFERENCIA": "Nombre del producto"
    })

    # tus filtros
    out = out[out["Nombre del producto"] != "ABONO INTERESES AHORROS"]
    out = out[out["Monto"] > 0]

    return out


def generar_id_ingreso(df: pd.DataFrame) -> pd.DataFrame:
    """Genera un ID determinista por fila usando archivo + línea + fecha + monto + banco."""
    fecha_str = df["Fecha"].dt.strftime("%Y%m%d").fillna("")
    monto_str = df["Monto"].round(2).astype(str)
    banco_str = df["Banco_Origen"].astype(str).str.strip()
    arch_str  = df["Archivo_Origen"].astype(str)
    linea_str = df["Linea_Origen"].astype(str)

    bases = (
        arch_str + "|" +
        linea_str + "|" +
        fecha_str + "|" +
        monto_str + "|" +
        banco_str
    )

    df["ID_INGRESO"] = bases.apply(lambda x: hashlib.sha1(x.encode("utf-8")).hexdigest())
    return df


def procesar_ingresos_clientes_xls(files: list, usuario: str, casillero: str) -> pd.DataFrame:
    dfs = []
    for up in files:
        df_raw  = leer_ingresos_archivo(up)
        df_norm = normalizar_ingresos(df_raw, usuario, casillero)
        dfs.append(df_norm)

    if not dfs:
        return pd.DataFrame()

    out = pd.concat(dfs, ignore_index=True)

    # generar IDs
    out = generar_id_ingreso(out)

    # traer TRM (como lo hacías)
    try:
        fmax = out["Fecha"].max().strftime("%Y-%m-%d")
        url = f"https://www.datos.gov.co/resource/mcec-87by.json?vigenciadesde={fmax}T00:00:00.000"
        data = requests.get(url).json()
        trm = float(data[0]["valor"]) if data and "valor" in data[0] else None
    except Exception:
        trm = None
    out["TRM"] = trm

    return out.reset_index(drop=True)



from pathlib import Path
import io
import pandas as pd
import streamlit as st
import requests


def exportar_ingresos_csv_a_dropbox(out: pd.DataFrame, casillero: str):
    """
    Toma el DataFrame `out` (ingresos ya normalizados, con ID_INGRESO)
    y lo acumula en un archivo de Dropbox:

        ingresos_<casillero}_bancolombia.xlsx

    - Usa la misma carpeta de st.secrets["dropbox"]["remote_path"].
    - Concatena lo viejo + lo nuevo.
    - Elimina duplicados por ID_INGRESO, quedándose con el PRIMERO.
      (prioriza los que tengan Id_cliente / Factura llenos).
    - Asegura que existan las columnas: Id_cliente y Factura.
    """
    # Nada que hacer si no hay datos
    if out is None or out.empty:
        return

    if "ID_INGRESO" not in out.columns:
        st.warning(f"⚠️ No se encontró 'ID_INGRESO' para casillero {casillero}; no se exporta a Dropbox.")
        return

    # 1) Carpeta base tomada del histórico
    cfg = st.secrets["dropbox"]
    base_remote = cfg["remote_path"]  # ej: "/Conciliacion/Historico_mayoristas.xlsx"
    base_dir = PurePosixPath(base_remote).parent

    # Nombre final del archivo: ingresos_<casillero>_bancolombia.xlsx
    remote_path_ingresos = str(base_dir / f"ingresos_{casillero}_bancolombia.xlsx")

    # 2) Leer archivo existente (si no existe, se arranca vacío)
    try:
        md, res = dbx.files_download(remote_path_ingresos)
        buf_in = io.BytesIO(res.content)
        df_old = pd.read_excel(buf_in)
    except Exception:
        df_old = pd.DataFrame()

    # 3) Alinear columnas entre viejo y nuevo
    all_cols = list(df_old.columns)
    for c in out.columns:
        if c not in all_cols:
            all_cols.append(c)

    df_old = df_old.reindex(columns=all_cols)
    df_new = out.reindex(columns=all_cols)

    # 4) Concatenar: primero Dropbox, luego lo nuevo
    df_comb = pd.concat([df_old, df_new], ignore_index=True)
    df_comb["ID_INGRESO"] = df_comb["ID_INGRESO"].astype(str).str.strip()
    
    # 4.1) Asegurar columnas Id_cliente y Factura
    for col in ["Id_cliente", "Factura"]:
        if col not in df_comb.columns:
            df_comb[col] = ""
    
    # 4.2) NUEVA LÓGICA:
    # Si el ID ya existía en Dropbox, se conserva el de Dropbox.
    # Solo se agregan IDs nuevos.
    df_comb = df_comb.drop_duplicates(subset=["ID_INGRESO"], keep="first").copy()

    # 5) Guardar a Excel en memoria y subir a Dropbox
    buf_out = io.BytesIO()
    with pd.ExcelWriter(buf_out, engine="openpyxl") as writer:
        df_comb.to_excel(writer, sheet_name="Ingresos", index=False)
    buf_out.seek(0)

    dbx.files_upload(
        buf_out.read(),
        remote_path_ingresos,
        mode=dropbox.files.WriteMode.overwrite
    )

    st.success(f"✅ Archivo 'ingresos_{casillero}_bancolombia.xlsx' actualizado en Dropbox.")




def procesar_ingresos_clientes_csv(files: list, usuario: str, casillero: str) -> pd.DataFrame:
    dfs = []
    for up in files:
        # ---------- 1. Nombre, fecha y banco desde el nombre ----------
        fname = getattr(up, "name", "archivo_sin_nombre")
        stem = Path(fname).stem                  # ej. '20251030 Julian Bancolombia'
        partes = stem.split()

        # fecha del nombre
        fecha_archivo = None
        if partes:
            posible_fecha = partes[0]            # '20251030'
            try:
                fecha_archivo = pd.to_datetime(posible_fecha, format="%Y%m%d").date()
            except Exception:
                fecha_archivo = None

        # banco (última palabra)
        banco_archivo = partes[-1] if len(partes) >= 2 else "desconocido"

        # ---------- 2. Leer el CSV en memoria con distintos encodings ----------
        contenido = up.read() if hasattr(up, "read") else up

        texto = None
        for codec in ("utf-8", "utf-8-sig", "latin-1", "cp1252"):
            try:
                texto = contenido.decode(codec)
                break
            except UnicodeDecodeError:
                continue

        if texto is None:
            st.warning(f"⚠️ No se pudo decodificar '{fname}'. Se omite.")
            continue

        buf = io.StringIO(texto)
        df = pd.read_csv(buf, header=None, sep=",")

        # ---------- 3. Normalizar a 10 columnas ----------
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

        # ---------- 4. Parsear fechas de la columna FECHA ----------
        fechas_raw = df["FECHA"].astype(str).str.strip().str.zfill(8)
        f1 = pd.to_datetime(fechas_raw, format="%Y%m%d", errors="coerce")
        f2 = pd.to_datetime(fechas_raw, format="%d%m%Y", errors="coerce")
        df["FECHA"] = f1.fillna(f2).dt.date

        # ---------- 5. FILTRO por la fecha del nombre ----------
        if fecha_archivo is not None:
            df = df[df["FECHA"] == fecha_archivo].copy()

        # ---------- 6. Guardar origen ----------
        df["Archivo_Origen"] = fname
        df["Banco_Origen"] = banco_archivo
        df["Linea_Origen"] = df.reset_index().index  # lo dejamos por si lo quieres usar luego

        dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)

    # ---------- 7. Completar referencia ----------
    df["REFERENCIA"] = df["REFERENCIA"].fillna(df.get("DESCRIPCIÓN", ""))

    # ---------- 8. Limpiar ----------
    df = df.dropna(how="all", axis=1)

    df["Fecha"] = pd.to_datetime(df["FECHA"], errors="coerce")

    # LIMPIEZA DE VALOR
    df["VALOR"] = (
        df["VALOR"]
        .astype(str)
        .str.replace(",", "", regex=False)
        .str.strip()
        .astype(float)
    )

    # ---------- 9. Crear ID legible con consecutivo ----------
    fecha_str  = df["Fecha"].dt.strftime("%Y%m%d").fillna("")
    monto_str  = df["VALOR"].round(2).astype(str)
    usuario_str = str(usuario).strip()
    banco_str  = df["Banco_Origen"].astype(str).str.strip()

    df["ID_BASE"] = (
        fecha_str + "-" +
        monto_str + "-" +
        usuario_str + "-" +
        banco_str
    )

    contadores = {}
    ids = []
    for base in df["ID_BASE"]:
        n = contadores.get(base, 0) + 1
        contadores[base] = n
        ids.append(f"{base}-{n}")

    df["ID_INGRESO"] = ids
    df["Orden"] = df["ID_INGRESO"]

    # ---------- 10. Armar salida ----------
    df["Tipo"] = "Ingreso"
    df["Usuario"] = usuario
    df["Casillero"] = casillero
    df["Estado de Orden"] = ""

    out = df.rename(columns={
        "VALOR": "Monto",
        "REFERENCIA": "Nombre del producto"
    })[[
        "Fecha",
        "Tipo",
        "Monto",
        "Orden",
        "Usuario",
        "Casillero",
        "Estado de Orden",
        "Nombre del producto",
        "Archivo_Origen",
        "Banco_Origen",
        "ID_INGRESO"
    ]]

    # ---------- 11. Filtros de negocio ----------
    out = out[out["Nombre del producto"] != "ABONO INTERESES AHORROS"]
    out = out[out["Monto"] > 0]

    # ---------- 12. TRM ----------
    # ---------- 12. TRM ----------
    try:
        fmax = out["Fecha"].max().strftime("%Y-%m-%d")
        url = f"https://www.datos.gov.co/resource/mcec-87by.json?vigenciadesde={fmax}T00:00:00.000"
        data = requests.get(url).json()
        trm = float(data[0]["valor"]) if data and "valor" in data[0] else None
    except Exception:
        trm = None
    out["TRM"] = trm

    # ---------- 13. Exportar a Dropbox por mayorista (casillero) ----------
    try:
        exportar_ingresos_csv_a_dropbox(out, casillero)
    except Exception as e:
        st.warning(f"⚠️ No se pudieron exportar ingresos del casillero {casillero} a Dropbox: {e}")

    return out.reset_index(drop=True)









# === Config de cobros mensuales por casillero (fácil de cambiar) ===
COBROS_MENSUALES_CONF = {
    # casillero : {"inicio": "YYYY-MM-01", "monto": int}
    "1633": {"inicio": "2024-02-01", "monto": 879_000},
    "13608": {"inicio": "2025-11-01", "monto": 620000},
    "1444": {"inicio": "2026-03-01", "monto": 930_000},
}

def aplicar_cobro_contabilidad_mensual(historico, hoja, casillero, usuario, fecha_carga, inicio_yyyymm, monto, etiqueta_base="cobro contabilidad"):
    """
    Agrega un Egreso mensual fijo con Fecha = último día de cada mes, desde 'inicio_yyyymm'
    hasta el MES ANTERIOR a 'fecha_carga'. Idempotente (no duplica por Orden/Nombre del producto).

    IMPORTANTE:
    - YA NO toca ni descuenta el último TOTAL.
    - SOLO agrega movimientos Egreso.
    - Los TOTAL deben recalcularse después, en un bloque global.
    """
    import calendar
    from datetime import date

    if hoja not in historico:
        return historico

    dfh = historico[hoja].copy()

    fc_date = pd.to_datetime(fecha_carga, errors="coerce").date()
    last_of_prev_month = fc_date.replace(day=1) - timedelta(days=1)
    end_y, end_m = last_of_prev_month.year, last_of_prev_month.month

    start_date = pd.to_datetime(inicio_yyyymm, errors="coerce").date()
    start_y, start_m = start_date.year, start_date.month

    if (start_y, start_m) > (end_y, end_m):
        historico[hoja] = dfh
        return historico

    meses = {
        1: "enero", 2: "febrero", 3: "marzo", 4: "abril", 5: "mayo", 6: "junio",
        7: "julio", 8: "agosto", 9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre"
    }

    y, m = start_y, start_m

    while (y, m) <= (end_y, end_m):
        last_day = calendar.monthrange(y, m)[1]
        fecha_mes = date(y, m, last_day)
        orden_nombre = f"{etiqueta_base} ({meses[m]} {y})"

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

        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1

    historico[hoja] = dfh
    return historico





def asegurar_columnas_historico(df):
    """
    Garantiza que el histórico tenga las columnas mínimas esperadas.
    Si faltan, las crea vacías.
    """
    if df is None or not isinstance(df, pd.DataFrame):
        df = pd.DataFrame()

    columnas_base = [
        "Fecha",
        "Tipo",
        "Orden",
        "Monto",
        "Motivo",
        "TRM",
        "Usuario",
        "Casillero",
        "Estado de Orden",
        "Nombre del producto",
        "Fecha de Carga"
    ]

    for col in columnas_base:
        if col not in df.columns:
            df[col] = ""

    return df



def recalcular_totales_diarios(df, usuario, cas):
    """
    Recalcula TODOS los TOTAL desde cero usando:
    total_dia = total_anterior + ingresos_dia - egresos_dia

    - Elimina TOTAL anteriores
    - Agrupa por Fecha real
    - Si Fecha está vacía, usa Fecha de Carga
    - Devuelve movimientos + TOTAL nuevos
    """
    if df is None or df.empty:
        return df

    df = df.copy()

    # Quitar TOTAL viejos
    base = df[df["Tipo"].astype(str).str.upper() != "TOTAL"].copy()

    # Fechas limpias
    base["Fecha"] = pd.to_datetime(base["Fecha"], errors="coerce").dt.normalize()
    base["Fecha de Carga"] = pd.to_datetime(base["Fecha de Carga"], errors="coerce").dt.normalize()
    base["Monto"] = pd.to_numeric(base["Monto"], errors="coerce").fillna(0)

    # Si Fecha está vacía, usar Fecha de Carga
    mask_fecha_vacia = base["Fecha"].isna()
    base.loc[mask_fecha_vacia, "Fecha"] = base.loc[mask_fecha_vacia, "Fecha de Carga"]

    # Solo filas con fecha válida
    base = base[base["Fecha"].notna()].copy()

    # Normalizar tipo
    # Blindaje final ANTES del cálculo:
    # toda devolución debe sumar como ingreso positivo, sin importar cómo venga mezclada en combinado
    base["Tipo"] = base["Tipo"].astype(str).str.strip()
    base["Monto"] = pd.to_numeric(base["Monto"], errors="coerce").fillna(0)

    if "Motivo" in base.columns:
        motivo_norm = base["Motivo"].astype(str).str.strip().str.lower()
    else:
        motivo_norm = pd.Series("", index=base.index)

    if "Nombre del producto" in base.columns:
        nombre_norm = base["Nombre del producto"].astype(str).str.strip().str.lower()
    else:
        nombre_norm = pd.Series("", index=base.index)

    mask_dev = (
        motivo_norm.eq("devolucion") |
        nombre_norm.str.contains("devoluc", na=False)
    )

    # TODA devolución se fuerza a ingreso positivo
    base.loc[mask_dev, "Tipo"] = "Ingreso"
    base.loc[mask_dev, "Monto"] = base.loc[mask_dev, "Monto"].abs()

    tipo_upper = base["Tipo"].astype(str).str.strip().str.upper()

    ingresos_d = (
        base.loc[tipo_upper == "INGRESO"]
        .groupby("Fecha", dropna=False)["Monto"]
        .sum()
        .rename("Ingresos")
    )

    egresos_d = (
        base.loc[tipo_upper == "EGRESO"]
        .groupby("Fecha", dropna=False)["Monto"]
        .sum()
        .rename("Egresos")
    )

    resumen_d = pd.concat([ingresos_d, egresos_d], axis=1).fillna(0).reset_index()
    resumen_d = resumen_d.sort_values("Fecha").reset_index(drop=True)

    resumen_d["Saldo del día"] = resumen_d["Ingresos"] - resumen_d["Egresos"]
    resumen_d["Saldo acumulado"] = resumen_d["Saldo del día"].cumsum()

    tot_rows = pd.DataFrame({
        "Fecha": resumen_d["Fecha"],
        "Tipo": "Total",
        "Monto": resumen_d["Saldo acumulado"],
        "Orden": "",
        "Usuario": usuario,
        "Casillero": cas,
        "Estado de Orden": "",
        "Nombre del producto": "",
        "Motivo": "",
        "Fecha de Carga": resumen_d["Fecha"]
    })

    # Alinear columnas
    for col in base.columns:
        if col not in tot_rows.columns:
            tot_rows[col] = ""
    for col in tot_rows.columns:
        if col not in base.columns:
            base[col] = ""

    tot_rows = tot_rows[base.columns]

    salida = pd.concat([base, tot_rows], ignore_index=True)

    salida["_tipo_orden"] = salida["Tipo"].astype(str).str.upper().map({
        "INGRESO": 1,
        "EGRESO": 2,
        "TOTAL": 9
    }).fillna(5)

    salida = salida.sort_values(
        by=["Fecha", "_tipo_orden", "Fecha de Carga"],
        ascending=[True, True, True]
    ).drop(columns="_tipo_orden").reset_index(drop=True)

    return salida









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


def _safe_orden_fecha_id(df: pd.DataFrame, fecha_col="Fecha", id_col="ID_INGRESO") -> pd.DataFrame:
    """
    Orden robusto:
      - Fuerza Fecha a datetime64[ns].
      - Mapea a int64 (ns desde epoch); NaT al final.
      - Fuerza ID a str.
      - Evita el camino interno de Categorical que dispara el TypeError.
    """
    d = df.copy()
    d[fecha_col] = pd.to_datetime(d[fecha_col], errors="coerce")
    d[id_col] = d[id_col].astype(str)

    i8 = d[fecha_col].astype("datetime64[ns]").view("i8")
    i8 = np.where(i8 == np.iinfo("int64").min, np.iinfo("int64").max, i8)  # NaT al final

    d["_k_fecha"] = i8
    d["_k_id"] = d[id_col]

    d = d.sort_values(["_k_fecha", "_k_id"], kind="mergesort")
    return d.drop(columns=["_k_fecha", "_k_id"])




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

    
    
    
    st.markdown("---")
    st.header("3.1) Envios mayoristas (nuevo archivo unificado)")

    envios_may_file = st.file_uploader(
        "Sube el archivo 'Envios mayoristas' (hoja: 'Mayoristas')",
        type=["xls","xlsx"],
        key="envios_mayoristas_uploader"
    )

    envios_may = {}  # dict global para usar después en conciliaciones

    if envios_may_file:
        try:
            df_env = pd.read_excel(envios_may_file, sheet_name="Mayoristas")
        except Exception as e:
            st.error(f"❌ No se pudo leer la hoja 'Mayoristas': {e}")
            df_env = None

        if df_env is not None:
            envios_may = procesar_envios_mayoristas(df_env)
            if not envios_may:
                st.info("No se encontraron filas válidas o casilleros conocidos.")
            else:
                tabs_env = st.tabs(list(envios_may.keys()))
                for tab, key in zip(tabs_env, envios_may):
                    with tab:
                        st.dataframe(envios_may[key], use_container_width=True)
    else:
        st.info("📂 Aún no subes 'Envios mayoristas'")

    

    # 3) Ingresos Nathalia Ospina (CA1633)
    st.header("4) Ingresos Nathalia Ospina (CA1633)")
    nat_files = st.file_uploader(
        "Sube archivos .xls y .csv de Nathalia",
        type=["xls", "xlsx", "csv"],
        accept_multiple_files=True
    )
    
    # Confirmación antes de procesar
    confirm_nat = st.radio(
        "¿Estás seguro de que los archivos de Nathalia son los correctos?",
        ["No, quiero revisar", "Sí, procesar"],
        index=0,  # por defecto "No"
        horizontal=True,
        key="conf_nat"
    )
    
    ingresos_nath = {}
    
    if nat_files and confirm_nat == "Sí, procesar":
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
        df_nat = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    
        ingresos_nath["ingresos_1633"] = df_nat
    
        # Mostrar en la app
        if df_nat.empty:
            st.info("Sin movimientos válidos")
        else:
            st.dataframe(df_nat, use_container_width=True)
    
    elif nat_files and confirm_nat == "No, quiero revisar":
        st.warning("👀 Aún no se procesan los archivos de Nathalia. Revisa y luego marca 'Sí, procesar'.")
    else:
        st.info("📂 No subes archivos de Nathalia")
    
    st.markdown("---")


    # 4) Ingresos Cristian Javier Castro (CA14825)
    st.header("5) Ingresos Cristian Javier Castro (CA14825)")
    cris_files = st.file_uploader(
        "Sube archivos .xls y .csv de Cristian",
        type=["xls", "xlsx", "csv"],
        accept_multiple_files=True,
        key="cris_files_14825"
    )
    
    confirm_cris = st.radio(
        "¿Estás seguro de que los archivos de Cristian son los correctos?",
        ["No, quiero revisar", "Sí, procesar"],
        index=0,
        horizontal=True,
        key="conf_cris"
    )
    
    ingresos_cris = {}
    
    if cris_files and confirm_cris == "Sí, procesar":
        xls_files = [f for f in cris_files if f.name.lower().endswith((".xls", ".xlsx"))]
        csv_files = [f for f in cris_files if f.name.lower().endswith(".csv")]
    
        dfs = []
        if xls_files:
            df_xls = procesar_ingresos_clientes_xls(xls_files, "Cristian Javier Castro", "14825")
            dfs.append(df_xls)
        if csv_files:
            df_csv = procesar_ingresos_clientes_csv(csv_files, "Cristian Javier Castro", "14825")
            dfs.append(df_csv)
    
        df_cris = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        ingresos_cris["ingresos_14825"] = df_cris
    
        if df_cris.empty:
            st.info("Sin movimientos válidos")
        else:
            st.dataframe(df_cris, use_container_width=True)
    
    elif cris_files and confirm_cris == "No, quiero revisar":
        st.warning("👀 Aún no se procesan los archivos de Cristian. Revisa y luego marca 'Sí, procesar'.")
    else:
        st.info("📂 No subes archivos de Cristian")











     # 4) Ingresos Elvis (CA11591)
    st.header("5) Ingresos Elvis (CA11591)")
    elv_files = st.file_uploader(
        "Sube archivos .xls y .csv de Elvis",
        type=["xls", "xlsx", "csv"],
        accept_multiple_files=True
    )
    
    confirm_elv = st.radio(
        "¿Estás seguro de que los archivos de Elvis son los correctos?",
        ["No, quiero revisar", "Sí, procesar"],
        index=0,
        horizontal=True,
        key="conf_elv"
    )
    
    ingresos_elv = {}
    
    if elv_files and confirm_elv == "Sí, procesar":
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
    
        df_elv = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        ingresos_elv["ingresos_11591"] = df_elv
    
        if df_elv.empty:
            st.info("Sin movimientos válidos")
        else:
            st.dataframe(df_elv, use_container_width=True)
    
    elif elv_files and confirm_elv == "No, quiero revisar":
        st.warning("👀 Aún no se procesan los archivos de Elvis. Revisa y luego marca 'Sí, procesar'.")
    else:
        st.info("📂 No subes archivos de Elvis")
    
    st.markdown("---")

        
    # Ingresos Julian Sanchez (CA13608)
    st.header("6) Ingresos Julian Sanchez (CA13608)")
    jul_files = st.file_uploader(
        "Sube archivos .xls y .csv de Julian",
        type=["xls", "xlsx", "csv"],
        accept_multiple_files=True
    )
    
    confirm_jul = st.radio(
        "¿Estás seguro de que los archivos de Julian son los correctos?",
        ["No, quiero revisar", "Sí, procesar"],
        index=0,
        horizontal=True,
        key="conf_jul"
    )
    
    ingresos_jul = {}
    
    if jul_files and confirm_jul == "Sí, procesar":
        xls_files = [f for f in jul_files if f.name.lower().endswith((".xls", ".xlsx"))]
        csv_files = [f for f in jul_files if f.name.lower().endswith(".csv")]
    
        dfs = []
        if xls_files:
            df_xls = procesar_ingresos_clientes_xls(xls_files, "Julian Sanchez", "13608")
            dfs.append(df_xls)
        if csv_files:
            df_csv = procesar_ingresos_clientes_csv(csv_files, "Julian Sanchez", "13608")
            dfs.append(df_csv)
    
        df_jul = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        ingresos_jul["ingresos_13608"] = df_jul
    
        if df_jul.empty:
            st.info("Sin movimientos válidos")
        else:
            st.dataframe(df_jul, use_container_width=True)
    
    elif jul_files and confirm_jul == "No, quiero revisar":
        st.warning("👀 Aún no se procesan los archivos de Julian. Revisa y luego marca 'Sí, procesar'.")
    else:
        st.info("📂 No subes archivos de Julian")
    
    st.markdown("---")

    
    
    # 6) Ingresos Juan Felipe Laverde (CA9680)
    st.header("6) Ingresos Juan Felipe Laverde (CA9680)")
    laverde_files = st.file_uploader(
        "Sube archivos .xls y .csv de Juan Felipe Laverde",
        type=["xls", "xlsx", "csv"],
        accept_multiple_files=True,
        key="uploader_ingresos_9680"
    )
    
    confirm_9680 = st.radio(
        "¿Estás seguro de que los archivos de Juan Felipe Laverde son los correctos?",
        ["No, quiero revisar", "Sí, procesar"],
        index=0,
        horizontal=True,
        key="conf_9680"
    )
    
    ingresos_9680 = {}
    
    if laverde_files and confirm_9680 == "Sí, procesar":
        xls_files = [f for f in laverde_files if f.name.lower().endswith((".xls", ".xlsx"))]
        csv_files = [f for f in laverde_files if f.name.lower().endswith(".csv")]
    
        dfs = []
        if xls_files:
            df_xls = procesar_ingresos_clientes_xls(xls_files, "Juan Felipe Laverde", "9680")
            dfs.append(df_xls)
        if csv_files:
            df_csv = procesar_ingresos_clientes_csv(laverde_files, "Juan Felipe Laverde", "9680")
            dfs.append(df_csv)
    
        df_9680 = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        ingresos_9680["ingresos_9680"] = df_9680
    
        if df_9680.empty:
            st.info("Sin movimientos válidos")
        else:
            st.dataframe(df_9680, use_container_width=True)
    
    elif laverde_files and confirm_9680 == "No, quiero revisar":
        st.warning("👀 Aún no se procesan los archivos de Juan Felipe. Revisa y luego marca 'Sí, procesar'.")
    else:
        st.info("📂 No subes archivos de Juan Felipe Laverde")
    
    st.markdown("---")

    
    
    
    st.header("7) Ingresos Maria Moises (CA1444)")
    moises_files = st.file_uploader(
        "Sube archivos .csv de Maria Moises (Bancolombia)", 
        type=["csv"], 
        accept_multiple_files=True
    )
    
    confirm_moises = st.radio(
        "¿Estás seguro de que los archivos de Maria Moises son los correctos?",
        ["No, quiero revisar", "Sí, procesar"],
        index=0,
        horizontal=True,
        key="conf_moises"
    )
    
    ingresos_moises = {}
    
    if moises_files and confirm_moises == "Sí, procesar":
        df_moises = procesar_ingresos_clientes_csv(
            moises_files, "Maria Moises", "1444"
        )
    
        ingresos_moises["ingresos_1444"] = df_moises
    
        if df_moises.empty:
            st.info("Sin movimientos válidos")
        else:
            st.dataframe(df_moises, use_container_width=True)
    
    elif moises_files and confirm_moises == "No, quiero revisar":
        st.warning("👀 Aún no se procesan los archivos de Maria Moises. Revisa y luego marca 'Sí, procesar'.")
    else:
        st.info("📂 No subes archivos de Maria Moises")
    
    st.markdown("---")

    
    
    
    
    # 5) Conciliaciones
    # 5) Conciliaciones Finales
    st.header("8) Conciliaciones Finales")

    # asegúrate de que la lista incluya el nuevo casillero
    casilleros = ["9444", "14856", "11591", "1444", "1633", "13608", "9680", "14825"]

    conciliaciones = {}
    
    for cas in casilleros:
        key_ing = f"ingresos_{cas}"
    
        # tomar de cada fuente (si existe el dict y la clave)
        ing_j = ingresos_jul.get(key_ing)       if isinstance(ingresos_jul, dict)       else None
        ing_n = ingresos_nath.get(key_ing)      if isinstance(ingresos_nath, dict)      else None
        ing_e = ingresos_elv.get(key_ing)       if isinstance(ingresos_elv, dict)       else None
        ing_m = ingresos_moises.get(key_ing)    if isinstance(ingresos_moises, dict)    else None
        ing_9 = ingresos_9680.get(key_ing)      if isinstance(ingresos_9680, dict)      else None  # NUEVO
        ing_c = ingresos_cris.get(key_ing) if isinstance(ingresos_cris, dict) else None

        
    
        if ing_j is not None and not ing_j.empty:
            inc = ing_j
        elif ing_n is not None and not ing_n.empty:
            inc = ing_n
        elif ing_c is not None and not ing_c.empty:
            inc = ing_c
        elif ing_e is not None and not ing_e.empty:
            inc = ing_e
        elif ing_m is not None and not ing_m.empty:
            inc = ing_m
        elif ing_9 is not None and not ing_9.empty:
            inc = ing_9
        else:
            inc = None

    
        # ... (resto del loop: gmf_df, egr, ext, env, dev, frames, etc.)

        # ------------------ NUEVO: GMF 4x1000 SOLO PARA 1633 ------------------
# ------------------ GMF 4x1000 PARA 1633 Y 1444 ------------------
        gmf_df = None
        if cas in ("1633", "1444"):
            # Elegir de qué DF calcular el GMF (preferimos el ingreso real que se usó)
            base_ing = inc if (inc is not None and not inc.empty) else ing_n
            if base_ing is not None and not base_ing.empty:
                tmp = base_ing.copy()
        
                # Asegurar numérico
                tmp["Monto"] = pd.to_numeric(tmp["Monto"], errors="coerce").fillna(0)
        
                # Tomar SOLO movimientos de tipo Ingreso
                if "Tipo" in tmp.columns:
                    tmp = tmp[tmp["Tipo"].astype(str).str.strip().str.lower() == "ingreso"]
        
                # Evitar doble conteo si ya agregaste una fila GMF en otro paso
                if "Nombre del producto" in tmp.columns:
                    tmp = tmp[~tmp["Nombre del producto"].astype(str).str.contains("4x1000", case=False, na=False)]
        
                gmf_total = round(0.004 * tmp["Monto"].sum(), 2)
        
                if cas == "1633":
                    usuario = "Nathalia Ospina"
                elif cas == "1444":
                    usuario = "Maria Moises"
                else:
                    usuario = "Julian Sanchez"
        
                casillero_val = cas
                        
                # calcular la fecha a usar
                fecha_base = pd.to_datetime(base_ing.get("Fecha", pd.NaT), errors="coerce")
                fecha_gmf = fecha_base.max()
                if pd.isna(fecha_gmf):
                    fecha_gmf = pd.Timestamp.today().normalize()
        
                orden_gmf = f"GMF-4x1000-ACUM-{fecha_gmf.strftime('%Y%m%d')}"
                        
                if gmf_total != 0:
                    cols = list(base_ing.columns)
                    fila = {c: None for c in cols}
        
                    fila.update({
                        "Fecha": pd.Timestamp.today().normalize(),
                        "Tipo": "Egreso",
                        "Monto": gmf_total,
                        "Orden": orden_gmf,
                        "Usuario": usuario,
                        "Casillero": casillero_val,
                        "Estado de Orden": "",
                        "Nombre del producto": "GMF 4x1000 acumulado",
                    })
        
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
    
        # >>> NUEVO: ENVIOS MAYORISTAS por casillero <<<
        env = envios_may.get(f"envios_{cas}") if 'envios_may' in locals() else None

        # 3) Armar la lista de DataFrames válidos
        frames = []
        for df in (inc, egr, ext, env):  # << añade env aquí
            if df is not None and not df.empty:
                frames.append(df)

        if gmf_df is not None and not gmf_df.empty:
            frames.append(gmf_df)

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
    
        # Normalizar TODAS las hojas del histórico
        for nombre_hoja in list(historico.keys()):
            historico[nombre_hoja] = asegurar_columnas_historico(historico[nombre_hoja])
    
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
            # 1) Detectar hoja histórica existente
            hoja = next((h for h in historico if h.startswith(cas)), None)
            if hoja:
                hist_df = asegurar_columnas_historico(historico[hoja].copy())
                combinado = pd.concat([hist_df, dfn], ignore_index=True)
            else:
                hist_df = asegurar_columnas_historico(pd.DataFrame())
                combinado = pd.concat([hist_df, dfn], ignore_index=True)
                hoja = f"{cas} - sin_nombre"
                
            # 2) Dedups y limpiezas
            combinado["Orden"] = (
                combinado["Orden"]
                .astype(str)
                .str.strip()
                .str.replace(".0", "", regex=False)
            )
            
            combinado["Tipo"] = combinado["Tipo"].astype(str).str.strip()
            
            # eliminar duplicados egresos (sin tocar devoluciones que comparten Orden)
            mask_e = combinado["Tipo"].str.upper() == "EGRESO"
            if "Motivo" in combinado.columns:
                mask_dev_e = combinado["Motivo"].astype(str).str.strip().str.lower().str.contains("devoluc", na=False)
            else:
                mask_dev_e = pd.Series(False, index=combinado.index)
            mask_e_dedup = mask_e & ~mask_dev_e
            egrs   = combinado[mask_e_dedup].drop_duplicates(subset=["Orden"], keep="last")
            otros  = combinado[~mask_e_dedup]
            combinado = pd.concat([otros, egrs], ignore_index=True)

            
            # eliminar duplicados ingresos (pero NO los Ingreso_extra)
            # --- deduplicar ingresos (pero NO devoluciones) ---
            if "Motivo" in combinado.columns:
                tipo_norm = combinado["Tipo"].astype(str).str.strip().str.upper()
                motivo_norm = combinado["Motivo"].astype(str).str.strip().str.lower()
            
                es_ingreso = tipo_norm.eq("INGRESO")
                es_ingreso_extra = motivo_norm.eq("ingreso_extra")
                es_devolucion = motivo_norm.str.contains("devoluc", na=False)  # cubre Devolucion / Devolución
            
                # SOLO deduplica ingresos normales (no Ingreso_extra, no Devoluciones)
                mask_ing_base = es_ingreso & ~es_ingreso_extra & ~es_devolucion
            else:
                # Sin Motivo, no deduplicar ingresos para evitar borrar devoluciones
                mask_ing_base = pd.Series(False, index=combinado.index)
            
            ingr = combinado.loc[mask_ing_base].drop_duplicates(subset=["Orden", "Tipo"], keep="last")
            otros = combinado.loc[~mask_ing_base]
            combinado = pd.concat([otros, ingr], ignore_index=True)

            
            # --- deduplicar únicamente Ingreso_extra (si existe 'Motivo') ---
            if "Motivo" in combinado.columns:
                mask_x = (
                    combinado["Tipo"].eq("Ingreso") &
                    combinado["Motivo"].eq("Ingreso_extra")
                )
                # conserva un solo registro por Orden–Motivo
                iex = combinado.loc[mask_x].drop_duplicates(subset=["Orden", "Motivo"], keep="last")
                combinado = pd.concat([combinado.loc[~mask_x], iex], ignore_index=True)
            
            # completar ingresos nulos desde egresos por Orden (cuando aplique)
            mask_n = (combinado["Tipo"] == "Ingreso") & combinado["Monto"].isna()
            for i, row in combinado[mask_n].iterrows():
                o = row["Orden"]
                match = combinado[(combinado["Tipo"] == "Egreso") & (combinado["Orden"] == o)]
                if not match.empty:
                    combinado.at[i, "Monto"] = match["Monto"].iloc[0]
    

                
            # ---------- COMISIÓN QUINCENAL POR TOTALES (SOLO CA1444) ----------
            if cas == "1444":
                import calendar
            
                dfh = combinado.copy()
                dfh["Fecha_dt"] = pd.to_datetime(dfh["Fecha"], errors="coerce").dt.date
                dfh["Monto"] = pd.to_numeric(dfh["Monto"], errors="coerce")
            
                fc_date = pd.to_datetime(fecha_carga, errors="coerce").date()
                y, m, d = fc_date.year, fc_date.month, fc_date.day
            
                meses = {
                    1:"enero",2:"febrero",3:"marzo",4:"abril",5:"mayo",6:"junio",
                    7:"julio",8:"agosto",9:"septiembre",10:"octubre",11:"noviembre",12:"diciembre"
                }
            
                def agregar_comision_rango(dfh_local, ini_date, fin_date, etiqueta):
                    orden_nombre = f"Comision de ({etiqueta})"
            
                    existe = False
                    if "Orden" in dfh_local.columns:
                        existe = existe or dfh_local["Orden"].astype(str).str.lower().eq(orden_nombre.lower()).any()
                    if "Nombre del producto" in dfh_local.columns:
                        existe = existe or dfh_local["Nombre del producto"].astype(str).str.lower().eq(orden_nombre.lower()).any()
            
                    if existe:
                        return dfh_local
            
                    mask = (
                        dfh_local["Tipo"].astype(str).str.upper().eq("TOTAL")
                        & (dfh_local["Fecha_dt"] >= ini_date)
                        & (dfh_local["Fecha_dt"] <= fin_date)
                    )
            
                    serie = pd.to_numeric(dfh_local.loc[mask, "Monto"], errors="coerce")
                    serie = serie[serie < 0]
            
                    if serie.empty:
                        return dfh_local
            
                    comision = float(abs(serie.min()) * 0.015)
            
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
            
                    return pd.concat([dfh_local, nueva], ignore_index=True)
            
                if 1 <= d <= 15:
                    prev_y = y if m > 1 else y - 1
                    prev_m = m - 1 if m > 1 else 12
                    last_prev = calendar.monthrange(prev_y, prev_m)[1]
                    ini = pd.Timestamp(prev_y, prev_m, 16).date()
                    fin = pd.Timestamp(prev_y, prev_m, last_prev).date()
                    etiqueta = f"16-fin {meses[prev_m]} {prev_y}"
                    dfh = agregar_comision_rango(dfh, ini, fin, etiqueta)
            
                if d >= 16:
                    ini = pd.Timestamp(y, m, 1).date()
                    fin = pd.Timestamp(y, m, 15).date()
                    etiqueta = f"1-15 {meses[m]} {y}"
                    dfh = agregar_comision_rango(dfh, ini, fin, etiqueta)
            
                dfh = dfh.drop(columns=["Fecha_dt"], errors="ignore")
                combinado = dfh.copy()
            # ---------- /COMISIÓN QUINCENAL ----------
            
            # ---- Cobros mensuales de contabilidad (parametrizados por casillero) ----
            if cas in COBROS_MENSUALES_CONF:
                cfg = COBROS_MENSUALES_CONF[cas]
                tmp_hist = {hoja: combinado.copy()}
                tmp_hist = aplicar_cobro_contabilidad_mensual(
                    tmp_hist, hoja, cas, usuario, fecha_carga,
                    inicio_yyyymm=cfg["inicio"], monto=cfg["monto"], etiqueta_base="cobro contabilidad"
                )
                combinado = tmp_hist[hoja].copy()
            # -------------------------------------------------------------------------
            
            # ---------- RECÁLCULO FINAL DE TOTALES ----------
            combinado = recalcular_totales_diarios(
                combinado,
                usuario=usuario,
                cas=cas
            )
            historico[hoja] = combinado.copy()
            # ---------- /RECÁLCULO FINAL DE TOTALES ----------
                        
            
            


            # ---------- VALIDACIÓN DE DEVOLUCIONES vs EGRESOS (por Orden) ----------
            # ---------- VALIDACIÓN DE DEVOLUCIONES vs EGRESOS (por Orden) ----------
            df_valid = asegurar_columnas_historico(historico[hoja].copy())
            
            if not df_valid.empty:
                df_valid["Tipo"] = df_valid["Tipo"].astype(str).str.upper()
                df_valid["Orden"] = df_valid["Orden"].astype(str).str.strip()
                df_valid["Monto"] = pd.to_numeric(df_valid["Monto"], errors="coerce")
            
                egresos_por_orden = (
                    df_valid[df_valid["Tipo"] == "EGRESO"]
                    .groupby("Orden")["Monto"].sum(min_count=1)
                )
            
                if "Motivo" in df_valid.columns:
                    motivo_norm_v = df_valid["Motivo"].astype(str).str.strip().str.lower()
                    mask_dev = (df_valid["Tipo"] == "INGRESO") & motivo_norm_v.str.contains("devoluc", na=False)
                else:
                    mask_dev = (df_valid["Tipo"] == "INGRESO") & (
                        df_valid["Nombre del producto"].astype(str).str.lower().str.contains("devoluc", na=False)
                    )
                
                devoluciones_por_orden = (
                    df_valid[mask_dev]
                    .groupby("Orden")["Monto"].sum(min_count=1)
                )
                
                ordenes = sorted(set(devoluciones_por_orden.index) | set(egresos_por_orden.index))
                for o in ordenes:
                    eg = float(egresos_por_orden.get(o, 0.0) or 0.0)
                    dv = float(devoluciones_por_orden.get(o, 0.0) or 0.0)
                
                    if dv > 0 and eg <= 0:
                        msg = f"Devolución con orden inexistente en casillero {cas} — Orden {o}: devuelto ${dv:,.2f} y egresado ${eg:,.2f}."
                        st.error(f"🚨 {msg}")
                        errores_validacion.append(msg)
                    elif dv > eg:
                        exceso = dv - eg
                        msg = f"Devolución excedida en casillero {cas} — Orden {o}: devuelto ${dv:,.2f} > egresado ${eg:,.2f}. Exceso ${exceso:,.2f}."
                        st.error(f"🚨 {msg}")
                        errores_validacion.append(msg)

                        errores_validacion.append(msg)
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
        
        
         
                
        # --- Anexar/actualizar hoja con snapshot crudo unificado "ingresos_correal_completo" ---
# --- Anexar/actualizar hoja con snapshot crudo unificado "ingresos_correal_completo" ---
        SHEET_CORREAL = "ingresos_correal_completo"
        
        try:
            correal_df = st.session_state.get("1444_ingresos_correal_raw", None)
        except Exception:
            correal_df = None
        
        if isinstance(correal_df, pd.DataFrame) and not correal_df.empty:
            df_cor = correal_df.copy()
        
            # Tipos consistentes
            df_cor["Fecha"] = pd.to_datetime(df_cor["Fecha"], errors="coerce")
            df_cor["MontoCOP"] = pd.to_numeric(df_cor["MontoCOP"], errors="coerce")
            for c in ["Tipo","Orden","Usuario","Casillero","Estado de Orden",
                      "Nombre del producto","Archivo_Origen","Banco_Origen","ID_INGRESO"]:
                if c in df_cor.columns:
                    df_cor[c] = df_cor[c].astype(str)
        
            base_cols = [
                "Fecha","Tipo","MontoCOP","Orden","Usuario","Casillero",
                "Estado de Orden","Nombre del producto","Archivo_Origen",
                "Banco_Origen","ID_INGRESO"
            ]
            for c in base_cols:
                if c not in df_cor.columns:
                    df_cor[c] = pd.NA
            df_cor = df_cor[base_cols]
        
            if SHEET_CORREAL in historico:
                old_cor = historico[SHEET_CORREAL].copy()
        
                # Alinear columnas
                all_cols = list(dict.fromkeys(base_cols + [c for c in old_cor.columns if c not in base_cols]))
                for c in all_cols:
                    if c not in old_cor.columns:
                        old_cor[c] = pd.NA
                    if c not in df_cor.columns:
                        df_cor[c] = pd.NA
        
                # Normalizar tipos antes de unir
                old_cor["Fecha"] = pd.to_datetime(old_cor["Fecha"], errors="coerce")
                if "ID_INGRESO" in old_cor.columns:
                    old_cor["ID_INGRESO"] = old_cor["ID_INGRESO"].astype(str)
        
                merged = pd.concat([old_cor[all_cols], df_cor[all_cols]], ignore_index=True)
        
                # Dedup por ID
                if "ID_INGRESO" in merged.columns:
                    merged["ID_INGRESO"] = merged["ID_INGRESO"].astype(str)
                    merged = merged.drop_duplicates(subset=["ID_INGRESO"], keep="first")
        
                # 🚫 NO USAR sort_values(["Fecha","ID_INGRESO"])
                merged = _safe_orden_fecha_id(merged, fecha_col="Fecha", id_col="ID_INGRESO")
        
                historico[SHEET_CORREAL] = merged[all_cols]
            else:
                if "ID_INGRESO" in df_cor.columns:
                    df_cor["ID_INGRESO"] = df_cor["ID_INGRESO"].astype(str)
                    df_cor = df_cor.drop_duplicates(subset=["ID_INGRESO"], keep="first")
        
                df_cor = _safe_orden_fecha_id(df_cor, fecha_col="Fecha", id_col="ID_INGRESO")
        
                historico[SHEET_CORREAL] = df_cor
        # --- /fin ingresos_correal_completo ---
        
                # --- /fin ingresos_correal_completo ---
        
                                
                            
                
                
        
        
        
        
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
        
       # if modo_prueba == "No":
            # Enviar correos por casillero (solo a los configurados)
        #    for cas in st.secrets["zoho"]["recipients"].keys():
         #       obtener_y_enviar_alerta_saldo(historico, str(cas), fecha_carga)
       # else:
        #    st.info("Modo prueba activo: no se enviaron correos.")


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
