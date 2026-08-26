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
import hashlib
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
    """Sube (o sobrescribe) un archivo a Dropbox.

    🛡️ CAPA C: ANTES de sobrescribir el histórico vivo sube una copia de respaldo con
    WriteMode.add (nunca sobrescribe), para que ninguna corrida pueda destruir el estado
    anterior sin dejar rastro recuperable. Si el respaldo no se puede crear, NO se escribe.
    """
    cfg = st.secrets["dropbox"]
    remote = cfg["remote_path"]

    # --- 🛟 respaldo previo del estado vivo (capa C) ---
    try:
        _md, _resp = dbx.files_download(remote)
        ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        carpeta = str(PurePosixPath(remote).parent)
        stem = PurePosixPath(remote).stem
        backup_path = f"{carpeta}/{stem}_backup_{ts}.xlsx"
        dbx.files_upload(_resp.content, backup_path, mode=dropbox.files.WriteMode.add)
        st.info(f"🛟 Respaldo previo creado: `{backup_path}` ({len(_resp.content):,} bytes)")
    except dropbox.exceptions.ApiError as e:
        if _es_not_found(e):
            st.info("🛟 No hay histórico previo en Dropbox: se omite el respaldo (primera subida).")
        else:
            st.error(f"⛔ No se pudo crear el respaldo previo ({e}). NO se escribió el histórico.")
            st.stop()
    except Exception as e:
        st.error(f"⛔ No se pudo crear el respaldo previo ({e}). NO se escribió el histórico.")
        st.stop()

    try:
        dbx.files_upload(
            data,
            remote,
            mode=dropbox.files.WriteMode.overwrite
        )
        st.success("✅ Histórico subido a Dropbox")
    except Exception as e:
        st.error(f"❌ Error subiendo a Dropbox: {e}")


# ═══════════════════════════════════════════════════════════════════════════════════════
# 🛡️ BLINDAJE DE PERSISTENCIA DEL HISTÓRICO — capas A y B
#
# Contexto: main() no reconstruye las hojas desde las fuentes, las ACUMULA sobre el
# histórico que sube el operador y deduplica por 'Orden'. Eso preserva bien lo existente,
# PERO la subida final es un overwrite: si el archivo que subió el operador está rezagado
# respecto a Dropbox, todo lo escrito en el intervalo se pierde en silencio (fue el caso
# del 24-jul-2026, que borró 128 filas robinhood_ cargadas la noche anterior).
#   · CAPA A (guard_frescura_historico): check de concurrencia. Si la salida perdería Orden
#     que hoy existen en Dropbox, se detiene la app en vez de escribir.
#   · CAPA B (preservar_filas_tarjeta): reinyecta las filas de tarjeta del vivo que la
#     corrida actual no trae (no hay extractos de tarjeta en toda corrida).
# Ninguna de las dos toca el dedup, los egresos positivos, el "Saldo al cierre",
# recalcular_totales_diarios, ENVIOS_BLOQUEADOS ni la comisión de 1444.
# ═══════════════════════════════════════════════════════════════════════════════════════

# Prefijos de 'Orden' que identifican filas escritas por los módulos de tarjeta.
TARJETA_ORDEN_RE = r"^(?:amex_|rakuten_|robinhood_|capital_|usbank_|gastoamex|reembolsoamex)"


def _es_not_found(e: Exception) -> bool:
    """True si el ApiError de Dropbox es un 'archivo no encontrado'."""
    try:
        return bool(e.error.is_path() and e.error.get_path().is_not_found())
    except Exception:
        return "not_found" in str(e)


def _leer_historico_vivo() -> dict:
    """Descarga el histórico que AHORA MISMO vive en Dropbox -> {hoja: DataFrame}.

    Devuelve {} si todavía no existe (primera subida). Cualquier otro fallo se propaga:
    sin poder leer el estado vivo no se puede garantizar que la escritura no destruya datos.
    """
    cfg = st.secrets["dropbox"]
    try:
        _md, resp = dbx.files_download(cfg["remote_path"])
    except dropbox.exceptions.ApiError as e:
        if _es_not_found(e):
            return {}
        raise
    return pd.read_excel(io.BytesIO(resp.content), sheet_name=None)


def _norm_orden(serie: pd.Series) -> pd.Series:
    """Normaliza 'Orden' igual que el dedup de main() (línea ~3301)."""
    return serie.astype(str).str.strip().str.replace(".0", "", regex=False)


def _ordenes_significativas(df: pd.DataFrame) -> set:
    """Orden no vacíos de una hoja. Los TOTAL llevan Orden='' y quedan fuera a propósito."""
    if df is None or df.empty or "Orden" not in df.columns:
        return set()
    o = _norm_orden(df["Orden"])
    return set(o[~o.str.lower().isin({"", "nan", "none", "nat", "<na>"})])


def _orden_removible(orden: str) -> bool:
    """True si es LEGÍTIMO que un Orden exista en el vivo y no en la salida:
    envíos bloqueados (se purgan por diseño, línea ~3311) y comisiones quincenales de 1444
    (se recalculan y pueden retirarse cuando la quincena deja de tener Total negativo)."""
    norm = " ".join(str(orden).strip().lower().split())
    return norm in ENVIOS_BLOQUEADOS or norm.startswith("comision de (")


def preservar_filas_tarjeta(historico: dict, vivo=None) -> dict:
    """🛡️ CAPA B — reinyecta desde el histórico VIVO las filas de tarjeta que la salida no trae.

    Una corrida sin extractos de tarjeta no regenera esas filas; si además parte de un
    histórico rezagado, el overwrite las borraría. Se recuperan por prefijo de 'Orden' y se
    recalculan los TOTAL SOLO de las hojas efectivamente tocadas.
    """
    if vivo is None:
        vivo = _leer_historico_vivo()
    if not vivo:
        return historico

    reinyectadas = []
    for hoja, df_vivo in vivo.items():
        if hoja not in historico or df_vivo is None or df_vivo.empty:
            continue
        if "Orden" not in df_vivo.columns:
            continue

        o_vivo = _norm_orden(df_vivo["Orden"])
        mask_tarj = o_vivo.str.match(TARJETA_ORDEN_RE, na=False)
        if not mask_tarj.any():
            continue

        df_out = asegurar_columnas_historico(historico[hoja].copy())
        faltan_mask = mask_tarj & ~o_vivo.isin(_ordenes_significativas(df_out))
        if not faltan_mask.any():
            continue

        faltantes = asegurar_columnas_historico(df_vivo[faltan_mask].copy())
        for c in df_out.columns:
            if c not in faltantes.columns:
                faltantes[c] = ""
        combinado = pd.concat([df_out, faltantes[list(df_out.columns)]], ignore_index=True)

        # Recalcular TOTAL solo en hojas de saldo (las que ya los llevan).
        if combinado["Tipo"].astype(str).str.strip().str.upper().eq("TOTAL").any():
            cas = str(hoja).split(" - ")[0].strip()
            usuarios = combinado["Usuario"].astype(str).str.strip()
            usuarios = usuarios[~usuarios.str.lower().isin({"", "nan", "none"})]
            usuario = usuarios.mode().iloc[0] if not usuarios.empty else ""
            combinado = recalcular_totales_diarios(combinado, usuario=usuario, cas=cas)

        historico[hoja] = combinado
        reinyectadas.append((hoja, int(faltan_mask.sum())))

    if reinyectadas:
        detalle = " · ".join(f"{h}: +{n}" for h, n in reinyectadas)
        st.warning(
            "🛡️ Se reinyectaron filas de TARJETA que el histórico subido no traía y que sí "
            f"existen en Dropbox ({detalle}). Se recalcularon los totales de esas hojas."
        )
    return historico


def guard_frescura_historico(historico: dict) -> None:
    """🛡️ CAPA A — check de concurrencia, JUSTO ANTES de subir.

    Compara el histórico vivo de Dropbox contra lo que se va a escribir. Si el vivo tiene
    hojas u 'Orden' que la salida no tiene, el overwrite borraría datos: se detiene la app.
    """
    vivo = _leer_historico_vivo()
    if not vivo:
        return

    hojas_perdidas = [h for h in vivo if h not in historico]
    perdidas = {}
    for hoja, df_vivo in vivo.items():
        if hoja not in historico:
            continue
        faltan = _ordenes_significativas(df_vivo) - _ordenes_significativas(historico[hoja])
        faltan = sorted(o for o in faltan if not _orden_removible(o))
        if faltan:
            perdidas[hoja] = faltan

    if not hojas_perdidas and not perdidas:
        return

    st.error(
        "⛔ SUBIDA BLOQUEADA — el histórico que se iba a escribir PERDERÍA datos que ahora "
        "mismo existen en Dropbox. Lo más probable es que el archivo que subiste a la app esté "
        "rezagado (alguien escribió el histórico después de que lo descargaste). "
        "Vuelve a descargar el histórico de Dropbox y repite la corrida."
    )
    for h in hojas_perdidas:
        st.error(f"🚨 Desaparecería la hoja completa «{h}»")
    for hoja, ords in perdidas.items():
        st.error(f"🚨 «{hoja}»: se perderían {len(ords)} Orden")
        with st.expander(f"Ver Orden en riesgo — {hoja}"):
            st.write(", ".join(ords[:200]) + (" …" if len(ords) > 200 else ""))
    st.stop()



# — 1) Egresos (Compras) —
@st.cache_data
def procesar_egresos(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    casilleros = ["9444", "14856", "11591", "1444", "1633", "13608", "9680", "14825", "13297"]
    df = df.copy()

    # Fechas y tipos
    # La fecha base de los egresos es la de CREACIÓN de la orden, en hora Colombia
    # (Bogotá, UTC-5). 'Fecha Creación Orden' trae hora real en UTC, así que hay que
    # convertir antes de tomar el día para que las órdenes caigan en la quincena correcta.
    df["Fecha"] = (
        pd.to_datetime(df["Fecha Creación Orden"], errors="coerce", utc=True)
        .dt.tz_convert("America/Bogota")
        .dt.tz_localize(None)
    )
    df["Casillero"] = df["Casillero"].astype(str)

    # Filtrar casilleros manejados
    df = df[df["Casillero"].isin(casilleros)]

    # Cortes por casillero
    cutoff_13608 = pd.Timestamp("2025-09-18")
    cutoff_9680  = pd.Timestamp("2025-11-11")
    cutoff_14825 = pd.Timestamp("2026-02-11")
    cutoff_13297 = pd.Timestamp("2026-07-01")


    # Mantener 13608 solo desde 2025-09-18 y 9680 solo desde 2025-11-11
    df = df[
        ((df["Casillero"] != "13608") | (df["Fecha"] >= cutoff_13608)) &
        ((df["Casillero"] != "9680")  | (df["Fecha"] >= cutoff_9680)) &
        ((df["Casillero"] != "14825") | (df["Fecha"] >= cutoff_14825)) &
        ((df["Casillero"] != "13297") | (df["Fecha"] >= cutoff_13297))
    ]


    # Formatos y normalizaciones
    df["Fecha"] = df["Fecha"].dt.strftime("%Y-%m-%d")
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

    # Seleccionar columnas finales (la fecha ya es 'Fecha' = Fecha Creación Orden en hora Colombia)
    df = df[
        ["Fecha","Tipo","Monto","Orden","TRM","Usuario","Casillero","Estado de Orden","Nombre del producto"]
    ]

    # Alias de usuario conocido
    df.loc[df["Casillero"] == "9444", "Usuario"] = "Maira Alejandra Paez"
    df.loc[df["Casillero"] == "9680", "Usuario"] = "Juan Felipe Laverde"
    df.loc[df["Casillero"] == "14825", "Usuario"] = "Cristian Javier Castro"
    df.loc[df["Casillero"] == "13297", "Usuario"] = "Christian Trujillo"
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






# ──────────────────────────────────────────────────────────────────────────────
# Envíos BLOQUEADOS (DOBLE COBRO detectado, CA1444 Maria Moises).
# Estos 23 sub-envíos son el desglose de 4 "encabezados" que YA cobran el total
# del grupo; cobrarlos además individualmente duplica el cargo. Se conservan SOLO
# los encabezados (95079, 95390, 95401, 95412). Los sub-envíos de abajo:
#   - NUNCA se cargan desde un archivo de envíos nuevo (procesar_envios_mayoristas)
#   - se ELIMINAN del histórico en cada corrida (paso 6, antes de recalcular totales)
# ──────────────────────────────────────────────────────────────────────────────
ENVIOS_BLOQUEADOS_NUMS = {
    # Grupo 1  (encabezado Envio 95079 — SE CONSERVA)
    "95954", "95955", "95956", "95957", "95958", "95959", "95960",
    # Grupo 2  (encabezado Envio 95390 — SE CONSERVA)
    "95925", "95926", "95927", "95928", "95929", "95930",
    # Grupo 3  (encabezado Envio 95401 — SE CONSERVA)
    "95940", "95941", "95942", "95943",
    # Grupo 4  (encabezado Envio 95412 — SE CONSERVA)
    "95915", "95916", "95917", "95918", "95919", "95920",
}
# Órdenes normalizadas a bloquear, p.ej. "envio 95954"
ENVIOS_BLOQUEADOS = {f"envio {n}" for n in ENVIOS_BLOQUEADOS_NUMS}


def _es_envio_bloqueado(orden_series: pd.Series) -> pd.Series:
    """True donde la Orden corresponde a un envío bloqueado (doble cobro).
    Normaliza a minúsculas y colapsa espacios antes de comparar."""
    norm = (
        orden_series.astype(str)
        .str.strip()
        .str.lower()
        .str.split()
        .str.join(" ")
    )
    return norm.isin(ENVIOS_BLOQUEADOS)


@st.cache_data
def procesar_envios_mayoristas(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Lee la hoja 'Mayoristas' (Envios mayoristas) y devuelve un dict con un DF por casillero.
    Normaliza Fecha de dd-mm-YYYY -> YYYY-MM-DD para que sea consistente con el resto.
    """
    casilleros_validos = {"9444", "14856", "11591", "1444", "1633", "13608", "9680", "14825", "13297"}

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

    # 📦 PESO EN LIBRAS (para TARIFA_ENVIO_1444). Sale de la columna PESO del export del portal,
    # copiada a la hoja "Mayoristas" del archivo intermedio. Se aceptan los alias 'Peso_lb'/'Peso'
    # y se normaliza al nombre canónico del histórico (COL_PESO_HIST). Si el archivo no trae peso,
    # la columna queda vacía y la tarifa hace FAIL-SOFT (deja el Monto del portal intacto).
    _col_peso = next(
        (c for c in df2.columns if str(c).strip().casefold() in {"peso_lb", "peso"}),
        None
    )
    df2[COL_PESO_HIST] = (
        pd.to_numeric(df2[_col_peso], errors="coerce") if _col_peso else np.nan
    )

    # 💱 TRM del envío: la que usó el portal para convertir el VALOR en USD a COP. Se guarda
    # como rastro de auditoría (poder reconstruir los USD de cualquier envío desde el histórico,
    # igual que ya se hace con las filas de tarjeta). Si el archivo no la trae, queda vacía.
    # ⚠️ Para los envíos de 1444 que pasan por aplicar_tarifa_envio_por_peso, esta TRM se
    # SOBRESCRIBE después con la TRM oficial de _amex_trm_dia, que es la que de verdad se cobró.
    # NINGÚN consumidor de la columna TRM se ve afectado: _indice_trm_historico filtra por
    # Orden.startswith('amex_'/'rakuten_'/...), agregar_incentivo_amex filtra por Motivo de
    # tarjeta, y el GMF toma la TRM de los INGRESOS. Los envíos no entran en ninguno.
    _col_trm = next((c for c in df2.columns if str(c).strip().casefold() == "trm"), None)
    df2["TRM"] = pd.to_numeric(df2[_col_trm], errors="coerce") if _col_trm else np.nan

    # Filtrar filas válidas y casilleros conocidos
    df2 = df2.dropna(subset=["Fecha", "Monto"])
    df2 = df2[df2["Casillero"].isin(casilleros_validos)].copy()

    # 🚫 Envíos bloqueados (doble cobro): NUNCA cargarlos desde un archivo nuevo
    df2 = df2[~_es_envio_bloqueado(df2["Orden"])].copy()

    # Orden de columnas
    cols = ["Fecha","Tipo","Monto","Orden","Usuario","Casillero","Motivo","Nombre del producto",
            COL_PESO_HIST, "TRM"]
    df2 = df2[cols]

    # Dict por casillero
    salida = {}
    for cas in sorted(df2["Casillero"].unique()):
        salida[f"envios_{cas}"] = df2[df2["Casillero"] == cas].reset_index(drop=True)

    return salida


# ──────────────────────────────────────────────────────────────────────────────
# LISTA DE EXCLUSIÓN "TARJETAS COBRADAS" (anti-doble-cobro, defensa PRINCIPAL).
# Archivo en Dropbox (misma carpeta del histórico): tarjetas_cobradas.xlsx, hoja "cobradas",
# UNA fila por transacción YA COBRADA al mayorista antes del cargue 1-a-1, con su Orden
# EXACTO (amex_<Reference> / rakuten_<hash>) + columnas de auditoría. Se generó UNA VEZ con
# generar_tarjetas_cobradas.py desde el Excel de cobrados congelado (20260710) — los cobros
# posteriores entran por el flujo 1-a-1 normal y NO requieren mantener esta lista.
#   - procesar_amex / procesar_rakuten EXCLUYEN toda transacción cuyo Orden esté en la lista
#     (misma filosofía que ENVIOS_BLOQUEADOS: lista explícita de Orden vetados).
#   - Si la lista NO se puede leer -> NO se procesa nada (st.stop en la UI): procesar sin
#     lista recobraría todo lo ya cobrado.
#   - ROL DE LA FECHA DE CORTE (cambio de diseño): AMEX_FECHA_DESDE / RAKUTEN_FECHA_DESDE ya
#     NO son el filtro principal anti-doble-conteo; son solo un límite de sanidad para no
#     procesar historia irrelevante. La decisión cobrar/no-cobrar la toma la LISTA. Una
#     transacción vieja (>= corte) que NO esté en la lista SÍ entra: es una compra tardía
#     nunca cobrada (regla de negocio: NUNCA dejar de cobrar).
# ──────────────────────────────────────────────────────────────────────────────
TARJETAS_COBRADAS_FILENAME = "tarjetas_cobradas.xlsx"


@st.cache_data(ttl=600)  # cache 10 min: no re-descargar de Dropbox en cada rerun
def cargar_tarjetas_cobradas():
    """Lee de Dropbox la lista de exclusión y devuelve (set de Orden ya cobrados, DataFrame de
    'pendientes_rematch', DataFrame completo de 'cobradas'). Los pendientes son cobros REALES
    aún sin Orden (pre-asiento): el escudo también los excluye, por match fecha+monto
    (+CardMember en Amex). El 3er valor son los cobros CON SUS ATRIBUTOS, para la segunda
    barrera anti-recobro (ver bloque 'SEGUNDA BARRERA'). PROPAGA la excepción si el archivo no
    existe / no se puede leer / está vacío: el caller debe hacer st.stop() — sin lista NO se
    procesa (se recobraría)."""
    cfg = st.secrets["dropbox"]
    path = str(PurePosixPath(cfg["remote_path"]).parent / TARJETAS_COBRADAS_FILENAME)
    _, res = dbx.files_download(path)
    xls = pd.ExcelFile(io.BytesIO(res.content))
    df = pd.read_excel(xls, sheet_name="cobradas")
    if "Orden" not in df.columns:
        raise ValueError(f"{TARJETAS_COBRADAS_FILENAME}: falta la columna 'Orden' en la hoja 'cobradas'.")
    ordenes = set(df["Orden"].astype(str).str.strip()) - {"", "nan", "None"}
    if not ordenes:
        raise ValueError(f"{TARJETAS_COBRADAS_FILENAME}: la hoja 'cobradas' está vacía.")
    try:
        pendientes = pd.read_excel(xls, sheet_name="pendientes_rematch")
    except Exception:
        pendientes = pd.DataFrame()  # sin hoja de pendientes -> escudo solo por Orden
    return ordenes, pendientes, df


def _cobradas_info(msg: str):
    """st.info si Streamlit está disponible; en dry-run (sin st) no rompe."""
    try:
        st.info(msg)
    except Exception:
        pass


def _cobradas_warn(msg: str):
    """st.warning si Streamlit está disponible; en dry-run (sin st) no rompe."""
    try:
        st.warning(msg)
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════════════
# 🛡️ SEGUNDA BARRERA ANTI-RECOBRO — POR ATRIBUTOS (independiente del hash)
#
# PROBLEMA (verificado 2026-08-06): el anti-recobro dependía SOLO del 'Orden'. Robinhood
# re-expide la MISMA transacción con la FECHA corrida un día al asentar (6 cobros de mayo
# cambiaron de día entre dos descargas del CSV: Tory Burch 06→07-may, Costco 05→06-may,
# Shein 02→01-may…). Como 'Date' entra en el hash, el Orden cambia -> la entrada de la
# lista queda HUÉRFANA -> la transacción YA COBRADA vuelve a entrar y se recobra.
# En ese caso solo la ventana manual de Robinhood (≤22-jun) las marcó; una transacción de
# julio en adelante no habría tenido ninguna red.
#
# SOLUCIÓN: además del Orden, comparar ATRIBUTOS que NO dependen del hash. La lista guarda
# por cada cobro: merchant_norm, usd_abs, fecha_attr, card_norm, casillero (columnas que
# agrega enriquecer_tarjetas_cobradas.py). Una entrante se excluye si empata con un cobro
# por casillero + |USD| EXACTO + merchant normalizado + fecha dentro de ±3 días.
#
# 🔒 SOLO COMPITEN LOS COBROS HUÉRFANOS — clave para no bloquear compras legítimas:
#   un cobro cuyo Orden el extracto SIGUE generando ya hace su trabajo en la barrera 1, así
#   que se excluye del universo de esta barrera. Sin esto, un cobro vivo bloquearía por
#   atributos a una SEGUNDA compra real idéntica (mismo día/monto/comercio), que es un caso
#   real y frecuente (compras por ítem del mismo carrito).
#   Además, un cobro cuya fecha cae fuera del rango del extracto (±3 días) tampoco compite:
#   si el extracto no cubre esa fecha no se puede saber si su Orden se habría generado.
#
# CONSUMO 1:1: cada cobro huérfano tapa como máximo UNA entrante. Dos compras reales
# idénticas con un solo cobro huérfano -> una se excluye y la otra ENTRA (se cobra).
#
# DETERMINISTA: entrantes en orden (fecha, Orden); candidatos por (|Δfecha|, fecha, Orden).
#
# ⚠️ NO reemplaza al Orden para IDENTIDAD/dedup: el Orden sigue siendo el ID de la fila y el
# dedup por 'Orden' de main() no se toca. Esta es una barrera ADICIONAL solo anti-recobro.
# ⚠️ Un cobro SIN merchant_norm en la lista NO participa (casillero+USD+fecha sin merchant
# generaría falsos positivos que dejarían de cobrar compras reales).
#
# 🔏 SIGNO EN LA LLAVE (agregado 2026-08-10 por Capital). La llave era (casillero, merchant, |USD|)
# — CIEGA AL SIGNO. En Amex/Rakuten/Robinhood eso no molesta porque el reembolso se nombra distinto
# ("Refund from X" / "Refund: X"), pero en Capital One la Description de un reembolso es IDÉNTICA a
# la de su compra y el monto también: 16 de los 20 reembolsos del extracto caen a ≤3 días de su
# propia compra. Sin signo, un cobro-compra huérfano taparía al reembolso entrante (y viceversa).
# Ahora la lista puede traer una columna 'signo' ("Egreso"/"Ingreso") y la entrante su '_tipo_attr':
# el cobro solo compite si los signos coinciden. RETROCOMPATIBLE: 'signo' vacío o ausente = comodín,
# así que las 2.221 entradas amex/rakuten/robinhood se comportan EXACTAMENTE igual que antes.
# ══════════════════════════════════════════════════════════════════════════════════════
ANTIRECOBRO_ATTR_DIAS = 3          # ventana de fecha para considerar "la misma transacción"
ANTIRECOBRO_ATTR_ACTIVO = True     # 🚦 kill switch: False -> solo barrera por Orden (como antes)
ANTIRECOBRO_ATTR_COLS = ("merchant_norm", "usd_abs", "fecha_attr")


def _aviso_barrera_atributos(cobrados_df) -> None:
    """Hace VISIBLE en la UI si la segunda barrera está activa. Una lista sin las columnas de
    atributos (o con pocos merchant) deja el cargue con la protección de antes — hay que
    saberlo ANTES de cargar, no descubrirlo tras un recobro."""
    if not ANTIRECOBRO_ATTR_ACTIVO:
        st.warning("⚠️ Segunda barrera anti-recobro DESACTIVADA (ANTIRECOBRO_ATTR_ACTIVO=False): "
                   "solo protege el Orden. Una transacción re-fechada por el emisor se recobraría.")
        return
    faltan = [c for c in ANTIRECOBRO_ATTR_COLS
              if cobrados_df is None or c not in getattr(cobrados_df, "columns", [])]
    if faltan:
        st.warning(
            f"⚠️ La lista de exclusión NO trae las columnas de atributos ({', '.join(faltan)}): "
            f"la segunda barrera anti-recobro queda INACTIVA y solo protege el Orden. "
            f"Corre `enriquecer_tarjetas_cobradas.py` y sube la lista enriquecida a Dropbox."
        )
        return
    _n = len(cobrados_df)
    _con = int((cobrados_df["merchant_norm"].astype(str).str.strip() != "").sum())
    st.caption(f"🛡️ Segunda barrera anti-recobro (atributos) ACTIVA: {_con}/{_n} cobros con "
               f"merchant utilizable, ventana ±{ANTIRECOBRO_ATTR_DIAS} días.")


def _cobros_huerfanos_attr(cobrados_df, tarjeta: str, ordenes_extracto: set, rango) -> list:
    """Cobros de 'tarjeta' que perdieron su Orden: el extracto actual NO lo genera, su fecha
    cae dentro del rango cubierto por el extracto (±ANTIRECOBRO_ATTR_DIAS) y traen los
    atributos necesarios. Son los ÚNICOS que pueden excluir por atributos."""
    if cobrados_df is None or not len(cobrados_df):
        return []
    d = cobrados_df.copy()
    d.columns = [str(c).strip() for c in d.columns]
    req = ("Orden", "tarjeta", "casillero", "merchant_norm", "usd_abs", "fecha_attr")
    if any(c not in d.columns for c in req):
        return []          # lista sin enriquecer -> barrera inactiva (el caller avisa)
    d = d[d["tarjeta"].astype(str).str.strip().str.lower() == tarjeta].copy()
    if not len(d):
        return []
    d["_f"] = pd.to_datetime(d["fecha_attr"], errors="coerce")
    d["_m"] = d["merchant_norm"].astype(str).map(_norm_merchant)
    d["_u"] = pd.to_numeric(d["usd_abs"], errors="coerce").round(2)
    d["_c"] = d["casillero"].map(_cas_str)
    d = d[d["_f"].notna() & d["_u"].notna() & (d["_m"].str.strip() != "")]
    # 🔏 SIGNO (opcional, ver bloque de arriba): columna 'signo' con "Egreso"/"Ingreso". Las
    # entradas viejas (amex/rakuten/robinhood) NO la traen -> "" = comodín, comportamiento idéntico
    # al de antes. Solo Capital la puebla.
    d["_s"] = (d["signo"].astype(str).str.strip().str.title()
               if "signo" in d.columns else "")
    d.loc[~d["_s"].isin(["Egreso", "Ingreso"]), "_s"] = ""
    # huérfano = su Orden ya no lo genera el extracto
    d = d[~d["Orden"].astype(str).str.strip().isin(ordenes_extracto)]
    if rango is not None and len(d):
        ini, fin = rango
        tol = pd.Timedelta(days=ANTIRECOBRO_ATTR_DIAS)
        d = d[(d["_f"] >= ini - tol) & (d["_f"] <= fin + tol)]
    return [
        {"orden": str(r["Orden"]).strip(), "fecha": r["_f"], "merch": r["_m"],
         "usd": float(r["_u"]), "cas": r["_c"], "signo": r["_s"]}
        for _, r in d.sort_values(["_f", "Orden"], kind="mergesort").iterrows()
    ]


def _excluir_por_atributos(df, cobrados_df, tarjeta: str, ordenes_extracto: set, rango,
                           etiqueta: str):
    """🛡️ Barrera 2. 'df' debe traer _fecha (Timestamp), _usd, _cas, _orden y _merch_attr
    (merchant normalizado, sin prefijo de reembolso). Devuelve la lista de índices a excluir;
    avisa por st.warning con el detalle (entrante vs cobro y la diferencia de días)."""
    if not ANTIRECOBRO_ATTR_ACTIVO or df is None or not len(df):
        return []
    huerf = _cobros_huerfanos_attr(cobrados_df, tarjeta, ordenes_extracto, rango)
    if not huerf:
        return []
    # índice de cobros huérfanos por (casillero, merchant, usd) — consumo 1:1
    por_k: dict = {}
    for h in huerf:
        por_k.setdefault((h["cas"], h["merch"], round(h["usd"], 2)), []).append(h)
    usados, drop, detalle = set(), [], []
    tol = pd.Timedelta(days=ANTIRECOBRO_ATTR_DIAS)
    _hay_signo = "_tipo_attr" in df.columns
    orden_entrantes = df.sort_values(["_fecha", "_orden"], kind="mergesort").index
    for i in orden_entrantes:
        r = df.loc[i]
        k = (_cas_str(r["_cas"]), _norm_merchant(r["_merch_attr"]), round(float(r["_usd"]), 2))
        # 🔏 el cobro solo compite si su signo coincide con el de la entrante. Un cobro sin signo
        # ("" = entradas viejas) sigue siendo comodín: amex/rakuten/robinhood no cambian.
        _tipo = str(r["_tipo_attr"]).strip() if _hay_signo else ""
        cands = [h for h in por_k.get(k, [])
                 if h["orden"] not in usados and abs(h["fecha"] - r["_fecha"]) <= tol
                 and (not h["signo"] or not _tipo or h["signo"] == _tipo)]
        if not cands:
            continue
        # el más cercano en fecha; desempate determinista por (fecha, Orden)
        elegido = sorted(cands, key=lambda h: (abs(h["fecha"] - r["_fecha"]), h["fecha"], h["orden"]))[0]
        usados.add(elegido["orden"])
        drop.append(i)
        detalle.append(
            f"{r['_fecha']:%Y-%m-%d} USD {float(r['_usd']):.2f} {str(r['_merch_attr'])[:26]} "
            f"[{r['_orden']}] ≡ cobro {elegido['orden']} del {elegido['fecha']:%Y-%m-%d} "
            f"({int((r['_fecha'] - elegido['fecha']).days):+d}d)"
        )
    if drop:
        _cobradas_warn(
            f"🛡️ {etiqueta}: {len(drop)} transacción(es) YA COBRADA(S) detectadas POR ATRIBUTOS "
            f"(su Orden cambió entre descargas — el emisor la re-fechó/re-expidió) — EXCLUIDAS "
            f"del cargue, NO se recobran: " + "; ".join(detalle[:15])
            + (" …" if len(detalle) > 15 else "")
        )
    return drop


@st.cache_data(ttl=600)  # cache 10 min: no re-descargar de Dropbox en cada rerun
def cargar_hist_tarjetas():
    """Lee de Dropbox (SOLO LECTURA) el histórico vigente y devuelve UN DataFrame con las filas
    de tarjeta ya cargadas (Orden amex_* / rakuten_* / robinhood_* / capital_*) de todas las hojas. Sirve
    únicamente para darle a un reembolso la TRM de su compra original cuando esa compra ya no
    está en el extracto. NO es una defensa anti-doble-cobro: si falla, el cargue continúa con
    el comportamiento anterior (TRM del día del reembolso) — por eso el caller ignora el error.

    🐛 FIX 2026-08-06: faltaba 'robinhood_' — los Refund de Robinhood cuya compra original ya
    no estaba en el extracto NO encontraban su TRM en el histórico (índice vacío) y caían al
    fallback silencioso (TRM del día del reembolso), dejando un residuo en COP a cargo del
    mayorista. Robinhood es la tarjeta con más filas cargadas del histórico (128), así que el
    índice ahora sí aporta."""
    cfg = st.secrets["dropbox"]
    _, res = dbx.files_download(cfg["remote_path"])
    hojas = pd.read_excel(io.BytesIO(res.content), sheet_name=None)
    partes = []
    for _dfh in hojas.values():
        if "Orden" not in _dfh.columns:
            continue
        _o = _dfh["Orden"].astype(str).str.strip()
        _sel = _dfh[_o.str.startswith(("amex_", "rakuten_", "robinhood_", "capital_", "usbank_"))]
        if len(_sel):
            partes.append(_sel)
    return pd.concat(partes, ignore_index=True) if partes else pd.DataFrame()


def _hist_tarjetas_para_trm():
    """Envoltorio NO bloqueante de cargar_hist_tarjetas(): si Dropbox falla devuelve None y el
    cargue sigue igual que antes (los reembolsos sin compra en el extracto caen al fallback con
    warning). Nunca detiene el proceso: esto mejora la conversión, no protege contra recobro."""
    try:
        return cargar_hist_tarjetas()
    except Exception as e:
        _cobradas_info(f"ℹ️ No se pudo leer el histórico para la TRM de reembolsos ({e}); "
                       f"los reembolsos sin compra en el extracto usarán la TRM de su día.")
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Cargue "Tarjeta Amex" 1-a-1: cada transacción del extracto -> UNA fila propia en el
# histórico (ya NO se acumula por día), lista para entrar a conciliacion_<cas>.
#   - Solo estos 3 Card Members se cargan (el resto se IGNORA):
#       PAULA HERRERA -> 11591 ; JUAN P CORREAL -> 1444 ; JULIAN SANCHEZ -> 13608
#   - Amount > 0 = gasto     -> Egreso  (Monto POSITIVO, como el resto del histórico)
#     Amount < 0 = reembolso REAL de merchant -> Ingreso (Monto = abs).
#       * De los negativos se EXCLUYEN (no entran ni como ingreso ni como egreso) los
#         PAGOS a la tarjeta (los hace Encargomio) y los CRÉDITOS Amazon que no son
#         reembolso de compra. Ver AMEX_PAGO_PATTERNS / AMEX_CREDITO_EXCLUIR.
#   - Orden POR TRANSACCIÓN = "amex_<Reference>". 'Reference' es el ID nativo de Amex
#     (entero de 18 dígitos: 3|año|día-del-año del asiento|serial), 100% poblado, ÚNICO
#     y ESTABLE entre descargas (verificado con 2 snapshots reales, 371/371 idénticos).
#     Con el dedup existente por Orden (keep="last") el cargue es IDEMPOTENTE: recargar
#     el mismo extracto reemplaza filas idénticas (no-op) y una compra TARDÍA (asienta
#     hasta ~27 días después) entra apenas aparezca en un export, sin importar cuándo
#     se cargue -> NUNCA se deja de cobrar, NUNCA se duplica.
#     *** Sin 'Reference' válido en alguna fila, o con Reference repetido en el archivo,
#         procesar_amex LEVANTA ValueError (fail-loud: nunca inventa IDs). ***
#   - USD->COP con la TRM del día de la compra (datos.gov.co, mcec-87by) + 125 COP fijo.
#     *** SIN TRM de respaldo: si falta la TRM de algún día con movimiento,
#         procesar_amex LEVANTA ValueError con la lista de días (nunca inventa). ***
#   - Etiqueta en 'Nombre del producto' (incluye el Description del merchant);
#     tag 'Tarjeta Amex' en 'Motivo'.
# ──────────────────────────────────────────────────────────────────────────────
AMEX_CARD_MAP = {
    "PAULA HERRERA": "11591",
    "JUAN P CORREAL": "1444",
    "JULIAN SANCHEZ": "13608",
    # 🔁 K LOPEZ VELANDIA (Kelly) COMPRA PARA 1444 (Maria Moises): sus compras Amex son egresos
    # de 1444 — corrección de regla de negocio 2026-07-22 (antes se ignoraba, fue un error de
    # especificación). Su tarjeta (-23003) aparece en el extracto como "K LOPEZ VELANDIA"; se
    # agrega también la variante "KELLY P LOPEZVELANDIA" por si Amex la etiquetara distinto
    # (la normalización upper+colapso-espacios NO unifica ambas grafías -> ambas explícitas).
    "K LOPEZ VELANDIA": "1444",
    "KELLY P LOPEZVELANDIA": "1444",
    # ⚠️ Sus PAGOS a la tarjeta ("THANK YOU"/Category vacía) y CRÉDITOS Amazon (AMAZON PAY YOUR
    # CHARGES / WITH POINTS) siguen EXCLUIDOS por el filtro defensivo de negativos de más abajo:
    # Kelly es la TITULAR que paga la tarjeta, esos negativos NO son reembolsos de 1444.
}
AMEX_USUARIOS = {"11591": "Paula Herrera", "1444": "Maria Moises", "13608": "Julian Sanchez"}
AMEX_TRM_SPREAD = 125  # COP fijo que se suma a la TRM del día

# ── Blindaje defensivo de los NEGATIVOS (Amount < 0) ──────────────────────────
# Las TC son de Encargomio, amparadas a los mayoristas: el mayorista compra ->
# Encargomio paga la tarjeta -> Encargomio le cobra el gasto. Por eso, de los
# negativos SOLO el reembolso real de un merchant es Ingreso del mayorista. Se
# EXCLUYEN (ni ingreso ni egreso):
#   a) PAGOS a la tarjeta (los hace Encargomio): Description contiene "THANK YOU"
#      O Category vacía/NaN. En los extractos ambas señales son 100% equivalentes
#      (pago => "MOBILE/ONLINE PAYMENT - THANK YOU" con Category en blanco); se
#      usan las dos con OR por redundancia -> "Category vacía = pago".
#   b) CRÉDITOS Amazon que NO son reembolso de compra (liquidación / puntos):
#      Description contiene "AMAZON PAY YOUR CHARGES" o "AMAZON PAY WITH POINTS".
# Comparación case-insensitive. Solo afecta a Amount < 0; los Egreso (Amount > 0)
# no se tocan. Hoy el impacto es $0 (esos negativos están bajo un Card Member no
# mapeado, ya descartado); es blindaje para cuando cambie la estructura de tarjetas.
AMEX_PAGO_PATTERNS = ["THANK YOU"]
AMEX_CREDITO_EXCLUIR = ["AMAZON PAY YOUR CHARGES", "AMAZON PAY WITH POINTS"]

# ⚠️ CA1444 / COMISIÓN QUINCENAL.
#   POLÍTICA ACTUAL -> True: el gasto Amex de 1444 SÍ cuenta en la base de la comisión quincenal
#     (Amex baja el saldo -> sube la comisión). Con True el path de 1444 NO ejecuta el stash;
#     las filas Amex entran natural al recálculo y a la comisión.
#   False: aísla las filas Amex de la base de comisión (se ENFORCEA envolviendo el bloque de
#     comisión con stash/reincorporación, SIN modificar su lógica). El mecanismo se deja en el
#     código para poder volver a False si algún día cambia la política.
AMEX_AFECTA_COMISION_1444 = True

# 🚦 FECHA DE CORTE del cargue Amex — DISEÑO FINAL (3 reglas, decisión 2026-07-16):
#   1. EL HISTÓRICO DE COBRADOS MANDA: lo que esté en tarjetas_cobradas.xlsx (hoja
#      "cobradas" por Orden exacto, u hoja "pendientes_rematch" por fecha+monto+CardMember)
#      NO se vuelve a cobrar, nunca. Lista OBLIGATORIA: sin ella no se procesa (st.stop).
#   2. CORTE = ÚLTIMO MES: transacciones con FECHA DE COMPRA < corte se IGNORAN por completo
#      (historia vieja, ya liquidada a mano por el backoffice; ni se cobra ni se mira).
#   3. TODO LO NUEVO SE TOMA: fecha >= corte y fuera de lista/pendientes -> ENTRA y se cobra.
#      El dedup por Orden evita duplicar entre recargas.
#   - None -> INACTIVO (kill switch de emergencia: no se procesa nada).
AMEX_FECHA_DESDE = "2026-06-16"


def _amex_norm_cardmember(s) -> str:
    """Normaliza Card Member: MAYÚSCULAS + colapsa espacios dobles."""
    return " ".join(str(s).strip().upper().split())


def _amex_trm_dia(fecha_iso: str, _cache: dict):
    """TRM oficial (datos.gov.co, mcec-87by) VIGENTE en 'fecha_iso' (YYYY-MM-DD) + AMEX_TRM_SPREAD.
    Consulta por RANGO (vigenciadesde <= día <= vigenciahasta) para cubrir fines de semana/festivos
    (el filtro por vigenciadesde exacta de procesar_ingresos_extra devuelve vacío esos días).
    Devuelve float o None si no se encontró. Cachea por fecha."""
    if fecha_iso in _cache:
        return _cache[fecha_iso]
    trm = None
    try:
        ds = f"{fecha_iso}T00:00:00.000"
        url = (
            "https://www.datos.gov.co/resource/mcec-87by.json"
            f"?$where=vigenciadesde<='{ds}' AND vigenciahasta>='{ds}'"
        )
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if data and isinstance(data, list) and "valor" in data[0]:
            trm = float(data[0]["valor"]) + AMEX_TRM_SPREAD
    except Exception:
        trm = None
    _cache[fecha_iso] = trm
    return trm


# ──────────────────────────────────────────────────────────────────────────────
# REEMBOLSO CON LA TRM DE SU COMPRA ORIGINAL (neteo exacto en COP).
# PROBLEMA: un reembolso convertido con la TRM de SU día no cancela la compra que revierte
# (caso real: 2 compras eBay 15-jul TRM 3.377,11 vs sus 2 reembolsos 17-jul TRM 3.346,41 ->
# residuo de 41.350 COP a cargo del mayorista, que no compró nada).
# SOLUCIÓN: al reembolsar, se busca la COMPRA ORIGINAL y se usa SU TRM -> el Monto COP del
# reembolso queda idéntico al de la compra y el neto da exactamente 0.
#   - Universo de búsqueda: TODAS las compras del extracto, SIN filtrar por corte ni por lista
#     de cobradas (la compra original puede ser vieja o ya cobrada). Si no está en el extracto,
#     se busca en el HISTÓRICO ya cargado (filas amex_/rakuten_ de gasto) y se usa la TRM
#     guardada en esa fila.
#   - 🔒 SEGMENTACIÓN OBLIGATORIA: una candidata del EXTRACTO debe ser del MISMO Card Member;
#     una candidata del HISTÓRICO, del MISMO casillero (el histórico no guarda Card Member).
#     Sin esto un reembolso podría tomar la TRM de una compra idéntica de OTRO mayorista
#     (en los extractos reales hay 4 claves (merchant,USD) compartidas entre casilleros).
#   - REEMBOLSO TOTAL (monto exacto), en DOS PASADAS ordenadas — para TODOS los reembolsos se
#     agota la pasada 1 antes de empezar la 2, así el consumo 1:1 respeta la señal más fuerte:
#       Pasada 1: mismo Card Member + mismo merchant normalizado + |USD| EXACTO + fecha ANTERIOR
#                 + no emparejada. Extracto primero, luego histórico.
#       Pasada 2 (solo los que quedaron sin match): mismo Card Member + |USD| EXACTO al centavo
#                 + fecha ANTERIOR + no emparejada, SIN exigir merchant. El monto exacto dentro
#                 del mismo Card Member ya identifica la compra; el merchant solo estorba cuando
#                 el emisor escribe la descripción de la compra y su reembolso distinta (Amex:
#                 'AMAZON MARKEPLACE NA PA' en la compra vs 'AMAZON MARKETPLACE ...' en el
#                 reembolso). Varios candidatos -> la compra MÁS RECIENTE anterior al reembolso.
#     En ambas pasadas, varios candidatos -> la MÁS RECIENTE anterior (desempate por Reference/
#     Orden ascendente): determinista entre corridas, que es lo que exige la idempotencia.
#   - REEMBOLSO PARCIAL (pasada 3, solo los que siguen sin match; NO cambia con lo anterior):
#     devolución de parte de una compra. Candidatas = mismo merchant + mismo Card Member/
#     casillero + fecha ANTERIOR + USD de la compra ESTRICTAMENTE MAYOR + no emparejada. Se
#     empareja SOLO si hay EXACTAMENTE UNA candidata; con 0 o 2+ NO se empareja (fallback +
#     warning con el conteo). Regla estricta: con varias candidatas no hay criterio objetivo
#     (un ajuste de 1 centavo contra 16 compras del mismo merchant) y emparejar mal sería
#     silencioso. Conversión PROPORCIONAL: COP = round(USD_reembolso * TRM_compra).
#   - EXTRACTO PRIMERO en cada pasada: si el extracto aporta candidatas, el histórico NO se mira
#     (evita contar dos veces la misma compra que está en ambos y romper el conteo).
#   - Sin candidato -> comportamiento ANTERIOR (TRM del día del reembolso) + warning listando
#     los reembolsos para revisión manual.
# NO altera compras (Amount > 0), ni Orden, ni corte, ni lista de exclusión, ni idempotencia.
# ──────────────────────────────────────────────────────────────────────────────
def _norm_merchant(s) -> str:
    """Merchant/Description normalizado para emparejar: colapsa espacios + MAYÚSCULAS."""
    return " ".join(str(s).split()).upper()


def _norm_merchant_refund_rk(s) -> str:
    """Merchant de un REFUND de Rakuten. Rakuten nombra el reembolso "Refund from <MERCHANT>",
    así que sin quitar ese prefijo NUNCA coincidiría con el merchant de la compra. Verificado
    en el CSV real: 'Refund from JD 638 000000638' <-> compra 'JD 638 000000638'."""
    v = _norm_merchant(s)
    return v[len("REFUND FROM "):].strip() if v.startswith("REFUND FROM ") else v


def _cas_str(x) -> str:
    """Casillero como texto estable ('11591.0' -> '11591') para segmentar sin falsos negativos."""
    s = str(x).strip()
    return s[:-2] if s.endswith(".0") else s


def _indice_compras_historico(hist_df, prefijo: str, etiqueta: str) -> dict:
    """Índice {(casillero, merchant_norm): [{fecha, trm, orden, usd}, ...]} de las COMPRAS de
    tarjeta YA CARGADAS en el histórico, para reembolsos cuya compra original no está en el
    extracto. La clave incluye el CASILLERO: el histórico no guarda Card Member, así que el
    casillero es la segmentación disponible (impide cruzar mayoristas). Se indexa por merchant
    (no por monto) para poder resolver también reembolsos PARCIALES. El USD se recupera como
    Monto/TRM (la TRM guardada ya incluye el spread). Devuelve {} si no hay datos usables."""
    idx: dict = {}
    if hist_df is None or not len(hist_df):
        return idx
    d = hist_df.copy()
    d.columns = [str(c).strip() for c in d.columns]
    for col in ("Orden", "Monto", "TRM", "Fecha", "Nombre del producto", "Casillero"):
        if col not in d.columns:
            return idx
    d = d[d["Orden"].astype(str).str.strip().str.startswith(prefijo)].copy()
    d["_trm"] = pd.to_numeric(d["TRM"], errors="coerce")
    d["_monto"] = pd.to_numeric(d["Monto"], errors="coerce")
    d["_fecha"] = pd.to_datetime(d["Fecha"], errors="coerce")
    d = d[d["_trm"].notna() & (d["_trm"] > 0) & d["_monto"].notna() & d["_fecha"].notna()]
    # solo las filas de GASTO (una compra); los reembolsos ya cargados no son candidatos
    _pref = f"{etiqueta} - gasto - "
    _np = d["Nombre del producto"].astype(str)
    d = d[_np.str.startswith(_pref)].copy()
    if not len(d):
        return idx
    d["_merch"] = d["Nombre del producto"].astype(str).str[len(_pref):].map(_norm_merchant)
    d["_usd"] = (d["_monto"] / d["_trm"]).round(2)
    for _, r in d.iterrows():
        idx.setdefault((_cas_str(r["Casillero"]), r["_merch"]), []).append(
            {"fecha": r["_fecha"], "trm": float(r["_trm"]),
             "orden": str(r["Orden"]).strip(), "usd": float(r["_usd"])}
        )
    for k in idx:  # determinista: fecha asc, luego Orden asc
        idx[k].sort(key=lambda c: (c["fecha"], c["orden"]))
    return idx


def _resolver_trm_reembolsos(reembolsos, compras, hist_idx):
    """Empareja cada reembolso con su compra original (total o parcial) y devuelve
    (resueltos, sin_match, ambiguos_trm). Ver el bloque de arriba para las reglas.
      reembolsos: dicts {id, fecha (Timestamp), merch, usd, cm, cas} — se procesan en orden
                  (fecha, id) ascendente: determinista.
      compras:    dicts {id, fecha, merch, usd, cm, cas, trm_fecha} del EXTRACTO (universo
                  completo, sin filtrar por corte/lista).
      hist_idx:   índice de _indice_compras_historico, keyed (casillero, merchant).
    'resueltos' = {id_reembolso: (fecha_compra_iso, origen, trm_hist_o_None, es_parcial)};
    origen 'extracto' (la TRM se pide por fecha) u 'historico' (la TRM viene guardada).
    'sin_match' trae 'motivo' y 'n_candidatas' para el warning."""
    # Índice del extracto por (Card Member, merchant): pasada 1 (exacto) y pasada 3 (parcial).
    por_merch: dict = {}
    for c in compras:
        por_merch.setdefault((c["cm"], c["merch"]), []).append(c)
    # Índice del extracto por Card Member (sin merchant): pasada 2 (exacto sin merchant).
    por_cm: dict = {}
    for c in compras:
        por_cm.setdefault(c["cm"], []).append(c)
    for _d in (por_merch, por_cm):  # determinista: fecha asc, luego id asc
        for k in _d:
            _d[k].sort(key=lambda c: (c["fecha"], str(c["id"])))

    usados_ext, usados_hist = set(), set()
    resueltos, ambiguos = {}, []

    def _tomar_ext(r, cands):
        elegida = cands[-1]  # la MÁS RECIENTE anterior (lista ordenada), determinista
        if len({c["trm_fecha"] for c in cands}) > 1:
            ambiguos.append({**r, "n_candidatas": len(cands)})
        usados_ext.add(str(elegida["id"]))
        return (elegida["trm_fecha"], "extracto", None)

    def _tomar_hist(cands):
        elegida = cands[-1]
        usados_hist.add(elegida["orden"])
        return (elegida["fecha"].strftime("%Y-%m-%d"), "historico", elegida["trm"])

    def _hist_por_cas(cas):
        """Todas las compras del histórico de ese casillero (cualquier merchant), para pasada 2."""
        out = []
        for (hc, _hm), lst in hist_idx.items():
            if hc == cas:
                out += lst
        out.sort(key=lambda h: (h["fecha"], h["orden"]))
        return out

    ordenados = sorted(reembolsos, key=lambda x: (x["fecha"], str(x["id"])))

    # ── PASADA 1: EXACTO con merchant (extracto, luego histórico) — para todos ──
    restantes = []
    for r in ordenados:
        usd_r = round(float(r["usd"]), 2)
        ex = [c for c in por_merch.get((r["cm"], r["merch"]), [])
              if c["fecha"] < r["fecha"] and str(c["id"]) not in usados_ext
              and round(float(c["usd"]), 2) == usd_r]
        if ex:
            resueltos[r["id"]] = (*_tomar_ext(r, ex), False)
            continue
        hx = [h for h in hist_idx.get((r["cas"], r["merch"]), [])
              if h["fecha"] < r["fecha"] and h["orden"] not in usados_hist
              and abs(float(h["usd"]) - usd_r) < 0.005]
        if hx:
            resueltos[r["id"]] = (*_tomar_hist(hx), False)
            continue
        restantes.append(r)

    # ── PASADA 2: EXACTO sin merchant, mismo Card Member (los que quedaron sin match) ──
    restantes2 = []
    for r in restantes:
        usd_r = round(float(r["usd"]), 2)
        ex = [c for c in por_cm.get(r["cm"], [])
              if c["fecha"] < r["fecha"] and str(c["id"]) not in usados_ext
              and round(float(c["usd"]), 2) == usd_r]
        if ex:
            resueltos[r["id"]] = (*_tomar_ext(r, ex), False)
            continue
        hx = [h for h in _hist_por_cas(r["cas"])
              if h["fecha"] < r["fecha"] and h["orden"] not in usados_hist
              and abs(float(h["usd"]) - usd_r) < 0.005]
        if hx:
            resueltos[r["id"]] = (*_tomar_hist(hx), False)
            continue
        restantes2.append(r)

    # ── PASADA 3: PARCIAL con merchant, EXACTAMENTE UNA candidata (extracto primero) ──
    sin_match = []
    for r in restantes2:
        usd_r = round(float(r["usd"]), 2)
        pe = [c for c in por_merch.get((r["cm"], r["merch"]), [])
              if c["fecha"] < r["fecha"] and str(c["id"]) not in usados_ext
              and round(float(c["usd"]), 2) > usd_r]
        ph = [h for h in hist_idx.get((r["cas"], r["merch"]), [])
              if h["fecha"] < r["fecha"] and h["orden"] not in usados_hist
              and round(float(h["usd"]), 2) > usd_r]
        pool, origen = (pe, "extracto") if pe else (ph, "historico")
        if len(pool) == 1:
            elegida = pool[0]
            if origen == "extracto":
                usados_ext.add(str(elegida["id"]))
                resueltos[r["id"]] = (elegida["trm_fecha"], "extracto", None, True)
            else:
                usados_hist.add(elegida["orden"])
                resueltos[r["id"]] = (elegida["fecha"].strftime("%Y-%m-%d"), "historico",
                                      elegida["trm"], True)
            continue
        sin_match.append({**r, "n_candidatas": len(pool),
                          "motivo": ("sin compra mayor del mismo merchant" if not pool
                                     else f"{len(pool)} compras candidatas (se exige 1)")})
    return resueltos, sin_match, ambiguos


def procesar_amex(df: pd.DataFrame, fecha_desde=None, cobrados=None, pendientes=None,
                  hist_tarjetas=None, cobrados_df=None) -> dict[str, pd.DataFrame]:
    """Transforma la hoja 'Transaction Details' de Amex en {amex_<cas>: DF} con UNA fila COP por
    transacción (1-a-1, Orden = amex_<Reference>; ver bloque de arriba). Levanta ValueError si
    falta la TRM de cualquier día con movimiento, o si alguna fila no trae Reference válido.
    'fecha_desde' (opcional) descarta transacciones con FECHA DE COMPRA anterior a esa fecha.
    'cobrados' (OBLIGATORIO si fecha_desde está activo) = set de Orden ya cobrados
    (tarjetas_cobradas.xlsx): esas transacciones se EXCLUYEN (anti-doble-cobro).
    'pendientes' (opcional) = DataFrame 'pendientes_rematch' de la misma lista: cobros reales
    aún sin Orden; las transacciones que los matcheen también se EXCLUYEN.
    'hist_tarjetas' (opcional) = filas de tarjeta YA CARGADAS en el histórico (Orden/Monto/TRM/
    Fecha/'Nombre del producto'): solo se usa para darle a un reembolso la TRM de su compra
    original cuando esa compra no está en el extracto. No afecta qué filas se generan.
    'cobrados_df' (opcional) = hoja 'cobradas' COMPLETA con atributos: habilita la SEGUNDA
    BARRERA anti-recobro (excluye una transacción ya cobrada cuyo Orden cambió entre
    descargas). Sin él, el comportamiento es el anterior (solo barrera por Orden)."""
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    for col in ("Card Member", "Date", "Amount", "Reference"):
        if col not in df.columns:
            raise ValueError(f"La hoja 'Transaction Details' no tiene la columna '{col}'.")

    # INACTIVO sin fecha de corte: no se procesa nada (protección anti doble-conteo).
    # Se valida columnas ANTES (un archivo malo igual falla claro), luego se corta aquí.
    if fecha_desde is None:
        return {}

    # 🛡️ LISTA DE EXCLUSIÓN obligatoria: procesar sin lista recobraría lo ya cobrado.
    if cobrados is None:
        raise ValueError(
            "Falta la lista de exclusión 'tarjetas cobradas' (cobrados=None). "
            "No se procesa nada: sin la lista se recobrarían transacciones ya cobradas."
        )

    # Card Member -> casillero (ignora los que no están en el mapeo)
    df["_cas"] = df["Card Member"].map(_amex_norm_cardmember).map(AMEX_CARD_MAP)
    df = df[df["_cas"].notna()].copy()

    # Fecha de transacción (Amex viene MM/DD/YYYY)
    df["_fecha"] = pd.to_datetime(df["Date"], format="%m/%d/%Y", errors="coerce")
    df = df[df["_fecha"].notna()].copy()

    # 🔁 UNIVERSO DE COMPRAS para emparejar reembolsos (ver bloque "REEMBOLSO CON LA TRM DE SU
    # COMPRA ORIGINAL"): se captura ANTES del corte y ANTES de la lista de exclusión, porque la
    # compra que revierte un reembolso puede ser vieja o ya estar cobrada. Solo lectura: este
    # universo NO genera filas, únicamente presta la TRM/fecha de la compra.
    _u = df[pd.to_numeric(df["Amount"], errors="coerce") > 0].copy()
    _compras_universo = [
        {
            "id": str(r["Reference"]).strip().lstrip("'"),
            "fecha": r["_fecha"],
            "merch": _norm_merchant(r.get("Description", "")),
            "usd": round(float(pd.to_numeric(r["Amount"], errors="coerce")), 2),
            # 🔒 segmentación: Card Member para el extracto, casillero para el histórico
            "cm": _amex_norm_cardmember(r["Card Member"]),
            "cas": _cas_str(r["_cas"]),
            "trm_fecha": r["_fecha"].strftime("%Y-%m-%d"),
        }
        for _, r in _u.iterrows()
    ]

    # 🛡️ Universo de Orden y rango de fechas del extracto (para la SEGUNDA BARRERA): se toma
    # ANTES del corte y de la lista, sobre todas las filas mapeadas. Un cobro cuyo Orden esté
    # aquí NO es huérfano y no compite por atributos.
    _ref_univ = df["Reference"].astype(str).str.strip().str.lstrip("'")
    _ordenes_universo = set(("amex_" + _ref_univ)[_ref_univ.str.fullmatch(r"\d+", na=False)])
    _rango_extracto = (df["_fecha"].min(), df["_fecha"].max()) if len(df) else None

    if fecha_desde is not None:
        df = df[df["_fecha"] >= pd.Timestamp(fecha_desde)]

    # Signo -> Tipo ; Monto USD absoluto (Amount == 0 se descarta)
    df["_amount"] = pd.to_numeric(df["Amount"], errors="coerce")
    df = df[df["_amount"].notna() & (df["_amount"] != 0)].copy()

    # Blindaje defensivo: de los NEGATIVOS, excluir pagos a la tarjeta y créditos
    # Amazon (ver AMEX_PAGO_PATTERNS / AMEX_CREDITO_EXCLUIR). Los positivos (Egreso)
    # NO se tocan: el AND con `_neg` garantiza que solo se filtran Amount < 0.
    _neg = df["_amount"] < 0
    _desc_up = df.get("Description", pd.Series("", index=df.index)).astype(str).str.upper()
    _cat = df.get("Category", pd.Series("", index=df.index))
    _cat_vacia = _cat.isna() | _cat.astype(str).str.strip().str.lower().isin(["", "nan", "none"])
    _es_pago = _cat_vacia | _desc_up.apply(lambda d: any(p.upper() in d for p in AMEX_PAGO_PATTERNS))
    _es_credito = _desc_up.apply(lambda d: any(p.upper() in d for p in AMEX_CREDITO_EXCLUIR))
    df = df[~(_neg & (_es_pago | _es_credito))].copy()
    if df.empty:
        return {}

    df["_tipo"] = df["_amount"].apply(lambda a: "Egreso" if a > 0 else "Ingreso")
    df["_usd"] = df["_amount"].abs()
    df["_fecha_iso"] = df["_fecha"].dt.strftime("%Y-%m-%d")

    if df.empty:
        return {}

    # 1-a-1: Reference nativo de Amex -> Orden por transacción. FAIL-LOUD: sin Reference
    # numérico válido en TODAS las filas, o con Reference repetido dentro del archivo, NO se
    # genera ningún movimiento (nunca inventar IDs ni colapsar dos cobros en uno en silencio).
    # Nota: si pandas leyera Reference como float (p.ej. por un NaN en la columna) el valor de
    # 18 dígitos pierde precisión y quedaría "3.2e+17" -> NO matchea \d+ -> también falla claro.
    df["_ref"] = df["Reference"].astype(str).str.strip().str.lstrip("'")
    _ref_mala = ~df["_ref"].str.fullmatch(r"\d+")
    if _ref_mala.any():
        _ej = df.loc[_ref_mala, ["Date", "Description", "Amount"]].head(5).to_dict("records") \
              if "Description" in df.columns else df.loc[_ref_mala, ["Date", "Amount"]].head(5).to_dict("records")
        raise ValueError(
            f"{int(_ref_mala.sum())} transacciones Amex sin 'Reference' numérico válido. "
            f"No se genera ningún movimiento. Primeras: {_ej}"
        )
    _ref_dup = df["_ref"].duplicated(keep=False)
    if _ref_dup.any():
        raise ValueError(
            f"Reference repetido en el extracto Amex ({int(_ref_dup.sum())} filas): "
            f"{sorted(df.loc[_ref_dup, '_ref'].unique())[:5]}. No se genera ningún movimiento "
            f"(un Reference debe identificar UNA transacción; revisa el export)."
        )

    # 🛡️ Anti-doble-cobro (defensa PRINCIPAL): excluir transacciones cuyo Orden ya está en la
    # lista de cobradas. Va DESPUÉS de validar Reference (un archivo malo sigue fallando claro)
    # y ANTES de la TRM (no se piden TRM de días cuyo movimiento quedó 100% excluido).
    df["_orden"] = "amex_" + df["_ref"]
    _ya_cobradas = df["_orden"].isin(cobrados)
    if _ya_cobradas.any():
        _cobradas_info(f"🛡️ Amex: {int(_ya_cobradas.sum())} transacciones ya cobradas "
                       f"(lista de exclusión) — excluidas del cargue.")
        df = df[~_ya_cobradas].copy()
    if df.empty:
        return {}

    # 🛡️ ESCUDO DE PENDIENTES: los 'pendientes_rematch' son cobros REALES hechos antes de que
    # la compra asentara (aún sin Orden). Si una transacción del extracto matchea un pendiente
    # por (fecha compra, monto USD firmado, Card Member) también se excluye. COUNT-AWARE: cada
    # pendiente tapa UNA transacción; el desempate entre gemelas es por Reference ascendente
    # (determinista entre descargas: nunca se cuela un gemelo distinto en una recarga).
    if pendientes is not None and len(pendientes) and "tarjeta" in pendientes.columns:
        _p = pendientes[pendientes["tarjeta"].astype(str).str.strip().str.lower() == "amex"]
        if len(_p):
            _pk: dict = {}
            for _, _rp in _p.iterrows():
                _k = (str(_rp["fecha_compra"]).strip()[:10],
                      round(float(_rp["monto_usd"]), 2),
                      _amex_norm_cardmember(_rp["card_member"] if "card_member" in _p.columns else ""))
                _pk[_k] = _pk.get(_k, 0) + 1
            _cmn = df["Card Member"].map(_amex_norm_cardmember)
            _amt2 = df["_amount"].round(2)
            _drop = []
            for _k, _n in _pk.items():
                _m = df[(df["_fecha_iso"] == _k[0]) & (_amt2 == _k[1]) & (_cmn == _k[2])]
                if len(_m):
                    _drop += list(_m.sort_values("_ref").index[:_n])
            if _drop:
                _cobradas_info(f"🛡️ Amex: {len(_drop)} transacciones matchean cobros PENDIENTES "
                               f"de rematch (ya cobradas pre-asiento) — excluidas del cargue.")
                df = df.drop(index=_drop)
    if df.empty:
        return {}

    # Description limpio para 'Nombre del producto' (colapsa los espacios múltiples del extracto)
    _desc = df.get("Description", pd.Series("", index=df.index)).fillna("")
    df["_desc"] = _desc.astype(str).map(lambda s: " ".join(s.split()))

    # 🛡️ SEGUNDA BARRERA ANTI-RECOBRO (por atributos): atrapa una transacción ya cobrada cuyo
    # Orden cambió entre descargas. Va DESPUÉS de la lista y de los pendientes, y ANTES de la
    # TRM (no se piden TRM de días cuyo movimiento quedó excluido).
    df["_merch_attr"] = df["_desc"].map(_norm_merchant)
    _drop_attr = _excluir_por_atributos(df, cobrados_df, "amex", _ordenes_universo,
                                        _rango_extracto, "Amex")
    if _drop_attr:
        df = df.drop(index=_drop_attr)
    if df.empty:
        return {}

    # 🔁 REEMBOLSO -> TRM DE SU COMPRA ORIGINAL (neteo exacto en COP). Solo toca los Ingreso.
    _reembolsos = [
        {"id": r["_ref"], "fecha": r["_fecha"], "merch": _norm_merchant(r["_desc"]),
         "usd": round(float(r["_usd"]), 2),
         "cm": _amex_norm_cardmember(r["Card Member"]), "cas": _cas_str(r["_cas"])}
        for _, r in df[df["_tipo"] == "Ingreso"].iterrows()
    ]
    _trm_ok, _trm_sin_match, _trm_ambiguos = _resolver_trm_reembolsos(
        _reembolsos, _compras_universo,
        _indice_compras_historico(hist_tarjetas, "amex_", "Tarjeta Amex"),
    )
    if _trm_sin_match:
        _cobradas_warn(
            "⚠️ Amex: {} reembolso(s) sin compra original identificable (ni total ni parcial) "
            "— se usa la TRM de su propio día, como antes. REVISAR a mano: {}"
            .format(len(_trm_sin_match),
                    "; ".join(f"{x['fecha']:%Y-%m-%d} USD {x['usd']:.2f} {x['merch'][:36]} "
                              f"[{x['motivo']}]" for x in _trm_sin_match[:10]))
        )
    if _trm_ambiguos:
        _cobradas_info(
            f"ℹ️ Amex: {len(_trm_ambiguos)} reembolso(s) tenían varias compras candidatas con "
            f"TRM distintas; se tomó la compra MÁS RECIENTE anterior al reembolso."
        )

    # TRM por día (+125). Recolecta TODOS los días faltantes antes de decidir (sin default).
    # Incluye los días de las COMPRAS ORIGINALES de los reembolsos emparejados (su TRM es la
    # que se aplica), no solo los días de las filas que se van a generar.
    trm_cache: dict = {}
    faltantes = set()
    _dias = set(df["_fecha_iso"].unique()) | {
        f for f, origen, _, _p in _trm_ok.values() if origen == "extracto"
    }
    for f_iso in sorted(_dias):
        if _amex_trm_dia(f_iso, trm_cache) is None:
            faltantes.add(f_iso)
    if faltantes:
        dias = ", ".join(sorted(faltantes))
        raise ValueError(
            f"Sin TRM (datos.gov.co) para los días con movimiento Amex: {dias}. "
            f"No se genera ningún movimiento (no hay TRM de respaldo)."
        )

    filas = []
    for _, r in df.iterrows():
        cas, tipo, f_iso = r["_cas"], r["_tipo"], r["_fecha_iso"]
        trm = trm_cache[f_iso]
        etq = "gasto" if tipo == "Egreso" else "reembolso"
        # Reembolso emparejado: se convierte con la TRM de la COMPRA (no la del día del
        # reembolso) -> el COP del reembolso queda igual al de la compra y el neto da 0.
        _m = _trm_ok.get(r["_ref"]) if tipo == "Ingreso" else None
        if _m:
            _f_compra, _origen, _trm_hist, _parcial = _m
            trm = _trm_hist if _origen == "historico" else trm_cache[_f_compra]
            etq = f"reembolso{' parcial' if _parcial else ''} (TRM compra {_f_compra})"
        monto = round(float(r["_usd"]) * trm)  # COP, POSITIVO
        filas.append({
            "Fecha": f_iso,
            "Tipo": tipo,
            "Monto": monto,
            "Orden": r["_orden"],
            "Motivo": "Tarjeta Amex",
            "TRM": round(trm, 2),
            "Usuario": AMEX_USUARIOS[cas],
            "Casillero": cas,
            "Estado de Orden": "",
            "Nombre del producto": f"Tarjeta Amex - {etq} - {r['_desc']}",
        })

    out = pd.DataFrame(filas)
    salida = {}
    for cas in sorted(out["Casillero"].unique()):
        salida[f"amex_{cas}"] = out[out["Casillero"] == cas].reset_index(drop=True)
    return salida


# ──────────────────────────────────────────────────────────────────────────────
# Cargue "Tarjeta Rakuten" (módulo PARALELO a Amex, lógica propia; NO reusa procesar_amex).
# SOLO Maria Moises -> casillero 1444. Fuente: CSV Rakuten
#   (columnas: Date, Amount, Type, Merchant, Category, Method).
#   - Se FILTRA POR LA COLUMNA `Type` (NO por el signo: PAYMENT y REFUND son ambos negativos):
#       TRANSACTION -> Egreso (gasto)         ; REFUND -> Ingreso (devolución, Monto = abs)
#       PAYMENT / OFFER / AUTH -> IGNORAR       ; Type NUEVO/desconocido -> IGNORAR + st.warning
#   - USD -> COP con la MISMA TRM que Amex (_amex_trm_dia: datos.gov.co por rango, +125).
#     *** SIN TRM de respaldo: si falta la TRM de un día con movimiento, LEVANTA ValueError. ***
#   - 1-a-1: cada TRANSACTION/REFUND -> UNA fila propia (ya NO se acumula por día).
#     Rakuten NO trae ID nativo -> Orden determinista por transacción:
#       clave = "<Date>|<Amount>|<Merchant>|<seq>" con los valores CRUDOS del CSV (Date con
#       HH:MM:SS; sin normalizar, para que la clave sea reproducible byte a byte) y
#       seq = nº de ocurrencia (0,1,...) de esa clave exacta dentro del archivo — duplicados
#       exactos son compras reales distintas en el mismo segundo (p.ej. cargos por ítem de
#       Adidas: verificado 2x$74.90 el 2026-04-24 22:54:32). seq es estable entre descargas
#       porque las filas de una misma clave son idénticas entre sí (da igual cuál recibe 0 o 1).
#       Orden = "rakuten_" + sha1(clave, utf-8)[:12]  (48 bits; colisión ~10^-8 a esta escala).
#     Con el dedup existente por Orden (keep="last") el cargue es IDEMPOTENTE (recargar = no-op;
#     compras tardías entran apenas aparezcan en el export "_All").
#     ⚠️ PENDIENTE DE ACTIVAR: la estabilidad del timestamp entre 2 descargas reales aún no se
#     verificó -> RAKUTEN_FECHA_DESDE se queda en None hasta hacer esa verificación.
#   - Motivo = "Tarjeta Rakuten" (tag EXACTO que captura el incentivo combinado de 1444).
# ──────────────────────────────────────────────────────────────────────────────
RAKUTEN_CASILLERO = "1444"
RAKUTEN_USUARIO = "Maria Moises"
RAKUTEN_TIPO_MAP = {"TRANSACTION": "Egreso", "REFUND": "Ingreso"}  # el resto se ignora
RAKUTEN_TIPOS_IGNORAR = {"PAYMENT", "OFFER", "AUTH"}
RAKUTEN_COLS = ["Date", "Amount", "Type", "Merchant", "Category", "Method"]

# 🚦 FECHA DE CORTE del cargue Rakuten — DISEÑO FINAL (las MISMAS 3 reglas que Amex, ver
#   arriba: 1. el histórico de cobrados MANDA; 2. corte = último mes, lo anterior se ignora;
#   3. todo lo nuevo fuera de lista/pendientes se toma).
#   - None -> INACTIVO (kill switch de emergencia).
#   ⏳ Pendiente vigente (decisión del usuario: activar de una vez): verificar la estabilidad
#   del timestamp del CSV con una 2ª descarga; si un timestamp cambiara entre descargas, el
#   hash cambia y la transacción re-entraría — la lista/pendientes NO la taparían.
RAKUTEN_FECHA_DESDE = "2026-06-16"


def _rakuten_warn(msg: str):
    """st.warning si Streamlit está disponible; en dry-run (sin st) no rompe."""
    try:
        st.warning(msg)
    except Exception:
        pass


def _rakuten_parse_amount(x) -> float:
    """USD Rakuten '$1,234.56' / '-$1,234.56' / '($1,234.56)' -> float. NaN si no parsea."""
    s = str(x).strip().replace("$", "").replace(",", "")
    neg = s.startswith("(") and s.endswith(")")
    s = s.strip("()")
    try:
        v = float(s)
    except ValueError:
        return float("nan")
    return -v if neg else v


def procesar_rakuten(df: pd.DataFrame, fecha_desde=None, cobrados=None, pendientes=None,
                     hist_tarjetas=None, cobrados_df=None) -> dict[str, pd.DataFrame]:
    """Transforma el CSV Rakuten en {rakuten_1444: DF} con UNA fila COP por transacción
    (1-a-1, Orden = rakuten_<sha1-12>; ver bloque de arriba). Filtra por `Type`. Levanta
    ValueError si falta la TRM de cualquier día con movimiento. 'fecha_desde' descarta
    transacciones anteriores; None -> no procesa. 'cobrados' (OBLIGATORIO si fecha_desde
    está activo) = set de Orden ya cobrados: esas transacciones se EXCLUYEN. 'pendientes'
    (opcional) = DataFrame 'pendientes_rematch': auth cobrados aún sin asentar; las
    TRANSACTION que los matcheen (timestamp+monto, o el timestamp completo si asentó
    partido) también se EXCLUYEN."""
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    faltan = [c for c in RAKUTEN_COLS if c not in df.columns]
    if faltan:
        raise ValueError(f"El CSV Rakuten no tiene las columnas esperadas: {', '.join(faltan)}.")

    # INACTIVO sin fecha de corte: no se procesa nada (se valida columnas ANTES).
    if fecha_desde is None:
        return {}

    # 🛡️ LISTA DE EXCLUSIÓN obligatoria: procesar sin lista recobraría lo ya cobrado.
    if cobrados is None:
        raise ValueError(
            "Falta la lista de exclusión 'tarjetas cobradas' (cobrados=None). "
            "No se procesa nada: sin la lista se recobrarían transacciones ya cobradas."
        )

    # Tipo por la columna Type (NO por signo). Desconocidos -> avisar + ignorar (no cargar a ciegas).
    df["_type"] = df["Type"].astype(str).str.strip().str.upper()
    conocidos = set(RAKUTEN_TIPO_MAP) | RAKUTEN_TIPOS_IGNORAR
    desconocidos = sorted(set(df["_type"].unique()) - conocidos)
    if desconocidos:
        _rakuten_warn("⚠️ Rakuten: tipos NO reconocidos (ignorados): "
                      f"{', '.join(desconocidos)}. Revisa si Rakuten agregó un tipo nuevo.")
    df["_tipo"] = df["_type"].map(RAKUTEN_TIPO_MAP)
    df = df[df["_tipo"].notna()].copy()  # solo TRANSACTION/REFUND

    # Fecha (solo día) y monto USD absoluto (Amount == 0 se descarta)
    df["_fecha"] = pd.to_datetime(df["Date"], format="%Y/%m/%d, %H:%M:%S", errors="coerce")
    df = df[df["_fecha"].notna()].copy()

    # 🛡️ Universo de Orden y rango del extracto para la SEGUNDA BARRERA. Se calcula sobre TODAS
    # las TRANSACTION/REFUND con monto != 0 del CSV (antes del corte y de la lista) — el mismo
    # universo con el que se generó la lista de cobradas, para que 'huérfano' signifique lo
    # mismo en ambos lados.
    _uu = df.copy()
    _uu["_a"] = _uu["Amount"].map(_rakuten_parse_amount)
    _uu = _uu[_uu["_a"].notna() & (_uu["_a"] != 0)]
    _ku = (_uu["Date"].astype(str) + "|" + _uu["Amount"].astype(str) + "|" + _uu["Merchant"].astype(str))
    _su = _ku.groupby(_ku).cumcount().astype(str)
    _ordenes_universo = set("rakuten_" + (_ku + "|" + _su).map(
        lambda s: hashlib.sha1(s.encode("utf-8")).hexdigest()[:12]))
    _rango_extracto = (df["_fecha"].min(), df["_fecha"].max()) if len(df) else None

    # 🔁 UNIVERSO DE COMPRAS (TRANSACTION) para emparejar REFUND: igual que en Amex, se captura
    # ANTES del corte y de la lista de exclusión (la compra revertida puede ser vieja/ya cobrada).
    _u = df[(df["_type"] == "TRANSACTION")].copy()
    _u["_amt_u"] = _u["Amount"].map(_rakuten_parse_amount)
    _u = _u[_u["_amt_u"].notna() & (_u["_amt_u"] != 0)]
    _compras_universo = [
        {
            "id": f"{r['_fecha']:%Y-%m-%d %H:%M:%S}|{r['_amt_u']}|{_norm_merchant(r['Merchant'])}",
            "fecha": r["_fecha"],
            "merch": _norm_merchant(r["Merchant"]),
            "usd": round(abs(float(r["_amt_u"])), 2),
            # 🔒 segmentación: Rakuten es de UNA sola tarjeta (1444), pero se puebla igual que
            # Amex para que _resolver_trm_reembolsos aplique la misma regla sin casos especiales.
            "cm": RAKUTEN_USUARIO,
            "cas": RAKUTEN_CASILLERO,
            "trm_fecha": r["_fecha"].strftime("%Y-%m-%d"),
        }
        for _, r in _u.iterrows()
    ]

    if fecha_desde is not None:
        df = df[df["_fecha"] >= pd.Timestamp(fecha_desde)]
    df["_amount"] = df["Amount"].map(_rakuten_parse_amount)
    df = df[df["_amount"].notna() & (df["_amount"] != 0)].copy()
    df["_usd"] = df["_amount"].abs()
    df["_fecha_iso"] = df["_fecha"].dt.strftime("%Y-%m-%d")
    if df.empty:
        return {}

    # 1-a-1: Orden determinista por transacción (ver esquema en el bloque de arriba).
    # Clave con los valores CRUDOS del CSV + contador de ocurrencia para duplicados exactos.
    _clave = (df["Date"].astype(str) + "|" + df["Amount"].astype(str) + "|" + df["Merchant"].astype(str))
    _seq = _clave.groupby(_clave).cumcount().astype(str)
    df["_orden"] = "rakuten_" + (_clave + "|" + _seq).map(
        lambda s: hashlib.sha1(s.encode("utf-8")).hexdigest()[:12]
    )
    # FAIL-LOUD: una colisión de hash entre claves distintas colapsaría dos cobros en uno.
    if df["_orden"].duplicated().any():
        raise ValueError(
            "Colisión de hash en el Orden Rakuten (dos transacciones distintas generaron el "
            "mismo ID). No se genera ningún movimiento; reporta este archivo."
        )

    # 🛡️ Anti-doble-cobro (defensa PRINCIPAL): excluir transacciones cuyo Orden ya está en la
    # lista de cobradas. Va ANTES de la TRM (no pedir TRM de días 100% excluidos).
    _ya_cobradas = df["_orden"].isin(cobrados)
    if _ya_cobradas.any():
        _cobradas_info(f"🛡️ Rakuten: {int(_ya_cobradas.sum())} transacciones ya cobradas "
                       f"(lista de exclusión) — excluidas del cargue.")
        df = df[~_ya_cobradas].copy()
    if df.empty:
        return {}

    # 🛡️ ESCUDO DE PENDIENTES: los 'pendientes_rematch' Rakuten son auth COBRADOS que aún no
    # habían asentado. Cuando su TRANSACTION firme aparezca en un CSV futuro se excluye:
    # primero por (timestamp + monto) exacto; si el auth asentó PARTIDO (montos distintos),
    # se excluyen TODAS las TRANSACTION de ese timestamp exacto (misma regla del generador de
    # la lista). Desempate por Orden ascendente (determinista entre descargas).
    if pendientes is not None and len(pendientes) and "tarjeta" in pendientes.columns:
        _p = pendientes[pendientes["tarjeta"].astype(str).str.strip().str.lower() == "rakuten"]
        _drop = []
        for _, _rp in _p.iterrows():
            _ts = pd.to_datetime(str(_rp["fecha_compra"]), errors="coerce")
            if pd.isna(_ts):
                continue
            _amt = round(abs(float(_rp["monto_usd"])), 2)
            _vivos = df[(df["_fecha"] == _ts) & (~df.index.isin(_drop))]
            _exactos = _vivos[_vivos["_usd"].round(2) == _amt]
            if len(_exactos):
                _drop.append(_exactos.sort_values("_orden").index[0])
            elif len(_vivos):
                _drop += list(_vivos.index)  # auth asentado partido: tapa todo el timestamp
        if _drop:
            _cobradas_info(f"🛡️ Rakuten: {len(_drop)} transacciones matchean cobros PENDIENTES "
                           f"de rematch (auth ya cobrados) — excluidas del cargue.")
            df = df.drop(index=_drop)
    if df.empty:
        return {}

    # 🛡️ SEGUNDA BARRERA ANTI-RECOBRO (por atributos). El merchant de un REFUND lleva el prefijo
    # "Refund from " -> se normaliza igual que en la lista para que compare contra la compra.
    df["_cas"] = RAKUTEN_CASILLERO
    df["_merch_attr"] = df["Merchant"].map(_norm_merchant_refund_rk)
    _drop_attr = _excluir_por_atributos(df, cobrados_df, "rakuten", _ordenes_universo,
                                        _rango_extracto, "Rakuten")
    if _drop_attr:
        df = df.drop(index=_drop_attr)
    if df.empty:
        return {}

    # 🔁 REFUND -> TRM DE SU COMPRA ORIGINAL (neteo exacto en COP). Solo toca los Ingreso.
    _reembolsos = [
        {"id": r["_orden"], "fecha": r["_fecha"], "merch": _norm_merchant_refund_rk(r["Merchant"]),
         "usd": round(float(r["_usd"]), 2),
         "cm": RAKUTEN_USUARIO, "cas": RAKUTEN_CASILLERO}
        for _, r in df[df["_tipo"] == "Ingreso"].iterrows()
    ]
    _trm_ok, _trm_sin_match, _trm_ambiguos = _resolver_trm_reembolsos(
        _reembolsos, _compras_universo,
        _indice_compras_historico(hist_tarjetas, "rakuten_", "Tarjeta Rakuten"),
    )
    if _trm_sin_match:
        _cobradas_warn(
            "⚠️ Rakuten: {} REFUND sin compra original identificable (ni total ni parcial) "
            "— se usa la TRM de su propio día, como antes. REVISAR a mano: {}"
            .format(len(_trm_sin_match),
                    "; ".join(f"{x['fecha']:%Y-%m-%d} USD {x['usd']:.2f} {x['merch'][:36]} "
                              f"[{x['motivo']}]" for x in _trm_sin_match[:10]))
        )
    if _trm_ambiguos:
        _cobradas_info(
            f"ℹ️ Rakuten: {len(_trm_ambiguos)} REFUND tenían varias compras candidatas con TRM "
            f"distintas; se tomó la compra MÁS RECIENTE anterior al reembolso."
        )

    # TRM por día (+125), misma función que Amex. Recolecta faltantes antes de decidir (sin default).
    # Incluye los días de las COMPRAS ORIGINALES de los REFUND emparejados.
    trm_cache: dict = {}
    faltantes = set()
    _dias = set(df["_fecha_iso"].unique()) | {
        f for f, origen, _, _p in _trm_ok.values() if origen == "extracto"
    }
    for f_iso in sorted(_dias):
        if _amex_trm_dia(f_iso, trm_cache) is None:
            faltantes.add(f_iso)
    if faltantes:
        dias = ", ".join(sorted(faltantes))
        raise ValueError(
            f"Sin TRM (datos.gov.co) para los días con movimiento Rakuten: {dias}. "
            f"No se genera ningún movimiento (no hay TRM de respaldo)."
        )

    cas = RAKUTEN_CASILLERO
    filas = []
    for _, r in df.iterrows():
        tipo, f_iso = r["_tipo"], r["_fecha_iso"]
        trm = trm_cache[f_iso]
        etq = "gasto" if tipo == "Egreso" else "reembolso"
        # REFUND emparejado: TRM de la COMPRA original -> neto exacto 0 en COP.
        _m = _trm_ok.get(r["_orden"]) if tipo == "Ingreso" else None
        if _m:
            _f_compra, _origen, _trm_hist, _parcial = _m
            trm = _trm_hist if _origen == "historico" else trm_cache[_f_compra]
            etq = f"reembolso{' parcial' if _parcial else ''} (TRM compra {_f_compra})"
        monto = round(float(r["_usd"]) * trm)  # COP, POSITIVO
        merch = " ".join(str(r["Merchant"]).split())
        filas.append({
            "Fecha": f_iso,
            "Tipo": tipo,
            "Monto": monto,
            "Orden": r["_orden"],
            "Motivo": "Tarjeta Rakuten",
            "TRM": round(trm, 2),
            "Usuario": RAKUTEN_USUARIO,
            "Casillero": cas,
            "Estado de Orden": "",
            "Nombre del producto": f"Tarjeta Rakuten - {etq} - {merch}",
        })

    out = pd.DataFrame(filas)
    return {f"rakuten_{cas}": out.reset_index(drop=True)}


# ──────────────────────────────────────────────────────────────────────────────
# Cargue "Tarjeta Robinhood" (módulo PARALELO a Rakuten, lógica propia; NO reusa procesar_*).
# SOLO 2 Cardholders -> casillero 1444: "Juan Pablo Correal Perez" y "Maria Moises" (el resto se
# IGNORA). Fuente: CSV Robinhood (10 cols: Date, Time, Cardholder, Amount, Points, Balance,
# Status, Type, Merchant, Description).
#   - FILTRO por Status (NO por signo): solo "Posted" entra; "Pending"/"Declined"/otros se
#     IGNORAN (pendiente aún no firme; declinada nunca se cobró). Status desconocido -> ignorar
#     + st.warning.
#   - FILTRO por Type: Purchase -> Egreso ; Refund -> Ingreso (Monto = abs). Payment/Fee/Other
#     se IGNORAN. Type desconocido -> ignorar + st.warning.
#   - Orden 1-a-1 = "robinhood_<sha1-12 de 'Date|Time|Amount|Merchant|seq'>" con los valores
#     CRUDOS del CSV + seq = nº de ocurrencia (0,1,...) de esa clave exacta. Duplicados exactos
#     son compras reales distintas (verificado Kocespay.Korea x2 el 2026-07-23 1:19 AM $1.15 ->
#     2 Orden). El Orden se calcula sobre TODO el set 1444 (ANTES de filtrar por Status), para
#     que el seq sea estable aunque una fila pase de Pending a Posted entre descargas.
#     FAIL-LOUD ante colisión de hash (dos claves distintas -> mismo ID).
#   - USD -> COP con la MISMA TRM que Amex (_amex_trm_dia: datos.gov.co, +125). *** SIN respaldo:
#     si falta la TRM de un día con movimiento, LEVANTA ValueError. ***
#   - Reembolsos (Refund): usan la TRM de su compra original (_resolver_trm_reembolsos, 3 pasadas
#     ya implementadas) para netear exacto. El merchant del Refund viene como "Refund: <merchant>"
#     -> se normaliza (_norm_merchant_refund_robin) para que coincida con la compra.
#   - Motivo = "Tarjeta Robinhood" (tag EXACTO que captura el incentivo combinado de 1444).
#   - 🚩 BLINDAJE VENTANA MANUAL (patrón auth-vs-asiento): toda fila ENTRANTE con FECHA <=
#     ROBINHOOD_VENTANA_MANUAL_FIN (2026-06-22, fin de los bloques manuales del backoffice) se
#     marca para REVISIÓN con st.warning y NO se asume aprobada. El Excel de cobrados guarda los
#     montos de AUTH y el CSV los de ASIENTO; cuando difieren, la lista por monto EXACTO no atrapa
#     el ya-cobrado (Uniqlo 172.78 auth -> 172.29 asiento; Hilton 772.98 -> 622.98+150). Por eso
#     los 2 in-window ya identificados se excluyen a mano en la lista (robinhood_2401ad154e35 /
#     robinhood_618c4f5efdbf) y cualquier futuro in-window queda visible para aprobación manual.
#     Fuera de la ventana, flujo normal.
# ──────────────────────────────────────────────────────────────────────────────
ROBINHOOD_CASILLERO = "1444"
ROBINHOOD_USUARIO = "Maria Moises"
# Cardholder EXACTO -> casillero (el resto se ignora). Santiago/Carlos Largo, Largo Kelly, etc. NO.
ROBINHOOD_CARDMAP = {"Juan Pablo Correal Perez": "1444", "Maria Moises": "1444"}
ROBINHOOD_TIPO_MAP = {"PURCHASE": "Egreso", "REFUND": "Ingreso"}  # el resto se ignora
ROBINHOOD_TIPOS_IGNORAR = {"PAYMENT", "FEE", "OTHER"}
ROBINHOOD_STATUS_OK = "POSTED"
ROBINHOOD_STATUS_IGNORAR = {"PENDING", "DECLINED", "OTHER"}
ROBINHOOD_COLS = ["Date", "Time", "Cardholder", "Amount", "Points", "Balance",
                  "Status", "Type", "Merchant", "Description"]

# 🚦 FECHA DE CORTE del cargue Robinhood — MISMAS 3 reglas que Amex/Rakuten (la LISTA manda; el
#   corte es solo límite de sanidad). 2026-04-14 = inicio de la ventana de cobrados del backoffice
#   (primer bloque "Compra Robinhood del 14 al 26 Abril"). Un pendiente pre-corte que asiente
#   después SÍ entra si no está en la lista (regla de negocio: NUNCA dejar de cobrar). None ->
#   INACTIVO (kill switch).
ROBINHOOD_FECHA_DESDE = "2026-04-14"
# Fin de la ventana de bloques manuales del backoffice: dentro de ella, las entrantes se marcan
# para revisión (blindaje auth-vs-asiento). Fuera, flujo normal.
ROBINHOOD_VENTANA_MANUAL_FIN = "2026-06-22"


def _norm_merchant_refund_robin(s) -> str:
    """Merchant de un Refund Robinhood: viene como 'Refund: <merchant>' -> se quita el prefijo
    para que coincida con el merchant de la compra original. Verificado en el CSV real:
    'Refund: Hilton' <-> compra 'Hilton'."""
    v = _norm_merchant(s)
    for p in ("REFUND: ", "REFUND FROM "):
        if v.startswith(p):
            return v[len(p):].strip()
    return v


def _robinhood_clave_y_seq(df: pd.DataFrame):
    """Clave e índice de repetición del Orden Robinhood (1-a-1), SIN la hora.

    Robinhood reexpide el MISMO movimiento con la hora corrida ±1h entre descargas
    (5 casos verificados comparando las descargas del 23-jul y 29-jul-2026). Con 'Time'
    dentro del hash eso cambiaba el Orden, dejaba huérfanas las entradas de la lista de
    exclusión y habría re-cobrado transacciones ya cobradas. Por eso la clave es
    Date|Amount|Merchant y el 'seq' desempata las repeticiones exactas del mismo día.

    El 'seq' se asigna sobre un ORDEN CANÓNICO y NO sobre el orden de lectura del archivo,
    para que dos descargas den siempre el mismo seq a la misma transacción. El orden canónico
    es (clave, cargable, hora):
      · CARGABLE primero. Solo las filas Posted+Purchase/Refund pueden llegar al histórico,
        así que son las únicas cuyo Orden importa; numerarlas antes que las Declined/Pending
        las aísla de ellas. Sin esto, el 27-abr-2026 la compra Dolphin $2.456,72 (Posted) y
        un intento Declined del mismo día/monto/comercio se intercambiaban el seq cuando solo
        una de las dos corría de hora -> la lista protegía a la Declined y RE-COBRABA la real.
      · La hora solo desempata entre filas cargables del mismo día (un corrimiento horario
        afecta por igual a las de esa fecha, así que el orden relativo se conserva) y NUNCA
        entra en el hash.
    """
    clave = (df["Date"].astype(str) + "|" + df["Amount"].astype(str) + "|" +
             df["Merchant"].astype(str))
    hora = pd.to_datetime(df["Time"].astype(str).str.strip(),
                          format="%I:%M %p", errors="coerce")
    cargable = (
        df["Status"].astype(str).str.strip().str.upper().eq(ROBINHOOD_STATUS_OK)
        & df["Type"].astype(str).str.strip().str.upper().isin(set(ROBINHOOD_TIPO_MAP))
    )
    canon = pd.DataFrame(
        {"_k": clave, "_c": (~cargable).astype(int), "_h": hora}
    ).sort_values(["_k", "_c", "_h"], kind="mergesort", na_position="last")
    seq = canon.groupby("_k").cumcount().reindex(df.index).astype(str)
    return clave, seq


def procesar_robinhood(df: pd.DataFrame, fecha_desde=None, cobrados=None, pendientes=None,
                       hist_tarjetas=None, cobrados_df=None) -> dict[str, pd.DataFrame]:
    """Transforma el CSV Robinhood en {robinhood_1444: DF} con UNA fila COP por transacción
    (1-a-1, Orden = robinhood_<sha1-12>; ver bloque de arriba). Filtra por Status ('Posted') y
    Type (Purchase/Refund). Levanta ValueError si falta la TRM de cualquier día con movimiento.
    'fecha_desde' descarta transacciones anteriores; None -> no procesa. 'cobrados' (OBLIGATORIO
    si fecha_desde activo) = set de Orden ya cobrados: se EXCLUYEN. 'pendientes' (opcional) =
    'pendientes_rematch': cobros sin fila firme en el CSV; los que matcheen (fecha+monto+merchant)
    se EXCLUYEN. 'hist_tarjetas' = filas de tarjeta del histórico para la TRM de reembolsos."""
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    faltan = [c for c in ROBINHOOD_COLS if c not in df.columns]
    if faltan:
        raise ValueError(f"El CSV Robinhood no tiene las columnas esperadas: {', '.join(faltan)}.")

    # INACTIVO sin fecha de corte: no se procesa nada (columnas validadas ANTES).
    if fecha_desde is None:
        return {}
    # 🛡️ LISTA DE EXCLUSIÓN obligatoria: procesar sin lista recobraría lo ya cobrado.
    if cobrados is None:
        raise ValueError(
            "Falta la lista de exclusión 'tarjetas cobradas' (cobrados=None). "
            "No se procesa nada: sin la lista se recobrarían transacciones ya cobradas."
        )

    # Cardholder -> casillero (ignora los que no están en el mapeo)
    df["_cas"] = df["Cardholder"].astype(str).str.strip().map(ROBINHOOD_CARDMAP)
    df = df[df["_cas"].notna()].copy()
    if df.empty:
        return {}

    # 1-a-1: Orden determinista sobre TODO el set 1444 (antes de filtrar Status) -> seq estable.
    _clave, _seq = _robinhood_clave_y_seq(df)
    df["_orden"] = "robinhood_" + (_clave + "|" + _seq).map(
        lambda s: hashlib.sha1(s.encode("utf-8")).hexdigest()[:12]
    )
    if df["_orden"].duplicated().any():
        raise ValueError(
            "Colisión de hash en el Orden Robinhood (dos transacciones distintas generaron el "
            "mismo ID). No se genera ningún movimiento; reporta este archivo."
        )

    # 🛡️ Universo de Orden y rango del extracto para la SEGUNDA BARRERA: TODO el set 1444, antes
    # de filtrar Status/Type/corte/lista (mismo universo con el que se generó la lista).
    _ordenes_universo = set(df["_orden"])
    _f_univ = pd.to_datetime(df["Date"], format="%Y-%m-%d", errors="coerce")
    _rango_extracto = (_f_univ.min(), _f_univ.max()) if _f_univ.notna().any() else None

    # Status: solo Posted. Desconocidos (ni Posted ni Pending/Declined/Other) -> avisar + ignorar.
    df["_st"] = df["Status"].astype(str).str.strip().str.upper()
    _st_conoc = {ROBINHOOD_STATUS_OK} | ROBINHOOD_STATUS_IGNORAR
    _st_desc = sorted(set(df["_st"].unique()) - _st_conoc)
    if _st_desc:
        _rakuten_warn("⚠️ Robinhood: Status NO reconocidos (ignorados): "
                      f"{', '.join(_st_desc)}. Revisa si Robinhood agregó un estado nuevo.")
    df = df[df["_st"] == ROBINHOOD_STATUS_OK].copy()

    # Type: Purchase/Refund. Desconocidos -> avisar + ignorar (no cargar a ciegas).
    df["_type"] = df["Type"].astype(str).str.strip().str.upper()
    _ty_conoc = set(ROBINHOOD_TIPO_MAP) | ROBINHOOD_TIPOS_IGNORAR
    _ty_desc = sorted(set(df["_type"].unique()) - _ty_conoc)
    if _ty_desc:
        _rakuten_warn("⚠️ Robinhood: Type NO reconocidos (ignorados): "
                      f"{', '.join(_ty_desc)}. Revisa si Robinhood agregó un tipo nuevo.")
    df["_tipo"] = df["_type"].map(ROBINHOOD_TIPO_MAP)
    df = df[df["_tipo"].notna()].copy()

    # Fecha (Robinhood viene YYYY-MM-DD) y monto USD absoluto (Amount == 0 se descarta)
    df["_fecha"] = pd.to_datetime(df["Date"], format="%Y-%m-%d", errors="coerce")
    df = df[df["_fecha"].notna()].copy()
    df["_amount"] = pd.to_numeric(df["Amount"], errors="coerce")
    df = df[df["_amount"].notna() & (df["_amount"] != 0)].copy()

    # 🔁 UNIVERSO DE COMPRAS (Purchase) para emparejar Refund: se captura ANTES del corte y de la
    # lista (la compra revertida puede ser vieja/ya cobrada). Solo lectura (presta TRM/fecha).
    _u = df[df["_tipo"] == "Egreso"].copy()
    _compras_universo = [
        {
            "id": r["_orden"],
            "fecha": r["_fecha"],
            "merch": _norm_merchant(r["Merchant"]),
            "usd": round(abs(float(r["_amount"])), 2),
            "cm": str(r["Cardholder"]).strip(),   # segmentación por Cardholder (Correal / Maria)
            "cas": ROBINHOOD_CASILLERO,
            "trm_fecha": r["_fecha"].strftime("%Y-%m-%d"),
        }
        for _, r in _u.iterrows()
    ]

    if fecha_desde is not None:
        df = df[df["_fecha"] >= pd.Timestamp(fecha_desde)]
    df["_usd"] = df["_amount"].abs()
    df["_fecha_iso"] = df["_fecha"].dt.strftime("%Y-%m-%d")
    if df.empty:
        return {}

    # 🛡️ Anti-doble-cobro (defensa PRINCIPAL): excluir Orden ya en la lista de cobradas.
    _ya = df["_orden"].isin(cobrados)
    if _ya.any():
        _cobradas_info(f"🛡️ Robinhood: {int(_ya.sum())} transacciones ya cobradas "
                       f"(lista de exclusión) — excluidas del cargue.")
        df = df[~_ya].copy()
    if df.empty:
        return {}

    # 🛡️ ESCUDO DE PENDIENTES: cobros reales sin fila firme en el CSV (auth). Si una transacción
    # matchea un pendiente por (fecha compra, monto USD, merchant) se excluye. Count-aware,
    # desempate por Orden ascendente (determinista).
    if pendientes is not None and len(pendientes) and "tarjeta" in pendientes.columns:
        _p = pendientes[pendientes["tarjeta"].astype(str).str.strip().str.lower() == "robinhood"]
        if len(_p):
            _pk: dict = {}
            for _, _rp in _p.iterrows():
                _k = (str(_rp["fecha_compra"]).strip()[:10],
                      round(abs(float(_rp["monto_usd"])), 2),
                      _norm_merchant(_rp["descripcion_excel"] if "descripcion_excel" in _p.columns else ""))
                _pk[_k] = _pk.get(_k, 0) + 1
            _mn = df["Merchant"].map(_norm_merchant)
            _a2 = df["_usd"].round(2)
            _drop = []
            for _k, _n in _pk.items():
                _m = df[(df["_fecha_iso"] == _k[0]) & (_a2 == _k[1]) & (_mn == _k[2])]
                if len(_m):
                    _drop += list(_m.sort_values("_orden").index[:_n])
            if _drop:
                _cobradas_info(f"🛡️ Robinhood: {len(_drop)} transacciones matchean cobros "
                               f"PENDIENTES de rematch — excluidas del cargue.")
                df = df.drop(index=_drop)
    if df.empty:
        return {}

    # 🛡️ SEGUNDA BARRERA ANTI-RECOBRO (por atributos) — el motivo por el que existe: Robinhood
    # re-fecha la transacción al asentar y su Orden cambia. El merchant de un Refund viene como
    # "Refund: <merchant>" -> se normaliza igual que en la lista.
    df["_merch_attr"] = df["Merchant"].map(_norm_merchant_refund_robin)
    _drop_attr = _excluir_por_atributos(df, cobrados_df, "robinhood", _ordenes_universo,
                                        _rango_extracto, "Robinhood")
    if _drop_attr:
        df = df.drop(index=_drop_attr)
    if df.empty:
        return {}

    # 🔁 Refund -> TRM DE SU COMPRA ORIGINAL (neteo exacto). Solo toca los Ingreso.
    _reembolsos = [
        {"id": r["_orden"], "fecha": r["_fecha"], "merch": _norm_merchant_refund_robin(r["Merchant"]),
         "usd": round(float(r["_usd"]), 2),
         "cm": str(r["Cardholder"]).strip(), "cas": ROBINHOOD_CASILLERO}
        for _, r in df[df["_tipo"] == "Ingreso"].iterrows()
    ]
    _trm_ok, _trm_sin_match, _trm_ambiguos = _resolver_trm_reembolsos(
        _reembolsos, _compras_universo,
        _indice_compras_historico(hist_tarjetas, "robinhood_", "Tarjeta Robinhood"),
    )
    if _trm_sin_match:
        _cobradas_warn(
            "⚠️ Robinhood: {} Refund sin compra original identificable (ni total ni parcial) "
            "— se usa la TRM de su propio día, como antes. REVISAR a mano: {}"
            .format(len(_trm_sin_match),
                    "; ".join(f"{x['fecha']:%Y-%m-%d} USD {x['usd']:.2f} {x['merch'][:36]} "
                              f"[{x['motivo']}]" for x in _trm_sin_match[:10]))
        )
    if _trm_ambiguos:
        _cobradas_info(
            f"ℹ️ Robinhood: {len(_trm_ambiguos)} Refund tenían varias compras candidatas con TRM "
            f"distintas; se tomó la compra MÁS RECIENTE anterior al reembolso."
        )

    # TRM por día (+125). Incluye los días de las COMPRAS ORIGINALES de los Refund emparejados.
    trm_cache: dict = {}
    faltantes = set()
    _dias = set(df["_fecha_iso"].unique()) | {
        f for f, origen, _, _p in _trm_ok.values() if origen == "extracto"
    }
    for f_iso in sorted(_dias):
        if _amex_trm_dia(f_iso, trm_cache) is None:
            faltantes.add(f_iso)
    if faltantes:
        dias = ", ".join(sorted(faltantes))
        raise ValueError(
            f"Sin TRM (datos.gov.co) para los días con movimiento Robinhood: {dias}. "
            f"No se genera ningún movimiento (no hay TRM de respaldo)."
        )

    cas = ROBINHOOD_CASILLERO
    filas = []
    for _, r in df.iterrows():
        tipo, f_iso = r["_tipo"], r["_fecha_iso"]
        trm = trm_cache[f_iso]
        etq = "gasto" if tipo == "Egreso" else "reembolso"
        _m = _trm_ok.get(r["_orden"]) if tipo == "Ingreso" else None
        if _m:
            _f_compra, _origen, _trm_hist, _parcial = _m
            trm = _trm_hist if _origen == "historico" else trm_cache[_f_compra]
            etq = f"reembolso{' parcial' if _parcial else ''} (TRM compra {_f_compra})"
        monto = round(float(r["_usd"]) * trm)  # COP, POSITIVO
        merch = " ".join(str(r["Merchant"]).split())
        filas.append({
            "Fecha": f_iso,
            "Tipo": tipo,
            "Monto": monto,
            "Orden": r["_orden"],
            "Motivo": "Tarjeta Robinhood",
            "TRM": round(trm, 2),
            "Usuario": ROBINHOOD_USUARIO,
            "Casillero": cas,
            "Estado de Orden": "",
            "Nombre del producto": f"Tarjeta Robinhood - {etq} - {merch}",
        })

    out = pd.DataFrame(filas)
    if out.empty:
        return {}

    # 🚩 BLINDAJE VENTANA MANUAL: entrantes con FECHA <= 22-jun -> marcar para REVISIÓN (no
    # asumir aprobadas). Los 2 in-window ya-cobrados están excluidos por la lista; esto atrapa
    # cualquier NUEVO in-window por el patrón auth-vs-asiento.
    _fin = pd.Timestamp(ROBINHOOD_VENTANA_MANUAL_FIN)
    _rev = out[pd.to_datetime(out["Fecha"], errors="coerce") <= _fin]
    if len(_rev):
        _det = "; ".join(
            f"{x['Fecha']} {x['Tipo']} USD {float(x['Monto'])/float(x['TRM']):.2f} "
            f"{x['Nombre del producto'].split(' - ')[-1][:28]}" for _, x in _rev.head(15).iterrows()
        )
        _cobradas_warn(
            f"🚩 Robinhood: {len(_rev)} transacción(es) ENTRANTE(s) con fecha ≤ "
            f"{ROBINHOOD_VENTANA_MANUAL_FIN} (ventana de bloques manuales del backoffice) — "
            f"REVISAR/APROBAR antes de cargar (posible ya-cobrado auth-vs-asiento): {_det}"
        )

    return {f"robinhood_{cas}": out.reset_index(drop=True)}


# ──────────────────────────────────────────────────────────────────────────────
# Cargue "Tarjeta Capital" (Capital One, 4ª tarjeta; módulo PARALELO, NO reusa procesar_*).
# SOLO Julian Sanchez -> casillero 13608. Fuente: CSV Capital One
#   (columnas: Transaction Date, Posted Date, Card No., Description, Category, Debit, Credit).
#
#   - SEGMENTACIÓN: se procesan SOLO las filas con Card No. == CAPITAL_CARD_NO ("1484"). Si el
#     CSV trae otras tarjetas se IGNORAN con aviso (Capital One deja bajar varias en un export).
#
#   - 💵 REGLA UNIFICADA (decisión 2026-08-10), idéntica a las otras 3 tarjetas:
#       Debit                       -> Egreso  (gasto)
#       Credit  Category=Merchandise-> Ingreso (devolución por cancelación: RESTA)
#       Credit  Category=Payment/Credit ("ELECTRONIC PAYMENT") -> IGNORAR (pago a la tarjeta:
#               es un abono del propio tarjetahabiente, ni suma ni resta)
#     Los Credit "Merchandise" son reembolsos eBay reales y el backoffice SÍ los netaba en sus
#     bloques manuales (p.ej. el cobro 152228 = 7.088,34 Debit − 5.583,00 Credit).
#     ⚠️ ANTI-DOBLE-ABONO: por eso los cobros-reembolso de la hoja congelada TAMBIÉN van a la
#     lista de exclusión (ver generar_capital_cobradas.py). Sin ellos, 4 reembolsos de julio ya
#     abonados dentro de los bloques 155110/155839 se abonarían por segunda vez.
#
#   - USD -> COP con la MISMA TRM que Amex (_amex_trm_dia: datos.gov.co, +125), por el día de
#     Transaction Date. *** SIN TRM de respaldo: si falta la de un día con movimiento, ValueError. ***
#
#   - 1-a-1: cada movimiento cargable -> UNA fila propia. Capital One NO trae ID nativo (no hay
#     Reference) -> Orden determinista por transacción:
#       clave = "<Transaction Date>|<Debit>|<Credit>|<Description>|<seq>"
#       Orden = "capital_" + sha1(clave, utf-8)[:12]
#     · Debit y Credit van como campos SEPARADOS (uno vacío): eso es lo que distingue una compra
#       de su reembolso, que en Capital comparten Description y monto. Verificado: 145/145 y
#       88/88 claves únicas en los dos extractos reales.
#     · Se usa TRANSACTION DATE (fecha de compra), no Posted Date: es la fecha con la que el
#       backoffice armó todos sus bloques ("del 9 al 13 Julio") y la que quedó en los cobros.
#     · VERIFICADO (2026-08-10) que Capital One NO re-fecha al asentar: de los 72 cobros de la
#       hoja congelada "Capital Julian" que caen dentro del rango del extracto, los 72 coinciden
#       con Transaction Date EXACTO, y 0 aparecen re-fechados. El lag Posted−Transaction es
#       0..2 días y no altera Transaction Date.
#     · La Description trae el order-id de eBay ("EBAY O*nn-nnnnn-nnnnn"), casi un ID nativo:
#       0 colisiones de clave en el extracto (145/145 y 88/88 únicas). El 'seq' queda como red
#       de seguridad de costo cero para dos compras idénticas el mismo día.
#     · seq sobre ORDEN CANÓNICO (clave, Posted Date) y NO sobre el orden de lectura, para que
#       dos descargas den el mismo seq a la misma transacción (misma razón que Robinhood).
#     Con el dedup existente por Orden (keep="last") el cargue es IDEMPOTENTE.
#
#   - Motivo = "Tarjeta Capital" (tag EXACTO que captura el incentivo; ver agregar_incentivo_amex).
#   - REEMBOLSOS: cada Ingreso se convierte a COP con la TRM DE SU COMPRA ORIGINAL (las mismas
#     3 pasadas de _resolver_trm_reembolsos que usan Amex/Rakuten/Robinhood), para que compra y
#     devolución neteen en 0 y no quede residuo cambiario. En Capital el merchant del reembolso
#     es la MISMA Description de la compra, así que no hay prefijo que quitar.
#   - COMISIÓN: 13608 NO tiene comisión quincenal (el bloque de comisión es `if cas == "1444"`).
#     Estas filas mueven el saldo de 13608 y nada más.
# ──────────────────────────────────────────────────────────────────────────────
CAPITAL_CASILLERO = "13608"
CAPITAL_USUARIO = "Julian Sanchez"
CAPITAL_CARD_NO = "1484"
CAPITAL_COLS = ["Transaction Date", "Posted Date", "Card No.", "Description", "Category",
                "Debit", "Credit"]
CAPITAL_CAT_PAGO = "Payment/Credit"   # pagos a la tarjeta: se ignoran SIEMPRE
CAPITAL_CAT_COMPRA = "Merchandise"
# 🚦 PERILLA: False (decisión vigente) -> los Credit de 'Merchandise' entran como Ingreso
# (devolución) con la TRM de su compra original. True -> se ignora TODO Credit (comportamiento
# anterior al 2026-08-10; deja el reembolso a cargo del mayorista).
CAPITAL_IGNORAR_CREDITOS = False

# 🚦 FECHA DE CORTE del cargue Capital — MISMAS 3 reglas que Amex/Rakuten/Robinhood:
#   1. el histórico de cobrados MANDA (la LISTA decide, no el corte);
#   2. el corte es solo límite de sanidad para no procesar historia irrelevante;
#   3. todo lo nuevo fuera de lista/pendientes se toma (nunca dejar de cobrar).
#   - None -> INACTIVO (kill switch de emergencia).
CAPITAL_FECHA_DESDE = "2026-07-01"


def _capital_cargables(df: pd.DataFrame) -> pd.Series:
    """Máscara de las filas que PUEDEN llegar al histórico: Debit > 0 (compra) o, si la perilla
    lo permite, Credit > 0 de Category 'Merchandise' (devolución). Los Credit de
    'Payment/Credit' (pagos a la tarjeta) nunca son cargables."""
    deb = pd.to_numeric(df["Debit"], errors="coerce")
    cre = pd.to_numeric(df["Credit"], errors="coerce")
    cat = df["Category"].astype(str).str.strip()
    m = deb.notna() & (deb > 0)
    if not CAPITAL_IGNORAR_CREDITOS:
        m = m | (cre.notna() & (cre > 0) & cat.eq(CAPITAL_CAT_COMPRA))
    return m


def _capital_clave_y_seq(df: pd.DataFrame):
    """Clave e índice de repetición del Orden Capital (1-a-1).

    Clave = Transaction Date | Debit | Credit | Description (valores CRUDOS del CSV: Capital One
    los escribe siempre como YYYY-MM-DD y \\d+\\.\\d{2}, verificado sobre 145 filas sin excepción).
    Debit y Credit son campos SEPARADOS (uno vacío): es lo único que distingue una compra de su
    reembolso, que comparten Description y monto.
    El 'seq' se asigna sobre un ORDEN CANÓNICO (clave, Posted Date) y NO sobre el orden de
    lectura, para que dos descargas den el mismo seq a la misma transacción. Dos filas con la
    misma clave son idénticas en todo lo que se hashea, así que da igual cuál recibe 0 o 1.
    Se espera recibir SOLO filas cargables (ver _capital_cargables): las ignoradas no generan
    Orden y por tanto no pueden robarle un seq a un movimiento real.
    """
    clave = (df["Transaction Date"].astype(str).str.strip() + "|"
             + df["Debit"].astype(str).str.strip() + "|"
             + df["Credit"].astype(str).str.strip() + "|"
             + df["Description"].astype(str).str.strip())
    canon = pd.DataFrame(
        {"_k": clave, "_p": df["Posted Date"].astype(str).str.strip()}
    ).sort_values(["_k", "_p"], kind="mergesort")
    seq = canon.groupby("_k").cumcount().reindex(df.index).astype(str)
    return clave, seq


def _capital_orden(clave: pd.Series, seq: pd.Series) -> pd.Series:
    """Orden 1-a-1 de Capital: capital_<sha1-12 de 'clave|seq'>."""
    return "capital_" + (clave + "|" + seq).map(
        lambda s: hashlib.sha1(s.encode("utf-8")).hexdigest()[:12]
    )


def procesar_capital(df: pd.DataFrame, fecha_desde=None, cobrados=None, pendientes=None,
                     hist_tarjetas=None, cobrados_df=None) -> dict[str, pd.DataFrame]:
    """Transforma el CSV Capital One en {capital_13608: DF} con UNA fila COP por COMPRA (Debit),
    1-a-1, Orden = capital_<sha1-12> (ver bloque de arriba). Ignora todos los Credit. Levanta
    ValueError si falta la TRM de cualquier día con movimiento. 'fecha_desde' descarta
    transacciones anteriores; None -> no procesa nada. 'cobrados' (OBLIGATORIO si fecha_desde
    está activo) = set de Orden ya cobrados: esas transacciones se EXCLUYEN. 'cobrados_df' = la
    lista completa con atributos, para la segunda barrera anti-recobro. 'hist_tarjetas' = filas
    de tarjeta del histórico, para darle a una devolución la TRM de su compra original cuando esa
    compra ya no está en el extracto.
    'pendientes' se acepta por simetría de firma con los otros módulos pero NO se usa: Capital no
    tiene auth pendientes de rematch."""
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    faltan = [c for c in CAPITAL_COLS if c not in df.columns]
    if faltan:
        raise ValueError(f"El CSV Capital One no tiene las columnas esperadas: {', '.join(faltan)}.")

    # INACTIVO sin fecha de corte: no se procesa nada (se validan columnas ANTES).
    if fecha_desde is None:
        return {}

    # 🛡️ LISTA DE EXCLUSIÓN obligatoria: procesar sin lista recobraría lo ya cobrado.
    if cobrados is None:
        raise ValueError(
            "Falta la lista de exclusión 'tarjetas cobradas' (cobrados=None). "
            "No se procesa nada: sin la lista se recobrarían transacciones ya cobradas."
        )

    # 🔒 SEGMENTACIÓN POR TARJETA: solo la 1484 (Julian). Otras se ignoran con aviso.
    _card = df["Card No."].astype(str).str.strip()
    _otras = sorted(set(_card.unique()) - {CAPITAL_CARD_NO})
    if _otras:
        _cobradas_warn(
            f"⚠️ Capital: el CSV trae {int((~_card.eq(CAPITAL_CARD_NO)).sum())} fila(s) de otras "
            f"tarjetas ({', '.join(_otras)}) — IGNORADAS. Solo se carga la {CAPITAL_CARD_NO} "
            f"(casillero {CAPITAL_CASILLERO})."
        )
    df = df[_card.eq(CAPITAL_CARD_NO)].copy()
    if df.empty:
        return {}

    # Category desconocida -> FAIL-LOUD (no cargar a ciegas si Capital One agrega un tipo nuevo).
    _cat = df["Category"].astype(str).str.strip()
    _cat_desconocidas = sorted(set(_cat.unique()) - {CAPITAL_CAT_COMPRA, CAPITAL_CAT_PAGO})
    if _cat_desconocidas:
        raise ValueError(
            f"Capital: Category NO reconocida en el CSV: {', '.join(_cat_desconocidas)}. "
            f"Esperadas: '{CAPITAL_CAT_COMPRA}' / '{CAPITAL_CAT_PAGO}'. No se genera ningún "
            f"movimiento (revisa si Capital One agregó una categoría nueva)."
        )

    df["_debit"] = pd.to_numeric(df["Debit"], errors="coerce")
    df["_credit"] = pd.to_numeric(df["Credit"], errors="coerce")
    # Una fila con Debit Y Credit a la vez sería un formato inesperado -> fail-loud.
    if (df["_debit"].notna() & df["_credit"].notna()).any():
        raise ValueError(
            "Capital: hay filas con Debit y Credit simultáneos (formato inesperado). "
            "No se genera ningún movimiento."
        )

    df["_fecha"] = pd.to_datetime(df["Transaction Date"], errors="coerce")
    if df["_fecha"].isna().any():
        raise ValueError(
            f"Capital: {int(df['_fecha'].isna().sum())} fila(s) con 'Transaction Date' ilegible. "
            f"No se genera ningún movimiento."
        )

    # Rango del extracto (sobre TODAS las filas, incluidas las ignoradas) para la 2ª barrera.
    _rango_extracto = (df["_fecha"].min(), df["_fecha"].max()) if len(df) else None

    # 💵 CLASIFICACIÓN (regla unificada): Debit -> Egreso, Credit 'Merchandise' -> Ingreso,
    # Credit 'Payment/Credit' -> IGNORAR (pago a la tarjeta, ni suma ni resta).
    _es_merch = _cat.eq(CAPITAL_CAT_COMPRA)
    _m_pago = df["_credit"].notna() & (df["_credit"] > 0) & ~_es_merch
    _m_dev = df["_credit"].notna() & (df["_credit"] > 0) & _es_merch
    if _m_pago.any():
        _cobradas_info(
            f"ℹ️ Capital: {int(_m_pago.sum())} pago(s) a la tarjeta ignorados "
            f"(USD {float(df.loc[_m_pago, '_credit'].sum()):,.2f}) — no son devolución."
        )
    if CAPITAL_IGNORAR_CREDITOS and _m_dev.any():
        _cobradas_warn(
            f"⚠️ Capital: {int(_m_dev.sum())} devolución(es) de '{CAPITAL_CAT_COMPRA}' "
            f"(USD {float(df.loc[_m_dev, '_credit'].sum()):,.2f}) NO se abonan al mayorista "
            f"(CAPITAL_IGNORAR_CREDITOS=True)."
        )

    # Solo lo cargable. El Orden se calcula sobre ESTE universo (antes del corte y de la lista),
    # para que el 'seq' no dependa de qué filas se filtren después.
    df = df[_capital_cargables(df)].copy()
    if df.empty:
        return {}
    _clave, _seq = _capital_clave_y_seq(df)
    df["_orden"] = _capital_orden(_clave, _seq)
    # FAIL-LOUD: una colisión de hash entre claves distintas colapsaría dos movimientos en uno.
    if df["_orden"].duplicated().any():
        raise ValueError(
            "Colisión de hash en el Orden Capital (dos transacciones distintas generaron el "
            "mismo ID). No se genera ningún movimiento; reporta este archivo."
        )
    _ordenes_universo = set(df["_orden"])
    df["_tipo"] = np.where(df["_debit"].notna() & (df["_debit"] > 0), "Egreso", "Ingreso")
    df["_usd"] = df["_debit"].fillna(df["_credit"]).abs()
    df["_merch_attr"] = df["Description"].map(_norm_merchant)

    # 🔁 UNIVERSO DE COMPRAS para emparejar los reembolsos: se captura ANTES del corte y de la
    # lista (la compra revertida puede ser vieja o ya cobrada). Solo presta fecha/TRM.
    _compras_universo = [
        {
            "id": r["_orden"],
            "fecha": r["_fecha"],
            "merch": r["_merch_attr"],
            "usd": round(float(r["_usd"]), 2),
            # Capital es de UNA sola tarjeta: se puebla 'cm' igual que Rakuten para que
            # _resolver_trm_reembolsos aplique la misma regla sin casos especiales.
            "cm": CAPITAL_USUARIO,
            "cas": CAPITAL_CASILLERO,
            "trm_fecha": r["_fecha"].strftime("%Y-%m-%d"),
        }
        for _, r in df[df["_tipo"] == "Egreso"].iterrows()
    ]

    if fecha_desde is not None:
        df = df[df["_fecha"] >= pd.Timestamp(fecha_desde)].copy()
    if df.empty:
        return {}
    df["_fecha_iso"] = df["_fecha"].dt.strftime("%Y-%m-%d")

    # 🛡️ Anti-doble-cobro / anti-doble-abono (defensa PRINCIPAL): excluir los movimientos cuyo
    # Orden ya está en la lista. Cubre por igual compras ya cobradas y reembolsos YA ABONADOS
    # por el backoffice dentro de sus bloques manuales. Va ANTES de la TRM.
    _ya = df["_orden"].isin(cobrados)
    if _ya.any():
        _neg = df[_ya & (df["_tipo"] == "Egreso")]
        _pos = df[_ya & (df["_tipo"] == "Ingreso")]
        _cobradas_info(
            f"🛡️ Capital: {int(_ya.sum())} movimiento(s) ya liquidados (lista de exclusión) — "
            f"excluidos: {len(_neg)} compra(s) ya cobrada(s) USD {float(_neg['_usd'].sum()):,.2f} "
            f"+ {len(_pos)} devolución(es) YA ABONADA(S) USD {float(_pos['_usd'].sum()):,.2f}."
        )
        df = df[~_ya].copy()
    if df.empty:
        return {}

    # 🛡️ SEGUNDA BARRERA ANTI-RECOBRO (por atributos, independiente del hash): tapa el caso en
    # que Capital One re-expidiera un movimiento ya liquidado con la fecha corrida. Aquí el
    # 'merchant' es la Description (trae el order-id de eBay) -> emparejamiento muy preciso.
    # 🔏 La llave incluye el SIGNO: sin él, un cobro-compra huérfano taparía al reembolso de esa
    # misma compra (misma Description y mismo monto, a ≤3 días).
    df["_cas"] = CAPITAL_CASILLERO
    df["_tipo_attr"] = df["_tipo"]
    _drop_attr = _excluir_por_atributos(df, cobrados_df, "capital", _ordenes_universo,
                                        _rango_extracto, "Capital")
    if _drop_attr:
        df = df.drop(index=_drop_attr)
    if df.empty:
        return {}

    # 🔁 Reembolso -> TRM DE SU COMPRA ORIGINAL (neteo exacto). Solo toca los Ingreso.
    _reembolsos = [
        {"id": r["_orden"], "fecha": r["_fecha"], "merch": r["_merch_attr"],
         "usd": round(float(r["_usd"]), 2),
         "cm": CAPITAL_USUARIO, "cas": CAPITAL_CASILLERO}
        for _, r in df[df["_tipo"] == "Ingreso"].iterrows()
    ]
    _trm_ok, _trm_sin_match, _trm_ambiguos = _resolver_trm_reembolsos(
        _reembolsos, _compras_universo,
        _indice_compras_historico(hist_tarjetas, "capital_", "Tarjeta Capital"),
    )
    if _trm_sin_match:
        _cobradas_warn(
            "⚠️ Capital: {} devolución(es) sin compra original identificable (ni total ni "
            "parcial) — se usa la TRM de su propio día. REVISAR a mano: {}"
            .format(len(_trm_sin_match),
                    "; ".join(f"{x['fecha']:%Y-%m-%d} USD {x['usd']:.2f} {x['merch'][:36]} "
                              f"[{x['motivo']}]" for x in _trm_sin_match[:10]))
        )
    if _trm_ambiguos:
        _cobradas_info(
            f"ℹ️ Capital: {len(_trm_ambiguos)} devolución(es) tenían varias compras candidatas "
            f"con TRM distintas; se tomó la compra MÁS RECIENTE anterior al reembolso."
        )

    # TRM por día (+125). Incluye los días de las COMPRAS ORIGINALES de los reembolsos casados.
    trm_cache: dict = {}
    _dias = set(df["_fecha_iso"].unique()) | {
        f for f, origen, _t, _p in _trm_ok.values() if origen == "extracto"
    }
    faltantes = {f for f in sorted(_dias) if _amex_trm_dia(f, trm_cache) is None}
    if faltantes:
        raise ValueError(
            f"Sin TRM (datos.gov.co) para los días con movimiento Capital: "
            f"{', '.join(sorted(faltantes))}. No se genera ningún movimiento "
            f"(no hay TRM de respaldo)."
        )

    filas = []
    for _, r in df.iterrows():
        tipo, f_iso = r["_tipo"], r["_fecha_iso"]
        trm = trm_cache[f_iso]
        etq = "gasto" if tipo == "Egreso" else "reembolso"
        _m = _trm_ok.get(r["_orden"]) if tipo == "Ingreso" else None
        if _m:
            _f_compra, _origen, _trm_hist, _parcial = _m
            trm = _trm_hist if _origen == "historico" else trm_cache[_f_compra]
            etq = f"reembolso{' parcial' if _parcial else ''} (TRM compra {_f_compra})"
        desc = " ".join(str(r["Description"]).split())
        filas.append({
            "Fecha": f_iso,
            "Tipo": tipo,
            "Monto": round(float(r["_usd"]) * trm),   # COP, POSITIVO (el signo lo lleva 'Tipo')
            "Orden": r["_orden"],
            "Motivo": "Tarjeta Capital",
            "TRM": round(trm, 2),
            "Usuario": CAPITAL_USUARIO,
            "Casillero": CAPITAL_CASILLERO,
            "Estado de Orden": "",
            "Nombre del producto": f"Tarjeta Capital - {etq} - {desc}",
        })

    out = pd.DataFrame(filas)
    if out.empty:
        return {}
    return {f"capital_{CAPITAL_CASILLERO}": out.reset_index(drop=True)}


# ──────────────────────────────────────────────────────────────────────────────
# Cargue "Tarjeta US Bank" (US Bank 0613, 5ª tarjeta; módulo PARALELO, NO reusa procesar_*).
# Es la PRIMERA tarjeta MULTI-CASILLERO: una sola cuenta con varias sub-tarjetas, cada una de
# un mayorista distinto. Fuente: CSV US Bank (Date, Transaction, Name, Memo, Amount).
#
#   - 🔑 SEGMENTACIÓN POR SUB-TARJETA, NO POR NOMBRE. El campo 'Memo' trae 6 subcampos
#     separados por ';':
#         <referencia>; <mcc>; <cod><cod><APELLIDO,NOMBRE>; ; ; <cod>0
#              [0]        [1]              [2]              [3][4]   [5]
#     El código de sub-tarjeta son los 4 primeros dígitos de [2] y es la LLAVE. El nombre que
#     sigue es solo control cruzado: si no coincide con el código se AVISA y manda el código.
#     Se hace así a propósito — en Amex hubo que mantener dos grafías de la misma persona
#     ('K LOPEZ VELANDIA' / 'KELLY P LOPEZVELANDIA') porque el emisor cambia la escritura.
#     El código no cambia nunca.
#
#   - 💵 CLASIFICACIÓN — las reglas se evalúan EN ESTE ORDEN, la primera que aplica decide:
#       1. titular vacío            -> IGNORAR
#       2. MCC 00300                -> IGNORAR (pago a la tarjeta)
#       3. MCC 00761                -> IGNORAR (cuota de manejo)
#       4. código 0534 (Santiago)   -> IGNORAR (no es mayorista)
#       5. beneficio Amazon         -> IGNORAR (ver abajo)
#       6. DEBIT                    -> Egreso  (compra)
#       7. CREDIT                   -> Ingreso (devolución)
#
#     ⚠️ LA REGLA 1 VA PRIMERA Y NO ES NEGOCIABLE. En el extracto de agosto hay un par
#     'PYMT REVERSAL' (DEBIT -6.022,36) / 'PYMT THANK YOU' (CREDIT +6.022,36) del 19-ago con
#     MCC 05999, NO 00300: un pago a la tarjeta que rebotó y se reversó. Filtrando solo por MCC
#     entrarían 6.022 USD como gasto real de alguien. Lo ÚNICO que los distingue es el titular
#     vacío.
#
#     ⚠️ EL MCC NO IDENTIFICA DEVOLUCIONES. De los 57 CREDIT con titular, 56 llevan MCC 05999
#     pero uno lleva 05311 (conservó el MCC del comercio). La regla correcta es
#     'CREDIT + titular + no es pago', NUNCA 'MCC == 05999'.
#
#   - 🔴 BENEFICIO AMAZON (USBANK_EXCLUIR_BENEFICIO_AMAZON): las filas CREDIT rotuladas
#     'AMAZON PAY YOUR CHARGES' NO son devoluciones del mayorista: son el abono del acuerdo
#     comercial de Amazon a la cuenta (Amazon lo tenía con Amex y lo trasladó a US Bank). El
#     beneficio lo consiguió Encargomio, no el mayorista. En el extracto de agosto son 31 filas
#     por 7.216,61 USD (~23,3 M COP), todas del 19-ago y todas en la sub-tarjeta madre 2529;
#     pasárselas a Maria como devolución sería regalarle ese margen. Tres evidencias:
#       · es la única etiqueta que existe SOLO del lado del crédito — toda devolución real llega
#         con el nombre del comercio, idéntico al de su compra (Amazon devuelve como
#         'AMAZON MARKETPLACE NA PA', eBay como 'PAYPAL *EBAY 800-456-3229');
#       · se pega en la sub-tarjeta madre de la cuenta, donde también caen los pagos;
#       · las devoluciones reales siguen al comprador: Santiago devolvió 280,97 USD en eBay y el
#         crédito quedó en SU sub-tarjeta 0534. Ninguna de las 31 llegó a Santiago, Julian ni
#         Paula, pese a que los tres compraron en Amazon.
#     Es una PERILLA a propósito: si el PDF del extracto demostrara que son returns, se pone en
#     False y se recarga (el cargue es idempotente).
#
#   - 1-a-1: Orden HÍBRIDO, porque no todas las filas traen referencia.
#       · con referencia -> "usbank_<referencia>"  (referencia numérica de 18 o 23 dígitos;
#         VERIFICADO: 0 duplicadas entre las filas cargables)
#       · sin referencia -> "usbank_<sha1-12 de 'Date|Amount|Name_norm|seq'>"
#     EL 'seq' NO ES OPCIONAL: entre las filas sin referencia hay 3 colisiones exactas de
#     (Date, Amount, Name) que afectan 6 filas (688,16 · 673,44 · 629,28, todas de Kelly). Sin
#     seq se colapsarían de a pares y se perderían ~1.991 USD. El seq se asigna sobre ORDEN
#     CANÓNICO (Date, Amount, Name) y NO sobre el orden de lectura, para que dos descargas den
#     el mismo seq a la misma transacción (misma razón que Robinhood y Capital). El hash NO usa
#     hora: el archivo no la trae.
#
#   - USD -> COP con la MISMA TRM que Amex (_amex_trm_dia: datos.gov.co, +125), por el día de
#     'Date'. *** SIN TRM de respaldo: si falta la de un día con movimiento, ValueError. ***
#   - Motivo = "Tarjeta US Bank" (tag EXACTO que capta agregar_incentivo_amex).
#   - REEMBOLSOS: cada Ingreso se convierte con la TRM DE SU COMPRA ORIGINAL (las 3 pasadas de
#     _resolver_trm_reembolsos). ⚠️ El 'Name' de US Bank es GENÉRICO ('PAYPAL *EBAY 800-456-3229'
#     se repite en cientos de compras), así que el emparejamiento por merchant es mucho menos
#     preciso que en Capital (que trae el order-id de eBay). Se ESPERA que varias devoluciones
#     caigan en el fallback de "TRM de su propio día" con warning; es aceptable porque compras y
#     devoluciones son del mismo día y la TRM coincide. NO forzar el emparejamiento.
#   - COMISIÓN: 1444 SÍ tiene comisión quincenal y USBANK_AFECTA_COMISION_1444=True, así que
#     estas filas ENTRAN en la base de la comisión (igual que Amex/Rakuten/Robinhood).
# ──────────────────────────────────────────────────────────────────────────────
USBANK_CARD_NO = "0613"
USBANK_COLS = ["Date", "Transaction", "Name", "Memo", "Amount"]
USBANK_MCC_PAGO = "00300"        # pago a la tarjeta: se ignora SIEMPRE
USBANK_MCC_FEE = "00761"         # cuota de manejo: se ignora SIEMPRE
USBANK_MAP_SUBTARJETA = {"0598": "11591", "0609": "13608"}
# 🔴 2529 (Kelly Lopez Velandia) SE IGNORA desde el 2026-08-25. En el primer cargue se mapeó a
# 1444 por analogía con Amex (donde Kelly SÍ compra para Maria Moises), y fue un ERROR: sus
# 1.063 movimientos se cobraron a Maria y hubo que revertirlos. Es además la tarjeta MADRE de la
# cuenta, donde se pegan los pagos y el beneficio de Amazon, así que un mapeo equivocado ahí es
# el más caro de todos (fueron 1.372 millones de COP).
# ⚠️ NO reasignarla a un casillero sin confirmación explícita del usuario: que Kelly compre para
# Maria en Amex NO implica que lo haga en US Bank.
USBANK_SUBTARJETAS_IGNORAR = {"0534", "2529"}   # 0534 Santiago Largo · 2529 Kelly (sin asignar)
USBANK_NOMBRE_ESPERADO = {                 # solo control cruzado (el código manda)
    "2529": "LOPEZ VELANDIA,KELLY P",
    "0598": "HERRERA,PAULA",
    "0609": "SANCHEZ,JULIAN",
    "0534": "LARGO,SANTIAGO",
}
USBANK_USUARIOS = {"1444": "Maria Moises", "11591": "Paula Herrera", "13608": "Julian Sanchez"}
USBANK_MOTIVO = "Tarjeta US Bank"
# 🚦 True (decisión vigente) -> las filas de US Bank ENTRAN en la base de la comisión de 1444.
USBANK_AFECTA_COMISION_1444 = True
# 🚦 True (decisión vigente) -> el abono 'AMAZON PAY YOUR CHARGES' NO se le pasa al mayorista.
USBANK_EXCLUIR_BENEFICIO_AMAZON = True
USBANK_ETIQUETA_BENEFICIO = "AMAZON PAY YOUR CHARGES"

# 🚦 FECHA DE CORTE — MISMAS 3 reglas que las otras 4 tarjetas. Es LÍMITE DE SANIDAD, no un
# corte real: la historia completa de la tarjeta arranca el 2026-08-17, así que no deja nada
# afuera. ⚠️ NO MOVERLA HACIA ATRÁS NUNCA: alcanzaría quincenas ya comisionadas de 1444 y, como
# las de inicio >= CUTOFF_COMISION_NUEVA se reescriben en cada corrida, la comisión se
# recalcularía EN SILENCIO.  None -> INACTIVO (kill switch).
USBANK_FECHA_DESDE = "2026-08-17"


def _usbank_partes(df: pd.DataFrame) -> pd.DataFrame:
    """Parte el 'Memo' en sus subcampos y devuelve las columnas auxiliares (_ref/_mcc/_cod)."""
    p = df["Memo"].astype(str).str.split(";")
    out = pd.DataFrame(index=df.index)
    out["_ref"] = p.str[0].str.strip()
    out["_mcc"] = p.str[1].str.strip()
    out["_tit"] = p.str[2].str.strip()
    out["_cod"] = out["_tit"].str[:4]
    # El código va REPETIDO antes del nombre ("25292529LOPEZ VELANDIA,KELLY P"), así que el
    # nombre empieza en la posición 8, no en la 4.
    out["_nom"] = out["_tit"].str[8:].str.strip()
    return out


def _usbank_es_beneficio(df: pd.DataFrame) -> pd.Series:
    """Filas del abono comercial de Amazon (no son devolución del mayorista)."""
    return (df["Transaction"].astype(str).str.strip().str.upper().eq("CREDIT")
            & df["Name"].astype(str).str.upper().str.contains(
                USBANK_ETIQUETA_BENEFICIO, regex=False, na=False))


def _usbank_clave_y_seq(df: pd.DataFrame):
    """Clave e índice de repetición para las filas SIN referencia.

    Clave = Date | Amount | Name normalizado (valores CRUDOS del CSV). El 'seq' se asigna sobre
    ORDEN CANÓNICO (clave) y NO sobre el orden de lectura, para que dos descargas den el mismo
    seq a la misma transacción. Dos filas con la misma clave son idénticas en todo lo que se
    hashea, así que da igual cuál recibe 0 o 1.
    """
    clave = (df["Date"].astype(str).str.strip() + "|"
             + df["Amount"].astype(str).str.strip() + "|"
             + df["Name"].map(_norm_merchant))
    canon = pd.DataFrame({"_k": clave}).sort_values("_k", kind="mergesort")
    seq = canon.groupby("_k").cumcount().reindex(df.index).astype(str)
    return clave, seq


def _usbank_orden(ref: pd.Series, clave: pd.Series, seq: pd.Series) -> pd.Series:
    """Orden híbrido: la referencia cuando existe; si no, sha1-12 de 'clave|seq'."""
    hashed = (clave + "|" + seq).map(
        lambda s: "usbank_" + hashlib.sha1(s.encode("utf-8")).hexdigest()[:12]
    )
    return np.where(ref.ne(""), "usbank_" + ref, hashed)


def procesar_usbank(df: pd.DataFrame, fecha_desde=None, cobrados=None, pendientes=None,
                    hist_tarjetas=None, cobrados_df=None) -> dict[str, pd.DataFrame]:
    """Transforma el CSV de US Bank en {usbank_<casillero>: DF} con UNA fila COP por movimiento.

    Es MULTI-CASILLERO: devuelve una entrada por cada casillero con movimientos. Levanta
    ValueError si faltan columnas o si falta la TRM de cualquier día con movimiento.
    'cobrados' (OBLIGATORIO) = set de Orden ya cobrados. 'pendientes' se acepta por simetría de
    firma pero NO se usa: US Bank no tiene auth pendientes de rematch.
    """
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    faltan = [c for c in USBANK_COLS if c not in df.columns]
    if faltan:
        raise ValueError(f"El CSV de US Bank no tiene las columnas esperadas: {', '.join(faltan)}.")

    if fecha_desde is None:
        return {}

    if cobrados is None:
        raise ValueError(
            "Falta la lista de exclusión 'tarjetas cobradas' (cobrados=None). "
            "No se procesa nada: sin la lista se recobrarían transacciones ya cobradas."
        )

    aux = _usbank_partes(df)
    df = pd.concat([df, aux], axis=1)

    df["_fecha"] = pd.to_datetime(df["Date"], errors="coerce")
    if df["_fecha"].isna().any():
        raise ValueError(
            f"US Bank: {int(df['_fecha'].isna().sum())} fila(s) con 'Date' ilegible. "
            f"No se genera ningún movimiento."
        )
    df["_amount"] = pd.to_numeric(df["Amount"], errors="coerce")
    if df["_amount"].isna().any():
        raise ValueError(
            f"US Bank: {int(df['_amount'].isna().sum())} fila(s) con 'Amount' ilegible. "
            f"No se genera ningún movimiento."
        )
    _tx = df["Transaction"].astype(str).str.strip().str.upper()
    _tx_raras = sorted(set(_tx.unique()) - {"DEBIT", "CREDIT"})
    if _tx_raras:
        raise ValueError(
            f"US Bank: 'Transaction' NO reconocido: {', '.join(_tx_raras)}. "
            f"Esperados: DEBIT / CREDIT. No se genera ningún movimiento."
        )

    # Rango del extracto (sobre TODAS las filas) para la 2ª barrera.
    _rango_extracto = (df["_fecha"].min(), df["_fecha"].max()) if len(df) else None

    # ── DESCARTES, en el orden de las reglas ─────────────────────────────────
    _sin_tit = df["_tit"].eq("")
    _pago = ~_sin_tit & df["_mcc"].eq(USBANK_MCC_PAGO)
    _fee = ~_sin_tit & ~_pago & df["_mcc"].eq(USBANK_MCC_FEE)
    _ignorar_cod = ~_sin_tit & ~_pago & ~_fee & df["_cod"].isin(USBANK_SUBTARJETAS_IGNORAR)
    _benef = pd.Series(False, index=df.index)
    if USBANK_EXCLUIR_BENEFICIO_AMAZON:
        _benef = ~_sin_tit & ~_pago & ~_fee & ~_ignorar_cod & _usbank_es_beneficio(df)

    # Avisos (por NATURALEZA del movimiento, que es como se audita el extracto)
    _n_pago_tot = int(df["_mcc"].eq(USBANK_MCC_PAGO).sum())
    if _n_pago_tot:
        _cobradas_info(
            f"ℹ️ US Bank: {_n_pago_tot} pago(s) a la tarjeta ignorados "
            f"(USD {float(df.loc[df['_mcc'].eq(USBANK_MCC_PAGO), '_amount'].sum()):,.2f}) "
            f"— no son devolución."
        )
    if int(_sin_tit.sum()):
        _cobradas_info(
            f"ℹ️ US Bank: {int(_sin_tit.sum())} fila(s) SIN TITULAR ignoradas "
            f"(reversos de pago y cuotas de la cuenta, no son de ningún mayorista)."
        )
    if int(_ignorar_cod.sum()):
        _cobradas_info(
            f"ℹ️ US Bank: {int(_ignorar_cod.sum())} fila(s) de sub-tarjetas excluidas "
            f"({', '.join(sorted(USBANK_SUBTARJETAS_IGNORAR))}) — no son mayoristas."
        )
    if int(_benef.sum()):
        _cobradas_info(
            f"🎁 US Bank: {int(_benef.sum())} fila(s) de «{USBANK_ETIQUETA_BENEFICIO}» "
            f"(USD {float(df.loc[_benef, '_amount'].sum()):,.2f}) NO se abonan al mayorista: "
            f"son el beneficio comercial de Amazon a la cuenta, no una devolución."
        )

    df = df[~(_sin_tit | _pago | _fee | _ignorar_cod | _benef)].copy()
    if df.empty:
        return {}

    # Sub-tarjeta desconocida -> FAIL-LOUD (no adivinar a quién cobrarle).
    _desconocidas = sorted(set(df["_cod"].unique()) - set(USBANK_MAP_SUBTARJETA))
    if _desconocidas:
        raise ValueError(
            f"US Bank: sub-tarjeta(s) NO mapeada(s): {', '.join(_desconocidas)}. "
            f"Añádelas a USBANK_MAP_SUBTARJETA o a USBANK_SUBTARJETAS_IGNORAR. "
            f"No se genera ningún movimiento."
        )

    # Control cruzado nombre vs código: el CÓDIGO manda, el nombre solo avisa.
    _esp = df["_cod"].map(USBANK_NOMBRE_ESPERADO)
    _mismatch = _esp.notna() & df["_nom"].str.upper().ne(_esp.str.upper())
    if _mismatch.any():
        _ej = df.loc[_mismatch, ["_cod", "_nom"]].drop_duplicates().head(5)
        _cobradas_warn(
            f"⚠️ US Bank: {int(_mismatch.sum())} fila(s) con el nombre distinto al esperado para "
            f"su código — se respeta el CÓDIGO: "
            + "; ".join(f"{r['_cod']} trae '{r['_nom']}' (esperado "
                        f"'{USBANK_NOMBRE_ESPERADO[r['_cod']]}')" for _, r in _ej.iterrows())
        )

    df["_cas"] = df["_cod"].map(USBANK_MAP_SUBTARJETA)
    df["_tipo"] = np.where(
        df["Transaction"].astype(str).str.strip().str.upper().eq("DEBIT"), "Egreso", "Ingreso")
    df["_usd"] = df["_amount"].abs()
    df["_merch_attr"] = df["Name"].map(_norm_merchant)

    # Orden híbrido. Se calcula sobre ESTE universo (antes del corte y de la lista) para que el
    # 'seq' no dependa de qué filas se filtren después.
    _clave, _seq = _usbank_clave_y_seq(df)
    df["_orden"] = _usbank_orden(df["_ref"], _clave, _seq)
    if df["_orden"].duplicated().any():
        _dups = df.loc[df["_orden"].duplicated(keep=False), "_orden"].unique()[:5]
        raise ValueError(
            f"US Bank: colisión de Orden ({len(_dups)}+ casos, p.ej. {', '.join(_dups)}). "
            f"Dos movimientos distintos generarían el mismo ID. No se genera ningún movimiento."
        )
    _ordenes_universo = set(df["_orden"])

    # 🔁 UNIVERSO DE COMPRAS para emparejar devoluciones: ANTES del corte y de la lista.
    _compras_universo = [
        {
            "id": r["_orden"],
            "fecha": r["_fecha"],
            "merch": r["_merch_attr"],
            "usd": round(float(r["_usd"]), 2),
            "cm": USBANK_USUARIOS.get(r["_cas"], r["_cas"]),
            "cas": r["_cas"],
            "trm_fecha": r["_fecha"].strftime("%Y-%m-%d"),
        }
        for _, r in df[df["_tipo"] == "Egreso"].iterrows()
    ]

    df = df[df["_fecha"] >= pd.Timestamp(fecha_desde)].copy()
    if df.empty:
        return {}
    df["_fecha_iso"] = df["_fecha"].dt.strftime("%Y-%m-%d")

    # 🛡️ Barrera 1 — lista de exclusión por Orden.
    _ya = df["_orden"].isin(cobrados)
    if _ya.any():
        _neg = df[_ya & (df["_tipo"] == "Egreso")]
        _pos = df[_ya & (df["_tipo"] == "Ingreso")]
        _cobradas_info(
            f"🛡️ US Bank: {int(_ya.sum())} movimiento(s) ya liquidados (lista de exclusión) — "
            f"excluidos: {len(_neg)} compra(s) USD {float(_neg['_usd'].sum()):,.2f} "
            f"+ {len(_pos)} devolución(es) USD {float(_pos['_usd'].sum()):,.2f}."
        )
        df = df[~_ya].copy()
    if df.empty:
        return {}

    # 🛡️ Barrera 2 — por atributos (independiente del hash), con SIGNO.
    df["_tipo_attr"] = df["_tipo"]
    _drop_attr = _excluir_por_atributos(df, cobrados_df, "usbank", _ordenes_universo,
                                        _rango_extracto, "US Bank")
    if _drop_attr:
        df = df.drop(index=_drop_attr)
    if df.empty:
        return {}

    # 🔁 Devolución -> TRM de su compra original.
    _reembolsos = [
        {"id": r["_orden"], "fecha": r["_fecha"], "merch": r["_merch_attr"],
         "usd": round(float(r["_usd"]), 2),
         "cm": USBANK_USUARIOS.get(r["_cas"], r["_cas"]), "cas": r["_cas"]}
        for _, r in df[df["_tipo"] == "Ingreso"].iterrows()
    ]
    _trm_ok, _trm_sin_match, _trm_ambiguos = _resolver_trm_reembolsos(
        _reembolsos, _compras_universo,
        _indice_compras_historico(hist_tarjetas, "usbank_", USBANK_MOTIVO),
    )
    if _trm_sin_match:
        _cobradas_warn(
            "⚠️ US Bank: {} devolución(es) sin compra original identificable — se usa la TRM de "
            "su propio día. Es lo ESPERADO en esta tarjeta (el 'Name' es genérico y no distingue "
            "la compra); el costo en COP es ~0 cuando compra y devolución son del mismo día. "
            "Revisar solo si alguna es de un día distinto al de su compra: {}"
            .format(len(_trm_sin_match),
                    "; ".join(f"{x['fecha']:%Y-%m-%d} USD {x['usd']:.2f} {x['merch'][:32]} "
                              f"[{x['motivo']}]" for x in _trm_sin_match[:10]))
        )
    if _trm_ambiguos:
        _cobradas_info(
            f"ℹ️ US Bank: {len(_trm_ambiguos)} devolución(es) tenían varias compras candidatas "
            f"con TRM distintas; se tomó la compra MÁS RECIENTE anterior a la devolución."
        )

    # TRM por día (+125), incluidos los días de las compras originales casadas.
    trm_cache: dict = {}
    _dias = set(df["_fecha_iso"].unique()) | {
        f for f, origen, _t, _p in _trm_ok.values() if origen == "extracto"
    }
    faltantes = {f for f in sorted(_dias) if _amex_trm_dia(f, trm_cache) is None}
    if faltantes:
        raise ValueError(
            f"Sin TRM (datos.gov.co) para los días con movimiento US Bank: "
            f"{', '.join(sorted(faltantes))}. No se genera ningún movimiento "
            f"(no hay TRM de respaldo)."
        )

    filas = []
    for _, r in df.iterrows():
        tipo, f_iso, cas = r["_tipo"], r["_fecha_iso"], r["_cas"]
        trm = trm_cache[f_iso]
        etq = "gasto" if tipo == "Egreso" else "devolucion"
        _m = _trm_ok.get(r["_orden"]) if tipo == "Ingreso" else None
        if _m:
            _f_compra, _origen, _trm_hist, _parcial = _m
            trm = _trm_hist if _origen == "historico" else trm_cache[_f_compra]
            etq = f"devolucion{' parcial' if _parcial else ''} (TRM compra {_f_compra})"
        desc = " ".join(str(r["Name"]).split())
        filas.append({
            "Fecha": f_iso,
            "Tipo": tipo,
            "Monto": round(float(r["_usd"]) * trm),   # COP, POSITIVO (el signo lo lleva 'Tipo')
            "Orden": r["_orden"],
            "Motivo": USBANK_MOTIVO,
            "TRM": round(trm, 2),
            "Usuario": USBANK_USUARIOS.get(cas, cas),
            "Casillero": cas,
            "Estado de Orden": "",
            "Nombre del producto": f"{USBANK_MOTIVO} - {etq} - {desc}",
        })

    out = pd.DataFrame(filas)
    if out.empty:
        return {}
    return {f"usbank_{cas}": g.reset_index(drop=True)
            for cas, g in out.groupby("Casillero")}


# ──────────────────────────────────────────────────────────────────────────────
# INCENTIVO AMEX MENSUAL (cashback). Por cada mes CERRADO, agrega un Ingreso al casillero
# = INCENTIVO_COP_POR_USD * USD_neto, donde USD_neto = Σ(USD egresos Amex) − Σ(USD ingresos Amex)
# y USD_fila = Monto_COP / TRM_fila (la TRM del histórico YA incluye el spread +125, así que
# COP/TRM recupera el USD original — no se ajusta spread).
#   - Solo casilleros Amex (AMEX_USUARIOS: 11591, 1444, 13608).
#   - Identifica las filas de tarjeta por Motivo EXACTO ∈ {"Tarjeta Amex", "Tarjeta Rakuten",
#     "Tarjeta Robinhood", "Tarjeta Capital"} y SUMA todas al mismo incentivo mensual de 1444 (un solo
#     incentivoamex_1444_<mes>), EXCLUYENDO
#     las propias filas de incentivo. (Motivo exacto = más robusto que buscar el texto "amex".)
#   - Idempotente: Orden único incentivoamex_<cas>_<YYYY-MM> + chequeo de existencia (no recrea
#     ni recalcula un mes ya creado; queda congelado).
#   - Mes cerrado = mes ANTERIOR a fecha_carga; se crean todos los meses cerrados desde
#     INCENTIVO_MES_INICIO que aún no tengan incentivo (robusto a corridas perdidas).
# ──────────────────────────────────────────────────────────────────────────────
INCENTIVO_AMEX_ACTIVO = False        # 🚦 activar (True) para que se generen los incentivos
INCENTIVO_COP_POR_USD = 25           # tarifa: COP de cashback por USD neto gastado en Amex
# Arranca en AGOSTO (no julio): julio 2026 es 100% legacy backoffice ("Compra Amex", Motivo
# vacío) y con la captura por Motivo EXACTO {"Tarjeta Amex","Tarjeta Rakuten"} no se calcularía
# bien -> se decidió NO generar incentivo de julio. Desde agosto los egresos ya traen el Motivo
# correcto (cargue nuevo Amex + Rakuten). El primer incentivo será el de agosto (en la 1ª corrida
# de septiembre).
INCENTIVO_MES_INICIO = "2026-08"     # primer mes cerrado a incentivar (no backfillea antes)
INCENTIVO_MESES_ES = {1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril", 5: "Mayo", 6: "Junio",
                      7: "Julio", 8: "Agosto", 9: "Septiembre", 10: "Octubre",
                      11: "Noviembre", 12: "Diciembre"}


def _incentivo_meses_objetivo(fecha_carga):
    """Lista de (año, mes) cerrados desde INCENTIVO_MES_INICIO hasta el mes ANTERIOR a fecha_carga."""
    fc = pd.to_datetime(fecha_carga, errors="coerce")
    if pd.isna(fc):
        return []
    y, m = int(fc.year), int(fc.month)
    prev_y, prev_m = (y - 1, 12) if m == 1 else (y, m - 1)
    ini_y, ini_m = int(INCENTIVO_MES_INICIO[:4]), int(INCENTIVO_MES_INICIO[5:7])
    meses, yy, mm = [], ini_y, ini_m
    while (yy, mm) <= (prev_y, prev_m):
        meses.append((yy, mm))
        yy, mm = (yy + 1, 1) if mm == 12 else (yy, mm + 1)
    return meses


def agregar_incentivo_amex(combinado, cas, usuario, fecha_carga):
    """Agrega (si no existe) un Ingreso de incentivo Amex por cada mes cerrado. Idempotente.
    No modifica filas existentes; solo agrega. Solo casilleros Amex."""
    if not INCENTIVO_AMEX_ACTIVO or cas not in AMEX_USUARIOS:
        return combinado
    if combinado is None or combinado.empty or "Orden" not in combinado.columns:
        return combinado

    df = combinado
    orden_s = df["Orden"].astype(str)
    nombre_s = df["Nombre del producto"].astype(str) if "Nombre del producto" in df.columns else pd.Series("", index=df.index)
    motivo_s = df["Motivo"].astype(str) if "Motivo" in df.columns else pd.Series("", index=df.index)
    fecha_dt = pd.to_datetime(df["Fecha"], errors="coerce")
    monto = pd.to_numeric(df["Monto"], errors="coerce")
    trm = pd.to_numeric(df["TRM"], errors="coerce") if "TRM" in df.columns else pd.Series(pd.NA, index=df.index)
    tipo_u = df["Tipo"].astype(str).str.strip().str.upper()

    # Captura por Motivo EXACTO (opción b): suma Amex + Rakuten al mismo incentivo de 1444.
    # Más robusto que el texto "amex" suelto (evita falsos positivos si un merchant se llamara
    # "Rakuten X"/"Amex Y"). Nota: las filas backoffice legacy "Compra Amex" (Motivo vacío) NO
    # entran — son pre-INCENTIVO_MES_INICIO, fuera de la ventana del incentivo.
    es_tarjeta = motivo_s.str.strip().isin(["Tarjeta Amex", "Tarjeta Rakuten", "Tarjeta Robinhood",
                                            "Tarjeta Capital", "Tarjeta US Bank"])
    es_incentivo = orden_s.str.startswith("incentivoamex_") | motivo_s.str.strip().eq("Incentivo Amex")
    tarjeta_mask = es_tarjeta & ~es_incentivo & tipo_u.isin(["EGRESO", "INGRESO"])

    ordenes_existentes = set(orden_s)
    nuevas = []
    for (yy, mm) in _incentivo_meses_objetivo(fecha_carga):
        orden_inc = f"incentivoamex_{cas}_{yy:04d}-{mm:02d}"
        if orden_inc in ordenes_existentes:
            continue  # ya existe -> no recrear ni recalcular (congelado)
        mes_mask = tarjeta_mask & (fecha_dt.dt.year == yy) & (fecha_dt.dt.month == mm)
        if not mes_mask.any():
            continue
        trm_mes = trm[mes_mask]
        if (trm_mes.isna() | (trm_mes <= 0)).any():
            st.warning(f"⚠️ Incentivo Amex {cas} {yy}-{mm:02d}: hay filas Amex sin TRM válida; no se crea el incentivo.")
            continue
        usd_fila = monto[mes_mask] / trm_mes
        tu_mes = tipo_u[mes_mask]
        usd_neto = usd_fila[tu_mes == "EGRESO"].sum() - usd_fila[tu_mes == "INGRESO"].sum()
        if usd_neto <= 0:
            continue  # omitir (net <= 0)
        monto_inc = round(INCENTIVO_COP_POR_USD * float(usd_neto))
        etiqueta = f"{INCENTIVO_MESES_ES[mm]} {yy}"
        nuevas.append({
            "Fecha": pd.to_datetime(fecha_carga).strftime("%Y-%m-%d"),
            "Tipo": "Ingreso",
            "Monto": monto_inc,
            "Orden": orden_inc,
            "Motivo": "Incentivo Amex",
            "TRM": "",
            "Usuario": usuario,
            "Casillero": cas,
            "Estado de Orden": "",
            "Nombre del producto": f"Incentivo Amex {etiqueta}",
        })

    if nuevas:
        combinado = pd.concat([combinado, pd.DataFrame(nuevas)], ignore_index=True)
    return combinado


# — Consignaciones (leídas por debajo desde Dropbox; las mantiene la app Dash) —
CONS_NOMBRES = {
    "9444": "Maira Alejandra Paez", "14856": "Jimmy Cortes", "11591": "Paula Herrera",
    "1444": "Maria Moises", "1633": "Nathalia Ospina", "13608": "julian sanchez",
    "9680": "Juan Felipe Laverde", "14825": "Cristian Javier Castro",
    "13297": "Christian Trujillo",
}


@st.cache_data(ttl=120)
def procesar_consignaciones_dropbox() -> dict[str, pd.DataFrame]:
    """Lee consignaciones_<cas>.xlsx de Dropbox (fuente: app Dash) y arma, por casillero,
    las filas a sumar al histórico SOLO de las APROBADAS:
      - consignación aprobada -> Ingreso_extra en su casillero B (Orden = ID, ej. Consignacion4)
      - retiro aprobado       -> ademas Egreso en el casillero que retira A (Orden = ID retiro)
    El dedup por 'Orden' del histórico evita duplicar al correr el generador varias veces.
    NO requiere subir archivo; lee directo de Dropbox. Excluye casilleros de prueba (PRUEBA-*)."""
    base_dir = PurePosixPath(cfg_dbx["remote_path"]).parent
    ing_rows = {c: [] for c in CONS_NOMBRES}   # ingresos por casillero B
    egr_rows = {c: [] for c in CONS_NOMBRES}   # egresos por casillero A (retiros)

    def _num(x):
        try:
            return float(x)
        except Exception:
            return 0.0

    REQ_COLS = ("Estado", "ID", "Monto")
    for cas in CONS_NOMBRES:
        path = str(base_dir / f"consignaciones_{cas}.xlsx")
        try:
            _, res = dbx.files_download(path)
            df = pd.read_excel(io.BytesIO(res.content), sheet_name="Consignaciones", dtype=str)
            if df is None or df.empty or any(c not in df.columns for c in REQ_COLS):
                continue
            df = df.fillna("")
            aprob = df[df["Estado"].astype(str).str.strip().str.lower() == "aprobada"]
            for _, r in aprob.iterrows():
                oid = str(r.get("ID", "")).strip()
                if not oid:
                    continue  # sin Orden no se puede deduplicar -> no inyectar
                fecha = str(r.get("Fecha", "")).strip()
                desc = str(r.get("Descripcion", "")).strip()
                ing_rows[cas].append({
                    "Fecha": fecha, "Tipo": "Ingreso", "Monto": _num(r.get("Monto")),
                    "Orden": oid, "Usuario": CONS_NOMBRES[cas],
                    "Casillero": cas, "Motivo": "Ingreso_extra", "Nombre del producto": desc,
                })
                a = str(r.get("Mayorista retira", "")).strip()
                rid = str(r.get("ID retiro", "")).strip()
                if a in CONS_NOMBRES and rid:  # retiro a casillero real, con id válido
                    egr_rows[a].append({
                        "Fecha": fecha, "Tipo": "Egreso", "Monto": _num(r.get("Egreso retiro")),
                        "Orden": rid, "Usuario": CONS_NOMBRES[a],
                        "Casillero": a, "Motivo": "Retiro",
                        "Nombre del producto": desc or ("Retiro " + rid),
                    })
        except Exception:
            continue

    cols = ["Fecha", "Tipo", "Monto", "Orden", "Usuario", "Casillero", "Motivo", "Nombre del producto"]
    salida = {}
    for cas in CONS_NOMBRES:
        filas = ing_rows[cas] + egr_rows[cas]
        if filas:
            salida[cas] = pd.DataFrame(filas)[cols]
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
# ──────────────────────────────────────────────────────────────────────────────
# COMISIÓN QUINCENAL — QUÉ CASILLEROS LA LLEVAN.
# Regla: 1,5% × |Total diario más negativo de la quincena|, es decir sobre el día en que el
# mayorista MÁS debe. El "Total" es el saldo ACUMULADO, no el neto del día: un saldo negativo
# no se reinicia con la quincena, arrastra, y cada quincena vuelve a cobrar sobre él hasta que
# se amortice.
#   · 'usuario' — el que va en la fila de comisión (debe coincidir con el de la hoja).
#   · 'desde'   — fecha de INICIO de la primera quincena que aplica (YYYY-MM-DD), o None para
#                 "sin restricción" (comportamiento histórico de 1444). Las quincenas que
#                 empiecen ANTES de esa fecha NO se cobran ni se crean.
#
# ⚠️ 9444 se añadió el 2026-08-26 con 'desde' = 2026-08-16: la primera comisión es la de la
# quincena 16-31 agosto, que se cobra en el primer cargue del 1 al 15 de septiembre. El
# 'desde' NO es decorativo: sin él, un cargue de hoy (día ≥ 16) calcularía "1-15 agosto" y le
# cobraría 2.426.065 COP de una quincena que el usuario dejó explícitamente fuera.
# ⚠️ NO mover un 'desde' hacia atrás: alcanzaría quincenas que nunca se cobraron y las cobraría
# de golpe.
COMISION_QUINCENAL_CONF = {
    "1444": {"usuario": "Maria Moises",        "desde": None},
    "9444": {"usuario": "Maira Alejandra Paez", "desde": "2026-08-16"},
}

COBROS_MENSUALES_CONF = {
    # casillero : {"inicio": "YYYY-MM-01", "monto": int}
    "1633": {"inicio": "2024-02-01", "monto": 879_000},
    "13608": {"inicio": "2025-11-01", "monto": 620000},
    "1444": {"inicio": "2026-03-01", "monto": 930_000},
}


# ──────────────────────────────────────────────────────────────────────────────
# TARIFA MÍNIMA DE ENVÍO por casillero (regla de negocio, envíos — NADA que ver con tarjetas).
#
# Un envío por debajo del mínimo se SUBE al mínimo exacto del día. El mínimo se fija en USD y se
# convierte a COP con la TRM del día del envío:
#     minimo_cop = round(USD * _amex_trm_dia(fecha))      # _amex_trm_dia YA incluye el +125
#
#   - SOLO SUBE: un envío igual o por encima del mínimo NO se toca.
#   - Se aplica sobre 'combinado' (histórico + lo nuevo) en main(), así que cubre de una sola vez
#     los envíos YA CARGADOS y los que entren en la corrida. Por eso NO va dentro de
#     procesar_envios_mayoristas, que solo ve el archivo nuevo.
#   - IDEMPOTENTE por construcción: subir al mínimo EXACTO es un punto fijo (en la siguiente
#     corrida Monto == mínimo y la condición '<' ya no se cumple).
#   - FAIL-SOFT (al revés que las tarjetas): si falta la TRM de un día —pasa con envíos fechados
#     hoy/mañana, cuando datos.gov.co aún no publica— se AVISA y la fila queda INTACTA. NO se
#     aborta el cargue: la regla se reevalúa en cada corrida y se auto-corrige cuando la TRM
#     aparezca. Con fail-loud, un envío de hoy bloquearía toda la carga.
#   - La TRM usada se escribe en la columna 'TRM' de las filas ajustadas (hoy vacía en los envíos)
#     como rastro de auditoría. VERIFICADO 2026-08-10 que es invisible para el resto del sistema:
#     Dash.py descarta la columna TRM para toda hoja que no sea 1444 (`load_data`, línea ~75), y
#     en el generador solo la leen las funciones de tarjeta, que filtran por prefijo de 'Orden'
#     (amex_/rakuten_/robinhood_/capital_) o por Motivo exacto "Tarjeta *" — nunca "Envio".
#
# 1633: la tarifa mínima entra en vigor el 2026-08-04 (los 1.998 envíos anteriores NO se tocan).
# ──────────────────────────────────────────────────────────────────────────────
TARIFA_MINIMA_ENVIO_USD = {"1633": 14.4}   # casillero -> USD mínimo por envío
TARIFA_MINIMA_ENVIO_DESDE = "2026-08-04"   # fecha de envío desde la que aplica (>= estricto)


# ──────────────────────────────────────────────────────────────────────────────
# TARIFA DE ENVÍO POR PESO — CA1444 (Maria Moises). Regla de negocio de ENVÍOS; NADA que ver
# con tarjetas (las filas de tarjeta llevan Motivo "Tarjeta *" y prefijo en 'Orden', nunca
# Motivo "Envio").
#
#     cobro_usd = max(peso_lb, min_libras) * usd_por_libra + fijo
#     monto_cop = round(cobro_usd * TRM_de_la_fila)         # la TRM del archivo de envíos
#
# 💱 LA TRM ES LA DEL ARCHIVO DE ENVÍOS, NO la de datos.gov.co (decisión de negocio,
# 2026-08-20). Antes esta regla consultaba _amex_trm_dia (oficial + 125); ahora usa la TRM
# que el portal trae en cada fila, que es la MISMA con la que se cobra a todos los demás
# casilleros (el generador del archivo hace Valor_cop = TRM_portal × VALOR). Así 1444 queda
# consistente con el resto y el cobro coincide con lo que cotizó el portal.
#   · CONSECUENCIA CONOCIDA Y ACEPTADA: el portal puede traer VARIAS TRM el mismo día (la del
#     momento de cada cotización: hasta 3 el 19-ago-2026), así que dos envíos idénticos del
#     mismo día pueden costar distinto. Medido sobre 553 envíos desde jul-2026, la TRM del
#     portal coincide con oficial+125 en el 49% de los casos y se desvía entre -109 y +86 COP.
#   · SIGUE SIENDO IDEMPOTENTE: la TRM viaja en la fila y se persiste en la columna TRM, así
#     que recalcular sobre el resultado da exactamente lo mismo (punto fijo).
#
# DIFERENCIA CLAVE CON LA TARIFA MÍNIMA DE 1633: aquella es un PISO (solo sube); esta REEMPLAZA
# el valor que trae el portal, así que puede subir O BAJAR el Monto. Por eso el resumen de la UI
# informa explícitamente cuántos envíos BAJARON.
#
#   - ALCANCE: TODOS los TIPO ENVIO (Encargomio, Estandar, cualquiera) y todos los perfiles de
#     envío, incluidos los livianos-de-alto-valor. Sin excepciones (decisión de negocio).
#   - Se aplica sobre 'combinado' (histórico + lo nuevo) en main(), ANTES del recálculo de totales
#     y del bloque de comisión quincenal, para que la comisión vea el saldo ya recalculado.
#     Por eso NO va dentro de procesar_envios_mayoristas, que solo ve el archivo nuevo.
#   - EL PESO VIAJA Y SE GUARDA: procesar_envios_mayoristas trae el peso del archivo de envíos y
#     la función lo persiste en la columna COL_PESO_HIST del histórico. Eso hace la regla
#     IDEMPOTENTE (fijar el valor exacto es punto fijo) y AUDITABLE, sin depender de tener el
#     export del portal a mano en cada corrida.
#   - FAIL-SOFT SIN TRM: si la fila no trae TRM (vacía / 0 / negativa / no numérica) se AVISA y
#     la fila queda INTACTA con el valor del portal. NO se aborta el cargue: se reevalúa en cada
#     corrida y se auto-corrige en cuanto el archivo de envíos traiga la TRM.
#   - FAIL-SOFT SIN PESO (propio de esta regla): si el peso viene vacío / 0 / negativo / no
#     numérico se AVISA y la fila queda INTACTA con el valor del portal. NO se aplica el mínimo
#     de 1 libra por defecto: un peso ausente es un ERROR DE DATOS, y cobrar 11 USD por un envío
#     grande sería un error caro y silencioso.
#   - La TRM usada se escribe en la columna 'TRM' de las filas recalculadas, y es lo que hace la
#     regla idempotente además de auditable. Dash.py la MUESTRA en la tabla de egresos como
#     columna "TRM" (leída de 'TRM_envio', el valor crudo capturado en load_data antes del +100
#     que se le suma a la TRM de 1444 para convertir sus ingresos en USD).
#
# 1444: entra en vigor el 2026-08-14. ⚠️ NO mover la fecha hacia atrás: alcanzaría envíos de
# quincenas YA COMISIONADAS y, como las quincenas con inicio >= CUTOFF_COMISION_NUEVA
# (2026-05-16) se REESCRIBEN en cada corrida, la comisión de esos períodos se recalcularía en
# silencio, sin ningún aviso.
# ──────────────────────────────────────────────────────────────────────────────
COL_PESO_HIST = "Peso_lb"   # nombre canónico del peso (LIBRAS) en el histórico y en el pipeline

TARIFA_ENVIO_1444 = {"usd_por_libra": 6.0, "fijo": 5.0, "min_libras": 1.0}
TARIFA_ENVIO_1444_DESDE = "2026-08-14"   # fecha de envío desde la que aplica (>= estricto)


def aplicar_tarifa_envio_por_peso(combinado: pd.DataFrame, cas: str, conf: dict, desde: str):
    """Recalcula por PESO el Monto de los envíos de 'cas' con Fecha >= 'desde'.

    Devuelve (df, resumen). 'resumen' = {"recalculadas": n, "subieron": n, "bajaron": n,
    "cop": delta_total, "sin_trm": [fechas], "sin_peso": [(fecha, orden)], "detalle": [...],
    "evaluadas": n} para mostrarlo en la UI.

    NO toca: filas que no sean envíos, envíos anteriores al corte, filas de otro casillero, ni
    envíos sin peso o sin TRM del día. Ver el bloque de arriba para el porqué de cada decisión.
    """
    resumen = {"recalculadas": 0, "subieron": 0, "bajaron": 0, "cop": 0.0,
               "sin_trm": [], "sin_peso": [], "detalle": [], "evaluadas": 0}
    if combinado is None or combinado.empty:
        return combinado, resumen

    df = combinado.copy()
    for c in ("Motivo", "Fecha", "Monto"):
        if c not in df.columns:
            return combinado, resumen
    if "TRM" not in df.columns:
        df["TRM"] = ""
    if COL_PESO_HIST not in df.columns:
        df[COL_PESO_HIST] = ""

    _fecha = pd.to_datetime(df["Fecha"], errors="coerce")
    _monto = pd.to_numeric(df["Monto"], errors="coerce")
    _peso = pd.to_numeric(df[COL_PESO_HIST], errors="coerce")

    # Casillero: el bucle de main() ya trabaja hoja por hoja, así que esto es una malla de
    # seguridad. Un valor vacío se acepta (la hoja ya es la del casillero); uno que diga
    # explícitamente OTRO casillero se descarta.
    _cas = df["Casillero"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    _cas_ok = _cas.eq(str(cas)) | _cas.str.lower().isin({"", "nan", "none"})

    # 🔒 Filtro ESTRICTO: solo envíos, solo de este casillero, solo con fecha >= corte.
    _sel = (
        df["Motivo"].astype(str).str.strip().str.casefold().eq("envio")
        & _cas_ok
        & _fecha.notna()
        & (_fecha >= pd.Timestamp(desde))
        & _monto.notna()
    )
    if not _sel.any():
        return combinado, resumen
    resumen["evaluadas"] = int(_sel.sum())

    # FAIL-SOFT sin peso: fuera del recálculo, con aviso. Se deja el Monto del portal.
    _sin_peso = _sel & (_peso.isna() | (_peso <= 0))
    if _sin_peso.any():
        resumen["sin_peso"] = [
            (str(_fecha.loc[i].date()), str(df.at[i, "Orden"])) for i in df.index[_sin_peso]
        ]
        _sel = _sel & ~_sin_peso
    if not _sel.any():
        return df, resumen

    usd_lb = float(conf["usd_por_libra"])
    fijo = float(conf["fijo"])
    min_lb = float(conf["min_libras"])

    # 💱 La TRM sale de la PROPIA FILA (la que trae el archivo de envíos), no de datos.gov.co.
    # Las filas que ya están en el histórico conservan la TRM con la que se cobraron, así que
    # recalcular sobre ellas reproduce el mismo Monto: la regla es punto fijo.
    _trm_fila = pd.to_numeric(df["TRM"], errors="coerce")

    # FAIL-SOFT sin TRM: fuera del recálculo, con aviso. Se deja el Monto del portal.
    _sin_trm = _sel & (_trm_fila.isna() | (_trm_fila <= 0))
    if _sin_trm.any():
        resumen["sin_trm"] = [str(df.at[i, "Orden"]) for i in df.index[_sin_trm]]
        _sel = _sel & ~_sin_trm
    if not _sel.any():
        return df, resumen

    for i in df.index[_sel]:
        trm = float(_trm_fila.loc[i])
        peso = float(_peso.loc[i])
        usd = max(peso, min_lb) * usd_lb + fijo
        nuevo = round(usd * trm)
        antes = float(_monto.loc[i])
        df.at[i, "Monto"] = nuevo
        df.at[i, "TRM"] = round(trm, 2)             # la TRM cobrada -> idempotencia + auditoría
        df.at[i, COL_PESO_HIST] = peso              # peso usado -> idempotencia
        if nuevo != antes:
            resumen["recalculadas"] += 1
            resumen["cop"] += nuevo - antes
            if nuevo > antes:
                resumen["subieron"] += 1
            else:
                resumen["bajaron"] += 1
            resumen["detalle"].append(
                (str(_fecha.loc[i].date()), str(df.at[i, "Orden"]), peso, round(usd, 2),
                 antes, nuevo)
            )
    return df, resumen



def aplicar_tarifa_minima_envios(combinado: pd.DataFrame, cas: str, usd: float, desde: str):
    """Sube al mínimo del día los envíos de 'cas' con Fecha >= 'desde' que estén por debajo.

    Devuelve (df, resumen). 'resumen' = {"ajustadas": n, "cop": total_subido, "sin_trm": [fechas],
    "detalle": [(fecha, orden, antes, despues)]} para mostrarlo en la UI.
    NO toca: filas que no sean envíos, envíos anteriores al corte, ni envíos ya en o sobre el
    mínimo. Ver el bloque de arriba para el porqué de cada decisión.
    """
    resumen = {"ajustadas": 0, "cop": 0.0, "sin_trm": [], "detalle": [], "evaluadas": 0}
    if combinado is None or combinado.empty:
        return combinado, resumen

    df = combinado.copy()
    for c in ("Motivo", "Fecha", "Monto"):
        if c not in df.columns:
            return combinado, resumen
    if "TRM" not in df.columns:
        df["TRM"] = ""

    _fecha = pd.to_datetime(df["Fecha"], errors="coerce")
    _monto = pd.to_numeric(df["Monto"], errors="coerce")
    # 🔒 Filtro ESTRICTO: solo envíos y solo con fecha >= corte. Todo lo anterior queda idéntico.
    _sel = (
        df["Motivo"].astype(str).str.strip().str.casefold().eq("envio")
        & _fecha.notna()
        & (_fecha >= pd.Timestamp(desde))
        & _monto.notna()
    )
    if not _sel.any():
        return combinado, resumen
    resumen["evaluadas"] = int(_sel.sum())

    trm_cache: dict = {}
    for f_iso in sorted(_fecha[_sel].dt.strftime("%Y-%m-%d").unique()):
        trm = _amex_trm_dia(f_iso, trm_cache)   # ya incluye el spread de +125
        if trm is None:
            resumen["sin_trm"].append(f_iso)
            continue                            # FAIL-SOFT: esas filas quedan intactas
        minimo = round(float(usd) * float(trm))
        _dia = _sel & _fecha.dt.strftime("%Y-%m-%d").eq(f_iso) & (_monto < minimo)
        for i in df.index[_dia]:
            antes = float(_monto.loc[i])
            df.at[i, "Monto"] = minimo
            df.at[i, "TRM"] = round(float(trm), 2)   # rastro de auditoría
            resumen["ajustadas"] += 1
            resumen["cop"] += minimo - antes
            resumen["detalle"].append((f_iso, str(df.at[i, "Orden"]), antes, minimo))
    return df, resumen

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
        "Fecha de Carga",
        # 📦 Peso en LIBRAS del envío (TARIFA_ENVIO_1444). Lo llenan TODAS las filas de envío
        # cuyo archivo de origen traiga la columna PESO, sin importar el casillero (en 1444 es
        # además la entrada de la tarifa por peso). Se declara aquí para que
        # la columna PERSISTA en el histórico corrida tras corrida (es lo que hace la regla
        # idempotente y auditable). Invisible para Dash.py, que selecciona columnas por nombre.
        COL_PESO_HIST,
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


    # 3.2) Tarjeta Amex (nuevo cargue)
    st.markdown("---")
    st.header("3.2) Tarjeta Amex")

    amex_file = st.file_uploader(
        "Sube el archivo de actividad Amex (hoja: 'Transaction Details')",
        type=["xls", "xlsx"],
        key="amex_uploader"
    )

    amex_may = {}  # dict global para usar después en conciliaciones

    # Estado del corte de fecha (MUY visible)
    if AMEX_FECHA_DESDE:
        st.success(f"✅ Corte Amex ACTIVO: solo transacciones con fecha ≥ {AMEX_FECHA_DESDE}")
    else:
        st.warning("⚠️ Amex INACTIVO — `AMEX_FECHA_DESDE` está en None. No se carga ninguna fila "
                   "(protección anti doble-conteo). Fija la fecha de corte (YYYY-MM-DD) para activar.")

    # 📌 REGLA OPERATIVA (cargue 1-a-1): las compras pueden ASENTAR hasta ~27 días después de
    # la fecha de compra y solo aparecen en exports descargados DESPUÉS de asentar.
    st.info("📌 Descarga SIEMPRE el export de RANGO AMPLIO (desde la fecha de corte hasta HOY), "
            "NUNCA solo 'el último mes/ciclo': hay compras que asientan hasta ~27 días tarde y "
            "solo salen en exports posteriores. Cargar de más NO duplica (el Orden identifica "
            "cada transacción y el dedup la reemplaza); cargar de menos SÍ pierde compras.")

    if amex_file:
        # 🔒 BLOQUEO DURO (1ª llave): sin fecha de corte NO se procesa nada. Se detiene ANTES de
        # leer/procesar el archivo -> imposible escribir una sola fila con AMEX_FECHA_DESDE=None.
        if AMEX_FECHA_DESDE is None:
            st.error("🔒 Cargue Amex BLOQUEADO: no hay fecha de corte definida "
                     "(AMEX_FECHA_DESDE=None). Define la fecha de corte antes de cargar para "
                     "evitar doble conteo con los egresos Amex del backoffice.")
            st.stop()

        # 🛡️ LISTA DE EXCLUSIÓN obligatoria (2ª llave, anti-doble-cobro): sin lista NO se
        # procesa nada — procesar a ciegas recobraría todo lo ya cobrado.
        try:
            tarjetas_cobradas, tarjetas_pendientes, tarjetas_cobradas_df = cargar_tarjetas_cobradas()
            st.caption(f"🛡️ Lista de exclusión cargada: {len(tarjetas_cobradas)} Orden ya "
                       f"cobrados + {len(tarjetas_pendientes)} pendientes de rematch.")
            _aviso_barrera_atributos(tarjetas_cobradas_df)
        except Exception as e:
            st.error(f"🔒 Cargue Amex BLOQUEADO: no se pudo leer '{TARJETAS_COBRADAS_FILENAME}' "
                     f"desde Dropbox ({e}). Sin la lista de exclusión se recobrarían "
                     f"transacciones ya cobradas. NO se procesa nada.")
            st.stop()

        try:
            # Header en la fila 7 del export Amex (índice 6)
            df_amex = pd.read_excel(amex_file, sheet_name="Transaction Details", header=6)
        except Exception as e:
            st.error(f"❌ No se pudo leer la hoja 'Transaction Details': {e}")
            df_amex = None

        if df_amex is not None:
            # 2ª llave: procesar_amex igual devuelve {} si fecha_desde es None (por si se llama aparte).
            try:
                amex_may = procesar_amex(df_amex, fecha_desde=AMEX_FECHA_DESDE,
                                         cobrados=tarjetas_cobradas,
                                         pendientes=tarjetas_pendientes,
                                         hist_tarjetas=_hist_tarjetas_para_trm(),
                                         cobrados_df=tarjetas_cobradas_df)
            except ValueError as e:
                st.error(f"⛔ {e}")
                st.stop()  # DETENER: falta TRM o columnas (sin default, como se acordó)
            if not amex_may:
                st.info("No hay transacciones Amex (de los 3 Card Members) desde la fecha de corte.")
            else:
                tabs_amex = st.tabs(list(amex_may.keys()))
                for tab, key in zip(tabs_amex, amex_may):
                    with tab:
                        st.dataframe(amex_may[key], use_container_width=True)
    else:
        st.info("📂 Aún no subes el archivo de Tarjeta Amex")


    # 3.3) Tarjeta Rakuten (cargue propio, SOLO Maria Moises / 1444)
    st.markdown("---")
    st.header("3.3) Tarjeta Rakuten")

    rakuten_file = st.file_uploader(
        "Sube el CSV de actividad Rakuten (columnas: Date, Amount, Type, Merchant, Category, Method)",
        type=["csv"],
        key="rakuten_uploader"
    )

    rakuten_may = {}  # dict global para usar después en conciliaciones (solo 1444)

    # Estado del corte de fecha (MUY visible), igual que Amex 3.2
    if RAKUTEN_FECHA_DESDE:
        st.success(f"✅ Corte Rakuten ACTIVO: solo transacciones con fecha ≥ {RAKUTEN_FECHA_DESDE}")
    else:
        st.warning("⚠️ Rakuten INACTIVO — `RAKUTEN_FECHA_DESDE` está en None. No se carga ninguna fila "
                   "(protección anti doble-conteo). Fija la fecha de corte (YYYY-MM-DD) para activar. "
                   "NO activar hasta verificar que el timestamp del CSV es estable entre 2 descargas.")

    # 📌 REGLA OPERATIVA (cargue 1-a-1): igual que Amex — rango amplio SIEMPRE.
    st.info("📌 Descarga SIEMPRE el export de historial completo ('Rakuten_Activity_All'), NUNCA "
            "solo 'el último mes': hay compras que asientan tarde y solo salen en exports "
            "posteriores. Cargar de más NO duplica (el Orden identifica cada transacción y el "
            "dedup la reemplaza); cargar de menos SÍ pierde compras.")

    if rakuten_file:
        # 🔒 BLOQUEO DURO: sin fecha de corte NO se procesa nada (se detiene ANTES de procesar).
        if RAKUTEN_FECHA_DESDE is None:
            st.error("🔒 Cargue Rakuten BLOQUEADO: no hay fecha de corte definida "
                     "(RAKUTEN_FECHA_DESDE=None). Define la fecha de corte antes de cargar.")
            st.stop()

        # 🛡️ LISTA DE EXCLUSIÓN obligatoria (anti-doble-cobro), igual que Amex.
        try:
            tarjetas_cobradas_rk, tarjetas_pendientes_rk, tarjetas_cobradas_rk_df = cargar_tarjetas_cobradas()
            st.caption(f"🛡️ Lista de exclusión cargada: {len(tarjetas_cobradas_rk)} Orden ya "
                       f"cobrados + {len(tarjetas_pendientes_rk)} pendientes de rematch.")
            _aviso_barrera_atributos(tarjetas_cobradas_rk_df)
            st.caption("⏳ Pendiente: verificar estabilidad del timestamp con una 2ª descarga "
                       "del CSV (si cambiara, el hash cambia y una cobrada podría re-entrar).")
        except Exception as e:
            st.error(f"🔒 Cargue Rakuten BLOQUEADO: no se pudo leer '{TARJETAS_COBRADAS_FILENAME}' "
                     f"desde Dropbox ({e}). Sin la lista de exclusión se recobrarían "
                     f"transacciones ya cobradas. NO se procesa nada.")
            st.stop()

        try:
            df_rakuten = pd.read_csv(rakuten_file)
        except Exception as e:
            st.error(f"❌ No se pudo leer el CSV Rakuten: {e}")
            df_rakuten = None

        if df_rakuten is not None:
            try:
                rakuten_may = procesar_rakuten(df_rakuten, fecha_desde=RAKUTEN_FECHA_DESDE,
                                               cobrados=tarjetas_cobradas_rk,
                                               pendientes=tarjetas_pendientes_rk,
                                               hist_tarjetas=_hist_tarjetas_para_trm(),
                                               cobrados_df=tarjetas_cobradas_rk_df)
            except ValueError as e:
                st.error(f"⛔ {e}")
                st.stop()  # DETENER: falta TRM o columnas (sin default, como Amex)
            if not rakuten_may:
                st.info("No hay transacciones Rakuten (TRANSACTION/REFUND) desde la fecha de corte.")
            else:
                for key, dfr in rakuten_may.items():
                    st.dataframe(dfr, use_container_width=True)
    else:
        st.info("📂 Aún no subes el CSV de Tarjeta Rakuten")


    # 3.4) Tarjeta Robinhood (cargue propio, SOLO Correal + Maria Moises / 1444)
    st.markdown("---")
    st.header("3.4) Tarjeta Robinhood")

    robinhood_file = st.file_uploader(
        "Sube el CSV de actividad Robinhood (columnas: Date, Time, Cardholder, Amount, Points, "
        "Balance, Status, Type, Merchant, Description)",
        type=["csv"],
        key="robinhood_uploader"
    )

    robinhood_may = {}  # dict global para usar después en conciliaciones (solo 1444)

    # Estado del corte de fecha (MUY visible), igual que Amex/Rakuten
    if ROBINHOOD_FECHA_DESDE:
        st.success(f"✅ Corte Robinhood ACTIVO: solo transacciones con fecha ≥ {ROBINHOOD_FECHA_DESDE}")
    else:
        st.warning("⚠️ Robinhood INACTIVO — `ROBINHOOD_FECHA_DESDE` está en None. No se carga "
                   "ninguna fila (protección anti doble-conteo). Fija la fecha de corte (YYYY-MM-DD).")

    # 📌 REGLA OPERATIVA (cargue 1-a-1): igual que Amex/Rakuten — rango amplio SIEMPRE.
    st.info("📌 Descarga SIEMPRE el export de historial completo de Robinhood, NUNCA solo 'el "
            "último mes': hay compras que asientan tarde y solo salen en exports posteriores. "
            "Cargar de más NO duplica (el Orden identifica cada transacción y el dedup la "
            "reemplaza); cargar de menos SÍ pierde compras.")

    if robinhood_file:
        # 🔒 BLOQUEO DURO: sin fecha de corte NO se procesa nada (se detiene ANTES de procesar).
        if ROBINHOOD_FECHA_DESDE is None:
            st.error("🔒 Cargue Robinhood BLOQUEADO: no hay fecha de corte definida "
                     "(ROBINHOOD_FECHA_DESDE=None). Define la fecha de corte antes de cargar.")
            st.stop()

        # 🛡️ LISTA DE EXCLUSIÓN obligatoria (anti-doble-cobro), igual que Amex/Rakuten.
        try:
            tarjetas_cobradas_rb, tarjetas_pendientes_rb, tarjetas_cobradas_rb_df = cargar_tarjetas_cobradas()
            st.caption(f"🛡️ Lista de exclusión cargada: {len(tarjetas_cobradas_rb)} Orden ya "
                       f"cobrados + {len(tarjetas_pendientes_rb)} pendientes de rematch.")
            _aviso_barrera_atributos(tarjetas_cobradas_rb_df)
        except Exception as e:
            st.error(f"🔒 Cargue Robinhood BLOQUEADO: no se pudo leer '{TARJETAS_COBRADAS_FILENAME}' "
                     f"desde Dropbox ({e}). Sin la lista de exclusión se recobrarían "
                     f"transacciones ya cobradas. NO se procesa nada.")
            st.stop()

        try:
            df_robinhood = pd.read_csv(robinhood_file)
        except Exception as e:
            st.error(f"❌ No se pudo leer el CSV Robinhood: {e}")
            df_robinhood = None

        if df_robinhood is not None:
            try:
                robinhood_may = procesar_robinhood(df_robinhood, fecha_desde=ROBINHOOD_FECHA_DESDE,
                                                   cobrados=tarjetas_cobradas_rb,
                                                   pendientes=tarjetas_pendientes_rb,
                                                   hist_tarjetas=_hist_tarjetas_para_trm(),
                                                   cobrados_df=tarjetas_cobradas_rb_df)
            except ValueError as e:
                st.error(f"⛔ {e}")
                st.stop()  # DETENER: falta TRM o columnas (sin default, como Amex/Rakuten)
            if not robinhood_may:
                st.info("No hay transacciones Robinhood (Posted Purchase/Refund) desde la fecha de corte.")
            else:
                for key, dfr in robinhood_may.items():
                    st.dataframe(dfr, use_container_width=True)
    else:
        st.info("📂 Aún no subes el CSV de Tarjeta Robinhood")


    st.markdown("---")
    st.header("3.5) Tarjeta Capital")

    capital_file = st.file_uploader(
        "Sube el CSV de Capital One (columnas: Transaction Date, Posted Date, Card No., "
        "Description, Category, Debit, Credit)",
        type=["csv"],
        key="capital_uploader"
    )

    capital_may = {}  # dict global para usar después en conciliaciones (solo 13608)

    # Estado del corte de fecha (MUY visible), igual que Amex/Rakuten/Robinhood
    if CAPITAL_FECHA_DESDE:
        st.success(f"✅ Corte Capital ACTIVO: solo transacciones con fecha ≥ {CAPITAL_FECHA_DESDE}")
    else:
        st.warning("⚠️ Capital INACTIVO — `CAPITAL_FECHA_DESDE` está en None. No se carga ninguna "
                   "fila (protección anti doble-conteo). Fija la fecha de corte (YYYY-MM-DD).")

    # 📌 REGLA OPERATIVA (cargue 1-a-1): igual que Amex/Rakuten/Robinhood — rango amplio SIEMPRE.
    st.info("📌 Descarga SIEMPRE el rango COMPLETO disponible en Capital One, NUNCA solo 'el "
            "último mes': hay compras que asientan tarde y solo salen en exports posteriores. "
            "Cargar de más NO duplica (el Orden identifica cada transacción y el dedup la "
            "reemplaza); cargar de menos SÍ pierde compras.")
    st.caption("💵 **Debit** → Egreso (gasto) · **Credit `Merchandise`** → Ingreso (devolución, "
               "con la TRM de su compra original) · **`ELECTRONIC PAYMENT`** → se ignora (pago a "
               "la tarjeta, ni suma ni resta).")

    if capital_file:
        # 🔒 BLOQUEO DURO: sin fecha de corte NO se procesa nada (se detiene ANTES de procesar).
        if CAPITAL_FECHA_DESDE is None:
            st.error("🔒 Cargue Capital BLOQUEADO: no hay fecha de corte definida "
                     "(CAPITAL_FECHA_DESDE=None). Define la fecha de corte antes de cargar.")
            st.stop()

        # 🛡️ LISTA DE EXCLUSIÓN obligatoria (anti-doble-cobro), igual que las otras 3.
        try:
            tarjetas_cobradas_cp, tarjetas_pendientes_cp, tarjetas_cobradas_cp_df = cargar_tarjetas_cobradas()
            st.caption(f"🛡️ Lista de exclusión cargada: {len(tarjetas_cobradas_cp)} Orden ya "
                       f"cobrados + {len(tarjetas_pendientes_cp)} pendientes de rematch.")
            _aviso_barrera_atributos(tarjetas_cobradas_cp_df)
        except Exception as e:
            st.error(f"🔒 Cargue Capital BLOQUEADO: no se pudo leer '{TARJETAS_COBRADAS_FILENAME}' "
                     f"desde Dropbox ({e}). Sin la lista de exclusión se recobrarían "
                     f"transacciones ya cobradas. NO se procesa nada.")
            st.stop()

        try:
            df_capital = pd.read_csv(capital_file)
        except Exception as e:
            st.error(f"❌ No se pudo leer el CSV Capital One: {e}")
            df_capital = None

        if df_capital is not None:
            try:
                capital_may = procesar_capital(df_capital, fecha_desde=CAPITAL_FECHA_DESDE,
                                               cobrados=tarjetas_cobradas_cp,
                                               pendientes=tarjetas_pendientes_cp,
                                               hist_tarjetas=_hist_tarjetas_para_trm(),
                                               cobrados_df=tarjetas_cobradas_cp_df)
            except ValueError as e:
                st.error(f"⛔ {e}")
                st.stop()  # DETENER: falta TRM, columnas o Category nueva (como las otras 3)
            if not capital_may:
                st.info("No hay compras Capital (Debit) desde la fecha de corte.")
            else:
                for key, dfr in capital_may.items():
                    st.dataframe(dfr, use_container_width=True)
    else:
        st.info("📂 Aún no subes el CSV de Tarjeta Capital")


    st.markdown("---")
    st.header("3.6) Tarjeta US Bank")

    usbank_file = st.file_uploader(
        "Sube el CSV de US Bank (columnas: Date, Transaction, Name, Memo, Amount)",
        type=["csv"],
        key="usbank_uploader"
    )

    usbank_may = {}  # dict global para usar después en conciliaciones (MULTI-casillero)

    if USBANK_FECHA_DESDE:
        st.success(f"✅ Corte US Bank ACTIVO: solo transacciones con fecha ≥ {USBANK_FECHA_DESDE}")
    else:
        st.warning("⚠️ US Bank INACTIVO — `USBANK_FECHA_DESDE` está en None. No se carga ninguna "
                   "fila (protección anti doble-conteo). Fija la fecha de corte (YYYY-MM-DD).")

    st.info("📌 Descarga SIEMPRE el rango COMPLETO disponible en US Bank, NUNCA solo 'el último "
            "mes': hay compras que asientan tarde y solo salen en exports posteriores. Cargar de "
            "más NO duplica (el Orden identifica cada transacción y el dedup la reemplaza); "
            "cargar de menos SÍ pierde compras.")
    st.caption("💳 Es la única tarjeta MULTI-CASILLERO: se reparte por **sub-tarjeta** "
               "(0598 → 11591 · 0609 → 13608 · **2529 Kelly y 0534 Santiago se IGNORAN**). "
               "**DEBIT** → Egreso · **CREDIT** → Ingreso (devolución) · pagos a la tarjeta, "
               "cuota de manejo y filas sin titular se ignoran.")
    if USBANK_EXCLUIR_BENEFICIO_AMAZON:
        st.caption(f"🎁 «{USBANK_ETIQUETA_BENEFICIO}» se DESCARTA: es el beneficio comercial de "
                   f"Amazon a la cuenta, no una devolución del mayorista.")

    if usbank_file:
        if USBANK_FECHA_DESDE is None:
            st.error("🔒 Cargue US Bank BLOQUEADO: no hay fecha de corte definida "
                     "(USBANK_FECHA_DESDE=None). Define la fecha de corte antes de cargar.")
            st.stop()

        try:
            tarjetas_cobradas_ub, tarjetas_pendientes_ub, tarjetas_cobradas_ub_df = cargar_tarjetas_cobradas()
            st.caption(f"🛡️ Lista de exclusión cargada: {len(tarjetas_cobradas_ub)} Orden ya "
                       f"cobrados + {len(tarjetas_pendientes_ub)} pendientes de rematch.")
            _aviso_barrera_atributos(tarjetas_cobradas_ub_df)
        except Exception as e:
            st.error(f"🔒 Cargue US Bank BLOQUEADO: no se pudo leer '{TARJETAS_COBRADAS_FILENAME}' "
                     f"desde Dropbox ({e}). Sin la lista de exclusión se recobrarían "
                     f"transacciones ya cobradas. NO se procesa nada.")
            st.stop()

        try:
            df_usbank = pd.read_csv(usbank_file)
        except Exception as e:
            st.error(f"❌ No se pudo leer el CSV de US Bank: {e}")
            df_usbank = None

        if df_usbank is not None:
            try:
                usbank_may = procesar_usbank(df_usbank, fecha_desde=USBANK_FECHA_DESDE,
                                             cobrados=tarjetas_cobradas_ub,
                                             pendientes=tarjetas_pendientes_ub,
                                             hist_tarjetas=_hist_tarjetas_para_trm(),
                                             cobrados_df=tarjetas_cobradas_ub_df)
            except ValueError as e:
                st.error(f"⛔ {e}")
                st.stop()  # DETENER: falta TRM, columnas o sub-tarjeta nueva (como las otras 4)
            if not usbank_may:
                st.info("No hay movimientos US Bank cargables desde la fecha de corte.")
            else:
                for key, dfr in usbank_may.items():
                    st.markdown(f"**{key}** — {len(dfr)} movimiento(s)")
                    st.dataframe(dfr, use_container_width=True)
    else:
        st.info("📂 Aún no subes el CSV de Tarjeta US Bank")


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


    # 7.1) Ingresos Christian Trujillo (CA13297)
    st.header("7.1) Ingresos Christian Trujillo (CA13297)")
    truj_files = st.file_uploader(
        "Sube archivos .xls y .csv de Christian Trujillo",
        type=["xls", "xlsx", "csv"],
        accept_multiple_files=True,
        key="truj_files_13297"
    )

    confirm_truj = st.radio(
        "¿Estás seguro de que los archivos de Christian Trujillo son los correctos?",
        ["No, quiero revisar", "Sí, procesar"],
        index=0,
        horizontal=True,
        key="conf_truj"
    )

    ingresos_truj = {}

    if truj_files and confirm_truj == "Sí, procesar":
        xls_files = [f for f in truj_files if f.name.lower().endswith((".xls", ".xlsx"))]
        csv_files = [f for f in truj_files if f.name.lower().endswith(".csv")]

        dfs = []
        if xls_files:
            df_xls = procesar_ingresos_clientes_xls(xls_files, "Christian Trujillo", "13297")
            dfs.append(df_xls)
        if csv_files:
            df_csv = procesar_ingresos_clientes_csv(csv_files, "Christian Trujillo", "13297")
            dfs.append(df_csv)

        df_truj = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        ingresos_truj["ingresos_13297"] = df_truj

        if df_truj.empty:
            st.info("Sin movimientos válidos")
        else:
            st.dataframe(df_truj, use_container_width=True)

    elif truj_files and confirm_truj == "No, quiero revisar":
        st.warning("👀 Aún no se procesan los archivos de Christian Trujillo. Revisa y luego marca 'Sí, procesar'.")
    else:
        st.info("📂 No subes archivos de Christian Trujillo")

    st.markdown("---")





    # 5) Conciliaciones
    # 5) Conciliaciones Finales
    st.header("8) Conciliaciones Finales")

    # asegúrate de que la lista incluya el nuevo casillero
    casilleros = ["9444", "14856", "11591", "1444", "1633", "13608", "9680", "14825", "13297"]

    conciliaciones = {}

    # Consignaciones/retiros aprobados (leídos por debajo de Dropbox; fuente: app Dash)
    consignaciones_hist = procesar_consignaciones_dropbox()

    for cas in casilleros:
        key_ing = f"ingresos_{cas}"
    
        # tomar de cada fuente (si existe el dict y la clave)
        ing_j = ingresos_jul.get(key_ing)       if isinstance(ingresos_jul, dict)       else None
        ing_n = ingresos_nath.get(key_ing)      if isinstance(ingresos_nath, dict)      else None
        ing_e = ingresos_elv.get(key_ing)       if isinstance(ingresos_elv, dict)       else None
        ing_m = ingresos_moises.get(key_ing)    if isinstance(ingresos_moises, dict)    else None
        ing_9 = ingresos_9680.get(key_ing)      if isinstance(ingresos_9680, dict)      else None  # NUEVO
        ing_c = ingresos_cris.get(key_ing) if isinstance(ingresos_cris, dict) else None
        ing_t = ingresos_truj.get(key_ing) if isinstance(ingresos_truj, dict) else None  # CA13297


    
        if ing_j is not None and not ing_j.empty:
            inc = ing_j
        elif ing_n is not None and not ing_n.empty:
            inc = ing_n
        elif ing_c is not None and not ing_c.empty:
            inc = ing_c
        elif ing_t is not None and not ing_t.empty:
            inc = ing_t
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

        # >>> NUEVO: CONSIGNACIONES/RETIROS aprobados (Ingreso_extra a B / Egreso a A) <<<
        cons = consignaciones_hist.get(cas) if 'consignaciones_hist' in locals() else None

        # >>> NUEVO: TARJETA AMEX por casillero (gasto/reembolso acumulado por día) <<<
        amex = amex_may.get(f"amex_{cas}") if 'amex_may' in locals() else None

        # >>> NUEVO: TARJETA RAKUTEN — módulo paralelo, SOLO 1444 (get devuelve None para el resto) <<<
        rakuten = rakuten_may.get(f"rakuten_{cas}") if 'rakuten_may' in locals() else None

        # >>> NUEVO: TARJETA ROBINHOOD — módulo paralelo, SOLO 1444 (get devuelve None para el resto) <<<
        robinhood = robinhood_may.get(f"robinhood_{cas}") if 'robinhood_may' in locals() else None

        # >>> NUEVO: TARJETA CAPITAL — módulo paralelo, SOLO 13608 (get devuelve None para el resto) <<<
        capital = capital_may.get(f"capital_{cas}") if 'capital_may' in locals() else None

        # >>> NUEVO: TARJETA US BANK — MULTI-casillero (1444 / 11591 / 13608); el get devuelve
        # None para los casilleros sin movimientos en el extracto. <<<
        usbank = usbank_may.get(f"usbank_{cas}") if 'usbank_may' in locals() else None

        # 3) Armar la lista de DataFrames válidos
        frames = []
        for df in (inc, egr, ext, env, cons, amex, rakuten, robinhood, capital, usbank):  # rakuten/robinhood 1444, capital 13608, usbank 1444/11591/13608
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
                hoja = f"{cas} - {CONS_NOMBRES.get(cas, 'sin_nombre')}"
                
            # 2) Dedups y limpiezas
            combinado["Orden"] = (
                combinado["Orden"]
                .astype(str)
                .str.strip()
                .str.replace(".0", "", regex=False)
            )

            # 🚫 Purga de envíos bloqueados (doble cobro): eliminarlos del histórico.
            # Corre ANTES del dedup y del recálculo de totales para que el saldo
            # se recompute sin estos cargos duplicados. Solo afecta CA1444.
            _mask_bloq = _es_envio_bloqueado(combinado["Orden"])
            if _mask_bloq.any():
                combinado = combinado[~_mask_bloq].reset_index(drop=True)

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

            # ---- Tarifa MÍNIMA de envío (parametrizada por casillero) ----
            # Va sobre 'combinado' (histórico + lo nuevo) y ANTES del recálculo de totales, para
            # que el saldo del día ya incorpore los envíos subidos al mínimo. Cubre de una sola
            # vez los envíos ya cargados y los nuevos. Ver el bloque de TARIFA_MINIMA_ENVIO_USD.
            if cas in TARIFA_MINIMA_ENVIO_USD:
                combinado, _tm = aplicar_tarifa_minima_envios(
                    combinado, cas,
                    usd=TARIFA_MINIMA_ENVIO_USD[cas],
                    desde=TARIFA_MINIMA_ENVIO_DESDE,
                )
                if _tm["ajustadas"]:
                    st.info(
                        f"📦 Tarifa mínima {cas}: {_tm['ajustadas']} envío(s) subidos al mínimo de "
                        f"USD {TARIFA_MINIMA_ENVIO_USD[cas]} (desde {TARIFA_MINIMA_ENVIO_DESDE}) — "
                        f"+COP {_tm['cop']:,.0f} en total."
                    )
                if _tm["sin_trm"]:
                    st.warning(
                        f"⚠️ Tarifa mínima {cas}: sin TRM para {', '.join(_tm['sin_trm'])} — "
                        f"esos envíos quedaron SIN ajustar (se corrigen solos en la próxima "
                        f"corrida, cuando datos.gov.co publique la TRM)."
                    )
            # -------------------------------------------------------------------------

            # ---- Tarifa de envío POR PESO (CA1444) ----
            # Mismo punto del pipeline que la tarifa mínima de 1633: sobre 'combinado'
            # (histórico + lo nuevo) y ANTES del incentivo, del recálculo de totales y del
            # bloque de comisión quincenal, para que la comisión vea el saldo ya recalculado.
            # A diferencia de la tarifa mínima, esta REEMPLAZA el valor del portal: puede bajarlo.
            # Ver el bloque de TARIFA_ENVIO_1444.
            if cas == "1444":
                combinado, _tp = aplicar_tarifa_envio_por_peso(
                    combinado, cas,
                    conf=TARIFA_ENVIO_1444,
                    desde=TARIFA_ENVIO_1444_DESDE,
                )
                if _tp["recalculadas"]:
                    st.info(
                        f"📦 Tarifa por peso {cas}: {_tp['recalculadas']} envío(s) recalculados a "
                        f"(max(lb, {TARIFA_ENVIO_1444['min_libras']:g}) × USD "
                        f"{TARIFA_ENVIO_1444['usd_por_libra']:g} + USD {TARIFA_ENVIO_1444['fijo']:g}) "
                        f"desde {TARIFA_ENVIO_1444_DESDE} — {_tp['subieron']} subieron, "
                        f"{_tp['bajaron']} BAJARON · neto COP {_tp['cop']:+,.0f}."
                    )
                if _tp["sin_peso"]:
                    st.warning(
                        f"⚠️ Tarifa por peso {cas}: {len(_tp['sin_peso'])} envío(s) SIN PESO "
                        f"({', '.join(o for _f, o in _tp['sin_peso'][:10])}"
                        f"{' …' if len(_tp['sin_peso']) > 10 else ''}) — quedaron con el valor del "
                        f"portal SIN recalcular. Revisa la columna de peso del archivo de envíos."
                    )
                if _tp["sin_trm"]:
                    st.warning(
                        f"⚠️ Tarifa por peso {cas}: {len(_tp['sin_trm'])} envío(s) SIN TRM "
                        f"({', '.join(_tp['sin_trm'][:10])}"
                        f"{' …' if len(_tp['sin_trm']) > 10 else ''}) — quedaron con el valor del "
                        f"portal SIN recalcular. Revisa la columna TRM del archivo de envíos."
                    )
            # -------------------------------------------------------------------------

            # ── [INCENTIVO AMEX] Cashback mensual (25 COP x USD neto Amex del mes cerrado).
            # Va ANTES del recálculo+comisión para que (por decisión de negocio) el incentivo SÍ
            # afecte la comisión quincenal de 1444. Idempotente: no recrea un mes ya existente.
            combinado = agregar_incentivo_amex(combinado, cas, usuario, fecha_carga)
            # ── /[INCENTIVO AMEX] ──

            # ── [AMEX/COMISIÓN 1444] Aislar filas de tarjeta del cálculo de comisión (flag False) ──
            # Con AMEX_AFECTA_COMISION_1444=False se retiran las filas de tarjeta de 1444 (Orden
            # legacy gastoamex_1444_/reembolsoamex_1444_ y 1-a-1 amex_<Reference>/rakuten_<hash>/
            # robinhood_<hash>) ANTES del recálculo+comisión y se reincorporan DESPUÉS. Así la
            # comisión quincenal NO ve el gasto de tarjeta (base intacta) pero el saldo final SÍ
            # lo incluye. NO se toca el código de comisión; solo se envuelve.
            _amex_stash_1444 = None
            if cas == "1444" and not AMEX_AFECTA_COMISION_1444:
                _m_amex = combinado["Orden"].astype(str).str.match(
                    r"^(?:gastoamex|reembolsoamex)_1444_|^(?:amex|rakuten|robinhood|capital)_", na=False
                )
                if _m_amex.any():
                    _amex_stash_1444 = combinado[_m_amex].copy()
                    combinado = combinado[~_m_amex].copy()
            # ── /[AMEX/COMISIÓN 1444] ──

            # ---------- RECÁLCULO FINAL DE TOTALES ----------
            combinado = recalcular_totales_diarios(
                combinado,
                usuario=usuario,
                cas=cas
            )
            # ---------- /RECÁLCULO FINAL DE TOTALES ----------

            # ---------- COMISIÓN QUINCENAL POR TOTALES (SOLO CA1444) ----------
            # Corre DESPUÉS del recálculo para usar el saldo final (incluye movimientos tardíos
            # subidos en esta misma corrida). Para períodos con inicio >= 2026-04-01, si la fila
            # ya existe se reescribe el Monto con el valor recalculado; para períodos anteriores
            # se mantiene el comportamiento viejo (skip si existe).
            if cas in COMISION_QUINCENAL_CONF:
                import calendar
                from datetime import date as _date

                _com_conf = COMISION_QUINCENAL_CONF[cas]
                _com_usuario = _com_conf["usuario"]
                _com_desde = (_date.fromisoformat(_com_conf["desde"])
                              if _com_conf.get("desde") else None)

                dfh = combinado.copy()
                dfh["Fecha_dt"] = pd.to_datetime(dfh["Fecha"], errors="coerce").dt.date
                dfh["Monto"] = pd.to_numeric(dfh["Monto"], errors="coerce")

                fc_date = pd.to_datetime(fecha_carga, errors="coerce").date()
                y, m, d = fc_date.year, fc_date.month, fc_date.day

                meses = {
                    1:"enero",2:"febrero",3:"marzo",4:"abril",5:"mayo",6:"junio",
                    7:"julio",8:"agosto",9:"septiembre",10:"octubre",11:"noviembre",12:"diciembre"
                }

                # Las comisiones quincenales con inicio ANTERIOR a esta fecha quedan congeladas
                # (se conservan tal cual están en el histórico). Desde la 2ª quincena de mayo
                # (16-31 may, la cobrada el 1 de junio) en adelante se recalculan con la nueva
                # fecha base (Fecha Creación Orden en hora Colombia).
                CUTOFF_COMISION_NUEVA = _date(2026, 5, 16)

                def agregar_comision_rango(dfh_local, ini_date, fin_date, etiqueta):
                    orden_nombre = f"Comision de ({etiqueta})"

                    # 🚦 Quincenas anteriores al 'desde' del casillero: NO se cobran ni se crean.
                    # Se sale ANTES de mirar nada, para no tocar una fila preexistente por error.
                    if _com_desde is not None and ini_date < _com_desde:
                        return dfh_local

                    es_nueva_logica = ini_date >= CUTOFF_COMISION_NUEVA

                    mask_existente = pd.Series(False, index=dfh_local.index)
                    if "Orden" in dfh_local.columns:
                        mask_existente = mask_existente | dfh_local["Orden"].astype(str).str.lower().eq(orden_nombre.lower())
                    if "Nombre del producto" in dfh_local.columns:
                        mask_existente = mask_existente | dfh_local["Nombre del producto"].astype(str).str.lower().eq(orden_nombre.lower())

                    existe = bool(mask_existente.any())

                    if existe and not es_nueva_logica:
                        return dfh_local

                    mask_tot = (
                        dfh_local["Tipo"].astype(str).str.upper().eq("TOTAL")
                        & (dfh_local["Fecha_dt"] >= ini_date)
                        & (dfh_local["Fecha_dt"] <= fin_date)
                    )

                    serie = pd.to_numeric(dfh_local.loc[mask_tot, "Monto"], errors="coerce")
                    serie = serie[serie < 0]

                    if serie.empty:
                        if existe and es_nueva_logica:
                            return dfh_local.loc[~mask_existente].copy()
                        return dfh_local

                    comision = float(abs(serie.min()) * 0.015)

                    if existe and es_nueva_logica:
                        dfh_local.loc[mask_existente, "Monto"] = comision
                        dfh_local.loc[mask_existente, "Fecha de Carga"] = fecha_carga
                        return dfh_local

                    nueva = pd.DataFrame([{
                        "Fecha": fc_date,
                        "Tipo": "Egreso",
                        "Orden": orden_nombre,
                        "Monto": comision,
                        "Motivo": "comision",
                        "TRM": "",
                        "Usuario": _com_usuario,
                        "Casillero": cas,
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

                # Recalcular TOTALES otra vez para que el saldo del día de carga incorpore la
                # fila de comisión recién agregada o actualizada.
                combinado = recalcular_totales_diarios(
                    combinado,
                    usuario=usuario,
                    cas=cas
                )
            # ---------- /COMISIÓN QUINCENAL ----------

            # ── [AMEX/COMISIÓN 1444] Reincorporar filas Amex y recalcular saldo final ──
            # (la comisión ya se calculó SIN ellas; ahora el saldo SÍ las incluye)
            if _amex_stash_1444 is not None:
                combinado = pd.concat([combinado, _amex_stash_1444], ignore_index=True)
                combinado = recalcular_totales_diarios(combinado, usuario=usuario, cas=cas)
            # ── /[AMEX/COMISIÓN 1444] ──

            historico[hoja] = combinado.copy()
                        
            
            


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
        
                                
                            
                
                
        
        
        
        
        # 🛡️ CAPA B — recuperar del histórico VIVO las filas de tarjeta que esta corrida no
        # trae (toda corrida sin extractos de tarjeta). Va justo antes de serializar.
        historico = preservar_filas_tarjeta(historico)

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
    
        # 🛡️ CAPA A — check de concurrencia contra el estado vivo de Dropbox. Si la escritura
        # perdería Orden que hoy existen allá, detiene la app ANTES de tocar producción.
        guard_frescura_historico(historico)

        # 2) Subida automática a Dropbox (con respaldo previo automático, capa C)
        upload_to_dropbox(data_bytes)
    else:
        st.info("📂 Aún no subes tu histórico")


    st.caption("Desarrollado con ❤️ y Streamlit")

if __name__=="__main__":
    main()
