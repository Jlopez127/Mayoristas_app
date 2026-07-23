#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Genera tarjetas_cobradas.xlsx — LISTA DE EXCLUSIÓN anti-doble-cobro del cargue 1-a-1.
SE CORRE UNA SOLA VEZ (utilitario, NO es parte del flujo normal de la app) y el resultado
se sube a Dropbox junto al histórico (misma carpeta de remote_path). Los cobros posteriores
al corte entran por el flujo 1-a-1 normal y NO requieren mantener esta lista.

Uso:
  python3 generar_tarjetas_cobradas.py <cobrados.xlsx> <activity_amex.xlsx> <rakuten.csv> <salida.xlsx>
  (sin argumentos usa los DEFAULTS de abajo)

Entradas:
  - Excel de cobrados CONGELADO (20260710Cobro tarjetas.xlsx). Hojas usadas:
      'Amex Paula' -> 11591 ; 'Amex Julian' -> 13608 ; 'Amex Correal' -> 1444
      (JUAN P CORREAL **y** K LOPEZ VELANDIA/KELLY P LOPEZVELANDIA -> 1444: Kelly compra para
      1444, corrección 2026-07-22) ; 'RakutenCorreal' -> 1444.
      Las demás hojas (Citibank/Robinhood/Capital/JuanDAlfonso) NO aplican.
  - Extracto(s) Amex (activity*.xlsx, hoja 'Transaction Details', header fila 7) — fuente de
    los Reference. Acepta VARIOS separados por coma y los combina (dedup por Reference); usar
    el más completo disponible (activity(3) ene-jul + activity(4) jun-jul). El combinado arranca
    2026-01-01: las filas cobradas de 2025 quedan en 'pendientes_rematch' hasta el extracto 2025.
  - CSV Rakuten (Rakuten_Activity_All.csv) — fuente de los hash.

Salida (xlsx, 3 hojas):
  - cobradas:           Orden | tarjeta | casillero | fecha_compra | monto_usd | nota | fuente
  - pendientes_rematch: filas cobradas SIN Orden todavía (pre-asiento / 2025) con sus datos
                        crudos, para rematchear cuando haya extracto/CSV más nuevo.
  - revision:           anomalías (cobros repetidos sin respaldo en extracto). NO van a la
                        lista; el histórico legacy NO se corrige (regla del usuario).

MATCHING:
  - Amex: count-aware por (fecha compra, monto USD firmado, Card Member). La Description NO
    se usa (el Excel guarda nombres 'pending' que cambian al asentar). Si una clave tiene
    n cobros y el extracto n candidatas, da igual cuál Reference recibe cada cobro: para el
    anti-recobro solo importa el CONTEO por clave (se asignan los primeros min(n_cob, n_ext)
    Reference en orden ascendente, determinista).
  - Rakuten: la hoja del Excel tiene las columnas CORRIDAS (el paste partió "fecha, hora"):
    Date=fecha, Amount=hora, Type=monto, Merchant=tipo (minúscula), Category=merchant.
    Se reconstruye y se matchea contra el CSV por (timestamp exacto, monto):
      * filas 'transaction' -> su TRANSACTION del CSV (consumo 1 a 1);
      * filas 'auth' (cobros hechos en estado pendiente) -> su TRANSACTION firme: primero por
        (timestamp+monto) exacto; si el auth asentó PARTIDO en varias TRANSACTION (mismo
        timestamp, montos distintos), se consumen TODAS las TRANSACTION restantes de ese
        timestamp (el auth cubría el total);
      * auth aún sin TRANSACTION firme en el CSV -> pendientes_rematch.
    El hash se calcula desde las filas del CSV (los strings del Excel NO son byte-exactos:
    float sin '$', tipo en minúscula, fecha partida, mojibake NBSP).

⚠️ El cálculo del Orden Rakuten REPLICA exactamente el de procesar_rakuten en
mayoristas_streamlit_app.py (clave "Date|Amount|Merchant|seq" sobre las filas
TRANSACTION/REFUND con monto != 0; sha1[:12]). Si aquello cambia, esto debe cambiar igual.
El dry-run cruza ambas salidas para verificar que coinciden.
"""
import datetime
import hashlib
import sys
import pandas as pd

# ── Parámetros ────────────────────────────────────────────────────────────────
DEFAULTS = [
    "/Users/julianlopez/Downloads/20260710Cobro tarjetas.xlsx",
    # Extracto(s) Amex combinados (coma-separados): activity(3) ene-jul + activity(4) jun-jul.
    "/Users/julianlopez/Library/CloudStorage/OneDrive-Personal/Encargomio/Dash_mayoristas/activity (3).xlsx,"
    "/Users/julianlopez/Library/CloudStorage/OneDrive-Personal/Encargomio/Dash_mayoristas/activity (4).xlsx",
    "/Users/julianlopez/Library/CloudStorage/OneDrive-Personal/Encargomio/Dash_mayoristas/Rakuten_Activity_All.csv",
    # CSV Robinhood (export completo).
    "/Users/julianlopez/Downloads/53fa9510-b899-438c-9c3c-7ea05980d12a.csv",
    "tarjetas_cobradas.xlsx",
]
COBRADOS_XLSX, EXTRACTO_AMEX, CSV_RAKUTEN, CSV_ROBINHOOD, SALIDA = (
    sys.argv[1:6] if len(sys.argv) >= 6 else DEFAULTS
)
FUENTE = "Excel cobrados 20260710"
EXTRACTO_INICIO = pd.Timestamp("2026-01-01")   # inicio del extracto Amex combinado disponible
# Kelly compra para 1444; su tarjeta -23003 aparece en el extracto como "K LOPEZ VELANDIA".
# En el Excel de cobrados figura con dos grafías -> se unifican a la forma del extracto.
KELLY_ALIAS = {"KELLY P LOPEZVELANDIA": "K LOPEZ VELANDIA"}
HOJAS_AMEX = [  # (hoja, casillero, Card Members aceptados -> ese casillero)
    ("Amex Paula", "11591", ["PAULA HERRERA"]),
    ("Amex Julian", "13608", ["JULIAN SANCHEZ"]),
    ("Amex Correal", "1444", ["JUAN P CORREAL", "K LOPEZ VELANDIA"]),  # JP Correal + Kelly
]

cobradas, pendientes, revision = [], [], []
kelly_ordenes = 0  # cuántos Orden de Kelly (-> 1444) entran a la lista (para el reporte)


def _norm_cm(s) -> str:
    n = " ".join(str(s).strip().upper().split())
    return KELLY_ALIAS.get(n, n)


# ═════ AMEX ═════ — combina uno o más extractos (coma-separados), dedup por Reference
ext_paths = [p.strip() for p in EXTRACTO_AMEX.split(",") if p.strip()]
ext = pd.concat(
    [pd.read_excel(p, sheet_name="Transaction Details", header=6) for p in ext_paths],
    ignore_index=True,
)
ext["_ref"] = ext["Reference"].astype(str).str.strip()
ext = ext.drop_duplicates(subset=["_ref"], keep="first").copy()  # union por Reference (estable)
ext["_d"] = pd.to_datetime(ext["Date"], format="%m/%d/%Y", errors="coerce")
ext["_amt"] = pd.to_numeric(ext["Amount"], errors="coerce").round(2)
ext["_cm"] = ext["Card Member"].map(_norm_cm)

# índice: (fecha, monto firmado, CM) -> lista de Reference (ascendente, determinista)
refs_por_clave: dict = {}
for _, r in ext.iterrows():
    refs_por_clave.setdefault((r["_d"], r["_amt"], r["_cm"]), []).append(r["_ref"])
for k in refs_por_clave:
    refs_por_clave[k].sort()

resumen_amex = {}
for hoja, cas, cms_ok in HOJAS_AMEX:
    raw = pd.read_excel(COBRADOS_XLSX, sheet_name=hoja, header=None).iloc[:, :6]
    raw.columns = ["fecha", "flag", "desc", "cm", "cta", "usd"]
    d = raw[raw["fecha"].notna() & raw["usd"].notna()].copy()
    d["_d"] = pd.to_datetime(d["fecha"].astype(str), format="%m/%d/%Y", errors="coerce")
    d["_amt"] = pd.to_numeric(d["usd"], errors="coerce").round(2)
    d["_cm"] = d["cm"].map(_norm_cm)  # normaliza + unifica grafía de Kelly
    n_sin_parse = int((d["_d"].isna() | d["_amt"].isna()).sum())
    d = d[d["_d"].notna() & d["_amt"].notna()].copy()

    n_otros_cm = int((~d["_cm"].isin(cms_ok)).sum())  # CM no aceptado en esta hoja (no debería haber)
    n_kelly_hoja = int((d["_cm"] == "K LOPEZ VELANDIA").sum())
    d = d[d["_cm"].isin(cms_ok)].copy()

    # 2025 (antes del extracto combinado) -> pendientes de rematch
    d2025 = d[d["_d"] < EXTRACTO_INICIO]
    for _, r in d2025.iterrows():
        pendientes.append({
            "tarjeta": "amex", "casillero": cas, "hoja_origen": hoja,
            "fecha_compra": r["_d"].strftime("%Y-%m-%d"), "monto_usd": r["_amt"],
            "card_member": r["_cm"], "descripcion_excel": str(r["desc"]).strip(),
            "motivo": "requiere extracto Amex oct-dic 2025 (fuera del extracto combinado)",
        })
    d = d[d["_d"] >= EXTRACTO_INICIO]

    # match count-aware por clave (fecha, monto, Card Member) — cada CM usa su propio índice
    n_ok = n_pend = n_sobra = 0
    usd_ok = 0.0
    for (fd, amt, cm), g in d.groupby(["_d", "_amt", "_cm"], sort=True):
        n_cob = len(g)
        refs = refs_por_clave.get((fd, amt, cm), [])
        take = min(n_cob, len(refs))
        for ref in refs[:take]:
            cobradas.append({
                "Orden": f"amex_{ref}", "tarjeta": "amex", "casillero": cas,
                "fecha_compra": fd.strftime("%Y-%m-%d"), "monto_usd": amt,
                "nota": "Kelly -> 1444" if cm == "K LOPEZ VELANDIA" else "", "fuente": FUENTE,
            })
        if cm == "K LOPEZ VELANDIA":
            globals()["kelly_ordenes"] += take
        n_ok += take
        usd_ok += abs(amt) * take
        if n_cob > len(refs) and len(refs) > 0:
            # cobro repetido sin respaldo: NO va a la lista; el legacy no se corrige
            n_sobra += n_cob - len(refs)
            revision.append({
                "tarjeta": "amex", "casillero": cas, "hoja_origen": hoja,
                "fecha_compra": fd.strftime("%Y-%m-%d"), "monto_usd": amt, "card_member": cm,
                "detalle": f"cobrada {n_cob} veces, extracto tiene {len(refs)}",
            })
        if len(refs) == 0:
            n_pend += n_cob
            for _, r in g.iterrows():
                pendientes.append({
                    "tarjeta": "amex", "casillero": cas, "hoja_origen": hoja,
                    "fecha_compra": fd.strftime("%Y-%m-%d"), "monto_usd": amt,
                    "card_member": cm, "descripcion_excel": str(r["desc"]).strip(),
                    "motivo": "sin match en extracto combinado (cobro pre-asiento) — "
                              "rematch con extracto más nuevo",
                })
    resumen_amex[hoja] = dict(cas=cas, ok=n_ok, usd=usd_ok, pend_2025=len(d2025),
                              pend_asiento=n_pend, sobras=n_sobra, kelly=n_kelly_hoja,
                              otros_cm=n_otros_cm, sin_parse=n_sin_parse)

# ═════ RAKUTEN ═════
csvr = pd.read_csv(CSV_RAKUTEN)
# ── Orden por fila del CSV: RÉPLICA EXACTA de procesar_rakuten (ver advertencia arriba) ──
d = csvr.copy()
d["_type"] = d["Type"].astype(str).str.strip().str.upper()
d["_tipo"] = d["_type"].map({"TRANSACTION": "Egreso", "REFUND": "Ingreso"})
d = d[d["_tipo"].notna()].copy()
d["_fecha"] = pd.to_datetime(d["Date"], format="%Y/%m/%d, %H:%M:%S", errors="coerce")
d = d[d["_fecha"].notna()].copy()


def _parse_amount(x) -> float:  # réplica de _rakuten_parse_amount
    s = str(x).strip().replace("$", "").replace(",", "")
    neg = s.startswith("(") and s.endswith(")")
    s = s.strip("()")
    try:
        v = float(s)
    except ValueError:
        return float("nan")
    return -v if neg else v


d["_amount"] = d["Amount"].map(_parse_amount)
d = d[d["_amount"].notna() & (d["_amount"] != 0)].copy()
_clave = d["Date"].astype(str) + "|" + d["Amount"].astype(str) + "|" + d["Merchant"].astype(str)
_seq = _clave.groupby(_clave).cumcount().astype(str)
d["_orden"] = "rakuten_" + (_clave + "|" + _seq).map(
    lambda s: hashlib.sha1(s.encode("utf-8")).hexdigest()[:12]
)
# ── /réplica ──

tx = d[d["_type"] == "TRANSACTION"].copy()          # el Excel de cobrados solo cubre gastos
tx["_abs"] = tx["_amount"].abs().round(2)

# hoja RakutenCorreal (columnas corridas) reconstruida
rk = pd.read_excel(COBRADOS_XLSX, sheet_name="RakutenCorreal", header=0)
rk = rk.rename(columns={"Date": "fecha_d", "Amount": "hora", "Type": "monto",
                        "Merchant": "tipo", "Category": "merchant"})
rk = rk[rk["fecha_d"].map(lambda v: not isinstance(v, str))].copy()   # fuera fila 'Total'
rk["dt"] = pd.to_datetime(rk["fecha_d"].astype(str).str[:10] + " " +
                          rk["hora"].astype(str).str.strip(), errors="coerce")
rk["amt"] = pd.to_numeric(rk["monto"], errors="coerce")
rk["tipo_n"] = rk["tipo"].astype(str).str.strip().str.lower()
n_rk_invalidas = int((rk["dt"].isna() | rk["amt"].isna()).sum())
rk = rk[rk["dt"].notna() & rk["amt"].notna()].copy()

# índices de consumo sobre las TRANSACTION del CSV (orden de archivo, determinista)
por_clave: dict = {}
por_ts: dict = {}
for idx, r in tx.iterrows():
    por_clave.setdefault((r["_fecha"], r["_abs"]), []).append(idx)
    por_ts.setdefault(r["_fecha"], []).append(idx)
consumidos: set = set()


def _consumir(idx):
    consumidos.add(idx)
    r = tx.loc[idx]
    return {
        "Orden": r["_orden"], "tarjeta": "rakuten", "casillero": "1444",
        "fecha_compra": r["_fecha"].strftime("%Y-%m-%d %H:%M:%S"),
        "monto_usd": round(float(r["_amount"]), 2), "fuente": FUENTE,
    }


n_tx_ok = n_auth_ok = n_auth_split = n_auth_pend = 0
# pass 1: filas 'transaction' del Excel -> su TRANSACTION exacta (timestamp+monto)
for _, r in rk[rk["tipo_n"] == "transaction"].iterrows():
    libres = [i for i in por_clave.get((r["dt"], round(abs(r["amt"]), 2)), []) if i not in consumidos]
    if libres:
        cobradas.append({**_consumir(libres[0]), "nota": "transaction exacta"})
        n_tx_ok += 1
    else:
        revision.append({"tarjeta": "rakuten", "casillero": "1444", "hoja_origen": "RakutenCorreal",
                         "fecha_compra": str(r["dt"]), "monto_usd": r["amt"],
                         "detalle": "transaction del Excel sin TRANSACTION libre en el CSV"})
# pass 2: filas 'auth' (cobros en estado pendiente) -> TRANSACTION firme
for _, r in rk[rk["tipo_n"] == "auth"].iterrows():
    libres = [i for i in por_clave.get((r["dt"], round(abs(r["amt"]), 2)), []) if i not in consumidos]
    if libres:
        cobradas.append({**_consumir(libres[0]), "nota": "auth asentado (monto exacto)"})
        n_auth_ok += 1
        continue
    en_ts = [i for i in por_ts.get(r["dt"], []) if i not in consumidos]
    if en_ts:  # auth asentado PARTIDO: consumir todas las TRANSACTION de ese timestamp
        for i in en_ts:
            cobradas.append({**_consumir(i),
                             "nota": f"auth ${abs(r['amt']):.2f} asentado partido"})
        n_auth_split += 1
    else:
        n_auth_pend += 1
        pendientes.append({
            "tarjeta": "rakuten", "casillero": "1444", "hoja_origen": "RakutenCorreal",
            "fecha_compra": r["dt"].strftime("%Y-%m-%d %H:%M:%S"), "monto_usd": abs(r["amt"]),
            "card_member": "", "descripcion_excel": str(r["merchant"]).strip(),
            "motivo": "auth cobrado aún SIN transacción firme en el CSV — rematch con CSV más nuevo",
        })

# informativo: TRANSACTION del CSV que quedan SIN cobrar (el cargue las cobrará — correcto)
ventana = (tx["_fecha"] >= rk["dt"].min()) & (tx["_fecha"] <= rk["dt"].max())
sin_cobrar = tx[ventana & ~tx.index.isin(consumidos)]
pre_ventana = tx[tx["_fecha"] < rk["dt"].min()]
refunds = d[d["_type"] == "REFUND"]

# ═════ ROBINHOOD ═════ (hoja 'RobinhoodCorreal' — pegado crudo del CSV; trae Correal + Maria)
# El Orden REPLICA exactamente procesar_robinhood: robinhood_<sha1-12 de Date|Time|Amount|Merchant|seq>
# con seq sobre TODO el set 1444 (Correal + Maria) en orden de archivo, ANTES de filtrar Status.
# Match CARDHOLDER-AWARE (la hoja mezcla 181 Correal + 13 Maria): cada cobro contra su fila CSV
# Posted Purchase/Refund del MISMO cardholder por (fecha + |amount| + merchant), count-aware 1:1,
# fallback ±3 días. Clave RELAJADA (sin la hora): el Excel guarda la hora del AUTH y el CSV la del
# ASIENTO, que difiere -> el minuto exacto no matchea (verificado: la hora estricta perdía 21
# compras ya cobradas). Los que no matcheen -> pendientes_rematch.
ROBINHOOD_CARDMAP = {"Juan Pablo Correal Perez": "1444", "Maria Moises": "1444"}
# 2 in-window ya-cobrados que asentaron con monto DISTINTO al auth cobrado por el backoffice
# (la lista por monto exacto no los atrapa): se excluyen a mano. (cardholder, fecha, |amount|, merchant)
ROBINHOOD_MANUAL = [
    ("Juan Pablo Correal Perez", "2026-05-30", 172.29, "Uniqlo"),   # auth $172.78 -> asiento $172.29
    ("Juan Pablo Correal Perez", "2026-06-15", 622.98, "Hilton"),   # auth $772.98 -> asiento $622.98+$150
]


def _norm_merch_rb(s) -> str:
    return " ".join(str(s).split()).upper()


def _frac_to_time(v):
    """Fracción Excel (0..1) o datetime.time -> datetime.time (reconstrucción determinista)."""
    if isinstance(v, datetime.time):
        return v
    try:
        t = round(float(v) * 1440)
        return datetime.time((t // 60) % 24, t % 60)
    except Exception:
        return None


# CSV Robinhood -> filas 1444 con Orden (idéntico a procesar_robinhood)
rob = pd.read_csv(CSV_ROBINHOOD)
rob["_cas"] = rob["Cardholder"].astype(str).str.strip().map(ROBINHOOD_CARDMAP)
rob = rob[rob["_cas"].notna()].copy().reset_index(drop=True)
_rk = (rob["Date"].astype(str) + "|" + rob["Time"].astype(str) + "|" +
       rob["Amount"].astype(str) + "|" + rob["Merchant"].astype(str))
_rseq = _rk.groupby(_rk).cumcount().astype(str)
rob["_orden"] = "robinhood_" + (_rk + "|" + _rseq).map(
    lambda s: hashlib.sha1(s.encode("utf-8")).hexdigest()[:12])
assert rob["_orden"].is_unique, "❌ Orden Robinhood duplicado en el CSV — revisar esquema"
# candidatas generables: Posted Purchase/Refund
robg = rob[(rob["Status"].astype(str).str.strip() == "Posted") &
           (rob["Type"].astype(str).str.strip().isin(["Purchase", "Refund"]))].copy()
robg["_dt"] = pd.to_datetime(robg["Date"], errors="coerce")
robg["_amt"] = pd.to_numeric(robg["Amount"], errors="coerce").abs().round(2)
robg["_m"] = robg["Merchant"].map(_norm_merch_rb)
robg["_ch"] = robg["Cardholder"].astype(str).str.strip()

# hoja RobinhoodCorreal (pegado crudo): date/time(fracción)/cardholder/amount/merchant/desc/.../
# points/balance/status/type/merchant/desc
rc = pd.read_excel(COBRADOS_XLSX, sheet_name="RobinhoodCorreal", header=None)
rc = rc[rc[0].map(lambda v: not isinstance(v, str))].copy()   # fuera fila 'Total'/encabezado
rc["amt"] = pd.to_numeric(rc[3], errors="coerce")
n_rc_inval = int(rc["amt"].isna().sum())
rc = rc[rc["amt"].notna()].copy()
rc["dt"] = pd.to_datetime(rc[0]); rc["dtk"] = rc["dt"].dt.strftime("%Y-%m-%d")
rc["aamt"] = rc["amt"].abs().round(2); rc["m"] = rc[4].map(_norm_merch_rb)
rc["ch"] = rc[2].astype(str).str.strip()

# match cardholder-aware por (cardholder, fecha, |amount|, merchant), count-aware 1:1
por_key = {}
for i, x in robg.iterrows():
    por_key.setdefault((x["_ch"], x["_dt"].strftime("%Y-%m-%d"), x["_amt"], x["_m"]), []).append(i)
rob_usados, rob_fb = set(), 0
n_rob_ok = 0
_pend_rob = []
for _, x in rc.iterrows():
    k = (x["ch"], x["dtk"], x["aamt"], x["m"])
    libres = [i for i in por_key.get(k, []) if i not in rob_usados]
    if libres:
        rob_usados.add(libres[0])
        cobradas.append({"Orden": robg.loc[libres[0], "_orden"], "tarjeta": "robinhood",
                         "casillero": "1444", "fecha_compra": x["dtk"],
                         "monto_usd": round(float(x["amt"]), 2), "nota": "match exacto",
                         "fuente": FUENTE})
        n_rob_ok += 1
    else:
        _pend_rob.append(x)
# fallback ±3 días (mismo cardholder + merchant + |amount|)
_still = []
for x in _pend_rob:
    cand = robg[(robg["_ch"] == x["ch"]) & (robg["_amt"] == x["aamt"]) & (robg["_m"] == x["m"]) &
                (robg["_dt"].between(x["dt"] - pd.Timedelta(days=3), x["dt"] + pd.Timedelta(days=3)))]
    cand = [i for i in cand.index if i not in rob_usados]
    if cand:
        rob_usados.add(cand[0]); rob_fb += 1; n_rob_ok += 1
        cobradas.append({"Orden": robg.loc[cand[0], "_orden"], "tarjeta": "robinhood",
                         "casillero": "1444", "fecha_compra": x["dtk"],
                         "monto_usd": round(float(x["amt"]), 2),
                         "nota": f"match fallback ±3d (CSV {robg.loc[cand[0],'_dt'].strftime('%Y-%m-%d')})",
                         "fuente": FUENTE})
    else:
        _still.append(x)
# no matcheados -> pendientes (cobros sin fila firme en el CSV: auth con monto distinto / ausentes)
n_rob_pend = 0
for x in _still:
    n_rob_pend += 1
    pendientes.append({"tarjeta": "robinhood", "casillero": "1444", "hoja_origen": "RobinhoodCorreal",
                       "fecha_compra": x["dtk"], "monto_usd": abs(float(x["amt"])),
                       "card_member": x["ch"], "descripcion_excel": str(x[4]).strip(),
                       "motivo": "cobro sin fila firme en el CSV (auth con monto distinto / ausente) — "
                                 "rematch con CSV más nuevo"})
# 2 in-window ya-cobrados con monto asiento != auth: excluir a mano (Orden desde el CSV)
n_rob_manual = 0
for ch, fkm, amt, merch in ROBINHOOD_MANUAL:
    hit = robg[(robg["_ch"] == ch) & (robg["_dt"].dt.strftime("%Y-%m-%d") == fkm) &
               (robg["_amt"] == round(amt, 2)) & (robg["_m"] == _norm_merch_rb(merch))]
    if not len(hit):
        raise SystemExit(f"❌ ROBINHOOD_MANUAL no encontrado en el CSV: {ch} {fkm} ${amt} {merch}")
    o = robg.loc[hit.index[0], "_orden"]
    if o in rob_usados:
        continue  # ya estaba en la lista por match
    rob_usados.add(o); n_rob_manual += 1
    cobradas.append({"Orden": o, "tarjeta": "robinhood", "casillero": "1444", "fecha_compra": fkm,
                     "monto_usd": round(amt, 2),
                     "nota": "EXCLUSION MANUAL: asentó con monto distinto al auth cobrado por backoffice",
                     "fuente": FUENTE})
# informativo Robinhood (por Orden, no por índice: rob_usados mezcla índices de match y Orden
# de la exclusión manual). Las que ENTRARÁN = Posted Purchase/Refund ≥ corte cuyo Orden NO está
# en la lista de cobradas Robinhood.
_rob_en_lista = {c["Orden"] for c in cobradas if c["tarjeta"] == "robinhood"}
rob_sin_cobrar = robg[(robg["_dt"] >= pd.Timestamp("2026-04-14")) & (~robg["_orden"].isin(_rob_en_lista))]

# ═════ Salida ═════
df_cob = pd.DataFrame(cobradas, columns=["Orden", "tarjeta", "casillero", "fecha_compra",
                                         "monto_usd", "nota", "fuente"])
assert df_cob["Orden"].is_unique, "❌ Orden duplicado en la lista — revisar matching"
df_pen = pd.DataFrame(pendientes)
df_rev = pd.DataFrame(revision)
with pd.ExcelWriter(SALIDA, engine="openpyxl") as w:
    df_cob.to_excel(w, sheet_name="cobradas", index=False)
    (df_pen if len(df_pen) else pd.DataFrame(columns=["tarjeta"])).to_excel(
        w, sheet_name="pendientes_rematch", index=False)
    (df_rev if len(df_rev) else pd.DataFrame(columns=["tarjeta"])).to_excel(
        w, sheet_name="revision", index=False)

print(f"═════ tarjetas_cobradas generado: {SALIDA} ═════")
print(f"TOTAL lista de exclusión: {len(df_cob)} Orden "
      f"({(df_cob.tarjeta == 'amex').sum()} amex + {(df_cob.tarjeta == 'rakuten').sum()} rakuten + "
      f"{(df_cob.tarjeta == 'robinhood').sum()} robinhood)")
for hoja, s in resumen_amex.items():
    print(f"  {hoja} (cas {s['cas']}): {s['ok']} Orden (${s['usd']:,.2f} USD) | "
          f"pendientes 2025: {s['pend_2025']} | pendientes pre-asiento: {s['pend_asiento']} | "
          f"cobros sin respaldo (revision): {s['sobras']} | filas Kelly en hoja: {s['kelly']} | "
          f"otros CM: {s['otros_cm']} | sin parse: {s['sin_parse']}")
print(f"  KELLY -> 1444: {kelly_ordenes} Orden agregados a la lista (compras de Kelly, antes ignoradas)")
rk_cob = df_cob[df_cob.tarjeta == "rakuten"]
print(f"  RakutenCorreal (cas 1444): {len(rk_cob)} Orden (${rk_cob.monto_usd.abs().sum():,.2f} USD) "
      f"[transaction exactas: {n_tx_ok} | auth monto exacto: {n_auth_ok} | "
      f"auth partidos: {n_auth_split} (cubren varias TRANSACTION) | auth pendientes: {n_auth_pend}] | "
      f"filas inválidas hoja: {n_rk_invalidas}")
print(f"  Rakuten SIN cobrar en ventana (entrarán al cargue, correcto): {len(sin_cobrar)} "
      f"(${sin_cobrar['_amount'].sum():,.2f})")
for _, r in sin_cobrar.iterrows():
    print(f"    - {r['_fecha']} ${r['_amount']:.2f} {r['Merchant']} -> {r['_orden']}")
print(f"  ⚠️ PENDIENTE-DECISIÓN: {len(pre_ventana)} TRANSACTION del CSV anteriores a la ventana "
      f"del Excel (${pre_ventana['_amount'].sum():,.2f}, "
      f"{pre_ventana['_fecha'].min().date()} → {pre_ventana['_fecha'].max().date()}) NO van a la "
      f"lista; si ya se cobraron por otra vía, agregarlas antes de activar Rakuten.")
print(f"  Nota: {len(refunds)} REFUND del CSV no están en el Excel de cobrados -> entrarán como "
      f"Ingreso al activar (verificar que no se hayan devuelto ya por otra vía).")
rb_cob = df_cob[df_cob.tarjeta == "robinhood"]
print(f"  RobinhoodCorreal (cas 1444): {len(rb_cob)} Orden (${rb_cob.monto_usd.abs().sum():,.2f} USD) "
      f"[match exacto: {n_rob_ok - rob_fb} | fallback ±3d: {rob_fb} | exclusión manual in-window: "
      f"{n_rob_manual}] | pendientes: {n_rob_pend} | filas inválidas hoja: {n_rc_inval}")
print(f"  Robinhood SIN cobrar (≥14-abr, entrarán al cargue): {len(rob_sin_cobrar)} "
      f"(${rob_sin_cobrar['_amt'].sum():,.2f}) | in-window ≤22-jun tras exclusión: "
      f"{int((rob_sin_cobrar['_dt'] <= pd.Timestamp('2026-06-22')).sum())} (esperado 0)")
print(f"Pendientes de rematch totales: {len(df_pen)} | Revision: {len(df_rev)}")
