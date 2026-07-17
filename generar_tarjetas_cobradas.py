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
      'Amex Paula' -> 11591 ; 'Amex Julian' -> 13608 ; 'Amex Correal' -> 1444 (SOLO
      JUAN P CORREAL: K LOPEZ VELANDIA / KELLY P LOPEZVELANDIA se ignoran SIEMPRE,
      decisión de negocio 2026-07-16) ; 'RakutenCorreal' -> 1444.
      Las demás hojas (Citibank/Robinhood/Capital/JuanDAlfonso) NO aplican.
  - Extracto Amex (activity*.xlsx, hoja 'Transaction Details', header fila 7) — fuente de
    los Reference. El extracto actual arranca 2026-01-01: las filas cobradas de 2025 quedan
    en 'pendientes_rematch' hasta tener el extracto oct-dic 2025.
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
import hashlib
import sys
import pandas as pd

# ── Parámetros ────────────────────────────────────────────────────────────────
DEFAULTS = [
    "/Users/julianlopez/Downloads/20260710Cobro tarjetas.xlsx",
    "/Users/julianlopez/Library/CloudStorage/OneDrive-Personal/Encargomio/Dash_mayoristas/activity (3).xlsx",
    "/Users/julianlopez/Library/CloudStorage/OneDrive-Personal/Encargomio/Dash_mayoristas/Rakuten_Activity_All.csv",
    "tarjetas_cobradas.xlsx",
]
COBRADOS_XLSX, EXTRACTO_AMEX, CSV_RAKUTEN, SALIDA = (
    sys.argv[1:5] if len(sys.argv) >= 5 else DEFAULTS
)
FUENTE = "Excel cobrados 20260710"
EXTRACTO_INICIO = pd.Timestamp("2026-01-01")   # inicio del extracto Amex disponible
CM_IGNORADOS = {"K LOPEZ VELANDIA", "KELLY P LOPEZVELANDIA"}  # SIEMPRE ignoradas
HOJAS_AMEX = [  # (hoja, casillero, Card Member esperado)
    ("Amex Paula", "11591", "PAULA HERRERA"),
    ("Amex Julian", "13608", "JULIAN SANCHEZ"),
    ("Amex Correal", "1444", "JUAN P CORREAL"),
]

cobradas, pendientes, revision = [], [], []


def _norm_cm(s) -> str:
    return " ".join(str(s).strip().upper().split())


# ═════ AMEX ═════
ext = pd.read_excel(EXTRACTO_AMEX, sheet_name="Transaction Details", header=6)
ext["_d"] = pd.to_datetime(ext["Date"], format="%m/%d/%Y", errors="coerce")
ext["_amt"] = pd.to_numeric(ext["Amount"], errors="coerce").round(2)
ext["_cm"] = ext["Card Member"].map(_norm_cm)
ext["_ref"] = ext["Reference"].astype(str).str.strip()

# índice: (fecha, monto firmado, CM) -> lista de Reference (ascendente, determinista)
refs_por_clave: dict = {}
for _, r in ext.iterrows():
    refs_por_clave.setdefault((r["_d"], r["_amt"], r["_cm"]), []).append(r["_ref"])
for k in refs_por_clave:
    refs_por_clave[k].sort()

resumen_amex = {}
for hoja, cas, cm_esp in HOJAS_AMEX:
    raw = pd.read_excel(COBRADOS_XLSX, sheet_name=hoja, header=None).iloc[:, :6]
    raw.columns = ["fecha", "flag", "desc", "cm", "cta", "usd"]
    d = raw[raw["fecha"].notna() & raw["usd"].notna()].copy()
    d["_d"] = pd.to_datetime(d["fecha"].astype(str), format="%m/%d/%Y", errors="coerce")
    d["_amt"] = pd.to_numeric(d["usd"], errors="coerce").round(2)
    d["_cm"] = d["cm"].map(_norm_cm)
    n_sin_parse = int((d["_d"].isna() | d["_amt"].isna()).sum())
    d = d[d["_d"].notna() & d["_amt"].notna()].copy()

    n_klopez = int(d["_cm"].isin(CM_IGNORADOS).sum())          # ⛔ K Lopez: fuera SIEMPRE
    n_otros_cm = int((~d["_cm"].isin(CM_IGNORADOS) & (d["_cm"] != cm_esp)).sum())
    d = d[d["_cm"] == cm_esp].copy()

    # 2025 (antes del extracto disponible) -> pendientes de rematch
    d2025 = d[d["_d"] < EXTRACTO_INICIO]
    for _, r in d2025.iterrows():
        pendientes.append({
            "tarjeta": "amex", "casillero": cas, "hoja_origen": hoja,
            "fecha_compra": r["_d"].strftime("%Y-%m-%d"), "monto_usd": r["_amt"],
            "card_member": cm_esp, "descripcion_excel": str(r["desc"]).strip(),
            "motivo": "requiere extracto Amex oct-dic 2025 (fuera del extracto actual)",
        })
    d = d[d["_d"] >= EXTRACTO_INICIO]

    # match count-aware por clave
    n_ok = n_pend = n_sobra = 0
    usd_ok = 0.0
    grupos = d.groupby(["_d", "_amt"], sort=True)
    for (fd, amt), g in grupos:
        n_cob = len(g)
        refs = refs_por_clave.get((fd, amt, cm_esp), [])
        take = min(n_cob, len(refs))
        for ref in refs[:take]:
            cobradas.append({
                "Orden": f"amex_{ref}", "tarjeta": "amex", "casillero": cas,
                "fecha_compra": fd.strftime("%Y-%m-%d"), "monto_usd": amt,
                "nota": "", "fuente": FUENTE,
            })
        n_ok += take
        usd_ok += abs(amt) * take
        if n_cob > len(refs) and len(refs) > 0:
            # cobro repetido sin respaldo: NO va a la lista; el legacy no se corrige
            n_sobra += n_cob - len(refs)
            revision.append({
                "tarjeta": "amex", "casillero": cas, "hoja_origen": hoja,
                "fecha_compra": fd.strftime("%Y-%m-%d"), "monto_usd": amt,
                "detalle": f"cobrada {n_cob} veces, extracto tiene {len(refs)}",
            })
        if len(refs) == 0:
            n_pend += n_cob
            for _, r in g.iterrows():
                pendientes.append({
                    "tarjeta": "amex", "casillero": cas, "hoja_origen": hoja,
                    "fecha_compra": fd.strftime("%Y-%m-%d"), "monto_usd": amt,
                    "card_member": cm_esp, "descripcion_excel": str(r["desc"]).strip(),
                    "motivo": "sin match en extracto del 12-jul (cobro pre-asiento) — "
                              "rematch con extracto más nuevo",
                })
    resumen_amex[hoja] = dict(cas=cas, ok=n_ok, usd=usd_ok, pend_2025=len(d2025),
                              pend_asiento=n_pend, sobras=n_sobra, klopez=n_klopez,
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
      f"({(df_cob.tarjeta == 'amex').sum()} amex + {(df_cob.tarjeta == 'rakuten').sum()} rakuten)")
for hoja, s in resumen_amex.items():
    print(f"  {hoja} (cas {s['cas']}): {s['ok']} Orden (${s['usd']:,.2f} USD) | "
          f"pendientes 2025: {s['pend_2025']} | pendientes pre-asiento: {s['pend_asiento']} | "
          f"cobros sin respaldo (revision): {s['sobras']} | ignoradas K Lopez: {s['klopez']} | "
          f"otros CM: {s['otros_cm']} | sin parse: {s['sin_parse']}")
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
print(f"Pendientes de rematch totales: {len(df_pen)} | Revision: {len(df_rev)}")
