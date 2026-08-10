#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ajuste del incentivo TC de JULIO 2026 de Julian Sanchez (13608).

POR QUÉ
-------
El incentivo de julio se cargó el 2026-08-06 con 15.617 COP, calculado SOLO con la única fila
Amex 1-a-1 de julio (USD 624,68 × 25). En ese momento la tarjeta Capital One (1484) todavía no
estaba en el sistema, así que TODO el gasto de julio de esa tarjeta quedó fuera del cashback.

REGLA UNIFICADA (decisión del usuario, 2026-08-10) — igual para las 4 tarjetas:
    egresos (gastos) SUMAN · devoluciones por cancelación (Ingresos) RESTAN
    USD neto = gastos − devoluciones ; incentivo = neto × 25 COP/USD
Los PAGOS a la tarjeta ("ELECTRONIC PAYMENT" en Capital, "THANK YOU" en Amex) NO son devolución
y NO restan: son abonos del propio tarjetahabiente al saldo de la tarjeta.

EL MONTO NO ESTÁ HARDCODEADO: se calcula aquí desde las fuentes (extracto Capital + histórico
vivo) y se compara contra MONTO_ESPERADO como control de sanidad. Si las fuentes cambian, el
script avisa y aborta en vez de escribir un número viejo.

POR QUÉ NO USA EL PIPELINE DE main()
------------------------------------
`main()` deduplica ingresos con `drop_duplicates(["Orden","Tipo"], keep="last")` (línea ~4123) y
en la hoja 13608 eso COLAPSA el duplicado preexistente `146356` (dos Ingreso con el mismo Orden,
342.172,75 c/u) que el usuario decidió conservar → −342.172,75 COP. Aquí solo se REEMPLAZA el
Monto de una fila existente y se recalculan los totales: cero dedup, cero riesgo.

BLINDAJE: capa B (`preservar_filas_tarjeta`) → guard A (`guard_frescura_historico`) →
capa C (backup automático dentro de `upload_to_dropbox`). Todas son las funciones REALES.

  sin argumentos  -> dry-run (0 escrituras)
  --escribir      -> escribe a Dropbox
"""
import sys, os, io, warnings
warnings.filterwarnings("ignore")
import pandas as pd

ESCRIBIR = "--escribir" in sys.argv

# ── Parámetros ───────────────────────────────────────────────────────────────
CASILLERO = "13608"
USUARIO = "julian sanchez"
ORDEN = "incentivo_tc_13608_2026-07"
FECHA_INCENTIVO = "2026-08-01"      # el incentivo de julio se PAGA en agosto (no toca comisión)
MOTIVO = "Incentivo TC"
NOMBRE_PRODUCTO = "Incentivo TC Julio 2026 (25 COP x USD neto)"
COP_POR_USD = 25
MES = ("2026-07-01", "2026-07-31")

CSV_CAPITAL = "/Users/julianlopez/Downloads/2026-08-10_transaction_download (5).csv"
CAPITAL_CARD_NO = "1484"

MONTO_ANTERIOR = 15617
MONTO_ESPERADO = 640612            # control de sanidad del cálculo (ver banner 2)
REV_ESPERADA = "01658b33d47524a00000002f34b3f21"
SALDO_ESPERADO = -9155868.368      # último TOTAL de 13608 al 2026-08-10 15:45


def saldo(d):
    t = d[d["Tipo"].astype(str).str.strip().str.upper() == "TOTAL"]
    return float(pd.to_numeric(t["Monto"], errors="coerce").iloc[-1]) if len(t) else float("nan")


def main():
    import harness
    mod = harness.cargar_app()
    SEP = "=" * 84
    def banner(t): print(f"\n{SEP}\n{t}\n{SEP}")

    print(f"MODO: {'🔴 ESCRITURA REAL' if ESCRIBIR else '🟢 DRY-RUN (0 escrituras)'}")
    assert mod.INCENTIVO_AMEX_ACTIVO is False, "INCENTIVO_AMEX_ACTIVO cambió; abortar"
    print(f"INCENTIVO_AMEX_ACTIVO = {mod.INCENTIVO_AMEX_ACTIVO}  — este script NO lo toca")

    # ── 1) histórico vivo fresco ─────────────────────────────────────────────
    banner("1) HISTÓRICO VIVO FRESCO")
    cfg = mod.st.secrets["dropbox"]
    md = mod.dbx.files_get_metadata(cfg["remote_path"])
    print(f"  rev={md.rev}  modificado={md.server_modified}  size={md.size:,}")
    _, res = mod.dbx.files_download(cfg["remote_path"])
    vivo = pd.read_excel(io.BytesIO(res.content), sheet_name=None)
    HOJA = next(h for h in vivo if h.startswith(CASILLERO))
    s0 = saldo(vivo[HOJA])
    print(f"  hoja '{HOJA}': {len(vivo[HOJA])} filas · saldo COP {s0:,.2f}")
    movido = (md.rev != REV_ESPERADA) or abs(s0 - SALDO_ESPERADO) > 0.01
    if movido:
        print(f"  🚨 EL HISTÓRICO SE MOVIÓ (rev esperada {REV_ESPERADA}, "
              f"saldo esperado {SALDO_ESPERADO:,.2f})")
        raise SystemExit("⛔ ABORTA: rehacer el dry-run con el histórico fresco.")
    print("  ✔ rev y saldo idénticos a los del dry-run")

    # ── 2) cálculo del incentivo DESDE LAS FUENTES ───────────────────────────
    banner("2) CÁLCULO — regla unificada (gastos − devoluciones) × 25")
    ini, fin = pd.Timestamp(MES[0]), pd.Timestamp(MES[1])

    # 2a) CAPITAL — desde el extracto
    cap = pd.read_csv(CSV_CAPITAL, dtype=str)
    cap = cap[cap["Card No."].astype(str).str.strip() == CAPITAL_CARD_NO].copy()
    cap["_deb"] = pd.to_numeric(cap["Debit"], errors="coerce")
    cap["_cre"] = pd.to_numeric(cap["Credit"], errors="coerce")
    cap["_f"] = pd.to_datetime(cap["Transaction Date"], errors="coerce")
    cap["_cat"] = cap["Category"].astype(str).str.strip()
    capj = cap[(cap["_f"] >= ini) & (cap["_f"] <= fin)]
    cap_eg = capj[capj["_deb"].notna()]
    cap_dev = capj[capj["_cre"].notna() & capj["_cat"].eq(mod.CAPITAL_CAT_COMPRA)]
    cap_pago = capj[capj["_cre"].notna() & capj["_cat"].eq(mod.CAPITAL_CAT_PAGO)]
    usd_cap_eg = float(cap_eg["_deb"].sum())
    usd_cap_dev = float(cap_dev["_cre"].sum())

    # 2b) AMEX — desde el histórico vivo (Motivo exacto, USD = Monto/TRM)
    h = vivo[HOJA].copy()
    h["_f"] = pd.to_datetime(h["Fecha"], errors="coerce")
    h["_m"] = pd.to_numeric(h["Monto"], errors="coerce")
    h["_t"] = pd.to_numeric(h["TRM"], errors="coerce")
    hj = h[(h["_f"] >= ini) & (h["_f"] <= fin) &
           h["Motivo"].astype(str).str.strip().eq("Tarjeta Amex")]
    am_eg = hj[hj["Tipo"].astype(str).str.strip().eq("Egreso")]
    am_dev = hj[hj["Tipo"].astype(str).str.strip().eq("Ingreso")]
    usd_am_eg = float((am_eg["_m"] / am_eg["_t"]).sum())
    usd_am_dev = float((am_dev["_m"] / am_dev["_t"]).sum())

    gastos = usd_am_eg + usd_cap_eg
    devol = usd_am_dev + usd_cap_dev
    neto = gastos - devol
    monto = round(COP_POR_USD * neto)

    print(f"  {'concepto':<40}{'n':>5}{'USD':>16}")
    print(f"  {'-'*61}")
    print(f"  {'Egresos Amex julio':<40}{len(am_eg):>5}{usd_am_eg:>16,.2f}")
    print(f"  {'Egresos Capital julio':<40}{len(cap_eg):>5}{usd_cap_eg:>16,.2f}")
    print(f"  {'(−) Devoluciones Amex julio':<40}{len(am_dev):>5}{usd_am_dev:>16,.2f}")
    print(f"  {'(−) Devoluciones Capital julio':<40}{len(cap_dev):>5}{usd_cap_dev:>16,.2f}")
    print(f"  {'-'*61}")
    print(f"  {'USD NETO':<45}{neto:>16,.2f}")
    print(f"  {'× 25 COP/USD':<45}{monto:>16,}")
    print(f"\n  pagos a la tarjeta EXCLUIDOS (no son devolución): "
          f"{len(cap_pago)} filas, USD {float(cap_pago['_cre'].sum()):,.2f}")
    print(f"  monto anterior {MONTO_ANTERIOR:,}  →  nuevo {monto:,}   "
          f"DELTA +{monto - MONTO_ANTERIOR:,}")
    if monto != MONTO_ESPERADO:
        raise SystemExit(f"⛔ ABORTA: el cálculo dio {monto:,} y se esperaba "
                         f"{MONTO_ESPERADO:,}. Las fuentes cambiaron; revisar.")
    print(f"  ✔ coincide con MONTO_ESPERADO ({MONTO_ESPERADO:,})")

    # ── 3) reemplazo de la fila (SIN dedup) ──────────────────────────────────
    banner("3) REEMPLAZO DE LA FILA (sin dedup — el duplicado 146356 no se toca)")
    historico = {k: mod.asegurar_columnas_historico(v.copy()) for k, v in vivo.items()}
    d = historico[HOJA]
    m = d["Orden"].astype(str).str.strip().eq(ORDEN)
    if int(m.sum()) != 1:
        raise SystemExit(f"⛔ ABORTA: se esperaba 1 fila '{ORDEN}' y hay {int(m.sum())}.")
    r = d.loc[m].iloc[0]
    print(f"  fila encontrada: Fecha={str(r['Fecha'])[:10]}  Tipo={str(r['Tipo']).strip()}  "
          f"Motivo={str(r['Motivo']).strip()}  Monto={float(r['Monto']):,.0f}")
    for campo, esperado in (("Tipo", "Ingreso"), ("Motivo", MOTIVO)):
        if str(r[campo]).strip() != esperado:
            raise SystemExit(f"⛔ ABORTA: {campo} = '{r[campo]}' (se esperaba '{esperado}').")
    if str(r["Fecha"])[:10] != FECHA_INCENTIVO:
        raise SystemExit(f"⛔ ABORTA: Fecha = {str(r['Fecha'])[:10]} (se esperaba {FECHA_INCENTIVO}).")

    fecha_carga = pd.Timestamp.today().strftime("%Y-%m-%d")
    d.loc[m, "Monto"] = monto
    d.loc[m, "Nombre del producto"] = NOMBRE_PRODUCTO
    d.loc[m, "Fecha de Carga"] = fecha_carga
    print(f"  Monto {MONTO_ANTERIOR:,} → {monto:,}   (Orden, Fecha, Tipo y Motivo SIN CAMBIO)")
    n0 = len(d)
    historico[HOJA] = mod.recalcular_totales_diarios(d, usuario=USUARIO, cas=CASILLERO)
    print(f"  filas {n0} → {len(historico[HOJA])} (recalcular_totales_diarios)")

    # ── 4) capa B ────────────────────────────────────────────────────────────
    banner("4) 🛡️ CAPA B — preservar_filas_tarjeta")
    historico = mod.preservar_filas_tarjeta(historico, vivo=vivo)
    print("  ✔ ejecutada")

    # ── 5) diff ──────────────────────────────────────────────────────────────
    banner("5) DIFF")
    for hoja in vivo:
        a, b = vivo[hoja], historico[hoja]
        igual = len(a) == len(b) and (("Orden" not in a.columns) or
                                      set(a["Orden"].astype(str)) == set(b["Orden"].astype(str)))
        if igual and hoja != HOJA:
            print(f"  {hoja:<34} sin cambios ({len(a)}) ✔ verbatim")
        else:
            print(f"  {hoja:<34} {len(a)} → {len(b)} | saldo {saldo(a):,.2f} → {saldo(b):,.2f} "
                  f"({saldo(b)-saldo(a):+,.2f})")
    h6 = historico[HOJA]
    n146 = int(h6["Orden"].astype(str).str.strip().eq("146356").sum())
    print(f"\n  duplicado 146356 en 13608: {n146} filas {'✔ INTACTO' if n146 == 2 else '🚨'}")
    if n146 != 2:
        raise SystemExit("⛔ ABORTA: el duplicado 146356 cambió.")
    n_inc = sum(int((b["Orden"].astype(str).str.strip() == ORDEN).sum())
                for b in historico.values() if "Orden" in b.columns)
    print(f"  {ORDEN}: {n_inc} fila(s) {'✔' if n_inc == 1 else '🚨'}")
    if n_inc != 1:
        raise SystemExit("⛔ ABORTA: duplicado del incentivo.")
    n_com = int(h6["Orden"].astype(str).str.lower().str.startswith("comision de (").sum())
    print(f"  filas de comisión en 13608: {n_com} {'✔ (Julian no lleva comisión)' if n_com == 0 else '🚨'}")
    if n_com != 0:
        raise SystemExit("⛔ ABORTA: apareció una comisión en 13608.")
    for c in ("11591", "1444"):
        hh = next(h for h in vivo if h.startswith(c))
        o = f"incentivo_tc_{c}_2026-07"
        va = float(vivo[hh].loc[vivo[hh]["Orden"].astype(str).str.strip() == o, "Monto"].iloc[0])
        vb = float(historico[hh].loc[historico[hh]["Orden"].astype(str).str.strip() == o, "Monto"].iloc[0])
        print(f"  {o:<30} {va:,.0f} → {vb:,.0f} {'✔ sin cambio' if va == vb else '🚨'}")

    # ── 6) bytes + guard A ───────────────────────────────────────────────────
    banner("6) EXCEL EN MEMORIA + 🛡️ GUARD A")
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        for hh, dfh in historico.items():
            w.book.create_sheet(hh[:31])
            dfh.to_excel(w, sheet_name=hh[:31], index=False)
    buf.seek(0)
    data_bytes = buf.read()
    print(f"  {len(data_bytes):,} bytes | {len(historico)} hojas")
    harness.clear_msgs()
    try:
        mod.guard_frescura_historico(historico)
        print("  ✅ GUARD A PASA (0 pérdidas)")
    except harness._Stop:
        for n, t in harness.MENSAJES:
            print(f"  [{n}] {t[:400]}")
        raise SystemExit("⛔ GUARD A BLOQUEÓ")

    print(f"\n  SALDO 13608: {s0:,.2f} → {saldo(historico[HOJA]):,.2f} "
          f"({saldo(historico[HOJA]) - s0:+,.2f})")

    if not ESCRIBIR:
        banner("DRY-RUN TERMINADO — 0 ESCRITURAS")
        print("  Para escribir: python3 ajustar_incentivo_julio_julian.py --escribir")
        return

    # ── 7) escritura ─────────────────────────────────────────────────────────
    banner("7) RE-VERIFICACIÓN DE FRESCURA + SUBIDA (capa C hace backup)")
    md_pre = mod.dbx.files_get_metadata(cfg["remote_path"])
    if md_pre.rev != REV_ESPERADA:
        raise SystemExit(f"⛔ ABORTA SIN ESCRIBIR: el histórico se movió (rev {md_pre.rev}).")
    print(f"  ✔ rev sin cambios ({md_pre.rev})")
    harness.clear_msgs()
    mod.upload_to_dropbox(data_bytes)
    backup = None
    for n, t in harness.MENSAJES:
        print(f"  [{n}] {t}")
        if "Respaldo previo creado" in t and "`" in t:
            backup = t.split("`")[1]

    # ── 8) validación post-escritura ─────────────────────────────────────────
    banner("8) VALIDACIÓN POST-ESCRITURA")
    md2 = mod.dbx.files_get_metadata(cfg["remote_path"])
    print(f"  rev NUEVA = {md2.rev}   modificado = {md2.server_modified}   size = {md2.size:,}")
    print(f"  backup    = {backup}")
    _, r2 = mod.dbx.files_download(cfg["remote_path"])
    rel = pd.read_excel(io.BytesIO(r2.content), sheet_name=None)
    ok = True
    def chk(n, c, det=""):
        nonlocal ok
        print(f"  {'✔' if c else '🚨'} {n:<52} {det}")
        ok = ok and bool(c)
    f = rel[HOJA][rel[HOJA]["Orden"].astype(str).str.strip() == ORDEN]
    chk("fila de incentivo presente (1)", len(f) == 1, f"{len(f)}")
    if len(f):
        rr = f.iloc[0]
        chk(f"Monto = {monto:,}", round(float(rr["Monto"])) == monto, f"{rr['Monto']}")
        chk("Tipo/Motivo/Fecha intactos",
            str(rr["Tipo"]).strip() == "Ingreso" and str(rr["Motivo"]).strip() == MOTIVO
            and str(rr["Fecha"])[:10] == FECHA_INCENTIVO,
            f"{rr['Tipo']} / {rr['Motivo']} / {str(rr['Fecha'])[:10]}")
    chk("duplicado 146356 intacto (2 filas)",
        int(rel[HOJA]["Orden"].astype(str).str.strip().eq("146356").sum()) == 2)
    chk("0 filas de comisión en 13608",
        int(rel[HOJA]["Orden"].astype(str).str.lower().str.startswith("comision de (").sum()) == 0)
    chk("saldo 13608", True, f"COP {saldo(rel[HOJA]):,.2f}")
    for c in ("11591", "1444"):
        hh = next(h for h in vivo if h.startswith(c))
        o = f"incentivo_tc_{c}_2026-07"
        va = float(vivo[hh].loc[vivo[hh]["Orden"].astype(str).str.strip() == o, "Monto"].iloc[0])
        vb = float(rel[hh].loc[rel[hh]["Orden"].astype(str).str.strip() == o, "Monto"].iloc[0])
        chk(f"{o} sin cambio", va == vb, f"{vb:,.0f}")
    for hoja in vivo:
        if hoja == HOJA:
            continue
        a, b = vivo[hoja], rel[hoja]
        ig = len(a) == len(b)
        if ig and "Orden" in a.columns:
            ig = set(a["Orden"].astype(str)) == set(b["Orden"].astype(str))
        if ig and "Monto" in a.columns:
            ig = abs(pd.to_numeric(a["Monto"], errors="coerce").fillna(0).sum() -
                     pd.to_numeric(b["Monto"], errors="coerce").fillna(0).sum()) < 0.01
        chk(f"verbatim: {hoja}", ig, f"{len(a)} filas")
    chk("INCENTIVO_AMEX_ACTIVO sigue False", mod.INCENTIVO_AMEX_ACTIVO is False,
        f"{mod.INCENTIVO_AMEX_ACTIVO}")
    dups = []
    for hoja, b in rel.items():
        if "Orden" not in b.columns:
            continue
        oo = b["Orden"].astype(str).str.strip()
        oo = oo[oo.str.startswith(("amex_", "rakuten_", "robinhood_", "capital_", "incentivo"))]
        dups += list(oo[oo.duplicated()].unique())
    chk("0 Orden duplicados (tarjeta/incentivo)", not dups, f"{len(dups)}")
    chk(f"{len(vivo)} hojas", len(rel) == len(vivo), f"{len(rel)}")
    print(f"\n  {'✅ AJUSTE COMPLETO Y VERIFICADO' if ok else '🚨 REVISAR'}")
    print(f"  rev nueva: {md2.rev}\n  rollback:  {backup}")


if __name__ == "__main__":
    sys.path.insert(0, os.environ.get("HARNESS_DIR", "."))
    main()
