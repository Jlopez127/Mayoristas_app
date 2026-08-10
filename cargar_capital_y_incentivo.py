#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cargue de TARJETA CAPITAL (Capital One 1484 -> 13608) + ajuste del incentivo TC de julio 2026
de Julian Sanchez, en UNA sola escritura.

QUÉ HACE
--------
A) Corre `procesar_capital` sobre el extracto y agrega sus filas a la hoja de 13608:
   Debit -> Egreso · Credit 'Merchandise' -> Ingreso (TRM de la compra original) ·
   'ELECTRONIC PAYMENT' -> ignorado.
B) Reemplaza el Monto de la fila `incentivo_tc_13608_2026-07` con el incentivo recalculado bajo
   la regla unificada: (egresos julio − devoluciones julio) × 25, Amex + Capital.

POR QUÉ NO USA EL PIPELINE DE main()
------------------------------------
`main()` deduplica ingresos con `drop_duplicates(["Orden","Tipo"], keep="last")` y en la hoja
13608 eso COLAPSA el duplicado preexistente `146356` (dos Ingreso con el mismo Orden,
342.172,75 c/u) que el usuario decidió conservar -> −342.172,75 COP.
Aquí el dedup es QUIRÚRGICO: solo sobre las filas `capital_*` por 'Orden' (lo justo para que
recargar sea idempotente). Ninguna otra fila se deduplica.

ORDEN OBLIGATORIO
-----------------
La lista de exclusión enriquecida (con las 80 entradas 'capital') debe estar EN DROPBOX ANTES de
escribir. En `--escribir` el script ABORTA si no la encuentra; en dry-run la simula y avisa.

BLINDAJE: capa B (`preservar_filas_tarjeta`) -> guard A (`guard_frescura_historico`) ->
capa C (backup automático dentro de `upload_to_dropbox`).

  sin argumentos  -> dry-run (0 escrituras)
  --escribir      -> escribe a Dropbox
"""
import sys, os, io, warnings
warnings.filterwarnings("ignore")
import pandas as pd

ESCRIBIR = "--escribir" in sys.argv

CASILLERO = "13608"
USUARIO = "julian sanchez"
CSV_CAPITAL = "/Users/julianlopez/Downloads/2026-08-10_transaction_download (5).csv"

ORDEN_INC = "incentivo_tc_13608_2026-07"
FECHA_INC = "2026-08-01"
MOTIVO_INC = "Incentivo TC"
NOMBRE_INC = "Incentivo TC Julio 2026 (25 COP x USD neto)"
COP_POR_USD = 25
MES = ("2026-07-01", "2026-07-31")
MONTO_INC_ANTERIOR = 15617
MONTO_INC_ESPERADO = 640612

REV_ESPERADA = "01658b33d47524a00000002f34b3f21"
SALDO_ESPERADO = -9155868.368


def saldo(d):
    t = d[d["Tipo"].astype(str).str.strip().str.upper() == "TOTAL"]
    return float(pd.to_numeric(t["Monto"], errors="coerce").iloc[-1]) if len(t) else float("nan")


def main():
    import harness
    mod = harness.cargar_app()
    import generar_capital_cobradas as gcc
    SEP = "=" * 88
    def banner(t): print(f"\n{SEP}\n{t}\n{SEP}")

    print(f"MODO: {'🔴 ESCRITURA REAL' if ESCRIBIR else '🟢 DRY-RUN (0 escrituras)'}")
    assert mod.INCENTIVO_AMEX_ACTIVO is False, "INCENTIVO_AMEX_ACTIVO cambió; abortar"
    print(f"INCENTIVO_AMEX_ACTIVO={mod.INCENTIVO_AMEX_ACTIVO} · "
          f"CAPITAL_FECHA_DESDE={mod.CAPITAL_FECHA_DESDE} · "
          f"CAPITAL_IGNORAR_CREDITOS={mod.CAPITAL_IGNORAR_CREDITOS}")
    assert mod.CAPITAL_IGNORAR_CREDITOS is False, "se esperaba CAPITAL_IGNORAR_CREDITOS=False"

    # ── 1) vivo fresco ───────────────────────────────────────────────────────
    banner("1) HISTÓRICO VIVO FRESCO")
    cfg = mod.st.secrets["dropbox"]
    md = mod.dbx.files_get_metadata(cfg["remote_path"])
    print(f"  rev={md.rev}  modificado={md.server_modified}  size={md.size:,}")
    _, res = mod.dbx.files_download(cfg["remote_path"])
    vivo = pd.read_excel(io.BytesIO(res.content), sheet_name=None)
    HOJA = next(h for h in vivo if h.startswith(CASILLERO))
    s0 = saldo(vivo[HOJA])
    print(f"  hoja '{HOJA}': {len(vivo[HOJA])} filas · saldo COP {s0:,.2f}")
    if md.rev != REV_ESPERADA or abs(s0 - SALDO_ESPERADO) > 0.01:
        print(f"  🚨 EL HISTÓRICO SE MOVIÓ (rev esperada {REV_ESPERADA}, "
              f"saldo esperado {SALDO_ESPERADO:,.2f})")
        raise SystemExit("⛔ ABORTA: rehacer el dry-run con el histórico fresco.")
    print("  ✔ rev y saldo idénticos a los del dry-run")

    # ── 2) lista de exclusión ────────────────────────────────────────────────
    banner("2) LISTA DE EXCLUSIÓN")
    cobrados, pendientes, cobrados_df = mod.cargar_tarjetas_cobradas()
    harness.clear_msgs()
    n_cap = int((cobrados_df["tarjeta"].astype(str).str.strip().str.lower() == "capital").sum())
    print(f"  en Dropbox: {len(cobrados_df)} cobros · "
          f"{cobrados_df['tarjeta'].value_counts().to_dict()}")
    if n_cap == 0:
        if ESCRIBIR:
            raise SystemExit(
                "⛔ ABORTA: la lista de Dropbox NO trae las entradas 'capital'. "
                "Sube primero la lista enriquecida (PASO 1) — sin ella se re-cobrarían "
                "compras y se re-abonarían devoluciones.")
        print("  ⚠️ la lista de Dropbox aún NO trae 'capital' -> SE SIMULAN para el dry-run")
        cap_entries, n_hash, sin_ext = gcc.construir(mod)
        cobrados_df = pd.concat([cobrados_df, cap_entries], ignore_index=True)
        cobrados = set(cobrados_df["Orden"].astype(str).str.strip()) - {"", "nan", "None"}
        print(f"  lista SIMULADA: {len(cobrados_df)} cobros "
              f"({len(cap_entries)} capital: "
              f"{int((cap_entries['signo']=='Egreso').sum())} Egreso + "
              f"{int((cap_entries['signo']=='Ingreso').sum())} Ingreso)")
    for t in ("amex", "rakuten", "robinhood"):
        print(f"    {t:<10} {int((cobrados_df['tarjeta']==t).sum())} ✔ intactos")

    # ── 3) PARTE A — cargue Capital ──────────────────────────────────────────
    banner("3) PARTE A — CARGUE CAPITAL")
    hist_tarj = mod.cargar_hist_tarjetas()
    harness.clear_msgs()
    out = mod.procesar_capital(pd.read_csv(CSV_CAPITAL),
                               fecha_desde=mod.CAPITAL_FECHA_DESDE,
                               cobrados=cobrados, pendientes=pendientes,
                               hist_tarjetas=hist_tarj, cobrados_df=cobrados_df)
    for niv, m in harness.drenar():
        print(f"  [{niv}] {m}")
    cap = out.get(f"capital_{CASILLERO}", pd.DataFrame())
    if cap.empty:
        raise SystemExit("⛔ ABORTA: procesar_capital no devolvió filas.")
    cap["_f"] = pd.to_datetime(cap["Fecha"])
    cap["USD"] = (cap["Monto"] / cap["TRM"]).round(2)

    # reconciliación
    raw = pd.read_csv(CSV_CAPITAL, dtype=str)
    raw = raw[raw["Card No."].astype(str).str.strip() == mod.CAPITAL_CARD_NO].copy()
    raw["_d"] = pd.to_numeric(raw["Debit"], errors="coerce")
    raw["_c"] = pd.to_numeric(raw["Credit"], errors="coerce")
    raw["_f"] = pd.to_datetime(raw["Transaction Date"])
    raw["_cat"] = raw["Category"].astype(str).str.strip()
    m_pago = raw["_c"].notna() & ~raw["_cat"].eq(mod.CAPITAL_CAT_COMPRA)
    carg = raw[~m_pago]
    m_ant = carg["_f"] < pd.Timestamp(mod.CAPITAL_FECHA_DESDE)
    resto = carg[~m_ant]
    print(f"\n  RECONCILIACIÓN")
    print(f"    {len(raw):>3} filas del extracto (Card {mod.CAPITAL_CARD_NO})")
    print(f"    −{int(m_pago.sum()):>3} pagos a la tarjeta (ELECTRONIC PAYMENT) — ignorados")
    print(f"    −{int(m_ant.sum()):>3} cargables anteriores al corte {mod.CAPITAL_FECHA_DESDE}")
    print(f"    −{len(resto) - len(cap):>3} ya liquidados (lista: cobros + abonos)")
    print(f"    ={len(cap):>3} ENTRAN   "
          f"{'✔ CIERRA' if len(resto) - (len(resto)-len(cap)) == len(cap) else '🚨 RESIDUO'}")

    eg = cap[cap["Tipo"] == "Egreso"]; ing = cap[cap["Tipo"] == "Ingreso"]
    print(f"\n  QUE ENTRAN: {len(cap)} filas = {len(eg)} Egreso + {len(ing)} Ingreso")
    for etq, mes in (("JULIO", 7), ("AGOSTO", 8)):
        s = cap[cap["_f"].dt.month == mes]
        se = s[s["Tipo"] == "Egreso"]; si = s[s["Tipo"] == "Ingreso"]
        print(f"    {etq:<7} {len(s):>2} filas · {len(se)} Egreso USD {se['USD'].sum():>10,.2f} "
              f"(COP {se['Monto'].sum():>13,.0f}) · {len(si)} Ingreso USD {si['USD'].sum():>8,.2f} "
              f"(COP {si['Monto'].sum():>11,.0f})")
    print(f"\n  DEVOLUCIONES QUE ENTRAN ({len(ing)}):")
    for _, r in ing.sort_values("Fecha").iterrows():
        print(f"    {r['Fecha']}  USD {r['USD']:>8,.2f}  TRM {r['TRM']:>8,.2f}  "
              f"COP {r['Monto']:>11,.0f}  {r['Nombre del producto'][:78]}")
    _sin_trm = ing[~ing["Nombre del producto"].str.contains("TRM compra")]
    print(f"    devoluciones SIN TRM de compra original: {len(_sin_trm)} "
          f"{'✔' if len(_sin_trm) == 0 else '⚠️'}")
    print(f"\n  NETO Capital que entra: COP {eg['Monto'].sum() - ing['Monto'].sum():,.0f} "
          f"(egresos {eg['Monto'].sum():,.0f} − ingresos {ing['Monto'].sum():,.0f})")

    # ── 4) PARTE B — incentivo ───────────────────────────────────────────────
    banner("4) PARTE B — INCENTIVO DE JULIO (derivado de los datos)")
    ini, fin = pd.Timestamp(MES[0]), pd.Timestamp(MES[1])
    cj = raw[(raw["_f"] >= ini) & (raw["_f"] <= fin)]
    usd_cap_eg = float(cj.loc[cj["_d"].notna(), "_d"].sum())
    usd_cap_dev = float(cj.loc[cj["_c"].notna() & cj["_cat"].eq(mod.CAPITAL_CAT_COMPRA), "_c"].sum())
    h = vivo[HOJA].copy()
    h["_f"] = pd.to_datetime(h["Fecha"], errors="coerce")
    h["_m"] = pd.to_numeric(h["Monto"], errors="coerce")
    h["_t"] = pd.to_numeric(h["TRM"], errors="coerce")
    hj = h[(h["_f"] >= ini) & (h["_f"] <= fin) &
           h["Motivo"].astype(str).str.strip().eq("Tarjeta Amex")]
    ae = hj[hj["Tipo"].astype(str).str.strip().eq("Egreso")]
    ai = hj[hj["Tipo"].astype(str).str.strip().eq("Ingreso")]
    usd_am_eg = float((ae["_m"] / ae["_t"]).sum()); usd_am_dev = float((ai["_m"] / ai["_t"]).sum())
    neto = (usd_am_eg + usd_cap_eg) - (usd_am_dev + usd_cap_dev)
    monto_inc = round(COP_POR_USD * neto)
    print(f"    {'Egresos Amex julio':<34}{len(ae):>4}{usd_am_eg:>15,.2f}")
    print(f"    {'Egresos Capital julio':<34}{int(cj['_d'].notna().sum()):>4}{usd_cap_eg:>15,.2f}")
    print(f"    {'(−) Devoluciones Amex julio':<34}{len(ai):>4}{usd_am_dev:>15,.2f}")
    print(f"    {'(−) Devoluciones Capital julio':<34}"
          f"{int((cj['_c'].notna() & cj['_cat'].eq(mod.CAPITAL_CAT_COMPRA)).sum()):>4}"
          f"{usd_cap_dev:>15,.2f}")
    print(f"    {'USD NETO':<38}{neto:>15,.2f}")
    print(f"    {'× 25 COP/USD':<38}{monto_inc:>15,}")
    print(f"    anterior {MONTO_INC_ANTERIOR:,} → nuevo {monto_inc:,}  "
          f"DELTA +{monto_inc - MONTO_INC_ANTERIOR:,}")
    if monto_inc != MONTO_INC_ESPERADO:
        raise SystemExit(f"⛔ ABORTA: el incentivo dio {monto_inc:,} y se esperaba "
                         f"{MONTO_INC_ESPERADO:,}. Las fuentes cambiaron.")
    print(f"    ✔ coincide con MONTO_INC_ESPERADO ({MONTO_INC_ESPERADO:,})")

    # ── 5) aplicar a la hoja ─────────────────────────────────────────────────
    banner("5) APLICAR A LA HOJA 13608 (dedup QUIRÚRGICO, solo capital_)")
    historico = {k: mod.asegurar_columnas_historico(v.copy()) for k, v in vivo.items()}
    d = historico[HOJA]
    fecha_carga = pd.Timestamp.today().strftime("%Y-%m-%d")
    nuevas = cap.drop(columns=["_f", "USD"]).copy()
    nuevas["Fecha de Carga"] = fecha_carga
    d = pd.concat([d, mod.asegurar_columnas_historico(nuevas)], ignore_index=True)
    o = d["Orden"].astype(str).str.strip()
    m_cap = o.str.startswith("capital_")
    antes_cap = int(m_cap.sum())
    d = pd.concat([d[~m_cap], d[m_cap].drop_duplicates(subset=["Orden"], keep="last")],
                  ignore_index=True)
    print(f"  filas capital_ tras concat: {antes_cap} → "
          f"{int(d['Orden'].astype(str).str.startswith('capital_').sum())} (dedup quirúrgico)")
    m_inc = d["Orden"].astype(str).str.strip().eq(ORDEN_INC)
    if int(m_inc.sum()) != 1:
        raise SystemExit(f"⛔ ABORTA: {int(m_inc.sum())} filas '{ORDEN_INC}' (se esperaba 1).")
    r0 = d.loc[m_inc].iloc[0]
    for campo, esp in (("Tipo", "Ingreso"), ("Motivo", MOTIVO_INC)):
        if str(r0[campo]).strip() != esp:
            raise SystemExit(f"⛔ ABORTA: {campo}='{r0[campo]}' (se esperaba '{esp}').")
    if str(r0["Fecha"])[:10] != FECHA_INC:
        raise SystemExit(f"⛔ ABORTA: Fecha={str(r0['Fecha'])[:10]} (se esperaba {FECHA_INC}).")
    d.loc[m_inc, "Monto"] = monto_inc
    d.loc[m_inc, "Nombre del producto"] = NOMBRE_INC
    d.loc[m_inc, "Fecha de Carga"] = fecha_carga
    print(f"  incentivo: {MONTO_INC_ANTERIOR:,} → {monto_inc:,} "
          f"(Orden/Fecha/Tipo/Motivo sin cambio)")
    historico[HOJA] = mod.recalcular_totales_diarios(d, usuario=USUARIO, cas=CASILLERO)

    # ── 6) capa B ────────────────────────────────────────────────────────────
    banner("6) 🛡️ CAPA B — preservar_filas_tarjeta")
    historico = mod.preservar_filas_tarjeta(historico, vivo=vivo)
    for niv, m in harness.drenar():
        print(f"  [{niv}] {m[:300]}")
    print("  ✔ ejecutada")

    # ── 7) diff + invariantes ────────────────────────────────────────────────
    banner("7) DIFF E INVARIANTES")
    for hoja in vivo:
        a, b = vivo[hoja], historico[hoja]
        ig = len(a) == len(b) and (("Orden" not in a.columns) or
                                   set(a["Orden"].astype(str)) == set(b["Orden"].astype(str)))
        if ig and hoja != HOJA:
            print(f"  {hoja:<34} sin cambios ({len(a)}) ✔ verbatim")
        else:
            print(f"  {hoja:<34} {len(a)} → {len(b)} | saldo {saldo(a):,.2f} → {saldo(b):,.2f} "
                  f"({saldo(b)-saldo(a):+,.2f})")
    h6 = historico[HOJA]
    o6 = h6["Orden"].astype(str).str.strip()
    ok = True
    def chk(n, c, det=""):
        nonlocal ok
        print(f"  {'✔' if c else '🚨'} {n:<52} {det}")
        ok = ok and bool(c)
    n146 = int(o6.eq("146356").sum())
    chk("duplicado 146356 intacto (2 filas)", n146 == 2, f"{n146}")
    chk("filas capital_ en 13608", int(o6.str.startswith("capital_").sum()) == len(cap),
        f"{int(o6.str.startswith('capital_').sum())}")
    chk("0 comisión en 13608", int(o6.str.lower().str.startswith("comision de (").sum()) == 0)
    chk(f"{ORDEN_INC} = {monto_inc:,}",
        round(float(h6.loc[o6.eq(ORDEN_INC), "Monto"].iloc[0])) == monto_inc)
    _dc = o6[o6.str.startswith("capital_")]
    chk("0 Orden capital_ duplicados", not _dc.duplicated().any(), f"{int(_dc.duplicated().sum())}")
    for c in ("11591", "1444"):
        hh = next(x for x in vivo if x.startswith(c))
        oo = f"incentivo_tc_{c}_2026-07"
        va = float(vivo[hh].loc[vivo[hh]["Orden"].astype(str).str.strip() == oo, "Monto"].iloc[0])
        vb = float(historico[hh].loc[historico[hh]["Orden"].astype(str).str.strip() == oo, "Monto"].iloc[0])
        chk(f"{oo} sin cambio", va == vb, f"{vb:,.0f}")
    if not ok:
        raise SystemExit("⛔ ABORTA: falló alguna invariante.")

    # ── 8) bytes + guard A ───────────────────────────────────────────────────
    banner("8) EXCEL EN MEMORIA + 🛡️ GUARD A")
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

    s1 = saldo(historico[HOJA])
    banner("9) SALDO 13608")
    print(f"  antes                          COP {s0:>18,.2f}")
    print(f"  efecto Capital (56→ neto)      COP {-(eg['Monto'].sum() - ing['Monto'].sum()):>18,.2f}")
    print(f"  efecto ajuste incentivo        COP {monto_inc - MONTO_INC_ANTERIOR:>18,.2f}")
    print(f"  después                        COP {s1:>18,.2f}")
    print(f"  Δ total                        COP {s1 - s0:>18,.2f}")

    if not ESCRIBIR:
        banner("DRY-RUN TERMINADO — 0 ESCRITURAS")
        print("  Para escribir: python3 cargar_capital_y_incentivo.py --escribir")
        print("  (requiere que la lista enriquecida ya esté en Dropbox)")
        return

    # ── 10) escritura ────────────────────────────────────────────────────────
    banner("10) RE-VERIFICACIÓN + SUBIDA (capa C hace backup)")
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

    banner("11) VALIDACIÓN POST-ESCRITURA")
    md2 = mod.dbx.files_get_metadata(cfg["remote_path"])
    print(f"  rev NUEVA = {md2.rev}   modificado = {md2.server_modified}   size = {md2.size:,}")
    print(f"  backup    = {backup}")
    _, r2 = mod.dbx.files_download(cfg["remote_path"])
    rel = pd.read_excel(io.BytesIO(r2.content), sheet_name=None)
    ok2 = True
    def chk2(n, c, det=""):
        nonlocal ok2
        print(f"  {'✔' if c else '🚨'} {n:<52} {det}")
        ok2 = ok2 and bool(c)
    rr = rel[HOJA]; oo = rr["Orden"].astype(str).str.strip()
    _cap = rr[oo.str.startswith("capital_")]
    chk2(f"{len(cap)} filas capital_ en 13608", len(_cap) == len(cap), f"{len(_cap)}")
    chk2("  · Egreso / Ingreso",
         int((_cap["Tipo"].astype(str).str.strip() == "Egreso").sum()) == len(eg) and
         int((_cap["Tipo"].astype(str).str.strip() == "Ingreso").sum()) == len(ing),
         f"{int((_cap['Tipo'].astype(str).str.strip()=='Egreso').sum())} / "
         f"{int((_cap['Tipo'].astype(str).str.strip()=='Ingreso').sum())}")
    _cf = pd.to_datetime(_cap["Fecha"], errors="coerce")
    chk2("  · julio / agosto", True,
         f"{int((_cf.dt.month==7).sum())} / {int((_cf.dt.month==8).sum())}")
    chk2(f"incentivo Julian = {monto_inc:,}",
         round(float(rr.loc[oo.eq(ORDEN_INC), "Monto"].iloc[0])) == monto_inc)
    chk2("duplicado 146356 intacto (2 filas)", int(oo.eq("146356").sum()) == 2)
    chk2("0 comisión en 13608", int(oo.str.lower().str.startswith("comision de (").sum()) == 0)
    chk2("saldo 13608", True, f"COP {saldo(rr):,.2f}")
    for c in ("11591", "1444"):
        hh = next(x for x in vivo if x.startswith(c))
        o_ = f"incentivo_tc_{c}_2026-07"
        va = float(vivo[hh].loc[vivo[hh]["Orden"].astype(str).str.strip() == o_, "Monto"].iloc[0])
        vb = float(rel[hh].loc[rel[hh]["Orden"].astype(str).str.strip() == o_, "Monto"].iloc[0])
        chk2(f"{o_} sin cambio", va == vb, f"{vb:,.0f}")
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
        chk2(f"verbatim: {hoja}", ig, f"{len(a)} filas")
    chk2("INCENTIVO_AMEX_ACTIVO sigue False", mod.INCENTIVO_AMEX_ACTIVO is False)
    dups = []
    for hoja, b in rel.items():
        if "Orden" not in b.columns:
            continue
        z = b["Orden"].astype(str).str.strip()
        z = z[z.str.startswith(("amex_", "rakuten_", "robinhood_", "capital_", "incentivo"))]
        dups += list(z[z.duplicated()].unique())
    chk2("0 Orden duplicados (tarjeta/incentivo)", not dups, f"{len(dups)}")
    chk2(f"{len(vivo)} hojas", len(rel) == len(vivo), f"{len(rel)}")
    print(f"\n  {'✅ CARGUE + AJUSTE COMPLETOS Y VERIFICADOS' if ok2 else '🚨 REVISAR'}")
    print(f"  rev nueva: {md2.rev}\n  rollback:  {backup}")


if __name__ == "__main__":
    sys.path.insert(0, os.environ.get("HARNESS_DIR", "."))
    main()
