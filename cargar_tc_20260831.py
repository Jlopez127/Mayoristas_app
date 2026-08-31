#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cargue de tarjetas del 2026-08-31, en UNA sola escritura.

QUÉ ENTRA
---------
A) CAPITAL (Capital One 1484) -> hoja 13608, con `procesar_capital`.
   Extracto: 2026-08-31_transaction_download (5).csv (2026-06-30 → 2026-08-29).
   ⚠️ RANGO COMPLETO a propósito: con el extracto corto (desde 31-jul) las 2 devoluciones
   eBay del 5-ago (USD 856,52 c/u) pierden su compra original del 12-jul, caen al fallback
   de TRM y DEGRADAN dos filas ya correctas (3.373,87 → 3.329,51 = −75.990 COP a Julian).
B) US BANK (0613) -> hojas 11591 (sub 0598 Paula) y 13608 (sub 0609 Julian), con
   `procesar_usbank`. Kelly (2529) y Santiago (0534) siguen IGNORADOS por constante.
   Extracto: Credit Card - 0613_07-31-2026_09-04-2026.csv (2026-08-17 → 2026-08-28).
C) APPLE PAY -> hoja 1444, 4 compras del 2026-08-25 cargadas A MANO (USD 8.216,10).
   La tarjeta se le prestó a 1444 de forma TEMPORAL y NO se va a procesar su extracto:
   el Apple Card lo usan también Santiago y Kelly para gasto que NO es de 1444, así que
   no hay módulo ni lo debe haber. Las 4 filas se derivan del extracto de Apple Card
   (verificadas una a una) y se convierten con la MISMA TRM que las tarjetas
   (`_amex_trm_dia` = oficial datos.gov.co + 125), no con una TRM inventada.

POR QUÉ NO USA EL PIPELINE DE main()
------------------------------------
`main()` deduplica ingresos con drop_duplicates(["Orden","Tipo"]) y en 13608 eso COLAPSA el
duplicado preexistente `146356` (2 Ingreso del mismo Orden) que el usuario decidió conservar.
Aquí el dedup es QUIRÚRGICO: solo sobre las filas del prefijo que toca cada hoja.
Tampoco se corre el bloque de comisión quincenal: hoy es día ≥16, su ventana recalcularía
"1-15 agosto" (ya escrita, 502.298,14) y ninguna fila de este cargue cae en esa quincena.

BLINDAJE: capa B (`preservar_filas_tarjeta`) -> guard A (`guard_frescura_historico`) ->
capa C (backup automático dentro de `upload_to_dropbox`).

  sin argumentos  -> dry-run (0 escrituras)
  --escribir      -> escribe a Dropbox
"""
import sys, os, io, warnings
warnings.filterwarnings("ignore")
import pandas as pd

ESCRIBIR = "--escribir" in sys.argv

CSV_CAPITAL = "/Users/julianlopez/Downloads/2026-08-31_transaction_download (5).csv"
CSV_USBANK = "/Users/julianlopez/Downloads/Credit Card - 0613_07-31-2026_09-04-2026.csv"

# ── C) Apple Pay -> 1444 ──────────────────────────────────────────────────────
# Verificadas contra "Apple Card Transactions Aug 01 2026 - Aug 27 2026.csv": las 4 existen
# con ese monto y esa fecha exactos. El resto del gasto de esa tarjeta NO es de 1444.
APPLEPAY_CASILLERO = "1444"
APPLEPAY_FECHA = "2026-08-25"
APPLEPAY_MOTIVO = "Tarjeta Apple Pay"
APPLEPAY_USD_TOTAL = 8216.10
APPLEPAY_COMPRAS = [   # (secuencia, comercio, USD)
    ("01", "Ross Stores", 2980.94),
    ("02", "Calvin Klein", 1459.27),
    ("03", "Calvin Klein", 2456.41),
    ("04", "Calvin Klein", 1319.48),
]

HOJAS = {"11591": None, "13608": None, "1444": None}   # se resuelven contra el vivo

# Barandas de frescura: se rellenan tras el dry-run y el modo --escribir las exige.
REV_ESPERADA = "0165a1a4f78b90b00000002f34b3f21"
SALDOS_ESPERADOS = {
    "11591": -17223361.65,
    "13608": 169116689.60,
    "1444": 85148635.21,
}


def saldo(d):
    t = d[d["Tipo"].astype(str).str.strip().str.upper() == "TOTAL"]
    return float(pd.to_numeric(t["Monto"], errors="coerce").iloc[-1]) if len(t) else float("nan")


def usuario_de_totales(d):
    """Usuario tal como YA lo llevan las filas TOTAL de la hoja (recalcular las reescribe
    todas; pasar otro valor renombraría miles de filas sin razón)."""
    t = d[d["Tipo"].astype(str).str.strip().str.upper() == "TOTAL"]
    u = t["Usuario"].astype(str).str.strip()
    u = u[~u.str.lower().isin({"", "nan", "none"})]
    if len(u):
        return u.mode().iloc[0]
    u = d["Usuario"].astype(str).str.strip()
    u = u[~u.str.lower().isin({"", "nan", "none"})]
    return u.mode().iloc[0] if len(u) else ""


def cas_de_totales(d, fallback):
    """Casillero tal como YA lo llevan las filas TOTAL (11591 lo guarda como float)."""
    t = d[d["Tipo"].astype(str).str.strip().str.upper() == "TOTAL"]
    v = t["Casillero"].dropna()
    return v.iloc[-1] if len(v) else fallback


def main():
    import harness
    mod = harness.cargar_app()
    SEP = "=" * 92
    def banner(t): print(f"\n{SEP}\n{t}\n{SEP}")

    print(f"MODO: {'🔴 ESCRITURA REAL' if ESCRIBIR else '🟢 DRY-RUN (0 escrituras)'}")
    print(f"CAPITAL_FECHA_DESDE={mod.CAPITAL_FECHA_DESDE} · USBANK_FECHA_DESDE={mod.USBANK_FECHA_DESDE}")
    print(f"USBANK_MAP_SUBTARJETA={mod.USBANK_MAP_SUBTARJETA} · IGNORAR={sorted(mod.USBANK_SUBTARJETAS_IGNORAR)}")
    assert mod.INCENTIVO_AMEX_ACTIVO is False, "INCENTIVO_AMEX_ACTIVO cambió; abortar"
    assert "2529" in mod.USBANK_SUBTARJETAS_IGNORAR, "Kelly (2529) volvió a estar mapeada; abortar"
    assert mod.USBANK_MAP_SUBTARJETA == {"0598": "11591", "0609": "13608"}, "mapa US Bank cambió"

    # ── 1) vivo fresco ───────────────────────────────────────────────────────
    banner("1) HISTÓRICO VIVO FRESCO")
    cfg = mod.st.secrets["dropbox"]
    md = mod.dbx.files_get_metadata(cfg["remote_path"])
    print(f"  rev={md.rev}  modificado={md.server_modified}  size={md.size:,}")
    _, res = mod.dbx.files_download(cfg["remote_path"])
    vivo = pd.read_excel(io.BytesIO(res.content), sheet_name=None)
    for cas in HOJAS:
        HOJAS[cas] = next(h for h in vivo if h.split(" - ")[0].strip() == cas)
    s0 = {cas: saldo(vivo[h]) for cas, h in HOJAS.items()}
    for cas, h in HOJAS.items():
        print(f"  {cas:<6} '{h}': {len(vivo[h]):>5} filas · saldo COP {s0[cas]:>18,.2f}")
    if md.rev != REV_ESPERADA or any(abs(s0[c] - SALDOS_ESPERADOS[c]) > 0.01 for c in HOJAS):
        print(f"  🚨 EL HISTÓRICO SE MOVIÓ (rev esperada {REV_ESPERADA})")
        if ESCRIBIR:
            raise SystemExit("⛔ ABORTA: rehacer el dry-run con el histórico fresco.")
        print("  ⚠️ dry-run continúa, pero actualizar REV_ESPERADA/SALDOS_ESPERADOS antes de escribir")
    else:
        print("  ✔ rev y saldos idénticos a los esperados")

    # ── 2) lista de exclusión ────────────────────────────────────────────────
    banner("2) LISTA DE EXCLUSIÓN (obligatoria)")
    cobrados, pendientes, cobrados_df = mod.cargar_tarjetas_cobradas()
    harness.clear_msgs()
    vc = cobrados_df["tarjeta"].astype(str).str.strip().str.lower().value_counts().to_dict()
    print(f"  {len(cobrados_df)} cobros · {vc}")
    for t in ("capital", "usbank"):
        if vc.get(t, 0) == 0:
            raise SystemExit(f"⛔ ABORTA: la lista no trae entradas '{t}'.")
    if not {"card_norm", "merchant_norm", "usd_abs", "fecha_attr", "signo"} <= set(cobrados_df.columns):
        raise SystemExit("⛔ ABORTA: la lista no trae columnas de atributos (barrera 2 inactiva).")
    print("  ✔ trae capital + usbank y las columnas de atributos (barrera 2 activa)")

    hist_tarj = mod.cargar_hist_tarjetas()
    harness.clear_msgs()

    # ── 3) PARTE A — Capital ─────────────────────────────────────────────────
    banner("3) PARTE A — CAPITAL (1484 -> 13608)")
    raw_cap = pd.read_csv(CSV_CAPITAL)
    _f = pd.to_datetime(raw_cap["Transaction Date"])
    print(f"  extracto: {len(raw_cap)} filas · {_f.min().date()} → {_f.max().date()}")
    out_cap = mod.procesar_capital(raw_cap.copy(), fecha_desde=mod.CAPITAL_FECHA_DESDE,
                                   cobrados=cobrados, pendientes=pendientes,
                                   hist_tarjetas=hist_tarj, cobrados_df=cobrados_df)
    for niv, m in harness.drenar():
        print(f"  [{niv}] {m[:400]}")
    cap = out_cap.get(f"capital_{mod.CAPITAL_CASILLERO}", pd.DataFrame())
    print(f"  claves devueltas: {list(out_cap)}")

    if not cap.empty:
        cap["_f"] = pd.to_datetime(cap["Fecha"])
        cap["USD"] = (cap["Monto"] / cap["TRM"]).round(2)
        r = raw_cap.copy()
        r["_d"] = pd.to_numeric(r["Debit"], errors="coerce")
        r["_c"] = pd.to_numeric(r["Credit"], errors="coerce")
        r["_f"] = pd.to_datetime(r["Transaction Date"])
        r["_cat"] = r["Category"].astype(str).str.strip()
        r = r[r["Card No."].astype(str).str.strip() == mod.CAPITAL_CARD_NO]
        m_pago = r["_c"].notna() & ~r["_cat"].eq(mod.CAPITAL_CAT_COMPRA)
        carg = r[~m_pago]
        m_ant = carg["_f"] < pd.Timestamp(mod.CAPITAL_FECHA_DESDE)
        resto = carg[~m_ant]
        print(f"\n  RECONCILIACIÓN")
        print(f"    {len(r):>3} filas Card {mod.CAPITAL_CARD_NO}")
        print(f"    −{int(m_pago.sum()):>3} pagos a la tarjeta (ELECTRONIC PAYMENT)")
        print(f"    −{int(m_ant.sum()):>3} anteriores al corte {mod.CAPITAL_FECHA_DESDE}")
        print(f"    −{len(resto) - len(cap):>3} ya liquidados (lista/atributos)")
        print(f"    ={len(cap):>3} ENTRAN")
        eg_c = cap[cap["Tipo"] == "Egreso"]; in_c = cap[cap["Tipo"] == "Ingreso"]
        print(f"\n  {len(eg_c)} Egreso USD {eg_c['USD'].sum():>10,.2f} (COP {eg_c['Monto'].sum():>13,.0f})")
        print(f"  {len(in_c)} Ingreso USD {in_c['USD'].sum():>10,.2f} (COP {in_c['Monto'].sum():>13,.0f})")
        print(f"  rango de fechas: {cap['_f'].min().date()} → {cap['_f'].max().date()}")
        if len(in_c):
            print(f"\n  DEVOLUCIONES ({len(in_c)}):")
            for _, x in in_c.sort_values("Fecha").iterrows():
                print(f"    {x['Fecha']}  USD {x['USD']:>8,.2f}  TRM {x['TRM']:>8,.2f}  "
                      f"COP {x['Monto']:>11,.0f}  {x['Nombre del producto'][:74]}")
            sin = in_c[~in_c["Nombre del producto"].str.contains("TRM compra")]
            print(f"    devoluciones SIN TRM de compra original: {len(sin)} {'✔' if len(sin)==0 else '⚠️ REVISAR'}")
        print(f"\n  NETO Capital: COP {eg_c['Monto'].sum() - in_c['Monto'].sum():,.0f}")
    else:
        print("  (0 filas nuevas de Capital)")

    # ── 4) PARTE B — US Bank ─────────────────────────────────────────────────
    banner("4) PARTE B — US BANK (0613 -> 11591 Paula + 13608 Julian)")
    raw_ub = pd.read_csv(CSV_USBANK)
    _fu = pd.to_datetime(raw_ub["Date"])
    print(f"  extracto: {len(raw_ub)} filas · {_fu.min().date()} → {_fu.max().date()}")
    out_ub = mod.procesar_usbank(raw_ub.copy(), fecha_desde=mod.USBANK_FECHA_DESDE,
                                 cobrados=cobrados, pendientes=pendientes,
                                 hist_tarjetas=hist_tarj, cobrados_df=cobrados_df)
    for niv, m in harness.drenar():
        print(f"  [{niv}] {m[:400]}")
    print(f"  claves devueltas: {list(out_ub)}")
    claves_ok = {f"usbank_{c}" for c in ("11591", "13608")}
    if set(out_ub) - claves_ok:
        raise SystemExit(f"⛔ ABORTA: US Bank devolvió casilleros inesperados: {set(out_ub) - claves_ok}")

    def sub(memo):
        p = str(memo).split(";")
        return p[2].strip()[:4] if len(p) >= 3 and p[2].strip() else "(sin titular)"
    rb = raw_ub.copy()
    rb["_sub"] = rb["Memo"].map(sub)
    rb["_f"] = pd.to_datetime(rb["Date"])
    print(f"\n  RECONCILIACIÓN por sub-tarjeta (todo el extracto)")
    for s, g in rb.groupby("_sub"):
        destino = mod.USBANK_MAP_SUBTARJETA.get(s, "IGNORADA")
        print(f"    {s:<13} {len(g):>5} filas  USD {g['Amount'].sum():>13,.2f}  -> {destino}")
    for cas in ("11591", "13608"):
        ub = out_ub.get(f"usbank_{cas}", pd.DataFrame())
        s = {v: k for k, v in mod.USBANK_MAP_SUBTARJETA.items()}[cas]
        cand = rb[(rb["_sub"] == s) & (rb["_f"] >= pd.Timestamp(mod.USBANK_FECHA_DESDE))]
        print(f"\n  {cas} (sub {s}): {len(cand)} en extracto ≥ corte → {len(ub)} ENTRAN "
              f"({len(cand) - len(ub)} ya liquidadas/ya cargadas)")
        if not ub.empty:
            e = ub[ub["Tipo"] == "Egreso"]; i = ub[ub["Tipo"] == "Ingreso"]
            ub["USD"] = (ub["Monto"] / ub["TRM"]).round(2)
            print(f"    {len(e)} Egreso COP {e['Monto'].sum():>13,.0f} · "
                  f"{len(i)} Ingreso COP {i['Monto'].sum():>11,.0f} · "
                  f"fechas {pd.to_datetime(ub['Fecha']).min().date()} → {pd.to_datetime(ub['Fecha']).max().date()}")
            for _, x in ub.sort_values("Fecha").iterrows():
                print(f"      {x['Fecha']}  {x['Tipo']:<7} COP {x['Monto']:>11,.0f}  "
                      f"{str(x['Nombre del producto'])[:70]}")

    # ── 5) PARTE C — Apple Pay a mano -> 1444 ────────────────────────────────
    banner("5) PARTE C — APPLE PAY (4 compras del 25-ago) -> 1444")
    trm_cache = {}
    trm_ap = mod._amex_trm_dia(APPLEPAY_FECHA, trm_cache)
    if trm_ap is None:
        raise SystemExit(f"⛔ ABORTA: sin TRM para {APPLEPAY_FECHA} (datos.gov.co).")
    print(f"  TRM {APPLEPAY_FECHA} = {trm_ap:,.2f} (oficial {trm_ap - mod.AMEX_TRM_SPREAD:,.2f} + {mod.AMEX_TRM_SPREAD})")
    ap_filas = []
    for seq, comercio, usd in APPLEPAY_COMPRAS:
        ap_filas.append({
            "Fecha": APPLEPAY_FECHA,
            "Tipo": "Egreso",
            "Monto": round(float(usd) * trm_ap),
            "Orden": f"applepay_{APPLEPAY_FECHA.replace('-', '')}_{seq}",
            "Motivo": APPLEPAY_MOTIVO,
            "TRM": round(trm_ap, 2),
            "Usuario": "Maria Moises",
            "Casillero": APPLEPAY_CASILLERO,
            "Estado de Orden": "",
            "Nombre del producto": f"Tarjeta Apple Pay - gasto - {comercio}",
        })
    ap = pd.DataFrame(ap_filas)
    _sum_usd = sum(u for _, _, u in APPLEPAY_COMPRAS)
    if abs(_sum_usd - APPLEPAY_USD_TOTAL) > 0.005:
        raise SystemExit(f"⛔ ABORTA: las 4 compras suman USD {_sum_usd:,.2f} y se esperaba "
                         f"{APPLEPAY_USD_TOTAL:,.2f}.")
    for _, x in ap.iterrows():
        print(f"    {x['Orden']:<24} {x['Nombre del producto']:<45} "
              f"USD {x['Monto']/trm_ap:>9,.2f}  COP {x['Monto']:>12,.0f}")
    print(f"  {'TOTAL':<24} {'':<45} USD {_sum_usd:>9,.2f}  COP {ap['Monto'].sum():>12,.0f}  ✔ USD cuadra")

    # ── 6) aplicar a las hojas (dedup QUIRÚRGICO por prefijo) ────────────────
    banner("6) APLICAR A LAS HOJAS")
    historico = {k: mod.asegurar_columnas_historico(v.copy()) for k, v in vivo.items()}
    fecha_carga = pd.Timestamp.today().strftime("%Y-%m-%d")
    aporte = {
        "11591": [("usbank_", out_ub.get("usbank_11591", pd.DataFrame()))],
        "13608": [("capital_", cap.drop(columns=["_f", "USD"], errors="ignore")),
                  ("usbank_", out_ub.get("usbank_13608", pd.DataFrame()))],
        "1444": [("applepay_", ap)],
    }
    n_nuevas = {}
    for cas, bloques in aporte.items():
        hoja = HOJAS[cas]
        d = historico[hoja]
        antes = len(d)
        for prefijo, nuevas in bloques:
            if nuevas is None or nuevas.empty:
                print(f"  {cas} {prefijo:<10} 0 filas")
                continue
            nv = nuevas.copy()
            nv["Fecha de Carga"] = fecha_carga
            d = pd.concat([d, mod.asegurar_columnas_historico(nv)], ignore_index=True)
            o = d["Orden"].astype(str).str.strip()
            m = o.str.startswith(prefijo)
            tras = int(m.sum())
            d = pd.concat([d[~m], d[m].drop_duplicates(subset=["Orden"], keep="last")],
                          ignore_index=True)
            final = int(d["Orden"].astype(str).str.strip().str.startswith(prefijo).sum())
            print(f"  {cas} {prefijo:<10} +{len(nv):>3} filas · {prefijo}* {tras} → {final} (dedup quirúrgico)")
        n_nuevas[cas] = len(d) - antes
        u, c = usuario_de_totales(historico[hoja]), cas_de_totales(historico[hoja], cas)
        historico[hoja] = mod.recalcular_totales_diarios(d, usuario=u, cas=c)
        print(f"  {cas} recalculado con usuario='{u}' casillero={c!r} · filas {antes} → {len(historico[hoja])}")

    # ── 7) capa B ────────────────────────────────────────────────────────────
    banner("7) 🛡️ CAPA B — preservar_filas_tarjeta")
    historico = mod.preservar_filas_tarjeta(historico, vivo=vivo)
    for niv, m in harness.drenar():
        print(f"  [{niv}] {m[:300]}")
    print("  ✔ ejecutada")

    # ── 8) diff + invariantes ────────────────────────────────────────────────
    banner("8) DIFF E INVARIANTES")
    for hoja in vivo:
        a, b = vivo[hoja], historico[hoja]
        ig = len(a) == len(b) and (("Orden" not in a.columns) or
                                   set(a["Orden"].astype(str)) == set(b["Orden"].astype(str)))
        if ig and hoja not in HOJAS.values():
            print(f"  {hoja:<34} sin cambios ({len(a)}) ✔ verbatim")
        else:
            print(f"  {hoja:<34} {len(a)} → {len(b)} | saldo {saldo(a):,.2f} → {saldo(b):,.2f} "
                  f"({saldo(b)-saldo(a):+,.2f})")
    ok = True
    def chk(n, c, det=""):
        nonlocal ok
        print(f"  {'✔' if c else '🚨'} {n:<58} {det}")
        ok = ok and bool(c)
    o13 = historico[HOJAS["13608"]]["Orden"].astype(str).str.strip()
    o11 = historico[HOJAS["11591"]]["Orden"].astype(str).str.strip()
    o14 = historico[HOJAS["1444"]]["Orden"].astype(str).str.strip()
    chk("duplicado 146356 intacto (2 filas)", int(o13.eq("146356").sum()) == 2,
        f"{int(o13.eq('146356').sum())}")
    chk("1444 sin filas usbank_ (Kelly NO se cobra)", int(o14.str.startswith("usbank_").sum()) == 0)
    chk("1444 sin filas capital_", int(o14.str.startswith("capital_").sum()) == 0)
    chk("4 filas applepay_ en 1444", int(o14.str.startswith("applepay_").sum()) == 4,
        f"{int(o14.str.startswith('applepay_').sum())}")
    for nom, o in (("11591", o11), ("13608", o13), ("1444", o14)):
        z = o[o.str.startswith(("amex_", "rakuten_", "robinhood_", "capital_", "usbank_", "applepay_"))]
        chk(f"0 Orden de tarjeta duplicados en {nom}", not z.duplicated().any(),
            f"{int(z.duplicated().sum())}")
    com = historico[HOJAS["1444"]]
    com_v = vivo[HOJAS["1444"]]
    mc = com["Nombre del producto"].astype(str).str.lower().str.startswith("comision de (")
    mcv = com_v["Nombre del producto"].astype(str).str.lower().str.startswith("comision de (")
    chk("comisiones de 1444 intactas (mismas filas y montos)",
        int(mc.sum()) == int(mcv.sum()) and
        abs(pd.to_numeric(com.loc[mc, "Monto"]).sum() - pd.to_numeric(com_v.loc[mcv, "Monto"]).sum()) < 0.01,
        f"{int(mc.sum())} filas · Σ {pd.to_numeric(com.loc[mc,'Monto']).sum():,.2f}")
    for hoja in vivo:
        if hoja in HOJAS.values():
            continue
        a, b = vivo[hoja], historico[hoja]
        igual = len(a) == len(b)
        if igual and "Monto" in a.columns:
            igual = abs(pd.to_numeric(a["Monto"], errors="coerce").fillna(0).sum() -
                        pd.to_numeric(b["Monto"], errors="coerce").fillna(0).sum()) < 0.01
        chk(f"verbatim: {hoja}", igual, f"{len(a)} filas")
    if not ok:
        raise SystemExit("⛔ ABORTA: falló alguna invariante.")

    # ── 9) bytes + guard A ───────────────────────────────────────────────────
    banner("9) EXCEL EN MEMORIA + 🛡️ GUARD A")
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
            print(f"  [{n}] {t[:500]}")
        raise SystemExit("⛔ GUARD A BLOQUEÓ")

    # ── 10) saldos ───────────────────────────────────────────────────────────
    banner("10) SALDOS")
    s1 = {cas: saldo(historico[h]) for cas, h in HOJAS.items()}
    for cas in ("11591", "13608", "1444"):
        print(f"  {cas:<6} COP {s0[cas]:>18,.2f} → {s1[cas]:>18,.2f}   Δ {s1[cas]-s0[cas]:>+16,.2f}"
              f"   (+{n_nuevas[cas]} filas)")

    if not ESCRIBIR:
        banner("DRY-RUN TERMINADO — 0 ESCRITURAS")
        print("  Para escribir: python3 cargar_tc_20260831.py --escribir")
        return

    # ── 11) escritura ────────────────────────────────────────────────────────
    banner("11) RE-VERIFICACIÓN + SUBIDA (capa C hace backup)")
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

    banner("12) VALIDACIÓN POST-ESCRITURA (leyendo de vuelta)")
    md2 = mod.dbx.files_get_metadata(cfg["remote_path"])
    print(f"  rev NUEVA = {md2.rev}   modificado = {md2.server_modified}   size = {md2.size:,}")
    print(f"  backup    = {backup}")
    _, r2 = mod.dbx.files_download(cfg["remote_path"])
    rel = pd.read_excel(io.BytesIO(r2.content), sheet_name=None)
    ok2 = True
    def chk2(n, c, det=""):
        nonlocal ok2
        print(f"  {'✔' if c else '🚨'} {n:<58} {det}")
        ok2 = ok2 and bool(c)
    for cas, h in HOJAS.items():
        chk2(f"saldo {cas}", abs(saldo(rel[h]) - s1[cas]) < 0.01, f"COP {saldo(rel[h]):,.2f}")
    q13 = rel[HOJAS["13608"]]["Orden"].astype(str).str.strip()
    q14 = rel[HOJAS["1444"]]["Orden"].astype(str).str.strip()
    chk2("duplicado 146356 intacto (2 filas)", int(q13.eq("146356").sum()) == 2)
    chk2("4 filas applepay_ en 1444", int(q14.str.startswith("applepay_").sum()) == 4)
    chk2("1444 sin usbank_", int(q14.str.startswith("usbank_").sum()) == 0)
    dups = []
    for hoja, b in rel.items():
        if "Orden" not in b.columns:
            continue
        z = b["Orden"].astype(str).str.strip()
        z = z[z.str.startswith(("amex_", "rakuten_", "robinhood_", "capital_", "usbank_", "applepay_"))]
        dups += list(z[z.duplicated()].unique())
    chk2("0 Orden de tarjeta duplicados", not dups, f"{len(dups)}")
    chk2(f"{len(vivo)} hojas", len(rel) == len(vivo), f"{len(rel)}")
    for hoja in vivo:
        if hoja in HOJAS.values():
            continue
        a, b = vivo[hoja], rel[hoja]
        igual = len(a) == len(b)
        if igual and "Monto" in a.columns:
            igual = abs(pd.to_numeric(a["Monto"], errors="coerce").fillna(0).sum() -
                        pd.to_numeric(b["Monto"], errors="coerce").fillna(0).sum()) < 0.01
        chk2(f"verbatim: {hoja}", igual, f"{len(a)} filas")
    print(f"\n  {'✅ CARGUE COMPLETO Y VERIFICADO' if ok2 else '🚨 REVISAR'}")
    print(f"  rev nueva: {md2.rev}\n  rollback:  {backup}")


if __name__ == "__main__":
    sys.path.insert(0, os.environ.get("HARNESS_DIR", "."))
    main()
