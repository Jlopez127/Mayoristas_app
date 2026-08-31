#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cargue de TARJETA INTUIT (6ª tarjeta) -> hoja 1444 (Maria Moises).

QUÉ HACE
--------
Corre `procesar_intuit` sobre el extracto y agrega sus filas a la hoja de 1444.
Historia COMPLETA desde el primer día: no hay extracto anterior ni entradas 'intuit' en la
lista de exclusión, así que TODO lo Settled de Maria Moises entra por primera vez.

CIFRAS DE CONTROL: se recalculan de forma INDEPENDIENTE (parseando el CSV a mano, sin usar el
módulo) y se comparan contra lo que devolvió `procesar_intuit`. Si alguna no coincide, ABORTA.

POR QUÉ NO USA EL PIPELINE DE main()
------------------------------------
`main()` deduplica ingresos con drop_duplicates(["Orden","Tipo"]) y en 13608 eso colapsaría el
duplicado preexistente `146356`. Aquí el dedup es QUIRÚRGICO: solo sobre las filas `intuit_` de
la hoja 1444. Tampoco se corre el bloque de comisión quincenal (hoy es día ≥16: recalcularía
"1-15 agosto", ya escrita, y ninguna fila de este cargue cae en esa quincena); se VERIFICA que
las 7 filas de comisión de 1444 quedan idénticas.

BLINDAJE: capa B (`preservar_filas_tarjeta`) -> guard A (`guard_frescura_historico`) ->
capa C (backup automático dentro de `upload_to_dropbox`).

  sin argumentos  -> dry-run (0 escrituras)
  --escribir      -> escribe a Dropbox
"""
import sys, os, io, warnings
warnings.filterwarnings("ignore")
import pandas as pd

ESCRIBIR = "--escribir" in sys.argv

CSV_INTUIT = "/Users/julianlopez/Downloads/transactions_1788184532.csv"
CASILLERO = "1444"

# ── §10 — cifras de control esperadas ────────────────────────────────────────
ESP_FILAS_ARCHIVO = 8
ESP_A_CARGAR = 7
ESP_DESCARTADAS = 1
ESP_SANTIAGO = 1
ESP_PENDING = 1
ESP_NO_RECONOCIDAS = 0
ESP_NETO_USD = 818.41

# Barandas de frescura (se exigen en --escribir).
REV_ESPERADA = None          # se fija tras el dry-run
SALDO_ESPERADO = None


def saldo(d):
    t = d[d["Tipo"].astype(str).str.strip().str.upper() == "TOTAL"]
    return float(pd.to_numeric(t["Monto"], errors="coerce").iloc[-1]) if len(t) else float("nan")


def usuario_de_totales(d):
    t = d[d["Tipo"].astype(str).str.strip().str.upper() == "TOTAL"]
    u = t["Usuario"].astype(str).str.strip()
    u = u[~u.str.lower().isin({"", "nan", "none"})]
    return u.mode().iloc[0] if len(u) else ""


def cas_de_totales(d, fallback):
    t = d[d["Tipo"].astype(str).str.strip().str.upper() == "TOTAL"]
    v = t["Casillero"].dropna()
    return v.iloc[-1] if len(v) else fallback


def main():
    import harness
    mod = harness.cargar_app()
    SEP = "=" * 92
    def banner(t): print(f"\n{SEP}\n{t}\n{SEP}")
    ok_global = True
    def chk(n, c, det=""):
        nonlocal ok_global
        print(f"  {'✔' if c else '🚨'} {n:<56} {det}")
        ok_global = ok_global and bool(c)

    print(f"MODO: {'🔴 ESCRITURA REAL' if ESCRIBIR else '🟢 DRY-RUN (0 escrituras)'}")
    print(f"INTUIT_FECHA_DESDE={mod.INTUIT_FECHA_DESDE} · "
          f"INTUIT_MAP_USUARIO={mod.INTUIT_MAP_USUARIO} · "
          f"IGNORAR={sorted(mod.INTUIT_USUARIOS_IGNORAR)} · "
          f"STATUS={sorted(mod.INTUIT_STATUS_VALIDOS)}")
    assert mod.INCENTIVO_AMEX_ACTIVO is False, "INCENTIVO_AMEX_ACTIVO cambió; abortar"
    assert mod.INTUIT_AFECTA_COMISION_1444 is True

    # ── 0) los 3 enganches ───────────────────────────────────────────────────
    banner("0) ENGANCHES DEL §7 (verificados sobre el módulo YA IMPORTADO)")
    chk("intuit_ en TARJETA_ORDEN_RE (capa B)", "intuit_" in mod.TARJETA_ORDEN_RE,
        mod.TARJETA_ORDEN_RE)
    import inspect
    _src_hist = inspect.getsource(mod.cargar_hist_tarjetas)
    chk("intuit_ en el índice de compras del histórico", '"intuit_"' in _src_hist)
    _src_inc = inspect.getsource(mod.agregar_incentivo_amex)
    chk('"Tarjeta Intuit" en es_tarjeta del incentivo', '"Tarjeta Intuit"' in _src_inc)

    # ── 1) vivo fresco ───────────────────────────────────────────────────────
    banner("1) HISTÓRICO VIVO FRESCO")
    cfg = mod.st.secrets["dropbox"]
    md = mod.dbx.files_get_metadata(cfg["remote_path"])
    print(f"  rev={md.rev}  modificado={md.server_modified}  size={md.size:,}")
    _, res = mod.dbx.files_download(cfg["remote_path"])
    vivo = pd.read_excel(io.BytesIO(res.content), sheet_name=None)
    HOJA = next(h for h in vivo if h.split(" - ")[0].strip() == CASILLERO)
    s0 = saldo(vivo[HOJA])
    print(f"  hoja '{HOJA}': {len(vivo[HOJA])} filas · saldo COP {s0:,.2f}")
    if ESCRIBIR:
        if REV_ESPERADA is None or SALDO_ESPERADO is None:
            raise SystemExit("⛔ ABORTA: fija REV_ESPERADA/SALDO_ESPERADO con el dry-run antes de escribir.")
        if md.rev != REV_ESPERADA or abs(s0 - SALDO_ESPERADO) > 0.01:
            raise SystemExit(f"⛔ ABORTA: el histórico se movió (rev {md.rev}, saldo {s0:,.2f}). "
                             f"Rehacer el dry-run.")
        print("  ✔ rev y saldo idénticos a los del dry-run")

    # ── 2) lista de exclusión ────────────────────────────────────────────────
    banner("2) LISTA DE EXCLUSIÓN (obligatoria)")
    cobrados, pendientes, cobrados_df = mod.cargar_tarjetas_cobradas()
    harness.clear_msgs()
    vc = cobrados_df["tarjeta"].astype(str).str.strip().str.lower().value_counts().to_dict()
    print(f"  {len(cobrados_df)} cobros · {vc}")
    chk("'cobrados' es un set (nunca None)", isinstance(cobrados, set), type(cobrados).__name__)
    n_int = sum(1 for o in cobrados if str(o).startswith("intuit_"))
    chk("0 entradas 'intuit_' en la lista (1er cargue)", n_int == 0, f"{n_int}")
    chk("la lista trae columnas de atributos (barrera 2 activa)",
        {"card_norm", "merchant_norm", "usd_abs", "fecha_attr", "signo"} <= set(cobrados_df.columns))

    hist_tarj = mod.cargar_hist_tarjetas()
    harness.clear_msgs()

    # ── 3) recuento INDEPENDIENTE del CSV (sin usar el módulo) ───────────────
    banner("3) CIFRAS DE CONTROL — RECUENTO INDEPENDIENTE DEL CSV")
    raw = pd.read_csv(CSV_INTUIT, encoding="utf-8-sig", dtype=str)
    raw.columns = [str(c).strip() for c in raw.columns]
    print(f"  columnas: {list(raw.columns)}")
    usd = (raw["Amount"].str.strip().str.replace("$", "", regex=False)
                        .str.replace(",", "", regex=False).astype(float))
    user = raw["User"].str.split().str.join(" ").str.lower()
    status = raw["Status"].str.strip()
    m_user_no = ~user.isin({"maria moises"})
    m_pend = ~m_user_no & status.str.lower().eq("pending")
    m_otro = ~m_user_no & ~status.isin({"Settled"}) & ~m_pend
    m_no_compra = ~m_user_no & status.isin({"Settled"}) & ~(usd > 0)
    m_carga = ~m_user_no & status.isin({"Settled"}) & (usd > 0)
    i_filas, i_carga = len(raw), int(m_carga.sum())
    i_santi = int((m_user_no & user.eq("santiago largo")).sum())
    i_pend_tot = int(status.str.lower().eq("pending").sum())
    i_neto = round(float(usd[m_carga].sum()), 2)
    print(f"  filas del archivo ................. {i_filas}")
    print(f"  a cargar (Settled, user mapeado) .. {i_carga}")
    print(f"  descartadas ....................... {i_filas - i_carga}")
    print(f"    · usuario no mapeado ............ {int(m_user_no.sum())} (santiago largo: {i_santi})")
    print(f"    · Pending (de user mapeado) ..... {int(m_pend.sum())}   [Pending en TODO el archivo: {i_pend_tot}]")
    print(f"    · otros Status .................. {int(m_otro.sum())}")
    print(f"    · Amount ≤ 0 (no reconocidas) ... {int(m_no_compra.sum())}")
    print(f"  NETO USD .......................... {i_neto:,.2f}")
    print(f"\n  FILAS DESCARTADAS, una por una:")
    for i, r in raw[~m_carga].iterrows():
        razones = []
        if m_user_no[i]: razones.append(f"usuario '{r['User']}' no mapeado")
        if status[i].lower() == "pending": razones.append("Status=Pending (monto no final)")
        elif not m_user_no[i] and status[i] not in {"Settled"}: razones.append(f"Status={status[i]}")
        if m_no_compra[i]: razones.append("Amount ≤ 0 (no se interpreta)")
        print(f"    {r['Date']:<14} {r['Merchant'][:26]:<26} {r['Amount']:>10}  → {' + '.join(razones)}")

    # ── 4) procesar_intuit ───────────────────────────────────────────────────
    banner("4) procesar_intuit")
    out = mod.procesar_intuit(raw.copy(), fecha_desde=mod.INTUIT_FECHA_DESDE,
                              cobrados=cobrados, pendientes=pendientes,
                              hist_tarjetas=hist_tarj, cobrados_df=cobrados_df)
    for niv, m in harness.drenar():
        print(f"  [{niv}] {m[:400]}")
    print(f"  claves devueltas: {list(out)}")
    chk("hoja destino: SOLO 1444", set(out) == {f"intuit_{CASILLERO}"}, str(set(out)))
    itu = out.get(f"intuit_{CASILLERO}", pd.DataFrame())
    if itu.empty:
        raise SystemExit("⛔ ABORTA: procesar_intuit no devolvió filas.")
    itu["USD"] = (itu["Monto"] / itu["TRM"]).round(2)
    print()
    for _, r in itu.sort_values(["Fecha", "Orden"]).iterrows():
        print(f"    {r['Fecha']}  {r['Tipo']:<7} {r['Orden']:<20} USD {r['USD']:>9,.2f}  "
              f"TRM {r['TRM']:>8,.2f}  COP {r['Monto']:>11,.0f}  {r['Nombre del producto'][:46]}")
    _por_dia = itu.groupby(["Fecha", "TRM"])["Monto"].agg(["size", "sum"])
    print(f"\n  NETO por día (TRM oficial + {mod.AMEX_TRM_SPREAD}):")
    for (f, t), r in _por_dia.iterrows():
        print(f"    {f}  TRM {t:,.2f} × {int(r['size'])} fila(s) = COP {r['sum']:,.0f}")
    neto_usd = round(float(itu.loc[itu.Tipo == "Egreso", "USD"].sum()
                           - itu.loc[itu.Tipo == "Ingreso", "USD"].sum()), 2)
    neto_cop = float(itu.loc[itu.Tipo == "Egreso", "Monto"].sum()
                     - itu.loc[itu.Tipo == "Ingreso", "Monto"].sum())
    print(f"\n  NETO: USD {neto_usd:,.2f} · COP {neto_cop:,.0f}")

    # ── 5) contraste con §10 ─────────────────────────────────────────────────
    banner("5) CONTRASTE CON LAS CIFRAS DE CONTROL DEL §10")
    chk(f"filas del archivo = {ESP_FILAS_ARCHIVO}", i_filas == ESP_FILAS_ARCHIVO, f"{i_filas}")
    chk(f"filas a cargar = {ESP_A_CARGAR}", len(itu) == ESP_A_CARGAR == i_carga,
        f"módulo {len(itu)} · recuento independiente {i_carga}")
    chk(f"descartadas = {ESP_DESCARTADAS}", i_filas - i_carga == ESP_DESCARTADAS, f"{i_filas - i_carga}")
    chk(f"santiago largo = {ESP_SANTIAGO}", i_santi == ESP_SANTIAGO, f"{i_santi}")
    chk(f"Pending omitidas = {ESP_PENDING}", i_pend_tot == ESP_PENDING, f"{i_pend_tot}")
    chk(f"excluidas por no reconocidas = {ESP_NO_RECONOCIDAS}",
        int(m_no_compra.sum()) + int(m_otro.sum()) == ESP_NO_RECONOCIDAS,
        f"{int(m_no_compra.sum()) + int(m_otro.sum())}")
    chk(f"neto = USD {ESP_NETO_USD:,.2f}", abs(neto_usd - ESP_NETO_USD) < 0.005 and
        abs(i_neto - ESP_NETO_USD) < 0.005, f"módulo {neto_usd:,.2f} · independiente {i_neto:,.2f}")
    chk("todas Egreso", set(itu["Tipo"]) == {"Egreso"}, str(set(itu["Tipo"])))
    chk("todas de 1444", set(itu["Casillero"].astype(str)) == {CASILLERO},
        str(set(itu["Casillero"].astype(str))))
    chk(f"Orden únicos con prefijo intuit_ = {ESP_A_CARGAR}",
        itu["Orden"].nunique() == ESP_A_CARGAR and itu["Orden"].str.startswith("intuit_").all(),
        f"{itu['Orden'].nunique()}")
    chk("0 colisiones de hash", not itu["Orden"].duplicated().any(),
        f"{int(itu['Orden'].duplicated().sum())}")

    # Idempotencia: reprocesar da EXACTAMENTE lo mismo (hash estable).
    out2 = mod.procesar_intuit(raw.copy(), fecha_desde=mod.INTUIT_FECHA_DESDE,
                               cobrados=cobrados, pendientes=pendientes,
                               hist_tarjetas=hist_tarj, cobrados_df=cobrados_df)
    harness.clear_msgs()
    i2 = out2[f"intuit_{CASILLERO}"]
    chk("idempotente: 2ª pasada idéntica (Orden/Monto/TRM)",
        itu.drop(columns=["USD"]).equals(i2), "")
    # Con los 7 Orden ya en la lista, la 2ª corrida no debe cargar NADA.
    out3 = mod.procesar_intuit(raw.copy(), fecha_desde=mod.INTUIT_FECHA_DESDE,
                               cobrados=cobrados | set(itu["Orden"]), pendientes=pendientes,
                               hist_tarjetas=hist_tarj, cobrados_df=cobrados_df)
    harness.clear_msgs()
    chk("con los 7 Orden en la lista -> 0 filas (barrera 1 funciona)", out3 == {}, str(list(out3)))

    if not ok_global:
        raise SystemExit("⛔ ABORTA: alguna cifra de control no cuadra.")

    # ── 6) aplicar a la hoja 1444 ────────────────────────────────────────────
    banner("6) APLICAR A LA HOJA 1444 (dedup QUIRÚRGICO, solo intuit_)")
    historico = {k: mod.asegurar_columnas_historico(v.copy()) for k, v in vivo.items()}
    fecha_carga = pd.Timestamp.today().strftime("%Y-%m-%d")
    d = historico[HOJA]
    antes = len(d)
    nuevas = itu.drop(columns=["USD"]).copy()
    nuevas["Fecha de Carga"] = fecha_carga
    d = pd.concat([d, mod.asegurar_columnas_historico(nuevas)], ignore_index=True)
    o = d["Orden"].astype(str).str.strip()
    m = o.str.startswith("intuit_")
    d = pd.concat([d[~m], d[m].drop_duplicates(subset=["Orden"], keep="last")], ignore_index=True)
    u, c = usuario_de_totales(historico[HOJA]), cas_de_totales(historico[HOJA], CASILLERO)
    historico[HOJA] = mod.recalcular_totales_diarios(d, usuario=u, cas=c)
    print(f"  filas {antes} → {len(historico[HOJA])} · recalculado con usuario='{u}' casillero={c!r}")

    # ── 7) capa B ────────────────────────────────────────────────────────────
    banner("7) 🛡️ CAPA B — preservar_filas_tarjeta")
    historico = mod.preservar_filas_tarjeta(historico, vivo=vivo)
    for niv, m_ in harness.drenar():
        print(f"  [{niv}] {m_[:300]}")
    print("  ✔ ejecutada")

    # ── 8) diff + invariantes ────────────────────────────────────────────────
    banner("8) DIFF E INVARIANTES")
    for hoja in vivo:
        a, b = vivo[hoja], historico[hoja]
        ig = len(a) == len(b) and (("Orden" not in a.columns) or
                                   set(a["Orden"].astype(str)) == set(b["Orden"].astype(str)))
        if ig and hoja != HOJA:
            print(f"  {hoja:<34} sin cambios ({len(a)}) ✔ verbatim")
        else:
            print(f"  {hoja:<34} {len(a)} → {len(b)} | saldo {saldo(a):,.2f} → {saldo(b):,.2f} "
                  f"({saldo(b)-saldo(a):+,.2f})")
    ok_global = True
    for hoja in vivo:
        if hoja == HOJA:
            continue
        oi = historico[hoja]["Orden"].astype(str).str.strip() if "Orden" in historico[hoja].columns else pd.Series(dtype=str)
        chk(f"0 filas intuit_ fuera de 1444: {hoja[:26]}", int(oi.str.startswith("intuit_").sum()) == 0)
    h4 = historico[HOJA]
    o4 = h4["Orden"].astype(str).str.strip()
    chk(f"{ESP_A_CARGAR} filas intuit_ en 1444", int(o4.str.startswith("intuit_").sum()) == ESP_A_CARGAR,
        f"{int(o4.str.startswith('intuit_').sum())}")
    z = o4[o4.str.startswith(("amex_", "rakuten_", "robinhood_", "capital_", "usbank_",
                              "intuit_", "applepay_"))]
    chk("0 Orden de tarjeta duplicados en 1444", not z.duplicated().any(),
        f"{int(z.duplicated().sum())}")
    mc = h4["Nombre del producto"].astype(str).str.lower().str.startswith("comision de (")
    mcv = vivo[HOJA]["Nombre del producto"].astype(str).str.lower().str.startswith("comision de (")
    chk("comisiones de 1444 intactas (mismas filas y montos)",
        int(mc.sum()) == int(mcv.sum()) and
        abs(pd.to_numeric(h4.loc[mc, "Monto"]).sum() - pd.to_numeric(vivo[HOJA].loc[mcv, "Monto"]).sum()) < 0.01,
        f"{int(mc.sum())} filas · Σ {pd.to_numeric(h4.loc[mc,'Monto']).sum():,.2f}")
    # Se usa el MISMO criterio del guard A (_ordenes_significativas): los TOTAL llevan Orden
    # vacío y se reescriben en cada recálculo — compararlos como texto daría un falso positivo
    # ('nan' del vivo vs '' del recalculado) sin que se haya perdido ninguna fila.
    _prev = mod._ordenes_significativas(vivo[HOJA])
    _post = mod._ordenes_significativas(h4)
    chk("0 Orden previos perdidos en 1444", not (_prev - _post), f"{len(_prev - _post)}")
    chk("filas TOTAL: mismas o más", 
        int((h4["Tipo"].astype(str).str.upper() == "TOTAL").sum()) >=
        int((vivo[HOJA]["Tipo"].astype(str).str.upper() == "TOTAL").sum()),
        f"{int((vivo[HOJA]['Tipo'].astype(str).str.upper()=='TOTAL').sum())} → "
        f"{int((h4['Tipo'].astype(str).str.upper()=='TOTAL').sum())}")
    for hoja in vivo:
        if hoja == HOJA:
            continue
        a, b = vivo[hoja], historico[hoja]
        igual = len(a) == len(b)
        if igual and "Monto" in a.columns:
            igual = abs(pd.to_numeric(a["Monto"], errors="coerce").fillna(0).sum() -
                        pd.to_numeric(b["Monto"], errors="coerce").fillna(0).sum()) < 0.01
        chk(f"verbatim: {hoja}", igual, f"{len(a)} filas")
    if not ok_global:
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

    # ── 10) saldo y comisión ─────────────────────────────────────────────────
    banner("10) SALDO 1444 Y COMISIÓN")
    s1 = saldo(historico[HOJA])
    print(f"  antes            COP {s0:>18,.2f}")
    print(f"  efecto Intuit    COP {-neto_cop:>18,.2f}")
    print(f"  después          COP {s1:>18,.2f}   Δ {s1-s0:>+16,.2f}")
    _tot = historico[HOJA][historico[HOJA]["Tipo"].astype(str).str.upper() == "TOTAL"].copy()
    _tot["_f"] = pd.to_datetime(_tot["Fecha"], errors="coerce")
    _q = _tot[(_tot["_f"] >= "2026-08-16") & (_tot["_f"] <= "2026-08-31")]
    _m = pd.to_numeric(_q["Monto"], errors="coerce")
    print(f"  quincena 16-31 ago: Total diario más negativo = "
          f"{_m.min():,.2f} ({'sin día negativo → 0 comisión' if _m.min() >= 0 else 'GENERARÍA comisión'})")
    print(f"  (esa quincena se calcula en la ventana 1-15 septiembre, no hoy)")

    if not ESCRIBIR:
        banner("DRY-RUN TERMINADO — 0 ESCRITURAS")
        print(f"  Para escribir, fija en el script:")
        print(f"    REV_ESPERADA   = \"{md.rev}\"")
        print(f"    SALDO_ESPERADO = {s0}")
        print(f"  y corre: python3 cargar_intuit_20260831.py --escribir")
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
    ok_global = True
    rr = rel[HOJA]; oo = rr["Orden"].astype(str).str.strip()
    chk(f"{ESP_A_CARGAR} filas intuit_ en 1444", int(oo.str.startswith("intuit_").sum()) == ESP_A_CARGAR)
    chk("0 duplicados intuit_", not oo[oo.str.startswith("intuit_")].duplicated().any())
    chk("0 Orden previos perdidos", not (_prev - mod._ordenes_significativas(rr)),
        f"{len(_prev - mod._ordenes_significativas(rr))}")
    chk("saldo 1444", abs(saldo(rr) - s1) < 0.01, f"COP {saldo(rr):,.2f}")
    for hoja in vivo:
        if hoja == HOJA:
            continue
        b = rel[hoja]
        oi = b["Orden"].astype(str).str.strip() if "Orden" in b.columns else pd.Series(dtype=str)
        chk(f"0 intuit_ en {hoja[:30]}", int(oi.str.startswith("intuit_").sum()) == 0)
        a = vivo[hoja]
        igual = len(a) == len(b)
        if igual and "Monto" in a.columns:
            igual = abs(pd.to_numeric(a["Monto"], errors="coerce").fillna(0).sum() -
                        pd.to_numeric(b["Monto"], errors="coerce").fillna(0).sum()) < 0.01
        chk(f"verbatim: {hoja[:30]}", igual, f"{len(a)} filas")
    chk(f"{len(vivo)} hojas", len(rel) == len(vivo), f"{len(rel)}")
    print(f"\n  {'✅ CARGUE COMPLETO Y VERIFICADO' if ok_global else '🚨 REVISAR'}")
    print(f"  rev nueva: {md2.rev}\n  rollback:  {backup}")


if __name__ == "__main__":
    sys.path.insert(0, os.environ.get("HARNESS_DIR", "."))
    main()
