#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cargue Intuit del 2026-09-02 -> hoja 1444, + reajuste del incentivo de agosto.

DE DÓNDE SALE EL EXTRACTO
-------------------------
Intuit descarga de a 7 movimientos, así que el usuario hizo 13 descargas parciales. El archivo
`intuit_acumulado_2026-09-02.csv` es EXACTAMENTE la unión de esas 13 (80 movimientos únicos por
Merchant|Amount|Date|Status|User; 0 filas de diferencia en ambos sentidos). Se verifica aquí.

QUÉ ENTRA (decidido con el usuario el 2026-09-02)
-------------------------------------------------
- Maria Moises -> 1444. Con el corte movido al 26-ago entran 16 movimientos Settled:
  los 4 del 29-ago y los 12 del 26-28 que el corte anterior descartaba.
- 'santiago largo' -> NO ENTRA. Su gasto no es de 1444 (mismo criterio que en Robinhood).
  Son 42 Settled por USD 21.020,75 que quedan fuera A PROPÓSITO.
- 'Elvis Martinez' -> 11591 (Paula Herrera). Hoy su única fila es Pending por USD 0,02, así
  que no carga nada; queda mapeado para cuando liquide algo real.
- Las Pending nunca se cargan (regla de Intuit: su Orden es hash del monto).

EL INCENTIVO DE AGOSTO HAY QUE REAJUSTARLO A MANO
-------------------------------------------------
`incentivoamex_1444_2026-08` se creó el 1-sep por COP 1.011.464 y el código lo deja CONGELADO
(no lo recalcula aunque lleguen movimientos tarde). Estas 16 compras son de AGOSTO, así que su
base cambió. Se recalcula con la MISMA regla del módulo (25 COP × USD neto del mes) y se
reescribe el Monto de esa fila. Antes de tocarla se COMPRUEBA que la fórmula reproduce el
1.011.464 actual: si no lo reproduce, aborta.

LA COMISIÓN NO SE TOCA
----------------------
La quincena 1-15 ago está escrita y hoy (día 2) no está en la ventana. La de 16-31 ago no
existe porque 1444 no tuvo NINGÚN día negativo, y con estas 16 filas sigue sin tenerlo
(verificado). Se comprueba que ambas cosas siguen igual al terminar.

  sin argumentos  -> dry-run (0 escrituras)
  --escribir      -> escribe a Dropbox
"""
import sys, os, io, csv, glob, time, warnings
warnings.filterwarnings("ignore")
import pandas as pd

ESCRIBIR = "--escribir" in sys.argv

CSV_ACUM = "/Users/julianlopez/Downloads/intuit_acumulado_2026-09-02.csv"
PARCIALES = "/Users/julianlopez/Downloads/transactions_*.csv"
CASILLERO = "1444"

REV_ESPERADA = "0165a7f6e16832300000002f34b3f21"
SALDO_ESPERADO = 66015038.41
INTUIT_PREVIAS = 7
INCENTIVO_ORDEN = "incentivoamex_1444_2026-08"
INCENTIVO_ACTUAL = 1011464.0
ESP_NUEVAS = 16
ESP_USD_NUEVO = 11371.61
ESP_SANTIAGO_FUERA = 42


def saldo(d):
    t = d[d["Tipo"].astype(str).str.strip().str.upper() == "TOTAL"]
    return float(pd.to_numeric(t["Monto"], errors="coerce").iloc[-1]) if len(t) else float("nan")


def usuario_de_totales(d):
    t = d[d["Tipo"].astype(str).str.strip().str.upper() == "TOTAL"]
    u = t["Usuario"].astype(str).str.strip()
    u = u[~u.str.lower().isin({"", "nan", "none"})]
    return u.mode().iloc[0] if len(u) else ""


def cas_de_totales(d, fb):
    t = d[d["Tipo"].astype(str).str.strip().str.upper() == "TOTAL"]
    v = t["Casillero"].dropna()
    return v.iloc[-1] if len(v) else fb


def usd_neto_mes(d, ini, fin):
    """USD neto de tarjeta del mes, con la MISMA regla de agregar_incentivo_amex:
    USD_fila = Monto/TRM, egresos menos ingresos, filas capturadas por Motivo 'Tarjeta *'."""
    x = d.copy()
    x["_f"] = pd.to_datetime(x["Fecha"], errors="coerce")
    x = x[(x["_f"] >= pd.Timestamp(ini)) & (x["_f"] <= pd.Timestamp(fin))]
    x = x[x["Motivo"].astype(str).str.strip().str.startswith("Tarjeta ")]
    x["_usd"] = pd.to_numeric(x["Monto"], errors="coerce") / pd.to_numeric(x["TRM"], errors="coerce")
    eg = x.loc[x["Tipo"].astype(str).str.strip() == "Egreso", "_usd"].sum()
    ing = x.loc[x["Tipo"].astype(str).str.strip() == "Ingreso", "_usd"].sum()
    return float(eg - ing)


def main():
    import harness
    mod = harness.cargar_app()
    SEP = "=" * 94
    def banner(t): print(f"\n{SEP}\n{t}\n{SEP}")
    ok = True
    def chk(n, c, det=""):
        nonlocal ok
        print(f"  {'✔' if c else '🚨'} {n:<62} {det}")
        ok = ok and bool(c)

    # datos.gov.co está intermitente estos días: reintentos. Si aun así falta un día, el módulo
    # aborta solo (no hay TRM de respaldo) — fail-safe, nunca inventa una TRM.
    _o = mod._amex_trm_dia
    def _trm(f, c=None, *a, **k):
        for i in range(6):
            v = _o(f, c if c is not None else {}, *a, **k)
            if v is not None:
                return v
            time.sleep(1.5)
        return None
    mod._amex_trm_dia = _trm

    print(f"MODO: {'🔴 ESCRITURA REAL' if ESCRIBIR else '🟢 DRY-RUN (0 escrituras)'}")
    print(f"INTUIT_FECHA_DESDE={mod.INTUIT_FECHA_DESDE} · MAP={mod.INTUIT_MAP_USUARIO} · "
          f"IGNORAR={sorted(mod.INTUIT_USUARIOS_IGNORAR)}")
    assert mod.INTUIT_MAP_USUARIO == {"maria moises": "1444", "elvis martinez": "11591"}
    assert "santiago largo" in mod.INTUIT_USUARIOS_IGNORAR, "Santiago dejó de estar ignorado"
    assert mod.INTUIT_FECHA_DESDE == "2026-08-26"

    # ── 1) el acumulado ES la unión de los parciales ──────────────────────────
    banner("1) EL ACUMULADO ES LA UNIÓN DE LAS 13 DESCARGAS")
    def leer(p):
        with open(p, encoding="utf-8-sig", newline="") as f:
            return list(csv.DictReader(f))
    K = lambda r: (r["Merchant"], r["Amount"], r["Date"], r["Status"], r["User"])
    union = {K(r) for p in sorted(glob.glob(PARCIALES)) for r in leer(p)}
    acum = {K(r) for r in leer(CSV_ACUM)}
    print(f"  {len(sorted(glob.glob(PARCIALES)))} parciales → {len(union)} únicos · acumulado {len(acum)}")
    chk("el acumulado no pierde nada de los parciales", not (union - acum), f"{len(union - acum)}")
    chk("el acumulado no inventa nada", not (acum - union), f"{len(acum - union)}")

    # ── 2) vivo fresco ────────────────────────────────────────────────────────
    banner("2) HISTÓRICO VIVO FRESCO")
    cfg = mod.st.secrets["dropbox"]
    md = mod.dbx.files_get_metadata(cfg["remote_path"])
    _, res = mod.dbx.files_download(cfg["remote_path"])
    vivo = pd.read_excel(io.BytesIO(res.content), sheet_name=None)
    HOJA = next(h for h in vivo if h.split(" - ")[0].strip() == CASILLERO)
    s0 = saldo(vivo[HOJA])
    o0 = vivo[HOJA]["Orden"].astype(str).str.strip()
    print(f"  rev={md.rev}  modificado={md.server_modified}  size={md.size:,}")
    print(f"  '{HOJA}': {len(vivo[HOJA])} filas · saldo COP {s0:,.2f} · intuit_={int(o0.str.startswith('intuit_').sum())}")
    if md.rev != REV_ESPERADA or abs(s0 - SALDO_ESPERADO) > 0.01:
        print(f"  🚨 EL HISTÓRICO SE MOVIÓ (esperaba {REV_ESPERADA})")
        if ESCRIBIR:
            raise SystemExit("⛔ ABORTA: rehacer el dry-run con el histórico fresco.")
    else:
        print("  ✔ rev y saldo idénticos a los esperados")
    chk(f"{INTUIT_PREVIAS} filas intuit_ previas",
        int(o0.str.startswith("intuit_").sum()) == INTUIT_PREVIAS)

    # ── 3) lista + procesar ───────────────────────────────────────────────────
    banner("3) PROCESAR EL EXTRACTO")
    cobrados, pendientes, cobrados_df = mod.cargar_tarjetas_cobradas()
    harness.clear_msgs()
    hist_tarj = mod.cargar_hist_tarjetas()
    harness.clear_msgs()
    raw = pd.read_csv(CSV_ACUM, encoding="utf-8-sig")
    out = mod.procesar_intuit(raw.copy(), fecha_desde=mod.INTUIT_FECHA_DESDE, cobrados=cobrados,
                              pendientes=pendientes, hist_tarjetas=hist_tarj, cobrados_df=cobrados_df)
    for niv, m in harness.drenar():
        print(f"  [{niv}] {m[:300]}")
    print(f"  claves devueltas: {list(out)}")
    chk("solo devuelve 1444 (Elvis no tiene Settled)", set(out) == {"intuit_1444"}, str(set(out)))
    nuevas = out.get("intuit_1444", pd.DataFrame())

    # reconciliación independiente, parseando el CSV a mano
    r = raw.copy()
    r["_f"] = pd.to_datetime(r["Date"], format="%b %d, %Y", errors="coerce")
    r["_u"] = r["User"].astype(str).str.strip().str.lower()
    r["_usd"] = pd.to_numeric(r["Amount"].astype(str).str.replace(r"[$,]", "", regex=True), errors="coerce")
    print(f"\n  RECONCILIACIÓN")
    print(f"    {len(r):>3} movimientos en el acumulado")
    sant = r[r["_u"] == "santiago largo"]
    print(f"    −{len(sant):>2} de 'santiago largo' (NO son de 1444) — {int((sant['Status']=='Settled').sum())} Settled, USD {sant.loc[sant['Status']=='Settled','_usd'].sum():,.2f}")
    chk(f"quedan fuera las {ESP_SANTIAGO_FUERA} Settled de Santiago",
        int((sant["Status"] == "Settled").sum()) == ESP_SANTIAGO_FUERA)
    elv = r[r["_u"].str.contains("elvis")]
    print(f"    −{len(elv):>2} de 'Elvis Martinez' -> 11591 ({int((elv['Status']=='Settled').sum())} Settled)")
    m = r[r["_u"] == "maria moises"]
    print(f"    ={len(m):>2} de Maria: {int((m['Status']=='Settled').sum())} Settled + {int((m['Status']=='Pending').sum())} Pending")
    ms = m[m["Status"] == "Settled"]
    ant = ms[ms["_f"] < pd.Timestamp(mod.INTUIT_FECHA_DESDE)]
    print(f"       −{len(ant)} anteriores al corte {mod.INTUIT_FECHA_DESDE}")
    cand = ms[ms["_f"] >= pd.Timestamp(mod.INTUIT_FECHA_DESDE)]
    print(f"       −{len(cand) - len(nuevas)} ya liquidadas (lista de exclusión)")
    print(f"       ={len(nuevas)} ENTRAN")
    chk(f"entran {ESP_NUEVAS}", len(nuevas) == ESP_NUEVAS, f"{len(nuevas)}")
    if nuevas.empty:
        raise SystemExit("⛔ ABORTA: 0 filas nuevas.")
    nv = nuevas.copy()
    nv["_usd"] = pd.to_numeric(nv["Monto"]) / pd.to_numeric(nv["TRM"])
    chk(f"suman USD {ESP_USD_NUEVO:,.2f}", abs(nv["_usd"].sum() - ESP_USD_NUEVO) < 0.02,
        f"{nv['_usd'].sum():,.2f}")
    chk("todas Egreso / Tarjeta Intuit / casillero 1444",
        set(nv["Tipo"]) == {"Egreso"} and set(nv["Motivo"]) == {"Tarjeta Intuit"}
        and set(nv["Casillero"].astype(str)) == {"1444"})
    f = pd.to_datetime(nv["Fecha"])
    print(f"\n  {len(nv)} filas · USD {nv['_usd'].sum():,.2f} · COP {pd.to_numeric(nv['Monto']).sum():,.0f} · {f.min().date()} → {f.max().date()}")
    for _, x in nv.sort_values("Fecha").iterrows():
        print(f"    {x['Fecha']}  USD {x['_usd']:>9,.2f}  TRM {float(x['TRM']):>8,.2f}  "
              f"COP {x['Monto']:>11,.0f}  {str(x['Nombre del producto'])[:52]}")

    # prueba del extracto corto sobre las 7 ya cargadas
    banner("4) 🧪 PRUEBA DEL EXTRACTO CORTO (las 7 ya cargadas)")
    todo = mod.procesar_intuit(raw.copy(), fecha_desde=mod.INTUIT_FECHA_DESDE, cobrados=set(),
                               pendientes=None, hist_tarjetas=hist_tarj, cobrados_df=None)
    harness.clear_msgs()
    t14 = todo.get("intuit_1444", pd.DataFrame()).copy()
    vi = vivo[HOJA].copy()
    vi["Orden"] = vi["Orden"].astype(str).str.strip()
    vi = vi[vi["Orden"].str.startswith("intuit_")]
    t14["Orden"] = t14["Orden"].astype(str).str.strip()
    j = vi.merge(t14, on="Orden", suffixes=("_h", "_n"))
    dM = (pd.to_numeric(j["Monto_n"], errors="coerce") - pd.to_numeric(j["Monto_h"], errors="coerce")).abs()
    dT = (pd.to_numeric(j["TRM_n"], errors="coerce") - pd.to_numeric(j["TRM_h"], errors="coerce")).abs()
    chk("las 7 ya cargadas se reproducen igual", len(j) == INTUIT_PREVIAS, f"{len(j)}")
    chk("0 diferencias en Monto", int((dM > 0.5).sum()) == 0)
    chk("0 diferencias en TRM", int((dT > 0.005).sum()) == 0)
    if not ok:
        raise SystemExit("⛔ ABORTA: falló alguna verificación.")

    # ── 5) aplicar ────────────────────────────────────────────────────────────
    banner("5) APLICAR A 1444 (dedup quirúrgico por prefijo)")
    fecha_carga = pd.Timestamp.today().strftime("%Y-%m-%d")
    historico = {k: mod.asegurar_columnas_historico(v.copy()) for k, v in vivo.items()}
    d = historico[HOJA]
    antes = len(d)
    x = nuevas.copy()
    x["Fecha de Carga"] = fecha_carga
    d = pd.concat([d, mod.asegurar_columnas_historico(x)], ignore_index=True)
    o = d["Orden"].astype(str).str.strip()
    mm = o.str.startswith("intuit_")
    d = pd.concat([d[~mm], d[mm].drop_duplicates(subset=["Orden"], keep="last")], ignore_index=True)
    print(f"  intuit_ {INTUIT_PREVIAS} → {int(d['Orden'].astype(str).str.startswith('intuit_').sum())}")
    u, c = usuario_de_totales(historico[HOJA]), cas_de_totales(historico[HOJA], CASILLERO)
    d = mod.recalcular_totales_diarios(d, usuario=u, cas=c)

    # ── 6) reajustar el incentivo de agosto ───────────────────────────────────
    banner("6) 💰 REAJUSTE DEL INCENTIVO DE AGOSTO (la fila está congelada)")
    base_antes = usd_neto_mes(vivo[HOJA], "2026-08-01", "2026-08-31")
    calc_antes = round(base_antes * mod.INCENTIVO_COP_POR_USD)
    print(f"  base ANTES: USD {base_antes:,.2f} × {mod.INCENTIVO_COP_POR_USD} = COP {calc_antes:,.0f}")
    print(f"  fila escrita el 1-sep:                     COP {INCENTIVO_ACTUAL:,.0f}")
    chk("la fórmula reproduce el incentivo ya escrito", abs(calc_antes - INCENTIVO_ACTUAL) < 2,
        "si no, NO se toca la fila")
    if not ok:
        raise SystemExit("⛔ ABORTA: no puedo reproducir el incentivo actual; no toco la fila.")
    base_desp = usd_neto_mes(d, "2026-08-01", "2026-08-31")
    calc_desp = round(base_desp * mod.INCENTIVO_COP_POR_USD)
    print(f"  base DESPUÉS: USD {base_desp:,.2f} × {mod.INCENTIVO_COP_POR_USD} = COP {calc_desp:,.0f}")
    print(f"  ajuste: COP {INCENTIVO_ACTUAL:,.0f} → {calc_desp:,.0f}  (Δ +{calc_desp - INCENTIVO_ACTUAL:,.0f})")
    chk("el delta = 25 × USD nuevo", abs((calc_desp - calc_antes) - ESP_USD_NUEVO * 25) < 3,
        f"{calc_desp - calc_antes:,.0f} vs {ESP_USD_NUEVO*25:,.0f}")
    minc = d["Orden"].astype(str).str.strip() == INCENTIVO_ORDEN
    chk("existe 1 sola fila de incentivo de agosto", int(minc.sum()) == 1, f"{int(minc.sum())}")
    d.loc[minc, "Monto"] = float(calc_desp)
    d.loc[minc, "Fecha de Carga"] = fecha_carga
    d = mod.recalcular_totales_diarios(d, usuario=u, cas=c)
    historico[HOJA] = d
    n_nuevas = len(d) - antes

    # ── 7) la comisión no se mueve ────────────────────────────────────────────
    banner("7) LA COMISIÓN DE 1444 NO SE TOCA")
    a = vivo[HOJA]["Nombre del producto"].astype(str).str.lower().str.startswith("comision de (")
    b = d["Nombre del producto"].astype(str).str.lower().str.startswith("comision de (")
    chk("mismas filas de comisión y mismos montos",
        int(a.sum()) == int(b.sum()) and
        abs(pd.to_numeric(vivo[HOJA].loc[a, "Monto"]).sum() - pd.to_numeric(d.loc[b, "Monto"]).sum()) < 0.01,
        f"{int(b.sum())} filas · Σ {pd.to_numeric(d.loc[b,'Monto']).sum():,.2f}")
    d2 = d.copy(); d2["_f"] = pd.to_datetime(d2["Fecha"], errors="coerce")
    tt = d2[d2["Tipo"].astype(str).str.strip().str.upper() == "TOTAL"]
    w = pd.to_numeric(tt[(tt["_f"] >= pd.Timestamp("2026-08-16")) & (tt["_f"] <= pd.Timestamp("2026-08-31"))]["Monto"], errors="coerce")
    chk("16-31 ago sigue sin ningún día negativo -> sin comisión nueva",
        int((w < 0).sum()) == 0, f"mínimo COP {w.min():,.0f}")

    # ── 8) capa B + invariantes + guard A ─────────────────────────────────────
    banner("8) 🛡️ CAPA B + INVARIANTES + GUARD A")
    historico = mod.preservar_filas_tarjeta(historico, vivo=vivo)
    harness.drenar()
    o1 = historico[HOJA]["Orden"].astype(str).str.strip()
    chk("intuit_ = 7 + 16", int(o1.str.startswith("intuit_").sum()) == INTUIT_PREVIAS + ESP_NUEVAS,
        f"{int(o1.str.startswith('intuit_').sum())}")
    for p, n in (("robinhood_", 249), ("rakuten_", 73), ("amex_", 151), ("applepay_", 4)):
        chk(f"{p} intactas", int(o1.str.startswith(p).sum()) == n, f"{int(o1.str.startswith(p).sum())}")
    vac = {"", "nan", "none", "nat"}
    perd = {y for y in o0 if y.lower() not in vac} - {y for y in o1 if y.lower() not in vac}
    chk("0 Orden previos perdidos", not perd, f"{len(perd)}")
    z = o1[o1.str.startswith(("amex_", "rakuten_", "robinhood_", "intuit_", "applepay_"))]
    chk("0 Orden de tarjeta duplicados", not z.duplicated().any())
    for hoja in vivo:
        if hoja == HOJA:
            continue
        A, B = vivo[hoja], historico[hoja]
        igual = len(A) == len(B)
        if igual and "Monto" in A.columns:
            igual = abs(pd.to_numeric(A["Monto"], errors="coerce").fillna(0).sum() -
                        pd.to_numeric(B["Monto"], errors="coerce").fillna(0).sum()) < 0.01
        chk(f"verbatim: {hoja}", igual, f"{len(A)} filas")
    if not ok:
        raise SystemExit("⛔ ABORTA: falló alguna invariante.")
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w2:
        for hh, dfh in historico.items():
            w2.book.create_sheet(hh[:31])
            dfh.to_excel(w2, sheet_name=hh[:31], index=False)
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

    banner("9) SALDO")
    s1 = saldo(historico[HOJA])
    print(f"  1444: COP {s0:>16,.2f} → {s1:>16,.2f}   Δ {s1-s0:>+14,.2f}   (+{n_nuevas} filas)")
    print(f"     compras Intuit  −{pd.to_numeric(nv['Monto']).sum():,.0f}")
    print(f"     incentivo       +{calc_desp - INCENTIVO_ACTUAL:,.0f}")

    if not ESCRIBIR:
        banner("DRY-RUN TERMINADO — 0 ESCRITURAS")
        print("  Para escribir: python3 cargar_intuit_20260902.py --escribir")
        return

    banner("10) SUBIDA (capa C hace el respaldo)")
    md_pre = mod.dbx.files_get_metadata(cfg["remote_path"])
    if md_pre.rev != REV_ESPERADA:
        raise SystemExit(f"⛔ ABORTA SIN ESCRIBIR: el histórico se movió (rev {md_pre.rev}).")
    harness.clear_msgs()
    mod.upload_to_dropbox(data_bytes)
    backup = None
    for n, t in harness.MENSAJES:
        print(f"  [{n}] {t}")
        if "Respaldo previo creado" in t and "`" in t:
            backup = t.split("`")[1]
    harness.clear_msgs()

    banner("11) VALIDACIÓN POST-ESCRITURA")
    md2 = mod.dbx.files_get_metadata(cfg["remote_path"])
    print(f"  rev NUEVA = {md2.rev}  size = {md2.size:,}")
    print(f"  backup    = {backup}")
    _, r2 = mod.dbx.files_download(cfg["remote_path"])
    rel = pd.read_excel(io.BytesIO(r2.content), sheet_name=None)
    ok2 = True
    def chk2(n, c, det=""):
        nonlocal ok2
        print(f"  {'✔' if c else '🚨'} {n:<62} {det}")
        ok2 = ok2 and bool(c)
    q = rel[HOJA]["Orden"].astype(str).str.strip()
    chk2("saldo 1444", abs(saldo(rel[HOJA]) - s1) < 0.01, f"COP {saldo(rel[HOJA]):,.2f}")
    chk2("intuit_ = 23", int(q.str.startswith("intuit_").sum()) == 23,
         f"{int(q.str.startswith('intuit_').sum())}")
    inc = rel[HOJA][q == INCENTIVO_ORDEN]
    chk2("incentivo de agosto reajustado",
         abs(float(pd.to_numeric(inc["Monto"]).iloc[0]) - calc_desp) < 1,
         f"COP {float(pd.to_numeric(inc['Monto']).iloc[0]):,.0f}")
    chk2("0 Orden previos perdidos",
         not ({y for y in o0 if y.lower() not in vac} - {y for y in q if y.lower() not in vac}))
    for hoja in vivo:
        if hoja == HOJA:
            continue
        A, B = vivo[hoja], rel[hoja]
        igual = len(A) == len(B)
        if igual and "Monto" in A.columns:
            igual = abs(pd.to_numeric(A["Monto"], errors="coerce").fillna(0).sum() -
                        pd.to_numeric(B["Monto"], errors="coerce").fillna(0).sum()) < 0.01
        chk2(f"verbatim: {hoja}", igual, f"{len(A)} filas")
    print(f"\n  {'✅ CARGUE COMPLETO Y VERIFICADO' if ok2 else '🚨 REVISAR'}")
    print(f"  rev nueva: {md2.rev}\n  rollback:  {backup}")


if __name__ == "__main__":
    sys.path.insert(0, os.environ.get("HARNESS_DIR", "."))
    main()
