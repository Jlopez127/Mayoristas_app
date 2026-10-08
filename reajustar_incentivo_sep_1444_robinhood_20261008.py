#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SEGUNDO reajuste del incentivo de SEPTIEMBRE 2026, solo 1444, el mismo 8-oct-2026.

reajustar_incentivo_sep_20261008.py lo dejó en 1.267.930, pero faltaba cargar Robinhood
(cargar_robinhood_20261008.py, 32 filas de Correal 25-sep -> 6-oct) y se cargó DESPUÉS.
Mismo patrón: lo cargado antes de hoy (Fecha de Carga < 8-oct) debe reproducir 1.267.930;
Δ = 25 × USD de septiembre cargado hoy (las filas de Robinhood de septiembre).

  sin argumentos  -> dry-run (0 escrituras)
  --escribir REV  -> escribe a Dropbox (REV = rev del dry-run) y refresca la copia de OneDrive
"""
import sys, os, io, shutil, warnings
warnings.filterwarnings("ignore")
import pandas as pd

ESCRIBIR = "--escribir" in sys.argv
REV_ESPERADA = sys.argv[sys.argv.index("--escribir") + 1] if ESCRIBIR else None
MES = "2026-09"
MES_INI, MES_FIN = pd.Timestamp("2026-09-01"), pd.Timestamp("2026-09-30")
CORTE = pd.Timestamp("2026-10-08")          # lo cargado ANTES de hoy ya está en el 1.267.930 del primer reajuste
INCENTIVO_ACTUAL = {"1444": 1267930.0}
ONEDRIVE_DIR = "/Users/julianlopez/Library/CloudStorage/OneDrive-Personal/Historico Carga/Conciliacion/Mayoristas"
ONEDRIVE_ANT = f"{ONEDRIVE_DIR}/Antiguos"


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


def base_usd(d, hasta=None, desde=None):
    """USD neto de septiembre con la regla de agregar_incentivo_amex (Motivo exacto, sin las
    filas de incentivo). `hasta`/`desde` filtran por Fecha de Carga (< hasta, >= desde)."""
    o = d["Orden"].astype(str)
    mot = d["Motivo"].astype(str).str.strip()
    tu = d["Tipo"].astype(str).str.strip().str.upper()
    f = pd.to_datetime(d["Fecha"], errors="coerce")
    fc = pd.to_datetime(d["Fecha de Carga"], errors="coerce")
    es_tarjeta = mot.isin(["Tarjeta Amex", "Tarjeta Rakuten", "Tarjeta Robinhood", "Tarjeta Capital",
                           "Tarjeta US Bank", "Tarjeta Intuit", "Tarjeta Apple Pay", "Tarjeta Discover"])
    es_inc = o.str.startswith("incentivoamex_") | mot.eq("Incentivo Amex")
    m = es_tarjeta & ~es_inc & tu.isin(["EGRESO", "INGRESO"]) & (f >= MES_INI) & (f <= MES_FIN)
    if hasta is not None:
        m &= fc < hasta
    if desde is not None:
        m &= fc >= desde
    trm = pd.to_numeric(d["TRM"], errors="coerce")
    assert not (trm[m].isna() | (trm[m] <= 0)).any(), "fila de tarjeta sin TRM válida"
    usd = pd.to_numeric(d["Monto"], errors="coerce")[m] / trm[m]
    return float(usd[tu[m] == "EGRESO"].sum() - usd[tu[m] == "INGRESO"].sum()), int(m.sum())


def main():
    import harness
    mod = harness.cargar_app()
    SEP = "=" * 94
    def banner(t): print(f"\n{SEP}\n{t}\n{SEP}")
    ok = True
    def chk(n, c, det=""):
        nonlocal ok
        print(f"  {'✔' if c else '🚨'} {n:<66} {det}")
        ok = ok and bool(c)

    print(f"MODO: {'🔴 ESCRITURA REAL' if ESCRIBIR else '🟢 DRY-RUN (0 escrituras)'}")
    for fn in ("procesar_egresos", "procesar_amex", "procesar_rakuten", "procesar_robinhood",
               "procesar_capital", "procesar_usbank", "procesar_intuit", "procesar_discover",
               "agregar_incentivo_amex"):
        def _boom(*a, _n=fn, **k):
            raise AssertionError(f"{_n} fue llamada — este script solo reajusta 3 filas")
        setattr(mod, fn, _boom)
    chk("la tarifa del módulo sigue en 25 COP/USD", mod.INCENTIVO_COP_POR_USD == 25)

    banner("1) HISTÓRICO VIVO FRESCO")
    cfg = mod.st.secrets["dropbox"]
    md = mod.dbx.files_get_metadata(cfg["remote_path"])
    _, res = mod.dbx.files_download(cfg["remote_path"])
    vivo = pd.read_excel(io.BytesIO(res.content), sheet_name=None)
    HOJA = {cas: next(h for h in vivo if h.split(" - ")[0].strip() == cas) for cas in INCENTIVO_ACTUAL}
    print(f"  rev={md.rev}  modificado={md.server_modified}  size={md.size:,}")
    if ESCRIBIR and md.rev != REV_ESPERADA:
        raise SystemExit(f"⛔ ABORTA: el histórico se movió (esperaba {REV_ESPERADA}). Rehacer dry-run.")
    s0 = {cas: saldo(vivo[h]) for cas, h in HOJA.items()}

    banner(f"2) REPRODUCIR LO ESCRITO Y CALCULAR EL NUEVO VALOR ({MES})")
    hoy = pd.Timestamp.today().strftime("%Y-%m-%d")
    historico = {k: mod.asegurar_columnas_historico(v.copy()) for k, v in vivo.items()}
    nuevo = {}
    for cas, h in HOJA.items():
        d = vivo[h]
        orden = f"incentivoamex_{cas}_{MES}"
        m = d["Orden"].astype(str).str.strip() == orden
        chk(f"{cas}: existe 1 sola fila {orden}", int(m.sum()) == 1, f"{int(m.sum())}")
        escrito = float(pd.to_numeric(d.loc[m, "Monto"]).iloc[0])
        chk(f"{cas}: el Monto escrito es el esperado", abs(escrito - INCENTIVO_ACTUAL[cas]) < 1, f"{escrito:,.0f}")
        b_antes, n_antes = base_usd(d, hasta=CORTE)
        b_tarde, n_tarde = base_usd(d, desde=CORTE)
        b_total, n_total = base_usd(d)
        chk(f"{cas}: toda fila de sep tiene Fecha de Carga", n_antes + n_tarde == n_total, f"{n_antes}+{n_tarde} vs {n_total}")
        calc_a = round(b_antes * 25)
        chk(f"{cas}: la fórmula reproduce el incentivo escrito", abs(calc_a - escrito) < 2,
            f"fórmula {calc_a:,.0f} vs escrito {escrito:,.0f}")
        calc_d = round(b_total * 25)
        chk(f"{cas}: Δ = 25 × USD de sep cargado hoy ({b_tarde:,.2f})",
            abs((calc_d - escrito) - b_tarde * 25) < 2, f"Δ {calc_d - escrito:+,.0f}")
        print(f"     filas sep {n_antes} + {n_tarde} tardías · USD {b_antes:,.2f} → {b_total:,.2f}   "
              f"COP {escrito:,.0f} → {calc_d:,.0f}  (Δ {calc_d - escrito:+,.0f})")
        nuevo[cas] = calc_d
    if not ok:
        raise SystemExit("⛔ ABORTA: no reproduzco algún incentivo; no toco ninguna fila.")
    for cas, h in HOJA.items():
        d = historico[h]
        m = d["Orden"].astype(str).str.strip() == f"incentivoamex_{cas}_{MES}"
        d.loc[m, "Monto"] = float(nuevo[cas])
        d.loc[m, "Fecha de Carga"] = hoy
        historico[h] = mod.recalcular_totales_diarios(d, usuario=usuario_de_totales(vivo[h]),
                                                      cas=cas_de_totales(vivo[h], cas))

    banner("3) INVARIANTES")
    vac = {"", "nan", "none", "nat"}
    for cas, h in HOJA.items():
        A, B = vivo[h], historico[h]
        oa, ob = A["Orden"].astype(str).str.strip(), B["Orden"].astype(str).str.strip()
        orden = f"incentivoamex_{cas}_{MES}"
        def movs(df, o):
            x = df[(df["Tipo"].astype(str).str.strip().str.upper() != "TOTAL") & (o != orden)]
            return round(float(pd.to_numeric(x["Monto"], errors="coerce").fillna(0).sum()), 2), len(x)
        chk(f"{cas}: ningún otro movimiento cambió", movs(A, oa) == movs(B, ob), f"{movs(A, oa)[1]} filas")
        chk(f"{cas}: misma cantidad de movimientos (0 agregados/borrados)",
            int((A["Tipo"].astype(str).str.strip().str.upper() != "TOTAL").sum()) ==
            int((B["Tipo"].astype(str).str.strip().str.upper() != "TOTAL").sum()))
        perd = {y for y in oa if y.lower() not in vac} - {y for y in ob if y.lower() not in vac}
        chk(f"{cas}: 0 Orden previos perdidos", not perd, f"{len(perd)}")
        a = A["Nombre del producto"].astype(str).str.lower().str.startswith("comision de (")
        b = B["Nombre del producto"].astype(str).str.lower().str.startswith("comision de (")
        chk(f"{cas}: la comisión no se toca", int(a.sum()) == int(b.sum()) and
            abs(pd.to_numeric(A.loc[a, "Monto"]).sum() - pd.to_numeric(B.loc[b, "Monto"]).sum()) < 0.01,
            f"{int(b.sum())} filas")
        s1 = saldo(B)
        chk(f"{cas}: el saldo sube exactamente el Δ", abs((s1 - s0[cas]) - (nuevo[cas] - INCENTIVO_ACTUAL[cas])) < 0.01,
            f"{s0[cas]:,.2f} → {s1:,.2f}")
    for hoja in vivo:
        if hoja in HOJA.values():
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
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        for hh, dfh in historico.items():
            w.book.create_sheet(hh[:31])
            dfh.to_excel(w, sheet_name=hh[:31], index=False)
    buf.seek(0); data_bytes = buf.read()
    harness.clear_msgs()
    try:
        mod.guard_frescura_historico(historico)
        print("  ✅ GUARD A PASA (0 pérdidas)")
    except harness._Stop:
        for n, t in harness.MENSAJES:
            print(f"  [{n}] {t[:400]}")
        raise SystemExit("⛔ GUARD A BLOQUEÓ")

    banner("4) RESUMEN")
    tot = 0
    for cas, h in HOJA.items():
        dl = nuevo[cas] - INCENTIVO_ACTUAL[cas]; tot += dl
        print(f"  {h:<28} incentivo {INCENTIVO_ACTUAL[cas]:>12,.0f} → {nuevo[cas]:>12,.0f}  Δ {dl:>+10,.0f}"
              f"   saldo → {saldo(historico[h]):>18,.2f}")
    print(f"  {'TOTAL':<28} {'':>40}  Δ {tot:>+10,.0f}")

    if not ESCRIBIR:
        banner("DRY-RUN TERMINADO — 0 ESCRITURAS")
        print(f"  Para escribir: python3 reajustar_incentivo_sep_1444_robinhood_20261008.py --escribir {md.rev}")
        return

    banner("5) SUBIDA A DROPBOX (capa C hace el respaldo)")
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

    banner("6) VALIDACIÓN POST-ESCRITURA")
    md2 = mod.dbx.files_get_metadata(cfg["remote_path"])
    print(f"  rev NUEVA = {md2.rev}  size = {md2.size:,}")
    print(f"  backup    = {backup}")
    _, r2 = mod.dbx.files_download(cfg["remote_path"])
    contenido = r2.content
    rel = pd.read_excel(io.BytesIO(contenido), sheet_name=None)
    ok2 = True
    def chk2(n, c, det=""):
        nonlocal ok2
        print(f"  {'✔' if c else '🚨'} {n:<64} {det}")
        ok2 = ok2 and bool(c)
    for cas, h in HOJA.items():
        q = rel[h]["Orden"].astype(str).str.strip() == f"incentivoamex_{cas}_{MES}"
        chk2(f"{cas}: incentivo = {nuevo[cas]:,.0f}", int(q.sum()) == 1 and
             abs(float(pd.to_numeric(rel[h].loc[q, "Monto"]).iloc[0]) - nuevo[cas]) < 0.01)
        chk2(f"{cas}: saldo", abs(saldo(rel[h]) - saldo(historico[h])) < 0.01, f"COP {saldo(rel[h]):,.2f}")
    for hoja in vivo:
        if hoja in HOJA.values():
            continue
        A, B = vivo[hoja], rel[hoja]
        igual = len(A) == len(B)
        if igual and "Monto" in A.columns:
            igual = abs(pd.to_numeric(A["Monto"], errors="coerce").fillna(0).sum() -
                        pd.to_numeric(B["Monto"], errors="coerce").fillna(0).sum()) < 0.01
        chk2(f"verbatim: {hoja}", igual, f"{len(A)} filas")

    banner("7) COPIA A ONEDRIVE")
    hoyc = pd.Timestamp.today().strftime("%Y%m%d")
    destino = f"{ONEDRIVE_DIR}/{hoyc}_Historico_mayoristas.xlsx"
    os.makedirs(ONEDRIVE_ANT, exist_ok=True)
    # ⚠️ COPIAR, no mover (el 18-sep OneDrive perdió un archivo movido).
    for f in sorted(os.listdir(ONEDRIVE_DIR)):
        if f.endswith("_Historico_mayoristas.xlsx"):
            src_f = f"{ONEDRIVE_DIR}/{f}"
            dst_f = f"{ONEDRIVE_ANT}/{f.replace('.xlsx','')}_PRE_incentivo_sep_1444_robinhood.xlsx"
            if os.path.exists(dst_f):
                raise SystemExit(f"⛔ ya existe {dst_f}; no se pisa. Revisar a mano.")
            shutil.copy2(src_f, dst_f)
            if not (os.path.exists(dst_f) and os.path.getsize(dst_f) == os.path.getsize(src_f)):
                raise SystemExit(f"⛔ la copia a Antiguos no quedó bien; NO se reemplaza {f}.")
            if f != os.path.basename(destino):
                os.remove(src_f)
            print(f"  archivada: {f} → Antiguos/{os.path.basename(dst_f)}")
    with open(destino, "wb") as fh:
        fh.write(contenido)
    print(f"  escrita:   {destino}  ({os.path.getsize(destino):,} bytes)")
    chk2("la copia de OneDrive pesa lo mismo que el vivo",
         os.path.getsize(destino) == md2.size, f"{os.path.getsize(destino):,} vs {md2.size:,}")

    print(f"\n  {'✅ INCENTIVO DE SEPTIEMBRE REAJUSTADO Y VERIFICADO' if ok2 else '🚨 REVISAR'}")
    print(f"  rev nueva: {md2.rev}\n  rollback:  {backup}")


if __name__ == "__main__":
    sys.path.insert(0, os.environ.get("HARNESS_DIR", "."))
    main()
