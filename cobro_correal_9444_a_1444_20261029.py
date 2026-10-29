#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
1444 · María Moises — cobro de los ingresos "Correal" de 2026 que entraron a la 9444 (Maira) y nunca
se le cobraron a la 1444. Pedido de Julián el 29-oct-2026 ("hagamoslo en 2026").

Patrón de los cobros que SÍ existen (Extra18, 24, 28, 34, 35, 38): Ingreso_extra NEGATIVO en 1444 por
el monto que entró a la 9444 + 1%. Faltan 8 de 2026 (el de dic-2025, colegio 5.347.888, queda fuera):

    15-abr Wendy Aragon        4.740.000   →  -4.787.400
    04-may Juan Correal Sant   8.000.000   →  -8.080.000
    04-jun colegio bilingüe   10.000.000   → -10.100.000
    10-ago Juan Correal Sant  10.000.000   → -10.100.000   (Extra329; el Extra330 "Bancolombia" ya es Extra38 de 1444)
    11-sep Juan Correal Sant  10.000.000   → -10.100.000
    05-oct Juan Correal Sant  10.000.000   → -10.100.000
    05-oct Kevin Montero       3.000.000   →  -3.030.000
    06-oct Kevin Montero       5.000.000   →  -5.050.000
                              60.740.000   → -61.347.400

FECHA = HOY, no la original: la comisión quincenal de 1444 se RECALCULA desde el 16-may con el peor
saldo negativo de cada quincena; fechar atrás cambiaría comisiones ya cobradas. La fecha original va
en el Nombre del producto. Orden propio (no ExtraN) para que re-subir ingresos_extra.xlsx no los pise
(dedup de Ingreso_extra por Orden+Motivo, keep="last"); se agregan también a ese archivo.

  sin argumentos  -> dry-run (0 escrituras)
  --escribir REV  -> escribe a Dropbox (REV = rev del dry-run), ingresos_extra.xlsx y la copia de OneDrive
"""
import sys, os, io, shutil, warnings
warnings.filterwarnings("ignore")
import pandas as pd

ESCRIBIR = "--escribir" in sys.argv
REV_ESPERADA = sys.argv[sys.argv.index("--escribir") + 1] if ESCRIBIR else None
CAS = "1444"
CAS_ORIGEN = "9444"
FECHA = pd.Timestamp("2026-10-29")
COMISION = 0.01
# (fecha 9444, Orden en 9444, nombre en 9444, monto 9444)
COBROS = [
    ("2026-04-15", "Extra212", "Wendy Aragon se", 4740000.0),
    ("2026-05-04", "Extra230", "Juan Correal Sant", 8000000.0),
    ("2026-06-04", "Extra273", "colegio bilingue santi", 10000000.0),
    ("2026-08-10", "Extra329", "Juan Correal Sant", 10000000.0),
    ("2026-09-11", "Extra365", "Juan Correal Sant", 10000000.0),
    ("2026-10-05", "Extra384", "Juan Correal Sant", 10000000.0),
    ("2026-10-05", "Extra385", "Kevin Montero San", 3000000.0),
    ("2026-10-06", "Extra386", "Kevin Montero San", 5000000.0),
]
TOTAL_9444 = 60740000.0
ABONO = 4570000.0
ORDEN_ABONO = "abono_perdida1_202610_1444"
NOMBRE_ABONO = "Abono perdida 1"
TOTAL_COBRO = 61347400.0
EXTRA = "/Users/julianlopez/Library/CloudStorage/OneDrive-Personal/Historico Carga/Conciliacion/Mayoristas/ingresos_extra.xlsx"
ONEDRIVE_DIR = "/Users/julianlopez/Library/CloudStorage/OneDrive-Personal/Historico Carga/Conciliacion/Mayoristas"
ONEDRIVE_ANT = f"{ONEDRIVE_DIR}/Antiguos"


def orden_de(f, o9):
    return f"cobro_correal_{CAS_ORIGEN}_{f.replace('-', '')}_{o9}"


def nombre_de(f, n9, m9):
    return f"Cobro Correal: ingreso a 9444 del {f} ({n9}, {o_fmt(m9)}) + 1%"


def o_fmt(v):
    return f"{v:,.0f}".replace(",", ".")


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


def main():
    import harness
    mod = harness.cargar_app()
    SEP = "=" * 94
    def banner(t): print(f"\n{SEP}\n{t}\n{SEP}")
    ok = True
    def chk(n, c, det=""):
        nonlocal ok
        print(f"  {'✔' if c else '🚨'} {n:<70} {det}")
        ok = ok and bool(c)
    print(f"MODO: {'🔴 ESCRITURA REAL' if ESCRIBIR else '🟢 DRY-RUN (0 escrituras)'}")
    for fn in ("procesar_egresos", "procesar_amex", "procesar_rakuten", "procesar_robinhood",
               "procesar_capital", "procesar_usbank", "procesar_intuit", "procesar_discover",
               "agregar_incentivo_amex"):
        def _boom(*a, _n=fn, **k):
            raise AssertionError(f"{_n} fue llamada — este script solo agrega cobros")
        setattr(mod, fn, _boom)

    filas_cobro = []
    for f, o9, n9, m9 in COBROS:
        filas_cobro.append({"orden": orden_de(f, o9), "monto": -round(m9 * (1 + COMISION), 2),
                            "nombre": nombre_de(f, n9, m9), "f9": f, "o9": o9, "m9": m9})
    chk("suma 9444 = 60.740.000", abs(sum(c[3] for c in COBROS) - TOTAL_9444) < 0.01)
    chk("suma cobro = 61.347.400", abs(-sum(x["monto"] for x in filas_cobro) - TOTAL_COBRO) < 0.01)

    banner("1) HISTÓRICO VIVO FRESCO")
    cfg = mod.st.secrets["dropbox"]
    md = mod.dbx.files_get_metadata(cfg["remote_path"])
    _, res = mod.dbx.files_download(cfg["remote_path"])
    vivo = pd.read_excel(io.BytesIO(res.content), sheet_name=None)
    HOJA = next(h for h in vivo if h.split(" - ")[0].strip() == CAS and h.strip().endswith("Moises"))
    H9 = next(h for h in vivo if h.split(" - ")[0].strip() == CAS_ORIGEN)
    s0 = saldo(vivo[HOJA])
    print(f"  rev={md.rev}  modificado={md.server_modified}  size={md.size:,}")
    print(f"  '{HOJA}': saldo {s0:,.2f}")
    if ESCRIBIR and md.rev != REV_ESPERADA:
        raise SystemExit(f"⛔ ABORTA: el histórico se movió (esperaba {REV_ESPERADA}). Rehacer dry-run.")

    banner("2) VERIFICACIONES PREVIAS")
    d9 = vivo[H9]
    o9s = d9["Orden"].astype(str).str.strip()
    for x in filas_cobro:
        r = d9[(o9s == x["o9"]) & (d9["Motivo"].astype(str).str.strip() == "Ingreso_extra")]
        chk(f"9444 tiene {x['o9']} {x['f9']} por {o_fmt(x['m9'])}",
            len(r) == 1 and abs(float(pd.to_numeric(r["Monto"]).iloc[0]) - x["m9"]) < 0.01
            and pd.to_datetime(r["Fecha"]).iloc[0].strftime("%Y-%m-%d") == x["f9"],
            "" if len(r) != 1 else str(r["Nombre del producto"].iloc[0]))
    o0 = vivo[HOJA]["Orden"].astype(str).str.strip()
    chk("1444 todavía no tiene ninguno de estos cobros ni el abono", not o0.isin([x["orden"] for x in filas_cobro] + [ORDEN_ABONO]).any())
    ex = pd.read_excel(EXTRA, sheet_name=None)
    HEX = next(h for h in ex if h.split("-")[0].strip() == CAS)
    chk("ingresos_extra.xlsx todavía no los tiene",
        not ex[HEX]["Orden"].astype(str).str.strip().isin([x["orden"] for x in filas_cobro]).any(),
        f"hoja '{HEX}' {len(ex[HEX])} filas")
    if not ok:
        raise SystemExit("⛔ ABORTA.")

    banner("3) APLICAR — cada cobro en SU fecha + abono perdida 1 de octubre hoy")
    hoy = pd.Timestamp.today().strftime("%Y-%m-%d")
    historico = {k: mod.asegurar_columnas_historico(v.copy()) for k, v in vivo.items()}
    d = historico[HOJA]
    u_orig = usuario_de_totales(vivo[HOJA]) or "Maria Moises"
    cas_t = cas_de_totales(vivo[HOJA], CAS)
    nuevas = []
    for x in filas_cobro:
        fila = {c: "" for c in d.columns}
        fila.update({"Fecha": pd.Timestamp(x["f9"]), "Tipo": "Ingreso", "Monto": x["monto"], "Orden": x["orden"],
                     "Motivo": "Ingreso_extra", "TRM": "", "Usuario": u_orig, "Casillero": int(CAS),
                     "Nombre del producto": x["nombre"], "Fecha de Carga": hoy})
        nuevas.append(fila)
        print(f"  + {x['f9']} {x['orden']:<42} {x['monto']:>16,.2f}")
    fila = {c: "" for c in d.columns}
    fila.update({"Fecha": FECHA, "Tipo": "Ingreso", "Monto": ABONO, "Orden": ORDEN_ABONO,
                 "Motivo": "Ingreso_extra", "TRM": "", "Usuario": u_orig, "Casillero": int(CAS),
                 "Nombre del producto": NOMBRE_ABONO, "Fecha de Carga": hoy})
    nuevas.append(fila)
    print(f"  + {FECHA:%Y-%m-%d} {ORDEN_ABONO:<42} {ABONO:>16,.2f}  {NOMBRE_ABONO}")
    d = pd.concat([d, pd.DataFrame(nuevas)], ignore_index=True)
    d = mod.recalcular_totales_diarios(d, usuario=u_orig, cas=cas_t)

    def tots(df):
        t = df[df["Tipo"].astype(str).str.strip().str.upper() == "TOTAL"].copy()
        return t.assign(F=pd.to_datetime(t["Fecha"]).dt.normalize()).groupby("F")["Monto"].last().astype(float)

    banner("3b) COMISIÓN QUINCENAL RECALCULADA (1,5% × |día más negativo|), quincena por quincena en orden")
    import calendar
    MESES = {4: "abril", 5: "mayo", 6: "junio", 7: "julio", 8: "agosto", 9: "septiembre", 10: "octubre"}
    QUINCENAS = []
    for m in range(4, 11):
        QUINCENAS.append((pd.Timestamp(2026, m, 1), pd.Timestamp(2026, m, 15), f"1-15 {MESES[m]} 2026"))
        if m < 10:  # 16-31 oct no ha cerrado: la calcula la app en su ventana (1-15 nov)
            QUINCENAS.append((pd.Timestamp(2026, m, 16), pd.Timestamp(2026, m, calendar.monthrange(2026, m)[1]),
                              f"16-fin {MESES[m]} 2026"))
    resumen_com = []
    for ini, fin, et in QUINCENAS:
        orden_c = f"Comision de ({et})"
        tt = tots(d)
        w = tt[(tt.index >= ini) & (tt.index <= fin)]
        peor = float(w.min()) if len(w) else 0.0
        com = round(abs(peor) * 0.015, 2) if peor < 0 else 0.0
        mk = d["Orden"].astype(str).str.strip().str.lower().eq(orden_c.lower())
        antes_c = float(pd.to_numeric(d.loc[mk, "Monto"], errors="coerce").sum()) if mk.any() else None
        if mk.any():
            d.loc[mk, "Monto"] = com
            d.loc[mk, "Fecha de Carga"] = hoy
        elif com > 0:
            fila = {c: "" for c in d.columns}
            fila.update({"Fecha": fin + pd.Timedelta(days=1), "Tipo": "Egreso", "Orden": orden_c, "Monto": com,
                         "Motivo": "comision", "TRM": "", "Usuario": u_orig, "Casillero": CAS,
                         "Nombre del producto": orden_c, "Fecha de Carga": hoy})
            d = pd.concat([d, pd.DataFrame([fila])], ignore_index=True)
        d = mod.recalcular_totales_diarios(d, usuario=u_orig, cas=cas_t)
        resumen_com.append((et, peor, antes_c, com))
        print(f"  {et:<24} peor día {peor:>16,.2f}   comisión {('—' if antes_c is None else f'{antes_c:,.2f}'):>14} → {com:>14,.2f}")
    historico[HOJA] = d
    DELTA_COM = sum(c - (a or 0.0) for _, _, a, c in resumen_com)
    print(f"  Δ comisiones = {DELTA_COM:,.2f}")
    ta, tb = tots(vivo[HOJA]), tots(historico[HOJA])

    banner("4) INVARIANTES")
    s1 = saldo(historico[HOJA])
    esperado = TOTAL_COBRO - ABONO + DELTA_COM
    chk("saldo baja exactamente cobros − abono + Δ comisiones", abs((s0 - s1) - esperado) < 0.01,
        f"{s0:,.2f} → {s1:,.2f}")
    def movs(df):
        return int((df["Tipo"].astype(str).str.strip().str.upper() != "TOTAL").sum())
    n_nuevas_com = sum(1 for _, _, a, c in resumen_com if a is None and c > 0)
    chk(f"+9 filas (+{n_nuevas_com} comisiones nuevas), no se borra nada",
        movs(historico[HOJA]) == movs(vivo[HOJA]) + 9 + n_nuevas_com)
    comun = ta.index[ta.index < pd.Timestamp("2026-04-01")]
    chk("saldos diarios ANTERIORES al 1-abr idénticos", (tb.reindex(comun) - ta.reindex(comun)).abs().max() < 0.01, f"{len(comun)} días")
    recalc = {f"comision de ({et})".lower() for _, _, et in QUINCENAS}
    def comis(df):
        c = df[df["Orden"].astype(str).str.contains("Comision", case=False, na=False)]
        c = c[~c["Orden"].astype(str).str.strip().str.lower().isin(recalc)]
        return sorted(zip(c["Orden"].astype(str), pd.to_numeric(c["Monto"]).round(2)))
    chk("comisiones fuera de abr–oct idénticas", comis(vivo[HOJA]) == comis(historico[HOJA]), f"{len(comis(vivo[HOJA]))}")
    vac = {"", "nan", "none", "nat"}
    a = {y for y in vivo[HOJA]["Orden"].astype(str).str.strip() if y.lower() not in vac}
    b = {y for y in historico[HOJA]["Orden"].astype(str).str.strip() if y.lower() not in vac}
    chk(f"{HOJA}: 0 Orden perdidos", not (a - b), f"{len(a - b)}")
    for hoja in vivo:
        if hoja == HOJA:
            continue
        A, B = vivo[hoja], historico[hoja]
        igual = len(A) == len(B)
        if igual and "Monto" in A.columns:
            igual = abs(pd.to_numeric(A["Monto"], errors="coerce").fillna(0).sum() -
                        pd.to_numeric(B["Monto"], errors="coerce").fillna(0).sum()) < 0.01
        chk(f"verbatim: {hoja}", igual, f"{len(A)} filas")
    import json
    json.dump({"s0": s0, "s1": s1, "delta_com": DELTA_COM, "com": resumen_com,
               "diario": {k.strftime("%Y-%m-%d"): [float(ta.get(k, float("nan"))), float(v)] for k, v in tb.items() if k >= pd.Timestamp("2026-04-01")}},
              open(os.environ.get("RES_JSON", "/dev/null"), "w"), default=str)
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

    if not ESCRIBIR:
        banner("DRY-RUN TERMINADO — 0 ESCRITURAS")
        print(f"  Para escribir: python3 cobro_correal_9444_a_1444_20261029.py --escribir {md.rev}")
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
    orl = rel[HOJA]["Orden"].astype(str).str.strip()
    for x in filas_cobro:
        q = orl == x["orden"]
        chk2(f"1444: {x['orden']}", int(q.sum()) == 1 and abs(float(pd.to_numeric(rel[HOJA].loc[q, "Monto"]).iloc[0]) - x["monto"]) < 0.01)
    chk2("1444: abono perdida 1 de octubre", int((orl == ORDEN_ABONO).sum()) == 1)
    for et, _, _, com in resumen_com:
        q = orl.str.lower() == f"comision de ({et})".lower()
        chk2(f"1444: comisión {et}", (com == 0 and not q.any()) or abs(float(pd.to_numeric(rel[HOJA].loc[q, "Monto"]).sum()) - com) < 0.01)
    chk2("1444: saldo", abs(saldo(rel[HOJA]) - s1) < 0.01, f"COP {saldo(rel[HOJA]):,.2f}")
    for hoja in vivo:
        if hoja == HOJA:
            continue
        A, B = vivo[hoja], rel[hoja]
        igual = len(A) == len(B)
        if igual and "Monto" in A.columns:
            igual = abs(pd.to_numeric(A["Monto"], errors="coerce").fillna(0).sum() -
                        pd.to_numeric(B["Monto"], errors="coerce").fillna(0).sum()) < 0.01
        chk2(f"verbatim: {hoja}", igual, f"{len(A)} filas")

    banner("7) ingresos_extra.xlsx (OneDrive) — mismas filas, mismo Orden")
    import openpyxl
    bk = EXTRA.replace(".xlsx", f"_backup_{pd.Timestamp.today():%Y%m%d_%H%M%S}_pre_cobro_correal.xlsx")
    shutil.copy2(EXTRA, bk)
    chk2("respaldo de ingresos_extra.xlsx", os.path.getsize(bk) == os.path.getsize(EXTRA), bk)
    wb = openpyxl.load_workbook(EXTRA); ws = wb[HEX]
    cab = [c.value for c in ws[1]]
    for x in filas_cobro:
        val = {"Tipo": "Ingreso", "Fecha": pd.Timestamp(x["f9"]).to_pydatetime(), "Orden": x["orden"], "Monto": x["monto"],
               "Usuario": u_orig, "Casillero": int(CAS), "Motivo": "Ingreso_extra", "Nombre del producto": x["nombre"]}
        ws.append([val.get(c) for c in cab])
    val = {"Tipo": "Ingreso", "Fecha": FECHA.to_pydatetime(), "Orden": ORDEN_ABONO, "Monto": ABONO,
           "Usuario": u_orig, "Casillero": int(CAS), "Motivo": "Ingreso_extra", "Nombre del producto": NOMBRE_ABONO}
    ws.append([val.get(c) for c in cab])
    wb.save(EXTRA)
    ex2 = pd.read_excel(EXTRA, sheet_name=None)
    oex = ex2[HEX]["Orden"].astype(str)
    chk2("ingresos_extra.xlsx: los 8 cobros quedaron una vez cada uno",
         all(int((oex == x["orden"]).sum()) == 1 for x in filas_cobro))
    chk2("ingresos_extra.xlsx: las demás hojas intactas", all(ex2[k].equals(ex[k]) for k in ex if k != HEX))

    banner("8) COPIA A ONEDRIVE")
    hoyc = pd.Timestamp.today().strftime("%Y%m%d")
    destino = f"{ONEDRIVE_DIR}/{hoyc}_Historico_mayoristas.xlsx"
    os.makedirs(ONEDRIVE_ANT, exist_ok=True)
    # ⚠️ COPIAR, no mover (el 18-sep OneDrive perdió un archivo movido).
    for f in sorted(os.listdir(ONEDRIVE_DIR)):
        if f.endswith("_Historico_mayoristas.xlsx"):
            src_f = f"{ONEDRIVE_DIR}/{f}"
            dst_f = f"{ONEDRIVE_ANT}/{f.replace('.xlsx','')}_PRE_cobro_correal.xlsx"
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
    import hashlib
    chk2("la copia de OneDrive es idéntica al vivo (sha256)",
         hashlib.sha256(open(destino, "rb").read()).hexdigest() == hashlib.sha256(contenido).hexdigest())

    print(f"\n  {'✅ COBROS CORREAL APLICADOS A 1444 Y VERIFICADOS' if ok2 else '🚨 REVISAR'}")
    print(f"  rev nueva: {md2.rev}\n  rollback:  {backup}")


if __name__ == "__main__":
    sys.path.insert(0, os.environ.get("HARNESS_DIR", "."))
    main()
