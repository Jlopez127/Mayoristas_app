#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Consignación del 7-oct-2026 por COP 1.760.000: Juan Felipe Laverde (9680) debía girarla a Encargomío y por
error la giró a Paula Herrera (11591). Decisión del usuario (8-oct): la consignación de Paula NO se toca;
se le descuenta con un Ingreso_extra NEGATIVO justificado y se le abona a Laverde con un Ingreso_extra positivo.

  11591: Ingreso / Ingreso_extra, Monto -1.760.000, Orden traslado_20261007_1760000_a9680, Fecha 7-oct.
  9680:  Ingreso / Ingreso_extra, Monto +1.760.000, Orden traslado_20261007_1760000_de11591, Fecha 7-oct.
  No hay regla en el código: son filas manuales (el dedup de Ingreso_extra es por Orden+Motivo y estos
  Orden no chocan con la serie ExtraN de ingresos_extra.xlsx).

  sin argumentos  -> dry-run (0 escrituras)
  --escribir REV  -> escribe a Dropbox (REV = rev del dry-run) y refresca la copia de OneDrive
"""
import sys, os, io, shutil, warnings
warnings.filterwarnings("ignore")
import pandas as pd

ESCRIBIR = "--escribir" in sys.argv
REV_ESPERADA = sys.argv[sys.argv.index("--escribir") + 1] if ESCRIBIR else None
CAS = "11591"
CAS_DEST = "9680"
ORDEN = "20261007-1760000-Elvis-Bancolombia-1"
ORDEN_ORIG = "traslado_20261007_1760000_a9680"
ORDEN_DEST = "traslado_20261007_1760000_de11591"
MONTO = 1760000.0
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
            raise AssertionError(f"{_n} fue llamada — este script solo mueve una consignación")
        setattr(mod, fn, _boom)

    banner("2) HISTÓRICO VIVO FRESCO")
    cfg = mod.st.secrets["dropbox"]
    md = mod.dbx.files_get_metadata(cfg["remote_path"])
    _, res = mod.dbx.files_download(cfg["remote_path"])
    vivo = pd.read_excel(io.BytesIO(res.content), sheet_name=None)
    HOJA = next(h for h in vivo if h.split(" - ")[0].strip() == CAS)
    HDEST = next(h for h in vivo if h.split(" - ")[0].strip() == CAS_DEST)
    s0, sd0 = saldo(vivo[HOJA]), saldo(vivo[HDEST])
    print(f"  rev={md.rev}  modificado={md.server_modified}  size={md.size:,}")
    print(f"  '{HOJA}': saldo {s0:,.2f} · '{HDEST}': saldo {sd0:,.2f}")
    if ESCRIBIR and md.rev != REV_ESPERADA:
        raise SystemExit(f"⛔ ABORTA: el histórico se movió (esperaba {REV_ESPERADA}). Rehacer dry-run.")

    banner("3) LAS FILAS")
    o0 = vivo[HOJA]["Orden"].astype(str).str.strip()
    m = o0.str.replace(".0", "", regex=False) == ORDEN
    r = vivo[HOJA][m]
    print(r[["Fecha", "Tipo", "Monto", "Orden", "Nombre del producto", "Fecha de Carga"]].to_string())
    chk("existe UNA sola vez en 11591, como Ingreso de 1.760.000 del 7-oct",
        len(r) == 1 and str(r["Tipo"].iloc[0]).strip() == "Ingreso"
        and abs(float(r["Monto"].iloc[0]) - MONTO) < 0.01
        and pd.Timestamp(r["Fecha"].iloc[0]).date() == pd.Timestamp("2026-10-07").date())
    od = vivo[HDEST]["Orden"].astype(str).str.strip()
    chk("9680 todavía no tiene la fila destino", not (od == ORDEN_DEST).any())
    chk("11591 todavía no tiene el ajuste", not (o0 == ORDEN_ORIG).any())
    if not ok:
        raise SystemExit("⛔ ABORTA.")

    banner("4) APLICAR")
    hoy = pd.Timestamp.today().strftime("%Y-%m-%d")
    historico = {k: mod.asegurar_columnas_historico(v.copy()) for k, v in vivo.items()}
    d = historico[HOJA]
    u_orig = usuario_de_totales(vivo[HOJA]) or "Paula Herrera"
    fila = {c: "" for c in d.columns}
    fila.update({"Fecha": pd.Timestamp("2026-10-07"), "Tipo": "Ingreso", "Monto": -MONTO, "Orden": ORDEN_ORIG,
                 "Motivo": "Ingreso_extra", "TRM": "", "Usuario": u_orig, "Casillero": int(CAS),
                 "Nombre del producto": "Ajuste: consignación de 1.760.000 del 7-oct-2026 que Juan Felipe Laverde (9680) debía girar a Encargomío y por error giró a Paula Herrera; se traslada a 9680",
                 "Fecha de Carga": hoy})
    d = pd.concat([d, pd.DataFrame([fila])], ignore_index=True)
    historico[HOJA] = mod.recalcular_totales_diarios(d, usuario=u_orig, cas=cas_de_totales(vivo[HOJA], CAS))
    dd = historico[HDEST]
    u_dest = usuario_de_totales(vivo[HDEST]) or "Juan Felipe Laverde"
    nueva = {c: "" for c in dd.columns}
    nueva.update({"Fecha": pd.Timestamp("2026-10-07"), "Tipo": "Ingreso", "Monto": MONTO, "Orden": ORDEN_DEST,
                  "Motivo": "Ingreso_extra", "TRM": "", "Usuario": u_dest, "Casillero": int(CAS_DEST),
                  "Nombre del producto": "Ajuste: consignación de 1.760.000 del 7-oct-2026 que debía girar a Encargomío y por error giró a Paula Herrera (11591); se le abona aquí",
                  "Fecha de Carga": hoy})
    dd = pd.concat([dd, pd.DataFrame([nueva])], ignore_index=True)
    historico[HDEST] = mod.recalcular_totales_diarios(dd, usuario=u_dest, cas=cas_de_totales(vivo[HDEST], CAS_DEST))
    print(historico[HOJA][historico[HOJA]["Orden"].astype(str).isin([ORDEN, ORDEN_ORIG])][["Fecha","Tipo","Monto","Orden","Motivo"]].to_string())
    print(historico[HDEST][historico[HDEST]["Orden"].astype(str) == ORDEN_DEST][["Fecha","Tipo","Monto","Orden","Motivo","Usuario"]].to_string())

    banner("5) INVARIANTES")
    s1, sd1 = saldo(historico[HOJA]), saldo(historico[HDEST])
    chk("Paula baja exactamente 1.760.000", abs((s1 - s0) + MONTO) < 0.01, f"{s0:,.2f} → {s1:,.2f}")
    chk("Laverde sube exactamente 1.760.000", abs((sd1 - sd0) - MONTO) < 0.01, f"{sd0:,.2f} → {sd1:,.2f}")
    def movs(df):
        return int((df["Tipo"].astype(str).str.strip().str.upper() != "TOTAL").sum())
    chk("Paula: +1 fila de movimiento (no se borra nada)", movs(historico[HOJA]) == movs(vivo[HOJA]) + 1)
    chk("la consignación original de Paula sigue intacta en 1.760.000", abs(float(pd.to_numeric(historico[HOJA].loc[historico[HOJA]["Orden"].astype(str).str.strip() == ORDEN, "Monto"]).sum()) - MONTO) < 0.01)
    chk("Laverde: +1 fila de movimiento", movs(historico[HDEST]) == movs(vivo[HDEST]) + 1)
    vac = {"", "nan", "none", "nat"}
    for h in (HOJA, HDEST):
        a = {y for y in vivo[h]["Orden"].astype(str).str.strip() if y.lower() not in vac}
        b = {y for y in historico[h]["Orden"].astype(str).str.strip() if y.lower() not in vac}
        chk(f"{h}: 0 Orden perdidos", not (a - b), f"{len(a - b)}")
    for hoja in vivo:
        if hoja in (HOJA, HDEST):
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

    if not ESCRIBIR:
        banner("DRY-RUN TERMINADO — 0 ESCRITURAS")
        print(f"  Para escribir: python3 mover_consignacion_laverde_20261008.py --escribir {md.rev}")
        return

    banner("7) SUBIDA A DROPBOX (capa C hace el respaldo)")
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

    banner("8) VALIDACIÓN POST-ESCRITURA")
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
    q = rel[HOJA]["Orden"].astype(str).str.strip().str.replace(".0", "", regex=False) == ORDEN
    qa = rel[HOJA]["Orden"].astype(str).str.strip() == ORDEN_ORIG
    chk2("11591: ajuste de -1.760.000 presente", int(qa.sum()) == 1 and abs(float(pd.to_numeric(rel[HOJA].loc[qa, "Monto"]).sum()) + MONTO) < 0.01)
    chk2("11591: saldo", abs(saldo(rel[HOJA]) - s1) < 0.01, f"COP {saldo(rel[HOJA]):,.2f}")
    qd = rel[HDEST]["Orden"].astype(str).str.strip() == ORDEN_DEST
    chk2("9680: fila de 1.760.000 presente", int(qd.sum()) == 1 and abs(float(pd.to_numeric(rel[HDEST].loc[qd, "Monto"]).sum()) - MONTO) < 0.01)
    chk2("9680: saldo", abs(saldo(rel[HDEST]) - sd1) < 0.01, f"COP {saldo(rel[HDEST]):,.2f}")
    for hoja in vivo:
        if hoja in (HOJA, HDEST):
            continue
        A, B = vivo[hoja], rel[hoja]
        igual = len(A) == len(B)
        if igual and "Monto" in A.columns:
            igual = abs(pd.to_numeric(A["Monto"], errors="coerce").fillna(0).sum() -
                        pd.to_numeric(B["Monto"], errors="coerce").fillna(0).sum()) < 0.01
        chk2(f"verbatim: {hoja}", igual, f"{len(A)} filas")

    banner("9) COPIA A ONEDRIVE")
    hoy = pd.Timestamp.today().strftime("%Y%m%d")
    destino = f"{ONEDRIVE_DIR}/{hoy}_Historico_mayoristas.xlsx"
    os.makedirs(ONEDRIVE_ANT, exist_ok=True)
    # ⚠️ COPIAR, no mover (el 18-sep OneDrive perdió un archivo movido): copiar, verificar
    # tamaño y solo entonces borrar/sobrescribir.
    for f in sorted(os.listdir(ONEDRIVE_DIR)):
        if f.endswith("_Historico_mayoristas.xlsx"):
            src_f = f"{ONEDRIVE_DIR}/{f}"
            dst_f = f"{ONEDRIVE_ANT}/{f.replace('.xlsx','')}_PRE_consignacion_laverde.xlsx"
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

    print(f"\n  {'✅ CONSIGNACIÓN MOVIDA DE 11591 A 9680 Y VERIFICADO' if ok2 else '🚨 REVISAR'}")
    print(f"  rev nueva: {md2.rev}\n  rollback:  {backup}")
    print("\n  ↩️  Para revertir: borrar las 2 filas traslado_20261007_1760000_* y recalcular, o restaurar el backup.")


if __name__ == "__main__":
    sys.path.insert(0, os.environ.get("HARNESS_DIR", "."))
    main()
