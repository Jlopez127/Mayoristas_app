#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Aplica al histórico la anulación de la orden 170314 de 11591 (agotada, recomprada como 170541).

Usa la MISMA función que corre dentro de la app (`_neutralizar_compras_tc_propia`), no una
réplica: lo que se escribe hoy es idéntico a lo que la app volverá a aplicar en cada corrida.
Solo toca la hoja de 11591: Monto de la 170314 -> 0 + Motivo, y recalcula sus Totales.

  sin argumentos  -> dry-run (0 escrituras)
  --escribir      -> escribe a Dropbox y reemplaza la copia de OneDrive
"""
import sys, os, io, warnings
warnings.filterwarnings("ignore")
import pandas as pd

ESCRIBIR = "--escribir" in sys.argv

CAS = "11591"
ORDEN = "170314"
MONTO_ANTES = 2771521.8
FECHA_ORDEN = pd.Timestamp("2026-09-14")
REV_ESPERADA = os.environ.get("REV_ESPERADA", "")

ONEDRIVE_DIR = "/Users/julianlopez/Library/CloudStorage/OneDrive-Personal/Historico Carga/Conciliacion/Mayoristas"
ONEDRIVE_ANT = f"{ONEDRIVE_DIR}/Antiguos"


def saldo(d):
    t = d[d["Tipo"].astype(str).str.strip().str.upper() == "TOTAL"]
    return float(pd.to_numeric(t["Monto"], errors="coerce").iloc[-1]) if len(t) else float("nan")


def totales(d):
    t = d[d["Tipo"].astype(str).str.strip().str.upper() == "TOTAL"].copy()
    t["_f"] = pd.to_datetime(t["Fecha"], errors="coerce")
    t["_m"] = pd.to_numeric(t["Monto"], errors="coerce")
    return t.groupby("_f")["_m"].last()


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
        print(f"  {'✔' if c else '🚨'} {n:<64} {det}")
        ok = ok and bool(c)

    print(f"MODO: {'🔴 ESCRITURA REAL' if ESCRIBIR else '🟢 DRY-RUN (0 escrituras)'}")

    # 🧯 este script no carga tarjetas, ni egresos, ni incentivos: solo aplica la regla
    for fn in ("procesar_egresos", "procesar_amex", "procesar_rakuten", "procesar_robinhood",
               "procesar_capital", "procesar_usbank", "procesar_intuit", "procesar_discover",
               "agregar_incentivo_amex"):
        def _boom(*a, _n=fn, **k):
            raise AssertionError(f"{_n} fue llamada — este script solo aplica la regla")
        setattr(mod, fn, _boom)

    banner("0) LA REGLA QUE SE VA A APLICAR")
    chk(f"{ORDEN} está en COMPRAS_TC_PROPIA para {CAS}",
        mod.COMPRAS_TC_PROPIA.get(ORDEN, ("", ""))[0] == CAS, str(mod.COMPRAS_TC_PROPIA.get(ORDEN)))

    banner("1) HISTÓRICO VIVO FRESCO")
    cfg = mod.st.secrets["dropbox"]
    md = mod.dbx.files_get_metadata(cfg["remote_path"])
    _, res = mod.dbx.files_download(cfg["remote_path"])
    vivo = pd.read_excel(io.BytesIO(res.content), sheet_name=None)
    HOJA = next(h for h in vivo if h.split(" - ")[0].strip() == CAS)
    print(f"  rev={md.rev}  modificado={md.server_modified}  size={md.size:,}  hojas={len(vivo)}")
    if ESCRIBIR and md.rev != REV_ESPERADA:
        raise SystemExit(f"⛔ ABORTA: el histórico se movió (esperaba {REV_ESPERADA!r}). "
                         f"Rehacer el dry-run con el histórico fresco.")
    s0 = saldo(vivo[HOJA])
    o0 = vivo[HOJA]["Orden"].astype(str).str.strip()
    print(f"  {HOJA:<32} {len(vivo[HOJA]):>6} filas · saldo COP {s0:>15,.2f}")

    banner("2) APLICAR LA REGLA (la misma función de la app)")
    historico = {k: mod.asegurar_columnas_historico(v.copy()) for k, v in vivo.items()}
    d = historico[HOJA]
    norm = d["Orden"].astype(str).str.strip().str.replace(".0", "", regex=False)
    antes = pd.to_numeric(d["Monto"], errors="coerce").fillna(0)
    fila0 = d.loc[norm == ORDEN]
    chk(f"{ORDEN} existe una sola vez, como Egreso",
        len(fila0) == 1 and str(fila0["Tipo"].iloc[0]).strip().upper() == "EGRESO", f"{len(fila0)} fila(s)")
    chk(f"{ORDEN} vale hoy COP {MONTO_ANTES:,.2f}",
        len(fila0) == 1 and abs(float(pd.to_numeric(fila0["Monto"]).iloc[0]) - MONTO_ANTES) < 0.01)
    d = mod._neutralizar_compras_tc_propia(d, CAS)
    for niv, m in harness.drenar():
        print(f"    [{niv}] {m}")
    desp = pd.to_numeric(d["Monto"], errors="coerce").fillna(0)
    chk("delta de Monto = -MONTO_ANTES", abs(float(desp.sum() - antes.sum()) + MONTO_ANTES) < 0.01,
        f"{float(desp.sum() - antes.sum()):,.2f}")
    chk("no se crearon ni borraron filas", len(d) == len(vivo[HOJA]), f"{len(vivo[HOJA])} → {len(d)}")
    f1 = d.loc[norm == ORDEN]
    chk(f"{ORDEN} queda en 0 con su Motivo", float(f1["Monto"].iloc[0]) == 0,
        f"Motivo = {f1['Motivo'].iloc[0]!r}")
    otras = (norm != ORDEN).values
    chk("las demás filas quedan idénticas",
        abs(float(antes[otras].sum() - desp[otras].sum())) < 0.01, f"{int(otras.sum())} filas")
    historico[HOJA] = mod.recalcular_totales_diarios(d, usuario=usuario_de_totales(vivo[HOJA]),
                                                     cas=cas_de_totales(vivo[HOJA], CAS))

    banner("3) INVARIANTES")
    vac = {"", "nan", "none", "nat"}
    o1 = historico[HOJA]["Orden"].astype(str).str.strip()
    perd = {y for y in o0 if y.lower() not in vac} - {y for y in o1 if y.lower() not in vac}
    chk(f"{CAS}: 0 Orden previos perdidos", not perd, f"{len(perd)}")
    t0, t1 = totales(vivo[HOJA]), totales(historico[HOJA])
    chk("mismos días con Total", list(t0.index) == list(t1.index), f"{len(t0)} → {len(t1)}")
    dt = (t1 - t0.reindex(t1.index)).round(2)
    chk("Totales ANTES del 14-sep idénticos", (dt[dt.index < FECHA_ORDEN].abs() < 0.01).all(),
        f"{int((dt[dt.index < FECHA_ORDEN].abs() >= 0.01).sum())} distintos")
    chk(f"Totales DESDE el 14-sep suben exactamente {MONTO_ANTES:,.2f}",
        (abs(dt[dt.index >= FECHA_ORDEN] - MONTO_ANTES) < 0.01).all(),
        f"{int((dt.index >= FECHA_ORDEN).sum())} días")
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

    banner("4) SALDO")
    s1 = saldo(historico[HOJA])
    print(f"  {HOJA:<32} COP {s0:>15,.2f} → {s1:>15,.2f}   Δ {s1-s0:>+13,.2f}")

    if not ESCRIBIR:
        banner("DRY-RUN TERMINADO — 0 ESCRITURAS")
        print(f"  Para escribir: REV_ESPERADA={md.rev} python3 anular_170314_20260918.py --escribir")
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
    chk2("hay respaldo capa C", bool(backup), str(backup))
    q = rel[HOJA]["Orden"].astype(str).str.strip().str.replace(".0", "", regex=False)
    chk2(f"{CAS}: saldo", abs(saldo(rel[HOJA]) - s1) < 0.01, f"COP {saldo(rel[HOJA]):,.2f}")
    fr = rel[HOJA].loc[q == ORDEN]
    chk2(f"{ORDEN} = 0 en Dropbox", len(fr) == 1 and float(pd.to_numeric(fr["Monto"]).iloc[0]) == 0,
         f"Motivo = {fr['Motivo'].iloc[0]!r}" if len(fr) else "no está")
    chk2(f"{CAS}: 0 Orden previos perdidos",
         not ({y for y in o0 if y.lower() not in vac} - {y for y in rel[HOJA]["Orden"].astype(str).str.strip() if y.lower() not in vac}))
    for hoja in vivo:
        if hoja == HOJA:
            continue
        A, B = vivo[hoja], rel[hoja]
        igual = len(A) == len(B)
        if igual and "Monto" in A.columns:
            igual = abs(pd.to_numeric(A["Monto"], errors="coerce").fillna(0).sum() -
                        pd.to_numeric(B["Monto"], errors="coerce").fillna(0).sum()) < 0.01
        chk2(f"verbatim: {hoja}", igual, f"{len(A)} filas")

    banner("7) COPIA A ONEDRIVE (reemplaza la que hay; la anterior va a Antiguos)")
    import shutil
    hoy = pd.Timestamp.today().strftime("%Y%m%d")
    destino = f"{ONEDRIVE_DIR}/{hoy}_Historico_mayoristas.xlsx"
    os.makedirs(ONEDRIVE_ANT, exist_ok=True)
    # ⚠️ COPIAR, no mover: el 18-sep un shutil.move a Antiguos seguido de escribir el nuevo en
    # la misma ruta hizo que OneDrive (File Provider) perdiera el archivo movido. Se copia, se
    # comprueba que la copia existe con el mismo tamaño, y solo entonces se sobrescribe.
    for f in sorted(os.listdir(ONEDRIVE_DIR)):
        if f.endswith("_Historico_mayoristas.xlsx"):
            src = f"{ONEDRIVE_DIR}/{f}"
            dst = f"{ONEDRIVE_ANT}/{f.replace('.xlsx', '')}_PRE_anular170314.xlsx"
            if os.path.exists(dst):
                raise SystemExit(f"⛔ ya existe {dst}; no se pisa. Revisar a mano.")
            shutil.copy2(src, dst)
            if not (os.path.exists(dst) and os.path.getsize(dst) == os.path.getsize(src)):
                raise SystemExit(f"⛔ la copia a Antiguos no quedó bien; NO se reemplaza {f}.")
            if f != os.path.basename(destino):
                os.remove(src)
            print(f"  archivada: {f} → Antiguos/{os.path.basename(dst)}")
    with open(destino, "wb") as fh:
        fh.write(contenido)
    print(f"  escrita:   {destino}  ({os.path.getsize(destino):,} bytes)")
    chk2("la copia de OneDrive pesa lo mismo que el vivo",
         os.path.getsize(destino) == md2.size, f"{os.path.getsize(destino):,} vs {md2.size:,}")

    print(f"\n  {'✅ ANULACIÓN APLICADA Y VERIFICADA' if ok2 else '🚨 REVISAR'}")
    print(f"  rev nueva: {md2.rev}\n  rollback:  {backup}")


if __name__ == "__main__":
    sys.path.insert(0, os.environ.get("HARNESS_DIR", "."))
    main()
