#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Neutraliza en 1444 (MariaPaz Shop) 4 envíos del reclamo 2.1 ("PEC consolidados y cobrados
individualmente"): 110112, 110068, 110036 y 110680. Pedido de Julián del 30-oct-2026: "estos 4 los
puedes borrar". La tarifa por peso de 1444 filtra Motivo == "Envio", así que al cambiar el Motivo
no los vuelve a cobrar (se simula abajo).
Patrón COMPRAS_TC_PROPIA: Monto = 0 + Motivo, re-aplicado en cada corrida (el portal no lo puede revivir).

  sin argumentos  -> dry-run (0 escrituras)
  --escribir REV  -> escribe a Dropbox (REV = rev del dry-run) y refresca la copia de OneDrive
"""
import sys, os, io, shutil, warnings
warnings.filterwarnings("ignore")
import pandas as pd

ESCRIBIR = "--escribir" in sys.argv
REV_ESPERADA = sys.argv[sys.argv.index("--escribir") + 1] if ESCRIBIR else None
CAS = "1444"
OBJETIVO = ["Envio 110112", "Envio 110068", "Envio 110036", "Envio 110680"]
PREVIAS = []
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
            raise AssertionError(f"{_n} fue llamada — este script solo neutraliza filas")
        setattr(mod, fn, _boom)

    NUEVOS = {o: m for o, (c, m) in mod.COMPRAS_TC_PROPIA.items() if c == CAS and o in OBJETIVO}
    banner("1) REGLA EN EL CÓDIGO")
    chk("los 4 envíos en COMPRAS_TC_PROPIA para 1444", sorted(NUEVOS) == sorted(OBJETIVO), f"{len(NUEVOS)}")
    chk("ninguno es removible para el guard A", not any(mod._orden_removible(o) for o in NUEVOS))

    banner("2) HISTÓRICO VIVO FRESCO")
    cfg = mod.st.secrets["dropbox"]
    md = mod.dbx.files_get_metadata(cfg["remote_path"])
    _, res = mod.dbx.files_download(cfg["remote_path"])
    vivo = pd.read_excel(io.BytesIO(res.content), sheet_name=None)
    HOJA = next(h for h in vivo if h.split(" - ")[0].strip() == CAS and h.strip().endswith("Moises"))
    s0 = saldo(vivo[HOJA])
    o0 = vivo[HOJA]["Orden"].astype(str).str.strip()
    print(f"  rev={md.rev}  modificado={md.server_modified}  size={md.size:,}")
    print(f"  '{HOJA}': {len(vivo[HOJA])} filas · saldo COP {s0:,.2f}")
    if ESCRIBIR and md.rev != REV_ESPERADA:
        raise SystemExit(f"⛔ ABORTA: el histórico se movió (esperaba {REV_ESPERADA}). Rehacer dry-run.")

    banner("3) LAS FILAS")
    m_new = o0.str.replace(".0", "", regex=False).isin(NUEVOS)
    m_old = o0.isin(PREVIAS)
    chk("cada envío existe una sola vez, como Egreso de Envio", int(m_new.sum()) == len(OBJETIVO)
        and sorted(o0[m_new].str.replace(".0", "", regex=False)) == sorted(OBJETIVO)
        and (vivo[HOJA].loc[m_new, "Tipo"].astype(str).str.strip() == "Egreso").all()
        and (vivo[HOJA].loc[m_new, "Motivo"].astype(str).str.strip() == "Envio").all(), f"{int(m_new.sum())}")
    print(vivo[HOJA][m_new | m_old][["Fecha", "Tipo", "Orden", "Monto", "Motivo"]].to_string())
    monto_new = float(pd.to_numeric(vivo[HOJA].loc[m_new, "Monto"]).sum())
    monto_old = float(pd.to_numeric(vivo[HOJA].loc[m_old, "Monto"]).sum())
    print(f"  Σ a neutralizar COP {monto_new:,.2f}")
    chk("COP = 36.296 + 36.296 + 36.296 + 36.481 = 145.369", abs(monto_new - 145369) < 1)
    if not ok:
        raise SystemExit("⛔ ABORTA.")

    banner("4) NEUTRALIZAR (con la función del generador)")
    historico = {k: mod.asegurar_columnas_historico(v.copy()) for k, v in vivo.items()}
    d = mod._neutralizar_compras_tc_propia(historico[HOJA], CAS)
    d.loc[d["Orden"].astype(str).str.strip().str.replace(".0","",regex=False).isin(NUEVOS), "Fecha de Carga"] = pd.Timestamp.today().strftime("%Y-%m-%d")
    u, c = usuario_de_totales(vivo[HOJA]), cas_de_totales(vivo[HOJA], CAS)
    historico[HOJA] = mod.recalcular_totales_diarios(d, usuario=u, cas=c)
    print(historico[HOJA][historico[HOJA]["Orden"].astype(str).str.strip().str.replace(".0","",regex=False).isin(NUEVOS)][["Fecha","Orden","Monto","Motivo"]].to_string())

    banner("5) INVARIANTES")
    dd = historico[HOJA]
    o1 = dd["Orden"].astype(str).str.strip().str.replace(".0", "", regex=False)
    chk("siguen existiendo los 4", int(o1.isin(NUEVOS).sum()) == len(OBJETIVO))
    chk("quedó en Monto 0", float(pd.to_numeric(dd.loc[o1.isin(NUEVOS), "Monto"]).abs().sum()) == 0)
    chk("no se borró ni se agregó ninguna fila", len(dd) == len(vivo[HOJA]), f"{len(vivo[HOJA])} → {len(dd)}")
    def movs(df):
        x = df.copy(); x["_o"] = x["Orden"].astype(str).str.strip().str.replace(".0", "", regex=False)
        x = x[(x["Tipo"].astype(str).str.strip().str.upper() != "TOTAL") & ~x["_o"].isin(NUEVOS)]
        return pd.to_numeric(x["Monto"], errors="coerce").fillna(0).sum(), len(x)
    sa, na = movs(vivo[HOJA]); sb, nb = movs(dd)
    chk("ningún otro movimiento de 1444 cambió", abs(sa - sb) < 0.01 and na == nb, f"Σ {sa:,.2f} vs {sb:,.2f}")
    vac = {"", "nan", "none", "nat"}
    perd = {y for y in o0 if y.lower() not in vac} - {y for y in o1 if y.lower() not in vac}
    chk("0 Orden previos perdidos", not perd, f"{len(perd)}")
    for hoja in vivo:
        if hoja == HOJA:
            continue
        A, B = vivo[hoja], historico[hoja]
        igual = len(A) == len(B)
        if igual and "Monto" in A.columns:
            igual = abs(pd.to_numeric(A["Monto"], errors="coerce").fillna(0).sum() -
                        pd.to_numeric(B["Monto"], errors="coerce").fillna(0).sum()) < 0.01
        chk(f"verbatim: {hoja}", igual, f"{len(A)} filas")

    banner("5b) SIMULACIÓN DE LA PRÓXIMA CORRIDA (el portal los vuelve a traer)")
    entrantes = vivo[HOJA][m_new].copy()           # tal como vienen del archivo de envíos: con monto
    comb = pd.concat([dd, entrantes], ignore_index=True)
    comb = mod._neutralizar_compras_tc_propia(comb, CAS)
    comb, _tp = mod.aplicar_tarifa_envio_por_peso(comb, CAS, conf=mod.TARIFA_ENVIO_1444, desde=mod.TARIFA_ENVIO_1444_DESDE)
    eg = comb[comb["Tipo"].astype(str).str.strip() == "Egreso"]
    eg = eg.drop_duplicates(subset=["Orden"], keep="last")
    chk("tras neutralizar + tarifa por peso + dedup keep=last, siguen en 0",
        float(pd.to_numeric(eg.loc[eg["Orden"].astype(str).str.strip().str.replace(".0","",regex=False).isin(NUEVOS), "Monto"]).abs().sum()) == 0)
    chk("otro casillero NO se ve afectado (regla gateada)",
        mod._neutralizar_compras_tc_propia(entrantes.copy(), "11591")["Monto"].sum() == entrantes["Monto"].sum())
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

    banner("6) SALDO")
    s1 = saldo(historico[HOJA])
    print(f"  1444: COP {s0:>16,.2f} → {s1:>16,.2f}   Δ {s1-s0:>+14,.2f}")
    chk("el saldo SUBE exactamente lo neutralizado", abs((s1 - s0) - monto_new) < 1)
    MONTO_ACTUAL = monto_new
    ORDEN = None

    if not ESCRIBIR:
        banner("DRY-RUN TERMINADO — 0 ESCRITURAS")
        print(f"  Para escribir: python3 neutralizar_envios_consolidados_1444_20261030.py --escribir {md.rev}")
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
    q = rel[HOJA]["Orden"].astype(str).str.strip().str.replace(".0", "", regex=False)
    chk2("siguen ahí los 4", int(q.isin(NUEVOS).sum()) == len(OBJETIVO))
    chk2("Monto = 0", float(pd.to_numeric(rel[HOJA].loc[q.isin(NUEVOS), "Monto"]).abs().sum()) == 0)
    chk2("saldo 1444", abs(saldo(rel[HOJA]) - s1) < 0.01, f"COP {saldo(rel[HOJA]):,.2f}")
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

    banner("9) COPIA A ONEDRIVE")
    hoy = pd.Timestamp.today().strftime("%Y%m%d")
    destino = f"{ONEDRIVE_DIR}/{hoy}_Historico_mayoristas.xlsx"
    os.makedirs(ONEDRIVE_ANT, exist_ok=True)
    # ⚠️ COPIAR, no mover (el 18-sep OneDrive perdió un archivo movido): copiar, verificar
    # tamaño y solo entonces borrar/sobrescribir.
    for f in sorted(os.listdir(ONEDRIVE_DIR)):
        if f.endswith("_Historico_mayoristas.xlsx"):
            src_f = f"{ONEDRIVE_DIR}/{f}"
            dst_f = f"{ONEDRIVE_ANT}/{f.replace('.xlsx','')}_PRE_neutralizar_consolidados_1444.xlsx"
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

    print(f"\n  {'✅ NEUTRALIZACIÓN APLICADA Y VERIFICADO' if ok2 else '🚨 REVISAR'}")
    print(f"  rev nueva: {md2.rev}\n  rollback:  {backup}")
    print("\n  ↩️  Para revertir: quitar los 4 envíos de COMPRAS_TC_PROPIA y restaurar el backup.")


if __name__ == "__main__":
    sys.path.insert(0, os.environ.get("HARNESS_DIR", "."))
    main()
