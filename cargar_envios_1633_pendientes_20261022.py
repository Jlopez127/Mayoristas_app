#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Carga en el histórico VIEJO 61 envíos de Nathalia Ospina (1633) que nunca se cobraron (COP 3.117.013).
Pedido por la sesión de Mayoristas 2.0 a nombre de Julián (22-oct-2026); la v2 ya los tiene (lote 19).

Por qué faltaban: en 1633 el envío se cobra cuando el backoffice ya lo modificó (recalcula USD, re-fija
TRM y BORRA el estado). El script de Henry solo trae los de estado vacío: si pasa a Enviado/Entregado
antes de que Henry corra, se pierde. 59 de oct-2026, el 94958 (jun-2026) y el 73111 (oct-2025).

Archivo: mayoristas-paridad/snapshots/20261022_Envios_mayoristas_1633_pendientes.xlsx (hoja "Mayoristas",
formato de Envios_mayoristas.xlsx, Monto = int(TRM×USD)). Se pasa por las MISMAS funciones de la app:
procesar_envios_mayoristas → concat con el histórico → aplicar_tarifa_minima_envios (14,4 USD × TRM
oficial+125, desde 4-ago-2026) → recalcular_totales_diarios. Cada monto final se compara con el de la v2.

  sin argumentos  -> dry-run (0 escrituras)
  --escribir REV  -> escribe a Dropbox (REV = rev del dry-run) y la copia de OneDrive
"""
import sys, os, io, shutil, warnings
warnings.filterwarnings("ignore")
import pandas as pd

ESCRIBIR = "--escribir" in sys.argv
REV_ESPERADA = sys.argv[sys.argv.index("--escribir") + 1] if ESCRIBIR else None
CAS = "1633"
ARCH = "/Users/julianlopez/mayoristas-paridad/snapshots/20261022_Envios_mayoristas_1633_pendientes.xlsx"
TOTAL_ESPERADO = 3117013
ONEDRIVE_DIR = "/Users/julianlopez/Library/CloudStorage/OneDrive-Personal/Historico Carga/Conciliacion/Mayoristas"
ONEDRIVE_ANT = f"{ONEDRIVE_DIR}/Antiguos"
# montos finales esperados (lote 19 de la v2, mensaje_dash_1633.txt)
_RANGOS = [((108265, 108265), 48021), ((108465, 108477), 48111), ((108644, 108675), 48440),
           ((108877, 108903), 47800), ((109253, 109258), 47800), ((109263, 109263), 52907),
           ((109267, 109267), 52727), ((109424, 109428), 47617), ((109596, 109616), 47846),
           ((109912, 110149), 47515), ((110279, 110281), 47251), ((110283, 110289), 47251),
           ((110282, 110282), 49747), ((94958, 94958), 46355), ((73111, 73111), 236607)]
_CUENTAS = {(108465, 108477): 2, (108644, 108675): 12, (108877, 108903): None, (109253, 109258): None,
            (109424, 109428): 2, (109596, 109616): 4, (109912, 110149): 15}
ESPERADO = {}


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
        print(f"  {'✔' if c else '🚨'} {n:<72} {det}")
        ok = ok and bool(c)
    print(f"MODO: {'🔴 ESCRITURA REAL' if ESCRIBIR else '🟢 DRY-RUN (0 escrituras)'}")

    banner("1) ARCHIVO DE ENVÍOS PENDIENTES")
    raw = pd.read_excel(ARCH, sheet_name="Mayoristas")
    print(f"  {len(raw)} filas · Σ Monto archivo {raw['Monto'].sum():,.0f}")
    nums = raw["Orden"].astype(str).str.extract(r"(\d+)")[0].astype(int)
    for (lo, hi), m in _RANGOS:
        for n in nums[(nums >= lo) & (nums <= hi)]:
            ESPERADO[f"Envio {n}"] = m
    chk("61 envíos únicos, todos CA1633, Egreso/Envio", len(raw) == 61 and raw["Orden"].nunique() == 61
        and set(raw["Casillero"].astype(str)) == {"1633"} and set(raw["Tipo"]) == {"Egreso"} and set(raw["Motivo"]) == {"Envio"})
    chk("cada envío tiene un monto esperado en el mensaje de la v2", len(ESPERADO) == 61)
    chk("grupos del mensaje: 16 en 108877-108903 + 109253-109258",
        int(((nums >= 108877) & (nums <= 108903)).sum() + ((nums >= 109253) & (nums <= 109258)).sum()) == 16)
    for (lo, hi), c in _CUENTAS.items():
        if c is not None:
            chk(f"grupo {lo}-{hi}: {c} envíos", int(((nums >= lo) & (nums <= hi)).sum()) == c)
    chk("Σ esperado = 3.117.013", sum(ESPERADO.values()) == TOTAL_ESPERADO, f"{sum(ESPERADO.values()):,}")
    nuevos = mod.procesar_envios_mayoristas(raw)
    chk("procesar_envios_mayoristas devuelve solo envios_1633, sin descartar filas",
        list(nuevos) == ["envios_1633"] and len(nuevos["envios_1633"]) == 61)
    dfn = nuevos["envios_1633"].copy()
    dfn["Fecha de Carga"] = pd.Timestamp.today().strftime("%Y-%m-%d")

    banner("2) HISTÓRICO VIVO FRESCO")
    cfg = mod.st.secrets["dropbox"]
    md = mod.dbx.files_get_metadata(cfg["remote_path"])
    _, res = mod.dbx.files_download(cfg["remote_path"])
    vivo = pd.read_excel(io.BytesIO(res.content), sheet_name=None)
    HOJA = next(h for h in vivo if h.split(" - ")[0].strip() == CAS)
    s0 = saldo(vivo[HOJA])
    print(f"  rev={md.rev}  modificado={md.server_modified}  size={md.size:,}")
    print(f"  '{HOJA}': saldo {s0:,.2f}")
    if ESCRIBIR and md.rev != REV_ESPERADA:
        raise SystemExit(f"⛔ ABORTA: el histórico se movió (esperaba {REV_ESPERADA}). Rehacer dry-run.")
    todas = set()
    for h, d in vivo.items():
        if "Orden" in d.columns:
            todas |= set(d["Orden"].astype(str).str.strip())
    chk("ninguno de los 61 está en NINGUNA hoja del histórico", not (set(ESPERADO) & todas),
        str(sorted(set(ESPERADO) & todas))[:200])

    banner("3) APLICAR (mismas funciones que la app)")
    historico = {k: mod.asegurar_columnas_historico(v.copy()) for k, v in vivo.items()}
    hist = historico[HOJA]
    comb = pd.concat([hist, dfn], ignore_index=True)
    comb["Orden"] = comb["Orden"].astype(str).str.strip().str.replace(".0", "", regex=False)
    antes_m = pd.to_numeric(comb["Monto"], errors="coerce").copy()
    comb, tm = mod.aplicar_tarifa_minima_envios(comb, CAS, usd=mod.TARIFA_MINIMA_ENVIO_USD[CAS],
                                                desde=mod.TARIFA_MINIMA_ENVIO_DESDE)
    print(f"  tarifa mínima: {tm['ajustadas']} subidos · +COP {tm['cop']:,.0f} · sin TRM: {tm['sin_trm']}")
    chk("tarifa mínima: ninguna fecha sin TRM", not tm["sin_trm"])
    desp_m = pd.to_numeric(comb["Monto"], errors="coerce")
    camb = set(comb.loc[(antes_m - desp_m).abs() > 0.001, "Orden"])
    chk("la tarifa mínima solo toca envíos de los 61 (los ya cargados no se mueven)", camb <= set(ESPERADO), str(sorted(camb - set(ESPERADO)))[:200])
    cm = comb.set_index("Orden")
    malos = [(o, float(cm.loc[o, "Monto"]), m) for o, m in ESPERADO.items() if abs(float(cm.loc[o, "Monto"]) - m) > 0.5]
    chk("los 61 montos finales = los de la v2, uno por uno", not malos, str(malos)[:400])
    tot = sum(float(cm.loc[o, "Monto"]) for o in ESPERADO)
    chk("Σ final = 3.117.013", abs(tot - TOTAL_ESPERADO) < 0.5, f"{tot:,.0f}")
    u = usuario_de_totales(vivo[HOJA]) or "Nathalia Ospina"
    historico[HOJA] = mod.recalcular_totales_diarios(comb, usuario=u, cas=cas_de_totales(vivo[HOJA], CAS))

    banner("4) INVARIANTES")
    s1 = saldo(historico[HOJA])
    chk("saldo baja exactamente 3.117.013", abs((s0 - s1) - TOTAL_ESPERADO) < 0.5, f"{s0:,.2f} → {s1:,.2f}")
    def movs(df):
        return int((df["Tipo"].astype(str).str.strip().str.upper() != "TOTAL").sum())
    chk("+61 filas de movimiento (no se borra nada)", movs(historico[HOJA]) == movs(vivo[HOJA]) + 61)
    def tots(df):
        t = df[df["Tipo"].astype(str).str.strip().str.upper() == "TOTAL"].copy()
        return t.assign(F=pd.to_datetime(t["Fecha"]).dt.normalize()).groupby("F")["Monto"].last().astype(float)
    ta, tb = tots(vivo[HOJA]), tots(historico[HOJA])
    comun = ta.index[ta.index < pd.Timestamp("2025-10-21")]
    chk("saldos diarios anteriores al 21-oct-2025 (envío más viejo) idénticos",
        (tb.reindex(comun) - ta.reindex(comun)).abs().max() < 0.01, f"{len(comun)} días")
    print(pd.DataFrame({"antes": ta, "después": tb}).tail(5).to_string(float_format=lambda v: f"{v:,.2f}"))
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
    _, tm2 = mod.aplicar_tarifa_minima_envios(historico[HOJA], CAS, usd=mod.TARIFA_MINIMA_ENVIO_USD[CAS],
                                              desde=mod.TARIFA_MINIMA_ENVIO_DESDE)
    chk("idempotente: una 2ª pasada de la tarifa mínima no cambia nada", tm2["ajustadas"] == 0)
    if not ok:
        raise SystemExit("⛔ ABORTA: falló alguna verificación.")

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
        print(f"  Para escribir: python3 cargar_envios_1633_pendientes_20261022.py --escribir {md.rev}")
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
    _rm = rel[HOJA].set_index(rel[HOJA]["Orden"].astype(str).str.strip())
    chk2("1633: los 61 envíos presentes una vez, con el monto esperado",
         all(int((rel[HOJA]["Orden"].astype(str).str.strip() == o).sum()) == 1 and abs(float(_rm.loc[o, "Monto"]) - m) < 0.5 for o, m in ESPERADO.items()))
    chk2("1633: saldo", abs(saldo(rel[HOJA]) - s1) < 0.01, f"COP {saldo(rel[HOJA]):,.2f}")
    for hoja in vivo:
        if hoja == HOJA:
            continue
        A, B = vivo[hoja], rel[hoja]
        igual = len(A) == len(B)
        if igual and "Monto" in A.columns:
            igual = abs(pd.to_numeric(A["Monto"], errors="coerce").fillna(0).sum() -
                        pd.to_numeric(B["Monto"], errors="coerce").fillna(0).sum()) < 0.01
        chk2(f"verbatim: {hoja}", igual, f"{len(A)} filas")

    banner("10) COPIA A ONEDRIVE")
    hoy = pd.Timestamp.today().strftime("%Y%m%d")
    destino = f"{ONEDRIVE_DIR}/{hoy}_Historico_mayoristas.xlsx"
    os.makedirs(ONEDRIVE_ANT, exist_ok=True)
    # ⚠️ COPIAR, no mover (el 18-sep OneDrive perdió un archivo movido): copiar, verificar
    # tamaño y solo entonces borrar/sobrescribir.
    for f in sorted(os.listdir(ONEDRIVE_DIR)):
        if f.endswith("_Historico_mayoristas.xlsx"):
            src_f = f"{ONEDRIVE_DIR}/{f}"
            dst_f = f"{ONEDRIVE_ANT}/{f.replace('.xlsx','')}_PRE_envios_1633_pendientes.xlsx"
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

    print(f"\n  {'✅ 61 ENVÍOS DE 1633 CARGADOS Y VERIFICADOS' if ok2 else '🚨 REVISAR'}")
    print(f"  rev nueva: {md2.rev}\n  rollback:  {backup}")


if __name__ == "__main__":
    sys.path.insert(0, os.environ.get("HARNESS_DIR", "."))
    main()
