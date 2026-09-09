#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Aplica al histórico la corrección del SUBCOBRO POR ERROR DE CANTIDAD del portal 3.0.

Usa la MISMA función que corre dentro de la app (`_corregir_subcobro_cantidad`), no una
réplica: así lo que se escribe hoy es idéntico a lo que la app volverá a aplicar en cada
corrida. Del correo de Pablo Agudelo (2026-09-08) hay 13 órdenes; 8 ya están en el histórico
y son las que se corrigen aquí. Las otras 5 (168066, 168362, 168378, 168380 y la pendiente
168510) todavía no han entrado: cuando lleguen, la regla ya las espera en el diccionario.

  sin argumentos  -> dry-run (0 escrituras)
  --escribir      -> escribe a Dropbox y refresca la copia de OneDrive
"""
import sys, os, io, warnings
warnings.filterwarnings("ignore")
import pandas as pd

ESCRIBIR = "--escribir" in sys.argv

CASILLEROS = ["9444", "9680", "14825"]
REV_ESPERADA = "0165b0c0759d11500000002f34b3f21"
# las 8 que ya están en el histórico, y su valor esperado tras la corrección
# Tras la corrida del 9-sep las 13 órdenes están en el histórico, todas con el valor del bug.
# El valor correcto se deriva del propio diccionario (USD debido x TRM de la fila), así que aquí
# solo se fija cuántas se esperan y el delta total por casillero.
N_ESPERADAS = {"9444": 5, "9680": 6, "14825": 2}
DELTA_ESPERADO = {"9444": 15991693, "9680": 19889581, "14825": 3361204}

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
        print(f"  {'✔' if c else '🚨'} {n:<64} {det}")
        ok = ok and bool(c)

    print(f"MODO: {'🔴 ESCRITURA REAL' if ESCRIBIR else '🟢 DRY-RUN (0 escrituras)'}")

    # 🧯 este script no carga tarjetas, ni egresos, ni incentivos: solo aplica la regla
    for fn in ("procesar_egresos", "procesar_amex", "procesar_rakuten", "procesar_robinhood",
               "procesar_capital", "procesar_usbank", "procesar_intuit", "agregar_incentivo_amex"):
        def _boom(*a, _n=fn, **k):
            raise AssertionError(f"{_n} fue llamada — este script solo aplica la regla")
        setattr(mod, fn, _boom)

    banner("0) LA REGLA QUE SE VA A APLICAR")
    print(f"  SUBCOBRO_CANTIDAD_30: {len(mod.SUBCOBRO_CANTIDAD_30)} órdenes")
    for o, (c, u) in sorted(mod.SUBCOBRO_CANTIDAD_30.items()):
        print(f"    {o}  cas {c:<6} USD debido {u:>10,.2f}")
    chk("son 13 órdenes (las del correo)", len(mod.SUBCOBRO_CANTIDAD_30) == 13)

    banner("1) HISTÓRICO VIVO FRESCO")
    cfg = mod.st.secrets["dropbox"]
    md = mod.dbx.files_get_metadata(cfg["remote_path"])
    _, res = mod.dbx.files_download(cfg["remote_path"])
    vivo = pd.read_excel(io.BytesIO(res.content), sheet_name=None)
    HOJA = {c: next(h for h in vivo if h.split(" - ")[0].strip() == c) for c in CASILLEROS}
    print(f"  rev={md.rev}  modificado={md.server_modified}  size={md.size:,}  hojas={len(vivo)}")
    if md.rev != REV_ESPERADA:
        print(f"  ⚠️  el histórico se movió (esperaba {REV_ESPERADA})")
        if ESCRIBIR:
            raise SystemExit("⛔ ABORTA: rehacer el dry-run con el histórico fresco.")
    else:
        print("  ✔ rev idéntica a la del dry-run")
    s0, o0 = {}, {}
    for c, h in HOJA.items():
        s0[c] = saldo(vivo[h])
        o0[c] = vivo[h]["Orden"].astype(str).str.strip()
        print(f"  {h:<32} {len(vivo[h]):>6} filas · saldo COP {s0[c]:>15,.2f}")

    banner("2) APLICAR LA REGLA (la misma función de la app)")
    historico = {k: mod.asegurar_columnas_historico(v.copy()) for k, v in vivo.items()}
    for c, h in HOJA.items():
        d = historico[h]
        antes_montos = pd.to_numeric(d["Monto"], errors="coerce").fillna(0)
        d = mod._corregir_subcobro_cantidad(d, c)
        for niv, m in harness.drenar():
            print(f"    [{niv}] {m}")
        desp_montos = pd.to_numeric(d["Monto"], errors="coerce").fillna(0)
        delta = float(desp_montos.sum() - antes_montos.sum())
        chk(f"{c}: delta de Monto = {DELTA_ESPERADO[c]:,}",
            abs(delta - DELTA_ESPERADO[c]) < 2, f"{delta:,.0f}")
        chk(f"{c}: no se crearon ni borraron filas", len(d) == len(vivo[h]),
            f"{len(vivo[h])} → {len(d)}")
        # cada orden del diccionario que esté en la hoja quedó en USD debido x TRM
        norm = d["Orden"].astype(str).str.strip()
        mias = [o for o, (cc, _u) in mod.SUBCOBRO_CANTIDAD_30.items()
                if cc == c and (norm == o).any()]
        chk(f"{c}: {N_ESPERADAS[c]} órdenes del diccionario presentes",
            len(mias) == N_ESPERADAS[c], f"{len(mias)}: {sorted(mias)}")
        for orden in sorted(mias):
            usd = mod.SUBCOBRO_CANTIDAD_30[orden][1]
            fila = d.loc[norm == orden]
            trm = float(pd.to_numeric(fila["TRM"]).iloc[0])
            v = float(pd.to_numeric(fila["Monto"]).iloc[0])
            chk(f"    {orden} = USD {usd:,.2f} x TRM {trm:,.2f}", abs(v - round(usd*trm, 2)) <= 2,
                f"COP {v:,.0f}")
        # nada fuera del diccionario se movió
        otras = ~norm.isin(mod.SUBCOBRO_CANTIDAD_30)
        chk(f"{c}: las {int(otras.sum())} filas ajenas quedan idénticas",
            abs(float(antes_montos[otras.values].sum() - desp_montos[otras.values].sum())) < 0.01)
        u, cc = usuario_de_totales(vivo[h]), cas_de_totales(vivo[h], c)
        historico[h] = mod.recalcular_totales_diarios(d, usuario=u, cas=cc)

    banner("3) INVARIANTES")
    vac = {"", "nan", "none", "nat"}
    for c, h in HOJA.items():
        o1 = historico[h]["Orden"].astype(str).str.strip()
        perd = {y for y in o0[c] if y.lower() not in vac} - {y for y in o1 if y.lower() not in vac}
        chk(f"{c}: 0 Orden previos perdidos", not perd, f"{len(perd)}")
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

    banner("4) SALDOS")
    s1 = {}
    for c, h in HOJA.items():
        s1[c] = saldo(historico[h])
        print(f"  {h:<32} COP {s0[c]:>15,.2f} → {s1[c]:>15,.2f}   Δ {s1[c]-s0[c]:>+13,.2f}")

    if not ESCRIBIR:
        banner("DRY-RUN TERMINADO — 0 ESCRITURAS")
        print("  Para escribir: python3 aplicar_subcobro_20260908.py --escribir")
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
    for c, h in HOJA.items():
        q = rel[h]["Orden"].astype(str).str.strip()
        chk2(f"{c}: saldo", abs(saldo(rel[h]) - s1[c]) < 0.01, f"COP {saldo(rel[h]):,.2f}")
        for orden, (cc, usd) in sorted(mod.SUBCOBRO_CANTIDAD_30.items()):
            if cc != c or not (q == orden).any():
                continue
            fila = rel[h].loc[q == orden]
            trm = float(pd.to_numeric(fila["TRM"]).iloc[0])
            v = float(pd.to_numeric(fila["Monto"]).iloc[0])
            chk2(f"  {orden} = USD {usd:,.2f} x TRM {trm:,.2f}",
                 abs(v - round(usd*trm, 2)) <= 2, f"COP {v:,.0f}")
        chk2(f"{c}: 0 Orden previos perdidos",
             not ({y for y in o0[c] if y.lower() not in vac} - {y for y in q if y.lower() not in vac}))
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
    import shutil
    hoy = pd.Timestamp.today().strftime("%Y%m%d")
    destino = f"{ONEDRIVE_DIR}/{hoy}_Historico_mayoristas.xlsx"
    os.makedirs(ONEDRIVE_ANT, exist_ok=True)
    for f in sorted(os.listdir(ONEDRIVE_DIR)):
        if f.endswith("_Historico_mayoristas.xlsx") and f != os.path.basename(destino):
            shutil.move(f"{ONEDRIVE_DIR}/{f}",
                        f"{ONEDRIVE_ANT}/{f.replace('.xlsx','')}_PRE_subcobro2.xlsx")
            print(f"  archivada: {f}")
    with open(destino, "wb") as fh:
        fh.write(contenido)
    print(f"  escrita:   {destino}  ({os.path.getsize(destino):,} bytes)")
    chk2("la copia de OneDrive pesa lo mismo que el vivo",
         os.path.getsize(destino) == md2.size, f"{os.path.getsize(destino):,} vs {md2.size:,}")

    print(f"\n  {'✅ CORRECCIÓN APLICADA Y VERIFICADA' if ok2 else '🚨 REVISAR'}")
    print(f"  rev nueva: {md2.rev}\n  rollback:  {backup}")
    print("\n  ℹ️  Las otras 5 órdenes del correo entrarán ya corregidas cuando lleguen al reporte.")


if __name__ == "__main__":
    sys.path.insert(0, os.environ.get("HARNESS_DIR", "."))
    main()
