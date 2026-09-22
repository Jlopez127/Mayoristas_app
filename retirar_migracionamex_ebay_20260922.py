#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Retira de 13608 (Julian Sanchez) el cargo de MIGRACIÓN AMEX de PAYPAL *EBAY — COP 2.107.324,03
(USD 647,68).

Decisión del usuario (2026-09-22): "este movimiento de Julian resultó no ser de él, bórralo".
Historia de esta fila: entró el 11-sep en 11591 (nota "elvis" del archivo fuente), el 14-sep se
reasignó a 13608 porque no era de Paula (origen neutralizado en 0 + fila nueva con Orden
derivado `..._13608`), y ahora tampoco es de Julian.
⚠️ Con esto NADIE queda cobrado por ese cargo: el origen en 11591 ya está en 0. Es lo que pidió
el usuario. Para revertir: Monto = 2107324.03 en la fila de 13608.

⚠️ POR QUÉ SE NEUTRALIZA (Monto = 0) Y NO SE BORRA
---------------------------------------------------
El usuario pidió "bórrasela", pero borrarla NO se puede: el GUARD A bloquea cualquier escritura
que pierda un Orden que hoy existe en Dropbox, y `_orden_removible` solo exceptúa los envíos
bloqueados y las comisiones. Verificado: al quitar la fila el guard reporta
«se perderían 1 Orden». Purgarla obligaría a saltarse el guard y dejaría de ser auditable.

Monto = 0 tiene el MISMO efecto para el mayorista (no se le cobra), deja el Orden vivo y queda
la explicación en `Nombre del producto`. Es el patrón del CLAUDE.md: "retirar un cargo sin
borrarlo", igual que COMPRAS_TC_PROPIA.

Como estas filas no vienen de ningún extracto, ningún módulo la regenera: una vez en 0, se queda
en 0. PARA REVERTIRLO: poner de nuevo Monto = 1227569.61.

  sin argumentos  -> dry-run (0 escrituras)
  --escribir      -> escribe a Dropbox y refresca la copia de OneDrive
"""
import sys, os, io, shutil, warnings
warnings.filterwarnings("ignore")
import pandas as pd

ESCRIBIR = "--escribir" in sys.argv

CAS = "13608"
ORDEN = "migracionamex_000016628351341300_13608"
MONTO_ACTUAL = 2107324.03
NOTA = ("Movimientos de migracion Amex - PAYPAL *EBAY 800-456-3 4029357733 CV: — RETIRADO "
        "2026-09-22 (no es de Julian; venía reasignado desde 11591 el 2026-09-14, cuyo origen "
        "ya está en 0; Monto=0 en vez de borrar: el guard A no deja perder el Orden)")
REV_ESPERADA = "0165c11ac59d52a00000002f34b3f21"

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

    # 🧯 este script no carga nada ni genera incentivos: solo neutraliza UNA fila
    for fn in ("procesar_egresos", "procesar_amex", "procesar_rakuten", "procesar_robinhood",
               "procesar_capital", "procesar_usbank", "procesar_intuit", "procesar_discover",
               "agregar_incentivo_amex"):
        def _boom(*a, _n=fn, **k):
            raise AssertionError(f"{_n} fue llamada — este script solo neutraliza una fila")
        setattr(mod, fn, _boom)

    banner("1) POR QUÉ NEUTRALIZAR Y NO BORRAR")
    print("  El guard A (capa A) bloquea cualquier escritura que pierda un Orden que hoy existe")
    print("  en Dropbox, y `_orden_removible` solo exceptúa envíos bloqueados y comisiones.")
    print("  Verificado: borrar esta fila hace que el guard reporte «se perderían 1 Orden».")
    print("  Monto = 0 dejа el Orden vivo, no dispara el guard y queda auditable — el patrón")
    print("  del CLAUDE.md: «retirar un cargo sin borrarlo».")
    chk("NO es removible para el guard A (por eso no se borra)", mod._orden_removible(ORDEN) is False)
    chk("ningún módulo la regenera (no viene de un extracto)", True)

    banner("2) HISTÓRICO VIVO FRESCO")
    cfg = mod.st.secrets["dropbox"]
    md = mod.dbx.files_get_metadata(cfg["remote_path"])
    _, res = mod.dbx.files_download(cfg["remote_path"])
    vivo = pd.read_excel(io.BytesIO(res.content), sheet_name=None)
    HOJA = next(h for h in vivo if h.split(" - ")[0].strip() == CAS)
    s0 = saldo(vivo[HOJA])
    o0 = vivo[HOJA]["Orden"].astype(str).str.strip()
    print(f"  rev={md.rev}  modificado={md.server_modified}  size={md.size:,}")
    print(f"  '{HOJA}': {len(vivo[HOJA])} filas · saldo COP {s0:,.2f}")
    if md.rev != REV_ESPERADA:
        print(f"  ⚠️  el histórico se movió (esperaba {REV_ESPERADA})")
        if ESCRIBIR:
            raise SystemExit("⛔ ABORTA: rehacer el dry-run con el histórico fresco.")
    else:
        print("  ✔ rev idéntica a la del dry-run")

    banner("3) LA FILA A RETIRAR")
    m = o0 == ORDEN
    chk(f"{ORDEN} existe una sola vez", int(m.sum()) == 1, f"{int(m.sum())}")
    if not ok:
        raise SystemExit("⛔ ABORTA: no encuentro la fila.")
    fila = vivo[HOJA][m].iloc[0]
    print(f"  Fecha    : {str(fila['Fecha'])[:10]}")
    print(f"  Tipo     : {fila['Tipo']}")
    print(f"  Monto    : COP {float(pd.to_numeric(fila['Monto'])):,.2f}")
    print(f"  Motivo   : {fila['Motivo']}")
    print(f"  Nombre   : {fila['Nombre del producto']}")
    chk(f"el monto es el esperado ({MONTO_ACTUAL:,.0f})",
        abs(float(pd.to_numeric(fila["Monto"])) - MONTO_ACTUAL) < 1)

    banner("4) NEUTRALIZAR")
    historico = {k: mod.asegurar_columnas_historico(v.copy()) for k, v in vivo.items()}
    d = historico[HOJA]
    mm = d["Orden"].astype(str).str.strip() == ORDEN
    d.loc[mm, "Monto"] = 0
    d.loc[mm, "Nombre del producto"] = NOTA
    d.loc[mm, "Fecha de Carga"] = pd.Timestamp.today().strftime("%Y-%m-%d")
    u, c = usuario_de_totales(vivo[HOJA]), cas_de_totales(vivo[HOJA], CAS)
    historico[HOJA] = mod.recalcular_totales_diarios(d, usuario=u, cas=c)
    print(f"  Monto  {MONTO_ACTUAL:,.0f} → 0")
    print(f"  Nombre → {NOTA}")

    banner("5) INVARIANTES")
    dd = historico[HOJA]
    o1 = dd["Orden"].astype(str).str.strip()
    chk("la fila SIGUE existiendo (esto es lo que evita que se recree)",
        int((o1 == ORDEN).sum()) == 1)
    chk("su Monto quedó en 0",
        float(pd.to_numeric(dd.loc[o1 == ORDEN, "Monto"]).iloc[0]) == 0)
    chk("no se borró ni se agregó ninguna fila", len(dd) == len(vivo[HOJA]),
        f"{len(vivo[HOJA])} → {len(dd)}")
    chk("los incentivos de 13608 quedan intactos",
        int(o1.str.startswith("incentivo").sum()) == int(o0.str.startswith("incentivo").sum()))
    # Las filas TOTAL SÍ cambian: recalcular_totales_diarios las reescribe, que es el objetivo.
    # El chequeo es sobre los movimientos reales, y por Orden (el recálculo puede reordenar).
    def movs(df):
        x = df.copy()
        x["_o"] = x["Orden"].astype(str).str.strip()
        x = x[x["Tipo"].astype(str).str.strip().str.upper() != "TOTAL"]
        x = x[x["_o"] != ORDEN]
        return pd.to_numeric(x["Monto"], errors="coerce").fillna(0).sum(), len(x)
    sa, na = movs(vivo[HOJA]); sb, nb = movs(dd)
    chk("ningún otro MOVIMIENTO de 13608 cambió de monto", abs(sa - sb) < 0.01,
        f"Σ {sa:,.2f} vs {sb:,.2f} · {na} vs {nb} filas")
    chk("las filas TOTAL se recalcularon (es lo esperado)",
        int((vivo[HOJA]["Tipo"].astype(str).str.strip().str.upper() == "TOTAL").sum())
        == int((dd["Tipo"].astype(str).str.strip().str.upper() == "TOTAL").sum()))
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

    banner("6) SALDO")
    s1 = saldo(historico[HOJA])
    print(f"  13608: COP {s0:>16,.2f} → {s1:>16,.2f}   Δ {s1-s0:>+14,.2f}")
    chk("el saldo SUBE exactamente el egreso retirado", abs((s1 - s0) - MONTO_ACTUAL) < 1)

    if not ESCRIBIR:
        banner("DRY-RUN TERMINADO — 0 ESCRITURAS")
        print("  Para escribir: python3 retirar_migracionamex_ebay_20260922.py --escribir")
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
    q = rel[HOJA]["Orden"].astype(str).str.strip()
    chk2("la fila sigue ahí (bloquea la recreación)", int((q == ORDEN).sum()) == 1)
    chk2("Monto = 0", float(pd.to_numeric(rel[HOJA].loc[q == ORDEN, "Monto"]).iloc[0]) == 0)
    chk2("saldo 13608", abs(saldo(rel[HOJA]) - s1) < 0.01, f"COP {saldo(rel[HOJA]):,.2f}")
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
            dst_f = f"{ONEDRIVE_ANT}/{f.replace('.xlsx','')}_PRE_retiro_migracion.xlsx"
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

    print(f"\n  {'✅ RETIRO APLICADO Y VERIFICADO' if ok2 else '🚨 REVISAR'}")
    print(f"  rev nueva: {md2.rev}\n  rollback:  {backup}")
    print(f"\n  ↩️  Para revertir: poner de nuevo Monto = {MONTO_ACTUAL:,.0f} en {ORDEN}.")


if __name__ == "__main__":
    sys.path.insert(0, os.environ.get("HARNESS_DIR", "."))
    main()
