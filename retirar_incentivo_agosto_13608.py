#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Retira el INCENTIVO DE AGOSTO 2026 de 13608 (Julian Sanchez) — COP 1.094.363.

Decisión del usuario (2026-09-09): "por regla ese de agosto de él no va". Puede volver a aplicar
más adelante, así que el retiro se hace de forma REVERSIBLE.

⚠️ POR QUÉ SE NEUTRALIZA (Monto = 0) Y NO SE BORRA
---------------------------------------------------
`agregar_incentivo_amex` sólo se salta un mes si el Orden YA EXISTE:

    if orden_inc in ordenes_existentes:
        continue  # ya existe -> no recrear ni recalcular (congelado)

Y agosto sigue en `_incentivo_meses_objetivo` indefinidamente (verificado: 9-sep, 30-sep y
1-oct lo siguen devolviendo). Si se BORRARA la fila, el Orden desaparecería y **la próxima
corrida la volvería a crear** con el valor recalculado. Neutralizarla deja el Orden vivo, que es
justo lo que bloquea la recreación — mismo patrón que COMPRAS_TC_PROPIA y que la regla del
CLAUDE.md: "retirar un cargo sin borrarlo".

El Motivo se deja intacto ("Incentivo Amex") porque `es_incentivo` lo usa para excluir la fila
de su propia base; la explicación va en `Nombre del producto`, que es texto libre.

PARA REVERTIRLO en el futuro: poner de nuevo el Monto en 1094363 (o el que corresponda) y
limpiar la nota del nombre. No hay que recrear nada.

  sin argumentos  -> dry-run (0 escrituras)
  --escribir      -> escribe a Dropbox y refresca la copia de OneDrive
"""
import sys, os, io, shutil, warnings
warnings.filterwarnings("ignore")
import pandas as pd

ESCRIBIR = "--escribir" in sys.argv

CAS = "13608"
ORDEN = "incentivoamex_13608_2026-08"
MONTO_ACTUAL = 1094363.0
NOTA = "Incentivo Amex Agosto 2026 — RETIRADO 2026-09-09 (no aplica por regla; Monto=0 para que no se recree)"
REV_ESPERADA = "0165b0dd15b9cbc00000002f34b3f21"

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
               "procesar_capital", "procesar_usbank", "procesar_intuit", "agregar_incentivo_amex"):
        def _boom(*a, _n=fn, **k):
            raise AssertionError(f"{_n} fue llamada — este script solo neutraliza una fila")
        setattr(mod, fn, _boom)

    banner("1) POR QUÉ NEUTRALIZAR Y NO BORRAR")
    for f in ("2026-09-09", "2026-09-30", "2026-10-01"):
        print(f"  _incentivo_meses_objetivo({f}) = {mod._incentivo_meses_objetivo(f)}")
    print("  → agosto sigue siendo mes objetivo: si el Orden desaparece, se recrea.")
    chk("el incentivo está ACTIVO (por eso recrearía)", mod.INCENTIVO_AMEX_ACTIVO is True)
    chk("13608 es casillero Amex (entra al incentivo)", CAS in mod.AMEX_USUARIOS)

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
    chk("los otros incentivos de 13608 quedan intactos",
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
    chk("el saldo baja exactamente el incentivo retirado", abs((s0 - s1) - MONTO_ACTUAL) < 1)

    if not ESCRIBIR:
        banner("DRY-RUN TERMINADO — 0 ESCRITURAS")
        print("  Para escribir: python3 retirar_incentivo_agosto_13608.py --escribir")
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
    for f in sorted(os.listdir(ONEDRIVE_DIR)):
        if f.endswith("_Historico_mayoristas.xlsx") and f != os.path.basename(destino):
            shutil.move(f"{ONEDRIVE_DIR}/{f}",
                        f"{ONEDRIVE_ANT}/{f.replace('.xlsx','')}_PRE_retiro_incentivo.xlsx")
            print(f"  archivada: {f}")
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
