#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Retira Extra42, Extra43 y Extra44 de la hoja de 1444 (Maria Moises). Borrado INTENCIONAL.

QUÉ SE BORRA
------------
Tres Ingreso_extra fechados 2026-08-31, cargados en la corrida de las 14:41 (rev
0165a5cfe6bdeb80), por COP 12.320.000 en total:
    Extra42  5.000.000  "Abono perdida 2"
    Extra43  2.750.000  "Abono perdida 3"
    Extra44  4.570.000  "Abono perdida 1"
Pedido explícito del usuario. Solo tocan la hoja de 1444: los Extra42/43/44 de 11591, 1633 y
9444 son OTRAS filas (Extra reinicia numeración por casillero) y NO se tocan.

POR QUÉ SE SALTA LA CAPA A
--------------------------
`guard_frescura_historico` bloquea CUALQUIER pérdida de Orden, incluidas las intencionales, y
`_orden_removible` NO se debe ampliar (debilitaría el guard para siempre). El patrón del
CLAUDE.md para este caso es: OK del usuario -> respaldo capa C -> verificar por cuenta propia
que la ÚNICA pérdida es la buscada -> saltar la capa A SOLO en esta corrida.
Aquí además se EJECUTA la capa A a propósito para comprobar que bloquea nombrando exactamente
esos 3 Orden y nada más: es la evidencia de que no se pierde otra cosa.

El respaldo capa C lo hace `upload_to_dropbox`, que NO llama a la capa A.

⚠️ VUELVEN SI SE RE-SUBEN: los Ingreso_extra se deduplican por (Orden, Motivo) con keep="last",
así que si se vuelve a subir a la app un archivo de ingresos extra que traiga esas 3 filas en la
hoja de 1444, se re-crean. El `ingresos_extra.xlsx` de OneDrive (10:37) NO las tiene; el archivo
que se subió a las 14:41 sí.

  sin argumentos  -> dry-run (0 escrituras)
  --escribir      -> escribe a Dropbox
"""
import sys, os, io, warnings
warnings.filterwarnings("ignore")
import pandas as pd

ESCRIBIR = "--escribir" in sys.argv

CASILLERO = "1444"
BORRAR = ["Extra42", "Extra43", "Extra44"]
ESP_MONTOS = {"Extra42": 5000000.0, "Extra43": 2750000.0, "Extra44": 4570000.0}
ESP_SUMA = 12320000.0
REV_ESPERADA = "0165a5cfe6bdeb800000002f34b3f21"
SALDO_ESPERADO = 75632342.21

# lo que se cargó hoy en tarjetas: debe quedar INTACTO
TC_ESPERADO = {"1444": {"robinhood_": 249, "rakuten_": 73, "intuit_": 7, "applepay_": 4,
                        "amex_": 151, "capital_": 0, "usbank_": 0},
               "13608": {"capital_": 115, "usbank_": 16, "amex_": 1},
               "11591": {"usbank_": 15, "amex_": 93}}


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
        print(f"  {'✔' if c else '🚨'} {n:<62} {det}")
        ok = ok and bool(c)

    print(f"MODO: {'🔴 ESCRITURA REAL' if ESCRIBIR else '🟢 DRY-RUN (0 escrituras)'}")

    # ── 1) vivo fresco ────────────────────────────────────────────────────────
    banner("1) HISTÓRICO VIVO FRESCO")
    cfg = mod.st.secrets["dropbox"]
    md = mod.dbx.files_get_metadata(cfg["remote_path"])
    _, res = mod.dbx.files_download(cfg["remote_path"])
    vivo = pd.read_excel(io.BytesIO(res.content), sheet_name=None)
    HOJA = next(h for h in vivo if h.split(" - ")[0].strip() == CASILLERO)
    s0 = saldo(vivo[HOJA])
    print(f"  rev={md.rev}  modificado={md.server_modified}  size={md.size:,}")
    print(f"  hoja '{HOJA}': {len(vivo[HOJA])} filas · saldo COP {s0:,.2f}")
    if md.rev != REV_ESPERADA or abs(s0 - SALDO_ESPERADO) > 0.01:
        print(f"  🚨 EL HISTÓRICO SE MOVIÓ (esperaba rev {REV_ESPERADA})")
        if ESCRIBIR:
            raise SystemExit("⛔ ABORTA: rehacer el dry-run con el histórico fresco.")
    else:
        print("  ✔ rev y saldo idénticos a los esperados")

    # ── 2) las 3 filas ────────────────────────────────────────────────────────
    banner("2) LAS 3 FILAS A RETIRAR (solo en 1444)")
    d0 = vivo[HOJA]
    o0 = d0["Orden"].astype(str).str.strip()
    m = o0.isin(BORRAR)
    sel = d0[m]
    print(sel[["Fecha", "Tipo", "Orden", "Monto", "Motivo", "Nombre del producto"]].to_string(index=False))
    chk("son exactamente 3 filas", len(sel) == 3, f"{len(sel)}")
    chk("todas Ingreso / Ingreso_extra",
        set(sel["Tipo"].astype(str).str.strip()) == {"Ingreso"} and
        set(sel["Motivo"].astype(str).str.strip()) == {"Ingreso_extra"})
    montos = {str(r["Orden"]).strip(): float(r["Monto"]) for _, r in sel.iterrows()}
    chk("los montos son los esperados", montos == ESP_MONTOS, str(montos))
    chk(f"suman COP {ESP_SUMA:,.0f}", abs(pd.to_numeric(sel["Monto"]).sum() - ESP_SUMA) < 0.01,
        f"{pd.to_numeric(sel['Monto']).sum():,.0f}")
    otros = {h: int(v[h]["Orden"].astype(str).str.strip().isin(BORRAR).sum())
             for h in vivo if "Orden" in v[h].columns} if False else {}
    for h in vivo:
        if h == HOJA or "Orden" not in vivo[h].columns:
            continue
        n = int(vivo[h]["Orden"].astype(str).str.strip().isin(BORRAR).sum())
        if n:
            otros[h] = n
    print(f"  ℹ️ los mismos códigos existen en otras hojas y NO se tocan: {otros}")

    # ── 3) construir la salida ────────────────────────────────────────────────
    banner("3) RETIRAR + RECALCULAR TOTALES (solo 1444)")
    historico = {k: mod.asegurar_columnas_historico(v.copy()) for k, v in vivo.items()}
    d = historico[HOJA]
    mm = d["Orden"].astype(str).str.strip().isin(BORRAR)
    print(f"  quitando {int(mm.sum())} filas de {len(d)}")
    d = d[~mm].copy()
    u, c = usuario_de_totales(historico[HOJA]), cas_de_totales(historico[HOJA], CASILLERO)
    historico[HOJA] = mod.recalcular_totales_diarios(d, usuario=u, cas=c)
    s1 = saldo(historico[HOJA])
    print(f"  recalculado con usuario='{u}' casillero={c!r}")
    print(f"  saldo {s0:,.2f} → {s1:,.2f}   Δ {s1 - s0:+,.2f}")
    chk(f"el saldo baja exactamente {ESP_SUMA:,.0f}", abs((s0 - s1) - ESP_SUMA) < 1.0,
        f"Δ {s0 - s1:,.2f}")

    # ── 4) la única pérdida son esas 3 ────────────────────────────────────────
    banner("4) VERIFICAR POR CUENTA PROPIA QUE LA ÚNICA PÉRDIDA SON ESAS 3")
    vac = {"", "nan", "none", "nat"}
    for hoja in vivo:
        if "Orden" not in vivo[hoja].columns:
            continue
        a = {x for x in vivo[hoja]["Orden"].astype(str).str.strip() if x.lower() not in vac}
        b = {x for x in historico[hoja]["Orden"].astype(str).str.strip() if x.lower() not in vac}
        perd = a - b
        esperado = set(BORRAR) if hoja == HOJA else set()
        chk(f"pérdidas en «{hoja}»", perd == esperado, f"{sorted(perd) if perd else 'ninguna'}")
    # el resto de hojas, verbatim
    for hoja in vivo:
        if hoja == HOJA:
            continue
        a, b = vivo[hoja], historico[hoja]
        igual = len(a) == len(b)
        if igual and "Monto" in a.columns:
            igual = abs(pd.to_numeric(a["Monto"], errors="coerce").fillna(0).sum() -
                        pd.to_numeric(b["Monto"], errors="coerce").fillna(0).sum()) < 0.01
        chk(f"verbatim: {hoja}", igual, f"{len(a)} filas")

    # ── 5) las tarjetas de hoy, intactas ──────────────────────────────────────
    banner("5) 💳 LO QUE SE CARGÓ HOY EN TARJETAS SIGUE INTACTO")
    for cas, esperado in TC_ESPERADO.items():
        h = next(x for x in historico if x.split(" - ")[0].strip() == cas)
        o = historico[h]["Orden"].astype(str).str.strip()
        for pref, n in esperado.items():
            chk(f"{cas} {pref}", int(o.str.startswith(pref).sum()) == n,
                f"{int(o.str.startswith(pref).sum())} (esperado {n})")
    # y con los MISMOS importes que en el vivo
    for cas in TC_ESPERADO:
        h = next(x for x in historico if x.split(" - ")[0].strip() == cas)
        prefs = ("amex_", "rakuten_", "robinhood_", "capital_", "usbank_", "intuit_", "applepay_")
        A = vivo[h].copy(); B = historico[h].copy()
        for X in (A, B):
            X["Orden"] = X["Orden"].astype(str).str.strip()
        A = A[A["Orden"].str.startswith(prefs)][["Orden", "Monto", "TRM", "Tipo"]]
        B = B[B["Orden"].str.startswith(prefs)][["Orden", "Monto", "TRM", "Tipo"]]
        j = A.merge(B, on="Orden", suffixes=("_v", "_n"))
        dM = (pd.to_numeric(j["Monto_n"], errors="coerce") -
              pd.to_numeric(j["Monto_v"], errors="coerce")).abs()
        dT = (pd.to_numeric(j["TRM_n"], errors="coerce") -
              pd.to_numeric(j["TRM_v"], errors="coerce")).abs()
        chk(f"{cas}: {len(j)} filas de tarjeta con Monto/TRM/Tipo idénticos",
            len(j) == len(A) and int((dM > 0.5).sum()) == 0 and int((dT > 0.005).sum()) == 0
            and (j["Tipo_v"] == j["Tipo_n"]).all())
    # comisiones de 1444
    a = vivo[HOJA]["Nombre del producto"].astype(str).str.lower().str.startswith("comision de (")
    b = historico[HOJA]["Nombre del producto"].astype(str).str.lower().str.startswith("comision de (")
    chk("comisiones de 1444 intactas",
        int(a.sum()) == int(b.sum()) and
        abs(pd.to_numeric(vivo[HOJA].loc[a, "Monto"]).sum() -
            pd.to_numeric(historico[HOJA].loc[b, "Monto"]).sum()) < 0.01,
        f"{int(b.sum())} filas")
    if not ok:
        raise SystemExit("⛔ ABORTA: falló alguna verificación.")

    # ── 6) capa A: debe bloquear, y SOLO por esas 3 ───────────────────────────
    banner("6) 🛡️ CAPA A — se ejecuta A PROPÓSITO para ver qué reclama")
    harness.clear_msgs()
    bloqueo = False
    try:
        mod.guard_frescura_historico(historico)
        print("  ⚠️ la capa A NO bloqueó (inesperado en un borrado intencional)")
    except harness._Stop:
        bloqueo = True
    msgs = [t for _, t in harness.MENSAJES]
    harness.clear_msgs()
    for t in msgs:
        print(f"    · {t[:200]}")
    nombrados = [o for o in BORRAR if any(o in t for t in msgs)]
    otros_ordenes = [t for t in msgs if "se perderían" in t and "1 Orden" not in t]
    chk("la capa A bloquea", bloqueo)
    chk("reclama exactamente los 3 Orden que queremos quitar",
        sorted(nombrados) == sorted(BORRAR), f"{sorted(nombrados)}")
    chk("reclama una sola hoja (1444)", sum(1 for t in msgs if "se perderían" in t) == 1)
    print("  → pérdida confirmada = la buscada. Se SALTA la capa A solo en esta corrida.")
    print("    (NO se toca _orden_removible: debilitaría el guard para siempre)")

    # ── 7) bytes ──────────────────────────────────────────────────────────────
    banner("7) EXCEL EN MEMORIA")
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        for hh, dfh in historico.items():
            w.book.create_sheet(hh[:31])
            dfh.to_excel(w, sheet_name=hh[:31], index=False)
    buf.seek(0)
    data_bytes = buf.read()
    print(f"  {len(data_bytes):,} bytes | {len(historico)} hojas")
    if not ok:
        raise SystemExit("⛔ ABORTA: falló alguna verificación.")

    if not ESCRIBIR:
        banner("DRY-RUN TERMINADO — 0 ESCRITURAS")
        print("  Para escribir: python3 borrar_extra42_44_1444.py --escribir")
        return

    # ── 8) escritura ──────────────────────────────────────────────────────────
    banner("8) SUBIDA (capa C hace el respaldo; capa A saltada a propósito)")
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

    banner("9) VALIDACIÓN POST-ESCRITURA (leyendo de vuelta)")
    md2 = mod.dbx.files_get_metadata(cfg["remote_path"])
    print(f"  rev NUEVA = {md2.rev}  modificado = {md2.server_modified}  size = {md2.size:,}")
    print(f"  backup    = {backup}")
    _, r2 = mod.dbx.files_download(cfg["remote_path"])
    rel = pd.read_excel(io.BytesIO(r2.content), sheet_name=None)
    ok2 = True
    def chk2(n, c, det=""):
        nonlocal ok2
        print(f"  {'✔' if c else '🚨'} {n:<62} {det}")
        ok2 = ok2 and bool(c)
    q = rel[HOJA]["Orden"].astype(str).str.strip()
    chk2("Extra42/43/44 YA NO están en 1444", int(q.isin(BORRAR).sum()) == 0,
         f"{int(q.isin(BORRAR).sum())}")
    chk2("saldo 1444", abs(saldo(rel[HOJA]) - s1) < 0.01, f"COP {saldo(rel[HOJA]):,.2f}")
    for cas, esperado in TC_ESPERADO.items():
        h = next(x for x in rel if x.split(" - ")[0].strip() == cas)
        o = rel[h]["Orden"].astype(str).str.strip()
        for pref, n in esperado.items():
            chk2(f"{cas} {pref}", int(o.str.startswith(pref).sum()) == n,
                 f"{int(o.str.startswith(pref).sum())}")
    for hoja in vivo:
        if hoja == HOJA:
            continue
        a, b = vivo[hoja], rel[hoja]
        igual = len(a) == len(b)
        if igual and "Monto" in a.columns:
            igual = abs(pd.to_numeric(a["Monto"], errors="coerce").fillna(0).sum() -
                        pd.to_numeric(b["Monto"], errors="coerce").fillna(0).sum()) < 0.01
        chk2(f"verbatim: {hoja}", igual, f"{len(a)} filas")
    chk2(f"{len(vivo)} hojas", len(rel) == len(vivo), f"{len(rel)}")
    print(f"\n  {'✅ BORRADO COMPLETO Y VERIFICADO' if ok2 else '🚨 REVISAR'}")
    print(f"  rev nueva: {md2.rev}\n  rollback:  {backup}")


if __name__ == "__main__":
    sys.path.insert(0, os.environ.get("HARNESS_DIR", "."))
    main()
