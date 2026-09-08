#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Carga el INCENTIVO DE JUNIO 2026 de 1444 (Maria Moises) — COP 2.362.658.

POR QUÉ FALTABA
---------------
El incentivo de 25 COP/USD se pagó a 1444 en marzo (parcial 21-31), abril, mayo, julio y agosto.
Junio nunca se pagó: cae justo en la transición del cobro manual del backoffice al módulo 1-a-1
(corte 16-jun), y el incentivo se quedó sin dueño en el medio. Los pagos viejos están bajo los
nombres "Reembolso beneficio" y "Abonos de Cashback", por eso no aparecían buscando "incentivo".

DE DÓNDE SALE LA CIFRA
----------------------
Suma de los bloques `Compra ...` de la hoja de 1444 cuyo rango DECLARADO cae íntegro en junio,
más el neto del módulo 1-a-1 con fecha de compra en junio:

    bloques manuales de junio (17 filas)   USD  92.973,63
    módulo 1-a-1 (Robinhood 23-30 jun
      menos reembolso Rakuten 20-jun)      USD   1.532,70
                                           ─────────────
    base                                   USD  94.506,33  × 25 = COP 2.362.658

CRITERIO CONSERVADOR (decisión del usuario: "que no elevemos cifras"):
  · NO se prorratean los 3 bloques a caballo entre meses (Rakuten 25may-1jun, Robinhood
    10may-2jun, Amex 30jun-1jul). Incluirlos daría 2.569.627; prorratearlos, 2.392.176.
  · NO se reclaman los ~134.000 de déficit de marzo/mayo/julio.
  · NO se reclama enero, febrero ni el 1-20 de marzo: el primer pago dice literal
    "21 al 31 Marzo", así que el acuerdo arrancó ahí.

VALIDADO POR DOS CAMINOS
------------------------
El balance global del acuerdo (21-mar → 31-ago) da un faltante de COP 2.496.881 sobre un gasto
de USD 307.465,80. El 95% de ese faltante es junio, lo que confirma la cifra por un camino
independiente del cálculo mes a mes.

Se detectó y corrigió una errata del histórico: el bloque "Compra Amex del 27 al 30 Mayo" está
registrado el 4-may (imposible) — son compras de ABRIL. Sin esa corrección, abril parecía pagado
de más en ~197.000 y se le habría descontado a junio una plata que sí se debe.

  sin argumentos  -> dry-run (0 escrituras)
  --escribir      -> escribe a Dropbox y refresca la copia de OneDrive
"""
import sys, os, io, shutil, time, warnings
warnings.filterwarnings("ignore")
import pandas as pd

ESCRIBIR = "--escribir" in sys.argv

CAS, HOJA_PREF = "1444", "1444"
ORDEN = "incentivo_tc_1444_2026-06"
MONTO = 2362658.0
FECHA = "2026-07-01"          # patrón: el día 1 del mes siguiente (jul->1-ago, ago->1-sep)
MOTIVO = "Incentivo TC"
NOMBRE = "Incentivo TC Junio 2026 (25 COP x USD neto)"
BASE_USD = 94506.33

REV_ESPERADA = "0165af8059c3dc900000002f34b3f21"
SALDO_ESPERADO = None          # se lee del vivo; el cargue de tarjetas aún no se ha hecho

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
    SEP = "=" * 92
    def banner(t): print(f"\n{SEP}\n{t}\n{SEP}")
    ok = True
    def chk(n, c, det=""):
        nonlocal ok
        print(f"  {'✔' if c else '🚨'} {n:<62} {det}")
        ok = ok and bool(c)

    print(f"MODO: {'🔴 ESCRITURA REAL' if ESCRIBIR else '🟢 DRY-RUN (0 escrituras)'}")

    # 🧯 este script NO carga tarjetas ni genera incentivos automáticos
    for fn in ("procesar_amex", "procesar_rakuten", "procesar_robinhood", "procesar_capital",
               "procesar_usbank", "procesar_intuit", "agregar_incentivo_amex"):
        def _boom(*a, _n=fn, **k):
            raise AssertionError(f"{_n} fue llamada — este script solo agrega UNA fila")
        setattr(mod, fn, _boom)

    banner("1) HISTÓRICO VIVO FRESCO")
    cfg = mod.st.secrets["dropbox"]
    md = mod.dbx.files_get_metadata(cfg["remote_path"])
    _, res = mod.dbx.files_download(cfg["remote_path"])
    vivo = pd.read_excel(io.BytesIO(res.content), sheet_name=None)
    HOJA = next(h for h in vivo if h.split(" - ")[0].strip() == HOJA_PREF)
    s0 = saldo(vivo[HOJA])
    o0 = vivo[HOJA]["Orden"].astype(str).str.strip()
    print(f"  rev={md.rev}  modificado={md.server_modified}  size={md.size:,}  hojas={len(vivo)}")
    print(f"  '{HOJA}': {len(vivo[HOJA])} filas · saldo COP {s0:,.2f}")
    if md.rev != REV_ESPERADA:
        print(f"  ⚠️  el histórico se movió (esperaba {REV_ESPERADA})")
        if ESCRIBIR:
            raise SystemExit("⛔ ABORTA: rehacer el dry-run con el histórico fresco.")
    else:
        print("  ✔ rev idéntica a la del dry-run")

    banner("2) LA FILA NO EXISTE Y LOS OTROS MESES SIGUEN AHÍ")
    chk(f"{ORDEN} NO existe todavía", not (o0 == ORDEN).any())
    prev = [x for x in o0 if x.startswith(("incentivo_tc_", "incentivoamex_"))]
    print(f"  incentivos ya presentes en la hoja: {sorted(prev)}")
    chk("hay exactamente 2 incentivos previos", len(prev) == 2, f"{len(prev)}")
    # los pagos viejos viven en filas Extra*, no llevan prefijo de incentivo
    viejos = vivo[HOJA]["Nombre del producto"].astype(str).str.lower().str.contains(
        "reembolso beneficio|abonos de cashback", na=False)
    print(f"  pagos viejos (Extra*, 'Reembolso beneficio'): {int(viejos.sum())} filas · "
          f"COP {pd.to_numeric(vivo[HOJA].loc[viejos,'Monto']).sum():,.0f}")
    chk("los 4 pagos viejos siguen en la hoja", int(viejos.sum()) == 4, f"{int(viejos.sum())}")

    banner("3) LA CIFRA")
    print(f"  base       USD {BASE_USD:>12,.2f}")
    print(f"  x 25       COP {BASE_USD*25:>12,.0f}")
    print(f"  a cargar   COP {MONTO:>12,.0f}")
    chk("el monto es exactamente la base x 25", abs(BASE_USD * 25 - MONTO) < 1)
    chk("no hay ningún ingreso de ese monto ya en la hoja",
        not (pd.to_numeric(vivo[HOJA]["Monto"], errors="coerce").round(0) == round(MONTO)).any())

    banner("4) APLICAR")
    fecha_carga = pd.Timestamp.today().strftime("%Y-%m-%d")
    historico = {k: mod.asegurar_columnas_historico(v.copy()) for k, v in vivo.items()}
    d = historico[HOJA]
    u, c = usuario_de_totales(d), cas_de_totales(d, CAS)
    fila = {col: pd.NA for col in d.columns}
    fila.update({"Fecha": pd.Timestamp(FECHA), "Tipo": "Ingreso", "Orden": ORDEN,
                 "Monto": MONTO, "Motivo": MOTIVO, "Usuario": u, "Casillero": int(CAS),
                 "Nombre del producto": NOMBRE, "Fecha de Carga": pd.Timestamp(fecha_carga)})
    antes = len(d)
    d = pd.concat([d, pd.DataFrame([fila])], ignore_index=True)
    o = d["Orden"].astype(str).str.strip()
    mm = o.str.startswith("incentivo_tc_")
    d = pd.concat([d[~mm], d[mm].drop_duplicates(subset=["Orden"], keep="last")], ignore_index=True)
    d = mod.recalcular_totales_diarios(d, usuario=u, cas=c)
    historico[HOJA] = d
    print(f"  filas {antes} → {len(d)}  (Usuario='{u}', Casillero={c})")

    banner("5) INVARIANTES")
    o1 = d["Orden"].astype(str).str.strip()
    chk("la fila nueva existe una sola vez", int((o1 == ORDEN).sum()) == 1)
    chk("los 2 incentivos previos siguen intactos",
        all(int((o1 == p).sum()) == 1 for p in prev))
    for p, n in (("amex_", 151), ("rakuten_", 73), ("robinhood_", 249), ("intuit_", 23),
                 ("applepay_", 4)):
        chk(f"{p} intactas = {n}", int(o1.str.startswith(p).sum()) == n,
            f"{int(o1.str.startswith(p).sum())}")
    vac = {"", "nan", "none", "nat"}
    perd = {y for y in o0 if y.lower() not in vac} - {y for y in o1 if y.lower() not in vac}
    chk("0 Orden previos perdidos", not perd, f"{len(perd)}")
    a = vivo[HOJA]["Nombre del producto"].astype(str).str.lower().str.startswith("comision de (")
    b = d["Nombre del producto"].astype(str).str.lower().str.startswith("comision de (")
    chk("la comisión de 1444 no se mueve",
        int(a.sum()) == int(b.sum()) and
        abs(pd.to_numeric(vivo[HOJA].loc[a, "Monto"]).sum()
            - pd.to_numeric(d.loc[b, "Monto"]).sum()) < 0.01,
        f"{int(b.sum())} filas · Σ {pd.to_numeric(d.loc[b,'Monto']).sum():,.2f}")
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
    print(f"  1444: COP {s0:>16,.2f} → {s1:>16,.2f}   Δ {s1-s0:>+14,.2f}")
    chk("el saldo sube exactamente el monto del incentivo", abs((s1 - s0) - MONTO) < 1)

    if not ESCRIBIR:
        banner("DRY-RUN TERMINADO — 0 ESCRITURAS")
        print("  Para escribir: python3 cargar_incentivo_junio_1444.py --escribir")
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
        print(f"  {'✔' if c else '🚨'} {n:<62} {det}")
        ok2 = ok2 and bool(c)
    q = rel[HOJA]["Orden"].astype(str).str.strip()
    chk2("la fila quedó escrita", int((q == ORDEN).sum()) == 1)
    fila_esc = rel[HOJA][q == ORDEN].iloc[0]
    chk2("monto correcto", abs(float(pd.to_numeric(fila_esc["Monto"])) - MONTO) < 1,
         f"COP {float(pd.to_numeric(fila_esc['Monto'])):,.0f}")
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
    # la copia vigente anterior se archiva en Antiguos/ antes de poner la nueva
    for f in sorted(os.listdir(ONEDRIVE_DIR)):
        if f.endswith("_Historico_mayoristas.xlsx") and f != os.path.basename(destino):
            org = f"{ONEDRIVE_DIR}/{f}"
            arch = f"{ONEDRIVE_ANT}/{f.replace('.xlsx','')}_PRE_incentivo_junio.xlsx"
            shutil.move(org, arch)
            print(f"  archivada: {f}  →  Antiguos/{os.path.basename(arch)}")
    with open(destino, "wb") as fh:
        fh.write(contenido)
    print(f"  escrita:   {destino}  ({os.path.getsize(destino):,} bytes)")
    chk2("la copia de OneDrive pesa lo mismo que el vivo",
         os.path.getsize(destino) == md2.size, f"{os.path.getsize(destino):,} vs {md2.size:,}")

    print(f"\n  {'✅ CARGUE COMPLETO Y VERIFICADO' if ok2 else '🚨 REVISAR'}")
    print(f"  rev nueva: {md2.rev}\n  rollback:  {backup}")
    print("\n  ℹ️  No hace falta tocar tarjetas_cobradas.xlsx: esta fila no viene de un extracto.")


if __name__ == "__main__":
    sys.path.insert(0, os.environ.get("HARNESS_DIR", "."))
    main()
