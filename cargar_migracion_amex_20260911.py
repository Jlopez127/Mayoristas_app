#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Carga los CARGOS SIN RECONOCER de la migración Amex → US Bank (19-ago-2026).

QUÉ SON
-------
`Cargos_sin_reconocer_ventana_15diasfina.xlsx` (hoja "Pendientes Kelly"): 56 cargos por
USD 14.569,15 que el banco cobró en la ventana del 6 al 18 de agosto y para los que no existe
respaldo — ni compra en el backoffice, ni extracto de Amex, ni número de orden. Salen de un
embudo de 965 cargos del lote de migración del 19-ago.

⚠️ EL PROPIO ANÁLISIS DICE QUE **ES UN TECHO, NO UNA CIFRA EXACTA**:
   "Una compra puede facturarse en varios cargos (...) El riesgo real es MENOR."
   "A nivel agregado no hay gasto sin respaldo: en esos 15 días el banco cobró $143.805,42 y el
    backoffice registró $149.546,88 — $5.741,46 MÁS."
Por eso el usuario avisó que probablemente haya que ELIMINAR algunos después. El diseño de esta
carga está hecho para que eso sea fácil (ver abajo).

DECISIONES DEL USUARIO (2026-09-11)
-----------------------------------
- Mapeo por la columna `Nota`:  elvis -> 11591 Paula · correal -> 1444 Maria · Julian -> 13608
- **Fecha 2026-08-15**, no la del archivo (19-ago): el gasto es del 1 al 15 de agosto.
- El cargo #34 (jdsports.com, USD 140,00) NO trae nota -> **queda FUERA** hasta identificarlo.
  Se cargan 55 de 56, USD 14.429,15.
- `Motivo = "Migracion Amex"` (NO "Tarjeta Amex") y `Orden = migracionamex_<referencia>`:
    · NO entran a la base del incentivo de 25 COP/USD (`es_tarjeta` exige Motivo EXACTO
      "Tarjeta Amex"), así que no hay que reajustar ningún incentivo congelado.
    · Quedan aislados y se identifican de una por Motivo o por prefijo de Orden — que es lo que
      hace fácil retirarlos si el análisis se estrecha.

POR QUÉ SOBREVIVEN A LAS CORRIDAS
---------------------------------
Ningún módulo genera estas filas, así que una corrida no las regenera. Viven solo en el
histórico: el dedup por Orden no las toca (no llegan del reporte) y `migracionamex_` se agregó a
`TARJETA_ORDEN_RE` para que la CAPA B las reinyecte si una corrida partiera de un histórico
rezagado — mismo trato que `applepay_`, que también es manual.

  sin argumentos  -> dry-run (0 escrituras)
  --escribir      -> escribe a Dropbox, refresca OneDrive y deja un Excel por mayorista
"""
import sys, os, io, shutil, time, warnings
warnings.filterwarnings("ignore")
import pandas as pd
from openpyxl import load_workbook

ESCRIBIR = "--escribir" in sys.argv

FUENTE = "/Users/julianlopez/Downloads/Cargos_sin_reconocer_ventana_15diasfina.xlsx"
HOJA_FUENTE = "Pendientes Kelly"
FECHA = "2026-08-15"
MOTIVO = "Migracion Amex"
PREFIJO = "migracionamex_"
MAP = {"elvis": "11591", "correal": "1444", "julian": "13608"}
NOMBRES = {"11591": "Paula Herrera", "1444": "Maria Moises", "13608": "Julian Sanchez"}
ESP = {"1444": (40, 7097.45), "11591": (14, 6954.41), "13608": (1, 377.29)}
ESP_TOTAL_USD = 14429.15
ESP_FUERA = 1          # el cargo sin nota

REV_ESPERADA = "0165b341cb8d16900000002f34b3f21"
ONEDRIVE_DIR = "/Users/julianlopez/Library/CloudStorage/OneDrive-Personal/Historico Carga/Conciliacion/Mayoristas"
ONEDRIVE_ANT = f"{ONEDRIVE_DIR}/Antiguos"
DESCARGAS = "/Users/julianlopez/Downloads"


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


def leer_fuente():
    """Lee con openpyxl: la 'Referencia del banco' es TEXTO y pandas la degradaría a float,
    perdiendo precisión justo en lo que se usa como Orden."""
    ws = load_workbook(FUENTE, data_only=True)[HOJA_FUENTE]
    hdr = [c.value for c in ws[5]]
    filas = [dict(zip(hdr, r)) for r in ws.iter_rows(min_row=6, values_only=True) if r[0] is not None]
    return pd.DataFrame(filas)


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

    _o = mod._amex_trm_dia
    def _trm(f, c=None, *a, **k):
        for i in range(6):
            v = _o(f, c if c is not None else {}, *a, **k)
            if v is not None:
                return v
            time.sleep(1.5)
        return None
    mod._amex_trm_dia = _trm

    # 🧯 este script no corre ningún módulo ni el incentivo
    for fn in ("procesar_egresos", "procesar_amex", "procesar_rakuten", "procesar_robinhood",
               "procesar_capital", "procesar_usbank", "procesar_intuit", "agregar_incentivo_amex"):
        def _boom(*a, _n=fn, **k):
            raise AssertionError(f"{_n} fue llamada — este script solo agrega filas a mano")
        setattr(mod, fn, _boom)

    print(f"MODO: {'🔴 ESCRITURA REAL' if ESCRIBIR else '🟢 DRY-RUN (0 escrituras)'}")

    banner("1) LA FUENTE")
    src = leer_fuente()
    src["_usd"] = pd.to_numeric(src["Monto USD"], errors="coerce")
    src["_ref"] = src["Referencia del banco"].astype(str).str.strip()
    src["_nota"] = src["Nota"].astype(str).str.strip().str.lower()
    src["_cas"] = src["_nota"].map(MAP)
    print(f"  {len(src)} cargos · USD {src['_usd'].sum():,.2f}")
    chk("56 cargos leídos", len(src) == 56, f"{len(src)}")
    chk("referencias únicas (sirven como Orden)", src["_ref"].nunique() == len(src),
        f"{src['_ref'].nunique()}")
    chk("ninguna referencia degradada a número",
        src["_ref"].str.fullmatch(r"\d{18}").all())
    fuera = src[src["_cas"].isna()]
    print(f"  fuera por no tener nota: {len(fuera)} · USD {fuera['_usd'].sum():,.2f}")
    for _, r in fuera.iterrows():
        print(f"     #{int(r['#'])} USD {r['_usd']:,.2f}  {str(r['Comercio'])[:46]}  ref={r['_ref']}")
    chk(f"queda fuera exactamente {ESP_FUERA} (el sin nota)", len(fuera) == ESP_FUERA)
    ent = src[src["_cas"].notna()].copy()
    chk(f"entran 55 por USD {ESP_TOTAL_USD:,.2f}",
        len(ent) == 55 and abs(ent["_usd"].sum() - ESP_TOTAL_USD) < 0.02,
        f"{len(ent)} · USD {ent['_usd'].sum():,.2f}")
    for cas, (n, u) in ESP.items():
        g = ent[ent["_cas"] == cas]
        chk(f"{cas} {NOMBRES[cas]:<16} {n} cargos · USD {u:,.2f}",
            len(g) == n and abs(g["_usd"].sum() - u) < 0.02,
            f"{len(g)} · USD {g['_usd'].sum():,.2f}")
    if not ok:
        raise SystemExit("⛔ ABORTA: la fuente no cuadra.")

    banner("2) TRM DEL 15-AGO (oficial + 125, igual que el resto de tarjetas)")
    trm = mod._amex_trm_dia(pd.Timestamp(FECHA).date(), {})
    print(f"  TRM {FECHA} = {trm}")
    chk("TRM obtenida", trm is not None and float(trm) > 0)
    if not ok:
        raise SystemExit("⛔ ABORTA: sin TRM no se convierte nada (no se inventa).")
    trm = float(trm)

    banner("3) HISTÓRICO VIVO FRESCO")
    cfg = mod.st.secrets["dropbox"]
    md = mod.dbx.files_get_metadata(cfg["remote_path"])
    _, res = mod.dbx.files_download(cfg["remote_path"])
    vivo = pd.read_excel(io.BytesIO(res.content), sheet_name=None)
    HOJA = {c: next(h for h in vivo if h.split(" - ")[0].strip() == c) for c in ESP}
    print(f"  rev={md.rev}  modificado={md.server_modified}  size={md.size:,}")
    s0, o0 = {}, {}
    for c, h in HOJA.items():
        s0[c] = saldo(vivo[h]); o0[c] = vivo[h]["Orden"].astype(str).str.strip()
        print(f"  {h:<32} {len(vivo[h]):>6} filas · saldo COP {s0[c]:>16,.2f}")
        chk(f"{c}: no hay filas {PREFIJO} previas",
            int(o0[c].str.startswith(PREFIJO).sum()) == 0)
    if md.rev != REV_ESPERADA:
        print(f"  ⚠️  el histórico se movió (esperaba {REV_ESPERADA})")
        if ESCRIBIR:
            raise SystemExit("⛔ ABORTA: rehacer el dry-run con el histórico fresco.")
    else:
        print("  ✔ rev idéntica a la del dry-run")

    banner("4) ARMAR LAS FILAS")
    nuevas = {}
    for cas in ESP:
        g = ent[ent["_cas"] == cas]
        filas = []
        for _, r in g.iterrows():
            com = " ".join(str(r["Comercio"]).split())[:60]
            filas.append({
                "Fecha": pd.Timestamp(FECHA), "Tipo": "Egreso",
                "Orden": f"{PREFIJO}{r['_ref']}",
                "Monto": round(float(r["_usd"]) * trm, 2),
                "Motivo": MOTIVO, "TRM": trm,
                "Usuario": usuario_de_totales(vivo[HOJA[cas]]), "Casillero": int(cas),
                "Estado de Orden": "",
                "Nombre del producto": f"Movimientos de migracion Amex - {com}",
            })
        nuevas[cas] = pd.DataFrame(filas)
        d = nuevas[cas]
        print(f"  {cas} {NOMBRES[cas]:<16} {len(d):>2} filas · USD {g['_usd'].sum():>9,.2f} · "
              f"COP {pd.to_numeric(d['Monto']).sum():>12,.0f}")
    todas = pd.concat(nuevas.values(), ignore_index=True)
    chk("0 Orden duplicados entre los tres", not todas["Orden"].duplicated().any())
    ya = set().union(*[set(o0[c]) for c in o0])
    chk("ningún Orden nuevo existe ya en el histórico",
        not (set(todas["Orden"]) & ya))
    chk("todas Egreso / Motivo Migracion Amex / fecha 15-ago",
        set(todas["Tipo"]) == {"Egreso"} and set(todas["Motivo"]) == {MOTIVO}
        and set(pd.to_datetime(todas["Fecha"]).dt.strftime("%Y-%m-%d")) == {FECHA})
    chk(f"COP total = USD {ESP_TOTAL_USD:,.2f} x {trm:,.2f}",
        abs(pd.to_numeric(todas["Monto"]).sum() - ESP_TOTAL_USD * trm) < 2,
        f"COP {pd.to_numeric(todas['Monto']).sum():,.0f}")
    if not ok:
        raise SystemExit("⛔ ABORTA: las filas no cuadran.")

    banner("5) APLICAR")
    fecha_carga = pd.Timestamp.today().strftime("%Y-%m-%d")
    historico = {k: mod.asegurar_columnas_historico(v.copy()) for k, v in vivo.items()}
    for cas, h in HOJA.items():
        d = historico[h]
        antes = len(d)
        x = nuevas[cas].copy(); x["Fecha de Carga"] = fecha_carga
        d = pd.concat([d, mod.asegurar_columnas_historico(x)], ignore_index=True)
        o = d["Orden"].astype(str).str.strip()
        m = o.str.startswith(PREFIJO)
        d = pd.concat([d[~m], d[m].drop_duplicates(subset=["Orden"], keep="last")], ignore_index=True)
        u, c = usuario_de_totales(vivo[h]), cas_de_totales(vivo[h], cas)
        historico[h] = mod.recalcular_totales_diarios(d, usuario=u, cas=c)
        print(f"  {h:<32} {antes} → {len(historico[h])} filas")

    banner("6) NI COMISIÓN NI INCENTIVO SE MUEVEN")
    for cas, h in HOJA.items():
        a = vivo[h]["Nombre del producto"].astype(str).str.lower().str.startswith("comision de (")
        b = historico[h]["Nombre del producto"].astype(str).str.lower().str.startswith("comision de (")
        chk(f"{cas}: comisión intacta",
            int(a.sum()) == int(b.sum()) and
            abs(pd.to_numeric(vivo[h].loc[a, "Monto"]).sum()
                - pd.to_numeric(historico[h].loc[b, "Monto"]).sum()) < 0.01,
            f"{int(b.sum())} filas · Σ {pd.to_numeric(historico[h].loc[b,'Monto']).sum():,.2f}")
        qa = vivo[h]["Orden"].astype(str).str.startswith("incentivo")
        qb = historico[h]["Orden"].astype(str).str.startswith("incentivo")
        chk(f"{cas}: incentivos intactos",
            int(qa.sum()) == int(qb.sum()) and
            abs(pd.to_numeric(vivo[h].loc[qa, "Monto"]).sum()
                - pd.to_numeric(historico[h].loc[qb, "Monto"]).sum()) < 0.01,
            f"{int(qb.sum())} filas · Σ {pd.to_numeric(historico[h].loc[qb,'Monto']).sum():,.0f}")
    # el Motivo nuevo NO debe entrar a la base del incentivo
    chk("'Migracion Amex' NO está en la lista blanca del incentivo",
        MOTIVO not in ["Tarjeta Amex", "Tarjeta Rakuten", "Tarjeta Robinhood", "Tarjeta Capital",
                       "Tarjeta US Bank", "Tarjeta Intuit", "Tarjeta Apple Pay"])

    banner("7) CAPA B + INVARIANTES + GUARD A")
    chk(f"{PREFIJO} está en TARJETA_ORDEN_RE (capa B lo reinyecta)",
        PREFIJO in mod.TARJETA_ORDEN_RE)
    historico = mod.preservar_filas_tarjeta(historico, vivo=vivo)
    harness.drenar()
    vac = {"", "nan", "none", "nat"}
    for cas, h in HOJA.items():
        o1 = historico[h]["Orden"].astype(str).str.strip()
        chk(f"{cas} {PREFIJO} = {ESP[cas][0]}",
            int(o1.str.startswith(PREFIJO).sum()) == ESP[cas][0],
            f"{int(o1.str.startswith(PREFIJO).sum())}")
        perd = {y for y in o0[cas] if y.lower() not in vac} - {y for y in o1 if y.lower() not in vac}
        chk(f"{cas}: 0 Orden previos perdidos", not perd, f"{len(perd)}")
        chk(f"{cas}: 0 Orden duplicados", not o1[o1.str.startswith(PREFIJO)].duplicated().any())
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

    banner("8) SALDOS")
    s1 = {}
    for cas, h in HOJA.items():
        s1[cas] = saldo(historico[h])
        cop = pd.to_numeric(nuevas[cas]["Monto"]).sum()
        print(f"  {h:<32} COP {s0[cas]:>16,.2f} → {s1[cas]:>16,.2f}   Δ {s1[cas]-s0[cas]:>+14,.2f}"
              f"  ({len(nuevas[cas])} cargos · USD {ESP[cas][1]:,.2f})")

    if not ESCRIBIR:
        banner("DRY-RUN TERMINADO — 0 ESCRITURAS")
        print("  Para escribir: python3 cargar_migracion_amex_20260911.py --escribir")
        return

    banner("9) SUBIDA A DROPBOX (capa C hace el respaldo)")
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

    banner("10) VALIDACIÓN POST-ESCRITURA")
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
    for cas, h in HOJA.items():
        q = rel[h]["Orden"].astype(str).str.strip()
        chk2(f"{cas}: saldo", abs(saldo(rel[h]) - s1[cas]) < 0.01, f"COP {saldo(rel[h]):,.2f}")
        chk2(f"{cas}: {ESP[cas][0]} filas {PREFIJO}",
             int(q.str.startswith(PREFIJO).sum()) == ESP[cas][0],
             f"{int(q.str.startswith(PREFIJO).sum())}")
        usd = (pd.to_numeric(rel[h].loc[q.str.startswith(PREFIJO), "Monto"])
               / pd.to_numeric(rel[h].loc[q.str.startswith(PREFIJO), "TRM"])).sum()
        chk2(f"{cas}: USD {ESP[cas][1]:,.2f}", abs(usd - ESP[cas][1]) < 0.05, f"{usd:,.2f}")
        chk2(f"{cas}: 0 Orden previos perdidos",
             not ({y for y in o0[cas] if y.lower() not in vac} - {y for y in q if y.lower() not in vac}))
    for hoja in vivo:
        if hoja in HOJA.values():
            continue
        A, B = vivo[hoja], rel[hoja]
        igual = len(A) == len(B)
        if igual and "Monto" in A.columns:
            igual = abs(pd.to_numeric(A["Monto"], errors="coerce").fillna(0).sum() -
                        pd.to_numeric(B["Monto"], errors="coerce").fillna(0).sum()) < 0.01
        chk2(f"verbatim: {hoja}", igual, f"{len(A)} filas")

    banner("11) COPIA A ONEDRIVE")
    hoy = pd.Timestamp.today().strftime("%Y%m%d")
    destino = f"{ONEDRIVE_DIR}/{hoy}_Historico_mayoristas.xlsx"
    os.makedirs(ONEDRIVE_ANT, exist_ok=True)
    for f in sorted(os.listdir(ONEDRIVE_DIR)):
        if f.endswith("_Historico_mayoristas.xlsx") and f != os.path.basename(destino):
            shutil.move(f"{ONEDRIVE_DIR}/{f}",
                        f"{ONEDRIVE_ANT}/{f.replace('.xlsx','')}_PRE_migracion_amex.xlsx")
            print(f"  archivada: {f}")
    with open(destino, "wb") as fh:
        fh.write(contenido)
    print(f"  escrita:   {destino}  ({os.path.getsize(destino):,} bytes)")

    banner("12) UN EXCEL POR MAYORISTA EN DESCARGAS")
    for cas, h in HOJA.items():
        q = rel[h]["Orden"].astype(str).str.strip()
        d = rel[h][q.str.startswith(PREFIJO)].copy()
        d["USD"] = pd.to_numeric(d["Monto"]) / pd.to_numeric(d["TRM"])
        d["Referencia del banco"] = d["Orden"].astype(str).str.replace(PREFIJO, "", regex=False)
        cols = ["Fecha", "Tipo", "Orden", "Referencia del banco", "Monto", "USD", "TRM",
                "Motivo", "Nombre del producto", "Usuario", "Casillero", "Fecha de Carga"]
        d = d[[c for c in cols if c in d.columns]].sort_values("Monto", ascending=False)
        tot = pd.DataFrame([{"Fecha": "TOTAL", "Monto": pd.to_numeric(d["Monto"]).sum(),
                             "USD": d["USD"].sum()}])
        out = f"{DESCARGAS}/{hoy}_migracion_amex_{cas}_{NOMBRES[cas].replace(' ','_')}.xlsx"
        with pd.ExcelWriter(out, engine="openpyxl") as w:
            pd.concat([d, tot], ignore_index=True).to_excel(w, sheet_name="Migracion Amex", index=False)
            ws = w.sheets["Migracion Amex"]
            for col, anc in (("A", 12), ("C", 34), ("D", 22), ("E", 14), ("F", 12), ("G", 10),
                             ("H", 18), ("I", 52)):
                ws.column_dimensions[col].width = anc
            ws.freeze_panes = "A2"
        print(f"  {out}")
        print(f"     {len(d)} cargos · COP {pd.to_numeric(d['Monto']).sum():,.0f} · USD {d['USD'].sum():,.2f}")

    print(f"\n  {'✅ CARGUE COMPLETO Y VERIFICADO' if ok2 else '🚨 REVISAR'}")
    print(f"  rev nueva: {md2.rev}\n  rollback:  {backup}")
    print(f"\n  ↩️  Para retirar alguno: borrar su fila {PREFIJO}<ref>. Ningún módulo la recrea.")


if __name__ == "__main__":
    sys.path.insert(0, os.environ.get("HARNESS_DIR", "."))
    main()
