#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PRIMER CARGUE de DISCOVER (cuenta 4244 -> 13608 Julian Sanchez), 2026-09-14.

Fuente: los dos 'xls' de Discover (HTML disfrazado): Statement 10-sep (84 filas,
10-ago -> 9-sep) + Recent Activity 14-sep (22 filas, 10 -> 12-sep).

⚠️ La cuenta es de SANTIAGO LARGO. Decisión explícita del usuario (2026-09-14): todas
sus compras desde el 9-sep-2026 inclusive son de Julian. Lo anterior se ignora — era
gasto de Santiago, que en las demás tarjetas NO se cobra a nadie.

LAS DOS ESCRITURAS
------------------
Este script hace las DOS que exige la regla 5 del CLAUDE.md, en este orden:
  1. el HISTÓRICO (las filas nuevas)
  2. `tarjetas_cobradas.xlsx` (las mismas filas, para la barrera 2 por atributos)
El orden importa: si la lista fuera primero, el módulo excluiría esas filas y no cargaría nada.
Discover genera su Orden por HASH, así que la entrada en la lista NO es opcional: si el emisor
re-fecha un movimiento el hash cambia y, sin entrada, ninguna de las dos barreras lo ve y se
re-cobra. Es el caso que costó COP 4.799.142.

Los atributos de la lista se CAPTURAN envolviendo `_excluir_por_atributos`, no se reconstruyen
del histórico: así usan la misma normalización que la barrera (importa en los reembolsos, cuyo
`Nombre del producto` no lleva el merchant crudo).

  sin argumentos  -> dry-run (0 escrituras)
  --escribir      -> escribe histórico + lista + copia OneDrive
"""
import sys, os, io, shutil, time, warnings
from datetime import datetime
from pathlib import PurePosixPath
warnings.filterwarnings("ignore")
import pandas as pd
import dropbox

ESCRIBIR = "--escribir" in sys.argv

FUENTES = ["/Users/julianlopez/Downloads/Discover-Statement-20260910.xls",
           "/Users/julianlopez/Downloads/Discover-RecentActivity-20260914.xls"]
CAS, TARJETA, PREFIJO = "13608", "discover", "discover_"
CARD_NORM = "JULIAN SANCHEZ"
NOTA = "cargue Discover 2026-09-14 (1er cargue de la tarjeta; corte 9-sep, cuenta de Santiago Largo que compra para Julian)"

REV_ESPERADA = "0165b709a65d29500000002f34b3f21"
PREV_HIST = 0                # 1er cargue: no hay filas discover_ todavía
ESP_NUEVAS = 22
ESP_USD_NETO = 13060.17
ESP_TIPOS = {"Egreso": 22}

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

    _o = mod._amex_trm_dia
    def _trm(f, c=None, *a, **k):
        for i in range(6):
            v = _o(f, c if c is not None else {}, *a, **k)
            if v is not None:
                return v
            time.sleep(1.5)
        return None
    mod._amex_trm_dia = _trm

    # 🧯 tripwires: este script solo carga Discover
    for fn in ("procesar_amex", "procesar_rakuten", "procesar_robinhood", "procesar_usbank",
               "procesar_intuit", "procesar_capital", "procesar_egresos", "agregar_incentivo_amex"):
        def _boom(*a, _n=fn, **k):
            raise AssertionError(f"{_n} fue llamada — este script solo carga Discover")
        setattr(mod, fn, _boom)

    print(f"MODO: {'🔴 ESCRITURA REAL' if ESCRIBIR else '🟢 DRY-RUN (0 escrituras)'}")
    print(f"CAPITAL_FECHA_DESDE = {mod.DISCOVER_FECHA_DESDE}")

    banner("1) HISTÓRICO VIVO FRESCO")
    cfg = mod.st.secrets["dropbox"]
    md = mod.dbx.files_get_metadata(cfg["remote_path"])
    _, res = mod.dbx.files_download(cfg["remote_path"])
    vivo = pd.read_excel(io.BytesIO(res.content), sheet_name=None)
    HOJA = next(h for h in vivo if h.split(" - ")[0].strip() == CAS)
    s0 = saldo(vivo[HOJA]); o0 = vivo[HOJA]["Orden"].astype(str).str.strip()
    print(f"  rev={md.rev}  modificado={md.server_modified}  size={md.size:,}")
    print(f"  '{HOJA}': {len(vivo[HOJA])} filas · saldo COP {s0:,.2f}")
    chk(f"{PREV_HIST} filas {PREFIJO} previas",
        int(o0.str.startswith(PREFIJO).sum()) == PREV_HIST,
        f"{int(o0.str.startswith(PREFIJO).sum())}")
    if md.rev != REV_ESPERADA:
        print(f"  ⚠️  el histórico se movió (esperaba {REV_ESPERADA})")
        if ESCRIBIR:
            raise SystemExit("⛔ ABORTA: rehacer el dry-run con el histórico fresco.")
    else:
        print("  ✔ rev idéntica a la del dry-run")

    banner("2) PROCESAR — capturando los atributos de la propia barrera 2")
    capturado = {}
    _orig = mod._excluir_por_atributos
    def _wrap(df, cobrados_df, tarjeta, ordenes_extracto, rango, etiqueta):
        capturado[tarjeta] = df.copy()
        return _orig(df, cobrados_df, tarjeta, ordenes_extracto, rango, etiqueta)
    mod._excluir_por_atributos = _wrap
    cobrados, pendientes, cobrados_df = mod.cargar_tarjetas_cobradas(); harness.clear_msgs()
    hist_t = mod.cargar_hist_tarjetas(); harness.clear_msgs()
    raw = pd.concat([mod.leer_discover(f) for f in FUENTES], ignore_index=True)
    out = mod.procesar_discover(raw.copy(), fecha_desde=mod.DISCOVER_FECHA_DESDE, cobrados=cobrados,
                               pendientes=pendientes, hist_tarjetas=hist_t, cobrados_df=cobrados_df)
    msgs = harness.drenar()
    mod._excluir_por_atributos = _orig
    print(f"  extracto: {len(raw)} filas · lista: {len(cobrados)} cobrados")
    for n, m in msgs:
        print(f"  [{n}] {m[:220]}")
    chk("capturado el df de la barrera", TARJETA in capturado,
        f"{len(capturado.get(TARJETA, []))} filas")
    nuevas = out.get(f"{PREFIJO}{CAS}", pd.DataFrame())
    ya = set(o0)
    nuevas = nuevas[~nuevas["Orden"].astype(str).isin(ya)].copy()

    banner("3) RECONCILIACIÓN")
    chk(f"entran {ESP_NUEVAS} filas nuevas", len(nuevas) == ESP_NUEVAS, f"{len(nuevas)}")
    if nuevas.empty:
        raise SystemExit("⛔ ABORTA: 0 filas nuevas.")
    nuevas["_usd"] = pd.to_numeric(nuevas["Monto"]) / pd.to_numeric(nuevas["TRM"])
    nuevas["_s"] = nuevas["Tipo"].map({"Egreso": 1, "Ingreso": -1})
    neto = float((nuevas["_usd"] * nuevas["_s"]).sum())
    chk(f"USD neto {ESP_USD_NETO:,.2f}", abs(neto - ESP_USD_NETO) < 0.02, f"{neto:,.2f}")
    chk(f"tipos {ESP_TIPOS}", nuevas["Tipo"].value_counts().to_dict() == ESP_TIPOS,
        str(nuevas["Tipo"].value_counts().to_dict()))
    chk("Motivo/Casillero/Orden coherentes",
        set(nuevas["Motivo"]) == {"Tarjeta Discover"}
        and set(nuevas["Casillero"].astype(str)) == {CAS}
        and nuevas["Orden"].astype(str).str.startswith(PREFIJO).all())
    chk("ninguna está ya en la lista de exclusión",
        not (set(nuevas["Orden"].astype(str)) & set(cobrados)))
    f = pd.to_datetime(nuevas["Fecha"])
    print(f"\n  {len(nuevas)} filas · {f.min().date()} → {f.max().date()} · "
          f"COP neto {float((pd.to_numeric(nuevas['Monto'])*nuevas['_s']).sum()):,.0f}")
    for _, r in nuevas.sort_values("Fecha").iterrows():
        print(f"    {str(r['Fecha'])[:10]} {str(r['Tipo']):<7} USD {r['_usd']:>9,.2f} "
              f"COP {pd.to_numeric(r['Monto']):>12,.0f}  {str(r['Nombre del producto'])[:48]}")

    banner("4) 🧪 PRUEBA DEL EXTRACTO CORTO (1er cargue: no hay previas que comparar)")
    todo = mod.procesar_discover(raw.copy(), fecha_desde=mod.DISCOVER_FECHA_DESDE, cobrados=set(),
                                pendientes=None, hist_tarjetas=hist_t, cobrados_df=None)
    harness.clear_msgs()
    t = todo.get(f"{PREFIJO}{CAS}", pd.DataFrame()).copy()
    vi = vivo[HOJA].copy(); vi["Orden"] = vi["Orden"].astype(str).str.strip()
    vi = vi[vi["Orden"].str.startswith(PREFIJO)]
    t["Orden"] = t["Orden"].astype(str).str.strip()
    j = vi.merge(t, on="Orden", suffixes=("_h", "_n"))
    # ⚠️ criterio en USD, no en COP (regla 8): la TRM de un reembolso se hereda de su compra y
    # puede reasignarse al reprocesar sin lista. Lo que no puede moverse es el USD ni el Tipo.
    uh = pd.to_numeric(j["Monto_h"]) / pd.to_numeric(j["TRM_h"])
    un = pd.to_numeric(j["Monto_n"]) / pd.to_numeric(j["TRM_n"])
    dP = j["Tipo_n"].astype(str).str.strip() != j["Tipo_h"].astype(str).str.strip()
    reasig = int(((pd.to_numeric(j["TRM_n"]) - pd.to_numeric(j["TRM_h"])).abs() > 0.005).sum())
    chk(f"{len(j)} ya cargadas se reproducen igual (USD)",
        int(((un - uh).abs() > 0.02).sum()) == 0 and int(dP.sum()) == 0,
        f"ΔUSD={int(((un-uh).abs()>0.02).sum())} ΔTipo={int(dP.sum())}"
        + (f" · {reasig} reembolso(s) con TRM reasignada" if reasig else ""))
    if not ok:
        raise SystemExit("⛔ ABORTA: falló alguna verificación.")

    banner("5) APLICAR AL HISTÓRICO")
    fecha_carga = pd.Timestamp.today().strftime("%Y-%m-%d")
    historico = {k: mod.asegurar_columnas_historico(v.copy()) for k, v in vivo.items()}
    d = historico[HOJA]; antes = len(d)
    x = nuevas.drop(columns=["_usd", "_s"]).copy(); x["Fecha de Carga"] = fecha_carga
    d = pd.concat([d, mod.asegurar_columnas_historico(x)], ignore_index=True)
    o = d["Orden"].astype(str).str.strip(); mm = o.str.startswith(PREFIJO)
    d = pd.concat([d[~mm], d[mm].drop_duplicates(subset=["Orden"], keep="last")], ignore_index=True)
    u, c = usuario_de_totales(vivo[HOJA]), cas_de_totales(vivo[HOJA], CAS)
    historico[HOJA] = mod.recalcular_totales_diarios(d, usuario=u, cas=c)
    print(f"  {PREFIJO} {PREV_HIST} → "
          f"{int(historico[HOJA]['Orden'].astype(str).str.startswith(PREFIJO).sum())}  "
          f"({antes} → {len(historico[HOJA])} filas)")

    banner("6) INVARIANTES + CAPA B + GUARD A")
    historico = mod.preservar_filas_tarjeta(historico, vivo=vivo); harness.drenar()
    o1 = historico[HOJA]["Orden"].astype(str).str.strip()
    chk(f"{PREFIJO} = {PREV_HIST + ESP_NUEVAS}",
        int(o1.str.startswith(PREFIJO).sum()) == PREV_HIST + ESP_NUEVAS,
        f"{int(o1.str.startswith(PREFIJO).sum())}")
    vac = {"", "nan", "none", "nat"}
    perd = {y for y in o0 if y.lower() not in vac} - {y for y in o1 if y.lower() not in vac}
    chk("0 Orden previos perdidos", not perd, f"{len(perd)}")
    chk("0 Orden de tarjeta duplicados",
        not o1[o1.str.startswith(("amex_", "capital_", "usbank_", "discover_", "migracionamex_"))].duplicated().any())
    a = vivo[HOJA]["Nombre del producto"].astype(str).str.lower().str.startswith("comision de (")
    b = historico[HOJA]["Nombre del producto"].astype(str).str.lower().str.startswith("comision de (")
    chk("comisión intacta", int(a.sum()) == int(b.sum()))
    qa = vivo[HOJA]["Orden"].astype(str).str.startswith("incentivo")
    qb = historico[HOJA]["Orden"].astype(str).str.startswith("incentivo")
    chk("incentivos intactos (incluye el de agosto retirado en 0)",
        int(qa.sum()) == int(qb.sum()) and
        abs(pd.to_numeric(vivo[HOJA].loc[qa, "Monto"]).sum()
            - pd.to_numeric(historico[HOJA].loc[qb, "Monto"]).sum()) < 0.01,
        f"{int(qb.sum())} filas · Σ {pd.to_numeric(historico[HOJA].loc[qb,'Monto']).sum():,.0f}")
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
    buf.seek(0); data_hist = buf.read()
    print(f"  {len(data_hist):,} bytes | {len(historico)} hojas")
    harness.clear_msgs()
    try:
        mod.guard_frescura_historico(historico)
        print("  ✅ GUARD A PASA (0 pérdidas)")
    except harness._Stop:
        for n, t2 in harness.MENSAJES:
            print(f"  [{n}] {t2[:400]}")
        raise SystemExit("⛔ GUARD A BLOQUEÓ")

    banner("7) SALDO")
    s1 = saldo(historico[HOJA])
    print(f"  {HOJA}: COP {s0:>16,.2f} → {s1:>16,.2f}   Δ {s1-s0:>+14,.2f}")

    # ── ENTRADAS PARA LA LISTA (2ª escritura) ────────────────────────────────
    banner("8) ENTRADAS PARA tarjetas_cobradas.xlsx")
    carpeta = str(PurePosixPath(cfg["remote_path"]).parent)
    remote_l = f"{carpeta}/{mod.TARJETAS_COBRADAS_FILENAME}"
    md_l = mod.dbx.files_get_metadata(remote_l)
    _, res_l = mod.dbx.files_download(remote_l)
    contenido_previo = res_l.content
    xls = pd.ExcelFile(io.BytesIO(contenido_previo))
    libro = {hh: xls.parse(hh) for hh in xls.sheet_names}
    cob = libro["cobradas"]
    prev_cap = int((cob["tarjeta"].astype(str).str.strip().str.lower() == TARJETA).sum())
    print(f"  lista rev={md_l.rev} · 'cobradas': {len(cob)} · '{TARJETA}': {prev_cap}")

    cap = capturado[TARJETA].copy()
    cap["_orden"] = cap["_orden"].astype(str).str.strip()
    cap = cap.drop_duplicates(subset=["_orden"], keep="first").set_index("_orden")
    falt = [oo for oo in nuevas["Orden"].astype(str) if oo not in cap.index]
    chk("todas las nuevas tienen atributos del módulo", not falt, f"{len(falt)}")
    if falt:
        raise SystemExit("⛔ ABORTA: hay filas sin atributos derivables del extracto.")
    filas, dif_usd, dif_f = [], 0, 0
    for _, r in nuevas.sort_values(["Fecha", "Orden"]).iterrows():
        cc = cap.loc[str(r["Orden"])]
        usd = round(abs(float(cc["_usd"])), 2)
        if abs(usd - abs(float(r["_usd"]))) > 0.02:
            dif_usd += 1
        f_attr = pd.to_datetime(cc["_fecha"])
        if f_attr.date() != pd.to_datetime(r["Fecha"]).date():
            dif_f += 1
        filas.append({
            "Orden": str(r["Orden"]), "tarjeta": TARJETA, "casillero": int(CAS),
            "fecha_compra": f_attr, "monto_usd": usd, "nota": NOTA,
            "fuente": f"historico_mayoristas.xlsx (cargue {fecha_carga})",
            "card_norm": CARD_NORM, "merchant_norm": mod._norm_merchant(cc["_merch_attr"]),
            "usd_abs": usd, "fecha_attr": f_attr, "attr_fuente": f"extracto {TARJETA}",
            "signo": str(r["Tipo"]).strip(),
        })
    chk("USD del módulo == USD del histórico", dif_usd == 0, f"{dif_usd} difieren")
    chk("fecha del módulo == fecha de la fila", dif_f == 0, f"{dif_f} difieren")
    nuevas_l = pd.DataFrame(filas)[list(cob.columns)]
    chk(f"{ESP_NUEVAS} entradas nuevas", len(nuevas_l) == ESP_NUEVAS, f"{len(nuevas_l)}")
    chk("ningún atributo de la barrera 2 vacío",
        nuevas_l[["merchant_norm", "usd_abs", "fecha_attr", "signo"]].notna().all().all()
        and (nuevas_l["merchant_norm"].astype(str).str.strip() != "").all()
        and nuevas_l["signo"].isin(["Egreso", "Ingreso"]).all())
    chk("el signo distingue compra de devolución",
        nuevas_l["signo"].value_counts().to_dict() == ESP_TIPOS,
        str(nuevas_l["signo"].value_counts().to_dict()))
    chk("0 Orden repetidos contra la lista",
        not set(nuevas_l["Orden"]) & set(cob["Orden"].astype(str).str.strip()))
    libro["cobradas"] = pd.concat([cob, nuevas_l], ignore_index=True)
    chk("0 Orden duplicados en toda la lista",
        not libro["cobradas"]["Orden"].astype(str).str.strip().duplicated().any())
    for hh in xls.sheet_names:
        if hh != "cobradas":
            chk(f"'{hh}' intacta", libro[hh].equals(xls.parse(hh)), f"{len(libro[hh])} filas")
    print(f"  'cobradas': {len(cob)} → {len(libro['cobradas'])} · "
          f"'{TARJETA}': {prev_cap} → {prev_cap + ESP_NUEVAS}")
    if not ok:
        raise SystemExit("⛔ ABORTA: falló alguna verificación de la lista.")
    bl = io.BytesIO()
    with pd.ExcelWriter(bl, engine="openpyxl") as w:
        for hh, dd in libro.items():
            dd.to_excel(w, sheet_name=hh, index=False)
    bl.seek(0); data_lista = bl.read()
    print(f"  lista nueva: {len(data_lista):,} bytes")

    if not ESCRIBIR:
        banner("DRY-RUN TERMINADO — 0 ESCRITURAS")
        print("  Para escribir: python3 cargar_discover_20260914.py --escribir")
        return

    banner("9) ESCRITURA 1/2 — HISTÓRICO")
    if mod.dbx.files_get_metadata(cfg["remote_path"]).rev != REV_ESPERADA:
        raise SystemExit("⛔ ABORTA SIN ESCRIBIR: el histórico se movió.")
    harness.clear_msgs()
    mod.upload_to_dropbox(data_hist)
    backup_h = None
    for n, t2 in harness.MENSAJES:
        print(f"  [{n}] {t2}")
        if "Respaldo previo creado" in t2 and "`" in t2:
            backup_h = t2.split("`")[1]
    harness.clear_msgs()
    md2 = mod.dbx.files_get_metadata(cfg["remote_path"])
    _, r2 = mod.dbx.files_download(cfg["remote_path"])
    contenido_hist = r2.content
    rel = pd.read_excel(io.BytesIO(contenido_hist), sheet_name=None)
    ok2 = True
    def chk2(n, c, det=""):
        nonlocal ok2
        print(f"  {'✔' if c else '🚨'} {n:<64} {det}")
        ok2 = ok2 and bool(c)
    q = rel[HOJA]["Orden"].astype(str).str.strip()
    chk2("saldo", abs(saldo(rel[HOJA]) - s1) < 0.01, f"COP {saldo(rel[HOJA]):,.2f}")
    chk2(f"{PREFIJO} = {PREV_HIST + ESP_NUEVAS}",
         int(q.str.startswith(PREFIJO).sum()) == PREV_HIST + ESP_NUEVAS)
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
    print(f"  rev histórico NUEVA = {md2.rev}")

    banner("10) ESCRITURA 2/2 — LISTA DE EXCLUSIÓN")
    if mod.dbx.files_get_metadata(remote_l).rev != md_l.rev:
        raise SystemExit(f"⛔ ABORTA: la lista se movió. El HISTÓRICO YA SE ESCRIBIÓ "
                         f"(rev {md2.rev}) — registrar las {ESP_NUEVAS} entradas a mano.")
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_l = f"{carpeta}/{PurePosixPath(remote_l).stem}_backup_{ts}_pre_discover.xlsx"
    mod.dbx.files_upload(contenido_previo, backup_l, mode=dropbox.files.WriteMode.add)
    print(f"  🛟 respaldo: {backup_l} ({len(contenido_previo):,} bytes)")
    mod.dbx.files_upload(data_lista, remote_l, mode=dropbox.files.WriteMode.overwrite)
    md_l2 = mod.dbx.files_get_metadata(remote_l)
    _, rl2 = mod.dbx.files_download(remote_l)
    c2 = pd.ExcelFile(io.BytesIO(rl2.content)).parse("cobradas")
    chk2(f"'cobradas' = {len(cob) + ESP_NUEVAS}", len(c2) == len(cob) + ESP_NUEVAS, f"{len(c2)}")
    chk2(f"'{TARJETA}' = {prev_cap + ESP_NUEVAS}",
         int((c2["tarjeta"].astype(str).str.lower() == TARJETA).sum()) == prev_cap + ESP_NUEVAS)
    chk2("0 Orden duplicados", not c2["Orden"].astype(str).str.strip().duplicated().any())
    print(f"  rev lista NUEVA = {md_l2.rev}")

    banner("11) 🔥 PRUEBA DE FUEGO: recargar el extracto ya no cobra nada")
    cob2, pen2, cdf2 = mod.cargar_tarjetas_cobradas(); harness.clear_msgs()
    ht2 = mod.cargar_hist_tarjetas(); harness.clear_msgs()
    out2 = mod.procesar_discover(pd.read_csv(CSV), fecha_desde=mod.DISCOVER_FECHA_DESDE,
                                cobrados=cob2, pendientes=pen2, hist_tarjetas=ht2, cobrados_df=cdf2)
    for n, m in harness.drenar():
        print(f"  [{n}] {m[:200]}")
    tot = sum(len(v) for v in out2.values())
    chk2("recargar el extracto NO cobra nada", tot == 0, f"{tot} filas")

    banner("12) COPIA A ONEDRIVE")
    hoy = pd.Timestamp.today().strftime("%Y%m%d")
    destino = f"{ONEDRIVE_DIR}/{hoy}_Historico_mayoristas.xlsx"
    os.makedirs(ONEDRIVE_ANT, exist_ok=True)
    for f2 in sorted(os.listdir(ONEDRIVE_DIR)):
        if f2.endswith("_Historico_mayoristas.xlsx") and f2 != os.path.basename(destino):
            shutil.move(f"{ONEDRIVE_DIR}/{f2}",
                        f"{ONEDRIVE_ANT}/{f2.replace('.xlsx','')}_PRE_discover.xlsx")
            print(f"  archivada: {f2}")
    with open(destino, "wb") as fh:
        fh.write(contenido_hist)
    print(f"  escrita: {destino} ({os.path.getsize(destino):,} bytes)")

    print(f"\n  {'✅ CARGUE COMPLETO — LAS DOS ESCRITURAS' if ok2 else '🚨 REVISAR'}")
    print(f"  histórico rev {md2.rev}   rollback {backup_h}")
    print(f"  lista     rev {md_l2.rev}   rollback {backup_l}")


if __name__ == "__main__":
    sys.path.insert(0, os.environ.get("HARNESS_DIR", "."))
    main()
