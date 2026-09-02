#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Registra en `tarjetas_cobradas.xlsx` las 16 filas intuit_ del cargue del 2026-09-02.

Regla 5 del CLAUDE.md: todo cargue termina en DOS escrituras, histórico Y lista. Sin entrada en
la lista, la barrera 2 (por atributos) no cubre esas filas y una re-expedición del movimiento
—que en Intuit cambia el hash, porque el Orden es hash del monto— se re-cobraría.

Los atributos se capturan envolviendo `_excluir_por_atributos`, no se reconstruyen del histórico.

  sin argumentos  -> dry-run (0 escrituras)
  --escribir      -> respalda (WriteMode.add) y sobrescribe la lista
"""
import sys, os, io, time, warnings
from datetime import datetime
from pathlib import PurePosixPath
warnings.filterwarnings("ignore")
import pandas as pd
import dropbox

ESCRIBIR = "--escribir" in sys.argv
CSV = "/Users/julianlopez/Downloads/intuit_acumulado_2026-09-02.csv"
TARJETA, PREFIJO, CASILLERO = "intuit", "intuit_", 1444
CARD_NORM = "MARIA MOISES"
NOTA = "cargue Intuit 2026-09-02 (13 descargas parciales acumuladas; corte movido al 26-ago)"
FUENTE = "historico_mayoristas.xlsx rev 0165a81a9b99f850 (cargue 2026-09-02)"
REV_HIST = "0165a81a9b99f8500000002f34b3f21"
ESP_TOTAL_HIST = 23      # 7 previas + 16 nuevas
ESP_NUEVAS = 16
ESP_ANTES = 2754
PREV_INTUIT = 7
OTRAS = {"amex": 1679, "robinhood": 436, "rakuten": 428, "capital": 195, "usbank": 9}


def main():
    import harness
    mod = harness.cargar_app()
    SEP = "=" * 92
    def banner(t): print(f"\n{SEP}\n{t}\n{SEP}")
    ok = True
    def chk(n, c, det=""):
        nonlocal ok
        print(f"  {'✔' if c else '🚨'} {n:<60} {det}")
        ok = ok and bool(c)
    _o = mod._amex_trm_dia
    def _trm(f, c=None, *a, **k):
        for i in range(6):
            v = _o(f, c if c is not None else {}, *a, **k)
            if v is not None:
                return v
            time.sleep(1.5)
    mod._amex_trm_dia = _trm

    print(f"MODO: {'🔴 ESCRITURA REAL' if ESCRIBIR else '🟢 DRY-RUN (0 escrituras)'}")
    cfg = mod.st.secrets["dropbox"]
    carpeta = str(PurePosixPath(cfg["remote_path"]).parent)
    remote = f"{carpeta}/{mod.TARJETAS_COBRADAS_FILENAME}"

    banner("1) FILAS intuit_ DEL HISTÓRICO VIVO")
    md_h = mod.dbx.files_get_metadata(cfg["remote_path"])
    _, res = mod.dbx.files_download(cfg["remote_path"])
    hojas = pd.read_excel(io.BytesIO(res.content), sheet_name=None)
    print(f"  histórico rev={md_h.rev}")
    chk("es el histórico del cargue de hoy", md_h.rev == REV_HIST)
    partes = []
    for hoja, d in hojas.items():
        if "Orden" not in d.columns:
            continue
        sel = d[d["Orden"].astype(str).str.strip().str.startswith(PREFIJO)].copy()
        if len(sel):
            sel["_hoja"] = hoja
            partes.append(sel)
    hist = pd.concat(partes, ignore_index=True)
    chk(f"{ESP_TOTAL_HIST} filas intuit_", len(hist) == ESP_TOTAL_HIST, f"{len(hist)}")
    chk("todas en 1444", set(hist["_hoja"].unique()) == {"1444 - Maria Moises"})

    banner("2) ATRIBUTOS DE LA PROPIA BARRERA 2")
    cap = {}
    _orig = mod._excluir_por_atributos
    def _wrap(df, cobrados_df, tarjeta, ordenes, rango, etiqueta):
        cap[tarjeta] = df.copy()
        return _orig(df, cobrados_df, tarjeta, ordenes, rango, etiqueta)
    mod._excluir_por_atributos = _wrap
    harness.clear_msgs()
    cobrados, pendientes, cobrados_df = mod.cargar_tarjetas_cobradas(); harness.clear_msgs()
    hist_t = mod.cargar_hist_tarjetas(); harness.clear_msgs()
    mod.procesar_intuit(pd.read_csv(CSV, encoding="utf-8-sig"), fecha_desde=mod.INTUIT_FECHA_DESDE,
                        cobrados=cobrados, pendientes=pendientes, hist_tarjetas=hist_t,
                        cobrados_df=cobrados_df)
    harness.clear_msgs()
    mod._excluir_por_atributos = _orig
    chk("capturado el df de la barrera", TARJETA in cap, f"{len(cap.get(TARJETA, []))} filas")
    if not ok:
        raise SystemExit("⛔ ABORTA")

    banner("3) ENTRADAS NUEVAS")
    md_l = mod.dbx.files_get_metadata(remote)
    _, res_l = mod.dbx.files_download(remote)
    previo = res_l.content
    xls = pd.ExcelFile(io.BytesIO(previo))
    libro = {h: xls.parse(h) for h in xls.sheet_names}
    cob = libro["cobradas"]
    print(f"  lista rev={md_l.rev} · {len(cob)} entradas")
    chk(f"la lista tiene {ESP_ANTES}", len(cob) == ESP_ANTES, f"{len(cob)}")
    chk(f"'intuit' previas = {PREV_INTUIT}",
        int((cob["tarjeta"].astype(str).str.lower() == TARJETA).sum()) == PREV_INTUIT)
    ya = set(cob["Orden"].astype(str).str.strip())
    c = cap[TARJETA].copy()
    c["_orden"] = c["_orden"].astype(str).str.strip()
    c = c.drop_duplicates(subset=["_orden"], keep="first").set_index("_orden")
    h = hist.copy(); h["Orden"] = h["Orden"].astype(str).str.strip()
    h = h[~h["Orden"].isin(ya)]
    chk(f"{ESP_NUEVAS} filas del histórico aún sin registrar", len(h) == ESP_NUEVAS, f"{len(h)}")
    falt = [o for o in h["Orden"] if o not in c.index]
    chk("todas tienen atributos del extracto", not falt, f"{len(falt)}")
    if not ok:
        raise SystemExit("⛔ ABORTA")
    h["_usd_h"] = (pd.to_numeric(h["Monto"]) / pd.to_numeric(h["TRM"])).round(2)
    filas, dU, dF = [], 0, 0
    for _, r in h.sort_values(["Fecha", "Orden"]).iterrows():
        x = c.loc[r["Orden"]]
        usd = round(abs(float(x["_usd"])), 2)
        if abs(usd - abs(float(r["_usd_h"]))) > 0.02: dU += 1
        fa = pd.to_datetime(x["_fecha"])
        if fa.date() != pd.to_datetime(r["Fecha"]).date(): dF += 1
        filas.append({"Orden": r["Orden"], "tarjeta": TARJETA, "casillero": CASILLERO,
                      "fecha_compra": fa, "monto_usd": usd, "nota": NOTA, "fuente": FUENTE,
                      "card_norm": CARD_NORM, "merchant_norm": mod._norm_merchant(x["_merch_attr"]),
                      "usd_abs": usd, "fecha_attr": fa, "attr_fuente": "extracto intuit",
                      "signo": str(r["Tipo"]).strip()})
    chk("USD del módulo == USD del histórico", dU == 0, f"{dU}")
    chk("fecha del módulo == fecha del histórico", dF == 0, f"{dF}")
    nuevas = pd.DataFrame(filas)[list(cob.columns)]
    chk("atributos completos",
        nuevas[["merchant_norm", "usd_abs", "fecha_attr", "signo"]].notna().all().all()
        and (nuevas["merchant_norm"].str.strip() != "").all())
    chk("0 Orden repetidos contra la lista", not set(nuevas["Orden"]) & ya)
    print(f"  {len(nuevas)} entradas · USD {nuevas['usd_abs'].sum():,.2f} · "
          f"{nuevas['fecha_attr'].min().date()} → {nuevas['fecha_attr'].max().date()}")

    banner("4) LIBRO NUEVO")
    libro["cobradas"] = pd.concat([cob, nuevas], ignore_index=True)
    tot = len(libro["cobradas"])
    print(f"  'cobradas': {len(cob)} → {tot}")
    print(f"  por tarjeta: {libro['cobradas']['tarjeta'].astype(str).str.lower().value_counts().to_dict()}")
    chk(f"total = {ESP_ANTES + ESP_NUEVAS}", tot == ESP_ANTES + ESP_NUEVAS, f"{tot}")
    for hh in ("pendientes_rematch", "revision"):
        chk(f"'{hh}' intacta", libro[hh].equals(xls.parse(hh)), f"{len(libro[hh])} filas")
    for t, n in OTRAS.items():
        chk(f"'{t}' sin cambios",
            int((libro["cobradas"]["tarjeta"].astype(str).str.lower() == t).sum()) == n, f"{n}")
    chk("0 Orden duplicados", not libro["cobradas"]["Orden"].astype(str).str.strip().duplicated().any())
    if not ok:
        raise SystemExit("⛔ ABORTA")
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        for hh, dd in libro.items():
            dd.to_excel(w, sheet_name=hh, index=False)
    buf.seek(0); data = buf.read()
    print(f"  {len(data):,} bytes")

    if not ESCRIBIR:
        banner("DRY-RUN TERMINADO — 0 ESCRITURAS")
        return

    banner("5) 🛟 RESPALDO + SUBIDA")
    if mod.dbx.files_get_metadata(remote).rev != md_l.rev:
        raise SystemExit("⛔ ABORTA: la lista se movió.")
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    bk = f"{carpeta}/{PurePosixPath(remote).stem}_backup_{ts}_pre_intuit0902.xlsx"
    mod.dbx.files_upload(previo, bk, mode=dropbox.files.WriteMode.add)
    print(f"  🛟 respaldo: {bk}")
    mod.dbx.files_upload(data, remote, mode=dropbox.files.WriteMode.overwrite)
    print("  ✅ lista subida")

    banner("6) VALIDACIÓN + PRUEBA DE FUEGO")
    md2 = mod.dbx.files_get_metadata(remote)
    _, r2 = mod.dbx.files_download(remote)
    x2 = pd.ExcelFile(io.BytesIO(r2.content)); c2 = x2.parse("cobradas")
    ok2 = True
    def chk2(n, cc, det=""):
        nonlocal ok2
        print(f"  {'✔' if cc else '🚨'} {n:<60} {det}")
        ok2 = ok2 and bool(cc)
    print(f"  rev NUEVA = {md2.rev}  size = {md2.size:,}")
    chk2(f"{ESP_ANTES + ESP_NUEVAS} entradas", len(c2) == ESP_ANTES + ESP_NUEVAS, f"{len(c2)}")
    chk2(f"'intuit' = {PREV_INTUIT + ESP_NUEVAS}",
         int((c2["tarjeta"].astype(str).str.lower() == TARJETA).sum()) == PREV_INTUIT + ESP_NUEVAS)
    chk2("0 Orden duplicados", not c2["Orden"].astype(str).str.strip().duplicated().any())
    chk2("las 3 hojas", x2.sheet_names == xls.sheet_names)
    cob2, pen2, cdf2 = mod.cargar_tarjetas_cobradas(); harness.clear_msgs()
    o = mod.procesar_intuit(pd.read_csv(CSV, encoding="utf-8-sig"),
                            fecha_desde=mod.INTUIT_FECHA_DESDE, cobrados=cob2, pendientes=pen2,
                            hist_tarjetas=hist_t, cobrados_df=cdf2)
    for n, t in harness.drenar():
        print(f"  [{n}] {t[:200]}")
    n_ = sum(len(vv) for vv in o.values())
    chk2("recargar el acumulado NO cobra nada", n_ == 0, f"{n_} filas")
    print(f"\n  {'✅ LISTA ACTUALIZADA Y VERIFICADA' if ok2 else '🚨 REVISAR'}")
    print(f"  rev nueva: {md2.rev}\n  rollback:  {bk}")


if __name__ == "__main__":
    sys.path.insert(0, os.environ.get("HARNESS_DIR", "."))
    main()
