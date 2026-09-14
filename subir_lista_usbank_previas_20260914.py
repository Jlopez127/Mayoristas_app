#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cierra el hueco de US Bank en `tarjetas_cobradas.xlsx` (2026-09-14).

Tras el cargue de hoy, la prueba de fuego mostró que recargar el extracto de US Bank SIGUE
devolviendo 22 filas. No es doble cobro —el dedup por Orden las unifica y son idénticas—, pero
es el hueco conocido: son filas `usbank_` que YA estaban en el histórico antes de hoy y que
nunca entraron a la lista, porque a US Bank se le dejaba fuera a propósito por usar ID nativo.

Sin entrada en la lista, si el emisor re-expidiera una de ellas con otro ID, ninguna de las dos
barreras la vería. Es el mismo caso que el 31-ago costó COP 4.799.142 con Robinhood, y se cierra
igual: registrando las previas, no solo las nuevas.

Se registran las que el extracto actual todavía genera (atributos CAPTURADOS de la propia
barrera, no reconstruidos del histórico). Las `usbank_` más viejas que ya no aparecen en ningún
extracto no se pueden registrar así — y tampoco corren riesgo: si el extracto no las genera,
nunca llegan a ser un cobro huérfano.

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
CSV = "/Users/julianlopez/Downloads/Credit Card - 0613_08-14-2026_09-18-2026.csv"
NOTA = ("registro retroactivo 2026-09-14: filas usbank_ ya cargadas que nunca entraron a la "
        "lista (US Bank se dejaba fuera por usar ID nativo); habilita la barrera 2")
CARD_NORM = {"11591": "PAULA HERRERA", "13608": "JULIAN SANCHEZ"}


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
    _o = mod._amex_trm_dia
    def _trm(f, c=None, *a, **k):
        for i in range(6):
            v = _o(f, c if c is not None else {}, *a, **k)
            if v is not None:
                return v
            time.sleep(1.5)
        return None
    mod._amex_trm_dia = _trm
    print(f"MODO: {'🔴 ESCRITURA REAL' if ESCRIBIR else '🟢 DRY-RUN (0 escrituras)'}")

    cfg = mod.st.secrets["dropbox"]
    carpeta = str(PurePosixPath(cfg["remote_path"]).parent)
    remote = f"{carpeta}/{mod.TARJETAS_COBRADAS_FILENAME}"

    banner("1) QUÉ SIGUE DEVOLVIENDO EL EXTRACTO (capturando atributos de la barrera)")
    capturado = {}
    _orig = mod._excluir_por_atributos
    def _wrap(df, cobrados_df, tarjeta, ordenes_extracto, rango, etiqueta):
        capturado[tarjeta] = df.copy()
        return _orig(df, cobrados_df, tarjeta, ordenes_extracto, rango, etiqueta)
    mod._excluir_por_atributos = _wrap
    cobrados, pendientes, cobrados_df = mod.cargar_tarjetas_cobradas(); harness.clear_msgs()
    ht = mod.cargar_hist_tarjetas(); harness.clear_msgs()
    out = mod.procesar_usbank(pd.read_csv(CSV), fecha_desde=mod.USBANK_FECHA_DESDE,
                              cobrados=cobrados, pendientes=pendientes,
                              hist_tarjetas=ht, cobrados_df=cobrados_df)
    harness.clear_msgs()
    mod._excluir_por_atributos = _orig
    devueltas = pd.concat(out.values(), ignore_index=True) if out else pd.DataFrame()
    print(f"  el extracto sigue devolviendo {len(devueltas)} fila(s)")
    chk("capturado el df de la barrera", "usbank" in capturado)

    _, res = mod.dbx.files_download(cfg["remote_path"])
    vivo = pd.read_excel(io.BytesIO(res.content), sheet_name=None)
    ya_hist = {}
    for cas in CARD_NORM:
        h = next(x for x in vivo if x.split(" - ")[0].strip() == cas)
        ya_hist[cas] = set(vivo[h]["Orden"].astype(str).str.strip())
    chk("todas las devueltas YA están en el histórico (nada nuevo que cobrar)",
        all(o in ya_hist[str(r["Casillero"])] for o, r in
            zip(devueltas["Orden"].astype(str), devueltas.to_dict("records"))) if len(devueltas) else True)
    if not ok or devueltas.empty:
        raise SystemExit("⛔ ABORTA" if not ok else "✅ nada que registrar: el extracto ya no devuelve nada.")

    banner("2) ENTRADAS NUEVAS PARA LA LISTA")
    md_l = mod.dbx.files_get_metadata(remote)
    _, res_l = mod.dbx.files_download(remote)
    previo = res_l.content
    xls = pd.ExcelFile(io.BytesIO(previo))
    libro = {h: xls.parse(h) for h in xls.sheet_names}
    cob = libro["cobradas"]
    ya_lista = set(cob["Orden"].astype(str).str.strip())
    cap = capturado["usbank"].copy()
    cap["_orden"] = cap["_orden"].astype(str).str.strip()
    cap = cap.drop_duplicates(subset=["_orden"], keep="first").set_index("_orden")
    filas = []
    for _, r in devueltas.iterrows():
        o = str(r["Orden"]).strip()
        if o in ya_lista or o not in cap.index:
            continue
        cc = cap.loc[o]
        usd = round(abs(float(cc["_usd"])), 2)
        f_attr = pd.to_datetime(cc["_fecha"])
        filas.append({"Orden": o, "tarjeta": "usbank", "casillero": int(r["Casillero"]),
                      "fecha_compra": f_attr, "monto_usd": usd, "nota": NOTA,
                      "fuente": "historico_mayoristas.xlsx (registro retroactivo 2026-09-14)",
                      "card_norm": CARD_NORM[str(r["Casillero"])],
                      "merchant_norm": mod._norm_merchant(cc["_merch_attr"]),
                      "usd_abs": usd, "fecha_attr": f_attr, "attr_fuente": "extracto usbank",
                      "signo": str(r["Tipo"]).strip()})
    nuevas = pd.DataFrame(filas)[list(cob.columns)]
    print(f"  lista rev={md_l.rev} · 'cobradas': {len(cob)} · usbank: "
          f"{int((cob['tarjeta'].astype(str).str.lower()=='usbank').sum())}")
    print(f"  entradas a agregar: {len(nuevas)}")
    chk("hay entradas que agregar", len(nuevas) > 0, f"{len(nuevas)}")
    chk("ningún atributo de la barrera 2 vacío",
        len(nuevas) and nuevas[["merchant_norm", "usd_abs", "fecha_attr", "signo"]].notna().all().all()
        and (nuevas["merchant_norm"].astype(str).str.strip() != "").all())
    chk("0 Orden repetidos contra la lista", not set(nuevas["Orden"]) & ya_lista)
    libro["cobradas"] = pd.concat([cob, nuevas], ignore_index=True)
    chk("0 Orden duplicados en toda la lista",
        not libro["cobradas"]["Orden"].astype(str).str.strip().duplicated().any())
    for h in xls.sheet_names:
        if h != "cobradas":
            chk(f"'{h}' intacta", libro[h].equals(xls.parse(h)), f"{len(libro[h])} filas")
    print(f"  'cobradas': {len(cob)} → {len(libro['cobradas'])} · por tarjeta: "
          f"{libro['cobradas']['tarjeta'].astype(str).str.lower().value_counts().to_dict()}")
    if not ok:
        raise SystemExit("⛔ ABORTA: falló alguna verificación.")
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        for h, d in libro.items():
            d.to_excel(w, sheet_name=h, index=False)
    buf.seek(0); data = buf.read()

    if not ESCRIBIR:
        banner("DRY-RUN TERMINADO — 0 ESCRITURAS")
        return

    banner("3) RESPALDO + SUBIDA")
    if mod.dbx.files_get_metadata(remote).rev != md_l.rev:
        raise SystemExit("⛔ ABORTA: la lista se movió.")
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = f"{carpeta}/{PurePosixPath(remote).stem}_backup_{ts}_pre_usbank_previas.xlsx"
    mod.dbx.files_upload(previo, backup, mode=dropbox.files.WriteMode.add)
    print(f"  🛟 respaldo: {backup}")
    mod.dbx.files_upload(data, remote, mode=dropbox.files.WriteMode.overwrite)
    md2 = mod.dbx.files_get_metadata(remote)
    print(f"  rev NUEVA = {md2.rev}")

    banner("4) 🔥 PRUEBA DE FUEGO")
    c2, p2, cd2 = mod.cargar_tarjetas_cobradas(); harness.clear_msgs()
    h2 = mod.cargar_hist_tarjetas(); harness.clear_msgs()
    o2 = mod.procesar_usbank(pd.read_csv(CSV), fecha_desde=mod.USBANK_FECHA_DESDE,
                             cobrados=c2, pendientes=p2, hist_tarjetas=h2, cobrados_df=cd2)
    harness.clear_msgs()
    n = sum(len(v) for v in o2.values())
    print(f"  {'✅' if n == 0 else '🚨'} recargar el extracto de US Bank devuelve {n} filas")
    print(f"\n  rev nueva: {md2.rev}\n  rollback:  {backup}")


if __name__ == "__main__":
    sys.path.insert(0, os.environ.get("HARNESS_DIR", "."))
    main()
