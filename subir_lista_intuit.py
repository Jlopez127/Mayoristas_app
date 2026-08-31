#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Añade a `tarjetas_cobradas.xlsx` (Dropbox) las 7 entradas 'intuit' del cargue del 31-ago-2026.

POR QUÉ, si el dedup por Orden ya evita duplicar
------------------------------------------------
La lista es lo que habilita la BARRERA 2 (anti-recobro por atributos). Esa barrera solo puede
usar cobros HUÉRFANOS: los que están en la lista y cuyo Orden el extracto ya no genera. Sin
entradas 'intuit' en la lista, si Intuit re-expidiera un movimiento con otra fecha o el hash
cambiara por cualquier razón, la transacción volvería a entrar y NADA la detendría — el Orden
(barrera 1) ya no serviría, que es justo el caso que costó COP 4.799.142 con Robinhood.

Las 7 entradas se derivan del HISTÓRICO recién escrito, no de una lista a mano: así lo que se
declara "ya cobrado" es exactamente lo que se cobró.

  sin argumentos  -> dry-run (0 escrituras)
  --escribir      -> respalda (WriteMode.add) y sobrescribe la lista
"""
import sys, os, io, warnings
from datetime import datetime
from pathlib import PurePosixPath
warnings.filterwarnings("ignore")
import pandas as pd
import dropbox

ESCRIBIR = "--escribir" in sys.argv

TARJETA = "intuit"
CASILLERO = "1444"
NOTA = "cargue inicial Intuit 2026-08-31 (historia completa de la tarjeta)"
FUENTE = "extracto intuit transactions_1788184532.csv"
CARD_NORM = "MARIA MOISES"
ATTR_FUENTE = "extracto intuit"
ESP_NUEVAS = 7
ESP_USD = 818.41
ESP_TOTAL_ANTES = 2310          # estado real de la lista (NO 3.373, ver informe §8)
PREFIJO_NOMBRE = "Tarjeta Intuit - gasto - "


def main():
    import harness
    mod = harness.cargar_app()
    SEP = "=" * 92
    def banner(t): print(f"\n{SEP}\n{t}\n{SEP}")
    ok = True
    def chk(n, c, det=""):
        nonlocal ok
        print(f"  {'✔' if c else '🚨'} {n:<56} {det}")
        ok = ok and bool(c)

    print(f"MODO: {'🔴 ESCRITURA REAL' if ESCRIBIR else '🟢 DRY-RUN (0 escrituras)'}")
    cfg = mod.st.secrets["dropbox"]
    carpeta = str(PurePosixPath(cfg["remote_path"]).parent)
    remote = f"{carpeta}/{mod.TARJETAS_COBRADAS_FILENAME}"

    # ── 1) las filas REALES del histórico ────────────────────────────────────
    banner("1) FILAS intuit_ DEL HISTÓRICO VIVO (la fuente de la verdad)")
    md_h = mod.dbx.files_get_metadata(cfg["remote_path"])
    _, res = mod.dbx.files_download(cfg["remote_path"])
    hojas = pd.read_excel(io.BytesIO(res.content), sheet_name=None)
    print(f"  histórico rev={md_h.rev}")
    partes = []
    for hoja, d in hojas.items():
        if "Orden" not in d.columns:
            continue
        sel = d[d["Orden"].astype(str).str.strip().str.startswith("intuit_")].copy()
        if len(sel):
            sel["_hoja"] = hoja
            partes.append(sel)
    hist = pd.concat(partes, ignore_index=True) if partes else pd.DataFrame()
    chk(f"{ESP_NUEVAS} filas intuit_ en el histórico", len(hist) == ESP_NUEVAS, f"{len(hist)}")
    chk("todas en la hoja de 1444", set(hist["_hoja"].unique()) == {"1444 - Maria Moises"},
        str(set(hist["_hoja"].unique())))
    chk("todas Egreso", set(hist["Tipo"]) == {"Egreso"}, str(set(hist["Tipo"])))
    hist["_usd"] = (pd.to_numeric(hist["Monto"]) / pd.to_numeric(hist["TRM"])).round(2)
    chk(f"suman USD {ESP_USD:,.2f}", abs(hist["_usd"].sum() - ESP_USD) < 0.005,
        f"{hist['_usd'].sum():,.2f}")
    _nom = hist["Nombre del producto"].astype(str)
    chk("todas con el prefijo de gasto de Intuit", _nom.str.startswith(PREFIJO_NOMBRE).all())

    # ── 2) lista viva ────────────────────────────────────────────────────────
    banner("2) LISTA VIVA")
    md_l = mod.dbx.files_get_metadata(remote)
    _, res_l = mod.dbx.files_download(remote)
    contenido_previo = res_l.content
    print(f"  {remote}\n  rev={md_l.rev}  size={md_l.size:,}  modificado={md_l.server_modified}")
    xls = pd.ExcelFile(io.BytesIO(contenido_previo))
    libro = {h: xls.parse(h) for h in xls.sheet_names}
    cob = libro["cobradas"]
    print(f"  hojas: {xls.sheet_names} · 'cobradas': {len(cob)} filas")
    print(f"  por tarjeta: {cob['tarjeta'].astype(str).str.lower().value_counts().to_dict()}")
    chk(f"la lista tiene {ESP_TOTAL_ANTES} entradas", len(cob) == ESP_TOTAL_ANTES, f"{len(cob)}")
    chk("0 entradas 'intuit' previas",
        int((cob["tarjeta"].astype(str).str.strip().str.lower() == TARJETA).sum()) == 0)

    # ── 3) construir las entradas ────────────────────────────────────────────
    banner("3) ENTRADAS NUEVAS (derivadas del histórico, con atributos y SIGNO)")
    filas = []
    for _, r in hist.sort_values(["Fecha", "Orden"]).iterrows():
        merch = str(r["Nombre del producto"])[len(PREFIJO_NOMBRE):]
        f = pd.to_datetime(r["Fecha"])
        filas.append({
            "Orden": str(r["Orden"]).strip(),
            "tarjeta": TARJETA,
            "casillero": int(CASILLERO),
            "fecha_compra": f,
            "monto_usd": float(r["_usd"]),
            "nota": NOTA,
            "fuente": FUENTE,
            "card_norm": CARD_NORM,
            "merchant_norm": mod._norm_merchant(merch),
            "usd_abs": abs(float(r["_usd"])),
            "fecha_attr": f,
            "attr_fuente": ATTR_FUENTE,
            "signo": str(r["Tipo"]).strip(),
        })
    nuevas = pd.DataFrame(filas)[list(cob.columns)]
    print(nuevas.to_string(index=False))
    chk(f"{ESP_NUEVAS} entradas", len(nuevas) == ESP_NUEVAS, f"{len(nuevas)}")
    chk("ninguna columna vacía en los atributos de la barrera 2",
        nuevas[["merchant_norm", "usd_abs", "fecha_attr", "signo"]].notna().all().all()
        and (nuevas["merchant_norm"].str.strip() != "").all())
    chk("0 Orden repetidos contra la lista",
        not set(nuevas["Orden"]) & set(cob["Orden"].astype(str).str.strip()))
    chk("mismas columnas que la hoja 'cobradas'", list(nuevas.columns) == list(cob.columns))

    # ── 4) libro nuevo ───────────────────────────────────────────────────────
    banner("4) LIBRO NUEVO")
    libro["cobradas"] = pd.concat([cob, nuevas], ignore_index=True)
    total = len(libro["cobradas"])
    print(f"  'cobradas': {len(cob)} → {total}")
    print(f"  por tarjeta: "
          f"{libro['cobradas']['tarjeta'].astype(str).str.lower().value_counts().to_dict()}")
    chk(f"total = {ESP_TOTAL_ANTES + ESP_NUEVAS}", total == ESP_TOTAL_ANTES + ESP_NUEVAS, f"{total}")
    for h in ("pendientes_rematch", "revision"):
        chk(f"'{h}' intacta", libro[h].equals(xls.parse(h)), f"{len(libro[h])} filas")
    for t in ("amex", "rakuten", "robinhood", "capital", "usbank"):
        a = int((cob["tarjeta"].astype(str).str.lower() == t).sum())
        b = int((libro["cobradas"]["tarjeta"].astype(str).str.lower() == t).sum())
        chk(f"'{t}' sin cambios", a == b, f"{b}")
    chk("0 Orden duplicados en toda la lista",
        not libro["cobradas"]["Orden"].astype(str).str.strip().duplicated().any())

    if not ok:
        raise SystemExit("⛔ ABORTA: falló alguna verificación.")

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        for h, d in libro.items():
            d.to_excel(w, sheet_name=h, index=False)
    buf.seek(0)
    data = buf.read()
    print(f"  {len(data):,} bytes")

    if not ESCRIBIR:
        banner("DRY-RUN TERMINADO — 0 ESCRITURAS")
        print("  Para escribir: python3 subir_lista_intuit.py --escribir")
        return

    # ── 5) respaldo + subida ─────────────────────────────────────────────────
    banner("5) 🛟 RESPALDO (WriteMode.add) + SUBIDA")
    md_pre = mod.dbx.files_get_metadata(remote)
    if md_pre.rev != md_l.rev:
        raise SystemExit(f"⛔ ABORTA SIN ESCRIBIR: la lista se movió (rev {md_pre.rev}).")
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = f"{carpeta}/{PurePosixPath(remote).stem}_backup_{ts}_pre_intuit.xlsx"
    mod.dbx.files_upload(contenido_previo, backup_path, mode=dropbox.files.WriteMode.add)
    print(f"  🛟 respaldo creado: {backup_path} ({len(contenido_previo):,} bytes)")
    mod.dbx.files_upload(data, remote, mode=dropbox.files.WriteMode.overwrite)
    print(f"  ✅ lista subida")

    # ── 6) verificación leyendo de vuelta ────────────────────────────────────
    banner("6) VALIDACIÓN POST-ESCRITURA (leyendo de vuelta)")
    md2 = mod.dbx.files_get_metadata(remote)
    print(f"  rev NUEVA = {md2.rev}  size = {md2.size:,}")
    _, r2 = mod.dbx.files_download(remote)
    x2 = pd.ExcelFile(io.BytesIO(r2.content))
    c2 = x2.parse("cobradas")
    ok = True
    chk(f"{ESP_TOTAL_ANTES + ESP_NUEVAS} entradas", len(c2) == ESP_TOTAL_ANTES + ESP_NUEVAS, f"{len(c2)}")
    chk(f"{ESP_NUEVAS} entradas 'intuit'",
        int((c2["tarjeta"].astype(str).str.lower() == TARJETA).sum()) == ESP_NUEVAS)
    chk("0 Orden duplicados", not c2["Orden"].astype(str).str.strip().duplicated().any())
    chk("las 3 hojas", x2.sheet_names == xls.sheet_names, str(x2.sheet_names))

    # La prueba de fuego: con la lista nueva, reprocesar el extracto no debe cargar NADA.
    banner("7) PRUEBA REAL: reprocesar el extracto con la lista nueva")
    # Nota: bajo el harness `cargar_tarjetas_cobradas` no lleva el caché de Streamlit, así que
    # esta llamada ya lee la lista recién subida (no hace falta invalidar nada).
    cobrados, pendientes, cobrados_df = mod.cargar_tarjetas_cobradas()
    harness.clear_msgs()
    raw = pd.read_csv("/Users/julianlopez/Downloads/transactions_1788184532.csv",
                      encoding="utf-8-sig")
    out = mod.procesar_intuit(raw, fecha_desde=mod.INTUIT_FECHA_DESDE, cobrados=cobrados,
                              pendientes=pendientes, hist_tarjetas=None, cobrados_df=cobrados_df)
    for niv, m in harness.drenar():
        print(f"  [{niv}] {m[:300]}")
    chk("recargar el mismo extracto NO cobra nada", out == {}, str(list(out)))
    print(f"\n  {'✅ LISTA ACTUALIZADA Y VERIFICADA' if ok else '🚨 REVISAR'}")
    print(f"  rev nueva: {md2.rev}\n  rollback:  {backup_path}")


if __name__ == "__main__":
    sys.path.insert(0, os.environ.get("HARNESS_DIR", "."))
    main()
