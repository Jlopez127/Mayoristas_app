#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Registra en `tarjetas_cobradas.xlsx` las 115 filas capital_ del histórico.

POR QUÉ
-------
Cierra el último hueco encontrado el 31-ago-2026 al auditar la cobertura de la lista por tarjeta:

    Robinhood 249/249 ✔   Rakuten 73/73 ✔   Intuit 7/7 ✔
    Capital     0/115 ⚠️  ← este script
    Amex 0/245 · US Bank 9/31 · Apple Pay 0/4  -> Orden con ID NATIVO estable, riesgo bajo

Capital importa porque su Orden es `capital_<sha1-12>`, un HASH: si el emisor re-fecha o
re-expide un movimiento el hash cambia, la barrera 1 no lo ve —no está en la lista— y la
barrera 2 tampoco —no hay cobro huérfano que lo tape— y se RE-COBRA. Es exactamente la
exposición que tenían Robinhood y Rakuten esta mañana (ver `subir_lista_robinhood_rakuten_*`).
Amex, US Bank y Apple Pay quedan fuera a propósito: sus Orden son IDs nativos estables.

Registrarlas NO puede dejar de cobrar nada: mientras el extracto siga generando ese Orden, la
barrera 1 lo consume y nunca llega a ser huérfano (`_cobros_huerfanos_attr`).

Los atributos se capturan envolviendo `_excluir_por_atributos`, así usan la misma normalización
que la barrera en vez de reconstruirse del histórico.

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

CSV_CAPITAL = "/Users/julianlopez/Downloads/2026-08-31_transaction_download (5).csv"
TARJETA = "capital"
PREFIJO = "capital_"
CASILLERO = 13608
CARD_NORM = "JULIAN SANCHEZ"
NOTA = ("registro retroactivo 2026-08-31: filas del modulo 1-a-1 que nunca entraron a la lista "
        "(habilita la barrera 2 por atributos; el Orden de Capital es hash)")
FUENTE = "historico_mayoristas.xlsx rev 0165a59826bb3430 (cargue 2026-08-31)"

REV_HIST_ESPERADA = "0165a59826bb34300000002f34b3f21"
ESP_FILAS = 115
ESP_TOTAL_ANTES = 2639
PREV_CAPITAL = 80
# el resto de la lista no se toca
OTRAS = {"amex": 1679, "robinhood": 436, "rakuten": 428, "usbank": 9, "intuit": 7}


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
    cfg = mod.st.secrets["dropbox"]
    carpeta = str(PurePosixPath(cfg["remote_path"]).parent)
    remote = f"{carpeta}/{mod.TARJETAS_COBRADAS_FILENAME}"

    # ── 1) las filas REALES del histórico ────────────────────────────────────
    banner("1) FILAS capital_ DEL HISTÓRICO VIVO (la fuente de la verdad)")
    md_h = mod.dbx.files_get_metadata(cfg["remote_path"])
    _, res = mod.dbx.files_download(cfg["remote_path"])
    hojas = pd.read_excel(io.BytesIO(res.content), sheet_name=None)
    print(f"  histórico rev={md_h.rev}")
    chk("el histórico es el del cargue de hoy", md_h.rev == REV_HIST_ESPERADA)
    partes = []
    for hoja, d in hojas.items():
        if "Orden" not in d.columns:
            continue
        sel = d[d["Orden"].astype(str).str.strip().str.startswith(PREFIJO)].copy()
        if len(sel):
            sel["_hoja"] = hoja
            partes.append(sel)
    hist = pd.concat(partes, ignore_index=True) if partes else pd.DataFrame()
    chk(f"{ESP_FILAS} filas {PREFIJO} en el histórico", len(hist) == ESP_FILAS, f"{len(hist)}")
    chk("todas en la hoja de 13608", set(hist["_hoja"].unique()) == {"13608 - julian sanchez"},
        str(set(hist["_hoja"].unique())))
    chk("todas con Motivo 'Tarjeta Capital'",
        set(hist["Motivo"].astype(str).str.strip()) == {"Tarjeta Capital"})
    print(f"  Tipo: {hist['Tipo'].value_counts().to_dict()} · "
          f"fechas {pd.to_datetime(hist['Fecha']).min().date()} → "
          f"{pd.to_datetime(hist['Fecha']).max().date()}")

    # ── 2) atributos capturados del MÓDULO ───────────────────────────────────
    banner("2) ATRIBUTOS CAPTURADOS DE LA PROPIA BARRERA 2")
    capturado = {}
    _orig = mod._excluir_por_atributos
    def _wrap(df, cobrados_df, tarjeta, ordenes_extracto, rango, etiqueta):
        capturado[tarjeta] = df.copy()
        return _orig(df, cobrados_df, tarjeta, ordenes_extracto, rango, etiqueta)
    mod._excluir_por_atributos = _wrap
    harness.clear_msgs()
    cobrados, pendientes, cobrados_df = mod.cargar_tarjetas_cobradas()
    harness.clear_msgs()
    hist_tarj = mod.cargar_hist_tarjetas()
    harness.clear_msgs()
    mod.procesar_capital(pd.read_csv(CSV_CAPITAL), fecha_desde=mod.CAPITAL_FECHA_DESDE,
                         cobrados=cobrados, pendientes=pendientes,
                         hist_tarjetas=hist_tarj, cobrados_df=cobrados_df)
    harness.clear_msgs()
    mod._excluir_por_atributos = _orig
    chk("capturado el df de la barrera de capital", TARJETA in capturado,
        f"{len(capturado.get(TARJETA, [])) if TARJETA in capturado else 0} filas")
    if not ok:
        raise SystemExit("⛔ ABORTA: no se pudieron capturar los atributos del módulo.")

    # ── 3) construir las entradas ────────────────────────────────────────────
    banner("3) ENTRADAS (Orden del histórico × atributos del módulo)")
    md_l = mod.dbx.files_get_metadata(remote)
    _, res_l = mod.dbx.files_download(remote)
    contenido_previo = res_l.content
    xls = pd.ExcelFile(io.BytesIO(contenido_previo))
    libro = {h: xls.parse(h) for h in xls.sheet_names}
    cob = libro["cobradas"]
    print(f"  lista rev={md_l.rev} · 'cobradas': {len(cob)} filas")
    chk(f"la lista tiene {ESP_TOTAL_ANTES} entradas", len(cob) == ESP_TOTAL_ANTES, f"{len(cob)}")
    chk(f"'capital' previas = {PREV_CAPITAL}",
        int((cob["tarjeta"].astype(str).str.strip().str.lower() == TARJETA).sum()) == PREV_CAPITAL)

    cap = capturado[TARJETA].copy()
    cap["_orden"] = cap["_orden"].astype(str).str.strip()
    cap = cap.drop_duplicates(subset=["_orden"], keep="first").set_index("_orden")
    h = hist.copy()
    h["Orden"] = h["Orden"].astype(str).str.strip()
    falt = [o for o in h["Orden"] if o not in cap.index]
    chk("todos los Orden del histórico están en el extracto", not falt,
        f"{len(falt)} sin atributos {falt[:3]}")
    if falt:
        raise SystemExit("⛔ ABORTA: hay filas sin atributos derivables del extracto.")

    h["_usd_hist"] = (pd.to_numeric(h["Monto"]) / pd.to_numeric(h["TRM"])).round(2)
    filas, dif_usd, dif_f = [], 0, 0
    for _, r in h.sort_values(["Fecha", "Orden"]).iterrows():
        c = cap.loc[r["Orden"]]
        usd = round(abs(float(c["_usd"])), 2)
        if abs(usd - abs(float(r["_usd_hist"]))) > 0.02:
            dif_usd += 1
        f_attr = pd.to_datetime(c["_fecha"])
        if f_attr.date() != pd.to_datetime(r["Fecha"]).date():
            dif_f += 1
        filas.append({
            "Orden": r["Orden"], "tarjeta": TARJETA, "casillero": CASILLERO,
            "fecha_compra": f_attr, "monto_usd": usd, "nota": NOTA, "fuente": FUENTE,
            "card_norm": CARD_NORM, "merchant_norm": mod._norm_merchant(c["_merch_attr"]),
            "usd_abs": usd, "fecha_attr": f_attr, "attr_fuente": f"extracto {TARJETA}",
            "signo": str(r["Tipo"]).strip(),
        })
    chk("USD del módulo == USD del histórico", dif_usd == 0, f"{dif_usd} difieren")
    chk("fecha del módulo == fecha del histórico", dif_f == 0, f"{dif_f} difieren")

    nuevas = pd.DataFrame(filas)[list(cob.columns)]
    chk(f"{ESP_FILAS} entradas nuevas", len(nuevas) == ESP_FILAS, f"{len(nuevas)}")
    chk("ningún atributo de la barrera 2 vacío",
        nuevas[["merchant_norm", "usd_abs", "fecha_attr", "signo"]].notna().all().all()
        and (nuevas["merchant_norm"].astype(str).str.strip() != "").all()
        and (nuevas["signo"].isin(["Egreso", "Ingreso"])).all())
    chk("el signo distingue compra de devolución",
        nuevas["signo"].value_counts().to_dict() == hist["Tipo"].value_counts().to_dict(),
        str(nuevas["signo"].value_counts().to_dict()))
    chk("0 Orden repetidos contra la lista",
        not set(nuevas["Orden"]) & set(cob["Orden"].astype(str).str.strip()))
    chk("0 Orden repetidos entre sí", not nuevas["Orden"].duplicated().any())
    print(f"\n  USD total: {nuevas['usd_abs'].sum():,.2f} · "
          f"fechas {nuevas['fecha_attr'].min().date()} → {nuevas['fecha_attr'].max().date()}")
    print("\n  muestra:")
    print(nuevas.head(3).to_string(index=False))

    # ── 4) libro nuevo ───────────────────────────────────────────────────────
    banner("4) LIBRO NUEVO")
    libro["cobradas"] = pd.concat([cob, nuevas], ignore_index=True)
    total = len(libro["cobradas"])
    print(f"  'cobradas': {len(cob)} → {total}")
    print(f"  por tarjeta: "
          f"{libro['cobradas']['tarjeta'].astype(str).str.lower().value_counts().to_dict()}")
    chk(f"total = {ESP_TOTAL_ANTES + ESP_FILAS}", total == ESP_TOTAL_ANTES + ESP_FILAS, f"{total}")
    for hh in ("pendientes_rematch", "revision"):
        chk(f"'{hh}' intacta", libro[hh].equals(xls.parse(hh)), f"{len(libro[hh])} filas")
    for t, n in OTRAS.items():
        chk(f"'{t}' sin cambios",
            int((libro["cobradas"]["tarjeta"].astype(str).str.lower() == t).sum()) == n, f"{n}")
    chk("0 Orden duplicados en toda la lista",
        not libro["cobradas"]["Orden"].astype(str).str.strip().duplicated().any())
    if not ok:
        raise SystemExit("⛔ ABORTA: falló alguna verificación.")

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        for hh, dd in libro.items():
            dd.to_excel(w, sheet_name=hh, index=False)
    buf.seek(0)
    data = buf.read()
    print(f"  {len(data):,} bytes")

    if not ESCRIBIR:
        banner("DRY-RUN TERMINADO — 0 ESCRITURAS")
        print("  Para escribir: python3 subir_lista_capital_20260831.py --escribir")
        return

    # ── 5) respaldo + subida ─────────────────────────────────────────────────
    banner("5) 🛟 RESPALDO (WriteMode.add) + SUBIDA")
    md_pre = mod.dbx.files_get_metadata(remote)
    if md_pre.rev != md_l.rev:
        raise SystemExit(f"⛔ ABORTA SIN ESCRIBIR: la lista se movió (rev {md_pre.rev}).")
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = f"{carpeta}/{PurePosixPath(remote).stem}_backup_{ts}_pre_capital.xlsx"
    mod.dbx.files_upload(contenido_previo, backup_path, mode=dropbox.files.WriteMode.add)
    print(f"  🛟 respaldo creado: {backup_path} ({len(contenido_previo):,} bytes)")
    mod.dbx.files_upload(data, remote, mode=dropbox.files.WriteMode.overwrite)
    print("  ✅ lista subida")

    # ── 6) verificación leyendo de vuelta ────────────────────────────────────
    banner("6) VALIDACIÓN POST-ESCRITURA (leyendo de vuelta)")
    md2 = mod.dbx.files_get_metadata(remote)
    print(f"  rev NUEVA = {md2.rev}  size = {md2.size:,}")
    _, r2 = mod.dbx.files_download(remote)
    x2 = pd.ExcelFile(io.BytesIO(r2.content))
    c2 = x2.parse("cobradas")
    ok = True
    chk(f"{ESP_TOTAL_ANTES + ESP_FILAS} entradas", len(c2) == ESP_TOTAL_ANTES + ESP_FILAS, f"{len(c2)}")
    chk(f"'capital' = {PREV_CAPITAL + ESP_FILAS}",
        int((c2["tarjeta"].astype(str).str.lower() == TARJETA).sum()) == PREV_CAPITAL + ESP_FILAS,
        f"{int((c2['tarjeta'].astype(str).str.lower() == TARJETA).sum())}")
    for t, n in OTRAS.items():
        chk(f"'{t}' intacta", int((c2["tarjeta"].astype(str).str.lower() == t).sum()) == n, f"{n}")
    chk("0 Orden duplicados", not c2["Orden"].astype(str).str.strip().duplicated().any())
    chk("las 3 hojas", x2.sheet_names == xls.sheet_names, str(x2.sheet_names))

    # ── 7) prueba de fuego ───────────────────────────────────────────────────
    banner("7) PRUEBA DE FUEGO: reprocesar el extracto de Capital con la lista nueva")
    cobrados2, pendientes2, cdf2 = mod.cargar_tarjetas_cobradas()
    harness.clear_msgs()
    out = mod.procesar_capital(pd.read_csv(CSV_CAPITAL), fecha_desde=mod.CAPITAL_FECHA_DESDE,
                               cobrados=cobrados2, pendientes=pendientes2,
                               hist_tarjetas=hist_tarj, cobrados_df=cdf2)
    for niv, m in harness.drenar():
        print(f"  [{niv}] {m[:220]}")
    n = sum(len(vv) for vv in out.values())
    chk("recargar el extracto de Capital NO cobra nada", n == 0, f"{n} filas")
    print(f"\n  {'✅ LISTA ACTUALIZADA Y VERIFICADA' if ok else '🚨 REVISAR'}")
    print(f"  rev nueva: {md2.rev}\n  rollback:  {backup_path}")


if __name__ == "__main__":
    sys.path.insert(0, os.environ.get("HARNESS_DIR", "."))
    main()
