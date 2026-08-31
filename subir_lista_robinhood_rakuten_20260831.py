#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Registra en `tarjetas_cobradas.xlsx` TODAS las filas robinhood_ y rakuten_ del histórico.

POR QUÉ, si el dedup por Orden ya evita duplicar
------------------------------------------------
Igual que con Intuit: la lista es lo que habilita la BARRERA 2 (anti-recobro por atributos),
que solo puede usar cobros HUÉRFANOS —los que están en la lista y cuyo Orden el extracto ya no
genera—. Al 31-ago-2026 el histórico tenía 249 filas robinhood_ y 73 rakuten_ y NINGUNA estaba
en la lista: los 187 cobros 'robinhood' y 355 'rakuten' que sí están vienen del Excel del
backoffice (20260710), no de los cargues del módulo. Mientras el hash no se mueva no pasa nada,
pero Robinhood RE-FECHA movimientos (este mismo cargue detectó 6 así): si re-fecha una de las
que ya están en el histórico, su hash cambia, la barrera 1 no la ve —no está en la lista— y la
barrera 2 tampoco —no hay cobro huérfano que la tape— y se RE-COBRA. Es el caso que costó
COP 4.799.142 con Robinhood.

Registrarlas NO puede dejar de cobrar nada: mientras el extracto siga generando su Orden, la
barrera 1 las consume y nunca llegan a ser huérfanas (`_cobros_huerfanos_attr`).

LOS ATRIBUTOS SE CAPTURAN DEL PROPIO MÓDULO, no se reconstruyen del histórico: se envuelve
`_excluir_por_atributos` para quedarse con el df que la barrera recibe, que ya trae `_merch_attr`
(merchant normalizado, sin prefijo de reembolso), `_usd`, `_fecha`, `_cas` y `_tipo_attr`. Así
lo que se declara "ya cobrado" usa exactamente la misma normalización que la barrera.

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

CSV_ROBIN = "/Users/julianlopez/Downloads/1bc6f1a7-41f2-4da0-be35-ed3e3af0e259.csv"
CSV_RAKU = "/Users/julianlopez/Downloads/Rakuten_Activity_All (4).csv"
CASILLERO = 1444
NOTA = ("registro retroactivo 2026-08-31: filas del modulo 1-a-1 que nunca entraron a la lista "
        "(habilita la barrera 2 por atributos)")
FUENTE = "historico_mayoristas.xlsx rev 0165a59826bb3430 (cargue 2026-08-31)"

REV_HIST_ESPERADA = "0165a59826bb34300000002f34b3f21"
ESP = {"robinhood_": 249, "rakuten_": 73}
ESP_TOTAL_ANTES = 2317
PREV_EN_LISTA = {"robinhood": 187, "rakuten": 355}


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
    banner("1) FILAS robinhood_/rakuten_ DEL HISTÓRICO VIVO (la fuente de la verdad)")
    md_h = mod.dbx.files_get_metadata(cfg["remote_path"])
    _, res = mod.dbx.files_download(cfg["remote_path"])
    hojas = pd.read_excel(io.BytesIO(res.content), sheet_name=None)
    print(f"  histórico rev={md_h.rev}")
    chk("el histórico es el del cargue de hoy", md_h.rev == REV_HIST_ESPERADA)
    hist = {}
    for pref in ESP:
        partes = []
        for hoja, d in hojas.items():
            if "Orden" not in d.columns:
                continue
            sel = d[d["Orden"].astype(str).str.strip().str.startswith(pref)].copy()
            if len(sel):
                sel["_hoja"] = hoja
                partes.append(sel)
        hist[pref] = pd.concat(partes, ignore_index=True) if partes else pd.DataFrame()
        chk(f"{ESP[pref]} filas {pref} en el histórico", len(hist[pref]) == ESP[pref],
            f"{len(hist[pref])}")
        chk(f"{pref} todas en la hoja de 1444",
            set(hist[pref]["_hoja"].unique()) == {"1444 - Maria Moises"},
            str(set(hist[pref]["_hoja"].unique())))

    # ── 2) atributos capturados del MÓDULO (no reconstruidos) ────────────────
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
    mod.procesar_robinhood(pd.read_csv(CSV_ROBIN), fecha_desde=mod.ROBINHOOD_FECHA_DESDE,
                           cobrados=cobrados, pendientes=pendientes,
                           hist_tarjetas=hist_tarj, cobrados_df=cobrados_df)
    harness.clear_msgs()
    mod.procesar_rakuten(pd.read_csv(CSV_RAKU), fecha_desde=mod.RAKUTEN_FECHA_DESDE,
                         cobrados=cobrados, pendientes=pendientes,
                         hist_tarjetas=hist_tarj, cobrados_df=cobrados_df)
    harness.clear_msgs()
    mod._excluir_por_atributos = _orig
    for t in ("robinhood", "rakuten"):
        chk(f"capturado el df de la barrera de {t}", t in capturado,
            f"{len(capturado.get(t, [])) if t in capturado else 0} filas")
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
    for t, n in PREV_EN_LISTA.items():
        chk(f"'{t}' previas = {n}",
            int((cob["tarjeta"].astype(str).str.strip().str.lower() == t).sum()) == n)

    filas = []
    for pref, tarjeta in (("robinhood_", "robinhood"), ("rakuten_", "rakuten")):
        cap = capturado[tarjeta].copy()
        cap["_orden"] = cap["_orden"].astype(str).str.strip()
        cap = cap.drop_duplicates(subset=["_orden"], keep="first").set_index("_orden")
        h = hist[pref].copy()
        h["Orden"] = h["Orden"].astype(str).str.strip()
        falt = [o for o in h["Orden"] if o not in cap.index]
        chk(f"{tarjeta}: todos los Orden del histórico están en el extracto",
            not falt, f"{len(falt)} sin atributos {falt[:3]}")
        if falt:
            continue
        h["_usd_hist"] = (pd.to_numeric(h["Monto"]) / pd.to_numeric(h["TRM"])).round(2)
        dif_usd, dif_f = 0, 0
        for _, r in h.sort_values(["Fecha", "Orden"]).iterrows():
            c = cap.loc[r["Orden"]]
            usd = round(abs(float(c["_usd"])), 2)
            if abs(usd - abs(float(r["_usd_hist"]))) > 0.02:
                dif_usd += 1
            f_attr = pd.to_datetime(c["_fecha"])
            if f_attr.date() != pd.to_datetime(r["Fecha"]).date():
                dif_f += 1
            card = (str(c["Cardholder"]).strip().upper() if "Cardholder" in cap.columns
                    else mod.RAKUTEN_USUARIO.upper())
            filas.append({
                "Orden": r["Orden"],
                "tarjeta": tarjeta,
                "casillero": CASILLERO,
                "fecha_compra": f_attr,
                "monto_usd": usd,
                "nota": NOTA,
                "fuente": FUENTE,
                "card_norm": card,
                "merchant_norm": mod._norm_merchant(c["_merch_attr"]),
                "usd_abs": usd,
                "fecha_attr": f_attr,
                "attr_fuente": f"extracto {tarjeta}",
                "signo": str(r["Tipo"]).strip(),
            })
        chk(f"{tarjeta}: USD del módulo == USD del histórico", dif_usd == 0, f"{dif_usd} difieren")
        chk(f"{tarjeta}: fecha del módulo == fecha del histórico", dif_f == 0, f"{dif_f} difieren")

    nuevas = pd.DataFrame(filas)[list(cob.columns)]
    n_esp = sum(ESP.values())
    chk(f"{n_esp} entradas nuevas", len(nuevas) == n_esp, f"{len(nuevas)}")
    chk("ningún atributo de la barrera 2 vacío",
        nuevas[["merchant_norm", "usd_abs", "fecha_attr", "signo"]].notna().all().all()
        and (nuevas["merchant_norm"].astype(str).str.strip() != "").all()
        and (nuevas["signo"].isin(["Egreso", "Ingreso"])).all())
    chk("0 Orden repetidos contra la lista",
        not set(nuevas["Orden"]) & set(cob["Orden"].astype(str).str.strip()))
    chk("0 Orden repetidos entre sí", not nuevas["Orden"].duplicated().any())
    print(f"\n  por tarjeta: {nuevas['tarjeta'].value_counts().to_dict()}")
    print(f"  por signo:   {nuevas['signo'].value_counts().to_dict()}")
    print(f"  USD total:   {nuevas['usd_abs'].sum():,.2f}")
    print(f"  fechas:      {nuevas['fecha_attr'].min().date()} → {nuevas['fecha_attr'].max().date()}")
    print("\n  muestra:")
    print(nuevas.head(4).to_string(index=False))

    # ── 4) libro nuevo ───────────────────────────────────────────────────────
    banner("4) LIBRO NUEVO")
    libro["cobradas"] = pd.concat([cob, nuevas], ignore_index=True)
    total = len(libro["cobradas"])
    print(f"  'cobradas': {len(cob)} → {total}")
    print(f"  por tarjeta: "
          f"{libro['cobradas']['tarjeta'].astype(str).str.lower().value_counts().to_dict()}")
    chk(f"total = {ESP_TOTAL_ANTES + n_esp}", total == ESP_TOTAL_ANTES + n_esp, f"{total}")
    for h in ("pendientes_rematch", "revision"):
        chk(f"'{h}' intacta", libro[h].equals(xls.parse(h)), f"{len(libro[h])} filas")
    for t in ("amex", "capital", "usbank", "intuit"):
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
        print("  Para escribir: python3 subir_lista_robinhood_rakuten_20260831.py --escribir")
        return

    # ── 5) respaldo + subida ─────────────────────────────────────────────────
    banner("5) 🛟 RESPALDO (WriteMode.add) + SUBIDA")
    md_pre = mod.dbx.files_get_metadata(remote)
    if md_pre.rev != md_l.rev:
        raise SystemExit(f"⛔ ABORTA SIN ESCRIBIR: la lista se movió (rev {md_pre.rev}).")
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = f"{carpeta}/{PurePosixPath(remote).stem}_backup_{ts}_pre_rbrk.xlsx"
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
    chk(f"{ESP_TOTAL_ANTES + n_esp} entradas", len(c2) == ESP_TOTAL_ANTES + n_esp, f"{len(c2)}")
    for t, prev in PREV_EN_LISTA.items():
        esp = prev + ESP[t + "_"]
        chk(f"'{t}' = {esp}", int((c2["tarjeta"].astype(str).str.lower() == t).sum()) == esp,
            f"{int((c2['tarjeta'].astype(str).str.lower() == t).sum())}")
    chk("0 Orden duplicados", not c2["Orden"].astype(str).str.strip().duplicated().any())
    chk("las 3 hojas", x2.sheet_names == xls.sheet_names, str(x2.sheet_names))

    # ── 7) prueba de fuego ───────────────────────────────────────────────────
    banner("7) PRUEBA DE FUEGO: reprocesar los dos extractos con la lista nueva")
    cobrados2, pendientes2, cdf2 = mod.cargar_tarjetas_cobradas()
    harness.clear_msgs()
    o_rb = mod.procesar_robinhood(pd.read_csv(CSV_ROBIN), fecha_desde=mod.ROBINHOOD_FECHA_DESDE,
                                  cobrados=cobrados2, pendientes=pendientes2,
                                  hist_tarjetas=hist_tarj, cobrados_df=cdf2)
    for niv, m in harness.drenar():
        print(f"  [robinhood/{niv}] {m[:220]}")
    o_rk = mod.procesar_rakuten(pd.read_csv(CSV_RAKU), fecha_desde=mod.RAKUTEN_FECHA_DESDE,
                                cobrados=cobrados2, pendientes=pendientes2,
                                hist_tarjetas=hist_tarj, cobrados_df=cdf2)
    for niv, m in harness.drenar():
        print(f"  [rakuten/{niv}] {m[:220]}")
    n_rb = sum(len(v) for v in o_rb.values())
    n_rk = sum(len(v) for v in o_rk.values())
    chk("recargar el extracto de Robinhood NO cobra nada", n_rb == 0, f"{n_rb} filas")
    chk("recargar el extracto de Rakuten NO cobra nada", n_rk == 0, f"{n_rk} filas")
    print(f"\n  {'✅ LISTA ACTUALIZADA Y VERIFICADA' if ok else '🚨 REVISAR'}")
    print(f"  rev nueva: {md2.rev}\n  rollback:  {backup_path}")


if __name__ == "__main__":
    sys.path.insert(0, os.environ.get("HARNESS_DIR", "."))
    main()
