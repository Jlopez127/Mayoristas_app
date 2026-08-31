#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cargue de ROBINHOOD + RAKUTEN del 2026-08-31, en UNA sola escritura. Ambas -> hoja 1444.

QUÉ ENTRA
---------
A) ROBINHOOD -> 1444, con `procesar_robinhood`.
   Extracto: 1bc6f1a7-...csv (2026-01-01 → 2026-08-30, 1.769 filas).
   Solo cuentan los Cardholder de ROBINHOOD_CARDMAP (Juan Pablo Correal Perez / Maria Moises).
   Santiago Largo (1.204), Carlos Largo (52) y Largo Kelly (51) NO son de 1444 y se ignoran
   por constante — se reconcilian aquí para dejar constancia de que salieron a propósito.
B) RAKUTEN -> 1444, con `procesar_rakuten`.
   Extracto: Rakuten_Activity_All (4).csv (2025-11-21 → 2026-08-30, 570 filas).
   Solo TRANSACTION/REFUND; PAYMENT/OFFER/AUTH se ignoran por constante. Las 4 AUTH del
   28..30-ago son autorizaciones sin asentar: NO entran (asentarán como TRANSACTION).

RANGO COMPLETO A PROPÓSITO (regla 2 del backoffice): recargar no duplica —la lista y el hash
lo impiden— pero cargar de menos deja movimientos tardíos por fuera.

POR QUÉ NO USA EL PIPELINE DE main()
------------------------------------
`main()` deduplica ingresos con drop_duplicates(["Orden","Tipo"]); aquí el dedup es QUIRÚRGICO,
solo sobre las filas del prefijo de cada tarjeta en 1444.

COMISIÓN QUINCENAL: no se corre. Hoy es día ≥16, así que su ventana es "1-15 agosto" (ya
escrita) y NINGUNA fila de este cargue cae ahí. Las filas de 16-31 agosto que entran hoy sí
contarán para la comisión de esa quincena, que se calcula en el primer cargue de septiembre
(AMEX_AFECTA_COMISION_1444=True -> las filas de tarjeta SÍ pesan en la comisión de 1444).
Se VERIFICA que las comisiones ya escritas quedan idénticas.

INCENTIVO: `INCENTIVO_AMEX_ACTIVO` es True desde hoy, pero este script NUNCA llama a
`agregar_incentivo_amex`, y además con fecha_carga de hoy (31-ago) `_incentivo_meses_objetivo`
devuelve [] — el primer incentivo automático es el de agosto, en la 1ª corrida de septiembre.
Ambas cosas se comprueban abajo.

BLINDAJE: capa B (`preservar_filas_tarjeta`) -> guard A (`guard_frescura_historico`) ->
capa C (backup automático dentro de `upload_to_dropbox`).

  sin argumentos  -> dry-run (0 escrituras)
  --escribir      -> escribe a Dropbox
"""
import sys, os, io, warnings
warnings.filterwarnings("ignore")
import pandas as pd

ESCRIBIR = "--escribir" in sys.argv

CSV_ROBIN = "/Users/julianlopez/Downloads/1bc6f1a7-41f2-4da0-be35-ed3e3af0e259.csv"
CSV_RAKU = "/Users/julianlopez/Downloads/Rakuten_Activity_All (4).csv"
CASILLERO = "1444"

# Barandas de frescura (se exigen en --escribir).
REV_ESPERADA = "0165a58afd6b4b700000002f34b3f21"
SALDO_ESPERADO = 56285536.21

# Conteos del vivo ANTES del cargue (para el diff).
ESP_RAKU_PREVIAS = 65
ESP_ROBIN_PREVIAS = 226


def saldo(d):
    t = d[d["Tipo"].astype(str).str.strip().str.upper() == "TOTAL"]
    return float(pd.to_numeric(t["Monto"], errors="coerce").iloc[-1]) if len(t) else float("nan")


def usuario_de_totales(d):
    t = d[d["Tipo"].astype(str).str.strip().str.upper() == "TOTAL"]
    u = t["Usuario"].astype(str).str.strip()
    u = u[~u.str.lower().isin({"", "nan", "none"})]
    if len(u):
        return u.mode().iloc[0]
    u = d["Usuario"].astype(str).str.strip()
    u = u[~u.str.lower().isin({"", "nan", "none"})]
    return u.mode().iloc[0] if len(u) else ""


def cas_de_totales(d, fallback):
    t = d[d["Tipo"].astype(str).str.strip().str.upper() == "TOTAL"]
    v = t["Casillero"].dropna()
    return v.iloc[-1] if len(v) else fallback


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

    print(f"MODO: {'🔴 ESCRITURA REAL' if ESCRIBIR else '🟢 DRY-RUN (0 escrituras)'}")
    print(f"ROBINHOOD_FECHA_DESDE={mod.ROBINHOOD_FECHA_DESDE} · "
          f"VENTANA_MANUAL_FIN={mod.ROBINHOOD_VENTANA_MANUAL_FIN} · "
          f"CARDMAP={mod.ROBINHOOD_CARDMAP}")
    print(f"RAKUTEN_FECHA_DESDE={mod.RAKUTEN_FECHA_DESDE} · "
          f"IGNORAR={sorted(mod.RAKUTEN_TIPOS_IGNORAR)}")
    assert mod.ROBINHOOD_CARDMAP == {"Juan Pablo Correal Perez": "1444", "Maria Moises": "1444"}, \
        "el cardmap de Robinhood cambió; abortar"
    assert mod.AMEX_AFECTA_COMISION_1444 is True

    # ── 0) el incentivo NO se dispara en esta corrida ─────────────────────────
    banner("0) INCENTIVO — verificar que esta corrida no lo toca")
    fecha_carga = pd.Timestamp.today().strftime("%Y-%m-%d")
    meses_obj = mod._incentivo_meses_objetivo(pd.Timestamp(fecha_carga))
    print(f"  INCENTIVO_AMEX_ACTIVO={mod.INCENTIVO_AMEX_ACTIVO} · fecha_carga={fecha_carga}")
    chk("meses a incentivar hoy = [] (el 1o es agosto, en septiembre)", meses_obj == [],
        f"{meses_obj}")
    # tripwire REAL: si algo llamara al incentivo, revienta la corrida (no un chequeo por texto)
    _inc_llamado = []
    _inc_orig = mod.agregar_incentivo_amex
    def _inc_trip(*a, **k):
        _inc_llamado.append(1)
        raise SystemExit("⛔ ABORTA: algo llamó a agregar_incentivo_amex en esta corrida.")
    mod.agregar_incentivo_amex = _inc_trip
    print("  ✔ tripwire armado sobre agregar_incentivo_amex")

    # ── 1) vivo fresco ────────────────────────────────────────────────────────
    banner("1) HISTÓRICO VIVO FRESCO")
    cfg = mod.st.secrets["dropbox"]
    md = mod.dbx.files_get_metadata(cfg["remote_path"])
    print(f"  rev={md.rev}  modificado={md.server_modified}  size={md.size:,}")
    _, res = mod.dbx.files_download(cfg["remote_path"])
    vivo = pd.read_excel(io.BytesIO(res.content), sheet_name=None)
    HOJA = next(h for h in vivo if h.split(" - ")[0].strip() == CASILLERO)
    s0 = saldo(vivo[HOJA])
    o0 = vivo[HOJA]["Orden"].astype(str).str.strip()
    n0 = {p: int(o0.str.startswith(p).sum()) for p in ("rakuten_", "robinhood_")}
    print(f"  hoja '{HOJA}': {len(vivo[HOJA])} filas · saldo COP {s0:,.2f}")
    print(f"  previas: rakuten_={n0['rakuten_']} · robinhood_={n0['robinhood_']}")
    if md.rev != REV_ESPERADA or abs(s0 - SALDO_ESPERADO) > 0.01 or \
       n0["rakuten_"] != ESP_RAKU_PREVIAS or n0["robinhood_"] != ESP_ROBIN_PREVIAS:
        print(f"  🚨 EL HISTÓRICO SE MOVIÓ (rev esperada {REV_ESPERADA})")
        if ESCRIBIR:
            raise SystemExit("⛔ ABORTA: rehacer el dry-run con el histórico fresco.")
        print("  ⚠️ dry-run continúa, pero actualizar las barandas antes de escribir")
    else:
        print("  ✔ rev, saldo y conteos idénticos a los esperados")

    # ── 2) lista de exclusión ─────────────────────────────────────────────────
    banner("2) LISTA DE EXCLUSIÓN (obligatoria)")
    cobrados, pendientes, cobrados_df = mod.cargar_tarjetas_cobradas()
    harness.clear_msgs()
    vc = cobrados_df["tarjeta"].astype(str).str.strip().str.lower().value_counts().to_dict()
    print(f"  {len(cobrados_df)} cobros · {vc}")
    for t in ("robinhood", "rakuten"):
        if vc.get(t, 0) == 0:
            raise SystemExit(f"⛔ ABORTA: la lista no trae entradas '{t}'.")
    if not {"card_norm", "merchant_norm", "usd_abs", "fecha_attr", "signo"} <= set(cobrados_df.columns):
        raise SystemExit("⛔ ABORTA: la lista no trae columnas de atributos (barrera 2 inactiva).")
    print("  ✔ trae robinhood + rakuten y las columnas de atributos (barrera 2 activa)")
    hist_tarj = mod.cargar_hist_tarjetas()
    harness.clear_msgs()

    def prueba_extracto_corto(nom, nuevas, prefijo):
        """CLAUDE.md: reprocesar filas YA CARGADAS debe dar 0 diferencias en Monto y TRM.
        Es la trampa que costó −75.990 COP con el extracto corto de Capital (dos veces).
        Devuelve (df_reprocesadas_merge, df_nuevas_de_verdad)."""
        vi = vivo[HOJA].copy()
        vi["Orden"] = vi["Orden"].astype(str).str.strip()
        vi = vi[vi["Orden"].str.startswith(prefijo)]
        nu = nuevas.copy()
        nu["Orden"] = nu["Orden"].astype(str).str.strip()
        m = vi.merge(nu, on="Orden", suffixes=("_hist", "_new"))
        dM = (pd.to_numeric(m["Monto_new"], errors="coerce")
              - pd.to_numeric(m["Monto_hist"], errors="coerce"))
        dT = (pd.to_numeric(m["TRM_new"], errors="coerce")
              - pd.to_numeric(m["TRM_hist"], errors="coerce"))
        dTipo = m["Tipo_hist"].astype(str).str.strip() != m["Tipo_new"].astype(str).str.strip()
        print(f"\n  🧪 PRUEBA DEL EXTRACTO CORTO ({nom}): {len(m)} filas ya cargadas se reprocesan")
        chk(f"  {nom}: 0 diferencias en Monto", int((dM.abs() > 0.5).sum()) == 0,
            f"Σ hist {pd.to_numeric(m['Monto_hist']).sum():,.0f} · Δ {dM.sum():+,.2f}")
        chk(f"  {nom}: 0 diferencias en TRM", int((dT.abs() > 0.005).sum()) == 0)
        chk(f"  {nom}: 0 cambios de Tipo", int(dTipo.sum()) == 0)
        if int((dM.abs() > 0.5).sum()):
            cols = ["Orden", "Fecha_hist", "Monto_hist", "Monto_new", "TRM_hist", "TRM_new"]
            print(m.loc[dM.abs() > 0.5, cols].head(15).to_string(index=False))
        nuevas_reales = nu[~nu["Orden"].isin(set(vi["Orden"]))].copy()
        return m, nuevas_reales

    # ── 3) PARTE A — Robinhood ────────────────────────────────────────────────
    banner("3) PARTE A — ROBINHOOD -> 1444")
    raw_rb = pd.read_csv(CSV_ROBIN)
    _fr = pd.to_datetime(raw_rb["Date"])
    print(f"  extracto: {len(raw_rb)} filas · {_fr.min().date()} → {_fr.max().date()}")
    out_rb = mod.procesar_robinhood(raw_rb.copy(), fecha_desde=mod.ROBINHOOD_FECHA_DESDE,
                                    cobrados=cobrados, pendientes=pendientes,
                                    hist_tarjetas=hist_tarj, cobrados_df=cobrados_df)
    msgs_rb = harness.drenar()
    for niv, m in msgs_rb:
        print(f"  [{niv}] {m[:500]}")
    print(f"  claves devueltas: {list(out_rb)}")
    if set(out_rb) - {f"robinhood_{CASILLERO}"}:
        raise SystemExit(f"⛔ ABORTA: Robinhood devolvió casilleros inesperados: {set(out_rb)}")
    rb = out_rb.get(f"robinhood_{CASILLERO}", pd.DataFrame())

    # reconciliación INDEPENDIENTE (parseo a mano, sin usar el módulo)
    r = raw_rb.copy()
    r["_f"] = pd.to_datetime(r["Date"])
    r["_ch"] = r["Cardholder"].astype(str).str.strip()
    r["_st"] = r["Status"].astype(str).str.strip().str.upper()
    r["_ty"] = r["Type"].astype(str).str.strip().str.upper()
    print(f"\n  RECONCILIACIÓN ROBINHOOD")
    print(f"    {len(r):>5} filas en el extracto")
    mios = r["_ch"].isin(mod.ROBINHOOD_CARDMAP)
    print(f"    −{int((~mios).sum()):>4} de titulares que NO son de 1444:")
    for ch, g in r[~mios].groupby("_ch"):
        print(f"           {ch:<26} {len(g):>5} filas  USD {g['Amount'].sum():>12,.2f}")
    m = r[mios]
    m_st = m["_st"] != "POSTED"
    print(f"    −{int(m_st.sum()):>4} no Posted ({m[m_st]['_st'].value_counts().to_dict()})")
    m2 = m[~m_st]
    m_ty = ~m2["_ty"].isin(set(mod.ROBINHOOD_TIPO_MAP))
    print(f"    −{int(m_ty.sum()):>4} tipo ignorado ({m2[m_ty]['_ty'].value_counts().to_dict()})")
    m3 = m2[~m_ty]
    m_ant = m3["_f"] < pd.Timestamp(mod.ROBINHOOD_FECHA_DESDE)
    print(f"    −{int(m_ant.sum()):>4} anteriores al corte {mod.ROBINHOOD_FECHA_DESDE}")
    cand_rb = m3[~m_ant]
    print(f"    −{len(cand_rb) - len(rb):>4} ya liquidadas (lista / atributos)")
    print(f"    ={len(rb):>4} devueltas por procesar_robinhood")
    chk("candidatas ≥ devueltas (el filtro nunca inventa filas)", len(cand_rb) >= len(rb),
        f"{len(cand_rb)} vs {len(rb)}")
    _m_rb, rb_nuevas = prueba_extracto_corto("ROBINHOOD", rb, "robinhood_") if not rb.empty else (None, rb)
    print(f"\n  DE ESAS {len(rb)}: {len(rb) - len(rb_nuevas)} ya estaban en el histórico "
          f"(se reescriben idénticas) · {len(rb_nuevas)} son NUEVAS DE VERDAD")

    if not rb_nuevas.empty:
        rb2 = rb_nuevas.copy()
        rb2["_f"] = pd.to_datetime(rb2["Fecha"])
        rb2["USD"] = (pd.to_numeric(rb2["Monto"]) / pd.to_numeric(rb2["TRM"])).round(2)
        e = rb2[rb2["Tipo"] == "Egreso"]; i = rb2[rb2["Tipo"] == "Ingreso"]
        print(f"\n  {len(e):>3} Egreso  USD {e['USD'].sum():>10,.2f}  COP {e['Monto'].sum():>13,.0f}")
        print(f"  {len(i):>3} Ingreso USD {i['USD'].sum():>10,.2f}  COP {i['Monto'].sum():>13,.0f}")
        print(f"  fechas {rb2['_f'].min().date()} → {rb2['_f'].max().date()}")
        # 🚩 blindaje ventana manual
        inw = rb2[rb2["_f"] <= pd.Timestamp(mod.ROBINHOOD_VENTANA_MANUAL_FIN)]
        chk(f"0 entrantes dentro de la ventana manual (≤{mod.ROBINHOOD_VENTANA_MANUAL_FIN})",
            len(inw) == 0, f"{len(inw)}" + (" ⚠️ REQUIEREN APROBACIÓN A MANO" if len(inw) else ""))
        for _, x in inw.iterrows():
            print(f"      ⚠️ IN-WINDOW {x['Fecha']} USD {x['USD']:>9,.2f} {str(x['Nombre del producto'])[:60]}")
        print(f"\n  DETALLE ({len(rb2)} filas):")
        for _, x in rb2.sort_values(["_f", "Orden"]).iterrows():
            print(f"    {x['Fecha']}  {x['Tipo']:<7} USD {x['USD']:>9,.2f}  TRM {float(x['TRM']):>8,.2f}"
                  f"  COP {x['Monto']:>11,.0f}  {str(x['Nombre del producto'])[:56]}")
        if len(i):
            sin = i[~i["Nombre del producto"].astype(str).str.contains("TRM compra")]
            chk("devoluciones con TRM de su compra original", len(sin) == 0,
                f"{len(sin)} sin TRM original")
    else:
        print("  (0 filas NUEVAS de Robinhood — todo lo devuelto ya estaba)")

    # ── 4) PARTE B — Rakuten ──────────────────────────────────────────────────
    banner("4) PARTE B — RAKUTEN -> 1444")
    raw_rk = pd.read_csv(CSV_RAKU)
    _fk = pd.to_datetime(raw_rk["Date"].astype(str).str.split(",").str[0], format="%Y/%m/%d")
    print(f"  extracto: {len(raw_rk)} filas · {_fk.min().date()} → {_fk.max().date()}")
    out_rk = mod.procesar_rakuten(raw_rk.copy(), fecha_desde=mod.RAKUTEN_FECHA_DESDE,
                                  cobrados=cobrados, pendientes=pendientes,
                                  hist_tarjetas=hist_tarj, cobrados_df=cobrados_df)
    msgs_rk = harness.drenar()
    for niv, m in msgs_rk:
        print(f"  [{niv}] {m[:500]}")
    print(f"  claves devueltas: {list(out_rk)}")
    if set(out_rk) - {f"rakuten_{CASILLERO}"}:
        raise SystemExit(f"⛔ ABORTA: Rakuten devolvió casilleros inesperados: {set(out_rk)}")
    rk = out_rk.get(f"rakuten_{CASILLERO}", pd.DataFrame())

    k = raw_rk.copy()
    k["_f"] = _fk
    k["_ty"] = k["Type"].astype(str).str.strip().str.upper()
    print(f"\n  RECONCILIACIÓN RAKUTEN")
    print(f"    {len(k):>5} filas en el extracto")
    k_ig = ~k["_ty"].isin(set(mod.RAKUTEN_TIPO_MAP))
    print(f"    −{int(k_ig.sum()):>4} tipo ignorado ({k[k_ig]['_ty'].value_counts().to_dict()})")
    k2 = k[~k_ig]
    k_ant = k2["_f"] < pd.Timestamp(mod.RAKUTEN_FECHA_DESDE)
    print(f"    −{int(k_ant.sum()):>4} anteriores al corte {mod.RAKUTEN_FECHA_DESDE}")
    cand_rk = k2[~k_ant]
    print(f"    −{len(cand_rk) - len(rk):>4} ya liquidadas (lista / atributos)")
    print(f"    ={len(rk):>4} devueltas por procesar_rakuten")
    chk("candidatas ≥ devueltas (Rakuten)", len(cand_rk) >= len(rk), f"{len(cand_rk)} vs {len(rk)}")
    _m_rk, rk_nuevas = prueba_extracto_corto("RAKUTEN", rk, "rakuten_") if not rk.empty else (None, rk)
    print(f"\n  DE ESAS {len(rk)}: {len(rk) - len(rk_nuevas)} ya estaban en el histórico "
          f"(se reescriben idénticas) · {len(rk_nuevas)} son NUEVAS DE VERDAD")
    n_auth = int((k["_ty"] == "AUTH").sum())
    print(f"    ({n_auth} AUTH sin asentar quedan esperando — entrarán como TRANSACTION)")

    if not rk_nuevas.empty:
        rk2 = rk_nuevas.copy()
        rk2["_f"] = pd.to_datetime(rk2["Fecha"])
        rk2["USD"] = (pd.to_numeric(rk2["Monto"]) / pd.to_numeric(rk2["TRM"])).round(2)
        e = rk2[rk2["Tipo"] == "Egreso"]; i = rk2[rk2["Tipo"] == "Ingreso"]
        print(f"\n  {len(e):>3} Egreso  USD {e['USD'].sum():>10,.2f}  COP {e['Monto'].sum():>13,.0f}")
        print(f"  {len(i):>3} Ingreso USD {i['USD'].sum():>10,.2f}  COP {i['Monto'].sum():>13,.0f}")
        print(f"  fechas {rk2['_f'].min().date()} → {rk2['_f'].max().date()}")
        print(f"\n  DETALLE ({len(rk2)} filas):")
        for _, x in rk2.sort_values(["_f", "Orden"]).iterrows():
            print(f"    {x['Fecha']}  {x['Tipo']:<7} USD {x['USD']:>9,.2f}  TRM {float(x['TRM']):>8,.2f}"
                  f"  COP {x['Monto']:>11,.0f}  {str(x['Nombre del producto'])[:56]}")
        if len(i):
            sin = i[~i["Nombre del producto"].astype(str).str.contains("TRM compra")]
            chk("devoluciones con TRM de su compra original (Rakuten)", len(sin) == 0,
                f"{len(sin)} sin TRM original")
    else:
        print("  (0 filas NUEVAS de Rakuten — todo lo devuelto ya estaba)")

    if rb_nuevas.empty and rk_nuevas.empty:
        banner("NADA QUE CARGAR — 0 filas NUEVAS en ambas tarjetas")
        return

    # ── 5) aplicar a la hoja (dedup QUIRÚRGICO por prefijo) ───────────────────
    banner("5) APLICAR A LA HOJA DE 1444")
    historico = {kk: mod.asegurar_columnas_historico(v.copy()) for kk, v in vivo.items()}
    d = historico[HOJA]
    antes = len(d)
    for prefijo, nuevas in (("robinhood_", rb), ("rakuten_", rk)):
        if nuevas is None or nuevas.empty:
            print(f"  {prefijo:<12} 0 filas")
            continue
        nv = nuevas.drop(columns=["_f", "USD"], errors="ignore").copy()
        nv["Fecha de Carga"] = fecha_carga
        d = pd.concat([d, mod.asegurar_columnas_historico(nv)], ignore_index=True)
        o = d["Orden"].astype(str).str.strip()
        mpre = o.str.startswith(prefijo)
        tras = int(mpre.sum())
        d = pd.concat([d[~mpre], d[mpre].drop_duplicates(subset=["Orden"], keep="last")],
                      ignore_index=True)
        final = int(d["Orden"].astype(str).str.strip().str.startswith(prefijo).sum())
        print(f"  {prefijo:<12} +{len(nv):>3} filas · {prefijo}* {tras} → {final} (dedup quirúrgico)")
    n_nuevas = len(d) - antes
    u, c = usuario_de_totales(historico[HOJA]), cas_de_totales(historico[HOJA], CASILLERO)
    historico[HOJA] = mod.recalcular_totales_diarios(d, usuario=u, cas=c)
    print(f"  recalculado con usuario='{u}' casillero={c!r} · filas {antes} → {len(historico[HOJA])}")

    # ── 6) capa B ─────────────────────────────────────────────────────────────
    banner("6) 🛡️ CAPA B — preservar_filas_tarjeta")
    historico = mod.preservar_filas_tarjeta(historico, vivo=vivo)
    for niv, m in harness.drenar():
        print(f"  [{niv}] {m[:300]}")
    print("  ✔ ejecutada")

    # ── 7) diff + invariantes ─────────────────────────────────────────────────
    banner("7) DIFF E INVARIANTES")
    for hoja in vivo:
        a, b = vivo[hoja], historico[hoja]
        if hoja == HOJA:
            print(f"  {hoja:<34} {len(a)} → {len(b)} | saldo {saldo(a):,.2f} → {saldo(b):,.2f} "
                  f"({saldo(b)-saldo(a):+,.2f})")
        else:
            igual = len(a) == len(b) and set(a["Orden"].astype(str)) == set(b["Orden"].astype(str))
            print(f"  {hoja:<34} sin cambios ({len(a)}) {'✔ verbatim' if igual else '🚨 CAMBIÓ'}")

    o1 = historico[HOJA]["Orden"].astype(str).str.strip()
    # el dedup por Orden es UNIÓN, no suma: las reprocesadas reescriben su fila
    chk("robinhood_ en 1444 = previas + nuevas de verdad",
        int(o1.str.startswith("robinhood_").sum()) == n0["robinhood_"] + len(rb_nuevas),
        f"{n0['robinhood_']} + {len(rb_nuevas)} → {int(o1.str.startswith('robinhood_').sum())}")
    chk("rakuten_ en 1444 = previas + nuevas de verdad",
        int(o1.str.startswith("rakuten_").sum()) == n0["rakuten_"] + len(rk_nuevas),
        f"{n0['rakuten_']} + {len(rk_nuevas)} → {int(o1.str.startswith('rakuten_').sum())}")
    chk("intuit_ intactas (7)", int(o1.str.startswith("intuit_").sum()) == 7)
    chk("applepay_ intactas (4)", int(o1.str.startswith("applepay_").sum()) == 4)
    chk("amex_ intactas (151)", int(o1.str.startswith("amex_").sum()) == 151)
    chk("1444 sigue sin usbank_/capital_",
        int(o1.str.startswith(("usbank_", "capital_")).sum()) == 0)
    z = o1[o1.str.startswith(("amex_", "rakuten_", "robinhood_", "capital_", "usbank_",
                              "intuit_", "applepay_"))]
    chk("0 Orden de tarjeta duplicados en 1444", not z.duplicated().any(),
        f"{int(z.duplicated().sum())}")
    # ningún Orden previo se pierde. Las filas TOTAL no llevan Orden (NaN) y las regenera
    # recalcular_totales_diarios, así que el placeholder 'nan'/'' no cuenta como pérdida.
    _vacios = {"", "nan", "none", "nat"}
    _o0 = {x for x in o0 if x.lower() not in _vacios}
    _o1 = {x for x in o1 if x.lower() not in _vacios}
    perdidos = _o0 - _o1
    chk("0 Orden previos perdidos en 1444", not perdidos,
        f"{len(perdidos)} {sorted(perdidos)[:5] if perdidos else ''}")
    # comisiones intactas
    com, comv = historico[HOJA], vivo[HOJA]
    mc = com["Nombre del producto"].astype(str).str.lower().str.startswith("comision de (")
    mcv = comv["Nombre del producto"].astype(str).str.lower().str.startswith("comision de (")
    chk("comisiones de 1444 intactas (mismas filas y montos)",
        int(mc.sum()) == int(mcv.sum()) and
        abs(pd.to_numeric(com.loc[mc, "Monto"]).sum() - pd.to_numeric(comv.loc[mcv, "Monto"]).sum()) < 0.01,
        f"{int(mc.sum())} filas · Σ {pd.to_numeric(com.loc[mc,'Monto']).sum():,.2f}")
    chk("0 filas de incentivo creadas",
        int(o1.str.startswith("incentivoamex_").sum()) ==
        int(o0.str.startswith("incentivoamex_").sum()),
        f"{int(o1.str.startswith('incentivoamex_').sum())}")
    for hoja in vivo:
        if hoja == HOJA:
            continue
        a, b = vivo[hoja], historico[hoja]
        igual = len(a) == len(b)
        if igual and "Monto" in a.columns:
            igual = abs(pd.to_numeric(a["Monto"], errors="coerce").fillna(0).sum() -
                        pd.to_numeric(b["Monto"], errors="coerce").fillna(0).sum()) < 0.01
        chk(f"verbatim: {hoja}", igual, f"{len(a)} filas")
    if not ok:
        raise SystemExit("⛔ ABORTA: falló alguna invariante.")

    # ── 8) bytes + guard A ────────────────────────────────────────────────────
    banner("8) EXCEL EN MEMORIA + 🛡️ GUARD A")
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
            print(f"  [{n}] {t[:500]}")
        raise SystemExit("⛔ GUARD A BLOQUEÓ")

    # ── 9) saldo ──────────────────────────────────────────────────────────────
    banner("9) SALDO 1444")
    s1 = saldo(historico[HOJA])
    print(f"  COP {s0:>18,.2f} → {s1:>18,.2f}   Δ {s1-s0:>+16,.2f}   (+{n_nuevas} filas)")

    if not ESCRIBIR:
        banner("DRY-RUN TERMINADO — 0 ESCRITURAS")
        print("  Para escribir: python3 cargar_robinhood_rakuten_20260831.py --escribir")
        return

    # ── 10) escritura ─────────────────────────────────────────────────────────
    banner("10) RE-VERIFICACIÓN + SUBIDA (capa C hace backup)")
    md_pre = mod.dbx.files_get_metadata(cfg["remote_path"])
    if md_pre.rev != REV_ESPERADA:
        raise SystemExit(f"⛔ ABORTA SIN ESCRIBIR: el histórico se movió (rev {md_pre.rev}).")
    print(f"  ✔ rev sin cambios ({md_pre.rev})")
    harness.clear_msgs()
    mod.upload_to_dropbox(data_bytes)
    backup = None
    for n, t in harness.MENSAJES:
        print(f"  [{n}] {t}")
        if "Respaldo previo creado" in t and "`" in t:
            backup = t.split("`")[1]

    banner("11) VALIDACIÓN POST-ESCRITURA (leyendo de vuelta)")
    md2 = mod.dbx.files_get_metadata(cfg["remote_path"])
    print(f"  rev NUEVA = {md2.rev}   modificado = {md2.server_modified}   size = {md2.size:,}")
    print(f"  backup    = {backup}")
    _, r2 = mod.dbx.files_download(cfg["remote_path"])
    rel = pd.read_excel(io.BytesIO(r2.content), sheet_name=None)
    ok2 = True
    def chk2(n, c, det=""):
        nonlocal ok2
        print(f"  {'✔' if c else '🚨'} {n:<60} {det}")
        ok2 = ok2 and bool(c)
    q = rel[HOJA]["Orden"].astype(str).str.strip()
    chk2("saldo 1444", abs(saldo(rel[HOJA]) - s1) < 0.01, f"COP {saldo(rel[HOJA]):,.2f}")
    chk2("robinhood_", int(q.str.startswith("robinhood_").sum()) == n0["robinhood_"] + len(rb_nuevas),
         f"{int(q.str.startswith('robinhood_').sum())}")
    chk2("rakuten_", int(q.str.startswith("rakuten_").sum()) == n0["rakuten_"] + len(rk_nuevas),
         f"{int(q.str.startswith('rakuten_').sum())}")
    chk2("intuit_ (7) + applepay_ (4) intactas",
         int(q.str.startswith("intuit_").sum()) == 7 and int(q.str.startswith("applepay_").sum()) == 4)
    _q = {x for x in q if x.lower() not in {"", "nan", "none", "nat"}}
    chk2("0 Orden previos perdidos", not (_o0 - _q), f"{len(_o0 - _q)}")
    dups = []
    for hoja, b in rel.items():
        if "Orden" not in b.columns:
            continue
        zz = b["Orden"].astype(str).str.strip()
        zz = zz[zz.str.startswith(("amex_", "rakuten_", "robinhood_", "capital_", "usbank_",
                                   "intuit_", "applepay_"))]
        dups += list(zz[zz.duplicated()].unique())
    chk2("0 Orden de tarjeta duplicados", not dups, f"{len(dups)}")
    chk2(f"{len(vivo)} hojas", len(rel) == len(vivo), f"{len(rel)}")
    for hoja in vivo:
        if hoja == HOJA:
            continue
        a, b = vivo[hoja], rel[hoja]
        igual = len(a) == len(b)
        if igual and "Monto" in a.columns:
            igual = abs(pd.to_numeric(a["Monto"], errors="coerce").fillna(0).sum() -
                        pd.to_numeric(b["Monto"], errors="coerce").fillna(0).sum()) < 0.01
        chk2(f"verbatim: {hoja}", igual, f"{len(a)} filas")
    print(f"\n  {'✅ CARGUE COMPLETO Y VERIFICADO' if ok2 else '🚨 REVISAR'}")
    print(f"  rev nueva: {md2.rev}\n  rollback:  {backup}")


if __name__ == "__main__":
    sys.path.insert(0, os.environ.get("HARNESS_DIR", "."))
    main()
