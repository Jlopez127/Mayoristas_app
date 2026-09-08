#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cargue de tarjetas del 2026-09-07 — CINCO tarjetas, TRES casilleros.

QUÉ ENTRA
---------
    Rakuten    -> 1444    Rakuten_Activity_All (5).csv
    Robinhood  -> 1444    fca49fe3-6783-478c-aca0-a9a61f9cf59c.csv
    Capital    -> 13608   2026-09-07_transaction_download (4).csv   (card 1484)
    US Bank    -> 11591 + 13608   Credit Card - 0613_08-07-2026_09-11-2026.csv
    Intuit     -> 11591   intuit_acumulado.csv

AMEX NO VA. El usuario no tiene el extracto hoy (decisión 2026-09-07). Lo que Amex traiga de
agosto y entre después de este cargue NO recibirá incentivo: la fila `incentivoamex_*_2026-08`
se reajusta aquí y vuelve a quedar congelada.

SUB-TARJETA 6554 (US Bank)
--------------------------
Apareció "SAENZ,JOSE IGNACIO" con un único movimiento (Amazon Web Services, 2-sep, USD 2.249,74).
Decisión del usuario: IGNORAR — es gasto de la empresa, no de un mayorista. Va a
USBANK_SUBTARJETAS_IGNORAR en el mismo commit. Sin eso `procesar_usbank` aborta (fail-loud).

INTUIT: LAS 10 FILAS SIN `User`
-------------------------------
10 movimientos del 5-7 sep venían con `User` vacío en las descargas parciales y en el acumulado
quedaron como 'santiago largo' (USD 2.560,90). Confirmado por el usuario el 2026-09-07: son de
Santiago, NO se cobran. Quedan fuera junto con las otras 90 suyas.

EL INCENTIVO DE AGOSTO SE REAJUSTA A MANO (los 3 casilleros)
------------------------------------------------------------
`incentivoamex_<cas>_2026-08` se creó el 1-sep y el código lo deja CONGELADO. Parte de lo que
entra hoy es de AGOSTO (US Bank 11591 y 13608, Rakuten y Robinhood de 1444), así que su base
cambió. Se recalcula con la MISMA regla del módulo (25 COP × USD neto del mes) y, ANTES de tocar
nada, se comprueba que la fórmula reproduce el valor ya escrito en cada hoja. Si no lo reproduce
en alguna, ABORTA sin escribir.

LA COMISIÓN NO SE TOCA
----------------------
Este script no ejecuta el bloque de comisión. Verifica que las filas de comisión existentes
quedan idénticas y reporta si algún día de la ventana viva (1-15 sep) queda negativo — eso lo
cobraría la próxima corrida REAL de la app, no este script.

  sin argumentos  -> dry-run (0 escrituras)
  --escribir      -> escribe a Dropbox
"""
import sys, os, io, csv, glob, time, warnings
warnings.filterwarnings("ignore")
import pandas as pd

ESCRIBIR = "--escribir" in sys.argv

DL = "/Users/julianlopez/Downloads"
CSV_INTUIT = "/Users/julianlopez/Library/CloudStorage/OneDrive-Personal/Encargomio/Compras TC/intuit_acumulado.csv"
PARCIALES_INTUIT = f"{DL}/transactions_*.csv"

FUENTES = [
    ("rakuten",   "procesar_rakuten",   "RAKUTEN_FECHA_DESDE",   f"{DL}/Rakuten_Activity_All (5).csv"),
    ("robinhood", "procesar_robinhood", "ROBINHOOD_FECHA_DESDE", f"{DL}/fca49fe3-6783-478c-aca0-a9a61f9cf59c.csv"),
    ("capital",   "procesar_capital",   "CAPITAL_FECHA_DESDE",   f"{DL}/2026-09-07_transaction_download (4).csv"),
    ("usbank",    "procesar_usbank",    "USBANK_FECHA_DESDE",    f"{DL}/Credit Card - 0613_08-07-2026_09-11-2026.csv"),
    ("intuit",    "procesar_intuit",    "INTUIT_FECHA_DESDE",    CSV_INTUIT),
]

REV_ESPERADA = "0165aa78fa813ce00000002f34b3f21"
SALDOS_ESPERADOS = {"11591": 30007765.87, "1444": 45534009.21, "13608": 141129542.80}

# Conteos por prefijo ANTES del cargue (leídos del vivo el 2026-09-07).
PREVIAS = {
    "11591": {"amex_": 93, "usbank_": 15, "intuit_": 0},
    "1444":  {"amex_": 151, "rakuten_": 73, "robinhood_": 249, "intuit_": 23, "applepay_": 4},
    "13608": {"amex_": 1, "capital_": 115, "usbank_": 16},
}
# Lo que debe entrar, por (casillero, prefijo): (devueltas, nuevas de verdad, USD neto nuevas).
# ⚠️ `devueltas` != `nuevas`: el dedup por Orden es UNIÓN, no suma. US Bank vuelve a devolver 22
# filas ya cargadas el 31-ago porque su Orden es ID NATIVO y se dejó FUERA de la lista de
# exclusión a propósito (ver cobertura al 31-ago). Se verifica que reentran IDÉNTICAS.
ESPERADO = {
    ("1444",  "rakuten_"):   (35, 35, 5117.89),
    ("1444",  "robinhood_"): (16, 16, 2658.14),
    ("13608", "capital_"):   (4,   4, 1166.56),
    ("11591", "usbank_"):    (28, 19, 10357.42),
    ("13608", "usbank_"):    (19,  6,  980.49),
    ("11591", "intuit_"):    (1,   1,    0.02),
}
INCENTIVO_MES = "2026-08"
MES_INI, MES_FIN = "2026-08-01", "2026-08-31"
# Valor escrito hoy en cada hoja: la fórmula DEBE reproducirlo antes de tocar la fila.
INCENTIVO_ACTUAL = {"11591": 414167.0, "1444": 1295754.0, "13608": 1094363.0}


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


def usd_neto_mes(d, ini, fin):
    """USD neto de tarjeta del mes, con la MISMA regla de agregar_incentivo_amex:
    USD_fila = Monto/TRM, egresos menos ingresos, filas capturadas por Motivo 'Tarjeta *'."""
    x = d.copy()
    x["_f"] = pd.to_datetime(x["Fecha"], errors="coerce")
    x = x[(x["_f"] >= pd.Timestamp(ini)) & (x["_f"] <= pd.Timestamp(fin))]
    x = x[x["Motivo"].astype(str).str.strip().str.startswith("Tarjeta ")]
    x["_usd"] = pd.to_numeric(x["Monto"], errors="coerce") / pd.to_numeric(x["TRM"], errors="coerce")
    eg = x.loc[x["Tipo"].astype(str).str.strip() == "Egreso", "_usd"].sum()
    ing = x.loc[x["Tipo"].astype(str).str.strip() == "Ingreso", "_usd"].sum()
    return float(eg - ing)


def usd_de(d):
    return float((pd.to_numeric(d["Monto"]) / pd.to_numeric(d["TRM"]) *
                  d["Tipo"].map({"Egreso": 1, "Ingreso": -1})).sum())


def main():
    import harness
    mod = harness.cargar_app()
    SEP = "=" * 96
    def banner(t): print(f"\n{SEP}\n{t}\n{SEP}")
    ok = True
    def chk(n, c, det=""):
        nonlocal ok
        print(f"  {'✔' if c else '🚨'} {n:<66} {det}")
        ok = ok and bool(c)

    # datos.gov.co está intermitente: reintentos. Si aun así falta un día, el módulo aborta solo
    # (no hay TRM de respaldo) — fail-safe, nunca inventa una TRM.
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

    banner("0) PERILLAS Y MAPAS")
    for k in ("RAKUTEN_FECHA_DESDE", "ROBINHOOD_FECHA_DESDE", "CAPITAL_FECHA_DESDE",
              "USBANK_FECHA_DESDE", "INTUIT_FECHA_DESDE", "INCENTIVO_COP_POR_USD"):
        print(f"  {k:<26} = {getattr(mod, k)}")
    print(f"  USBANK_MAP_SUBTARJETA      = {mod.USBANK_MAP_SUBTARJETA}")
    print(f"  USBANK_SUBTARJETAS_IGNORAR = {sorted(mod.USBANK_SUBTARJETAS_IGNORAR)}")
    print(f"  INTUIT_MAP_USUARIO         = {mod.INTUIT_MAP_USUARIO}")
    chk("6554 ignorada (Jose I. Saenz, AWS)", "6554" in mod.USBANK_SUBTARJETAS_IGNORAR)
    chk("6554 NO mapeada a ningún casillero", "6554" not in mod.USBANK_MAP_SUBTARJETA)
    chk("US Bank sigue mapeando solo 0598/0609",
        mod.USBANK_MAP_SUBTARJETA == {"0598": "11591", "0609": "13608"})
    chk("santiago largo sigue ignorado en Intuit", "santiago largo" in mod.INTUIT_USUARIOS_IGNORAR)
    # 🧯 tripwire: este script NO debe crear filas de incentivo, solo reajustar las existentes.
    def _boom(*a, **k):
        raise AssertionError("agregar_incentivo_amex fue llamada — este script NO la ejecuta")
    mod.agregar_incentivo_amex = _boom

    banner("1) EL ACUMULADO DE INTUIT ES LA UNIÓN DE LOS PARCIALES")
    def leer(p):
        with open(p, encoding="utf-8-sig", newline="") as f:
            return list(csv.DictReader(f))
    # Los parciales traen `User` vacío en algunas filas; el acumulado lo rellenó. La llave de
    # cobertura excluye User a propósito (esa asignación se validó aparte con el usuario).
    K = lambda r: (r["Merchant"], r["Amount"], r["Date"], r["Status"])
    # En Downloads conviven los parciales del 2-sep con los de hoy: solo cuentan los de hoy.
    HOY = pd.Timestamp.today().date()
    parciales = [p for p in sorted(glob.glob(PARCIALES_INTUIT))
                 if pd.Timestamp(os.path.getmtime(p), unit="s", tz="UTC").tz_convert(
                     "America/Bogota").date() == HOY]
    union = {K(r) for p in parciales for r in leer(p)}
    acum_rows = leer(CSV_INTUIT)
    acum = {K(r) for r in acum_rows}
    print(f"  {len(parciales)} parciales → {len(union)} únicos · acumulado {len(acum_rows)} filas / {len(acum)} únicos")
    chk("el acumulado no pierde nada de los parciales", not (union - acum), f"{len(union - acum)}")
    sant = sum(1 for r in acum_rows if r["User"].strip().lower() == "santiago largo")
    print(f"  'santiago largo' en el acumulado: {sant} filas (NO se cobran, decisión del usuario)")

    banner("2) HISTÓRICO VIVO FRESCO")
    cfg = mod.st.secrets["dropbox"]
    md = mod.dbx.files_get_metadata(cfg["remote_path"])
    _, res = mod.dbx.files_download(cfg["remote_path"])
    vivo = pd.read_excel(io.BytesIO(res.content), sheet_name=None)
    HOJA = {c: next(h for h in vivo if h.split(" - ")[0].strip() == c) for c in PREVIAS}
    print(f"  rev={md.rev}  modificado={md.server_modified}  size={md.size:,}  hojas={len(vivo)}")
    o0 = {}
    for c, h in HOJA.items():
        o0[c] = vivo[h]["Orden"].astype(str).str.strip()
        print(f"  {h:<30} {len(vivo[h]):>6} filas · saldo COP {saldo(vivo[h]):>16,.2f}")
        for p, n in PREVIAS[c].items():
            chk(f"    {c} {p} previas = {n}", int(o0[c].str.startswith(p).sum()) == n,
                f"{int(o0[c].str.startswith(p).sum())}")
    movido = md.rev != REV_ESPERADA or any(
        abs(saldo(vivo[HOJA[c]]) - s) > 0.01 for c, s in SALDOS_ESPERADOS.items())
    if movido:
        print(f"  🚨 EL HISTÓRICO SE MOVIÓ (esperaba rev {REV_ESPERADA})")
        if ESCRIBIR:
            raise SystemExit("⛔ ABORTA: rehacer el dry-run con el histórico fresco.")
    else:
        print("  ✔ rev y los 3 saldos idénticos a los esperados")

    banner("3) PROCESAR LOS 5 EXTRACTOS")
    cobrados, pendientes, cobrados_df = mod.cargar_tarjetas_cobradas()
    harness.clear_msgs()
    hist_t = mod.cargar_hist_tarjetas()
    harness.clear_msgs()
    print(f"  lista de exclusión: {len(cobrados)} Orden cobrados · {len(pendientes)} pendientes")
    nuevas = {}          # (casillero, prefijo) -> DF
    crudos = {}          # nombre -> DF crudo (para la prueba del extracto corto)
    for nom, fname, cname, path in FUENTES:
        raw = pd.read_csv(path, encoding="utf-8-sig")
        crudos[nom] = raw
        out = getattr(mod, fname)(raw.copy(), fecha_desde=getattr(mod, cname), cobrados=cobrados,
                                  pendientes=pendientes, hist_tarjetas=hist_t, cobrados_df=cobrados_df)
        print(f"\n  ── {nom.upper()} (desde {getattr(mod, cname)}) · {len(raw)} filas crudas")
        for niv, m in harness.drenar():
            print(f"     [{niv}] {m[:230]}")
        for k, d in out.items():
            cas = k.split("_", 1)[1]
            if d.empty:
                continue
            nuevas[(cas, f"{nom}_")] = d
            f = pd.to_datetime(d["Fecha"])
            print(f"     → {k}: {len(d)} filas · USD neto {usd_de(d):>10,.2f} · "
                  f"COP {pd.to_numeric(d['Monto']).sum():>13,.0f} · {f.min().date()}→{f.max().date()} "
                  f"· {d['Tipo'].value_counts().to_dict()}")

    banner("4) RECONCILIACIÓN CONTRA LO ESPERADO")
    chk("mismas claves esperadas", set(nuevas) == set(ESPERADO),
        f"{sorted(set(nuevas) ^ set(ESPERADO))}")
    realmente_nuevas = {}
    for k, (n, nn, u) in ESPERADO.items():
        cas, pref = k
        d = nuevas.get(k)
        if d is None:
            chk(f"{cas} {pref:<11} devuelve {n}", False, "AUSENTE"); continue
        h = vivo[HOJA[cas]].copy(); h["Orden"] = h["Orden"].astype(str).str.strip()
        ya = set(h["Orden"])
        nv = d[~d["Orden"].astype(str).isin(ya)].copy()
        realmente_nuevas[k] = nv
        chk(f"{cas} {pref:<11} devuelve {n} · {nn} nuevas · USD {u:,.2f}",
            len(d) == n and len(nv) == nn and abs(usd_de(nv) - u) < 0.02,
            f"{len(d)} devueltas · {len(nv)} nuevas · USD {usd_de(nv):,.2f}")
        # las que reentran YA ESTABAN: deben ser idénticas (Monto/TRM/Tipo), si no, hay recobro
        j = d.merge(h, on="Orden", suffixes=("_n", "_h"))
        if len(j):
            dM = (pd.to_numeric(j["Monto_n"]) - pd.to_numeric(j["Monto_h"])).abs()
            dT = (pd.to_numeric(j["TRM_n"]) - pd.to_numeric(j["TRM_h"])).abs()
            dP = j["Tipo_n"].astype(str).str.strip() != j["Tipo_h"].astype(str).str.strip()
            chk(f"{cas} {pref:<11} las {len(j)} que reentran son IDÉNTICAS",
                int((dM > 0.5).sum()) == 0 and int((dT > 0.005).sum()) == 0 and int(dP.sum()) == 0,
                f"ΔMonto={int((dM>0.5).sum())} ΔTRM={int((dT>0.005).sum())} ΔTipo={int(dP.sum())}")
    for (cas, pref), d in nuevas.items():
        motivo_esp = {"rakuten_": "Tarjeta Rakuten", "robinhood_": "Tarjeta Robinhood",
                      "capital_": "Tarjeta Capital", "usbank_": "Tarjeta US Bank",
                      "intuit_": "Tarjeta Intuit"}[pref]
        chk(f"{cas} {pref:<11} Motivo/Casillero/Orden coherentes",
            set(d["Motivo"]) == {motivo_esp}
            and set(d["Casillero"].astype(str)) == {cas}
            and d["Orden"].astype(str).str.startswith(pref).all()
            and set(d["Tipo"]) <= {"Egreso", "Ingreso"})
    todas = pd.concat(nuevas.values(), ignore_index=True)
    chk("0 Orden duplicados entre las 5 tarjetas", not todas["Orden"].astype(str).duplicated().any())
    chk("ningún Orden entrante está ya en la lista de exclusión",
        not (set(todas["Orden"].astype(str)) & set(cobrados)))
    ya = set().union(*[set(o0[c]) for c in o0])
    tn = pd.concat(realmente_nuevas.values(), ignore_index=True) if realmente_nuevas else pd.DataFrame()
    chk("ningún Orden REALMENTE NUEVO está ya en el histórico",
        not (set(tn["Orden"].astype(str)) & ya) if len(tn) else False)
    if not ok:
        raise SystemExit("⛔ ABORTA: falló la reconciliación.")

    banner("5) 🧪 PRUEBA DEL EXTRACTO CORTO (reprocesar sin lista y comparar con lo ya cargado)")
    for nom, fname, cname, path in FUENTES:
        todo = getattr(mod, fname)(crudos[nom].copy(), fecha_desde=getattr(mod, cname),
                                   cobrados=set(), pendientes=None, hist_tarjetas=hist_t,
                                   cobrados_df=None)
        harness.clear_msgs()
        for k, t in todo.items():
            cas = k.split("_", 1)[1]
            if cas not in HOJA or t.empty:
                continue
            vi = vivo[HOJA[cas]].copy()
            vi["Orden"] = vi["Orden"].astype(str).str.strip()
            vi = vi[vi["Orden"].str.startswith(f"{nom}_")]
            t = t.copy(); t["Orden"] = t["Orden"].astype(str).str.strip()
            j = vi.merge(t, on="Orden", suffixes=("_h", "_n"))
            if j.empty:
                print(f"  ·  {nom}/{cas}: 0 solapadas (el extracto no alcanza lo ya cargado)")
                continue
            # ⚠️ El criterio se fija en USD, no en COP (regla 8 del CLAUDE.md): la TRM de un
            # REEMBOLSO se hereda de su compra original y, al reprocesar sin lista, hay más
            # compras candidatas y `_resolver_trm_reembolsos` puede elegir otra. Eso mueve el COP
            # sin que cambie el movimiento. Lo que NO puede moverse es el USD ni el Tipo.
            uh = pd.to_numeric(j["Monto_h"], errors="coerce") / pd.to_numeric(j["TRM_h"], errors="coerce")
            un = pd.to_numeric(j["Monto_n"], errors="coerce") / pd.to_numeric(j["TRM_n"], errors="coerce")
            dU = (un - uh).abs()
            dT = (pd.to_numeric(j["TRM_n"], errors="coerce") - pd.to_numeric(j["TRM_h"], errors="coerce")).abs()
            dP = (j["Tipo_n"].astype(str).str.strip() != j["Tipo_h"].astype(str).str.strip())
            reasig = j[(dT > 0.005) & (dU <= 0.02)]
            chk(f"{nom}/{cas}: {len(j)} ya cargadas se reproducen igual (USD)",
                int((dU > 0.02).sum()) == 0 and int(dP.sum()) == 0,
                f"ΔUSD={int((dU>0.02).sum())} ΔTipo={int(dP.sum())}"
                + (f" · {len(reasig)} reembolso(s) con TRM reasignada (COP distinto, USD igual)"
                   if len(reasig) else ""))
            for _, rr in reasig.iterrows():
                chk(f"    …{str(rr['Orden'])[-12:]} es un reembolso (TRM heredada)",
                    "reembolso" in str(rr["Nombre del producto_h"]).lower()
                    or str(rr["Tipo_h"]).strip() == "Ingreso",
                    f"USD {float(un.loc[rr.name]):,.2f} · TRM {float(rr['TRM_h']):,.2f}→{float(rr['TRM_n']):,.2f}")
    if not ok:
        raise SystemExit("⛔ ABORTA: el reproceso no reproduce lo ya cargado.")

    banner("6) APLICAR (dedup quirúrgico por prefijo, hoja por hoja)")
    fecha_carga = pd.Timestamp.today().strftime("%Y-%m-%d")
    historico = {k: mod.asegurar_columnas_historico(v.copy()) for k, v in vivo.items()}
    n_nuevas = {}
    for cas, h in HOJA.items():
        d = historico[h]
        antes = len(d)
        prefs = [p for (c, p) in nuevas if c == cas]
        for pref in prefs:
            x = nuevas[(cas, pref)].copy()
            x["Fecha de Carga"] = fecha_carga
            d = pd.concat([d, mod.asegurar_columnas_historico(x)], ignore_index=True)
            o = d["Orden"].astype(str).str.strip()
            mm = o.str.startswith(pref)
            d = pd.concat([d[~mm], d[mm].drop_duplicates(subset=["Orden"], keep="last")],
                          ignore_index=True)
        u, c = usuario_de_totales(historico[h]), cas_de_totales(historico[h], cas)
        d = mod.recalcular_totales_diarios(d, usuario=u, cas=c)
        historico[h] = d
        n_nuevas[cas] = len(d) - antes
        o1 = d["Orden"].astype(str).str.strip()
        print(f"  {h:<30} +{n_nuevas[cas]:>3} filas · " +
              " · ".join(f"{p}{PREVIAS[cas].get(p,0)}→{int(o1.str.startswith(p).sum())}" for p in prefs))

    banner(f"7) 💰 REAJUSTE DEL INCENTIVO DE {INCENTIVO_MES} (las filas están congeladas)")
    ajustes = {}
    for cas, h in HOJA.items():
        orden = f"incentivoamex_{cas}_{INCENTIVO_MES}"
        base_a = usd_neto_mes(vivo[h], MES_INI, MES_FIN)
        calc_a = round(base_a * mod.INCENTIVO_COP_POR_USD)
        chk(f"{cas}: la fórmula reproduce el incentivo ya escrito",
            abs(calc_a - INCENTIVO_ACTUAL[cas]) < 2,
            f"formula {calc_a:,.0f} vs escrito {INCENTIVO_ACTUAL[cas]:,.0f}")
        m = historico[h]["Orden"].astype(str).str.strip() == orden
        chk(f"{cas}: existe 1 sola fila {orden}", int(m.sum()) == 1, f"{int(m.sum())}")
        base_d = usd_neto_mes(historico[h], MES_INI, MES_FIN)
        calc_d = round(base_d * mod.INCENTIVO_COP_POR_USD)
        # el delta debe ser exactamente 25 × (USD de AGOSTO que entra hoy en esta hoja)
        usd_ago = 0.0
        for (c, p), d in realmente_nuevas.items():
            if c != cas or d.empty:
                continue
            x = d.copy(); x["_f"] = pd.to_datetime(x["Fecha"])
            x = x[(x["_f"] >= pd.Timestamp(MES_INI)) & (x["_f"] <= pd.Timestamp(MES_FIN))]
            if len(x):
                usd_ago += usd_de(x)
        chk(f"{cas}: el delta = 25 × USD de agosto que entra ({usd_ago:,.2f})",
            abs((calc_d - calc_a) - usd_ago * mod.INCENTIVO_COP_POR_USD) < 3,
            f"Δ {calc_d - calc_a:,.0f}")
        print(f"     base USD {base_a:,.2f} → {base_d:,.2f}   "
              f"COP {INCENTIVO_ACTUAL[cas]:,.0f} → {calc_d:,.0f}  (Δ +{calc_d - INCENTIVO_ACTUAL[cas]:,.0f})")
        ajustes[cas] = calc_d
    if not ok:
        raise SystemExit("⛔ ABORTA: no puedo reproducir algún incentivo; no toco ninguna fila.")
    for cas, h in HOJA.items():
        d = historico[h]
        m = d["Orden"].astype(str).str.strip() == f"incentivoamex_{cas}_{INCENTIVO_MES}"
        d.loc[m, "Monto"] = float(ajustes[cas])
        d.loc[m, "Fecha de Carga"] = fecha_carga
        u, c = usuario_de_totales(vivo[h]), cas_de_totales(vivo[h], cas)
        historico[h] = mod.recalcular_totales_diarios(d, usuario=u, cas=c)

    banner("8) LA COMISIÓN NO SE TOCA")
    for cas, h in HOJA.items():
        a = vivo[h]["Nombre del producto"].astype(str).str.lower().str.startswith("comision de (")
        b = historico[h]["Nombre del producto"].astype(str).str.lower().str.startswith("comision de (")
        chk(f"{cas}: mismas filas de comisión y mismos montos",
            int(a.sum()) == int(b.sum()) and
            abs(pd.to_numeric(vivo[h].loc[a, "Monto"]).sum()
                - pd.to_numeric(historico[h].loc[b, "Monto"]).sum()) < 0.01,
            f"{int(b.sum())} filas · Σ {pd.to_numeric(historico[h].loc[b,'Monto']).sum():,.2f}")
    # aviso: días negativos en la ventana viva -> los cobraría la próxima corrida REAL de la app
    for cas, h in HOJA.items():
        d = historico[h].copy(); d["_f"] = pd.to_datetime(d["Fecha"], errors="coerce")
        tt = d[d["Tipo"].astype(str).str.strip().str.upper() == "TOTAL"]
        w = pd.to_numeric(tt[(tt["_f"] >= pd.Timestamp("2026-09-01")) &
                             (tt["_f"] <= pd.Timestamp("2026-09-15"))]["Monto"], errors="coerce")
        neg = int((w < 0).sum())
        print(f"  {'⚠️' if neg else 'ℹ️ '} {cas}: 1-15 sep con {neg} día(s) negativo(s)"
              f"{'' if w.empty else f' · mínimo COP {w.min():,.0f}'}")

    banner("9) 🛡️ CAPA B + INVARIANTES + GUARD A")
    historico = mod.preservar_filas_tarjeta(historico, vivo=vivo)
    harness.drenar()
    vac = {"", "nan", "none", "nat"}
    for cas, h in HOJA.items():
        o1 = historico[h]["Orden"].astype(str).str.strip()
        for p, n in PREVIAS[cas].items():
            esp = n + len(realmente_nuevas.get((cas, p), []))
            chk(f"{cas} {p:<11} = {n} + {esp - n}", int(o1.str.startswith(p).sum()) == esp,
                f"{int(o1.str.startswith(p).sum())}")
        perd = {y for y in o0[cas] if y.lower() not in vac} - {y for y in o1 if y.lower() not in vac}
        chk(f"{cas}: 0 Orden previos perdidos", not perd, f"{len(perd)}")
        z = o1[o1.str.startswith(("amex_", "rakuten_", "robinhood_", "capital_", "usbank_",
                                  "intuit_", "applepay_", "incentivoamex_"))]
        chk(f"{cas}: 0 Orden de tarjeta duplicados", not z.duplicated().any())
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
    with pd.ExcelWriter(buf, engine="openpyxl") as w2:
        for hh, dfh in historico.items():
            w2.book.create_sheet(hh[:31])
            dfh.to_excel(w2, sheet_name=hh[:31], index=False)
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

    banner("10) SALDOS")
    s1 = {}
    for cas, h in HOJA.items():
        s0 = saldo(vivo[h]); s1[cas] = saldo(historico[h])
        print(f"  {h:<30} COP {s0:>16,.2f} → {s1[cas]:>16,.2f}   Δ {s1[cas]-s0:>+15,.2f}  (+{n_nuevas[cas]} filas)")
        for (c, p), d in sorted(realmente_nuevas.items()):
            if c == cas and len(d):
                cop = float((pd.to_numeric(d["Monto"]) * d["Tipo"].map({"Egreso": 1, "Ingreso": -1})).sum())
                print(f"       {p:<11} {-cop:>+15,.0f}  ({len(d)} filas · USD {usd_de(d):,.2f})")
        print(f"       {'incentivo':<11} +{ajustes[cas]-INCENTIVO_ACTUAL[cas]:>14,.0f}")

    if not ESCRIBIR:
        banner("DRY-RUN TERMINADO — 0 ESCRITURAS")
        print("  Para escribir: python3 cargar_tc_20260907.py --escribir")
        return

    banner("11) SUBIDA (capa C hace el respaldo)")
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

    banner("12) VALIDACIÓN POST-ESCRITURA (leyendo de vuelta)")
    md2 = mod.dbx.files_get_metadata(cfg["remote_path"])
    print(f"  rev NUEVA = {md2.rev}  size = {md2.size:,}")
    print(f"  backup    = {backup}")
    _, r2 = mod.dbx.files_download(cfg["remote_path"])
    rel = pd.read_excel(io.BytesIO(r2.content), sheet_name=None)
    ok2 = True
    def chk2(n, c, det=""):
        nonlocal ok2
        print(f"  {'✔' if c else '🚨'} {n:<66} {det}")
        ok2 = ok2 and bool(c)
    for cas, h in HOJA.items():
        q = rel[h]["Orden"].astype(str).str.strip()
        chk2(f"{cas}: saldo", abs(saldo(rel[h]) - s1[cas]) < 0.01, f"COP {saldo(rel[h]):,.2f}")
        for p, n in PREVIAS[cas].items():
            esp = n + len(realmente_nuevas.get((cas, p), []))
            chk2(f"{cas} {p:<11} = {esp}", int(q.str.startswith(p).sum()) == esp,
                 f"{int(q.str.startswith(p).sum())}")
        inc = rel[h][q == f"incentivoamex_{cas}_{INCENTIVO_MES}"]
        chk2(f"{cas}: incentivo de {INCENTIVO_MES} reajustado",
             len(inc) == 1 and abs(float(pd.to_numeric(inc["Monto"]).iloc[0]) - ajustes[cas]) < 1,
             f"COP {float(pd.to_numeric(inc['Monto']).iloc[0]):,.0f}")
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
    print(f"\n  {'✅ CARGUE COMPLETO Y VERIFICADO' if ok2 else '🚨 REVISAR'}")
    print(f"  rev nueva: {md2.rev}\n  rollback:  {backup}")
    print("\n  ⏭️  FALTA LA 2ª ESCRITURA: registrar estos Orden en tarjetas_cobradas.xlsx")
    print("      -> python3 subir_lista_tc_20260907.py")


if __name__ == "__main__":
    sys.path.insert(0, os.environ.get("HARNESS_DIR", "."))
    main()
