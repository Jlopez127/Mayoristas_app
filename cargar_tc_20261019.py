#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cargue TC del 2026-10-19 — cinco tarjetas (Robinhood no se descargó). Una escritura de histórico y una de lista.

    Capital   -> 13608 + 11591   2026-10-19_transaction_download (1).csv (card 1484; traspaso a Paula 9-sep)
                 (las otras descargas de hoy: 7418 / 9467 / 5263 / 4403 NO son de la app)
    US Bank   -> 11591 + 13608   Credit Card - 0613_09-19-2026_10-23-2026.csv (0598 Paula, 0609 Julian)
    Intuit    -> 1444 + 11591    Compras TC/intuit_acumulado.csv — Intuit CAMBIÓ DE FORMATO el 19-oct
                 (ibcc*.csv: Date,Description,Amount,Card Last 4,Transaction Channel,Status,Address).
                 Iris lo convierte al acumulado de siempre con Compras TC/intuit_nuevo_formato.py:
                 lo ya acumulado manda (mismo Merchant/Amount/Date => mismo Orden) y solo entra lo
                 nuevo. El conversor NO actualiza el Status de lo que casa: Expedia 48,06 de Elvis
                 (13-oct) quedó 'Pending' aunque el export dice 'settled' -> aquí se fuerza Settled
                 en memoria (el Status no entra en el hash del Orden).
    Rakuten   -> 1444            Rakuten_Activity_All (1).csv
    Discover  -> 13608           Discover-RecentActivity-20261019.xls (10-oct → 17-oct)
    Robinhood -> —               no se descargó hoy

Esqueleto de cargar_tc_20260928.py (dos escrituras, captura de la barrera 2, copiar-no-mover).

  sin argumentos           -> dry-run (0 escrituras)
  --escribir <rev>         -> escribe histórico + lista + copia OneDrive, solo si la rev del
                              histórico vivo es <rev> (la que imprimió el dry-run)
"""
import sys, os, io, shutil, time, warnings
from datetime import datetime
from pathlib import PurePosixPath
warnings.filterwarnings("ignore")
import pandas as pd
import dropbox

ESCRIBIR = "--escribir" in sys.argv
REV_ARG = sys.argv[sys.argv.index("--escribir") + 1] if ESCRIBIR else None

DL = "/Users/julianlopez/Downloads"
CSV_CAPITAL = f"{DL}/2026-10-19_transaction_download (1).csv"
CSV_RAKUTEN = f"{DL}/Rakuten_Activity_All (1).csv"
XLS_DISCOVER = [f"{DL}/Discover-RecentActivity-20261019.xls"]
CSV_USBANK = f"{DL}/Credit Card - 0613_09-19-2026_10-23-2026.csv"
# ↩️ devolución JD Sports −258,94 de María (18-oct) de la compra del 24-sep: solo con OK de Julián
DEV_JD = "--dev-jd" in sys.argv
CSV_INTUIT = "/Users/julianlopez/Library/CloudStorage/OneDrive-Personal/Encargomio/Compras TC/intuit_acumulado.csv"

# tarjeta -> (nombre del módulo, constante de corte, prefijo, card_norm por casillero)
CARD_NORM = {"13608": "JULIAN SANCHEZ", "11591": "PAULA HERRERA", "1444": "MARIA MOISES"}
NOTA = "cargue TC 2026-10-19 (Capital + US Bank + Intuit + Rakuten + Discover)"

REV_ESPERADA = REV_ARG
# lo que debe entrar: (casillero, prefijo) -> (filas, USD neto con signo: egreso +, ingreso -)
ESPERADO = {
    ("11591", "capital_"): (None, None),
    ("13608", "capital_"): (None, None),
    ("1444",  "rakuten_"): (None, None),   # se fija tras revisar el dry-run fila por fila
    ("13608", "discover_"): (None, None),
    ("11591", "usbank_"):  (None, None),
    ("13608", "usbank_"):  (None, None),
    ("1444",  "intuit_"):  (None, None),
    ("11591", "intuit_"):  (None, None),
}
PREVIAS = {}     # se leen del vivo y se imprimen (cambian con cada corrida diaria)

ONEDRIVE_DIR = "/Users/julianlopez/Library/CloudStorage/OneDrive-Personal/Historico Carga/Conciliacion/Mayoristas"
ONEDRIVE_ANT = f"{ONEDRIVE_DIR}/Antiguos"


def TARJETAS(mod):
    return (("capital", mod.procesar_capital, mod.CAPITAL_FECHA_DESDE),
            ("usbank", mod.procesar_usbank, mod.USBANK_FECHA_DESDE),
            ("intuit", mod.procesar_intuit, mod.INTUIT_FECHA_DESDE),
            ("rakuten", mod.procesar_rakuten, mod.RAKUTEN_FECHA_DESDE),
            ("discover", mod.procesar_discover, mod.DISCOVER_FECHA_DESDE))


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
    SEP = "=" * 96
    def banner(t): print(f"\n{SEP}\n{t}\n{SEP}")
    ok = True
    def chk(n, c, det=""):
        nonlocal ok
        print(f"  {'✔' if c else '🚨'} {n:<66} {det}")
        ok = ok and bool(c)

    _o = mod._amex_trm_dia
    def _trm(f, c=None, *a, **k):
        # ⚠️ _amex_trm_dia CACHEA el None de un fallo: sin sacarlo de la caché, los reintentos
        # devolvían el None cacheado sin volver a consultar datos.gov.co.
        c = c if c is not None else {}
        for i in range(8):
            if c.get(f, 0) is None:
                c.pop(f, None)
            v = _o(f, c, *a, **k)
            if v is not None:
                return v
            time.sleep(2 + i)
        return None
    mod._amex_trm_dia = _trm

    # 🧯 tripwires: nunca Amex (ya no se carga, 21-sep), egresos del portal ni el incentivo
    for fn in ("procesar_amex", "procesar_egresos", "agregar_incentivo_amex"):
        def _boom(*a, _n=fn, **k):
            raise AssertionError(f"{_n} fue llamada — este script no carga esa tarjeta")
        setattr(mod, fn, _boom)

    print(f"MODO: {'🔴 ESCRITURA REAL' if ESCRIBIR else '🟢 DRY-RUN (0 escrituras)'}")

    banner("0) PERILLAS")
    for k in ("CAPITAL_FECHA_DESDE", "CAPITAL_FECHA_TRASPASO", "CAPITAL_CASILLERO",
              "CAPITAL_CASILLERO_DESDE", "CAPITAL_CARD_NO", "USBANK_FECHA_DESDE",
              "USBANK_MAP_SUBTARJETA", "USBANK_SUBTARJETAS_IGNORAR",
              "INTUIT_FECHA_DESDE", "INTUIT_MAP_USUARIO", "RAKUTEN_FECHA_DESDE", "RAKUTEN_CASILLERO",
              "DISCOVER_FECHA_DESDE", "DISCOVER_CASILLERO", "DISCOVER_EXCLUIR_RANGOS",
              "DISCOVER_RETENIDAS", "ROBINHOOD_FECHA_DESDE", "ROBINHOOD_CARDMAP"):
        print(f"  {k:<26} = {getattr(mod, k)}")
    chk("Capital: traspaso a Paula el 2026-09-09",
        mod.CAPITAL_FECHA_TRASPASO == "2026-09-09" and mod.CAPITAL_CASILLERO_DESDE == "11591")
    chk("US Bank: 0598 -> Paula, 0609 -> Julian",
        mod.USBANK_MAP_SUBTARJETA == {"0598": "11591", "0609": "13608"})
    chk("Intuit: Maria -> 1444, Elvis -> 11591, Juan Pablo Correal -> 1444",
        mod.INTUIT_MAP_USUARIO == {"maria moises": "1444", "elvis martinez": "11591",
                                   "juan pablo correal": "1444"})
    chk("Robinhood: Juan Pablo y Maria -> 1444 (Santiago, Carlos y Kelly no)",
        mod.ROBINHOOD_CARDMAP == {"Juan Pablo Correal Perez": "1444", "Maria Moises": "1444"})
    chk("las seis tarjetas protegidas por la capa B",
        all(p in mod.TARJETA_ORDEN_RE for p in ("capital_", "usbank_", "intuit_", "rakuten_",
                                                "discover_", "robinhood_")))
    chk("Discover: a Julian desde el 9-sep, Amazon 9-14 excluida y la del 15-sep retenida",
        mod.DISCOVER_FECHA_DESDE == "2026-09-09" and mod.DISCOVER_CASILLERO == "13608"
        and ("AMAZON", "2026-09-09", "2026-09-14") in mod.DISCOVER_EXCLUIR_RANGOS
        and "AMAZON.COM*5L3RC1722" in mod.DISCOVER_RETENIDAS)

    banner("1) HISTÓRICO VIVO FRESCO")
    cfg = mod.st.secrets["dropbox"]
    md = mod.dbx.files_get_metadata(cfg["remote_path"])
    _, res = mod.dbx.files_download(cfg["remote_path"])
    vivo = pd.read_excel(io.BytesIO(res.content), sheet_name=None)
    HOJA = {c: next(h for h in vivo if h.split(" - ")[0].strip() == c) for c in ("13608", "11591", "1444")}
    PREF = ("amex_", "rakuten_", "robinhood_", "capital_", "usbank_", "intuit_", "discover_",
            "applepay_", "migracionamex_")
    for c, h in HOJA.items():
        oo = vivo[h]["Orden"].astype(str).str.strip()
        PREVIAS[c] = {p: int(oo.str.startswith(p).sum()) for p in PREF if oo.str.startswith(p).any()}
    chk("hoja de 1444 = '1444 - Maria Moises' (no la COP)", HOJA["1444"] == "1444 - Maria Moises",
        HOJA["1444"])
    print(f"  rev={md.rev}  modificado={md.server_modified}  size={md.size:,}")
    s0, o0 = {}, {}
    for c, h in HOJA.items():
        s0[c] = saldo(vivo[h]); o0[c] = vivo[h]["Orden"].astype(str).str.strip()
        print(f"  {h:<32} {len(vivo[h]):>6} filas · saldo COP {s0[c]:>16,.2f}")
        print(f"     previas: {PREVIAS[c]}")
    if md.rev != REV_ESPERADA:
        print(f"  ⚠️  rev actual {md.rev} (esperada {REV_ESPERADA}) — para escribir: --escribir {md.rev}")
        if ESCRIBIR:
            raise SystemExit("⛔ ABORTA: rehacer el dry-run con el histórico fresco.")
    else:
        print("  ✔ rev idéntica a la del dry-run")
    if not ok:
        raise SystemExit("⛔ ABORTA: el histórico no es el esperado.")

    banner("2) PROCESAR — capturando los atributos de la propia barrera 2")
    capturado = {}
    _orig = mod._excluir_por_atributos
    def _wrap(df, cobrados_df, tarjeta, ordenes_extracto, rango, etiqueta):
        capturado[tarjeta] = df.copy()
        return _orig(df, cobrados_df, tarjeta, ordenes_extracto, rango, etiqueta)
    mod._excluir_por_atributos = _wrap
    cobrados, pendientes, cobrados_df = mod.cargar_tarjetas_cobradas(); harness.clear_msgs()
    hist_t = mod.cargar_hist_tarjetas(); harness.clear_msgs()
    print(f"  lista de exclusión: {len(cobrados)} cobrados · {len(pendientes)} pendientes")

    disc = [mod.leer_discover(x) for x in XLS_DISCOVER]
    crudos = {"capital": pd.read_csv(CSV_CAPITAL),
              "usbank": pd.read_csv(CSV_USBANK),
              "intuit": pd.read_csv(CSV_INTUIT, encoding="utf-8-sig"),
              "rakuten": pd.read_csv(CSV_RAKUTEN),
              "discover": pd.concat(disc, ignore_index=True)}
    # Expedia 48,06 (Elvis, 13-oct): 'settled' en el export nuevo, 'Pending' en el acumulado porque
    # intuit_nuevo_formato.py no actualiza el Status de lo que casa. Se fuerza Settled SOLO en memoria.
    ci = crudos["intuit"]
    _exp = (ci["Merchant"].astype(str).str.startswith("Expedia") & ci["Amount"].astype(str).eq("$48.06")
            & ci["Date"].astype(str).eq("Oct 13, 2026") & ci["User"].astype(str).str.strip().eq("Elvis Martinez"))
    # (19-oct, tarde) Iris ya corrigió el conversor: la fila viene Settled. Se deja la guarda.
    chk("Intuit: Expedia 48,06 de Elvis (13-oct) está en el acumulado (1 fila)",
        int(_exp.sum()) == 1, str(set(ci.loc[_exp, "Status"])))
    ci.loc[_exp, "Status"] = "Settled"
    # Capital sigue trayendo la 'Other Services' del 14-oct (PAYPAL *friasjose512 USD 4.929,21 del
    # 12-oct, YA CARGADA a Paula) y procesar_capital hace fail-loud. Julián (14-oct): "es de Paula, cobrar". Solo en
    # este cargue se reetiqueta a 'Merchandise': es un Debit (cargable por cualquier Category) y
    # la Category NO entra en la clave del hash, así que el Orden es el mismo que dará el módulo
    # cuando acepte la categoría. El código de la app no cambia.
    _ret = (crudos["capital"]["Category"].astype(str).str.strip().eq("Other Services")
            & pd.to_numeric(crudos["capital"]["Debit"], errors="coerce").gt(0))
    chk("Capital: 1 sola fila 'Other Services', y es un Debit (PAYPAL *friasjose512 4929.21)",
        int(_ret.sum()) == 1 and
        set(crudos["capital"].loc[_ret, "Description"]) == {"PAYPAL *friasjose512"} and
        float(pd.to_numeric(crudos["capital"].loc[_ret, "Debit"]).sum()) == 4929.21)
    crudos["capital"].loc[_ret, "Category"] = "Merchandise"
    chk("el extracto de Capital es SOLO de la 1484",
        set(crudos["capital"]["Card No."].astype(str)) == {str(mod.CAPITAL_CARD_NO)},
        str(set(crudos["capital"]["Card No."].astype(str))))
    salidas = {}
    DEV_ATTR = {"compra": None, "orden": None}
    for nom, fn, corte in TARJETAS(mod):
        out = fn(crudos[nom].copy(), fecha_desde=corte, cobrados=cobrados, pendientes=pendientes,
                 hist_tarjetas=hist_t, cobrados_df=cobrados_df)
        print(f"\n  ── {nom.upper()} · {len(crudos[nom])} filas crudas · corte {corte}")
        for n, m in harness.drenar():
            print(f"     [{n}] {m[:200]}")
        salidas.update(out)
    mod._excluir_por_atributos = _orig

    banner("3) RECONCILIACIÓN")
    nuevas = {}
    for (cas, pref), (n_esp, usd_esp) in ESPERADO.items():
        clave = f"{pref}{cas}"
        d = salidas.get(clave, pd.DataFrame())
        d = d[~d["Orden"].astype(str).isin(set(o0[cas]))].copy() if len(d) else d
        if len(d):
            d["_usd"] = pd.to_numeric(d["Monto"]) / pd.to_numeric(d["TRM"])
            d["_s"] = d["Tipo"].map({"Egreso": 1, "Ingreso": -1})
        nuevas[(cas, pref)] = d
        neto = float((d["_usd"] * d["_s"]).sum()) if len(d) else 0.0
        chk(f"{clave:<18} {'?' if n_esp is None else n_esp} filas · USD neto {'—' if usd_esp is None else f'{usd_esp:,.2f}'}",
            (n_esp is None or len(d) == n_esp) and (usd_esp is None or abs(neto - usd_esp) < 0.02), f"{len(d)} · USD {neto:,.2f}")
    # ↩️ DEVOLUCIÓN Intuit JD Sports −258,94 de María (18-oct), de la compra JD Sports 258,94 del
    # 24-sep YA CARGADA en 1444. procesar_intuit no carga montos ≤ 0, así que se arma copiando la
    # fila de la compra del histórico: misma TRM (netea en 0), Tipo Ingreso, Orden intuit_dev_<sha1-12>.
    # Solo entra con --dev-jd (OK de Julián); sin él solo se muestra.
    import hashlib
    hh1 = vivo[HOJA["1444"]].copy(); hh1["Orden"] = hh1["Orden"].astype(str).str.strip()
    hh1 = hh1[hh1["Orden"].str.startswith("intuit_")]
    _u = pd.to_numeric(hh1["Monto"]) / pd.to_numeric(hh1["TRM"])
    m_c = pd.to_datetime(hh1["Fecha"]).dt.strftime("%Y-%m-%d").eq("2026-09-24") & (_u - 258.94).abs().lt(0.005)
    chk("Intuit: la compra JD Sports 258,94 del 24-sep de María está en 1444 (1 fila)", int(m_c.sum()) == 1)
    compra = hh1[m_c].iloc[0]
    ORDEN_DEV = "intuit_dev_" + hashlib.sha1(
        "Oct 18, 2026|-$258.94|Jd Sports|Maria Moises".encode("utf-8")).hexdigest()[:12]
    d1 = nuevas[("1444", "intuit_")]
    dev = compra.reindex(d1.columns.drop(["_usd", "_s"])).copy()
    dev["Orden"] = ORDEN_DEV; dev["Tipo"] = "Ingreso"
    dev["Fecha"] = type(d1["Fecha"].iloc[0])("2026-10-18") if isinstance(d1["Fecha"].iloc[0], str) else pd.Timestamp("2026-10-18")
    # mismo formato que las devoluciones de las otras tarjetas (Julián, 19-oct: "ingreso por devolución
    # como en las otras tc"): 'Tarjeta X - reembolso (TRM compra AAAA-MM-DD) - COMERCIO'
    dev["Nombre del producto"] = "Tarjeta Intuit - reembolso (TRM compra 2026-09-24) - Jd Sports"
    dev["Motivo"] = "Tarjeta Intuit"
    chk("devolución: Motivo/TRM de la compra", str(compra["Motivo"]).strip() == "Tarjeta Intuit", str(compra["Motivo"]))
    dev["_usd"] = 258.94; dev["_s"] = -1
    print("  ↩️  devolución JD Sports " + ("SE CARGA (--dev-jd)" if DEV_JD else "PROPUESTA (no entra sin --dev-jd)") + ":")
    print(pd.DataFrame([compra.reindex(dev.index), dev])[["Fecha", "Tipo", "Monto", "Orden", "TRM",
                                                        "Nombre del producto"]].to_string(index=False))
    DEV_ATTR = {"compra": str(compra["Orden"]), "orden": ORDEN_DEV, "fecha": "2026-10-18",
                "merch": "Jd Sports", "usd": 258.94}
    if DEV_JD:
        nuevas[("1444", "intuit_")] = pd.concat([d1, dev.to_frame().T], ignore_index=True)
    todas = pd.concat([d for d in nuevas.values() if len(d)], ignore_index=True)
    chk("0 Orden duplicados", not todas["Orden"].duplicated().any())
    chk("ninguna está ya en la lista de exclusión",
        not (set(todas["Orden"].astype(str)) & set(cobrados)))
    ya = set().union(*[set(o0[c]) for c in o0])
    chk("ningún Orden nuevo está ya en el histórico", not (set(todas["Orden"].astype(str)) & ya))
    extra = {k: len(v) for k, v in salidas.items()
             if (k.split("_", 1)[1], k.split("_", 1)[0] + "_") not in ESPERADO and len(v)}
    chk("ninguna salida fuera de lo esperado", not extra, str(extra))
    # US Bank reentra filas ya cargadas (no estaba en la lista): deben ser IDÉNTICAS
    for (cas, pref) in [k for k in ESPERADO if k[1] == "usbank_"]:
        d_all = salidas.get(f"{pref}{cas}", pd.DataFrame())
        hh = vivo[HOJA[cas]].copy(); hh["Orden"] = hh["Orden"].astype(str).str.strip()
        if d_all.empty or "Orden" not in d_all.columns:
            continue
        j = d_all.merge(hh, on="Orden", suffixes=("_n", "_h"))
        if len(j):
            dM = (pd.to_numeric(j["Monto_n"]) - pd.to_numeric(j["Monto_h"])).abs()
            chk(f"{cas} usbank_: las {len(j)} que reentran son IDÉNTICAS",
                int((dM > 0.5).sum()) == 0, f"{int((dM>0.5).sum())} difieren")
    print(f"\n  TOTAL a cargar: {len(todas)} filas · "
          f"USD neto {float((todas['_usd']*todas['_s']).sum()):,.2f}")
    for (cas, pref), d in sorted(nuevas.items()):
        if not len(d):
            continue
        f = pd.to_datetime(d["Fecha"])
        print(f"    {cas} {pref:<11} {len(d):>2} filas · {f.min().date()}→{f.max().date()} · "
              f"COP neto {float((pd.to_numeric(d['Monto'])*d['_s']).sum()):>13,.0f} · "
              f"{d['Tipo'].value_counts().to_dict()}")
    if not ok:
        raise SystemExit("⛔ ABORTA: falló la reconciliación.")

    banner("4) 🧪 PRUEBA DEL EXTRACTO CORTO (lo ya cargado se reproduce igual, en USD)")
    for nom, fn, corte in TARJETAS(mod):
        todo = fn(crudos[nom].copy(), fecha_desde=corte, cobrados=set(), pendientes=None,
                  hist_tarjetas=hist_t, cobrados_df=None)
        harness.clear_msgs()
        if nom == "capital":
            tp = todo.get("capital_11591", pd.DataFrame())
            ya_p = {y for y in o0["11591"] if y.startswith("capital_")}
            chk("capital sin lista: a Paula solo va lo suyo (lo ya cargado + lo nuevo)",
                set(tp["Orden"].astype(str)) == ya_p | set(nuevas[("11591", "capital_")]["Orden"].astype(str)) if len(nuevas[("11591", "capital_")]) else set(tp["Orden"].astype(str)) == ya_p,
                f"{len(tp)} fila(s)")
        for clave, t in todo.items():
            cas = clave.split("_", 1)[1]
            if cas not in HOJA or t.empty:
                continue
            vi = vivo[HOJA[cas]].copy(); vi["Orden"] = vi["Orden"].astype(str).str.strip()
            vi = vi[vi["Orden"].str.startswith(f"{nom}_")]
            t = t.copy(); t["Orden"] = t["Orden"].astype(str).str.strip()
            j = vi.merge(t, on="Orden", suffixes=("_h", "_n"))
            if j.empty:
                print(f"  ·  {nom}/{cas}: 0 solapadas (nada ya cargado que comparar)")
                continue
            uh = pd.to_numeric(j["Monto_h"]) / pd.to_numeric(j["TRM_h"])
            un = pd.to_numeric(j["Monto_n"]) / pd.to_numeric(j["TRM_n"])
            dP = j["Tipo_n"].astype(str).str.strip() != j["Tipo_h"].astype(str).str.strip()
            reasig = int(((pd.to_numeric(j["TRM_n"]) - pd.to_numeric(j["TRM_h"])).abs() > 0.005).sum())
            chk(f"{nom}/{cas}: {len(j)} ya cargadas se reproducen igual (USD)",
                int(((un - uh).abs() > 0.02).sum()) == 0 and int(dP.sum()) == 0,
                f"ΔUSD={int(((un-uh).abs()>0.02).sum())} ΔTipo={int(dP.sum())}"
                + (f" · {reasig} reembolso(s) con TRM reasignada" if reasig else ""))
    if not ok:
        raise SystemExit("⛔ ABORTA: el reproceso no reproduce lo ya cargado.")

    fecha_carga = pd.Timestamp.today().strftime("%Y-%m-%d")

    banner("5) APLICAR AL HISTÓRICO")
    historico = {k: mod.asegurar_columnas_historico(v.copy()) for k, v in vivo.items()}
    for cas, h in HOJA.items():
        d = historico[h]; antes = len(d)
        prefs = [p for (c, p) in nuevas if c == cas and len(nuevas[(c, p)])]
        for pref in prefs:
            x = nuevas[(cas, pref)].drop(columns=["_usd", "_s"]).copy()
            x["Fecha de Carga"] = fecha_carga
            d = pd.concat([d, mod.asegurar_columnas_historico(x)], ignore_index=True)
            o = d["Orden"].astype(str).str.strip(); mm = o.str.startswith(pref)
            d = pd.concat([d[~mm], d[mm].drop_duplicates(subset=["Orden"], keep="last")],
                          ignore_index=True)
        u, c = usuario_de_totales(vivo[h]), cas_de_totales(vivo[h], cas)
        historico[h] = mod.recalcular_totales_diarios(d, usuario=u, cas=c)
        print(f"  {h:<32} {antes} → {len(historico[h])} filas · " +
              " · ".join(f"{p}{PREVIAS[cas].get(p,0)}→"
                         f"{int(historico[h]['Orden'].astype(str).str.startswith(p).sum())}"
                         for p in prefs))

    banner("6) COMISIÓN E INCENTIVOS")
    for cas, h in HOJA.items():
        a = vivo[h]["Nombre del producto"].astype(str).str.lower().str.startswith("comision de (")
        b = historico[h]["Nombre del producto"].astype(str).str.lower().str.startswith("comision de (")
        chk(f"{cas}: comisión intacta",
            int(a.sum()) == int(b.sum()) and
            abs(pd.to_numeric(vivo[h].loc[a, "Monto"]).sum()
                - pd.to_numeric(historico[h].loc[b, "Monto"]).sum()) < 0.01,
            f"{int(b.sum())} filas")
        qa = vivo[h]["Orden"].astype(str).str.startswith("incentivo")
        qb = historico[h]["Orden"].astype(str).str.startswith("incentivo")
        chk(f"{cas}: incentivos intactos",
            int(qa.sum()) == int(qb.sum()) and
            abs(pd.to_numeric(vivo[h].loc[qa, "Monto"]).sum()
                - pd.to_numeric(historico[h].loc[qb, "Monto"]).sum()) < 0.01,
            f"{int(qb.sum())} filas · Σ {pd.to_numeric(historico[h].loc[qb,'Monto']).sum():,.0f}")
    for cas, h in HOJA.items():
        if cas not in {str(k) for k in mod.COMISION_QUINCENAL_CONF}:
            chk(f"{cas} no tiene comisión quincenal (no se recalcula nada)", True)
            continue
        # la comisión es 1,5% × |Total diario MÁS NEGATIVO de la quincena|. Si la base de cada
        # quincena de septiembre (min(0, peor día)) es la misma antes y después, no cambia.
        def _base(dd, ini, fin):
            dd = dd.copy(); dd["_f"] = pd.to_datetime(dd["Fecha"], errors="coerce")
            tt = dd[(dd["Tipo"].astype(str).str.strip().str.upper() == "TOTAL") &
                    (dd["_f"] >= pd.Timestamp(ini)) & (dd["_f"] <= pd.Timestamp(fin))]
            w = pd.to_numeric(tt["Monto"], errors="coerce")
            return (min(0.0, float(w.min())) if len(w) else 0.0), (float(w.min()) if len(w) else None)
        for ini, fin in (("2026-10-01", "2026-10-15"), ("2026-10-16", "2026-10-31")):
            (ba, ma), (bd, md_) = _base(vivo[h], ini, fin), _base(historico[h], ini, fin)
            print(f"  {'✔' if abs(ba - bd) < 0.01 else '⚠️ '} {cas}: base de comisión {ini[5:]}→{fin[5:]} "
                  f"{'no cambia' if abs(ba - bd) < 0.01 else 'CAMBIA (la recalcula la próxima corrida)'}  "
                  f"peor día {ma:,.0f} → {md_:,.0f} · comisión {abs(ba)*0.015:,.2f} → {abs(bd)*0.015:,.2f}")
    banner("7) INVARIANTES + CAPA B + GUARD A")
    historico = mod.preservar_filas_tarjeta(historico, vivo=vivo); harness.drenar()
    vac = {"", "nan", "none", "nat"}
    for cas, h in HOJA.items():
        o1 = historico[h]["Orden"].astype(str).str.strip()
        for p, n in PREVIAS[cas].items():
            esp = n + len(nuevas.get((cas, p), []))
            chk(f"{cas} {p:<11} = {n} + {esp-n}", int(o1.str.startswith(p).sum()) == esp,
                f"{int(o1.str.startswith(p).sum())}")
        perd = {y for y in o0[cas] if y.lower() not in vac} - {y for y in o1 if y.lower() not in vac}
        chk(f"{cas}: 0 Orden previos perdidos", not perd, f"{len(perd)}")
        z = o1[o1.str.startswith(("amex_", "rakuten_", "robinhood_", "capital_", "usbank_",
                                  "intuit_", "discover_", "applepay_", "migracionamex_"))]
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

    banner("8) SALDOS")
    s1 = {}
    for cas, h in HOJA.items():
        s1[cas] = saldo(historico[h])
        print(f"  {h:<32} COP {s0[cas]:>16,.2f} → {s1[cas]:>16,.2f}   Δ {s1[cas]-s0[cas]:>+14,.2f}")
        for (c, p), d in sorted(nuevas.items()):
            if c == cas and len(d):
                cop = float((pd.to_numeric(d["Monto"]) * d["_s"]).sum())
                print(f"       {p:<11} {-cop:>+15,.0f}  ({len(d)} filas · USD {float((d['_usd']*d['_s']).sum()):,.2f})")
        esperado_cop = -sum(float((pd.to_numeric(d["Monto"]) * d["_s"]).sum())
                            for (c, p), d in nuevas.items() if c == cas and len(d))
        chk(f"{cas}: Δ saldo = Σ de lo cargado", abs((s1[cas] - s0[cas]) - esperado_cop) < 1.0,
            f"{esperado_cop:,.0f}")

    banner("9) ENTRADAS PARA tarjetas_cobradas.xlsx")
    carpeta = str(PurePosixPath(cfg["remote_path"]).parent)
    remote_l = f"{carpeta}/{mod.TARJETAS_COBRADAS_FILENAME}"
    md_l = mod.dbx.files_get_metadata(remote_l)
    _, res_l = mod.dbx.files_download(remote_l)
    contenido_previo = res_l.content
    xls = pd.ExcelFile(io.BytesIO(contenido_previo))
    libro = {hh: xls.parse(hh) for hh in xls.sheet_names}
    cob = libro["cobradas"]
    prev = cob["tarjeta"].astype(str).str.strip().str.lower().value_counts().to_dict()
    print(f"  lista rev={md_l.rev} · 'cobradas': {len(cob)} · por tarjeta: {prev}")
    filas = []
    for (cas, pref), d in sorted(nuevas.items()):
        if not len(d):
            continue
        tarjeta = pref.rstrip("_")
        cap = capturado[tarjeta].copy()
        cap["_orden"] = cap["_orden"].astype(str).str.strip()
        cap = cap.drop_duplicates(subset=["_orden"], keep="first").set_index("_orden")
        if DEV_ATTR["orden"] in set(d["Orden"].astype(str)):
            base = cap.iloc[0].copy()
            base["_fecha"] = pd.Timestamp(DEV_ATTR["fecha"]); base["_usd"] = DEV_ATTR["usd"]
            base["_merch_attr"] = DEV_ATTR["merch"]; base["_cas"] = "1444"
            cap.loc[DEV_ATTR["orden"]] = base
        falt = [x for x in d["Orden"].astype(str) if x not in cap.index]
        chk(f"{cas} {pref:<11} todas con atributos del módulo", not falt, f"{len(falt)}")
        if falt:
            continue
        for _, r in d.sort_values(["Fecha", "Orden"]).iterrows():
            cc = cap.loc[str(r["Orden"])]
            usd = round(abs(float(cc["_usd"])), 2)
            f_attr = pd.to_datetime(cc["_fecha"])
            cas_barrera = str(cc["_cas"]).strip()
            esperado_b = str(mod.CAPITAL_CASILLERO) if tarjeta == "capital" else cas
            if cas_barrera != esperado_b:
                chk(f"{r['Orden']}: casillero de la barrera {cas_barrera} = {esperado_b}", False)
            filas.append({
                "Orden": str(r["Orden"]), "tarjeta": tarjeta, "casillero": int(cas_barrera),
                "fecha_compra": f_attr, "monto_usd": usd, "nota": NOTA,
                "fuente": f"historico_mayoristas.xlsx (cargue {fecha_carga})",
                "card_norm": CARD_NORM[cas],
                "merchant_norm": mod._norm_merchant(cc["_merch_attr"]),
                "usd_abs": usd, "fecha_attr": f_attr, "attr_fuente": f"extracto {tarjeta}",
                "signo": str(r["Tipo"]).strip(),
            })
    nuevas_l = pd.DataFrame(filas)[list(cob.columns)]
    print("  casillero en la lista por tarjeta:",
          nuevas_l.groupby(["tarjeta", "casillero"]).size().to_dict())
    chk(f"{len(todas)} entradas nuevas", len(nuevas_l) == len(todas), f"{len(nuevas_l)}")
    chk("ningún atributo de la barrera 2 vacío",
        nuevas_l[["merchant_norm", "usd_abs", "fecha_attr", "signo"]].notna().all().all()
        and (nuevas_l["merchant_norm"].astype(str).str.strip() != "").all()
        and nuevas_l["signo"].isin(["Egreso", "Ingreso"]).all())
    chk("0 Orden repetidos contra la lista",
        not set(nuevas_l["Orden"]) & set(cob["Orden"].astype(str).str.strip()))
    libro["cobradas"] = pd.concat([cob, nuevas_l], ignore_index=True)
    chk("0 Orden duplicados en toda la lista",
        not libro["cobradas"]["Orden"].astype(str).str.strip().duplicated().any())
    for hh in xls.sheet_names:
        if hh != "cobradas":
            chk(f"'{hh}' intacta", libro[hh].equals(xls.parse(hh)), f"{len(libro[hh])} filas")
    nuevo_por_t = libro["cobradas"]["tarjeta"].astype(str).str.lower().value_counts().to_dict()
    print(f"  'cobradas': {len(cob)} → {len(libro['cobradas'])} · por tarjeta: {nuevo_por_t}")
    if not ok:
        raise SystemExit("⛔ ABORTA: falló alguna verificación de la lista.")
    bl = io.BytesIO()
    with pd.ExcelWriter(bl, engine="openpyxl") as w2:
        for hh, dd in libro.items():
            dd.to_excel(w2, sheet_name=hh, index=False)
    bl.seek(0); data_lista = bl.read()
    print(f"  lista nueva: {len(data_lista):,} bytes")

    if not ESCRIBIR:
        banner("DRY-RUN TERMINADO — 0 ESCRITURAS")
        print(f"  Para escribir: python3 cargar_tc_20261019.py --escribir {md.rev}" + (" --dev-jd" if DEV_JD else "") + "")
        return

    banner("10) ESCRITURA 1/2 — HISTÓRICO")
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
        print(f"  {'✔' if c else '🚨'} {n:<66} {det}")
        ok2 = ok2 and bool(c)
    for cas, h in HOJA.items():
        q = rel[h]["Orden"].astype(str).str.strip()
        chk2(f"{cas}: saldo", abs(saldo(rel[h]) - s1[cas]) < 0.01, f"COP {saldo(rel[h]):,.2f}")
        for p, n in PREVIAS[cas].items():
            esp = n + len(nuevas.get((cas, p), []))
            chk2(f"{cas} {p:<11} = {esp}", int(q.str.startswith(p).sum()) == esp)
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
    print(f"  rev histórico NUEVA = {md2.rev}")

    banner("11) ESCRITURA 2/2 — LISTA DE EXCLUSIÓN")
    if mod.dbx.files_get_metadata(remote_l).rev != md_l.rev:
        raise SystemExit(f"⛔ ABORTA: la lista se movió. El HISTÓRICO YA SE ESCRIBIÓ "
                         f"(rev {md2.rev}) — registrar las {len(todas)} entradas a mano.")
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_l = f"{carpeta}/{PurePosixPath(remote_l).stem}_backup_{ts}_pre_tc.xlsx"
    mod.dbx.files_upload(contenido_previo, backup_l, mode=dropbox.files.WriteMode.add)
    print(f"  🛟 respaldo: {backup_l} ({len(contenido_previo):,} bytes)")
    mod.dbx.files_upload(data_lista, remote_l, mode=dropbox.files.WriteMode.overwrite)
    md_l2 = mod.dbx.files_get_metadata(remote_l)
    _, rl2 = mod.dbx.files_download(remote_l)
    c2 = pd.ExcelFile(io.BytesIO(rl2.content)).parse("cobradas")
    chk2(f"'cobradas' = {len(cob) + len(todas)}", len(c2) == len(cob) + len(todas), f"{len(c2)}")
    for t, n in nuevo_por_t.items():
        chk2(f"'{t}' = {n}", int((c2["tarjeta"].astype(str).str.lower() == t).sum()) == n)
    chk2("0 Orden duplicados", not c2["Orden"].astype(str).str.strip().duplicated().any())
    print(f"  rev lista NUEVA = {md_l2.rev}")

    banner("12) 🔥 PRUEBA DE FUEGO: recargar los extractos ya no cobra nada")
    cob2, pen2, cdf2 = mod.cargar_tarjetas_cobradas(); harness.clear_msgs()
    ht2 = mod.cargar_hist_tarjetas(); harness.clear_msgs()
    tot = 0
    for nom, fn, corte in TARJETAS(mod):
        o3 = fn(crudos[nom].copy(), fecha_desde=corte, cobrados=cob2, pendientes=pen2,
                hist_tarjetas=ht2, cobrados_df=cdf2)
        harness.clear_msgs()
        n3 = sum(len(v) for v in o3.values())
        chk2(f"{nom}: recargar no cobra nada", n3 == 0, f"{n3} filas")
        tot += n3

    banner("13) COPIA A ONEDRIVE")
    hoy = pd.Timestamp.today().strftime("%Y%m%d")
    destino = f"{ONEDRIVE_DIR}/{hoy}_Historico_mayoristas.xlsx"
    os.makedirs(ONEDRIVE_ANT, exist_ok=True)
    # ⚠️ COPIAR, no mover: el 18-sep un shutil.move a Antiguos seguido de escribir el nuevo en
    # la misma ruta hizo que OneDrive perdiera el archivo movido. Se copia, se comprueba el
    # tamaño, y solo entonces se borra/sobrescribe.
    for f2 in sorted(os.listdir(ONEDRIVE_DIR)):
        if f2.endswith("_Historico_mayoristas.xlsx"):
            src = f"{ONEDRIVE_DIR}/{f2}"
            dst = f"{ONEDRIVE_ANT}/{f2.replace('.xlsx', '')}_PRE_tc.xlsx"
            if os.path.exists(dst):
                raise SystemExit(f"⛔ ya existe {dst}; no se pisa. Revisar a mano.")
            shutil.copy2(src, dst)
            if not (os.path.exists(dst) and os.path.getsize(dst) == os.path.getsize(src)):
                raise SystemExit(f"⛔ la copia a Antiguos no quedó bien; NO se reemplaza {f2}.")
            if f2 != os.path.basename(destino):
                os.remove(src)
            print(f"  archivada: {f2} → Antiguos/{os.path.basename(dst)}")
    with open(destino, "wb") as fh:
        fh.write(contenido_hist)
    print(f"  escrita: {destino} ({os.path.getsize(destino):,} bytes)")
    chk2("la copia de OneDrive pesa lo mismo que el vivo",
         os.path.getsize(destino) == md2.size, f"{os.path.getsize(destino):,} vs {md2.size:,}")

    print(f"\n  {'✅ CARGUE COMPLETO — LAS DOS ESCRITURAS' if ok2 else '🚨 REVISAR'}")
    print(f"  histórico rev {md2.rev}   rollback {backup_h}")
    print(f"  lista     rev {md_l2.rev}   rollback {backup_l}")


if __name__ == "__main__":
    sys.path.insert(0, os.environ.get("HARNESS_DIR", "."))
    main()
