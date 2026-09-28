#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cargue TC del 2026-09-28, en UNA sola escritura de histórico y UNA de lista.

    Capital   -> 13608 + 11591   2026-09-28_transaction_download (1).csv (card 1484; la 7418 NO)
                 2 reembolsos: eBay 25-sep (compra 11-ago -> Julian) y Walmart 20-sep (-> Paula)
    US Bank   -> 11591 + 13608   Credit Card - 0613_08-01-2026_10-02-2026.csv (22 Paula, 6 Julian)
    Intuit    -> 1444            Compras TC/intuit_acumulado.csv (12; 4 de Juan Pablo Correal,
                                 usuario nuevo -> 1444 por decisión del usuario 28-sep)
    Rakuten   -> 1444            Rakuten_Activity_All (8).csv (20)
    Discover  -> 13608           Discover-RecentActivity-20260928.xls (22)

MÁS 7 DEVOLUCIONES AMEX MANUALES A PAULA (11591), USD 3.015,75 (decisión del usuario 28-sep):
las devolvió la migración Amex -> US Bank en la sub-tarjeta 2529 (Kelly, ignorada), así que
ningún módulo las abona. Van "como si fueran de Amex" (Motivo 'Tarjeta Amex', Orden amex_...)
para que la capa B las proteja. TRM de SU COMPRA; la de PayPal sin compra, la de su día.
NO van a la lista: no vienen de ningún extracto procesado. ⚠️ Si algún día se asigna la
sub-tarjeta 2529, esos 8 créditos de Kelly hay que excluirlos o se abonan dos veces.

Mismo esqueleto que cargar_tc_20260921.py (ver su docstring: dos escrituras, captura de la
barrera 2, casillero de Capital en la lista = 13608, copiar-no-mover en OneDrive).

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
CSV_CAPITAL = f"{DL}/2026-09-28_transaction_download (1).csv"
CSV_USBANK = f"{DL}/Credit Card - 0613_08-01-2026_10-02-2026.csv"
CSV_INTUIT = "/Users/julianlopez/Library/CloudStorage/OneDrive-Personal/Encargomio/Compras TC/intuit_acumulado.csv"
CSV_RAKUTEN = f"{DL}/Rakuten_Activity_All (8).csv"
XLS_DISCOVER = f"{DL}/Discover-RecentActivity-20260928.xls"

# tarjeta -> (nombre del módulo, constante de corte, prefijo, card_norm por casillero)
CARD_NORM = {"13608": "JULIAN SANCHEZ", "11591": "PAULA HERRERA", "1444": "MARIA MOISES"}
NOTA = "cargue TC 2026-09-28 (Capital 1484 + US Bank + Intuit + Rakuten + Discover)"

REV_ESPERADA = REV_ARG
# lo que debe entrar: (casillero, prefijo) -> (filas, USD neto con signo: egreso +, ingreso -)
ESPERADO = {
    ("11591", "capital_"): (1,   -449.00),
    ("13608", "capital_"): (1,  -1046.96),
    ("11591", "usbank_"):  (22,  4642.08),
    ("13608", "usbank_"):  (6,   1054.93),
    ("1444",  "intuit_"):  (12,  1714.81),
    ("1444",  "rakuten_"): (20,  4602.57),
    ("13608", "discover_"): (22, None),      # None = solo se exige el conteo (USD se imprime)
}
PREVIAS = {}     # se leen del vivo y se imprimen (cambian con cada corrida diaria)

# 7 devoluciones Amex manuales a Paula: (fecha, orden, usd, trm, fecha_compra, descripción)
DEV_AMEX = [
    ("2026-08-19", "amex_devkelly_320262170334344553", 78.20, 3355.44, "2026-08-04", "PAYPAL *EBAY 800-4564029357733 CA"),
    ("2026-08-19", "amex_devkelly_320262170336056318", 782.00, 3355.44, "2026-08-04", "PAYPAL *EBAY 800-4564029357733 CA"),
    ("2026-08-19", "amex_devkelly_320262170336062910", 312.80, 3355.44, "2026-08-04", "PAYPAL *EBAY 800-4564029357733 CA"),
    ("2026-08-19", "amex_devkelly_000020460319343069", 456.32, 3253.65, "2026-08-15", "PAYPAL *EBAY (migracion Amex)"),
    ("2026-08-19", "amex_devkelly_000016645372241301", 629.28, 3253.65, "2026-08-15", "PAYPAL *EBAY (migracion Amex)"),
    ("2026-09-03", "amex_devkelly_000016528673582302", 436.08, 3253.65, "2026-08-15", "LAPTOP OMAR - PAYPAL *EBAY (migracion Amex)"),
    ("2026-09-03", "amex_devkelly_casopaypal_20260903", 321.07, 3265.55, None, "CASO PAYPAL CERRADO"),
]
# el Orden original que cada una devuelve (debe existir en la hoja de Paula)
DEV_AMEX_ORIGEN = {
    "amex_devkelly_320262170334344553": "amex_320262170334344553",
    "amex_devkelly_320262170336056318": "amex_320262170336056318",
    "amex_devkelly_320262170336062910": "amex_320262170336062910",
    "amex_devkelly_000020460319343069": "migracionamex_000020460319343069",
    "amex_devkelly_000016645372241301": "migracionamex_000016645372241301",
    "amex_devkelly_000016528673582302": "migracionamex_000016528673582302",
}

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

    # 🧯 tripwires: solo Capital, US Bank, Intuit, Rakuten y Discover, y nunca el incentivo
    for fn in ("procesar_amex", "procesar_robinhood", "procesar_egresos", "agregar_incentivo_amex"):
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
              "DISCOVER_RETENIDAS"):
        print(f"  {k:<26} = {getattr(mod, k)}")
    chk("Capital: traspaso a Paula el 2026-09-09",
        mod.CAPITAL_FECHA_TRASPASO == "2026-09-09" and mod.CAPITAL_CASILLERO_DESDE == "11591")
    chk("US Bank: 0598 -> Paula, 0609 -> Julian",
        mod.USBANK_MAP_SUBTARJETA == {"0598": "11591", "0609": "13608"})
    chk("Intuit: Maria -> 1444, Elvis -> 11591, Juan Pablo Correal -> 1444",
        mod.INTUIT_MAP_USUARIO == {"maria moises": "1444", "elvis martinez": "11591",
                                   "juan pablo correal": "1444"})
    chk("capital_, usbank_, intuit_, rakuten_ y discover_ protegidos por la capa B",
        all(p in mod.TARJETA_ORDEN_RE for p in ("capital_", "usbank_", "intuit_", "rakuten_", "discover_")))
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

    crudos = {
        "capital": pd.read_csv(CSV_CAPITAL),
        "usbank": pd.read_csv(CSV_USBANK),
        "intuit": pd.read_csv(CSV_INTUIT, encoding="utf-8-sig"),
        "rakuten": pd.read_csv(CSV_RAKUTEN),
        "discover": mod.leer_discover(XLS_DISCOVER),
    }
    chk("el extracto de Capital es SOLO de la 1484",
        set(crudos["capital"]["Card No."].astype(str)) == {str(mod.CAPITAL_CARD_NO)},
        str(set(crudos["capital"]["Card No."].astype(str))))
    salidas = {}
    for nom, fn, corte in (("capital", mod.procesar_capital, mod.CAPITAL_FECHA_DESDE),
                           ("usbank", mod.procesar_usbank, mod.USBANK_FECHA_DESDE),
                           ("intuit", mod.procesar_intuit, mod.INTUIT_FECHA_DESDE),
                           ("rakuten", mod.procesar_rakuten, mod.RAKUTEN_FECHA_DESDE),
                           ("discover", mod.procesar_discover, mod.DISCOVER_FECHA_DESDE)):
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
        chk(f"{clave:<18} {n_esp} filas · USD neto {'—' if usd_esp is None else f'{usd_esp:,.2f}'}",
            len(d) == n_esp and (usd_esp is None or abs(neto - usd_esp) < 0.02), f"{len(d)} · USD {neto:,.2f}")
    todas = pd.concat([d for d in nuevas.values() if len(d)], ignore_index=True)
    chk("0 Orden duplicados entre las cinco tarjetas", not todas["Orden"].duplicated().any())
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
    for nom, fn, corte in (("capital", mod.procesar_capital, mod.CAPITAL_FECHA_DESDE),
                           ("usbank", mod.procesar_usbank, mod.USBANK_FECHA_DESDE),
                           ("intuit", mod.procesar_intuit, mod.INTUIT_FECHA_DESDE),
                           ("rakuten", mod.procesar_rakuten, mod.RAKUTEN_FECHA_DESDE),
                           ("discover", mod.procesar_discover, mod.DISCOVER_FECHA_DESDE)):
        todo = fn(crudos[nom].copy(), fecha_desde=corte, cobrados=set(), pendientes=None,
                  hist_tarjetas=hist_t, cobrados_df=None)
        harness.clear_msgs()
        if nom == "capital":
            tp = todo.get("capital_11591", pd.DataFrame())
            ya_p = {y for y in o0["11591"] if y.startswith("capital_")}
            chk("capital sin lista: a Paula solo va lo suyo (lo ya cargado + lo nuevo)",
                set(tp["Orden"].astype(str)) == ya_p | set(nuevas[("11591", "capital_")]["Orden"].astype(str)),
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

    banner("4b) 7 DEVOLUCIONES AMEX MANUALES A PAULA (sub-tarjeta Kelly en US Bank)")
    fecha_carga = pd.Timestamp.today().strftime("%Y-%m-%d")
    hp = vivo[HOJA["11591"]].copy(); hp["Orden"] = hp["Orden"].astype(str).str.strip()
    filas_m = []
    for f, orden, usd, trm, f_compra, desc in DEV_AMEX:
        orig = DEV_AMEX_ORIGEN.get(orden)
        if orig:
            r0 = hp[hp["Orden"] == orig]
            usd0 = float(pd.to_numeric(r0["Monto"]).sum() / pd.to_numeric(r0["TRM"]).iloc[0]) if len(r0) else 0
            chk(f"{orig} existe, es Egreso de USD {usd:,.2f} a TRM {trm}",
                len(r0) == 1 and str(r0["Tipo"].iloc[0]).strip() == "Egreso"
                and abs(usd0 - usd) < 0.01 and abs(float(r0["TRM"].iloc[0]) - trm) < 0.005,
                f"{len(r0)} fila(s) · USD {usd0:,.2f}")
            etq = f"reembolso (TRM compra {f_compra})"
        else:
            t_dia = mod._amex_trm_dia(f)
            chk(f"{orden}: TRM {trm} = la del {f} (sin compra identificable)",
                t_dia is not None and abs(float(t_dia) - trm) < 0.005, f"{t_dia}")
            etq = "reembolso (sin compra identificable, TRM de su dia)"
        chk(f"{orden} no existe todavía", orden not in set(o0["11591"]))
        filas_m.append({
            "Fecha": pd.Timestamp(f), "Tipo": "Ingreso", "Orden": orden,
            "Monto": float(round(usd * trm)), "Motivo": "Tarjeta Amex", "TRM": trm,
            "Usuario": "Paula Herrera", "Casillero": "11591", "Estado de Orden": None,
            "Nombre del producto": f"Tarjeta Amex - {etq} - {desc} (devuelto en US Bank, sub-tarjeta 2529)",
            "Fecha de Carga": fecha_carga,
        })
    manual = pd.DataFrame(filas_m)
    manual["_usd"] = manual["Monto"] / manual["TRM"]; manual["_s"] = -1
    chk("7 filas · USD 3.015,75", len(manual) == 7 and abs(sum(x[2] for x in DEV_AMEX) - 3015.75) < 0.001)
    print(manual[["Fecha", "Orden", "Monto", "TRM"]].to_string())
    print(f"  Σ COP {manual['Monto'].sum():,.0f}")
    if not ok:
        raise SystemExit("⛔ ABORTA: las devoluciones manuales no cuadran.")

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
        if cas == "11591":
            x = manual.drop(columns=["_usd", "_s"]).copy()
            d = pd.concat([d, mod.asegurar_columnas_historico(x)], ignore_index=True)
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
        for ini, fin in (("2026-09-01", "2026-09-15"), ("2026-09-16", "2026-09-30")):
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
            esp = n + len(nuevas.get((cas, p), [])) + (len(manual) if (cas, p) == ("11591", "amex_") else 0)
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
        if cas == "11591":
            print(f"       dev. Amex   {manual['Monto'].sum():>+15,.0f}  (7 filas manuales · USD 3,015.75)")
        esperado_cop = -sum(float((pd.to_numeric(d["Monto"]) * d["_s"]).sum())
                            for (c, p), d in nuevas.items() if c == cas and len(d))
        if cas == "11591":
            esperado_cop += float(manual["Monto"].sum())
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
        print(f"  Para escribir: python3 cargar_tc_20260928.py --escribir {md.rev}")
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
            esp = n + len(nuevas.get((cas, p), [])) + (len(manual) if (cas, p) == ("11591", "amex_") else 0)
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

    banner("12) 🔥 PRUEBA DE FUEGO: recargar los cinco extractos ya no cobra nada")
    cob2, pen2, cdf2 = mod.cargar_tarjetas_cobradas(); harness.clear_msgs()
    ht2 = mod.cargar_hist_tarjetas(); harness.clear_msgs()
    tot = 0
    for nom, fn, corte in (("capital", mod.procesar_capital, mod.CAPITAL_FECHA_DESDE),
                           ("usbank", mod.procesar_usbank, mod.USBANK_FECHA_DESDE),
                           ("intuit", mod.procesar_intuit, mod.INTUIT_FECHA_DESDE),
                           ("rakuten", mod.procesar_rakuten, mod.RAKUTEN_FECHA_DESDE),
                           ("discover", mod.procesar_discover, mod.DISCOVER_FECHA_DESDE)):
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
