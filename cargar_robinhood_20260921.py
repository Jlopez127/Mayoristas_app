#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cargue de ROBINHOOD del 2026-09-21 (tarde), sobre el histórico que dejó cargar_tc_20260921.py.

    Robinhood -> 1444 Maria            62e7dfc7-baa2-4341-86df-c437910afb67.csv (1-ene -> 21-sep)

29 movimientos de Juan Pablo Correal (12 -> 19-sep). La barrera 2 excluye 6 compras de mayo que
Robinhood re-fechó (ya cobradas). 3 Refund de 'Globetrotter Qeeq' (USD 9,37, Juan Pablo) no
encuentran compra por nombre y usan la TRM de su día (diferencia < COP 1.000): son de 1444.

LAS DOS ESCRITURAS (regla 5 del CLAUDE.md)
------------------------------------------
  1. el HISTÓRICO
  2. `tarjetas_cobradas.xlsx` con las MISMAS filas (barrera 2 por atributos)
En ese orden: si la lista fuera primero, los módulos excluirían las filas y no cargarían nada.
Capital, Discover, Intuit y Rakuten generan su Orden por HASH: la entrada en la lista NO es
opcional. US Bank usa ID NATIVO y hasta ahora se dejaba fuera a propósito — aquí SÍ se registra:
la regla 5 dice que todo cargue son dos escrituras, y registrar de más no puede dejar de cobrar
nada (mientras el extracto siga generando ese Orden, la barrera 1 lo consume).

⚠️ US Bank REENTRA filas ya cargadas (22 en este extracto) justo por no estar en la lista. Se
verifican IDÉNTICAS y se excluyen del conteo de nuevas; al registrarlas ahora, dejarán de
reentrar.

Los atributos se CAPTURAN envolviendo `_excluir_por_atributos`, no se reconstruyen del
histórico: así usan la misma normalización que la barrera.

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

DL = "/Users/julianlopez/Downloads"
CSV_ROBINHOOD = f"{DL}/62e7dfc7-baa2-4341-86df-c437910afb67.csv"

# tarjeta -> (nombre del módulo, constante de corte, prefijo, card_norm por casillero)
CARD_NORM = {"13608": "JULIAN SANCHEZ", "11591": "PAULA HERRERA", "1444": "MARIA MOISES"}
NOTA = "cargue Robinhood 2026-09-21 (1-ene -> 21-sep)"

REV_ESPERADA = "0165c00d66205f000000002f34b3f21"
# lo que debe entrar: (casillero, prefijo) -> (filas, USD neto)
ESPERADO = {
    ("1444", "robinhood_"): (29, 1705.13),
}
# filas por prefijo que ya hay en el histórico, por casillero
PREVIAS = {"1444": {"intuit_": 42, "amex_": 151, "rakuten_": 165, "robinhood_": 290,
                    "applepay_": 4, "migracionamex_": 40}}

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
        for i in range(6):
            v = _o(f, c if c is not None else {}, *a, **k)
            if v is not None:
                return v
            time.sleep(1.5)
        return None
    mod._amex_trm_dia = _trm

    # 🧯 tripwires: solo Robinhood, y nunca el incentivo
    for fn in ("procesar_amex", "procesar_capital", "procesar_usbank", "procesar_intuit",
               "procesar_rakuten", "procesar_discover", "procesar_egresos", "agregar_incentivo_amex"):
        def _boom(*a, _n=fn, **k):
            raise AssertionError(f"{_n} fue llamada — este script no carga esa tarjeta")
        setattr(mod, fn, _boom)

    print(f"MODO: {'🔴 ESCRITURA REAL' if ESCRIBIR else '🟢 DRY-RUN (0 escrituras)'}")

    banner("0) PERILLAS")
    for k in ("ROBINHOOD_FECHA_DESDE", "ROBINHOOD_CASILLERO", "ROBINHOOD_CARDMAP",
              "ROBINHOOD_STATUS_OK", "ROBINHOOD_VENTANA_MANUAL_FIN"):
        print(f"  {k:<30} = {getattr(mod, k)}")
    chk("Robinhood: Juan Pablo y Maria -> 1444 (Santiago, Carlos y Kelly no)",
        mod.ROBINHOOD_CARDMAP == {"Juan Pablo Correal Perez": "1444", "Maria Moises": "1444"})
    chk("robinhood_ protegido por la capa B", "robinhood_" in mod.TARJETA_ORDEN_RE)

    banner("1) HISTÓRICO VIVO FRESCO")
    cfg = mod.st.secrets["dropbox"]
    md = mod.dbx.files_get_metadata(cfg["remote_path"])
    _, res = mod.dbx.files_download(cfg["remote_path"])
    vivo = pd.read_excel(io.BytesIO(res.content), sheet_name=None)
    HOJA = {c: next(h for h in vivo if h.split(" - ")[0].strip() == c) for c in PREVIAS}
    chk("hoja de 1444 = '1444 - Maria Moises' (no la COP)", HOJA["1444"] == "1444 - Maria Moises",
        HOJA["1444"])
    print(f"  rev={md.rev}  modificado={md.server_modified}  size={md.size:,}")
    s0, o0 = {}, {}
    for c, h in HOJA.items():
        s0[c] = saldo(vivo[h]); o0[c] = vivo[h]["Orden"].astype(str).str.strip()
        print(f"  {h:<32} {len(vivo[h]):>6} filas · saldo COP {s0[c]:>16,.2f}")
        for p, n in PREVIAS[c].items():
            chk(f"  {c} {p:<11} previas = {n}", int(o0[c].str.startswith(p).sum()) == n,
                f"{int(o0[c].str.startswith(p).sum())}")
    if md.rev != REV_ESPERADA:
        print(f"  ⚠️  el histórico se movió (esperaba {REV_ESPERADA})")
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

    crudos = {"robinhood": pd.read_csv(CSV_ROBINHOOD)}
    salidas = {}
    for nom, fn, corte in (("robinhood", mod.procesar_robinhood, mod.ROBINHOOD_FECHA_DESDE),):
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
        chk(f"{clave:<18} {n_esp} filas · USD neto {usd_esp:,.2f}",
            len(d) == n_esp and abs(neto - usd_esp) < 0.02, f"{len(d)} · USD {neto:,.2f}")
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
    for nom, fn, corte in (("robinhood", mod.procesar_robinhood, mod.ROBINHOOD_FECHA_DESDE),):
        todo = fn(crudos[nom].copy(), fecha_desde=corte, cobrados=set(), pendientes=None,
                  hist_tarjetas=hist_t, cobrados_df=None)
        harness.clear_msgs()
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

    banner("5) APLICAR AL HISTÓRICO")
    fecha_carga = pd.Timestamp.today().strftime("%Y-%m-%d")
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
        for ini, fin in (("2026-09-01", "2026-09-15"), ("2026-09-16", "2026-09-30")):
            (ba, ma), (bd, md_) = _base(vivo[h], ini, fin), _base(historico[h], ini, fin)
            chk(f"{cas}: base de comisión {ini[5:]}→{fin[5:]} no cambia",
                abs(ba - bd) < 0.01,
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
        print("  Para escribir: python3 cargar_robinhood_20260921.py --escribir")
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
    backup_l = f"{carpeta}/{PurePosixPath(remote_l).stem}_backup_{ts}_pre_robinhood.xlsx"
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

    banner("12) 🔥 PRUEBA DE FUEGO: recargar el extracto ya no cobra nada")
    cob2, pen2, cdf2 = mod.cargar_tarjetas_cobradas(); harness.clear_msgs()
    ht2 = mod.cargar_hist_tarjetas(); harness.clear_msgs()
    tot = 0
    for nom, fn, corte in (("robinhood", mod.procesar_robinhood, mod.ROBINHOOD_FECHA_DESDE),):
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
            dst = f"{ONEDRIVE_ANT}/{f2.replace('.xlsx', '')}_PRE_robinhood.xlsx"
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
