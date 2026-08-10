#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Genera las entradas de TARJETA CAPITAL (Capital One 1484 -> casillero 13608) para la lista de
exclusión `tarjetas_cobradas.xlsx`, hoja "cobradas".

CONTEXTO
--------
Los cobros viejos de Capital NO están en la lista (la lista se generó con amex/rakuten/robinhood).
La hoja congelada "Capital Julian" del Excel `20260710Cobro tarjetas.xlsx` tiene 80 filas de la
tarjeta 1484 ya cobradas al mayorista en bloques manuales del backoffice. Sin sus Orden en la
lista, el cargue 1-a-1 las RE-COBRARÍA.

DOS TIPOS DE ENTRADA
--------------------
1. CON ORDEN (barrera 1, la fuerte): el cobro se encuentra en un extracto real -> se le asigna
   EXACTAMENTE el Orden `capital_<sha1-12>` que el módulo generará para esa fila. Se toma del
   extracto (no se recalcula aparte) para que el `seq` canónico coincida por construcción.
2. SOLO ATRIBUTOS (barrera 2): el cobro NO aparece en ningún extracto disponible (el export de
   Capital One no llega tan atrás). No se puede hashear, así que entra con un Orden CENTINELA
   `capital_sinextracto_<n>` — que el módulo nunca puede generar (siempre produce
   `capital_<12 hex>`) — y con sus atributos poblados (casillero, |USD|, merchant, fecha).
   Cuando un extracto futuro traiga esa compra, la barrera por atributos la atrapa.

COMPRAS **Y** DEVOLUCIONES
--------------------------
Con la regla unificada (2026-08-10) los Credit 'Merchandise' entran como Ingreso, así que la
lista necesita AMBOS tipos de entrada:
  · cobros Debit  -> anti-doble-COBRO  (no volver a cobrar una compra ya facturada)
  · cobros Credit -> anti-doble-ABONO  (no volver a abonar una devolución que el backoffice ya
    neteó dentro de sus bloques manuales; p.ej. el bloque 152228 = 7.088,34 Debit − 5.583,00
    Credit, o el 155839 = 7.280,72 − 941,16)

🔏 CADA ENTRADA LLEVA 'signo' ("Egreso"/"Ingreso"). Es indispensable: en Capital One la
Description de un reembolso es IDÉNTICA a la de su compra y el monto también (16 de los 20
reembolsos del extracto caen a ≤3 días de su propia compra). Sin el signo, un cobro-compra
huérfano taparía por atributos al reembolso entrante, y viceversa. La barrera del app ignora el
signo cuando viene vacío, así que las entradas amex/rakuten/robinhood no cambian.

  sin argumentos -> dry-run (imprime, no escribe)
  --out RUTA.xlsx -> escribe SOLO un archivo local con las filas nuevas (nunca toca Dropbox)
"""
import sys, pathlib
import pandas as pd
import numpy as np

BASE = pathlib.Path(__file__).resolve().parent
EXCEL_COBROS = pathlib.Path(
    "/Users/julianlopez/Library/CloudStorage/OneDrive-Personal/Encargomio/Dash_mayoristas"
) / "20260710Cobro tarjetas.xlsx"
HOJA_COBROS = "Capital Julian"

# Extractos de Capital One disponibles. Se usa la UNIÓN: mientras más historia cubran, más
# cobros se pueden hashear (barrera 1) en vez de quedar solo con atributos (barrera 2).
EXTRACTOS = [
    pathlib.Path("/Users/julianlopez/Downloads/2026-08-10_transaction_download (2).csv"),
    pathlib.Path("/Users/julianlopez/Downloads/2026-08-10_transaction_download (5).csv"),
]

CARD_NO = "1484"
CASILLERO = "13608"
TARJETA = "capital"          # valor de la columna 'tarjeta' (lo filtra _cobros_huerfanos_attr)
CARD_NORM = "JULIAN SANCHEZ"


def _app():
    """Importa el módulo real para reusar _capital_clave_y_seq/_capital_orden/_norm_merchant."""
    sys.path.insert(0, str(BASE))
    import harness
    return harness.cargar_app()


def cobros_capital() -> pd.DataFrame:
    """Las 80 filas Card 1484 de la hoja congelada, con el subtotal parásito neutralizado."""
    d = pd.read_excel(EXCEL_COBROS, sheet_name=HOJA_COBROS)
    d = d[d["Card No."].astype(str).str.strip() == CARD_NO].copy()
    d["_td"] = pd.to_datetime(d["Transaction Date"], errors="coerce")
    d["_deb"] = pd.to_numeric(d["Debit"], errors="coerce")
    d["_cre"] = pd.to_numeric(d["Credit"], errors="coerce")
    # ⚠️ La hoja trae UN subtotal de bloque escrito dentro de la columna Credit, en la MISMA fila
    # de una compra (13-jun, O*19-14751-73078: Debit 642,62 y "Credit" 5.495,01 = suma de los 12
    # Debit del bloque). No es un reembolso: se anula o inflaría un cobro fantasma.
    _ambos = d["_deb"].notna() & d["_cre"].notna()
    if _ambos.any():
        print(f"  ⚠️ {int(_ambos.sum())} fila(s) con Debit Y Credit (subtotal mal ubicado): "
              f"se anula el Credit -> {list(d.loc[_ambos, '_cre'].round(2))}")
        d.loc[_ambos, "_cre"] = pd.NA
    d["_desc"] = d["Description"].astype(str).str.strip()
    return d.reset_index(drop=True)


def extracto_ordenes(mod) -> pd.DataFrame:
    """Unión de los extractos disponibles (Card 1484, filas CARGABLES) con su Orden capital_<hash>.

    El Orden se calcula con las MISMAS funciones del módulo y con el seq canónico sobre TODAS
    las filas cargables del extracto, que es exactamente lo que hará `procesar_capital`."""
    partes = []
    for p in EXTRACTOS:
        if not p.exists():
            print(f"  ⚠️ extracto no encontrado, se omite: {p.name}")
            continue
        e = pd.read_csv(p, dtype=str)
        e = e[e["Card No."].astype(str).str.strip() == CARD_NO].copy()
        e = e[mod._capital_cargables(e)].copy()
        e["_deb"] = pd.to_numeric(e["Debit"], errors="coerce")
        e["_cre"] = pd.to_numeric(e["Credit"], errors="coerce")
        clave, seq = mod._capital_clave_y_seq(e)
        e["Orden"] = mod._capital_orden(clave, seq)
        e["_signo"] = np.where(e["_deb"].notna() & (e["_deb"] > 0), "Egreso", "Ingreso")
        e["_usd"] = e["_deb"].fillna(e["_cre"])
        e["_archivo"] = p.name
        partes.append(e)
        print(f"  · {p.name}: {len(e)} cargables "
              f"({int((e['_signo'] == 'Egreso').sum())} compras + "
              f"{int((e['_signo'] == 'Ingreso').sum())} devoluciones), "
              f"{e['Transaction Date'].min()} → {e['Transaction Date'].max()}")
    if not partes:
        raise SystemExit("⛔ No hay ningún extracto de Capital disponible.")
    u = pd.concat(partes, ignore_index=True)
    # dedup por Orden: la misma compra en dos extractos produce el MISMO Orden (se verifica).
    antes = len(u)
    u = u.drop_duplicates(subset=["Orden"], keep="first").reset_index(drop=True)
    print(f"  · unión: {antes} filas -> {len(u)} movimientos únicos por Orden")
    return u


def construir(mod):
    cob = cobros_capital()
    ext = extracto_ordenes(mod)

    # Los 80 cobros se parten en COMPRAS (Debit) y DEVOLUCIONES (Credit). Ambos generan entrada.
    cob["_signo"] = np.where(cob["_deb"].notna() & (cob["_deb"] > 0), "Egreso",
                     np.where(cob["_cre"].notna() & (cob["_cre"] > 0), "Ingreso", ""))
    cob["_usd"] = cob["_deb"].fillna(cob["_cre"])
    mov = cob[cob["_signo"] != ""].copy()
    n_eg = int((mov["_signo"] == "Egreso").sum())
    n_in = int((mov["_signo"] == "Ingreso").sum())
    print(f"\n  cobros Card {CARD_NO}: {len(cob)}  ->  {n_eg} compras (Debit) + "
          f"{n_in} devoluciones (Credit)   [ambas generan entrada]")

    # match count-aware 1:1 por (fecha, |USD|, Description, SIGNO)
    def k(fecha_iso, usd, desc, signo):
        return f"{fecha_iso}|{usd:.2f}|{str(desc).strip().upper()}|{signo}"
    ext["_k"] = [k(a, b, c, d) for a, b, c, d in zip(
        ext["Transaction Date"].astype(str).str.strip(), ext["_usd"],
        ext["Description"], ext["_signo"])]
    mov["_k"] = [k(a, b, c, d) for a, b, c, d in zip(
        mov["_td"].dt.strftime("%Y-%m-%d"), mov["_usd"], mov["_desc"], mov["_signo"])]
    pool = {}
    for _, r in ext.sort_values("Orden").iterrows():
        pool.setdefault(r["_k"], []).append(r["Orden"])

    filas, sin_extracto = [], []
    for _, r in mov.sort_values(["_td", "_signo", "_desc"]).iterrows():
        f_iso = r["_td"].strftime("%Y-%m-%d")
        base = {
            "tarjeta": TARJETA,
            "casillero": CASILLERO,
            "fecha_compra": f_iso,
            "monto_usd": round(float(r["_usd"]), 2),
            "signo": r["_signo"],
            "card_norm": CARD_NORM,
            "merchant_norm": mod._norm_merchant(r["_desc"]),
            "usd_abs": round(abs(float(r["_usd"])), 2),
            "fecha_attr": f_iso,
        }
        _que = "compra ya cobrada" if r["_signo"] == "Egreso" else "devolución YA ABONADA"
        cand = pool.get(r["_k"])
        if cand:
            base.update({
                "Orden": cand.pop(0),
                "nota": f"{_que} en bloque manual ({HOJA_COBROS})",
                "fuente": "Capital Julian + extracto",
                "attr_fuente": "extracto capital",
            })
            filas.append(base)
        else:
            base["_que"] = _que
            sin_extracto.append(base)

    # las que no se pudieron hashear -> Orden centinela + atributos (barrera 2)
    for i, base in enumerate(sin_extracto, start=1):
        _que = base.pop("_que")
        base.update({
            "Orden": f"capital_sinextracto_{i:03d}",
            "nota": f"{_que}, SIN respaldo en extracto — protegida solo por ATRIBUTOS",
            "fuente": "Capital Julian sin extracto",
            "attr_fuente": "excel cobrados (Capital Julian)",
        })
        filas.append(base)

    cols = ["Orden", "tarjeta", "casillero", "fecha_compra", "monto_usd", "signo", "nota",
            "fuente", "card_norm", "merchant_norm", "usd_abs", "fecha_attr", "attr_fuente"]
    out = pd.DataFrame(filas)[cols].sort_values(["fecha_compra", "signo", "Orden"]).reset_index(drop=True)
    return out, len(filas) - len(sin_extracto), sin_extracto


def main():
    print("=" * 78)
    print("GENERAR ENTRADAS 'capital' PARA tarjetas_cobradas.xlsx  (dry-run, 0 escrituras)")
    print("=" * 78)
    mod = _app()
    print(f"\nEXTRACTOS")
    out, n_hash, sin_ext = construir(mod)

    print(f"\nRESULTADO")
    print(f"  entradas generadas          : {len(out)}")
    print(f"    · signo Egreso  (anti-doble-COBRO): {int((out['signo']=='Egreso').sum())}")
    print(f"    · signo Ingreso (anti-doble-ABONO): {int((out['signo']=='Ingreso').sum())}")
    print(f"  · CON Orden (barrera 1)     : {n_hash}")
    print(f"  · solo ATRIBUTOS (barrera 2): {len(sin_ext)}")
    if sin_ext:
        f = sorted(x["fecha_attr"] for x in sin_ext)
        print(f"    rango: {f[0]} → {f[-1]}   USD {sum(x['usd_abs'] for x in sin_ext):,.2f}")
        for x in sin_ext:
            print(f"      {x['fecha_attr']}  {x['signo']:<8} USD {x['usd_abs']:>9,.2f}  "
                  f"{x['merchant_norm']}")
    print(f"  Orden duplicados            : {int(out['Orden'].duplicated().sum())}")
    print(f"  merchant_norm no vacío      : {int((out['merchant_norm'] != '').sum())}/{len(out)}")
    print(f"  rango fecha_attr            : {out['fecha_attr'].min()} → {out['fecha_attr'].max()}")
    print(f"  USD compras                 : {out.loc[out['signo']=='Egreso','usd_abs'].sum():,.2f}")
    print(f"  USD devoluciones            : {out.loc[out['signo']=='Ingreso','usd_abs'].sum():,.2f}")
    print("\n  DEVOLUCIONES protegidas (anti-doble-abono):")
    for _, r in out[out["signo"] == "Ingreso"].iterrows():
        _b1 = "Orden" if not str(r["Orden"]).startswith("capital_sinextracto") else "atributos"
        print(f"    {r['fecha_attr']}  USD {r['usd_abs']:>9,.2f}  {r['merchant_norm']:<24} "
              f"[{_b1}]  {r['Orden']}")

    if "--out" in sys.argv:
        dest = pathlib.Path(sys.argv[sys.argv.index("--out") + 1])
        with pd.ExcelWriter(dest, engine="openpyxl") as w:
            out.to_excel(w, sheet_name="capital_nuevas", index=False)
        print(f"\n  💾 escrito SOLO local: {dest}  (Dropbox NO se tocó)")
    else:
        print("\n  (dry-run: nada escrito. Usa --out RUTA.xlsx para volcar a un archivo local.)")
    return out


if __name__ == "__main__":
    main()
