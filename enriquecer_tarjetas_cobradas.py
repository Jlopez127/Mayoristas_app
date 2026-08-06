#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enriquece tarjetas_cobradas.xlsx con los ATRIBUTOS de cada cobro, para habilitar la
SEGUNDA BARRERA anti-recobro (independiente del hash) de mayoristas_streamlit_app.py.

POR QUÉ
-------
El anti-recobro dependía SOLO del 'Orden' (hash). Robinhood re-expide la misma transacción
con la FECHA corrida (verificado 2026-08-06: 6 cobros de mayo cambiaron de día entre dos
descargas del CSV) -> el hash cambia -> la entrada de la lista queda HUÉRFANA -> la
transacción ya cobrada vuelve a entrar. Con los atributos guardados, procesar_* puede
reconocerla aunque su Orden haya cambiado.

QUÉ HACE (y qué NO)
-------------------
  · NO regenera la lista: parte de la lista VIGENTE y CONSERVA sus 'Orden' tal cual
    (regla del usuario). Solo AÑADE columnas.
  · Resuelve los atributos de cada Orden contra los extractos disponibles, replicando el
    cálculo de Orden de cada procesador (mismo hash/clave que mayoristas_streamlit_app.py).
  · Para los Orden que ningún extracto genera (huérfanos / fuera del rango de los extractos)
    cae al Excel de cobrados congelado por (tarjeta, fecha, |USD|) y toma de ahí la
    descripción; si tampoco está, deja el merchant VACÍO.
  · Un cobro SIN merchant NO participa de la barrera por atributos (el código lo ignora):
    sin merchant, casillero+USD+fecha±3d generaría falsos positivos que bloquearían cobros
    legítimos. Prefiere cobrar de más que dejar de cobrar mal.

SALIDA
------
Escribe un xlsx LOCAL con las 3 hojas originales; 'cobradas' gana 4 columnas:
    card_norm | merchant_norm | usd_abs | attr_fuente
NO sube nada a Dropbox (eso es un paso manual posterior).

Uso:
  python3 enriquecer_tarjetas_cobradas.py <lista_actual.xlsx> <salida.xlsx>
"""
import hashlib
import sys
import pandas as pd

# ── Fuentes de atributos (los mismos extractos con que se generó la lista + los frescos) ──
EXTRACTOS_AMEX = [
    "/Users/julianlopez/Library/CloudStorage/OneDrive-Personal/Encargomio/Dash_mayoristas/activity (3).xlsx",
    "/Users/julianlopez/Library/CloudStorage/OneDrive-Personal/Encargomio/Dash_mayoristas/activity (4).xlsx",
    "/Users/julianlopez/Downloads/activity (1).xlsx",
    "/Users/julianlopez/Downloads/activity (2).xlsx",
    "/Users/julianlopez/Downloads/activity (5).xlsx",
    "/Users/julianlopez/Downloads/activity (6).xlsx",
]
EXTRACTOS_RAKUTEN = [
    "/Users/julianlopez/Library/CloudStorage/OneDrive-Personal/Encargomio/Dash_mayoristas/Rakuten_Activity_All.csv",
    "/Users/julianlopez/Downloads/Rakuten_Activity_All (2).csv",
]
EXTRACTOS_ROBINHOOD = [
    "/Users/julianlopez/Downloads/53fa9510-b899-438c-9c3c-7ea05980d12a.csv",
    "/Users/julianlopez/Downloads/51208e5c-6cfc-4af7-8985-7bd907a7c9ff.csv",
    "/Users/julianlopez/Downloads/02de847e-a99f-465f-84d1-b154aebdfd5d.csv",
]
COBRADOS_XLSX = "/Users/julianlopez/Downloads/20260710Cobro tarjetas.xlsx"

AMEX_CARD_MAP = {
    "PAULA HERRERA": "11591", "JUAN P CORREAL": "1444", "JULIAN SANCHEZ": "13608",
    "K LOPEZ VELANDIA": "1444", "KELLY P LOPEZVELANDIA": "1444",
}
ROBINHOOD_CARDMAP = {"Juan Pablo Correal Perez": "1444", "Maria Moises": "1444"}


def _norm_merchant(s) -> str:
    return " ".join(str(s).split()).upper()


def _norm_cm(s) -> str:
    return " ".join(str(s).strip().upper().split())


def _sha(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:12]


def _leer(path, lector, etiqueta):
    try:
        return lector(path)
    except Exception as e:
        print(f"  ⚠️  no se pudo leer {etiqueta} {path}: {e}")
        return None


# ══════════════════════════════════════════════════════════════════════════════
# Índices {Orden -> atributos} por tarjeta (replican el cálculo de Orden de la app)
# ══════════════════════════════════════════════════════════════════════════════
def idx_amex() -> dict:
    """Orden = 'amex_<Reference>'. Atributos: Description (merchant) + Card Member."""
    idx = {}
    for p in EXTRACTOS_AMEX:
        df = _leer(p, lambda x: pd.read_excel(x, sheet_name="Transaction Details", header=6), "Amex")
        if df is None:
            continue
        df.columns = [str(c).strip() for c in df.columns]
        if any(c not in df.columns for c in ("Reference", "Card Member", "Date", "Amount")):
            print(f"  ⚠️  {p}: sin las columnas esperadas (header distinto) — se omite")
            continue
        df["_cas"] = df["Card Member"].map(_norm_cm).map(AMEX_CARD_MAP)
        df = df[df["_cas"].notna()]
        for _, r in df.iterrows():
            ref = str(r["Reference"]).strip().lstrip("'")
            if not ref.isdigit():
                continue
            o = "amex_" + ref
            if o in idx:
                continue
            f = pd.to_datetime(r["Date"], format="%m/%d/%Y", errors="coerce")
            idx[o] = {
                "card_norm": _norm_cm(r["Card Member"]),
                "merchant_norm": _norm_merchant(r.get("Description", "")),
                "usd_abs": round(abs(float(pd.to_numeric(r["Amount"], errors="coerce"))), 2),
                "fecha_attr": None if pd.isna(f) else f.strftime("%Y-%m-%d"),
                "attr_fuente": "extracto amex",
            }
    return idx


def idx_rakuten() -> dict:
    """Orden = 'rakuten_<sha1-12 de Date|Amount|Merchant|seq>' sobre las filas
    TRANSACTION/REFUND con monto != 0 (idéntico al generador original)."""
    idx = {}
    for p in EXTRACTOS_RAKUTEN:
        df = _leer(p, pd.read_csv, "Rakuten")
        if df is None:
            continue
        df.columns = [str(c).strip() for c in df.columns]
        t = df["Type"].astype(str).str.strip().str.upper()
        df = df[t.isin({"TRANSACTION", "REFUND"})].copy()
        amt = df["Amount"].astype(str).str.replace("$", "", regex=False).str.replace(",", "", regex=False)
        amt = pd.to_numeric(amt.str.strip("()"), errors="coerce")
        df = df[amt.notna() & (amt != 0)].copy()
        df["_amt"] = amt[amt.notna() & (amt != 0)]
        clave = (df["Date"].astype(str) + "|" + df["Amount"].astype(str) + "|" + df["Merchant"].astype(str))
        seq = clave.groupby(clave).cumcount().astype(str)
        df["_orden"] = "rakuten_" + (clave + "|" + seq).map(_sha)
        for _, r in df.iterrows():
            o = r["_orden"]
            if o in idx:
                continue
            f = pd.to_datetime(r["Date"], format="%Y/%m/%d, %H:%M:%S", errors="coerce")
            m = _norm_merchant(r["Merchant"])
            if m.startswith("REFUND FROM "):
                m = m[len("REFUND FROM "):].strip()
            idx[o] = {
                "card_norm": "MARIA MOISES",
                "merchant_norm": m,
                "usd_abs": round(abs(float(r["_amt"])), 2),
                "fecha_attr": None if pd.isna(f) else f.strftime("%Y-%m-%d"),
                "attr_fuente": "extracto rakuten",
            }
    return idx


def idx_robinhood() -> dict:
    """Orden = 'robinhood_<sha1-12 de Date|Amount|Merchant|seq>' con seq en orden canónico
    (clave, cargable, hora) sobre TODO el set 1444 — idéntico a _robinhood_clave_y_seq."""
    idx = {}
    for p in EXTRACTOS_ROBINHOOD:
        df = _leer(p, pd.read_csv, "Robinhood")
        if df is None:
            continue
        df.columns = [str(c).strip() for c in df.columns]
        df = df[df["Cardholder"].astype(str).str.strip().isin(ROBINHOOD_CARDMAP)].copy()
        clave = (df["Date"].astype(str) + "|" + df["Amount"].astype(str) + "|" + df["Merchant"].astype(str))
        hora = pd.to_datetime(df["Time"].astype(str).str.strip(), format="%I:%M %p", errors="coerce")
        cargable = (df["Status"].astype(str).str.strip().str.upper().eq("POSTED")
                    & df["Type"].astype(str).str.strip().str.upper().isin({"PURCHASE", "REFUND"}))
        canon = pd.DataFrame({"_k": clave, "_c": (~cargable).astype(int), "_h": hora}).sort_values(
            ["_k", "_c", "_h"], kind="mergesort", na_position="last")
        seq = canon.groupby("_k").cumcount().reindex(df.index).astype(str)
        df["_orden"] = "robinhood_" + (clave + "|" + seq).map(_sha)
        for _, r in df.iterrows():
            o = r["_orden"]
            if o in idx:
                continue
            f = pd.to_datetime(r["Date"], format="%Y-%m-%d", errors="coerce")
            m = _norm_merchant(r["Merchant"])
            for pre in ("REFUND: ", "REFUND FROM "):
                if m.startswith(pre):
                    m = m[len(pre):].strip()
            idx[o] = {
                "card_norm": _norm_cm(r["Cardholder"]),
                "merchant_norm": m,
                "usd_abs": round(abs(float(pd.to_numeric(r["Amount"], errors="coerce"))), 2),
                "fecha_attr": None if pd.isna(f) else f.strftime("%Y-%m-%d"),
                "attr_fuente": "extracto robinhood",
            }
    return idx


# ══════════════════════════════════════════════════════════════════════════════
# Fallback: Excel de cobrados congelado -> descripción por (tarjeta, fecha, |USD|)
# ══════════════════════════════════════════════════════════════════════════════
def idx_excel_cobrados() -> dict:
    """{(tarjeta, fecha_iso, usd_abs): [merchant_norm, ...]} desde el Excel congelado."""
    out = {}
    try:
        xl = pd.ExcelFile(COBRADOS_XLSX)
    except Exception as e:
        print(f"  ⚠️  no se pudo abrir el Excel de cobrados: {e}")
        return out
    # Amex: hojas 'Amex *' -> columnas Date / Description / Amount / Card Member
    for hoja in xl.sheet_names:
        h = hoja.strip().lower()
        try:
            d = pd.read_excel(xl, sheet_name=hoja, header=None)
        except Exception:
            continue
        if h.startswith("amex"):
            for _, r in d.iterrows():
                f = pd.to_datetime(r.get(0), errors="coerce")
                a = pd.to_numeric(r.get(3), errors="coerce")
                if pd.isna(f) or pd.isna(a):
                    continue
                out.setdefault(("amex", f.strftime("%Y-%m-%d"), round(abs(float(a)), 2)),
                               []).append(_norm_merchant(r.get(1, "")))
        elif h.startswith("robinhood"):
            for _, r in d.iterrows():
                f = pd.to_datetime(r.get(0), errors="coerce")
                a = pd.to_numeric(r.get(3), errors="coerce")
                if pd.isna(f) or pd.isna(a):
                    continue
                out.setdefault(("robinhood", f.strftime("%Y-%m-%d"), round(abs(float(a)), 2)),
                               []).append(_norm_merchant(r.get(4, "")))
        elif h.startswith("rakuten"):
            # columnas CORRIDAS: Date=fecha, Amount=hora, Type=monto, Merchant=tipo, Category=merchant
            for _, r in d.iterrows():
                f = pd.to_datetime(r.get(0), errors="coerce")
                a = pd.to_numeric(str(r.get(2)).replace("$", "").replace(",", ""), errors="coerce")
                if pd.isna(f) or pd.isna(a):
                    continue
                out.setdefault(("rakuten", f.strftime("%Y-%m-%d"), round(abs(float(a)), 2)),
                               []).append(_norm_merchant(r.get(4, "")))
    return out


def main():
    entrada = sys.argv[1] if len(sys.argv) > 1 else "tarjetas_cobradas.xlsx"
    salida = sys.argv[2] if len(sys.argv) > 2 else "tarjetas_cobradas_enriquecida.xlsx"

    xl = pd.ExcelFile(entrada)
    cob = pd.read_excel(xl, sheet_name="cobradas")
    n0 = len(cob)
    print(f"lista de entrada: {entrada} — {n0} cobros")
    if "Orden" not in cob.columns:
        raise SystemExit("❌ la hoja 'cobradas' no tiene columna 'Orden'")

    print("\nconstruyendo índices de atributos desde los extractos…")
    idx = {}
    idx.update(idx_amex())
    idx.update(idx_rakuten())
    idx.update(idx_robinhood())
    print(f"  Orden resueltos por extracto: {len(idx)}")
    fb = idx_excel_cobrados()
    print(f"  claves del Excel de cobrados (fallback): {len(fb)}")

    filas = []
    n_ext = n_fb = n_vacio = 0
    for _, r in cob.iterrows():
        o = str(r["Orden"]).strip()
        a = idx.get(o)
        if a is not None:
            n_ext += 1
        else:
            # fallback por (tarjeta, fecha_compra, monto_usd) contra el Excel congelado
            tj = str(r.get("tarjeta", "")).strip().lower()
            f = pd.to_datetime(r.get("fecha_compra"), format="mixed", errors="coerce")
            usd = round(abs(float(r.get("monto_usd", 0) or 0)), 2)
            cand = fb.get((tj, f.strftime("%Y-%m-%d") if pd.notna(f) else "", usd), [])
            cand = [c for c in cand if c and c != "NAN"]
            if len(cand) == 1:
                a = {"card_norm": "", "merchant_norm": cand[0], "usd_abs": usd,
                     "fecha_attr": f.strftime("%Y-%m-%d") if pd.notna(f) else None,
                     "attr_fuente": "excel cobrados"}
                n_fb += 1
            else:
                a = {"card_norm": "", "merchant_norm": "", "usd_abs": usd,
                     "fecha_attr": f.strftime("%Y-%m-%d") if pd.notna(f) else None,
                     "attr_fuente": f"sin merchant ({len(cand)} candidatas)"}
                n_vacio += 1
        filas.append(a)

    attr = pd.DataFrame(filas, index=cob.index)
    for c in ("card_norm", "merchant_norm", "usd_abs", "fecha_attr", "attr_fuente"):
        cob[c] = attr[c]

    print(f"\natributos resueltos:")
    print(f"  desde extracto ....... {n_ext}")
    print(f"  desde Excel cobrados . {n_fb}")
    print(f"  SIN merchant ......... {n_vacio}  (no participan de la barrera por atributos)")
    con_m = int((cob["merchant_norm"].astype(str).str.strip() != "").sum())
    print(f"  CON merchant (barrera activa): {con_m}/{n0}")
    print("\npor tarjeta:")
    for t, g in cob.groupby("tarjeta"):
        cm = int((g["merchant_norm"].astype(str).str.strip() != "").sum())
        print(f"  {t:<10} {len(g):>5} cobros | con merchant {cm:>5} ({cm/len(g)*100:.1f}%)")

    hojas = {"cobradas": cob}
    for h in xl.sheet_names:
        if h != "cobradas":
            hojas[h] = pd.read_excel(xl, sheet_name=h)
    with pd.ExcelWriter(salida, engine="openpyxl") as w:
        for h, d in hojas.items():
            d.to_excel(w, sheet_name=h, index=False)
    print(f"\n✅ escrito (LOCAL, no Dropbox): {salida}")
    print(f"   hojas: {list(hojas)} | 'cobradas' {len(cob)} filas x {len(cob.columns)} columnas")
    assert len(cob) == n0, "❌ se perdieron cobros al enriquecer"
    assert set(cob["Orden"]) == set(pd.read_excel(xl, sheet_name='cobradas')["Orden"]), \
        "❌ cambiaron los Orden"
    print("   ✔ conteo y Orden idénticos a la lista de entrada")


if __name__ == "__main__":
    main()
