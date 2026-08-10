#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PASO 1 — Sube a Dropbox la lista de exclusión `tarjetas_cobradas.xlsx` con las entradas de
TARJETA CAPITAL anexadas.

QUÉ HACE
--------
1. Descarga la lista viva y crea un RESPALDO con WriteMode.add (nunca sobrescribe).
2. Anexa a la hoja "cobradas" las 80 entradas 'capital' de generar_capital_cobradas.py
   (66 signo=Egreso anti-doble-cobro + 14 signo=Ingreso anti-doble-abono).
   Las filas amex/rakuten/robinhood NO se tocan: se copian tal cual, en el mismo orden.
   La columna nueva 'signo' queda VACÍA para ellas -> comodín -> comportamiento idéntico.
3. Copia las demás hojas ("pendientes_rematch", "revision") verbatim.
4. Sube y RE-LEE de Dropbox para validar.

  sin argumentos  -> dry-run (0 escrituras)
  --escribir      -> sube a Dropbox
"""
import sys, os, io, warnings
warnings.filterwarnings("ignore")
import pandas as pd
import dropbox
from pathlib import PurePosixPath

ESCRIBIR = "--escribir" in sys.argv
FILENAME = "tarjetas_cobradas.xlsx"
N_PREVIAS_ESPERADAS = 2221
N_CAPITAL_ESPERADAS = 80


def main():
    import harness
    mod = harness.cargar_app()
    import generar_capital_cobradas as gcc
    SEP = "=" * 88
    def banner(t): print(f"\n{SEP}\n{t}\n{SEP}")

    print(f"MODO: {'🔴 ESCRITURA REAL' if ESCRIBIR else '🟢 DRY-RUN (0 escrituras)'}")
    cfg = mod.st.secrets["dropbox"]
    remote = str(PurePosixPath(cfg["remote_path"]).parent / FILENAME)

    # ── 1) lista viva ────────────────────────────────────────────────────────
    banner("1) LISTA VIVA")
    md = mod.dbx.files_get_metadata(remote)
    print(f"  {remote}")
    print(f"  rev={md.rev}  modificado={md.server_modified}  size={md.size:,}")
    _, res = mod.dbx.files_download(remote)
    contenido_previo = res.content
    xls = pd.ExcelFile(io.BytesIO(contenido_previo))
    hojas = {h: pd.read_excel(xls, sheet_name=h) for h in xls.sheet_names}
    print(f"  hojas: {xls.sheet_names}")
    prev = hojas["cobradas"]
    print(f"  cobradas: {len(prev)} filas · {prev['tarjeta'].value_counts().to_dict()}")
    if len(prev) != N_PREVIAS_ESPERADAS:
        raise SystemExit(f"⛔ ABORTA: se esperaban {N_PREVIAS_ESPERADAS} filas previas y hay {len(prev)}.")
    if (prev["tarjeta"].astype(str).str.strip().str.lower() == "capital").any():
        raise SystemExit("⛔ ABORTA: la lista YA trae entradas 'capital' (¿se subió antes?).")

    # ── 2) entradas capital ──────────────────────────────────────────────────
    banner("2) ENTRADAS CAPITAL")
    cap, n_hash, sin_ext = gcc.construir(mod)
    harness.clear_msgs()
    print(f"  generadas: {len(cap)}  "
          f"({int((cap['signo']=='Egreso').sum())} Egreso + {int((cap['signo']=='Ingreso').sum())} Ingreso)")
    print(f"  con Orden (barrera 1): {n_hash} · solo atributos (barrera 2): {len(sin_ext)}")
    if len(cap) != N_CAPITAL_ESPERADAS:
        raise SystemExit(f"⛔ ABORTA: se esperaban {N_CAPITAL_ESPERADAS} entradas y hay {len(cap)}.")
    choque = set(cap["Orden"].astype(str)) & set(prev["Orden"].astype(str))
    print(f"  Orden que chocan con los previos: {len(choque)} {'✔' if not choque else '🚨 '+str(choque)}")
    if choque:
        raise SystemExit("⛔ ABORTA: colisión de Orden con la lista existente.")

    # ── 3) armar la hoja nueva ───────────────────────────────────────────────
    banner("3) HOJA 'cobradas' NUEVA")
    # 'signo' vacío para las previas (comodín). Se respeta el orden original de columnas y filas.
    base = prev.copy()
    if "signo" not in base.columns:
        base["signo"] = ""
    cols = list(base.columns)
    for c in cols:
        if c not in cap.columns:
            cap[c] = ""
    nueva = pd.concat([base, cap[cols]], ignore_index=True)
    print(f"  {len(prev)} + {len(cap)} = {len(nueva)} filas")
    print(f"  desglose: {nueva['tarjeta'].value_counts().to_dict()}")

    # las previas deben quedar IDÉNTICAS (salvo la columna 'signo' nueva, vacía)
    _a = prev.reset_index(drop=True)
    _b = nueva.iloc[:len(prev)].reset_index(drop=True)
    difs = [c for c in prev.columns if not _a[c].astype(str).equals(_b[c].astype(str))]
    print(f"  previas idénticas en las {len(prev.columns)} columnas originales: "
          f"{'✔ SÍ' if not difs else '🚨 difieren en ' + str(difs)}")
    if difs:
        raise SystemExit("⛔ ABORTA: las filas previas cambiaron.")
    print(f"  'signo' vacío en las previas: "
          f"{int((_b['signo'].astype(str).str.strip() == '').sum())}/{len(prev)} ✔")

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        nueva.to_excel(w, sheet_name="cobradas", index=False)
        for h, d in hojas.items():
            if h != "cobradas":
                d.to_excel(w, sheet_name=h[:31], index=False)
    buf.seek(0)
    data = buf.read()
    print(f"  excel en memoria: {len(data):,} bytes · hojas {xls.sheet_names}")

    if not ESCRIBIR:
        banner("DRY-RUN TERMINADO — 0 ESCRITURAS")
        print("  Para escribir: python3 subir_lista_capital.py --escribir")
        return

    # ── 4) respaldo + subida ─────────────────────────────────────────────────
    banner("4) 🛟 RESPALDO (WriteMode.add) + SUBIDA")
    md_pre = mod.dbx.files_get_metadata(remote)
    if md_pre.rev != md.rev:
        raise SystemExit(f"⛔ ABORTA SIN ESCRIBIR: la lista se movió (rev {md_pre.rev}).")
    ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    carpeta = str(PurePosixPath(remote).parent)
    backup_path = f"{carpeta}/{PurePosixPath(remote).stem}_backup_{ts}.xlsx"
    mod.dbx.files_upload(contenido_previo, backup_path, mode=dropbox.files.WriteMode.add)
    print(f"  🛟 respaldo creado: {backup_path} ({len(contenido_previo):,} bytes)")
    mod.dbx.files_upload(data, remote, mode=dropbox.files.WriteMode.overwrite)
    print("  ✅ lista subida")

    # ── 5) re-lectura y validación ───────────────────────────────────────────
    banner("5) RE-LECTURA DE DROPBOX Y VALIDACIÓN")
    md2 = mod.dbx.files_get_metadata(remote)
    print(f"  rev NUEVA = {md2.rev}   modificado = {md2.server_modified}   size = {md2.size:,}")
    _, r2 = mod.dbx.files_download(remote)
    x2 = pd.ExcelFile(io.BytesIO(r2.content))
    rel = pd.read_excel(x2, sheet_name="cobradas")
    ok = True
    def chk(n, c, det=""):
        nonlocal ok
        print(f"  {'✔' if c else '🚨'} {n:<52} {det}")
        ok = ok and bool(c)
    chk("total de cobros", len(rel) == len(nueva), f"{len(rel)}")
    vc = rel["tarjeta"].astype(str).str.strip().str.lower().value_counts().to_dict()
    for t, n in (("amex", 1679), ("rakuten", 355), ("robinhood", 187), ("capital", 80)):
        chk(f"  {t}", vc.get(t) == n, f"{vc.get(t)}")
    _r = rel.iloc[:len(prev)].reset_index(drop=True)
    d2 = [c for c in prev.columns if not prev.reset_index(drop=True)[c].astype(str).equals(_r[c].astype(str))]
    chk("previas byte-idénticas (todas las columnas originales)", not d2, f"{d2 or 'sin diferencias'}")
    cp = rel[rel["tarjeta"].astype(str).str.strip().str.lower() == "capital"]
    chk("capital: signo poblado", int((cp["signo"].astype(str).str.strip() != "").sum()) == len(cp),
        f"{int((cp['signo'].astype(str).str.strip()!='').sum())}/{len(cp)}")
    chk("capital: Egreso / Ingreso",
        int((cp["signo"] == "Egreso").sum()) == 66 and int((cp["signo"] == "Ingreso").sum()) == 14,
        f"{int((cp['signo']=='Egreso').sum())} / {int((cp['signo']=='Ingreso').sum())}")
    for c in ("merchant_norm", "usd_abs", "fecha_attr", "casillero"):
        chk(f"capital: {c} poblado",
            int(cp[c].astype(str).str.strip().replace("nan", "").ne("").sum()) == len(cp),
            f"{int(cp[c].astype(str).str.strip().replace('nan','').ne('').sum())}/{len(cp)}")
    chk("previas SIN signo (comodín)",
        int(rel.iloc[:len(prev)]["signo"].astype(str).str.strip().replace("nan", "").ne("").sum()) == 0)
    chk("0 Orden duplicados", not rel["Orden"].astype(str).duplicated().any(),
        f"{int(rel['Orden'].astype(str).duplicated().sum())}")
    chk("hojas conservadas", list(x2.sheet_names) == list(xls.sheet_names), f"{list(x2.sheet_names)}")
    for h in xls.sheet_names:
        if h == "cobradas":
            continue
        a, b = hojas[h], pd.read_excel(x2, sheet_name=h)
        chk(f"verbatim: {h}", len(a) == len(b) and a.astype(str).equals(b.astype(str)), f"{len(a)} filas")
    # el app la lee bien
    mod.cargar_tarjetas_cobradas.clear() if hasattr(mod.cargar_tarjetas_cobradas, "clear") else None
    co, pe, cd = mod.cargar_tarjetas_cobradas()
    harness.clear_msgs()
    chk("cargar_tarjetas_cobradas() la lee", len(cd) == len(nueva), f"{len(cd)} cobros / {len(co)} Orden")
    print(f"\n  {'✅ LISTA SUBIDA Y VERIFICADA' if ok else '🚨 REVISAR'}")
    print(f"  rev nueva: {md2.rev}\n  rollback:  {backup_path}")


if __name__ == "__main__":
    sys.path.insert(0, os.environ.get("HARNESS_DIR", "."))
    main()
