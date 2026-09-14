#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Reasigna 3 cargos de MIGRACIÓN AMEX entre Julian (13608) y Paula (11591). 2026-09-14.

    migracionamex_000016617026178304  BT*GOWHOLESALE   USD   377,29   13608 -> 11591
    migracionamex_000016559751147304  AMAZON.COM       USD   653,97   11591 -> 13608
    migracionamex_000016628351341300  PAYPAL *EBAY     USD   647,68   11591 -> 13608

Decisión del usuario (2026-09-14): la nota del archivo fuente los asignó mal.

⚠️ NO SE PUEDEN MOVER DE HOJA, HAY QUE NEUTRALIZAR + RECREAR
------------------------------------------------------------
El GUARD A compara **por hoja**, no sobre el conjunto global: sacar un Orden de una hoja cuenta
como pérdida aunque aparezca en otra. Verificado — al mover las 3 reporta
«11591: se perderían 2 Orden» y «13608: se perderían 1 Orden».

Por eso, en cada hoja ORIGEN la fila se **neutraliza** (Monto = 0 + nota que dice a dónde se
fue) y en la hoja DESTINO se crea una fila nueva con un Orden derivado `<orden>_<casillero>`,
que no colisiona con el original. Mismo patrón que COMPRAS_TC_PROPIA: "retirar un cargo sin
borrarlo".

El efecto es SUMA CERO entre los dos casilleros: lo que baja de uno sube en el otro.

Como estas filas no vienen de ningún extracto, ningún módulo las regenera. El prefijo
`migracionamex_` está en TARJETA_ORDEN_RE, así que la capa B protege también las nuevas.

  sin argumentos  -> dry-run (0 escrituras)
  --escribir      -> escribe a Dropbox y refresca la copia de OneDrive
"""
import sys, os, io, shutil, warnings
warnings.filterwarnings("ignore")
import pandas as pd

ESCRIBIR = "--escribir" in sys.argv

USUARIOS = {"13608": "Julian Sanchez", "11591": "Paula Herrera"}
# Orden -> (casillero origen, casillero destino, USD esperado)
MOV = {
    "migracionamex_000016617026178304": ("13608", "11591", 377.29),
    "migracionamex_000016559751147304": ("11591", "13608", 653.97),
    "migracionamex_000016628351341300": ("11591", "13608", 647.68),
}
REV_ESPERADA = "0165b72688f0a5100000002f34b3f21"
PREVIAS_MIG = {"13608": 1, "11591": 14}

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
    SEP = "=" * 94
    def banner(t): print(f"\n{SEP}\n{t}\n{SEP}")
    ok = True
    def chk(n, c, det=""):
        nonlocal ok
        print(f"  {'✔' if c else '🚨'} {n:<64} {det}")
        ok = ok and bool(c)

    for fn in ("procesar_egresos", "procesar_amex", "procesar_rakuten", "procesar_robinhood",
               "procesar_capital", "procesar_usbank", "procesar_intuit", "procesar_discover",
               "agregar_incentivo_amex"):
        def _boom(*a, _n=fn, **k):
            raise AssertionError(f"{_n} fue llamada — este script solo reasigna 3 filas")
        setattr(mod, fn, _boom)

    print(f"MODO: {'🔴 ESCRITURA REAL' if ESCRIBIR else '🟢 DRY-RUN (0 escrituras)'}")

    banner("1) POR QUÉ NEUTRALIZAR + RECREAR, Y NO MOVER")
    print("  El guard A compara POR HOJA: sacar un Orden de una hoja cuenta como pérdida aunque")
    print("  la fila aparezca en otra. Verificado: mover las 3 da «11591: se perderían 2 Orden»")
    print("  y «13608: se perderían 1 Orden». Por eso origen -> Monto 0, destino -> fila nueva.")
    for o in MOV:
        chk(f"{o[-18:]} no es removible para el guard", mod._orden_removible(o) is False)
    chk("migracionamex_ protegido por la capa B", "migracionamex_" in mod.TARJETA_ORDEN_RE)

    banner("2) HISTÓRICO VIVO FRESCO")
    cfg = mod.st.secrets["dropbox"]
    md = mod.dbx.files_get_metadata(cfg["remote_path"])
    _, res = mod.dbx.files_download(cfg["remote_path"])
    vivo = pd.read_excel(io.BytesIO(res.content), sheet_name=None)
    HOJA = {c: next(h for h in vivo if h.split(" - ")[0].strip() == c) for c in USUARIOS}
    s0, o0 = {}, {}
    print(f"  rev={md.rev}  modificado={md.server_modified}  size={md.size:,}")
    for c, h in HOJA.items():
        s0[c] = saldo(vivo[h]); o0[c] = vivo[h]["Orden"].astype(str).str.strip()
        n_mig = int(o0[c].str.startswith("migracionamex_").sum())
        print(f"  {h:<32} {len(vivo[h]):>6} filas · saldo COP {s0[c]:>16,.2f} · migracionamex_ {n_mig}")
        chk(f"{c}: {PREVIAS_MIG[c]} filas migracionamex_ previas", n_mig == PREVIAS_MIG[c], f"{n_mig}")
    if md.rev != REV_ESPERADA:
        print(f"  ⚠️  el histórico se movió (esperaba {REV_ESPERADA})")
        if ESCRIBIR:
            raise SystemExit("⛔ ABORTA: rehacer el dry-run con el histórico fresco.")
    else:
        print("  ✔ rev idéntica a la del dry-run")

    banner("3) LAS 3 FILAS")
    origen = {}
    for o, (org, dst, usd_esp) in MOV.items():
        d = vivo[HOJA[org]]
        m = d["Orden"].astype(str).str.strip() == o
        chk(f"{o[-18:]} existe 1 vez en {org}", int(m.sum()) == 1, f"{int(m.sum())}")
        if int(m.sum()) != 1:
            continue
        r = d[m].iloc[0]
        usd = float(pd.to_numeric(r["Monto"])) / float(pd.to_numeric(r["TRM"]))
        chk(f"  USD {usd_esp:,.2f}", abs(usd - usd_esp) < 0.02, f"{usd:,.2f}")
        chk(f"  destino {dst} NO tiene ya ese Orden derivado",
            not (vivo[HOJA[dst]]["Orden"].astype(str).str.strip() == f"{o}_{dst}").any())
        origen[o] = r
        print(f"     {org} → {dst} · COP {pd.to_numeric(r['Monto']):>12,.2f} · "
              f"{str(r['Nombre del producto'])[:56]}")
    if not ok:
        raise SystemExit("⛔ ABORTA: las filas no son las esperadas.")

    banner("4) NEUTRALIZAR EN ORIGEN + CREAR EN DESTINO")
    hoy = pd.Timestamp.today().strftime("%Y-%m-%d")
    historico = {k: mod.asegurar_columnas_historico(v.copy()) for k, v in vivo.items()}
    for o, (org, dst, _u) in MOV.items():
        r = origen[o]
        # origen: Monto 0 + nota
        d = historico[HOJA[org]]
        m = d["Orden"].astype(str).str.strip() == o
        base = str(r["Nombre del producto"]).split(" — ")[0]
        d.loc[m, "Monto"] = 0
        d.loc[m, "Nombre del producto"] = (
            f"{base} — REASIGNADO a {dst} el {hoy} (no era de {USUARIOS[org]}; "
            f"Monto=0 en vez de borrar: el guard A no deja perder el Orden)")
        d.loc[m, "Fecha de Carga"] = hoy
        # destino: fila nueva con Orden derivado
        nueva = r.to_dict()
        nueva["Orden"] = f"{o}_{dst}"
        nueva["Casillero"] = int(dst)
        nueva["Usuario"] = USUARIOS[dst]
        nueva["Fecha de Carga"] = pd.Timestamp(hoy)
        nueva["Nombre del producto"] = f"{base} — reasignado desde {org} el {hoy}"
        historico[HOJA[dst]] = pd.concat(
            [historico[HOJA[dst]], mod.asegurar_columnas_historico(pd.DataFrame([nueva]))],
            ignore_index=True)
        print(f"  {o[-18:]}  {org} → 0 · {dst} + COP {pd.to_numeric(r['Monto']):,.2f}")
    for c, h in HOJA.items():
        u, cc = usuario_de_totales(vivo[h]), cas_de_totales(vivo[h], c)
        historico[h] = mod.recalcular_totales_diarios(historico[h], usuario=u, cas=cc)

    banner("5) INVARIANTES")
    for c, h in HOJA.items():
        o1 = historico[h]["Orden"].astype(str).str.strip()
        entran = sum(1 for o, (org, dst, _u) in MOV.items() if dst == c)
        chk(f"{c}: migracionamex_ = {PREVIAS_MIG[c]} + {entran}",
            int(o1.str.startswith("migracionamex_").sum()) == PREVIAS_MIG[c] + entran,
            f"{int(o1.str.startswith('migracionamex_').sum())}")
        chk(f"{c}: 0 Orden duplicados", not o1[o1.str.startswith("migracionamex_")].duplicated().any())
        vac = {"", "nan", "none", "nat"}
        perd = {y for y in o0[c] if y.lower() not in vac} - {y for y in o1 if y.lower() not in vac}
        chk(f"{c}: 0 Orden previos perdidos", not perd, f"{len(perd)}")
    for o, (org, dst, _u) in MOV.items():
        vo = float(pd.to_numeric(historico[HOJA[org]].loc[
            historico[HOJA[org]]["Orden"].astype(str).str.strip() == o, "Monto"]).iloc[0])
        vd = float(pd.to_numeric(historico[HOJA[dst]].loc[
            historico[HOJA[dst]]["Orden"].astype(str).str.strip() == f"{o}_{dst}", "Monto"]).iloc[0])
        chk(f"{o[-18:]}: origen 0 · destino {pd.to_numeric(origen[o]['Monto']):,.2f}",
            vo == 0 and abs(vd - float(pd.to_numeric(origen[o]["Monto"]))) < 0.01,
            f"{vo:,.2f} / {vd:,.2f}")
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
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        for hh, dfh in historico.items():
            w.book.create_sheet(hh[:31])
            dfh.to_excel(w, sheet_name=hh[:31], index=False)
    buf.seek(0); data = buf.read()
    print(f"  {len(data):,} bytes | {len(historico)} hojas")
    harness.clear_msgs()
    try:
        mod.guard_frescura_historico(historico)
        print("  ✅ GUARD A PASA (0 pérdidas)")
    except harness._Stop:
        for n, t in harness.MENSAJES:
            print(f"  [{n}] {t[:400]}")
        raise SystemExit("⛔ GUARD A BLOQUEÓ")

    banner("6) SALDOS — el intercambio es SUMA CERO")
    s1 = {}
    tot = 0
    for c, h in HOJA.items():
        s1[c] = saldo(historico[h]); tot += s1[c] - s0[c]
        print(f"  {h:<32} COP {s0[c]:>16,.2f} → {s1[c]:>16,.2f}   Δ {s1[c]-s0[c]:>+14,.2f}")
    chk("la suma de los dos deltas es 0", abs(tot) < 0.02, f"{tot:,.2f}")

    if not ESCRIBIR:
        banner("DRY-RUN TERMINADO — 0 ESCRITURAS")
        print("  Para escribir: python3 reasignar_migracion_amex_20260914.py --escribir")
        return

    banner("7) SUBIDA A DROPBOX")
    if mod.dbx.files_get_metadata(cfg["remote_path"]).rev != REV_ESPERADA:
        raise SystemExit("⛔ ABORTA SIN ESCRIBIR: el histórico se movió.")
    harness.clear_msgs()
    mod.upload_to_dropbox(data)
    backup = None
    for n, t in harness.MENSAJES:
        print(f"  [{n}] {t}")
        if "Respaldo previo creado" in t and "`" in t:
            backup = t.split("`")[1]
    harness.clear_msgs()

    banner("8) VALIDACIÓN POST-ESCRITURA")
    md2 = mod.dbx.files_get_metadata(cfg["remote_path"])
    _, r2 = mod.dbx.files_download(cfg["remote_path"])
    contenido = r2.content
    rel = pd.read_excel(io.BytesIO(contenido), sheet_name=None)
    ok2 = True
    def chk2(n, c, det=""):
        nonlocal ok2
        print(f"  {'✔' if c else '🚨'} {n:<64} {det}")
        ok2 = ok2 and bool(c)
    print(f"  rev NUEVA = {md2.rev}  size = {md2.size:,}\n  backup = {backup}")
    for c, h in HOJA.items():
        chk2(f"{c}: saldo", abs(saldo(rel[h]) - s1[c]) < 0.01, f"COP {saldo(rel[h]):,.2f}")
    for o, (org, dst, _u) in MOV.items():
        qo = rel[HOJA[org]]["Orden"].astype(str).str.strip()
        qd = rel[HOJA[dst]]["Orden"].astype(str).str.strip()
        vo = float(pd.to_numeric(rel[HOJA[org]].loc[qo == o, "Monto"]).iloc[0])
        chk2(f"{o[-18:]} en {org} = 0", vo == 0, f"{vo:,.2f}")
        chk2(f"{o[-18:]}_{dst} existe en {dst}", int((qd == f"{o}_{dst}").sum()) == 1)
    for hoja in vivo:
        if hoja in HOJA.values():
            continue
        A, B = vivo[hoja], rel[hoja]
        igual = len(A) == len(B)
        if igual and "Monto" in A.columns:
            igual = abs(pd.to_numeric(A["Monto"], errors="coerce").fillna(0).sum() -
                        pd.to_numeric(B["Monto"], errors="coerce").fillna(0).sum()) < 0.01
        chk2(f"verbatim: {hoja}", igual, f"{len(A)} filas")

    banner("9) COPIA A ONEDRIVE")
    hoy2 = pd.Timestamp.today().strftime("%Y%m%d")
    destino = f"{ONEDRIVE_DIR}/{hoy2}_Historico_mayoristas.xlsx"
    os.makedirs(ONEDRIVE_ANT, exist_ok=True)
    for f2 in sorted(os.listdir(ONEDRIVE_DIR)):
        if f2.endswith("_Historico_mayoristas.xlsx") and f2 != os.path.basename(destino):
            shutil.move(f"{ONEDRIVE_DIR}/{f2}",
                        f"{ONEDRIVE_ANT}/{f2.replace('.xlsx','')}_PRE_reasignacion.xlsx")
            print(f"  archivada: {f2}")
    with open(destino, "wb") as fh:
        fh.write(contenido)
    print(f"  escrita: {destino} ({os.path.getsize(destino):,} bytes)")
    chk2("la copia de OneDrive pesa lo mismo que el vivo",
         os.path.getsize(destino) == md2.size)

    print(f"\n  {'✅ REASIGNACIÓN APLICADA Y VERIFICADA' if ok2 else '🚨 REVISAR'}")
    print(f"  rev nueva: {md2.rev}\n  rollback:  {backup}")


if __name__ == "__main__":
    sys.path.insert(0, os.environ.get("HARNESS_DIR", "."))
    main()
