# Mayoristas_app — guía de trabajo

Generador del histórico de conciliación de mayoristas. App Streamlit desplegada desde GitHub
`main` (Streamlit Cloud re-despliega solo al pushear).

**Artefacto central:** `/Historico Mayoristas/historico_mayoristas.xlsx` en Dropbox — una hoja
por casillero. Es **dinero real de clientes**. Credenciales en `.streamlit/secrets.toml`.

**Repo hermano:** `Dash_mayoristas` (visor). Solo LEE el histórico, nunca lo escribe.

---

## 🔴 REGLA DE ORO: no romper nada que ya funcione

1. **Dry-run antes de escribir.** Nunca escribir a Dropbox sin OK explícito del usuario.
2. **Backup antes de sobrescribir.** Si el backup falla, no se escribe.
3. **Orden seguro:** lista de exclusión → merge de código → cargue. Nunca al revés.
4. **Verificar leyendo de vuelta** desde Dropbox después de cada escritura.
5. **Trabajar en rama**, commit pequeño, merge `--no-ff`.
6. **Cargar el histórico del vivo fresco de Dropbox**, nunca de copia local ni backup.
7. Toda decisión que mueva plata **se confirma antes de escribir**.
8. Los criterios de aceptación se fijan **en USD**, no en COP: el COP depende de la asignación
   de TRM por reembolso y puede variar legítimamente entre corridas.

### Zonas intocables (parar y explicar el riesgo ANTES de editar)

`recalcular_totales_diarios` · dedup por `Orden` · "Saldo al cierre" = último Total ·
blocklist `ENVIOS_BLOQUEADOS` · el bloque de comisión quincenal · flujo de secrets/Dropbox/Siigo ·
los cuatro `procesar_*` de tarjetas existentes.

**Verificar por AST** que esas funciones quedan idénticas tras cualquier cambio.

---

## Arquitectura en 30 segundos

**Cinco tarjetas**, cada una con su módulo paralelo (no reusan código entre sí):

| Módulo | Casillero | Orden (ID de cada movimiento) |
|---|---|---|
| `procesar_amex` | 11591 · 13608 · 1444 | `amex_<Reference>` (ID nativo) |
| `procesar_rakuten` | 1444 | `rakuten_<sha1-12>` |
| `procesar_robinhood` | 1444 | `robinhood_<sha1-12>` **sin hora** |
| `procesar_capital` | 13608 | `capital_<sha1-12>` |
| `procesar_usbank` | 11591 · 13608 | `usbank_<ref>` o `usbank_<sha1-12>` (híbrido) |

**USD → COP** con `_amex_trm_dia()`: TRM oficial de datos.gov.co **+ 125**, consulta por rango de
vigencia (cubre fines de semana). **Sin TRM de respaldo: si falta un día con movimiento, el
cargue aborta.** (Los envíos son la excepción: fail-soft.)

**Reembolsos** usan la TRM de su compra original (3 pasadas de `_resolver_trm_reembolsos`) para
netear exacto y no dejar residuo cambiario.

**Tarifas de envío** (filas con `Motivo == "Envio"`): 1633 tiene mínimo de 14,4 USD (solo sube);
1444 tiene tarifa por peso `(max(lb,1)×6)+5` que **reemplaza** el valor del portal.
⚠️ La de 1444 usa la TRM **del archivo de envíos**, no `_amex_trm_dia` — es la única regla de
precio que no consulta datos.gov.co.

---

## Anti-doble-cobro: dos barreras + tres capas de blindaje

**La lista manda**, el corte por fecha es solo límite de sanidad.
Archivo: `/Historico Mayoristas/tarjetas_cobradas.xlsx`. **Obligatoria**: si no se puede leer,
`st.stop()`.

1. **Por `Orden`** — exclusión directa.
2. **Por atributos** — mismo casillero + |USD| exacto + merchant normalizado + fecha ±3 días +
   **mismo signo**. Consumo 1:1. El signo es esencial: en Capital un reembolso tiene idéntica
   descripción y monto que su compra.

**Blindaje A+B+C** (nació de perder 128 filas de Robinhood el 24-jul-2026):

- **A** `guard_frescura_historico` — si la salida perdería algún `Orden` del vivo, `st.stop()`.
- **B** `preservar_filas_tarjeta` — reinyecta filas de tarjeta por prefijo (`TARJETA_ORDEN_RE`).
- **C** backup automático antes de sobrescribir.

> ⚠️ **La capa A también bloquea los borrados INTENCIONALES.** Si hay que retirar filas a
> propósito, re-descargar no lo resuelve. El patrón correcto es: pedir OK al usuario, hacer un
> respaldo capa C manual, verificar por cuenta propia que la única pérdida es la buscada, y
> saltar la capa A **solo en esa corrida**. **NUNCA ampliar `_orden_removible`** para permitirlo:
> debilitaría el guard para siempre.

---

## Añadir una tarjeta nueva: los 3 enganches que no admiten olvido

Modelar el módulo sobre `procesar_capital` (el más reciente y completo), y **no olvidar**:

| Qué | Dónde | Por qué |
|---|---|---|
| Prefijo en `TARJETA_ORDEN_RE` | constante de módulo | sin esto la **capa B** no reinyecta sus filas → es el fallo exacto que costó 128 filas de Robinhood |
| Prefijo en `_hist_tarjetas_para_trm` | función | sin esto un reembolso no encuentra la TRM de su compra original |
| `Motivo` en `es_tarjeta` | dentro de `agregar_incentivo_amex` | decide si la tarjeta genera incentivo |

Más: bloque de constantes, sección propia en la UI con uploader propio, y añadirla a `frames`
en `main()`.

---

## Comisión quincenal

`1,5% × |Total diario más negativo de la quincena|` — el día en que el mayorista **más debe**.
Config en `COMISION_QUINCENAL_CONF` (hoy: **1444** y **9444**).

⚠️ **El "Total diario" es el saldo ACUMULADO, no el neto del día.** Un saldo negativo no se
reinicia con la quincena: arrastra, y cada quincena vuelve a cobrar sobre él hasta amortizarlo.
No son eventos independientes.

⚠️ **Una quincena solo se recalcula DENTRO de su ventana:**
día 1–15 → calcula *16-fin del mes anterior* · día ≥ 16 → calcula *1-15 del mes en curso*.
Pasada la ventana, queda congelada aunque lleguen movimientos tarde. `CUTOFF_COMISION_NUEVA`
solo decide si una fila existente se reescribe *dentro* de su ventana.

⚠️ **Nunca mover un `desde` ni un corte hacia atrás**: alcanzaría quincenas ya comisionadas y
recalcularía la comisión **en silencio**.

---

## Trampas que ya costaron dinero (no repetirlas)

| Trampa | Qué pasó |
|---|---|
| Prefijo olvidado en el regex | 128 filas de Robinhood borradas por sobrescritura (24-jul-2026) |
| Emisor que re-fecha | Robinhood y Capital re-expiden movimientos con otra fecha → barrera por atributos **con signo** |
| Mapear una sub-tarjeta por parecido | Kelly (US Bank 2529) se mapeó a 1444 por analogía con Amex: **1.372 millones** cobrados de más, hubo que revertir |
| Extracto de rango corto | Reembolsos pierden su compra original y caen al fallback de TRM. **Bajar siempre el rango completo** |
| Cifra fijada a mano | Un incentivo inventado se detectó antes de escribir. **Todo número se deriva de los datos** |
| Criterio de aceptación en COP | Casi detiene un cargue correcto. Los criterios van **en USD** |
| `if dfn.empty: continue` | Una hoja solo entra al pipeline si la corrida produce ≥1 fila para ella. De ahí venía el "duplicado que a veces colapsaba y a veces no" |

---

## Reglas operativas del backoffice

1. **🛑 No cobrar tarjetas a mano** para fechas ≥ el corte de cada tarjeta. El sistema las cobra;
   un cobro manual las duplica y **el dedup no puede verlo** (Orden numérico vs. con prefijo).
2. **Cargar siempre rango amplio** de extractos: recargar no duplica, cargar de menos deja
   movimientos tardíos por fuera.
3. La copia Dropbox → OneDrive (`Historico Carga/Conciliacion/Mayoristas/`) la hace el usuario
   a mano.
4. El archivo intermedio de envíos debe traer la columna **`PESO`**; sin ella la tarifa de 1444
   cae en fail-soft y no cobra.

---

## Cómo probar sin escribir

El módulo se puede importar fuera de Streamlit con un stub, para correr dry-runs contra los
datos reales sin tocar producción:

- Stubbear `streamlit` en `sys.modules` **antes** de importar el módulo.
- `st.stop` debe **levantar excepción**, no ser un no-op: si no, los blindajes no abortan de verdad.
- `st.cache_data` debe detectar el uso sin paréntesis (`@st.cache_data`): si devuelve
  `lambda f: f` a secas, convierte la función decorada en la identidad y el dry-run miente.
- El merge/dedup de `main()` hay que replicarlo **fiel**: recomputar las máscaras después de
  cada `concat` (usan el índice nuevo) y respetar que el dedup de ingresos excluye
  `Ingreso_extra` y devoluciones.

---

## Dónde está cada cosa

Las líneas se mueven; buscar por nombre:

- **Constantes/perillas**: `AMEX_*`, `RAKUTEN_*`, `ROBINHOOD_*`, `CAPITAL_*`, `USBANK_*`,
  `TARIFA_*`, `COMISION_QUINCENAL_CONF`, `COBROS_MENSUALES_CONF`, `INCENTIVO_*`
- **Blindaje**: `TARJETA_ORDEN_RE`, `preservar_filas_tarjeta`, `guard_frescura_historico`,
  `upload_to_dropbox`
- **Tarjetas**: `procesar_amex` · `procesar_rakuten` · `procesar_robinhood` · `procesar_capital`
  · `procesar_usbank`
- **Envíos**: `procesar_envios_mayoristas`, `aplicar_tarifa_minima_envios`,
  `aplicar_tarifa_envio_por_peso`
- **Saldos**: `recalcular_totales_diarios`, `asegurar_columnas_historico`
- **Comisión/incentivo**: buscar `COMISIÓN QUINCENAL POR TOTALES` y `agregar_incentivo_amex`
