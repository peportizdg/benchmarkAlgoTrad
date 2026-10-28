# benchmarkAlgoTrad

Banco de pruebas para comparar FIX, HTTP/1.1, HTTP/2 y WebSocket a nivel de
mensajes: latencia, throughput, jitter y qué pasa cuando se cae la conexión.
Parte de mi tesis.

## Setup

```bash
python3.14 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
./build-quickfix-ssl.sh    # solo si vas a correr FIX sobre TLS
```

## Correr

```bash
./run.sh full --tag loopback-plain
./run.sh full --tag loopback-tls --tls
sudo ./netem.sh on 5ms 1ms  && ./run.sh full --tag net5ms  --scenarios order_routing,market_data_lat --reps 3 --nmsg 2000 && sudo ./netem.sh off
sudo ./netem.sh on 25ms 5ms && ./run.sh full --tag net25ms --scenarios order_routing,market_data_lat --reps 3 --nmsg 2000 && sudo ./netem.sh off
```

Los datos crudos y los resúmenes quedan en `results/`.

## Gráficos

```bash
./run.sh plots results/raw_loopback-plain.csv results/summary_loopback-plain.csv
python -m benchmark.plots fault --faults results/faults_loopback-plain.csv
python -m benchmark.plots convergence \
  --point 0:results/summary_loopback-plain.csv \
  --point 10:results/summary_net5ms.csv \
  --point 50:results/summary_net25ms.csv \
  --scenario order_routing --concurrency 1
```

## Notas

- `netem.sh` usa `tc` para simular latencia de red, por eso pide sudo.
- La latencia se mide en modo abierto a tasa fija (con corrección de omisión
  coordinada) y el throughput en modo cerrado saturando; están separados a
  propósito para no medir tiempo de cola como si fuera latencia.
- FIX sobre TLS necesita quickfix compilado con OpenSSL (`build-quickfix-ssl.sh`);
  el wheel de PyPI no lo trae.


Google Form de Encuesta a Participantes:

https://docs.google.com/forms/d/1kaf_8kFDIlTTghlc3mvm8uL7yusc2LFVMS6u2fh4fGE/

## Revisión del comité (octubre 2026)

Todo lo nuevo va en `results/revision/` y `plots/revision/`; `results/` original
no se toca. El informe con los números y las respuestas es
`results/revision/REPORTE_REVISION.md`.

### Repeticiones por condición

| Condición | Escenarios | Repeticiones | Mensajes útiles por rep. |
|---|---|---|---|
| loopback-plain, loopback-tls (agosto) | los cinco | 5 | 20.000 (market data) / 10.000 (resto) |
| net5ms, net25ms (agosto) | order_routing, market_data_lat | 3 | 2.000 |
| revision/net10ms, revision/net50ms | order_routing, market_data_lat | 5 | 2.000 |
| revision/faults-* (fallas instrumentadas) | fault_injection | 5 | 6.000 |
| revision/faults_fix_variants | fault_injection, solo FIX | 5 | 6.000 |
| revision/verif-* (contadores, loopback) | los cuatro sin fallas | 1 | idem loopback |

### Configuración de FIX (QuickFIX 1.16)

Latencia/throughput: `MemoryStore`, `PersistMessages=N`, `ResetOnLogon/Logout/Disconnect=Y`.
Prueba de fallas: `FileStore` en ambos lados, `PersistMessages=Y`,
`ResetOnLogon/Logout/Disconnect=N`, `HeartBtInt=30`, `ReconnectInterval=1` (iniciador).
En todas: `ValidateLengthAndChecksum=N`, `UseDataDictionary=N`, `SocketNodelay=Y`.
El volcado completo (archivos .cfg y valores efectivos que resuelve
`FIX::Session`, también de las variantes sin recuperación) está en
`results/revision/fix_session_config.txt`:

```bash
.venv/bin/python -m benchmark.fix_config > results/revision/fix_session_config.txt
```

### Configuración de netem

`tc qdisc add dev lo root netem delay <D> <J> distribution normal`, sin pérdida
de paquetes, sobre `lo`. El retardo se aplica en cada sentido: `5ms 1ms` ⇒ RTT
≈ 10 ms; `25ms 5ms` ⇒ RTT ≈ 50 ms. Las corridas de agosto usaron `sudo ./netem.sh
on` sobre el `lo` del host; las de la revisión usan `./netem.sh ns`, que aplica
la misma regla al `lo` de un network namespace propio (sin sudo). El estado de
la qdisc queda en `meta_<tag>.json` (`qdisc_lo`) y al final de `netem_runs.log`.

### Comandos

```bash
# 5. Red emulada, 5 repeticiones
./netem.sh ns 5ms 1ms -- ./run.sh full --tag net10ms --out results/revision/net10ms \
    --scenarios order_routing,market_data_lat --reps 5 --nmsg 2000
./netem.sh ns 25ms 5ms -- ./run.sh full --tag net50ms --out results/revision/net50ms \
    --scenarios order_routing,market_data_lat --reps 5 --nmsg 2000

# 3. Prueba de fallas instrumentada (mismos parámetros) y contadores en loopback
./run.sh full --scenarios fault_injection --tag faults-plain --out results/revision/faults
./run.sh full --scenarios fault_injection --tag faults-tls --tls --out results/revision/faults
./run.sh full --scenarios market_data_tput,market_data_lat,order_routing,post_trade \
    --reps 1 --tag verif-plain --out results/revision/verif
./run.sh full --scenarios market_data_tput,market_data_lat,order_routing,post_trade \
    --reps 1 --tag verif-tls --tls --out results/revision/verif

# 8. FIX sin recuperación
for v in no_persist reset_on_logon; do
  ./run.sh full --protocols fix --scenarios fault_injection --fix-variant $v \
      --tag $v --out results/revision/faults_fix_variants
done

# 1. Estadística (bootstrap jerárquico, 10.000 iteraciones, semilla 20261026)
.venv/bin/python -m benchmark.stats --out results/revision \
    --cond loopback-plain=results/raw_loopback-plain.csv \
    --cond loopback-tls=results/raw_loopback-tls.csv \
    --cond net5ms=results/raw_net5ms.csv --cond net25ms=results/raw_net25ms.csv \
    --cond net10ms=results/revision/net10ms/raw_net10ms.csv \
    --cond net50ms=results/revision/net50ms/raw_net50ms.csv
.venv/bin/python -m benchmark.plots ci --stats results/revision/stats_cells.csv \
    --out plots/revision --rtt loopback-plain:0 --rtt net10ms:10 --rtt net50ms:50

# 3. Indicadores de error (plazo T = 500 ms por defecto, --deadline-ms)
R=results/revision
.venv/bin/python -m benchmark.errors --out $R \
    --cond loopback-plain=results/raw_loopback-plain.csv \
    --cond loopback-tls=results/raw_loopback-tls.csv \
    --cond net5ms=results/raw_net5ms.csv --cond net25ms=results/raw_net25ms.csv \
    --cond net10ms=$R/net10ms/raw_net10ms.csv:$R/net10ms/counters_net10ms.csv \
    --cond net50ms=$R/net50ms/raw_net50ms.csv:$R/net50ms/counters_net50ms.csv \
    --cond verif-plain=$R/verif/raw_verif-plain.csv:$R/verif/counters_verif-plain.csv \
    --cond verif-tls=$R/verif/raw_verif-tls.csv:$R/verif/counters_verif-tls.csv \
    --faults original-plain=results/raw_loopback-plain.csv:results/faults_loopback-plain.csv \
    --faults original-tls=results/raw_loopback-tls.csv:results/faults_loopback-tls.csv \
    --faults faults-plain=$R/faults/raw_faults-plain.csv:$R/faults/faults_faults-plain.csv \
    --faults faults-tls=$R/faults/raw_faults-tls.csv:$R/faults/faults_faults-tls.csv \
    --faults no_persist=$R/faults_fix_variants/raw_no_persist.csv:$R/faults_fix_variants/faults_no_persist.csv \
    --faults reset_on_logon=$R/faults_fix_variants/raw_reset_on_logon.csv:$R/faults_fix_variants/faults_reset_on_logon.csv

# 7. Perfilado (py-spy fuera del .venv: pip install py-spy en otro entorno)
.venv/bin/python -m benchmark.profile_echo --py-spy "$(command -v py-spy)" --out results/revision/profile
.venv/bin/python results/revision/profile/pyspy_summary.py      # top-10 y categorías FIX
PYTHONPATH=$PWD .venv/bin/python results/revision/profile/fix_micro.py   # cotas de parseo/checksum
for c in 2 2,3 2-5; do                                          # efecto de la afinidad de CPU
  PYTHONPATH=$PWD TAG=$c taskset -c $c .venv/bin/python results/revision/profile/affinity_probe.py fix,ws
done
```

`run.sh` llama a `.venv/bin/python` por ruta: el `bin/activate` del `.venv`
apunta a la ubicación anterior del repo y, sin esto, `python` resolvía a otro
entorno del PATH.

## Campaña final (octubre 2026)

Reemplaza todos los resultados de la tesis. Corrige los errores de medición E1–E6,
E9 y E11 de la revisión, más la espera precisa del generador open-loop (detalle en
`entrega/REPORTE_FINAL.md` §1). Las salidas van a `results/final/`, `plots/final/`
y `entrega/`; `results/` y `results/revision/` no se tocan.

```bash
# 0. Pruebas de las correcciones (10, ~3 s)
.venv/bin/python -m unittest discover -s tests -t .

# 1. Herramienta de perfilado (fuera del .venv del proyecto)
.venv/bin/python -m venv .venv-tools && .venv-tools/bin/pip install py-spy

# 2. Smoke (1 rep., 2 % de los mensajes, sin tocar el governor) -> results/final/smoke/
./run_final.sh --smoke

# 3. Campaña completa, ~3 h 45 min. Root solo fija el governor en "performance" y lo
#    restaura al final (trap); las corridas van como el usuario que llamó a sudo.
#    Afinidad libre, 5 repeticiones. Log: results/final/run_final.log
sudo ./run_final.sh            # --from N retoma desde la fase N

# Fases: 1 recalibración (conc. 16, 5 rep., mediana -> config/calibration_final.json)
#        2 loopback-plain  3 loopback-tls
#        4 red emulada (./netem.sh ns: delay 5ms 1ms y 25ms 5ms, distribución normal)
#        5 fallas sin/con TLS  6 variantes FIX (no_persist, reset_on_logon)
#        7 perfilado + afinidad (libre, 1, 2, 4 núcleos)  8 volcado de config de QuickFIX

# 4. Análisis (bootstrap jerárquico 10.000 iteraciones, margen ±5 %)
F=results/final
.venv/bin/python -m benchmark.stats --out $F \
    --cond loopback-plain=$F/loopback-plain/raw_loopback-plain.csv \
    --cond loopback-tls=$F/loopback-tls/raw_loopback-tls.csv \
    --cond net10ms=$F/net10ms/raw_net10ms.csv --cond net50ms=$F/net50ms/raw_net50ms.csv
.venv/bin/python -m benchmark.errors --out $F \
    --cond loopback-plain=$F/loopback-plain/raw_loopback-plain.csv:$F/loopback-plain/counters_loopback-plain.csv \
    --cond loopback-tls=$F/loopback-tls/raw_loopback-tls.csv:$F/loopback-tls/counters_loopback-tls.csv \
    --cond net10ms=$F/net10ms/raw_net10ms.csv:$F/net10ms/counters_net10ms.csv \
    --cond net50ms=$F/net50ms/raw_net50ms.csv:$F/net50ms/counters_net50ms.csv \
    --faults faults-plain=$F/faults/raw_faults-plain.csv:$F/faults/faults_faults-plain.csv \
    --faults faults-tls=$F/faults/raw_faults-tls.csv:$F/faults/faults_faults-tls.csv \
    --faults no_persist=$F/faults_fix_variants/raw_no_persist.csv:$F/faults_fix_variants/faults_no_persist.csv \
    --faults reset_on_logon=$F/faults_fix_variants/raw_reset_on_logon.csv:$F/faults_fix_variants/faults_reset_on_logon.csv

# 5. Figuras (plots/final/fig_*.png a 300 dpi, resto en plots/final/otras/), tablas
#    (entrega/tablas_tesis.md), reporte (entrega/REPORTE_FINAL.md, desde
#    results/final/reporte_plantilla.md) y zip (entrega/entrega_tesis.zip)
.venv/bin/python -m benchmark.entrega todo
```

Configuración registrada en `meta_<condición>.json` de cada corrida: governor, kernel,
CPU, afinidad, versiones (Python, QuickFIX, websockets, FastAPI, uvicorn, Hypercorn,
HTTPX), pool HTTP/1.1 de market data (`http_pool`), tasas, regla netem y qdisc
efectiva, compresión WebSocket y modo del generador. Configuración efectiva de
QuickFIX de todas las fases: `results/final/fix_session_config.txt`.
