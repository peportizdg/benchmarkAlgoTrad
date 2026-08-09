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
