#!/usr/bin/env bash
# Emulación de latencia de red sobre la interfaz loopback con tc/netem.
#
# El banco corre cliente y servidor en el mismo host (loopback), donde la
# latencia de red es ~0. Esto deja ver solo el overhead de CPU/librería de cada
# protocolo, lo cual NO refleja un despliegue real contra un broker. netem
# inyecta delay (+ jitter) por paquete para que las mediciones reflejen
# condiciones realistas.
#
# OJO: aplica a TODO el tráfico de loopback de la máquina mientras esté activo,
# y requiere sudo.
#
# El delay se aplica en CADA sentido, así que el RTT agregado ≈ 2 × delay.
#
# Uso:
#   sudo ./netem.sh on  5ms 1ms     # 5 ms ± 1 ms por sentido  => ~10 ms de RTT
#   sudo ./netem.sh on  1ms          # 1 ms fijo por sentido    => ~2 ms de RTT
#   sudo ./netem.sh status
#   sudo ./netem.sh off
#
# Sin sudo: `ns` corre un comando dentro de un network namespace propio
# (unshare -rn), con su propio lo y la misma regla netem. No toca el lo del host.
#   ./netem.sh ns 5ms 1ms -- ./run.sh full --tag net10ms ...
#
# Flujo típico para la tesis:
#   sudo ./netem.sh on 5ms 1ms
#   ./run.sh full --tag net5ms
#   sudo ./netem.sh off
#   ./run.sh full --tag loopback           # baseline sin red
#
set -euo pipefail

DEV="${NETEM_DEV:-lo}"
cmd="${1:-status}"

case "$cmd" in
  on)
    delay="${2:?Pasá el delay, ej: 5ms}"
    jitter="${3:-}"
    tc qdisc del dev "$DEV" root 2>/dev/null || true
    if [[ -n "$jitter" ]]; then
      tc qdisc add dev "$DEV" root netem delay "$delay" "$jitter" distribution normal
      echo "netem ON en $DEV: delay=$delay jitter=$jitter (RTT ≈ 2× delay)"
    else
      tc qdisc add dev "$DEV" root netem delay "$delay"
      echo "netem ON en $DEV: delay=$delay (RTT ≈ 2× delay)"
    fi
    ;;
  off)
    tc qdisc del dev "$DEV" root 2>/dev/null || true
    echo "netem OFF en $DEV"
    ;;
  status)
    tc qdisc show dev "$DEV"
    ;;
  ns)
    delay="${2:?Pasá el delay, ej: 5ms}"
    jitter="${3:?Pasá el jitter, ej: 1ms}"
    shift 3
    [[ "${1:-}" == "--" ]] && shift
    exec unshare -rn bash -c '
      set -euo pipefail
      ip link set lo up
      tc qdisc add dev lo root netem delay "$1" "$2" distribution normal
      echo "netem ON (netns) en lo: $(tc qdisc show dev lo)"
      shift 2
      "$@"
      echo "== tc -s qdisc al terminar:"; tc -s qdisc show dev lo' _ "$delay" "$jitter" "$@"
    ;;
  *)
    echo "Uso: sudo ./netem.sh {on <delay> [jitter] | off | status} | ./netem.sh ns <delay> <jitter> -- <cmd...>"
    exit 1
    ;;
esac
