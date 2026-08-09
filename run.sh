#!/usr/bin/env bash
# Atajos para correr el banco de pruebas con el venv del proyecto.
#
#   ./run.sh smoke [args...]              # corrida reducida (verifica que anda)
#   ./run.sh full  [--tag T] [--tls]      # corrida completa
#   ./run.sh plots <raw.csv> [summary.csv]# gráficos de una corrida
#
set -euo pipefail
cd "$(dirname "$0")"

if [[ ! -d .venv ]]; then
  echo "No existe .venv — crealo: python3.14 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt"
  exit 1
fi
source .venv/bin/activate

cmd="${1:-smoke}"
shift || true

case "$cmd" in
  smoke)
    python -m benchmark.runner --quick "$@"
    ;;
  full)
    python -m benchmark.runner "$@"
    ;;
  plots)
    raw="${1:?Pasá el CSV crudo: ./run.sh plots results/raw_TAG.csv [summary.csv]}"
    summary="${2:-}"
    if [[ -n "$summary" ]]; then
      python -m benchmark.plots run --raw "$raw" --summary "$summary"
    else
      python -m benchmark.plots run --raw "$raw"
    fi
    ;;
  *)
    echo "Comando desconocido: $cmd (usá smoke | full | plots)"
    exit 1
    ;;
esac
