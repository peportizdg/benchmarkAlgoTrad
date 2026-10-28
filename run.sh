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
# Se llama al intérprete del .venv por ruta: el bin/activate quedó con la ruta
# vieja del repo y, sin esto, `python` resolvía a otro venv del PATH.
# El .venv es build estándar (con GIL). Si el shell trae PYTHON_GIL=0 heredado
# del entorno free-threaded de m4ssockets, Python aborta al arrancar. Lo saco.
unset PYTHON_GIL VIRTUAL_ENV PYTHONHOME PYTHONPATH
python() { "$PWD/.venv/bin/python" "$@"; }

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
