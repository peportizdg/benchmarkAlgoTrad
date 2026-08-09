#!/usr/bin/env bash
# Reconstruye quickfix con soporte SSL/TLS (HAVE_SSL), que el wheel de PyPI NO
# trae. Necesario solo para correr FIX sobre TLS (./run.sh full --tls).
# Requiere: libssl-dev, gcc/g++, y el venv del proyecto activo.
set -euo pipefail
cd "$(dirname "$0")"
source .venv/bin/activate

tmp="$(mktemp -d)"
pip download quickfix --no-binary :all: --no-deps -d "$tmp"
tar xzf "$tmp"/quickfix-*.tar.gz -C "$tmp"
src="$(echo "$tmp"/quickfix-*/)"

# Inyecta HAVE_SSL + linkeo de OpenSSL en el Extension() de setup.py.
python - "$src/setup.py" <<'PY'
import sys, re
p = sys.argv[1]
s = open(p).read()
if "HAVE_SSL" not in s:
    s = s.replace(
        "extra_compile_args=[",
        "define_macros=[('HAVE_SSL', '1')],\n            libraries=['ssl', 'crypto'],\n            extra_compile_args=[",
        1,
    )
    open(p, "w").write(s)
    print("setup.py parcheado con HAVE_SSL + OpenSSL")
else:
    print("setup.py ya tenía HAVE_SSL")
PY

pip install --force-reinstall --no-deps "$src"
python -c "import quickfix as fix; a=fix.SSLSocketAcceptor; print('quickfix con SSL: OK')"
echo "Listo. Ya podés correr: ./run.sh full --tls"
