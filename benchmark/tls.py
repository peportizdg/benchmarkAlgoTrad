"""
Certificado TLS autofirmado para el transporte HTTP/2.

HTTP/2 en la práctica corre casi siempre sobre TLS (negociación de ALPN). Para
el banco generamos un certificado autofirmado de localhost una sola vez y lo
reutilizamos. El cliente valida con verify=False (es un cert local de prueba).

El overhead de TLS es parte legítima del costo de HTTP/2 tal como se usa en
producción, y se documenta como tal en el README.
"""

from __future__ import annotations

import os
import subprocess

TLS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config", "tls")
CERT_PATH = os.path.join(TLS_DIR, "localhost.crt")
KEY_PATH = os.path.join(TLS_DIR, "localhost.key")


def ensure_cert() -> tuple[str, str]:
    """Genera el par cert/key autofirmado si no existe. Devuelve (cert, key)."""
    os.makedirs(TLS_DIR, exist_ok=True)
    if os.path.exists(CERT_PATH) and os.path.exists(KEY_PATH):
        return CERT_PATH, KEY_PATH
    subprocess.run(
        [
            "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
            "-keyout", KEY_PATH, "-out", CERT_PATH,
            "-days", "3650", "-subj", "/CN=localhost",
            "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return CERT_PATH, KEY_PATH
