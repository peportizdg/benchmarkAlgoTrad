"""
Servidor de eco HTTP/2 (h2 sobre TLS) con Hypercorn, reutilizando la misma app
FastAPI que el servidor HTTP/1.1.

A diferencia de HTTP/1.1, HTTP/2 multiplexa múltiples streams (mensajes en vuelo)
sobre una sola conexión TCP, igual que WebSocket/FIX. Por eso es la comparación
más justa contra esos transportes.

Se ejecuta como proceso independiente:
    python -m benchmark.servers.http2_server --port 8443
"""

from __future__ import annotations

import argparse
import asyncio
import sys

from hypercorn.asyncio import serve
from hypercorn.config import Config

from benchmark.servers.http_server import app  # misma app de eco
from benchmark.tls import ensure_cert


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--host", default="127.0.0.1")
    args = p.parse_args()

    cert, key = ensure_cert()
    config = Config()
    config.bind = [f"{args.host}:{args.port}"]
    config.certfile = cert
    config.keyfile = key
    config.alpn_protocols = ["h2"]  # fuerza HTTP/2
    config.loglevel = "ERROR"
    config.accesslog = None
    # Multiplexing real: permitir muchos streams concurrentes sobre una conexión
    # y no cerrarla por keep-alive durante la corrida.
    config.h2_max_concurrent_streams = 1024
    config.keep_alive_timeout = 300.0
    config.keep_alive_max_requests = sys.maxsize  # el default (1000) cierra la conexión

    try:
        asyncio.run(serve(app, config))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
