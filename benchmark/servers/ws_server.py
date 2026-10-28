"""
Servidor de eco WebSocket con la librería ``websockets``.

Por cada frame binario recibido, estampa el timestamp de recepción del servidor
en los bytes [16:24] y lo reenvía al cliente.

Se ejecuta como proceso independiente:
    python -m benchmark.servers.ws_server --port 8001
"""

from __future__ import annotations

import argparse
import asyncio
import ssl

import websockets

from benchmark.common import now_ns, stamp_server_recv


async def handler(ws) -> None:
    async for message in ws:
        t = now_ns()
        if isinstance(message, str):
            message = message.encode()
        out = stamp_server_recv(message, t) if len(message) >= 24 else message
        await ws.send(out)


async def run(host: str, port: int, tls: bool) -> None:
    ssl_ctx = None
    if tls:
        from benchmark.tls import ensure_cert
        cert, key = ensure_cert()
        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ssl_ctx.load_cert_chain(cert, key)
    # max_size=None: sin límite de tamaño de frame (mensajes grandes post-trade).
    # compression=None: FIX y HTTP no comprimen; el relleno es muy compresible.
    async with websockets.serve(handler, host, port, max_size=None,
                                ping_interval=None, ssl=ssl_ctx, compression=None):
        await asyncio.Future()  # corre para siempre


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--tls", action="store_true")
    args = p.parse_args()
    try:
        asyncio.run(run(args.host, args.port, args.tls))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
