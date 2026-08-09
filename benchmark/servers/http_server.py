"""
Servidor de eco HTTP/REST con FastAPI + uvicorn.

Endpoint: POST /echo
  - Recibe el cuerpo binario (mensaje del banco), estampa el timestamp de
    recepción del servidor en los bytes [16:24] y devuelve el mismo buffer.

Se ejecuta como proceso independiente:
    python -m benchmark.servers.http_server --port 8000 [--workers 1]
"""

from __future__ import annotations

import argparse

from fastapi import FastAPI, Request, Response

from benchmark.common import now_ns, stamp_server_recv

app = FastAPI(title="bench-echo-http")


@app.post("/echo")
async def echo(request: Request) -> Response:
    body = await request.body()
    # Estampar lo antes posible tras tener el payload completo.
    t = now_ns()
    out = stamp_server_recv(body, t) if len(body) >= 24 else body
    return Response(content=out, media_type="application/octet-stream")


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


def main() -> None:
    import uvicorn

    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--workers", type=int, default=1)
    p.add_argument("--tls", action="store_true", help="HTTP/1.1 sobre TLS (https)")
    args = p.parse_args()

    ssl_kw = {}
    if args.tls:
        from benchmark.tls import ensure_cert
        cert, key = ensure_cert()
        ssl_kw = dict(ssl_certfile=cert, ssl_keyfile=key)

    uvicorn.run(
        "benchmark.servers.http_server:app",
        host=args.host,
        port=args.port,
        workers=args.workers,
        log_level="warning",
        access_log=False,
        **ssl_kw,
    )


if __name__ == "__main__":
    main()
