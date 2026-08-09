"""
Sesión HTTP/REST y HTTP/2 (httpx). Cubre texto plano y TLS.

- HTTP/1.1: cada mensaje en vuelo usa su propia conexión (el pool da la
  contrapresión). TLS => https con h1.
- HTTP/2: multiplexa N streams sobre una conexión. Siempre sobre TLS (httpx no
  hace h2c en texto plano).

`roundtrip` es reentrante (varias llamadas concurrentes) porque cada request
HTTP es independiente.
"""

from __future__ import annotations

import httpx

from benchmark.common import build_payload, now_ns
from benchmark.clients.driver import RTResult, result_from_bytes

# Reintentos ante errores de transporte (reset por recycle de conexión h2 sobre
# TLS en CPython 3.14). En fault_injection se ponen en 0 para exponer la pérdida.
DEFAULT_RETRIES = 4


class HttpSession:
    def __init__(self, *, base_url: str, http2: bool, tls: bool, persistent: bool,
                 concurrency: int, retries: int = DEFAULT_RETRIES):
        self.url = base_url.rstrip("/") + "/echo"
        self.http2 = http2
        self.tls = tls
        self.persistent = persistent
        self.concurrency = concurrency
        self.retries = retries
        self._client = None
        self.echoed_seqs: set[int] = set()  # para conteo de recuperados en fault

    def _opts(self) -> dict:
        # HTTP/1.1 necesita una conexión por mensaje en vuelo; HTTP/2 una sola.
        max_conns = 1 if self.http2 else max(self.concurrency * 2, 2)
        limits = httpx.Limits(
            max_connections=max_conns,
            max_keepalive_connections=max_conns if self.persistent else 0,
        )
        opts = dict(timeout=30.0, http2=self.http2, limits=limits)
        if self.tls:
            opts["verify"] = False
        return opts

    async def start(self):
        if self.persistent:
            self._client = httpx.AsyncClient(**self._opts())
            # Pre-warm: descarta el costo de establecer el pool/HS del primer
            # request (que si no domina el desvío en conc=1).
            try:
                await self._client.post(self.url, content=build_payload(0, 64))
            except Exception:
                pass

    async def stop(self):
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def roundtrip(self, seq: int, size: int) -> RTResult:
        for _ in range(self.retries + 1):
            payload = build_payload(seq, size)  # re-estampa t_send en cada intento
            try:
                if self.persistent:
                    resp = await self._client.post(self.url, content=payload)
                    t_recv = now_ns()
                else:
                    async with httpx.AsyncClient(**self._opts()) as c:
                        resp = await c.post(self.url, content=payload)
                        t_recv = now_ns()
                if resp.status_code == 200:
                    self.echoed_seqs.add(seq)
                    return result_from_bytes(resp.content, t_recv)
                return RTResult(0, 0, 0, ok=False)
            except httpx.TransportError:
                continue  # reintentar
            except Exception:
                return RTResult(0, 0, 0, ok=False)
        return RTResult(0, 0, 0, ok=False)
