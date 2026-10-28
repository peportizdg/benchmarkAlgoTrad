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
from benchmark.clients.driver import RTResult, new_counters, result_from_bytes

# Reintentos ante errores de transporte (reset por recycle de conexión h2 sobre
# TLS en CPython 3.14). En fault_injection se ponen en 0 para exponer la pérdida.
DEFAULT_RETRIES = 4


class _CountingBackend:
    """Envuelve el backend de red de httpcore para contar conexiones TCP nuevas
    (costo cero por request: solo se ejecuta al abrir una conexión)."""

    def __init__(self, inner, session):
        self._inner = inner
        self._s = session

    async def connect_tcp(self, *args, **kwargs):
        stream = await self._inner.connect_tcp(*args, **kwargs)
        self._s.counters["connects"] += 1
        if self._s._lost_conn:
            self._s.counters["reconnects"] += 1
        return stream

    def __getattr__(self, name):
        return getattr(self._inner, name)


class HttpSession:
    def __init__(self, *, base_url: str, http2: bool, tls: bool, persistent: bool,
                 concurrency: int, retries: int = DEFAULT_RETRIES, pool: int | None = None):
        self.url = base_url.rstrip("/") + "/echo"
        self.http2 = http2
        self.tls = tls
        self.persistent = persistent
        self.concurrency = concurrency
        self.retries = retries
        self.pool = pool
        self._client = None
        self.echoed_seqs: set[int] = set()  # para conteo de recuperados en fault
        self.counters = new_counters()
        self._lost_conn = False

    def _opts(self) -> dict:
        # HTTP/1.1 necesita una conexión por mensaje en vuelo; HTTP/2 una sola.
        max_conns = 1 if self.http2 else (self.pool or max(self.concurrency * 2, 2))
        limits = httpx.Limits(
            max_connections=max_conns,
            max_keepalive_connections=max_conns if self.persistent else 0,
        )
        # verify=False también sin TLS: si no, httpx carga las CAs (~12 ms) en cada
        # cliente nuevo, y sin keep-alive se crea uno por mensaje.
        return dict(timeout=30.0, http2=self.http2, limits=limits, verify=False)

    async def start(self):
        if self.persistent:
            self._client = httpx.AsyncClient(**self._opts())
            pool = self._client._transport._pool
            pool._network_backend = _CountingBackend(pool._network_backend, self)
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
        payload = build_payload(seq, size)  # un solo t_send: los intentos fallidos cuentan
        for attempt in range(self.retries + 1):
            if attempt:
                self.counters["retx_out"] += 1
            try:
                if self.persistent:
                    resp = await self._client.post(self.url, content=payload)
                    t_recv = now_ns()
                else:
                    async with httpx.AsyncClient(**self._opts()) as c:
                        resp = await c.post(self.url, content=payload)
                        t_recv = now_ns()
                if resp.status_code == 200:
                    if seq in self.echoed_seqs:
                        self.counters["dup_delivered"] += 1
                    self.echoed_seqs.add(seq)
                    return result_from_bytes(resp.content, t_recv)
                return RTResult(0, 0, 0, ok=False)
            except httpx.TransportError:
                self.counters["transport_errors"] += 1
                self._lost_conn = True
                continue  # reintentar
            except Exception:
                return RTResult(0, 0, 0, ok=False)
        return RTResult(0, 0, 0, ok=False)
