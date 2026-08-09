"""
Sesión WebSocket (librería ``websockets``). Texto plano (ws://) y TLS (wss://).

En modo persistente mantiene UNA conexión y un receptor en segundo plano que
demultiplexa las respuestas por `seq` hacia futures, de modo que `roundtrip`
puede invocarse concurrentemente (necesario para open-loop y closed-loop con
concurrency>1 sobre una sola conexión).
"""

from __future__ import annotations

import asyncio
import ssl

import websockets

from benchmark.common import build_payload, now_ns, parse_header
from benchmark.clients.driver import RTResult, result_from_bytes


def _ssl_ctx(tls: bool):
    if not tls:
        return None
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE  # cert autofirmado local
    return ctx


class WsSession:
    def __init__(self, *, url: str, tls: bool, persistent: bool):
        self.url = url
        self.tls = tls
        self.persistent = persistent
        self._conn = None
        self._pending: dict[int, asyncio.Future] = {}
        self._recv_task = None
        self._alive = False
        self._conn_lock = asyncio.Lock()
        self.echoed_seqs: set[int] = set()  # para conteo de recuperados en fault

    async def _connect(self):
        return await websockets.connect(
            self.url, max_size=None, ping_interval=None, ssl=_ssl_ctx(self.tls),
        )

    async def _receiver(self):
        try:
            async for msg in self._conn:
                t_recv = now_ns()
                if isinstance(msg, str):
                    msg = msg.encode()
                if len(msg) < 24:
                    continue
                seq = parse_header(msg).seq
                self.echoed_seqs.add(seq)
                fut = self._pending.pop(seq, None)
                if fut is not None and not fut.done():
                    fut.set_result((msg, t_recv))
        except Exception:
            pass
        finally:
            # Cortó la conexión: fallar todo lo pendiente (mensajes perdidos).
            self._alive = False
            for fut in list(self._pending.values()):
                if not fut.done():
                    fut.set_result((None, 0))
            self._pending.clear()

    async def _ensure_conn(self):
        """Reconecta si la conexión se cayó (necesario tras una falla)."""
        if self._alive and self._conn is not None:
            return
        async with self._conn_lock:
            if self._alive and self._conn is not None:
                return
            self._conn = await self._connect()
            self._alive = True
            self._recv_task = asyncio.create_task(self._receiver())

    async def start(self):
        if self.persistent:
            await self._ensure_conn()
            try:
                await self.roundtrip(-1, 64)  # pre-warm
            except Exception:
                pass

    async def stop(self):
        if self._recv_task is not None:
            self._recv_task.cancel()
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def roundtrip(self, seq: int, size: int) -> RTResult:
        if self.persistent:
            try:
                await self._ensure_conn()
            except Exception:
                return RTResult(0, 0, 0, ok=False)
            loop = asyncio.get_event_loop()
            fut = loop.create_future()
            self._pending[seq] = fut
            try:
                await self._conn.send(build_payload(seq, size))
                msg, t_recv = await asyncio.wait_for(fut, timeout=30.0)
                if msg is None:
                    return RTResult(0, 0, 0, ok=False)
                return result_from_bytes(msg, t_recv)
            except Exception:
                self._pending.pop(seq, None)
                return RTResult(0, 0, 0, ok=False)
        else:
            try:
                async with await self._connect() as c:
                    await c.send(build_payload(seq, size))
                    msg = await c.recv()
                    t_recv = now_ns()
                    if isinstance(msg, str):
                        msg = msg.encode()
                    return result_from_bytes(msg, t_recv)
            except Exception:
                return RTResult(0, 0, 0, ok=False)
