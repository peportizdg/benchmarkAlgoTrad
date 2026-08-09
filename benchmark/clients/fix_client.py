"""
Sesión FIX (initiator) con QuickFIX, puenteada a asyncio.

Los callbacks de QuickFIX corren en su propio hilo; las respuestas se entregan al
event-loop con ``loop.call_soon_threadsafe`` resolviendo futures por `seq`. Así
`roundtrip` es una corrutina normal y FIX encaja en el mismo motor de carga que
HTTP/WS (open-loop y closed-loop, con multiplexación sobre la única sesión).

Soporta TLS (SSLSocketInitiator) y sesión persistente (para fault_injection:
QuickFIX detecta gaps y pide Resend Request al reconectar).

`echoed_seqs` acumula TODO seq que recibió eco alguna vez (incluso reenvíos
tardíos post-reconexión); lo usa el escenario de fallas para contar recuperados.
"""

from __future__ import annotations

import asyncio
import threading

import quickfix as fix

from benchmark.common import now_ns
from benchmark.clients.driver import RTResult
from benchmark import fix_config as fc

_FIX_BASE_OVERHEAD = 90  # overhead aprox. de un NewOrderSingle mínimo (bytes)


class _App(fix.Application):
    def __init__(self, session):
        super().__init__()
        self.s = session

    def onCreate(self, sessionID):  # noqa: N802
        self.s.session_id = sessionID

    def onLogon(self, sessionID):  # noqa: N802
        self.s.session_id = sessionID
        self.s._set_logon(True)

    def onLogout(self, sessionID):  # noqa: N802
        self.s._set_logon(False)

    def toAdmin(self, message, sessionID):  # noqa: N802
        pass

    def fromAdmin(self, message, sessionID):  # noqa: N802
        pass

    def toApp(self, message, sessionID):  # noqa: N802
        pass

    def fromApp(self, message, sessionID):  # noqa: N802
        t_recv = now_ns()
        seq = _get(message, fc.TAG_SEQ)
        t_send = int(_get(message, fc.TAG_T_SEND, "0"))
        t_srv = int(_get(message, fc.TAG_T_SRV_RECV, "0"))
        if seq is None:
            return
        res = RTResult(t_send, t_srv, t_recv, ok=True)
        self.s._deliver(seq, res)


def _get(message, tag, default=None):
    f = fix.StringField(tag)
    try:
        message.getField(f)
        return f.getString()
    except fix.FieldNotFound:
        return default


class FixSession:
    def __init__(self, *, cfg_path: str, tls: bool = False):
        self.cfg_path = cfg_path
        self.tls = tls
        self.session_id = None
        self._loop = None
        self._app = None
        self._initiator = None
        self._pending: dict[str, asyncio.Future] = {}
        self._lock = threading.Lock()
        self._logon_event = asyncio.Event()
        self.logged_on = False
        self.echoed_seqs: set[int] = set()

    # --- puente desde el hilo de quickfix hacia el loop ---
    def _set_logon(self, up: bool):
        if self._loop is None:
            return
        self._loop.call_soon_threadsafe(self._apply_logon, up)

    def _apply_logon(self, up: bool):
        self.logged_on = up
        if up:
            self._logon_event.set()
        else:
            self._logon_event.clear()

    def _deliver(self, seq: str, res: RTResult):
        if self._loop is None:
            return
        self._loop.call_soon_threadsafe(self._resolve, seq, res)

    def _resolve(self, seq: str, res: RTResult):
        try:
            self.echoed_seqs.add(int(seq))
        except (TypeError, ValueError):
            pass
        fut = self._pending.pop(seq, None)
        if fut is not None and not fut.done():
            fut.set_result(res)

    async def start(self, logon_timeout: float = 15.0):
        self._loop = asyncio.get_event_loop()
        # IMPORTANTE: settings/store/log DEBEN seguir vivos mientras viva el
        # initiator (son objetos C++). Si se los deja como locales, el GC los
        # libera y quickfix segfaultea al operar/parar. Se guardan en self.
        self._settings = fix.SessionSettings(self.cfg_path)
        self._app = _App(self)
        if _is_persistent(self._settings):
            self._store = fix.FileStoreFactory(self._settings)
        else:
            self._store = fix.MemoryStoreFactory()
        self._log = fix.ScreenLogFactory(self._settings)
        if self.tls:
            self._initiator = fix.SSLSocketInitiator(
                self._app, self._store, self._settings, self._log)
        else:
            self._initiator = fix.SocketInitiator(
                self._app, self._store, self._settings, self._log)
        self._initiator.start()
        await asyncio.wait_for(self._logon_event.wait(), timeout=logon_timeout)
        try:
            await self.roundtrip(-1, 64)  # pre-warm
        except Exception:
            pass

    async def stop(self):
        if self._initiator is not None:
            self._initiator.stop()
            self._initiator = None

    async def wait_logon(self, timeout: float) -> bool:
        try:
            await asyncio.wait_for(self._logon_event.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    async def roundtrip(self, seq: int, size: int, timeout: float = 30.0) -> RTResult:
        skey = str(seq)
        fut = self._loop.create_future()
        self._pending[skey] = fut
        t_send = now_ns()
        msg = _build_order(seq, t_send, size)
        try:
            fix.Session.sendToTarget(msg, self.session_id)
        except Exception:
            self._pending.pop(skey, None)
            return RTResult(0, 0, 0, ok=False)
        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except Exception:
            self._pending.pop(skey, None)
            return RTResult(0, 0, 0, ok=False)


def _is_persistent(settings) -> bool:
    # Si la config declara FileStorePath, es sesión persistente.
    try:
        d = settings.get()
        return d.has("FileStorePath")
    except Exception:
        return False


def _build_order(seq: int, t_send: int, size: int) -> fix.Message:
    pad_len = max(0, size - _FIX_BASE_OVERHEAD)
    msg = fix.Message()
    msg.getHeader().setField(fix.StringField(35, "D"))  # NewOrderSingle
    msg.setField(fix.StringField(fc.TAG_SEQ, str(seq)))
    msg.setField(fix.StringField(fc.TAG_T_SEND, str(t_send)))
    if pad_len > 0:
        msg.setField(fix.StringField(fc.TAG_PADDING, "A" * pad_len))
    return msg
