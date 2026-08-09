"""
Servidor de eco FIX (acceptor) con QuickFIX sobre TCP.

Recibe NewOrderSingle (35=D) del cliente y responde con ExecutionReport (35=8),
copiando el seq (tag 11) y t_send (tag 20001), agregando su propio timestamp de
recepción (tag 20003) y devolviendo el relleno (tag 58) para igualar tamaño.

Se ejecuta como proceso independiente:
    python -m benchmark.servers.fix_server --port 9000
"""

from __future__ import annotations

import argparse
import os
import signal
import tempfile
import threading

import quickfix as fix

from benchmark.common import now_ns
from benchmark import fix_config as fc


class EchoAcceptor(fix.Application):
    def onCreate(self, sessionID):  # noqa: N802 (API de quickfix)
        pass

    def onLogon(self, sessionID):  # noqa: N802
        pass

    def onLogout(self, sessionID):  # noqa: N802
        pass

    def toAdmin(self, message, sessionID):  # noqa: N802
        pass

    def fromAdmin(self, message, sessionID):  # noqa: N802
        pass

    def toApp(self, message, sessionID):  # noqa: N802
        pass

    def fromApp(self, message, sessionID):  # noqa: N802
        # Estampar lo antes posible.
        t_srv = now_ns()
        seq = _get(message, fc.TAG_SEQ)
        t_send = _get(message, fc.TAG_T_SEND)
        padding = _get(message, fc.TAG_PADDING, default="")

        reply = fix.Message()
        header = reply.getHeader()
        header.setField(fix.StringField(35, "8"))  # ExecutionReport
        reply.setField(fix.StringField(fc.TAG_SEQ, seq))
        reply.setField(fix.StringField(fc.TAG_T_SEND, t_send))
        reply.setField(fix.StringField(fc.TAG_T_SRV_RECV, str(t_srv)))
        if padding:
            reply.setField(fix.StringField(fc.TAG_PADDING, padding))
        try:
            fix.Session.sendToTarget(reply, sessionID)
        except fix.SessionNotFound:
            pass


def _get(message, tag, default=None):
    f = fix.StringField(tag)
    try:
        message.getField(f)
        return f.getString()
    except fix.FieldNotFound:
        return default


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--cfg", default=None, help="Ruta cfg; si no, se genera temp")
    p.add_argument("--tls", action="store_true")
    p.add_argument("--persistent", action="store_true",
                   help="sesión persistente en disco (para fault_injection)")
    p.add_argument("--store-dir", default=None, help="FileStorePath si persistent")
    args = p.parse_args()

    cfg_path = args.cfg
    if cfg_path is None:
        tmpdir = tempfile.mkdtemp(prefix="fixsrv_")
        store_dir = args.store_dir
        cert = key = None
        if args.tls:
            from benchmark.tls import ensure_cert
            cert, key = ensure_cert()
        if args.persistent and store_dir is None:
            store_dir = os.path.join(tmpdir, "store")
            os.makedirs(store_dir, exist_ok=True)
        cfg_path = fc.write_acceptor_cfg(
            os.path.join(tmpdir, "acceptor.cfg"), args.port,
            tls=args.tls, cert=cert, key=key,
            persistent=args.persistent, store_dir=store_dir,
        )

    settings = fix.SessionSettings(cfg_path)
    app = EchoAcceptor()
    store = fix.FileStoreFactory(settings) if args.persistent else fix.MemoryStoreFactory()
    log = fix.ScreenLogFactory(settings)
    if args.tls:
        acceptor = fix.SSLSocketAcceptor(app, store, settings, log)
    else:
        acceptor = fix.SocketAcceptor(app, store, settings, log)
    acceptor.start()

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    try:
        stop.wait()
    finally:
        acceptor.stop()


if __name__ == "__main__":
    main()
