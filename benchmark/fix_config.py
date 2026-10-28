"""
Generación de configs de QuickFIX (acceptor / initiator), con variantes:

  - texto plano (SocketAcceptor/Initiator) o TLS (SSLSocketAcceptor/Initiator).
  - sesión efímera (MemoryStore, ResetOn*=Y) para latency/throughput, o sesión
    PERSISTENTE (FileStore en disco, ResetOn*=N, PersistMessages=Y) para el
    escenario de fallas: así, tras reiniciar el proceso servidor, QuickFIX
    recupera los seqnums y puede reenviar mensajes vía Resend Request.

Sin diccionario de datos (UseDataDictionary=N): usamos tags de usuario ≥ 20000.

VARIANTS: overrides del escenario de fallas para la revisión (FIX sin
recuperación). Un valor None quita la clave; sin FileStorePath ambos lados usan
MemoryStore.

    python -m benchmark.fix_config     # vuelca la config efectiva (texto)
"""

from __future__ import annotations

import os

BEGIN_STRING = "FIX.4.4"
SERVER_COMP = "ECHO_SERVER"
CLIENT_COMP = "ECHO_CLIENT"

TAG_SEQ = 11
TAG_T_SEND = 20001
TAG_T_SRV_RECV = 20003
TAG_PADDING = 58

VARIANTS = {
    "no_persist": {"PersistMessages": "N", "FileStorePath": None},
    "reset_on_logon": {"ResetOnLogon": "Y"},
}


def uses_file_store(persistent: bool, overrides: dict | None = None) -> bool:
    if overrides and "FileStorePath" in overrides:
        return overrides["FileStorePath"] is not None
    return persistent


def _common(persistent: bool, store_dir: str | None, overrides: dict | None = None) -> str:
    if persistent:
        # Sesión persistente en disco: sobrevive el reinicio del servidor.
        lines = [
            "PersistMessages=Y",
            "ResetOnLogon=N",
            "ResetOnLogout=N",
            "ResetOnDisconnect=N",
            f"FileStorePath={store_dir}",
        ]
    else:
        lines = [
            "PersistMessages=N",
            "ResetOnLogon=Y",
            "ResetOnLogout=Y",
            "ResetOnDisconnect=Y",
        ]
    lines += [
        "UseDataDictionary=N",
        "ValidateUserDefinedFields=N",
        "ValidateFieldsOutOfOrder=N",
        "ValidateFieldsHaveValues=N",
        "ValidateLengthAndChecksum=N",
        "ScreenLogShowIncoming=N",
        "ScreenLogShowOutgoing=N",
        "ScreenLogShowEvents=N",
        "SocketNodelay=Y",
        "StartTime=00:00:00",
        "EndTime=00:00:00",
    ]
    for key, value in (overrides or {}).items():
        lines = [ln for ln in lines if not ln.startswith(key + "=")]
        if value is not None:
            lines.append(f"{key}={value}")
    return "\n".join(lines) + "\n"


def _tls_acceptor(cert: str, key: str) -> str:
    # No se fija CertificateVerifyLevel => el acceptor no exige cert de cliente.
    return (f"ServerCertificateFile={cert}\n"
            f"ServerCertificateKeyFile={key}\n")


def _tls_initiator(ca: str) -> str:
    # El initiator siempre verifica el cert del server (SSL_VERIFY_PEER); se
    # confía el cert autofirmado usándolo como autoridad certificante.
    return f"CertificationAuthoritiesFile={ca}\n"


def write_acceptor_cfg(path, port, *, tls=False, cert=None, key=None,
                       persistent=False, store_dir=None, overrides=None) -> str:
    body = f"""[DEFAULT]
ConnectionType=acceptor
{_common(persistent, store_dir, overrides)}"""
    if tls:
        body += _tls_acceptor(cert, key)
    body += f"""SocketAcceptPort={port}

[SESSION]
BeginString={BEGIN_STRING}
SenderCompID={SERVER_COMP}
TargetCompID={CLIENT_COMP}
HeartBtInt=30
"""
    with open(path, "w") as f:
        f.write(body)
    return path


def write_initiator_cfg(path, port, host="127.0.0.1", *, tls=False, ca=None,
                        persistent=False, store_dir=None, overrides=None) -> str:
    body = f"""[DEFAULT]
ConnectionType=initiator
{_common(persistent, store_dir, overrides)}"""
    if tls:
        body += _tls_initiator(ca)
    body += f"""SocketConnectHost={host}
SocketConnectPort={port}
ReconnectInterval=1

[SESSION]
BeginString={BEGIN_STRING}
SenderCompID={CLIENT_COMP}
TargetCompID={SERVER_COMP}
HeartBtInt=30
"""
    with open(path, "w") as f:
        f.write(body)
    return path


def dump_effective(variant: str | None = None, persistent: bool = True, tls: bool = False) -> str:
    """Config tal como la arman runner.py (persistent=False: latencia/throughput) o
    fault.py (persistent=True), más los valores EFECTIVOS que QuickFIX resuelve
    para cada sesión (getters de FIX::Session)."""
    import tempfile
    import quickfix as fix

    overrides = VARIANTS[variant] if variant else None
    tmp = tempfile.mkdtemp(prefix="fixcfg_")
    store = os.path.join(tmp, "store")
    cert = key = None
    if tls:
        from benchmark.tls import ensure_cert
        cert, key = ensure_cert()
    paths = {
        "acceptor": write_acceptor_cfg(os.path.join(tmp, "acc.cfg"), 0, tls=tls, cert=cert,
                                       key=key, persistent=persistent,
                                       store_dir=store if persistent else None,
                                       overrides=overrides),
        "initiator": write_initiator_cfg(os.path.join(tmp, "init.cfg"), 0, tls=tls, ca=cert,
                                         persistent=persistent,
                                         store_dir=store + "_cli" if persistent else None,
                                         overrides=overrides),
    }
    file_store = uses_file_store(persistent, overrides)

    class _Noop(fix.Application):
        def onCreate(self, s): pass
        def onLogon(self, s): pass
        def onLogout(self, s): pass
        def toAdmin(self, m, s): pass
        def fromAdmin(self, m, s): pass
        def toApp(self, m, s): pass
        def fromApp(self, m, s): pass

    phase = "fallas" if persistent else "latencia/throughput"
    out = [f"# fase: {phase} | TLS: {'sí' if tls else 'no'} | variante: {variant or 'original'}"]
    keep = []
    for role, path in paths.items():
        settings = fix.SessionSettings(path)
        app = _Noop()
        store_f = fix.FileStoreFactory(settings) if file_store else fix.MemoryStoreFactory()
        log_f = fix.ScreenLogFactory(settings)
        if tls:
            cls = fix.SSLSocketAcceptor if role == "acceptor" else fix.SSLSocketInitiator
        else:
            cls = fix.SocketAcceptor if role == "acceptor" else fix.SocketInitiator
        engine = cls(app, store_f, settings, log_f)  # crea la sesión sin abrir sockets
        keep += [settings, app, store_f, log_f, engine]
        sender, target = (SERVER_COMP, CLIENT_COMP) if role == "acceptor" else (CLIENT_COMP, SERVER_COMP)
        ses = fix.Session.lookupSession(fix.SessionID(BEGIN_STRING, sender, target))
        with open(path) as f:
            text = f.read().replace(tmp, "<tmp>")
        out += [f"\n## {role} — archivo .cfg generado (puerto y rutas varían por corrida)", text.rstrip(),
                f"\n## {role} — valores efectivos (FIX::Session)",
                f"MessageStore={'FileStore' if file_store else 'MemoryStore'}"]
        for getter in ("getPersistMessages", "getResetOnLogon", "getResetOnLogout",
                       "getResetOnDisconnect", "getRefreshOnLogon", "getSendRedundantResendRequests",
                       "getCheckLatency", "getMaxLatency", "getCheckCompId",
                       "getValidateLengthAndChecksum", "getLogonTimeout", "getLogoutTimeout",
                       "getMillisecondsInTimeStamp"):
            out.append(f"{getter[3:]}={getattr(ses, getter)()}")
    return "\n".join(out) + "\n"


if __name__ == "__main__":
    import sys
    text = "\n".join([dump_effective(None, False, False), dump_effective(None, False, True),
                      dump_effective(None, True, False), dump_effective(None, True, True),
                      *(dump_effective(v) for v in VARIANTS)])
    sys.stdout.write(text)
    sys.stdout.flush()
    os._exit(0)  # evita el segfault de quickfix al destruir objetos C++ en el exit
