"""
Generación de configs de QuickFIX (acceptor / initiator), con variantes:

  - texto plano (SocketAcceptor/Initiator) o TLS (SSLSocketAcceptor/Initiator).
  - sesión efímera (MemoryStore, ResetOn*=Y) para latency/throughput, o sesión
    PERSISTENTE (FileStore en disco, ResetOn*=N, PersistMessages=Y) para el
    escenario de fallas: así, tras reiniciar el proceso servidor, QuickFIX
    recupera los seqnums y puede reenviar mensajes vía Resend Request.

Sin diccionario de datos (UseDataDictionary=N): usamos tags de usuario ≥ 20000.
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


def _common(persistent: bool, store_dir: str | None) -> str:
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
                       persistent=False, store_dir=None) -> str:
    body = f"""[DEFAULT]
ConnectionType=acceptor
{_common(persistent, store_dir)}"""
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
                        persistent=False, store_dir=None) -> str:
    body = f"""[DEFAULT]
ConnectionType=initiator
{_common(persistent, store_dir)}"""
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
