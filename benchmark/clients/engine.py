"""
Fábrica de sesiones y despacho por patrón de carga.

`build_session` arma la sesión del transporte según protocolo/TLS/persistencia.
`run_cell` corre una celda (una combinación protocolo × escenario × config) con
el motor que corresponde al patrón (open_rate vs closed-loop) y devuelve las
muestras más metadatos (si se abortó y por qué).
"""

from __future__ import annotations

from dataclasses import dataclass

from benchmark.clients.driver import (
    Ctx, AbortCell, run_closed_loop, run_open_rate,
)
from benchmark.clients.http_client import HttpSession, DEFAULT_RETRIES
from benchmark.clients.ws_client import WsSession
from benchmark.clients.fix_client import FixSession


@dataclass
class CellResult:
    samples: list
    aborted: bool = False
    reason: str = ""


def build_session(protocol: str, *, port=None, cfg_path=None, tls=False,
                  persistent=True, concurrency=1, retries=DEFAULT_RETRIES):
    if protocol in ("http", "http2"):
        use_tls = tls or protocol == "http2"
        scheme = "https" if use_tls else "http"
        base = f"{scheme}://127.0.0.1:{port}"
        return HttpSession(
            base_url=base, http2=(protocol == "http2"), tls=use_tls,
            persistent=persistent, concurrency=concurrency, retries=retries,
        )
    if protocol == "ws":
        scheme = "wss" if tls else "ws"
        return WsSession(url=f"{scheme}://127.0.0.1:{port}", tls=tls,
                         persistent=persistent)
    if protocol == "fix":
        return FixSession(cfg_path=cfg_path, tls=tls)
    raise ValueError(f"protocolo desconocido: {protocol}")


async def run_cell(session, scenario, ctx: Ctx, *, rate: float | None = None) -> CellResult:
    await session.start()
    try:
        if scenario.pattern == "open_rate":
            if not rate or rate <= 0:
                raise ValueError("open_rate requiere una tasa λ calibrada")
            try:
                samples = await run_open_rate(session, scenario, ctx, rate)
                return CellResult(samples=samples)
            except AbortCell as e:
                return CellResult(samples=getattr(e, "samples", []),
                                  aborted=True, reason=str(e))
        else:  # lockstep / saturate → régimen cerrado
            samples = await run_closed_loop(session, scenario, ctx)
            return CellResult(samples=samples)
    finally:
        await session.stop()
