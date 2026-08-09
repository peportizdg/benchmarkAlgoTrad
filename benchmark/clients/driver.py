"""
Motor de generación de carga, común a los cuatro transportes.

Abstracción `Session`
---------------------
Cada transporte expone una sesión async con:
    async start(), async stop(), async roundtrip(seq, size) -> RTResult
`roundtrip` puede invocarse concurrentemente muchas veces sobre la misma sesión;
el transporte se encarga de demultiplexar las respuestas por `seq` (necesario en
WS y FIX, que multiplexan sobre una sola conexión).

Regímenes de carga
------------------
- run_closed_loop:  N workers, cada uno hace send→espera→send. Los mensajes en
  vuelo quedan acotados a `concurrency` (CONTRAPRESIÓN real, igual en los cuatro
  transportes). Sirve para:
    * mode="throughput" (saturar y medir capacidad; concurrency alto),
    * mode="latency" patrón lockstep (concurrency bajo; conc=1 = latencia pura).

- run_open_rate:  régimen ABIERTO. Dispara a tasa λ FIJA sin esperar la
  respuesta. La latencia se mide contra el instante PROGRAMADO (corrección de
  omisión coordinada). Aborta la celda si el backlog crece de forma sostenida
  (señal de que λ satura y la medición dejaría de ser válida).
"""

from __future__ import annotations

import asyncio
import itertools
from dataclasses import dataclass

from benchmark.common import Sample, parse_header, now_ns


@dataclass
class RTResult:
    """Resultado de un round-trip, ya normalizado entre transportes."""
    t_send_ns: int
    t_srv_recv_ns: int
    t_recv_ns: int
    ok: bool = True


@dataclass
class Ctx:
    """Contexto de una celda (constante para todas sus muestras)."""
    protocol: str
    scenario: str
    mode: str
    tls: int
    repetition: int
    seed: int
    concurrency: int
    msg_size: int
    persistent: int
    target_rate: float = 0.0


class AbortCell(Exception):
    """La celda se aborta (p.ej. λ satura en modo open-loop)."""


def new_seq_counter():
    return itertools.count(0)


def result_from_bytes(echoed: bytes, t_recv_ns: int) -> RTResult:
    """Normaliza el payload binario eco (HTTP/HTTP2/WS) a RTResult."""
    if echoed is None or len(echoed) < 24:
        return RTResult(0, 0, 0, ok=False)
    h = parse_header(echoed)
    return RTResult(h.t_send_ns, h.t_srv_recv_ns, t_recv_ns, ok=True)


def _sample(ctx: Ctx, seq: int, is_warmup: int, res: RTResult, t_sched_ns: int) -> Sample:
    return Sample(
        protocol=ctx.protocol, scenario=ctx.scenario, mode=ctx.mode, tls=ctx.tls,
        repetition=ctx.repetition, seed=ctx.seed, concurrency=ctx.concurrency,
        msg_size=ctx.msg_size, persistent=ctx.persistent, target_rate=ctx.target_rate,
        seq=seq, is_warmup=is_warmup, t_sched_ns=t_sched_ns,
        t_send_ns=res.t_send_ns, t_recv_ns=res.t_recv_ns,
        t_srv_recv_ns=res.t_srv_recv_ns, error=0 if res.ok else 1,
    )


# ---------------------------------------------------------------------------
# Régimen cerrado (closed-loop): lockstep y saturate
# ---------------------------------------------------------------------------

async def run_closed_loop(session, scenario, ctx: Ctx) -> list:
    """N workers; cada uno serial. In-flight acotado a concurrency.

    El total de mensajes es POR CELDA (warmup + n_messages), repartido entre los
    `concurrency` workers. Es decir: la cantidad de mensajes NO se multiplica por
    la concurrencia. Así el volumen (y el tiempo) quedan acotados sin importar el
    nivel de concurrencia, y se siguen obteniendo ~n_messages muestras útiles.
    """
    seq = new_seq_counter()
    samples: list = []
    conc = max(ctx.concurrency, 1)
    # Reparto (ceil) del total y del warmup entre los workers.
    per_worker = -(-(scenario.warmup_messages + scenario.n_messages) // conc)
    warm_per_worker = -(-scenario.warmup_messages // conc)

    async def worker():
        for i in range(per_worker):
            s = next(seq)
            is_warmup = 1 if i < warm_per_worker else 0
            try:
                res = await session.roundtrip(s, scenario.msg_size)
            except Exception:
                res = RTResult(0, 0, 0, ok=False)
            samples.append(_sample(ctx, s, is_warmup, res, t_sched_ns=0))

    await asyncio.gather(*(worker() for _ in range(conc)))
    return samples


# ---------------------------------------------------------------------------
# Régimen abierto (open-loop): open_rate con corrección de omisión coordinada
# ---------------------------------------------------------------------------

async def run_open_rate(session, scenario, ctx: Ctx, rate: float,
                        abort_on_backlog: bool = True) -> list:
    """Dispara a tasa λ=`rate` msg/s sin esperar respuesta (open-loop).

    - Latencia corregida por omisión coordinada: se guarda t_sched (instante
      programado) y la latencia se calcula contra él en :class:`Sample`.
    - Aborta (AbortCell) si el backlog (enviados-recibidos) crece de forma
      monótona sostenida => λ satura y la celda no es válida.
    """
    if rate <= 0:
        raise ValueError("open_rate requiere rate > 0")

    seq = new_seq_counter()
    samples: list = []
    total = scenario.warmup_messages + scenario.n_messages
    interval = 1.0 / rate

    pending = 0
    received = 0
    aborted = {"flag": False, "reason": ""}
    loop = asyncio.get_event_loop()

    async def fire(seq_i: int, is_warmup: int, t_sched_ns: int):
        nonlocal pending, received
        try:
            res = await session.roundtrip(seq_i, scenario.msg_size)
        except Exception:
            res = RTResult(0, 0, 0, ok=False)
        pending -= 1
        received += 1
        samples.append(_sample(ctx, seq_i, is_warmup, res, t_sched_ns))

    async def monitor():
        # Vigila el backlog; si crece de forma monótona en varias ventanas
        # consecutivas, aborta (la tasa satura).
        nonlocal pending
        streak = 0
        last = 0
        # Umbral: más de 2 s de tráfico acumulado en vuelo.
        hard_cap = max(int(rate * 2.0), 200)
        while not aborted["flag"]:
            await asyncio.sleep(0.25)
            cur = pending
            if cur > hard_cap:
                streak += 1
            elif cur > last:
                streak += 1
            else:
                streak = 0
            last = cur
            if streak >= 8:  # ~2 s de crecimiento sostenido
                aborted["flag"] = True
                aborted["reason"] = f"backlog creciente sostenido (pending={cur})"
                return

    tasks: list = []
    mon = asyncio.create_task(monitor()) if abort_on_backlog else None
    t0 = loop.time()
    try:
        for i in range(total):
            target = t0 + i * interval
            delay = target - loop.time()
            if delay > 0:
                await asyncio.sleep(delay)
            if aborted["flag"]:
                exc = AbortCell(aborted["reason"])
                exc.samples = samples
                raise exc
            s = next(seq)
            is_warmup = 1 if i < scenario.warmup_messages else 0
            t_sched_ns = now_ns()  # instante programado ≈ ahora (schedule fijo)
            pending += 1
            tasks.append(asyncio.create_task(fire(s, is_warmup, t_sched_ns)))
        # Drenar respuestas en vuelo con gracia.
        if tasks:
            await asyncio.wait(tasks, timeout=30.0)
    finally:
        aborted["flag"] = True
        if mon is not None:
            mon.cancel()
        for t in tasks:
            if not t.done():
                t.cancel()
    return samples
