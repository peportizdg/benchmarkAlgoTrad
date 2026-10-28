"""
Escenario de inyección de fallas.

Tráfico sostenido (open-loop, tasa moderada). A mitad de corrida se MATA el
proceso servidor (kill -9), se espera un tiempo de outage y se relanza EL MISMO
servidor (mismo puerto; para FIX, mismo FileStore en disco). Después se cuenta,
cotejando por identificador de mensaje (seq), no por conteo:

  - msgs_sent      : mensajes emitidos (sin warm-up)
  - msgs_lost      : emitidos que nunca recibieron eco
  - msgs_recovered : entregados cuyo seq viajó retransmitido (FIX: PossDupFlag=Y
                     tras un ResendRequest); recuperación real, no por ventana
  - recovery_time_ms : desde el kill hasta el primer eco de un mensaje programado
                     después del kill (= t_restore_from_kill_ms)
  - outage_ms      : duración real del corte

Indicadores agregados en la revisión (no cambian los anteriores):
  - sched_in_outage / delivered_sched_in_outage : programados (t_sched) dentro de
    [t_kill, t_up] y cuántos de ellos recibieron eco
  - recovered_by_retx : entregados cuyo seq viajó con PossDupFlag=Y (FIX)
  - contadores de transporte (driver.COUNTER_FIELDS) y t_restore_*_ms : desde
    t_kill / t_up hasta el primer eco de un mensaje programado después de t_kill

Expectativa: FIX recupera los mensajes perdidos; WS/HTTP recuperan el FLUJO al
reconectar pero PIERDEN los mensajes en vuelo (no tienen recuperación propia).
"""

from __future__ import annotations

import asyncio
import csv
import os
import subprocess
import sys
import tempfile
import time

from benchmark.common import free_port, now_ns
from benchmark import fix_config as fc
from benchmark.clients.driver import COUNTER_FIELDS, Ctx, run_open_rate
from benchmark.clients.engine import build_session

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTAGE_S = 3.0

FAULT_FIELDS = [
    "protocol", "tls", "scenario", "repetition", "target_rate",
    "msgs_sent", "msgs_lost", "msgs_recovered", "recovery_time_ms", "outage_ms",
    "fix_variant", "t_kill_ns", "t_up_ns", "sched_in_outage", "delivered_sched_in_outage",
    "recovered_by_retx", *COUNTER_FIELDS, "t_restore_from_kill_ms", "t_restore_from_up_ms",
]


def _spawn(cmd):
    return subprocess.Popen(cmd, cwd=ROOT, stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL)


def _stop(proc):
    if proc and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


def _wait_tcp(port, timeout=15.0):
    import socket
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.1)
    return False


def _build_server(proto, port, tls, tmp, overrides=None):
    """Devuelve (cmd, cfg_init, store_dir). El cmd se reutiliza para reiniciar
    con estado estable (mismo store para FIX)."""
    store_dir = None
    cfg_init = None
    if proto == "http":
        cmd = [sys.executable, "-m", "benchmark.servers.http_server", "--port", str(port)]
        if tls:
            cmd.append("--tls")
    elif proto == "http2":
        cmd = [sys.executable, "-m", "benchmark.servers.http2_server", "--port", str(port)]
    elif proto == "ws":
        cmd = [sys.executable, "-m", "benchmark.servers.ws_server", "--port", str(port)]
        if tls:
            cmd.append("--tls")
    elif proto == "fix":
        store_dir = os.path.join(tmp, "store")
        os.makedirs(store_dir, exist_ok=True)
        cert = key = ca = None
        if tls:
            from benchmark.tls import ensure_cert
            cert, key = ensure_cert()
            ca = cert
        acc = os.path.join(tmp, "acc.cfg")
        fc.write_acceptor_cfg(acc, port, tls=tls, cert=cert, key=key,
                              persistent=True, store_dir=store_dir, overrides=overrides)
        cfg_init = fc.write_initiator_cfg(os.path.join(tmp, "init.cfg"), port, tls=tls,
                                          ca=ca, persistent=True,
                                          store_dir=store_dir + "_cli", overrides=overrides)
        cmd = [sys.executable, "-m", "benchmark.servers.fix_server", "--port", str(port),
               "--cfg", acc]
        if fc.uses_file_store(True, overrides):
            cmd += ["--persistent", "--store-dir", store_dir]
        if tls:
            cmd.append("--tls")
    else:
        raise ValueError(proto)
    return cmd, cfg_init, store_dir


async def _drive(session, scen, ctx, rate, holder, kill_at, restart_fn):
    state = {"t_kill": 0, "t_up": 0}

    async def injector():
        await asyncio.sleep(kill_at)
        holder["proc"].kill()          # crash duro
        state["t_kill"] = now_ns()
        await asyncio.sleep(OUTAGE_S)
        holder["proc"] = await asyncio.to_thread(restart_fn)
        state["t_up"] = now_ns()

    await session.start()
    try:
        results = await asyncio.gather(
            run_open_rate(session, scen, ctx, rate, abort_on_backlog=False),
            injector(),
        )
    finally:
        await session.stop()
    return results[0], state


def _run_one(proto, scen, rate, tls, rep, log, fix_variant=None):
    port = free_port()
    tmp = tempfile.mkdtemp(prefix=f"fault_{proto}_")
    overrides = fc.VARIANTS[fix_variant] if fix_variant else None
    cmd, cfg_init, store_dir = _build_server(proto, port, tls, tmp, overrides)
    proc = _spawn(cmd)
    if not _wait_tcp(port):
        _stop(proc)
        log(f"  [!] fault {proto}: servidor no listo")
        return None, []

    def restart_fn():
        np = _spawn(cmd)
        _wait_tcp(port)
        return np

    holder = {"proc": proc}
    proto_tls = 1 if (tls or proto == "http2") else 0
    ctx = Ctx(protocol=proto, scenario=scen.name, mode="latency", tls=proto_tls,
              repetition=rep, seed=1000 + rep, concurrency=1, msg_size=scen.msg_size,
              persistent=1, target_rate=rate)
    # retries=0: HTTP/WS deben EXPONER la pérdida, no enmascararla reintentando.
    session = build_session(proto, port=port, cfg_path=cfg_init, tls=tls,
                            persistent=True, concurrency=1, retries=0)

    total = scen.warmup_messages + scen.n_messages
    expected_dur = total / rate if rate > 0 else 10.0
    kill_at = scen.fault_at_fraction * expected_dur

    try:
        samples, state = asyncio.run(_drive(session, scen, ctx, rate, holder,
                                            kill_at, restart_fn))
    finally:
        _stop(holder["proc"])

    row = _account(proto, proto_tls, scen, rep, rate, samples, session, state)
    row["fix_variant"] = (fix_variant or "original") if proto == "fix" else ""
    return row, samples


def _account(proto, proto_tls, scen, rep, rate, samples, session, state):
    t_up = state.get("t_up", 0)
    t_kill = state.get("t_kill", 0)

    sent = {s.seq for s in samples if not s.is_warmup and s.seq >= 0}
    echoed = getattr(session, "echoed_seqs", set())
    lost_final = sent - echoed            # emitidos que nunca recibieron eco

    outage_ms = (t_up - t_kill) / 1e6 if (t_up and t_kill) else float("nan")

    # Revisión: ventana por instante PROGRAMADO (t_sched existe también en los
    # errores, cuyo t_send queda en 0) y recuperación atribuible a retransmisión.
    useful = [s for s in samples if not s.is_warmup and s.seq >= 0]
    sched_out = {s.seq for s in useful if t_kill and t_up and t_kill <= s.t_sched_ns <= t_up}
    retx = getattr(session, "retx_seqs", set())
    first_after = [s.t_recv_ns for s in useful
                   if not s.error and t_kill and s.t_sched_ns >= t_kill]
    t_first = min(first_after) if first_after else 0
    t_restore = round((t_first - t_kill) / 1e6, 3) if t_first else ""

    return {
        "protocol": proto, "tls": proto_tls, "scenario": scen.name,
        "repetition": rep, "target_rate": round(rate, 2),
        "msgs_sent": len(sent), "msgs_lost": len(lost_final),
        "msgs_recovered": len(sent & echoed & retx),
        "recovery_time_ms": t_restore,
        "outage_ms": round(outage_ms, 1) if outage_ms == outage_ms else "",
        "t_kill_ns": t_kill, "t_up_ns": t_up,
        "sched_in_outage": len(sched_out),
        "delivered_sched_in_outage": len(sched_out & echoed),
        "recovered_by_retx": len(sent & echoed & retx),
        **getattr(session, "counters", {}),
        "t_restore_from_kill_ms": t_restore,
        "t_restore_from_up_ms": round((t_first - t_up) / 1e6, 3) if t_first and t_up else "",
    }


def run_faults(protocols, fault_scenarios, tls, capacity, writer, out_dir, stamp, log,
               fix_variant=None):
    faults_path = os.path.join(out_dir, f"faults_{stamp}.csv")
    with open(faults_path, "w", newline="") as f:
        fw = csv.DictWriter(f, fieldnames=FAULT_FIELDS)
        fw.writeheader()
        for scen in fault_scenarios:
            log(f"[fault_injection]")
            for proto in protocols:
                # Tasa fija (moderada) si está definida; si no, fracción de la
                # capacidad calibrada. La fija mantiene las corridas acotadas y
                # comparables entre protocolos con capacidades muy distintas.
                if getattr(scen, "fixed_rate", 0) and scen.fixed_rate > 0:
                    rate = scen.fixed_rate
                else:
                    rate = scen.rate_fraction * capacity.get(proto, 0)
                if rate <= 0:
                    log(f"  - {proto}: sin tasa, se omite")
                    continue
                for rep in range(scen.repetitions):
                    t0 = time.time()
                    row, samples = _run_one(proto, scen, rate, tls, rep, log, fix_variant)
                    if row is None:
                        continue
                    for s in samples:
                        writer.writerow(s.as_row())
                    fw.writerow(row)
                    log(f"  - {proto} rep={rep}: sent={row['msgs_sent']} "
                        f"lost={row['msgs_lost']} recovered={row['msgs_recovered']} "
                        f"outage={row['outage_ms']}ms recovery={row['recovery_time_ms']}ms "
                        f"({time.time()-t0:.1f}s)")
    log(f"   fallas -> {faults_path}")
