"""
Orquestador del banco de pruebas (versión con regímenes separados).

Flujo:
  1. Metadatos del host -> sidecar meta_<tag>.json (reproducibilidad).
  2. Calibración de capacidad (una vez): satura market_data_tput por protocolo en
     loopback-plain, mide msg/s y guarda config/calibration.json. La λ del modo
     latency (open-loop) se deriva como rate_fraction × capacidad, y queda FIJA
     para todas las condiciones (plain, TLS, netem) => latencia comparable entre
     RTTs.
  3. Por protocolo: levanta el servidor y corre las celdas (escenario × variante
     × repetición) con el motor que corresponde al patrón.
  4. fault_injection se maneja aparte (corta/reinicia el servidor).
  5. Escribe raw_<tag>.csv, summary_<tag>.csv y (si hay) faults_<tag>.csv.

Uso:
  python -m benchmark.runner --tag loopback-plain
  python -m benchmark.runner --tag loopback-tls --tls
  python -m benchmark.runner --quick               # smoke
  python -m benchmark.runner --calibrate           # fuerza recalibración
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import math
import os
import statistics
import socket
import subprocess
import sys
import tempfile
import time
from datetime import datetime

from benchmark.common import CSV_FIELDS, free_port, host_metadata, now_ns
from benchmark.scenarios import SCENARIOS, DEFAULT_ORDER, get_scenario, scaled
from benchmark import fix_config as fc
from benchmark.clients.driver import COUNTER_FIELDS, Ctx
from benchmark.clients.engine import build_session, run_cell
from benchmark.clients import fix_client

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS_DIR = os.path.join(ROOT, "results")
CALIB_PATH = os.path.join(ROOT, "config", "calibration.json")

ALL_PROTOCOLS = ["fix", "http", "http2", "ws"]

# Parámetros de la fase de calibración.
CALIB_CONCURRENCY = 16
CALIB_MESSAGES = 4000
CALIB_WARMUP = 500


# ---------------------------------------------------------------------------
# Procesos / readiness
# ---------------------------------------------------------------------------

def _wait_tcp(host, port, timeout=15.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.1)
    return False


def _wait_http(url, timeout=15.0, http2=False, tls=False) -> bool:
    import httpx

    deadline = time.time() + timeout
    kw = dict(timeout=0.5, http2=http2)
    if tls or http2:
        kw["verify"] = False
    while time.time() < deadline:
        try:
            with httpx.Client(**kw) as c:
                if c.get(url).status_code == 200:
                    return True
        except Exception:
            time.sleep(0.1)
    return False


def _spawn(module, *args) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-m", module, *args],
        cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


def _stop(proc):
    if proc and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


def _start_server(proto, port, tls, *, persistent=False, store_dir=None):
    """Levanta el servidor de eco de `proto` y devuelve (proc, cfg_initiator)."""
    cfg_init = None
    if proto == "http":
        args = ["--port", str(port)] + (["--tls"] if tls else [])
        proc = _spawn("benchmark.servers.http_server", *args)
        ready = _wait_http(f"{'https' if tls else 'http'}://127.0.0.1:{port}/health", tls=tls)
    elif proto == "http2":
        proc = _spawn("benchmark.servers.http2_server", "--port", str(port))
        ready = _wait_http(f"https://127.0.0.1:{port}/health", http2=True, tls=True)
    elif proto == "ws":
        args = ["--port", str(port)] + (["--tls"] if tls else [])
        proc = _spawn("benchmark.servers.ws_server", *args)
        ready = _wait_tcp("127.0.0.1", port)
    elif proto == "fix":
        tmp = tempfile.mkdtemp(prefix="fixcli_")
        cert = key = ca = None
        if tls:
            from benchmark.tls import ensure_cert
            cert, key = ensure_cert()
            ca = cert
        fc.write_acceptor_cfg(os.path.join(tmp, "acc.cfg"), port, tls=tls, cert=cert,
                              key=key, persistent=persistent, store_dir=store_dir)
        cfg_init = fc.write_initiator_cfg(os.path.join(tmp, "init.cfg"), port, tls=tls,
                                          ca=ca, persistent=persistent,
                                          store_dir=(store_dir + "_cli") if store_dir else None)
        acc_args = ["--port", str(port), "--cfg", os.path.join(tmp, "acc.cfg")]
        if tls:
            acc_args.append("--tls")
        if persistent:
            acc_args += ["--persistent", "--store-dir", store_dir]
        proc = _spawn("benchmark.servers.fix_server", *acc_args)
        ready = _wait_tcp("127.0.0.1", port)
    else:
        raise ValueError(proto)
    return proc, cfg_init, ready


# ---------------------------------------------------------------------------
# Calibración de capacidad (una sola vez)
# ---------------------------------------------------------------------------

def _throughput(samples) -> float:
    useful = [s for s in samples if not s.is_warmup and not s.error]
    if len(useful) < 2:
        return 0.0
    t_send = [s.t_send_ns for s in useful]
    t_recv = [s.t_recv_ns for s in useful]
    span = max(max(t_recv) - min(t_send), 1)
    return len(useful) / (span / 1e9)


def calibrate(protocols, log, reps=1) -> tuple[dict, dict]:
    """Mide la capacidad (msg/s) de cada protocolo saturando en loopback-plain;
    con reps > 1 devuelve la mediana y los valores por repetición."""
    from dataclasses import replace

    base = get_scenario("market_data_tput")
    scen = replace(base, n_messages=CALIB_MESSAGES, warmup_messages=CALIB_WARMUP,
                   concurrency_levels=[CALIB_CONCURRENCY], repetitions=1)
    cap, per_rep = {}, {}
    for proto in protocols:
        port = free_port()
        proc, cfg_init, ready = _start_server(proto, port, tls=False)
        if not ready:
            log(f"  [!] calibración {proto}: servidor no listo")
            _stop(proc)
            continue
        try:
            tls_flag = 1 if proto == "http2" else 0
            ctx = Ctx(protocol=proto, scenario=scen.name, mode="throughput",
                      tls=tls_flag, repetition=0, seed=0, concurrency=CALIB_CONCURRENCY,
                      msg_size=scen.msg_size, persistent=1)
            per_rep[proto] = []
            for rep in range(reps):
                session = build_session(proto, port=port, cfg_path=cfg_init, tls=False,
                                        persistent=True, concurrency=CALIB_CONCURRENCY)
                res = asyncio.run(run_cell(session, scen, replace(ctx, repetition=rep)))
                per_rep[proto].append(_throughput(res.samples))
            cap[proto] = statistics.median(per_rep[proto])
            log(f"  {proto}: capacidad ≈ {cap[proto]:.0f} msg/s "
                f"(reps: {', '.join(f'{v:.0f}' for v in per_rep[proto])})")
        finally:
            _stop(proc)
    return cap, per_rep


def load_or_calibrate(protocols, force, log, path=CALIB_PATH, reps=1) -> dict:
    if not force and os.path.exists(path):
        with open(path) as f:
            data = json.load(f)
        cap = data.get("capacity_msg_s", {})
        if all(p in cap for p in protocols):
            log(f"== Calibración cargada de {path}")
            return cap
    log(f"== Calibrando capacidad (loopback-plain, {reps} rep., conc {CALIB_CONCURRENCY})...")
    cap, per_rep = calibrate(protocols, log, reps)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump({"capacity_msg_s": cap, "capacity_per_rep_msg_s": per_rep,
                   "rate_fraction": get_scenario("market_data_lat").rate_fraction,
                   "concurrency": CALIB_CONCURRENCY, "messages": CALIB_MESSAGES,
                   "warmup": CALIB_WARMUP, "reps": reps, "meta": host_metadata(),
                   "calibrated_at": datetime.now().isoformat()}, f, indent=2)
    log(f"== Calibración guardada en {path}")
    return cap


# ---------------------------------------------------------------------------
# Ejecución de escenarios normales
# ---------------------------------------------------------------------------

def http_pool_size(rate: float, rtt_ms: float) -> int:
    """Conexiones HTTP/1.1 para open-loop: λ·RTT en vuelo (Little), ×2 de margen
    para jitter y conexiones nuevas, +2. Sin red emulada queda en 2 (como antes)."""
    return 2 * math.ceil(rate * rtt_ms / 1000.0) + 2


CELL_FIELDS = ["protocol", "scenario", "mode", "tls", "repetition", "concurrency",
               "msg_size", "persistent", "target_rate"]


def run_protocol(proto, scenarios, tls, capacity, writer, log, counters_writer,
                 rtt_ms=0.0, pools=None):
    port = free_port()
    proc, cfg_init, ready = _start_server(proto, port, tls=tls)
    if not ready:
        log(f"  [!] servidor {proto} no quedó listo; se omite")
        _stop(proc)
        return
    proto_tls = 1 if (tls or proto == "http2") else 0
    try:
        for scen in scenarios:
            rate = None
            if scen.pattern == "open_rate":
                cap = capacity.get(proto, 0)
                rate = scen.rate_fraction * cap
                if rate <= 0:
                    log(f"  - {proto}/{scen.name}: sin capacidad calibrada, se omite")
                    continue
            pool = http_pool_size(rate, rtt_ms) if (proto == "http" and rate) else None
            if pool and pools is not None:
                pools[f"{proto}/{scen.name}"] = pool
            for conc, persistent in scen.variants():
                if proto == "fix" and persistent == 0:
                    log(f"  - {proto}/{scen.name} c={conc} persist=0: omitido (FIX)")
                    continue
                for rep in range(scen.repetitions):
                    t0 = time.time()
                    ctx = Ctx(protocol=proto, scenario=scen.name, mode=scen.mode,
                              tls=proto_tls, repetition=rep, seed=1000 + rep,
                              concurrency=conc, msg_size=scen.msg_size,
                              persistent=persistent,
                              target_rate=(rate or 0.0))
                    session = build_session(proto, port=port, cfg_path=cfg_init,
                                            tls=tls, persistent=bool(persistent),
                                            concurrency=conc, pool=pool)
                    res = asyncio.run(run_cell(session, scen, ctx, rate=rate))
                    for s in res.samples:
                        writer.writerow(s.as_row())
                    counters_writer.writerow({
                        **{k: getattr(ctx, k) for k in CELL_FIELDS},
                        "aborted": int(res.aborted), **session.counters})
                    errs = sum(s.error for s in res.samples)
                    note = f" ABORTADA ({res.reason})" if res.aborted else ""
                    log(f"  - {proto}/{scen.name} c={conc} p={persistent} "
                        f"rep={rep}: {len(res.samples)} msgs, {errs} err, "
                        f"{time.time()-t0:.1f}s{note}")
    finally:
        _stop(proc)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(description="Banco de pruebas FIX/HTTP/HTTP2/WS")
    p.add_argument("--protocols", default=",".join(ALL_PROTOCOLS))
    p.add_argument("--scenarios", default=",".join(DEFAULT_ORDER))
    p.add_argument("--tls", action="store_true", help="correr las 4 pilas sobre TLS")
    p.add_argument("--quick", action="store_true")
    p.add_argument("--scale", type=float, default=0.02)
    p.add_argument("--calibrate", action="store_true", help="forzar recalibración")
    p.add_argument("--reps", type=int, default=None,
                   help="override de repeticiones (p.ej. 3 para corridas netem)")
    p.add_argument("--nmsg", type=int, default=None,
                   help="cap de mensajes útiles por celda (reduce netem; el RTT "
                        "domina, no hacen falta 10k muestras)")
    p.add_argument("--fix-variant", default=None, choices=sorted(fc.VARIANTS),
                   help="fault_injection con FIX sin recuperación (revisión)")
    p.add_argument("--calibration", default=CALIB_PATH, help="archivo de calibración")
    p.add_argument("--calib-reps", type=int, default=1)
    p.add_argument("--calibrate-only", action="store_true")
    p.add_argument("--rtt-ms", type=float, default=0.0,
                   help="RTT emulado (dimensiona el pool HTTP/1.1 de open-loop)")
    p.add_argument("--netem", default="", help="regla netem aplicada (solo se registra)")
    p.add_argument("--out", default=RESULTS_DIR)
    p.add_argument("--tag", default=None)
    args = p.parse_args()

    os.makedirs(args.out, exist_ok=True)
    stamp = args.tag or datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_path = os.path.join(args.out, f"raw_{stamp}.csv")

    protocols = [x.strip() for x in args.protocols.split(",") if x.strip()]
    scen_names = [x.strip() for x in args.scenarios.split(",") if x.strip()]
    from dataclasses import replace as _replace
    scenarios = []
    for n in scen_names:
        s = scaled(n, args.scale) if args.quick else get_scenario(n)
        if args.reps is not None:
            s = _replace(s, repetitions=args.reps)
        if args.nmsg is not None:
            s = _replace(s, n_messages=min(s.n_messages, args.nmsg),
                         warmup_messages=min(s.warmup_messages, max(args.nmsg // 10, 20)))
        scenarios.append(s)

    def log(m):
        print(m, flush=True)

    # Metadatos + sidecar.
    meta = host_metadata()
    meta.update({"tag": stamp, "tls": bool(args.tls), "quick": args.quick,
                 "scenarios": scen_names, "reps": args.reps, "nmsg": args.nmsg,
                 "fix_variant": args.fix_variant, "qdisc_lo": _qdisc("lo"),
                 "rtt_ms": args.rtt_ms, "netem": args.netem, "versions": _versions(),
                 "ws_compression": "desactivada (compression=None, cliente y servidor)",
                 "open_loop_timing": "preciso: sleep hasta 1,5 ms antes + sleep(0) hasta el instante",
                 "calibration": args.calibration})
    meta_path = os.path.join(args.out, f"meta_{stamp}.json")
    log(f"== tag={stamp} tls={args.tls} protocolos={protocols}")
    log(f"   host: {meta['cpu_model']} | kernel {meta['kernel']} | "
        f"gov={meta['cpu_governor']} | GIL={meta['python_gil']} | cpus={meta['cpu_affinity']}")

    # Calibración si hace falta (algún escenario open_rate).
    if args.calibrate_only:
        load_or_calibrate(protocols, True, log, args.calibration, args.calib_reps)
        return
    needs_calib = any(s.pattern == "open_rate" for s in scenarios)
    capacity = {}
    if needs_calib:
        capacity = load_or_calibrate(protocols, args.calibrate, log, args.calibration,
                                     args.calib_reps)
    pools = {}

    normal = [s for s in scenarios if not s.fault]
    faults = [s for s in scenarios if s.fault]

    counters_path = os.path.join(args.out, f"counters_{stamp}.csv")
    with open(raw_path, "w", newline="") as f, open(counters_path, "w", newline="") as cf:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        counters_writer = csv.DictWriter(cf, fieldnames=CELL_FIELDS + ["aborted"] + COUNTER_FIELDS)
        counters_writer.writeheader()
        for proto in protocols:
            if normal:
                log(f"[{proto}]")
                run_protocol(proto, normal, args.tls, capacity, writer, log, counters_writer,
                             args.rtt_ms, pools)
        # fault_injection (import diferido para no acoplar).
        if faults:
            from benchmark.fault import run_faults
            run_faults(protocols, faults, args.tls, capacity, writer,
                       args.out, stamp, log, fix_variant=args.fix_variant)

    meta.update({"capacity_msg_s": capacity, "http_pool": pools,
                 "rates_msg_s": {p: get_scenario("market_data_lat").rate_fraction * c
                                 for p, c in capacity.items()}})
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)
    _write_summary(raw_path, args.out, stamp, meta, log)
    log("== Listo")


def _versions() -> dict:
    from importlib.metadata import PackageNotFoundError, version
    out = {"python": sys.version.split()[0]}
    for pkg in ("quickfix", "websockets", "fastapi", "uvicorn", "hypercorn", "httpx",
                "httpcore", "h2", "numpy", "pandas"):
        try:
            out[pkg] = version(pkg)
        except PackageNotFoundError:
            out[pkg] = "no instalado"
    return out


def _qdisc(dev) -> str:
    try:
        return subprocess.run(["tc", "qdisc", "show", "dev", dev], capture_output=True,
                              text=True, timeout=5).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return "unknown"


def _write_summary(raw_path, out_dir, stamp, meta, log):
    import pandas as pd
    from benchmark.metrics import summarize_all

    df = pd.read_csv(raw_path)
    if df.empty:
        log("   (sin datos)")
        return
    summary = summarize_all(df)
    # Documentar host en el summary (reproducibilidad).
    for k in ("cpu_model", "kernel", "cpu_governor", "cpu_affinity", "python_gil", "tag"):
        summary[k] = meta.get(k)
    sum_path = os.path.join(out_dir, f"summary_{stamp}.csv")
    summary.to_csv(sum_path, index=False)
    log(f"   resumen -> {sum_path}")


if __name__ == "__main__":
    main()
