"""
Núcleo compartido del banco de pruebas.

Define el formato de mensaje común a los tres transportes (FIX, HTTP, WebSocket)
y los relojes de alta resolución usados para estampar timestamps.

Reloj
-----
Usamos ``time.monotonic_ns()`` que en Linux mapea a ``CLOCK_MONOTONIC``. A
diferencia de ``time.perf_counter()`` (cuyo punto de referencia es indefinido y
distinto por proceso), ``CLOCK_MONOTONIC`` comparte la misma referencia entre
procesos del mismo host. Esto permite medir latencia *one-way* (un solo sentido)
entre el cliente y el servidor cuando corren en la misma máquina, además del RTT
(round-trip) que no depende de relojes compartidos.

ASUNCIÓN: cliente y servidor corren en el MISMO host (entorno local controlado).
Si se distribuye a varias máquinas, las métricas one-way dejan de ser válidas;
el RTT sigue siendo correcto.

Formato de mensaje (payload binario, fijo + relleno)
----------------------------------------------------
Offset  Tamaño  Campo
0       8       seq            (uint64, little-endian) número de secuencia
8       8       t_send_ns      (uint64) monotonic_ns en el envío del cliente
16      8       t_srv_recv_ns  (uint64) monotonic_ns al recibir el servidor (lo
                               estampa el servidor en el eco; 0 si no aplica)
24      N        relleno        bytes para alcanzar el tamaño objetivo

El servidor reescribe los bytes 16:24 con su timestamp de recepción y devuelve
el mismo buffer. El cliente, al recibir, calcula RTT y one-way.
"""

from __future__ import annotations

import os
import struct
import time
from dataclasses import dataclass

# ---------------------------------------------------------------------------
# Reloj
# ---------------------------------------------------------------------------

# Alias explícito para dejar claro qué reloj se usa en todo el banco.
now_ns = time.monotonic_ns

HEADER_FMT = "<QQQ"  # seq, t_send_ns, t_srv_recv_ns
HEADER_SIZE = struct.calcsize(HEADER_FMT)  # 24 bytes

# Tamaño mínimo de mensaje = header. Cualquier tamaño objetivo menor se eleva.
MIN_SIZE = HEADER_SIZE


def build_payload(seq: int, size: int, t_send_ns: int | None = None) -> bytes:
    """Construye un mensaje de ``size`` bytes con header + relleno.

    El relleno es pseudoaleatorio pero determinístico por ``seq`` para evitar
    que la compresión de transporte (si la hubiera) sesgue los resultados.
    """
    if size < MIN_SIZE:
        size = MIN_SIZE
    if t_send_ns is None:
        t_send_ns = now_ns()
    header = struct.pack(HEADER_FMT, seq, t_send_ns, 0)
    pad_len = size - HEADER_SIZE
    if pad_len <= 0:
        return header
    # Relleno determinístico y barato (no aleatorio criptográfico).
    pad = (bytes((seq + i) & 0xFF for i in range(min(pad_len, 64))) * (pad_len // 64 + 1))[:pad_len]
    return header + pad


def stamp_server_recv(buf: bytes | bytearray, t_srv_recv_ns: int | None = None) -> bytes:
    """Reescribe el campo t_srv_recv_ns dentro del buffer (lado servidor)."""
    if t_srv_recv_ns is None:
        t_srv_recv_ns = now_ns()
    mv = bytearray(buf)
    struct.pack_into("<Q", mv, 16, t_srv_recv_ns)
    return bytes(mv)


@dataclass
class ParsedHeader:
    seq: int
    t_send_ns: int
    t_srv_recv_ns: int


def parse_header(buf: bytes) -> ParsedHeader:
    seq, t_send_ns, t_srv_recv_ns = struct.unpack(HEADER_FMT, buf[:HEADER_SIZE])
    return ParsedHeader(seq, t_send_ns, t_srv_recv_ns)


# ---------------------------------------------------------------------------
# Esquema CSV de salida (datos crudos por mensaje)
# ---------------------------------------------------------------------------

CSV_FIELDS = [
    "protocol",     # fix | http | http2 | ws
    "scenario",     # market_data | order_routing | post_trade | fault_injection
    "mode",         # latency (open-loop, tasa fija) | throughput (closed-loop, satura)
    "tls",          # 1 si el transporte va cifrado (TLS), 0 texto plano
    "repetition",   # índice de repetición
    "seed",         # semilla de la repetición (reproducibilidad)
    "concurrency",  # mensajes en vuelo (throughput) o emisores (latency)
    "msg_size",     # tamaño de mensaje en bytes
    "persistent",   # 1 si conexión persistente, 0 si se reabre por mensaje
    "target_rate",  # λ ofrecida en msg/s (modo latency); 0 en throughput/lockstep
    "seq",          # número de secuencia global (id de mensaje para cotejo)
    "is_warmup",    # 1 si pertenece a la fase de warm-up
    "t_sched_ns",   # instante PROGRAMADO de envío (open-loop); 0 si no aplica
    "t_send_ns",    # instante REAL de envío (cliente)
    "t_recv_ns",    # timestamp de recepción de la respuesta (cliente)
    "t_srv_recv_ns",# timestamp de recepción en el servidor (0 si no aplica)
    "rtt_us",       # latencia (µs); en open-loop CORREGIDA por omisión coordinada
    "oneway_us",    # latencia un sentido cliente->servidor en us (NaN si no aplica)
    "send_lag_us",  # t_send - t_sched (µs); >0 = el emisor se atrasó (señal de carga)
    "error",        # 1 si hubo error en este mensaje
]


@dataclass
class Sample:
    protocol: str
    scenario: str
    seq: int
    is_warmup: int
    t_send_ns: int
    t_recv_ns: int
    t_srv_recv_ns: int
    mode: str = "latency"
    tls: int = 0
    repetition: int = 0
    seed: int = 0
    concurrency: int = 1
    msg_size: int = 0
    persistent: int = 1
    target_rate: float = 0.0
    # Instante programado (open-loop). Si es >0, la latencia se calcula contra
    # este instante y NO contra t_send_ns: es la corrección de omisión
    # coordinada (Gil Tene / Schroeder et al., "Open vs Closed", NSDI'06).
    t_sched_ns: int = 0
    error: int = 0

    @property
    def _t_ref(self) -> int:
        return self.t_sched_ns if self.t_sched_ns else self.t_send_ns

    @property
    def rtt_us(self) -> float:
        if self.error:
            return float("nan")
        return (self.t_recv_ns - self._t_ref) / 1000.0

    @property
    def oneway_us(self) -> float:
        if self.error or not self.t_srv_recv_ns:
            return float("nan")
        return (self.t_srv_recv_ns - self._t_ref) / 1000.0

    @property
    def send_lag_us(self) -> float:
        if not self.t_sched_ns or self.error:
            return float("nan")
        return (self.t_send_ns - self.t_sched_ns) / 1000.0

    def as_row(self) -> dict:
        return {
            "protocol": self.protocol,
            "scenario": self.scenario,
            "mode": self.mode,
            "tls": self.tls,
            "repetition": self.repetition,
            "seed": self.seed,
            "concurrency": self.concurrency,
            "msg_size": self.msg_size,
            "persistent": self.persistent,
            "target_rate": self.target_rate,
            "seq": self.seq,
            "is_warmup": self.is_warmup,
            "t_sched_ns": self.t_sched_ns,
            "t_send_ns": self.t_send_ns,
            "t_recv_ns": self.t_recv_ns,
            "t_srv_recv_ns": self.t_srv_recv_ns,
            "rtt_us": self.rtt_us,
            "oneway_us": self.oneway_us,
            "send_lag_us": self.send_lag_us,
            "error": self.error,
        }


# ---------------------------------------------------------------------------
# Utilidades de red
# ---------------------------------------------------------------------------

def free_port() -> int:
    """Devuelve un puerto TCP libre (para evitar colisiones entre corridas)."""
    import socket

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def host_metadata() -> dict:
    """Metadatos del host para reproducibilidad (van al sidecar y al summary)."""
    import platform
    import subprocess

    def _cpu_model() -> str:
        try:
            with open("/proc/cpuinfo") as f:
                for line in f:
                    if line.startswith("model name"):
                        return line.split(":", 1)[1].strip()
        except OSError:
            pass
        return platform.processor() or "unknown"

    def _governor() -> str:
        try:
            with open("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor") as f:
                return f.read().strip()
        except OSError:
            return "unknown"

    def _taskset() -> str:
        # Afinidad del proceso actual (si se lanzó con taskset queda reflejada).
        try:
            return ",".join(map(str, sorted(os.sched_getaffinity(0))))
        except (AttributeError, OSError):
            return "unknown"

    return {
        "cpu_model": _cpu_model(),
        "cpu_count": os.cpu_count(),
        "cpu_governor": _governor(),
        "cpu_affinity": _taskset(),
        "kernel": platform.release(),
        "python": platform.python_version(),
        "python_gil": _gil_enabled(),
    }


def _gil_enabled() -> str:
    try:
        import sys

        # Python 3.13+: sys._is_gil_enabled()
        f = getattr(sys, "_is_gil_enabled", None)
        if f is not None:
            return "enabled" if f() else "disabled"
    except Exception:
        pass
    return "unknown"


def env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))
