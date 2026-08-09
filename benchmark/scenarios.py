"""
Definición de escenarios y de los DOS regímenes de carga.

El error metodológico que se corrige (ver README §Validez): medir latencia bajo
un generador de carga en **régimen abierto sin contrapresión** hace que, en
transportes que multiplexan sobre una conexión (FIX, WS), el mensaje entre a una
cola y `t_send` se estampe al encolar, no al atender. La latencia medida termina
siendo tiempo de permanencia en cola (tiende a duración/2). Ref.: Schroeder,
Wierman & Harchol-Balter, "Open Versus Closed: A Cautionary Tale", NSDI '06.

Solución: dos modos EXPLÍCITOS.

  - mode="latency"  (open-loop): llegadas programadas a tasa λ FIJA por debajo de
    la saturación (λ = rate_fraction × capacidad calibrada). La latencia se mide
    contra el instante PROGRAMADO (corrección de omisión coordinada). Solo se
    reportan percentiles de latencia y jitter. Patrones: "open_rate" o "lockstep"
    (este último, closed-loop con in-flight = concurrency, sin backlog).

  - mode="throughput" (closed-loop): se satura deliberadamente con los mensajes
    en vuelo acotados a `concurrency` por un semáforo (contrapresión real, igual
    en los cuatro transportes). Solo se reportan throughput y tasa de errores;
    NO se emiten percentiles de latencia.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace


@dataclass
class Scenario:
    name: str
    mode: str                  # "latency" | "throughput"
    pattern: str               # "open_rate" | "saturate" | "lockstep"
    msg_size: int
    n_messages: int            # mensajes útiles por celda (objetivo: ≥10k)
    warmup_messages: int
    concurrency_levels: list
    persistent_levels: list
    repetitions: int = 5
    trim_initial: int = 100
    latency_metric: str = "rtt_us"

    # Solo open_rate: λ = rate_fraction × capacidad (msg/s) calibrada del
    # escenario `calibrate_from` para ese protocolo.
    rate_fraction: float = 0.25
    calibrate_from: str | None = None

    # Solo fault_injection.
    fault: bool = False
    fault_at_fraction: float = 0.5   # cortar a mitad de la corrida
    fault_kind: str = "kill"          # "kill" (reinicia server) | "tcp_drop"
    grace_s: float = 5.0              # espera post-reconexión para contar recuperados
    fixed_rate: float = 0.0           # tasa fija (msg/s) para fault; 0 = usar capacidad

    def variants(self):
        for conc in self.concurrency_levels:
            for persistent in self.persistent_levels:
                yield conc, persistent


# --- Definiciones ------------------------------------------------------------
#
# n_messages y repetitions están en valores de PRODUCCIÓN (tesis). Para pruebas
# rápidas usar --quick (aplica scaled()).

SCENARIOS = {
    # (1) market_data en modo THROUGHPUT: satura para medir capacidad.
    "market_data_tput": Scenario(
        name="market_data_tput",
        mode="throughput",
        pattern="saturate",
        msg_size=128,
        n_messages=20000,
        warmup_messages=2000,
        concurrency_levels=[1, 4, 16, 64],
        persistent_levels=[1],
        repetitions=5,
        trim_initial=0,           # en throughput no se miden percentiles
        latency_metric="rtt_us",
    ),
    # (2) market_data en modo LATENCY: open-loop a λ sub-saturación.
    "market_data_lat": Scenario(
        name="market_data_lat",
        mode="latency",
        pattern="open_rate",
        msg_size=128,
        n_messages=20000,
        warmup_messages=2000,
        concurrency_levels=[1],   # un solo emisor a tasa λ (open-loop)
        persistent_levels=[1],
        repetitions=5,
        trim_initial=200,
        latency_metric="oneway_us",
        rate_fraction=0.25,
        calibrate_from="market_data_tput",
    ),
    # (3) order_routing: lock-step (closed-loop). conc=1 = latencia pura.
    "order_routing": Scenario(
        name="order_routing",
        mode="latency",
        pattern="lockstep",
        msg_size=512,
        n_messages=10000,
        warmup_messages=1000,
        concurrency_levels=[1, 2, 8, 32],
        persistent_levels=[1],
        repetitions=5,
        trim_initial=50,
        latency_metric="rtt_us",
    ),
    # (4) post_trade: lock-step, mensajes grandes, con y sin persistencia.
    "post_trade": Scenario(
        name="post_trade",
        mode="latency",
        pattern="lockstep",
        msg_size=8192,
        n_messages=10000,
        warmup_messages=500,
        concurrency_levels=[1, 2, 4, 8],
        persistent_levels=[1, 0],
        repetitions=5,
        trim_initial=50,
        latency_metric="rtt_us",
    ),
    # (5) fault_injection: tráfico sostenido, corte a mitad, conteo de pérdidas.
    "fault_injection": Scenario(
        name="fault_injection",
        mode="latency",
        pattern="open_rate",
        msg_size=256,
        n_messages=6000,
        warmup_messages=300,
        concurrency_levels=[1],
        persistent_levels=[1],
        repetitions=5,
        trim_initial=0,
        latency_metric="rtt_us",
        calibrate_from="market_data_tput",
        fault=True,
        fault_at_fraction=0.5,
        fault_kind="kill",
        grace_s=8.0,
        # Tasa fija moderada (msg/s), igual para los 4 protocolos => corridas
        # acotadas y comparables (no depende de la capacidad de cada uno).
        fixed_rate=300.0,
    ),
}

# Orden por defecto de ejecución (calibración primero implícita para open_rate).
DEFAULT_ORDER = [
    "market_data_tput",
    "market_data_lat",
    "order_routing",
    "post_trade",
    "fault_injection",
]


def get_scenario(name: str) -> Scenario:
    if name not in SCENARIOS:
        raise KeyError(f"Escenario desconocido: {name}. Opciones: {list(SCENARIOS)}")
    return SCENARIOS[name]


def scaled(name: str, factor: float) -> Scenario:
    """Versión reducida para smoke tests."""
    s = get_scenario(name)
    return replace(
        s,
        n_messages=max(int(s.n_messages * factor), 200),
        warmup_messages=max(int(s.warmup_messages * factor), 20),
        trim_initial=max(int(s.trim_initial * factor), 0),
        repetitions=1,
    )
