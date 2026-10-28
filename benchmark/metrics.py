"""
Cálculo de métricas a partir de las muestras crudas, CONSCIENTE del modo.

Reglas (ver README §Validez):
  - mode="throughput": SOLO throughput y tasa de errores. NO se emiten
    percentiles de latencia (serían tiempo de cola). Las columnas lat_* quedan
    VACÍAS a propósito.
  - mode="latency": percentiles p50/p95/p99, jitter, y la tasa ofrecida.

Agregación entre repeticiones:
  - Se calcula la métrica por repetición y se reporta la MEDIANA entre reps más
    el rango intercuartil (IQR) entre reps. El IQR entre reps es la barra de
    error de los gráficos agregados.

Descarte:
  - Siempre se excluye el warm-up (is_warmup==1) y los errores.
  - Además se descartan las primeras `trim_initial` muestras útiles de cada rep
    (arranque en frío del JIT/cachés; incluye el 1er request del pool HTTP).
"""

from __future__ import annotations

import numpy as np

# Claves que identifican una CELDA (una combinación de condiciones).
CELL_KEYS = ["protocol", "scenario", "mode", "tls", "concurrency",
             "msg_size", "persistent", "target_rate"]


def _latency_metric_for(scenario: str) -> str:
    try:
        from benchmark.scenarios import get_scenario
        return get_scenario(scenario).latency_metric
    except Exception:
        return "rtt_us"


def _trim_for(scenario: str) -> int:
    try:
        from benchmark.scenarios import get_scenario
        return get_scenario(scenario).trim_initial
    except Exception:
        return 50


def useful_samples(df_rep, trim):
    """Muestras útiles de una repetición: sin warm-up ni errores, ordenadas por
    instante real de envío (t_send_ns) y sin las primeras `trim`."""
    useful = df_rep[(df_rep["is_warmup"] == 0) & (df_rep["error"] == 0)]
    useful = useful.sort_values("t_send_ns")
    # No descartar más de la cuenta en corridas chicas (protege smokes).
    eff_trim = min(trim, len(useful) // 5) if trim > 0 else 0
    if eff_trim > 0:
        useful = useful.iloc[eff_trim:]
    return useful


def _rep_stats(df_rep, metric, trim):
    """Métricas de UNA repetición de UNA celda."""
    attempts = len(df_rep)
    errors = int(df_rep["error"].sum())
    error_rate = errors / attempts if attempts else 0.0

    useful = useful_samples(df_rep, trim)

    lat = useful[metric].to_numpy(dtype=float)
    lat = lat[~np.isnan(lat)]

    # Throughput sostenido de la repetición.
    if len(useful) >= 2:
        t_send = useful["t_send_ns"].to_numpy(dtype=float)
        t_recv = useful["t_recv_ns"].to_numpy(dtype=float)
        span_s = max(t_recv.max() - t_send.min(), 1.0) / 1e9
        throughput = len(useful) / span_s
    else:
        throughput = float("nan")

    if lat.size == 0:
        return dict(error_rate=error_rate, samples=0, throughput=throughput,
                    p50=np.nan, p95=np.nan, p99=np.nan, mean=np.nan,
                    jitter_std=np.nan, jitter_pdv=np.nan)

    pdv = float(np.mean(np.abs(np.diff(lat)))) if lat.size > 1 else 0.0
    return dict(
        error_rate=error_rate, samples=int(lat.size), throughput=throughput,
        p50=float(np.percentile(lat, 50)), p95=float(np.percentile(lat, 95)),
        p99=float(np.percentile(lat, 99)), mean=float(np.mean(lat)),
        jitter_std=float(np.std(lat)), jitter_pdv=pdv,
    )


def _agg(values):
    """Mediana e IQR (entre repeticiones) de una lista de valores."""
    v = np.array([x for x in values if x == x], dtype=float)  # descarta NaN
    if v.size == 0:
        return float("nan"), float("nan")
    med = float(np.median(v))
    iqr = float(np.percentile(v, 75) - np.percentile(v, 25)) if v.size > 1 else 0.0
    return med, iqr


def summarize_all(df):
    """Un renglón por celda, agregando entre repeticiones. Mode-aware."""
    import pandas as pd

    rows = []
    for key, cell in df.groupby(CELL_KEYS):
        proto, scen, mode, tls, conc, size, persistent, rate = key
        metric = _latency_metric_for(scen)
        trim = _trim_for(scen)

        per_rep = [_rep_stats(g, metric, trim) for _, g in cell.groupby("repetition")]
        n_reps = len(per_rep)
        total_samples = int(sum(r["samples"] for r in per_rep))
        error_rate = float(np.mean([r["error_rate"] for r in per_rep]))

        tput_med, tput_iqr = _agg([r["throughput"] for r in per_rep])

        row = {
            "protocol": proto, "scenario": scen, "mode": mode, "tls": int(tls),
            "concurrency": int(conc), "msg_size": int(size),
            "persistent": int(persistent), "target_rate": float(rate),
            "reps": n_reps, "samples": total_samples, "error_rate": error_rate,
            "throughput_msg_s": tput_med, "throughput_iqr_msg_s": tput_iqr,
        }

        if mode == "throughput":
            # Percentiles de latencia deliberadamente vacíos.
            for c in ("lat_p50_us", "lat_p50_iqr_us", "lat_p95_us", "lat_p95_iqr_us",
                      "lat_p99_us", "lat_p99_iqr_us", "lat_mean_us",
                      "jitter_std_us", "jitter_pdv_us"):
                row[c] = ""
        else:
            p50_med, p50_iqr = _agg([r["p50"] for r in per_rep])
            p95_med, p95_iqr = _agg([r["p95"] for r in per_rep])
            p99_med, p99_iqr = _agg([r["p99"] for r in per_rep])
            mean_med, _ = _agg([r["mean"] for r in per_rep])
            jstd_med, _ = _agg([r["jitter_std"] for r in per_rep])
            jpdv_med, _ = _agg([r["jitter_pdv"] for r in per_rep])
            row.update({
                "lat_p50_us": p50_med, "lat_p50_iqr_us": p50_iqr,
                "lat_p95_us": p95_med, "lat_p95_iqr_us": p95_iqr,
                "lat_p99_us": p99_med, "lat_p99_iqr_us": p99_iqr,
                "lat_mean_us": mean_med,
                "jitter_std_us": jstd_med, "jitter_pdv_us": jpdv_med,
            })
        rows.append(row)

    return pd.DataFrame(rows)
