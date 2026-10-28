"""
Gráficos del banco de pruebas (matplotlib, 150 dpi).

Reglas (ver README §Validez):
  - Boxplots de latencia: SOLO celdas en modo latency.
  - Curvas de throughput vs concurrencia: SOLO celdas en modo throughput.
  - Gráfico de convergencia: p50 de latencia vs RTT de red inyectado (0/10/50 ms),
    escala log en y. Es el gráfico central de la tesis. Toma varios summary_*.csv.
  - Gráfico de fallas: mensajes perdidos vs recuperados por protocolo.
  - Barras de error = IQR entre repeticiones.

Uso:
  # Por corrida (boxplots latency, throughput, jitter):
  python -m benchmark.plots run --raw results/raw_TAG.csv --summary results/summary_TAG.csv

  # Convergencia (varias corridas con su RTT en ms):
  python -m benchmark.plots convergence --point 0:results/summary_loopback-plain.csv \\
      --point 10:results/summary_net5ms.csv --point 50:results/summary_net25ms.csv \\
      --scenario order_routing --concurrency 1

  # Fallas:
  python -m benchmark.plots fault --faults results/faults_faults.csv

  # Revisión: figuras con barras de error = IC 95 % bootstrap (stats_cells.csv):
  python -m benchmark.plots ci --stats results/revision/stats_cells.csv --out plots/revision \
      --rtt loopback-plain:0 --rtt net10ms:10 --rtt net50ms:50
"""

from __future__ import annotations

import argparse
import os

import numpy as np
import pandas as pd
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.ticker  # noqa: E402

PROTO_ORDER = ["fix", "http", "http2", "ws"]
PROTO_LABEL = {"fix": "FIX", "http": "HTTP/1.1", "http2": "HTTP/2", "ws": "WebSocket"}
PROTO_COLOR = {"fix": "#1f77b4", "http": "#ff7f0e", "http2": "#2ca02c", "ws": "#d62728"}


def _metric_col(scenario: str) -> str:
    try:
        from benchmark.scenarios import get_scenario
        return get_scenario(scenario).latency_metric
    except Exception:
        return "rtt_us"


def _is_fault(scenario: str) -> bool:
    try:
        from benchmark.scenarios import get_scenario
        return get_scenario(scenario).fault
    except Exception:
        return "fault" in scenario


# ---------------------------------------------------------------------------
# Por corrida
# ---------------------------------------------------------------------------

def boxplots_latency(raw: pd.DataFrame, out_dir: str) -> list:
    """Boxplot de latencia por protocolo, para cada escenario en modo latency.
    Se usa la menor concurrencia (latencia más pura) y persistencia=1."""
    paths = []
    lat = raw[raw["mode"] == "latency"]
    for scenario in sorted(lat["scenario"].unique()):
        if _is_fault(scenario):
            continue  # fault tiene su propio gráfico; su latencia mezcla el outage
        sub = lat[lat["scenario"] == scenario]
        conc = sub["concurrency"].min()
        sub = sub[(sub["concurrency"] == conc) & (sub["persistent"] == 1)]
        sub = sub[(sub["is_warmup"] == 0) & (sub["error"] == 0)]
        metric = _metric_col(scenario)
        data, labels, colors = [], [], []
        for proto in PROTO_ORDER:
            vals = sub[sub["protocol"] == proto][metric].dropna().to_numpy()
            if vals.size:
                data.append(vals)
                labels.append(PROTO_LABEL[proto])
                colors.append(PROTO_COLOR[proto])
        if not data:
            continue
        fig, ax = plt.subplots(figsize=(7, 4.5))
        bp = ax.boxplot(data, tick_labels=labels, showfliers=False, whis=(5, 95),
                        patch_artist=True)
        for patch, c in zip(bp["boxes"], colors):
            patch.set_facecolor(c)
            patch.set_alpha(0.5)
        ax.set_ylabel(f"Latencia {metric.replace('_us',' (µs)')}")
        ax.set_title(f"Latencia por protocolo — «{scenario}» (modo latency, conc={conc})")
        ax.grid(True, axis="y", alpha=0.3)
        fig.tight_layout()
        path = os.path.join(out_dir, f"boxplot_latency_{scenario}.png")
        fig.savefig(path, dpi=150)
        plt.close(fig)
        paths.append(path)
    return paths


def throughput_vs_concurrency(summary: pd.DataFrame, out_dir: str) -> list:
    """Curvas de throughput vs concurrencia (solo modo throughput) con IQR."""
    paths = []
    tp = summary[summary["mode"] == "throughput"].copy()
    tp["throughput_msg_s"] = pd.to_numeric(tp["throughput_msg_s"], errors="coerce")
    tp["throughput_iqr_msg_s"] = pd.to_numeric(tp["throughput_iqr_msg_s"], errors="coerce")
    for scenario in sorted(tp["scenario"].unique()):
        sub = tp[tp["scenario"] == scenario]
        fig, ax = plt.subplots(figsize=(7, 4.5))
        plotted = False
        for proto in PROTO_ORDER:
            ps = sub[sub["protocol"] == proto].sort_values("concurrency")
            if ps.empty:
                continue
            ax.errorbar(ps["concurrency"], ps["throughput_msg_s"],
                        yerr=ps["throughput_iqr_msg_s"], marker="o", capsize=3,
                        label=PROTO_LABEL[proto], color=PROTO_COLOR[proto])
            plotted = True
        if not plotted:
            plt.close(fig)
            continue
        ax.set_xlabel("Concurrencia (mensajes en vuelo)")
        ax.set_ylabel("Throughput sostenido (msg/s)")
        ax.set_title(f"Throughput vs concurrencia — «{scenario}» (modo throughput)")
        ax.grid(True, alpha=0.3)
        ax.legend()
        fig.tight_layout()
        path = os.path.join(out_dir, f"throughput_{scenario}.png")
        fig.savefig(path, dpi=150)
        plt.close(fig)
        paths.append(path)
    return paths


def jitter_vs_concurrency(summary: pd.DataFrame, out_dir: str) -> list:
    """Jitter (desvío de latencia) vs concurrencia, solo modo latency."""
    paths = []
    lat = summary[summary["mode"] == "latency"].copy()
    lat["jitter_std_us"] = pd.to_numeric(lat["jitter_std_us"], errors="coerce")
    for scenario in sorted(lat["scenario"].unique()):
        sub = lat[lat["scenario"] == scenario]
        if sub["concurrency"].nunique() < 2:
            continue  # sin barrido no aporta
        fig, ax = plt.subplots(figsize=(7, 4.5))
        plotted = False
        for proto in PROTO_ORDER:
            ps = sub[sub["protocol"] == proto].sort_values("concurrency")
            if ps.empty:
                continue
            ax.plot(ps["concurrency"], ps["jitter_std_us"], marker="s",
                    label=PROTO_LABEL[proto], color=PROTO_COLOR[proto])
            plotted = True
        if not plotted:
            plt.close(fig)
            continue
        ax.set_xlabel("Concurrencia (mensajes en vuelo)")
        ax.set_ylabel("Jitter — desvío estándar de latencia (µs)")
        ax.set_title(f"Jitter vs concurrencia — «{scenario}»")
        ax.grid(True, alpha=0.3)
        ax.legend()
        fig.tight_layout()
        path = os.path.join(out_dir, f"jitter_{scenario}.png")
        fig.savefig(path, dpi=150)
        plt.close(fig)
        paths.append(path)
    return paths


# ---------------------------------------------------------------------------
# Convergencia: p50 vs RTT inyectado (log y) — gráfico central
# ---------------------------------------------------------------------------

def convergence(points, scenario, concurrency, out_dir) -> str:
    """points: lista de (rtt_ms, summary_csv). Grafica p50 por protocolo vs RTT."""
    series = {p: {"rtt": [], "p50": [], "iqr": []} for p in PROTO_ORDER}
    for rtt_ms, csv_path in sorted(points, key=lambda x: x[0]):
        df = pd.read_csv(csv_path)
        df = df[(df["scenario"] == scenario) & (df["mode"] == "latency")
                & (df["concurrency"] == concurrency)]
        for proto in PROTO_ORDER:
            row = df[df["protocol"] == proto]
            if row.empty:
                continue
            p50 = pd.to_numeric(row["lat_p50_us"], errors="coerce").iloc[0]
            iqr = pd.to_numeric(row.get("lat_p50_iqr_us"), errors="coerce").iloc[0] \
                if "lat_p50_iqr_us" in row else 0.0
            if p50 == p50:
                series[proto]["rtt"].append(rtt_ms)
                series[proto]["p50"].append(p50)
                series[proto]["iqr"].append(iqr if iqr == iqr else 0.0)

    fig, ax = plt.subplots(figsize=(7.5, 5))
    plotted = False
    for proto in PROTO_ORDER:
        s = series[proto]
        if not s["rtt"]:
            continue
        ax.errorbar(s["rtt"], s["p50"], yerr=s["iqr"], marker="o", capsize=3,
                    label=PROTO_LABEL[proto], color=PROTO_COLOR[proto])
        plotted = True
    if not plotted:
        plt.close(fig)
        return ""
    ax.set_yscale("log")
    ax.set_xlabel("RTT de red inyectado (ms)")
    ax.set_ylabel("Latencia p50 (µs, escala log)")
    ax.set_title(f"Convergencia de latencia vs RTT de red — «{scenario}» (conc={concurrency})")
    ax.grid(True, which="both", alpha=0.3)
    ax.legend()
    fig.tight_layout()
    path = os.path.join(out_dir, f"convergence_{scenario}.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path


# ---------------------------------------------------------------------------
# Fallas
# ---------------------------------------------------------------------------

def fault_plot(faults_csv, out_dir) -> str:
    df = pd.read_csv(faults_csv)
    # Mediana entre repeticiones por protocolo.
    g = df.groupby("protocol").agg(
        lost=("msgs_lost", "median"),
        recovered=("msgs_recovered", "median"),
    ).reindex([p for p in PROTO_ORDER if p in df["protocol"].unique()])
    protos = list(g.index)
    x = np.arange(len(protos))
    w = 0.38
    fig, ax = plt.subplots(figsize=(7, 4.5))
    ax.bar(x - w / 2, g["lost"], w, label="Perdidos", color="#d62728", alpha=0.8)
    ax.bar(x + w / 2, g["recovered"], w, label="Recuperados", color="#2ca02c", alpha=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels([PROTO_LABEL[p] for p in protos])
    ax.set_ylabel("Mensajes (mediana entre repeticiones)")
    ax.set_title("Inyección de fallas: mensajes perdidos vs recuperados")
    ax.grid(True, axis="y", alpha=0.3)
    ax.legend()
    fig.tight_layout()
    path = os.path.join(out_dir, "fault_recovery.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path


# ---------------------------------------------------------------------------
# Revisión: barras de error = IC 95 % bootstrap jerárquico (benchmark.stats)
# ---------------------------------------------------------------------------

# Paleta validada para daltonismo con todos los pares (la de arriba confunde
# verde/naranja en protanopía); el marcador y el trazo dan la identidad sin color.
CI_STYLE = {
    "fix": dict(color="#2a78d6", marker="o", linestyle="-"),
    "http": dict(color="#eb6834", marker="s", linestyle="--"),
    "http2": dict(color="#1baf7a", marker="^", linestyle="-."),
    "ws": dict(color="#52514e", marker="D", linestyle=":"),
}
CI_YLABEL = {
    "lat_p50_us": "Latencia p50 (µs)", "lat_p99_us": "Latencia p99 (µs)",
    "jitter_ipdv_us": "Jitter (IPDV, µs)", "throughput_msg_s": "Throughput sostenido (msg/s)",
}


def _ci_series(ax, df, x, stat):
    ends = []
    for proto in PROTO_ORDER:
        ps = df[df["protocol"] == proto].sort_values(x)
        if ps.empty or ps[stat].isna().all():
            continue
        y = ps[stat].to_numpy()
        yerr = np.vstack([y - ps[f"{stat}_ci_lo"].to_numpy(), ps[f"{stat}_ci_hi"].to_numpy() - y])
        ax.errorbar(ps[x], y, yerr=np.clip(yerr, 0, None), capsize=3, linewidth=2,
                    markersize=7, label=PROTO_LABEL[proto], **CI_STYLE[proto])
        ends.append((PROTO_LABEL[proto], ps[x].iloc[-1], y[-1]))
    return ends


def _end_labels(ax, ends, min_gap_pt=10):
    """Etiquetas directas al final de cada serie, separadas verticalmente si chocan
    (en puntos, para que la separación no dependa del dpi con que se guarde)."""
    k = 72.0 / ax.figure.dpi
    pts = sorted((ax.transData.transform((x, y))[1] * k, name, x, y) for name, x, y in ends)
    placed = []
    for py, name, x, y in pts:
        placed.append(max(py, placed[-1] + min_gap_pt) if placed else py)
    for (py, name, x, y), ty in zip(pts, placed):
        ax.annotate(name, (x, y), xytext=(6, ty - py), textcoords="offset points",
                    va="center", fontsize=8, color="#52514e")


def _ci_fig(df, x, stats, title, xlabel, path, logy=True, logx=False):
    fig, axes = plt.subplots(1, len(stats), figsize=(6.5 * len(stats), 4.5), squeeze=False)
    ends_by_stat = {}
    for ax, stat in zip(axes[0], stats):
        ends_by_stat[stat] = _ci_series(ax, df, x, stat)
        if logy:
            ax.set_yscale("log")
        if logx:
            ax.set_xscale("log", base=2)
            ax.set_xticks(sorted(df[x].unique()))
            ax.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
        ax.set_xlabel(xlabel)
        ax.set_ylabel(CI_YLABEL[stat])
        ax.grid(True, which="major", alpha=0.25)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
        ax.legend(fontsize=8, frameon=False)
    fig.suptitle(title + "\nbarras: IC 95 % bootstrap jerárquico", fontsize=10)
    fig.tight_layout()
    for ax, stat in zip(axes[0], stats):  # con escalas y layout ya fijados
        _end_labels(ax, ends_by_stat[stat])
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def ci_plots(stats_csv, out_dir, rtt_map) -> list:
    df = pd.read_csv(stats_csv)
    df = df[df["aborted_reps"] == 0]  # celdas con open-loop abortado no son válidas
    paths = []
    conc_label = "Concurrencia (mensajes en vuelo)"
    for cond in sorted(df["condition"].unique()):
        d = df[df["condition"] == cond]
        lat = d[d["mode"] == "latency"]
        for scen in ("order_routing", "post_trade"):
            for persistent in sorted(lat[lat["scenario"] == scen]["persistent"].unique()):
                sub = lat[(lat["scenario"] == scen) & (lat["persistent"] == persistent)]
                if sub["concurrency"].nunique() < 2:
                    continue
                suffix = "" if persistent else "_nopersist"
                name = f"«{scen}»{'' if persistent else ' sin keep-alive'} — {cond}"
                paths.append(_ci_fig(sub, "concurrency", ["lat_p50_us", "lat_p99_us"],
                                     f"Latencia vs concurrencia {name}", conc_label,
                                     os.path.join(out_dir, f"latency_{scen}{suffix}_{cond}.png"),
                                     logx=True))
                paths.append(_ci_fig(sub, "concurrency", ["jitter_ipdv_us"],
                                     f"Jitter vs concurrencia {name}", conc_label,
                                     os.path.join(out_dir, f"jitter_{scen}{suffix}_{cond}.png"),
                                     logx=True))
        tp = d[d["mode"] == "throughput"]
        for scen in sorted(tp["scenario"].unique()):
            paths.append(_ci_fig(tp[tp["scenario"] == scen], "concurrency", ["throughput_msg_s"],
                                 f"Throughput vs concurrencia «{scen}» — {cond}", conc_label,
                                 os.path.join(out_dir, f"throughput_{scen}_{cond}.png"),
                                 logy=False, logx=True))
    if rtt_map:
        conv = df[df["condition"].isin(rtt_map) & (df["scenario"] == "order_routing")
                  & (df["concurrency"] == 1)].copy()
        conv["rtt_ms"] = conv["condition"].map(rtt_map)
        paths.append(_ci_fig(conv, "rtt_ms", ["lat_p50_us"],
                             "Convergencia: latencia vs RTT de red «order_routing» (conc=1)",
                             "RTT de red inyectado (ms)",
                             os.path.join(out_dir, "convergence_order_routing.png")))
    return paths


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    pr = sub.add_parser("run", help="gráficos de una corrida")
    pr.add_argument("--raw", required=True)
    pr.add_argument("--summary", default=None)
    pr.add_argument("--out", default=None)

    pc = sub.add_parser("convergence", help="p50 vs RTT inyectado (varias corridas)")
    pc.add_argument("--point", action="append", required=True,
                    help="RTT_ms:summary.csv (repetir)")
    pc.add_argument("--scenario", default="order_routing")
    pc.add_argument("--concurrency", type=int, default=1)
    pc.add_argument("--out", default="plots")

    pf = sub.add_parser("fault", help="perdidos vs recuperados")
    pf.add_argument("--faults", required=True)
    pf.add_argument("--out", default="plots")

    pci = sub.add_parser("ci", help="revisión: figuras con IC 95 %% bootstrap")
    pci.add_argument("--stats", required=True, help="stats_cells.csv de benchmark.stats")
    pci.add_argument("--rtt", action="append", default=[],
                     help="condición:RTT_ms para la convergencia (repetir)")
    pci.add_argument("--out", default="plots/revision")

    args = p.parse_args()

    if args.cmd == "ci":
        os.makedirs(args.out, exist_ok=True)
        rtt_map = {c: float(r) for c, r in (x.rsplit(":", 1) for x in args.rtt)}
        paths = ci_plots(args.stats, args.out, rtt_map)
        print(f"Generados {len(paths)} gráficos en {args.out}")

    elif args.cmd == "run":
        out_dir = args.out or os.path.join(os.path.dirname(os.path.dirname(args.raw)), "plots")
        os.makedirs(out_dir, exist_ok=True)
        raw = pd.read_csv(args.raw)
        summ = pd.read_csv(args.summary) if args.summary else None
        if summ is None:
            from benchmark.metrics import summarize_all
            summ = summarize_all(raw)
        paths = []
        paths += boxplots_latency(raw, out_dir)
        paths += throughput_vs_concurrency(summ, out_dir)
        paths += jitter_vs_concurrency(summ, out_dir)
        print(f"Generados {len(paths)} gráficos en {out_dir}:")
        for x in paths:
            print("  -", x)

    elif args.cmd == "convergence":
        os.makedirs(args.out, exist_ok=True)
        points = []
        for pt in args.point:
            rtt, path = pt.split(":", 1)
            points.append((float(rtt), path))
        path = convergence(points, args.scenario, args.concurrency, args.out)
        print("convergencia ->", path or "(sin datos)")

    elif args.cmd == "fault":
        os.makedirs(args.out, exist_ok=True)
        print("fallas ->", fault_plot(args.faults, args.out))


if __name__ == "__main__":
    main()
