"""Post-hoc analysis of raw_timings.csv.

Produces:
  - summary_stats.csv (mean and std-dev per cell)
  - 4 grouped bar charts (one per CRUD operation)
  - 1 heatmap of indexing-improvement ratios
All figures saved as PNG at 300 DPI in `output/figures/`.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # no display required on Windows headless runs
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

import config

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger("analyze")

OPERATIONS = ["create", "read", "update", "delete"]
DATABASES = ["mysql", "mongodb"]
INDEXING = ["indexed", "no_index"]


def _size_label(n: int) -> str:
    """Render dataset size as 10K / 50K / 100K for chart axes."""
    if n >= 1_000_000:
        return f"{n // 1_000_000}M"
    if n >= 1_000:
        return f"{n // 1_000}K"
    return str(n)


def _ieee_style() -> None:
    sns.set_theme(context="paper", style="whitegrid", font_scale=1.0)
    plt.rcParams.update(
        {
            "figure.dpi": 120,
            "savefig.dpi": 300,
            "savefig.bbox": "tight",
            "axes.titleweight": "bold",
            "axes.labelweight": "bold",
            "legend.frameon": True,
            "legend.framealpha": 0.9,
        }
    )


def load(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df["execution_time_ms"] = pd.to_numeric(df["execution_time_ms"], errors="coerce")
    df = df.dropna(subset=["execution_time_ms"])
    return df


def compute_summary(df: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        df.groupby(["database", "dataset_size", "indexing", "operation"])[
            "execution_time_ms"
        ]
        .agg(["count", "mean", "std", "min", "max"])
        .reset_index()
        .rename(
            columns={
                "count": "n_trials",
                "mean": "mean_ms",
                "std": "std_ms",
                "min": "min_ms",
                "max": "max_ms",
            }
        )
    )
    grouped[["mean_ms", "std_ms", "min_ms", "max_ms"]] = grouped[
        ["mean_ms", "std_ms", "min_ms", "max_ms"]
    ].round(4)
    return grouped


def compute_improvement(summary: pd.DataFrame) -> pd.DataFrame:
    """Indexing improvement ratio per (db, size, op):
    (mean_no_index - mean_indexed) / mean_no_index * 100
    Positive = index made it faster.
    """
    pivot = summary.pivot_table(
        index=["database", "dataset_size", "operation"],
        columns="indexing",
        values="mean_ms",
    ).reset_index()
    pivot["improvement_pct"] = (
        (pivot["no_index"] - pivot["indexed"]) / pivot["no_index"] * 100.0
    ).round(2)
    return pivot


def grouped_bar_per_op(df: pd.DataFrame, summary: pd.DataFrame, out_dir: Path) -> None:
    """One figure per CRUD op; X = dataset size, hue = (db × indexing)."""
    summary = summary.copy()
    summary["combo"] = (
        summary["database"].str.upper() + " (" + summary["indexing"] + ")"
    )

    combo_order = [
        "MYSQL (indexed)",
        "MYSQL (no_index)",
        "MONGODB (indexed)",
        "MONGODB (no_index)",
    ]
    palette = {
        "MYSQL (indexed)": "#1f77b4",
        "MYSQL (no_index)": "#aec7e8",
        "MONGODB (indexed)": "#2ca02c",
        "MONGODB (no_index)": "#98df8a",
    }

    for op in OPERATIONS:
        sub = summary[summary["operation"] == op].copy()
        if sub.empty:
            log.warning("No data for op=%s", op)
            continue
        # Render sizes as "10K"/"50K"/"100K" so matplotlib treats them as
        # discrete categories (not numbers) and the labels stay readable.
        size_int = sub["dataset_size"].astype(int)
        sub["size_label"] = size_int.apply(_size_label)
        ordered_labels = [_size_label(s) for s in sorted(size_int.unique())]
        sub["size_label"] = pd.Categorical(sub["size_label"], categories=ordered_labels, ordered=True)
        sub = sub.sort_values(["size_label", "combo"])

        fig, ax = plt.subplots(figsize=(7.0, 4.2))
        sns.barplot(
            data=sub,
            x="size_label",
            y="mean_ms",
            hue="combo",
            hue_order=[c for c in combo_order if c in sub["combo"].unique()],
            palette=palette,
            ax=ax,
            errorbar=None,
        )

        # Manual error bars (std dev) — seaborn doesn't accept precomputed std
        # cleanly across versions, so we draw them by iterating bar containers.
        # Snapshot the list before adding any errorbar containers (each call
        # to ax.errorbar() appends an ErrorbarContainer that would otherwise
        # extend ax.containers mid-iteration).
        sizes_sorted = ordered_labels
        combos_present = [c for c in combo_order if c in sub["combo"].unique()]
        bar_containers = list(ax.containers)
        for i, container in enumerate(bar_containers):
            combo = combos_present[i]
            for patch, size in zip(container, sizes_sorted):
                row = sub[(sub["combo"] == combo) & (sub["size_label"] == size)]
                if row.empty:
                    continue
                std = row["std_ms"].iloc[0]
                if pd.isna(std):
                    continue
                x = patch.get_x() + patch.get_width() / 2
                y = patch.get_height()
                ax.errorbar(x, y, yerr=std, fmt="none", ecolor="black", capsize=3, lw=0.8)

        ax.set_title(f"{op.capitalize()} latency vs dataset size")
        ax.set_xlabel("Dataset size (records)")
        ax.set_ylabel("Mean execution time (ms)")
        ax.legend(title="", loc="upper left", fontsize=8)
        fig.tight_layout()
        out_path = out_dir / f"bar_{op}.png"
        fig.savefig(out_path)
        plt.close(fig)
        log.info("Saved %s", out_path)


def heatmap_improvement(improvement: pd.DataFrame, out_dir: Path) -> None:
    df = improvement.copy()
    df["row"] = df["operation"].str.capitalize() + " — " + df["database"].str.upper()
    pivot = df.pivot_table(
        index="row", columns="dataset_size", values="improvement_pct"
    )
    desired_order = [
        f"{op.capitalize()} — {db.upper()}" for op in OPERATIONS for db in DATABASES
    ]
    pivot = pivot.reindex([r for r in desired_order if r in pivot.index])

    fig, ax = plt.subplots(figsize=(6.5, 5.5))
    sns.heatmap(
        pivot,
        annot=True,
        fmt=".1f",
        cmap="RdYlGn",
        center=0,
        cbar_kws={"label": "Indexing improvement (%)"},
        ax=ax,
        linewidths=0.5,
        linecolor="white",
    )
    ax.set_title("Indexing improvement: (no_index − indexed) / no_index × 100")
    ax.set_xlabel("Dataset size (records)")
    ax.set_ylabel("Operation × Database")
    fig.tight_layout()
    out_path = out_dir / "heatmap_improvement.png"
    fig.savefig(out_path)
    plt.close(fig)
    log.info("Saved %s", out_path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        type=Path,
        default=config.RAW_CSV_PATH,
        help=f"Path to raw_timings.csv (default: {config.RAW_CSV_PATH})",
    )
    args = parser.parse_args()

    if not args.input.exists():
        raise SystemExit(f"Input CSV not found: {args.input}")

    _ieee_style()
    config.FIGURES_DIR.mkdir(parents=True, exist_ok=True)

    df = load(args.input)
    log.info("Loaded %d rows from %s", len(df), args.input)

    summary = compute_summary(df)
    summary.to_csv(config.SUMMARY_CSV_PATH, index=False)
    log.info("Wrote summary stats -> %s", config.SUMMARY_CSV_PATH)

    improvement = compute_improvement(summary)
    improvement_path = config.OUTPUT_DIR / "improvement_ratios.csv"
    improvement.to_csv(improvement_path, index=False)
    log.info("Wrote improvement ratios -> %s", improvement_path)

    grouped_bar_per_op(df, summary, config.FIGURES_DIR)
    heatmap_improvement(improvement, config.FIGURES_DIR)

    log.info("Analysis done. Figures in %s", config.FIGURES_DIR)


if __name__ == "__main__":
    main()
