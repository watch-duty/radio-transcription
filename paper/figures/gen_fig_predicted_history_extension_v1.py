# /// script
# requires-python = ">=3.11"
# dependencies = ["matplotlib==3.11.0", "numpy==2.5.1"]
# ///
"""Generate versioned figures from the aggregate extension analysis only.

The generator accepts no raw predictions, transcripts, references, provider
artifacts, or alternate result tables. It validates and reads only
``experiments/results/predicted_history_extension_analysis.json`` and emits
new, version-suffixed figure and figure-data artifacts.
"""

# ruff: noqa: EM101, EM102, TRY003, TRY004

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import os
import pathlib
import tempfile
import typing

import matplotlib
import numpy as np
from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.figure import Figure
from matplotlib.patches import FancyBboxPatch

if typing.TYPE_CHECKING:
    from matplotlib.axes import Axes

matplotlib.use("Agg")

_REPO_ROOT = pathlib.Path(__file__).parents[2]
_FIGURE_DIR = pathlib.Path(__file__).resolve().parent
_ANALYSIS_PATH = (
    _REPO_ROOT / "experiments/results/predicted_history_extension_analysis.json"
)
_ANALYSIS_FILENAME = "predicted_history_extension_analysis.json"
_DATA_CSV = "data/predicted_history_extension_v1.csv"
_DATA_JSON = "data/predicted_history_extension_v1.json"
_EXACT_P13_PDF = "fig_predicted_history_extension_exact_p13_v1.pdf"
_EXACT_P13_PNG = "fig_predicted_history_extension_exact_p13_v1.png"
_TRADEOFF_PDF = "fig_predicted_history_extension_tradeoff_v1.pdf"
_TRADEOFF_PNG = "fig_predicted_history_extension_tradeoff_v1.png"
_MATRIX_PDF = "fig_predicted_history_extension_matrix_v1.pdf"
_MATRIX_PNG = "fig_predicted_history_extension_matrix_v1.png"
_RECEIPT = "fig_predicted_history_extension_v1_receipt.json"

_TARGETS: typing.Final = (
    "base",
    "no_context_sft",
    "prior_context_sft",
)
_CONDITIONS: typing.Final = ("P0", "R0", "R1", "R2", "R4", "R8")
_CURVE_CONDITIONS: typing.Final = ("P0", "R1", "R2", "R4", "R8")
_PRIOR_TURN_CAPS: typing.Final = {
    "P0": 0,
    "R0": 0,
    "R1": 1,
    "R2": 2,
    "R4": 4,
    "R8": 8,
}
_FIGURE_ROLES: typing.Final = {
    "P0": "exact_p13_curve_origin",
    "R0": "detached_option_d_prompt_control",
    "R1": "exact_p13_curve",
    "R2": "exact_p13_curve",
    "R4": "exact_p13_curve",
    "R8": "exact_p13_curve",
}
_TARGET_STYLES: typing.Final = {
    "base": {
        "label": "Base",
        "color": "#0072B2",
        "marker": "o",
        "linestyle": "-",
    },
    "no_context_sft": {
        "label": "No-context SFT",
        "color": "#E69F00",
        "marker": "s",
        "linestyle": "--",
    },
    "prior_context_sft": {
        "label": "Prior-context SFT",
        "color": "#009E73",
        "marker": "^",
        "linestyle": "-.",
    },
}
_TARGET_SUBTITLES: typing.Final = {
    "base": "no supervised adaptation",
    "no_context_sft": "SFT without context",
    "prior_context_sft": "SFT with prior transcripts",
}
_PRIVATE_KEYS: typing.Final = frozenset(
    {
        "audio_uri",
        "candidate_tokens",
        "cluster_id",
        "history",
        "history_turns",
        "prediction",
        "reference",
        "row_id",
        "row_id_hash",
        "source_family",
        "stable_id",
        "telemetry_rows",
    }
)
_FULL_WIDTH_INCHES: typing.Final = 6.75
_PNG_DPI: typing.Final = 300
_POPULATION: typing.Final = "main_p13_primary_4098"
_POPULATION_ROWS: typing.Final = 4_098
_CSV_FIELDS: typing.Final = (
    "population",
    "population_row_count",
    "condition",
    "prior_turn_cap",
    "figure_role",
    "target",
    "target_label",
    "wer",
    "wer_lower",
    "wer_upper",
    "context_copy_rate",
    "context_copy_rate_lower",
    "context_copy_rate_upper",
)


class ArtifactConflictError(RuntimeError):
    """Raised when a generated artifact differs from an existing artifact."""


class _Metric(typing.NamedTuple):
    estimate: float
    lower: float
    upper: float


class _Point(typing.NamedTuple):
    condition: str
    target: str
    wer: _Metric
    context_copy_rate: _Metric


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _file_record(path: pathlib.Path) -> dict[str, int | str]:
    payload = path.read_bytes()
    return {"bytes": len(payload), "sha256": _sha256_bytes(payload)}


def _assert_aggregate_only(value: typing.Any) -> None:
    if isinstance(value, dict):
        private = _PRIVATE_KEYS.intersection(value)
        if private:
            raise ValueError(f"analysis contains private key: {min(private)}")
        for nested in value.values():
            _assert_aggregate_only(nested)
    elif isinstance(value, list):
        for nested in value:
            _assert_aggregate_only(nested)


def _finite_number(raw: typing.Any, *, label: str) -> float:
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise ValueError(f"{label} is not numeric")
    value = float(raw)
    if not math.isfinite(value):
        raise ValueError(f"{label} is not finite")
    return value


def _metric(
    cell: typing.Mapping[str, typing.Any],
    metric: str,
) -> _Metric:
    metrics = cell.get("metrics")
    if not isinstance(metrics, dict) or metric not in metrics:
        raise ValueError(f"required {metric} metric is missing")
    summary = metrics[metric]
    if not isinstance(summary, dict) or "estimate" not in summary:
        raise ValueError(f"required {metric} metric is unavailable")
    estimate = _finite_number(summary.get("estimate"), label=metric)
    lower = _finite_number(summary.get("lower"), label=f"{metric} lower")
    upper = _finite_number(summary.get("upper"), label=f"{metric} upper")
    if estimate < 0 or lower < 0 or not lower <= estimate <= upper:
        raise ValueError(f"required {metric} interval is invalid")
    if metric == "context_copy_rate" and upper > 100:
        raise ValueError("context-copy interval exceeds 100 percent")
    return _Metric(estimate=estimate, lower=lower, upper=upper)


def _load_analysis(  # noqa: PLR0912
    path: pathlib.Path,
) -> tuple[
    dict[tuple[str, str], _Point],
    dict[str, typing.Any],
    bytes,
]:
    if path.name != _ANALYSIS_FILENAME:
        raise ValueError("figure input must use the extension analyzer name")
    payload = path.read_bytes()
    try:
        report = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError("extension analysis is invalid JSON") from None
    if not isinstance(report, dict):
        raise ValueError("extension analysis must be an object")
    canonical = (json.dumps(report, indent=2, sort_keys=True) + "\n").encode()
    if payload != canonical:
        raise ValueError("extension analysis bytes are not canonical")
    _assert_aggregate_only(report)
    if (
        report.get("schema_version") != 1
        or report.get("analysis_id")
        != "predicted_history_extension_analysis_v1"
    ):
        raise ValueError("extension analysis schema differs")
    identity = report.get("identity")
    if (
        not isinstance(identity, dict)
        or "analysis_components" not in identity
        or any(
            not isinstance(value, str)
            or len(value) != 64
            or any(character not in "0123456789abcdef" for character in value)
            for value in identity.values()
        )
    ):
        raise ValueError("extension analysis identity is invalid")
    method = report.get("method")
    if not isinstance(method, dict) or (
        method.get("bootstrap_resamples"),
        method.get("bootstrap_seed"),
        method.get("confidence"),
        method.get("main_population"),
    ) != (
        10_000,
        20_260_711,
        0.95,
        "fixed_4098_normalized_nonempty",
    ):
        raise ValueError("extension figure inference contract differs")
    comparability = report.get("response_model_version_comparability")
    hashes = (
        comparability.get("hashes") if isinstance(comparability, dict) else None
    )
    parent_hashes = hashes.get("parent") if isinstance(hashes, dict) else None
    p13_hashes = hashes.get("p13_zero") if isinstance(hashes, dict) else None
    hash_sets = (parent_hashes, p13_hashes)
    if any(
        not isinstance(hash_set, dict)
        or set(hash_set) != set(_TARGETS)
        or any(
            not isinstance(value, str)
            or len(value) != 64
            or any(character not in "0123456789abcdef" for character in value)
            for value in hash_set.values()
        )
        for hash_set in hash_sets
    ):
        raise ValueError("response-model comparability hashes are invalid")
    derived_p13_matches = {
        target: p13_hashes[target] == parent_hashes[target]
        for target in _TARGETS
    }
    matches = (
        comparability.get("matches_parent")
        if isinstance(comparability, dict)
        else None
    )
    p13_matches = (
        matches.get("p13_zero_vs_parent") if isinstance(matches, dict) else None
    )
    if (
        not isinstance(p13_matches, dict)
        or set(p13_matches) != set(_TARGETS)
        or any(not isinstance(value, bool) for value in p13_matches.values())
        or p13_matches != derived_p13_matches
        or any(value is not True for value in derived_p13_matches.values())
    ):
        raise ValueError(
            "exact-P13 curve is unavailable after response-model mismatch"
        )
    populations = report.get("populations")
    population = (
        populations.get(_POPULATION) if isinstance(populations, dict) else None
    )
    if (
        not isinstance(population, dict)
        or population.get("row_count") != _POPULATION_ROWS
    ):
        raise ValueError("extension figure population differs")
    cells = population.get("cells")
    expected = {
        f"{condition}-{target}"
        for target in _TARGETS
        for condition in _CONDITIONS
    }
    if not isinstance(cells, dict) or set(cells) != expected:
        raise ValueError("extension figure requires a complete 18-cell matrix")
    points = {}
    for target in _TARGETS:
        for condition in _CONDITIONS:
            cell = cells[f"{condition}-{target}"]
            if not isinstance(cell, dict):
                raise ValueError("extension figure cell is invalid")
            point = _Point(
                condition=condition,
                target=target,
                wer=_metric(cell, "wer"),
                context_copy_rate=_metric(cell, "context_copy_rate"),
            )
            if condition in {"P0", "R0"} and any(
                not math.isclose(
                    value,
                    0.0,
                    rel_tol=0.0,
                    abs_tol=1e-12,
                )
                for value in (
                    point.context_copy_rate.estimate,
                    point.context_copy_rate.lower,
                    point.context_copy_rate.upper,
                )
            ):
                raise ValueError("structural no-history copy rate is nonzero")
            points[(target, condition)] = point
    return points, report, payload


def _public_rows(
    points: typing.Mapping[tuple[str, str], _Point],
) -> list[dict[str, typing.Any]]:
    rows = []
    for target in _TARGETS:
        style = _TARGET_STYLES[target]
        for condition in _CONDITIONS:
            point = points[(target, condition)]
            rows.append(
                {
                    "population": _POPULATION,
                    "population_row_count": _POPULATION_ROWS,
                    "condition": condition,
                    "prior_turn_cap": _PRIOR_TURN_CAPS[condition],
                    "figure_role": _FIGURE_ROLES[condition],
                    "target": target,
                    "target_label": style["label"],
                    "wer": point.wer.estimate,
                    "wer_lower": point.wer.lower,
                    "wer_upper": point.wer.upper,
                    "context_copy_rate": point.context_copy_rate.estimate,
                    "context_copy_rate_lower": point.context_copy_rate.lower,
                    "context_copy_rate_upper": point.context_copy_rate.upper,
                }
            )
    return rows


def _csv_text(rows: typing.Sequence[typing.Mapping[str, typing.Any]]) -> str:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(
        buffer,
        fieldnames=_CSV_FIELDS,
        lineterminator="\n",
    )
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def _csv_row_to_json(row: typing.Mapping[str, str]) -> dict[str, typing.Any]:
    """Convert one canonical public CSV row to its typed JSON twin."""
    return {
        "population": row["population"],
        "population_row_count": int(row["population_row_count"]),
        "condition": row["condition"],
        "prior_turn_cap": int(row["prior_turn_cap"]),
        "figure_role": row["figure_role"],
        "target": row["target"],
        "target_label": row["target_label"],
        "wer": float(row["wer"]),
        "wer_lower": float(row["wer_lower"]),
        "wer_upper": float(row["wer_upper"]),
        "context_copy_rate": float(row["context_copy_rate"]),
        "context_copy_rate_lower": float(row["context_copy_rate_lower"]),
        "context_copy_rate_upper": float(row["context_copy_rate_upper"]),
    }


def _figure_data_json(
    *,
    analysis_sha256: str,
    analysis_identity: typing.Mapping[str, str],
    csv_payload: str,
) -> str:
    csv_rows = list(csv.DictReader(io.StringIO(csv_payload, newline="")))
    value = {
        "schema_version": 1,
        "artifact_kind": "predicted_history_extension_figure_data_v1",
        "analysis_sha256": analysis_sha256,
        "analysis_identity": dict(analysis_identity),
        "population": _POPULATION,
        "population_row_count": _POPULATION_ROWS,
        "curve_conditions": list(_CURVE_CONDITIONS),
        "curve_prior_turn_caps": [
            _PRIOR_TURN_CAPS[condition] for condition in _CURVE_CONDITIONS
        ],
        "detached_prompt_control": "R0",
        "matrix_main_conditions": list(_CURVE_CONDITIONS),
        "matrix_detached_control": "R0",
        "metrics": ["wer", "context_copy_rate"],
        "response_model_versions_matched": True,
        "cross_target_role": "descriptive_unmatched_historical_systems",
        "rows": [_csv_row_to_json(row) for row in csv_rows],
    }
    return json.dumps(value, indent=2, sort_keys=True) + "\n"


def _publication_style() -> None:
    matplotlib.rcParams.update(
        {
            "font.family": "serif",
            "font.serif": ["DejaVu Serif"],
            "font.size": 8.5,
            "axes.labelsize": 9,
            "axes.linewidth": 0.7,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.grid": False,
            "xtick.labelsize": 8,
            "ytick.labelsize": 8,
            "xtick.major.width": 0.7,
            "ytick.major.width": 0.7,
            "legend.fontsize": 8,
            "legend.frameon": False,
            "lines.linewidth": 1.7,
            "lines.markersize": 5,
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
            "figure.dpi": _PNG_DPI,
            "savefig.dpi": _PNG_DPI,
            "savefig.bbox": None,
            "savefig.pad_inches": 0.0,
        }
    )


def _error_magnitudes(metric: _Metric) -> tuple[float, float]:
    return metric.estimate - metric.lower, metric.upper - metric.estimate


def _style_y_axis(axis: Axes) -> None:
    axis.set_ylabel("WER (%)")
    axis.grid(axis="y", color="#B0BEC5", linewidth=0.6, alpha=0.35)
    axis.set_axisbelow(True)


def _plot_exact_p13(
    points: typing.Mapping[tuple[str, str], _Point],
    *,
    pdf_path: pathlib.Path,
    png_path: pathlib.Path,
) -> None:
    _publication_style()
    figure = Figure(figsize=(_FULL_WIDTH_INCHES, 2.8), facecolor="white")
    FigureCanvasAgg(figure)
    grid = figure.add_gridspec(1, 2, width_ratios=(5.0, 1.15), wspace=0.08)
    curve_axis = figure.add_subplot(grid[0, 0])
    control_axis = figure.add_subplot(grid[0, 1], sharey=curve_axis)
    x_values = np.arange(len(_CURVE_CONDITIONS))
    for target in _TARGETS:
        style = _TARGET_STYLES[target]
        curve = [points[(target, condition)] for condition in _CURVE_CONDITIONS]
        estimates = np.asarray([point.wer.estimate for point in curve])
        lower = np.asarray([point.wer.lower for point in curve])
        upper = np.asarray([point.wer.upper for point in curve])
        curve_axis.errorbar(
            x_values,
            estimates,
            yerr=np.vstack((estimates - lower, upper - estimates)),
            color=style["color"],
            marker=style["marker"],
            linestyle=style["linestyle"],
            label=style["label"],
            markeredgecolor="white",
            markeredgewidth=0.55,
            elinewidth=0.65,
            capsize=1.8,
            zorder=3,
        )
        control = points[(target, "R0")]
        control_error = _error_magnitudes(control.wer)
        control_axis.errorbar(
            [0],
            [control.wer.estimate],
            yerr=np.asarray(control_error)[:, np.newaxis],
            color=style["color"],
            marker=style["marker"],
            linestyle="none",
            markeredgecolor="white",
            markeredgewidth=0.55,
            elinewidth=0.65,
            capsize=1.8,
            zorder=3,
        )
    curve_axis.set_xticks(x_values)
    curve_axis.set_xticklabels(("P0\n0", "R1\n1", "R2\n2", "R4\n4", "R8\n8"))
    curve_axis.set_xlabel("Exact-P13 maximum prior predicted turns")
    curve_axis.set_xlim(-0.25, len(_CURVE_CONDITIONS) - 0.75)
    curve_axis.text(
        0.01,
        0.98,
        "Exact-P13 interface",
        transform=curve_axis.transAxes,
        ha="left",
        va="top",
        fontsize=7.2,
        color="#546E7A",
    )
    curve_axis.text(
        0.99,
        0.02,
        "Cross-target levels are descriptive.",
        transform=curve_axis.transAxes,
        ha="right",
        va="bottom",
        fontsize=6.6,
        color="#546E7A",
    )
    _style_y_axis(curve_axis)

    control_axis.set_facecolor("#F2F4F5")
    control_axis.spines["left"].set_color("#7B8794")
    control_axis.spines["left"].set_linestyle("--")
    control_axis.spines["bottom"].set_visible(False)
    control_axis.set_xlim(-0.5, 0.5)
    control_axis.set_xticks(())
    control_axis.tick_params(axis="y", left=False, labelleft=False)
    control_axis.grid(axis="y", color="#B0BEC5", linewidth=0.6, alpha=0.35)
    control_axis.set_axisbelow(True)
    control_axis.text(
        0.5,
        0.96,
        "Option-D R0\nprompt control\nnot on curve",
        transform=control_axis.transAxes,
        ha="center",
        va="top",
        fontsize=7.2,
        color="#546E7A",
        bbox={
            "boxstyle": "round,pad=0.3",
            "facecolor": "white",
            "edgecolor": "none",
            "alpha": 0.82,
        },
    )
    handles, labels = curve_axis.get_legend_handles_labels()
    figure.legend(
        handles,
        labels,
        loc="upper center",
        bbox_to_anchor=(0.5, 0.99),
        ncol=len(_TARGETS),
        handlelength=2.5,
        columnspacing=1.6,
    )
    figure.subplots_adjust(left=0.09, right=0.99, bottom=0.2, top=0.81)
    _save_figure(
        figure,
        pdf_path=pdf_path,
        png_path=png_path,
        title="Exact-P13 WER curve with detached Option-D prompt control",
    )


def _plot_tradeoff(
    points: typing.Mapping[tuple[str, str], _Point],
    *,
    pdf_path: pathlib.Path,
    png_path: pathlib.Path,
) -> None:
    _publication_style()
    figure = Figure(figsize=(_FULL_WIDTH_INCHES, 3.0), facecolor="white")
    FigureCanvasAgg(figure)
    axis = figure.subplots()
    for target in _TARGETS:
        style = _TARGET_STYLES[target]
        curve = [points[(target, condition)] for condition in _CURVE_CONDITIONS]
        copy = np.asarray([point.context_copy_rate.estimate for point in curve])
        wer = np.asarray([point.wer.estimate for point in curve])
        copy_lower = np.asarray(
            [point.context_copy_rate.lower for point in curve]
        )
        copy_upper = np.asarray(
            [point.context_copy_rate.upper for point in curve]
        )
        wer_lower = np.asarray([point.wer.lower for point in curve])
        wer_upper = np.asarray([point.wer.upper for point in curve])
        axis.plot(
            copy,
            wer,
            color=style["color"],
            marker=style["marker"],
            linestyle=style["linestyle"],
            label=style["label"],
            markeredgecolor="white",
            markeredgewidth=0.55,
            zorder=3,
        )
        axis.errorbar(
            copy,
            wer,
            xerr=np.vstack((copy - copy_lower, copy_upper - copy)),
            yerr=np.vstack((wer - wer_lower, wer_upper - wer)),
            fmt="none",
            ecolor=style["color"],
            elinewidth=0.6,
            capsize=1.6,
            alpha=0.5,
            zorder=2,
        )
        for index, condition in enumerate(_CURVE_CONDITIONS):
            label = "P0" if condition == "P0" else condition.removeprefix("R")
            offset_y = -8 if target == "prior_context_sft" else 6
            vertical_alignment = (
                "top" if target == "prior_context_sft" else "bottom"
            )
            axis.annotate(
                label,
                (copy[index], wer[index]),
                xytext=(4, offset_y),
                textcoords="offset points",
                va=vertical_alignment,
                fontsize=6.5,
                color=style["color"],
                bbox={
                    "boxstyle": "round,pad=0.12",
                    "facecolor": "white",
                    "edgecolor": "none",
                    "alpha": 0.82,
                },
                zorder=4,
            )
    axis.set_xlabel("Context-copy diagnostic (%)")
    axis.set_ylabel("WER (%)")
    axis.set_xlim(left=0)
    axis.grid(color="#B0BEC5", linewidth=0.6, alpha=0.35)
    axis.set_axisbelow(True)
    axis.legend(
        loc="upper center",
        bbox_to_anchor=(0.5, 1.13),
        ncol=len(_TARGETS),
        handlelength=2.5,
        columnspacing=1.6,
    )
    axis.text(
        1.0,
        -0.2,
        "Exact-P13 curve; labels are P0 and maximum prior-turn caps "
        "1, 2, 4, and 8. "
        "Option-D R0 is excluded; cross-target levels are descriptive.",
        transform=axis.transAxes,
        ha="right",
        va="top",
        fontsize=7,
        color="#546E7A",
    )
    figure.subplots_adjust(left=0.09, right=0.99, bottom=0.23, top=0.84)
    _save_figure(
        figure,
        pdf_path=pdf_path,
        png_path=png_path,
        title="Exact-P13 WER versus context-copy diagnostic",
    )


def _matrix_box(
    axis: Axes,
    *,
    x: float,
    y: float,
    width: float,
    height: float,
    facecolor: str,
    edgecolor: str,
    linestyle: str = "-",
    linewidth: float = 0.8,
    zorder: int = 1,
) -> None:
    axis.add_patch(
        FancyBboxPatch(
            (x, y),
            width,
            height,
            boxstyle="round,pad=0.003,rounding_size=0.008",
            facecolor=facecolor,
            edgecolor=edgecolor,
            linestyle=linestyle,
            linewidth=linewidth,
            zorder=zorder,
        )
    )


def _plot_matrix(  # noqa: PLR0915
    points: typing.Mapping[tuple[str, str], _Point],
    *,
    pdf_path: pathlib.Path,
    png_path: pathlib.Path,
) -> None:
    """Render all 18 cells without treating R0 as exact-P13 at zero."""
    _publication_style()
    figure = Figure(figsize=(_FULL_WIDTH_INCHES, 3.25), facecolor="white")
    FigureCanvasAgg(figure)
    axis = figure.add_axes((0.01, 0.01, 0.98, 0.98))
    axis.set_xlim(0.0, 1.0)
    axis.set_ylim(0.0, 1.0)
    axis.set_axis_off()

    label_x = 0.012
    label_width = 0.208
    main_x = 0.236
    main_width = 0.556
    main_cell_width = main_width / len(_CURVE_CONDITIONS)
    control_x = 0.836
    control_width = 0.15
    header_y = 0.79
    header_height = 0.115
    row_height = 0.16
    row_bottoms = (0.575, 0.37, 0.165)

    _matrix_box(
        axis,
        x=main_x - 0.007,
        y=0.15,
        width=main_width + 0.014,
        height=0.77,
        facecolor="#F5FBF8",
        edgecolor="#009E73",
        linewidth=0.85,
        zorder=0,
    )
    _matrix_box(
        axis,
        x=control_x - 0.009,
        y=0.15,
        width=control_width + 0.018,
        height=0.77,
        facecolor="#FFF5EF",
        edgecolor="#D55E00",
        linestyle="--",
        linewidth=1.0,
        zorder=0,
    )
    axis.text(
        label_x + label_width / 2,
        0.953,
        "Historical target",
        ha="center",
        va="center",
        fontsize=7.5,
        weight="bold",
        color="#263238",
    )
    axis.text(
        main_x + main_width / 2,
        0.953,
        "Exact-P13 interface · rolling predicted history",
        ha="center",
        va="center",
        fontsize=7.5,
        weight="bold",
        color="#007F5F",
    )
    axis.text(
        control_x + control_width / 2,
        0.953,
        "Detached control",
        ha="center",
        va="center",
        fontsize=7.2,
        weight="bold",
        color="#B34E00",
    )

    for index, condition in enumerate(_CURVE_CONDITIONS):
        x = main_x + index * main_cell_width + 0.004
        width = main_cell_width - 0.008
        _matrix_box(
            axis,
            x=x,
            y=header_y,
            width=width,
            height=header_height,
            facecolor="#E8F5F0",
            edgecolor="#009E73",
            linewidth=0.8,
        )
        cap = _PRIOR_TURN_CAPS[condition]
        subtitle = "no history" if condition == "P0" else f"cap {cap}"
        axis.text(
            x + width / 2,
            header_y + 0.073,
            condition,
            ha="center",
            va="center",
            fontsize=7.8,
            weight="bold",
            color="#263238",
        )
        axis.text(
            x + width / 2,
            header_y + 0.03,
            subtitle,
            ha="center",
            va="center",
            fontsize=6.2,
            color="#007F5F",
        )

    _matrix_box(
        axis,
        x=control_x,
        y=header_y,
        width=control_width,
        height=header_height,
        facecolor="#FCE9DF",
        edgecolor="#D55E00",
        linestyle="--",
        linewidth=0.9,
    )
    axis.text(
        control_x + control_width / 2,
        header_y + 0.073,
        "R0 · Option D",
        ha="center",
        va="center",
        fontsize=7.3,
        weight="bold",
        color="#263238",
    )
    axis.text(
        control_x + control_width / 2,
        header_y + 0.03,
        "distinct prompt\nand interface",
        ha="center",
        va="center",
        fontsize=6.0,
        linespacing=0.95,
        color="#B34E00",
    )

    for target, row_y in zip(_TARGETS, row_bottoms, strict=True):
        style = _TARGET_STYLES[target]
        _matrix_box(
            axis,
            x=label_x,
            y=row_y,
            width=label_width,
            height=row_height,
            facecolor="#F7F9FA",
            edgecolor=style["color"],
            linewidth=1.05,
        )
        axis.text(
            label_x + 0.015,
            row_y + 0.102,
            style["label"],
            ha="left",
            va="center",
            fontsize=7.2,
            weight="bold",
            color="#263238",
        )
        axis.text(
            label_x + 0.015,
            row_y + 0.052,
            _TARGET_SUBTITLES[target],
            ha="left",
            va="center",
            fontsize=6.0,
            color="#60717C",
        )
        for index, condition in enumerate(_CURVE_CONDITIONS):
            point = points[(target, condition)]
            x = main_x + index * main_cell_width + 0.004
            width = main_cell_width - 0.008
            _matrix_box(
                axis,
                x=x,
                y=row_y,
                width=width,
                height=row_height,
                facecolor="white",
                edgecolor=style["color"],
                linewidth=0.75,
            )
            axis.text(
                x + width / 2,
                row_y + 0.102,
                f"{point.wer.estimate:.2f}",
                ha="center",
                va="center",
                fontsize=7.1,
                weight="bold",
                color="#263238",
            )
            axis.text(
                x + width / 2,
                row_y + 0.049,
                f"{point.wer.lower:.2f}–{point.wer.upper:.2f}",
                ha="center",
                va="center",
                fontsize=6.0,
                color="#60717C",
            )
        control = points[(target, "R0")]
        _matrix_box(
            axis,
            x=control_x,
            y=row_y,
            width=control_width,
            height=row_height,
            facecolor="white",
            edgecolor=style["color"],
            linestyle="--",
            linewidth=0.85,
        )
        axis.text(
            control_x + control_width / 2,
            row_y + 0.102,
            f"{control.wer.estimate:.2f}",
            ha="center",
            va="center",
            fontsize=7.1,
            weight="bold",
            color="#263238",
        )
        axis.text(
            control_x + control_width / 2,
            row_y + 0.049,
            f"{control.wer.lower:.2f}–{control.wer.upper:.2f}",
            ha="center",
            va="center",
            fontsize=6.0,
            color="#60717C",
        )

    axis.plot(
        (0.814, 0.814),
        (0.15, 0.92),
        color="#8A99A5",
        linestyle="--",
        linewidth=0.8,
        zorder=2,
    )
    axis.text(
        label_x,
        0.092,
        "Cells report WER % [95% cluster-bootstrap CI]; n = 4,098 each. "
        "Nonzero history uses rolling model predictions only.",
        ha="left",
        va="center",
        fontsize=6.0,
        color="#526975",
    )
    axis.text(
        label_x,
        0.043,
        "R0 changes the prompt/interface and is not P13 at zero; "
        "across-target levels are descriptive.",
        ha="left",
        va="center",
        fontsize=6.0,
        color="#526975",
    )
    _save_figure(
        figure,
        pdf_path=pdf_path,
        png_path=png_path,
        title="Complete 18-cell predicted-history experimental matrix",
    )


def _save_figure(
    figure: Figure,
    *,
    pdf_path: pathlib.Path,
    png_path: pathlib.Path,
    title: str,
) -> None:
    metadata = {
        "Title": title,
        "Author": "Anonymous",
        "Creator": "gen_fig_predicted_history_extension_v1.py",
        "CreationDate": None,
        "ModDate": None,
    }
    figure.savefig(
        pdf_path,
        format="pdf",
        metadata=metadata,
        facecolor="white",
    )
    figure.savefig(
        png_path,
        format="png",
        dpi=_PNG_DPI,
        metadata={
            "Software": "gen_fig_predicted_history_extension_v1.py",
            "Title": title,
        },
        facecolor="white",
    )
    figure.clear()


def _verify_candidate(
    candidate: pathlib.Path,
    destination: pathlib.Path,
) -> None:
    if destination.is_symlink():
        raise ArtifactConflictError(
            f"refusing symbolic link at artifact path: {destination.name}"
        )
    if destination.exists() and _file_record(candidate) != _file_record(
        destination
    ):
        raise ArtifactConflictError(
            f"refusing to overwrite nonidentical artifact: {destination.name}"
        )


def _publish_candidate(
    candidate: pathlib.Path,
    destination: pathlib.Path,
) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.is_symlink() or destination.exists():
        _verify_candidate(candidate, destination)
        return
    try:
        destination.hardlink_to(candidate)
    except FileExistsError:
        _verify_candidate(candidate, destination)
    directory_fd = os.open(destination.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _receipt(
    *,
    input_record: typing.Mapping[str, typing.Any],
    candidates: typing.Mapping[str, pathlib.Path],
) -> dict[str, typing.Any]:
    return {
        "schema_version": 1,
        "artifact_kind": "predicted_history_extension_figures_v1",
        "generator": {
            "path": ("paper/figures/gen_fig_predicted_history_extension_v1.py"),
            "sha256": _file_record(pathlib.Path(__file__))["sha256"],
            "matplotlib_version": matplotlib.__version__,
            "numpy_version": np.__version__,
        },
        "reproduction": {
            "generate": (
                "uv run --script "
                "paper/figures/gen_fig_predicted_history_extension_v1.py"
            ),
            "test": (
                "uv run --script experiments/scripts/"
                "run_predicted_history_extension_figure_tests.py"
            ),
        },
        "input": dict(input_record),
        "outputs": {
            name: _file_record(path)
            for name, path in sorted(candidates.items())
        },
        "figure_contract": {
            "population": _POPULATION,
            "population_row_count": _POPULATION_ROWS,
            "targets": list(_TARGETS),
            "curve_conditions": list(_CURVE_CONDITIONS),
            "curve_prior_turn_caps": [
                _PRIOR_TURN_CAPS[condition] for condition in _CURVE_CONDITIONS
            ],
            "detached_prompt_control": "R0",
            "matrix_cell_count": len(_TARGETS) * len(_CONDITIONS),
            "matrix_main_conditions": list(_CURVE_CONDITIONS),
            "matrix_detached_control": "R0",
            "matrix_cell_content": "wer_with_95_percent_ci",
            "tradeoff_includes_p0": True,
            "tradeoff_excludes_r0": True,
            "response_model_versions_matched": True,
            "cross_target_role": ("descriptive_unmatched_historical_systems"),
            "metrics": ["wer", "context_copy_rate"],
            "uncertainty": "analyzer_supplied_95_percent_cluster_bootstrap_ci",
            "context_copy_role": (
                "reference_proxy_lexical_diagnostic_not_semantic_hallucination"
            ),
            "palette": "Okabe-Ito",
            "target_styles": {
                target: {
                    "color": style["color"],
                    "marker": style["marker"],
                    "linestyle": style["linestyle"],
                }
                for target, style in _TARGET_STYLES.items()
            },
            "full_width_inches": _FULL_WIDTH_INCHES,
            "png_dpi": _PNG_DPI,
        },
    }


def generate_figure_bundle(
    *,
    analysis_path: pathlib.Path,
    output_dir: pathlib.Path,
) -> dict[str, typing.Any]:
    """Render and create-or-verify the complete versioned figure bundle."""
    points, report, analysis_payload = _load_analysis(analysis_path)
    input_record = {
        "path": f"experiments/results/{_ANALYSIS_FILENAME}",
        "bytes": len(analysis_payload),
        "sha256": _sha256_bytes(analysis_payload),
        "analysis_id": report["analysis_id"],
        "analysis_identity": dict(report["identity"]),
    }
    rows = _public_rows(points)
    csv_payload = _csv_text(rows)
    data_json_payload = _figure_data_json(
        analysis_sha256=typing.cast("str", input_record["sha256"]),
        analysis_identity=typing.cast(
            "typing.Mapping[str, str]", report["identity"]
        ),
        csv_payload=csv_payload,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        dir=output_dir,
        prefix=".predicted-history-extension-figures-v1-",
    ) as temporary:
        temporary_dir = pathlib.Path(temporary)
        candidates = {
            name: temporary_dir / name
            for name in (
                _DATA_CSV,
                _DATA_JSON,
                _EXACT_P13_PDF,
                _EXACT_P13_PNG,
                _TRADEOFF_PDF,
                _TRADEOFF_PNG,
                _MATRIX_PDF,
                _MATRIX_PNG,
            )
        }
        for path in candidates.values():
            path.parent.mkdir(parents=True, exist_ok=True)
        candidates[_DATA_CSV].write_text(csv_payload, encoding="utf-8")
        candidates[_DATA_JSON].write_text(
            data_json_payload,
            encoding="utf-8",
        )
        _plot_exact_p13(
            points,
            pdf_path=candidates[_EXACT_P13_PDF],
            png_path=candidates[_EXACT_P13_PNG],
        )
        _plot_tradeoff(
            points,
            pdf_path=candidates[_TRADEOFF_PDF],
            png_path=candidates[_TRADEOFF_PNG],
        )
        _plot_matrix(
            points,
            pdf_path=candidates[_MATRIX_PDF],
            png_path=candidates[_MATRIX_PNG],
        )
        if _file_record(analysis_path) != {
            "bytes": input_record["bytes"],
            "sha256": input_record["sha256"],
        }:
            raise ValueError(
                "extension analysis input changed during generation"
            )
        receipt = _receipt(
            input_record=input_record,
            candidates=candidates,
        )
        receipt_path = temporary_dir / _RECEIPT
        receipt_path.write_text(
            json.dumps(receipt, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        all_candidates = {**candidates, _RECEIPT: receipt_path}
        for name, candidate in all_candidates.items():
            _verify_candidate(candidate, output_dir / name)
        for name, candidate in all_candidates.items():
            _publish_candidate(candidate, output_dir / name)
    return receipt


def parser() -> argparse.ArgumentParser:
    """Return the fixed aggregate-input figure generator parser."""
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument(
        "--output-dir",
        type=pathlib.Path,
        default=_FIGURE_DIR,
    )
    return value


def main(argv: typing.Sequence[str] | None = None) -> int:
    """Generate the versioned extension figure bundle."""
    arguments = parser().parse_args(argv)
    generate_figure_bundle(
        analysis_path=_ANALYSIS_PATH,
        output_dir=arguments.output_dir,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
