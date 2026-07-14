# /// script
# requires-python = ">=3.11"
# dependencies = ["matplotlib==3.11.0"]
# ///
"""Generate the prediction-only task-contract figure.

The figure is rendered from a checked-in, publication-safe JSON contract. The
renderer is deterministic and emits vector PDF, 300-DPI PNG, and a digest
receipt. It never reads private predictions, transcripts, or source locators.
"""

# ruff: noqa: EM101, TRY003, TRY004

from __future__ import annotations

import argparse
import hashlib
import io
import json
import math
import pathlib
import tempfile
import typing

import matplotlib
import matplotlib.axes
import matplotlib.figure
import matplotlib.patches

_FIGURE_DIR = pathlib.Path(__file__).resolve().parent
_CONTRACT_PATH = _FIGURE_DIR / "data/predicted_history_concepts.json"

_TASK_PDF = "fig_task_contract.pdf"
_TASK_PNG = "fig_task_contract.png"
_RECEIPT = "fig_predicted_history_concepts_receipt.json"

# Colorblind-safe Okabe--Ito accents on restrained academic fills.
_INK = "#263238"
_MUTED = "#5F6B73"
_BLUE = "#0072B2"
_SKY = "#56B4E9"
_GREEN = "#009E73"
_AMBER = "#E69F00"
_VERMILION = "#D55E00"
_PALE_BLUE = "#EDF6FB"
_PALE_GREEN = "#ECF8F3"
_PALE_AMBER = "#FFF6E5"
_PALE_RED = "#FFF0EB"
_PALE_GRAY = "#F5F7F8"
_GRID = "#D5DBDE"

_PNG_DPI = 300
_TASK_SIZE = (6.75, 3.20)


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _file_record(path: pathlib.Path) -> dict[str, int | str]:
    payload = path.read_bytes()
    return {"bytes": len(payload), "sha256": _sha256_bytes(payload)}


def _load_contract(path: pathlib.Path) -> dict[str, typing.Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError("concept figure contract is unreadable") from error
    if not isinstance(value, dict):
        raise ValueError("concept figure contract must be an object")
    _validate_contract(value)
    return value


def _validate_contract(contract: dict[str, typing.Any]) -> None:
    if contract.get("schema_version") != 1:
        raise ValueError("concept figure contract schema differs")
    history = contract.get("history_contract")
    if not isinstance(history, dict):
        raise ValueError("history contract is missing")
    if history.get("source") != "same_target_rolling_predictions_only":
        raise ValueError("history must contain rolling model predictions only")
    if history.get("reference_is_history_donor") is not False:
        raise ValueError("reference history is forbidden")
    if history.get("reference_is_inference_input") is not False:
        raise ValueError("reference input is forbidden")
    if history.get("reference_usage") != "scoring_only_after_inference":
        raise ValueError("reference must be scoring-only")
    if history.get("desired_current_audio_content_authority") is not True:
        raise ValueError("desired contract must assign audio content authority")

    serialized = json.dumps(contract, sort_keys=True).lower()
    if "oracle" in serialized or "ground-truth prior" in serialized:
        raise ValueError("reference-derived history language is forbidden")


def _box(
    axis: matplotlib.axes.Axes,
    x_position: float,
    y_position: float,
    width: float,
    height: float,
    *,
    face: str,
    edge: str,
    linewidth: float = 1.0,
    linestyle: str = "-",
    radius: float = 0.012,
    zorder: float = 1.0,
) -> matplotlib.patches.FancyBboxPatch:
    patch = matplotlib.patches.FancyBboxPatch(
        (x_position, y_position),
        width,
        height,
        boxstyle=f"round,pad=0.008,rounding_size={radius}",
        facecolor=face,
        edgecolor=edge,
        linewidth=linewidth,
        linestyle=linestyle,
        zorder=zorder,
    )
    axis.add_patch(patch)
    return patch


def _arrow(
    axis: matplotlib.axes.Axes,
    start: tuple[float, float],
    end: tuple[float, float],
    *,
    color: str,
    linewidth: float = 1.6,
    linestyle: str = "-",
    connectionstyle: str = "arc3,rad=0",
    zorder: float = 3.0,
) -> None:
    axis.add_patch(
        matplotlib.patches.FancyArrowPatch(
            start,
            end,
            arrowstyle="-|>",
            mutation_scale=10,
            color=color,
            linewidth=linewidth,
            linestyle=linestyle,
            connectionstyle=connectionstyle,
            shrinkA=2,
            shrinkB=2,
            zorder=zorder,
        )
    )


def _new_figure(
    size: tuple[float, float],
) -> tuple[matplotlib.figure.Figure, matplotlib.axes.Axes]:
    figure = matplotlib.figure.Figure(figsize=size, facecolor="white")
    axis = figure.add_axes((0, 0, 1, 1))
    axis.set_xlim(0, 1)
    axis.set_ylim(0, 1)
    axis.axis("off")
    return figure, axis


def _draw_header(
    axis: matplotlib.axes.Axes,
    *,
    title: str,
    subtitle: str,
) -> None:
    axis.text(
        0.025,
        0.955,
        title,
        ha="left",
        va="top",
        fontsize=10.2,
        fontweight="bold",
        color=_INK,
    )
    axis.text(
        0.975,
        0.952,
        subtitle,
        ha="right",
        va="top",
        fontsize=7.0,
        color=_MUTED,
    )
    axis.plot(
        (0.025, 0.975),
        (0.905, 0.905),
        color=_GRID,
        linewidth=0.8,
        zorder=0,
    )


def _draw_history_stack(
    axis: matplotlib.axes.Axes,
    task: dict[str, typing.Any],
    history: dict[str, typing.Any],
) -> None:
    _box(
        axis,
        0.025,
        0.415,
        0.245,
        0.395,
        face=_PALE_GREEN,
        edge=_GREEN,
        linewidth=1.2,
    )
    axis.text(
        0.1475,
        0.765,
        task["history_title"],
        ha="center",
        va="center",
        fontsize=8.4,
        fontweight="bold",
        color=_INK,
    )
    _box(
        axis,
        0.072,
        0.697,
        0.151,
        0.035,
        face="white",
        edge=_GREEN,
        linewidth=0.8,
        radius=0.008,
    )
    axis.text(
        0.1475,
        0.714,
        task["history_badge"],
        ha="center",
        va="center",
        fontsize=5.8,
        fontweight="bold",
        color=_GREEN,
    )
    for index, label in enumerate(task["history_cards"]):
        y_position = 0.620 - index * 0.071
        _box(
            axis,
            0.055 + index * 0.004,
            y_position,
            0.185,
            0.047,
            face="white",
            edge="#A9D9C8",
            linewidth=0.7,
            radius=0.007,
        )
        axis.text(
            0.1475 + index * 0.004,
            y_position + 0.0235,
            label,
            ha="center",
            va="center",
            fontsize=6.5,
            color=_INK,
        )
    eligibility = (
        f"{history['eligibility'][0]} · {history['eligibility'][1]}\n"
        f"{history['eligibility'][2]} · {history['eligibility'][3]}\n"
        f"{history['eligibility'][4]}"
    )
    axis.text(
        0.1475,
        0.445,
        eligibility,
        ha="center",
        va="center",
        fontsize=5.2,
        color=_MUTED,
        linespacing=1.08,
    )


def _draw_audio(
    axis: matplotlib.axes.Axes,
    task: dict[str, typing.Any],
) -> None:
    _box(
        axis,
        0.315,
        0.495,
        0.165,
        0.250,
        face=_PALE_BLUE,
        edge=_BLUE,
        linewidth=1.3,
    )
    axis.text(
        0.3975,
        0.700,
        task["audio_title"],
        ha="center",
        va="center",
        fontsize=8.4,
        fontweight="bold",
        color=_INK,
    )
    x_values = [0.337 + index * 0.00068 for index in range(180)]
    wave_values = []
    for index, _ in enumerate(x_values):
        envelope = math.sin(math.pi * index / 179) ** 0.7
        carrier = 0.020 * math.sin(25 * math.pi * index / 179)
        carrier += 0.012 * math.sin(43 * math.pi * index / 179)
        wave_values.append(0.620 + envelope * carrier)
    axis.plot(x_values, wave_values, color=_BLUE, linewidth=0.9)
    axis.text(
        0.3975,
        0.545,
        task["audio_subtitle"],
        ha="center",
        va="center",
        fontsize=6.5,
        fontweight="bold",
        color=_BLUE,
    )


def _draw_interface_and_output(
    axis: matplotlib.axes.Axes,
    task: dict[str, typing.Any],
) -> None:
    _box(
        axis,
        0.575,
        0.405,
        0.200,
        0.410,
        face=_PALE_GRAY,
        edge=_INK,
        linewidth=1.2,
    )
    axis.text(
        0.675,
        0.755,
        task["interface_title"],
        ha="center",
        va="center",
        fontsize=8.4,
        fontweight="bold",
        color=_INK,
    )
    axis.text(
        0.675,
        0.705,
        task["interface_subtitle"],
        ha="center",
        va="center",
        fontsize=6.6,
        color=_MUTED,
    )
    axis.plot((0.605, 0.745), (0.665, 0.665), color=_GRID, linewidth=0.8)
    _box(
        axis,
        0.607,
        0.565,
        0.136,
        0.055,
        face=_PALE_GREEN,
        edge=_GREEN,
        linewidth=0.8,
        radius=0.008,
    )
    axis.text(
        0.675,
        0.592,
        "history text channel",
        ha="center",
        va="center",
        fontsize=6.3,
        color=_GREEN,
        fontweight="bold",
    )
    _box(
        axis,
        0.607,
        0.475,
        0.136,
        0.055,
        face=_PALE_BLUE,
        edge=_BLUE,
        linewidth=0.8,
        radius=0.008,
    )
    axis.text(
        0.675,
        0.502,
        "current audio channel",
        ha="center",
        va="center",
        fontsize=6.3,
        color=_BLUE,
        fontweight="bold",
    )
    axis.text(
        0.675,
        0.437,
        "generate one transcript",
        ha="center",
        va="center",
        fontsize=6.1,
        color=_MUTED,
    )

    _box(
        axis,
        0.835,
        0.505,
        0.140,
        0.230,
        face=_PALE_AMBER,
        edge=_VERMILION,
        linewidth=1.3,
    )
    axis.text(
        0.905,
        0.682,
        task["output_title"],
        ha="center",
        va="center",
        fontsize=7.9,
        fontweight="bold",
        color=_INK,
    )
    axis.text(
        0.905,
        0.615,
        task["output_subtitle"],
        ha="center",
        va="center",
        fontsize=8.3,
        fontweight="bold",
        color=_VERMILION,
    )
    axis.text(
        0.905,
        0.550,
        "no prior/future turn",
        ha="center",
        va="center",
        fontsize=5.9,
        color=_MUTED,
    )


def _draw_task_arrows(
    axis: matplotlib.axes.Axes,
    task: dict[str, typing.Any],
) -> None:
    _arrow(
        axis,
        (0.274, 0.665),
        (0.570, 0.600),
        color=_GREEN,
        linewidth=1.5,
        linestyle="--",
        connectionstyle="arc3,rad=-0.08",
    )
    axis.text(
        0.421,
        0.850,
        task["history_role"],
        ha="center",
        va="center",
        fontsize=6.7,
        color=_GREEN,
        fontweight="bold",
    )
    axis.text(
        0.421,
        0.820,
        "must not supply transcript content",
        ha="center",
        va="center",
        fontsize=5.9,
        color=_MUTED,
    )
    _arrow(
        axis,
        (0.485, 0.565),
        (0.570, 0.505),
        color=_BLUE,
        linewidth=2.2,
        connectionstyle="arc3,rad=0.08",
    )
    axis.text(
        0.520,
        0.390,
        task["audio_role"],
        ha="center",
        va="center",
        fontsize=6.7,
        color=_BLUE,
        fontweight="bold",
        rotation=-8,
    )
    _arrow(axis, (0.780, 0.610), (0.830, 0.610), color=_VERMILION)

    axis.plot(
        (0.905, 0.905, 0.1475),
        (0.500, 0.285, 0.285),
        color=_AMBER,
        linewidth=1.2,
        linestyle="--",
    )
    _arrow(
        axis,
        (0.1475, 0.285),
        (0.1475, 0.408),
        color=_AMBER,
        linewidth=1.2,
        linestyle="--",
    )
    axis.text(
        0.535,
        0.255,
        task["feedback"],
        ha="center",
        va="center",
        fontsize=6.2,
        color=_MUTED,
    )


def _render_task(contract: dict[str, typing.Any]) -> matplotlib.figure.Figure:
    task = contract["task"]
    history = contract["history_contract"]
    figure, axis = _new_figure(_TASK_SIZE)
    _draw_header(
        axis,
        title=task["title"],
        subtitle="deployment trajectory · no reference-derived history",
    )
    _draw_history_stack(axis, task, history)
    _draw_audio(axis, task)
    _draw_interface_and_output(axis, task)
    _draw_task_arrows(axis, task)

    _box(
        axis,
        0.025,
        0.035,
        0.950,
        0.105,
        face="white",
        edge=_GRID,
        linewidth=0.9,
        linestyle="--",
    )
    axis.text(
        0.500,
        0.087,
        task["scoring"],
        ha="center",
        va="center",
        fontsize=6.6,
        color=_INK,
        fontweight="bold",
    )
    axis.text(
        0.041,
        0.144,
        "SCORING-ONLY BOUNDARY",
        ha="left",
        va="bottom",
        fontsize=5.6,
        color=_MUTED,
        fontweight="bold",
    )
    return figure


def _figure_bytes(
    figure: matplotlib.figure.Figure,
    *,
    output_format: str,
    title: str,
) -> bytes:
    buffer = io.BytesIO()
    if output_format == "pdf":
        metadata: dict[str, typing.Any] = {
            "Title": title,
            "Author": "Anonymous",
            "Creator": "gen_fig_predicted_history_concepts.py",
            "Producer": "Matplotlib",
            "CreationDate": None,
            "ModDate": None,
        }
        figure.savefig(
            buffer,
            format="pdf",
            metadata=metadata,
            facecolor="white",
        )
    else:
        figure.savefig(
            buffer,
            format="png",
            dpi=_PNG_DPI,
            metadata={"Software": "gen_fig_predicted_history_concepts.py"},
            facecolor="white",
        )
    return buffer.getvalue()


def _write_if_changed(path: pathlib.Path, payload: bytes) -> None:
    if path.exists() and path.read_bytes() == payload:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        dir=path.parent,
        prefix=f".{path.name}.",
        delete=False,
    ) as handle:
        temporary_path = pathlib.Path(handle.name)
        handle.write(payload)
        handle.flush()
    temporary_path.chmod(0o644)
    temporary_path.replace(path)


def _render_outputs(
    contract: dict[str, typing.Any],
) -> dict[str, bytes]:
    task_figure = _render_task(contract)
    try:
        return {
            _TASK_PDF: _figure_bytes(
                task_figure,
                output_format="pdf",
                title="Prediction-only rolling context at inference",
            ),
            _TASK_PNG: _figure_bytes(
                task_figure,
                output_format="png",
                title="Prediction-only rolling context at inference",
            ),
        }
    finally:
        task_figure.clear()


def generate_figure_bundle(
    *,
    contract_path: pathlib.Path = _CONTRACT_PATH,
    output_dir: pathlib.Path = _FIGURE_DIR,
) -> dict[str, typing.Any]:
    """Generate the task-contract figure and an authentication receipt.

    Args:
        contract_path: Publication-safe JSON task contract.
        output_dir: Destination for PDF, PNG, and receipt artifacts.

    Returns:
        The exact receipt written beside the generated figures.

    Raises:
        ValueError: The input contract is malformed or violates prediction-only
            history requirements.
    """
    contract_path = contract_path.resolve()
    output_dir = output_dir.resolve()
    contract = _load_contract(contract_path)
    with matplotlib.rc_context(
        {
            "font.family": "sans-serif",
            "font.sans-serif": ["DejaVu Sans"],
            "font.size": 8,
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
            "savefig.bbox": None,
            "savefig.pad_inches": 0,
        }
    ):
        outputs = _render_outputs(contract)
    for name, payload in outputs.items():
        _write_if_changed(output_dir / name, payload)

    history = contract["history_contract"]
    receipt: dict[str, typing.Any] = {
        "schema_version": 1,
        "artifact_kind": "predicted_history_concept_figures",
        "contract": {
            "bytes": contract_path.stat().st_size,
            "sha256": _sha256_bytes(contract_path.read_bytes()),
        },
        "figure_contract": {
            "history_source": history["source"],
            "reference_is_inference_input": history[
                "reference_is_inference_input"
            ],
            "reference_is_history_donor": history["reference_is_history_donor"],
        },
        "outputs": {
            name: _file_record(output_dir / name) for name in sorted(outputs)
        },
    }
    receipt_payload = (
        json.dumps(receipt, indent=2, sort_keys=True) + "\n"
    ).encode()
    _write_if_changed(output_dir / _RECEIPT, receipt_payload)
    return receipt


def parser() -> argparse.ArgumentParser:
    """Return the aggregate-only concept-figure parser."""
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument(
        "--output-dir",
        type=pathlib.Path,
        default=_FIGURE_DIR,
    )
    return value


def main(argv: typing.Sequence[str] | None = None) -> None:
    """Generate the checked-in prediction-only conceptual figure bundle."""
    arguments = parser().parse_args(argv)
    generate_figure_bundle(output_dir=arguments.output_dir)


if __name__ == "__main__":
    main()
