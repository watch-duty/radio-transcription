"""Compatibility shim — re-exports from the split inference modules.

Inference functions are split across three modules on a (model framework x
data source) axis:

=========================  =========================  ==============================
                           GCS-manifest data          Streaming HF dataset
-------------------------  -------------------------  ------------------------------
HuggingFace transformers   ``inference_hf``           ``public_dataset_evaluation``
NeMo SALM                  ``inference_nemo``         (n/a)
=========================  =========================  ==============================

Notebooks import ``from common.inference_pipeline_runner import …``. A later
retrofit will route them to the focused submodules directly; until then this
shim preserves the old import path.

Do NOT add eager imports of heavy modules here — that defeats the [hf] extra
gating. All imports are deferred via ``__getattr__``.
"""

from importlib import import_module
from typing import Any

_EXPORTS = {
    "run_inference_pipeline": (
        "common.inference_nemo",
        "run_inference_pipeline",
    ),
    "run_huggingface_inference_pipeline": (
        "common.inference_hf",
        "run_huggingface_inference_pipeline",
    ),
    "run_test_baseline_inference_evaluation": (
        "common.public_dataset_evaluation",
        "run_test_baseline_inference_evaluation",
    ),
}


def __getattr__(name: str) -> Any:
    if name in _EXPORTS:
        module_name, attr_name = _EXPORTS[name]
        return getattr(import_module(module_name), attr_name)
    msg = f"module 'common.inference_pipeline_runner' has no attribute {name!r}"
    raise AttributeError(msg)
