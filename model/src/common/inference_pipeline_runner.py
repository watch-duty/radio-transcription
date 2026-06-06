"""Compatibility shim — re-exports from the split inference modules.

Inference functions are split across three modules on a (model framework ×
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


def __getattr__(name: str):
    if name == "run_inference_pipeline":
        from common.inference_nemo import run_inference_pipeline

        return run_inference_pipeline
    if name == "run_huggingface_inference_pipeline":
        from common.inference_hf import run_huggingface_inference_pipeline

        return run_huggingface_inference_pipeline
    if name == "run_test_baseline_inference_evaluation":
        from common.public_dataset_evaluation import (
            run_test_baseline_inference_evaluation,
        )

        return run_test_baseline_inference_evaluation
    raise AttributeError(
        f"module 'common.inference_pipeline_runner' has no attribute {name!r}"
    )
