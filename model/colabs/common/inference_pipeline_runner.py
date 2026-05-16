"""Compatibility shim — re-exports from the split modules.

Notebooks import ``from common.inference_pipeline_runner import …``. Phase 2 will
retrofit them to import from the focused submodules directly. Until then, this shim
preserves the old import path (Pitfall 7).

Do NOT add eager imports of heavy modules here — that defeats the [hf] extra gating
(Pitfall 8). All imports are deferred via ``__getattr__``.
"""


def __getattr__(name: str):
    if name == "run_inference_pipeline":
        from common.inference_nemo import run_inference_pipeline
        return run_inference_pipeline
    if name == "run_huggingface_inference_pipeline":
        from common.inference_hf import run_huggingface_inference_pipeline
        return run_huggingface_inference_pipeline
    if name == "run_test_baseline_inference_evaluation":
        from common.baseline_eval import run_test_baseline_inference_evaluation
        return run_test_baseline_inference_evaluation
    raise AttributeError(
        f"module 'common.inference_pipeline_runner' has no attribute {name!r}"
    )
