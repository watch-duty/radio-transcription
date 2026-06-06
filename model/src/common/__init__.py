"""Shared transcription library for the radio-transcription model subtree.

``common/`` is the engine layer used by every notebook in ``model/colabs/``.
The scoring, manifest, GCS, audio, auth, and prompt helpers each live in
their own focused module; ASR inference is split across three modules on a
model × data-source axis — see ``inference_pipeline_runner.py``'s docstring
for the module map.
"""
