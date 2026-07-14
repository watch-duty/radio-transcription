# Current generated tables only

Do not edit numerical tables by hand. They are generated from checked
aggregate results by the commands in [`paper/README.md`](../README.md) and must
agree with the experiment ledger, claims--evidence matrix, and output receipts.
The aggregate-only renderer is
[`render_public_tables.py`](render_public_tables.py); immutable internal run IDs
are translated to reviewer-facing K and model-variant labels only in this
presentation layer.

The authoritative prediction-only tables are:

- [`predicted_history_extension_main.md`](predicted_history_extension_main.md),
  the current 18-cell main matrix with fixed-interface K=0 controls and paired
  contrasts;
- [`predicted_history_reference_absent_entity.md`](predicted_history_reference_absent_entity.md),
  the 500-row intent-to-treat/498-row quality lexical-stress extension;
- [`predicted_history_stress.md`](predicted_history_stress.md); and
- [`predicted_history_breakdown.md`](predicted_history_breakdown.md), when the
  post-outcome exploratory breakdown package is present.

In the main and stress tables, `Copy rate` means a lexical
context-copy event proxy divided by that model-variant/condition's successful rows
with nonempty tokenized history: the prediction contains a contiguous span of
at least three tokens found within one supplied turn but absent from the
current reference. K=0 and the no-history-interface prompt control display
zero by construction, but they are different prompt/interface conditions. Neither copy rate nor the
reference-absent candidate-adoption diagnostic is a semantic hallucination
rate. Reference absence does not establish acoustic absence.

Earlier MAIN6, COST-MIN, and window tables used deployment-inadmissible
reference-derived history. They are retained only in internal repository
history and are excluded from the current paper and sanitized release
allowlist.
