# Generated reviewer-facing figures

Do not edit figure bytes by hand. The task diagram and three result figures are
generated from publication-safe contracts or aggregate analysis; no generator
opens audio, transcript text, row-level predictions, or source locators.

```bash
uv run --script paper/figures/gen_fig_predicted_history_concepts.py
uv run --script paper/figures/gen_fig_predicted_history_extension_v1.py
```

`gen_fig_predicted_history_concepts.py` renders only
`fig_task_contract.{pdf,png}` and a digest receipt from
`data/predicted_history_concepts.json`. The contract states that history comes
only from same-model rolling predictions and that references are joined only
after inference for scoring.

`gen_fig_predicted_history_extension_v1.py` reads only
`experiments/results/predicted_history_extension_analysis.json`. It validates
the complete 18-cell matrix, places K=0 at the origin of the fixed-interface
curve, keeps the no-history-interface prompt control detached, and emits:

- `data/predicted_history_extension_v1.{csv,json}`;
- `fig_predicted_history_extension_exact_p13_v1.{pdf,png}`;
- `fig_predicted_history_extension_matrix_v1.{pdf,png}`;
- `fig_predicted_history_extension_tradeoff_v1.{pdf,png}`; and
- `fig_predicted_history_extension_v1_receipt.json`.

The historical `exact_p13` filename is immutable provenance; figures, captions,
and paper prose use “fixed history interface.” Superseded pre-extension matrix,
window, and trade-off figures are not reviewer-facing and are excluded from the
sanitized release package.

For a clean replay:

```bash
FIGURE_REPLAY_DIR="$(mktemp -d)"
uv run --script paper/figures/gen_fig_predicted_history_extension_v1.py \
  --output-dir "$FIGURE_REPLAY_DIR"
cmp "$FIGURE_REPLAY_DIR/data/predicted_history_extension_v1.csv" \
  paper/figures/data/predicted_history_extension_v1.csv
cmp "$FIGURE_REPLAY_DIR/data/predicted_history_extension_v1.json" \
  paper/figures/data/predicted_history_extension_v1.json
cmp "$FIGURE_REPLAY_DIR/fig_predicted_history_extension_exact_p13_v1.pdf" \
  paper/figures/fig_predicted_history_extension_exact_p13_v1.pdf
cmp "$FIGURE_REPLAY_DIR/fig_predicted_history_extension_exact_p13_v1.png" \
  paper/figures/fig_predicted_history_extension_exact_p13_v1.png
cmp "$FIGURE_REPLAY_DIR/fig_predicted_history_extension_matrix_v1.pdf" \
  paper/figures/fig_predicted_history_extension_matrix_v1.pdf
cmp "$FIGURE_REPLAY_DIR/fig_predicted_history_extension_matrix_v1.png" \
  paper/figures/fig_predicted_history_extension_matrix_v1.png
cmp "$FIGURE_REPLAY_DIR/fig_predicted_history_extension_tradeoff_v1.pdf" \
  paper/figures/fig_predicted_history_extension_tradeoff_v1.pdf
cmp "$FIGURE_REPLAY_DIR/fig_predicted_history_extension_tradeoff_v1.png" \
  paper/figures/fig_predicted_history_extension_tradeoff_v1.png
cmp "$FIGURE_REPLAY_DIR/fig_predicted_history_extension_v1_receipt.json" \
  paper/figures/fig_predicted_history_extension_v1_receipt.json
```
