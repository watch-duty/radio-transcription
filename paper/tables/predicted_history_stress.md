## Predicted-history stress study

| Cell | WER | CER | Insertions/100 | Copy rate | Prior-only rate |
|---|---:|---:|---:|---:|---:|
| S-r0-base | 38.02 | 28.27 | 4.15 | 0.00 | 0.00 |
| S-correct_r8-base | 37.28 | 26.53 | 7.33 | 1.61 | 8.31 |
| S-unrelated_predicted-base | 39.74 | 29.12 | 6.42 | 1.00 | 7.06 |
| S-stale_same_source_predicted-base | 36.66 | 26.67 | 6.65 | 2.01 | 8.08 |
| S-shuffled_correct_predicted-base | 35.98 | 25.73 | 6.48 | 2.01 | 8.60 |
| S-r0-no_context_sft | 28.36 | 21.04 | 3.21 | 0.00 | 0.00 |
| S-correct_r8-no_context_sft | 25.15 | 18.23 | 3.44 | 1.00 | 6.25 |
| S-unrelated_predicted-no_context_sft | 28.07 | 20.46 | 3.50 | 0.20 | 3.68 |
| S-stale_same_source_predicted-no_context_sft | 25.22 | 18.31 | 3.24 | 1.41 | 5.92 |
| S-shuffled_correct_predicted-no_context_sft | 25.71 | 18.05 | 3.73 | 1.20 | 6.89 |
| S-r0-prior_context_sft | 28.33 | 20.96 | 3.50 | 0.00 | 0.00 |
| S-correct_r8-prior_context_sft | 24.93 | 18.33 | 3.70 | 2.41 | 7.94 |
| S-unrelated_predicted-prior_context_sft | 29.89 | 22.05 | 4.28 | 1.00 | 5.92 |
| S-stale_same_source_predicted-prior_context_sft | 26.39 | 19.79 | 3.79 | 1.81 | 8.07 |
| S-shuffled_correct_predicted-prior_context_sft | 25.58 | 18.51 | 3.60 | 2.61 | 7.95 |

Lexical measures are reference-proxy diagnostics, not semantic hallucination metrics.
