## Predicted-history stress study

| Model variant | Serving condition | WER | CER | Insertions/100 | Copy rate | Prior-only rate |
|---|---|---:|---:|---:|---:|---:|
| Untuned base model | No-history-interface prompt control | 38.02 | 28.27 | 4.15 | 0.00 | 0.00 |
| Untuned base model | Ordered same-source K=8 predicted history | 37.28 | 26.53 | 7.33 | 1.61 | 8.31 |
| Untuned base model | Unrelated plausible predicted history | 39.74 | 29.12 | 6.42 | 1.00 | 7.06 |
| Untuned base model | Stale same-source predicted history | 36.66 | 26.67 | 6.65 | 2.01 | 8.08 |
| Untuned base model | Shuffled same-source predicted history | 35.98 | 25.73 | 6.48 | 2.01 | 8.60 |
| History-free SFT | No-history-interface prompt control | 28.36 | 21.04 | 3.21 | 0.00 | 0.00 |
| History-free SFT | Ordered same-source K=8 predicted history | 25.15 | 18.23 | 3.44 | 1.00 | 6.25 |
| History-free SFT | Unrelated plausible predicted history | 28.07 | 20.46 | 3.50 | 0.20 | 3.68 |
| History-free SFT | Stale same-source predicted history | 25.22 | 18.31 | 3.24 | 1.41 | 5.92 |
| History-free SFT | Shuffled same-source predicted history | 25.71 | 18.05 | 3.73 | 1.20 | 6.89 |
| Prior-transcript SFT | No-history-interface prompt control | 28.33 | 20.96 | 3.50 | 0.00 | 0.00 |
| Prior-transcript SFT | Ordered same-source K=8 predicted history | 24.93 | 18.33 | 3.70 | 2.41 | 7.94 |
| Prior-transcript SFT | Unrelated plausible predicted history | 29.89 | 22.05 | 4.28 | 1.00 | 5.92 |
| Prior-transcript SFT | Stale same-source predicted history | 26.39 | 19.79 | 3.79 | 1.81 | 8.07 |
| Prior-transcript SFT | Shuffled same-source predicted history | 25.58 | 18.51 | 3.60 | 2.61 | 7.95 |

Lexical measures are reference-proxy diagnostics, not semantic hallucination metrics.
