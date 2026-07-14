## Fixed-interface predicted-history matrix

Primary quality population: n=4098 paired rows.

| Model variant | Serving condition | WER | CER | Insertions/100 | Copy rate | Prior-only rate |
|---|---|---:|---:|---:|---:|---:|
| Untuned base model | K=0 (empty history) | 32.56 | 23.31 | 3.78 | 0.00 | 0.00 |
| Untuned base model | No-history-interface prompt control | 32.54 | 23.21 | 3.75 | 0.00 | 0.00 |
| Untuned base model | K=1 | 33.43 | 23.34 | 5.58 | 3.70 | 3.94 |
| Untuned base model | K=2 | 33.51 | 23.10 | 6.04 | 4.09 | 4.88 |
| Untuned base model | K=4 | 32.48 | 22.64 | 5.44 | 4.19 | 5.45 |
| Untuned base model | K=8 | 32.14 | 22.41 | 5.70 | 4.39 | 6.05 |
| History-free SFT | K=0 (empty history) | 23.85 | 16.41 | 3.15 | 0.00 | 0.00 |
| History-free SFT | No-history-interface prompt control | 23.78 | 16.54 | 2.90 | 0.00 | 0.00 |
| History-free SFT | K=1 | 23.34 | 16.16 | 3.53 | 1.87 | 2.01 |
| History-free SFT | K=2 | 22.89 | 15.67 | 3.33 | 2.33 | 2.47 |
| History-free SFT | K=4 | 22.30 | 15.32 | 3.31 | 2.76 | 3.18 |
| History-free SFT | K=8 | 21.99 | 15.05 | 3.18 | 2.96 | 3.72 |
| Prior-transcript SFT | K=0 (empty history) | 23.78 | 16.43 | 3.65 | 0.00 | 0.00 |
| Prior-transcript SFT | No-history-interface prompt control | 23.99 | 16.80 | 3.42 | 0.00 | 0.00 |
| Prior-transcript SFT | K=1 | 23.00 | 15.80 | 3.75 | 1.96 | 2.05 |
| Prior-transcript SFT | K=2 | 22.58 | 15.55 | 3.76 | 2.76 | 3.00 |
| Prior-transcript SFT | K=4 | 22.14 | 15.29 | 3.63 | 3.94 | 4.02 |
| Prior-transcript SFT | K=8 | 22.01 | 15.15 | 3.77 | 4.40 | 4.84 |

### Paired deltas

| Comparison | n | Paired delta WER | 95% CI | Holm | Status |
|---|---:|---:|---:|---:|---|
| K=1 − K=0 Untuned base model | 4098 | 0.87 | [0.06, 1.81] | 0.13 | available |
| K=2 − K=0 Untuned base model | 4098 | 0.95 | [-0.04, 2.13] | 0.18 | available |
| K=4 − K=0 Untuned base model | 4098 | -0.07 | [-0.98, 1.01] | 0.87 | available |
| K=8 − K=0 Untuned base model | 4098 | -0.42 | [-1.35, 0.66] | 0.83 | available |
| K=1 − K=0 History-free SFT | 4098 | -0.51 | [-1.01, 0.02] | 0.06 | available |
| K=2 − K=0 History-free SFT | 4098 | -0.96 | [-1.45, -0.45] | 0.00 | available |
| K=4 − K=0 History-free SFT | 4098 | -1.54 | [-2.07, -0.99] | 0/10,000 | available |
| K=8 − K=0 History-free SFT | 4098 | -1.85 | [-2.42, -1.25] | 0/10,000 | available |
| K=1 − K=0 Prior-transcript SFT | 4098 | -0.78 | [-1.27, -0.28] | 0.00 | available |
| K=2 − K=0 Prior-transcript SFT | 4098 | -1.20 | [-1.72, -0.70] | 0/10,000 | available |
| K=4 − K=0 Prior-transcript SFT | 4098 | -1.63 | [-2.24, -0.99] | 0/10,000 | available |
| K=8 − K=0 Prior-transcript SFT | 4098 | -1.76 | [-2.46, -1.05] | 0/10,000 | available |
| K=0 − No-history-interface prompt control Untuned base model | 4098 | 0.02 | [-0.44, 0.50] | — | available |
| K=0 − No-history-interface prompt control History-free SFT | 4098 | 0.06 | [-0.38, 0.48] | — | available |
| K=0 − No-history-interface prompt control Prior-transcript SFT | 4098 | -0.22 | [-0.76, 0.28] | — | available |

K=0 renders the fixed history interface with an explicit empty-history block. The no-history-interface prompt control uses a distinct prompt and wrapper and is not a point on the K-window curve.
