## Exact-P13 predicted-history extension

Primary quality population: n=4098 paired rows.

| Cell | WER | CER | Insertions/100 | Copy rate | Prior-only rate |
|---|---:|---:|---:|---:|---:|
| P0-base | 32.56 | 23.31 | 3.78 | 0.00 | 0.00 |
| R0-base | 32.54 | 23.21 | 3.75 | 0.00 | 0.00 |
| R1-base | 33.43 | 23.34 | 5.58 | 3.70 | 3.94 |
| R2-base | 33.51 | 23.10 | 6.04 | 4.09 | 4.88 |
| R4-base | 32.48 | 22.64 | 5.44 | 4.19 | 5.45 |
| R8-base | 32.14 | 22.41 | 5.70 | 4.39 | 6.05 |
| P0-no_context_sft | 23.85 | 16.41 | 3.15 | 0.00 | 0.00 |
| R0-no_context_sft | 23.78 | 16.54 | 2.90 | 0.00 | 0.00 |
| R1-no_context_sft | 23.34 | 16.16 | 3.53 | 1.87 | 2.01 |
| R2-no_context_sft | 22.89 | 15.67 | 3.33 | 2.33 | 2.47 |
| R4-no_context_sft | 22.30 | 15.32 | 3.31 | 2.76 | 3.18 |
| R8-no_context_sft | 21.99 | 15.05 | 3.18 | 2.96 | 3.72 |
| P0-prior_context_sft | 23.78 | 16.43 | 3.65 | 0.00 | 0.00 |
| R0-prior_context_sft | 23.99 | 16.80 | 3.42 | 0.00 | 0.00 |
| R1-prior_context_sft | 23.00 | 15.80 | 3.75 | 1.96 | 2.05 |
| R2-prior_context_sft | 22.58 | 15.55 | 3.76 | 2.76 | 3.00 |
| R4-prior_context_sft | 22.14 | 15.29 | 3.63 | 3.94 | 4.02 |
| R8-prior_context_sft | 22.01 | 15.15 | 3.77 | 4.40 | 4.84 |

### Paired deltas

| Comparison | n | Paired delta WER | 95% CI | Holm | Status |
|---|---:|---:|---:|---:|---|
| p13_r1_minus_p0_base | 4098 | 0.87 | [0.06, 1.81] | 0.13 | available |
| p13_r2_minus_p0_base | 4098 | 0.95 | [-0.04, 2.13] | 0.18 | available |
| p13_r4_minus_p0_base | 4098 | -0.07 | [-0.98, 1.01] | 0.87 | available |
| p13_r8_minus_p0_base | 4098 | -0.42 | [-1.35, 0.66] | 0.83 | available |
| p13_r1_minus_p0_no_context_sft | 4098 | -0.51 | [-1.01, 0.02] | 0.06 | available |
| p13_r2_minus_p0_no_context_sft | 4098 | -0.96 | [-1.45, -0.45] | 0.00 | available |
| p13_r4_minus_p0_no_context_sft | 4098 | -1.54 | [-2.07, -0.99] | 0.00 | available |
| p13_r8_minus_p0_no_context_sft | 4098 | -1.85 | [-2.42, -1.25] | 0.00 | available |
| p13_r1_minus_p0_prior_context_sft | 4098 | -0.78 | [-1.27, -0.28] | 0.00 | available |
| p13_r2_minus_p0_prior_context_sft | 4098 | -1.20 | [-1.72, -0.70] | 0.00 | available |
| p13_r4_minus_p0_prior_context_sft | 4098 | -1.63 | [-2.24, -0.99] | 0.00 | available |
| p13_r8_minus_p0_prior_context_sft | 4098 | -1.76 | [-2.46, -1.05] | 0.00 | available |
| prompt_p0_minus_option_d_r0_base | 4098 | 0.02 | [-0.44, 0.50] | — | available |
| prompt_p0_minus_option_d_r0_no_context_sft | 4098 | 0.06 | [-0.38, 0.48] | — | available |
| prompt_p0_minus_option_d_r0_prior_context_sft | 4098 | -0.22 | [-0.76, 0.28] | — | available |

P0 is the exact-P13 structural no-history control. Option-D R0 is a distinct prompt/interface control and is not on the P13 window curve.
