## Reference-absent prediction lexical stress

Quality population: n=498 paired rows; candidate-adoption ITT population: n=500.

| Cell | WER | CER | Insertions/100 | Candidate adoption |
|---|---:|---:|---:|---:|
| S-correct_r8-base | 37.28 | 26.53 | 7.33 | 0.00 |
| S-reference_absent_entity_predicted-base | 37.28 | 26.70 | 7.39 | 0.80 |
| S-correct_r8-no_context_sft | 25.15 | 18.23 | 3.44 | 0.00 |
| S-reference_absent_entity_predicted-no_context_sft | 25.51 | 18.27 | 3.24 | 0.40 |
| S-correct_r8-prior_context_sft | 24.93 | 18.33 | 3.70 | 0.00 |
| S-reference_absent_entity_predicted-prior_context_sft | 25.45 | 18.75 | 3.92 | 1.80 |

### Paired deltas

| Target | Quality n | Paired delta WER | 95% CI | Holm | Adoption n | Candidate-adoption delta | 95% CI | Holm | Status |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| base | 498 | 0.00 | [-1.95, 1.89] | 1.00 | 500 | 0.80 | [0.00, 2.15] | 0.45 | available |
| no_context_sft | 498 | 0.36 | [-1.12, 1.78] | 1.00 | 500 | 0.40 | [0.00, 1.37] | 0.47 | available |
| prior_context_sft | 498 | 0.52 | [-1.60, 2.16] | 1.00 | 500 | 1.80 | [0.20, 3.90] | 0.11 | available |

Candidate adoption is an intent-to-treat lexical diagnostic. It does not measure acoustic support or semantic hallucination.
