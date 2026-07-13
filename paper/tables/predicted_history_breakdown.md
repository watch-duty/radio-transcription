# Predicted-history descriptive breakdown

**Post-outcome exploratory analysis.** Intervals are descriptive 95% source-cluster-then-row percentile intervals; subgroup patterns are not confirmatory or causal.

| Dimension | Stratum | Rows | Clusters | Target | R0 WER | R1 WER | R8 WER | R8 - R1 (pp) |
|---|---:|---:|---:|---|---:|---:|---:|---:|
| duration_seconds | q1 | 1026 | 294 | base | 59.68 [54.68, 64.55] | 71.78 [63.85, 79.94] | 69.78 [61.18, 79.30] | -1.99 [-7.30, 3.62] |
| duration_seconds | q1 | 1026 | 294 | no_context_sft | 39.43 [34.97, 43.85] | 41.07 [35.30, 47.24] | 36.73 [32.19, 41.61] | -4.34 [-8.24, -0.99] |
| duration_seconds | q1 | 1026 | 294 | prior_context_sft | 36.73 [32.38, 41.11] | 37.08 [32.24, 42.05] | 34.96 [30.23, 39.79] | -2.13 [-4.80, 0.35] |
| duration_seconds | q2 | 1024 | 312 | base | 45.23 [41.25, 49.22] | 46.57 [41.97, 51.28] | 44.36 [40.14, 48.71] | -2.20 [-4.19, -0.23] |
| duration_seconds | q2 | 1024 | 312 | no_context_sft | 32.09 [28.78, 35.31] | 31.48 [28.20, 34.85] | 29.00 [25.89, 32.17] | -2.48 [-4.22, -0.94] |
| duration_seconds | q2 | 1024 | 312 | prior_context_sft | 32.89 [29.61, 36.16] | 31.05 [27.89, 34.26] | 29.52 [26.25, 32.78] | -1.52 [-3.09, 0.05] |
| duration_seconds | q3 | 1023 | 369 | base | 38.87 [35.60, 42.05] | 38.63 [35.13, 42.35] | 37.20 [33.78, 40.70] | -1.43 [-2.72, -0.17] |
| duration_seconds | q3 | 1023 | 369 | no_context_sft | 27.68 [25.05, 30.27] | 26.82 [24.31, 29.31] | 25.32 [22.98, 27.69] | -1.50 [-2.56, -0.43] |
| duration_seconds | q3 | 1023 | 369 | prior_context_sft | 28.12 [25.48, 30.77] | 26.45 [23.89, 29.00] | 25.15 [22.71, 27.64] | -1.30 [-2.42, -0.16] |
| duration_seconds | q4 | 1025 | 435 | base | 21.61 [19.65, 23.85] | 21.19 [19.23, 23.43] | 20.34 [18.33, 22.73] | -0.85 [-1.42, -0.20] |
| duration_seconds | q4 | 1025 | 435 | no_context_sft | 17.07 [15.56, 18.75] | 16.56 [15.09, 18.17] | 16.08 [14.52, 17.86] | -0.48 [-1.04, 0.09] |
| duration_seconds | q4 | 1025 | 435 | prior_context_sft | 17.44 [15.85, 19.16] | 16.81 [15.27, 18.53] | 16.31 [14.68, 18.22] | -0.50 [-1.13, 0.17] |
| normalized_reference_word_count | q1 | 1279 | 357 | base | 57.46 [52.46, 62.31] | 67.74 [59.76, 76.14] | 65.63 [57.37, 74.78] | -2.11 [-6.83, 3.09] |
| normalized_reference_word_count | q1 | 1279 | 357 | no_context_sft | 38.65 [34.38, 43.03] | 40.43 [34.84, 46.61] | 35.81 [31.53, 40.16] | -4.62 [-8.46, -1.42] |
| normalized_reference_word_count | q1 | 1279 | 357 | prior_context_sft | 37.18 [33.07, 41.41] | 36.59 [31.93, 41.43] | 34.41 [30.16, 38.69] | -2.18 [-4.79, 0.35] |
| normalized_reference_word_count | q2 | 894 | 307 | base | 43.62 [39.50, 47.69] | 44.73 [40.17, 49.30] | 42.06 [37.76, 46.36] | -2.67 [-4.82, -0.43] |
| normalized_reference_word_count | q2 | 894 | 307 | no_context_sft | 29.15 [25.86, 32.41] | 27.63 [24.41, 30.92] | 25.72 [22.71, 28.70] | -1.92 [-3.41, -0.53] |
| normalized_reference_word_count | q2 | 894 | 307 | prior_context_sft | 29.50 [26.25, 32.83] | 27.66 [24.58, 30.78] | 26.50 [23.27, 29.84] | -1.16 [-2.88, 0.55] |
| normalized_reference_word_count | q3 | 1012 | 376 | base | 35.93 [32.77, 38.96] | 35.61 [32.40, 38.82] | 34.34 [30.94, 37.75] | -1.27 [-2.41, -0.11] |
| normalized_reference_word_count | q3 | 1012 | 376 | no_context_sft | 25.65 [23.12, 28.05] | 25.10 [22.65, 27.43] | 23.59 [21.33, 25.90] | -1.50 [-2.54, -0.48] |
| normalized_reference_word_count | q3 | 1012 | 376 | prior_context_sft | 26.47 [24.01, 28.85] | 25.37 [22.95, 27.77] | 24.15 [21.74, 26.59] | -1.22 [-2.28, -0.15] |
| normalized_reference_word_count | q4 | 913 | 413 | base | 23.04 [20.78, 25.64] | 22.72 [20.39, 25.34] | 21.96 [19.53, 24.65] | -0.77 [-1.40, -0.11] |
| normalized_reference_word_count | q4 | 913 | 413 | no_context_sft | 18.54 [16.78, 20.55] | 18.03 [16.29, 19.97] | 17.54 [15.64, 19.69] | -0.49 [-1.09, 0.14] |
| normalized_reference_word_count | q4 | 913 | 413 | prior_context_sft | 18.72 [16.95, 20.68] | 17.94 [16.21, 19.87] | 17.34 [15.50, 19.40] | -0.59 [-1.23, 0.09] |
| source_family | family_4c3745b67e8b2870112abf88a5a2b61b2c830468532bfe7a2e87256e0cd80284 | 212 | 58 | base | 35.65 [29.15, 42.72] | 35.03 [28.47, 42.08] | 37.28 [29.50, 45.92] | 2.24 [-0.33, 5.84] |
| source_family | family_4c3745b67e8b2870112abf88a5a2b61b2c830468532bfe7a2e87256e0cd80284 | 212 | 58 | no_context_sft | 26.73 [21.13, 32.74] | 26.26 [20.47, 32.28] | 25.58 [20.20, 31.07] | -0.68 [-2.28, 0.58] |
| source_family | family_4c3745b67e8b2870112abf88a5a2b61b2c830468532bfe7a2e87256e0cd80284 | 212 | 58 | prior_context_sft | 27.89 [22.13, 34.04] | 26.19 [20.61, 31.96] | 25.65 [20.11, 31.39] | -0.54 [-1.86, 0.66] |
| source_family | family_cfe4a0c17333295ddaf2e81d70107fff8776f38a35428023e9e714d555eaaf0a | 1020 | 25 | base | 39.73 [34.80, 45.58] | 42.25 [35.99, 49.81] | 38.99 [32.82, 46.34] | -3.25 [-5.10, -1.68] |
| source_family | family_cfe4a0c17333295ddaf2e81d70107fff8776f38a35428023e9e714d555eaaf0a | 1020 | 25 | no_context_sft | 29.20 [24.59, 34.01] | 28.49 [23.75, 33.60] | 26.48 [21.73, 31.57] | -2.01 [-3.64, -0.43] |
| source_family | family_cfe4a0c17333295ddaf2e81d70107fff8776f38a35428023e9e714d555eaaf0a | 1020 | 25 | prior_context_sft | 29.31 [24.86, 34.20] | 28.08 [23.65, 32.72] | 26.89 [22.25, 31.88] | -1.18 [-2.89, 0.56] |
| source_family | family_d2bdf7e7e9214a9b572ef85ec0e5e3fe6c2569a36dd7b7b866cc9ac9d90409b5 | 1239 | 35 | base | 31.86 [25.44, 40.64] | 33.85 [26.48, 44.55] | 32.56 [25.03, 43.50] | -1.29 [-3.12, 0.64] |
| source_family | family_d2bdf7e7e9214a9b572ef85ec0e5e3fe6c2569a36dd7b7b866cc9ac9d90409b5 | 1239 | 35 | no_context_sft | 25.14 [21.01, 30.75] | 25.24 [21.04, 31.04] | 22.54 [18.49, 28.14] | -2.70 [-4.27, -1.39] |
| source_family | family_d2bdf7e7e9214a9b572ef85ec0e5e3fe6c2569a36dd7b7b866cc9ac9d90409b5 | 1239 | 35 | prior_context_sft | 24.94 [21.08, 30.17] | 24.47 [20.47, 29.94] | 22.33 [18.24, 27.88] | -2.15 [-3.49, -0.69] |
| source_family | family_d9802a9e56c66fc8b496baa71556b77db30a8fe5e1c876f03d6611556d69afa0 | 1627 | 497 | base | 29.22 [27.00, 31.54] | 28.84 [26.68, 31.21] | 28.07 [25.93, 30.36] | -0.77 [-1.38, -0.20] |
| source_family | family_d9802a9e56c66fc8b496baa71556b77db30a8fe5e1c876f03d6611556d69afa0 | 1627 | 497 | no_context_sft | 20.07 [18.40, 21.87] | 19.43 [17.74, 21.22] | 19.14 [17.46, 20.94] | -0.29 [-0.78, 0.17] |
| source_family | family_d9802a9e56c66fc8b496baa71556b77db30a8fe5e1c876f03d6611556d69afa0 | 1627 | 497 | prior_context_sft | 20.48 [18.73, 22.31] | 19.35 [17.64, 21.15] | 19.12 [17.43, 20.87] | -0.23 [-0.69, 0.22] |

R1 and R8 use the fixed P13 prompt; R0 uses the registered context-free prompt, so R0-to-nonzero differences remain prompt-plus-history bundles. Companion JSON/CSV include automatic context-copy and prior-only token reference proxies where their history-conditioned denominators are defined. Context-copy is the percentage of successful rows with nonempty safety-tokenized realized history whose prediction has a contiguous span of at least three tokens found within one prior turn but absent from the current reference; rows without realized history contribute to neither numerator nor denominator. Prior-only token rate uses all safety-token occurrences in successful predictions as its denominator. This automatic proxy analysis does not measure semantic hallucination or safety.

Reproduction requires the ignored authenticated input binding and an owner-only exact 32-byte HMAC salt supplied through `$OWNER_ONLY_HMAC_SALT_FILE`; neither private input is published.
The result JSON records a locked-environment command using fresh `reproductions/` destinations plus byte-comparison commands; canonical create-only destinations are never overwritten.
