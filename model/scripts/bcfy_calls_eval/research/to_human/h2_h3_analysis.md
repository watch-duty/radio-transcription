# H2 / H3 — framing-effect slices (re-analysis, no new inference)

Improvement = baseline − moderate (capped WER, positive = framing better). Framed segments only.

## gemini  (n=746 framed)

**H2 — by metadata richness:**
| group | n | improvement | 95% CI |
|---|--:|--:|--:|
| 1 agency+service only | 29 | +1.06 | [-5.46, +7.87] |
| 2 +system | 337 | +0.14 | [-2.17, +2.40] |
| 3 +geo (county&state) | 380 | -0.18 | [-1.90, +1.51] |

**By service class** (WatchDuty cares about fire):
| group | n | improvement | 95% CI |
|---|--:|--:|--:|
| fire | 101 | -1.17 | [-3.81, +1.02] |
| fire/EMS | 21 | +3.06 | [-6.46, +14.97] |
| law enforcement | 607 | +0.20 | [-1.35, +1.75] |
| other | 4 | -8.21 | [-35.72, +8.57] |
| paging | 13 | -1.88 | [-5.16, +0.00] |

**H3 — by GT-word length bucket:**
| group | n | improvement | 95% CI |
|---|--:|--:|--:|
| 1-2 | 146 | -1.71 | [-6.16, +2.40] |
| 3-5 | 247 | +0.81 | [-1.79, +3.49] |
| 6-10 | 217 | +0.66 | [-1.12, +2.46] |
| 11-20 | 98 | -0.90 | [-3.68, +1.47] |
| 21+ | 38 | +0.09 | [-1.51, +1.52] |

## chirp  (n=746 framed)

**H2 — by metadata richness:**
| group | n | improvement | 95% CI |
|---|--:|--:|--:|
| 1 agency+service only | 29 | +0.29 | [-2.59, +3.45] |
| 2 +system | 337 | +2.90 | [+1.07, +4.81] |
| 3 +geo (county&state) | 380 | +2.00 | [+0.48, +3.61] |

**By service class** (WatchDuty cares about fire):
| group | n | improvement | 95% CI |
|---|--:|--:|--:|
| fire | 101 | +1.03 | [-1.77, +4.09] |
| fire/EMS | 21 | +1.47 | [-1.91, +4.85] |
| law enforcement | 607 | +2.60 | [+1.27, +3.99] |
| other | 4 | +5.54 | [+0.00, +11.25] |
| paging | 13 | +0.77 | [+0.00, +2.31] |

**H3 — by GT-word length bucket:**
| group | n | improvement | 95% CI |
|---|--:|--:|--:|
| 1-2 | 146 | +5.82 | [+2.40, +9.93] |
| 3-5 | 247 | +2.12 | [-0.26, +4.58] |
| 6-10 | 217 | +1.16 | [-0.25, +2.55] |
| 11-20 | 98 | +0.82 | [-0.59, +2.27] |
| 21+ | 38 | +1.08 | [-0.40, +2.70] |
