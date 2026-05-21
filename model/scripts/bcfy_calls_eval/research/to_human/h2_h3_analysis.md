# H2 / H3 — framing-effect slices (re-analysis, no new inference)

Improvement = baseline − moderate (capped WER, positive = framing better). Framed segments only.

## gemini  (n=746 framed)

**H2 — by metadata richness:**
| group | n | improvement | 95% CI |
|---|--:|--:|--:|
| 1 agency+service only | 29 | -1.77 | [-8.40, +3.41] |
| 2 +system | 337 | +3.22 | [+0.82, +5.66] |
| 3 +geo (county&state) | 380 | +1.87 | [-0.65, +4.33] |

**By service class** (WatchDuty cares about fire):
| group | n | improvement | 95% CI |
|---|--:|--:|--:|
| fire | 101 | +0.54 | [-3.35, +4.58] |
| fire/EMS | 21 | +2.38 | [+0.00, +7.14] |
| law enforcement | 607 | +2.69 | [+0.72, +4.65] |
| other | 4 | -1.18 | [-17.43, +11.25] |
| paging | 13 | +1.03 | [+0.00, +3.08] |

**H3 — by GT-word length bucket:**
| group | n | improvement | 95% CI |
|---|--:|--:|--:|
| 1-2 | 162 | +4.94 | [+0.00, +9.88] |
| 3-5 | 241 | +3.51 | [+0.76, +6.41] |
| 6-10 | 211 | +1.28 | [-1.19, +3.78] |
| 11-20 | 94 | -2.65 | [-6.68, +0.62] |
| 21+ | 38 | +2.03 | [+0.52, +3.62] |

## chirp  (n=746 framed)

**H2 — by metadata richness:**
| group | n | improvement | 95% CI |
|---|--:|--:|--:|
| 1 agency+service only | 29 | -0.27 | [-2.79, +2.92] |
| 2 +system | 337 | +3.07 | [+1.07, +5.09] |
| 3 +geo (county&state) | 380 | +1.26 | [-0.39, +2.94] |

**By service class** (WatchDuty cares about fire):
| group | n | improvement | 95% CI |
|---|--:|--:|--:|
| fire | 101 | +1.05 | [-1.81, +4.19] |
| fire/EMS | 21 | -2.15 | [-11.59, +4.36] |
| law enforcement | 607 | +2.33 | [+0.91, +3.79] |
| other | 4 | +3.75 | [+0.00, +11.25] |
| paging | 13 | +1.14 | [+0.00, +3.13] |

**H3 — by GT-word length bucket:**
| group | n | improvement | 95% CI |
|---|--:|--:|--:|
| 1-2 | 162 | +6.48 | [+3.09, +10.19] |
| 3-5 | 241 | +0.10 | [-2.39, +2.60] |
| 6-10 | 211 | +2.06 | [+0.59, +3.58] |
| 11-20 | 94 | -0.55 | [-3.01, +1.65] |
| 21+ | 38 | +1.20 | [-0.74, +3.30] |
