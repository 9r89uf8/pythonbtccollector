# H3 — Optional validation of one low-risk condition

This optional follow-up to the [descriptive study](H3_TWAP_LEADER_RISK_STUDY.md) asks whether one condition supports a **1% maximum official losing-risk target** on new markets. It is a plan, not an active evaluation.

## Freeze one condition

Use the main study's full-history grids (available qualifying markets from
2026-08-16 onward) to choose at most one broad condition. Its September 10
pre/post comparison is descriptive, not a restriction on candidate selection:

> At checkpoint T, TWAP lead X >= X_min and spot-to-TWAP gap Y >= Y_min.

Record the candidates inspected, the chosen condition's historical frequency and its likely evaluation count. Before evaluation starts, freeze T, X_min, Y_min, the observation and exclusion rules, and the statistical procedure.

Use the main study's validated 60-second settlement identity, Decimal arithmetic, definitions and freshness rules. K comes from the exact TWAP event stamped at market start and actually received by the checkpoint. Count missing or late opening events as unavailable; never fill K from a later reconciliation. Retain the main study's sampled-spot limitation. Score the checkpoint's leader against the verified official winner.

## Observe seven new days

Record exact UTC start and exclusive end boundaries spanning **seven consecutive new UTC days**, with market membership determined by market start. The reporting cutoff is **24 hours after the last included market closes**. Freeze these boundaries before any evaluation markets begin.

Include every qualifying market once. Separate unavailable inputs from unknown outcomes. Do not change the condition, loosen exclusions or extend the period because the results look promising or the count is small. Preserve the outcome evidence available by the reporting cutoff.

## Calculate and report

At the fixed reporting cutoff, define:

- **N:** all qualifying markets.
- **L:** known official losses of the recorded leader.
- **U:** qualifying markets without a verified official Up/Down outcome.

Report the descriptive losing rate **L / (N - U)**, or unavailable when no outcomes are known. Calculate a **one-sided 95% Clopper–Pearson upper bound using L + U possible losses out of N**. Display unknowns separately from actual losses.

Report **Supported** only when this conservative upper bound is at most 1%; otherwise report **Not established**. With N = 0, the bound is unavailable. A bound above 1% does not prove the true risk exceeds 1%.

With zero losses and no unknown outcomes, the independent-market benchmark is **299 qualifying markets**. Seven days contain at most **2,016 markets**, requiring about **14.8% qualification** before missingness. See [NIST's exact binomial bounds](https://itl.nist.gov/div898/software/dataplot/refman2/auxillar/exacbino.htm).

Show daily and Up/Down counts descriptively. Support assumes independent, comparable markets and applies to the single frozen pooled condition and observation protocol over the evaluated period. It does not separately establish each direction's risk or guarantee future performance. One prespecified claim needs no multiple-condition adjustment.
