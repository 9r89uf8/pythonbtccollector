# Receipt-clock leader-risk companion

These descriptive grids use one initial paired cohort per panel and checkpoint.
The raw_twap basis uses the current received TWAP. The projected_close basis
uses the continuation forecast of the closing price. Each basis defines its own
leader, X and Y, so a row may change direction or cell. Projected close is never
relabeled as the current TWAP. Original leader-risk artifacts remain unchanged.

X is the absolute reference-price lead over the decision-time strike in basis
points. Y is leader-signed spot minus the selected reference, divided by that
strike and expressed in basis points. Exact reference=strike ties have no leader;
they are excluded separately for each basis and counted in coverage.csv.

N includes unknown official outcomes U; resolved n=N-U; L is the number of
resolved outcomes in which that basis's leader loses. Loss rate is L/n, blank
when n=0. Empty grid cells remain explicit. There are no confidence intervals,
profitability tests, execution assumptions, or mispricing claims here.

Panel membership is inherited from the validated receipt pilot's primary and
fresh_3s paired availability flags. Coverage before each basis's tie exclusions
is identical. This companion does not independently reconstruct source arrivals.
