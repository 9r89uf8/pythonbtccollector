# Research Questions

These questions concern five-minute BTC Up/Down markets. They preserve the research objectives without carrying forward a study design, model, or threshold. The **settlement leader** is the side indicated by the settlement TWAP relative to the Price to Beat. The **market favorite** is the side favored by quoted market prices.

## Q1 — When is Up or Down locked?

- When can the current settlement leader be considered locked, and what chance remains that it loses officially?
- How does that risk change with time remaining, distance from the Price to Beat, and whether Up or Down leads?
- How reliable is the market favorite's apparent confidence compared with settlement risk, especially when the favorite and settlement leader disagree?
- Does spot or futures information improve this judgment beyond the current settlement TWAP, and how often does an apparent lock later reverse or become uncertain?

## Q2 — Where do flips happen in the final 20 seconds?

- When are settlement-leader changes and market-favorite changes most common, and when does the last change occur?
- How do Up-to-Down and Down-to-Up changes differ? How common are repeated changes, brief reversals, and changes that persist?
- How does flip risk vary with time remaining and the distance of TWAP, spot, and futures prices from the Price to Beat? How do observed changes differ from a current leader or favorite ultimately losing?
- What happens when the settlement TWAP is only **$10 from the Price to Beat**, compared with spot or futures being **$10 on one side of the Price to Beat while TWAP remains elsewhere**? How do these situations overlap or differ when the prices indicate opposite sides?

## Q3 — Which observable conditions increase late-flip risk?

- Which conditions add information beyond time remaining and the current TWAP margin: momentum, acceleration, volatility, price divergence, spot confirmation, or market confidence?
- What additional information comes from trade flow, order-book imbalance and depth, spreads, futures basis, funding, open interest, or observed market stress?
- Does the relationship between current spot/futures prices and the evolving TWAP help explain which apparent leads are vulnerable?
- Which patterns reflect a greater chance of a real flip, and which mainly reflect uncertainty from stale quotes, missing updates, or feed disagreement? How consistent are the relationships across directions and market conditions?

## Q4 — How long after a futures move does TWAP visibly respond?

- How long does it take for a futures move to be accompanied by a spot response, a settlement-TWAP response, and an update visible to an observer?
- How much of the observed delay comes from price response, oracle update timing, and delivery of the update?
- How do delays differ between futures-only moves and moves confirmed by spot, and with move size, duration, and market conditions? Does the response arrive before the market closes?

## Q5 — Can a brief futures move materially change TWAP?

- How do a move's size, duration, reversal, and spot confirmation relate to the size of the TWAP response? How do brief excursions compare with sustained moves?
- How much movement is shared between futures and spot, and how does spot movement relate to the subsequent TWAP response?
- How large is the temporary TWAP displacement, how much remains at settlement, and how do volatility, direction, distance from the Price to Beat, and time remaining affect those outcomes?
- Under what conditions can such a move carry TWAP across the Price to Beat or change the final winning side? How does a temporary change in the rolling TWAP differ from a change in the value used for final settlement?
