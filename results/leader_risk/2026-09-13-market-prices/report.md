# Market prices beside TWAP leader risk

Frozen study: **2026-08-16T00:00:00.000Z** through **2026-09-12T21:15:00.000Z** (exclusive), **8,014 markets / 64,112 checkpoint rows**. Official outcomes remain frozen at **2026-09-12T22:17:21.776Z**. Retained quote history was extracted at **2026-09-13T00:40:38.993Z**.

The quoted token is the recorded TWAP leader: Up or Down. The full study and quote-matched subset have separate loss counts; an affordable ask band is scored on its own observations.

A matched quote requires correct token identity, a retained sample and row receipt no more than 3,000 ms old, independently fresh leader bid and ask source/receipt clocks, and bid <= ask. All clocks must be at or before the checkpoint; the freshness limit is inclusive. Fresh ask-only and bid-only counts remain separate. Opposite-outcome quote freshness does not refresh or invalidate the leader's sides.

**These are retained sampled quotes, not executable prices or fills.** The one-second sampler can skip missing-side states, so a later withdrawal may be absent while an older retained quote still passes the age rule. Boundary prices of exactly $0 and $1 remain reported and counted; neither establishes orderability. No fees, slippage, execution delay, profit or trading edge are estimated here.

## Coverage and matched prices

| T (seconds) | Full-study N | Matched N | Matched losses/resolved; interval; U | Median bid | Median ask | Matched ask=0 / ask=1 |
|---:|---:|---:|:---|---:|---:|---:|
| 120 | 7498 | 6068 | 1142/6043 (18.90%; 95% 17.93%–19.90%); U=25 | $0.88 | $0.89 | 0 / 516 |
| 90 | 7473 | 6098 | 921/6073 (15.17%; 95% 14.29%–16.09%); U=25 | $0.94 | $0.95 | 0 / 1345 |
| 60 | 7487 | 6275 | 633/6250 (10.13%; 95% 9.40%–10.90%); U=25 | $0.98 | $0.99 | 0 / 2884 |
| 30 | 7471 | 6785 | 359/6758 (5.31%; 95% 4.80%–5.87%); U=27 | $0.99 | $1.00 | 0 / 5553 |
| 15 | 7489 | 6960 | 214/6933 (3.09%; 95% 2.70%–3.52%); U=27 | $0.99 | $1.00 | 0 / 6418 |
| 10 | 7477 | 7005 | 142/6980 (2.03%; 95% 1.73%–2.39%); U=25 | $0.99 | $1.00 | 0 / 6707 |
| 5 | 7493 | 7071 | 81/7045 (1.15%; 95% 0.93%–1.43%); U=26 | $0.99 | $1.00 | 0 / 6940 |
| 3 | 7497 | 7099 | 61/7074 (0.86%; 95% 0.67%–1.11%); U=25 | $0.99 | $1.00 | 0 / 7001 |

[coverage.csv](coverage.csv) retains ask/bid availability separately and overlapping exclusion reasons. `ask_only_N` and `bid_only_N` mean exactly one fresh side; `paired_quote_N` means both sides are fresh, including any crossed pair. `matched_N` additionally rejects crossed pairs. Loss rates and Wilson intervals use resolved observations only; U remains explicit.

## 120 seconds remaining

Each cell: median ask/bid; matched loss rate and interval; U; matched/full N; ask=1 count.

| X (bp) \ Y (bp) | <-2 | [-2,0) | [0,2) | >=2 |
|:---|:---|:---|:---|:---|
| [0,1) | ask $0.23; bid $0.22<br>66/89 (74.16%; 95% 64.20%–82.12%); U=0<br>N=89/135; ask=1: 0 | ask $0.51; bid $0.50<br>207/418 (49.52%; 95% 44.75%–54.30%); U=1<br>N=419/494; ask=1: 0 | ask $0.70; bid $0.69<br>135/479 (28.18%; 95% 24.34%–32.37%); U=1<br>N=480/558; ask=1: 0 | ask $0.81; bid $0.79<br>9/69 (13.04%; 95% 7.02%–22.97%); U=0<br>N=69/116; ask=1: 1 |
| [1,2) | ask $0.35; bid $0.34<br>43/73 (58.90%; 95% 47.45%–69.47%); U=0<br>N=73/118; ask=1: 0 | ask $0.65; bid $0.64<br>103/309 (33.33%; 95% 28.31%–38.77%); U=0<br>N=309/373; ask=1: 0 | ask $0.83; bid $0.82<br>71/382 (18.59%; 95% 15.01%–22.79%); U=2<br>N=384/464; ask=1: 0 | ask $0.88; bid $0.87<br>11/90 (12.22%; 95% 6.96%–20.57%); U=1<br>N=91/125; ask=1: 0 |
| [2,4) | ask $0.46; bid $0.45<br>57/95 (60.00%; 95% 49.95%–69.28%); U=1<br>N=96/182; ask=1: 0 | ask $0.78; bid $0.77<br>119/410 (29.02%; 95% 24.84%–33.60%); U=0<br>N=410/524; ask=1: 0 | ask $0.90; bid $0.89<br>77/564 (13.65%; 95% 11.06%–16.73%); U=0<br>N=564/663; ask=1: 2 | ask $0.92; bid $0.91<br>16/165 (9.70%; 95% 6.06%–15.17%); U=0<br>N=165/234; ask=1: 1 |
| [4,8) | ask $0.67; bid $0.66<br>58/157 (36.94%; 95% 29.79%–44.72%); U=2<br>N=159/272; ask=1: 0 | ask $0.89; bid $0.88<br>59/404 (14.60%; 95% 11.49%–18.38%); U=0<br>N=404/498; ask=1: 2 | ask $0.95; bid $0.94<br>39/600 (6.50%; 95% 4.79%–8.76%); U=1<br>N=601/673; ask=1: 21 | ask $0.96; bid $0.95<br>14/263 (5.32%; 95% 3.20%–8.74%); U=3<br>N=266/345; ask=1: 22 |
| [8,infinity) | ask $0.92; bid $0.91<br>29/257 (11.28%; 95% 7.97%–15.74%); U=3<br>N=260/345; ask=1: 16 | ask $0.97; bid $0.96<br>8/272 (2.94%; 95% 1.50%–5.70%); U=3<br>N=275/322; ask=1: 44 | ask $0.99; bid $0.98<br>9/426 (2.11%; 95% 1.12%–3.97%); U=3<br>N=429/467; ask=1: 147 | ask $0.999; bid $0.99<br>12/521 (2.30%; 95% 1.32%–3.98%); U=4<br>N=525/590; ask=1: 260 |

## 90 seconds remaining

Each cell: median ask/bid; matched loss rate and interval; U; matched/full N; ask=1 count.

| X (bp) \ Y (bp) | <-2 | [-2,0) | [0,2) | >=2 |
|:---|:---|:---|:---|:---|
| [0,1) | ask $0.18; bid $0.17<br>61/71 (85.92%; 95% 75.98%–92.17%); U=0<br>N=71/115; ask=1: 0 | ask $0.51; bid $0.50<br>209/409 (51.10%; 95% 46.27%–55.91%); U=1<br>N=410/481; ask=1: 0 | ask $0.76; bid $0.75<br>97/406 (23.89%; 95% 20.00%–28.27%); U=1<br>N=407/492; ask=1: 0 | ask $0.90; bid $0.89<br>8/78 (10.26%; 95% 5.29%–18.95%); U=2<br>N=80/128; ask=1: 2 |
| [1,2) | ask $0.29; bid $0.28<br>54/70 (77.14%; 95% 66.05%–85.41%); U=1<br>N=71/119; ask=1: 0 | ask $0.68; bid $0.67<br>85/261 (32.57%; 95% 27.17%–38.47%); U=1<br>N=262/340; ask=1: 0 | ask $0.90; bid $0.89<br>44/371 (11.86%; 95% 8.95%–15.55%); U=1<br>N=372/448; ask=1: 1 | ask $0.94; bid $0.93<br>3/75 (4.00%; 95% 1.37%–11.11%); U=0<br>N=75/112; ask=1: 7 |
| [2,4) | ask $0.455; bid $0.445<br>74/127 (58.27%; 95% 49.57%–66.48%); U=1<br>N=128/192; ask=1: 0 | ask $0.84; bid $0.83<br>79/385 (20.52%; 95% 16.79%–24.84%); U=0<br>N=385/493; ask=1: 1 | ask $0.95; bid $0.94<br>47/557 (8.44%; 95% 6.40%–11.04%); U=0<br>N=557/644; ask=1: 11 | ask $0.97; bid $0.96<br>11/153 (7.19%; 95% 4.06%–12.41%); U=0<br>N=153/226; ask=1: 15 |
| [4,8) | ask $0.70; bid $0.69<br>47/150 (31.33%; 95% 24.45%–39.14%); U=0<br>N=150/246; ask=1: 0 | ask $0.95; bid $0.94<br>36/396 (9.09%; 95% 6.64%–12.33%); U=2<br>N=398/515; ask=1: 30 | ask $0.98; bid $0.97<br>17/599 (2.84%; 95% 1.78%–4.50%); U=0<br>N=599/675; ask=1: 139 | ask $0.99; bid $0.98<br>4/298 (1.34%; 95% 0.52%–3.40%); U=1<br>N=299/363; ask=1: 116 |
| [8,infinity) | ask $0.96; bid $0.9545<br>32/278 (11.51%; 95% 8.27%–15.80%); U=4<br>N=282/368; ask=1: 76 | ask $0.999; bid $0.99<br>2/339 (0.59%; 95% 0.16%–2.13%); U=3<br>N=342/374; ask=1: 168 | ask $1.00; bid $0.99<br>7/458 (1.53%; 95% 0.74%–3.12%); U=4<br>N=462/506; ask=1: 310 | ask $1.00; bid $0.99<br>4/592 (0.68%; 95% 0.26%–1.72%); U=3<br>N=595/636; ask=1: 469 |

## 60 seconds remaining

Each cell: median ask/bid; matched loss rate and interval; U; matched/full N; ask=1 count.

| X (bp) \ Y (bp) | <-2 | [-2,0) | [0,2) | >=2 |
|:---|:---|:---|:---|:---|
| [0,1) | ask $0.095; bid $0.08<br>69/76 (90.79%; 95% 82.19%–95.47%); U=0<br>N=76/119; ask=1: 0 | ask $0.565; bid $0.55<br>175/380 (46.05%; 95% 41.11%–51.08%); U=0<br>N=380/451; ask=1: 0 | ask $0.89; bid $0.87<br>46/379 (12.14%; 95% 9.22%–15.81%); U=1<br>N=380/459; ask=1: 1 | ask $0.955; bid $0.945<br>4/60 (6.67%; 95% 2.62%–15.93%); U=0<br>N=60/91; ask=1: 6 |
| [1,2) | ask $0.17; bid $0.15<br>52/63 (82.54%; 95% 71.38%–89.96%); U=1<br>N=64/100; ask=1: 0 | ask $0.815; bid $0.80<br>64/249 (25.70%; 95% 20.67%–31.47%); U=1<br>N=250/347; ask=1: 0 | ask $0.97; bid $0.95<br>16/325 (4.92%; 95% 3.05%–7.85%); U=0<br>N=325/397; ask=1: 22 | ask $0.98; bid $0.97<br>4/68 (5.88%; 95% 2.31%–14.17%); U=0<br>N=68/91; ask=1: 10 |
| [2,4) | ask $0.37; bid $0.36<br>65/100 (65.00%; 95% 55.25%–73.64%); U=0<br>N=100/171; ask=1: 0 | ask $0.95; bid $0.94<br>40/380 (10.53%; 95% 7.83%–14.02%); U=1<br>N=381/467; ask=1: 26 | ask $0.99; bid $0.98<br>24/580 (4.14%; 95% 2.80%–6.08%); U=0<br>N=580/677; ask=1: 164 | ask $1.00; bid $0.99<br>2/170 (1.18%; 95% 0.32%–4.19%); U=3<br>N=173/217; ask=1: 91 |
| [4,8) | ask $0.82; bid $0.805<br>43/150 (28.67%; 95% 22.03%–36.36%); U=0<br>N=150/247; ask=1: 2 | ask $0.99; bid $0.98<br>8/426 (1.88%; 95% 0.95%–3.66%); U=2<br>N=428/511; ask=1: 181 | ask $1.00; bid $0.99<br>6/647 (0.93%; 95% 0.43%–2.01%); U=1<br>N=648/715; ask=1: 493 | ask $1.00; bid $0.99<br>2/307 (0.65%; 95% 0.18%–2.34%); U=2<br>N=309/355; ask=1: 255 |
| [8,infinity) | ask $1.00; bid $0.99<br>11/366 (3.01%; 95% 1.69%–5.30%); U=1<br>N=367/438; ask=1: 227 | ask $1.00; bid $0.99<br>2/412 (0.49%; 95% 0.13%–1.75%); U=7<br>N=419/445; ask=1: 340 | ask $1.00; bid $0.99<br>0/535 (0.00%; 95% 0.00%–0.71%); U=2<br>N=537/581; ask=1: 511 | ask $1.00; bid $0.99<br>0/577 (0.00%; 95% 0.00%–0.66%); U=3<br>N=580/608; ask=1: 555 |

## 30 seconds remaining

Each cell: median ask/bid; matched loss rate and interval; U; matched/full N; ask=1 count.

| X (bp) \ Y (bp) | <-2 | [-2,0) | [0,2) | >=2 |
|:---|:---|:---|:---|:---|
| [0,1) | ask $0.02; bid $0.01<br>61/65 (93.85%; 95% 85.22%–97.58%); U=0<br>N=65/82; ask=1: 0 | ask $0.75; bid $0.73<br>140/337 (41.54%; 95% 36.41%–46.87%); U=0<br>N=337/417; ask=1: 18 | ask $0.99; bid $0.98<br>14/397 (3.53%; 95% 2.11%–5.83%); U=2<br>N=399/458; ask=1: 111 | ask $1.00; bid $0.99<br>0/69 (0.00%; 95% 0.00%–5.27%); U=1<br>N=70/91; ask=1: 60 |
| [1,2) | ask $0.105; bid $0.095<br>51/66 (77.27%; 95% 65.83%–85.71%); U=0<br>N=66/95; ask=1: 0 | ask $0.99; bid $0.98<br>24/239 (10.04%; 95% 6.84%–14.51%); U=1<br>N=240/285; ask=1: 99 | ask $1.00; bid $0.99<br>4/343 (1.17%; 95% 0.45%–2.96%); U=1<br>N=344/379; ask=1: 282 | ask $1.00; bid $0.99<br>1/101 (0.99%; 95% 0.17%–5.40%); U=0<br>N=101/116; ask=1: 95 |
| [2,4) | ask $0.76; bid $0.67<br>43/91 (47.25%; 95% 37.32%–57.41%); U=0<br>N=91/141; ask=1: 10 | ask $1.00; bid $0.99<br>6/480 (1.25%; 95% 0.57%–2.70%); U=0<br>N=480/530; ask=1: 380 | ask $1.00; bid $0.99<br>0/615 (0.00%; 95% 0.00%–0.62%); U=0<br>N=615/658; ask=1: 608 | ask $1.00; bid $0.99<br>0/158 (0.00%; 95% 0.00%–2.37%); U=2<br>N=160/180; ask=1: 158 |
| [4,8) | ask $1.00; bid $0.99<br>13/207 (6.28%; 95% 3.71%–10.45%); U=3<br>N=210/250; ask=1: 154 | ask $1.00; bid $0.99<br>1/539 (0.19%; 95% 0.03%–1.04%); U=2<br>N=541/565; ask=1: 529 | ask $1.00; bid $0.99<br>0/664 (0.00%; 95% 0.00%–0.58%); U=2<br>N=666/696; ask=1: 666 | ask $1.00; bid $0.99<br>0/284 (0.00%; 95% 0.00%–1.33%); U=0<br>N=284/301; ask=1: 283 |
| [8,infinity) | ask $1.00; bid $0.99<br>1/436 (0.23%; 95% 0.04%–1.29%); U=1<br>N=437/465; ask=1: 429 | ask $1.00; bid $0.99<br>0/440 (0.00%; 95% 0.00%–0.87%); U=3<br>N=443/465; ask=1: 440 | ask $1.00; bid $0.99<br>0/618 (0.00%; 95% 0.00%–0.62%); U=1<br>N=619/659; ask=1: 615 | ask $1.00; bid $0.99<br>0/609 (0.00%; 95% 0.00%–0.63%); U=8<br>N=617/638; ask=1: 616 |

## 15 seconds remaining

Each cell: median ask/bid; matched loss rate and interval; U; matched/full N; ask=1 count.

| X (bp) \ Y (bp) | <-2 | [-2,0) | [0,2) | >=2 |
|:---|:---|:---|:---|:---|
| [0,1) | ask $0.01; bid $0.00<br>58/62 (93.55%; 95% 84.55%–97.46%); U=0<br>N=62/91; ask=1: 0 | ask $0.988; bid $0.97<br>105/352 (29.83%; 95% 25.29%–34.81%); U=0<br>N=352/420; ask=1: 124 | ask $1.00; bid $0.99<br>8/396 (2.02%; 95% 1.03%–3.94%); U=0<br>N=396/449; ask=1: 280 | ask $1.00; bid $0.99<br>0/65 (0.00%; 95% 0.00%–5.58%); U=1<br>N=66/76; ask=1: 64 |
| [1,2) | ask $0.795; bid $0.775<br>27/66 (40.91%; 95% 29.87%–52.95%); U=0<br>N=66/85; ask=1: 12 | ask $1.00; bid $0.99<br>3/273 (1.10%; 95% 0.37%–3.18%); U=0<br>N=273/303; ask=1: 251 | ask $1.00; bid $0.99<br>1/353 (0.28%; 95% 0.05%–1.59%); U=1<br>N=354/376; ask=1: 352 | ask $1.00; bid $0.99<br>0/84 (0.00%; 95% 0.00%–4.37%); U=1<br>N=85/96; ask=1: 84 |
| [2,4) | ask $1.00; bid $0.99<br>10/127 (7.87%; 95% 4.33%–13.89%); U=1<br>N=128/151; ask=1: 99 | ask $1.00; bid $0.99<br>1/480 (0.21%; 95% 0.04%–1.17%); U=0<br>N=480/513; ask=1: 476 | ask $1.00; bid $0.99<br>0/578 (0.00%; 95% 0.00%–0.66%); U=0<br>N=578/608; ask=1: 576 | ask $1.00; bid $0.99<br>0/161 (0.00%; 95% 0.00%–2.33%); U=1<br>N=162/178; ask=1: 162 |
| [4,8) | ask $1.00; bid $0.99<br>1/198 (0.51%; 95% 0.09%–2.80%); U=2<br>N=200/214; ask=1: 189 | ask $1.00; bid $0.99<br>0/589 (0.00%; 95% 0.00%–0.65%); U=2<br>N=591/614; ask=1: 589 | ask $1.00; bid $0.99<br>0/689 (0.00%; 95% 0.00%–0.55%); U=1<br>N=690/720; ask=1: 688 | ask $1.00; bid $0.99<br>0/277 (0.00%; 95% 0.00%–1.37%); U=4<br>N=281/304; ask=1: 281 |
| [8,infinity) | ask $1.00; bid $0.99<br>0/417 (0.00%; 95% 0.00%–0.91%); U=0<br>N=417/440; ask=1: 415 | ask $1.00; bid $0.99<br>0/493 (0.00%; 95% 0.00%–0.77%); U=4<br>N=497/516; ask=1: 495 | ask $1.00; bid $0.99<br>0/651 (0.00%; 95% 0.00%–0.59%); U=5<br>N=656/689; ask=1: 656 | ask $1.00; bid $0.99<br>0/622 (0.00%; 95% 0.00%–0.61%); U=4<br>N=626/646; ask=1: 625 |

## 10 seconds remaining

Each cell: median ask/bid; matched loss rate and interval; U; matched/full N; ask=1 count.

| X (bp) \ Y (bp) | <-2 | [-2,0) | [0,2) | >=2 |
|:---|:---|:---|:---|:---|
| [0,1) | ask $0.01; bid $0.00<br>54/61 (88.52%; 95% 78.16%–94.33%); U=0<br>N=61/93; ask=1: 2 | ask $1.00; bid $0.99<br>73/345 (21.16%; 95% 17.18%–25.77%); U=0<br>N=345/390; ask=1: 205 | ask $1.00; bid $0.99<br>4/405 (0.99%; 95% 0.38%–2.51%); U=1<br>N=406/447; ask=1: 349 | ask $1.00; bid $0.99<br>0/62 (0.00%; 95% 0.00%–5.83%); U=0<br>N=62/75; ask=1: 62 |
| [1,2) | ask $1.00; bid $0.99<br>9/52 (17.31%; 95% 9.38%–29.73%); U=0<br>N=52/76; ask=1: 29 | ask $1.00; bid $0.99<br>1/294 (0.34%; 95% 0.06%–1.90%); U=0<br>N=294/315; ask=1: 288 | ask $1.00; bid $0.99<br>1/362 (0.28%; 95% 0.05%–1.55%); U=0<br>N=362/378; ask=1: 362 | ask $1.00; bid $0.99<br>0/74 (0.00%; 95% 0.00%–4.93%); U=1<br>N=75/84; ask=1: 75 |
| [2,4) | ask $1.00; bid $0.99<br>0/133 (0.00%; 95% 0.00%–2.81%); U=1<br>N=134/149; ask=1: 128 | ask $1.00; bid $0.99<br>0/490 (0.00%; 95% 0.00%–0.78%); U=0<br>N=490/509; ask=1: 488 | ask $1.00; bid $0.99<br>0/580 (0.00%; 95% 0.00%–0.66%); U=0<br>N=580/615; ask=1: 580 | ask $1.00; bid $0.99<br>0/160 (0.00%; 95% 0.00%–2.34%); U=0<br>N=160/178; ask=1: 160 |
| [4,8) | ask $1.00; bid $0.99<br>0/200 (0.00%; 95% 0.00%–1.88%); U=1<br>N=201/213; ask=1: 200 | ask $1.00; bid $0.99<br>0/594 (0.00%; 95% 0.00%–0.64%); U=2<br>N=596/622; ask=1: 596 | ask $1.00; bid $0.99<br>0/697 (0.00%; 95% 0.00%–0.55%); U=1<br>N=698/729; ask=1: 697 | ask $1.00; bid $0.99<br>0/257 (0.00%; 95% 0.00%–1.47%); U=4<br>N=261/284; ask=1: 261 |
| [8,infinity) | ask $1.00; bid $0.99<br>0/409 (0.00%; 95% 0.00%–0.93%); U=1<br>N=410/433; ask=1: 410 | ask $1.00; bid $0.99<br>0/518 (0.00%; 95% 0.00%–0.74%); U=3<br>N=521/543; ask=1: 519 | ask $1.00; bid $0.99<br>0/657 (0.00%; 95% 0.00%–0.58%); U=5<br>N=662/690; ask=1: 661 | ask $1.00; bid $0.99<br>0/630 (0.00%; 95% 0.00%–0.61%); U=5<br>N=635/654; ask=1: 635 |

## 5 seconds remaining

Each cell: median ask/bid; matched loss rate and interval; U; matched/full N; ask=1 count.

| X (bp) \ Y (bp) | <-2 | [-2,0) | [0,2) | >=2 |
|:---|:---|:---|:---|:---|
| [0,1) | ask $0.02; bid $0.01<br>36/57 (63.16%; 95% 50.18%–74.48%); U=0<br>N=57/82; ask=1: 16 | ask $1.00; bid $0.99<br>39/368 (10.60%; 95% 7.85%–14.16%); U=0<br>N=368/412; ask=1: 308 | ask $1.00; bid $0.99<br>5/403 (1.24%; 95% 0.53%–2.87%); U=1<br>N=404/436; ask=1: 384 | ask $1.00; bid $0.99<br>0/68 (0.00%; 95% 0.00%–5.35%); U=0<br>N=68/83; ask=1: 67 |
| [1,2) | ask $1.00; bid $0.99<br>1/73 (1.37%; 95% 0.24%–7.36%); U=1<br>N=74/81; ask=1: 71 | ask $1.00; bid $0.99<br>0/327 (0.00%; 95% 0.00%–1.16%); U=1<br>N=328/337; ask=1: 327 | ask $1.00; bid $0.99<br>0/342 (0.00%; 95% 0.00%–1.11%); U=0<br>N=342/358; ask=1: 342 | ask $1.00; bid $0.99<br>0/82 (0.00%; 95% 0.00%–4.48%); U=0<br>N=82/90; ask=1: 82 |
| [2,4) | ask $1.00; bid $0.99<br>0/131 (0.00%; 95% 0.00%–2.85%); U=1<br>N=132/144; ask=1: 131 | ask $1.00; bid $0.99<br>0/514 (0.00%; 95% 0.00%–0.74%); U=0<br>N=514/535; ask=1: 513 | ask $1.00; bid $0.99<br>0/580 (0.00%; 95% 0.00%–0.66%); U=0<br>N=580/612; ask=1: 579 | ask $1.00; bid $0.99<br>0/133 (0.00%; 95% 0.00%–2.81%); U=1<br>N=134/155; ask=1: 134 |
| [4,8) | ask $1.00; bid $0.99<br>0/210 (0.00%; 95% 0.00%–1.80%); U=0<br>N=210/215; ask=1: 210 | ask $1.00; bid $0.99<br>0/582 (0.00%; 95% 0.00%–0.66%); U=0<br>N=582/610; ask=1: 582 | ask $1.00; bid $0.99<br>0/693 (0.00%; 95% 0.00%–0.55%); U=4<br>N=697/723; ask=1: 697 | ask $1.00; bid $0.99<br>0/264 (0.00%; 95% 0.00%–1.43%); U=3<br>N=267/286; ask=1: 266 |
| [8,infinity) | ask $1.00; bid $0.99<br>0/391 (0.00%; 95% 0.00%–0.97%); U=2<br>N=393/415; ask=1: 393 | ask $1.00; bid $0.999<br>0/542 (0.00%; 95% 0.00%–0.70%); U=2<br>N=544/567; ask=1: 544 | ask $1.00; bid $0.99<br>0/661 (0.00%; 95% 0.00%–0.58%); U=5<br>N=666/690; ask=1: 665 | ask $1.00; bid $0.99<br>0/624 (0.00%; 95% 0.00%–0.61%); U=5<br>N=629/662; ask=1: 629 |

## 3 seconds remaining

Each cell: median ask/bid; matched loss rate and interval; U; matched/full N; ask=1 count.

| X (bp) \ Y (bp) | <-2 | [-2,0) | [0,2) | >=2 |
|:---|:---|:---|:---|:---|
| [0,1) | ask $0.875; bid $0.67<br>24/50 (48.00%; 95% 34.80%–61.49%); U=0<br>N=50/74; ask=1: 20 | ask $1.00; bid $0.99<br>33/369 (8.94%; 95% 6.44%–12.29%); U=0<br>N=369/405; ask=1: 322 | ask $1.00; bid $0.99<br>3/414 (0.72%; 95% 0.25%–2.11%); U=0<br>N=414/444; ask=1: 404 | ask $1.00; bid $0.99<br>0/67 (0.00%; 95% 0.00%–5.42%); U=0<br>N=67/83; ask=1: 67 |
| [1,2) | ask $1.00; bid $0.99<br>1/86 (1.16%; 95% 0.21%–6.30%); U=1<br>N=87/94; ask=1: 85 | ask $1.00; bid $0.99<br>0/311 (0.00%; 95% 0.00%–1.22%); U=1<br>N=312/322; ask=1: 311 | ask $1.00; bid $0.99<br>0/346 (0.00%; 95% 0.00%–1.10%); U=0<br>N=346/361; ask=1: 344 | ask $1.00; bid $0.99<br>0/76 (0.00%; 95% 0.00%–4.81%); U=1<br>N=77/84; ask=1: 77 |
| [2,4) | ask $1.00; bid $0.99<br>0/122 (0.00%; 95% 0.00%–3.05%); U=0<br>N=122/129; ask=1: 121 | ask $1.00; bid $0.99<br>0/511 (0.00%; 95% 0.00%–0.75%); U=1<br>N=512/531; ask=1: 511 | ask $1.00; bid $0.99<br>0/592 (0.00%; 95% 0.00%–0.64%); U=0<br>N=592/620; ask=1: 591 | ask $1.00; bid $0.99<br>0/135 (0.00%; 95% 0.00%–2.77%); U=1<br>N=136/155; ask=1: 136 |
| [4,8) | ask $1.00; bid $0.99<br>0/208 (0.00%; 95% 0.00%–1.81%); U=0<br>N=208/214; ask=1: 208 | ask $1.00; bid $0.99<br>0/578 (0.00%; 95% 0.00%–0.66%); U=1<br>N=579/603; ask=1: 579 | ask $1.00; bid $0.99<br>0/716 (0.00%; 95% 0.00%–0.53%); U=2<br>N=718/747; ask=1: 717 | ask $1.00; bid $0.99<br>0/259 (0.00%; 95% 0.00%–1.46%); U=4<br>N=263/281; ask=1: 262 |
| [8,infinity) | ask $1.00; bid $0.999<br>0/412 (0.00%; 95% 0.00%–0.92%); U=2<br>N=414/435; ask=1: 414 | ask $1.00; bid $0.999<br>0/518 (0.00%; 95% 0.00%–0.74%); U=3<br>N=521/544; ask=1: 521 | ask $1.00; bid $0.999<br>0/700 (0.00%; 95% 0.00%–0.55%); U=4<br>N=704/732; ask=1: 703 | ask $1.00; bid $0.99<br>0/604 (0.00%; 95% 0.00%–0.63%); U=4<br>N=608/639; ask=1: 608 |

## Price-band companion

[market_price_cells.csv](market_price_cells.csv) contains all 160 cells with full-study and matched-subset counts, intervals and price medians. [price_bands.csv](price_bands.csv) contains six disjoint ask bands for every cell: exactly 0, (0,0.90), [0.90,0.95), [0.95,0.98), [0.98,1), and exactly 1. Band rates are recomputed within the same matched bid/ask subset; empty bands have unavailable rates.

These are descriptive historical associations. Medians do not show every opportunity, a quote is not a fill, and a low full-region loss rate cannot be assigned automatically to a cheaper subset. Pointwise Wilson intervals assume independent, comparable markets and do not correct for searching cells or temporal dependence. Repeated checkpoints are never pooled into an independent-market claim. [manifest.json](manifest.json) records both snapshots, code and input hashes.
