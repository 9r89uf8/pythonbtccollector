-- Spot-to-TWAP response pilot, part 3 (read-only, research only). 2026-09-13.
-- Question left open by the merged protocol: what does the TWAP do in seconds where the retained
-- Chainlink spot history has no sample? Parts 1-2 verified the 60-sample mean only on complete
-- windows (about 13% of seconds). A duration-weighted TWAP would carry the last price forward
-- through a silent second. This file tests that reconstruction on ALL seconds of one week and
-- (sections 11-12). The projection rerun with the same carry-forward policy is section 13 in
-- pilot_part3b.sql; a first formulation of it here joined against whole-week per-second series and
-- was terminated after 10 minutes without output, so it was moved to a per-market formulation.
-- Alignment: newest constituent stamp = t - 3 s, i.e. constituents [t-62 s, t-3 s] (pilot_part2 section 8).
-- Week: 1788220800000 = 2026-09-01T00:00Z .. 1788825600000 = 2026-09-08T00:00Z. Provider clocks.
-- Run: sudo -u postgres psql -X -At -v ON_ERROR_STOP=1 -d price_collector -f pilot_part3.sql

BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout = '1500s';

\echo === 11. Carry-forward reconstruction on every second of the week: TWAP(t) vs mean over [t-62 s, t-3 s] of the last-known Chainlink spot, split by whether the raw window was complete; the retained-sample mean is shown beside it ===
\echo window_class|n_twap_seconds|ff_p50_bps|ff_p99_bps|ff_max_bps|retained_mean_p50_bps|retained_mean_p99_bps|retained_mean_max_bps
with sec as (
  select generate_series(1788220800000 - 200000, 1788825600000, 1000) t
), raw as (
  select sample_second_ms t, price from price_samples
  where instrument_id=2 and sample_second_ms between 1788220800000 - 200000 and 1788825600000
), ff as (
  select sec.t, raw.price is not null as present,
         max(raw.t) over (order by sec.t rows between unbounded preceding and current row) as last_t
  from sec left join raw using (t)
), ffp as (
  select ff.t, ff.present, r2.price as p_ff
  from ff join raw r2 on r2.t = ff.last_t
), win as (
  select t,
         avg(p_ff) over (order by t range between 59000 preceding and current row) as ff60,
         avg(case when present then p_ff end) over (order by t range between 59000 preceding and current row) as retained60,
         sum(case when present then 1 else 0 end) over (order by t range between 59000 preceding and current row) as n_raw
  from ffp
), w as (
  select sample_second_ms t, price w from price_samples where instrument_id=4 and sample_second_ms between 1788220800000 and 1788825600000
)
select case when win.n_raw = 60 then 'complete (60 raw samples)' else 'incomplete (<60 raw samples)' end,
  count(*),
  round(percentile_cont(0.5) within group (order by abs(w.w-win.ff60)/w.w*1e4)::numeric,4),
  round(percentile_cont(0.99) within group (order by abs(w.w-win.ff60)/w.w*1e4)::numeric,4),
  round(max(abs(w.w-win.ff60)/w.w*1e4)::numeric,3),
  round(percentile_cont(0.5) within group (order by abs(w.w-win.retained60)/w.w*1e4)::numeric,4),
  round(percentile_cont(0.99) within group (order by abs(w.w-win.retained60)/w.w*1e4)::numeric,4),
  round(max(abs(w.w-win.retained60)/w.w*1e4)::numeric,3)
from w join win on win.t = w.t - 3000
group by 1 order by 1;

\echo === 12. Missing Chainlink spot seconds in the week: run-length distribution of consecutive silent seconds ===
\echo run_length_s|runs
with sec as (
  select generate_series(1788220800000, 1788825600000, 1000) t
), raw as (
  select sample_second_ms t from price_samples where instrument_id=2 and sample_second_ms between 1788220800000 and 1788825600000
), m as (
  select sec.t, raw.t is null as missing,
         sum(case when raw.t is null then 0 else 1 end) over (order by sec.t) as grp
  from sec left join raw using (t)
), runs as (
  select grp, count(*) as len from m where missing group by grp
)
select case when len >= 10 then '10+' else len::text end, count(*) from runs group by 1 order by min(len);

COMMIT;
