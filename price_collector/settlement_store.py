"""Bounded settlement publication evidence and historical win-rate summaries.

No feed, Redis publication, order path, or research import. The runtime owns its
durable outbox; database acknowledgement must not be mistaken for publication.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from copy import deepcopy
from decimal import Decimal
import json
from typing import Any, Mapping

from price_collector.settlement_history import (
    cohort_key, daily_summary, history_summary, observe, observation_window_s,
    market_condition_summary, freeze_daily_unknowns,
    combined_condition_summary, retrospective_cohort, retrospective_condition_daily_summary,
    RETROSPECTIVE_COHORT_PREFIX,
)

from price_collector.ghost_twap_store import (
    GhostAuditConflict, _canonical, _json_object, _transition, validate_record,
)

DAY_MS = 86_400_000
INDIVIDUAL_MS = 7 * DAY_MS
REPORT_MS = 90 * DAY_MS
MAX_BATCH = 100
WARN_BYTES = 256 * 1024 * 1024
STOP_BYTES = 512 * 1024 * 1024
ROW_CAP = 200_000
SIGNALS = ("ghost", "twap", "spot")
RULE = "btc-5m-twap-60"
SOURCE = "https://data.chain.link/streams/btc-usd-twap-60s-streams"


def _int(value: Any) -> int:
    if isinstance(value, bool):
        raise ValueError("boolean clock")
    number = int(value)
    if str(number) != str(value) or not 0 <= number <= 9_223_372_036_854_775_807:
        raise ValueError("invalid integer clock")
    return number


def _decimal(value: Any) -> Decimal:
    if not isinstance(value, (str, Decimal)):
        raise ValueError("financial value must be exact text")
    result = Decimal(value)
    if not result.is_finite():
        raise ValueError("nonfinite financial value")
    return result


def _public(frozen: dict) -> dict:
    return {key: value for key, value in frozen.items() if key not in ("slots", "slot_inputs")}


def publication_eligible(frozen: Mapping, state: Mapping) -> bool:
    """Actual acknowledged wire eligibility, independent of subsequent revocation."""
    try:
        publication = state.get("publication", {})
        if (frozen.get("status") != "available"
                or frozen.get("quality") not in ("healthy", "degraded")
                or publication.get("status") != "acknowledged"
                or publication.get("eligible_before_close") is not True):
            return False
        decision = _int(frozen["decision_wall_ns"])
        attempt, ack = (_int(publication[key]) for key in ("attempt_wall_ns", "ack_wall_ns"))
        deadline = min(_int(frozen["valid_until_wall_ns"]), _int(frozen["market_end_ms"]) * 1_000_000)
        if not decision <= attempt <= ack < deadline:
            return False
        attempted = _json_object(publication["attempted_payload"])
        if any(attempted.get(key) != value for key, value in _public(dict(frozen)).items()):
            return False
        if (attempted.get("publication_state") != "attempted"
                or _int(attempted["publication_attempt_wall_ns"]) != attempt):
            return False
        if "ack_monotonic_ns" in publication:
            if not (_int(frozen["decision_monotonic_ns"]) <= _int(publication["attempt_monotonic_ns"])
                    <= _int(publication["ack_monotonic_ns"])):
                return False
            if (_int(attempted["publication_attempt_monotonic_ns"]) != _int(publication["attempt_monotonic_ns"])
                    or _int(publication["ack_monotonic_ns"]) - _int(frozen["decision_monotonic_ns"]) >= deadline - decision):
                return False
        return True
    except (AttributeError, KeyError, TypeError, ValueError):
        return False


def history_eligible(frozen: Mapping, state: Mapping) -> bool:
    """Observed conditions earn no forecast or publication credit."""
    if frozen.get("schema_version") != 4:
        return publication_eligible(frozen, state)
    try:
        observation = state["observation"]
        return (observation_window_s(frozen) == 300
                and frozen.get("status") == "available" and frozen.get("quality") == "healthy"
                and frozen.get("reasons") == []
                and observation.get("status") == "recorded"
                and state.get("publication", {}).get("status") == "not_published"
                and _int(observation["decision_wall_ns"]) == _int(frozen["decision_wall_ns"])
                and _int(observation["decision_monotonic_ns"]) == _int(frozen["decision_monotonic_ns"]))
    except (KeyError, TypeError, ValueError):
        return False


def _settlement_transition(old: Mapping, new: Mapping) -> str:
    if new["version"] <= old["version"]:
        return _transition(old, new)
    old_state, new_state = _json_object(old["state_json"]), _json_object(new["state_json"])
    observation = old_state.get("observation", {})
    if observation.get("status") == "recorded" and observation != new_state.get("observation"):
        raise GhostAuditConflict("recorded market observation is immutable")
    prior, current = old_state.get("publication", {}), new_state.get("publication", {})
    if not isinstance(prior, dict) or not isinstance(current, dict):
        raise GhostAuditConflict("settlement publication must be an object")
    if (prior.get("attempted_payload") is not None
            and prior["attempted_payload"] != current.get("attempted_payload")):
        raise GhostAuditConflict("settlement attempted payload is immutable")
    if (prior.get("status") == "acknowledged"
            and prior.get("eligible_before_close") != current.get("eligible_before_close")):
        raise GhostAuditConflict("settlement acknowledged eligibility is immutable")
    if new["version"] > old["version"] and prior.get("status") == "attempted":
        # The ordinary ghost calls this transient state 'attempting'. Preserve
        # its clock/version protections while allowing unknown-send recovery.
        old_state["publication"]["status"] = "attempting"
        old = dict(old, state_json=_canonical(old_state))
    return _transition(old, new)


def official_outcome(row: Mapping, cutoff_ms: int) -> dict | None:
    """Only recorded official winner/payout identity; never infer from price."""
    try:
        checked = _int(row["last_checked_ms"])
        winner = row["winner"]
        if (checked > cutoff_ms or row["resolution_status"] != "resolved"
                or row["resolution_type"] != "winner" or winner not in ("Up", "Down")
                or row["settlement_reference"] != "chainlink_twap"
                or row["settlement_window_s"] != 60 or row["settlement_source_url"] != SOURCE
                or row["settlement_rule_version"] != RULE
                or row["reconciled_settlement_rule_version"] != RULE
                or row.get("resolution_source") not in ("polymarket_gamma", "polymarket_clob_rest", "polymarket_clob_ws")
                or not row.get("condition_id") or not row.get("up_token_id") or not row.get("down_token_id")
                or row["up_token_id"] == row["down_token_id"]
                or row["winning_token_id"] != row["up_token_id" if winner == "Up" else "down_token_id"]
                or _decimal(row["up_payout"]) != (1 if winner == "Up" else 0)
                or _decimal(row["down_payout"]) != (1 if winner == "Down" else 0)):
            return None
        return {"winner": winner.lower(), "last_checked_ms": checked,
                "resolution_source": row["resolution_source"], "rule_version": RULE,
                "condition_id": row["condition_id"], "up_token_id": row["up_token_id"],
                "down_token_id": row["down_token_id"]}
    except (KeyError, TypeError, ValueError, ArithmeticError):
        return None


def capture_outcome(body: dict, row: Mapping, cutoff_ms: int) -> dict:
    """Preserve the as-of observation, never backdate a later mutable row."""
    result = json.loads(_canonical(body))
    checked = row.get("last_checked_ms")
    if checked is None or _int(checked) > cutoff_ms:
        return result
    if _int(checked) < result.get("outcome_checked_ms", 0):
        return result
    result["outcome_checked_ms"] = _int(checked)
    result["official_outcome"] = official_outcome(row, cutoff_ms)
    return result


class SettlementStore:
    def __init__(self, pool: Any, start_ms: int = 0, *, market_conditions: bool = False,
                 history_cohort: str | None = None, history_policy: dict | None = None):
        # A legacy constructor argument is accepted for queued old evidence;
        # it never arms a study or changes the frozen record's association.
        self.pool = pool
        self.market_conditions = market_conditions
        self.history_cohort = history_cohort
        self.history_policy = deepcopy(history_policy)
        self._retrospective_seen: dict[str, None] = {}

    @asynccontextmanager
    async def _connection(self):
        async with self.pool.acquire(timeout=5) as connection:
            async with connection.transaction():
                await connection.execute("SET LOCAL statement_timeout = '5s'")
                await connection.execute("SET LOCAL lock_timeout = '500ms'")
                yield connection

    async def persist(self, record: Mapping) -> str:
        incoming = validate_record(record)
        frozen, state = _json_object(incoming["frozen_json"]), _json_object(incoming["state_json"])
        start, end = _int(frozen["market_start_ms"]), _int(frozen["market_end_ms"])
        market = _int(frozen["market_id"])
        window_ms = observation_window_s(frozen) * 1000
        evaluation_start = _int(frozen.get("evaluation_start_ms", 0))
        if evaluation_start % DAY_MS:
            raise ValueError("frozen evaluation start must be a UTC day boundary")
        if (start % 300_000 or end != start + 300_000 or market != start // 300_000
                or _int(frozen["target_source_timestamp_ms"]) != end
                or incoming["created_ms"] != incoming["decision_wall_ns"] // 1_000_000
                or not (end - window_ms) * 1_000_000 <= incoming["decision_wall_ns"] < end * 1_000_000):
            raise ValueError("decision outside frozen settlement schedule")
        compact = _canonical(_public(frozen))
        async with self._connection() as connection:
            inserted = await connection.fetchval("""INSERT INTO settlement_audit
                (run_id,decision_id,evaluation_start_ms,market_id,market_start_ms,market_end_ms,
                 decision_wall_ns,created_ms,frozen_json,frozen_sha256,compact_json,state_json,version,terminal)
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
                ON CONFLICT DO NOTHING RETURNING true""", incoming["run_id"], incoming["decision_id"],
                evaluation_start, market, start, end, incoming["decision_wall_ns"], incoming["created_ms"],
                incoming["frozen_json"], incoming["frozen_sha256"], compact, incoming["state_json"],
                incoming["version"], incoming["terminal"])
            if not inserted:
                old = await connection.fetchrow("SELECT * FROM settlement_audit WHERE run_id=$1 AND decision_id=$2 FOR UPDATE",
                                                incoming["run_id"], incoming["decision_id"])
                if old is None: raise GhostAuditConflict("missing settlement identity")
                old = dict(old)
                if old["frozen_sha256"] != incoming["frozen_sha256"] or old["evaluation_start_ms"] != evaluation_start:
                    raise GhostAuditConflict("frozen settlement evidence differs")
                old["frozen_json"] = incoming["frozen_json"]  # hash already checked, may have compacted
                transition = _settlement_transition(old, incoming)
                if transition in ("stale", "unchanged"):
                    return transition
                previous_target = _json_object(old["state_json"]).get("target", {})
                current_target = state.get("target", {})
                if previous_target.get("first_event") is not None and (
                        previous_target["first_event"] != current_target.get("first_event")
                        or previous_target.get("status") != current_target.get("status")):
                    raise GhostAuditConflict("first settlement target is immutable")
                await connection.execute("UPDATE settlement_audit SET state_json=$3,version=$4,terminal=$5 WHERE run_id=$1 AND decision_id=$2",
                                         incoming["run_id"], incoming["decision_id"], incoming["state_json"], incoming["version"], incoming["terminal"])
        return "inserted" if inserted else "updated"

    async def guard(self) -> dict:
        async with self._connection() as connection:
            return await self._guard(connection)

    async def _guard(self, connection) -> dict:
        row = await connection.fetchrow("""SELECT
                pg_total_relation_size('settlement_audit')+pg_total_relation_size('settlement_market_evaluation')+
                pg_total_relation_size('settlement_evaluation_reports')+
                pg_total_relation_size('settlement_history_markets')+
                pg_total_relation_size('settlement_history_daily') AS relation_bytes,
                (SELECT count(*) FROM (SELECT 1 FROM settlement_audit LIMIT 200001) bounded) AS row_count""")
        used, count = int(row["relation_bytes"]), int(row["row_count"])
        return {"relation_bytes": used, "row_count": count, "warning": used >= WARN_BYTES,
                "capacity_ok": used < STOP_BYTES - 16 * 1024 * 1024 and count < ROW_CAP,
                "stop_bytes": STOP_BYTES, "row_cap": ROW_CAP}

    async def _backfill_legacy_conditions(self, now_ms: int) -> dict:
        """Reclassify one retained paired day, preserving its original outcomes.

        Only compact, separately identified daily totals are written. Selection
        is keyed by the source day's semantic fingerprint, so refreshed worker
        timestamps do not produce updates. The existing memory/disk caps remain.
        """
        async with self._connection() as connection:
            source = await connection.fetchrow("""WITH sources AS (
                SELECT s.*, h.retained_count,h.retained_updated_ms, encode(sha256(convert_to(
                    (s.body_json::jsonb-'generated_at_ms')::text||'|'||h.retained_count::text||'|'||
                    h.retained_updated_ms::text,'UTF8')),'hex') AS source_fingerprint
                FROM settlement_history_daily s JOIN LATERAL (
                    SELECT count(*) AS retained_count,max(updated_ms) AS retained_updated_ms
                    FROM (SELECT updated_ms FROM settlement_history_markets
                        WHERE cohort=s.cohort AND market_start_ms>=s.day_ms
                          AND market_start_ms<s.day_ms+86400000 AND market_end_ms>$5
                          AND market_end_ms+120000<=$7 LIMIT 289) bounded
                    ) h ON h.retained_count>0 WHERE s.day_ms >= $1
                  AND s.day_ms <= $2
                  AND s.body_json::jsonb#>>'{description,selection_version}'
                    IN ('first-ack-5s-v1','first-ack-5s-v2')
                  AND ((s.body_json::jsonb#>>'{description,selection_version}'='first-ack-5s-v1'
                    AND NOT (s.body_json::jsonb->'description' ? 'observation_window_s')
                    AND NOT (s.body_json::jsonb->'description' ? 'sampling_interval_ms'))
                    OR (s.body_json::jsonb#>>'{description,selection_version}'='first-ack-5s-v2'
                    AND s.body_json::jsonb#>'{description,observation_window_s}'='60'::jsonb
                    AND s.body_json::jsonb#>'{description,sampling_interval_ms}'='2000'::jsonb))
                  AND s.body_json::jsonb#>>'{description,model_version}'='chainlink-60s-offset3-settlement-v1'
                  AND s.body_json::jsonb#>>'{description,reference_version}'='causal-observed-website-opening-v1'
                  AND s.body_json::jsonb#>>'{description,settlement_rule_version}'='btc-5m-twap-60'
                  AND coalesce(s.body_json::jsonb#>'{description,policy}','{}'::jsonb)=$4::jsonb)
                SELECT s.cohort,s.day_ms,s.updated_ms,s.final,s.body_json,s.source_fingerprint,
                    s.retained_count,s.retained_updated_ms
                FROM sources s LEFT JOIN settlement_history_daily d
                  ON d.day_ms=s.day_ms AND d.cohort=encode(sha256(convert_to($3||s.cohort,'UTF8')),'hex')
                WHERE (d.cohort IS NULL OR (NOT d.final AND
                    d.body_json::jsonb->>'source_summary_fingerprint' IS DISTINCT FROM s.source_fingerprint))
                  AND NOT (s.cohort||':'||s.day_ms::text||':'||s.source_fingerprint)=ANY($6::text[])
                ORDER BY (d.cohort IS NULL) DESC,s.day_ms,s.cohort LIMIT 1""",
                (now_ms - INDIVIDUAL_MS) // DAY_MS * DAY_MS, now_ms // DAY_MS * DAY_MS,
                RETROSPECTIVE_COHORT_PREFIX, _canonical(self.history_policy or {}),
                now_ms - INDIVIDUAL_MS, list(self._retrospective_seen), now_ms)
            if source is None:
                return dict(status="complete", pending=False, processed_groups=0, reason=None)
            cohort, day = source["cohort"], int(source["day_ms"])
            derived_cohort = retrospective_cohort(cohort)
            # Lock/recheck the selected source and destination. A concurrent
            # source rebuild or another worker must not freeze a mixed version.
            locked = await connection.fetchrow("""SELECT updated_ms,final,body_json
                FROM settlement_history_daily WHERE cohort=$1 AND day_ms=$2 FOR SHARE""", cohort, day)
            pending = dict(status="pending", pending=True, processed_groups=0, reason=None,
                           last_source_cohort=cohort, last_day_ms=day)
            if locked is None or locked["body_json"] != source["body_json"]:
                return dict(pending, reason="source_changed")
            existing = await connection.fetchrow("""SELECT final,body_json FROM settlement_history_daily
                WHERE cohort=$1 AND day_ms=$2 FOR UPDATE""", derived_cohort, day)
            if existing and existing["final"]:
                return dict(pending, reason="already_final")
            rows = await connection.fetch("""SELECT body_json,updated_ms FROM settlement_history_markets
                WHERE cohort=$1 AND market_start_ms >= $2 AND market_start_ms < $3
                  AND market_end_ms>$4 AND market_end_ms+120000<=$5
                ORDER BY market_id LIMIT 289 FOR SHARE""",
                cohort, day, day + DAY_MS, now_ms - INDIVIDUAL_MS, now_ms)
            if len(rows) > 288:
                raise ValueError("retrospective daily market history exceeds fixed calendar")
            if not rows:
                return dict(pending, reason="source_expired_or_not_closed")
            if (len(rows) != source["retained_count"]
                    or max(row["updated_ms"] for row in rows) != source["retained_updated_ms"]):
                return dict(pending, reason="source_changed")
            source_daily = _json_object(source["body_json"])
            if (source_daily["cohort"] != cohort or source_daily["day_ms"] != day
                    or source_daily["final"] != source["final"]):
                raise ValueError("retrospective source identity differs")
            markets = [_json_object(row["body_json"]) for row in rows]
            if (len(markets) > source_daily["observed_markets"] or any(
                    market["market_start_ms"] < source_daily["covered_start_ms"]
                    or market["market_end_ms"] > source_daily["covered_end_ms"] for market in markets)):
                return dict(pending, reason="source_summary_pending")
            body = retrospective_condition_daily_summary(
                markets,
                source_daily=source_daily, day_ms=day, now_ms=now_ms)
            if body["cohort"] != derived_cohort or body["final"] != source["final"]:
                raise ValueError("retrospective derived identity or finality differs")
            body.update(source_summary_updated_ms=int(source["updated_ms"]),
                        source_summary_fingerprint=source["source_fingerprint"])
            def meaningful(value):
                return {key: item for key, item in value.items() if key not in (
                    "generated_at_ms", "source_generated_at_ms", "source_summary_updated_ms",
                    "source_summary_fingerprint")}
            if existing and meaningful(_json_object(existing["body_json"])) == meaningful(body):
                token = f"{cohort}:{day}:{source['source_fingerprint']}"
                self._retrospective_seen[token] = None
                while len(self._retrospective_seen) > 64:
                    self._retrospective_seen.pop(next(iter(self._retrospective_seen)))
                return dict(pending, processed_groups=1, reason="unchanged_conditions")
            encoded = _canonical(body)
            size = len(encoded.encode("utf-8"))
            if size > 65536:
                raise ValueError("retrospective daily history exceeds byte cap")
            guard = await self._guard(connection)
            reserve = 4 * size + 16 * 1024
            if (not guard["capacity_ok"]
                    or guard["relation_bytes"] + reserve >= STOP_BYTES - 16 * 1024 * 1024):
                return dict(pending, status="capacity_paused", reason="history_capacity_reserve")
            await connection.execute("""INSERT INTO settlement_history_daily
                (cohort,day_ms,updated_ms,final,body_json) VALUES($1,$2,$3,$4,$5)
                ON CONFLICT(cohort,day_ms) DO UPDATE SET updated_ms=EXCLUDED.updated_ms,
                final=EXCLUDED.final,body_json=EXCLUDED.body_json WHERE NOT settlement_history_daily.final""",
                derived_cohort, day, now_ms, body["final"], encoded)
            return dict(pending, processed_groups=1)

    async def _fold_audit(self, now_ms: int) -> int:
        """One indexed page; version markers also catch a later publication ACK."""
        async with self._connection() as connection:
            rows = await connection.fetch("""SELECT run_id,decision_id,version,compact_json,state_json
                FROM settlement_audit WHERE history_folded_version < version
                AND created_ms>$1 AND market_end_ms+120000<=$2
                ORDER BY created_ms,run_id,decision_id
                LIMIT 100 FOR UPDATE SKIP LOCKED""", now_ms - INDIVIDUAL_MS, now_ms)
            for row in rows:
                frozen, state = _json_object(row["compact_json"]), _json_object(row["state_json"])
                cohort = cohort_key(frozen)
                start, end = int(frozen["market_start_ms"]), int(frozen["market_end_ms"])
                day = start // DAY_MS * DAY_MS
                final = await connection.fetchval("""SELECT final FROM settlement_history_daily
                    WHERE cohort=$1 AND day_ms=$2""", cohort, day)
                if not final:
                    previous = await connection.fetchval("""SELECT body_json FROM settlement_history_markets
                        WHERE cohort=$1 AND market_id=$2 FOR UPDATE""", cohort, frozen["market_id"])
                    body = observe(_json_object(previous) if previous else {}, frozen, state,
                                   eligible=history_eligible(frozen, state))
                    await connection.execute("""INSERT INTO settlement_history_markets
                        (cohort,market_id,market_start_ms,market_end_ms,updated_ms,body_json)
                        VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(cohort,market_id) DO UPDATE
                        SET updated_ms=EXCLUDED.updated_ms,body_json=EXCLUDED.body_json""",
                        cohort, int(frozen["market_id"]), start, end, now_ms, _canonical(body))
                await connection.execute("""UPDATE settlement_audit SET history_folded_version=$3
                    WHERE run_id=$1 AND decision_id=$2""", row["run_id"], row["decision_id"], row["version"])
        return len(rows)

    async def _rebuild_days(self, now_ms: int, *, complete: bool) -> None:
        # A cohort change starts a separate history. Only recent, mutable days
        # are rebuilt; frozen daily totals survive individual-row expiry.
        async with self._connection() as connection:
            groups = await connection.fetch("""SELECT DISTINCT h.cohort,
                (h.market_start_ms/86400000)*86400000 AS day_ms
                FROM settlement_history_markets h LEFT JOIN settlement_history_daily d
                  ON d.cohort=h.cohort AND d.day_ms=(h.market_start_ms/86400000)*86400000
                WHERE NOT coalesce(d.final,false) AND h.market_end_ms>$1
                ORDER BY day_ms,h.cohort LIMIT 32""", now_ms - INDIVIDUAL_MS)
        for group in groups:
            cohort, day = group["cohort"], int(group["day_ms"])
            async with self._connection() as connection:
                existing = await connection.fetchrow("""SELECT final,body_json FROM settlement_history_daily
                    WHERE cohort=$1 AND day_ms=$2 FOR UPDATE""", cohort, day)
                if existing and existing["final"]:
                    continue
                pending = await connection.fetchval("""SELECT EXISTS(SELECT 1 FROM settlement_audit
                    WHERE history_folded_version < version AND created_ms>$1
                    AND market_start_ms >= $2 AND market_start_ms < $3
                    AND market_end_ms+120000<=$4)""",
                    now_ms - INDIVIDUAL_MS, day, day + DAY_MS, now_ms)
                if pending:
                    # An old day's first page is not its complete history.
                    # Finish its retained input before either replacing or
                    # freezing totals, including the pre-expiry fallback.
                    continue
                prior = _json_object(existing["body_json"]) if existing else None
                window_ms = prior["description"].get("observation_window_s", 30) * 1000 if prior else 30_000
                first_observation_ms = (prior.get("covered_start_ms") or day) + 300_000 - window_ms if prior else None
                if prior and now_ms >= first_observation_ms + INDIVIDUAL_MS:
                    # If the worker was disabled through expiry, retain its
                    # last totals instead of replacing them with a partial day.
                    # Day+7 midnight alone does not mean an input has expired.
                    body = freeze_daily_unknowns(prior)
                    body.update(final=True, outcome_freeze_ms=now_ms, persistence_complete=False)
                    await connection.execute("""UPDATE settlement_history_daily
                        SET final=true,updated_ms=$3,body_json=$4 WHERE cohort=$1 AND day_ms=$2""",
                        cohort, day, now_ms, _canonical(body))
                    continue
                # One UTC day has at most 288 five-minute markets. The complete
                # day is read in one bounded transaction so replacement is exact.
                rows = await connection.fetch("""SELECT h.market_id,h.body_json,
                    m.settlement_reference,m.settlement_window_s,m.settlement_source_url,
                    m.settlement_rule_version,m.condition_id,m.up_token_id,m.down_token_id,
                    r.resolution_status,r.resolution_type,r.winner,r.winning_token_id,r.up_payout,
                    r.down_payout,r.last_checked_ms,r.resolution_source,r.reconciled_settlement_rule_version
                    FROM settlement_history_markets h
                    LEFT JOIN polymarket_btc_5m_markets m USING(market_id)
                    LEFT JOIN polymarket_btc_5m_resolutions r USING(market_id)
                    WHERE h.cohort=$1 AND h.market_start_ms >= $2 AND h.market_start_ms < $3
                    ORDER BY h.market_id LIMIT 289 FOR UPDATE OF h""", cohort, day, day + DAY_MS)
                if len(rows) > 288:
                    raise ValueError("daily market history exceeds fixed calendar")
                markets = []
                for row in rows:
                    original = _json_object(row["body_json"])
                    body = capture_outcome(original, row, now_ms)
                    if body != original:
                        await connection.execute("""UPDATE settlement_history_markets
                            SET body_json=$3,updated_ms=$4 WHERE cohort=$1 AND market_id=$2""",
                            cohort, row["market_id"], _canonical(body), now_ms)
                    markets.append(body)
                if not markets:
                    continue
                # Freeze incomplete evidence before individual retention can
                # remove the start of the day; the report keeps that caveat.
                final = (complete and now_ms >= day + 2 * DAY_MS) or now_ms >= day + 6 * DAY_MS
                body = daily_summary(markets, day_ms=day, now_ms=now_ms, final=final)
                body["persistence_complete"] = complete
                await connection.execute("""INSERT INTO settlement_history_daily
                    (cohort,day_ms,updated_ms,final,body_json) VALUES($1,$2,$3,$4,$5)
                    ON CONFLICT(cohort,day_ms) DO UPDATE SET updated_ms=EXCLUDED.updated_ms,
                    final=EXCLUDED.final,body_json=EXCLUDED.body_json
                    WHERE NOT settlement_history_daily.final""", cohort, day, now_ms, final, _canonical(body))

    async def maintain(self, now_ms: int, *, persistence_complete: bool = True) -> dict:
        # At most 400 decisions per pass, each page in a short transaction.
        # A restart resumes via durable folded versions, not an in-memory cursor.
        for _ in range(4):
            if await self._fold_audit(now_ms) < MAX_BATCH:
                break
        async with self._connection() as connection:
            pending = await connection.fetchval("""SELECT EXISTS(SELECT 1 FROM settlement_audit
                WHERE history_folded_version < version AND created_ms>$1
                AND market_end_ms+120000<=$2)""", now_ms - INDIVIDUAL_MS, now_ms)
        complete = persistence_complete and not pending
        await self._rebuild_days(now_ms, complete=complete)
        retrospective = None
        if self.market_conditions and self.history_policy is not None:
            retrospective = (await self._backfill_legacy_conditions(now_ms) if complete else
                dict(status="persistence_pending", pending=True, processed_groups=0, reason="audit_catchup"))
        async with self._connection() as connection:
            days = await connection.fetch("""SELECT body_json FROM settlement_history_daily
                WHERE day_ms>$1 ORDER BY day_ms,cohort LIMIT 4097""",
                (now_ms - REPORT_MS) // DAY_MS * DAY_MS)
            if len(days) > 4096:
                raise ValueError("historical cohort capacity exceeded")
            await connection.execute("""WITH batch AS (SELECT run_id,decision_id FROM settlement_audit
                WHERE terminal AND frozen_json IS NOT NULL AND market_end_ms+120000<=$1
                ORDER BY created_ms,run_id,decision_id LIMIT 100 FOR UPDATE SKIP LOCKED)
                UPDATE settlement_audit a SET frozen_json=NULL FROM batch b
                WHERE a.run_id=b.run_id AND a.decision_id=b.decision_id""", now_ms)
            await connection.execute("""WITH batch AS (SELECT run_id,decision_id FROM settlement_audit
                WHERE created_ms<=$1 ORDER BY created_ms,run_id,decision_id LIMIT 100 FOR UPDATE SKIP LOCKED)
                DELETE FROM settlement_audit a USING batch b WHERE a.run_id=b.run_id AND a.decision_id=b.decision_id""", now_ms - INDIVIDUAL_MS)
            for table, key, cutoff in (("settlement_history_markets", "market_end_ms", now_ms - INDIVIDUAL_MS),
                                       ("settlement_history_daily", "day_ms", (now_ms - REPORT_MS) // DAY_MS * DAY_MS)):
                await connection.execute("""WITH batch AS (SELECT ctid FROM """ + table + " WHERE " + key + """<=$1
                    ORDER BY """ + key + " LIMIT 100 FOR UPDATE SKIP LOCKED) DELETE FROM " + table +
                    " t USING batch b WHERE t.ctid=b.ctid", cutoff)
        records = [_json_object(row["body_json"]) for row in days]
        if self.market_conditions:
            if self.history_policy is not None:
                summary = combined_condition_summary(records, now_ms, complete=complete,
                    cohort_id=self.history_cohort, policy=self.history_policy)
                summary["retrospective_backfill"] = retrospective
                return summary
            return market_condition_summary(records, now_ms, complete=complete, cohort_id=self.history_cohort)
        return history_summary(records, now_ms, complete=complete)
