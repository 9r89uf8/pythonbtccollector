"""Bounded settlement evidence and a two-day, frozen-outcome evaluation.

No feed, Redis publication, order path, or research import. The runtime owns its
durable outbox; database acknowledgement must not be mistaken for publication.
"""
from __future__ import annotations

from collections import Counter
from contextlib import asynccontextmanager
from decimal import Decimal, localcontext
import json
from typing import Any, Mapping

from price_collector.ghost_twap_store import (
    GhostAuditConflict, _canonical, _json_object, _transition, validate_record,
)

DAY_MS = 86_400_000
EVALUATION_DAYS = 2
INDIVIDUAL_MS = 7 * DAY_MS
REPORT_MS = 90 * DAY_MS
MAX_BATCH = 100
MAX_MARKETS = 576
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


def _signal_qualifies(signal: Mapping, frozen: Mapping) -> bool:
    try:
        with localcontext() as context:
            context.prec = 80
            reference = _decimal(frozen["reference"]["price_to_beat"])
            difference = _decimal(signal["price"]) - reference
            return (signal.get("qualifies") is True and reference > 0
                    and abs(difference) * 10_000 >= reference * 2
                    and signal.get("side") == ("up" if difference > 0 else "down"))
    except (KeyError, TypeError, ValueError, ArithmeticError):
        return False


def _order(frozen: Mapping, state: Mapping) -> tuple:
    return (_int(state["publication"]["ack_wall_ns"]), frozen["run_id"], frozen["decision_id"])


def _settlement_transition(old: Mapping, new: Mapping) -> str:
    if new["version"] <= old["version"]:
        return _transition(old, new)
    old_state, new_state = _json_object(old["state_json"]), _json_object(new["state_json"])
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


def update_market(body: dict, frozen: dict, state: dict, *, inserted: bool,
                  previous_state: dict | None = None) -> dict:
    """A signal owns its first qualification; paired baselines use ghost's instant."""
    result = json.loads(_canonical(body)) if body else {
        "observations": 0, "available_observations": 0, "quality_counts": {},
        "reason_counts": {}, "first_calls": {}, "last_states": {}, "revocations": {},
        "publication_status_counts": {}, "eligible_publications": 0,
    }
    if inserted:
        result["observations"] += 1
        result["available_observations"] += int(frozen.get("status") == "available")
        quality = str(frozen.get("quality", "unavailable"))
        result["quality_counts"][quality] = result["quality_counts"].get(quality, 0) + 1
        for reason in set(frozen.get("reasons", [])):
            reason = str(reason)[:100]
            result["reason_counts"][reason] = result["reason_counts"].get(reason, 0) + 1
    statuses = result["publication_status_counts"]
    status = str(state.get("publication", {}).get("status", "unknown"))
    previous_status = str((previous_state or {}).get("publication", {}).get("status", "unknown"))
    if inserted or previous_state is not None and status != previous_status:
        if not inserted:
            statuses[previous_status] = max(0, statuses.get(previous_status, 0) - 1)
        statuses[status] = statuses.get(status, 0) + 1
    if publication_eligible(frozen, state) and (inserted or not publication_eligible(frozen, previous_state or {})):
        result["eligible_publications"] += 1
    if publication_eligible(frozen, state):
        for name in SIGNALS:
            signal = frozen.get("signals", {}).get(name, {})
            if not _signal_qualifies(signal, frozen):
                continue
            previous = result["first_calls"].get(name)
            order = _order(frozen, state)
            if previous is None or order < (_int(previous["order"][0]), *previous["order"][1:]):
                result["first_calls"][name] = {
                    "order": [str(order[0]), order[1], order[2]],
                    "signal": signal, "frozen": _public(frozen),
                    "publication": state["publication"],
                }
    # Only advance diagnostics on later decision clocks; recovery of older audit
    # rows cannot manufacture a revocation. A revocation never removes a call.
    decision_order = (_int(frozen["decision_wall_ns"]), frozen["run_id"], frozen["decision_id"])
    previous_order = result.get("latest_decision_order")
    if previous_order is None or decision_order > (_int(previous_order[0]), *previous_order[1:]):
        for name in SIGNALS:
            signal = frozen.get("signals", {}).get(name, {})
            side = signal.get("side") if frozen.get("status") == "available" and _signal_qualifies(signal, frozen) else None
            first = result["first_calls"].get(name)
            last_side = result["last_states"].get(name)
            if first and last_side == first["signal"]["side"] and side != last_side:
                result["revocations"][name] = result["revocations"].get(name, 0) + 1
            result["last_states"][name] = side
        result["latest_decision_order"] = [str(decision_order[0]), *decision_order[1:]]
    return result


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


def _outcome(body: Mapping, call: Mapping, cutoff_ms: int) -> dict | None:
    outcome = body.get("official_outcome")
    if not outcome or _int(outcome["last_checked_ms"]) > cutoff_ms:
        return None
    reference = call["frozen"].get("reference", {})
    if any(reference.get(key) != outcome.get(key) for key in ("condition_id", "up_token_id", "down_token_id")):
        return None
    return outcome


def build_report(rows: list[Mapping], start_ms: int, now_ms: int, *, final: bool,
                 persistence_complete: bool) -> dict:
    end = start_ms + EVALUATION_DAYS * DAY_MS
    cutoff, due = end + DAY_MS, end + DAY_MS + 6 * 3_600_000
    eligible_end = min(end, max(start_ms, now_ms))
    scheduled = max(0, (eligible_end - start_ms) // 300_000)
    deadline_missed = final and now_ms > due
    complete = persistence_complete and not deadline_missed
    known = {int(row["market_id"]): row for row in rows
             if start_ms <= int(row["market_start_ms"]) < end and int(row["market_end_ms"]) <= eligible_end}
    output = {"schema_version": 1, "rule_version": "settlement-first-2bp-v1",
              "evaluation_start_ms": start_ms, "evaluation_end_ms": end,
              "outcome_cutoff_ms": cutoff, "report_due_ms": due, "as_of_ms": min(now_ms, cutoff),
              "generated_ms": now_ms, "final": final, "persistence_complete": persistence_complete,
              "status": "final" if final and complete else "final_incomplete" if final else "collecting",
              "report_deadline_missed": deadline_missed,
              "scheduled_markets": scheduled, "observed_markets": len(known),
              "no_observation_markets": scheduled - len(known), "signals": {},
              "paired_at_ghost_first": {}, "quality_counts": {}, "reason_counts": {},
              "publication_status_counts": {},
              "limitations": ["No calibrated individual-market probability or confidence tier",
                              "Abstentions count in coverage, not as forecast losses",
                              "TWAP and spot baseline calls are evaluated only at eligible acknowledged ghost settlement publications; ghost-specific unavailability also removes baseline opportunities",
                              "Unknown official outcomes remain unknown; later outcomes cannot change final report"]}
    quality, reasons, publication_statuses = Counter(), Counter(), Counter()
    for row in known.values():
        body = _json_object(row["body_json"])
        quality.update(body["quality_counts"]); reasons.update(body["reason_counts"])
        publication_statuses.update(body["publication_status_counts"])
    output["quality_counts"], output["reason_counts"] = dict(quality), dict(reasons)
    output["publication_status_counts"] = dict(publication_statuses)
    for name in SIGNALS:
        group = {"calls": 0, "resolved": 0, "losses": 0, "unknown": 0,
                 "revocations": 0, "by_day": {}, "by_side": {}, "by_quality": {},
                 "abstention_reasons": {"no_observation": scheduled - len(known),
                                        "no_eligible_publication": 0, "below_threshold": 0}}
        remaining = []
        for row in known.values():
            body = _json_object(row["body_json"])
            call = body["first_calls"].get(name)
            if not call:
                reason = "below_threshold" if body["eligible_publications"] else "no_eligible_publication"
                group["abstention_reasons"][reason] += 1
                continue
            remaining.append(int(row["market_end_ms"]) * 1_000_000 - _int(call["publication"]["ack_wall_ns"]))
            outcome = _outcome(body, call, min(now_ms, cutoff))
            side = call["signal"]["side"]
            values = {"calls": 1, "resolved": int(outcome is not None),
                      "losses": int(outcome is not None and side != outcome["winner"]),
                      "unknown": int(outcome is None)}
            for key, value in values.items(): group[key] += value
            group["revocations"] += body["revocations"].get(name, 0)
            for bucket, label in (("by_day", str(int(row["market_start_ms"]) // DAY_MS * DAY_MS)),
                                  ("by_side", side), ("by_quality", call["frozen"]["quality"])):
                entry = group[bucket].setdefault(label, dict.fromkeys(values, 0))
                for key, value in values.items(): entry[key] += value
        group["abstentions"] = scheduled - group["calls"]
        remaining.sort()
        group["call_timing"] = {"count": len(remaining),
            "minimum_remaining_ns": str(remaining[0]) if remaining else None,
            "median_remaining_ns": str((remaining[(len(remaining)-1)//2] + remaining[len(remaining)//2]) // 2) if remaining else None,
            "maximum_remaining_ns": str(remaining[-1]) if remaining else None}
        with localcontext() as context:
            context.prec = 80
            group["loss_rate"] = str(Decimal(group["losses"]) / group["resolved"]) if group["resolved"] else None
            group["coverage"] = str(Decimal(group["calls"]) / scheduled) if scheduled else None
        output["signals"][name] = group
    for baseline in ("twap", "spot"):
        counts = {"paired_resolved": 0, "ghost_only_correct": 0, "baseline_only_correct": 0,
                  "both_correct": 0, "both_wrong": 0, "unknown_outcome": 0, "baseline_tie_or_missing": 0}
        for row in known.values():
            body = _json_object(row["body_json"]); call = body["first_calls"].get("ghost")
            if not call: continue
            outcome = _outcome(body, call, min(now_ms, cutoff))
            if outcome is None: counts["unknown_outcome"] += 1; continue
            baseline_side = call["frozen"].get("signals", {}).get(baseline, {}).get("side")
            if baseline_side not in ("up", "down"): counts["baseline_tie_or_missing"] += 1; continue
            ghost_correct = call["signal"]["side"] == outcome["winner"]
            baseline_correct = baseline_side == outcome["winner"]
            counts["paired_resolved"] += 1
            key = "both_correct" if ghost_correct and baseline_correct else "both_wrong" if not ghost_correct and not baseline_correct else "ghost_only_correct" if ghost_correct else "baseline_only_correct"
            counts[key] += 1
        output["paired_at_ghost_first"][baseline] = counts
    return output


class SettlementStore:
    def __init__(self, pool: Any, start_ms: int):
        if type(start_ms) is not int or start_ms < 0 or start_ms % DAY_MS:
            raise ValueError("evaluation start must be a UTC day boundary")
        self.pool, self.start_ms = pool, start_ms
        self.end_ms = start_ms + EVALUATION_DAYS * DAY_MS
        self.cutoff_ms = self.end_ms + DAY_MS
        self.report_due_ms = self.cutoff_ms + 6 * 3_600_000
        self._outcome_cursor = -1
        self._finalized = False

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
        evaluation_start = _int(frozen.get("evaluation_start_ms", self.start_ms))
        if evaluation_start % DAY_MS:
            raise ValueError("frozen evaluation start must be a UTC day boundary")
        if (start % 300_000 or end != start + 300_000 or market != start // 300_000
                or _int(frozen["target_source_timestamp_ms"]) != end
                or incoming["created_ms"] != incoming["decision_wall_ns"] // 1_000_000
                or not (end - 30_000) * 1_000_000 <= incoming["decision_wall_ns"] < end * 1_000_000):
            raise ValueError("decision outside frozen settlement schedule")
        compact = _canonical(_public(frozen))
        previous_state = None
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
                previous_state = _json_object(old["state_json"])
                current_target = state.get("target", {})
                if previous_target.get("first_event") is not None and (
                        previous_target["first_event"] != current_target.get("first_event")
                        or previous_target.get("status") != current_target.get("status")):
                    raise GhostAuditConflict("first settlement target is immutable")
                await connection.execute("UPDATE settlement_audit SET state_json=$3,version=$4,terminal=$5 WHERE run_id=$1 AND decision_id=$2",
                                         incoming["run_id"], incoming["decision_id"], incoming["state_json"], incoming["version"], incoming["terminal"])
            if not (evaluation_start and evaluation_start <= start < evaluation_start + EVALUATION_DAYS * DAY_MS):
                return "inserted" if inserted else "updated"
            await connection.execute("""INSERT INTO settlement_market_evaluation
                (evaluation_start_ms,market_id,market_start_ms,market_end_ms,body_json)
                VALUES($1,$2,$3,$4,'{}') ON CONFLICT DO NOTHING""", evaluation_start, market, start, end)
            previous = await connection.fetchval("""SELECT body_json FROM settlement_market_evaluation
                WHERE evaluation_start_ms=$1 AND market_id=$2 FOR UPDATE""", evaluation_start, market)
            body = update_market(_json_object(previous), frozen, state, inserted=bool(inserted), previous_state=previous_state)
            await connection.execute("UPDATE settlement_market_evaluation SET body_json=$3 WHERE evaluation_start_ms=$1 AND market_id=$2",
                                     evaluation_start, market, _canonical(body))
        return "inserted" if inserted else "updated"

    async def guard(self) -> dict:
        async with self._connection() as connection:
            row = await connection.fetchrow("""SELECT
                pg_total_relation_size('settlement_audit')+pg_total_relation_size('settlement_market_evaluation')+
                pg_total_relation_size('settlement_evaluation_reports') AS relation_bytes,
                (SELECT count(*) FROM (SELECT 1 FROM settlement_audit LIMIT 200001) bounded) AS row_count""")
        used, count = int(row["relation_bytes"]), int(row["row_count"])
        return {"relation_bytes": used, "row_count": count, "warning": used >= WARN_BYTES,
                "capacity_ok": used < STOP_BYTES - 16 * 1024 * 1024 and count < ROW_CAP,
                "stop_bytes": STOP_BYTES, "row_cap": ROW_CAP}

    async def _capture_outcomes(self, now_ms: int) -> bool:
        async with self._connection() as connection:
            rows = await connection.fetch("""SELECT e.market_id,e.body_json,m.settlement_reference,m.settlement_window_s,
                m.settlement_source_url,m.settlement_rule_version,m.condition_id,m.up_token_id,m.down_token_id,
                r.resolution_status,r.resolution_type,r.winner,r.winning_token_id,r.up_payout,r.down_payout,
                r.last_checked_ms,r.resolution_source,r.reconciled_settlement_rule_version
                FROM settlement_market_evaluation e
                LEFT JOIN polymarket_btc_5m_markets m USING(market_id)
                LEFT JOIN polymarket_btc_5m_resolutions r USING(market_id)
                WHERE e.evaluation_start_ms=$1 AND e.market_id>$2 ORDER BY e.market_id LIMIT 100
                FOR UPDATE OF e""", self.start_ms, self._outcome_cursor)
            for row in rows:
                body = _json_object(row["body_json"])
                replacement = capture_outcome(body, row, min(now_ms, self.cutoff_ms))
                if replacement != body:
                    await connection.execute("UPDATE settlement_market_evaluation SET body_json=$3 WHERE evaluation_start_ms=$1 AND market_id=$2",
                                             self.start_ms, row["market_id"], _canonical(replacement))
            self._outcome_cursor = int(rows[-1]["market_id"]) if len(rows) == MAX_BATCH else -1
            return self._outcome_cursor == -1

    async def report(self, now_ms: int, *, persistence_complete: bool = True) -> dict:
        if not self.start_ms:
            return {"schema_version": 1, "status": "unarmed", "final": False,
                    "evaluation_start_ms": 0, "persistence_complete": persistence_complete}
        if now_ms < self.start_ms:
            return {"schema_version": 1, "status": "scheduled", "final": False,
                    "evaluation_start_ms": self.start_ms, "evaluation_end_ms": self.end_ms,
                    "outcome_cutoff_ms": self.cutoff_ms, "report_due_ms": self.report_due_ms,
                    "persistence_complete": persistence_complete}
        if now_ms >= self.start_ms + REPORT_MS:
            return {"schema_version": 1, "status": "expired", "final": True,
                    "evaluation_start_ms": self.start_ms}
        async with self._connection() as connection:
            existing = await connection.fetchrow("SELECT * FROM settlement_evaluation_reports WHERE evaluation_start_ms=$1", self.start_ms)
            if existing and existing["final"]:
                self._finalized = True
                return _json_object(existing["body_json"])
            rows, cursor = [], -1
            while True:
                batch = await connection.fetch("""SELECT * FROM settlement_market_evaluation
                    WHERE evaluation_start_ms=$1 AND market_id>$2 ORDER BY market_id LIMIT 100""", self.start_ms, cursor)
                rows.extend(batch)
                if len(rows) > MAX_MARKETS: raise ValueError("settlement market report exceeds fixed schedule")
                if len(batch) < MAX_BATCH: break
                cursor = int(batch[-1]["market_id"])
            final = now_ms >= self.report_due_ms or now_ms >= self.cutoff_ms and persistence_complete
            report = build_report(rows, self.start_ms, now_ms, final=final,
                                  persistence_complete=persistence_complete)
            encoded = _canonical(report)
            result = await connection.fetchval("""INSERT INTO settlement_evaluation_reports
                (evaluation_start_ms,created_ms,updated_ms,final,body_json) VALUES($1,$2,$2,$3,$4)
                ON CONFLICT(evaluation_start_ms) DO UPDATE SET updated_ms=EXCLUDED.updated_ms,
                final=EXCLUDED.final,body_json=EXCLUDED.body_json
                WHERE NOT settlement_evaluation_reports.final RETURNING body_json""", self.start_ms, now_ms, report["final"], encoded)
            if result is None:
                result = await connection.fetchval("SELECT body_json FROM settlement_evaluation_reports WHERE evaluation_start_ms=$1", self.start_ms)
            result = _json_object(result)
            self._finalized = result["final"]
            return result

    async def maintain(self, now_ms: int, *, persistence_complete: bool = True) -> dict:
        if (self.start_ms and not self._finalized and self.cutoff_ms <= now_ms < self.start_ms + REPORT_MS
                and (persistence_complete or now_ms >= self.report_due_ms)):
            # A bounded 576-market sweep closes the outcome snapshot before
            # freezing. Each page has its own short transaction and row limit.
            self._outcome_cursor = -1
            for _ in range(MAX_MARKETS // MAX_BATCH + 1):
                if await self._capture_outcomes(now_ms):
                    break
        elif self.start_ms and now_ms < self.report_due_ms:
            await self._capture_outcomes(now_ms)
        report = await self.report(now_ms, persistence_complete=persistence_complete)
        async with self._connection() as connection:
            await connection.execute("""WITH batch AS (SELECT run_id,decision_id FROM settlement_audit
                WHERE terminal AND frozen_json IS NOT NULL AND market_end_ms+120000<=$1
                ORDER BY created_ms,run_id,decision_id LIMIT 100 FOR UPDATE SKIP LOCKED)
                UPDATE settlement_audit a SET frozen_json=NULL FROM batch b
                WHERE a.run_id=b.run_id AND a.decision_id=b.decision_id""", now_ms)
            await connection.execute("""WITH batch AS (SELECT run_id,decision_id FROM settlement_audit
                WHERE created_ms<=$1 ORDER BY created_ms,run_id,decision_id LIMIT 100 FOR UPDATE SKIP LOCKED)
                DELETE FROM settlement_audit a USING batch b WHERE a.run_id=b.run_id AND a.decision_id=b.decision_id""", now_ms - INDIVIDUAL_MS)
            await connection.execute("""WITH batch AS (SELECT evaluation_start_ms,market_id FROM settlement_market_evaluation
                WHERE market_end_ms<=$1 ORDER BY market_end_ms LIMIT 100 FOR UPDATE SKIP LOCKED)
                DELETE FROM settlement_market_evaluation e USING batch b
                WHERE e.evaluation_start_ms=b.evaluation_start_ms AND e.market_id=b.market_id""", now_ms - INDIVIDUAL_MS)
            await connection.execute("""WITH batch AS (SELECT evaluation_start_ms FROM settlement_evaluation_reports
                WHERE created_ms<=$1 ORDER BY created_ms LIMIT 100 FOR UPDATE SKIP LOCKED)
                DELETE FROM settlement_evaluation_reports r USING batch b
                WHERE r.evaluation_start_ms=b.evaluation_start_ms""", now_ms - REPORT_MS)
        return report


class _ExistingConnectionPool:
    """Use the retention job's operator connection without acquiring another."""
    def __init__(self, connection):
        self.connection = connection

    @asynccontextmanager
    async def acquire(self, *, timeout):
        yield self.connection


async def finalize_disabled_evaluations(connection, now_ms: int) -> dict:
    """Best-effort bounded fallback before expiry if the optional producer stops.

    Only previously captured as-of outcomes are used. The outbox is not owned by
    this process, so its final report explicitly has persistence_complete=false.
    """
    if not await connection.fetchval("SELECT to_regclass('public.settlement_evaluation_reports')::text"):
        return {"finalized": 0, "errors": {}}
    rows = await connection.fetch("""SELECT evaluation_start_ms FROM (
        SELECT evaluation_start_ms FROM settlement_evaluation_reports
        WHERE NOT final AND evaluation_start_ms<=$1 AND evaluation_start_ms>$2
        UNION
        SELECT e.evaluation_start_ms FROM settlement_market_evaluation e
        WHERE e.evaluation_start_ms<=$1 AND e.evaluation_start_ms>$2 AND NOT EXISTS (
            SELECT 1 FROM settlement_evaluation_reports r WHERE r.evaluation_start_ms=e.evaluation_start_ms)
        GROUP BY e.evaluation_start_ms) due ORDER BY evaluation_start_ms LIMIT 10""",
        now_ms - (EVALUATION_DAYS + 1) * DAY_MS - 6 * 3_600_000, now_ms - REPORT_MS)
    result = {"finalized": 0, "errors": {}}
    for row in rows:
        start = int(row["evaluation_start_ms"])
        try:
            report = await SettlementStore(_ExistingConnectionPool(connection), start).report(
                now_ms, persistence_complete=False)
            result["finalized"] += int(report.get("final") is True)
        except Exception as error:
            result["errors"][str(start)] = type(error).__name__
    return result
