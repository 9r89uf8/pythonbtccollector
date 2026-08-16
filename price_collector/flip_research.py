"""Permanent post-resolution research records for BTC five-minute flips.

The analyzer in this module deliberately separates source-time ordering from
causal availability. A Chainlink update is eligible for crossing research only
when it was received before market end, and a cutoff feature is eligible only
when both its observation and source timestamps were available by that cutoff.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Iterable, Mapping, Optional, Sequence

from price_collector.binance_microstructure import MICROSTRUCTURE_VALUE_COLUMNS


LOGGER = logging.getLogger("price_collector.flip_research")

SPOT_FLIP_DEFINITION_VERSION = 1
TWAP_30S_FLIP_DEFINITION_VERSION = 2
TWAP_60S_FLIP_DEFINITION_VERSION = 3
TWAP_FLIP_DEFINITION_VERSION = TWAP_60S_FLIP_DEFINITION_VERSION
FLIP_DEFINITION_VERSION = TWAP_FLIP_DEFINITION_VERSION
TWAP_30S_TOPIC = "crypto_prices_twap_thirty"
TWAP_30S_WINDOW_SECONDS = 30
TWAP_30S_SOURCE_URL = (
    "https://data.chain.link/streams/btc-usd-twap-30s-streams"
)
TWAP_30S_RULE_VERSION = "btc-5m-twap-30"
TWAP_60S_TOPIC = "crypto_prices_twap_sixty"
TWAP_60S_WINDOW_SECONDS = 60
TWAP_60S_SOURCE_URL = (
    "https://data.chain.link/streams/btc-usd-twap-60s-streams"
)
TWAP_60S_RULE_VERSION = "btc-5m-twap-60"
TWAP_60S_CUTOVER_MS = 1_786_665_600_000
FLIP_WINDOW_SECONDS = 20
CHAINLINK_CUTOFF_FRESH_MS = 10_000
MAX_CONFIRMED_CROSSING_GAP_MS = CHAINLINK_CUTOFF_FRESH_MS
# Live RTDS measurements show the one-second TWAP frames normally arrive
# roughly 1.2--2.8 seconds after their provider timestamp.  Freshness is a
# causal-age guard, while the independent density checks below prevent this
# allowance from hiding missing one-second observations.
TWAP_CUTOFF_FRESH_MS = 5_000
TWAP_MAX_OBSERVATION_GAP_MS = 1_500
# Coverage is anchored outside both edges of the final 20-second window: the
# last raw source event before T-20 through the first persisted event after the
# market boundary. This prevents survivor-selected bounds from hiding a hole.
TWAP_MIN_COVERAGE_SPAN_MS = FLIP_WINDOW_SECONDS * 1000
TWAP_MIN_COVERAGE_EVENT_COUNT = FLIP_WINDOW_SECONDS + 1
PROBABILITY_CUTOFF_FRESH_MS = 15_000
FLIP_FINALIZATION_GRACE_MS = 30_000
FLIP_EVALUATOR_POLL_SECONDS = 5
FLIP_EVALUATOR_BATCH_SIZE = 20
FLIP_EVALUATOR_MAX_BACKOFF_SECONDS = 300

STRICT_UP = "Up"
STRICT_DOWN = "Down"
TIE = "tie"

EVALUATION_CONFIRMED_FLIP = "confirmed_flip"
EVALUATION_NON_FLIP = "non_flip"
EVALUATION_AMBIGUOUS = "ambiguous"

ARCHIVE_NOT_REQUIRED = "not_required"
ARCHIVE_PENDING = "pending"
ARCHIVE_COMPLETE = "complete"
ARCHIVE_FAILED = "failed"


@dataclass(frozen=True)
class TwapFlipIdentity:
    topic: str
    window_seconds: int
    source_url: str
    rule_version: str

    @property
    def source_reference(self) -> str:
        return f"chainlink_twap_{self.window_seconds}s"


_TWAP_IDENTITIES = {
    TWAP_30S_FLIP_DEFINITION_VERSION: TwapFlipIdentity(
        topic=TWAP_30S_TOPIC,
        window_seconds=TWAP_30S_WINDOW_SECONDS,
        source_url=TWAP_30S_SOURCE_URL,
        rule_version=TWAP_30S_RULE_VERSION,
    ),
    TWAP_60S_FLIP_DEFINITION_VERSION: TwapFlipIdentity(
        topic=TWAP_60S_TOPIC,
        window_seconds=TWAP_60S_WINDOW_SECONDS,
        source_url=TWAP_60S_SOURCE_URL,
        rule_version=TWAP_60S_RULE_VERSION,
    ),
}


def _market_rule_sql(
    definition_expression: str,
    *,
    market_alias: str,
    window_alias: str,
) -> str:
    """Build the exact immutable definition-to-market identity predicate."""

    return f"""
        (
            (
                {definition_expression} = {TWAP_30S_FLIP_DEFINITION_VERSION}
                AND {window_alias}.market_start_ms < {TWAP_60S_CUTOVER_MS}
                AND {market_alias}.settlement_reference = 'chainlink_twap'
                AND {market_alias}.settlement_window_s = {TWAP_30S_WINDOW_SECONDS}
                AND {market_alias}.settlement_source_url = '{TWAP_30S_SOURCE_URL}'
                AND {market_alias}.settlement_rule_version = '{TWAP_30S_RULE_VERSION}'
            )
            OR (
                {definition_expression} = {TWAP_60S_FLIP_DEFINITION_VERSION}
                AND {window_alias}.market_start_ms >= {TWAP_60S_CUTOVER_MS}
                AND {market_alias}.settlement_reference = 'chainlink_twap'
                AND {market_alias}.settlement_window_s = {TWAP_60S_WINDOW_SECONDS}
                AND {market_alias}.settlement_source_url = '{TWAP_60S_SOURCE_URL}'
                AND {market_alias}.settlement_rule_version = '{TWAP_60S_RULE_VERSION}'
            )
            OR (
                {definition_expression} = {SPOT_FLIP_DEFINITION_VERSION}
                AND {market_alias}.settlement_reference = 'chainlink_spot'
                AND {market_alias}.settlement_rule_version = 'chainlink-spot-v1'
            )
        )
    """


def _twap_identity_for_definition(
    definition_version: int,
) -> Optional[TwapFlipIdentity]:
    return _TWAP_IDENTITIES.get(definition_version)


def _is_twap_definition(definition_version: int) -> bool:
    return _twap_identity_for_definition(definition_version) is not None


@dataclass(frozen=True)
class ChainlinkObservation:
    sample_second_ms: int
    price: Decimal
    provider_event_ms: int
    received_ms: int
    provider_message_ms: Optional[int] = None
    received_wall_ns: Optional[int] = None


@dataclass(frozen=True)
class ProbabilityObservation:
    sample_second_ms: int
    received_ms: int
    provider_event_ms: Optional[int]
    up_bid: Optional[Decimal]
    up_ask: Optional[Decimal]
    up_mid: Optional[Decimal]
    down_bid: Optional[Decimal]
    down_ask: Optional[Decimal]
    down_mid: Optional[Decimal]
    up_prob_norm: Optional[Decimal]
    down_prob_norm: Optional[Decimal]
    up_provider_event_ms: Optional[int]
    up_received_ms: Optional[int]
    down_provider_event_ms: Optional[int]
    down_received_ms: Optional[int]


@dataclass(frozen=True)
class FlipEvent:
    event_sequence: int
    previous_side: str
    new_side: str
    previous_price: Decimal
    new_price: Decimal
    previous_sample_second_ms: int
    sample_second_ms: int
    previous_provider_event_ms: int
    provider_event_ms: int
    previous_received_ms: int
    received_ms: int
    observation_gap_ms: int
    milliseconds_before_expiry: int
    decisive: bool
    observation_precision: str = "one_second_summary"

    @property
    def direction(self) -> str:
        return (
            "down_to_up"
            if self.previous_side == STRICT_DOWN
            else "up_to_down"
        )


@dataclass(frozen=True)
class CutoffObservation:
    seconds_before_end: int
    cutoff_ms: int
    chainlink: Optional[ChainlinkObservation]
    chainlink_source_age_ms: Optional[int]
    chainlink_receive_age_ms: Optional[int]
    chainlink_fresh: bool
    signed_distance: Optional[Decimal]
    absolute_distance: Optional[Decimal]
    apparent_side: Optional[str]
    probability: Optional[ProbabilityObservation]
    probability_source_age_ms: Optional[int]
    probability_receive_age_ms: Optional[int]
    up_probability_source_age_ms: Optional[int]
    up_probability_receive_age_ms: Optional[int]
    down_probability_source_age_ms: Optional[int]
    down_probability_receive_age_ms: Optional[int]
    probability_fresh: bool
    microstructure: Optional[Mapping[str, Any]]
    cutoff_reversal: Optional[bool]

    @property
    def flipped_after_cutoff(self) -> Optional[bool]:
        return self.cutoff_reversal


@dataclass(frozen=True)
class FlipAnalysis:
    market_id: int
    market_start_ms: int
    market_end_ms: int
    definition_version: int
    evaluation_status: str
    resolution_type: Optional[str]
    threshold: Optional[Decimal]
    official_close: Optional[Decimal]
    winner: Optional[str]
    events: tuple[FlipEvent, ...]
    cutoffs: tuple[CutoffObservation, ...]
    chainlink_observation_count: int
    chainlink_strict_observation_count: int
    chainlink_first_provider_event_ms: Optional[int]
    chainlink_last_provider_event_ms: Optional[int]
    chainlink_max_gap_ms: Optional[int]
    touch_count: int
    fresh_cutoff_count: int
    probability_cutoff_count: int
    fresh_probability_cutoff_count: int
    microstructure_cutoff_count: int
    source_microstructure_row_count: int
    first_crossing_ms_before_end: Optional[int]
    last_crossing_ms_before_end: Optional[int]
    decisive_event_sequence: Optional[int]
    data_quality: Mapping[str, Any]

    @property
    def crossing_count(self) -> int:
        return len(self.events)

    @property
    def needs_archive(self) -> bool:
        return self.evaluation_status in {
            EVALUATION_CONFIRMED_FLIP,
            EVALUATION_AMBIGUOUS,
        }


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _require_decimal_or_none(value: Any, field_name: str) -> Optional[Decimal]:
    if value is None:
        return None
    if not isinstance(value, Decimal):
        raise TypeError(f"{field_name} must be Decimal or None")
    if not value.is_finite():
        raise ValueError(f"{field_name} must be finite")
    return value


def side_for_price(price: Decimal, threshold: Decimal) -> str:
    """Return the strict threshold side, preserving equality as a tie."""

    _require_decimal_or_none(price, "price")
    _require_decimal_or_none(threshold, "threshold")
    if price > threshold:
        return STRICT_UP
    if price < threshold:
        return STRICT_DOWN
    return TIE


def _chainlink_from_row(row: Mapping[str, Any]) -> ChainlinkObservation:
    return ChainlinkObservation(
        sample_second_ms=int(row["sample_second_ms"]),
        price=_require_decimal_or_none(row["price"], "chainlink price"),  # type: ignore[arg-type]
        provider_event_ms=int(row["provider_event_ms"]),
        received_ms=int(row["received_ms"]),
        provider_message_ms=(
            int(row["provider_message_ms"])
            if row.get("provider_message_ms") is not None
            else None
        ),
        received_wall_ns=(
            int(row["received_wall_ns"])
            if row.get("received_wall_ns") is not None
            else None
        ),
    )


def _observation_received_by(
    observation: ChainlinkObservation,
    cutoff_ms: int,
) -> bool:
    if observation.received_wall_ns is not None:
        return observation.received_wall_ns <= cutoff_ms * 1_000_000
    return observation.received_ms <= cutoff_ms


def _observation_received_before(
    observation: ChainlinkObservation,
    boundary_ms: int,
) -> bool:
    if observation.received_wall_ns is not None:
        return observation.received_wall_ns < boundary_ms * 1_000_000
    return observation.received_ms < boundary_ms


def _observation_received_order_ns(observation: ChainlinkObservation) -> int:
    return (
        observation.received_wall_ns
        if observation.received_wall_ns is not None
        else observation.received_ms * 1_000_000
    )


def normalize_gap_interval_ms(
    *,
    gap_start_provider_ms: Optional[int],
    gap_end_provider_ms: Optional[int],
    gap_start_wall_ns: int,
    gap_end_wall_ns: Optional[int],
) -> tuple[int, Optional[int], str]:
    """Choose one coherent clock for a persisted gap interval."""

    provider_bounds_valid = gap_start_provider_ms is not None and (
        (
            gap_end_provider_ms is not None
            and gap_end_provider_ms > gap_start_provider_ms
        )
        or (
            gap_end_provider_ms is None
            and gap_end_wall_ns is None
        )
    )
    if provider_bounds_valid:
        return gap_start_provider_ms, gap_end_provider_ms, "provider"

    wall_start_ms = gap_start_wall_ns // 1_000_000
    if gap_end_wall_ns is None:
        return wall_start_ms, None, "wall"
    wall_end_ms = gap_end_wall_ns // 1_000_000
    return min(wall_start_ms, wall_end_ms), max(
        wall_start_ms,
        wall_end_ms,
    ), "wall"


def gap_interval_overlaps_final_window(
    *,
    gap_start_ms: int,
    gap_end_ms: Optional[int],
    market_end_ms: int,
) -> bool:
    """Return whether a half-open source gap overlaps the final 20 seconds.

    A missing recovery event leaves the gap open. Bounds are normalized onto
    one clock before reaching this helper.
    """

    window_start_ms = market_end_ms - FLIP_WINDOW_SECONDS * 1000
    return gap_start_ms < market_end_ms and (
        gap_end_ms is None or gap_end_ms > window_start_ms
    )


def _probability_from_row(row: Mapping[str, Any]) -> ProbabilityObservation:
    return ProbabilityObservation(
        sample_second_ms=int(row["sample_second_ms"]),
        received_ms=int(row["received_ms"]),
        provider_event_ms=(
            int(row["provider_event_ms"])
            if row.get("provider_event_ms") is not None
            else None
        ),
        up_bid=_require_decimal_or_none(row.get("up_bid"), "up_bid"),
        up_ask=_require_decimal_or_none(row.get("up_ask"), "up_ask"),
        up_mid=_require_decimal_or_none(row.get("up_mid"), "up_mid"),
        down_bid=_require_decimal_or_none(row.get("down_bid"), "down_bid"),
        down_ask=_require_decimal_or_none(row.get("down_ask"), "down_ask"),
        down_mid=_require_decimal_or_none(row.get("down_mid"), "down_mid"),
        up_prob_norm=_require_decimal_or_none(
            row.get("up_prob_norm"),
            "up_prob_norm",
        ),
        down_prob_norm=_require_decimal_or_none(
            row.get("down_prob_norm"),
            "down_prob_norm",
        ),
        up_provider_event_ms=(
            int(row["up_provider_event_ms"])
            if row.get("up_provider_event_ms") is not None
            else None
        ),
        up_received_ms=(
            int(row["up_received_ms"])
            if row.get("up_received_ms") is not None
            else None
        ),
        down_provider_event_ms=(
            int(row["down_provider_event_ms"])
            if row.get("down_provider_event_ms") is not None
            else None
        ),
        down_received_ms=(
            int(row["down_received_ms"])
            if row.get("down_received_ms") is not None
            else None
        ),
    )


def _latest_causal_chainlink(
    observations: Sequence[ChainlinkObservation],
    cutoff_ms: int,
) -> Optional[ChainlinkObservation]:
    candidates = (
        observation
        for observation in observations
        if observation.provider_event_ms <= cutoff_ms
        and _observation_received_by(observation, cutoff_ms)
    )
    return max(
        candidates,
        key=lambda item: (
            item.provider_event_ms,
            item.sample_second_ms,
            item.received_ms,
            _observation_received_order_ns(item),
        ),
        default=None,
    )


def _latest_causal_probability(
    observations: Sequence[ProbabilityObservation],
    cutoff_ms: int,
) -> Optional[ProbabilityObservation]:
    candidates = (
        observation
        for observation in observations
        if observation.sample_second_ms <= cutoff_ms
        and observation.received_ms <= cutoff_ms
        and (
            observation.provider_event_ms is None
            or observation.provider_event_ms <= cutoff_ms
        )
    )
    return max(
        candidates,
        key=lambda item: (
            item.sample_second_ms,
            item.received_ms,
            item.provider_event_ms or -1,
        ),
        default=None,
    )


def _microstructure_by_second(
    rows: Iterable[Mapping[str, Any]],
) -> dict[int, Mapping[str, Any]]:
    result: dict[int, Mapping[str, Any]] = {}
    for row in rows:
        sample_second_ms = row.get("sample_second_ms")
        if isinstance(sample_second_ms, int) and not isinstance(
            sample_second_ms,
            bool,
        ):
            result[sample_second_ms] = dict(row)
    return result


def _build_cutoffs(
    *,
    market_end_ms: int,
    threshold: Optional[Decimal],
    winner: Optional[str],
    chainlink: Sequence[ChainlinkObservation],
    probabilities: Sequence[ProbabilityObservation],
    microstructure_rows: Iterable[Mapping[str, Any]],
    definition_version: int,
) -> tuple[CutoffObservation, ...]:
    microstructure = _microstructure_by_second(microstructure_rows)
    cutoffs: list[CutoffObservation] = []

    for seconds_before_end in range(FLIP_WINDOW_SECONDS, 0, -1):
        cutoff_ms = market_end_ms - seconds_before_end * 1000
        chainlink_observation = _latest_causal_chainlink(chainlink, cutoff_ms)
        source_age_ms = (
            cutoff_ms - chainlink_observation.provider_event_ms
            if chainlink_observation is not None
            else None
        )
        receive_age_ms = (
            cutoff_ms - chainlink_observation.received_ms
            if chainlink_observation is not None
            else None
        )
        if _is_twap_definition(definition_version):
            chainlink_fresh = (
                source_age_ms is not None
                and 0 <= source_age_ms < TWAP_CUTOFF_FRESH_MS
            )
        else:
            chainlink_fresh = (
                source_age_ms is not None
                and 0 <= source_age_ms <= CHAINLINK_CUTOFF_FRESH_MS
            )

        apparent_side: Optional[str] = None
        signed_distance: Optional[Decimal] = None
        absolute_distance: Optional[Decimal] = None
        if chainlink_observation is not None and threshold is not None:
            signed_distance = chainlink_observation.price - threshold
            absolute_distance = abs(signed_distance)
            apparent_side = side_for_price(chainlink_observation.price, threshold)

        probability_observation = _latest_causal_probability(
            probabilities,
            cutoff_ms,
        )
        probability_source_age_ms = (
            max(source_ages)
            if (
                probability_observation is not None
                and (
                    source_ages := [
                        cutoff_ms - source_ms
                        for source_ms in (
                            probability_observation.up_provider_event_ms,
                            probability_observation.down_provider_event_ms,
                        )
                        if source_ms is not None
                    ]
                )
            )
            else None
        )
        up_probability_source_age_ms = (
            cutoff_ms - probability_observation.up_provider_event_ms
            if probability_observation is not None
            and probability_observation.up_provider_event_ms is not None
            else None
        )
        up_probability_receive_age_ms = (
            cutoff_ms - probability_observation.up_received_ms
            if probability_observation is not None
            and probability_observation.up_received_ms is not None
            else None
        )
        down_probability_source_age_ms = (
            cutoff_ms - probability_observation.down_provider_event_ms
            if probability_observation is not None
            and probability_observation.down_provider_event_ms is not None
            else None
        )
        down_probability_receive_age_ms = (
            cutoff_ms - probability_observation.down_received_ms
            if probability_observation is not None
            and probability_observation.down_received_ms is not None
            else None
        )
        probability_receive_age_ms = (
            max(
                up_probability_receive_age_ms,
                down_probability_receive_age_ms,
            )
            if up_probability_receive_age_ms is not None
            and down_probability_receive_age_ms is not None
            else None
        )
        probability_fresh = (
            probability_receive_age_ms is not None
            and 0
            <= probability_receive_age_ms
            <= PROBABILITY_CUTOFF_FRESH_MS
            and (
                probability_source_age_ms is None
                or 0
                <= probability_source_age_ms
                <= PROBABILITY_CUTOFF_FRESH_MS
            )
        )

        cutoff_reversal = None
        if winner in {STRICT_UP, STRICT_DOWN} and apparent_side in {
            STRICT_UP,
            STRICT_DOWN,
        }:
            cutoff_reversal = apparent_side != winner

        cutoffs.append(
            CutoffObservation(
                seconds_before_end=seconds_before_end,
                cutoff_ms=cutoff_ms,
                chainlink=chainlink_observation,
                chainlink_source_age_ms=source_age_ms,
                chainlink_receive_age_ms=receive_age_ms,
                chainlink_fresh=chainlink_fresh,
                signed_distance=signed_distance,
                absolute_distance=absolute_distance,
                apparent_side=apparent_side,
                probability=probability_observation,
                probability_source_age_ms=probability_source_age_ms,
                probability_receive_age_ms=probability_receive_age_ms,
                up_probability_source_age_ms=(
                    up_probability_source_age_ms
                ),
                up_probability_receive_age_ms=(
                    up_probability_receive_age_ms
                ),
                down_probability_source_age_ms=(
                    down_probability_source_age_ms
                ),
                down_probability_receive_age_ms=(
                    down_probability_receive_age_ms
                ),
                probability_fresh=probability_fresh,
                microstructure=microstructure.get(cutoff_ms - 1000),
                cutoff_reversal=cutoff_reversal,
            )
        )

    return tuple(cutoffs)


def _build_crossings(
    *,
    market_end_ms: int,
    threshold: Decimal,
    winner: Optional[str],
    observations: Sequence[ChainlinkObservation],
    observation_precision: str = "one_second_summary",
) -> tuple[FlipEvent, ...]:
    interval_start_ms = market_end_ms - FLIP_WINDOW_SECONDS * 1000
    causal = sorted(
        (
            observation
            for observation in observations
            if observation.provider_event_ms < market_end_ms
            and _observation_received_before(observation, market_end_ms)
        ),
        key=lambda item: (
            item.provider_event_ms,
            item.sample_second_ms,
            item.received_ms,
            _observation_received_order_ns(item),
        ),
    )

    provisional: list[FlipEvent] = []
    last_strict: Optional[tuple[str, ChainlinkObservation]] = None
    for observation in causal:
        current_side = side_for_price(observation.price, threshold)
        if current_side == TIE:
            continue

        if last_strict is not None:
            previous_side, previous = last_strict
            if (
                previous_side != current_side
                and interval_start_ms
                <= observation.provider_event_ms
                < market_end_ms
            ):
                provisional.append(
                    FlipEvent(
                        event_sequence=len(provisional) + 1,
                        previous_side=previous_side,
                        new_side=current_side,
                        previous_price=previous.price,
                        new_price=observation.price,
                        previous_sample_second_ms=previous.sample_second_ms,
                        sample_second_ms=observation.sample_second_ms,
                        previous_provider_event_ms=previous.provider_event_ms,
                        provider_event_ms=observation.provider_event_ms,
                        previous_received_ms=previous.received_ms,
                        received_ms=observation.received_ms,
                        observation_gap_ms=(
                            observation.provider_event_ms
                            - previous.provider_event_ms
                        ),
                        milliseconds_before_expiry=(
                            market_end_ms - observation.provider_event_ms
                        ),
                        decisive=False,
                        observation_precision=observation_precision,
                    )
                )

        last_strict = (current_side, observation)

    decisive_sequence = next(
        (
            event.event_sequence
            for event in reversed(provisional)
            if event.new_side == winner
        ),
        None,
    )
    return tuple(
        FlipEvent(
            **{
                **event.__dict__,
                "decisive": event.event_sequence == decisive_sequence,
            }
        )
        for event in provisional
    )


def _crossing_coverage_gap_ms(
    event: FlipEvent,
    observations: Sequence[ChainlinkObservation],
    *,
    market_end_ms: int,
) -> int:
    """Return the largest raw source-time gap bracketing one crossing.

    Flip events intentionally connect strict-side observations across equality
    touches.  Completeness must therefore inspect every intervening raw TWAP
    frame instead of mistaking a valid tie for a missing observation.
    """

    provider_times = sorted(
        {
            observation.provider_event_ms
            for observation in observations
            if event.previous_provider_event_ms
            <= observation.provider_event_ms
            <= event.provider_event_ms
            and _observation_received_before(observation, market_end_ms)
        }
    )
    if not provider_times:
        return event.observation_gap_ms
    if provider_times[0] > event.previous_provider_event_ms:
        provider_times.insert(0, event.previous_provider_event_ms)
    if provider_times[-1] < event.provider_event_ms:
        provider_times.append(event.provider_event_ms)
    return max(
        (
            current - previous
            for previous, current in zip(provider_times, provider_times[1:])
        ),
        default=0,
    )


def analyze_market(
    *,
    market_id: int,
    market_start_ms: int,
    market_end_ms: int,
    resolution_type: Optional[str],
    threshold: Optional[Decimal],
    official_close: Optional[Decimal],
    winner: Optional[str],
    chainlink_rows: Sequence[Mapping[str, Any]],
    probability_rows: Sequence[Mapping[str, Any]],
    microstructure_rows: Sequence[Mapping[str, Any]],
    source_gap_rows: Sequence[Mapping[str, Any]] = (),
    definition_version: int = FLIP_DEFINITION_VERSION,
) -> FlipAnalysis:
    """Classify one resolved market without performing I/O."""

    if market_end_ms != market_start_ms + 300_000:
        raise ValueError("market must be a 300-second half-open window")
    twap_identity = _twap_identity_for_definition(definition_version)
    threshold = _require_decimal_or_none(threshold, "threshold")
    official_close = _require_decimal_or_none(official_close, "official_close")

    chainlink = tuple(_chainlink_from_row(row) for row in chainlink_rows)
    probabilities = tuple(
        _probability_from_row(row) for row in probability_rows
    )
    official_winner = winner if winner in {STRICT_UP, STRICT_DOWN} else None
    events = (
        _build_crossings(
            market_end_ms=market_end_ms,
            threshold=threshold,
            winner=official_winner,
            observations=chainlink,
            observation_precision=(
                "exact_twap_event"
                if _is_twap_definition(definition_version)
                else "one_second_summary"
            ),
        )
        if threshold is not None
        else ()
    )
    cutoffs = _build_cutoffs(
        market_end_ms=market_end_ms,
        threshold=threshold,
        winner=official_winner,
        chainlink=chainlink,
        probabilities=probabilities,
        microstructure_rows=microstructure_rows,
        definition_version=definition_version,
    )

    official_complete = (
        resolution_type == "winner"
        and threshold is not None
        and official_close is not None
        and official_winner is not None
    )
    all_cutoffs_fresh = all(cutoff.chainlink_fresh for cutoff in cutoffs)
    maximum_crossing_gap_ms = (
        TWAP_MAX_OBSERVATION_GAP_MS
        if _is_twap_definition(definition_version)
        else MAX_CONFIRMED_CROSSING_GAP_MS
    )
    crossing_coverage_gaps = [
        _crossing_coverage_gap_ms(
            event,
            chainlink,
            market_end_ms=market_end_ms,
        )
        for event in events
    ]
    crossings_have_fresh_brackets = all(
        gap_ms <= maximum_crossing_gap_ms
        for gap_ms in crossing_coverage_gaps
    )
    selected_cutoff_event_times = sorted(
        {
            cutoff.chainlink.provider_event_ms
            for cutoff in cutoffs
            if cutoff.chainlink is not None
        }
    )
    flip_window_start_ms = market_end_ms - FLIP_WINDOW_SECONDS * 1000
    pre_window_event_ms = max(
        (
            observation.provider_event_ms
            for observation in chainlink
            if observation.provider_event_ms < flip_window_start_ms
        ),
        default=None,
    )
    post_boundary_event_ms = min(
        (
            observation.provider_event_ms
            for observation in chainlink
            if observation.provider_event_ms > market_end_ms
        ),
        default=None,
    )
    if pre_window_event_ms is not None and post_boundary_event_ms is not None:
        causal_interval_start_ms = pre_window_event_ms
        causal_interval_end_ms = post_boundary_event_ms
        causal_interval_event_times = sorted(
            {
                observation.provider_event_ms
                for observation in chainlink
                if causal_interval_start_ms
                <= observation.provider_event_ms
                <= causal_interval_end_ms
            }
        )
    else:
        causal_interval_start_ms = None
        causal_interval_end_ms = None
        causal_interval_event_times = []
    causal_interval_gaps = [
        current - previous
        for previous, current in zip(
            causal_interval_event_times,
            causal_interval_event_times[1:],
        )
    ]
    causal_interval_span_ms = (
        causal_interval_end_ms - causal_interval_start_ms
        if causal_interval_start_ms is not None
        and causal_interval_end_ms is not None
        else None
    )
    expected_causal_interval_event_count = (
        causal_interval_span_ms // 1000 + 1
        if causal_interval_span_ms is not None
        else None
    )
    twap_cutoffs_dense = (
        all_cutoffs_fresh
        and causal_interval_span_ms is not None
        and causal_interval_span_ms >= TWAP_MIN_COVERAGE_SPAN_MS
        and len(causal_interval_event_times) >= TWAP_MIN_COVERAGE_EVENT_COUNT
        and expected_causal_interval_event_count is not None
        and len(causal_interval_event_times)
            >= expected_causal_interval_event_count
        and (
            not causal_interval_gaps
            or max(causal_interval_gaps) <= TWAP_MAX_OBSERVATION_GAP_MS
        )
    )
    known_source_gap = (
        _is_twap_definition(definition_version)
        and bool(source_gap_rows)
    )
    if not official_complete or known_source_gap:
        evaluation_status = EVALUATION_AMBIGUOUS
    elif events and crossings_have_fresh_brackets:
        evaluation_status = EVALUATION_CONFIRMED_FLIP
    elif events:
        evaluation_status = EVALUATION_AMBIGUOUS
    elif (
        twap_cutoffs_dense
        if _is_twap_definition(definition_version)
        else all_cutoffs_fresh
    ):
        evaluation_status = EVALUATION_NON_FLIP
    else:
        evaluation_status = EVALUATION_AMBIGUOUS

    analysis_start_ms = market_end_ms - FLIP_WINDOW_SECONDS * 1000
    causal_window_observations = sorted(
        (
            observation
            for observation in chainlink
            if analysis_start_ms
            <= observation.provider_event_ms
            < market_end_ms
            and _observation_received_before(observation, market_end_ms)
        ),
        key=lambda item: (
            item.provider_event_ms,
            item.sample_second_ms,
            item.received_ms,
            _observation_received_order_ns(item),
        ),
    )
    strict_window_observations = [
        observation
        for observation in causal_window_observations
        if threshold is not None
        and side_for_price(observation.price, threshold) != TIE
    ]
    touch_count = sum(
        threshold is not None
        and side_for_price(observation.price, threshold) == TIE
        for observation in causal_window_observations
    )
    observation_gaps = [
        current.provider_event_ms - previous.provider_event_ms
        for previous, current in zip(
            causal_window_observations,
            causal_window_observations[1:],
        )
    ]
    decisive = next((event for event in events if event.decisive), None)
    crossing_offsets = [
        event.milliseconds_before_expiry for event in events
    ]
    return FlipAnalysis(
        market_id=market_id,
        market_start_ms=market_start_ms,
        market_end_ms=market_end_ms,
        definition_version=definition_version,
        evaluation_status=evaluation_status,
        resolution_type=resolution_type,
        threshold=threshold,
        official_close=official_close,
        winner=official_winner,
        events=events,
        cutoffs=cutoffs,
        chainlink_observation_count=len(causal_window_observations),
        chainlink_strict_observation_count=len(strict_window_observations),
        chainlink_first_provider_event_ms=(
            causal_window_observations[0].provider_event_ms
            if causal_window_observations
            else None
        ),
        chainlink_last_provider_event_ms=(
            causal_window_observations[-1].provider_event_ms
            if causal_window_observations
            else None
        ),
        chainlink_max_gap_ms=max(observation_gaps) if observation_gaps else None,
        touch_count=touch_count,
        fresh_cutoff_count=sum(cutoff.chainlink_fresh for cutoff in cutoffs),
        probability_cutoff_count=sum(
            cutoff.probability is not None for cutoff in cutoffs
        ),
        fresh_probability_cutoff_count=sum(
            cutoff.probability_fresh for cutoff in cutoffs
        ),
        microstructure_cutoff_count=sum(
            cutoff.microstructure is not None for cutoff in cutoffs
        ),
        source_microstructure_row_count=len(microstructure_rows),
        first_crossing_ms_before_end=(
            max(crossing_offsets) if crossing_offsets else None
        ),
        last_crossing_ms_before_end=(
            min(crossing_offsets) if crossing_offsets else None
        ),
        decisive_event_sequence=(
            decisive.event_sequence if decisive is not None else None
        ),
        data_quality={
            "official_complete": official_complete,
            "source_reference": (
                twap_identity.source_reference
                if twap_identity is not None
                else "chainlink_spot"
            ),
            "known_source_gap_count": len(source_gap_rows),
            "all_cutoffs_fresh": all_cutoffs_fresh,
            "twap_cutoffs_dense": twap_cutoffs_dense,
            "distinct_cutoff_event_count": len(selected_cutoff_event_times),
            "twap_causal_interval_start_ms": causal_interval_start_ms,
            "twap_causal_interval_end_ms": causal_interval_end_ms,
            "twap_pre_window_event_ms": pre_window_event_ms,
            "twap_post_boundary_event_ms": post_boundary_event_ms,
            "twap_causal_interval_span_ms": causal_interval_span_ms,
            "twap_causal_interval_event_count": len(
                causal_interval_event_times
            ),
            "twap_causal_interval_expected_event_count": (
                expected_causal_interval_event_count
            ),
            "twap_causal_interval_max_gap_ms": (
                max(causal_interval_gaps) if causal_interval_gaps else None
            ),
            "crossing_coverage_max_gap_ms": (
                max(crossing_coverage_gaps)
                if crossing_coverage_gaps
                else None
            ),
            "stale_crossing_count": sum(
                gap_ms > maximum_crossing_gap_ms
                for gap_ms in crossing_coverage_gaps
            ),
            "missing_chainlink_cutoffs": (
                FLIP_WINDOW_SECONDS
                - sum(cutoff.chainlink is not None for cutoff in cutoffs)
            ),
            "stale_chainlink_cutoffs": sum(
                cutoff.chainlink is not None and not cutoff.chainlink_fresh
                for cutoff in cutoffs
            ),
            "missing_probability_cutoffs": (
                FLIP_WINDOW_SECONDS
                - sum(cutoff.probability is not None for cutoff in cutoffs)
            ),
            "missing_microstructure_cutoffs": sum(
                cutoff.microstructure is None for cutoff in cutoffs
            ),
        },
    )


EVALUATION_TABLE = "polymarket_btc_5m_flip_evaluations"
EVENT_TABLE = "polymarket_btc_5m_flip_events"
CUTOFF_TABLE = "polymarket_btc_5m_flip_cutoffs"
MICROSTRUCTURE_ARCHIVE_TABLE = "binance_microstructure_1s_flip_archive"

_MICROSTRUCTURE_ARCHIVE_COLUMNS = (
    "symbol",
    "market_id",
    "sample_second_ms",
    "sample_second_at",
    *MICROSTRUCTURE_VALUE_COLUMNS,
    "received_ms",
    "created_at",
)
_MICROSTRUCTURE_ARCHIVE_COLUMN_SQL = ", ".join(
    _MICROSTRUCTURE_ARCHIVE_COLUMNS
)
_MICROSTRUCTURE_ARCHIVE_UPDATE_SQL = ", ".join(
    f"{column} = EXCLUDED.{column}"
    for column in _MICROSTRUCTURE_ARCHIVE_COLUMNS
    if column not in {"symbol", "sample_second_ms"}
)

_EVALUATION_COLUMNS = (
    "market_id",
    "definition_version",
    "evaluation_status",
    "observation_precision",
    "price_to_beat",
    "official_close_price",
    "official_winner",
    "analysis_start_ms",
    "analysis_end_ms",
    "crossing_count",
    "touch_count",
    "first_crossing_ms_before_end",
    "last_crossing_ms_before_end",
    "decisive_event_sequence",
    "decisive_flip_ms_before_end",
    "decisive_flip_direction",
    "chainlink_observation_count",
    "chainlink_strict_observation_count",
    "chainlink_first_provider_event_ms",
    "chainlink_last_provider_event_ms",
    "chainlink_max_gap_ms",
    "chainlink_cutoff_count",
    "fresh_chainlink_cutoff_count",
    "probability_cutoff_count",
    "fresh_probability_cutoff_count",
    "microstructure_cutoff_count",
    "quality_flags",
    "archive_status",
    "source_microstructure_row_count",
    "archived_microstructure_row_count",
    "retention_safe",
    "evaluated_ms",
    "archived_ms",
    "evaluation_attempts",
    "next_retry_ms",
    "last_error",
)

_EVENT_COLUMNS = (
    "market_id",
    "definition_version",
    "event_sequence",
    "previous_side",
    "new_side",
    "direction",
    "previous_price",
    "new_price",
    "previous_sample_second_ms",
    "sample_second_ms",
    "previous_provider_event_ms",
    "provider_event_ms",
    "previous_received_ms",
    "received_ms",
    "observation_gap_ms",
    "observed_ms_before_end",
    "is_decisive",
    "observation_precision",
)

_CUTOFF_COLUMNS = (
    "market_id",
    "definition_version",
    "seconds_before_end",
    "cutoff_ms",
    "chainlink_sample_second_ms",
    "chainlink_price",
    "chainlink_provider_event_ms",
    "chainlink_received_ms",
    "chainlink_source_age_ms",
    "chainlink_received_age_ms",
    "chainlink_fresh",
    "price_distance",
    "absolute_price_distance",
    "apparent_side",
    "probability_sample_second_ms",
    "probability_provider_event_ms",
    "probability_received_ms",
    "probability_source_age_ms",
    "probability_received_age_ms",
    "up_probability_provider_event_ms",
    "up_probability_received_ms",
    "up_probability_source_age_ms",
    "up_probability_received_age_ms",
    "down_probability_provider_event_ms",
    "down_probability_received_ms",
    "down_probability_source_age_ms",
    "down_probability_received_age_ms",
    "probability_fresh",
    "up_bid",
    "up_ask",
    "up_mid",
    "down_bid",
    "down_ask",
    "down_mid",
    "up_prob_norm",
    "down_prob_norm",
    "official_winner",
    "flipped_after_cutoff",
    "microstructure_symbol",
    "microstructure_sample_second_ms",
    *MICROSTRUCTURE_VALUE_COLUMNS,
    "microstructure_received_ms",
    "microstructure_available",
    "quality_flags",
)


def _insert_do_nothing_sql(
    table: str,
    columns: Sequence[str],
    conflict_columns: Sequence[str],
) -> str:
    names = ", ".join(columns)
    placeholders = ", ".join(f"${index}" for index in range(1, len(columns) + 1))
    conflicts = ", ".join(conflict_columns)
    return (
        f"INSERT INTO {table} ({names}) VALUES ({placeholders}) "
        f"ON CONFLICT ({conflicts}) DO NOTHING"
    )


_INSERT_EVALUATION_SQL = _insert_do_nothing_sql(
    EVALUATION_TABLE,
    _EVALUATION_COLUMNS,
    ("market_id", "definition_version"),
)
_EVALUATION_RETRY_UPDATES = ", ".join(
    f"{column} = EXCLUDED.{column}"
    for column in _EVALUATION_COLUMNS
    if column not in {"market_id", "definition_version"}
)
_INSERT_EVALUATION_SQL = (
    _INSERT_EVALUATION_SQL.removesuffix("DO NOTHING")
    + "DO UPDATE SET "
    + _EVALUATION_RETRY_UPDATES
    + ", updated_at = now() "
    + f"WHERE {EVALUATION_TABLE}.observation_precision = "
    + "'evaluation_failed'"
)
_INSERT_EVENT_SQL = _insert_do_nothing_sql(
    EVENT_TABLE,
    _EVENT_COLUMNS,
    ("market_id", "definition_version", "event_sequence"),
)
_INSERT_CUTOFF_SQL = _insert_do_nothing_sql(
    CUTOFF_TABLE,
    _CUTOFF_COLUMNS,
    ("market_id", "definition_version", "seconds_before_end"),
)


class FlipArchiveVerificationError(RuntimeError):
    pass


def market_rule_supports_flip_definition(
    market: Mapping[str, Any],
    definition_version: int,
) -> bool:
    twap_identity = _twap_identity_for_definition(definition_version)
    if twap_identity is not None:
        market_start_ms = market.get("market_start_ms")
        if market_start_ms is not None:
            starts_after_cutover = int(market_start_ms) >= TWAP_60S_CUTOVER_MS
            if (
                definition_version == TWAP_30S_FLIP_DEFINITION_VERSION
                and starts_after_cutover
            ) or (
                definition_version == TWAP_60S_FLIP_DEFINITION_VERSION
                and not starts_after_cutover
            ):
                return False
        return (
            market.get("settlement_reference") == "chainlink_twap"
            and market.get("settlement_window_s")
            == twap_identity.window_seconds
            and market.get("settlement_source_url") == twap_identity.source_url
            and market.get("settlement_rule_version")
            == twap_identity.rule_version
        )
    if definition_version == SPOT_FLIP_DEFINITION_VERSION:
        return (
            market.get("settlement_reference") == "chainlink_spot"
            and market.get("settlement_rule_version") is not None
        )
    return False


def _quality_flags(analysis: FlipAnalysis) -> list[str]:
    flags: list[str] = []
    observation_name = (
        "twap"
        if _is_twap_definition(analysis.definition_version)
        else "chainlink"
    )
    if analysis.resolution_type != "winner":
        flags.append("official_resolution_not_winner")
    if analysis.threshold is None:
        flags.append("missing_price_to_beat")
    if analysis.official_close is None:
        flags.append("missing_official_close")
    if analysis.winner not in {STRICT_UP, STRICT_DOWN}:
        flags.append("missing_official_winner")
    if analysis.fresh_cutoff_count < FLIP_WINDOW_SECONDS:
        flags.append(f"incomplete_fresh_{observation_name}_cutoffs")
    if (
        _is_twap_definition(analysis.definition_version)
        and not analysis.data_quality.get("twap_cutoffs_dense")
    ):
        flags.append("incomplete_dense_twap_cutoffs")
    if analysis.probability_cutoff_count < FLIP_WINDOW_SECONDS:
        flags.append("missing_probability_cutoffs")
    if analysis.fresh_probability_cutoff_count < FLIP_WINDOW_SECONDS:
        flags.append("stale_probability_cutoffs")
    if analysis.microstructure_cutoff_count < FLIP_WINDOW_SECONDS:
        flags.append("missing_microstructure_cutoffs")
    if analysis.touch_count:
        flags.append("threshold_touch_observed")
    if analysis.data_quality.get("stale_crossing_count"):
        flags.append("stale_crossing_gap")
    if analysis.data_quality.get("known_source_gap_count"):
        flags.append("twap_stream_gap")
    if (
        _is_twap_definition(analysis.definition_version)
        and analysis.chainlink_observation_count == 0
    ):
        flags.append("missing_twap")
    return flags


def _cutoff_quality_flags(
    cutoff: CutoffObservation,
    definition_version: int = FLIP_DEFINITION_VERSION,
) -> list[str]:
    flags: list[str] = []
    observation_name = (
        "twap"
        if _is_twap_definition(definition_version)
        else "chainlink"
    )
    if cutoff.chainlink is None:
        flags.append(f"missing_{observation_name}")
    elif not cutoff.chainlink_fresh:
        flags.append(f"stale_{observation_name}")
    if cutoff.apparent_side == TIE:
        flags.append("threshold_touch")
    if cutoff.probability is None:
        flags.append("missing_probability")
    elif (
        cutoff.probability.up_received_ms is None
        or cutoff.probability.down_received_ms is None
    ):
        flags.append("unknown_probability_component_freshness")
    elif not cutoff.probability_fresh:
        flags.append("stale_probability")
    if cutoff.microstructure is None:
        flags.append("missing_microstructure")
    return flags


def _evaluation_arguments(
    analysis: FlipAnalysis,
    *,
    evaluated_ms: int,
    evaluation_attempts: int,
) -> tuple[Any, ...]:
    decisive = next((event for event in analysis.events if event.decisive), None)
    needs_archive = analysis.needs_archive
    return (
        analysis.market_id,
        analysis.definition_version,
        analysis.evaluation_status,
        (
            "exact_twap_event"
            if _is_twap_definition(analysis.definition_version)
            else "one_second_summary"
        ),
        analysis.threshold,
        analysis.official_close,
        analysis.winner,
        analysis.market_end_ms - FLIP_WINDOW_SECONDS * 1000,
        analysis.market_end_ms,
        analysis.crossing_count,
        analysis.touch_count,
        analysis.first_crossing_ms_before_end,
        analysis.last_crossing_ms_before_end,
        analysis.decisive_event_sequence,
        (
            decisive.milliseconds_before_expiry
            if decisive is not None
            else None
        ),
        decisive.direction if decisive is not None else None,
        analysis.chainlink_observation_count,
        analysis.chainlink_strict_observation_count,
        analysis.chainlink_first_provider_event_ms,
        analysis.chainlink_last_provider_event_ms,
        analysis.chainlink_max_gap_ms,
        sum(cutoff.chainlink is not None for cutoff in analysis.cutoffs),
        analysis.fresh_cutoff_count,
        analysis.probability_cutoff_count,
        analysis.fresh_probability_cutoff_count,
        analysis.microstructure_cutoff_count,
        _quality_flags(analysis),
        ARCHIVE_PENDING if needs_archive else ARCHIVE_NOT_REQUIRED,
        analysis.source_microstructure_row_count,
        0,
        not needs_archive,
        evaluated_ms,
        None,
        max(1, int(evaluation_attempts)),
        None,
        None,
    )


def _event_arguments(
    analysis: FlipAnalysis,
    event: FlipEvent,
) -> tuple[Any, ...]:
    return (
        analysis.market_id,
        analysis.definition_version,
        event.event_sequence,
        event.previous_side,
        event.new_side,
        event.direction,
        event.previous_price,
        event.new_price,
        event.previous_sample_second_ms,
        event.sample_second_ms,
        event.previous_provider_event_ms,
        event.provider_event_ms,
        event.previous_received_ms,
        event.received_ms,
        event.observation_gap_ms,
        event.milliseconds_before_expiry,
        event.decisive,
        event.observation_precision,
    )


def _cutoff_arguments(
    analysis: FlipAnalysis,
    cutoff: CutoffObservation,
) -> tuple[Any, ...]:
    chainlink = cutoff.chainlink
    probability = cutoff.probability
    microstructure = cutoff.microstructure
    microstructure_values = (
        tuple(microstructure.get(column) for column in MICROSTRUCTURE_VALUE_COLUMNS)
        if microstructure is not None
        else (None,) * len(MICROSTRUCTURE_VALUE_COLUMNS)
    )
    return (
        analysis.market_id,
        analysis.definition_version,
        cutoff.seconds_before_end,
        cutoff.cutoff_ms,
        chainlink.sample_second_ms if chainlink is not None else None,
        chainlink.price if chainlink is not None else None,
        chainlink.provider_event_ms if chainlink is not None else None,
        chainlink.received_ms if chainlink is not None else None,
        cutoff.chainlink_source_age_ms,
        cutoff.chainlink_receive_age_ms,
        cutoff.chainlink_fresh,
        cutoff.signed_distance,
        cutoff.absolute_distance,
        cutoff.apparent_side,
        probability.sample_second_ms if probability is not None else None,
        probability.provider_event_ms if probability is not None else None,
        probability.received_ms if probability is not None else None,
        cutoff.probability_source_age_ms,
        cutoff.probability_receive_age_ms,
        probability.up_provider_event_ms if probability is not None else None,
        probability.up_received_ms if probability is not None else None,
        cutoff.up_probability_source_age_ms,
        cutoff.up_probability_receive_age_ms,
        (
            probability.down_provider_event_ms
            if probability is not None
            else None
        ),
        probability.down_received_ms if probability is not None else None,
        cutoff.down_probability_source_age_ms,
        cutoff.down_probability_receive_age_ms,
        cutoff.probability_fresh,
        probability.up_bid if probability is not None else None,
        probability.up_ask if probability is not None else None,
        probability.up_mid if probability is not None else None,
        probability.down_bid if probability is not None else None,
        probability.down_ask if probability is not None else None,
        probability.down_mid if probability is not None else None,
        probability.up_prob_norm if probability is not None else None,
        probability.down_prob_norm if probability is not None else None,
        analysis.winner,
        cutoff.flipped_after_cutoff,
        microstructure.get("symbol") if microstructure is not None else None,
        (
            microstructure.get("sample_second_ms")
            if microstructure is not None
            else None
        ),
        *microstructure_values,
        (
            microstructure.get("received_ms")
            if microstructure is not None
            else None
        ),
        microstructure is not None,
        _cutoff_quality_flags(cutoff, analysis.definition_version),
    )


async def fetch_due_flip_markets(
    pool: Any,
    *,
    now_ms: int,
    definition_version: int = FLIP_DEFINITION_VERSION,
    finalization_grace_ms: int = FLIP_FINALIZATION_GRACE_MS,
    limit: int = FLIP_EVALUATOR_BATCH_SIZE,
) -> list[dict[str, Any]]:
    """Return complete official resolutions needing evaluation or archival."""

    twap_identity = _twap_identity_for_definition(definition_version)
    if twap_identity is None:
        if definition_version != SPOT_FLIP_DEFINITION_VERSION:
            raise ValueError(
                f"unsupported flip definition version: {definition_version}"
            )
        # The lateral TWAP watermark is irrelevant to the spot definition but
        # remains type-stable in the shared query.
        twap_identity = _TWAP_IDENTITIES[TWAP_FLIP_DEFINITION_VERSION]

    async with pool.acquire() as connection:
        rows = await connection.fetch(
            f"""
            SELECT
                r.market_id,
                mw.market_start_ms,
                mw.market_end_ms,
                r.resolution_type,
                r.chainlink_open_price,
                r.chainlink_close_price,
                r.winner,
                pm.settlement_reference,
                pm.settlement_window_s,
                pm.settlement_source_url,
                pm.settlement_rule_version,
                twap_watermark.provider_event_ms
                    AS twap_persistence_watermark_event_ms,
                evaluation.evaluation_status AS existing_evaluation_status,
                evaluation.observation_precision
                    AS existing_observation_precision,
                evaluation.archive_status AS existing_archive_status,
                evaluation.retention_safe AS existing_retention_safe,
                COALESCE(evaluation.evaluation_attempts, 0)
                    AS evaluation_attempts,
                evaluation.next_retry_ms
            FROM polymarket_btc_5m_resolutions r
            JOIN polymarket_btc_5m_markets pm ON pm.market_id = r.market_id
            JOIN market_windows mw ON mw.market_id = r.market_id
            LEFT JOIN {EVALUATION_TABLE} evaluation
              ON evaluation.market_id = r.market_id
             AND evaluation.definition_version = $2
            LEFT JOIN LATERAL (
                SELECT event.provider_event_ms
                FROM polymarket_twap_events event
                WHERE event.topic = $5::TEXT
                  AND event.symbol = 'btc/usd'
                  AND event.window_s = $6::SMALLINT
                  AND event.provider_event_ms > mw.market_end_ms
                ORDER BY
                    event.provider_event_ms ASC,
                    event.received_wall_ns ASC
                LIMIT 1
            ) twap_watermark ON TRUE
            WHERE r.resolution_status = 'resolved'
              AND r.resolution_type IS NOT NULL
              AND r.chainlink_open_price IS NOT NULL
              AND r.chainlink_close_price IS NOT NULL
              AND (
                    (
                        $2::SMALLINT IN (
                            {TWAP_30S_FLIP_DEFINITION_VERSION},
                            {TWAP_60S_FLIP_DEFINITION_VERSION}
                        )
                        AND pm.settlement_reference = 'chainlink_twap'
                        AND pm.settlement_window_s = $6::SMALLINT
                        AND pm.settlement_source_url = $7::TEXT
                        AND pm.settlement_rule_version = $8::TEXT
                        AND r.reconciled_settlement_rule_version =
                            pm.settlement_rule_version
                        AND (
                            (
                                $2::SMALLINT =
                                    {TWAP_30S_FLIP_DEFINITION_VERSION}
                                AND mw.market_start_ms <
                                    {TWAP_60S_CUTOVER_MS}
                            )
                            OR (
                                $2::SMALLINT =
                                    {TWAP_60S_FLIP_DEFINITION_VERSION}
                                AND mw.market_start_ms >=
                                    {TWAP_60S_CUTOVER_MS}
                            )
                        )
                        AND (
                            twap_watermark.provider_event_ms > mw.market_end_ms
                            OR (
                                $2::SMALLINT =
                                    {TWAP_30S_FLIP_DEFINITION_VERSION}
                                AND $1::BIGINT >= {TWAP_60S_CUTOVER_MS}
                                AND mw.market_end_ms =
                                    {TWAP_60S_CUTOVER_MS}
                            )
                        )
                    )
                    OR (
                        $2::SMALLINT = {SPOT_FLIP_DEFINITION_VERSION}
                        AND pm.settlement_reference = 'chainlink_spot'
                        AND pm.settlement_rule_version IS NOT NULL
                    )
              )
              AND mw.market_end_ms <= $1::BIGINT - $3::BIGINT
              AND (
                    evaluation.market_id IS NULL
                    OR (
                        evaluation.retention_safe = FALSE
                        AND evaluation.archive_status IN ('pending', 'failed')
                        AND (
                            evaluation.next_retry_ms IS NULL
                            OR evaluation.next_retry_ms <= $1
                        )
                    )
                    OR (
                        evaluation.archive_status = 'complete'
                        AND EXISTS (
                            SELECT 1
                            FROM binance_microstructure_1s live_row
                            LEFT JOIN {MICROSTRUCTURE_ARCHIVE_TABLE} archived_row
                              ON archived_row.symbol = live_row.symbol
                             AND archived_row.sample_second_ms =
                                    live_row.sample_second_ms
                            WHERE live_row.market_id = r.market_id
                              AND (
                                    archived_row.sample_second_ms IS NULL
                                    OR archived_row.received_ms <
                                        live_row.received_ms
                              )
                        )
                    )
              )
            ORDER BY mw.market_end_ms ASC, r.market_id ASC
            LIMIT $4
            """,
            now_ms,
            definition_version,
            max(0, int(finalization_grace_ms)),
            max(1, int(limit)),
            twap_identity.topic,
            twap_identity.window_seconds,
            twap_identity.source_url,
            twap_identity.rule_version,
        )
    return [dict(row) for row in rows]


async def _load_market_inputs(
    connection: Any,
    *,
    market_id: int,
    definition_version: int = FLIP_DEFINITION_VERSION,
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    twap_identity = _twap_identity_for_definition(definition_version)
    if twap_identity is not None:
        # Deliberately source-time global: an event at the exact ending
        # boundary is keyed to the following market in its materialized row,
        # but still brackets this market's close and must remain queryable.
        chainlink_rows = await connection.fetch(
            """
            SELECT
                event.sample_second_ms,
                event.price,
                event.provider_event_ms,
                event.provider_message_ms,
                event.received_wall_ns,
                (event.received_wall_ns / 1000000)::BIGINT AS received_ms
            FROM polymarket_twap_events event
            JOIN market_windows target ON target.market_id = $1
            WHERE event.topic = $2::TEXT
              AND event.symbol = 'btc/usd'
              AND event.window_s = $3::SMALLINT
              AND event.provider_event_ms >= target.market_start_ms
              AND event.provider_event_ms <= COALESCE(
                    (
                        SELECT MIN(watermark.provider_event_ms)
                        FROM polymarket_twap_events watermark
                        WHERE watermark.topic = $2::TEXT
                          AND watermark.symbol = 'btc/usd'
                          AND watermark.window_s = $3::SMALLINT
                          AND watermark.provider_event_ms >
                                target.market_end_ms
                    ),
                    target.market_end_ms
              )
            ORDER BY
                event.provider_event_ms ASC,
                event.received_wall_ns ASC,
                event.connection_id ASC,
                event.receive_sequence ASC
            """,
            market_id,
            twap_identity.topic,
            twap_identity.window_seconds,
        )
        source_gap_rows = await connection.fetch(
            """
            WITH target AS (
                SELECT market_start_ms, market_end_ms
                FROM market_windows
                WHERE market_id = $1
            ),
            feed_sessions AS (
                SELECT
                    session.*,
                    ROW_NUMBER() OVER (
                        ORDER BY
                            session.created_at ASC,
                            session.connected_wall_ns ASC,
                            session.connection_id ASC
                    ) AS session_order
                FROM polymarket_twap_sessions session
                WHERE session.topic = $2::TEXT
                  AND session.symbol = 'btc/usd'
                  AND session.window_s = $3::SMALLINT
            ),
            session_event_bounds AS (
                SELECT
                    session.*,
                    last_event.provider_event_ms
                        AS last_event_provider_ms,
                    last_event.received_wall_ns
                        AS last_event_received_wall_ns
                FROM feed_sessions session
                LEFT JOIN LATERAL (
                    SELECT
                        event.provider_event_ms,
                        event.received_wall_ns
                    FROM polymarket_twap_events event
                    WHERE event.connection_id = session.connection_id
                      AND event.topic = $2::TEXT
                      AND event.symbol = 'btc/usd'
                      AND event.window_s = $3::SMALLINT
                    ORDER BY event.receive_sequence DESC
                    LIMIT 1
                ) last_event ON TRUE
            ),
            session_recoveries AS (
                SELECT
                    current.connection_id,
                    next_session.connection_id AS next_connection_id,
                    next_session.subscribed_wall_ns
                        AS next_subscribed_wall_ns,
                    recovery.provider_event_ms
                        AS recovery_provider_event_ms,
                    recovery.received_wall_ns
                        AS recovery_received_wall_ns
                FROM session_event_bounds current
                LEFT JOIN LATERAL (
                    SELECT future.connection_id, future.subscribed_wall_ns
                    FROM feed_sessions future
                    WHERE future.session_order > current.session_order
                    ORDER BY future.session_order ASC
                    LIMIT 1
                ) next_session ON TRUE
                LEFT JOIN LATERAL (
                    SELECT
                        event.provider_event_ms,
                        event.received_wall_ns
                    FROM feed_sessions future
                    JOIN polymarket_twap_events event
                      ON event.connection_id = future.connection_id
                     AND event.topic = $2::TEXT
                     AND event.symbol = 'btc/usd'
                     AND event.window_s = $3::SMALLINT
                    WHERE future.session_order > current.session_order
                    ORDER BY
                        future.session_order ASC,
                        event.receive_sequence ASC
                    LIMIT 1
                ) recovery ON TRUE
            ),
            explicit_intervals AS (
                SELECT
                    gap.connection_id,
                    recovery.next_connection_id,
                    'explicit_gap'::TEXT AS interval_kind,
                    gap.reason,
                    COALESCE(
                        gap.last_provider_event_ms,
                        session.last_event_provider_ms
                    ) AS gap_start_provider_ms,
                    recovery.recovery_provider_event_ms
                        AS gap_end_provider_ms,
                    COALESCE(
                        session.last_event_received_wall_ns,
                        gap.last_accepted_received_ms * 1000000,
                        session.subscribed_wall_ns
                    ) AS gap_start_wall_ns,
                    recovery.recovery_received_wall_ns AS gap_end_wall_ns,
                    gap.detected_wall_ns,
                    recovery.next_subscribed_wall_ns
                FROM polymarket_twap_gaps gap
                JOIN session_event_bounds session
                  ON session.connection_id = gap.connection_id
                JOIN session_recoveries recovery
                  ON recovery.connection_id = gap.connection_id
            ),
            session_transition_intervals AS (
                SELECT
                    session.connection_id,
                    recovery.next_connection_id,
                    CASE
                        WHEN session.disconnected_wall_ns IS NULL
                            THEN 'orphan_session_reconnect'
                        WHEN session.close_reason = 'cancelled'
                            THEN 'cancelled_session_reconnect'
                        ELSE 'session_reconnect'
                    END AS interval_kind,
                    COALESCE(
                        session.close_reason,
                        'missing_session_finish'
                    ) AS reason,
                    COALESCE(
                        session.last_event_provider_ms,
                        session.last_provider_event_ms
                    ) AS gap_start_provider_ms,
                    recovery.recovery_provider_event_ms
                        AS gap_end_provider_ms,
                    COALESCE(
                        session.last_event_received_wall_ns,
                        session.last_accepted_received_ms * 1000000,
                        session.subscribed_wall_ns
                    ) AS gap_start_wall_ns,
                    recovery.recovery_received_wall_ns AS gap_end_wall_ns,
                    session.disconnected_wall_ns AS detected_wall_ns,
                    recovery.next_subscribed_wall_ns
                FROM session_event_bounds session
                JOIN session_recoveries recovery
                  ON recovery.connection_id = session.connection_id
                WHERE recovery.next_connection_id IS NOT NULL
                  AND NOT EXISTS (
                        SELECT 1
                        FROM polymarket_twap_gaps explicit_gap
                        WHERE explicit_gap.connection_id =
                            session.connection_id
                  )
            ),
            intervals AS (
                SELECT * FROM explicit_intervals
                UNION ALL
                SELECT * FROM session_transition_intervals
            ),
            interval_basis AS (
                SELECT
                    interval.*,
                    CASE
                        WHEN interval.gap_start_provider_ms IS NOT NULL
                         AND (
                                interval.gap_end_provider_ms >
                                    interval.gap_start_provider_ms
                                OR (
                                    interval.gap_end_provider_ms IS NULL
                                    AND interval.gap_end_wall_ns IS NULL
                                )
                             )
                            THEN 'provider'
                        ELSE 'wall'
                    END AS time_basis
                FROM intervals interval
            ),
            normalized_intervals AS (
                SELECT
                    interval.*,
                    CASE interval.time_basis
                        WHEN 'provider' THEN
                            interval.gap_start_provider_ms
                        WHEN 'wall' THEN
                            CASE
                                WHEN interval.gap_end_wall_ns IS NULL THEN
                                    interval.gap_start_wall_ns / 1000000
                                ELSE LEAST(
                                    interval.gap_start_wall_ns,
                                    interval.gap_end_wall_ns
                                ) / 1000000
                            END
                    END::BIGINT AS gap_start_ms,
                    CASE interval.time_basis
                        WHEN 'provider' THEN
                            interval.gap_end_provider_ms
                        WHEN 'wall' THEN
                            CASE
                                WHEN interval.gap_end_wall_ns IS NULL THEN NULL
                                ELSE GREATEST(
                                    interval.gap_start_wall_ns,
                                    interval.gap_end_wall_ns
                                ) / 1000000
                            END
                    END::BIGINT AS gap_end_ms
                FROM interval_basis interval
            )
            SELECT
                interval.connection_id,
                interval.next_connection_id,
                interval.interval_kind,
                interval.reason,
                interval.gap_start_ms,
                interval.gap_end_ms,
                interval.gap_start_provider_ms,
                interval.gap_end_provider_ms,
                interval.gap_start_wall_ns,
                interval.gap_end_wall_ns,
                interval.detected_wall_ns,
                interval.next_subscribed_wall_ns,
                interval.time_basis
            FROM normalized_intervals interval
            CROSS JOIN target
            WHERE (
                    interval.gap_start_ms < target.market_end_ms
                    AND COALESCE(
                        interval.gap_end_ms,
                        9223372036854775807::BIGINT
                    ) > target.market_end_ms - 20000
                  )
            ORDER BY
                interval.gap_start_ms ASC,
                interval.connection_id ASC
            """,
            market_id,
            twap_identity.topic,
            twap_identity.window_seconds,
        )
    elif definition_version == SPOT_FLIP_DEFINITION_VERSION:
        chainlink_rows = await connection.fetch(
            """
            SELECT
                ps.sample_second_ms,
                ps.price,
                ps.provider_event_ms,
                ps.provider_message_ms,
                ps.received_ms
            FROM price_samples ps
            JOIN instruments instrument
              ON instrument.instrument_id = ps.instrument_id
            JOIN providers provider
              ON provider.provider_id = instrument.provider_id
            WHERE ps.market_id = $1
              AND provider.provider_code = 'polymarket_chainlink_rtds'
              AND instrument.symbol = 'BTCUSD'
              AND ps.provider_event_ms IS NOT NULL
            ORDER BY
                ps.provider_event_ms ASC,
                ps.sample_second_ms ASC,
                ps.received_ms ASC
            """,
            market_id,
        )
        source_gap_rows = []
    else:
        raise ValueError(f"unsupported flip definition version: {definition_version}")
    probability_rows = await connection.fetch(
        """
        SELECT
            sample_second_ms,
            provider_event_ms,
            received_ms,
            up_bid,
            up_ask,
            up_mid,
            down_bid,
            down_ask,
            down_mid,
            up_prob_norm,
            down_prob_norm,
            up_provider_event_ms,
            up_received_ms,
            down_provider_event_ms,
            down_received_ms
        FROM polymarket_probability_samples
        WHERE market_id = $1
          AND source = 'polymarket_clob'
        ORDER BY sample_second_ms ASC
        """,
        market_id,
    )
    microstructure_rows = await connection.fetch(
        """
        SELECT *
        FROM binance_microstructure_1s
        WHERE market_id = $1
        ORDER BY sample_second_ms ASC
        """,
        market_id,
    )
    source_gaps = [dict(row) for row in source_gap_rows]
    if twap_identity is not None:
        normalized_source_gaps: list[dict[str, Any]] = []
        for row in source_gaps:
            gap_start_ms, gap_end_ms, time_basis = normalize_gap_interval_ms(
                gap_start_provider_ms=(
                    int(row["gap_start_provider_ms"])
                    if row.get("gap_start_provider_ms") is not None
                    else None
                ),
                gap_end_provider_ms=(
                    int(row["gap_end_provider_ms"])
                    if row.get("gap_end_provider_ms") is not None
                    else None
                ),
                gap_start_wall_ns=int(row["gap_start_wall_ns"]),
                gap_end_wall_ns=(
                    int(row["gap_end_wall_ns"])
                    if row.get("gap_end_wall_ns") is not None
                    else None
                ),
            )
            row["gap_start_ms"] = gap_start_ms
            row["gap_end_ms"] = gap_end_ms
            row["time_basis"] = time_basis
            if gap_interval_overlaps_final_window(
                gap_start_ms=gap_start_ms,
                gap_end_ms=gap_end_ms,
                market_end_ms=(market_id + 1) * 300_000,
            ):
                normalized_source_gaps.append(row)
        source_gaps = normalized_source_gaps
        market_end_ms = (market_id + 1) * 300_000
        if (
            definition_version == TWAP_30S_FLIP_DEFINITION_VERSION
            and market_end_ms == TWAP_60S_CUTOVER_MS
            and not any(
                int(row["provider_event_ms"]) > market_end_ms
                for row in chainlink_rows
            )
        ):
            # The legacy settlement rule ended at the cutover. Without a
            # persisted post-boundary legacy frame, evaluate and archive the
            # affected market conservatively instead of leaving it pending
            # forever or treating incomplete terminal evidence as a non-flip.
            source_gaps.append(
                {
                    "gap_start_ms": market_end_ms,
                    "gap_end_ms": None,
                    "time_basis": "settlement_rule_cutover",
                }
            )
    return (
        [dict(row) for row in chainlink_rows],
        [dict(row) for row in probability_rows],
        [dict(row) for row in microstructure_rows],
        source_gaps,
    )


async def _persist_new_analysis(
    connection: Any,
    analysis: FlipAnalysis,
    *,
    evaluated_ms: int,
    evaluation_attempts: int,
) -> None:
    await connection.execute(
        _INSERT_EVALUATION_SQL,
        *_evaluation_arguments(
            analysis,
            evaluated_ms=evaluated_ms,
            evaluation_attempts=evaluation_attempts,
        ),
    )
    for event in analysis.events:
        await connection.execute(
            _INSERT_EVENT_SQL,
            *_event_arguments(analysis, event),
        )
    for cutoff in analysis.cutoffs:
        await connection.execute(
            _INSERT_CUTOFF_SQL,
            *_cutoff_arguments(analysis, cutoff),
        )


def flip_retry_delay_ms(
    attempt: int,
    *,
    base_seconds: int = FLIP_EVALUATOR_POLL_SECONDS,
    max_seconds: int = FLIP_EVALUATOR_MAX_BACKOFF_SECONDS,
) -> int:
    exponent = min(max(0, int(attempt) - 1), 16)
    seconds = min(
        max(1, int(max_seconds)),
        max(1, int(base_seconds)) * (2**exponent),
    )
    return seconds * 1000


async def _mark_evaluation_failed(
    pool: Any,
    market: Mapping[str, Any],
    *,
    definition_version: int,
    failed_ms: int,
    error: BaseException,
    base_retry_seconds: int,
    max_retry_seconds: int,
) -> None:
    """Persist pre-analysis failure state so one bad market cannot starve work."""

    market_id = int(market["market_id"])
    attempts = int(market.get("evaluation_attempts") or 0) + 1
    retry_ms = failed_ms + flip_retry_delay_ms(
        attempts,
        base_seconds=base_retry_seconds,
        max_seconds=max_retry_seconds,
    )
    winner = market.get("winner")
    official_winner = (
        str(winner) if winner in {STRICT_UP, STRICT_DOWN} else None
    )
    async with pool.acquire() as connection:
        await connection.execute(
            f"""
            INSERT INTO {EVALUATION_TABLE} (
                market_id,
                definition_version,
                evaluation_status,
                observation_precision,
                price_to_beat,
                official_close_price,
                official_winner,
                analysis_start_ms,
                analysis_end_ms,
                quality_flags,
                archive_status,
                retention_safe,
                evaluated_ms,
                evaluation_attempts,
                next_retry_ms,
                last_error
            )
            VALUES (
                $1,
                $2,
                'ambiguous',
                'evaluation_failed',
                $3,
                $4,
                $5,
                $6,
                $7,
                ARRAY['evaluation_failed']::TEXT[],
                'failed',
                FALSE,
                $8,
                $9,
                $10,
                $11
            )
            ON CONFLICT (market_id, definition_version)
            DO UPDATE SET
                price_to_beat = EXCLUDED.price_to_beat,
                official_close_price = EXCLUDED.official_close_price,
                official_winner = EXCLUDED.official_winner,
                analysis_start_ms = EXCLUDED.analysis_start_ms,
                analysis_end_ms = EXCLUDED.analysis_end_ms,
                quality_flags = EXCLUDED.quality_flags,
                archive_status = 'failed',
                retention_safe = FALSE,
                evaluated_ms = EXCLUDED.evaluated_ms,
                evaluation_attempts = EXCLUDED.evaluation_attempts,
                next_retry_ms = EXCLUDED.next_retry_ms,
                last_error = EXCLUDED.last_error,
                updated_at = now()
            WHERE {EVALUATION_TABLE}.observation_precision =
                    'evaluation_failed'
            """,
            market_id,
            definition_version,
            market.get("chainlink_open_price"),
            market.get("chainlink_close_price"),
            official_winner,
            int(market["market_end_ms"]) - FLIP_WINDOW_SECONDS * 1000,
            int(market["market_end_ms"]),
            failed_ms,
            attempts,
            retry_ms,
            repr(error)[:4000],
        )


async def _mark_archive_failed(
    pool: Any,
    *,
    market_id: int,
    definition_version: int,
    failed_ms: int,
    error: BaseException,
    base_retry_seconds: int,
    max_retry_seconds: int,
) -> None:
    async with pool.acquire() as connection:
        await connection.execute(
            f"""
            UPDATE {EVALUATION_TABLE}
            SET archive_status = 'failed',
                retention_safe = FALSE,
                evaluation_attempts = evaluation_attempts + 1,
                next_retry_ms = $3::BIGINT + (
                    LEAST(
                        $4::BIGINT,
                        $5::BIGINT
                            * (2 ^ LEAST(evaluation_attempts, 16))::BIGINT
                    ) * 1000
                ),
                last_error = $6,
                updated_at = now()
            WHERE market_id = $1
              AND definition_version = $2
            """,
            market_id,
            definition_version,
            failed_ms,
            max(1, int(max_retry_seconds)),
            max(1, int(base_retry_seconds)),
            repr(error)[:4000],
        )


async def archive_flip_microstructure(
    pool: Any,
    *,
    market_id: int,
    definition_version: int = FLIP_DEFINITION_VERSION,
    archived_ms: Optional[int] = None,
) -> tuple[int, int]:
    """Copy and verify one market before making it retention-safe."""

    completed_ms = _now_ms() if archived_ms is None else archived_ms
    async with pool.acquire() as connection:
        async with connection.transaction():
            recorded_source_count = await connection.fetchval(
                f"""
                SELECT source_microstructure_row_count
                FROM {EVALUATION_TABLE}
                WHERE market_id = $1
                  AND definition_version = $2
                FOR UPDATE
                """,
                market_id,
                definition_version,
            )
            if recorded_source_count is None:
                raise FlipArchiveVerificationError(
                    "flip evaluation disappeared before archival"
                )
            await connection.execute(
                f"""
                INSERT INTO {MICROSTRUCTURE_ARCHIVE_TABLE} (
                    {_MICROSTRUCTURE_ARCHIVE_COLUMN_SQL}
                )
                SELECT {_MICROSTRUCTURE_ARCHIVE_COLUMN_SQL}
                FROM binance_microstructure_1s
                WHERE market_id = $1
                ON CONFLICT (symbol, sample_second_ms)
                DO UPDATE SET
                    {_MICROSTRUCTURE_ARCHIVE_UPDATE_SQL}
                WHERE EXCLUDED.received_ms >=
                    {MICROSTRUCTURE_ARCHIVE_TABLE}.received_ms
                """,
                market_id,
            )
            archive_count = int(
                await connection.fetchval(
                    f"""
                    SELECT count(*)
                    FROM {MICROSTRUCTURE_ARCHIVE_TABLE}
                    WHERE market_id = $1
                    """,
                    market_id,
                )
            )
            unsynced_live_count = int(
                await connection.fetchval(
                    f"""
                    SELECT count(*)
                    FROM binance_microstructure_1s live_row
                    LEFT JOIN {MICROSTRUCTURE_ARCHIVE_TABLE} archived_row
                      ON archived_row.symbol = live_row.symbol
                     AND archived_row.sample_second_ms =
                            live_row.sample_second_ms
                    WHERE live_row.market_id = $1
                      AND (
                            archived_row.sample_second_ms IS NULL
                            OR archived_row.received_ms < live_row.received_ms
                      )
                    """,
                    market_id,
                )
            )
            if (
                archive_count < int(recorded_source_count)
                or unsynced_live_count != 0
            ):
                raise FlipArchiveVerificationError(
                    "microstructure archive verification failed: "
                    f"recorded_source={recorded_source_count}, "
                    f"archive={archive_count}, "
                    f"unsynced_live={unsynced_live_count}"
                )
            source_count = archive_count
            result = await connection.execute(
                f"""
                UPDATE {EVALUATION_TABLE}
                SET archive_status = 'complete',
                    source_microstructure_row_count = $3,
                    archived_microstructure_row_count = $4,
                    retention_safe = TRUE,
                    archived_ms = $5,
                    next_retry_ms = NULL,
                    last_error = NULL,
                    updated_at = now()
                WHERE market_id = $1
                  AND definition_version = $2
                  AND archive_status IN ('pending', 'failed', 'complete')
                """,
                market_id,
                definition_version,
                source_count,
                archive_count,
                completed_ms,
            )
            if result == "UPDATE 0":
                raise FlipArchiveVerificationError(
                    "flip evaluation disappeared before archive completion"
                )
    LOGGER.info(
        "polymarket_flip_archive_completed",
        extra={
            "event": "polymarket_flip_archive_completed",
            "market_id": market_id,
            "definition_version": definition_version,
            "source_microstructure_row_count": source_count,
            "archived_microstructure_row_count": archive_count,
            "retention_safe": True,
        },
    )
    return source_count, archive_count


async def evaluate_flip_market(
    pool: Any,
    market: Mapping[str, Any],
    *,
    definition_version: int = FLIP_DEFINITION_VERSION,
    now_ms: Optional[int] = None,
    base_retry_seconds: int = FLIP_EVALUATOR_POLL_SECONDS,
    max_retry_seconds: int = FLIP_EVALUATOR_MAX_BACKOFF_SECONDS,
) -> bool:
    """Evaluate or resume archival for one durable due-market record."""

    evaluated_ms = _now_ms() if now_ms is None else now_ms
    market_id = int(market["market_id"])
    if not market_rule_supports_flip_definition(market, definition_version):
        raise ValueError(
            "market settlement rule is unknown or incompatible with "
            f"flip definition v{definition_version}"
        )
    existing_archive_status = market.get("existing_archive_status")
    existing_evaluation_failed = (
        market.get("existing_observation_precision") == "evaluation_failed"
    )
    if (
        not existing_evaluation_failed
        and existing_archive_status
        in {
            ARCHIVE_PENDING,
            ARCHIVE_FAILED,
            ARCHIVE_COMPLETE,
        }
    ):
        try:
            await archive_flip_microstructure(
                pool,
                market_id=market_id,
                definition_version=definition_version,
                archived_ms=evaluated_ms,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await _mark_archive_failed(
                pool,
                market_id=market_id,
                definition_version=definition_version,
                failed_ms=evaluated_ms,
                error=exc,
                base_retry_seconds=base_retry_seconds,
                max_retry_seconds=max_retry_seconds,
            )
            LOGGER.warning(
                "polymarket_flip_archive_failed",
                extra={
                    "event": "polymarket_flip_archive_failed",
                    "market_id": market_id,
                    "definition_version": definition_version,
                    "error": repr(exc),
                },
            )
            return False
        return True

    evaluation_attempts = int(market.get("evaluation_attempts") or 0) + 1
    async with pool.acquire() as connection:
        async with connection.transaction():
            (
                chainlink_rows,
                probability_rows,
                microstructure_rows,
                source_gap_rows,
            ) = (
                await _load_market_inputs(
                    connection,
                    market_id=market_id,
                    definition_version=definition_version,
                )
            )
            analysis = analyze_market(
                market_id=market_id,
                market_start_ms=int(market["market_start_ms"]),
                market_end_ms=int(market["market_end_ms"]),
                resolution_type=(
                    str(market["resolution_type"])
                    if market.get("resolution_type") is not None
                    else None
                ),
                threshold=_require_decimal_or_none(
                    market.get("chainlink_open_price"),
                    "chainlink_open_price",
                ),
                official_close=_require_decimal_or_none(
                    market.get("chainlink_close_price"),
                    "chainlink_close_price",
                ),
                winner=(
                    str(market["winner"])
                    if market.get("winner") is not None
                    else None
                ),
                chainlink_rows=chainlink_rows,
                probability_rows=probability_rows,
                microstructure_rows=microstructure_rows,
                source_gap_rows=source_gap_rows,
                definition_version=definition_version,
            )
            await _persist_new_analysis(
                connection,
                analysis,
                evaluated_ms=evaluated_ms,
                evaluation_attempts=evaluation_attempts,
            )

    LOGGER.info(
        "polymarket_flip_evaluation_persisted",
        extra={
            "event": "polymarket_flip_evaluation_persisted",
            "market_id": market_id,
            "definition_version": definition_version,
            "evaluation_status": analysis.evaluation_status,
            "crossing_count": analysis.crossing_count,
            "fresh_chainlink_cutoff_count": analysis.fresh_cutoff_count,
            "archive_status": (
                ARCHIVE_PENDING
                if analysis.needs_archive
                else ARCHIVE_NOT_REQUIRED
            ),
            "source_microstructure_row_count": (
                analysis.source_microstructure_row_count
            ),
        },
    )

    if not analysis.needs_archive:
        return True

    try:
        await archive_flip_microstructure(
            pool,
            market_id=market_id,
            definition_version=definition_version,
            archived_ms=evaluated_ms,
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        await _mark_archive_failed(
            pool,
            market_id=market_id,
            definition_version=definition_version,
            failed_ms=evaluated_ms,
            error=exc,
            base_retry_seconds=base_retry_seconds,
            max_retry_seconds=max_retry_seconds,
        )
        LOGGER.warning(
            "polymarket_flip_archive_failed",
            extra={
                "event": "polymarket_flip_archive_failed",
                "market_id": market_id,
                "definition_version": definition_version,
                "error": repr(exc),
            },
        )
        return False
    return True


async def _evaluate_due_flip_definition_once(
    settings: Any,
    pool: Any,
    *,
    definition_version: int,
    now_ms: Optional[int] = None,
) -> int:
    current_ms = _now_ms() if now_ms is None else now_ms
    batch_size = max(
        1,
        int(
            getattr(
                settings,
                "POLYMARKET_FLIP_EVALUATOR_BATCH_SIZE",
                getattr(
                    settings,
                    "POLYMARKET_RESOLUTION_BATCH_SIZE",
                    FLIP_EVALUATOR_BATCH_SIZE,
                ),
            )
        ),
    )
    markets = await fetch_due_flip_markets(
        pool,
        now_ms=current_ms,
        definition_version=definition_version,
        finalization_grace_ms=FLIP_FINALIZATION_GRACE_MS,
        limit=batch_size,
    )
    completed = 0
    for market in markets:
        try:
            succeeded = await evaluate_flip_market(
                pool,
                market,
                definition_version=definition_version,
                now_ms=current_ms,
                base_retry_seconds=max(
                    1,
                    int(
                        getattr(
                            settings,
                            "POLYMARKET_RESOLUTION_POLL_SECONDS",
                            FLIP_EVALUATOR_POLL_SECONDS,
                        )
                    ),
                ),
                max_retry_seconds=max(
                    1,
                    int(
                        getattr(
                            settings,
                            "POLYMARKET_RESOLUTION_MAX_BACKOFF_SECONDS",
                            FLIP_EVALUATOR_MAX_BACKOFF_SECONDS,
                        )
                    ),
                ),
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            try:
                await _mark_evaluation_failed(
                    pool,
                    market,
                    definition_version=definition_version,
                    failed_ms=current_ms,
                    error=exc,
                    base_retry_seconds=max(
                        1,
                        int(
                            getattr(
                                settings,
                                "POLYMARKET_RESOLUTION_POLL_SECONDS",
                                FLIP_EVALUATOR_POLL_SECONDS,
                            )
                        ),
                    ),
                    max_retry_seconds=max(
                        1,
                        int(
                            getattr(
                                settings,
                                "POLYMARKET_RESOLUTION_MAX_BACKOFF_SECONDS",
                                FLIP_EVALUATOR_MAX_BACKOFF_SECONDS,
                            )
                        ),
                    ),
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception(
                    "polymarket_flip_evaluation_failure_state_failed",
                    extra={
                        "event": (
                            "polymarket_flip_evaluation_failure_state_failed"
                        ),
                        "market_id": market.get("market_id"),
                        "definition_version": definition_version,
                    },
                )
            LOGGER.exception(
                "polymarket_flip_evaluation_failed",
                extra={
                    "event": "polymarket_flip_evaluation_failed",
                    "market_id": market.get("market_id"),
                    "definition_version": definition_version,
                    "error": repr(exc),
                },
            )
            continue
        completed += bool(succeeded)
    return completed


async def evaluate_due_flip_markets_once(
    settings: Any,
    pool: Any,
    *,
    now_ms: Optional[int] = None,
) -> int:
    """Evaluate current 60-second markets and unfinished legacy 30-second work."""

    current_ms = _now_ms() if now_ms is None else now_ms
    completed = 0
    for definition_version in (
        TWAP_60S_FLIP_DEFINITION_VERSION,
        TWAP_30S_FLIP_DEFINITION_VERSION,
    ):
        completed += await _evaluate_due_flip_definition_once(
            settings,
            pool,
            definition_version=definition_version,
            now_ms=current_ms,
        )
    return completed


async def flip_evaluator_loop(settings: Any, pool: Any) -> None:
    """Retrying loop independent of both live probability and resolution I/O."""

    poll_seconds = max(
        1,
        int(
            getattr(
                settings,
                "POLYMARKET_RESOLUTION_POLL_SECONDS",
                FLIP_EVALUATOR_POLL_SECONDS,
            )
        ),
    )
    attempt = 0
    while True:
        try:
            await evaluate_due_flip_markets_once(settings, pool)
            attempt = 0
            await asyncio.sleep(poll_seconds)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            attempt += 1
            delay_ms = flip_retry_delay_ms(
                attempt,
                base_seconds=poll_seconds,
                max_seconds=max(
                    poll_seconds,
                    int(
                        getattr(
                            settings,
                            "POLYMARKET_RESOLUTION_MAX_BACKOFF_SECONDS",
                            FLIP_EVALUATOR_MAX_BACKOFF_SECONDS,
                        )
                    ),
                ),
            )
            LOGGER.error(
                "polymarket_flip_evaluator_restarting",
                extra={
                    "event": "polymarket_flip_evaluator_restarting",
                    "delay_seconds": delay_ms / 1000,
                    "error": repr(exc),
                },
            )
            await asyncio.sleep(delay_ms / 1000)


async def fetch_flip_markets(
    pool: Any,
    *,
    definition_version: int,
    within_seconds: int,
    kind: str,
    direction: Optional[str],
    winner: Optional[str],
    start_ms: Optional[int],
    end_ms: Optional[int],
    before_market_id: Optional[int],
    limit: int,
) -> list[dict[str, Any]]:
    """Fetch one row per matching market for the read-only list endpoint."""

    if kind not in {"any_crossing", "decisive_flip", "cutoff_reversal"}:
        raise ValueError(f"unknown flip kind: {kind!r}")
    if direction not in {None, "up_to_down", "down_to_up"}:
        raise ValueError(f"unknown flip direction: {direction!r}")
    within_ms = max(1, min(FLIP_WINDOW_SECONDS, int(within_seconds))) * 1000
    async with pool.acquire() as connection:
        rows = await connection.fetch(
            f"""
            SELECT
                evaluation.market_id,
                mw.market_start_ms,
                mw.market_end_ms,
                pm.settlement_reference,
                pm.settlement_window_s,
                pm.settlement_source_url,
                pm.settlement_rule_version,
                evaluation.evaluation_status,
                evaluation.price_to_beat,
                evaluation.official_close_price AS official_close,
                evaluation.official_winner AS winner,
                CASE
                    WHEN $3 = 'cutoff_reversal' THEN
                        CASE WHEN matching_cutoff.matches THEN 1 ELSE 0 END
                    ELSE matching_events.matching_count
                END::INTEGER AS matching_crossing_count,
                evaluation.crossing_count
                    AS total_crossing_count_last_20s,
                evaluation.first_crossing_ms_before_end,
                evaluation.last_crossing_ms_before_end,
                evaluation.decisive_flip_ms_before_end,
                evaluation.decisive_flip_direction,
                evaluation.archive_status,
                evaluation.source_microstructure_row_count
                    AS source_microstructure_rows,
                evaluation.archived_microstructure_row_count
                    AS archived_microstructure_rows,
                evaluation.retention_safe,
                evaluation.archived_ms AS archived_at_ms
            FROM {EVALUATION_TABLE} evaluation
            JOIN market_windows mw
              ON mw.market_id = evaluation.market_id
            JOIN polymarket_btc_5m_markets pm
              ON pm.market_id = evaluation.market_id
            LEFT JOIN LATERAL (
                SELECT count(*)::INTEGER AS matching_count
                FROM {EVENT_TABLE} event
                WHERE event.market_id = evaluation.market_id
                  AND event.definition_version = evaluation.definition_version
                  AND event.observed_ms_before_end <= $2
                  AND ($4::TEXT IS NULL OR event.direction = $4)
                  AND (
                        $3 <> 'decisive_flip'
                        OR event.is_decisive = TRUE
                  )
            ) matching_events ON TRUE
            LEFT JOIN LATERAL (
                SELECT EXISTS (
                    SELECT 1
                    FROM {CUTOFF_TABLE} cutoff
                    WHERE cutoff.market_id = evaluation.market_id
                      AND cutoff.definition_version =
                            evaluation.definition_version
                      AND cutoff.seconds_before_end =
                            ($2::BIGINT / 1000)::INTEGER
                      AND cutoff.chainlink_fresh = TRUE
                      AND cutoff.flipped_after_cutoff = TRUE
                      AND (
                            $4::TEXT IS NULL
                            OR (
                                $4 = 'up_to_down'
                                AND cutoff.apparent_side = 'Up'
                                AND cutoff.official_winner = 'Down'
                            )
                            OR (
                                $4 = 'down_to_up'
                                AND cutoff.apparent_side = 'Down'
                                AND cutoff.official_winner = 'Up'
                            )
                      )
                ) AS matches
            ) matching_cutoff ON TRUE
            WHERE evaluation.definition_version = $1
              AND {_market_rule_sql(
                    "$1::SMALLINT",
                    market_alias="pm",
                    window_alias="mw",
                  )}
              AND evaluation.observation_precision <> 'evaluation_failed'
              AND ($5::TEXT IS NULL OR evaluation.official_winner = $5)
              AND ($6::BIGINT IS NULL OR mw.market_start_ms >= $6)
              AND ($7::BIGINT IS NULL OR mw.market_start_ms < $7)
              AND ($8::BIGINT IS NULL OR evaluation.market_id < $8)
              AND (
                    ($3 = 'any_crossing'
                        AND matching_events.matching_count > 0)
                    OR
                    ($3 = 'decisive_flip'
                        AND matching_events.matching_count > 0)
                    OR
                    ($3 = 'cutoff_reversal'
                        AND matching_cutoff.matches)
              )
            ORDER BY evaluation.market_id DESC
            LIMIT $9
            """,
            definition_version,
            within_ms,
            kind,
            direction,
            winner,
            start_ms,
            end_ms,
            before_market_id,
            max(1, int(limit)),
        )
    return [dict(row) for row in rows]


def _cutoff_api_row(row: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result["chainlink_age_ms"] = result.get("chainlink_source_age_ms")
    result["signed_distance"] = result.get("price_distance")
    result["absolute_distance"] = result.get("absolute_price_distance")
    result["probability_age_ms"] = result.get("probability_source_age_ms")
    result["up_probability"] = result.get("up_ask")
    result["down_probability"] = result.get("down_ask")
    if result.get("microstructure_available"):
        result["microstructure"] = {
            "sample_second_ms": result.get("microstructure_sample_second_ms"),
            **{
                column: result.get(column)
                for column in MICROSTRUCTURE_VALUE_COLUMNS
            },
            "received_ms": result.get("microstructure_received_ms"),
        }
    else:
        result["microstructure"] = None
    return result


async def fetch_market_flip_analysis(
    pool: Any,
    *,
    market_id: int,
    definition_version: int,
) -> Optional[dict[str, Any]]:
    """Fetch the permanent evaluation, immutable events, and 20 cutoffs."""

    async with pool.acquire() as connection:
        evaluation = await connection.fetchrow(
            f"""
            SELECT
                evaluation.*,
                mw.market_start_ms,
                mw.market_end_ms,
                pm.settlement_reference,
                pm.settlement_window_s,
                pm.settlement_source_url,
                pm.settlement_rule_version,
                evaluation.official_close_price AS official_close,
                evaluation.official_winner AS winner,
                evaluation.source_microstructure_row_count
                    AS source_microstructure_rows,
                evaluation.archived_microstructure_row_count
                    AS archived_microstructure_rows,
                evaluation.evaluated_ms AS evaluated_at_ms,
                evaluation.archived_ms AS archived_at_ms,
                {FLIP_WINDOW_SECONDS}::INTEGER
                    AS expected_observation_count,
                evaluation.chainlink_observation_count
                    AS observed_observation_count,
                evaluation.chainlink_max_gap_ms
                    AS max_observation_gap_ms
            FROM {EVALUATION_TABLE} evaluation
            JOIN market_windows mw
              ON mw.market_id = evaluation.market_id
            JOIN polymarket_btc_5m_markets pm
              ON pm.market_id = evaluation.market_id
            WHERE evaluation.market_id = $1
              AND evaluation.definition_version = $2
              AND {_market_rule_sql(
                    "$2::SMALLINT",
                    market_alias="pm",
                    window_alias="mw",
                  )}
              AND evaluation.observation_precision <> 'evaluation_failed'
            """,
            market_id,
            definition_version,
        )
        if evaluation is None:
            return None
        events = await connection.fetch(
            f"""
            SELECT *
            FROM {EVENT_TABLE}
            WHERE market_id = $1
              AND definition_version = $2
            ORDER BY event_sequence ASC
            """,
            market_id,
            definition_version,
        )
        cutoffs = await connection.fetch(
            f"""
            SELECT *
            FROM {CUTOFF_TABLE}
            WHERE market_id = $1
              AND definition_version = $2
            ORDER BY seconds_before_end DESC
            """,
            market_id,
            definition_version,
        )

    evaluation_row = dict(evaluation)
    # These aliases are derived from the permanent cutoff rows rather than
    # inferred from raw source-row density.
    cutoff_rows = [_cutoff_api_row(dict(row)) for row in cutoffs]
    chainlink_cutoff_count = sum(
        row.get("chainlink_sample_second_ms") is not None for row in cutoff_rows
    )
    fresh_cutoff_count = sum(
        bool(row.get("chainlink_fresh")) for row in cutoff_rows
    )
    evaluation_row["fresh_observation_count"] = fresh_cutoff_count
    evaluation_row["missing_observation_count"] = (
        FLIP_WINDOW_SECONDS - chainlink_cutoff_count
    )
    evaluation_row["stale_observation_count"] = (
        chainlink_cutoff_count - fresh_cutoff_count
    )
    return {
        "evaluation": evaluation_row,
        "events": [dict(row) for row in events],
        "cutoffs": cutoff_rows,
    }


async def fetch_flip_distribution(
    pool: Any,
    *,
    definition_version: int,
    max_seconds: int,
    direction: Optional[str],
    start_ms: Optional[int],
    end_ms: Optional[int],
) -> dict[str, Any]:
    """Aggregate event-time and causal-cutoff distributions."""

    if direction not in {None, "up_to_down", "down_to_up"}:
        raise ValueError(f"unknown flip direction: {direction!r}")
    seconds = max(1, min(FLIP_WINDOW_SECONDS, int(max_seconds)))
    async with pool.acquire() as connection:
        population = await connection.fetchrow(
            f"""
            SELECT
                count(*)::INTEGER AS resolved_markets,
                count(*) FILTER (
                    WHERE evaluation.evaluation_status <> 'ambiguous'
                )::INTEGER AS eligible_markets,
                count(*) FILTER (
                    WHERE evaluation.evaluation_status = 'ambiguous'
                )::INTEGER AS ambiguous_markets,
                count(*) FILTER (
                    WHERE evaluation.evaluation_status <> 'ambiguous'
                      AND EXISTS (
                        SELECT 1
                        FROM {EVENT_TABLE} event
                        WHERE event.market_id = evaluation.market_id
                          AND event.definition_version =
                                evaluation.definition_version
                          AND event.observed_ms_before_end <=
                                $2::INTEGER * 1000
                          AND (
                                $3::TEXT IS NULL
                                OR event.direction = $3
                          )
                    )
                )::INTEGER AS markets_with_any_crossing
            FROM {EVALUATION_TABLE} evaluation
            JOIN market_windows mw
              ON mw.market_id = evaluation.market_id
            JOIN polymarket_btc_5m_markets pm
              ON pm.market_id = evaluation.market_id
            WHERE evaluation.definition_version = $1
              AND {_market_rule_sql(
                    "$1::SMALLINT",
                    market_alias="pm",
                    window_alias="mw",
                  )}
              AND evaluation.observation_precision <> 'evaluation_failed'
              AND ($4::BIGINT IS NULL OR mw.market_start_ms >= $4)
              AND ($5::BIGINT IS NULL OR mw.market_start_ms < $5)
            """,
            definition_version,
            seconds,
            direction,
            start_ms,
            end_ms,
        )
        crossing_rows = await connection.fetch(
            f"""
            WITH bins AS (
                SELECT generate_series($2::INTEGER, 1, -1) AS second
            ),
            eligible AS (
                SELECT
                    evaluation.market_id,
                    evaluation.evaluation_status
                FROM {EVALUATION_TABLE} evaluation
                JOIN market_windows mw
                  ON mw.market_id = evaluation.market_id
                JOIN polymarket_btc_5m_markets pm
                  ON pm.market_id = evaluation.market_id
                WHERE evaluation.definition_version = $1
                  AND {_market_rule_sql(
                        "$1::SMALLINT",
                        market_alias="pm",
                        window_alias="mw",
                      )}
                  AND evaluation.observation_precision <>
                        'evaluation_failed'
                  AND evaluation.evaluation_status <> 'ambiguous'
                  AND ($4::BIGINT IS NULL OR mw.market_start_ms >= $4)
                  AND ($5::BIGINT IS NULL OR mw.market_start_ms < $5)
            )
            SELECT
                bins.second * 1000 AS from_ms_before_end,
                (bins.second - 1) * 1000 AS to_ms_before_end,
                count(event.market_id) FILTER (
                    WHERE event.observed_ms_before_end >
                            (bins.second - 1) * 1000
                      AND event.observed_ms_before_end <= bins.second * 1000
                )::INTEGER AS crossing_event_count,
                count(DISTINCT event.market_id) FILTER (
                    WHERE event.observed_ms_before_end >
                            (bins.second - 1) * 1000
                      AND event.observed_ms_before_end <= bins.second * 1000
                )::INTEGER AS unique_market_count,
                count(DISTINCT event.market_id) FILTER (
                    WHERE event.is_decisive = TRUE
                      AND event.observed_ms_before_end >
                            (bins.second - 1) * 1000
                      AND event.observed_ms_before_end <= bins.second * 1000
                )::INTEGER AS decisive_flip_market_count,
                count(event.market_id) FILTER (
                    WHERE event.new_side = 'Up'
                      AND event.observed_ms_before_end >
                            (bins.second - 1) * 1000
                      AND event.observed_ms_before_end <= bins.second * 1000
                )::INTEGER AS to_up_count,
                count(event.market_id) FILTER (
                    WHERE event.new_side = 'Down'
                      AND event.observed_ms_before_end >
                            (bins.second - 1) * 1000
                      AND event.observed_ms_before_end <= bins.second * 1000
                )::INTEGER AS to_down_count,
                count(DISTINCT event.market_id) FILTER (
                    WHERE event.observed_ms_before_end <= bins.second * 1000
                )::INTEGER AS cumulative_unique_markets_within_window,
                CASE
                    WHEN count(DISTINCT eligible.market_id) FILTER (
                        WHERE eligible.evaluation_status <> 'ambiguous'
                    ) = 0
                    THEN NULL
                    ELSE ROUND(
                        (
                            count(DISTINCT event.market_id) FILTER (
                                WHERE event.observed_ms_before_end <=
                                        bins.second * 1000
                            )
                        )::NUMERIC
                        /
                        (
                            count(DISTINCT eligible.market_id) FILTER (
                                WHERE eligible.evaluation_status <>
                                        'ambiguous'
                            )
                        )::NUMERIC,
                        8
                    )
                END AS cumulative_market_rate
            FROM bins
            LEFT JOIN eligible ON TRUE
            LEFT JOIN {EVENT_TABLE} event
              ON event.market_id = eligible.market_id
             AND event.definition_version = $1
             AND event.observed_ms_before_end <= $2::INTEGER * 1000
             AND ($3::TEXT IS NULL OR event.direction = $3)
            GROUP BY bins.second
            ORDER BY bins.second DESC
            """,
            definition_version,
            seconds,
            direction,
            start_ms,
            end_ms,
        )
        cutoff_rows = await connection.fetch(
            f"""
            WITH seconds AS (
                SELECT generate_series($2::INTEGER, 1, -1) AS second
            ),
            filtered AS (
                SELECT
                    cutoff.seconds_before_end,
                    cutoff.flipped_after_cutoff,
                    cutoff.apparent_side,
                    cutoff.official_winner,
                    cutoff.chainlink_fresh
                FROM {CUTOFF_TABLE} cutoff
                JOIN market_windows mw
                  ON mw.market_id = cutoff.market_id
                JOIN polymarket_btc_5m_markets pm
                  ON pm.market_id = cutoff.market_id
                WHERE cutoff.definition_version = $1
                  AND {_market_rule_sql(
                        "$1::SMALLINT",
                        market_alias="pm",
                        window_alias="mw",
                      )}
                  AND cutoff.seconds_before_end <= $2
                  AND ($4::BIGINT IS NULL OR mw.market_start_ms >= $4)
                  AND ($5::BIGINT IS NULL OR mw.market_start_ms < $5)
            )
            SELECT
                seconds.second AS seconds_before_end,
                count(*) FILTER (
                    WHERE filtered.chainlink_fresh = TRUE
                      AND filtered.apparent_side IN ('Up', 'Down')
                      AND filtered.official_winner IN ('Up', 'Down')
                      AND (
                            $3::TEXT IS NULL
                            OR ($3 = 'up_to_down'
                                AND filtered.apparent_side = 'Up')
                            OR ($3 = 'down_to_up'
                                AND filtered.apparent_side = 'Down')
                      )
                )::INTEGER AS eligible_markets,
                count(*) FILTER (
                    WHERE filtered.chainlink_fresh = TRUE
                      AND filtered.flipped_after_cutoff = TRUE
                      AND (
                            $3::TEXT IS NULL
                            OR (
                                $3 = 'up_to_down'
                                AND filtered.apparent_side = 'Up'
                                AND filtered.official_winner = 'Down'
                            )
                            OR (
                                $3 = 'down_to_up'
                                AND filtered.apparent_side = 'Down'
                                AND filtered.official_winner = 'Up'
                            )
                      )
                )::INTEGER AS markets_reversed_by_close,
                CASE
                    WHEN count(*) FILTER (
                        WHERE filtered.chainlink_fresh = TRUE
                          AND filtered.apparent_side IN ('Up', 'Down')
                          AND filtered.official_winner IN ('Up', 'Down')
                          AND (
                                $3::TEXT IS NULL
                                OR ($3 = 'up_to_down'
                                    AND filtered.apparent_side = 'Up')
                                OR ($3 = 'down_to_up'
                                    AND filtered.apparent_side = 'Down')
                          )
                    ) = 0
                    THEN NULL
                    ELSE ROUND(
                        (
                            count(*) FILTER (
                                WHERE filtered.chainlink_fresh = TRUE
                                  AND filtered.flipped_after_cutoff = TRUE
                                  AND (
                                        $3::TEXT IS NULL
                                        OR (
                                            $3 = 'up_to_down'
                                            AND filtered.apparent_side = 'Up'
                                            AND filtered.official_winner =
                                                'Down'
                                        )
                                        OR (
                                            $3 = 'down_to_up'
                                            AND filtered.apparent_side = 'Down'
                                            AND filtered.official_winner = 'Up'
                                        )
                                  )
                            )
                        )::NUMERIC
                        /
                        (
                            count(*) FILTER (
                                WHERE filtered.chainlink_fresh = TRUE
                                  AND filtered.apparent_side IN ('Up', 'Down')
                                  AND filtered.official_winner IN ('Up', 'Down')
                                  AND (
                                        $3::TEXT IS NULL
                                        OR ($3 = 'up_to_down'
                                            AND filtered.apparent_side = 'Up')
                                        OR ($3 = 'down_to_up'
                                            AND filtered.apparent_side = 'Down')
                                  )
                            )
                        )::NUMERIC,
                        8
                    )
                END AS reversal_rate
            FROM seconds
            LEFT JOIN filtered
              ON filtered.seconds_before_end = seconds.second
            GROUP BY seconds.second
            ORDER BY seconds.second DESC
            """,
            definition_version,
            seconds,
            direction,
            start_ms,
            end_ms,
        )
    return {
        "population": (
            dict(population)
            if population is not None
            else {
                "resolved_markets": 0,
                "eligible_markets": 0,
                "ambiguous_markets": 0,
                "markets_with_any_crossing": 0,
            }
        ),
        "crossings_by_time": [dict(row) for row in crossing_rows],
        "cutoff_reversals": [dict(row) for row in cutoff_rows],
    }
