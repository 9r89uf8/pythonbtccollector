from __future__ import annotations

"""Causal scheduling and noncritical persistence for shadow evaluations.

The scheduler is synchronous and has no Redis, PostgreSQL, or asyncio
dependency.  The writer runtime deliberately sits behind a bounded,
drop-oldest queue so storage can never delay the live shadow loop.
"""

import asyncio
import heapq
import inspect
import logging
import time
from collections import deque
from dataclasses import dataclass, replace
from decimal import Decimal
from typing import (
    Any,
    Awaitable,
    Callable,
    Deque,
    Optional,
    Protocol,
    Sequence,
    Union,
)

from price_collector.market import MarketWindow
from price_collector.shadow_signal import (
    CatchupModel,
    EngineObservation,
    ModelSignal,
    ObservedPrice,
)


LOGGER = logging.getLogger("price_collector.shadow_signal_evaluation")
QUEUE_DROP_LOG_EVERY = 100
REJECTION_LOG_EVERY = 100
FORECAST_INPUT_AFTER_GENERATED = "forecast_input_after_generated"

OUTCOME_STATUS_AVAILABLE = "available"
OUTCOME_STATUS_UNAVAILABLE = "unavailable"
OUTCOME_STATUS_INTEGRITY_INVALID = "integrity_invalid"

OUTCOME_REASON_ENGINE_CLOCK_REGRESSION = "engine_clock_regression"
OUTCOME_REASON_CHAINLINK_OBSERVATION_GAP = "chainlink_observation_gap"
OUTCOME_REASON_CHAINLINK_RECEIVED_TIME_REGRESSION = (
    "chainlink_received_time_regression"
)
OUTCOME_REASON_CHAINLINK_SEQUENCE_GAP = "chainlink_sequence_gap"
OUTCOME_REASON_CHAINLINK_SEQUENCE_REGRESSION = "chainlink_sequence_regression"
OUTCOME_REASON_CHAINLINK_SEQUENCE_IDENTITY_MISMATCH = (
    "chainlink_sequence_identity_mismatch"
)
OUTCOME_REASON_CHAINLINK_PUBLISHER_EPOCH_CHANGE = (
    "chainlink_publisher_epoch_change"
)
OUTCOME_REASON_CHAINLINK_SEQUENCE_METADATA_LOSS = (
    "chainlink_sequence_metadata_loss"
)
OUTCOME_REASON_CHAINLINK_SEQUENCE_METADATA_RECOVERY = (
    "chainlink_sequence_metadata_recovery"
)
OUTCOME_REASON_CHAINLINK_SEQUENCE_CONFIRMATION_TIMEOUT = (
    "chainlink_sequence_confirmation_timeout"
)
OUTCOME_REASON_CHAINLINK_SEQUENCE_NOT_ESTABLISHED = (
    "chainlink_sequence_not_established"
)
OUTCOME_REASON_CHAINLINK_STARTUP_LEGACY_TO_SEQUENCED = (
    "chainlink_startup_legacy_to_sequenced"
)
OUTCOME_REASON_HISTORY_EPOCH_CHANGED = "outcome_history_epoch_changed"

_OUTCOME_STATUSES = frozenset(
    {
        OUTCOME_STATUS_AVAILABLE,
        OUTCOME_STATUS_UNAVAILABLE,
        OUTCOME_STATUS_INTEGRITY_INVALID,
    }
)


def _require_non_negative_int(value: object, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer")
    if value < 0:
        raise ValueError(f"{field_name} must be non-negative")


def _require_positive_int(value: object, field_name: str) -> None:
    _require_non_negative_int(value, field_name)
    if value == 0:
        raise ValueError(f"{field_name} must be positive")


def _require_non_empty_string(value: object, field_name: str) -> None:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    if not value.strip():
        raise ValueError(f"{field_name} must not be empty")


def _require_optional_decimal(value: object, field_name: str) -> None:
    if value is None:
        return
    if not isinstance(value, Decimal):
        raise TypeError(f"{field_name} must be Decimal")
    if not value.is_finite():
        raise ValueError(f"{field_name} must be finite")


@dataclass(frozen=True)
class ShadowEvaluationProvenance:
    selection_schema_version: int
    policy_version: str
    selection_fingerprint_sha256: str
    selection_artifact_sha256: str
    evidence_end_ms: int

    def __post_init__(self) -> None:
        _require_positive_int(
            self.selection_schema_version,
            "selection_schema_version",
        )
        _require_non_empty_string(
            self.policy_version,
            "policy_version",
        )
        for value, field_name in (
            (
                self.selection_fingerprint_sha256,
                "selection_fingerprint_sha256",
            ),
            (self.selection_artifact_sha256, "selection_artifact_sha256"),
        ):
            if (
                not isinstance(value, str)
                or len(value) != 64
                or any(character not in "0123456789abcdef" for character in value)
            ):
                raise ValueError(f"{field_name} must be a lowercase SHA-256 digest")
        _require_non_negative_int(
            self.evidence_end_ms,
            "evidence_end_ms",
        )


@dataclass(frozen=True)
class ShadowEvaluationRecord:
    selection_schema_version: int
    selection_policy_version: str
    selection_fingerprint_sha256: str
    selection_artifact_sha256: str
    selection_evidence_end_ms: int

    model_version: str
    beta: Decimal
    generated_ms: int
    target_ms: int
    matured_ms: int
    horizon_ms: int
    valid: bool
    status: str
    invalid_reasons: tuple[str, ...]
    state: str
    outcome_status: str
    outcome_invalid_reasons: tuple[str, ...]

    market_id: int
    market_start_ms: int
    market_end_ms: int
    ms_to_market_end: int
    full_horizon_before_market_end: bool

    chainlink_at_forecast: Optional[Decimal]
    chainlink_at_forecast_source_timestamp_ms: Optional[int]
    chainlink_at_forecast_received_ms: Optional[int]
    projected_chainlink: Optional[Decimal]
    pending_move: Optional[Decimal]
    pending_move_bps: Optional[Decimal]
    direction: Optional[str]

    futures_now: Optional[Decimal]
    futures_now_source_timestamp_ms: Optional[int]
    futures_now_received_ms: Optional[int]
    futures_reference: Optional[Decimal]
    futures_reference_source_timestamp_ms: Optional[int]
    futures_reference_received_ms: Optional[int]
    futures_reference_target_ms: Optional[int]
    futures_reference_gap_ms: Optional[int]
    futures_received_age_ms: Optional[int]
    chainlink_received_age_ms: Optional[int]

    actual_chainlink: Optional[Decimal]
    actual_chainlink_source_timestamp_ms: Optional[int]
    actual_chainlink_received_ms: Optional[int]
    actual_chainlink_age_at_target_ms: Optional[int]
    forecast_error: Optional[Decimal]
    baseline_error: Optional[Decimal]

    def __post_init__(self) -> None:
        for value, field_name in (
            (self.beta, "beta"),
            (self.chainlink_at_forecast, "chainlink_at_forecast"),
            (self.projected_chainlink, "projected_chainlink"),
            (self.pending_move, "pending_move"),
            (self.pending_move_bps, "pending_move_bps"),
            (self.futures_now, "futures_now"),
            (self.futures_reference, "futures_reference"),
            (self.actual_chainlink, "actual_chainlink"),
            (self.forecast_error, "forecast_error"),
            (self.baseline_error, "baseline_error"),
        ):
            _require_optional_decimal(value, field_name)
        if not isinstance(self.beta, Decimal):
            raise TypeError("beta must be Decimal")
        for value, field_name in (
            (self.generated_ms, "generated_ms"),
            (self.target_ms, "target_ms"),
            (self.matured_ms, "matured_ms"),
            (self.horizon_ms, "horizon_ms"),
            (self.market_id, "market_id"),
            (self.market_start_ms, "market_start_ms"),
            (self.market_end_ms, "market_end_ms"),
            (self.ms_to_market_end, "ms_to_market_end"),
        ):
            _require_non_negative_int(value, field_name)
        if self.target_ms != self.generated_ms + self.horizon_ms:
            raise ValueError("target_ms must equal generated_ms plus horizon_ms")
        if self.matured_ms < self.target_ms:
            raise ValueError("matured_ms must not precede target_ms")
        if self.market_end_ms <= self.market_start_ms:
            raise ValueError("market_end_ms must follow market_start_ms")
        _require_non_empty_string(self.model_version, "model_version")
        _require_non_empty_string(self.status, "status")
        _require_non_empty_string(self.state, "state")
        _require_non_empty_string(self.outcome_status, "outcome_status")
        if not isinstance(self.valid, bool):
            raise TypeError("valid must be boolean")
        if not isinstance(self.invalid_reasons, tuple) or not all(
            isinstance(reason, str) and reason
            for reason in self.invalid_reasons
        ):
            raise TypeError("invalid_reasons must be a tuple of non-empty strings")
        if not isinstance(self.outcome_invalid_reasons, tuple) or not all(
            isinstance(reason, str) and reason
            for reason in self.outcome_invalid_reasons
        ):
            raise TypeError(
                "outcome_invalid_reasons must be a tuple of non-empty strings"
            )
        if len(set(self.outcome_invalid_reasons)) != len(
            self.outcome_invalid_reasons
        ):
            raise ValueError("outcome_invalid_reasons must not contain duplicates")
        if self.outcome_status not in _OUTCOME_STATUSES:
            raise ValueError("outcome_status is invalid")
        if self.direction not in (None, "up", "down", "flat"):
            raise ValueError("direction is invalid")

        projection_values = (
            self.projected_chainlink,
            self.pending_move,
            self.pending_move_bps,
            self.direction,
        )
        if not self.valid:
            if any(value is not None for value in projection_values):
                raise ValueError(
                    "invalid evaluation must not contain projection output"
                )
        else:
            required_valid_values = (
                self.chainlink_at_forecast,
                self.futures_now,
                self.futures_reference,
                self.futures_reference_target_ms,
                self.futures_reference_gap_ms,
                *projection_values,
            )
            if any(value is None for value in required_valid_values):
                raise ValueError(
                    "valid evaluation requires complete projection inputs and output"
                )

            assert self.chainlink_at_forecast is not None
            assert self.futures_now is not None
            assert self.futures_reference is not None
            assert self.projected_chainlink is not None
            assert self.pending_move is not None
            assert self.pending_move_bps is not None
            assert self.direction is not None
            for value, field_name in (
                (self.chainlink_at_forecast, "chainlink_at_forecast"),
                (self.futures_now, "futures_now"),
                (self.futures_reference, "futures_reference"),
                (self.projected_chainlink, "projected_chainlink"),
            ):
                if value <= 0:
                    raise ValueError(f"{field_name} must be positive")

            expected_pending_move = (
                self.projected_chainlink - self.chainlink_at_forecast
            )
            if self.pending_move != expected_pending_move:
                raise ValueError("pending_move is inconsistent")
            expected_pending_move_bps = (
                self.pending_move / self.chainlink_at_forecast * Decimal("10000")
            )
            if self.pending_move_bps != expected_pending_move_bps:
                raise ValueError("pending_move_bps is inconsistent")
            expected_direction = (
                "up"
                if self.pending_move > 0
                else "down"
                if self.pending_move < 0
                else "flat"
            )
            if self.direction != expected_direction:
                raise ValueError("direction is inconsistent with pending_move")

        actual_values = (
            self.actual_chainlink,
            self.actual_chainlink_received_ms,
            self.actual_chainlink_age_at_target_ms,
        )
        if any(value is None for value in actual_values) and any(
            value is not None for value in actual_values
        ):
            raise ValueError("actual Chainlink value, receive time, and age are atomic")
        if (
            self.actual_chainlink is None
            and self.actual_chainlink_source_timestamp_ms is not None
        ):
            raise ValueError(
                "actual Chainlink source time requires an actual value"
            )
        if self.actual_chainlink_received_ms is not None:
            if self.actual_chainlink_received_ms > self.target_ms:
                raise ValueError("actual Chainlink cannot be received after target")
            expected_age = self.target_ms - self.actual_chainlink_received_ms
            if self.actual_chainlink_age_at_target_ms != expected_age:
                raise ValueError("actual Chainlink target age is inconsistent")

        if self.outcome_status == OUTCOME_STATUS_AVAILABLE:
            if self.actual_chainlink is None:
                raise ValueError("available outcome requires an actual value")
            if self.outcome_invalid_reasons:
                raise ValueError(
                    "available outcome must not contain invalid reasons"
                )
        elif self.outcome_status == OUTCOME_STATUS_UNAVAILABLE:
            if self.actual_chainlink is not None:
                raise ValueError("unavailable outcome must not contain an actual")
            if self.outcome_invalid_reasons:
                raise ValueError(
                    "unavailable outcome must not contain invalid reasons"
                )
        else:
            if self.actual_chainlink is not None:
                raise ValueError(
                    "integrity-invalid outcome must not contain an actual"
                )
            if not self.outcome_invalid_reasons:
                raise ValueError(
                    "integrity-invalid outcome requires an explicit reason"
                )

        expected_forecast_error = (
            self.projected_chainlink - self.actual_chainlink
            if self.projected_chainlink is not None
            and self.actual_chainlink is not None
            else None
        )
        if self.forecast_error != expected_forecast_error:
            raise ValueError("forecast_error is inconsistent")
        expected_baseline_error = (
            self.chainlink_at_forecast - self.actual_chainlink
            if self.chainlink_at_forecast is not None
            and self.actual_chainlink is not None
            else None
        )
        if self.baseline_error != expected_baseline_error:
            raise ValueError("baseline_error is inconsistent")


@dataclass(frozen=True, order=True)
class ShadowEvaluationCohortIdentity:
    """Existing persisted provenance plus generation time for one cohort."""

    selection_schema_version: int
    selection_policy_version: str
    selection_fingerprint_sha256: str
    selection_artifact_sha256: str
    selection_evidence_end_ms: int
    generated_ms: int


@dataclass(frozen=True)
class ShadowEvaluationCohortWriteResult:
    """Exact, mutually exclusive outcomes for a backend cohort batch."""

    persisted_cohort_ids: frozenset[
        ShadowEvaluationCohortIdentity
    ] = frozenset()
    rejected_cohort_ids: frozenset[
        ShadowEvaluationCohortIdentity
    ] = frozenset()
    deferred_cohort_ids: frozenset[
        ShadowEvaluationCohortIdentity
    ] = frozenset()

    def __post_init__(self) -> None:
        classifications = (
            ("persisted_cohort_ids", self.persisted_cohort_ids),
            ("rejected_cohort_ids", self.rejected_cohort_ids),
            ("deferred_cohort_ids", self.deferred_cohort_ids),
        )
        for field_name, cohort_ids in classifications:
            if not isinstance(cohort_ids, frozenset):
                raise TypeError(f"{field_name} must be a frozenset")
            if not all(
                isinstance(cohort_id, ShadowEvaluationCohortIdentity)
                for cohort_id in cohort_ids
            ):
                raise TypeError(
                    f"{field_name} must contain cohort identities"
                )
        if (
            self.persisted_cohort_ids & self.rejected_cohort_ids
            or self.persisted_cohort_ids & self.deferred_cohort_ids
            or self.rejected_cohort_ids & self.deferred_cohort_ids
        ):
            raise ValueError(
                "shadow evaluation cohort outcomes must be disjoint"
            )

    @property
    def cohort_ids(self) -> frozenset[ShadowEvaluationCohortIdentity]:
        return (
            self.persisted_cohort_ids
            | self.rejected_cohort_ids
            | self.deferred_cohort_ids
        )


def shadow_evaluation_cohort_identity(
    record: ShadowEvaluationRecord,
) -> ShadowEvaluationCohortIdentity:
    if not isinstance(record, ShadowEvaluationRecord):
        raise TypeError("cohort members must be ShadowEvaluationRecord values")
    return ShadowEvaluationCohortIdentity(
        selection_schema_version=record.selection_schema_version,
        selection_policy_version=record.selection_policy_version,
        selection_fingerprint_sha256=(
            record.selection_fingerprint_sha256
        ),
        selection_artifact_sha256=record.selection_artifact_sha256,
        selection_evidence_end_ms=record.selection_evidence_end_ms,
        generated_ms=record.generated_ms,
    )


@dataclass(frozen=True)
class ShadowEvaluationCohort:
    """A complete, indivisible candidate set for one generation instant."""

    records: tuple[ShadowEvaluationRecord, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.records, tuple) or not self.records:
            raise ValueError("evaluation cohort must contain records")
        identities = {
            shadow_evaluation_cohort_identity(record)
            for record in self.records
        }
        if len(identities) != 1:
            raise ValueError(
                "evaluation cohort records must share provenance and generated_ms"
            )
        versions = tuple(record.model_version for record in self.records)
        if len(set(versions)) != len(versions):
            raise ValueError("evaluation cohort model versions must be unique")
        generation_markets = {
            (
                record.market_id,
                record.market_start_ms,
                record.market_end_ms,
                record.ms_to_market_end,
            )
            for record in self.records
        }
        if len(generation_markets) != 1:
            raise ValueError(
                "evaluation cohort records must share generation-market fields"
            )

    @property
    def identity(self) -> ShadowEvaluationCohortIdentity:
        return shadow_evaluation_cohort_identity(self.records[0])

    @property
    def model_versions(self) -> tuple[str, ...]:
        return tuple(record.model_version for record in self.records)

    @property
    def row_count(self) -> int:
        return len(self.records)

    def require_model_versions(
        self,
        expected_model_versions: Sequence[str],
    ) -> None:
        expected = tuple(expected_model_versions)
        if self.model_versions != expected:
            raise ValueError(
                "evaluation cohort model set or order does not match writer models"
            )


@dataclass(frozen=True)
class _PendingEvaluation:
    model: CatchupModel
    signal: ModelSignal
    market: MarketWindow
    ms_to_market_end: int

    @property
    def target_ms(self) -> int:
        return self.signal.generated_ms + self.signal.horizon_ms


@dataclass(frozen=True)
class _PendingEvaluationCohort:
    sequence: int
    history_epoch: int
    evaluations: tuple[_PendingEvaluation, ...]
    generation_outcome_invalid_reasons: tuple[str, ...] = ()

    @property
    def minimum_target_ms(self) -> int:
        return min(evaluation.target_ms for evaluation in self.evaluations)

    @property
    def maximum_target_ms(self) -> int:
        return max(evaluation.target_ms for evaluation in self.evaluations)

    @property
    def row_count(self) -> int:
        return len(self.evaluations)


class ShadowEvaluationScheduler:
    """Schedule candidate snapshots and mature them against causal outcomes."""

    def __init__(
        self,
        *,
        models: Sequence[CatchupModel],
        provenance: ShadowEvaluationProvenance,
        cadence_ms: int = 500,
        max_observation_gap_ms: Optional[int] = None,
    ) -> None:
        self.models = tuple(models)
        if not self.models or not all(
            isinstance(model, CatchupModel) for model in self.models
        ):
            raise ValueError("models must contain at least one CatchupModel")
        versions = tuple(model.version for model in self.models)
        if len(set(versions)) != len(versions):
            raise ValueError("model versions must be unique")
        if not isinstance(provenance, ShadowEvaluationProvenance):
            raise TypeError("provenance must be ShadowEvaluationProvenance")
        _require_positive_int(cadence_ms, "cadence_ms")
        if max_observation_gap_ms is not None:
            _require_positive_int(
                max_observation_gap_ms,
                "max_observation_gap_ms",
            )
        if (
            provenance.selection_schema_version >= 3
            and max_observation_gap_ms is None
        ):
            raise ValueError(
                "schema v3 evaluation requires a bounded observation gap"
            )

        self.provenance = provenance
        self.cadence_ms = cadence_ms
        self.max_observation_gap_ms = max_observation_gap_ms
        self._models_by_version = {model.version: model for model in self.models}
        self._pending: list[
            tuple[int, int, _PendingEvaluationCohort]
        ] = []
        self._sequence = 0
        self._last_scheduled_bucket: Optional[int] = None
        self._last_observation_ms: Optional[int] = None

        self._chainlink_history: Deque[ObservedPrice] = deque()
        self._seen_chainlink_identities: set[
            tuple[Optional[int], int, Decimal]
        ] = set()
        self._last_observed_chainlink_identity: Optional[
            tuple[Optional[int], int, Decimal]
        ] = None
        self._chainlink_received_watermark: Optional[int] = None
        self._last_chainlink_publisher_epoch: Optional[str] = None
        self._last_chainlink_accepted_event_sequence: Optional[int] = None
        self._last_chainlink_sequence_identity: Optional[
            tuple[Optional[int], int, Decimal]
        ] = None
        self._last_sequence_continuity_observed_ms: Optional[int] = None
        self._history_epoch = 0
        self._outcome_history_resets: Deque[tuple[int, str]] = deque()
        self._regression_count = 0
        self._observation_gap_count = 0
        self._chainlink_sequence_gap_count = 0
        self._chainlink_sequence_regression_count = 0
        self._chainlink_sequence_identity_mismatch_count = 0
        self._chainlink_publisher_epoch_change_count = 0
        self._chainlink_sequence_metadata_loss_count = 0
        self._chainlink_sequence_confirmation_timeout_count = 0
        self._chainlink_sequence_metadata_lost = False
        self._chainlink_sequence_identity_conflicted = False
        self._chainlink_sequence_ever_established = False
        self._chainlink_startup_legacy_observed = False

    @property
    def pending_count(self) -> int:
        return sum(
            cohort.row_count
            for _target, _sequence, cohort in self._pending
        )

    @property
    def history_size(self) -> int:
        return len(self._chainlink_history)

    @property
    def regression_count(self) -> int:
        return self._regression_count

    @property
    def observation_gap_count(self) -> int:
        return self._observation_gap_count

    @property
    def chainlink_sequence_gap_count(self) -> int:
        return self._chainlink_sequence_gap_count

    @property
    def chainlink_sequence_regression_count(self) -> int:
        return self._chainlink_sequence_regression_count

    @property
    def chainlink_sequence_identity_mismatch_count(self) -> int:
        return self._chainlink_sequence_identity_mismatch_count

    @property
    def chainlink_publisher_epoch_change_count(self) -> int:
        return self._chainlink_publisher_epoch_change_count

    @property
    def chainlink_sequence_metadata_loss_count(self) -> int:
        return self._chainlink_sequence_metadata_loss_count

    @property
    def chainlink_sequence_confirmation_timeout_count(self) -> int:
        return self._chainlink_sequence_confirmation_timeout_count

    def observe(
        self,
        observation: EngineObservation,
        *,
        chainlink: Optional[ObservedPrice],
    ) -> tuple[ShadowEvaluationRecord, ...]:
        if not isinstance(observation, EngineObservation):
            raise TypeError("observation must be EngineObservation")
        if chainlink is not None and not isinstance(chainlink, ObservedPrice):
            raise TypeError("chainlink must be ObservedPrice or None")
        previous_observation_ms = self._last_observation_ms
        if (
            previous_observation_ms is not None
            and observation.generated_ms < previous_observation_ms
        ):
            # The pure engine resets on a wall-clock regression.  Advance the
            # outcome epoch as well so pre-reset forecasts can never mature
            # against post-reset observations.  Keep the cadence watermark to
            # avoid emitting a second cohort for an already-entered bucket.
            self._reset_outcome_history(
                reason=OUTCOME_REASON_ENGINE_CLOCK_REGRESSION,
            )
            self._regression_count += 1
            self._last_observation_ms = observation.generated_ms
            return ()
        sequence_was_established = self._chainlink_sequence_ever_established
        sequence_continuity_available = (
            chainlink is not None
            and chainlink.publisher_epoch is not None
            and self._last_chainlink_publisher_epoch is not None
        )
        sequence_discontinuity = self._observe_chainlink_delivery_sequence(
            chainlink
        )
        if (
            previous_observation_ms is not None
            and self.max_observation_gap_ms is not None
            and observation.generated_ms - previous_observation_ms
            > self.max_observation_gap_ms
            and not sequence_continuity_available
            and not sequence_discontinuity
            and (
                # V3 has no admitted outcome history before its first
                # sequence, so a long startup wait is not a history gap.
                self.provenance.selection_schema_version < 3
                or sequence_was_established
            )
        ):
            # A latest-value cache cannot reconstruct values overwritten while
            # it was not observed. Invalidate outstanding outcome history and
            # restart it from the newly acquired cache state.
            self._reset_outcome_history(
                reason=OUTCOME_REASON_CHAINLINK_OBSERVATION_GAP,
            )
            self._observation_gap_count += 1
        self._last_observation_ms = observation.generated_ms

        if (
            chainlink is not None
            and chainlink.publisher_epoch is not None
            and not self._chainlink_sequence_metadata_lost
            and not self._chainlink_sequence_identity_conflicted
        ):
            # Record the worker time of a successfully decoded sequenced
            # cache read. Any recognized reset path clears this watermark.
            self._last_sequence_continuity_observed_ms = (
                observation.generated_ms
            )

        chainlink_for_outcome = chainlink
        if (
            self._chainlink_sequence_metadata_lost
            or self._v3_sequence_not_established()
            or self._chainlink_sequence_identity_conflicted
        ):
            # Keep scheduling attempts for coverage, but never admit an
            # unsequenced v3 startup value or a disputed sequence identity
            # into causal target history.
            chainlink_for_outcome = None
        self._ingest_chainlink(chainlink_for_outcome)
        self._schedule_current_bucket(observation)
        matured = self._mature(observation.generated_ms)
        self._prune_history(observation.generated_ms)
        return matured

    def _v3_sequence_not_established(self) -> bool:
        return (
            self.provenance.selection_schema_version >= 3
            and not self._chainlink_sequence_ever_established
        )

    def _generation_outcome_invalid_reasons(self) -> tuple[str, ...]:
        reasons: list[str] = []
        if self._v3_sequence_not_established():
            reasons.append(OUTCOME_REASON_CHAINLINK_SEQUENCE_NOT_ESTABLISHED)
        if self._chainlink_sequence_identity_conflicted:
            reasons.append(
                OUTCOME_REASON_CHAINLINK_SEQUENCE_IDENTITY_MISMATCH
            )
        return tuple(reasons)

    def _reset_outcome_history(
        self,
        *,
        reason: str,
        reset_received_watermark: bool = False,
    ) -> None:
        _require_non_empty_string(reason, "reason")
        self._history_epoch += 1
        self._outcome_history_resets.append((self._history_epoch, reason))
        self._last_sequence_continuity_observed_ms = None
        self._chainlink_history.clear()
        self._seen_chainlink_identities.clear()
        self._last_observed_chainlink_identity = None
        if reset_received_watermark:
            self._chainlink_received_watermark = None

    def _observe_chainlink_delivery_sequence(
        self,
        chainlink: Optional[ObservedPrice],
    ) -> bool:
        if chainlink is None:
            return False
        if chainlink.publisher_epoch is None:
            if not self._chainlink_sequence_ever_established:
                self._chainlink_startup_legacy_observed = True
                return False
            if not self._chainlink_sequence_metadata_lost:
                self._chainlink_sequence_metadata_loss_count += 1
                self._reset_outcome_history(
                    reason=OUTCOME_REASON_CHAINLINK_SEQUENCE_METADATA_LOSS,
                    reset_received_watermark=True,
                )
            self._chainlink_sequence_metadata_lost = True
            # Metadata loss invalidates outcome continuity, but it must not
            # erase the last immutable sequence binding. Otherwise the same
            # epoch/sequence could return with changed event data and be
            # mistaken for an ordinary metadata recovery.
            return True
        publisher_epoch = chainlink.publisher_epoch
        sequence = chainlink.accepted_event_sequence
        assert sequence is not None

        previous_epoch = self._last_chainlink_publisher_epoch
        previous_sequence = self._last_chainlink_accepted_event_sequence
        previous_identity = self._last_chainlink_sequence_identity
        if previous_epoch is None:
            recovered_from_metadata_loss = self._chainlink_sequence_metadata_lost
            transitioned_from_startup_legacy = (
                self._chainlink_startup_legacy_observed
            )
            if recovered_from_metadata_loss or transitioned_from_startup_legacy:
                # No sequence can prove what happened before continuity became
                # available. Start a fresh outcome epoch at this sequenced
                # value before scoring resumes.
                self._reset_outcome_history(
                    reason=(
                        OUTCOME_REASON_CHAINLINK_SEQUENCE_METADATA_RECOVERY
                        if recovered_from_metadata_loss
                        else OUTCOME_REASON_CHAINLINK_STARTUP_LEGACY_TO_SEQUENCED
                    ),
                    reset_received_watermark=True,
                )
            self._chainlink_sequence_metadata_lost = False
            self._chainlink_sequence_identity_conflicted = False
            self._chainlink_sequence_ever_established = True
            self._chainlink_startup_legacy_observed = False
            self._last_chainlink_publisher_epoch = publisher_epoch
            self._last_chainlink_accepted_event_sequence = sequence
            self._last_chainlink_sequence_identity = chainlink.identity
            return (
                recovered_from_metadata_loss
                or transitioned_from_startup_legacy
            )

        assert previous_sequence is not None
        assert previous_identity is not None
        recovered_from_metadata_loss = self._chainlink_sequence_metadata_lost

        if self._chainlink_sequence_identity_conflicted:
            recovery_reason: Optional[str] = None
            if publisher_epoch != previous_epoch:
                self._chainlink_publisher_epoch_change_count += 1
                recovery_reason = (
                    OUTCOME_REASON_CHAINLINK_PUBLISHER_EPOCH_CHANGE
                )
            elif sequence > previous_sequence:
                if sequence > previous_sequence + 1:
                    self._chainlink_sequence_gap_count += 1
                    recovery_reason = OUTCOME_REASON_CHAINLINK_SEQUENCE_GAP
            else:
                # Neither disputed identity can re-establish what this
                # sequence meant. Stay quarantined until the publisher moves
                # forward or starts a new epoch.
                return False

            if recovery_reason is not None:
                self._reset_outcome_history(
                    reason=recovery_reason,
                    reset_received_watermark=True,
                )
            self._chainlink_sequence_metadata_lost = False
            self._chainlink_sequence_identity_conflicted = False
            self._last_chainlink_publisher_epoch = publisher_epoch
            self._last_chainlink_accepted_event_sequence = sequence
            self._last_chainlink_sequence_identity = chainlink.identity
            return recovery_reason is not None

        discontinuity_reason: Optional[str] = None
        if publisher_epoch != previous_epoch:
            self._chainlink_publisher_epoch_change_count += 1
            discontinuity_reason = (
                OUTCOME_REASON_CHAINLINK_PUBLISHER_EPOCH_CHANGE
            )
        else:
            if sequence < previous_sequence:
                self._chainlink_sequence_regression_count += 1
                discontinuity_reason = OUTCOME_REASON_CHAINLINK_SEQUENCE_REGRESSION
            elif (
                sequence == previous_sequence
                and chainlink.identity != previous_identity
            ):
                self._chainlink_sequence_identity_mismatch_count += 1
                self._chainlink_sequence_identity_conflicted = True
                discontinuity_reason = (
                    OUTCOME_REASON_CHAINLINK_SEQUENCE_IDENTITY_MISMATCH
                )
            elif sequence > previous_sequence + 1:
                self._chainlink_sequence_gap_count += 1
                discontinuity_reason = OUTCOME_REASON_CHAINLINK_SEQUENCE_GAP
            elif recovered_from_metadata_loss:
                discontinuity_reason = (
                    OUTCOME_REASON_CHAINLINK_SEQUENCE_METADATA_RECOVERY
                )

        if discontinuity_reason is not None:
            # Outstanding forecasts must not use history that crossed a proven
            # overwrite, producer restart, sequence regression, or immutable
            # sequence-identity violation. A conflicted sequence remains
            # quarantined instead of seeding the fresh history epoch.
            self._reset_outcome_history(
                reason=discontinuity_reason,
                reset_received_watermark=True,
            )
        if not self._chainlink_sequence_identity_conflicted:
            self._chainlink_sequence_metadata_lost = False
            self._last_chainlink_publisher_epoch = publisher_epoch
            self._last_chainlink_accepted_event_sequence = sequence
            self._last_chainlink_sequence_identity = chainlink.identity
        return discontinuity_reason is not None

    def _ingest_chainlink(self, chainlink: Optional[ObservedPrice]) -> None:
        if chainlink is None:
            return
        identity = chainlink.identity
        if identity == self._last_observed_chainlink_identity:
            return
        self._last_observed_chainlink_identity = identity

        watermark = self._chainlink_received_watermark
        if watermark is not None and chainlink.received_ms < watermark:
            # Clearing the history and advancing the epoch prevents an older
            # cached identity from being used as a target for any outstanding
            # forecast.  Recovery requires a value at or beyond the watermark.
            self._reset_outcome_history(
                reason=OUTCOME_REASON_CHAINLINK_RECEIVED_TIME_REGRESSION,
            )
            self._regression_count += 1
            self._seen_chainlink_identities.add(identity)
            return

        if identity in self._seen_chainlink_identities:
            return
        self._chainlink_history.append(chainlink)
        self._seen_chainlink_identities.add(identity)
        self._chainlink_received_watermark = (
            chainlink.received_ms
            if watermark is None
            else max(watermark, chainlink.received_ms)
        )

    def _schedule_current_bucket(self, observation: EngineObservation) -> None:
        bucket = observation.generated_ms // self.cadence_ms
        if (
            self._last_scheduled_bucket is not None
            and bucket <= self._last_scheduled_bucket
        ):
            return
        signals = {signal.model_version: signal for signal in observation.signals}
        if len(signals) != len(observation.signals):
            raise ValueError("observation contains duplicate model signals")
        missing = set(self._models_by_version) - set(signals)
        extra = set(signals) - set(self._models_by_version)
        if missing or extra:
            raise ValueError("observation model set does not match scheduler models")

        prepared: list[tuple[CatchupModel, ModelSignal]] = []
        for model in self.models:
            signal = signals[model.version]
            if signal.generated_ms != observation.generated_ms:
                raise ValueError("signal generated_ms does not match observation")
            if signal.horizon_ms != model.lag_ms:
                raise ValueError("signal horizon does not match model")
            signal = self._causal_evaluation_signal(signal)
            prepared.append((model, signal))

        pending_evaluations: list[_PendingEvaluation] = []
        for model, signal in prepared:
            pending_evaluations.append(
                _PendingEvaluation(
                    model=model,
                    signal=signal,
                    market=observation.market,
                    ms_to_market_end=observation.ms_to_market_end,
                )
            )

        # Commit cadence and heap state only after the complete candidate set
        # has validated, so a malformed observation cannot consume a bucket or
        # leave a partial cohort behind.
        self._last_scheduled_bucket = bucket
        self._sequence += 1
        cohort = _PendingEvaluationCohort(
            sequence=self._sequence,
            history_epoch=self._history_epoch,
            evaluations=tuple(pending_evaluations),
            # Capture quarantine at generation so later recovery cannot
            # retroactively make the cohort scoreable.
            generation_outcome_invalid_reasons=(
                self._generation_outcome_invalid_reasons()
            ),
        )
        heapq.heappush(
            self._pending,
            (cohort.maximum_target_ms, cohort.sequence, cohort),
        )

    @staticmethod
    def _causal_evaluation_signal(signal: ModelSignal) -> ModelSignal:
        after_generated = any(
            price is not None and price.received_ms > signal.generated_ms
            for price in (signal.chainlink_now, signal.futures_now)
        )
        if not after_generated:
            return signal
        reasons = tuple(
            dict.fromkeys(
                (*signal.invalid_reasons, FORECAST_INPUT_AFTER_GENERATED)
            )
        )
        return replace(
            signal,
            valid=False,
            status=FORECAST_INPUT_AFTER_GENERATED,
            invalid_reasons=reasons,
            projection=None,
        )

    def _mature(self, matured_ms: int) -> tuple[ShadowEvaluationRecord, ...]:
        records: list[ShadowEvaluationRecord] = []
        while self._pending and self._pending[0][0] <= matured_ms:
            _target_ms, _sequence, pending_cohort = self._pending[0]
            # Generation-time quarantine and later history resets are both
            # cohort-wide integrity failures; preserve each distinct cause.
            outcome_reasons = tuple(
                dict.fromkeys(
                    (
                        *pending_cohort.generation_outcome_invalid_reasons,
                        *self._outcome_reset_reasons_since(
                            pending_cohort.history_epoch
                        ),
                    )
                )
            )
            if (
                self.provenance.selection_schema_version >= 3
                and not outcome_reasons
            ):
                # V3 must observe sequenced cache continuity at or after the
                # longest target. Reuse the configured two-poll gap bound so
                # missing reads cannot leave pending cohorts unbounded.
                assert self.max_observation_gap_ms is not None
                confirmation_deadline_ms = (
                    pending_cohort.maximum_target_ms
                    + self.max_observation_gap_ms
                )
                confirmation_ms = (
                    self._last_sequence_continuity_observed_ms
                )
                confirmed_in_time = (
                    confirmation_ms is not None
                    and confirmation_ms >= pending_cohort.maximum_target_ms
                    and confirmation_ms <= confirmation_deadline_ms
                )
                if not confirmed_in_time:
                    if matured_ms < confirmation_deadline_ms:
                        break
                    outcome_reasons = (
                        OUTCOME_REASON_CHAINLINK_SEQUENCE_CONFIRMATION_TIMEOUT,
                    )
                    self._chainlink_sequence_confirmation_timeout_count += 1

            heapq.heappop(self._pending)
            cohort_records: list[ShadowEvaluationRecord] = []
            for pending in pending_cohort.evaluations:
                actual = (
                    None
                    if outcome_reasons
                    else self._actual_at(pending.target_ms)
                )
                outcome_status = (
                    OUTCOME_STATUS_INTEGRITY_INVALID
                    if outcome_reasons
                    else OUTCOME_STATUS_AVAILABLE
                    if actual is not None
                    else OUTCOME_STATUS_UNAVAILABLE
                )
                cohort_records.append(
                    self._record(
                        pending,
                        actual=actual,
                        matured_ms=matured_ms,
                        outcome_status=outcome_status,
                        outcome_invalid_reasons=outcome_reasons,
                    )
                )
            cohort = ShadowEvaluationCohort(
                tuple(cohort_records)
            )
            cohort.require_model_versions(
                tuple(model.version for model in self.models)
            )
            records.extend(cohort.records)
        return tuple(records)

    def _outcome_reset_reasons_since(
        self,
        history_epoch: int,
    ) -> tuple[str, ...]:
        if history_epoch == self._history_epoch:
            return ()
        reasons = tuple(
            dict.fromkeys(
                reason
                for reset_epoch, reason in self._outcome_history_resets
                if reset_epoch > history_epoch
            )
        )
        if reasons:
            return reasons
        return (OUTCOME_REASON_HISTORY_EPOCH_CHANGED,)

    def _actual_at(self, target_ms: int) -> Optional[ObservedPrice]:
        for price in reversed(self._chainlink_history):
            if price.received_ms <= target_ms:
                return price
        return None

    def _prune_history(self, now_ms: int) -> None:
        if self._pending:
            cutoff_ms = min(
                cohort.minimum_target_ms
                for _target, _sequence, cohort in self._pending
            )
        else:
            maximum_horizon = max(model.lag_ms for model in self.models)
            cutoff_ms = max(0, now_ms - maximum_horizon - self.cadence_ms)

        # Keep the newest event at/before the oldest pending target as its
        # predecessor, plus every later event.
        while (
            len(self._chainlink_history) >= 2
            and self._chainlink_history[1].received_ms <= cutoff_ms
        ):
            removed = self._chainlink_history.popleft()
            self._seen_chainlink_identities.discard(removed.identity)

        if self._pending:
            oldest_required_epoch = min(
                cohort.history_epoch
                for _target, _sequence, cohort in self._pending
            )
            while (
                self._outcome_history_resets
                and self._outcome_history_resets[0][0]
                <= oldest_required_epoch
            ):
                self._outcome_history_resets.popleft()
        else:
            self._outcome_history_resets.clear()

    def _record(
        self,
        pending: _PendingEvaluation,
        *,
        actual: Optional[ObservedPrice],
        matured_ms: int,
        outcome_status: str,
        outcome_invalid_reasons: tuple[str, ...],
    ) -> ShadowEvaluationRecord:
        signal = pending.signal
        projection = signal.projection
        chainlink = signal.chainlink_now
        futures_now = signal.futures_now
        reference = (
            signal.anchor.futures_reference
            if signal.anchor is not None
            else None
        )
        projected = (
            projection.projected_chainlink if projection is not None else None
        )
        actual_value = actual.value if actual is not None else None
        chainlink_value = chainlink.value if chainlink is not None else None
        return ShadowEvaluationRecord(
            selection_schema_version=(
                self.provenance.selection_schema_version
            ),
            selection_policy_version=(
                self.provenance.policy_version
            ),
            selection_fingerprint_sha256=(
                self.provenance.selection_fingerprint_sha256
            ),
            selection_artifact_sha256=(
                self.provenance.selection_artifact_sha256
            ),
            selection_evidence_end_ms=(
                self.provenance.evidence_end_ms
            ),
            model_version=signal.model_version,
            beta=pending.model.beta,
            generated_ms=signal.generated_ms,
            target_ms=pending.target_ms,
            matured_ms=matured_ms,
            horizon_ms=signal.horizon_ms,
            valid=signal.valid,
            status=signal.status,
            invalid_reasons=signal.invalid_reasons,
            state=signal.state,
            outcome_status=outcome_status,
            outcome_invalid_reasons=outcome_invalid_reasons,
            market_id=pending.market.market_id,
            market_start_ms=pending.market.market_start_ms,
            market_end_ms=pending.market.market_end_ms,
            ms_to_market_end=pending.ms_to_market_end,
            full_horizon_before_market_end=(
                signal.full_horizon_before_market_end
            ),
            chainlink_at_forecast=chainlink_value,
            chainlink_at_forecast_source_timestamp_ms=(
                chainlink.source_timestamp_ms if chainlink is not None else None
            ),
            chainlink_at_forecast_received_ms=(
                chainlink.received_ms if chainlink is not None else None
            ),
            projected_chainlink=projected,
            pending_move=(
                projection.pending_move if projection is not None else None
            ),
            pending_move_bps=(
                projection.pending_move_bps if projection is not None else None
            ),
            direction=(projection.direction if projection is not None else None),
            futures_now=(futures_now.value if futures_now is not None else None),
            futures_now_source_timestamp_ms=(
                futures_now.source_timestamp_ms
                if futures_now is not None
                else None
            ),
            futures_now_received_ms=(
                futures_now.received_ms if futures_now is not None else None
            ),
            futures_reference=(reference.value if reference is not None else None),
            futures_reference_source_timestamp_ms=(
                reference.source_timestamp_ms if reference is not None else None
            ),
            futures_reference_received_ms=(
                reference.received_ms if reference is not None else None
            ),
            futures_reference_target_ms=signal.futures_reference_target_ms,
            futures_reference_gap_ms=signal.futures_reference_gap_ms,
            futures_received_age_ms=signal.futures_received_age_ms,
            chainlink_received_age_ms=signal.chainlink_received_age_ms,
            actual_chainlink=actual_value,
            actual_chainlink_source_timestamp_ms=(
                actual.source_timestamp_ms if actual is not None else None
            ),
            actual_chainlink_received_ms=(
                actual.received_ms if actual is not None else None
            ),
            actual_chainlink_age_at_target_ms=(
                pending.target_ms - actual.received_ms
                if actual is not None
                else None
            ),
            forecast_error=(
                projected - actual_value
                if projected is not None and actual_value is not None
                else None
            ),
            baseline_error=(
                chainlink_value - actual_value
                if chainlink_value is not None and actual_value is not None
                else None
            ),
        )


class ShadowEvaluationBackend(Protocol):
    async def write_evaluation_cohorts(
        self,
        cohorts: Sequence[ShadowEvaluationCohort],
    ) -> ShadowEvaluationCohortWriteResult:
        ...

    async def delete_expired(
        self,
        *,
        cutoff_generated_ms: int,
        limit: int,
    ) -> int:
        ...

    async def close(self) -> None:
        ...


ShadowEvaluationBackendFactoryResult = Union[
    ShadowEvaluationBackend,
    Awaitable[ShadowEvaluationBackend],
]
ShadowEvaluationBackendFactory = Callable[
    [],
    ShadowEvaluationBackendFactoryResult,
]


@dataclass(frozen=True)
class ShadowEvaluationOfferResult:
    accepted: bool
    dropped_oldest: bool
    dropped_record: Optional[ShadowEvaluationRecord]
    dropped_records: tuple[ShadowEvaluationRecord, ...]
    queue_depth: int
    queue_cohort_depth: int


@dataclass
class ShadowEvaluationCounters:
    records_offered_total: int = 0
    records_enqueued_total: int = 0
    records_persisted_total: int = 0
    records_dropped_total: int = 0
    records_rejected_total: int = 0
    records_deferred_total: int = 0
    cohorts_offered_total: int = 0
    cohorts_enqueued_total: int = 0
    cohorts_persisted_total: int = 0
    cohorts_dropped_total: int = 0
    cohorts_rejected_total: int = 0
    cohorts_deferred_total: int = 0
    batches_succeeded_total: int = 0
    batches_failed_total: int = 0
    backend_creation_failures_total: int = 0
    cleanup_runs_total: int = 0
    cleanup_failures_total: int = 0
    records_deleted_total: int = 0
    queue_high_water: int = 0
    queue_cohort_high_water: int = 0
    last_batch_rows: int = 0


def _consume_task_result(task: "asyncio.Task[Any]") -> None:
    if task.cancelled():
        return
    try:
        task.result()
    except Exception:
        LOGGER.exception("shadow_evaluation_writer_task_failed")


class ShadowEvaluationWriterRuntime:
    """Bounded best-effort writer for matured evaluation records."""

    def __init__(
        self,
        *,
        backend_factory: ShadowEvaluationBackendFactory,
        candidate_model_versions: Sequence[str],
        queue_max_records: int,
        batch_max_rows: int,
        flush_ms: int,
        shutdown_timeout_ms: int,
        retry_ms: int = 5_000,
        retention_ms: Optional[int] = None,
        cleanup_interval_ms: int = 60_000,
        cleanup_batch_rows: int = 1_000,
        counters: Optional[ShadowEvaluationCounters] = None,
        now_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        if not callable(backend_factory):
            raise TypeError("backend_factory must be callable")
        self._candidate_model_versions = tuple(candidate_model_versions)
        if not self._candidate_model_versions:
            raise ValueError("candidate_model_versions must not be empty")
        if not all(
            isinstance(version, str) and version.strip()
            for version in self._candidate_model_versions
        ):
            raise TypeError(
                "candidate_model_versions must contain non-empty strings"
            )
        if len(set(self._candidate_model_versions)) != len(
            self._candidate_model_versions
        ):
            raise ValueError("candidate_model_versions must be unique")
        for value, field_name in (
            (queue_max_records, "queue_max_records"),
            (batch_max_rows, "batch_max_rows"),
            (flush_ms, "flush_ms"),
            (retry_ms, "retry_ms"),
            (shutdown_timeout_ms, "shutdown_timeout_ms"),
            (cleanup_interval_ms, "cleanup_interval_ms"),
            (cleanup_batch_rows, "cleanup_batch_rows"),
        ):
            _require_positive_int(value, field_name)
        if batch_max_rows > queue_max_records:
            raise ValueError("batch_max_rows cannot exceed queue capacity")
        candidate_count = len(self._candidate_model_versions)
        if queue_max_records < candidate_count:
            raise ValueError(
                "queue capacity must fit one complete evaluation cohort"
            )
        if batch_max_rows < candidate_count:
            raise ValueError(
                "batch_max_rows must fit one complete evaluation cohort"
            )
        if retention_ms is not None:
            _require_positive_int(retention_ms, "retention_ms")
            if cleanup_batch_rows < candidate_count:
                raise ValueError(
                    "cleanup_batch_rows must fit one complete evaluation cohort"
                )
        if not callable(now_ms):
            raise TypeError("now_ms must be callable")

        self.counters = counters or ShadowEvaluationCounters()
        self._backend_factory = backend_factory
        self._backend: Optional[ShadowEvaluationBackend] = None
        self._queue_max_records = queue_max_records
        self._batch_max_rows = batch_max_rows
        self._flush_seconds = flush_ms / 1_000
        self._retry_seconds = retry_ms / 1_000
        self._shutdown_timeout_seconds = shutdown_timeout_ms / 1_000
        self._retention_ms = retention_ms
        self._cleanup_interval_ms = cleanup_interval_ms
        self._cleanup_batch_rows = cleanup_batch_rows
        self._now_ms = now_ms
        self._next_cleanup_ms: Optional[int] = None
        self._retry_not_before: Optional[float] = None
        self._buffer: Deque[ShadowEvaluationCohort] = deque()
        self._buffered_rows = 0
        self._available: Optional[asyncio.Event] = None
        self._writer_task: Optional["asyncio.Task[None]"] = None
        self._active_batch: Optional[list[ShadowEvaluationCohort]] = None
        self._queue_overflow_drops = 0
        self._accepting_offers = True
        self._stop_requested = False
        self._shutdown_accounted = False
        self._shutdown_summary_logged = False

    @property
    def started(self) -> bool:
        return self._writer_task is not None

    @property
    def queue_depth(self) -> int:
        return self._buffered_rows

    @property
    def queue_cohort_depth(self) -> int:
        return len(self._buffer)

    def start(self) -> "asyncio.Task[None]":
        if self._stop_requested:
            raise RuntimeError("shadow evaluation writer is closed")
        if self._writer_task is None:
            self._available = asyncio.Event()
            if self._buffer:
                self._available.set()
            self._writer_task = asyncio.create_task(self._writer_loop())
            self._writer_task.add_done_callback(_consume_task_result)
        return self._writer_task

    def offer_nowait(
        self,
        record: ShadowEvaluationRecord,
    ) -> ShadowEvaluationOfferResult:
        """Compatibility entry point for a configured one-model cohort."""

        return self.offer_cohort_nowait((record,))

    def offer_cohort_nowait(
        self,
        records: Sequence[ShadowEvaluationRecord],
    ) -> ShadowEvaluationOfferResult:
        cohort = ShadowEvaluationCohort(tuple(records))
        cohort.require_model_versions(self._candidate_model_versions)
        offered_count = cohort.row_count
        self.counters.records_offered_total += offered_count
        self.counters.cohorts_offered_total += 1
        if not self._accepting_offers:
            self.counters.records_dropped_total += offered_count
            self.counters.cohorts_dropped_total += 1
            self._log_queue_drop(
                reason="writer_closed",
                dropped_cohort=cohort,
                occurrence=self.counters.records_dropped_total,
            )
            return ShadowEvaluationOfferResult(
                accepted=False,
                dropped_oldest=False,
                dropped_record=None,
                dropped_records=(),
                queue_depth=self._buffered_rows,
                queue_cohort_depth=len(self._buffer),
            )

        dropped_cohorts: list[ShadowEvaluationCohort] = []
        while (
            self._buffer
            and self._buffered_rows + cohort.row_count
            > self._queue_max_records
        ):
            dropped = self._buffer.popleft()
            self._buffered_rows -= dropped.row_count
            dropped_cohorts.append(dropped)
            self._record_queue_overflow(
                dropped,
                reason="queue_full_drop_oldest",
            )
        self._buffer.append(cohort)
        self._buffered_rows += cohort.row_count
        self.counters.records_enqueued_total += offered_count
        self.counters.cohorts_enqueued_total += 1
        self.counters.queue_high_water = max(
            self.counters.queue_high_water,
            self._buffered_rows,
        )
        self.counters.queue_cohort_high_water = max(
            self.counters.queue_cohort_high_water,
            len(self._buffer),
        )
        if self._available is not None:
            self._available.set()
        dropped_records = tuple(
            record
            for dropped_cohort in dropped_cohorts
            for record in dropped_cohort.records
        )
        return ShadowEvaluationOfferResult(
            accepted=True,
            dropped_oldest=bool(dropped_cohorts),
            dropped_record=(dropped_records[0] if dropped_records else None),
            dropped_records=dropped_records,
            queue_depth=self._buffered_rows,
            queue_cohort_depth=len(self._buffer),
        )

    def _record_queue_overflow(
        self,
        dropped_cohort: ShadowEvaluationCohort,
        *,
        reason: str,
    ) -> None:
        self.counters.records_dropped_total += dropped_cohort.row_count
        self.counters.cohorts_dropped_total += 1
        self._queue_overflow_drops += 1
        self._log_queue_drop(
            reason=reason,
            dropped_cohort=dropped_cohort,
            occurrence=self._queue_overflow_drops,
        )

    def _log_queue_drop(
        self,
        *,
        reason: str,
        dropped_cohort: Optional[ShadowEvaluationCohort],
        occurrence: int,
    ) -> None:
        if occurrence != 1 and occurrence % QUEUE_DROP_LOG_EVERY != 0:
            return
        LOGGER.warning(
            "shadow_signal_evaluation_queue_drop",
            extra={
                "event": "shadow_signal_evaluation_queue_drop",
                "reason": reason,
                "records_dropped_total": (
                    self.counters.records_dropped_total
                ),
                "queue_overflow_drops_total": self._queue_overflow_drops,
                "queue_overflow_cohorts_total": self._queue_overflow_drops,
                "cohorts_dropped_total": self.counters.cohorts_dropped_total,
                "queue_depth": self._buffered_rows,
                "queue_cohort_depth": len(self._buffer),
                "queue_max_records": self._queue_max_records,
                "dropped_model_versions": (
                    dropped_cohort.model_versions
                    if dropped_cohort is not None
                    else None
                ),
                "dropped_generated_ms": (
                    dropped_cohort.identity.generated_ms
                    if dropped_cohort is not None
                    else None
                ),
            },
        )

    async def close(self, *, timeout_ms: Optional[int] = None) -> None:
        self._accepting_offers = False
        self._stop_requested = True
        if self._available is not None:
            self._available.set()
        if self._writer_task is None:
            self._discard_remaining_once()
            self._log_shutdown_summary()
            return
        if self._writer_task.done():
            self._discard_remaining_once()
            self._log_shutdown_summary()
            return

        timeout_seconds = (
            self._shutdown_timeout_seconds
            if timeout_ms is None
            else max(0.001, timeout_ms / 1_000)
        )
        done, _pending = await asyncio.wait(
            {self._writer_task},
            timeout=timeout_seconds,
        )
        if self._writer_task not in done:
            self._writer_task.cancel()
            await asyncio.wait(
                {self._writer_task},
                timeout=min(0.1, max(0.01, timeout_seconds)),
            )
        self._discard_remaining_once()
        self._log_shutdown_summary()

    def _discard_remaining_once(self) -> None:
        if self._shutdown_accounted:
            return
        dropped = self._buffered_rows
        dropped_cohorts = len(self._buffer)
        self._buffer.clear()
        self._buffered_rows = 0
        self.counters.records_dropped_total += dropped
        self.counters.cohorts_dropped_total += dropped_cohorts
        self._shutdown_accounted = True

    def _log_shutdown_summary(self) -> None:
        if self._shutdown_summary_logged:
            return
        self._shutdown_summary_logged = True
        LOGGER.info(
            "shadow_signal_evaluation_writer_closed",
            extra={
                "event": "shadow_signal_evaluation_writer_closed",
                "records_offered_total": self.counters.records_offered_total,
                "records_persisted_total": (
                    self.counters.records_persisted_total
                ),
                "records_dropped_total": self.counters.records_dropped_total,
                "records_rejected_total": (
                    self.counters.records_rejected_total
                ),
                "records_deferred_total": (
                    self.counters.records_deferred_total
                ),
                "cohorts_offered_total": self.counters.cohorts_offered_total,
                "cohorts_enqueued_total": (
                    self.counters.cohorts_enqueued_total
                ),
                "cohorts_persisted_total": (
                    self.counters.cohorts_persisted_total
                ),
                "cohorts_dropped_total": self.counters.cohorts_dropped_total,
                "cohorts_rejected_total": (
                    self.counters.cohorts_rejected_total
                ),
                "cohorts_deferred_total": (
                    self.counters.cohorts_deferred_total
                ),
                "batches_succeeded_total": (
                    self.counters.batches_succeeded_total
                ),
                "batches_failed_total": self.counters.batches_failed_total,
                "backend_creation_failures_total": (
                    self.counters.backend_creation_failures_total
                ),
                "cleanup_failures_total": (
                    self.counters.cleanup_failures_total
                ),
                "records_deleted_total": self.counters.records_deleted_total,
                "queue_high_water": self.counters.queue_high_water,
                "queue_cohort_high_water": (
                    self.counters.queue_cohort_high_water
                ),
                "queue_depth": self._buffered_rows,
                "queue_cohort_depth": len(self._buffer),
                "active_batch_rows": (
                    0
                    if self._active_batch is None
                    else sum(
                        cohort.row_count for cohort in self._active_batch
                    )
                ),
                "active_batch_cohorts": (
                    0 if self._active_batch is None else len(self._active_batch)
                ),
                "writer_task_done": (
                    self._writer_task is None or self._writer_task.done()
                ),
            },
        )

    async def _ensure_backend(self) -> ShadowEvaluationBackend:
        if self._backend is not None:
            return self._backend
        try:
            result = self._backend_factory()
            if inspect.isawaitable(result):
                result = await result
        except asyncio.CancelledError:
            raise
        except Exception:
            self.counters.backend_creation_failures_total += 1
            raise
        self._backend = result
        return result

    async def _wait_for_item(self, timeout: float) -> bool:
        if self._buffer:
            return True
        event = self._available
        if event is None:
            return False
        event.clear()
        if self._buffer:
            event.set()
            return True
        try:
            await asyncio.wait_for(event.wait(), timeout=max(0.001, timeout))
        except asyncio.TimeoutError:
            return False
        return bool(self._buffer)

    async def _take_batch(self) -> list[ShadowEvaluationCohort]:
        if not await self._wait_for_item(self._flush_seconds):
            return []
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._flush_seconds
        batch: list[ShadowEvaluationCohort] = []
        batch_identities: set[ShadowEvaluationCohortIdentity] = set()
        batch_rows = 0
        try:
            while batch_rows < self._batch_max_rows:
                duplicate_identity_blocked = False
                while self._buffer:
                    next_cohort = self._buffer[0]
                    if (
                        batch_rows + next_cohort.row_count
                        > self._batch_max_rows
                    ):
                        break
                    # One exact identity partition cannot distinguish two
                    # occurrences, so flush duplicates in separate calls.
                    if next_cohort.identity in batch_identities:
                        duplicate_identity_blocked = True
                        break
                    batch.append(self._buffer.popleft())
                    batch_identities.add(next_cohort.identity)
                    batch_rows += next_cohort.row_count
                    self._buffered_rows -= next_cohort.row_count
                if (
                    batch_rows >= self._batch_max_rows
                    or self._stop_requested
                    or duplicate_identity_blocked
                    or (
                        self._buffer
                        and batch_rows + self._buffer[0].row_count
                        > self._batch_max_rows
                    )
                ):
                    break
                remaining = deadline - loop.time()
                if remaining <= 0 or not await self._wait_for_item(remaining):
                    break
        except asyncio.CancelledError:
            self.counters.records_dropped_total += batch_rows
            self.counters.cohorts_dropped_total += len(batch)
            raise
        return batch

    async def _drop_backend(self) -> None:
        backend = self._backend
        self._backend = None
        if backend is None:
            return
        try:
            await backend.close()
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception("shadow_evaluation_backend_close_failed")

    async def _write_batch(
        self,
        batch: Sequence[ShadowEvaluationCohort],
    ) -> None:
        batch_rows = sum(cohort.row_count for cohort in batch)
        self.counters.last_batch_rows = batch_rows
        try:
            batch_by_identity: dict[
                ShadowEvaluationCohortIdentity,
                ShadowEvaluationCohort,
            ] = {}
            for cohort in batch:
                if cohort.identity in batch_by_identity:
                    raise RuntimeError(
                        "shadow evaluation batch contains duplicate cohort "
                        "identities"
                    )
                batch_by_identity[cohort.identity] = cohort
            backend = await self._ensure_backend()
            write_result = await backend.write_evaluation_cohorts(batch)
            if not isinstance(
                write_result,
                ShadowEvaluationCohortWriteResult,
            ):
                raise RuntimeError(
                    "shadow evaluation backend returned an invalid result"
                )
            input_cohort_ids = frozenset(batch_by_identity)
            if write_result.cohort_ids != input_cohort_ids:
                raise RuntimeError(
                    "shadow evaluation backend result does not account "
                    "exactly for the complete cohort batch"
                )
            persisted_cohorts = tuple(
                cohort
                for cohort in batch
                if cohort.identity in write_result.persisted_cohort_ids
            )
            rejected_cohorts = tuple(
                cohort
                for cohort in batch
                if cohort.identity in write_result.rejected_cohort_ids
            )
            deferred_cohorts = tuple(
                cohort
                for cohort in batch
                if cohort.identity in write_result.deferred_cohort_ids
            )
            persisted = sum(
                cohort.row_count for cohort in persisted_cohorts
            )
            rejected = sum(cohort.row_count for cohort in rejected_cohorts)
            deferred = sum(cohort.row_count for cohort in deferred_cohorts)
        except asyncio.CancelledError:
            self.counters.records_dropped_total += batch_rows
            self.counters.cohorts_dropped_total += len(batch)
            raise
        except Exception:
            self.counters.batches_failed_total += 1
            self._requeue_failed_batch(batch)
            LOGGER.exception(
                "shadow_evaluation_batch_failed",
                extra={
                    "event": "shadow_evaluation_batch_failed",
                    "batch_rows": batch_rows,
                    "queue_depth": self._buffered_rows,
                    "records_dropped_total": (
                        self.counters.records_dropped_total
                    ),
                },
            )
            self._retry_not_before = (
                asyncio.get_running_loop().time() + self._retry_seconds
            )
            await self._drop_backend()
            return
        self._retry_not_before = None
        self.counters.batches_succeeded_total += 1
        previous_rejected_total = self.counters.records_rejected_total
        self.counters.records_persisted_total += persisted
        self.counters.records_rejected_total += rejected
        self.counters.records_deferred_total += deferred
        self.counters.records_dropped_total += rejected
        self.counters.cohorts_persisted_total += len(persisted_cohorts)
        self.counters.cohorts_rejected_total += len(rejected_cohorts)
        self.counters.cohorts_deferred_total += len(deferred_cohorts)
        self.counters.cohorts_dropped_total += len(rejected_cohorts)
        if rejected and (
            previous_rejected_total == 0
            or (
                self.counters.records_rejected_total // REJECTION_LOG_EVERY
                > previous_rejected_total // REJECTION_LOG_EVERY
            )
        ):
            LOGGER.error(
                "shadow_evaluation_batch_records_rejected",
                extra={
                    "event": "shadow_evaluation_batch_records_rejected",
                    "batch_rows": batch_rows,
                    "records_persisted": persisted,
                    "records_rejected": rejected,
                    "records_deferred": deferred,
                    "cohorts_persisted": len(persisted_cohorts),
                    "cohorts_rejected": len(rejected_cohorts),
                    "cohorts_deferred": len(deferred_cohorts),
                    "records_rejected_total": (
                        self.counters.records_rejected_total
                    ),
                    "queue_depth": self._buffered_rows,
                },
            )
        if deferred_cohorts:
            self._requeue_deferred_cohorts(deferred_cohorts)
        await self._maybe_cleanup(backend)

    def _requeue_failed_batch(
        self,
        batch: Sequence[ShadowEvaluationCohort],
    ) -> None:
        # The failed batch is older than anything currently queued. Prepending
        # it preserves order; if concurrent offers filled the queue while the
        # write was in flight, discard only whole oldest cohorts.
        self._buffer.extendleft(reversed(batch))
        self._buffered_rows += sum(cohort.row_count for cohort in batch)
        while self._buffered_rows > self._queue_max_records:
            dropped = self._buffer.popleft()
            self._buffered_rows -= dropped.row_count
            self._record_queue_overflow(
                dropped,
                reason="failed_batch_requeue_overflow",
            )
        self._update_queue_high_water()
        if self._available is not None and self._buffer:
            self._available.set()

    def _requeue_deferred_cohorts(
        self,
        cohorts: Sequence[ShadowEvaluationCohort],
    ) -> None:
        self._buffer.extendleft(reversed(cohorts))
        self._buffered_rows += sum(cohort.row_count for cohort in cohorts)
        while self._buffered_rows > self._queue_max_records:
            dropped = self._buffer.popleft()
            self._buffered_rows -= dropped.row_count
            self._record_queue_overflow(
                dropped,
                reason="deferred_cohort_requeue_overflow",
            )
        self._update_queue_high_water()
        if self._available is not None and self._buffer:
            self._available.set()

    def _update_queue_high_water(self) -> None:
        self.counters.queue_high_water = max(
            self.counters.queue_high_water,
            self._buffered_rows,
        )
        self.counters.queue_cohort_high_water = max(
            self.counters.queue_cohort_high_water,
            len(self._buffer),
        )

    async def _maybe_cleanup(self, backend: ShadowEvaluationBackend) -> None:
        if self._retention_ms is None:
            return
        now_ms = self._now_ms()
        if self._next_cleanup_ms is not None and now_ms < self._next_cleanup_ms:
            return
        self._next_cleanup_ms = now_ms + self._cleanup_interval_ms
        self.counters.cleanup_runs_total += 1
        try:
            deleted = await backend.delete_expired(
                cutoff_generated_ms=max(0, now_ms - self._retention_ms),
                limit=self._cleanup_batch_rows,
            )
            _require_non_negative_int(deleted, "deleted")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.counters.cleanup_failures_total += 1
            LOGGER.exception("shadow_evaluation_cleanup_failed")
            return
        self.counters.records_deleted_total += deleted

    async def _writer_loop(self) -> None:
        try:
            while True:
                if self._stop_requested and not self._buffer:
                    break
                if self._retry_not_before is not None:
                    retry_delay = (
                        self._retry_not_before
                        - asyncio.get_running_loop().time()
                    )
                    if retry_delay > 0:
                        if self._stop_requested:
                            break
                        await asyncio.sleep(retry_delay)
                    self._retry_not_before = None
                batch = await self._take_batch()
                if not batch:
                    continue
                self._active_batch = batch
                try:
                    await self._write_batch(batch)
                finally:
                    self._active_batch = None
        finally:
            await self._drop_backend()
