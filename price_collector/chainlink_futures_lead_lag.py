from __future__ import annotations

import argparse
import bisect
import csv
import hashlib
import json
import sys
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from price_collector.market import market_for_sample_second


BASIS_POINTS = Decimal("10000")
DEFAULT_MODEL_VERSION = "catchup_ratio_l3000_b100"
DEFAULT_PATTERN = (
    "btc_5m_market_*_shadow_evaluations_"
    "catchup_ratio_l3000_b100.json"
)


class LeadLagAnalysisError(RuntimeError):
    pass


@dataclass(frozen=True)
class SelectionIdentity:
    schema_version: int
    policy_version: str
    evidence_end_ms: int
    fingerprint_sha256: str
    artifact_sha256: str


@dataclass(frozen=True)
class PriceObservation:
    value: Decimal
    source_timestamp_ms: Optional[int]
    received_ms: int
    block_id: int


@dataclass(frozen=True)
class SnapshotRow:
    generated_ms: int
    chainlink: PriceObservation
    futures: PriceObservation


@dataclass(frozen=True)
class InputFileRecord:
    path: str
    sha256: str
    market_id: int
    attempts: int
    valid_forecasts: int
    scored: int
    invalid: int


@dataclass(frozen=True)
class LoadedObservationTape:
    chainlink: Tuple[PriceObservation, ...]
    futures: Tuple[PriceObservation, ...]
    market_ids: Tuple[int, ...]
    selection_identity: SelectionIdentity
    files: Tuple[InputFileRecord, ...]
    valid_snapshot_rows: int
    continuous_blocks: int


@dataclass(frozen=True)
class LeadLagSample:
    lag_ms: int
    block_id: int
    market_id: int
    previous_chainlink_time_ms: int
    current_chainlink_time_ms: int
    previous_chainlink_value: Decimal
    current_chainlink_value: Decimal
    previous_futures_time_ms: int
    current_futures_time_ms: int
    previous_futures_value: Decimal
    current_futures_value: Decimal
    previous_futures_age_ms: int
    current_futures_age_ms: int
    chainlink_move_bps: Decimal
    futures_move_bps: Decimal
    signed_error_bps: Decimal
    absolute_error_bps: Decimal


@dataclass(frozen=True)
class LagScore:
    lag_ms: int
    intervals: int
    mae_bps: Decimal
    median_absolute_error_bps: Decimal
    rmse_bps: Decimal
    mean_signed_error_bps: Decimal
    correlation: Optional[Decimal]
    directional_intervals: int
    directional_agreement: Optional[Decimal]
    equal_market_mae_bps: Optional[Decimal]
    qualifying_markets: int


@dataclass(frozen=True)
class MarketBestLag:
    market_id: int
    intervals: int
    best_lag_ms: int
    best_mae_bps: Decimal


@dataclass(frozen=True)
class LeadLagAnalysis:
    time_basis: str
    candidate_lags_ms: Tuple[int, ...]
    common_intervals: int
    lag_scores: Tuple[LagScore, ...]
    market_best_lags: Tuple[MarketBestLag, ...]
    samples_by_lag: Mapping[int, Tuple[LeadLagSample, ...]]

    @property
    def best_score(self) -> LagScore:
        return min(self.lag_scores, key=lambda score: (score.mae_bps, score.lag_ms))

    @property
    def equal_market_best_score(self) -> Optional[LagScore]:
        eligible = [
            score
            for score in self.lag_scores
            if score.equal_market_mae_bps is not None
        ]
        if not eligible:
            return None
        return min(
            eligible,
            key=lambda score: (score.equal_market_mae_bps, score.lag_ms),
        )


def _mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise LeadLagAnalysisError(f"{field} must be an object")
    return value


def _list(value: Any, field: str) -> List[Any]:
    if not isinstance(value, list):
        raise LeadLagAnalysisError(f"{field} must be an array")
    return value


def _integer(value: Any, field: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise LeadLagAnalysisError(f"{field} must be an integer")
    if value < minimum:
        raise LeadLagAnalysisError(f"{field} must be at least {minimum}")
    return value


def _optional_integer(value: Any, field: str) -> Optional[int]:
    if value is None:
        return None
    return _integer(value, field)


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise LeadLagAnalysisError(f"{field} must be non-empty text")
    return value


def _boolean(value: Any, field: str) -> bool:
    if not isinstance(value, bool):
        raise LeadLagAnalysisError(f"{field} must be a boolean")
    return value


def _decimal(value: Any, field: str) -> Decimal:
    if not isinstance(value, str) or not value:
        raise LeadLagAnalysisError(f"{field} must be a decimal string")
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise LeadLagAnalysisError(f"{field} must be a decimal string") from exc
    if not parsed.is_finite() or parsed <= 0:
        raise LeadLagAnalysisError(f"{field} must be a positive finite decimal")
    return parsed


def _selection_identity(value: Any, field: str) -> SelectionIdentity:
    identity = _mapping(value, field)
    return SelectionIdentity(
        schema_version=_integer(identity.get("schema_version"), f"{field}.schema_version", 1),
        policy_version=_text(identity.get("policy_version"), f"{field}.policy_version"),
        evidence_end_ms=_integer(identity.get("evidence_end_ms"), f"{field}.evidence_end_ms"),
        fingerprint_sha256=_text(
            identity.get("fingerprint_sha256"),
            f"{field}.fingerprint_sha256",
        ),
        artifact_sha256=_text(
            identity.get("artifact_sha256"),
            f"{field}.artifact_sha256",
        ),
    )


def _point_selection_identity(point: Mapping[str, Any], field: str) -> SelectionIdentity:
    return SelectionIdentity(
        schema_version=_integer(
            point.get("selection_schema_version"),
            f"{field}.selection_schema_version",
            1,
        ),
        policy_version=_text(
            point.get("selection_policy_version"),
            f"{field}.selection_policy_version",
        ),
        evidence_end_ms=_integer(
            point.get("selection_evidence_end_ms"),
            f"{field}.selection_evidence_end_ms",
        ),
        fingerprint_sha256=_text(
            point.get("selection_fingerprint_sha256"),
            f"{field}.selection_fingerprint_sha256",
        ),
        artifact_sha256=_text(
            point.get("selection_artifact_sha256"),
            f"{field}.selection_artifact_sha256",
        ),
    )


def _observation_from_point(
    point: Mapping[str, Any],
    prefix: str,
    field: str,
) -> PriceObservation:
    value = _decimal(point.get(prefix), f"{field}.{prefix}")
    source_timestamp_ms = _optional_integer(
        point.get(f"{prefix}_source_timestamp_ms"),
        f"{field}.{prefix}_source_timestamp_ms",
    )
    received_ms = _integer(
        point.get(f"{prefix}_received_ms"),
        f"{field}.{prefix}_received_ms",
    )
    return PriceObservation(
        value=value,
        source_timestamp_ms=source_timestamp_ms,
        received_ms=received_ms,
        block_id=-1,
    )


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_observation_tape(
    input_dir: Path,
    pattern: str = DEFAULT_PATTERN,
    expected_model_version: str = DEFAULT_MODEL_VERSION,
    max_generated_gap_ms: int = 1_500,
) -> LoadedObservationTape:
    if max_generated_gap_ms <= 0:
        raise ValueError("max_generated_gap_ms must be positive")

    paths = sorted(input_dir.glob(pattern))
    if not paths:
        raise LeadLagAnalysisError(f"no files matched {input_dir / pattern}")

    market_ids: List[int] = []
    input_files: List[InputFileRecord] = []
    rows_by_generated_ms: Dict[int, SnapshotRow] = {}
    common_identity: Optional[SelectionIdentity] = None
    common_model_metadata: Optional[Tuple[str, int, int, Decimal]] = None

    for path in paths:
        try:
            payload = json.loads(path.read_text(encoding="utf-8-sig"))
        except (OSError, json.JSONDecodeError) as exc:
            raise LeadLagAnalysisError(f"failed to read {path}: {exc}") from exc
        payload = _mapping(payload, str(path))
        if "export" in payload:
            raise LeadLagAnalysisError(f"{path} is a rounded export; use the canonical route")
        if _integer(payload.get("schema_version"), f"{path}.schema_version") != 2:
            raise LeadLagAnalysisError(f"{path} has an unsupported schema_version")

        market = _mapping(payload.get("market"), f"{path}.market")
        market_id = _integer(market.get("market_id"), f"{path}.market.market_id")
        market_start_ms = _integer(
            market.get("market_start_ms"),
            f"{path}.market.market_start_ms",
        )
        market_end_ms = _integer(
            market.get("market_end_ms"),
            f"{path}.market.market_end_ms",
        )
        expected_window = market_for_sample_second(market_start_ms)
        if (
            expected_window.market_id != market_id
            or expected_window.market_start_ms != market_start_ms
            or expected_window.market_end_ms != market_end_ms
        ):
            raise LeadLagAnalysisError(f"{path} has inconsistent market metadata")
        if market_id in market_ids:
            raise LeadLagAnalysisError(f"duplicate market_id {market_id}")
        market_ids.append(market_id)

        coverage = _mapping(payload.get("coverage"), f"{path}.coverage")
        if not _boolean(
            coverage.get("market_window_elapsed"),
            f"{path}.coverage.market_window_elapsed",
        ):
            raise LeadLagAnalysisError(f"{path} is not a completed market report")
        attempts = _integer(coverage.get("attempts"), f"{path}.coverage.attempts")
        valid_forecasts = _integer(
            coverage.get("valid_forecasts"),
            f"{path}.coverage.valid_forecasts",
        )
        scored = _integer(coverage.get("scored"), f"{path}.coverage.scored")
        invalid = _integer(coverage.get("invalid"), f"{path}.coverage.invalid")
        if valid_forecasts + invalid != attempts:
            raise LeadLagAnalysisError(
                f"{path} coverage valid_forecasts + invalid differs from attempts"
            )

        model = _mapping(payload.get("model"), f"{path}.model")
        model_version = _text(model.get("model_version"), f"{path}.model.model_version")
        horizon_ms = _integer(model.get("horizon_ms"), f"{path}.model.horizon_ms", 1)
        cadence_ms = _integer(
            model.get("evaluation_cadence_ms"),
            f"{path}.model.evaluation_cadence_ms",
            1,
        )
        model_beta = _decimal(model.get("beta"), f"{path}.model.beta")
        metadata = (model_version, horizon_ms, cadence_ms, model_beta)
        if model_version != expected_model_version:
            raise LeadLagAnalysisError(
                f"{path} uses model {model_version}, expected {expected_model_version}"
            )
        if common_model_metadata is None:
            common_model_metadata = metadata
        elif metadata != common_model_metadata:
            raise LeadLagAnalysisError("input reports mix model metadata")

        identities = _list(
            model.get("selection_identities"),
            f"{path}.model.selection_identities",
        )
        if len(identities) != 1:
            raise LeadLagAnalysisError(
                f"{path} must contain exactly one selection identity"
            )
        report_identity = _selection_identity(
            identities[0],
            f"{path}.model.selection_identities[0]",
        )
        if common_identity is None:
            common_identity = report_identity
        elif report_identity != common_identity:
            raise LeadLagAnalysisError("input reports mix selection identities")

        points = _list(payload.get("points"), f"{path}.points")
        if not points:
            raise LeadLagAnalysisError(f"{path} contains no evaluation points")
        if len(points) != attempts:
            raise LeadLagAnalysisError(
                f"{path} point count differs from coverage.attempts"
            )

        counted_valid_forecasts = 0
        for point_index, raw_point in enumerate(points):
            field = f"{path}.points[{point_index}]"
            point = _mapping(raw_point, field)
            if _text(point.get("model_version"), f"{field}.model_version") != model_version:
                raise LeadLagAnalysisError(f"{field} has a different model_version")
            if _decimal(point.get("beta"), f"{field}.beta") != model_beta:
                raise LeadLagAnalysisError(f"{field} has a different beta")
            generated_ms = _integer(point.get("generated_ms"), f"{field}.generated_ms")
            target_ms = _integer(point.get("target_ms"), f"{field}.target_ms")
            point_horizon_ms = _integer(
                point.get("horizon_ms"),
                f"{field}.horizon_ms",
                1,
            )
            if point_horizon_ms != horizon_ms or target_ms != generated_ms + horizon_ms:
                raise LeadLagAnalysisError(f"{field} has inconsistent target timing")
            if not (market_start_ms <= target_ms < market_end_ms):
                raise LeadLagAnalysisError(f"{field}.target_ms is outside its target market")
            if _point_selection_identity(point, field) != report_identity:
                raise LeadLagAnalysisError(f"{field} has a different selection identity")
            if not _boolean(point.get("valid"), f"{field}.valid"):
                continue
            counted_valid_forecasts += 1

            chainlink = _observation_from_point(
                point,
                "chainlink_at_forecast",
                field,
            )
            futures = _observation_from_point(
                point,
                "futures_at_forecast",
                field,
            )
            if chainlink.received_ms > generated_ms or futures.received_ms > generated_ms:
                raise LeadLagAnalysisError(f"{field} uses an input received after generation")
            row = SnapshotRow(
                generated_ms=generated_ms,
                chainlink=chainlink,
                futures=futures,
            )
            prior = rows_by_generated_ms.get(generated_ms)
            if prior is not None and prior != row:
                raise LeadLagAnalysisError(
                    f"conflicting valid snapshot rows at generated_ms={generated_ms}"
                )
            rows_by_generated_ms[generated_ms] = row

        if counted_valid_forecasts != valid_forecasts:
            raise LeadLagAnalysisError(
                f"{path} valid point count differs from coverage.valid_forecasts"
            )

        input_files.append(
            InputFileRecord(
                path=str(path),
                sha256=_file_sha256(path),
                market_id=market_id,
                attempts=attempts,
                valid_forecasts=valid_forecasts,
                scored=scored,
                invalid=invalid,
            )
        )

    ordered_market_ids = tuple(sorted(market_ids))
    for previous, current in zip(ordered_market_ids, ordered_market_ids[1:]):
        if current != previous + 1:
            raise LeadLagAnalysisError(
                f"market IDs are not consecutive: {previous} then {current}"
            )
    if common_identity is None:
        raise LeadLagAnalysisError("selection identity is missing")

    ordered_rows = [rows_by_generated_ms[key] for key in sorted(rows_by_generated_ms)]
    if not ordered_rows:
        raise LeadLagAnalysisError("no valid paired snapshot rows were found")

    chainlink_observations: List[PriceObservation] = []
    futures_observations: List[PriceObservation] = []
    seen_identities: Dict[str, set] = {"chainlink": set(), "futures": set()}
    receive_claims: Dict[str, Dict[int, Tuple[Optional[int], Decimal]]] = {
        "chainlink": {},
        "futures": {},
    }
    last_order: Dict[str, Tuple[int, int, Optional[int]]] = {}
    block_id = -1
    previous_generated_ms: Optional[int] = None

    for row in ordered_rows:
        if (
            previous_generated_ms is None
            or row.generated_ms - previous_generated_ms > max_generated_gap_ms
        ):
            block_id += 1
        previous_generated_ms = row.generated_ms

        for feed, original, destination in (
            ("chainlink", row.chainlink, chainlink_observations),
            ("futures", row.futures, futures_observations),
        ):
            prior_order = last_order.get(feed)
            if prior_order is not None and prior_order[0] == block_id:
                if original.received_ms < prior_order[1]:
                    raise LeadLagAnalysisError(
                        f"{feed} received timestamp regressed inside continuous block"
                    )
                if (
                    original.source_timestamp_ms is not None
                    and prior_order[2] is not None
                    and original.source_timestamp_ms < prior_order[2]
                ):
                    raise LeadLagAnalysisError(
                        f"{feed} source timestamp regressed inside continuous block"
                    )
            last_order[feed] = (
                block_id,
                original.received_ms,
                original.source_timestamp_ms,
            )
            claim = (original.source_timestamp_ms, original.value)
            existing_claim = receive_claims[feed].get(original.received_ms)
            if existing_claim is not None and existing_claim != claim:
                raise LeadLagAnalysisError(
                    f"{feed} has conflicting observations at received_ms="
                    f"{original.received_ms}"
                )
            receive_claims[feed][original.received_ms] = claim
            identity = (
                original.source_timestamp_ms,
                original.received_ms,
                original.value,
            )
            if identity in seen_identities[feed]:
                continue
            seen_identities[feed].add(identity)
            destination.append(
                PriceObservation(
                    value=original.value,
                    source_timestamp_ms=original.source_timestamp_ms,
                    received_ms=original.received_ms,
                    block_id=block_id,
                )
            )

    return LoadedObservationTape(
        chainlink=tuple(chainlink_observations),
        futures=tuple(futures_observations),
        market_ids=ordered_market_ids,
        selection_identity=common_identity,
        files=tuple(sorted(input_files, key=lambda record: record.market_id)),
        valid_snapshot_rows=len(ordered_rows),
        continuous_blocks=block_id + 1,
    )


def _observation_time(observation: PriceObservation, time_basis: str) -> Optional[int]:
    if time_basis == "received":
        return observation.received_ms
    if time_basis == "source":
        return observation.source_timestamp_ms
    raise ValueError("time_basis must be 'received' or 'source'")


def _timelines_by_block(
    observations: Sequence[PriceObservation],
    time_basis: str,
) -> Dict[int, List[Tuple[int, PriceObservation]]]:
    grouped: Dict[int, Dict[int, PriceObservation]] = {}
    for observation in observations:
        timestamp_ms = _observation_time(observation, time_basis)
        if timestamp_ms is None:
            continue
        block = grouped.setdefault(observation.block_id, {})
        prior = block.get(timestamp_ms)
        if prior is None or observation.received_ms >= prior.received_ms:
            block[timestamp_ms] = observation
    return {
        block_id: sorted(points.items(), key=lambda item: item[0])
        for block_id, points in grouped.items()
    }


def _asof(
    timeline: Sequence[Tuple[int, PriceObservation]],
    timeline_times: Sequence[int],
    query_ms: int,
    max_age_ms: int,
) -> Optional[Tuple[int, PriceObservation, int]]:
    index = bisect.bisect_right(timeline_times, query_ms) - 1
    if index < 0:
        return None
    timestamp_ms, observation = timeline[index]
    age_ms = query_ms - timestamp_ms
    if age_ms < 0 or age_ms > max_age_ms:
        return None
    return timestamp_ms, observation, age_ms


def _move_bps(previous: Decimal, current: Decimal) -> Decimal:
    with localcontext() as context:
        context.prec = 50
        return (current / previous - Decimal("1")) * BASIS_POINTS


def _median(values: Sequence[Decimal]) -> Decimal:
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / Decimal("2")


def _correlation(
    chainlink_moves: Sequence[Decimal],
    futures_moves: Sequence[Decimal],
) -> Optional[Decimal]:
    count = len(chainlink_moves)
    if count < 2:
        return None
    with localcontext() as context:
        context.prec = 50
        decimal_count = Decimal(count)
        chainlink_mean = sum(chainlink_moves, Decimal("0")) / decimal_count
        futures_mean = sum(futures_moves, Decimal("0")) / decimal_count
        covariance = sum(
            (
                (chainlink - chainlink_mean)
                * (futures - futures_mean)
            )
            for chainlink, futures in zip(chainlink_moves, futures_moves)
        )
        chainlink_variance = sum(
            (value - chainlink_mean) ** 2 for value in chainlink_moves
        )
        futures_variance = sum(
            (value - futures_mean) ** 2 for value in futures_moves
        )
        denominator_squared = chainlink_variance * futures_variance
        if denominator_squared == 0:
            return None
        return covariance / denominator_squared.sqrt()


def _lag_score(
    lag_ms: int,
    samples: Sequence[LeadLagSample],
    equal_market_mae_bps: Optional[Decimal],
    qualifying_markets: int,
) -> LagScore:
    if not samples:
        raise LeadLagAnalysisError(f"lag {lag_ms} has no samples")
    count = Decimal(len(samples))
    absolute_errors = [sample.absolute_error_bps for sample in samples]
    signed_errors = [sample.signed_error_bps for sample in samples]
    chainlink_moves = [sample.chainlink_move_bps for sample in samples]
    futures_moves = [sample.futures_move_bps for sample in samples]
    directional = [
        sample
        for sample in samples
        if sample.chainlink_move_bps != 0 and sample.futures_move_bps != 0
    ]
    agreement = None
    if directional:
        same_direction = sum(
            1
            for sample in directional
            if (sample.chainlink_move_bps > 0) == (sample.futures_move_bps > 0)
        )
        agreement = Decimal(same_direction) / Decimal(len(directional))
    with localcontext() as context:
        context.prec = 50
        return LagScore(
            lag_ms=lag_ms,
            intervals=len(samples),
            mae_bps=sum(absolute_errors, Decimal("0")) / count,
            median_absolute_error_bps=_median(absolute_errors),
            rmse_bps=(
                sum((error ** 2 for error in signed_errors), Decimal("0")) / count
            ).sqrt(),
            mean_signed_error_bps=sum(signed_errors, Decimal("0")) / count,
            correlation=_correlation(chainlink_moves, futures_moves),
            directional_intervals=len(directional),
            directional_agreement=agreement,
            equal_market_mae_bps=equal_market_mae_bps,
            qualifying_markets=qualifying_markets,
        )


def analyze_lead_lag(
    chainlink_observations: Sequence[PriceObservation],
    futures_observations: Sequence[PriceObservation],
    candidate_lags_ms: Sequence[int],
    time_basis: str = "received",
    max_chainlink_gap_ms: int = 5_000,
    max_futures_asof_age_ms: int = 1_000,
    minimum_market_intervals: int = 30,
) -> LeadLagAnalysis:
    if time_basis not in {"received", "source"}:
        raise ValueError("time_basis must be 'received' or 'source'")
    lags = tuple(sorted(set(candidate_lags_ms)))
    if not lags:
        raise ValueError("candidate_lags_ms must not be empty")
    if max_chainlink_gap_ms <= 0 or max_futures_asof_age_ms < 0:
        raise ValueError("gap limits must be non-negative and Chainlink gap positive")
    if minimum_market_intervals <= 0:
        raise ValueError("minimum_market_intervals must be positive")

    chainlink_by_block = _timelines_by_block(chainlink_observations, time_basis)
    futures_by_block = _timelines_by_block(futures_observations, time_basis)
    samples_by_lag: Dict[int, List[LeadLagSample]] = {lag: [] for lag in lags}

    for block_id, chainlink_timeline in chainlink_by_block.items():
        futures_timeline = futures_by_block.get(block_id)
        if futures_timeline is None or len(chainlink_timeline) < 2:
            continue
        futures_times = [timestamp_ms for timestamp_ms, _ in futures_timeline]

        for previous_point, current_point in zip(
            chainlink_timeline,
            chainlink_timeline[1:],
        ):
            previous_chainlink_time, previous_chainlink = previous_point
            current_chainlink_time, current_chainlink = current_point
            chainlink_gap_ms = current_chainlink_time - previous_chainlink_time
            if chainlink_gap_ms <= 0 or chainlink_gap_ms > max_chainlink_gap_ms:
                continue
            chainlink_move_bps = _move_bps(
                previous_chainlink.value,
                current_chainlink.value,
            )
            interval_samples: Dict[int, LeadLagSample] = {}

            for lag_ms in lags:
                previous_query_ms = previous_chainlink_time - lag_ms
                current_query_ms = current_chainlink_time - lag_ms
                previous_futures = _asof(
                    futures_timeline,
                    futures_times,
                    previous_query_ms,
                    max_futures_asof_age_ms,
                )
                current_futures = _asof(
                    futures_timeline,
                    futures_times,
                    current_query_ms,
                    max_futures_asof_age_ms,
                )
                if previous_futures is None or current_futures is None:
                    interval_samples = {}
                    break
                (
                    previous_futures_time,
                    previous_futures_observation,
                    previous_futures_age,
                ) = previous_futures
                (
                    current_futures_time,
                    current_futures_observation,
                    current_futures_age,
                ) = current_futures
                futures_move_bps = _move_bps(
                    previous_futures_observation.value,
                    current_futures_observation.value,
                )
                signed_error_bps = futures_move_bps - chainlink_move_bps
                market_id = market_for_sample_second(
                    (current_chainlink_time // 1000) * 1000
                ).market_id
                interval_samples[lag_ms] = LeadLagSample(
                    lag_ms=lag_ms,
                    block_id=block_id,
                    market_id=market_id,
                    previous_chainlink_time_ms=previous_chainlink_time,
                    current_chainlink_time_ms=current_chainlink_time,
                    previous_chainlink_value=previous_chainlink.value,
                    current_chainlink_value=current_chainlink.value,
                    previous_futures_time_ms=previous_futures_time,
                    current_futures_time_ms=current_futures_time,
                    previous_futures_value=previous_futures_observation.value,
                    current_futures_value=current_futures_observation.value,
                    previous_futures_age_ms=previous_futures_age,
                    current_futures_age_ms=current_futures_age,
                    chainlink_move_bps=chainlink_move_bps,
                    futures_move_bps=futures_move_bps,
                    signed_error_bps=signed_error_bps,
                    absolute_error_bps=abs(signed_error_bps),
                )

            if len(interval_samples) == len(lags):
                for lag_ms, sample in interval_samples.items():
                    samples_by_lag[lag_ms].append(sample)

    common_intervals = len(samples_by_lag[lags[0]])
    if common_intervals == 0:
        raise LeadLagAnalysisError("no common-support Chainlink intervals were found")
    if any(len(samples_by_lag[lag]) != common_intervals for lag in lags):
        raise AssertionError("candidate lags do not share common support")

    market_ids = sorted(
        {sample.market_id for sample in samples_by_lag[lags[0]]}
    )
    market_scores: Dict[int, Dict[int, Tuple[int, Decimal]]] = {}
    for market_id in market_ids:
        per_lag: Dict[int, Tuple[int, Decimal]] = {}
        for lag_ms in lags:
            market_samples = [
                sample
                for sample in samples_by_lag[lag_ms]
                if sample.market_id == market_id
            ]
            if not market_samples:
                continue
            mae = sum(
                (sample.absolute_error_bps for sample in market_samples),
                Decimal("0"),
            ) / Decimal(len(market_samples))
            per_lag[lag_ms] = (len(market_samples), mae)
        market_scores[market_id] = per_lag

    qualifying_market_ids = [
        market_id
        for market_id, scores in market_scores.items()
        if len(scores) == len(lags)
        and scores[lags[0]][0] >= minimum_market_intervals
    ]
    equal_market_mae: Dict[int, Optional[Decimal]] = {}
    for lag_ms in lags:
        if not qualifying_market_ids:
            equal_market_mae[lag_ms] = None
            continue
        equal_market_mae[lag_ms] = sum(
            (market_scores[market_id][lag_ms][1] for market_id in qualifying_market_ids),
            Decimal("0"),
        ) / Decimal(len(qualifying_market_ids))

    lag_scores = tuple(
        _lag_score(
            lag_ms,
            samples_by_lag[lag_ms],
            equal_market_mae[lag_ms],
            len(qualifying_market_ids),
        )
        for lag_ms in lags
    )
    market_best_lags = tuple(
        MarketBestLag(
            market_id=market_id,
            intervals=market_scores[market_id][lags[0]][0],
            best_lag_ms=min(
                lags,
                key=lambda lag_ms: (market_scores[market_id][lag_ms][1], lag_ms),
            ),
            best_mae_bps=min(
                market_scores[market_id][lag_ms][1] for lag_ms in lags
            ),
        )
        for market_id in qualifying_market_ids
    )

    return LeadLagAnalysis(
        time_basis=time_basis,
        candidate_lags_ms=lags,
        common_intervals=common_intervals,
        lag_scores=lag_scores,
        market_best_lags=market_best_lags,
        samples_by_lag={
            lag_ms: tuple(samples) for lag_ms, samples in samples_by_lag.items()
        },
    )


def _decimal_text(value: Optional[Decimal]) -> Optional[str]:
    if value is None:
        return None
    if value == 0:
        return "0"
    return format(value, "f")


def _integer_quantile(values: Sequence[int], numerator: int, denominator: int) -> int:
    ordered = sorted(values)
    index = ((len(ordered) - 1) * numerator) // denominator
    return ordered[index]


def analysis_payload(
    tape: LoadedObservationTape,
    analysis: LeadLagAnalysis,
    configuration: Mapping[str, Any],
) -> Dict[str, Any]:
    best = analysis.best_score
    equal_best = analysis.equal_market_best_score
    market_winners = [market.best_lag_ms for market in analysis.market_best_lags]
    return {
        "schema_version": 1,
        "analysis": "chainlink_futures_lead_lag",
        "time_basis": analysis.time_basis,
        "interpretation": "positive lag_ms means futures leads Chainlink",
        "configuration": dict(configuration),
        "input": {
            "markets": len(tape.market_ids),
            "first_market_id": tape.market_ids[0],
            "last_market_id": tape.market_ids[-1],
            "valid_snapshot_rows": tape.valid_snapshot_rows,
            "continuous_blocks": tape.continuous_blocks,
            "chainlink_observations": len(tape.chainlink),
            "futures_observations": len(tape.futures),
            "selection_identity": {
                "schema_version": tape.selection_identity.schema_version,
                "policy_version": tape.selection_identity.policy_version,
                "evidence_end_ms": tape.selection_identity.evidence_end_ms,
                "fingerprint_sha256": tape.selection_identity.fingerprint_sha256,
                "artifact_sha256": tape.selection_identity.artifact_sha256,
            },
            "files": [record.__dict__ for record in tape.files],
        },
        "common_intervals": analysis.common_intervals,
        "pooled_best": {
            "lag_ms": best.lag_ms,
            "mae_bps": _decimal_text(best.mae_bps),
            "correlation": _decimal_text(best.correlation),
        },
        "equal_market_best": None
        if equal_best is None
        else {
            "lag_ms": equal_best.lag_ms,
            "mean_market_mae_bps": _decimal_text(
                equal_best.equal_market_mae_bps
            ),
            "markets": equal_best.qualifying_markets,
        },
        "market_winner_distribution": None
        if not market_winners
        else {
            "markets": len(market_winners),
            "minimum_lag_ms": min(market_winners),
            "q1_lag_ms": _integer_quantile(market_winners, 1, 4),
            "median_lag_ms": _integer_quantile(market_winners, 1, 2),
            "q3_lag_ms": _integer_quantile(market_winners, 3, 4),
            "maximum_lag_ms": max(market_winners),
        },
        "lag_scores": [
            {
                "lag_ms": score.lag_ms,
                "intervals": score.intervals,
                "mae_bps": _decimal_text(score.mae_bps),
                "median_absolute_error_bps": _decimal_text(
                    score.median_absolute_error_bps
                ),
                "rmse_bps": _decimal_text(score.rmse_bps),
                "mean_signed_error_bps": _decimal_text(
                    score.mean_signed_error_bps
                ),
                "correlation": _decimal_text(score.correlation),
                "directional_intervals": score.directional_intervals,
                "directional_agreement": _decimal_text(
                    score.directional_agreement
                ),
                "equal_market_mae_bps": _decimal_text(
                    score.equal_market_mae_bps
                ),
                "qualifying_markets": score.qualifying_markets,
            }
            for score in analysis.lag_scores
        ],
        "limitations": [
            "This is descriptive timing measurement; it does not change or select the live model.",
            "The canonical reports expose 500 ms sampled latest-cache states, not every futures trade.",
            "received time measures the locally observable lead; source time is a separate diagnostic.",
        ],
    }


def _write_csv(path: Path, fieldnames: Sequence[str], rows: Iterable[Mapping[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_analysis_outputs(
    output_dir: Path,
    tape: LoadedObservationTape,
    analysis: LeadLagAnalysis,
    configuration: Mapping[str, Any],
) -> None:
    if output_dir.exists() and any(output_dir.iterdir()):
        raise LeadLagAnalysisError(f"output directory is not empty: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)
    payload = analysis_payload(tape, analysis, configuration)
    (output_dir / "summary.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    _write_csv(
        output_dir / "lag_scores.csv",
        (
            "lag_ms",
            "intervals",
            "mae_bps",
            "median_absolute_error_bps",
            "rmse_bps",
            "mean_signed_error_bps",
            "correlation",
            "directional_intervals",
            "directional_agreement",
            "equal_market_mae_bps",
            "qualifying_markets",
        ),
        (
            {
                "lag_ms": score.lag_ms,
                "intervals": score.intervals,
                "mae_bps": _decimal_text(score.mae_bps),
                "median_absolute_error_bps": _decimal_text(
                    score.median_absolute_error_bps
                ),
                "rmse_bps": _decimal_text(score.rmse_bps),
                "mean_signed_error_bps": _decimal_text(
                    score.mean_signed_error_bps
                ),
                "correlation": _decimal_text(score.correlation),
                "directional_intervals": score.directional_intervals,
                "directional_agreement": _decimal_text(
                    score.directional_agreement
                ),
                "equal_market_mae_bps": _decimal_text(
                    score.equal_market_mae_bps
                ),
                "qualifying_markets": score.qualifying_markets,
            }
            for score in analysis.lag_scores
        ),
    )

    _write_csv(
        output_dir / "per_market_best_lag.csv",
        ("market_id", "intervals", "best_lag_ms", "best_mae_bps"),
        (
            {
                "market_id": market.market_id,
                "intervals": market.intervals,
                "best_lag_ms": market.best_lag_ms,
                "best_mae_bps": _decimal_text(market.best_mae_bps),
            }
            for market in analysis.market_best_lags
        ),
    )

    winning_samples = analysis.samples_by_lag[analysis.best_score.lag_ms]
    _write_csv(
        output_dir / "winning_lag_audit.csv",
        (
            "market_id",
            "block_id",
            "lag_ms",
            "previous_chainlink_time_ms",
            "current_chainlink_time_ms",
            "previous_chainlink_value",
            "current_chainlink_value",
            "previous_futures_time_ms",
            "current_futures_time_ms",
            "previous_futures_value",
            "current_futures_value",
            "previous_futures_age_ms",
            "current_futures_age_ms",
            "chainlink_move_bps",
            "futures_move_bps",
            "signed_error_bps",
            "absolute_error_bps",
        ),
        (
            {
                "market_id": sample.market_id,
                "block_id": sample.block_id,
                "lag_ms": sample.lag_ms,
                "previous_chainlink_time_ms": sample.previous_chainlink_time_ms,
                "current_chainlink_time_ms": sample.current_chainlink_time_ms,
                "previous_chainlink_value": _decimal_text(
                    sample.previous_chainlink_value
                ),
                "current_chainlink_value": _decimal_text(
                    sample.current_chainlink_value
                ),
                "previous_futures_time_ms": sample.previous_futures_time_ms,
                "current_futures_time_ms": sample.current_futures_time_ms,
                "previous_futures_value": _decimal_text(
                    sample.previous_futures_value
                ),
                "current_futures_value": _decimal_text(
                    sample.current_futures_value
                ),
                "previous_futures_age_ms": sample.previous_futures_age_ms,
                "current_futures_age_ms": sample.current_futures_age_ms,
                "chainlink_move_bps": _decimal_text(sample.chainlink_move_bps),
                "futures_move_bps": _decimal_text(sample.futures_move_bps),
                "signed_error_bps": _decimal_text(sample.signed_error_bps),
                "absolute_error_bps": _decimal_text(sample.absolute_error_bps),
            }
            for sample in winning_samples
        ),
    )


def _candidate_lags(start_ms: int, end_ms: int, step_ms: int) -> Tuple[int, ...]:
    if step_ms <= 0:
        raise ValueError("lag step must be positive")
    if end_ms < start_ms:
        raise ValueError("lag end must be at least lag start")
    return tuple(range(start_ms, end_ms + 1, step_ms))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Measure how many milliseconds sampled Binance futures moves lead "
            "sampled Chainlink moves."
        )
    )
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--pattern", default=DEFAULT_PATTERN)
    parser.add_argument("--expected-model-version", default=DEFAULT_MODEL_VERSION)
    parser.add_argument("--time-basis", choices=("received", "source"), default="received")
    parser.add_argument("--lag-start-ms", type=int, default=0)
    parser.add_argument("--lag-end-ms", type=int, default=10_000)
    parser.add_argument("--lag-step-ms", type=int, default=500)
    parser.add_argument("--max-generated-gap-ms", type=int, default=1_500)
    parser.add_argument("--max-chainlink-gap-ms", type=int, default=5_000)
    parser.add_argument("--max-futures-asof-age-ms", type=int, default=1_000)
    parser.add_argument("--minimum-market-intervals", type=int, default=30)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        candidate_lags_ms = _candidate_lags(
            args.lag_start_ms,
            args.lag_end_ms,
            args.lag_step_ms,
        )
        input_dir = Path(args.input_dir).expanduser().resolve()
        output_dir = Path(args.output_dir).expanduser().resolve()
        tape = load_observation_tape(
            input_dir=input_dir,
            pattern=args.pattern,
            expected_model_version=args.expected_model_version,
            max_generated_gap_ms=args.max_generated_gap_ms,
        )
        analysis = analyze_lead_lag(
            chainlink_observations=tape.chainlink,
            futures_observations=tape.futures,
            candidate_lags_ms=candidate_lags_ms,
            time_basis=args.time_basis,
            max_chainlink_gap_ms=args.max_chainlink_gap_ms,
            max_futures_asof_age_ms=args.max_futures_asof_age_ms,
            minimum_market_intervals=args.minimum_market_intervals,
        )
        configuration = {
            "pattern": args.pattern,
            "expected_model_version": args.expected_model_version,
            "lag_start_ms": args.lag_start_ms,
            "lag_end_ms": args.lag_end_ms,
            "lag_step_ms": args.lag_step_ms,
            "max_generated_gap_ms": args.max_generated_gap_ms,
            "max_chainlink_gap_ms": args.max_chainlink_gap_ms,
            "max_futures_asof_age_ms": args.max_futures_asof_age_ms,
            "minimum_market_intervals": args.minimum_market_intervals,
        }
        write_analysis_outputs(output_dir, tape, analysis, configuration)
    except (LeadLagAnalysisError, OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    payload = analysis_payload(tape, analysis, configuration)
    print(
        json.dumps(
            {
                "output_dir": str(output_dir),
                "time_basis": analysis.time_basis,
                "common_intervals": analysis.common_intervals,
                "pooled_best": payload["pooled_best"],
                "equal_market_best": payload["equal_market_best"],
                "market_winner_distribution": payload[
                    "market_winner_distribution"
                ],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
