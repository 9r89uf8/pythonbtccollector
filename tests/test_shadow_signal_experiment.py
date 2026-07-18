import json
import hashlib
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
from uuid import UUID

import pytest

from price_collector.shadow_signal_experiment import (
    ActiveIncumbentFreeze,
    ArtifactBinding,
    AttemptIdentity,
    ControlMode,
    ExperimentValidationError,
    FROZEN_BOOTSTRAP_CONTRACT,
    FROZEN_CALIBRATION_GATES,
    FROZEN_HOLDOUT_GATES,
    FROZEN_QUALITY_GATES,
    ForecastCodeManifest,
    ForecastConfig,
    IncumbentProvenanceError,
    InspectedEvidenceBinding,
    ModelIdentity,
    POLICY_VERSION,
    SelectionAnchorProvenance,
    V4ExperimentContract,
    V4ForecastSettings,
    V4Preregistration,
    V4TerminalResult,
    V4_TIMING_CELLS,
    artifact_sha256,
    calibration_selection_report_payload,
    canonical_artifact_bytes,
    canonical_json_bytes,
    canonical_sha256,
    decode_strict_json,
    efficacy_completion_marker_payload,
    forecast_config_digest,
    non_lag_forecast_config_digest,
    preregistration_deadline_check_payload,
    pushed_preregistration_receipt_payload,
    receipt_deadline_check_payload,
    resolve_replacement_control,
    terminal_efficacy_report_payload,
    validate_preregistration,
    validate_terminal_result,
)


LINEAGE_ID = UUID("11111111-1111-4111-8111-111111111111")
EXPERIMENT_ID = UUID("22222222-2222-4222-8222-222222222222")
CALIBRATION_PARENT_EXPERIMENT_ID = UUID(
    "33333333-3333-4333-8333-333333333333"
)
HOLDOUT_PARENT_EXPERIMENT_ID = UUID(
    "44444444-4444-4444-8444-444444444444"
)
HOLDOUT_START_MS = 100 * 86_400_000


def digest(character):
    return character * 64


def text_digest(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def code_manifest(prefix="a"):
    characters = {
        "a": ("a", "b", "c", "d"),
        "e": ("e", "f", "0", "1"),
    }[prefix]
    return ForecastCodeManifest(
        anchor_formation_sha256=digest(characters[0]),
        futures_reference_selection_sha256=digest(characters[1]),
        projection_sha256=digest(characters[2]),
        forecast_validity_sha256=digest(characters[3]),
    )


def settings():
    return V4ForecastSettings(
        futures_stale_ms=1_000,
        chainlink_stale_ms=5_000,
        history_retention_ms=10_000,
    )


def binding(name="fixture", character="9"):
    return ArtifactBinding(
        artifact_type=name,
        schema_version=1,
        sha256=digest(character),
    )


def inspected_evidence(
    name="old_replay",
    character="4",
    *,
    evidence_scope="calibration_only",
    inspection_role="historical_replay",
    source_experiment_id="legacy-v2-evidence",
):
    return InspectedEvidenceBinding(
        artifact=binding(name, character),
        source_lineage_id="legacy-chainlink-evidence",
        source_experiment_id=source_experiment_id,
        window_start_ms=HOLDOUT_START_MS - 10 * 86_400_000,
        window_end_ms=HOLDOUT_START_MS - 9 * 86_400_000,
        inspection_role=inspection_role,
        evidence_scope=evidence_scope,
    )


def terminal_result_binding(result):
    return ArtifactBinding(
        artifact_type="chainlink_v4_terminal_result",
        schema_version=1,
        sha256=artifact_sha256(result.to_dict()),
    )


def retry_eligibility_payload(
    *, stage, allocated_attempt, parent_result, created_at_ms
):
    parent = terminal_result_binding(parent_result)
    return {
        "artifact_type": f"{stage}_retry_eligibility",
        "schema_version": 1,
        "parent_result": parent.to_dict(),
        "allocated_attempt": allocated_attempt.to_dict(),
        "failure_stage": parent_result.failure_stage,
        "created_at_ms": created_at_ms,
        "restoration_evidence": [
            ArtifactBinding(
                artifact_type=f"{stage}_restoration_evidence",
                schema_version=1,
                sha256=text_digest(
                    f"{stage}:restored:{allocated_attempt.experiment_id}"
                ),
            ).to_dict()
        ],
        "successors_remaining_after_allocation": 0,
    }


def calibration_successor_authorization_payload(
    *,
    allocated_attempt,
    parent_result,
    retry_payload,
    experiment_contract,
    candidate_day_ledger,
    selected_window,
    provenance_continuity_root,
):
    return {
        "artifact_type": "calibration_successor_authorization",
        "schema_version": 1,
        "attempt": allocated_attempt.to_dict(),
        "parent_result": terminal_result_binding(parent_result).to_dict(),
        "retry_eligibility_sha256": artifact_sha256(retry_payload),
        "experiment_contract_digest": experiment_contract.digest,
        "candidate_day_ledger_root": candidate_day_ledger.sha256,
        "selected_window_artifact": selected_window.to_dict(),
        "provenance_continuity_root": provenance_continuity_root,
        "successors_remaining_after_allocation": 0,
    }


def incumbent(*, config=None, code=None, installed=None, model_version=None):
    runtime_digest = digest("7")
    frozen_code = code or code_manifest()
    return ActiveIncumbentFreeze(
        selection_sha256=digest("2"),
        replay_config_sha256=digest("3"),
        primary_model_version=(
            model_version or "catchup_ratio_l3000_b100"
        ),
        forecast_config=config or settings().config_for_lag(3_000),
        forecast_code=frozen_code,
        loaded_runtime_identity_sha256=runtime_digest,
        installed_runtime_identity_sha256=installed or runtime_digest,
        invocation_start=binding("active_invocation_start_record", "8"),
        selection_artifact=binding("active_incumbent_selection", "2"),
        replay_config_artifact=binding(
            "active_incumbent_replay_configuration", "3"
        ),
        forecast_code_manifest_artifact=ArtifactBinding(
            artifact_type="active_forecast_code_manifest",
            schema_version=1,
            sha256=artifact_sha256(
                frozen_code.to_artifact_dict(
                    "active_forecast_code_manifest"
                )
            ),
        ),
        reconstruction_report=binding(
            "active_forecast_reconstruction_report", "6"
        ),
    )


def contract(*, active=None, v4_code=None):
    frozen_v4_code = v4_code or code_manifest()
    return V4ExperimentContract(
        forecast_settings=settings(),
        v4_forecast_code=frozen_v4_code,
        v4_forecast_code_manifest_artifact=ArtifactBinding(
            artifact_type="v4_forecast_code_manifest",
            schema_version=1,
            sha256=artifact_sha256(
                frozen_v4_code.to_artifact_dict(
                    "v4_forecast_code_manifest"
                )
            ),
        ),
        active_incumbent=active or incumbent(),
    )


def attempt(
    *,
    holdout_index=0,
    calibration_index=0,
    experiment_id=EXPERIMENT_ID,
):
    return AttemptIdentity(
        calibration_lineage_id=LINEAGE_ID,
        experiment_id=experiment_id,
        calibration_attempt_index=calibration_index,
        holdout_attempt_index=holdout_index,
    )


def origin_contract():
    def hashes_for(role):
        return {
            cell.cell_id: text_digest(f"{role}:{cell.cell_id}")
            for cell in V4_TIMING_CELLS
        }

    return {
        "origin_formula": (
            "generated_ms=cell.phase_offset_ms+500*k_"
            "restricted_to_half_open_window"
        ),
        "scheduled_count_per_cell": 172_800,
        "target_eligible_count_per_cell": 172_793,
        "scheduled_vector_sha256_by_cell": hashes_for("scheduled_vector"),
        "target_eligible_mask_sha256_by_cell": hashes_for(
            "target_eligible_mask"
        ),
        "target_eligible_vector_sha256_by_cell": hashes_for(
            "target_eligible_vector"
        ),
        "observed_mask_index": "target_eligible_origin_vector",
        "observed_mask_schemas": [
            "generation_eligible_mask",
            "common_scored_mask",
            "decision_eligible_mask",
            "per_origin_missing_reasons",
        ],
        "missing_origin_treatment": (
            "retain_position_no_shift_compaction_imputation_or_zero_fill"
        ),
        "coverage_thresholds": {
            "canonical_common_scored_minimum": 164_154,
            "robustness_common_scored_minimum": 155_514,
            "canonical_decision_eligible_minimum": 164_154,
            "robustness_decision_eligible_minimum": 155_514,
        },
    }


def calibration_artifacts(
    *,
    calibration_index=0,
    retry_binding=None,
    successor_binding=None,
    report_binding=None,
    completion_binding=None,
    authorization_binding=None,
):
    artifact_types = [
        *(
            ["calibration_retry_eligibility"]
            if calibration_index == 1
            else []
        ),
        "calibration_candidate_day_ledger",
        *(
            ["calibration_successor_authorization"]
            if calibration_index == 1
            else []
        ),
        "calibration_attempt_freeze",
        "calibration_archive_checkpoint_manifest",
        "calibration_raw_manifest",
        *[
            f"calibration_quality_report:{cell.cell_id}"
            for cell in V4_TIMING_CELLS
        ],
        "calibration_pre_efficacy_provenance_gate",
        "calibration_efficacy_started",
        "calibration_efficacy_ledger",
        "calibration_efficacy_report",
        "calibration_efficacy_completed",
        "final_analysis_checkpoint",
        "holdout_selection_authorization",
    ]
    artifacts = []
    for artifact_type in artifact_types:
        if artifact_type == "calibration_retry_eligibility" and (
            retry_binding is not None
        ):
            artifacts.append(retry_binding)
        elif artifact_type == "calibration_successor_authorization" and (
            successor_binding is not None
        ):
            artifacts.append(successor_binding)
        elif artifact_type == "calibration_efficacy_completed" and (
            completion_binding is not None
        ):
            artifacts.append(completion_binding)
        elif artifact_type == "calibration_efficacy_report" and (
            report_binding is not None
        ):
            artifacts.append(report_binding)
        elif artifact_type == "holdout_selection_authorization" and (
            authorization_binding is not None
        ):
            artifacts.append(authorization_binding)
        else:
            artifacts.append(
                ArtifactBinding(
                    artifact_type=artifact_type,
                    schema_version=1,
                    sha256=text_digest(artifact_type),
                )
            )
    return tuple(artifacts)


def successful_calibration_efficacy_evidence():
    candidate_mae = {
        "1500": Decimal("0.04"),
        "2000": Decimal("0.08"),
        "2500": Decimal("0.06"),
        "3000": Decimal("0.055"),
        "3500": Decimal("0.05"),
    }
    candidate_rmse = {
        lag: Decimal("0.02") for lag in candidate_mae
    }
    return {
        "ranking_metric": (
            "mae_skill_vs_horizon_matched_no_change_baseline"
        ),
        "candidate_canonical_mae_skill_by_lag": candidate_mae,
        "candidate_canonical_rmse_skill_by_lag": candidate_rmse,
        "candidate_mae_skill_by_robustness_cell": {
            cell.cell_id: dict(candidate_mae)
            for cell in V4_TIMING_CELLS[1:]
        },
        "ordered_candidate_lags_ms": [2_000, 2_500, 3_000, 3_500, 1_500],
        "winner_lag_ms": 2_000,
        "runner_up_lag_ms": 2_500,
        "winner_canonical_mae_skill": Decimal("0.08"),
        "winner_canonical_rmse_skill": Decimal("0.02"),
        "mae_skill_lead_over_runner_up": Decimal("0.02"),
        "winner_relative_deficit_by_robustness_cell": {
            cell.cell_id: Decimal("0") for cell in V4_TIMING_CELLS[1:]
        },
        "winner_promotion_eligible": True,
        "boundary_winner": False,
        "unique_best": True,
        "gate_results": {
            "winner_canonical_mae_skill": True,
            "winner_canonical_rmse_skill": True,
            "mae_skill_lead_over_runner_up": True,
            "relative_robustness_all_cells": True,
            "winner_promotion_eligible": True,
            "unique_best": True,
        },
        "all_gates_passed": True,
    }


def calibration_validation_artifacts(prereg):
    by_type = {
        artifact.artifact_type: artifact
        for artifact in prereg.calibration_artifacts
    }
    calibration_stage_attempt = attempt(
        holdout_index=None,
        calibration_index=prereg.attempt.calibration_attempt_index,
        experiment_id=(
            HOLDOUT_PARENT_EXPERIMENT_ID
            if prereg.attempt.holdout_attempt_index == 1
            else prereg.attempt.experiment_id
        ),
    )
    report_payload = calibration_selection_report_payload(
        attempt=calibration_stage_attempt,
        experiment_contract_digest=prereg.experiment_contract.digest,
        frozen_challenger=prereg.frozen_challenger,
        efficacy_ledger=by_type["calibration_efficacy_ledger"],
        efficacy_evidence=successful_calibration_efficacy_evidence(),
    )
    assert artifact_sha256(report_payload) == by_type[
        "calibration_efficacy_report"
    ].sha256
    completion_payload = efficacy_completion_marker_payload(
        attempt=calibration_stage_attempt,
        experiment_contract_digest=prereg.experiment_contract.digest,
        terminal_stage="calibration",
        efficacy_start_marker=by_type["calibration_efficacy_started"],
        prerequisite_artifacts=(
            by_type["calibration_attempt_freeze"],
            by_type["calibration_raw_manifest"],
            by_type["calibration_pre_efficacy_provenance_gate"],
        ),
        efficacy_report=by_type["calibration_efficacy_report"],
        immutable_efficacy_artifacts=(
            by_type["calibration_efficacy_started"],
            by_type["calibration_efficacy_ledger"],
            by_type["calibration_efficacy_report"],
        ),
        completed_at_ms=HOLDOUT_START_MS - 2 * 86_400_000,
    )
    assert artifact_sha256(completion_payload) == by_type[
        "calibration_efficacy_completed"
    ].sha256
    return (
        canonical_artifact_bytes(report_payload),
        canonical_artifact_bytes(completion_payload),
    )


def anchor_source_payload(prereg, *, successor_parent_result=None):
    anchor = prereg.selection_anchor_provenance
    if anchor.source_artifact.artifact_type == "calibration_efficacy_completed":
        return decode_strict_json(calibration_validation_artifacts(prereg)[1])
    if anchor.source_artifact.artifact_type == "holdout_retry_eligibility":
        parent = successor_parent_result or holdout_quality_failure_result(
            experiment_contract=prereg.experiment_contract,
            calibration_index=prereg.attempt.calibration_attempt_index,
            experiment_id=HOLDOUT_PARENT_EXPERIMENT_ID,
        )
        return retry_eligibility_payload(
            stage="holdout",
            allocated_attempt=prereg.attempt,
            parent_result=parent,
            created_at_ms=anchor.timestamp_ms,
        )
    return {
        "artifact_type": anchor.source_artifact.artifact_type,
        "schema_version": 1,
        anchor.timestamp_field: anchor.timestamp_ms,
    }


def anchor_authorization_payload(prereg):
    anchor = prereg.selection_anchor_provenance
    by_type = {
        artifact.artifact_type: artifact
        for artifact in prereg.calibration_artifacts
    }
    payload = {
        "artifact_type": anchor.authorization_artifact.artifact_type,
        "schema_version": 1,
        "attempt": prereg.attempt.to_dict(),
        "selection_anchor_source_sha256": anchor.source_artifact.sha256,
        "selection_anchor_ms": anchor.timestamp_ms,
        "frozen_challenger": prereg.frozen_challenger.to_dict(),
        "experiment_contract_digest": prereg.experiment_contract.digest,
        "calibration_efficacy_report_sha256": by_type[
            "calibration_efficacy_report"
        ].sha256,
        "calibration_efficacy_completed_sha256": by_type[
            "calibration_efficacy_completed"
        ].sha256,
    }
    if prereg.attempt.holdout_attempt_index == 1:
        payload["parent_result"] = prereg.prior_evidence["parent_result"]
        payload["retry_eligibility_sha256"] = (
            prereg.selection_anchor_provenance.source_artifact.sha256
        )
        payload["holdout_window"] = {
            "start_ms": prereg.holdout_start_ms,
            "end_ms": prereg.holdout_end_ms,
            "boundary": "[start_ms,end_ms)",
        }
        payload["candidate_day_ledger_root"] = (
            prereg.candidate_day_ledger_root
        )
        payload["provenance_continuity_root"] = (
            prereg.provenance_continuity_root
        )
        payload["successors_remaining_after_allocation"] = 0
    return payload


def anchor_validation_artifacts(prereg, *, successor_parent_result=None):
    return (
        canonical_artifact_bytes(
            anchor_source_payload(
                prereg, successor_parent_result=successor_parent_result
            )
        ),
        canonical_artifact_bytes(anchor_authorization_payload(prereg)),
    )


def preregistration_validation_kwargs(
    prereg,
    *,
    successor_parent_result=None,
    calibration_retry_created_at_ms=None,
):
    source, authorization = anchor_validation_artifacts(
        prereg, successor_parent_result=successor_parent_result
    )
    calibration_report, calibration_completion = (
        calibration_validation_artifacts(prereg)
    )
    by_calibration_type = {
        artifact.artifact_type: artifact
        for artifact in prereg.calibration_artifacts
    }
    validation = {
        "selection_anchor_source_artifact": source,
        "selection_anchor_authorization_artifact": authorization,
        "calibration_efficacy_report_artifact": calibration_report,
        "calibration_completion_marker_artifact": calibration_completion,
        "expected_calibration_efficacy_report": by_calibration_type[
            "calibration_efficacy_report"
        ],
        "expected_calibration_completion_marker": by_calibration_type[
            "calibration_efficacy_completed"
        ],
        "expected_prior_evidence_artifacts": tuple(
            InspectedEvidenceBinding.from_dict(
                item, f"prior_evidence.inspected_artifacts[{index}]"
            )
            for index, item in enumerate(
                prereg.prior_evidence["inspected_artifacts"]
            )
        ),
    }
    if prereg.attempt.holdout_attempt_index == 1:
        parent = successor_parent_result or holdout_quality_failure_result(
            experiment_contract=prereg.experiment_contract,
            calibration_index=prereg.attempt.calibration_attempt_index,
            experiment_id=HOLDOUT_PARENT_EXPERIMENT_ID,
        )
        validation["successor_parent_result_artifact"] = (
            canonical_artifact_bytes(parent.to_dict())
        )
        validation["expected_successor_parent_result"] = (
            terminal_result_binding(parent)
        )
        retry_payload = decode_strict_json(source)
        validation["expected_retry_restoration_evidence"] = tuple(
            ArtifactBinding.from_dict(
                item,
                f"holdout_retry_eligibility.restoration_evidence[{index}]",
            )
            for index, item in enumerate(
                retry_payload["restoration_evidence"]
            )
        )
        if prereg.attempt.calibration_attempt_index == 1:
            calibration_parent = calibration_insufficient_result(
                experiment_contract=prereg.experiment_contract,
                experiment_id=CALIBRATION_PARENT_EXPERIMENT_ID,
                created_at_ms=(
                    HOLDOUT_START_MS - 2 * 86_400_000 - 1
                ),
            )
            inherited_attempt = attempt(
                holdout_index=None,
                calibration_index=1,
                experiment_id=HOLDOUT_PARENT_EXPERIMENT_ID,
            )
            calibration_retry_payload = retry_eligibility_payload(
                stage="calibration",
                allocated_attempt=inherited_attempt,
                parent_result=calibration_parent,
                created_at_ms=HOLDOUT_START_MS - 2 * 86_400_000,
            )
            calibration_authorization_payload = (
                calibration_successor_authorization_payload(
                    allocated_attempt=inherited_attempt,
                    parent_result=calibration_parent,
                    retry_payload=calibration_retry_payload,
                    experiment_contract=prereg.experiment_contract,
                    candidate_day_ledger=by_calibration_type[
                        "calibration_candidate_day_ledger"
                    ],
                    selected_window=by_calibration_type[
                        "calibration_attempt_freeze"
                    ],
                    provenance_continuity_root=digest("f"),
                )
            )
            validation.update(
                calibration_parent_result_artifact=(
                    canonical_artifact_bytes(calibration_parent.to_dict())
                ),
                calibration_retry_eligibility_artifact=(
                    canonical_artifact_bytes(calibration_retry_payload)
                ),
                calibration_successor_authorization_artifact=(
                    canonical_artifact_bytes(
                        calibration_authorization_payload
                    )
                ),
                expected_calibration_parent_result=(
                    terminal_result_binding(calibration_parent)
                ),
                expected_calibration_retry_restoration_evidence=tuple(
                    ArtifactBinding.from_dict(
                        item,
                        (
                            "calibration_retry_eligibility."
                            f"restoration_evidence[{index}]"
                        ),
                    )
                    for index, item in enumerate(
                        calibration_retry_payload["restoration_evidence"]
                    )
                ),
            )
    elif prereg.attempt.calibration_attempt_index == 1:
        parent = calibration_insufficient_result(
            experiment_contract=prereg.experiment_contract,
            experiment_id=CALIBRATION_PARENT_EXPERIMENT_ID,
            created_at_ms=HOLDOUT_START_MS - 2 * 86_400_000 - 1,
        )
        calibration_stage_attempt = attempt(
            holdout_index=None,
            calibration_index=prereg.attempt.calibration_attempt_index,
            experiment_id=prereg.attempt.experiment_id,
        )
        retry_payload = retry_eligibility_payload(
            stage="calibration",
            allocated_attempt=calibration_stage_attempt,
            parent_result=parent,
            created_at_ms=(
                HOLDOUT_START_MS - 2 * 86_400_000
                if calibration_retry_created_at_ms is None
                else calibration_retry_created_at_ms
            ),
        )
        authorization_payload = calibration_successor_authorization_payload(
            allocated_attempt=calibration_stage_attempt,
            parent_result=parent,
            retry_payload=retry_payload,
            experiment_contract=prereg.experiment_contract,
            candidate_day_ledger=by_calibration_type[
                "calibration_candidate_day_ledger"
            ],
            selected_window=by_calibration_type[
                "calibration_attempt_freeze"
            ],
            provenance_continuity_root=digest("f"),
        )
        validation.update(
            successor_parent_result_artifact=canonical_artifact_bytes(
                parent.to_dict()
            ),
            calibration_retry_eligibility_artifact=(
                canonical_artifact_bytes(retry_payload)
            ),
            calibration_successor_authorization_artifact=(
                canonical_artifact_bytes(authorization_payload)
            ),
            expected_successor_parent_result=terminal_result_binding(parent),
            expected_retry_restoration_evidence=tuple(
                ArtifactBinding.from_dict(
                    item,
                    (
                        "calibration_retry_eligibility."
                        f"restoration_evidence[{index}]"
                    ),
                )
                for index, item in enumerate(
                    retry_payload["restoration_evidence"]
                )
            ),
        )
    if prereg.attempt.calibration_attempt_index == 1:
        validation[
            "expected_calibration_authorization_provenance_root"
        ] = digest("f")
    return validation


def bound_preregistration_validation_kwargs(result):
    prereg = preregistration(
        experiment_contract=result.experiment_contract,
        holdout_index=result.attempt.holdout_attempt_index,
        calibration_index=result.attempt.calibration_attempt_index,
        experiment_id=result.attempt.experiment_id,
    )
    raw = canonical_artifact_bytes(prereg.to_dict())
    assert hashlib.sha256(raw).hexdigest() == (
        result.preregistration_binding.sha256
    )
    validation = preregistration_validation_kwargs(prereg)
    receipt_raw, receipt_check_raw = receipt_validation_artifacts(
        prereg, receipt_present=result.pushed_receipt is not None
    )
    return {
        "preregistration_artifact": raw,
        "pushed_receipt_artifact": (
            receipt_raw if result.pushed_receipt is not None else None
        ),
        "receipt_deadline_check_artifact": receipt_check_raw,
        **validation,
    }


def receipt_validation_artifacts(prereg, *, receipt_present=True):
    preregistration_binding = ArtifactBinding(
        artifact_type="chainlink_v4_holdout_preregistration",
        schema_version=1,
        sha256=artifact_sha256(prereg.to_dict()),
    )
    if receipt_present:
        receipt_payload = pushed_preregistration_receipt_payload(
            attempt=prereg.attempt,
            preregistration=preregistration_binding,
            authoritative_remote_url_sha256=(
                prereg.authoritative_remote_url_sha256
            ),
            pushed_commit_id="1" * 40,
            observed_remote_ref=prereg.authoritative_remote_ref,
            observed_remote_commit_id="1" * 40,
            verified_at_ms=prereg.created_at_ms + 1,
        )
        receipt_binding = ArtifactBinding(
            artifact_type="holdout_pushed_preregistration_receipt",
            schema_version=1,
            sha256=artifact_sha256(receipt_payload),
        )
        checked_at_ms = prereg.created_at_ms + 2
    else:
        receipt_payload = None
        receipt_binding = None
        checked_at_ms = prereg.pushed_receipt_deadline_ms + 1
    check_payload = receipt_deadline_check_payload(
        attempt=prereg.attempt,
        preregistration=preregistration_binding,
        authoritative_remote_url_sha256=(
            prereg.authoritative_remote_url_sha256
        ),
        expected_remote_ref=prereg.authoritative_remote_ref,
        pushed_receipt_deadline_ms=prereg.pushed_receipt_deadline_ms,
        checked_at_ms=checked_at_ms,
        pushed_receipt=receipt_binding,
        pushed_receipt_payload_value=receipt_payload,
    )
    return (
        None if receipt_payload is None else canonical_artifact_bytes(receipt_payload),
        canonical_artifact_bytes(check_payload),
    )


def calibration_successor_validation_kwargs(result):
    parent = calibration_insufficient_result(
        experiment_contract=result.experiment_contract,
        experiment_id=CALIBRATION_PARENT_EXPERIMENT_ID,
        created_at_ms=HOLDOUT_START_MS - 2 * 86_400_000 - 1,
    )
    retry_payload = retry_eligibility_payload(
        stage="calibration",
        allocated_attempt=result.attempt,
        parent_result=parent,
        created_at_ms=HOLDOUT_START_MS - 2 * 86_400_000,
    )
    result_by_type = {
        artifact.artifact_type: artifact
        for artifact in result.evidence_artifacts
    }
    authorization_payload = calibration_successor_authorization_payload(
        allocated_attempt=result.attempt,
        parent_result=parent,
        retry_payload=retry_payload,
        experiment_contract=result.experiment_contract,
        candidate_day_ledger=result_by_type[
            "calibration_candidate_day_ledger"
        ],
        selected_window=result_by_type["calibration_attempt_freeze"],
        provenance_continuity_root=result.provenance_continuity_root,
    )
    return {
        "successor_parent_result_artifact": canonical_artifact_bytes(
            parent.to_dict()
        ),
        "calibration_retry_eligibility_artifact": canonical_artifact_bytes(
            retry_payload
        ),
        "calibration_successor_authorization_artifact": (
            canonical_artifact_bytes(authorization_payload)
        ),
        "expected_successor_parent_result": terminal_result_binding(parent),
        "expected_retry_restoration_evidence": tuple(
            ArtifactBinding.from_dict(
                item,
                (
                    "calibration_retry_eligibility."
                    f"restoration_evidence[{index}]"
                ),
            )
            for index, item in enumerate(
                retry_payload["restoration_evidence"]
            )
        ),
        "expected_calibration_authorization_provenance_root": (
            result.provenance_continuity_root
        ),
    }


def quality_cell(stage, cell, report_binding, *, passed=True, origin=None):
    common_scored = 172_793 if passed else 100_000
    decision_eligible = 172_793 if passed else 100_000
    binding_names = (
        "scheduled_vector",
        "target_eligible_mask",
        "target_eligible_vector",
        "generation_eligible_mask",
        "common_scored_mask",
        "decision_eligible_mask",
        "missing_reasons",
    )
    payload = {
        "cell_id": cell.cell_id,
        "scheduled_count": 172_800,
        "target_eligible_count": 172_793,
        "generation_eligible_count": 172_793,
        "common_scored_count": common_scored,
        "decision_eligible_count": decision_eligible,
        "cohort_classified_count": 172_793,
        "causal_violation_count": 0,
        "quality_report_binding": report_binding.to_dict(),
        "common_scored_gate_passed": passed,
        "decision_eligible_gate_passed": passed,
        "cell_quality_passed": passed,
    }
    for name in binding_names:
        artifact_type = f"{stage}_{name}:{cell.cell_id}"
        origin_hash_key = {
            "scheduled_vector": "scheduled_vector_sha256_by_cell",
            "target_eligible_mask": (
                "target_eligible_mask_sha256_by_cell"
            ),
            "target_eligible_vector": (
                "target_eligible_vector_sha256_by_cell"
            ),
        }.get(name)
        artifact_hash = (
            origin[origin_hash_key][cell.cell_id]
            if origin is not None and origin_hash_key is not None
            else text_digest(artifact_type)
        )
        payload[f"{name}_binding"] = ArtifactBinding(
            artifact_type=artifact_type,
            schema_version=1,
            sha256=artifact_hash,
        ).to_dict()
    return payload


def passed_quality_evidence(stage, reports, *, origin=None):
    return {
        "status": "passed",
        "stage": stage,
        "cells": [
            quality_cell(stage, cell, report, origin=origin)
            for cell, report in zip(V4_TIMING_CELLS, reports)
        ],
        "archive_health_passed": True,
        "provenance_passed": True,
        "structural_gate_infeasibility_report_binding": None,
        "failure_codes": [],
        "all_quality_gates_passed": True,
    }


def preregistration(
    *,
    experiment_contract=None,
    holdout_index=0,
    calibration_index=0,
    experiment_id=EXPERIMENT_ID,
    holdout_parent_result_override=None,
    calibration_retry_created_at_ms_override=None,
):
    frozen = experiment_contract or contract()
    attempt_identity = attempt(
        holdout_index=holdout_index,
        calibration_index=calibration_index,
        experiment_id=experiment_id,
    )
    calibration_attempt_identity = (
        attempt_identity
        if holdout_index == 0
        else attempt(
            holdout_index=0,
            calibration_index=calibration_index,
            experiment_id=HOLDOUT_PARENT_EXPERIMENT_ID,
        )
    )
    calibration_stage_attempt_identity = attempt(
        holdout_index=None,
        calibration_index=calibration_index,
        experiment_id=calibration_attempt_identity.experiment_id,
    )
    challenger = frozen.candidate_identity(2_000)
    holdout_start_ms = (
        HOLDOUT_START_MS + holdout_index * 4 * 86_400_000
    )
    calibration_completion_ms = HOLDOUT_START_MS - 2 * 86_400_000
    anchor_ms = (
        calibration_completion_ms
        if holdout_index == 0
        else holdout_start_ms - 2 * 86_400_000
    )
    archive_start = holdout_start_ms - 10_100
    publication_deadline = archive_start - 86_400_000 - 3_600_000
    calibration_ledger = ArtifactBinding(
        artifact_type="calibration_efficacy_ledger",
        schema_version=1,
        sha256=text_digest("calibration_efficacy_ledger"),
    )
    calibration_report_payload = calibration_selection_report_payload(
        attempt=calibration_stage_attempt_identity,
        experiment_contract_digest=frozen.digest,
        frozen_challenger=challenger,
        efficacy_ledger=calibration_ledger,
        efficacy_evidence=successful_calibration_efficacy_evidence(),
    )
    calibration_report = ArtifactBinding(
        artifact_type="calibration_efficacy_report",
        schema_version=1,
        sha256=artifact_sha256(calibration_report_payload),
    )
    calibration_completion_payload = efficacy_completion_marker_payload(
        attempt=calibration_stage_attempt_identity,
        experiment_contract_digest=frozen.digest,
        terminal_stage="calibration",
        efficacy_start_marker=ArtifactBinding(
            artifact_type="calibration_efficacy_started",
            schema_version=1,
            sha256=text_digest("calibration_efficacy_started"),
        ),
        prerequisite_artifacts=(
            ArtifactBinding(
                artifact_type="calibration_attempt_freeze",
                schema_version=1,
                sha256=text_digest("calibration_attempt_freeze"),
            ),
            ArtifactBinding(
                artifact_type="calibration_raw_manifest",
                schema_version=1,
                sha256=text_digest("calibration_raw_manifest"),
            ),
            ArtifactBinding(
                artifact_type="calibration_pre_efficacy_provenance_gate",
                schema_version=1,
                sha256=text_digest(
                    "calibration_pre_efficacy_provenance_gate"
                ),
            ),
        ),
        efficacy_report=calibration_report,
        immutable_efficacy_artifacts=(
            ArtifactBinding(
                artifact_type="calibration_efficacy_started",
                schema_version=1,
                sha256=text_digest("calibration_efficacy_started"),
            ),
            calibration_ledger,
            calibration_report,
        ),
        completed_at_ms=calibration_completion_ms,
    )
    calibration_completion = ArtifactBinding(
        artifact_type="calibration_efficacy_completed",
        schema_version=1,
        sha256=artifact_sha256(calibration_completion_payload),
    )
    initial_authorization_payload = {
        "artifact_type": "holdout_selection_authorization",
        "schema_version": 1,
        "attempt": calibration_attempt_identity.to_dict(),
        "selection_anchor_source_sha256": calibration_completion.sha256,
        "selection_anchor_ms": calibration_completion_ms,
        "frozen_challenger": challenger.to_dict(),
        "experiment_contract_digest": frozen.digest,
        "calibration_efficacy_report_sha256": calibration_report.sha256,
        "calibration_efficacy_completed_sha256": (
            calibration_completion.sha256
        ),
    }
    initial_authorization = ArtifactBinding(
        artifact_type="holdout_selection_authorization",
        schema_version=1,
        sha256=artifact_sha256(initial_authorization_payload),
    )
    prior_evidence = {
        "mode": "initial_holdout",
        "all_previously_inspected_evidence_is_calibration_only": True,
        "inspected_artifacts": [inspected_evidence().to_dict()],
    }
    calibration_parent = None
    calibration_retry = None
    calibration_successor = None
    if calibration_index == 1:
        calibration_parent_result = calibration_insufficient_result(
            experiment_contract=frozen,
            experiment_id=CALIBRATION_PARENT_EXPERIMENT_ID,
            created_at_ms=calibration_completion_ms - 1,
        )
        calibration_parent = terminal_result_binding(
            calibration_parent_result
        )
        calibration_retry_payload = retry_eligibility_payload(
            stage="calibration",
            allocated_attempt=calibration_stage_attempt_identity,
            parent_result=calibration_parent_result,
            created_at_ms=(
                calibration_completion_ms
                if calibration_retry_created_at_ms_override is None
                else calibration_retry_created_at_ms_override
            ),
        )
        calibration_retry = ArtifactBinding(
            artifact_type="calibration_retry_eligibility",
            schema_version=1,
            sha256=artifact_sha256(calibration_retry_payload),
        )
        calibration_successor_payload = (
            calibration_successor_authorization_payload(
                allocated_attempt=calibration_stage_attempt_identity,
                parent_result=calibration_parent_result,
                retry_payload=calibration_retry_payload,
                experiment_contract=frozen,
                candidate_day_ledger=ArtifactBinding(
                    artifact_type="calibration_candidate_day_ledger",
                    schema_version=1,
                    sha256=text_digest("calibration_candidate_day_ledger"),
                ),
                selected_window=ArtifactBinding(
                    artifact_type="calibration_attempt_freeze",
                    schema_version=1,
                    sha256=text_digest("calibration_attempt_freeze"),
                ),
                provenance_continuity_root=digest("f"),
            )
        )
        calibration_successor = ArtifactBinding(
            artifact_type="calibration_successor_authorization",
            schema_version=1,
            sha256=artifact_sha256(calibration_successor_payload),
        )
        prior_evidence = {
            "mode": "calibration_quality_only_successor",
            "parent_result": calibration_parent.to_dict(),
            "retry_eligibility": calibration_retry.to_dict(),
            "successor_authorization": calibration_successor.to_dict(),
            "prior_attempt_was_loss_free_quality_only": True,
            "no_calibration_efficacy_generated_or_exposed": True,
            "inspected_artifacts": [
                inspected_evidence(
                    "prior_calibration_quality",
                    "6",
                    evidence_scope="calibration_quality_only",
                    inspection_role="prior_attempt_quality_only",
                    source_experiment_id=str(CALIBRATION_PARENT_EXPERIMENT_ID),
                ).to_dict()
            ],
        }
    if holdout_index == 1:
        holdout_parent_result = (
            holdout_parent_result_override
            or holdout_quality_failure_result(
                experiment_contract=frozen,
                calibration_index=calibration_index,
                experiment_id=HOLDOUT_PARENT_EXPERIMENT_ID,
            )
        )
        holdout_parent = terminal_result_binding(holdout_parent_result)
        retry_payload = retry_eligibility_payload(
            stage="holdout",
            allocated_attempt=attempt_identity,
            parent_result=holdout_parent_result,
            created_at_ms=anchor_ms,
        )
        retry_binding = ArtifactBinding(
            artifact_type="holdout_retry_eligibility",
            schema_version=1,
            sha256=artifact_sha256(retry_payload),
        )
        successor_payload = {
            "artifact_type": "holdout_successor_authorization",
            "schema_version": 1,
            "attempt": attempt_identity.to_dict(),
            "selection_anchor_source_sha256": retry_binding.sha256,
            "selection_anchor_ms": anchor_ms,
            "frozen_challenger": challenger.to_dict(),
            "experiment_contract_digest": frozen.digest,
            "parent_result": holdout_parent.to_dict(),
            "retry_eligibility_sha256": retry_binding.sha256,
            "calibration_efficacy_report_sha256": calibration_report.sha256,
            "calibration_efficacy_completed_sha256": (
                calibration_completion.sha256
            ),
            "holdout_window": {
                "start_ms": holdout_start_ms,
                "end_ms": holdout_start_ms + 86_400_000,
                "boundary": "[start_ms,end_ms)",
            },
            "candidate_day_ledger_root": digest("e"),
            "provenance_continuity_root": digest("f"),
            "successors_remaining_after_allocation": 0,
        }
        successor_binding = ArtifactBinding(
            artifact_type="holdout_successor_authorization",
            schema_version=1,
            sha256=artifact_sha256(successor_payload),
        )
        prior_evidence = {
            "mode": "holdout_quality_only_successor",
            "parent_result": holdout_parent.to_dict(),
            "retry_eligibility": retry_binding.to_dict(),
            "successor_authorization": successor_binding.to_dict(),
            "inherited_ancestry": (
                [
                    calibration_parent.to_dict(),
                    calibration_retry.to_dict(),
                    calibration_successor.to_dict(),
                ]
                if calibration_index == 1
                else []
            ),
            "prior_attempt_was_loss_free_quality_only": True,
            "no_holdout_efficacy_generated_or_exposed": True,
            "inspected_artifacts": [
                inspected_evidence(
                    "prior_quality",
                    "6",
                    evidence_scope="holdout_quality_only",
                    inspection_role="prior_attempt_quality_only",
                    source_experiment_id=str(HOLDOUT_PARENT_EXPERIMENT_ID),
                ).to_dict()
            ],
        }
    calibration = calibration_artifacts(
        calibration_index=calibration_index,
        retry_binding=calibration_retry,
        successor_binding=calibration_successor,
        report_binding=calibration_report,
        completion_binding=calibration_completion,
        authorization_binding=initial_authorization,
    )
    by_type = {artifact.artifact_type: artifact for artifact in calibration}
    if holdout_index == 0:
        anchor = SelectionAnchorProvenance(
            mode="calibration_completion",
            source_artifact=by_type["calibration_efficacy_completed"],
            timestamp_field="completed_at_ms",
            timestamp_ms=anchor_ms,
            authorization_artifact=by_type[
                "holdout_selection_authorization"
            ],
        )
    else:
        anchor = SelectionAnchorProvenance(
            mode="retry_eligibility",
            source_artifact=ArtifactBinding.from_dict(
                prior_evidence["retry_eligibility"],
                "prior_evidence.retry_eligibility",
            ),
            timestamp_field="created_at_ms",
            timestamp_ms=anchor_ms,
            authorization_artifact=ArtifactBinding.from_dict(
                prior_evidence["successor_authorization"],
                "prior_evidence.successor_authorization",
            ),
        )
    return V4Preregistration(
        attempt=attempt_identity,
        experiment_contract=frozen,
        selection_anchor_provenance=anchor,
        frozen_challenger=challenger,
        calibration_artifacts=calibration,
        calibration_efficacy_started_sha256=text_digest(
            "calibration_efficacy_started"
        ),
        calibration_efficacy_completed_sha256=calibration_completion.sha256,
        holdout_start_ms=holdout_start_ms,
        holdout_end_ms=holdout_start_ms + 86_400_000,
        archive_input_start_ms=archive_start,
        archive_input_end_ms=(
            holdout_start_ms + 86_400_000 + 3_700
        ),
        seal_timeout_ms=60_000,
        seal_deadline_ms=holdout_start_ms + 86_400_000 + 60_000,
        candidate_day_ledger_root=digest("e"),
        provenance_continuity_root=digest("f"),
        preregistration_publication_deadline_ms=publication_deadline,
        pushed_receipt_deadline_ms=archive_start - 86_400_000,
        authoritative_remote_ref="refs/heads/main",
        authoritative_remote_url_sha256=text_digest(
            "git@github.com:example/polycollector.git"
        ),
        origin_contract=origin_contract(),
        archive_health_contract={
            "archive_boundary_contract_sha256": digest("0"),
            "partition_pair_contract_sha256": digest("1"),
            "capture_maintenance_contract_sha256": digest("2"),
            "headroom_projection_contract_sha256": digest("3"),
            "seal_feasibility_contract_sha256": digest("4"),
            "checkpoint_schedule_sha256": digest("5"),
            "failure_rules_sha256": digest("6"),
        },
        provenance_contract={
            "precalibration_provenance_freeze_sha256": digest("7"),
            "continuity_ledger_contract_sha256": digest("8"),
            "checkpoint_schedule_sha256": digest("9"),
            "experiment_environment_sha256": digest("a"),
            "producer_identity_set_sha256": digest("b"),
            "current_provenance_continuity_root": digest("f"),
        },
        prior_evidence=prior_evidence,
        created_at_ms=publication_deadline,
    )


def calibration_insufficient_result(
    *,
    experiment_contract=None,
    experiment_id=EXPERIMENT_ID,
    created_at_ms=HOLDOUT_START_MS,
    calibration_index=0,
):
    frozen = experiment_contract or contract()
    attempt_identity = attempt(
        holdout_index=None,
        calibration_index=calibration_index,
        experiment_id=experiment_id,
    )
    parent_result = None
    parent_binding = None
    retry_binding = None
    successor_binding = None
    ancestry = ()
    candidate_ledger = binding(
        "calibration_candidate_day_ledger", "1"
    )
    selected_window = binding("calibration_attempt_freeze", "5")
    checkpoint = binding("stage_terminal_checkpoint", "3")
    if calibration_index == 1:
        parent_result = calibration_insufficient_result(
            experiment_contract=frozen,
            experiment_id=CALIBRATION_PARENT_EXPERIMENT_ID,
            created_at_ms=HOLDOUT_START_MS - 2 * 86_400_000 - 1,
        )
        parent_binding = terminal_result_binding(parent_result)
        retry_payload = retry_eligibility_payload(
            stage="calibration",
            allocated_attempt=attempt_identity,
            parent_result=parent_result,
            created_at_ms=HOLDOUT_START_MS - 2 * 86_400_000,
        )
        retry_binding = ArtifactBinding(
            artifact_type="calibration_retry_eligibility",
            schema_version=1,
            sha256=artifact_sha256(retry_payload),
        )
        successor_payload = calibration_successor_authorization_payload(
            allocated_attempt=attempt_identity,
            parent_result=parent_result,
            retry_payload=retry_payload,
            experiment_contract=frozen,
            candidate_day_ledger=candidate_ledger,
            selected_window=selected_window,
            provenance_continuity_root=checkpoint.sha256,
        )
        successor_binding = ArtifactBinding(
            artifact_type="calibration_successor_authorization",
            schema_version=1,
            sha256=artifact_sha256(successor_payload),
        )
        ancestry = (parent_binding, retry_binding, successor_binding)
    quality_report = binding(
        "calibration_quality_report:canonical_p0", "2"
    )
    raw_manifest = binding("calibration_raw_manifest", "4")
    return V4TerminalResult(
        attempt=attempt_identity,
        experiment_contract=frozen,
        terminal_stage="calibration",
        decision="insufficient_evidence",
        failure_stage="calibration_quality",
        failure_reasons=("quality_gate_failure",),
        parent_result=parent_binding,
        ancestry=ancestry,
        retry_state={
            "calibration_successors_used": calibration_index,
            "holdout_successors_used": 0,
            "calibration_successors_remaining": (
                1 if calibration_index == 0 else 0
            ),
            "holdout_successors_remaining": 0,
            "successor_allowed": calibration_index == 0,
            "retries_exhausted": calibration_index == 1,
            "lineage_closed": calibration_index == 1,
        },
        selection_anchor_provenance=None,
        candidate_day_ledger_root=candidate_ledger.sha256,
        provenance_continuity_root=checkpoint.sha256,
        preregistration_binding=None,
        preregistration_publication_deadline_ms=None,
        receipt_deadline_check=None,
        pushed_receipt=None,
        holdout_attempted=False,
        calibration_efficacy_started=False,
        calibration_efficacy_completed=False,
        calibration_start_marker=None,
        calibration_completion_marker=None,
        holdout_efficacy_started=False,
        holdout_efficacy_completed=False,
        holdout_start_marker=None,
        holdout_completion_marker=None,
        terminal_efficacy_completed_at_ms=None,
        efficacy_attempt_consumed=False,
        evidence_artifacts=(
            *((retry_binding,) if retry_binding is not None else ()),
            candidate_ledger,
            *((successor_binding,) if successor_binding is not None else ()),
            selected_window,
            binding("calibration_archive_checkpoint_manifest", "6"),
            raw_manifest,
            quality_report,
            checkpoint,
        ),
        provenance_checkpoint=checkpoint,
        quality_evidence={
            "status": "failed",
            "stage": "calibration",
            "cells": [
                quality_cell(
                    "calibration",
                    V4_TIMING_CELLS[0],
                    quality_report,
                    passed=False,
                )
            ],
            "archive_health_passed": True,
            "provenance_passed": True,
            "structural_gate_infeasibility_report_binding": None,
            "failure_codes": [
                "common_scored_coverage_below_minimum",
                "decision_eligible_coverage_below_minimum",
                "quality_stage_incomplete",
            ],
            "all_quality_gates_passed": False,
        },
        efficacy_evidence=None,
        frozen_challenger=None,
        created_at_ms=created_at_ms,
    )


def holdout_promotion_result(
    *,
    experiment_contract=None,
    holdout_index=0,
    calibration_index=0,
    experiment_id=EXPERIMENT_ID,
):
    frozen = experiment_contract or contract()
    prereg = preregistration(
        experiment_contract=frozen,
        holdout_index=holdout_index,
        calibration_index=calibration_index,
        experiment_id=experiment_id,
    )
    calibration_by_type = {
        item.artifact_type: item for item in prereg.calibration_artifacts
    }
    calibration_start = calibration_by_type["calibration_efficacy_started"]
    calibration_completion = calibration_by_type[
        "calibration_efficacy_completed"
    ]
    selection_authorization = (
        prereg.selection_anchor_provenance.authorization_artifact
    )
    calibration_successor_inventory = ()
    if holdout_index == 0 and calibration_index == 0:
        parent_result = None
        ancestry = ()
        pre_candidate_inventory = (selection_authorization,)
        post_candidate_inventory = ()
    elif holdout_index == 1:
        prior = prereg.prior_evidence
        inherited_ancestry = tuple(
            ArtifactBinding.from_dict(
                item, f"prior_evidence.inherited_ancestry[{index}]"
            )
            for index, item in enumerate(prior["inherited_ancestry"])
        )
        parent_result = ArtifactBinding.from_dict(
            prior["parent_result"], "prior_evidence.parent_result"
        )
        retry_eligibility = ArtifactBinding.from_dict(
            prior["retry_eligibility"], "prior_evidence.retry_eligibility"
        )
        ancestry = inherited_ancestry + (
            parent_result,
            retry_eligibility,
            selection_authorization,
        )
        calibration_successor_inventory = inherited_ancestry[1:]
        pre_candidate_inventory = (retry_eligibility,)
        post_candidate_inventory = (selection_authorization,)
    else:
        prior = prereg.prior_evidence
        parent_result = ArtifactBinding.from_dict(
            prior["parent_result"], "prior_evidence.parent_result"
        )
        retry_eligibility = ArtifactBinding.from_dict(
            prior["retry_eligibility"], "prior_evidence.retry_eligibility"
        )
        calibration_successor = ArtifactBinding.from_dict(
            prior["successor_authorization"],
            "prior_evidence.successor_authorization",
        )
        ancestry = (
            parent_result,
            retry_eligibility,
            calibration_successor,
        )
        calibration_successor_inventory = (
            retry_eligibility,
            calibration_successor,
        )
        pre_candidate_inventory = (selection_authorization,)
        post_candidate_inventory = ()
    holdout_start = binding("holdout_efficacy_started", "6")
    preregistration_binding = ArtifactBinding(
        artifact_type="chainlink_v4_holdout_preregistration",
        schema_version=1,
        sha256=artifact_sha256(prereg.to_dict()),
    )
    pushed_receipt_raw, receipt_check_raw = receipt_validation_artifacts(prereg)
    pushed_receipt = ArtifactBinding(
        artifact_type="holdout_pushed_preregistration_receipt",
        schema_version=1,
        sha256=hashlib.sha256(pushed_receipt_raw).hexdigest(),
    )
    receipt_check = ArtifactBinding(
        artifact_type="holdout_receipt_deadline_check",
        schema_version=1,
        sha256=hashlib.sha256(receipt_check_raw).hexdigest(),
    )
    final_checkpoint = binding("final_analysis_checkpoint", "9")
    candidate_ledger = ArtifactBinding(
        artifact_type="holdout_candidate_day_ledger",
        schema_version=1,
        sha256=prereg.candidate_day_ledger_root,
    )
    raw_manifest = binding("holdout_raw_manifest", "1")
    pre_efficacy_provenance_gate = binding(
        "holdout_pre_efficacy_provenance_gate", "2"
    )
    quality_reports = tuple(
        ArtifactBinding(
            artifact_type=f"holdout_quality_report:{cell.cell_id}",
            schema_version=1,
            sha256=text_digest(f"holdout_quality_report:{cell.cell_id}"),
        )
        for cell in V4_TIMING_CELLS
    )
    robustness_skills = {
        cell.cell_id: Decimal("0.04") for cell in V4_TIMING_CELLS[1:]
    }
    robustness_improvements = {
        cell.cell_id: Decimal("0") for cell in V4_TIMING_CELLS[1:]
    }
    efficacy_evidence = {
        "challenger_canonical_mae_skill": Decimal("0.06"),
        "control_canonical_mae_skill": Decimal("0.035"),
        "mae_skill_improvement_vs_control": Decimal("0.025"),
        "improvement_bootstrap_lower_bound": Decimal("0.001"),
        "challenger_canonical_rmse_skill": Decimal("0.03"),
        "control_canonical_rmse_skill": Decimal("0.03"),
        "rmse_skill_improvement_vs_control": Decimal("0"),
        "challenger_mae_skill_by_robustness_cell": robustness_skills,
        "control_mae_skill_by_robustness_cell": robustness_skills,
        "mae_skill_improvement_vs_control_by_robustness_cell": (
            robustness_improvements
        ),
        "bootstrap_seed_sha256": preregistration_binding.sha256,
        "bootstrap_seed_int": str(int(preregistration_binding.sha256, 16)),
        "bootstrap_contract_digest": canonical_sha256(
            FROZEN_BOOTSTRAP_CONTRACT
        ),
        "bootstrap_replicate_count": 10_000,
        "bootstrap_defined_replicate_count": 10_000,
        "gate_results": {
            "challenger_canonical_mae_skill": True,
            "mae_skill_improvement_vs_control": True,
            "improvement_bootstrap_lower_bound": True,
            "challenger_canonical_rmse_skill": True,
            "rmse_skill_improvement_vs_control": True,
            "challenger_mae_skill_all_robustness_cells": True,
            "robustness_improvement_all_cells": True,
            "no_rerank_or_runner_up_fallback": True,
        },
        "all_gates_passed": True,
    }
    efficacy_ledger = binding("holdout_efficacy_ledger", "3")
    bootstrap_report = binding("holdout_bootstrap_report", "8")
    efficacy_report = ArtifactBinding(
        artifact_type="holdout_efficacy_report",
        schema_version=1,
        sha256=artifact_sha256(
            terminal_efficacy_report_payload(
                attempt=prereg.attempt,
                experiment_contract_digest=frozen.digest,
                terminal_stage="holdout",
                decision="promotion_eligible",
                efficacy_evidence=efficacy_evidence,
                efficacy_ledger=efficacy_ledger,
                bootstrap_report=bootstrap_report,
            )
        ),
    )
    terminal_completion_ms = prereg.archive_input_end_ms
    holdout_completion = ArtifactBinding(
        artifact_type="holdout_efficacy_completed",
        schema_version=1,
        sha256=artifact_sha256(
            efficacy_completion_marker_payload(
                attempt=prereg.attempt,
                experiment_contract_digest=frozen.digest,
                terminal_stage="holdout",
                efficacy_start_marker=holdout_start,
                prerequisite_artifacts=(
                    preregistration_binding,
                    pushed_receipt,
                    receipt_check,
                    raw_manifest,
                    pre_efficacy_provenance_gate,
                ),
                efficacy_report=efficacy_report,
                immutable_efficacy_artifacts=(
                    holdout_start,
                    efficacy_ledger,
                    bootstrap_report,
                    efficacy_report,
                ),
                completed_at_ms=terminal_completion_ms,
            )
        ),
    )
    return V4TerminalResult(
        attempt=prereg.attempt,
        experiment_contract=frozen,
        terminal_stage="holdout",
        decision="promotion_eligible",
        failure_stage=None,
        failure_reasons=(),
        parent_result=parent_result,
        ancestry=ancestry,
        retry_state={
            "calibration_successors_used": calibration_index,
            "holdout_successors_used": holdout_index,
            "calibration_successors_remaining": 0,
            "holdout_successors_remaining": 0,
            "successor_allowed": False,
            "retries_exhausted": False,
            "lineage_closed": True,
        },
        selection_anchor_provenance=prereg.selection_anchor_provenance,
        candidate_day_ledger_root=candidate_ledger.sha256,
        provenance_continuity_root=final_checkpoint.sha256,
        preregistration_binding=preregistration_binding,
        preregistration_publication_deadline_ms=(
            prereg.preregistration_publication_deadline_ms
        ),
        receipt_deadline_check=receipt_check,
        pushed_receipt=pushed_receipt,
        holdout_attempted=True,
        calibration_efficacy_started=True,
        calibration_efficacy_completed=True,
        calibration_start_marker=calibration_start,
        calibration_completion_marker=calibration_completion,
        holdout_efficacy_started=True,
        holdout_efficacy_completed=True,
        holdout_start_marker=holdout_start,
        holdout_completion_marker=holdout_completion,
        terminal_efficacy_completed_at_ms=terminal_completion_ms,
        efficacy_attempt_consumed=True,
        evidence_artifacts=(
            *calibration_successor_inventory,
            calibration_start,
            calibration_completion,
            *pre_candidate_inventory,
            candidate_ledger,
            *post_candidate_inventory,
            preregistration_binding,
            pushed_receipt,
            receipt_check,
            binding("holdout_archive_checkpoint_manifest", "a"),
            raw_manifest,
            *quality_reports,
            pre_efficacy_provenance_gate,
            holdout_start,
            efficacy_ledger,
            bootstrap_report,
            efficacy_report,
            holdout_completion,
            final_checkpoint,
        ),
        provenance_checkpoint=final_checkpoint,
        quality_evidence=passed_quality_evidence(
            "holdout", quality_reports, origin=prereg.origin_contract
        ),
        efficacy_evidence=efficacy_evidence,
        frozen_challenger=prereg.frozen_challenger,
        created_at_ms=prereg.archive_input_end_ms,
    )


def holdout_quality_failure_result(
    *,
    experiment_contract=None,
    calibration_index=0,
    experiment_id=EXPERIMENT_ID,
):
    frozen = experiment_contract or contract()
    promotion = holdout_promotion_result(
        experiment_contract=frozen,
        holdout_index=0,
        calibration_index=calibration_index,
        experiment_id=experiment_id,
    )
    prereg = preregistration(
        experiment_contract=frozen,
        holdout_index=0,
        calibration_index=calibration_index,
        experiment_id=experiment_id,
    )
    first_cell = V4_TIMING_CELLS[0]
    first_report = next(
        item
        for item in promotion.evidence_artifacts
        if item.artifact_type
        == f"holdout_quality_report:{first_cell.cell_id}"
    )
    checkpoint = ArtifactBinding(
        artifact_type="stage_terminal_checkpoint",
        schema_version=1,
        sha256=text_digest(
            f"holdout_quality_terminal_checkpoint:{experiment_id}"
        ),
    )
    keep_types = {
        "calibration_retry_eligibility",
        "calibration_successor_authorization",
        "calibration_efficacy_started",
        "calibration_efficacy_completed",
        "holdout_selection_authorization",
        "holdout_candidate_day_ledger",
        "chainlink_v4_holdout_preregistration",
        "holdout_pushed_preregistration_receipt",
        "holdout_receipt_deadline_check",
        "holdout_archive_checkpoint_manifest",
        "holdout_raw_manifest",
        first_report.artifact_type,
    }
    return replace(
        promotion,
        decision="insufficient_evidence",
        failure_stage="holdout_quality",
        failure_reasons=("quality_gate_failure",),
        retry_state={
            "calibration_successors_used": calibration_index,
            "holdout_successors_used": 0,
            "calibration_successors_remaining": 0,
            "holdout_successors_remaining": 1,
            "successor_allowed": True,
            "retries_exhausted": False,
            "lineage_closed": False,
        },
        holdout_efficacy_started=False,
        holdout_efficacy_completed=False,
        holdout_start_marker=None,
        holdout_completion_marker=None,
        terminal_efficacy_completed_at_ms=None,
        efficacy_attempt_consumed=False,
        evidence_artifacts=tuple(
            item
            for item in promotion.evidence_artifacts
            if item.artifact_type in keep_types
        )
        + (checkpoint,),
        provenance_checkpoint=checkpoint,
        provenance_continuity_root=checkpoint.sha256,
        quality_evidence={
            "status": "failed",
            "stage": "holdout",
            "cells": [
                quality_cell(
                    "holdout",
                    first_cell,
                    first_report,
                    passed=False,
                    origin=prereg.origin_contract,
                )
            ],
            "archive_health_passed": True,
            "provenance_passed": True,
            "structural_gate_infeasibility_report_binding": None,
            "failure_codes": [
                "common_scored_coverage_below_minimum",
                "decision_eligible_coverage_below_minimum",
                "quality_stage_incomplete",
            ],
            "all_quality_gates_passed": False,
        },
        efficacy_evidence=None,
    )


def calibration_retain_result(
    *, experiment_contract=None, candidate_mae_override=None
):
    frozen = experiment_contract or contract()
    candidate_ledger = binding("calibration_candidate_day_ledger", "0")
    calibration_start = binding("calibration_efficacy_started", "1")
    final_checkpoint = binding("final_analysis_checkpoint", "3")
    quality_reports = tuple(
        ArtifactBinding(
            artifact_type=f"calibration_quality_report:{cell.cell_id}",
            schema_version=1,
            sha256=text_digest(
                f"calibration_quality_report:{cell.cell_id}"
            ),
        )
        for cell in V4_TIMING_CELLS
    )
    candidate_mae = candidate_mae_override or {
        "1500": Decimal("0.04"),
        "2000": Decimal("0.05"),
        "2500": Decimal("0.055"),
        "3000": Decimal("0.07"),
        "3500": Decimal("0.06"),
    }
    candidate_rmse = {
        lag: Decimal("0.02") for lag in candidate_mae
    }
    robustness = {
        cell.cell_id: dict(candidate_mae) for cell in V4_TIMING_CELLS[1:]
    }
    ordered_lags = sorted(
        (int(lag) for lag in candidate_mae),
        key=lambda lag: (
            -candidate_mae[str(lag)],
            [1_500, 2_000, 2_500, 3_000, 3_500].index(lag),
        ),
    )
    winner_lag, runner_up_lag = ordered_lags[:2]
    winner_mae = candidate_mae[str(winner_lag)]
    winner_rmse = candidate_rmse[str(winner_lag)]
    lead = winner_mae - candidate_mae[str(runner_up_lag)]
    deficits = {
        cell.cell_id: max(candidate_mae.values()) - winner_mae
        for cell in V4_TIMING_CELLS[1:]
    }
    promotion_eligible = winner_lag in (1_500, 2_000, 2_500)
    unique_best = lead > 0
    gate_results = {
        "winner_canonical_mae_skill": winner_mae >= Decimal("0.05"),
        "winner_canonical_rmse_skill": winner_rmse > 0,
        "mae_skill_lead_over_runner_up": lead >= Decimal("0.01"),
        "relative_robustness_all_cells": all(
            value <= Decimal("0.01") for value in deficits.values()
        ),
        "winner_promotion_eligible": promotion_eligible,
        "unique_best": unique_best,
    }
    attempt_identity = attempt(holdout_index=None)
    efficacy_evidence = {
        "ranking_metric": (
            "mae_skill_vs_horizon_matched_no_change_baseline"
        ),
        "candidate_canonical_mae_skill_by_lag": candidate_mae,
        "candidate_canonical_rmse_skill_by_lag": candidate_rmse,
        "candidate_mae_skill_by_robustness_cell": robustness,
        "ordered_candidate_lags_ms": ordered_lags,
        "winner_lag_ms": winner_lag,
        "runner_up_lag_ms": runner_up_lag,
        "winner_canonical_mae_skill": winner_mae,
        "winner_canonical_rmse_skill": winner_rmse,
        "mae_skill_lead_over_runner_up": lead,
        "winner_relative_deficit_by_robustness_cell": deficits,
        "winner_promotion_eligible": promotion_eligible,
        "boundary_winner": winner_lag == 1_500,
        "unique_best": unique_best,
        "gate_results": gate_results,
        "all_gates_passed": all(gate_results.values()),
    }
    efficacy_ledger = binding("calibration_efficacy_ledger", "7")
    attempt_freeze = binding("calibration_attempt_freeze", "9")
    raw_manifest = binding("calibration_raw_manifest", "5")
    pre_efficacy_provenance_gate = binding(
        "calibration_pre_efficacy_provenance_gate", "6"
    )
    efficacy_report = ArtifactBinding(
        artifact_type="calibration_efficacy_report",
        schema_version=1,
        sha256=artifact_sha256(
            terminal_efficacy_report_payload(
                attempt=attempt_identity,
                experiment_contract_digest=frozen.digest,
                terminal_stage="calibration",
                decision="retain_incumbent",
                efficacy_evidence=efficacy_evidence,
                efficacy_ledger=efficacy_ledger,
            )
        ),
    )
    terminal_completion_ms = HOLDOUT_START_MS
    calibration_completion = ArtifactBinding(
        artifact_type="calibration_efficacy_completed",
        schema_version=1,
        sha256=artifact_sha256(
            efficacy_completion_marker_payload(
                attempt=attempt_identity,
                experiment_contract_digest=frozen.digest,
                terminal_stage="calibration",
                efficacy_start_marker=calibration_start,
                prerequisite_artifacts=(
                    attempt_freeze,
                    raw_manifest,
                    pre_efficacy_provenance_gate,
                ),
                efficacy_report=efficacy_report,
                immutable_efficacy_artifacts=(
                    calibration_start,
                    efficacy_ledger,
                    efficacy_report,
                ),
                completed_at_ms=terminal_completion_ms,
            )
        ),
    )
    return V4TerminalResult(
        attempt=attempt_identity,
        experiment_contract=frozen,
        terminal_stage="calibration",
        decision="retain_incumbent",
        failure_stage=None,
        failure_reasons=(),
        parent_result=None,
        ancestry=(),
        retry_state={
            "calibration_successors_used": 0,
            "holdout_successors_used": 0,
            "calibration_successors_remaining": 0,
            "holdout_successors_remaining": 0,
            "successor_allowed": False,
            "retries_exhausted": False,
            "lineage_closed": True,
        },
        selection_anchor_provenance=None,
        candidate_day_ledger_root=candidate_ledger.sha256,
        provenance_continuity_root=final_checkpoint.sha256,
        preregistration_binding=None,
        preregistration_publication_deadline_ms=None,
        receipt_deadline_check=None,
        pushed_receipt=None,
        holdout_attempted=False,
        calibration_efficacy_started=True,
        calibration_efficacy_completed=True,
        calibration_start_marker=calibration_start,
        calibration_completion_marker=calibration_completion,
        holdout_efficacy_started=False,
        holdout_efficacy_completed=False,
        holdout_start_marker=None,
        holdout_completion_marker=None,
        terminal_efficacy_completed_at_ms=terminal_completion_ms,
        efficacy_attempt_consumed=False,
        evidence_artifacts=(
            candidate_ledger,
            attempt_freeze,
            binding("calibration_archive_checkpoint_manifest", "a"),
            raw_manifest,
            *quality_reports,
            pre_efficacy_provenance_gate,
            calibration_start,
            efficacy_ledger,
            efficacy_report,
            calibration_completion,
            final_checkpoint,
        ),
        provenance_checkpoint=final_checkpoint,
        quality_evidence=passed_quality_evidence(
            "calibration", quality_reports
        ),
        efficacy_evidence=efficacy_evidence,
        frozen_challenger=None,
        created_at_ms=HOLDOUT_START_MS,
    )


def rebind_terminal_efficacy_report(
    result,
    *,
    decision=None,
    efficacy_evidence=None,
    evidence_artifacts=None,
    **changes,
):
    """Replace performance inputs together with their immutable report hash."""

    rebound_decision = decision or result.decision
    rebound_evidence = (
        result.efficacy_evidence
        if efficacy_evidence is None
        else efficacy_evidence
    )
    rebound_artifacts = tuple(
        result.evidence_artifacts
        if evidence_artifacts is None
        else evidence_artifacts
    )
    by_type = {
        artifact.artifact_type: artifact
        for artifact in rebound_artifacts
    }
    report_type = f"{result.terminal_stage}_efficacy_report"
    report = ArtifactBinding(
        artifact_type=report_type,
        schema_version=1,
        sha256=artifact_sha256(
            terminal_efficacy_report_payload(
                attempt=result.attempt,
                experiment_contract_digest=result.experiment_contract.digest,
                terminal_stage=result.terminal_stage,
                decision=rebound_decision,
                efficacy_evidence=rebound_evidence,
                efficacy_ledger=by_type[
                    f"{result.terminal_stage}_efficacy_ledger"
                ],
                bootstrap_report=(
                    by_type["holdout_bootstrap_report"]
                    if result.terminal_stage == "holdout"
                    else None
                ),
            )
        ),
    )
    rebound_artifacts = tuple(
        report if artifact.artifact_type == report_type else artifact
        for artifact in rebound_artifacts
    )
    completion_type = f"{result.terminal_stage}_efficacy_completed"
    completion_ms = changes.get(
        "terminal_efficacy_completed_at_ms",
        result.terminal_efficacy_completed_at_ms,
    )
    completion_marker = ArtifactBinding(
        artifact_type=completion_type,
        schema_version=1,
        sha256=artifact_sha256(
            efficacy_completion_marker_payload(
                attempt=result.attempt,
                experiment_contract_digest=result.experiment_contract.digest,
                terminal_stage=result.terminal_stage,
                efficacy_start_marker=by_type[
                    f"{result.terminal_stage}_efficacy_started"
                ],
                prerequisite_artifacts=tuple(
                    by_type[artifact_type]
                    for artifact_type in (
                        (
                            "calibration_attempt_freeze",
                            "calibration_raw_manifest",
                            "calibration_pre_efficacy_provenance_gate",
                        )
                        if result.terminal_stage == "calibration"
                        else (
                            "chainlink_v4_holdout_preregistration",
                            "holdout_pushed_preregistration_receipt",
                            "holdout_receipt_deadline_check",
                            "holdout_raw_manifest",
                            "holdout_pre_efficacy_provenance_gate",
                        )
                    )
                ),
                efficacy_report=report,
                immutable_efficacy_artifacts=(
                    (
                        by_type["calibration_efficacy_started"],
                        by_type["calibration_efficacy_ledger"],
                        report,
                    )
                    if result.terminal_stage == "calibration"
                    else (
                        by_type["holdout_efficacy_started"],
                        by_type["holdout_efficacy_ledger"],
                        by_type["holdout_bootstrap_report"],
                        report,
                    )
                ),
                completed_at_ms=completion_ms,
            )
        ),
    )
    rebound_artifacts = tuple(
        completion_marker
        if artifact.artifact_type == completion_type
        else artifact
        for artifact in rebound_artifacts
    )
    completion_field = f"{result.terminal_stage}_completion_marker"
    changes.setdefault(completion_field, completion_marker)
    return replace(
        result,
        decision=rebound_decision,
        efficacy_evidence=rebound_evidence,
        evidence_artifacts=rebound_artifacts,
        **changes,
    )


def test_policy_freezes_exact_family_roles_and_ordered_timing_cells():
    frozen = contract()
    payload = frozen.to_dict()

    assert payload["policy_version"] == POLICY_VERSION
    assert [
        candidate["forecast_config"]["lag_ms"]
        for candidate in payload["comparison_family"]
    ] == [1_500, 2_000, 2_500, 3_000, 3_500]
    assert payload["offline_evaluation_policy"][
        "promotion_eligible_lags_ms"
    ] == [1_500, 2_000, 2_500]
    assert payload["offline_evaluation_policy"][
        "incumbent_comparison_lag_ms"
    ] == 3_000
    assert payload["offline_evaluation_policy"]["guardrail_lag_ms"] == 3_500
    assert [
        cell["cell_id"]
        for cell in payload["offline_evaluation_policy"]["timing_cells"]
    ] == [cell.cell_id for cell in V4_TIMING_CELLS]
    assert payload["offline_evaluation_policy"]["timing_cells"][0][
        "role"
    ] == "canonical"
    assert all(
        cell["role"] == "robustness"
        for cell in payload["offline_evaluation_policy"]["timing_cells"][1:]
    )
    cell_use = payload["offline_evaluation_policy"][
        "timing_cell_use_contract"
    ]
    assert cell_use["pool_cells"] is False
    assert cell_use["ranking_estimation_bootstrap_and_decision_cell"] == (
        "canonical_p0"
    )
    assert cell_use["robustness_cells_are_rejection_only"] == [
        cell.cell_id for cell in V4_TIMING_CELLS[1:]
    ]
    ranking = payload["offline_evaluation_policy"][
        "calibration_ranking_contract"
    ]
    assert ranking["metric"] == (
        "mae_skill_vs_horizon_matched_no_change_baseline"
    )
    assert ranking["rmse_can_rank_or_break_tie"] is False
    assert ranking["unclear_or_tied_winner_action"] == "retain_incumbent"
    assert payload["experiment_policy"]["calibration_quality_gates"] == (
        json.loads(canonical_json_bytes(FROZEN_QUALITY_GATES))
    )
    assert payload["experiment_policy"]["calibration_gates"] == (
        json.loads(canonical_json_bytes(FROZEN_CALIBRATION_GATES))
    )


def test_unspecified_staleness_values_are_explicitly_frozen_not_inherited():
    first = settings()
    second = replace(first, futures_stale_ms=1_001)
    first_digest = forecast_config_digest(first.config_for_lag(3_000))
    second_digest = forecast_config_digest(second.config_for_lag(3_000))

    assert first_digest != second_digest
    with pytest.raises(TypeError):
        V4ForecastSettings()  # type: ignore[call-arg]


@pytest.mark.parametrize(
    "overrides",
    [
        {"beta": Decimal("0.99")},
        {"reference_max_gap_ms": 251},
        {"max_future_skew_ms": 1},
        {"history_retention_ms": 8_749},
        {"anchor_rule": "changed"},
        {"futures_reference_rule": "changed"},
        {"same_poll_reference_rule": "changed"},
        {"projection_rule": "changed"},
        {"forecast_validity_rule": "changed"},
    ],
)
def test_v4_settings_reject_mutated_frozen_fields(overrides):
    with pytest.raises(ExperimentValidationError):
        replace(settings(), **overrides)


def test_full_and_non_lag_forecast_digests_have_separate_responsibilities():
    baseline = settings().config_for_lag(3_000)
    lag_change = replace(baseline, lag_ms=2_500, horizon_ms=2_500)
    beta_change = replace(baseline, beta=Decimal("0.9"))
    history_change = replace(baseline, history_retention_ms=10_001)
    rule_change = replace(baseline, projection_rule="changed_projection")

    assert forecast_config_digest(baseline) != forecast_config_digest(lag_change)
    assert non_lag_forecast_config_digest(baseline) == (
        non_lag_forecast_config_digest(lag_change)
    )
    for changed in (beta_change, history_change, rule_change):
        assert forecast_config_digest(baseline) != forecast_config_digest(changed)
        assert non_lag_forecast_config_digest(baseline) != (
            non_lag_forecast_config_digest(changed)
        )


@pytest.mark.parametrize(
    ("field_name", "changed_value", "non_lag_digest_changes"),
    [
        ("lag_ms", 3_001, False),
        ("horizon_ms", 3_001, False),
        ("beta", Decimal("0.9"), True),
        ("futures_stale_ms", 1_001, True),
        ("chainlink_stale_ms", 5_001, True),
        ("reference_max_gap_ms", 251, True),
        ("history_retention_ms", 10_001, True),
        ("max_future_skew_ms", 1, True),
        ("anchor_rule", "changed_anchor", True),
        ("futures_reference_rule", "changed_reference", True),
        ("same_poll_reference_rule", "changed_same_poll", True),
        ("projection_rule", "changed_projection", True),
        ("forecast_validity_rule", "changed_validity", True),
    ],
)
def test_every_forecast_config_field_is_digest_bound_and_rejected_if_mutated(
    field_name, changed_value, non_lag_digest_changes
):
    frozen = contract()
    baseline = frozen.forecast_settings.config_for_lag(3_000)
    changed = replace(baseline, **{field_name: changed_value})

    assert forecast_config_digest(changed) != forecast_config_digest(baseline)
    assert (
        non_lag_forecast_config_digest(changed)
        != non_lag_forecast_config_digest(baseline)
    ) is non_lag_digest_changes

    payload = deepcopy(frozen.to_dict())
    payload["comparison_family"][3]["forecast_config"][field_name] = (
        json.loads(canonical_json_bytes(changed.to_dict()))[field_name]
    )
    with pytest.raises(ExperimentValidationError):
        frozen.validate_payload(payload)


def test_forecast_digest_golden_vectors_are_canonical():
    config = settings().config_for_lag(3_000)

    assert forecast_config_digest(config) == (
        "cbf2706f12f15e98a9b95ae79b0e8feb6467ee0b9ff4cf995140bc1e87b5bd29"
    )
    assert non_lag_forecast_config_digest(config) == (
        "721eeaab34755a171d9465911c3bf11048beaeaeff64df5f2e56c7e7d7092533"
    )


def test_policy_digest_covers_family_cohorts_timing_and_delivery_not_config():
    frozen = contract()
    policy = json.loads(canonical_json_bytes(frozen.offline_evaluation_policy))
    original_policy_digest = frozen.offline_evaluation_policy_digest
    original_forecast_digest = forecast_config_digest(
        frozen.forecast_settings.config_for_lag(3_000)
    )

    assert original_policy_digest == canonical_sha256(policy)

    mutations = []
    changed_family = deepcopy(policy)
    changed_family["comparison_family"].reverse()
    mutations.append(changed_family)
    changed_pairing = deepcopy(policy)
    changed_pairing["baseline_pairing"] = "changed"
    mutations.append(changed_pairing)
    changed_baseline_construction = deepcopy(policy)
    changed_baseline_construction["baseline_contract"] = "changed"
    mutations.append(changed_baseline_construction)
    changed_cohort = deepcopy(policy)
    changed_cohort["common_cohort_contract"] = "changed"
    mutations.append(changed_cohort)
    changed_decision_cohort = deepcopy(policy)
    changed_decision_cohort["decision_cohort_contract"] = "changed"
    mutations.append(changed_decision_cohort)
    changed_cadence = deepcopy(policy)
    changed_cadence["scheduling_contract"]["generation_interval_ms"] = 1_000
    mutations.append(changed_cadence)
    changed_scheduling_origin = deepcopy(policy)
    changed_scheduling_origin["scheduling_contract"]["origin"] = "rebased"
    mutations.append(changed_scheduling_origin)
    changed_tie = deepcopy(policy)
    changed_tie["poll_and_tie_order_contract"][
        "exact_poll_and_target_ties_are_eligible"
    ] = False
    mutations.append(changed_tie)
    changed_missing = deepcopy(policy)
    changed_missing["missing_origin_contract"] = "changed"
    mutations.append(changed_missing)
    changed_actual = deepcopy(policy)
    changed_actual["target_resolution_contract"] = "changed"
    mutations.append(changed_actual)
    changed_continuity = deepcopy(policy)
    changed_continuity["finalization_and_continuity_contract"][
        "allowance_ms"
    ] = 201
    mutations.append(changed_continuity)
    changed_cell = deepcopy(policy)
    changed_cell["timing_cells"][0]["phase_offset_ms"] = 100
    mutations.append(changed_cell)
    changed_delivery = deepcopy(policy)
    changed_delivery["raw_delivery_metadata_contract"][
        "publisher_epoch"
    ] = "fabricated"
    mutations.append(changed_delivery)
    changed_cell_use = deepcopy(policy)
    changed_cell_use["timing_cell_use_contract"]["pool_cells"] = True
    mutations.append(changed_cell_use)
    changed_ranking = deepcopy(policy)
    changed_ranking["calibration_ranking_contract"]["direction"] = (
        "lowest_first"
    )
    mutations.append(changed_ranking)
    changed_control_role = deepcopy(policy)
    changed_control_role["offline_replacement_control"][
        "control_role_participates_in_family_ranking"
    ] = True
    mutations.append(changed_control_role)
    changed_promotion_set = deepcopy(policy)
    changed_promotion_set["promotion_eligible_lags_ms"].append(3_000)
    mutations.append(changed_promotion_set)
    changed_raw_order = deepcopy(policy)
    changed_raw_order["poll_and_tie_order_contract"]["raw_order"].reverse()
    mutations.append(changed_raw_order)
    changed_delay_semantics = deepcopy(policy)
    changed_delay_semantics["raw_delivery_metadata_contract"][
        "delay_semantics"
    ] = "measured_latency"
    mutations.append(changed_delay_semantics)

    assert all(
        canonical_sha256(changed) != original_policy_digest
        for changed in mutations
    )
    assert forecast_config_digest(
        frozen.forecast_settings.config_for_lag(3_000)
    ) == original_forecast_digest


def test_matching_incumbent_aliases_v4_3000_only_when_code_and_config_match():
    frozen = contract()

    assert frozen.replacement_control.mode is ControlMode.V4_3000_ALIAS
    assert frozen.replacement_control.decision_scope == "lag_only"
    assert frozen.control_identity != frozen.candidate_identity(3_000)
    assert frozen.control_identity.model_role == (
        "offline_replay_replacement_control"
    )
    assert frozen.control_identity.model_version == (
        frozen.candidate_identity(3_000).model_version
    )
    assert frozen.control_identity.forecast_config_digest == (
        frozen.candidate_identity(3_000).forecast_config_digest
    )


def test_non_lag_config_mismatch_uses_distinct_operational_control():
    active_config = replace(
        settings().config_for_lag(3_000),
        reference_max_gap_ms=3_000,
        max_future_skew_ms=250,
    )
    frozen = contract(active=incumbent(config=active_config))

    assert frozen.replacement_control.mode is (
        ControlMode.DISTINCT_OPERATIONAL_CONTROL
    )
    assert frozen.replacement_control.decision_scope == (
        "complete_v4_challenger_configuration"
    )
    assert frozen.control_identity.model_role == (
        "offline_replay_replacement_control"
    )
    assert frozen.control_identity.model_version == "catchup_ratio_l3000_b100"
    assert frozen.control_identity != frozen.candidate_identity(3_000)


def test_code_mismatch_is_not_hidden_by_matching_model_name_or_config():
    active = incumbent(code=code_manifest("e"))
    resolution = resolve_replacement_control(
        active_incumbent=active,
        v4_3000_config=settings().config_for_lag(3_000),
        v4_forecast_code=code_manifest(),
    )

    assert active.primary_model_version == "catchup_ratio_l3000_b100"
    assert resolution.active_full_config_digest == (
        resolution.v4_3000_full_config_digest
    )
    assert resolution.mode is ControlMode.DISTINCT_OPERATIONAL_CONTROL


def test_active_primary_and_loaded_installed_provenance_fail_closed():
    with pytest.raises(IncumbentProvenanceError, match="identities differ"):
        incumbent(installed=digest("6"))
    with pytest.raises(IncumbentProvenanceError, match="lag_ms=3000"):
        incumbent(config=settings().config_for_lag(2_500))
    with pytest.raises(IncumbentProvenanceError, match="model version"):
        incumbent(model_version="catchup_ratio_l2500_b100")
    with pytest.raises(IncumbentProvenanceError, match="model version"):
        incumbent(model_version="unparseable-primary")


def test_contract_records_every_required_split_digest():
    digests = contract().to_dict()["digests"]

    assert set(digests) == {
        "active_incumbent_selection_sha256",
        "active_incumbent_replay_config_sha256",
        "active_incumbent_primary_model_version",
        "active_incumbent_forecast_config_digest",
        "active_incumbent_non_lag_forecast_config_digest",
        "v4_3000_forecast_config_digest",
        "v4_non_lag_forecast_config_digest",
        "active_incumbent_forecast_code_digest",
        "v4_forecast_code_digest",
        "offline_evaluation_policy_digest",
    }
    assert all(
        isinstance(value, str) and len(value) == 64
        for key, value in digests.items()
        if key != "active_incumbent_primary_model_version"
    )


def test_canonical_json_rejects_floats_duplicate_keys_and_nonfinite_values():
    with pytest.raises(ExperimentValidationError, match="floating-point"):
        canonical_json_bytes({"bad": 1.5})
    with pytest.raises(ExperimentValidationError, match="duplicate"):
        decode_strict_json('{"a":1,"a":2}')
    with pytest.raises(ExperimentValidationError, match="non-finite"):
        decode_strict_json('{"a":NaN}')


def test_preregistration_round_trip_is_strict_decimal_only_and_complete():
    frozen = contract()
    prereg = preregistration(experiment_contract=frozen)
    raw = canonical_artifact_bytes(prereg.to_dict())
    payload = validate_preregistration(
        raw,
        expected_contract=frozen,
        **preregistration_validation_kwargs(prereg),
        expected=prereg,
    )

    assert payload["artifact_type"] == "chainlink_v4_holdout_preregistration"
    assert payload["schema_version"] == 1
    assert payload["holdout_window"]["end_ms"] - payload[
        "holdout_window"
    ]["start_ms"] == 86_400_000
    assert payload["quality_gates"]["cohort_classification_percent"] == "1"
    assert payload["holdout_performance_gates"][
        "challenger_canonical_mae_skill_minimum"
    ] == "0.05"
    assert canonical_json_bytes(json.loads(raw)["bootstrap_contract"]) == (
        canonical_json_bytes(FROZEN_BOOTSTRAP_CONTRACT)
    )


@pytest.mark.parametrize(
    "mutate",
    [
        lambda payload: payload.update(policy_version="changed"),
        lambda payload: payload.update(schema_version=2),
        lambda payload: payload.update(unexpected=True),
        lambda payload: payload["timing_cells"].reverse(),
        lambda payload: payload["experiment_contract"]["comparison_family"][0][
            "forecast_config"
        ].update(lag_ms=1_501),
        lambda payload: payload["bootstrap_contract"].update(replicates=9_999),
        lambda payload: payload["quality_gates"].update(
            canonical_common_scored_minimum=1
        ),
        lambda payload: payload["holdout_performance_gates"].update(
            challenger_canonical_mae_skill_minimum="0.049"
        ),
        lambda payload: payload["origin_contract"].update(
            observed_counts_by_cell={}
        ),
        lambda payload: payload["prohibitions"].update(rerank=False),
        lambda payload: payload["decimal_context"].update(precision=28),
        lambda payload: payload["holdout_window"].update(
            end_ms=payload["holdout_window"]["end_ms"] + 1
        ),
        lambda payload: payload["prior_evidence"].update(
            all_previously_inspected_evidence_is_calibration_only=False
        ),
    ],
)
def test_preregistration_mutations_fail_closed(mutate):
    frozen = contract()
    prereg = preregistration(experiment_contract=frozen)
    payload = deepcopy(prereg.to_dict())
    mutate(payload)

    with pytest.raises(ExperimentValidationError):
        validate_preregistration(
            payload,
            expected_contract=frozen,
            **preregistration_validation_kwargs(prereg),
            expected=prereg,
        )


def test_preregistration_requires_eligible_shorter_challenger_and_timely_publish():
    frozen = contract()
    valid = preregistration(experiment_contract=frozen)

    with pytest.raises(ExperimentValidationError, match="eligible shorter"):
        replace(valid, frozen_challenger=frozen.candidate_identity(3_000))
    with pytest.raises(ExperimentValidationError, match="published late"):
        replace(valid, created_at_ms=valid.created_at_ms + 1)


def test_frozen_challenger_must_come_from_bound_calibration_report():
    original = preregistration()
    changed_challenger = original.experiment_contract.candidate_identity(2_500)
    authorization_payload = anchor_authorization_payload(original)
    authorization_payload["frozen_challenger"] = changed_challenger.to_dict()
    authorization_binding = ArtifactBinding(
        artifact_type="holdout_selection_authorization",
        schema_version=1,
        sha256=artifact_sha256(authorization_payload),
    )
    changed_artifacts = tuple(
        authorization_binding
        if artifact.artifact_type == "holdout_selection_authorization"
        else artifact
        for artifact in original.calibration_artifacts
    )
    changed_anchor = replace(
        original.selection_anchor_provenance,
        authorization_artifact=authorization_binding,
    )
    changed = replace(
        original,
        frozen_challenger=changed_challenger,
        calibration_artifacts=changed_artifacts,
        selection_anchor_provenance=changed_anchor,
    )
    original_validation = preregistration_validation_kwargs(original)

    with pytest.raises(
        ExperimentValidationError,
        match="immutable calibration report|derived calibration winner",
    ):
        validate_preregistration(
            canonical_artifact_bytes(changed.to_dict()),
            expected_contract=changed.experiment_contract,
            selection_anchor_source_artifact=original_validation[
                "selection_anchor_source_artifact"
            ],
            selection_anchor_authorization_artifact=(
                canonical_artifact_bytes(authorization_payload)
            ),
            calibration_efficacy_report_artifact=original_validation[
                "calibration_efficacy_report_artifact"
            ],
            calibration_completion_marker_artifact=original_validation[
                "calibration_completion_marker_artifact"
            ],
            expected_calibration_efficacy_report=original_validation[
                "expected_calibration_efficacy_report"
            ],
            expected_calibration_completion_marker=original_validation[
                "expected_calibration_completion_marker"
            ],
            expected_prior_evidence_artifacts=original_validation[
                "expected_prior_evidence_artifacts"
            ],
        )


def test_preregistration_prior_inventory_must_match_authoritative_inventory():
    prereg = preregistration()
    validation = preregistration_validation_kwargs(prereg)
    validation["expected_prior_evidence_artifacts"] = (
        inspected_evidence("decoy_prior_evidence", "d"),
    )

    with pytest.raises(ExperimentValidationError, match="authoritative"):
        validate_preregistration(
            canonical_artifact_bytes(prereg.to_dict()),
            expected_contract=prereg.experiment_contract,
            **validation,
        )

    validation.pop("expected_prior_evidence_artifacts")
    with pytest.raises(ExperimentValidationError, match="authoritative"):
        validate_preregistration(
            canonical_artifact_bytes(prereg.to_dict()),
            expected_contract=prereg.experiment_contract,
            **validation,
        )


def test_preregistration_retry_requires_loss_free_parent_declarations():
    retry = preregistration(holdout_index=1)
    validate_preregistration(
        retry.to_dict(),
        expected_contract=retry.experiment_contract,
        **preregistration_validation_kwargs(retry),
    )

    prior = dict(retry.prior_evidence)
    prior["no_holdout_efficacy_generated_or_exposed"] = False
    with pytest.raises(ExperimentValidationError, match="loss-free"):
        replace(retry, prior_evidence=prior)


def test_successor_cannot_use_a_promotion_result_as_its_parent():
    promoted_parent = holdout_promotion_result(
        experiment_id=HOLDOUT_PARENT_EXPERIMENT_ID
    )
    successor = preregistration(
        holdout_index=1,
        holdout_parent_result_override=promoted_parent,
    )

    with pytest.raises(
        ExperimentValidationError, match="unsupported fields|loss-free retryable"
    ):
        validate_preregistration(
            canonical_artifact_bytes(successor.to_dict()),
            expected_contract=successor.experiment_contract,
            **preregistration_validation_kwargs(
                successor, successor_parent_result=promoted_parent
            ),
        )


def test_successor_parent_must_be_an_intrinsically_valid_terminal_result():
    valid_parent = holdout_quality_failure_result(
        experiment_id=HOLDOUT_PARENT_EXPERIMENT_ID
    )
    forged_payload = deepcopy(valid_parent.to_dict())
    forged_payload["failure_reasons"] = ["fabricated_reason"]
    forged_payload["quality_evidence"] = {"status": "not_reached"}

    class ForgedParent:
        failure_stage = forged_payload["failure_stage"]

        @staticmethod
        def to_dict():
            return forged_payload

    forged_parent = ForgedParent()
    successor = preregistration(
        holdout_index=1,
        holdout_parent_result_override=forged_parent,
    )

    with pytest.raises(ExperimentValidationError):
        validate_preregistration(
            canonical_artifact_bytes(successor.to_dict()),
            expected_contract=successor.experiment_contract,
            **preregistration_validation_kwargs(
                successor, successor_parent_result=forged_parent
            ),
        )


def test_successor_retry_eligibility_cannot_predate_its_parent():
    backdated_ms = HOLDOUT_START_MS - 2 * 86_400_000 - 2
    successor = preregistration(
        calibration_index=1,
        calibration_retry_created_at_ms_override=backdated_ms,
    )

    with pytest.raises(ExperimentValidationError, match="predates its terminal"):
        validate_preregistration(
            canonical_artifact_bytes(successor.to_dict()),
            expected_contract=successor.experiment_contract,
            **preregistration_validation_kwargs(
                successor,
                calibration_retry_created_at_ms=backdated_ms,
            ),
        )


@pytest.mark.parametrize("holdout_index", [0, 1])
def test_preregistration_prior_evidence_cannot_hide_holdout_efficacy(
    holdout_index,
):
    prereg = preregistration(holdout_index=holdout_index)
    prior = dict(prereg.prior_evidence)
    evidence_scope = (
        "holdout_quality_only" if holdout_index == 1 else "calibration_only"
    )
    prior["inspected_artifacts"] = [
        inspected_evidence(
            "holdout_efficacy_report",
            "a",
            evidence_scope=evidence_scope,
            inspection_role=(
                "prior_attempt_quality_only"
                if holdout_index == 1
                else "historical_holdout"
            ),
        ).to_dict()
    ]

    with pytest.raises(ExperimentValidationError, match="holdout efficacy"):
        replace(prereg, prior_evidence=prior)


def test_initial_prior_evidence_accepts_old_holdout_as_calibration_input():
    prereg = preregistration()
    prior = dict(prereg.prior_evidence)
    prior["inspected_artifacts"] = [
        inspected_evidence(
            "legacy_old_holdout_report",
            "7",
            inspection_role="historical_holdout",
        ).to_dict()
    ]
    scoped = replace(prereg, prior_evidence=prior)

    validate_preregistration(
        canonical_artifact_bytes(scoped.to_dict()),
        expected_contract=scoped.experiment_contract,
        **preregistration_validation_kwargs(scoped),
    )


def test_initial_prior_evidence_cannot_relabel_current_holdout_quality():
    prereg = preregistration()
    validation = preregistration_validation_kwargs(prereg)
    validation["expected_prior_evidence_artifacts"] = (
        inspected_evidence(
            "old_replay",
            "4",
            evidence_scope="holdout_quality_only",
            inspection_role="prior_attempt_quality_only",
            source_experiment_id=str(HOLDOUT_PARENT_EXPERIMENT_ID),
        ),
    )

    with pytest.raises(ExperimentValidationError, match="authoritative inventory"):
        validate_preregistration(
            canonical_artifact_bytes(prereg.to_dict()),
            expected_contract=prereg.experiment_contract,
            **validation,
        )


def test_insufficient_result_round_trip_has_no_efficacy_fields():
    frozen = contract()
    result = calibration_insufficient_result(experiment_contract=frozen)
    payload = validate_terminal_result(
        canonical_artifact_bytes(result.to_dict()),
        expected_contract=frozen,
        expected=result,
    )

    assert payload["decision"] == "insufficient_evidence"
    assert "efficacy_evidence" not in payload
    assert payload["efficacy_attempt_consumed"] is False
    assert payload["holdout_attempted"] is False
    assert payload["attempt"]["holdout_attempt_index"] is None


def test_last_calibration_successor_records_retry_exhaustion():
    result = calibration_insufficient_result(calibration_index=1)
    payload = validate_terminal_result(
        canonical_artifact_bytes(result.to_dict()),
        expected_contract=result.experiment_contract,
        **calibration_successor_validation_kwargs(result),
        expected=result,
    )

    assert payload["attempt"]["calibration_attempt_index"] == 1
    assert payload["retry_state"] == {
        "calibration_successors_used": 1,
        "holdout_successors_used": 0,
        "calibration_successors_remaining": 0,
        "holdout_successors_remaining": 0,
        "successor_allowed": False,
        "retries_exhausted": True,
        "lineage_closed": True,
    }

    changed = deepcopy(result.to_dict())
    changed["retry_state"]["retries_exhausted"] = False
    with pytest.raises(ExperimentValidationError, match="retry_state"):
        validate_terminal_result(
            changed,
            expected_contract=result.experiment_contract,
            **calibration_successor_validation_kwargs(result),
        )


def test_calibration_successor_authorization_binds_selected_window_and_ledger():
    result = calibration_insufficient_result(calibration_index=1)
    validation = calibration_successor_validation_kwargs(result)
    authorization = decode_strict_json(
        validation["calibration_successor_authorization_artifact"]
    )
    authorization["selected_window_artifact"]["sha256"] = digest("e")
    authorization["candidate_day_ledger_root"] = digest("f")
    rebound_authorization = ArtifactBinding(
        artifact_type="calibration_successor_authorization",
        schema_version=1,
        sha256=artifact_sha256(authorization),
    )
    rebound = replace(
        result,
        ancestry=(*result.ancestry[:2], rebound_authorization),
        evidence_artifacts=tuple(
            (
                rebound_authorization
                if artifact.artifact_type
                == "calibration_successor_authorization"
                else artifact
            )
            for artifact in result.evidence_artifacts
        ),
    )
    validation["calibration_successor_authorization_artifact"] = (
        canonical_artifact_bytes(authorization)
    )

    with pytest.raises(
        ExperimentValidationError,
        match="parent, eligibility, and contract",
    ):
        validate_terminal_result(
            rebound.to_dict(),
            expected_contract=rebound.experiment_contract,
            **validation,
        )


def test_calibration_successor_authorization_binds_authoritative_provenance():
    result = calibration_insufficient_result(calibration_index=1)
    validation = calibration_successor_validation_kwargs(result)
    authorization = decode_strict_json(
        validation["calibration_successor_authorization_artifact"]
    )
    authorization["provenance_continuity_root"] = digest("e")
    rebound_authorization = ArtifactBinding(
        artifact_type="calibration_successor_authorization",
        schema_version=1,
        sha256=artifact_sha256(authorization),
    )
    rebound = replace(
        result,
        ancestry=(*result.ancestry[:2], rebound_authorization),
        evidence_artifacts=tuple(
            (
                rebound_authorization
                if artifact.artifact_type
                == "calibration_successor_authorization"
                else artifact
            )
            for artifact in result.evidence_artifacts
        ),
    )
    validation["calibration_successor_authorization_artifact"] = (
        canonical_artifact_bytes(authorization)
    )

    with pytest.raises(ExperimentValidationError, match="provenance root"):
        validate_terminal_result(
            rebound.to_dict(),
            expected_contract=rebound.experiment_contract,
            **validation,
        )


def test_holdout_promotion_round_trip_binds_challenger_control_and_markers():
    frozen = contract()
    result = holdout_promotion_result(experiment_contract=frozen)
    payload = validate_terminal_result(
        result.to_dict(),
        expected_contract=frozen,
        **bound_preregistration_validation_kwargs(result),
    )

    assert payload["decision"] == "promotion_eligible"
    assert payload["holdout_efficacy_started"] is True
    assert payload["holdout_efficacy_completed"] is True
    assert payload["efficacy_attempt_consumed"] is True
    assert payload["frozen_challenger"] == frozen.candidate_identity(
        2_000
    ).to_dict()
    assert payload["experiment_contract"]["control_identity"] == (
        frozen.control_identity.to_dict()
    )
    assert payload["efficacy_evidence"][
        "challenger_canonical_mae_skill"
    ] == "0.06"


def test_holdout_completion_cannot_precede_frozen_archive_input_tail():
    result = holdout_promotion_result()
    prereg = preregistration(experiment_contract=result.experiment_contract)
    early_completion = rebind_terminal_efficacy_report(
        result,
        terminal_efficacy_completed_at_ms=prereg.holdout_end_ms,
    )

    with pytest.raises(ExperimentValidationError, match="archive input tail"):
        validate_terminal_result(
            early_completion.to_dict(),
            expected_contract=early_completion.experiment_contract,
            **bound_preregistration_validation_kwargs(early_completion),
        )


def test_holdout_result_rejects_substituted_pushed_receipt_binding():
    result = holdout_promotion_result()
    substituted_receipt = replace(
        result.pushed_receipt,
        sha256=text_digest("substituted_pushed_receipt"),
    )
    substituted_inventory = tuple(
        (
            substituted_receipt
            if artifact.artifact_type
            == "holdout_pushed_preregistration_receipt"
            else artifact
        )
        for artifact in result.evidence_artifacts
    )
    with pytest.raises(ExperimentValidationError, match="completion marker"):
        replace(
            result,
            pushed_receipt=substituted_receipt,
            evidence_artifacts=substituted_inventory,
        )


def test_promotion_rejects_a_transitively_bound_but_late_pushed_receipt():
    result = holdout_promotion_result()
    prereg = preregistration(experiment_contract=result.experiment_contract)
    late_receipt_payload = pushed_preregistration_receipt_payload(
        attempt=result.attempt,
        preregistration=result.preregistration_binding,
        authoritative_remote_url_sha256=(
            prereg.authoritative_remote_url_sha256
        ),
        pushed_commit_id="2" * 40,
        observed_remote_ref=prereg.authoritative_remote_ref,
        observed_remote_commit_id="2" * 40,
        verified_at_ms=prereg.pushed_receipt_deadline_ms + 1,
    )
    late_receipt = ArtifactBinding(
        artifact_type="holdout_pushed_preregistration_receipt",
        schema_version=1,
        sha256=artifact_sha256(late_receipt_payload),
    )
    late_check_payload = receipt_deadline_check_payload(
        attempt=result.attempt,
        preregistration=result.preregistration_binding,
        authoritative_remote_url_sha256=(
            prereg.authoritative_remote_url_sha256
        ),
        expected_remote_ref=prereg.authoritative_remote_ref,
        pushed_receipt_deadline_ms=prereg.pushed_receipt_deadline_ms,
        checked_at_ms=prereg.pushed_receipt_deadline_ms + 2,
        pushed_receipt=late_receipt,
        pushed_receipt_payload_value=late_receipt_payload,
    )
    late_check = ArtifactBinding(
        artifact_type="holdout_receipt_deadline_check",
        schema_version=1,
        sha256=artifact_sha256(late_check_payload),
    )
    rebound_inventory = tuple(
        (
            late_receipt
            if artifact.artifact_type
            == "holdout_pushed_preregistration_receipt"
            else late_check
            if artifact.artifact_type == "holdout_receipt_deadline_check"
            else artifact
        )
        for artifact in result.evidence_artifacts
    )
    rebound = rebind_terminal_efficacy_report(
        result,
        evidence_artifacts=rebound_inventory,
        pushed_receipt=late_receipt,
        receipt_deadline_check=late_check,
    )
    validation = bound_preregistration_validation_kwargs(rebound)
    validation["pushed_receipt_artifact"] = canonical_artifact_bytes(
        late_receipt_payload
    )
    validation["receipt_deadline_check_artifact"] = canonical_artifact_bytes(
        late_check_payload
    )

    with pytest.raises(ExperimentValidationError, match="timely pushed receipt"):
        validate_terminal_result(
            rebound.to_dict(),
            expected_contract=rebound.experiment_contract,
            **validation,
        )


def test_holdout_result_requires_a_strictly_valid_bound_preregistration():
    result = holdout_promotion_result()
    validation = bound_preregistration_validation_kwargs(result)
    preregistration_payload = json.loads(
        validation["preregistration_artifact"]
    )
    preregistration_payload["quality_gates"]["causal_violations"] = 99
    preregistration_raw = canonical_artifact_bytes(preregistration_payload)
    preregistration_binding = replace(
        result.preregistration_binding,
        sha256=hashlib.sha256(preregistration_raw).hexdigest(),
    )
    efficacy = deepcopy(result.to_dict()["efficacy_evidence"])
    efficacy["bootstrap_seed_sha256"] = preregistration_binding.sha256
    efficacy["bootstrap_seed_int"] = str(
        int(preregistration_binding.sha256, 16)
    )
    rebound_artifacts = tuple(
        preregistration_binding
        if item.artifact_type
        == "chainlink_v4_holdout_preregistration"
        else item
        for item in result.evidence_artifacts
    )
    rebound = rebind_terminal_efficacy_report(
        result,
        efficacy_evidence=efficacy,
        evidence_artifacts=rebound_artifacts,
        preregistration_binding=preregistration_binding,
    )

    with pytest.raises(
        ExperimentValidationError,
        match="quality gates|pushed preregistration receipt",
    ):
        validate_terminal_result(
            rebound.to_dict(),
            expected_contract=rebound.experiment_contract,
            preregistration_artifact=preregistration_raw,
            selection_anchor_source_artifact=validation[
                "selection_anchor_source_artifact"
            ],
            selection_anchor_authorization_artifact=validation[
                "selection_anchor_authorization_artifact"
            ],
            calibration_efficacy_report_artifact=validation[
                "calibration_efficacy_report_artifact"
            ],
            calibration_completion_marker_artifact=validation[
                "calibration_completion_marker_artifact"
            ],
            expected_calibration_efficacy_report=validation[
                "expected_calibration_efficacy_report"
            ],
            expected_calibration_completion_marker=validation[
                "expected_calibration_completion_marker"
            ],
            expected_prior_evidence_artifacts=validation[
                "expected_prior_evidence_artifacts"
            ],
            pushed_receipt_artifact=validation[
                "pushed_receipt_artifact"
            ],
            receipt_deadline_check_artifact=validation[
                "receipt_deadline_check_artifact"
            ],
        )


def test_holdout_result_cannot_predate_anchor_or_holdout_completion():
    result = holdout_promotion_result()

    with pytest.raises(ExperimentValidationError, match="selection anchor"):
        before_anchor_ms = result.selection_anchor_provenance.timestamp_ms - 1
        rebind_terminal_efficacy_report(
            result,
            created_at_ms=before_anchor_ms,
            terminal_efficacy_completed_at_ms=before_anchor_ms,
        )

    early_result_ms = result.selection_anchor_provenance.timestamp_ms + 1
    after_anchor_but_before_holdout = rebind_terminal_efficacy_report(
        result,
        created_at_ms=early_result_ms,
        terminal_efficacy_completed_at_ms=early_result_ms,
    )
    with pytest.raises(
        ExperimentValidationError,
        match="necessary boundary|archive input tail",
    ):
        validate_terminal_result(
            after_anchor_but_before_holdout.to_dict(),
            expected_contract=after_anchor_but_before_holdout.experiment_contract,
            **bound_preregistration_validation_kwargs(
                after_anchor_but_before_holdout
            ),
        )


@pytest.mark.parametrize(
    "mutate",
    [
        lambda payload, frozen, _prereg: payload.update(
            frozen_challenger=frozen.candidate_identity(2_500).to_dict()
        ),
        lambda payload, _frozen, _prereg: (
            payload["calibration_start_marker"].update(
                sha256=text_digest("substituted_calibration_start")
            ),
            next(
                artifact
                for artifact in payload["evidence_artifacts"]
                if artifact["artifact_type"]
                == "calibration_efficacy_started"
            ).update(sha256=text_digest("substituted_calibration_start")),
        ),
        lambda payload, _frozen, prereg: payload["quality_evidence"][
            "cells"
        ][0]["scheduled_vector_binding"].update(
            sha256=prereg.origin_contract[
                "target_eligible_mask_sha256_by_cell"
            ]["canonical_p0"]
        ),
    ],
)
def test_holdout_result_cannot_substitute_preregistered_inputs(mutate):
    result = holdout_promotion_result()
    payload = deepcopy(result.to_dict())
    prereg = preregistration(experiment_contract=result.experiment_contract)
    mutate(payload, result.experiment_contract, prereg)

    with pytest.raises(
        ExperimentValidationError,
        match=(
            "bound preregistration|authoritative lineage|"
            "selection authorization"
        ),
    ):
        validate_terminal_result(
            payload,
            expected_contract=result.experiment_contract,
            **bound_preregistration_validation_kwargs(result),
        )


def test_successor_result_round_trip_binds_exact_preregistered_ancestry():
    result = holdout_promotion_result(holdout_index=1)
    payload = validate_terminal_result(
        result.to_dict(),
        expected_contract=result.experiment_contract,
        **bound_preregistration_validation_kwargs(result),
    )

    assert payload["attempt"]["holdout_attempt_index"] == 1
    assert [item["artifact_type"] for item in payload["ancestry"][-3:]] == [
        "chainlink_v4_terminal_result",
        "holdout_retry_eligibility",
        "holdout_successor_authorization",
    ]

    changed = deepcopy(result.to_dict())
    changed["parent_result"]["sha256"] = digest("d")
    changed["ancestry"][-3]["sha256"] = digest("d")
    with pytest.raises(
        ExperimentValidationError,
        match="bound preregistration|authoritative lineage",
    ):
        validate_terminal_result(
            changed,
            expected_contract=result.experiment_contract,
            **bound_preregistration_validation_kwargs(result),
        )


@pytest.mark.parametrize(
    ("field_name", "replacement_value"),
    [
        ("candidate_day_ledger_root", digest("0")),
        ("provenance_continuity_root", digest("0")),
        ("successors_remaining_after_allocation", 1),
    ],
)
def test_holdout_successor_authorization_binds_immutable_state(
    field_name, replacement_value
):
    prereg = preregistration(holdout_index=1)
    validation = preregistration_validation_kwargs(prereg)
    authorization = decode_strict_json(
        validation["selection_anchor_authorization_artifact"]
    )
    authorization[field_name] = replacement_value
    rebound_authorization = ArtifactBinding(
        artifact_type="holdout_successor_authorization",
        schema_version=1,
        sha256=artifact_sha256(authorization),
    )
    prior = dict(prereg.prior_evidence)
    prior["successor_authorization"] = rebound_authorization.to_dict()
    rebound = replace(
        prereg,
        selection_anchor_provenance=replace(
            prereg.selection_anchor_provenance,
            authorization_artifact=rebound_authorization,
        ),
        prior_evidence=prior,
    )
    validation["selection_anchor_authorization_artifact"] = (
        canonical_artifact_bytes(authorization)
    )

    with pytest.raises(ExperimentValidationError, match="selection authorization"):
        validate_preregistration(
            canonical_artifact_bytes(rebound.to_dict()),
            expected_contract=rebound.experiment_contract,
            **validation,
        )


def test_calibration_successor_holdout_binds_calibration_retry_ancestry():
    result = holdout_promotion_result(calibration_index=1)
    payload = validate_terminal_result(
        result.to_dict(),
        expected_contract=result.experiment_contract,
        **bound_preregistration_validation_kwargs(result),
    )

    assert payload["attempt"]["calibration_attempt_index"] == 1
    assert [item["artifact_type"] for item in payload["ancestry"][-3:]] == [
        "chainlink_v4_terminal_result",
        "calibration_retry_eligibility",
        "calibration_successor_authorization",
    ]
    prereg = preregistration(calibration_index=1)
    assert [
        artifact.artifact_type
        for artifact in prereg.calibration_artifacts[:4]
    ] == [
        "calibration_retry_eligibility",
        "calibration_candidate_day_ledger",
        "calibration_successor_authorization",
        "calibration_attempt_freeze",
    ]
    changed = deepcopy(result.to_dict())
    changed["parent_result"]["sha256"] = digest("d")
    changed["ancestry"][-3]["sha256"] = digest("d")
    with pytest.raises(
        ExperimentValidationError,
        match="bound preregistration|authoritative lineage",
    ):
        validate_terminal_result(
            changed,
            expected_contract=result.experiment_contract,
            **bound_preregistration_validation_kwargs(result),
        )


def test_calibration_markers_bind_full_identity_before_holdout_index_transition():
    prereg = preregistration(calibration_index=1)
    report_raw, completion_raw = calibration_validation_artifacts(prereg)
    report = decode_strict_json(report_raw)
    completion = decode_strict_json(completion_raw)
    expected_stage_attempt = attempt(
        holdout_index=None,
        calibration_index=1,
        experiment_id=prereg.attempt.experiment_id,
    )

    assert report["attempt_scope"]["experiment_id"] == str(
        expected_stage_attempt.experiment_id
    )
    assert report["attempt_scope"]["holdout_attempt_index"] is None
    assert completion["attempt_scope"] == report["attempt_scope"]
    assert prereg.attempt.holdout_attempt_index == 0

    successor = preregistration(calibration_index=1, holdout_index=1)
    inherited_report = decode_strict_json(
        calibration_validation_artifacts(successor)[0]
    )
    assert inherited_report["attempt_scope"]["experiment_id"] == str(
        HOLDOUT_PARENT_EXPERIMENT_ID
    )
    assert inherited_report["attempt_scope"]["experiment_id"] != str(
        successor.attempt.experiment_id
    )


def test_holdout_successor_preserves_calibration_successor_ancestry():
    result = holdout_promotion_result(
        calibration_index=1,
        holdout_index=1,
    )
    payload = validate_terminal_result(
        result.to_dict(),
        expected_contract=result.experiment_contract,
        **bound_preregistration_validation_kwargs(result),
    )

    assert [artifact["artifact_type"] for artifact in payload["ancestry"]] == [
        "chainlink_v4_terminal_result",
        "calibration_retry_eligibility",
        "calibration_successor_authorization",
        "chainlink_v4_terminal_result",
        "holdout_retry_eligibility",
        "holdout_successor_authorization",
    ]
    changed = deepcopy(result.to_dict())
    changed["ancestry"][0]["sha256"] = digest("c")
    with pytest.raises(
        ExperimentValidationError,
        match="bound preregistration|authoritative lineage",
    ):
        validate_terminal_result(
            changed,
            expected_contract=result.experiment_contract,
            **bound_preregistration_validation_kwargs(result),
        )


def test_calibration_3000_winner_retains_without_freezing_runner_up():
    result = calibration_retain_result()
    payload = validate_terminal_result(
        canonical_artifact_bytes(result.to_dict()),
        expected_contract=result.experiment_contract,
        expected=result,
    )

    assert payload["terminal_stage"] == "calibration"
    assert payload["decision"] == "retain_incumbent"
    assert payload["frozen_challenger"] is None
    assert payload["holdout_attempted"] is False
    assert payload["efficacy_evidence"]["winner_lag_ms"] == 3_000
    assert payload["efficacy_evidence"]["winner_promotion_eligible"] is False


@pytest.mark.parametrize(
    ("candidate_mae", "expected_winner", "expected_unique"),
    [
        (
            {
                "1500": Decimal("0.04"),
                "2000": Decimal("0.05"),
                "2500": Decimal("0.055"),
                "3000": Decimal("0.06"),
                "3500": Decimal("0.07"),
            },
            3_500,
            True,
        ),
        (
            {
                "1500": Decimal("0.04"),
                "2000": Decimal("0.07"),
                "2500": Decimal("0.07"),
                "3000": Decimal("0.06"),
                "3500": Decimal("0.05"),
            },
            2_000,
            False,
        ),
    ],
)
def test_calibration_longer_or_tied_winner_retains_without_runner_up_fallback(
    candidate_mae, expected_winner, expected_unique
):
    result = calibration_retain_result(
        candidate_mae_override=candidate_mae
    )
    payload = validate_terminal_result(
        canonical_artifact_bytes(result.to_dict()),
        expected_contract=result.experiment_contract,
        expected=result,
    )

    assert payload["decision"] == "retain_incumbent"
    assert payload["frozen_challenger"] is None
    assert payload["efficacy_evidence"]["winner_lag_ms"] == expected_winner
    assert payload["efficacy_evidence"]["unique_best"] is expected_unique


@pytest.mark.parametrize(
    "candidate_mae",
    [
        None,
        {
            "1500": Decimal("0.04"),
            "2000": Decimal("0.05"),
            "2500": Decimal("0.055"),
            "3000": Decimal("0.06"),
            "3500": Decimal("0.07"),
        },
        {
            "1500": Decimal("0.04"),
            "2000": Decimal("0.07"),
            "2500": Decimal("0.07"),
            "3000": Decimal("0.06"),
            "3500": Decimal("0.05"),
        },
    ],
)
def test_preregistration_report_rejects_3000_3500_and_tied_winners(
    candidate_mae,
):
    retained = calibration_retain_result(
        candidate_mae_override=candidate_mae
    )
    with pytest.raises(
        ExperimentValidationError, match="qualifying shorter winner"
    ):
        calibration_selection_report_payload(
            attempt=retained.attempt,
            experiment_contract_digest=retained.experiment_contract.digest,
            frozen_challenger=(
                retained.experiment_contract.candidate_identity(2_000)
            ),
            efficacy_ledger=next(
                artifact
                for artifact in retained.evidence_artifacts
                if artifact.artifact_type == "calibration_efficacy_ledger"
            ),
            efficacy_evidence=retained.efficacy_evidence,
        )


def test_holdout_one_failed_gate_retains_incumbent():
    promotion = holdout_promotion_result()
    evidence = deepcopy(promotion.to_dict()["efficacy_evidence"])
    evidence["improvement_bootstrap_lower_bound"] = "0"
    evidence["gate_results"]["improvement_bootstrap_lower_bound"] = False
    evidence["all_gates_passed"] = False
    result = rebind_terminal_efficacy_report(
        promotion,
        decision="retain_incumbent",
        efficacy_evidence=evidence,
    )
    payload = validate_terminal_result(
        canonical_artifact_bytes(result.to_dict()),
        expected_contract=result.experiment_contract,
        **bound_preregistration_validation_kwargs(result),
        expected=result,
    )

    assert payload["decision"] == "retain_incumbent"
    assert payload["efficacy_evidence"]["all_gates_passed"] is False
    assert payload["retry_state"]["lineage_closed"] is True


def test_terminal_efficacy_report_cannot_reuse_a_stale_completion_marker():
    promotion = holdout_promotion_result()
    evidence = deepcopy(promotion.to_dict()["efficacy_evidence"])
    evidence["improvement_bootstrap_lower_bound"] = "0"
    evidence["gate_results"]["improvement_bootstrap_lower_bound"] = False
    evidence["all_gates_passed"] = False
    retained = rebind_terminal_efficacy_report(
        promotion,
        decision="retain_incumbent",
        efficacy_evidence=evidence,
    )
    payload = deepcopy(retained.to_dict())
    payload["holdout_completion_marker"] = (
        promotion.holdout_completion_marker.to_dict()
    )
    for artifact in payload["evidence_artifacts"]:
        if artifact["artifact_type"] == "holdout_efficacy_completed":
            artifact.update(promotion.holdout_completion_marker.to_dict())

    with pytest.raises(ExperimentValidationError, match="completion marker"):
        validate_terminal_result(
            canonical_artifact_bytes(payload),
            expected_contract=retained.experiment_contract,
            **bound_preregistration_validation_kwargs(retained),
        )


def test_holdout_gate_boundaries_are_inclusive_or_strict_as_frozen():
    result = holdout_promotion_result()
    evidence = deepcopy(result.to_dict()["efficacy_evidence"])
    evidence.update(
        challenger_canonical_mae_skill="0.05",
        control_canonical_mae_skill="0.03",
        mae_skill_improvement_vs_control="0.02",
        improvement_bootstrap_lower_bound="0.0001",
        challenger_canonical_rmse_skill="0.01",
        control_canonical_rmse_skill="0.01",
        rmse_skill_improvement_vs_control="0",
    )
    evidence["challenger_mae_skill_by_robustness_cell"] = {
        cell.cell_id: "0.01" for cell in V4_TIMING_CELLS[1:]
    }
    evidence["control_mae_skill_by_robustness_cell"] = {
        cell.cell_id: "0.02" for cell in V4_TIMING_CELLS[1:]
    }
    evidence["mae_skill_improvement_vs_control_by_robustness_cell"] = {
        cell.cell_id: "-0.01" for cell in V4_TIMING_CELLS[1:]
    }

    boundary_result = rebind_terminal_efficacy_report(
        result, efficacy_evidence=evidence
    )

    assert boundary_result.decision == "promotion_eligible"
    assert all(boundary_result.efficacy_evidence["gate_results"].values())


def test_terminal_metrics_cannot_diverge_from_immutable_efficacy_report():
    result = holdout_promotion_result()
    payload = deepcopy(result.to_dict())
    payload["efficacy_evidence"].update(
        challenger_canonical_mae_skill="0.07",
        control_canonical_mae_skill="0.04",
        mae_skill_improvement_vs_control="0.03",
    )

    with pytest.raises(ExperimentValidationError, match="immutable report"):
        validate_terminal_result(
            payload,
            expected_contract=result.experiment_contract,
            **bound_preregistration_validation_kwargs(result),
        )


def test_result_marker_and_stage_states_are_derived_consistently():
    promotion = holdout_promotion_result()

    with pytest.raises(ExperimentValidationError, match="presence"):
        replace(promotion, holdout_completion_marker=None)
    with pytest.raises(ExperimentValidationError, match="holdout-only"):
        replace(
            promotion,
            terminal_stage="calibration",
            attempt=attempt(holdout_index=None),
            holdout_attempted=False,
            holdout_efficacy_started=False,
            holdout_efficacy_completed=False,
            holdout_start_marker=None,
            holdout_completion_marker=None,
            efficacy_attempt_consumed=False,
        )
    with pytest.raises(ExperimentValidationError, match="consumption"):
        replace(promotion, efficacy_attempt_consumed=False)


def test_insufficient_result_rejects_hidden_efficacy_values():
    result = calibration_insufficient_result()

    with pytest.raises(ExperimentValidationError, match="unsupported fields"):
        replace(
            result,
            quality_evidence={"coverage": False, "mae_skill": Decimal("0.1")},
        )
    payload = result.to_dict()
    payload["efficacy_evidence"] = {"mae": "1"}
    with pytest.raises(ExperimentValidationError, match="unsupported fields"):
        validate_terminal_result(
            payload,
            expected_contract=result.experiment_contract,
        )


def test_result_rejects_unknown_tampered_contract_and_second_terminal_id():
    result = calibration_insufficient_result()
    payload = result.to_dict()
    payload["unexpected"] = True
    with pytest.raises(ExperimentValidationError, match="unsupported fields"):
        validate_terminal_result(
            payload,
            expected_contract=result.experiment_contract,
        )

    payload = result.to_dict()
    payload["experiment_contract"]["offline_evaluation_policy"][
        "baseline_pairing"
    ] = "changed"
    with pytest.raises(ExperimentValidationError, match="frozen"):
        validate_terminal_result(
            payload,
            expected_contract=result.experiment_contract,
        )

    with pytest.raises(ExperimentValidationError, match="already has"):
        validate_terminal_result(
            result.to_dict(),
            expected_contract=result.experiment_contract,
            existing_terminal_experiment_ids=(EXPERIMENT_ID,),
        )


def test_artifact_bindings_require_type_schema_and_hash_not_a_name_only():
    frozen = contract()
    candidate = frozen.candidate_identity(3_000)
    control = frozen.control_identity

    assert candidate.model_version == control.model_version
    assert candidate != control
    assert candidate.forecast_config_digest == control.forecast_config_digest

    mismatched = contract(
        active=incumbent(
            config=replace(
                settings().config_for_lag(3_000),
                reference_max_gap_ms=3_000,
            )
        )
    )
    assert mismatched.candidate_identity(3_000).model_version == (
        mismatched.control_identity.model_version
    )
    assert mismatched.candidate_identity(3_000) != mismatched.control_identity
    with pytest.raises(ExperimentValidationError):
        ModelIdentity(
            model_role="offline_replay_replacement_control",
            model_version="catchup_ratio_l3000_b100",
            forecast_config_digest="name_only",
            offline_evaluation_policy_digest=digest("a"),
        )


def test_attempt_identity_requires_unique_canonical_uuid4_values():
    with pytest.raises(ExperimentValidationError, match="distinct"):
        AttemptIdentity(LINEAGE_ID, LINEAGE_ID, 0, None)
    with pytest.raises(ExperimentValidationError, match="UUID4"):
        AttemptIdentity(
            UUID("11111111-1111-1111-8111-111111111111"),
            EXPERIMENT_ID,
            0,
            None,
        )
    with pytest.raises(ExperimentValidationError, match="zero or one"):
        AttemptIdentity(LINEAGE_ID, EXPERIMENT_ID, 2, None)


def test_frozen_policy_constants_are_json_decimal_strings_without_floats():
    payload = json.loads(canonical_json_bytes(contract().to_dict()))

    assert payload["comparison_family"][0]["forecast_config"]["beta"] == "1"
    assert json.loads(canonical_json_bytes(FROZEN_HOLDOUT_GATES))[
        "challenger_canonical_mae_skill_minimum"
    ] == "0.05"
    assert json.loads(canonical_json_bytes(FROZEN_QUALITY_GATES))[
        "cohort_classification_percent"
    ] == "1"
    assert FROZEN_BOOTSTRAP_CONTRACT["replicates"] == 10_000


def test_forecast_config_strict_decode_rejects_unknown_or_non_string_decimal():
    config = settings().config_for_lag(3_000)
    payload = json.loads(canonical_json_bytes(config.to_dict()))
    payload["unknown"] = True
    with pytest.raises(ExperimentValidationError, match="unsupported fields"):
        ForecastConfig.from_dict(payload, "config")

    payload = json.loads(canonical_json_bytes(config.to_dict()))
    payload["beta"] = 1
    with pytest.raises(ExperimentValidationError, match="decimal string"):
        ForecastConfig.from_dict(payload, "config")


@pytest.mark.parametrize(
    "overrides",
    [
        {"beta": 1},
        {"beta": 1.0},
        {"reference_max_gap_ms": True},
        {"max_future_skew_ms": False},
    ],
)
def test_v4_settings_reject_numeric_type_aliases(overrides):
    with pytest.raises(ExperimentValidationError):
        replace(settings(), **overrides)


def test_forecast_code_manifest_has_strict_self_describing_envelope():
    manifest = code_manifest()
    artifact = manifest.to_artifact_dict("active_forecast_code_manifest")

    assert manifest.digest == canonical_sha256(manifest.component_payload())
    assert manifest.digest != canonical_sha256(manifest.to_dict())
    assert ForecastCodeManifest.from_artifact_dict(
        artifact,
        "forecast_code_manifest",
        expected_artifact_type="active_forecast_code_manifest",
    ) == manifest

    mutations = []
    for key, value in (
        ("artifact_type", "v4_forecast_code_manifest"),
        ("schema_version", 2),
        ("schema_version", True),
        ("component_digest_scheme", "changed"),
        ("forecast_code_digest_scheme", "changed"),
        ("forecast_code_digest", digest("f")),
    ):
        changed = deepcopy(artifact)
        changed[key] = value
        mutations.append(changed)
    changed_component = deepcopy(artifact)
    changed_component["components"]["projection_sha256"] = digest("f")
    mutations.append(changed_component)
    extra = deepcopy(artifact)
    extra["unexpected"] = True
    mutations.append(extra)

    for changed in mutations:
        with pytest.raises(ExperimentValidationError):
            ForecastCodeManifest.from_artifact_dict(
                changed,
                "forecast_code_manifest",
                expected_artifact_type="active_forecast_code_manifest",
            )


@pytest.mark.parametrize(
    "changed_component",
    [
        "anchor_formation",
        "futures_reference_selection",
        "projection",
        "forecast_validity",
    ],
)
def test_forecast_code_manifest_component_bytes_change_identity(
    changed_component,
):
    component_bytes = {
        "anchor_formation": b"anchor",
        "futures_reference_selection": b"reference",
        "projection": b"projection",
        "forecast_validity": b"validity",
    }
    first = ForecastCodeManifest.from_component_bytes(
        **component_bytes,
    )
    component_bytes[changed_component] += b"-changed"
    second = ForecastCodeManifest.from_component_bytes(
        **component_bytes,
    )

    assert first.digest != second.digest
    digest_field = f"{changed_component}_sha256"
    assert getattr(first, digest_field) != getattr(second, digest_field)


def test_active_incumbent_bindings_require_schema_one():
    active = incumbent()

    with pytest.raises(IncumbentProvenanceError, match="artifact identity"):
        replace(
            active,
            invocation_start=replace(
                active.invocation_start, schema_version=999
            ),
        )

    frozen = contract()
    with pytest.raises(ExperimentValidationError, match="manifest"):
        replace(
            frozen,
            v4_forecast_code_manifest_artifact=replace(
                frozen.v4_forecast_code_manifest_artifact,
                sha256=digest("f"),
            ),
        )


def test_active_incumbent_freeze_binds_every_provenance_role_and_hash():
    active = incumbent()
    mutations = (
        {
            "selection_artifact": replace(
                active.selection_artifact, sha256=digest("f")
            )
        },
        {
            "replay_config_artifact": replace(
                active.replay_config_artifact, sha256=digest("f")
            )
        },
        {
            "forecast_code_manifest_artifact": replace(
                active.forecast_code_manifest_artifact,
                sha256=digest("f"),
            )
        },
        {
            "invocation_start": replace(
                active.invocation_start,
                artifact_type="wrong_invocation_record",
            )
        },
        {
            "reconstruction_report": replace(
                active.reconstruction_report,
                artifact_type="wrong_reconstruction_report",
            )
        },
    )

    for mutation in mutations:
        with pytest.raises(IncumbentProvenanceError):
            replace(active, **mutation)


def test_preregistration_is_idempotent_and_owns_frozen_input_state():
    prereg = preregistration()
    rebuilt = replace(prereg)
    before = canonical_artifact_bytes(prereg.to_dict())

    assert rebuilt == prereg
    assert canonical_artifact_bytes(rebuilt.to_dict()) == before
    external = origin_contract()
    independent = replace(prereg, origin_contract=external)
    artifact_before = canonical_artifact_bytes(independent.to_dict())
    external["observed_mask_schemas"].append("tampered")
    assert canonical_artifact_bytes(independent.to_dict()) == artifact_before
    assert "tampered" not in independent.origin_contract[
        "observed_mask_schemas"
    ]


@pytest.mark.parametrize("bad_value", [None, 1, "not-an-array"])
def test_preregistration_observed_mask_schema_type_fails_closed(bad_value):
    prereg = preregistration()
    payload = deepcopy(prereg.to_dict())
    payload["origin_contract"]["observed_mask_schemas"] = bad_value

    with pytest.raises(ExperimentValidationError, match="array"):
        validate_preregistration(
            payload,
            expected_contract=prereg.experiment_contract,
            **preregistration_validation_kwargs(prereg),
        )


def test_preregistration_anchor_is_read_from_bound_source_artifact():
    prereg = preregistration()
    payload = deepcopy(prereg.to_dict())
    payload["selection_anchor_ms"] -= 1
    payload["selection_anchor_provenance"]["timestamp_ms"] -= 1

    with pytest.raises(ExperimentValidationError, match="hashed source"):
        validate_preregistration(
            payload,
            expected_contract=prereg.experiment_contract,
            **preregistration_validation_kwargs(prereg),
        )


def test_preregistration_rejects_boolean_bound_artifact_schema_version():
    prereg = preregistration()
    source, _ = anchor_validation_artifacts(prereg)
    calibration_report, calibration_completion = (
        calibration_validation_artifacts(prereg)
    )
    authorization_payload = anchor_authorization_payload(prereg)
    authorization_payload["schema_version"] = True
    authorization = canonical_artifact_bytes(authorization_payload)
    authorization_binding = ArtifactBinding(
        artifact_type="holdout_selection_authorization",
        schema_version=1,
        sha256=hashlib.sha256(authorization).hexdigest(),
    )
    selection_anchor = replace(
        prereg.selection_anchor_provenance,
        authorization_artifact=authorization_binding,
    )
    calibration = tuple(
        authorization_binding
        if item.artifact_type == "holdout_selection_authorization"
        else item
        for item in prereg.calibration_artifacts
    )
    rebound = replace(
        prereg,
        selection_anchor_provenance=selection_anchor,
        calibration_artifacts=calibration,
    )

    with pytest.raises(ExperimentValidationError, match="integer"):
        validate_preregistration(
            rebound.to_dict(),
            expected_contract=rebound.experiment_contract,
            selection_anchor_source_artifact=source,
            selection_anchor_authorization_artifact=authorization,
            calibration_efficacy_report_artifact=calibration_report,
            calibration_completion_marker_artifact=calibration_completion,
        )


def test_preregistration_rejects_noncanonical_raw_bytes():
    prereg = preregistration()
    compact_without_lf = canonical_json_bytes(prereg.to_dict())
    pretty = json.dumps(prereg.to_dict(), default=str, indent=2).encode("utf-8")

    for raw in (compact_without_lf, pretty):
        with pytest.raises(ExperimentValidationError, match="canonical JSON"):
            validate_preregistration(
                raw,
                expected_contract=prereg.experiment_contract,
                **preregistration_validation_kwargs(prereg),
            )


def test_terminal_result_rejects_unknown_failure_stage_and_reason():
    result = calibration_insufficient_result()

    with pytest.raises(ExperimentValidationError, match="failure_stage"):
        replace(result, failure_stage="banana")
    with pytest.raises(ExperimentValidationError, match="failure_reasons"):
        replace(result, failure_reasons=("price_went_down",))


def test_terminal_result_binds_immediately_preceding_provenance_root():
    result = calibration_retain_result()

    with pytest.raises(ExperimentValidationError, match="continuity root"):
        replace(result, provenance_continuity_root=digest("f"))


def test_terminal_markers_require_exact_type_and_schema():
    result = holdout_promotion_result()

    with pytest.raises(ExperimentValidationError, match="artifact identity"):
        replace(
            result,
            holdout_start_marker=binding("wrong_marker", "f"),
        )
    with pytest.raises(ExperimentValidationError, match="artifact identity"):
        replace(
            result,
            holdout_start_marker=replace(
                result.holdout_start_marker, schema_version=2
            ),
        )


def test_promotion_is_derived_from_every_holdout_gate():
    result = holdout_promotion_result()
    evidence = deepcopy(result.to_dict()["efficacy_evidence"])
    evidence["improvement_bootstrap_lower_bound"] = "0"
    evidence["gate_results"]["improvement_bootstrap_lower_bound"] = False
    evidence["all_gates_passed"] = False

    with pytest.raises(ExperimentValidationError, match="decision"):
        replace(result, efficacy_evidence=evidence)

    inconsistent = deepcopy(result.to_dict()["efficacy_evidence"])
    inconsistent["gate_results"]["challenger_canonical_mae_skill"] = False
    with pytest.raises(ExperimentValidationError, match="not derived"):
        replace(result, efficacy_evidence=inconsistent)

    wrong_seed = deepcopy(result.to_dict()["efficacy_evidence"])
    wrong_seed["bootstrap_seed_sha256"] = digest("f")
    wrong_seed["bootstrap_seed_int"] = str(int(digest("f"), 16))
    with pytest.raises(ExperimentValidationError, match="preregistration"):
        replace(result, efficacy_evidence=wrong_seed)


def test_performance_result_closes_retry_budget_and_requires_calibration():
    result = holdout_promotion_result()
    retry_state = dict(result.retry_state)
    retry_state["holdout_successors_remaining"] = 1

    with pytest.raises(ExperimentValidationError, match="retry_state"):
        replace(result, retry_state=retry_state)
    with pytest.raises(ExperimentValidationError, match="completed calibration"):
        replace(
            result,
            calibration_efficacy_started=False,
            calibration_efficacy_completed=False,
            calibration_start_marker=None,
            calibration_completion_marker=None,
        )


def test_terminal_result_rejects_hidden_or_unbound_evidence():
    result = calibration_insufficient_result()
    quality = deepcopy(result.to_dict()["quality_evidence"])
    quality["performance_estimate"] = "0.9"
    with pytest.raises(ExperimentValidationError, match="unsupported fields"):
        replace(result, quality_evidence=quality)

    with pytest.raises(ExperimentValidationError, match="unsupported artifact"):
        replace(
            result,
            evidence_artifacts=(
                *result.evidence_artifacts,
                binding("arbitrary_evidence", "f"),
            ),
        )
    with pytest.raises(ExperimentValidationError, match="ledger binding"):
        replace(result, candidate_day_ledger_root=digest("f"))


def test_terminal_result_rejects_noncanonical_raw_bytes():
    result = holdout_promotion_result()
    compact_without_lf = canonical_json_bytes(result.to_dict())

    with pytest.raises(ExperimentValidationError, match="canonical JSON"):
        validate_terminal_result(
            compact_without_lf,
            expected_contract=result.experiment_contract,
        )


def test_preregistration_requires_complete_schema_one_calibration_chain():
    prereg = preregistration()
    artifacts = list(prereg.calibration_artifacts)
    gate_index = next(
        index
        for index, artifact in enumerate(artifacts)
        if artifact.artifact_type
        == "calibration_pre_efficacy_provenance_gate"
    )

    with pytest.raises(ExperimentValidationError, match="complete frozen order"):
        replace(
            prereg,
            calibration_artifacts=tuple(
                artifact
                for index, artifact in enumerate(artifacts)
                if index != gate_index
            ),
        )
    artifacts[0] = replace(artifacts[0], schema_version=999)
    with pytest.raises(ExperimentValidationError, match="schema version"):
        replace(prereg, calibration_artifacts=tuple(artifacts))

    artifacts = list(prereg.calibration_artifacts)
    completion_index = next(
        index
        for index, artifact in enumerate(artifacts)
        if artifact.artifact_type == "calibration_efficacy_completed"
    )
    ledger_index = next(
        index
        for index, artifact in enumerate(artifacts)
        if artifact.artifact_type == "calibration_efficacy_ledger"
    )
    artifacts[completion_index], artifacts[ledger_index] = (
        artifacts[ledger_index],
        artifacts[completion_index],
    )
    with pytest.raises(ExperimentValidationError, match="complete frozen order"):
        replace(prereg, calibration_artifacts=tuple(artifacts))


def test_quality_decision_is_derived_from_counts_and_bound_report():
    result = holdout_promotion_result()
    quality = deepcopy(result.to_dict()["quality_evidence"])
    quality["cells"][0]["common_scored_count"] = 1
    quality["cells"][0]["decision_eligible_count"] = 1

    with pytest.raises(ExperimentValidationError, match="not derived"):
        replace(result, quality_evidence=quality)

    quality = deepcopy(result.to_dict()["quality_evidence"])
    quality["cells"][0]["quality_report_binding"]["sha256"] = digest("f")
    with pytest.raises(ExperimentValidationError, match="inventory differs"):
        replace(result, quality_evidence=quality)


@pytest.mark.parametrize(
    "field_name", ["cohort_classified_count", "causal_violation_count"]
)
def test_quality_counts_cannot_exceed_target_eligible_domain(field_name):
    result = calibration_insufficient_result()
    quality = deepcopy(result.to_dict()["quality_evidence"])
    quality["cells"][0][field_name] = 172_794

    with pytest.raises(ExperimentValidationError, match="inconsistent"):
        replace(result, quality_evidence=quality)


def test_successor_terminal_result_requires_exact_parent_ancestry():
    result = holdout_promotion_result()
    retry_state = dict(result.retry_state)
    retry_state["holdout_successors_used"] = 1
    retry_anchor = SelectionAnchorProvenance(
        mode="retry_eligibility",
        source_artifact=binding("holdout_retry_eligibility", "a"),
        timestamp_field="created_at_ms",
        timestamp_ms=result.selection_anchor_provenance.timestamp_ms,
        authorization_artifact=binding(
            "holdout_successor_authorization", "b"
        ),
    )

    with pytest.raises(ExperimentValidationError, match="parent_result"):
        replace(
            result,
            attempt=attempt(holdout_index=1),
            retry_state=retry_state,
            selection_anchor_provenance=retry_anchor,
        )


def test_terminal_result_marker_matrix_rejects_started_quality_failure():
    result = calibration_insufficient_result()
    marker = binding("calibration_efficacy_started", "f")

    with pytest.raises(ExperimentValidationError, match="marker state"):
        replace(
            result,
            calibration_efficacy_started=True,
            calibration_start_marker=marker,
            evidence_artifacts=(*result.evidence_artifacts, marker),
        )


def test_provenance_failures_close_retry_and_use_stage_correct_checkpoint():
    base = calibration_retain_result()
    post_start = replace(
        base,
        decision="insufficient_evidence",
        failure_stage="calibration_post_start_provenance",
        failure_reasons=("relevant_provenance_transition",),
        efficacy_evidence=None,
    )

    assert post_start.provenance_checkpoint.artifact_type == (
        "final_analysis_checkpoint"
    )
    stage_checkpoint = binding("stage_terminal_checkpoint", "9")
    with pytest.raises(ExperimentValidationError, match="artifact identity"):
        replace(
            post_start,
            provenance_checkpoint=stage_checkpoint,
            evidence_artifacts=tuple(
                stage_checkpoint
                if item == post_start.provenance_checkpoint
                else item
                for item in post_start.evidence_artifacts
            ),
        )

    pre_checkpoint = ArtifactBinding(
        artifact_type="stage_terminal_checkpoint",
        schema_version=1,
        sha256=text_digest("pre_efficacy_stage_terminal_checkpoint"),
    )
    pre_efficacy_inventory = tuple(
        item
        for item in base.evidence_artifacts
        if item.artifact_type
        not in {
            "calibration_efficacy_started",
            "calibration_efficacy_ledger",
            "calibration_efficacy_report",
            "calibration_efficacy_completed",
            "final_analysis_checkpoint",
        }
    ) + (pre_checkpoint,)
    pre_efficacy = replace(
        base,
        decision="insufficient_evidence",
        failure_stage="calibration_pre_efficacy_provenance",
        failure_reasons=("provenance_failure",),
        calibration_efficacy_started=False,
        calibration_efficacy_completed=False,
        calibration_start_marker=None,
        calibration_completion_marker=None,
        terminal_efficacy_completed_at_ms=None,
        efficacy_evidence=None,
        evidence_artifacts=pre_efficacy_inventory,
        provenance_checkpoint=pre_checkpoint,
        provenance_continuity_root=pre_checkpoint.sha256,
    )
    retry_state = dict(pre_efficacy.retry_state)
    retry_state.update(
        calibration_successors_remaining=1,
        successor_allowed=True,
        lineage_closed=False,
    )
    with pytest.raises(ExperimentValidationError, match="retry_state"):
        replace(pre_efficacy, retry_state=retry_state)


def test_preregistration_publication_miss_is_representable_without_artifact():
    promotion = holdout_promotion_result()
    deadline_check_payload = preregistration_deadline_check_payload(
        attempt=promotion.attempt,
        preregistration_publication_deadline_ms=(
            promotion.preregistration_publication_deadline_ms
        ),
        checked_at_ms=(
            promotion.preregistration_publication_deadline_ms + 1
        ),
    )
    deadline_check = ArtifactBinding(
        artifact_type="holdout_preregistration_deadline_check",
        schema_version=1,
        sha256=artifact_sha256(deadline_check_payload),
    )
    checkpoint = binding("stage_terminal_checkpoint", "b")
    anchor = promotion.selection_anchor_provenance
    candidate_ledger = next(
        item
        for item in promotion.evidence_artifacts
        if item.artifact_type == "holdout_candidate_day_ledger"
    )
    result = replace(
        promotion,
        decision="insufficient_evidence",
        failure_stage="preregistration_lead",
        failure_reasons=("preregistration_deadline_missed",),
        retry_state={
            "calibration_successors_used": 0,
            "holdout_successors_used": 0,
            "calibration_successors_remaining": 0,
            "holdout_successors_remaining": 1,
            "successor_allowed": True,
            "retries_exhausted": False,
            "lineage_closed": False,
        },
        preregistration_binding=None,
        receipt_deadline_check=deadline_check,
        pushed_receipt=None,
        holdout_efficacy_started=False,
        holdout_efficacy_completed=False,
        holdout_start_marker=None,
        holdout_completion_marker=None,
        terminal_efficacy_completed_at_ms=None,
        efficacy_attempt_consumed=False,
        evidence_artifacts=(
            promotion.calibration_start_marker,
            promotion.calibration_completion_marker,
            anchor.authorization_artifact,
            candidate_ledger,
            deadline_check,
            checkpoint,
        ),
        provenance_checkpoint=checkpoint,
        provenance_continuity_root=checkpoint.sha256,
        quality_evidence={"status": "not_reached"},
        efficacy_evidence=None,
    )

    prereg = preregistration(
        experiment_contract=result.experiment_contract,
        experiment_id=result.attempt.experiment_id,
        calibration_index=result.attempt.calibration_attempt_index,
        holdout_index=result.attempt.holdout_attempt_index,
    )
    anchor_validation = preregistration_validation_kwargs(prereg)
    terminal_anchor_validation = {
        "selection_anchor_source_artifact": anchor_validation[
            "selection_anchor_source_artifact"
        ],
        "selection_anchor_authorization_artifact": anchor_validation[
            "selection_anchor_authorization_artifact"
        ],
        "calibration_efficacy_report_artifact": anchor_validation[
            "calibration_efficacy_report_artifact"
        ],
        "expected_calibration_efficacy_report": anchor_validation[
            "expected_calibration_efficacy_report"
        ],
        "expected_calibration_completion_marker": anchor_validation[
            "expected_calibration_completion_marker"
        ],
        "receipt_deadline_check_artifact": canonical_artifact_bytes(
            deadline_check_payload
        ),
    }
    payload = validate_terminal_result(
        result.to_dict(),
        expected_contract=result.experiment_contract,
        **terminal_anchor_validation,
    )
    assert payload["preregistration_binding"] is None
    assert payload["retry_state"]["successor_allowed"] is True

    tampered_anchor_source = decode_strict_json(
        anchor_validation["selection_anchor_source_artifact"]
    )
    tampered_anchor_source["completed_at_ms"] += 1
    terminal_anchor_validation["selection_anchor_source_artifact"] = (
        canonical_artifact_bytes(tampered_anchor_source)
    )
    with pytest.raises(
        ExperimentValidationError,
        match="bytes differ from the bound SHA-256",
    ):
        validate_terminal_result(
            result.to_dict(),
            expected_contract=result.experiment_contract,
            **terminal_anchor_validation,
        )

    with pytest.raises(ExperimentValidationError, match="preregistration"):
        replace(
            result,
            failure_reasons=("pushed_receipt_missing_or_late",),
        )


def test_missing_pushed_receipt_is_loss_free_and_strictly_validated():
    promotion = holdout_promotion_result()
    prereg = preregistration(experiment_contract=promotion.experiment_contract)
    _, missing_receipt_check_raw = receipt_validation_artifacts(
        prereg, receipt_present=False
    )
    missing_receipt_check = ArtifactBinding(
        artifact_type="holdout_receipt_deadline_check",
        schema_version=1,
        sha256=hashlib.sha256(missing_receipt_check_raw).hexdigest(),
    )
    checkpoint = ArtifactBinding(
        artifact_type="stage_terminal_checkpoint",
        schema_version=1,
        sha256=text_digest("missing_receipt_terminal_checkpoint"),
    )
    keep_types = {
        "calibration_efficacy_started",
        "calibration_efficacy_completed",
        "holdout_selection_authorization",
        "holdout_candidate_day_ledger",
        "chainlink_v4_holdout_preregistration",
        "holdout_receipt_deadline_check",
    }
    result = replace(
        promotion,
        decision="insufficient_evidence",
        failure_stage="preregistration_lead",
        failure_reasons=("pushed_receipt_missing_or_late",),
        retry_state={
            "calibration_successors_used": 0,
            "holdout_successors_used": 0,
            "calibration_successors_remaining": 0,
            "holdout_successors_remaining": 1,
            "successor_allowed": True,
            "retries_exhausted": False,
            "lineage_closed": False,
        },
        pushed_receipt=None,
        receipt_deadline_check=missing_receipt_check,
        holdout_efficacy_started=False,
        holdout_efficacy_completed=False,
        holdout_start_marker=None,
        holdout_completion_marker=None,
        terminal_efficacy_completed_at_ms=None,
        efficacy_attempt_consumed=False,
        evidence_artifacts=tuple(
            (
                missing_receipt_check
                if item.artifact_type == "holdout_receipt_deadline_check"
                else item
            )
            for item in promotion.evidence_artifacts
            if item.artifact_type in keep_types
        )
        + (checkpoint,),
        provenance_checkpoint=checkpoint,
        provenance_continuity_root=checkpoint.sha256,
        quality_evidence={"status": "not_reached"},
        efficacy_evidence=None,
    )

    payload = validate_terminal_result(
        result.to_dict(),
        expected_contract=result.experiment_contract,
        **bound_preregistration_validation_kwargs(result),
    )
    assert payload["pushed_receipt"] is None
    assert payload["efficacy_attempt_consumed"] is False
    assert payload["retry_state"]["successor_allowed"] is True


def test_holdout_quality_failure_is_loss_free_and_strictly_validated():
    promotion = holdout_promotion_result()
    prereg = preregistration(
        experiment_contract=promotion.experiment_contract
    )
    first_cell = V4_TIMING_CELLS[0]
    first_report = next(
        item
        for item in promotion.evidence_artifacts
        if item.artifact_type
        == f"holdout_quality_report:{first_cell.cell_id}"
    )
    checkpoint = ArtifactBinding(
        artifact_type="stage_terminal_checkpoint",
        schema_version=1,
        sha256=text_digest("holdout_quality_terminal_checkpoint"),
    )
    keep_types = {
        "calibration_efficacy_started",
        "calibration_efficacy_completed",
        "holdout_selection_authorization",
        "holdout_candidate_day_ledger",
        "chainlink_v4_holdout_preregistration",
        "holdout_pushed_preregistration_receipt",
        "holdout_receipt_deadline_check",
        "holdout_archive_checkpoint_manifest",
        "holdout_raw_manifest",
        first_report.artifact_type,
    }
    result = replace(
        promotion,
        decision="insufficient_evidence",
        failure_stage="holdout_quality",
        failure_reasons=("quality_gate_failure",),
        retry_state={
            "calibration_successors_used": 0,
            "holdout_successors_used": 0,
            "calibration_successors_remaining": 0,
            "holdout_successors_remaining": 1,
            "successor_allowed": True,
            "retries_exhausted": False,
            "lineage_closed": False,
        },
        holdout_efficacy_started=False,
        holdout_efficacy_completed=False,
        holdout_start_marker=None,
        holdout_completion_marker=None,
        terminal_efficacy_completed_at_ms=None,
        efficacy_attempt_consumed=False,
        evidence_artifacts=tuple(
            item
            for item in promotion.evidence_artifacts
            if item.artifact_type in keep_types
        )
        + (checkpoint,),
        provenance_checkpoint=checkpoint,
        provenance_continuity_root=checkpoint.sha256,
        quality_evidence={
            "status": "failed",
            "stage": "holdout",
            "cells": [
                quality_cell(
                    "holdout",
                    first_cell,
                    first_report,
                    passed=False,
                    origin=prereg.origin_contract,
                )
            ],
            "archive_health_passed": True,
            "provenance_passed": True,
            "structural_gate_infeasibility_report_binding": None,
            "failure_codes": [
                "common_scored_coverage_below_minimum",
                "decision_eligible_coverage_below_minimum",
                "quality_stage_incomplete",
            ],
            "all_quality_gates_passed": False,
        },
        efficacy_evidence=None,
    )

    payload = validate_terminal_result(
        result.to_dict(),
        expected_contract=result.experiment_contract,
        **bound_preregistration_validation_kwargs(result),
    )
    assert payload["quality_evidence"]["status"] == "failed"
    assert payload["holdout_efficacy_started"] is False


def test_post_start_holdout_crash_consumes_attempt_and_closes_lineage():
    promotion = holdout_promotion_result()
    checkpoint = ArtifactBinding(
        artifact_type="stage_terminal_checkpoint",
        schema_version=1,
        sha256=text_digest("holdout_execution_terminal_checkpoint"),
    )
    omitted_types = {
        "holdout_efficacy_ledger",
        "holdout_bootstrap_report",
        "holdout_efficacy_report",
        "holdout_efficacy_completed",
        "final_analysis_checkpoint",
    }
    result = replace(
        promotion,
        decision="insufficient_evidence",
        failure_stage="holdout_efficacy_execution",
        failure_reasons=("efficacy_execution_failure",),
        holdout_efficacy_completed=False,
        holdout_completion_marker=None,
        terminal_efficacy_completed_at_ms=None,
        evidence_artifacts=tuple(
            item
            for item in promotion.evidence_artifacts
            if item.artifact_type not in omitted_types
        )
        + (checkpoint,),
        provenance_checkpoint=checkpoint,
        provenance_continuity_root=checkpoint.sha256,
        efficacy_evidence=None,
    )

    payload = validate_terminal_result(
        result.to_dict(),
        expected_contract=result.experiment_contract,
        **bound_preregistration_validation_kwargs(result),
    )
    assert payload["efficacy_attempt_consumed"] is True
    assert payload["retry_state"]["lineage_closed"] is True


def test_attempt_freeze_and_post_freeze_results_require_freeze_evidence():
    quality_failure = calibration_insufficient_result()
    without_freeze = tuple(
        item
        for item in quality_failure.evidence_artifacts
        if item.artifact_type != "calibration_attempt_freeze"
    )
    with pytest.raises(ExperimentValidationError, match="attempt freeze"):
        replace(quality_failure, evidence_artifacts=without_freeze)

    deadline_check = binding(
        "calibration_attempt_freeze_deadline_check", "a"
    )
    checkpoint = quality_failure.provenance_checkpoint
    candidate_ledger = quality_failure.evidence_artifacts[0]
    freeze_failure = replace(
        quality_failure,
        failure_stage="calibration_attempt_freeze",
        failure_reasons=("attempt_freeze_deadline_missed",),
        evidence_artifacts=(candidate_ledger, deadline_check, checkpoint),
        quality_evidence={"status": "not_reached"},
    )
    assert freeze_failure.failure_stage == "calibration_attempt_freeze"

    with pytest.raises(ExperimentValidationError, match="deadline check"):
        replace(
            freeze_failure,
            evidence_artifacts=(candidate_ledger, checkpoint),
        )


@pytest.mark.parametrize("terminal_stage", ["calibration", "holdout"])
def test_completed_stage_requires_its_immutable_efficacy_artifacts(
    terminal_stage,
):
    if terminal_stage == "calibration":
        performance = calibration_retain_result()
        failure_stage = "calibration_efficacy_artifact_integrity"
        omitted_type = "calibration_efficacy_report"
    else:
        performance = holdout_promotion_result()
        failure_stage = "holdout_efficacy_artifact_integrity"
        omitted_type = "holdout_bootstrap_report"
    checkpoint = ArtifactBinding(
        artifact_type="stage_terminal_checkpoint",
        schema_version=1,
        sha256=text_digest(
            f"{terminal_stage}_artifact_integrity_checkpoint"
        ),
    )
    integrity_failure = replace(
        performance,
        decision="insufficient_evidence",
        failure_stage=failure_stage,
        failure_reasons=("efficacy_artifact_integrity_failure",),
        evidence_artifacts=tuple(
            checkpoint
            if item == performance.provenance_checkpoint
            else item
            for item in performance.evidence_artifacts
        ),
        provenance_checkpoint=checkpoint,
        provenance_continuity_root=checkpoint.sha256,
        efficacy_evidence=None,
    )
    without_artifact = tuple(
        item
        for item in integrity_failure.evidence_artifacts
        if item.artifact_type != omitted_type
    )

    with pytest.raises(ExperimentValidationError, match="immutable efficacy"):
        replace(integrity_failure, evidence_artifacts=without_artifact)


def test_structural_quality_claim_requires_independent_feasibility_proof():
    ordinary_failure = calibration_insufficient_result()
    closed_retry_state = dict(ordinary_failure.retry_state)
    closed_retry_state.update(
        calibration_successors_remaining=0,
        successor_allowed=False,
        lineage_closed=True,
    )
    with pytest.raises(ExperimentValidationError, match="derived"):
        replace(
            ordinary_failure,
            failure_reasons=("structural_gate_infeasibility",),
            retry_state=closed_retry_state,
        )

    performance = calibration_retain_result()
    quality_reports = tuple(
        artifact
        for artifact in performance.evidence_artifacts
        if artifact.artifact_type.startswith("calibration_quality_report:")
    )
    claimed_structural = {
        "status": "failed",
        "stage": "calibration",
        "cells": [
            quality_cell("calibration", cell, report, passed=False)
            for cell, report in zip(V4_TIMING_CELLS, quality_reports)
        ],
        "archive_health_passed": True,
        "provenance_passed": True,
        "structural_gate_infeasibility_report_binding": binding(
            "calibration_structural_gate_infeasibility_report", "e"
        ).to_dict(),
        "failure_codes": [
            "common_scored_coverage_below_minimum",
            "decision_eligible_coverage_below_minimum",
            "structural_gate_infeasibility",
        ],
        "all_quality_gates_passed": False,
    }
    with pytest.raises(
        ExperimentValidationError, match="independently derived feasibility"
    ):
        replace(ordinary_failure, quality_evidence=claimed_structural)


def test_pre_efficacy_result_rejects_later_or_reordered_artifacts():
    result = calibration_insufficient_result()
    later_artifact = binding("calibration_efficacy_ledger", "a")
    with pytest.raises(ExperimentValidationError, match="stage-unavailable"):
        replace(
            result,
            evidence_artifacts=(
                *result.evidence_artifacts[:-1],
                later_artifact,
                result.evidence_artifacts[-1],
            ),
        )

    reordered = list(result.evidence_artifacts)
    reordered[2], reordered[3] = reordered[3], reordered[2]
    with pytest.raises(ExperimentValidationError, match="canonical stage order"):
        replace(result, evidence_artifacts=tuple(reordered))


def test_terminal_metrics_require_canonical_decimal_strings():
    result = holdout_promotion_result()
    payload = deepcopy(result.to_dict())
    payload["efficacy_evidence"][
        "challenger_canonical_mae_skill"
    ] = "0.0600"

    with pytest.raises(
        ExperimentValidationError, match="canonical fixed-point Decimal"
    ):
        validate_terminal_result(
            payload,
            expected_contract=result.experiment_contract,
            **bound_preregistration_validation_kwargs(result),
        )
