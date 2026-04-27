"""Batch-level upload-behavior injection for edge-side dedup experiments."""

from __future__ import annotations

from dataclasses import dataclass
import math
import random
from typing import Sequence

from .edge_batches import EdgeBatch, clone_edge_batches, make_edge_batch

SCENARIO_CHOICES = (
    "new_append",
    "exact_replay",
    "partial_overlap",
    "mixed_upload",
    "random_record_replay",
)

PAPER_SCENARIO_CHOICES = (
    "new_append",
    "exact_replay",
    "partial_overlap",
    "mixed_upload",
)

SCENARIO_DESCRIPTIONS = {
    "new_append": "Fresh non-overlapping uploads only; dupe-rate is ignored and realized duplicate rate is zero.",
    "exact_replay": "A fraction of eligible batches are replayed later as identical uploads.",
    "partial_overlap": (
        "Each eligible later batch prepends a duplicate prefix of size floor(dupe_rate * original_batch_records), "
        "capped by available committed history."
    ),
    "mixed_upload": (
        "Mixed upload behavior: even later batches become partial overlaps with overlap size floor(dupe_rate * batch_records), "
        "while a fraction of odd batches are replayed exactly later."
    ),
    "random_record_replay": "Legacy non-paper variant that appends random record replays as extra batches.",
}


@dataclass(frozen=True)
class InjectionResult:
    batches: list[EdgeBatch]
    scenario: str
    requested_dupe_rate: float
    realized_dupe_rate: float
    injected_duplicate_records: int
    max_injectable_duplicates: int
    scenario_description: str
    replay_batches_added: int = 0
    overlap_batches_modified: int = 0


def _validate_dupe_rate(dupe_rate: float) -> None:
    if not 0.0 <= dupe_rate <= 1.0:
        raise ValueError("dupe_rate must be within [0.0, 1.0]")


def _group_batch_indices_by_scope(batches: Sequence[EdgeBatch]) -> dict[tuple[str, str, str], list[int]]:
    grouped: dict[tuple[str, str, str], list[int]] = {}
    for index, batch in enumerate(batches):
        scope = (batch.tenant_id, batch.source_id, batch.file_epoch)
        grouped.setdefault(scope, []).append(index)
    return grouped


def _duplicate_prefix_records(
    reference_batches: Sequence[EdgeBatch],
    current_batch: EdgeBatch,
    *,
    overlap_count: int,
) -> tuple[dict, ...]:
    history_records: list[dict] = []
    for batch in reference_batches:
        same_scope = (
            batch.tenant_id == current_batch.tenant_id
            and batch.source_id == current_batch.source_id
            and batch.file_epoch == current_batch.file_epoch
            and batch.end_offset <= current_batch.start_offset
        )
        if same_scope:
            history_records.extend(batch.payload)
    if overlap_count <= 0 or not history_records:
        return ()
    return tuple(dict(record) for record in history_records[-overlap_count:])


def _with_overlap_prefix(batch: EdgeBatch, overlap_prefix: Sequence[dict]) -> EdgeBatch:
    overlap_count = len(overlap_prefix)
    return make_edge_batch(
        tenant_id=batch.tenant_id,
        source_id=batch.source_id,
        file_epoch=batch.file_epoch,
        seq_no=batch.seq_no,
        start_offset=batch.start_offset - overlap_count,
        end_offset=batch.end_offset,
        payload=list(overlap_prefix) + [dict(record) for record in batch.payload],
    )


def _target_from_maximum(dupe_rate: float, maximum: int) -> int:
    return min(maximum, math.floor(dupe_rate * maximum))


def _inject_new_append(base_batches: Sequence[EdgeBatch], *, dupe_rate: float) -> InjectionResult:
    return InjectionResult(
        batches=clone_edge_batches(base_batches),
        scenario="new_append",
        requested_dupe_rate=dupe_rate,
        realized_dupe_rate=0.0,
        injected_duplicate_records=0,
        max_injectable_duplicates=0,
        scenario_description=SCENARIO_DESCRIPTIONS["new_append"],
        replay_batches_added=0,
        overlap_batches_modified=0,
    )


def _inject_exact_replay(
    base_batches: Sequence[EdgeBatch],
    *,
    dupe_rate: float,
    seed: int | None,
) -> InjectionResult:
    base_clones = clone_edge_batches(base_batches)
    eligible_indices = list(range(len(base_clones)))
    maximum = sum(base_clones[index].payload_record_count for index in eligible_indices)
    target_batches = _target_from_maximum(dupe_rate, len(eligible_indices))
    if target_batches == 0:
        return InjectionResult(
            batches=base_clones,
            scenario="exact_replay",
            requested_dupe_rate=dupe_rate,
            realized_dupe_rate=0.0,
            injected_duplicate_records=0,
            max_injectable_duplicates=maximum,
            scenario_description=SCENARIO_DESCRIPTIONS["exact_replay"],
            replay_batches_added=0,
            overlap_batches_modified=0,
        )
    rng = random.Random(seed)
    chosen_indices = list(eligible_indices)
    rng.shuffle(chosen_indices)
    chosen_indices = chosen_indices[:target_batches]
    replay_batches = [base_clones[index].clone() for index in sorted(chosen_indices)]
    injected_records = sum(batch.payload_record_count for batch in replay_batches)
    result_batches = base_clones + replay_batches
    total_input_records = sum(batch.payload_record_count for batch in result_batches)
    realized_dupe_rate = injected_records / total_input_records if total_input_records else 0.0
    return InjectionResult(
        batches=result_batches,
        scenario="exact_replay",
        requested_dupe_rate=dupe_rate,
        realized_dupe_rate=realized_dupe_rate,
        injected_duplicate_records=injected_records,
        max_injectable_duplicates=maximum,
        scenario_description=SCENARIO_DESCRIPTIONS["exact_replay"],
        replay_batches_added=len(replay_batches),
        overlap_batches_modified=0,
    )


def _inject_partial_overlap(base_batches: Sequence[EdgeBatch], *, dupe_rate: float) -> InjectionResult:
    reference_batches = clone_edge_batches(base_batches)
    working_batches = clone_edge_batches(base_batches)
    grouped_indices = _group_batch_indices_by_scope(working_batches)
    maximum = 0
    injected_records = 0
    overlap_batches_modified = 0

    for indices in grouped_indices.values():
        for position, batch_index in enumerate(indices):
            if position == 0:
                continue
            batch = working_batches[batch_index]
            available_history = batch.start_offset
            if available_history <= 0 or batch.payload_record_count <= 0:
                continue
            maximum += min(batch.payload_record_count, available_history)
            overlap_count = min(
                available_history,
                _target_from_maximum(dupe_rate, batch.payload_record_count),
            )
            if overlap_count == 0:
                continue
            prefix = _duplicate_prefix_records(
                reference_batches,
                batch,
                overlap_count=overlap_count,
            )
            if not prefix:
                continue
            working_batches[batch_index] = _with_overlap_prefix(batch, prefix)
            injected_records += len(prefix)
            overlap_batches_modified += 1

    total_input_records = sum(batch.payload_record_count for batch in working_batches)
    realized_dupe_rate = injected_records / total_input_records if total_input_records else 0.0
    return InjectionResult(
        batches=working_batches,
        scenario="partial_overlap",
        requested_dupe_rate=dupe_rate,
        realized_dupe_rate=realized_dupe_rate,
        injected_duplicate_records=injected_records,
        max_injectable_duplicates=maximum,
        scenario_description=SCENARIO_DESCRIPTIONS["partial_overlap"],
        replay_batches_added=0,
        overlap_batches_modified=overlap_batches_modified,
    )


def _inject_random_record_replay(
    base_batches: Sequence[EdgeBatch],
    *,
    dupe_rate: float,
    seed: int | None,
) -> InjectionResult:
    base_clones = clone_edge_batches(base_batches)
    flat_records: list[dict] = []
    for batch in base_clones:
        flat_records.extend(dict(record) for record in batch.payload)
    maximum = len(flat_records)
    target_records = _target_from_maximum(dupe_rate, maximum)
    if target_records == 0:
        return InjectionResult(
            batches=base_clones,
            scenario="random_record_replay",
            requested_dupe_rate=dupe_rate,
            realized_dupe_rate=0.0,
            injected_duplicate_records=0,
            max_injectable_duplicates=maximum,
            scenario_description=SCENARIO_DESCRIPTIONS["random_record_replay"],
            replay_batches_added=0,
            overlap_batches_modified=0,
        )
    rng = random.Random(seed)
    candidate_indices = list(range(len(flat_records)))
    rng.shuffle(candidate_indices)
    chosen_records = [flat_records[index] for index in candidate_indices[:target_records]]
    anchor = base_clones[-1] if base_clones else make_edge_batch(
        tenant_id="tenant-1",
        source_id="source-1",
        file_epoch="epoch-1",
        seq_no=1,
        start_offset=0,
        end_offset=0,
        payload=[],
    )
    replay_batch = make_edge_batch(
        tenant_id=anchor.tenant_id,
        source_id=anchor.source_id,
        file_epoch=anchor.file_epoch,
        seq_no=anchor.seq_no + 1,
        start_offset=anchor.end_offset,
        end_offset=anchor.end_offset + len(chosen_records),
        payload=chosen_records,
    )
    result_batches = base_clones + [replay_batch]
    total_input_records = sum(batch.payload_record_count for batch in result_batches)
    realized_dupe_rate = target_records / total_input_records if total_input_records else 0.0
    return InjectionResult(
        batches=result_batches,
        scenario="random_record_replay",
        requested_dupe_rate=dupe_rate,
        realized_dupe_rate=realized_dupe_rate,
        injected_duplicate_records=target_records,
        max_injectable_duplicates=maximum,
        scenario_description=SCENARIO_DESCRIPTIONS["random_record_replay"],
        replay_batches_added=1,
        overlap_batches_modified=0,
    )


def _inject_mixed_upload(
    base_batches: Sequence[EdgeBatch],
    *,
    dupe_rate: float,
    seed: int | None,
) -> InjectionResult:
    reference_batches = clone_edge_batches(base_batches)
    working_batches = clone_edge_batches(base_batches)
    grouped_indices = _group_batch_indices_by_scope(working_batches)
    replay_candidates: list[int] = []
    maximum_overlap = 0
    injected_overlap_records = 0
    overlap_batches_modified = 0

    for indices in grouped_indices.values():
        for position, batch_index in enumerate(indices):
            batch = working_batches[batch_index]
            if position == 0:
                replay_candidates.append(batch_index)
                continue
            if position % 2 == 0:
                replay_candidates.append(batch_index)
                continue
            available_history = batch.start_offset
            if available_history <= 0 or batch.payload_record_count <= 0:
                continue
            maximum_overlap += min(batch.payload_record_count, available_history)
            overlap_count = min(
                available_history,
                _target_from_maximum(dupe_rate, batch.payload_record_count),
            )
            if overlap_count == 0:
                continue
            prefix = _duplicate_prefix_records(
                reference_batches,
                batch,
                overlap_count=overlap_count,
            )
            if not prefix:
                continue
            working_batches[batch_index] = _with_overlap_prefix(batch, prefix)
            injected_overlap_records += len(prefix)
            overlap_batches_modified += 1

    maximum_replay = sum(working_batches[index].payload_record_count for index in replay_candidates)
    replay_target = _target_from_maximum(dupe_rate, len(replay_candidates))
    rng = random.Random(seed)
    replay_indices = list(replay_candidates)
    rng.shuffle(replay_indices)
    replay_indices = replay_indices[:replay_target]
    replay_batches = [working_batches[index].clone() for index in sorted(replay_indices)]
    injected_replay_records = sum(batch.payload_record_count for batch in replay_batches)

    result_batches = working_batches + replay_batches
    injected_records = injected_overlap_records + injected_replay_records
    total_input_records = sum(batch.payload_record_count for batch in result_batches)
    realized_dupe_rate = injected_records / total_input_records if total_input_records else 0.0
    return InjectionResult(
        batches=result_batches,
        scenario="mixed_upload",
        requested_dupe_rate=dupe_rate,
        realized_dupe_rate=realized_dupe_rate,
        injected_duplicate_records=injected_records,
        max_injectable_duplicates=maximum_overlap + maximum_replay,
        scenario_description=SCENARIO_DESCRIPTIONS["mixed_upload"],
        replay_batches_added=len(replay_batches),
        overlap_batches_modified=overlap_batches_modified,
    )


def inject_upload_behavior(
    base_batches: Sequence[EdgeBatch],
    *,
    scenario: str,
    dupe_rate: float,
    seed: int | None,
) -> InjectionResult:
    _validate_dupe_rate(dupe_rate)
    if scenario not in SCENARIO_CHOICES:
        valid = ", ".join(SCENARIO_CHOICES)
        raise ValueError(f"Unknown scenario '{scenario}'. Valid scenarios: {valid}")
    if scenario == "new_append":
        return _inject_new_append(base_batches, dupe_rate=dupe_rate)
    if scenario == "exact_replay":
        return _inject_exact_replay(base_batches, dupe_rate=dupe_rate, seed=seed)
    if scenario == "partial_overlap":
        return _inject_partial_overlap(base_batches, dupe_rate=dupe_rate)
    if scenario == "mixed_upload":
        return _inject_mixed_upload(base_batches, dupe_rate=dupe_rate, seed=seed)
    return _inject_random_record_replay(base_batches, dupe_rate=dupe_rate, seed=seed)
