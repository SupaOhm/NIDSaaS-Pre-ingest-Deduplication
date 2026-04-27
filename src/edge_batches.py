"""Client-side edge upload batch construction and manipulation helpers."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
import hashlib
import random
from typing import Any, Sequence

from .canonicalize import record_size_bytes, serialize_record
from .io_utils import chunk_records

EDGE_BATCH_ORDER_CHOICES = ("sequential", "shuffled")
DEFAULT_FILE_EPOCH = "epoch-1"


def payload_record_count(payload: Sequence[dict[str, Any]]) -> int:
    return len(payload)


def payload_size_bytes(payload: Sequence[dict[str, Any]]) -> int:
    return sum(record_size_bytes(record) for record in payload)


def build_batch_hash(payload: Sequence[dict[str, Any]], *, hash_name: str = "sha256") -> str:
    digest = hashlib.new(hash_name)
    digest.update(b"[")
    for index, record in enumerate(payload):
        if index:
            digest.update(b",")
        digest.update(serialize_record(record).encode("utf-8"))
    digest.update(b"]")
    return digest.hexdigest()


@dataclass(frozen=True)
class EdgeBatch:
    tenant_id: str
    source_id: str
    file_epoch: str
    seq_no: int
    start_offset: int
    end_offset: int
    batch_hash: str
    payload: tuple[dict[str, Any], ...]

    @property
    def payload_record_count(self) -> int:
        return payload_record_count(self.payload)

    @property
    def payload_size_bytes(self) -> int:
        return payload_size_bytes(self.payload)

    @property
    def identity(self) -> tuple[int, int, str]:
        return (self.start_offset, self.end_offset, self.batch_hash)

    def clone(self) -> EdgeBatch:
        return make_edge_batch(
            tenant_id=self.tenant_id,
            source_id=self.source_id,
            file_epoch=self.file_epoch,
            seq_no=self.seq_no,
            start_offset=self.start_offset,
            end_offset=self.end_offset,
            payload=self.payload,
            batch_hash=self.batch_hash,
        )


def make_edge_batch(
    *,
    tenant_id: str,
    source_id: str,
    file_epoch: str,
    seq_no: int,
    start_offset: int,
    end_offset: int,
    payload: Sequence[dict[str, Any]],
    batch_hash: str | None = None,
) -> EdgeBatch:
    payload_records = tuple(deepcopy(record) for record in payload)
    computed_hash = batch_hash or build_batch_hash(payload_records)
    return EdgeBatch(
        tenant_id=tenant_id,
        source_id=source_id,
        file_epoch=file_epoch,
        seq_no=seq_no,
        start_offset=start_offset,
        end_offset=end_offset,
        batch_hash=computed_hash,
        payload=payload_records,
    )


def trim_edge_batch_prefix(batch: EdgeBatch, trimmed_record_count: int) -> EdgeBatch:
    if not 0 <= trimmed_record_count <= batch.payload_record_count:
        raise ValueError("trimmed_record_count must be within the payload length")
    trimmed_payload = batch.payload[trimmed_record_count:]
    return make_edge_batch(
        tenant_id=batch.tenant_id,
        source_id=batch.source_id,
        file_epoch=batch.file_epoch,
        seq_no=batch.seq_no,
        start_offset=batch.start_offset + trimmed_record_count,
        end_offset=batch.end_offset,
        payload=trimmed_payload,
    )


def clone_edge_batches(batches: Sequence[EdgeBatch]) -> list[EdgeBatch]:
    return [batch.clone() for batch in batches]


def freeze_edge_batches(batches: Sequence[EdgeBatch]) -> tuple[EdgeBatch, ...]:
    return tuple(batch.clone() for batch in batches)


def _resolve_file_epoch(record: dict[str, Any]) -> str:
    for field in ("file_epoch", "csv_file"):
        value = record.get(field)
        if value:
            return str(value)
    return DEFAULT_FILE_EPOCH


def build_edge_batches(
    records: Sequence[dict[str, Any]],
    *,
    batch_size: int,
    batch_order: str,
    seed: int | None,
) -> list[EdgeBatch]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if batch_order not in EDGE_BATCH_ORDER_CHOICES:
        valid = ", ".join(EDGE_BATCH_ORDER_CHOICES)
        raise ValueError(f"Unknown batch order '{batch_order}'. Valid values: {valid}")

    grouped_records: dict[tuple[str, str, str], list[tuple[int, dict[str, Any]]]] = {}
    for global_index, record in enumerate(records):
        scope = (
            str(record.get("tenant_id", "tenant-1")),
            str(record.get("source_id", "source-1")),
            _resolve_file_epoch(record),
        )
        grouped_records.setdefault(scope, []).append((global_index, deepcopy(record)))

    scheduled_batches: list[tuple[int, EdgeBatch]] = []
    for (tenant_id, source_id, file_epoch), scoped_records in grouped_records.items():
        payload_only = [record for _, record in scoped_records]
        global_indices = [global_index for global_index, _ in scoped_records]
        start_offset = 0
        for seq_index, payload in enumerate(chunk_records(payload_only, batch_size), start=1):
            first_index = global_indices[start_offset]
            end_offset = start_offset + len(payload)
            scheduled_batches.append(
                (
                    first_index,
                    make_edge_batch(
                        tenant_id=tenant_id,
                        source_id=source_id,
                        file_epoch=file_epoch,
                        seq_no=seq_index,
                        start_offset=start_offset,
                        end_offset=end_offset,
                        payload=payload,
                    ),
                )
            )
            start_offset = end_offset

    batches = [batch for _, batch in sorted(scheduled_batches, key=lambda item: item[0])]
    if batch_order == "shuffled":
        random.Random(seed).shuffle(batches)
    return batches
