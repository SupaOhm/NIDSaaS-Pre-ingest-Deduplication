"""Shared workload structures for CIC-backed edge upload experiments."""

from __future__ import annotations

from dataclasses import dataclass

from .edge_batches import EdgeBatch, clone_edge_batches, freeze_edge_batches


@dataclass(frozen=True)
class WorkloadMetadata:
    preset_name: str = ""
    dataset_name: str = "synthetic"
    csv_count: int = 0
    total_loaded_records: int = 0
    source_files_processed: tuple[str, ...] = ()
    source_file_encodings: tuple[str, ...] = ()
    detected_cic_columns: tuple[str, ...] = ()
    missing_cic_columns: tuple[str, ...] = ()
    schema_looks_like_cicflowmeter: bool = False
    cic_schema_quality: str = "n/a"
    file_epoch_mode: str = "per_csv"
    offset_unit: str = "record_index"
    batch_size: int = 0
    batch_order: str = "sequential"
    scenario_family: str = "new_append"
    requested_dupe_rate: float = 0.0
    realized_dupe_rate: float = 0.0
    injected_duplicate_records: int = 0
    tenant_mode: str = "single"
    num_tenants: int = 1
    source_mode: str = "single"
    num_sources: int = 1
    seed: int | None = None


@dataclass(frozen=True)
class PreparedWorkload:
    name: str
    description: str
    batches: tuple[EdgeBatch, ...]
    metadata: WorkloadMetadata

    def clone_batches(self) -> list[EdgeBatch]:
        return clone_edge_batches(self.batches)


__all__ = ["PreparedWorkload", "WorkloadMetadata", "freeze_edge_batches"]
