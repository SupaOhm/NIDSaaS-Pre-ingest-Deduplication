"""Metrics collection and result serialization for edge upload experiments."""

from __future__ import annotations

from dataclasses import asdict, dataclass

from .edge_batches import EdgeBatch
from .workload import WorkloadMetadata


@dataclass
class ExperimentResult:
    scenario: str
    mode: str
    tenant_aware: bool
    bloom_bits: int
    bloom_hashes: int
    total_input_batches: int
    total_transmitted_batches: int
    total_input_records: int
    total_transmitted_records: int
    total_input_bytes: int
    total_transmitted_bytes: int
    bytes_saved: int
    communication_reduction_pct: float
    accepted_payload_ratio: float
    duplicate_suppression_ratio: float
    gateway_ingress_reduction_pct: float
    dropped_batches: int
    trimmed_batches: int
    dropped_duplicate_records: int
    trimmed_overlapping_records: int
    processing_time_s: float
    avg_latency_us: float
    bloom_positive_count: int
    bloom_negative_count: int
    exact_lookup_count: int
    bloom_false_positive_count: int
    bloom_trigger_false_positive_pct: float
    bloom_estimated_memory_bytes: int
    exact_store_estimated_memory_bytes: int
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

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class ComparisonResult:
    preset_name: str
    scenario: str
    baseline_mode: str
    proposed_mode: str
    dataset_name: str
    csv_count: int
    tenant_aware: bool
    source_files_processed: tuple[str, ...]
    source_file_encodings: tuple[str, ...]
    detected_cic_columns: tuple[str, ...]
    missing_cic_columns: tuple[str, ...]
    schema_looks_like_cicflowmeter: bool
    cic_schema_quality: str
    file_epoch_mode: str
    offset_unit: str
    batch_size: int
    batch_order: str
    scenario_family: str
    requested_dupe_rate: float
    realized_dupe_rate: float
    injected_duplicate_records: int
    tenant_mode: str
    num_tenants: int
    source_mode: str
    num_sources: int
    seed: int | None
    baseline_transmitted_records: int
    proposed_transmitted_records: int
    record_reduction: int
    record_reduction_pct: float
    baseline_transmitted_bytes: int
    proposed_transmitted_bytes: int
    bytes_saved: int
    bytes_saved_delta: int
    baseline_processing_time_s: float
    proposed_processing_time_s: float
    communication_reduction_pct: float
    proposed_comm_reduction_pct: float
    proposed_accepted_payload_ratio: float
    proposed_duplicate_suppression_ratio: float
    proposed_gateway_ingress_reduction_pct: float
    dropped_batches: int
    trimmed_batches: int

    def to_dict(self) -> dict:
        return asdict(self)


class MetricsCollector:
    def __init__(
        self,
        scenario: str,
        mode: str,
        *,
        tenant_aware: bool = True,
        bloom_bits: int = 0,
        bloom_hashes: int = 0,
    ) -> None:
        self.scenario = scenario
        self.mode = mode
        self.tenant_aware = tenant_aware
        self.bloom_bits = bloom_bits
        self.bloom_hashes = bloom_hashes
        self.total_input_batches = 0
        self.total_transmitted_batches = 0
        self.total_input_records = 0
        self.total_transmitted_records = 0
        self.total_input_bytes = 0
        self.total_transmitted_bytes = 0
        self.dropped_batches = 0
        self.trimmed_batches = 0
        self.dropped_duplicate_records = 0
        self.trimmed_overlapping_records = 0
        self.bloom_positive_count = 0
        self.bloom_negative_count = 0
        self.exact_lookup_count = 0
        self.bloom_false_positive_count = 0

    def observe_input_batch(self, batch: EdgeBatch) -> None:
        self.total_input_batches += 1
        self.total_input_records += batch.payload_record_count
        self.total_input_bytes += batch.payload_size_bytes

    def observe_transmitted_batch(self, batch: EdgeBatch) -> None:
        self.total_transmitted_batches += 1
        self.total_transmitted_records += batch.payload_record_count
        self.total_transmitted_bytes += batch.payload_size_bytes

    def observe_trimmed_batch(
        self,
        original_batch: EdgeBatch,
        transmitted_batch: EdgeBatch,
        *,
        trimmed_record_count: int,
    ) -> None:
        self.trimmed_batches += 1
        self.trimmed_overlapping_records += trimmed_record_count
        self.observe_transmitted_batch(transmitted_batch)
        self.dropped_duplicate_records += (
            original_batch.payload_record_count - transmitted_batch.payload_record_count
        )

    def observe_dropped_batch(
        self,
        batch: EdgeBatch,
        *,
        reason: str,
        suppressed_record_count: int,
    ) -> None:
        self.dropped_batches += 1
        self.dropped_duplicate_records += suppressed_record_count

    def observe_bloom_positive(self) -> None:
        self.bloom_positive_count += 1
        self.exact_lookup_count += 1

    def observe_bloom_negative(self) -> None:
        self.bloom_negative_count += 1

    def observe_bloom_false_positive(self) -> None:
        self.bloom_false_positive_count += 1

    def finalize(
        self,
        processing_time_s: float,
        *,
        workload_metadata: WorkloadMetadata | None = None,
        bloom_memory_bytes: int = 0,
        exact_store_memory_bytes: int = 0,
    ) -> ExperimentResult:
        metadata = workload_metadata or WorkloadMetadata()
        bytes_saved = self.total_input_bytes - self.total_transmitted_bytes
        accepted_payload_ratio = (
            self.total_transmitted_bytes / self.total_input_bytes if self.total_input_bytes else 0.0
        )
        communication_reduction_pct = (
            (bytes_saved / self.total_input_bytes) * 100 if self.total_input_bytes else 0.0
        )
        avg_latency_us = (
            (processing_time_s * 1_000_000) / self.total_input_batches if self.total_input_batches else 0.0
        )
        duplicate_suppression_ratio = (
            (self.total_input_records - self.total_transmitted_records) / self.total_input_records
            if self.total_input_records
            else 0.0
        )
        gateway_ingress_reduction_pct = duplicate_suppression_ratio * 100
        bloom_trigger_false_positive_pct = (
            (self.bloom_false_positive_count / self.exact_lookup_count) * 100
            if self.exact_lookup_count
            else 0.0
        )
        return ExperimentResult(
            preset_name=metadata.preset_name,
            scenario=self.scenario,
            mode=self.mode,
            tenant_aware=self.tenant_aware,
            bloom_bits=self.bloom_bits,
            bloom_hashes=self.bloom_hashes,
            total_input_batches=self.total_input_batches,
            total_transmitted_batches=self.total_transmitted_batches,
            total_input_records=self.total_input_records,
            total_transmitted_records=self.total_transmitted_records,
            total_input_bytes=self.total_input_bytes,
            total_transmitted_bytes=self.total_transmitted_bytes,
            bytes_saved=bytes_saved,
            communication_reduction_pct=communication_reduction_pct,
            accepted_payload_ratio=accepted_payload_ratio,
            duplicate_suppression_ratio=duplicate_suppression_ratio,
            gateway_ingress_reduction_pct=gateway_ingress_reduction_pct,
            dropped_batches=self.dropped_batches,
            trimmed_batches=self.trimmed_batches,
            dropped_duplicate_records=self.dropped_duplicate_records,
            trimmed_overlapping_records=self.trimmed_overlapping_records,
            processing_time_s=processing_time_s,
            avg_latency_us=avg_latency_us,
            bloom_positive_count=self.bloom_positive_count,
            bloom_negative_count=self.bloom_negative_count,
            exact_lookup_count=self.exact_lookup_count,
            bloom_false_positive_count=self.bloom_false_positive_count,
            bloom_trigger_false_positive_pct=bloom_trigger_false_positive_pct,
            bloom_estimated_memory_bytes=bloom_memory_bytes,
            exact_store_estimated_memory_bytes=exact_store_memory_bytes,
            dataset_name=metadata.dataset_name,
            csv_count=metadata.csv_count,
            total_loaded_records=metadata.total_loaded_records,
            source_files_processed=metadata.source_files_processed,
            source_file_encodings=metadata.source_file_encodings,
            detected_cic_columns=metadata.detected_cic_columns,
            missing_cic_columns=metadata.missing_cic_columns,
            schema_looks_like_cicflowmeter=metadata.schema_looks_like_cicflowmeter,
            cic_schema_quality=metadata.cic_schema_quality,
            file_epoch_mode=metadata.file_epoch_mode,
            offset_unit=metadata.offset_unit,
            batch_size=metadata.batch_size,
            batch_order=metadata.batch_order,
            scenario_family=metadata.scenario_family,
            requested_dupe_rate=metadata.requested_dupe_rate,
            realized_dupe_rate=metadata.realized_dupe_rate,
            injected_duplicate_records=metadata.injected_duplicate_records,
            tenant_mode=metadata.tenant_mode,
            num_tenants=metadata.num_tenants,
            source_mode=metadata.source_mode,
            num_sources=metadata.num_sources,
            seed=metadata.seed,
        )


def build_comparison(baseline: ExperimentResult, proposed: ExperimentResult) -> ComparisonResult:
    record_reduction = baseline.total_transmitted_records - proposed.total_transmitted_records
    record_reduction_pct = (
        (record_reduction / baseline.total_transmitted_records) * 100
        if baseline.total_transmitted_records
        else 0.0
    )
    bytes_saved = baseline.total_transmitted_bytes - proposed.total_transmitted_bytes
    return ComparisonResult(
        preset_name=proposed.preset_name,
        scenario=baseline.scenario,
        baseline_mode=baseline.mode,
        proposed_mode=proposed.mode,
        dataset_name=proposed.dataset_name,
        csv_count=proposed.csv_count,
        tenant_aware=proposed.tenant_aware,
        source_files_processed=proposed.source_files_processed,
        source_file_encodings=proposed.source_file_encodings,
        detected_cic_columns=proposed.detected_cic_columns,
        missing_cic_columns=proposed.missing_cic_columns,
        schema_looks_like_cicflowmeter=proposed.schema_looks_like_cicflowmeter,
        cic_schema_quality=proposed.cic_schema_quality,
        file_epoch_mode=proposed.file_epoch_mode,
        offset_unit=proposed.offset_unit,
        batch_size=proposed.batch_size,
        batch_order=proposed.batch_order,
        scenario_family=proposed.scenario_family,
        requested_dupe_rate=proposed.requested_dupe_rate,
        realized_dupe_rate=proposed.realized_dupe_rate,
        injected_duplicate_records=proposed.injected_duplicate_records,
        tenant_mode=proposed.tenant_mode,
        num_tenants=proposed.num_tenants,
        source_mode=proposed.source_mode,
        num_sources=proposed.num_sources,
        seed=proposed.seed,
        baseline_transmitted_records=baseline.total_transmitted_records,
        proposed_transmitted_records=proposed.total_transmitted_records,
        record_reduction=record_reduction,
        record_reduction_pct=record_reduction_pct,
        baseline_transmitted_bytes=baseline.total_transmitted_bytes,
        proposed_transmitted_bytes=proposed.total_transmitted_bytes,
        bytes_saved=bytes_saved,
        bytes_saved_delta=bytes_saved,
        baseline_processing_time_s=baseline.processing_time_s,
        proposed_processing_time_s=proposed.processing_time_s,
        communication_reduction_pct=proposed.communication_reduction_pct,
        proposed_comm_reduction_pct=proposed.communication_reduction_pct,
        proposed_accepted_payload_ratio=proposed.accepted_payload_ratio,
        proposed_duplicate_suppression_ratio=proposed.duplicate_suppression_ratio,
        proposed_gateway_ingress_reduction_pct=proposed.gateway_ingress_reduction_pct,
        dropped_batches=proposed.dropped_batches,
        trimmed_batches=proposed.trimmed_batches,
    )
