"""Edge-side experiment modes for client upload communication-cost reduction."""

from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Any, Sequence

from .bloom import BloomFilter
from .canonicalize import CIC_FINGERPRINT_FIELDS
from .edge_batches import EdgeBatch, make_edge_batch, trim_edge_batch_prefix
from .edge_state import EdgeStateTable
from .exact_store import ExactFingerprintStore
from .fingerprint import FingerprintConfig, build_fingerprint
from .metrics import ExperimentResult, MetricsCollector
from .sink import CloudIngressSink
from .workload import WorkloadMetadata

EDGE_MODE_CHOICES = ("no_edge_dedup", "edge_metadata", "edge_bloom_variant")

DECISION_FORWARD_FULL = "forward_full"
DECISION_TRIMMED = "trimmed_overlap"
DECISION_DROP_EXACT_REPLAY = "drop_exact_replay"
DECISION_DROP_ALREADY_COVERED = "drop_already_covered"


@dataclass(frozen=True)
class EdgeDecision:
    action: str
    transmitted_batch: EdgeBatch | None
    suppressed_record_count: int
    trimmed_record_count: int


@dataclass
class EdgeBloomVariantConfig:
    bloom_bits: int = 100_000
    bloom_hashes: int = 4
    fingerprint_fields: tuple[str, ...] = CIC_FINGERPRINT_FIELDS
    tenant_aware: bool = True
    hash_name: str = "sha256"
    exact_max_size: int | None = None
    exact_ttl_seconds: float | None = None


def decide_edge_metadata(batch: EdgeBatch, state_table: EdgeStateTable) -> EdgeDecision:
    state = state_table.state_for(batch)
    committed_offset = state.committed_offset
    if batch.identity in state.accepted_batch_identities:
        return EdgeDecision(
            action=DECISION_DROP_EXACT_REPLAY,
            transmitted_batch=None,
            suppressed_record_count=batch.payload_record_count,
            trimmed_record_count=0,
        )
    if batch.end_offset <= committed_offset:
        return EdgeDecision(
            action=DECISION_DROP_ALREADY_COVERED,
            transmitted_batch=None,
            suppressed_record_count=batch.payload_record_count,
            trimmed_record_count=0,
        )
    if batch.start_offset < committed_offset < batch.end_offset:
        trimmed_count = committed_offset - batch.start_offset
        transmitted_batch = trim_edge_batch_prefix(batch, trimmed_count)
        state.accept(batch)
        return EdgeDecision(
            action=DECISION_TRIMMED,
            transmitted_batch=transmitted_batch,
            suppressed_record_count=trimmed_count,
            trimmed_record_count=trimmed_count,
        )
    transmitted_batch = batch.clone()
    state.accept(batch)
    return EdgeDecision(
        action=DECISION_FORWARD_FULL,
        transmitted_batch=transmitted_batch,
        suppressed_record_count=0,
        trimmed_record_count=0,
    )


class NoEdgeDedupRunner:
    def __init__(self) -> None:
        self.sink = CloudIngressSink()

    def run(
        self,
        scenario_name: str,
        batches: Sequence[EdgeBatch],
        *,
        workload_metadata: WorkloadMetadata | None = None,
        progress: Any | None = None,
    ) -> ExperimentResult:
        if progress is not None:
            progress.run_start(mode="no_edge_dedup", total_batches=len(batches))
        metrics = MetricsCollector(scenario=scenario_name, mode="no_edge_dedup")
        started = time.perf_counter()
        for batch_index, batch in enumerate(batches, start=1):
            metrics.observe_input_batch(batch)
            self.sink.transmit(batch)
            metrics.observe_transmitted_batch(batch)
            if progress is not None:
                progress.run_progress(processed_batches=batch_index, total_batches=len(batches))
        result = metrics.finalize(
            time.perf_counter() - started,
            workload_metadata=workload_metadata,
        )
        if progress is not None:
            progress.run_summary(result)
        return result


class EdgeMetadataRunner:
    def __init__(self) -> None:
        self.sink = CloudIngressSink()
        self.state_table = EdgeStateTable()

    def run(
        self,
        scenario_name: str,
        batches: Sequence[EdgeBatch],
        *,
        workload_metadata: WorkloadMetadata | None = None,
        progress: Any | None = None,
    ) -> ExperimentResult:
        if progress is not None:
            progress.run_start(mode="edge_metadata", total_batches=len(batches))
        metrics = MetricsCollector(scenario=scenario_name, mode="edge_metadata")
        started = time.perf_counter()
        for batch_index, batch in enumerate(batches, start=1):
            metrics.observe_input_batch(batch)
            decision = decide_edge_metadata(batch, self.state_table)
            if decision.transmitted_batch is None:
                metrics.observe_dropped_batch(
                    batch,
                    reason=decision.action,
                    suppressed_record_count=decision.suppressed_record_count,
                )
                if progress is not None:
                    progress.run_progress(processed_batches=batch_index, total_batches=len(batches))
                continue
            if decision.action == DECISION_TRIMMED:
                metrics.observe_trimmed_batch(
                    batch,
                    decision.transmitted_batch,
                    trimmed_record_count=decision.trimmed_record_count,
                )
            else:
                metrics.observe_transmitted_batch(decision.transmitted_batch)
            self.sink.transmit(decision.transmitted_batch)
            if progress is not None:
                progress.run_progress(processed_batches=batch_index, total_batches=len(batches))
        result = metrics.finalize(
            time.perf_counter() - started,
            workload_metadata=workload_metadata,
        )
        if progress is not None:
            progress.run_summary(result)
        return result


class EdgeBloomVariantRunner:
    def __init__(self, config: EdgeBloomVariantConfig) -> None:
        self.config = config
        self.sink = CloudIngressSink()
        self.bloom = BloomFilter(config.bloom_bits, config.bloom_hashes)
        self.exact_store = ExactFingerprintStore(
            max_size=config.exact_max_size,
            ttl_seconds=config.exact_ttl_seconds,
        )
        self.fingerprint_config = FingerprintConfig(
            fields=config.fingerprint_fields,
            tenant_aware=config.tenant_aware,
            hash_name=config.hash_name,
        )

    def run(
        self,
        scenario_name: str,
        batches: Sequence[EdgeBatch],
        *,
        workload_metadata: WorkloadMetadata | None = None,
        progress: Any | None = None,
    ) -> ExperimentResult:
        if progress is not None:
            progress.run_start(mode="edge_bloom_variant", total_batches=len(batches))
        metrics = MetricsCollector(
            scenario=scenario_name,
            mode="edge_bloom_variant",
            tenant_aware=self.config.tenant_aware,
            bloom_bits=self.config.bloom_bits,
            bloom_hashes=self.config.bloom_hashes,
        )
        started = time.perf_counter()
        for batch_index, batch in enumerate(batches, start=1):
            metrics.observe_input_batch(batch)
            accepted_records = []
            suppressed_records = 0
            for record in batch.payload:
                fingerprint = build_fingerprint(record, self.fingerprint_config)
                if self.bloom.contains(fingerprint):
                    metrics.observe_bloom_positive()
                    if self.exact_store.contains(fingerprint):
                        suppressed_records += 1
                        continue
                    metrics.observe_bloom_false_positive()
                else:
                    metrics.observe_bloom_negative()
                accepted_records.append(record)
                self.bloom.add(fingerprint)
                self.exact_store.add(fingerprint)
            if not accepted_records:
                metrics.observe_dropped_batch(
                    batch,
                    reason=DECISION_DROP_EXACT_REPLAY,
                    suppressed_record_count=suppressed_records,
                )
                if progress is not None:
                    progress.run_progress(processed_batches=batch_index, total_batches=len(batches))
                continue
            transmitted_batch = batch.clone()
            if len(accepted_records) != batch.payload_record_count:
                transmitted_batch = make_edge_batch(
                    tenant_id=batch.tenant_id,
                    source_id=batch.source_id,
                    file_epoch=batch.file_epoch,
                    seq_no=batch.seq_no,
                    start_offset=batch.start_offset,
                    end_offset=batch.start_offset + len(accepted_records),
                    payload=accepted_records,
                )
                metrics.observe_trimmed_batch(
                    batch,
                    transmitted_batch,
                    trimmed_record_count=suppressed_records,
                )
            else:
                metrics.observe_transmitted_batch(transmitted_batch)
            self.sink.transmit(transmitted_batch)
            if progress is not None:
                progress.run_progress(processed_batches=batch_index, total_batches=len(batches))
        result = metrics.finalize(
            time.perf_counter() - started,
            workload_metadata=workload_metadata,
            bloom_memory_bytes=self.bloom.byte_size,
            exact_store_memory_bytes=self.exact_store.estimated_memory_bytes(),
        )
        if progress is not None:
            progress.run_summary(result)
        return result
