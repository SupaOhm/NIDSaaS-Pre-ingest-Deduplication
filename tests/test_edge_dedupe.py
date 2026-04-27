from __future__ import annotations

import unittest

from src.datasets.cic_ids2017 import load_cic_ids2017_csvs
from src.edge_batches import build_edge_batches, make_edge_batch
from src.edge_dedupe import EdgeMetadataRunner, NoEdgeDedupRunner
from src.injection import inject_upload_behavior
from src.metrics import build_comparison


def _make_record(
    record_id: int,
    *,
    tenant_id: str = "tenant-1",
    source_id: str = "source-1",
    file_epoch: str = "epoch-1",
) -> dict:
    return {
        "tenant_id": tenant_id,
        "source_id": source_id,
        "file_epoch": file_epoch,
        "timestamp": f"2026-01-01T00:00:{record_id:02d}Z",
        "src_ip": f"10.0.0.{record_id + 1}",
        "dst_ip": "172.16.0.1",
        "src_port": 1000 + record_id,
        "dst_port": 80,
        "protocol": "6",
        "flow_duration": 100 + record_id,
        "bytes": 500 + record_id,
        "packets": 2,
        "event_type": "flow",
        "label": "BENIGN",
    }


class EdgeMetadataTests(unittest.TestCase):
    def setUp(self) -> None:
        records = [_make_record(index) for index in range(4)]
        self.base_batches = build_edge_batches(
            records,
            batch_size=2,
            batch_order="sequential",
            seed=42,
        )

    def test_exact_replay_is_dropped_at_edge(self) -> None:
        injected = inject_upload_behavior(
            self.base_batches,
            scenario="exact_replay",
            dupe_rate=1.0,
            seed=7,
        )

        result = EdgeMetadataRunner().run("exact_replay", injected.batches)

        self.assertEqual(result.total_input_batches, 4)
        self.assertEqual(result.total_transmitted_batches, 2)
        self.assertEqual(result.total_input_records, 8)
        self.assertEqual(result.total_transmitted_records, 4)
        self.assertEqual(result.dropped_batches, 2)
        self.assertEqual(result.trimmed_batches, 0)

    def test_partial_overlap_is_trimmed(self) -> None:
        injected = inject_upload_behavior(
            self.base_batches,
            scenario="partial_overlap",
            dupe_rate=1.0,
            seed=7,
        )

        result = EdgeMetadataRunner().run("partial_overlap", injected.batches)

        self.assertEqual(result.total_input_batches, 2)
        self.assertEqual(result.total_transmitted_batches, 2)
        self.assertEqual(result.total_input_records, 6)
        self.assertEqual(result.total_transmitted_records, 4)
        self.assertEqual(result.dropped_batches, 0)
        self.assertEqual(result.trimmed_batches, 1)
        self.assertEqual(result.trimmed_overlapping_records, 2)

    def test_new_append_forwards_everything(self) -> None:
        injected = inject_upload_behavior(
            self.base_batches,
            scenario="new_append",
            dupe_rate=0.9,
            seed=7,
        )

        result = EdgeMetadataRunner().run("new_append", injected.batches)

        self.assertEqual(result.total_input_records, 4)
        self.assertEqual(result.total_transmitted_records, 4)
        self.assertEqual(result.bytes_saved, 0)
        self.assertEqual(result.dropped_batches, 0)
        self.assertEqual(result.trimmed_batches, 0)

    def test_already_covered_batch_is_dropped(self) -> None:
        first = self.base_batches[0]
        already_covered = make_edge_batch(
            tenant_id=first.tenant_id,
            source_id=first.source_id,
            file_epoch=first.file_epoch,
            seq_no=99,
            start_offset=0,
            end_offset=1,
            payload=first.payload[:1],
        )

        result = EdgeMetadataRunner().run("already_covered", [first, already_covered])

        self.assertEqual(result.total_input_batches, 2)
        self.assertEqual(result.total_transmitted_batches, 1)
        self.assertEqual(result.dropped_batches, 1)
        self.assertEqual(result.total_transmitted_records, 2)

    def test_tenant_state_is_isolated(self) -> None:
        tenant_a = self.base_batches[0]
        tenant_b = make_edge_batch(
            tenant_id="tenant-2",
            source_id=tenant_a.source_id,
            file_epoch=tenant_a.file_epoch,
            seq_no=tenant_a.seq_no,
            start_offset=tenant_a.start_offset,
            end_offset=tenant_a.end_offset,
            payload=tenant_a.payload,
        )
        tenant_a_replay = tenant_a.clone()

        result = EdgeMetadataRunner().run(
            "tenant_isolation",
            [tenant_a, tenant_b, tenant_a_replay],
        )

        self.assertEqual(result.total_transmitted_batches, 2)
        self.assertEqual(result.dropped_batches, 1)
        self.assertEqual(result.total_transmitted_records, 4)

    def test_baseline_vs_edge_metadata_on_cic_workload(self) -> None:
        load_result = load_cic_ids2017_csvs(
            ["sample_data/cic_ids2017_sample.csv"],
            tenant_mode="single",
            num_tenants=1,
            source_mode="single",
            num_sources=1,
        )
        base_batches = build_edge_batches(
            load_result.records,
            batch_size=3,
            batch_order="sequential",
            seed=42,
        )
        injected = inject_upload_behavior(
            base_batches,
            scenario="exact_replay",
            dupe_rate=1.0,
            seed=42,
        )

        baseline = NoEdgeDedupRunner().run("exact_replay", injected.batches)
        proposed = EdgeMetadataRunner().run("exact_replay", injected.batches)
        comparison = build_comparison(baseline, proposed)

        self.assertEqual(baseline.total_transmitted_records, 6)
        self.assertEqual(proposed.total_transmitted_records, 3)
        self.assertEqual(proposed.dropped_batches, 1)
        self.assertEqual(comparison.baseline_mode, "no_edge_dedup")
        self.assertEqual(comparison.proposed_mode, "edge_metadata")
        self.assertEqual(comparison.record_reduction, 3)
        self.assertEqual(comparison.record_reduction_pct, 50.0)
        self.assertEqual(comparison.bytes_saved, 1537)
        self.assertEqual(comparison.dropped_batches, 1)
        self.assertEqual(comparison.trimmed_batches, 0)


if __name__ == "__main__":
    unittest.main()
