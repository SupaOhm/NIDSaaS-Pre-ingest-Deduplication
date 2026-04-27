from __future__ import annotations

import unittest

from src.edge_batches import build_edge_batches
from src.injection import inject_upload_behavior


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


class InjectionTests(unittest.TestCase):
    def setUp(self) -> None:
        records = [_make_record(index) for index in range(6)]
        self.base_batches = build_edge_batches(
            records,
            batch_size=2,
            batch_order="sequential",
            seed=42,
        )

    def test_dupe_rate_zero_injects_no_duplicates(self) -> None:
        result = inject_upload_behavior(
            self.base_batches,
            scenario="mixed_upload",
            dupe_rate=0.0,
            seed=11,
        )

        self.assertEqual(result.injected_duplicate_records, 0)
        self.assertEqual(result.realized_dupe_rate, 0.0)
        self.assertEqual(
            sum(batch.payload_record_count for batch in result.batches),
            sum(batch.payload_record_count for batch in self.base_batches),
        )

    def test_exact_replay_requested_vs_realized_dupe_rate_is_reported(self) -> None:
        result = inject_upload_behavior(
            self.base_batches,
            scenario="exact_replay",
            dupe_rate=0.5,
            seed=19,
        )

        self.assertEqual(result.injected_duplicate_records, 2)
        self.assertAlmostEqual(result.realized_dupe_rate, 2 / 8)

    def test_partial_overlap_dupe_rate_one_hits_maximum(self) -> None:
        result = inject_upload_behavior(
            self.base_batches,
            scenario="partial_overlap",
            dupe_rate=1.0,
            seed=5,
        )

        self.assertEqual(result.injected_duplicate_records, 4)
        self.assertEqual(result.max_injectable_duplicates, 4)

    def test_new_append_ignores_dupe_rate(self) -> None:
        result = inject_upload_behavior(
            self.base_batches,
            scenario="new_append",
            dupe_rate=1.0,
            seed=5,
        )

        self.assertEqual(result.injected_duplicate_records, 0)
        self.assertEqual(result.realized_dupe_rate, 0.0)

    def test_mixed_upload_is_deterministic_for_same_seed(self) -> None:
        left = inject_upload_behavior(
            self.base_batches,
            scenario="mixed_upload",
            dupe_rate=0.5,
            seed=7,
        )
        right = inject_upload_behavior(
            self.base_batches,
            scenario="mixed_upload",
            dupe_rate=0.5,
            seed=7,
        )

        self.assertEqual(left.injected_duplicate_records, right.injected_duplicate_records)
        self.assertEqual(left.realized_dupe_rate, right.realized_dupe_rate)
        self.assertEqual(left.batches, right.batches)


if __name__ == "__main__":
    unittest.main()
