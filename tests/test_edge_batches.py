from __future__ import annotations

import unittest

from src.datasets.cic_ids2017 import load_cic_ids2017_csvs
from src.edge_batches import build_batch_hash, build_edge_batches, make_edge_batch


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


class EdgeBatchTests(unittest.TestCase):
    def test_batch_hash_is_deterministic_for_equivalent_payloads(self) -> None:
        payload = [_make_record(0), _make_record(1)]
        left = make_edge_batch(
            tenant_id="tenant-1",
            source_id="source-1",
            file_epoch="epoch-1",
            seq_no=1,
            start_offset=0,
            end_offset=2,
            payload=payload,
        )
        right = make_edge_batch(
            tenant_id="tenant-1",
            source_id="source-1",
            file_epoch="epoch-1",
            seq_no=1,
            start_offset=0,
            end_offset=2,
            payload=[dict(record) for record in payload],
        )

        self.assertEqual(left.batch_hash, right.batch_hash)
        self.assertEqual(left.batch_hash, build_batch_hash(payload))

    def test_cic_backed_batch_generation_assigns_source_local_offsets(self) -> None:
        load_result = load_cic_ids2017_csvs(
            ["sample_data/cic_ids2017_sample.csv"],
            tenant_mode="single",
            num_tenants=1,
            source_mode="single",
            num_sources=1,
        )

        batches = build_edge_batches(
            load_result.records,
            batch_size=2,
            batch_order="sequential",
            seed=42,
        )

        self.assertEqual(len(batches), 2)
        self.assertEqual(batches[0].file_epoch, "cic_ids2017_sample.csv")
        self.assertEqual(batches[0].seq_no, 1)
        self.assertEqual(batches[0].start_offset, 0)
        self.assertEqual(batches[0].end_offset, 2)
        self.assertEqual(batches[1].seq_no, 2)
        self.assertEqual(batches[1].start_offset, 2)
        self.assertEqual(batches[1].end_offset, 3)


if __name__ == "__main__":
    unittest.main()
