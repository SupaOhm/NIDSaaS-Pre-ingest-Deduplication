from __future__ import annotations

import unittest

from src.canonicalize import canonical_json, canonicalize_record


class CanonicalizeTests(unittest.TestCase):
    def test_canonicalization_is_consistent_for_equivalent_records(self) -> None:
        left = {
            "tenant_id": " Tenant-A ",
            "source_id": " Sensor-1 ",
            "timestamp": "2026-01-01T00:00:00Z",
            "src_ip": "10.0.0.1",
            "dst_ip": "172.16.0.1",
            "src_port": "1234",
            "dst_port": 80,
            "protocol": "TCP ",
            "action": " Allow",
            "bytes": "512",
            "packets": "4",
            "event_type": " Flow ",
            "extra_field": "ignore me",
        }
        right = {
            "event_type": "flow",
            "packets": 4,
            "bytes": 512,
            "action": "allow",
            "protocol": "tcp",
            "dst_port": "80",
            "src_port": 1234,
            "dst_ip": "172.16.0.1",
            "src_ip": "10.0.0.1",
            "timestamp": "2026-01-01T00:00:00+00:00",
            "source_id": "sensor-1",
            "tenant_id": "tenant-a",
        }
        self.assertEqual(canonicalize_record(left), canonicalize_record(right))
        self.assertEqual(canonical_json(left), canonical_json(right))


if __name__ == "__main__":
    unittest.main()

