from __future__ import annotations

import unittest

from src.fingerprint import FingerprintConfig, build_fingerprint


class FingerprintTests(unittest.TestCase):
    def test_fingerprint_is_stable_for_equivalent_records(self) -> None:
        first = {
            "tenant_id": "tenant-a",
            "source_id": "sensor-1",
            "timestamp": "2026-01-01T00:00:00Z",
            "src_ip": "10.0.0.1",
            "dst_ip": "172.16.0.1",
            "src_port": "1234",
            "dst_port": 80,
            "protocol": "TCP",
            "action": "ALLOW",
            "bytes": "512",
            "packets": "4",
            "event_type": "Flow",
        }
        second = dict(first)
        second["timestamp"] = "2026-01-01T00:00:00+00:00"
        second["protocol"] = "tcp"
        self.assertEqual(build_fingerprint(first), build_fingerprint(second))

    def test_tenant_aware_mode_changes_cross_tenant_fingerprint_identity(self) -> None:
        left = {
            "tenant_id": "tenant-a",
            "source_id": "sensor-1",
            "timestamp": "2026-01-01T00:00:00Z",
            "src_ip": "10.0.0.1",
            "dst_ip": "172.16.0.1",
            "src_port": 1234,
            "dst_port": 80,
            "protocol": "tcp",
            "action": "allow",
            "bytes": 512,
            "packets": 4,
            "event_type": "flow",
        }
        right = dict(left)
        right["tenant_id"] = "tenant-b"

        tenant_aware = FingerprintConfig(tenant_aware=True)
        tenant_unaware = FingerprintConfig(tenant_aware=False)

        self.assertNotEqual(build_fingerprint(left, tenant_aware), build_fingerprint(right, tenant_aware))
        self.assertEqual(build_fingerprint(left, tenant_unaware), build_fingerprint(right, tenant_unaware))


if __name__ == "__main__":
    unittest.main()

