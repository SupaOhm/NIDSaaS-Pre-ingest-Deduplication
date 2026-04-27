"""Minimal cloud-ingress sink abstraction for communication-cost measurement."""

from __future__ import annotations

from dataclasses import dataclass

from .edge_batches import EdgeBatch


@dataclass
class CloudIngressSink:
    transmitted_batches: int = 0
    transmitted_records: int = 0
    transmitted_bytes: int = 0

    def transmit(self, batch: EdgeBatch) -> None:
        self.transmitted_batches += 1
        self.transmitted_records += batch.payload_record_count
        self.transmitted_bytes += batch.payload_size_bytes
