"""Per-source edge deduplication state."""

from __future__ import annotations

from dataclasses import dataclass, field

from .edge_batches import EdgeBatch


@dataclass(frozen=True)
class EdgeScopeKey:
    tenant_id: str
    source_id: str
    file_epoch: str


@dataclass
class EdgeSourceState:
    committed_offset: int = 0
    last_seq_no: int | None = None
    accepted_batch_identities: set[tuple[int, int, str]] = field(default_factory=set)

    def accept(self, batch: EdgeBatch) -> None:
        self.committed_offset = max(self.committed_offset, batch.end_offset)
        self.last_seq_no = batch.seq_no
        self.accepted_batch_identities.add(batch.identity)


class EdgeStateTable:
    def __init__(self) -> None:
        self._states: dict[EdgeScopeKey, EdgeSourceState] = {}

    @staticmethod
    def scope_for(batch: EdgeBatch) -> EdgeScopeKey:
        return EdgeScopeKey(
            tenant_id=batch.tenant_id,
            source_id=batch.source_id,
            file_epoch=batch.file_epoch,
        )

    def state_for(self, batch: EdgeBatch) -> EdgeSourceState:
        scope = self.scope_for(batch)
        if scope not in self._states:
            self._states[scope] = EdgeSourceState()
        return self._states[scope]
