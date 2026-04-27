"""Exact fingerprint store used after the Bloom filter pre-check."""

from __future__ import annotations

from collections import OrderedDict
import sys
import time


class ExactFingerprintStore:
    def __init__(self, max_size: int | None = None, ttl_seconds: float | None = None) -> None:
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self._entries: "OrderedDict[str, float]" = OrderedDict()

    def _now(self) -> float:
        return time.monotonic()

    def _evict_expired(self, now: float) -> None:
        if self.ttl_seconds is None:
            return
        while self._entries:
            fingerprint, last_seen = next(iter(self._entries.items()))
            if now - last_seen <= self.ttl_seconds:
                break
            self._entries.popitem(last=False)

    def contains(self, fingerprint: str, now: float | None = None) -> bool:
        current_time = self._now() if now is None else now
        self._evict_expired(current_time)
        return fingerprint in self._entries

    def add(self, fingerprint: str, now: float | None = None) -> None:
        current_time = self._now() if now is None else now
        self._evict_expired(current_time)
        if fingerprint in self._entries:
            self._entries.move_to_end(fingerprint)
        self._entries[fingerprint] = current_time
        if self.max_size is not None:
            while len(self._entries) > self.max_size:
                self._entries.popitem(last=False)

    def __len__(self) -> int:
        return len(self._entries)

    def estimated_memory_bytes(self) -> int:
        return sys.getsizeof(self._entries) + sum(
            sys.getsizeof(fingerprint) + sys.getsizeof(last_seen)
            for fingerprint, last_seen in self._entries.items()
        )

