"""Simple configurable Bloom filter for the optional edge-side record-dedup variant."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import math


@dataclass
class BloomConfig:
    num_bits: int
    num_hashes: int
    capacity_estimate: int | None = None


class BloomFilter:
    def __init__(self, num_bits: int, num_hashes: int, capacity_estimate: int | None = None) -> None:
        if num_bits <= 0:
            raise ValueError("num_bits must be positive")
        if num_hashes <= 0:
            raise ValueError("num_hashes must be positive")
        self.num_bits = num_bits
        self.num_hashes = num_hashes
        self.capacity_estimate = capacity_estimate
        self._bits = bytearray((num_bits + 7) // 8)

    def _hash_positions(self, item: str) -> list[int]:
        encoded = item.encode("utf-8")
        first = int.from_bytes(hashlib.sha256(encoded).digest(), "big")
        second = int.from_bytes(hashlib.blake2b(encoded, digest_size=16).digest(), "big") or 1
        return [((first + index * second) % self.num_bits) for index in range(self.num_hashes)]

    def add(self, item: str) -> None:
        for position in self._hash_positions(item):
            byte_index = position // 8
            bit_index = position % 8
            self._bits[byte_index] |= 1 << bit_index

    def contains(self, item: str) -> bool:
        for position in self._hash_positions(item):
            byte_index = position // 8
            bit_index = position % 8
            if not (self._bits[byte_index] & (1 << bit_index)):
                return False
        return True

    def __contains__(self, item: str) -> bool:
        return self.contains(item)

    @property
    def byte_size(self) -> int:
        return len(self._bits)

    def estimated_false_positive_rate(self, inserted_items: int) -> float:
        if inserted_items <= 0:
            return 0.0
        exponent = -self.num_hashes * inserted_items / self.num_bits
        return (1 - math.exp(exponent)) ** self.num_hashes
