from __future__ import annotations

import unittest

from src.bloom import BloomFilter


class BloomFilterTests(unittest.TestCase):
    def test_inserted_items_do_not_become_false_negatives(self) -> None:
        bloom = BloomFilter(num_bits=1024, num_hashes=3)
        fingerprints = ["alpha", "beta", "gamma", "delta"]
        for fingerprint in fingerprints:
            bloom.add(fingerprint)
        for fingerprint in fingerprints:
            self.assertTrue(bloom.contains(fingerprint))


if __name__ == "__main__":
    unittest.main()

