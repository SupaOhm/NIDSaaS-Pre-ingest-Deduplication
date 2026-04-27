"""Deterministic fingerprint generation for deduplication."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from typing import Iterable

from .canonicalize import DEFAULT_FINGERPRINT_FIELDS, canonical_json


@dataclass(frozen=True)
class FingerprintConfig:
    fields: tuple[str, ...] = DEFAULT_FINGERPRINT_FIELDS
    tenant_aware: bool = True
    hash_name: str = "sha256"


def build_fingerprint(record: dict, config: FingerprintConfig | None = None) -> str:
    active_config = config or FingerprintConfig()
    payload = canonical_json(
        record,
        fingerprint_fields=active_config.fields,
        tenant_aware=active_config.tenant_aware,
    )
    digest = hashlib.new(active_config.hash_name)
    digest.update(payload.encode("utf-8"))
    return digest.hexdigest()


def parse_fingerprint_fields(text: str | None) -> tuple[str, ...]:
    if not text:
        return DEFAULT_FINGERPRINT_FIELDS
    return tuple(field.strip() for field in text.split(",") if field.strip())

