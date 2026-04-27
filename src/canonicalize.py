"""Canonicalization utilities for stable per-record fingerprinting."""

from __future__ import annotations

from datetime import datetime
import json
from typing import Any, Iterable

DEFAULT_FINGERPRINT_FIELDS: tuple[str, ...] = (
    "source_id",
    "timestamp",
    "src_ip",
    "dst_ip",
    "src_port",
    "dst_port",
    "protocol",
    "flow_duration",
    "action",
    "bytes",
    "packets",
    "event_type",
)

# For CICFlowMeter-style rows, prefer the explicit flow-identity fields plus a
# small stable set of original flow statistics. We intentionally exclude Flow ID
# because it is a redundant formatted string derived from the 5-tuple and can
# vary between exporters, while the component identity fields remain stable.
CIC_FINGERPRINT_FIELDS: tuple[str, ...] = (
    "source_id",
    "timestamp",
    "src_ip",
    "dst_ip",
    "src_port",
    "dst_port",
    "protocol",
    "flow_duration",
    "total_fwd_packets",
    "total_bwd_packets",
    "total_length_fwd_packets",
    "total_length_bwd_packets",
    "label",
)

INTEGER_FIELDS = {
    "src_port",
    "dst_port",
    "bytes",
    "packets",
    "flow_duration",
    "total_fwd_packets",
    "total_bwd_packets",
    "total_length_fwd_packets",
    "total_length_bwd_packets",
}
LOWERCASE_FIELDS = {
    "tenant_id",
    "source_id",
    "src_ip",
    "dst_ip",
    "protocol",
    "action",
    "event_type",
    "flow_id",
    "label",
}


def _normalize_timestamp(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    candidate = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return text
    return parsed.isoformat()


def _normalize_integer(value: Any) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value == float("inf") or value == float("-inf"):
            return None
        return int(value)
    text = str(value).strip()
    if not text:
        return None
    lowered = text.casefold()
    if lowered in {"inf", "+inf", "-inf", "infinity", "+infinity", "-infinity", "nan"}:
        return None
    return int(float(text))


def normalize_value(field: str, value: Any) -> Any:
    """Normalize field values so semantically identical records hash the same way."""
    if field == "timestamp":
        return _normalize_timestamp(value)
    if field in INTEGER_FIELDS:
        return _normalize_integer(value)
    if value is None:
        return None
    if isinstance(value, str):
        normalized = value.strip()
        if field in LOWERCASE_FIELDS:
            normalized = normalized.lower()
        return normalized
    return value


def resolve_fingerprint_fields(
    fingerprint_fields: Iterable[str] | None = None,
    tenant_aware: bool = True,
) -> tuple[str, ...]:
    fields = list(fingerprint_fields or DEFAULT_FINGERPRINT_FIELDS)
    if tenant_aware:
        if "tenant_id" not in fields:
            fields.insert(0, "tenant_id")
    else:
        fields = [field for field in fields if field != "tenant_id"]
    return tuple(fields)


def canonicalize_record(
    record: dict[str, Any],
    fingerprint_fields: Iterable[str] | None = None,
    tenant_aware: bool = True,
) -> dict[str, Any]:
    """Return a canonical dict containing only the configured stable fields."""
    resolved_fields = resolve_fingerprint_fields(fingerprint_fields, tenant_aware=tenant_aware)
    canonical: dict[str, Any] = {}
    for field in resolved_fields:
        canonical[field] = normalize_value(field, record.get(field))
    return canonical


def canonical_json(
    record: dict[str, Any],
    fingerprint_fields: Iterable[str] | None = None,
    tenant_aware: bool = True,
) -> str:
    canonical = canonicalize_record(
        record,
        fingerprint_fields=fingerprint_fields,
        tenant_aware=tenant_aware,
    )
    return json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def serialize_record(record: dict[str, Any]) -> str:
    """Serialize the full record in a stable form for byte counting."""
    return json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def record_size_bytes(record: dict[str, Any]) -> int:
    return len(serialize_record(record).encode("utf-8"))
