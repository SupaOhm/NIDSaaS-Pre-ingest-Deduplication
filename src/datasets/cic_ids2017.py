"""CIC-IDS2017 / CICFlowMeter CSV loading and schema inspection."""

from __future__ import annotations

import csv
from dataclasses import dataclass
import hashlib
import math
from pathlib import Path
import re
from typing import Any, Callable, Sequence, TypeVar

TENANT_MODE_CHOICES = ("single", "round_robin", "hash")
SOURCE_MODE_CHOICES = ("single", "round_robin", "hash")

MATCH_DIRECT = "direct"
MATCH_ALIAS = "alias"
MATCH_FALLBACK = "fallback"

SCHEMA_QUALITY_CLEAN = "clean"
SCHEMA_QUALITY_PARTIAL = "partial"
SCHEMA_QUALITY_FALLBACK_HEAVY = "fallback-heavy"
CIC_CSV_ENCODINGS: tuple[str, ...] = ("utf-8-sig", "utf-8", "latin1", "cp1252")
_ProcessorResult = TypeVar("_ProcessorResult")


@dataclass(frozen=True)
class CICCanonicalColumn:
    canonical_name: str
    aliases: tuple[str, ...]
    required: bool = False


CANONICAL_CIC_COLUMNS: tuple[CICCanonicalColumn, ...] = (
    CICCanonicalColumn("flow_id", ("Flow ID", "FlowID"), required=True),
    CICCanonicalColumn("src_ip", ("Source IP", "Src IP", "SourceIP", "SrcIP"), required=True),
    CICCanonicalColumn("src_port", ("Source Port", "Src Port", "SourcePort", "SrcPort"), required=True),
    CICCanonicalColumn(
        "dst_ip",
        ("Destination IP", "Dst IP", "DestinationIP", "DstIP"),
        required=True,
    ),
    CICCanonicalColumn(
        "dst_port",
        ("Destination Port", "Dst Port", "DestinationPort", "DstPort"),
        required=True,
    ),
    CICCanonicalColumn("protocol", ("Protocol",), required=True),
    CICCanonicalColumn("timestamp", ("Timestamp",), required=True),
    CICCanonicalColumn("label", ("Label",), required=True),
    CICCanonicalColumn("flow_duration", ("Flow Duration",)),
    CICCanonicalColumn("total_fwd_packets", ("Total Fwd Packets", "Total Forward Packets")),
    CICCanonicalColumn("total_bwd_packets", ("Total Backward Packets", "Total Bwd Packets")),
    CICCanonicalColumn(
        "total_length_fwd_packets",
        (
            "Total Length of Fwd Packets",
            "Total Length of Forward Packets",
            "TotLen Fwd Pkts",
            "TotLen Fwd Packets",
        ),
    ),
    CICCanonicalColumn(
        "total_length_bwd_packets",
        (
            "Total Length of Bwd Packets",
            "Total Length of Backward Packets",
            "TotLen Bwd Pkts",
            "TotLen Bwd Packets",
        ),
    ),
)

CORE_CANONICAL_CIC_COLUMNS: tuple[str, ...] = tuple(
    column.canonical_name for column in CANONICAL_CIC_COLUMNS if column.required
)
EXPECTED_CANONICAL_CIC_COLUMNS: tuple[str, ...] = tuple(
    column.canonical_name for column in CANONICAL_CIC_COLUMNS
)


@dataclass(frozen=True)
class CICNormalizedColumn:
    index: int
    original_name: str
    normalized_name: str
    lookup_key: str
    family_normalized_name: str
    family_lookup_key: str


@dataclass(frozen=True)
class CICCanonicalMatch:
    raw_column_name: str
    normalized_column_name: str
    matched_alias: str
    match_mode: str


@dataclass(frozen=True)
class CICHeaderResolution:
    source_file: str
    columns: tuple[CICNormalizedColumn, ...]
    raw_columns: tuple[str, ...]
    normalized_columns: tuple[str, ...]
    original_to_normalized: tuple[tuple[str, str], ...]
    canonical_column_map: dict[str, str]
    canonical_indices: dict[str, int]
    canonical_matches: dict[str, CICCanonicalMatch]
    duplicate_alias_matches: dict[str, tuple[str, ...]]
    repeated_raw_columns: dict[str, tuple[str, ...]]
    detected_canonical_columns: tuple[str, ...]
    missing_canonical_columns: tuple[str, ...]
    missing_required_columns: tuple[str, ...]
    extra_columns: tuple[str, ...]
    looks_like_cicflowmeter: bool
    schema_quality: str

    def get_value(self, row: Sequence[str], canonical_name: str) -> Any:
        index = self.canonical_indices.get(canonical_name)
        if index is None:
            return None
        if index >= len(row):
            return None
        return row[index]


@dataclass(frozen=True)
class CICSchemaInspection:
    source_file: str
    row_count: int
    encoding_used: str
    raw_columns: tuple[str, ...]
    normalized_columns: tuple[str, ...]
    original_to_normalized: tuple[tuple[str, str], ...]
    canonical_column_map: dict[str, str]
    canonical_matches: dict[str, CICCanonicalMatch]
    duplicate_alias_matches: dict[str, tuple[str, ...]]
    repeated_raw_columns: dict[str, tuple[str, ...]]
    detected_canonical_columns: tuple[str, ...]
    missing_canonical_columns: tuple[str, ...]
    missing_required_columns: tuple[str, ...]
    extra_columns: tuple[str, ...]
    looks_like_cicflowmeter: bool
    schema_quality: str


@dataclass(frozen=True)
class CICLoadResult:
    records: list[dict[str, Any]]
    dataset_name: str
    csv_count: int
    total_loaded_records: int
    schema_reports: tuple[CICSchemaInspection, ...]
    source_files_processed: tuple[str, ...]
    source_file_encodings: tuple[str, ...]
    detected_canonical_columns: tuple[str, ...]
    missing_canonical_columns: tuple[str, ...]
    schema_looks_like_cicflowmeter: bool
    schema_quality: str


def normalize_cic_header_name(name: str) -> str:
    return " ".join(str(name or "").strip().split())


def normalize_cic_header_family_name(name: str) -> str:
    normalized = normalize_cic_header_name(name)
    return re.sub(r"\.\d+$", "", normalized)


def cic_header_lookup_key(name: str) -> str:
    normalized = normalize_cic_header_name(name)
    return "".join(character for character in normalized.casefold() if character.isalnum())


def cic_header_family_lookup_key(name: str) -> str:
    normalized = normalize_cic_header_family_name(name)
    return "".join(character for character in normalized.casefold() if character.isalnum())


def canonical_fallback_lookup_key(canonical_name: str) -> str:
    return "".join(character for character in canonical_name.casefold() if character.isalnum())


CANONICAL_CIC_ALIAS_KEYS = {
    column.canonical_name: tuple(cic_header_lookup_key(alias) for alias in column.aliases)
    for column in CANONICAL_CIC_COLUMNS
}

CANONICAL_CIC_ALIAS_NORMALIZED_KEYS = {
    column.canonical_name: tuple(normalize_cic_header_name(alias).casefold() for alias in column.aliases)
    for column in CANONICAL_CIC_COLUMNS
}


def _clean_cell(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    lowered = text.casefold()
    if lowered in {"nan", "inf", "+inf", "-inf", "infinity", "+infinity", "-infinity"}:
        return None
    return text


def _parse_integer(value: Any) -> int | None:
    text = _clean_cell(value)
    if text is None:
        return None
    compact = text.replace(",", "")
    try:
        numeric = float(compact)
    except ValueError:
        return None
    if not math.isfinite(numeric):
        return None
    return int(numeric)


def _normalize_protocol(value: Any) -> str | None:
    text = _clean_cell(value)
    if text is None:
        return None
    numeric = _parse_integer(text)
    if numeric is not None:
        return str(numeric)
    return text


def _determine_schema_quality(
    *,
    missing_required_columns: Sequence[str],
    missing_canonical_columns: Sequence[str],
    canonical_matches: dict[str, CICCanonicalMatch],
    duplicate_alias_matches: dict[str, tuple[str, ...]],
) -> str:
    if missing_required_columns:
        return SCHEMA_QUALITY_FALLBACK_HEAVY
    if any(match.match_mode == MATCH_FALLBACK for match in canonical_matches.values()):
        return SCHEMA_QUALITY_FALLBACK_HEAVY
    if missing_canonical_columns or duplicate_alias_matches:
        return SCHEMA_QUALITY_PARTIAL
    if any(match.match_mode == MATCH_ALIAS for match in canonical_matches.values()):
        return SCHEMA_QUALITY_PARTIAL
    return SCHEMA_QUALITY_CLEAN


def _classify_canonical_match(
    column: CICNormalizedColumn,
    canonical_column: CICCanonicalColumn,
) -> tuple[str, str]:
    normalized_key = column.normalized_name.casefold()
    alias_normalized_keys = CANONICAL_CIC_ALIAS_NORMALIZED_KEYS[canonical_column.canonical_name]
    for alias_index, alias in enumerate(canonical_column.aliases):
        if normalized_key == alias_normalized_keys[alias_index]:
            match_mode = MATCH_DIRECT if alias_index == 0 else MATCH_ALIAS
            return alias, match_mode
    fallback_key = canonical_fallback_lookup_key(canonical_column.canonical_name)
    if column.lookup_key == fallback_key:
        return canonical_column.canonical_name, MATCH_FALLBACK
    primary_alias = canonical_column.aliases[0]
    return primary_alias, MATCH_DIRECT


def _assignment_key(record: dict[str, Any], kind: str) -> str:
    parts = [
        str(record.get("flow_id", "")),
        str(record.get("timestamp", "")),
        str(record.get("src_ip", "")),
        str(record.get("dst_ip", "")),
        str(record.get("src_port", "")),
        str(record.get("dst_port", "")),
        str(record.get("protocol", "")),
        kind,
    ]
    return "|".join(parts)


def _assign_scope_id(
    *,
    kind: str,
    mode: str,
    count: int,
    record_index: int,
    record: dict[str, Any],
) -> str:
    if count <= 0:
        raise ValueError(f"{kind} count must be positive")
    if mode == "single":
        return f"{kind}-1"
    if mode == "round_robin":
        return f"{kind}-{(record_index % count) + 1}"
    if mode == "hash":
        digest = hashlib.sha256(_assignment_key(record, kind).encode("utf-8")).digest()
        return f"{kind}-{(int.from_bytes(digest[:8], 'big') % count) + 1}"
    valid = ", ".join(TENANT_MODE_CHOICES if kind == "tenant" else SOURCE_MODE_CHOICES)
    raise ValueError(f"Unknown {kind} mode '{mode}'. Valid values: {valid}")


def resolve_cic_headers(raw_header: Sequence[str], *, source_file: str) -> CICHeaderResolution:
    columns = tuple(
        CICNormalizedColumn(
            index=index,
            original_name=str(name),
            normalized_name=normalize_cic_header_name(str(name)),
            lookup_key=cic_header_lookup_key(str(name)),
            family_normalized_name=normalize_cic_header_family_name(str(name)),
            family_lookup_key=cic_header_family_lookup_key(str(name)),
        )
        for index, name in enumerate(raw_header)
    )
    by_lookup_key: dict[str, list[CICNormalizedColumn]] = {}
    by_family_lookup_key: dict[str, list[CICNormalizedColumn]] = {}
    for column in columns:
        by_lookup_key.setdefault(column.lookup_key, []).append(column)
        by_family_lookup_key.setdefault(column.family_lookup_key, []).append(column)

    canonical_column_map: dict[str, str] = {}
    canonical_indices: dict[str, int] = {}
    canonical_matches: dict[str, CICCanonicalMatch] = {}
    duplicate_alias_matches: dict[str, tuple[str, ...]] = {}
    matched_indices: set[int] = set()

    for canonical_column in CANONICAL_CIC_COLUMNS:
        matches: list[tuple[CICNormalizedColumn, str, str]] = []
        seen_indices: set[int] = set()
        for alias_index, alias in enumerate(canonical_column.aliases):
            alias_key = CANONICAL_CIC_ALIAS_KEYS[canonical_column.canonical_name][alias_index]
            match_mode = MATCH_DIRECT if alias_index == 0 else MATCH_ALIAS
            for column in by_lookup_key.get(alias_key, []):
                if column.index not in seen_indices:
                    matches.append((column, alias, match_mode))
                    seen_indices.add(column.index)
        if not matches:
            fallback_key = canonical_fallback_lookup_key(canonical_column.canonical_name)
            for column in by_lookup_key.get(fallback_key, []):
                if column.index not in seen_indices:
                    matches.append((column, canonical_column.canonical_name, MATCH_FALLBACK))
                    seen_indices.add(column.index)
        if matches:
            chosen, _, _ = matches[0]
            matched_alias, match_mode = _classify_canonical_match(chosen, canonical_column)
            canonical_column_map[canonical_column.canonical_name] = chosen.original_name
            canonical_indices[canonical_column.canonical_name] = chosen.index
            canonical_matches[canonical_column.canonical_name] = CICCanonicalMatch(
                raw_column_name=chosen.original_name,
                normalized_column_name=chosen.normalized_name,
                matched_alias=matched_alias,
                match_mode=match_mode,
            )
            matched_indices.add(chosen.index)
            if len(matches) > 1:
                duplicate_alias_matches[canonical_column.canonical_name] = tuple(
                    column.original_name for column, _, _ in matches
                )

    repeated_raw_columns = {
        columns_for_family[0].family_normalized_name: tuple(
            column.original_name for column in columns_for_family
        )
        for columns_for_family in by_family_lookup_key.values()
        if len(columns_for_family) > 1
    }

    detected_canonical_columns = tuple(
        column.canonical_name
        for column in CANONICAL_CIC_COLUMNS
        if column.canonical_name in canonical_column_map
    )
    missing_canonical_columns = tuple(
        column.canonical_name
        for column in CANONICAL_CIC_COLUMNS
        if column.canonical_name not in canonical_column_map
    )
    missing_required_columns = tuple(
        column.canonical_name
        for column in CANONICAL_CIC_COLUMNS
        if column.required and column.canonical_name not in canonical_column_map
    )
    extra_columns = tuple(
        column.normalized_name for column in columns if column.index not in matched_indices
    )
    schema_quality = _determine_schema_quality(
        missing_required_columns=missing_required_columns,
        missing_canonical_columns=missing_canonical_columns,
        canonical_matches=canonical_matches,
        duplicate_alias_matches=duplicate_alias_matches,
    )

    return CICHeaderResolution(
        source_file=source_file,
        columns=columns,
        raw_columns=tuple(column.original_name for column in columns),
        normalized_columns=tuple(column.normalized_name for column in columns),
        original_to_normalized=tuple(
            (column.original_name, column.normalized_name) for column in columns
        ),
        canonical_column_map=canonical_column_map,
        canonical_indices=canonical_indices,
        canonical_matches=canonical_matches,
        duplicate_alias_matches=duplicate_alias_matches,
        repeated_raw_columns=repeated_raw_columns,
        detected_canonical_columns=detected_canonical_columns,
        missing_canonical_columns=missing_canonical_columns,
        missing_required_columns=missing_required_columns,
        extra_columns=extra_columns,
        looks_like_cicflowmeter=not missing_required_columns,
        schema_quality=schema_quality,
    )


def _map_cic_row_to_record(
    row: Sequence[str],
    *,
    header: CICHeaderResolution,
    csv_file: Path,
    row_number: int,
) -> dict[str, Any]:
    total_fwd_packets = _parse_integer(header.get_value(row, "total_fwd_packets"))
    total_bwd_packets = _parse_integer(header.get_value(row, "total_bwd_packets"))
    total_length_fwd_packets = _parse_integer(header.get_value(row, "total_length_fwd_packets"))
    total_length_bwd_packets = _parse_integer(header.get_value(row, "total_length_bwd_packets"))

    packet_values = [value for value in (total_fwd_packets, total_bwd_packets) if value is not None]
    packets = sum(packet_values) if packet_values else None
    byte_values = [
        value
        for value in (total_length_fwd_packets, total_length_bwd_packets)
        if value is not None
    ]
    bytes_count = sum(byte_values) if byte_values else None
    label = _clean_cell(header.get_value(row, "label")) or "unlabeled"

    return {
        "tenant_id": "tenant-1",
        "source_id": "source-1",
        "timestamp": _clean_cell(header.get_value(row, "timestamp")),
        "src_ip": _clean_cell(header.get_value(row, "src_ip")),
        "dst_ip": _clean_cell(header.get_value(row, "dst_ip")),
        "src_port": _parse_integer(header.get_value(row, "src_port")),
        "dst_port": _parse_integer(header.get_value(row, "dst_port")),
        "protocol": _normalize_protocol(header.get_value(row, "protocol")),
        "flow_id": _clean_cell(header.get_value(row, "flow_id")),
        "flow_duration": _parse_integer(header.get_value(row, "flow_duration")),
        "total_fwd_packets": total_fwd_packets,
        "total_bwd_packets": total_bwd_packets,
        "total_length_fwd_packets": total_length_fwd_packets,
        "total_length_bwd_packets": total_length_bwd_packets,
        "label": label,
        "action": "flow",
        "bytes": bytes_count,
        "packets": packets,
        "event_type": label,
        "dataset_name": "cic_ids2017",
        "csv_file": csv_file.name,
        "row_number": row_number,
    }


def _build_schema_report(
    *,
    csv_path: Path,
    header: CICHeaderResolution,
    row_count: int,
    encoding_used: str,
) -> CICSchemaInspection:
    return CICSchemaInspection(
        source_file=csv_path.name,
        row_count=row_count,
        encoding_used=encoding_used,
        raw_columns=header.raw_columns,
        normalized_columns=header.normalized_columns,
        original_to_normalized=header.original_to_normalized,
        canonical_column_map=header.canonical_column_map,
        canonical_matches=header.canonical_matches,
        duplicate_alias_matches=header.duplicate_alias_matches,
        repeated_raw_columns=header.repeated_raw_columns,
        detected_canonical_columns=header.detected_canonical_columns,
        missing_canonical_columns=header.missing_canonical_columns,
        missing_required_columns=header.missing_required_columns,
        extra_columns=header.extra_columns,
        looks_like_cicflowmeter=header.looks_like_cicflowmeter,
        schema_quality=header.schema_quality,
    )


def _process_csv_with_fallback(
    csv_path: Path,
    *,
    progress: Any | None,
    processor: Callable[[csv.reader, str], _ProcessorResult],
) -> _ProcessorResult:
    last_error: UnicodeDecodeError | None = None
    for index, encoding in enumerate(CIC_CSV_ENCODINGS):
        try:
            with csv_path.open("r", encoding=encoding, newline="") as handle:
                reader = csv.reader(handle)
                return processor(reader, encoding)
        except UnicodeDecodeError as exc:
            last_error = exc
            next_encoding = CIC_CSV_ENCODINGS[index + 1] if index + 1 < len(CIC_CSV_ENCODINGS) else None
            if progress is not None and next_encoding is not None:
                progress.load_retry(failed_encoding=encoding, next_encoding=next_encoding)
    if last_error is not None:
        raise last_error
    raise ValueError(f"Unable to load {csv_path} with the supported encodings.")


def inspect_cic_csvs(csv_paths: Sequence[str | Path]) -> tuple[CICSchemaInspection, ...]:
    if not csv_paths:
        raise ValueError("At least one CIC-IDS2017 CSV path must be provided.")

    reports: list[CICSchemaInspection] = []
    for csv_path in [Path(path) for path in csv_paths]:
        reports.append(
            _process_csv_with_fallback(
                csv_path,
                progress=None,
                processor=lambda reader, encoding_used: _inspect_single_cic_csv(
                    csv_path,
                    reader,
                    encoding_used=encoding_used,
                ),
            )
        )
    return tuple(reports)


def _inspect_single_cic_csv(
    csv_path: Path,
    reader: csv.reader,
    *,
    encoding_used: str,
) -> CICSchemaInspection:
    try:
        raw_header = next(reader)
    except StopIteration as exc:
        raise ValueError(f"{csv_path} is empty and cannot be inspected.") from exc
    header = resolve_cic_headers(raw_header, source_file=csv_path.name)
    row_count = sum(1 for _ in reader)
    return _build_schema_report(
        csv_path=csv_path,
        header=header,
        row_count=row_count,
        encoding_used=encoding_used,
    )


def _combine_detected_columns(reports: Sequence[CICSchemaInspection]) -> tuple[str, ...]:
    if not reports:
        return ()
    common = set(reports[0].detected_canonical_columns)
    for report in reports[1:]:
        common &= set(report.detected_canonical_columns)
    return tuple(
        column.canonical_name
        for column in CANONICAL_CIC_COLUMNS
        if column.canonical_name in common
    )


def _combine_missing_columns(reports: Sequence[CICSchemaInspection]) -> tuple[str, ...]:
    missing_union = set()
    for report in reports:
        missing_union.update(report.missing_canonical_columns)
    return tuple(
        column.canonical_name
        for column in CANONICAL_CIC_COLUMNS
        if column.canonical_name in missing_union
    )


def _combine_schema_quality(reports: Sequence[CICSchemaInspection]) -> str:
    if not reports:
        return SCHEMA_QUALITY_PARTIAL
    if any(report.schema_quality == SCHEMA_QUALITY_FALLBACK_HEAVY for report in reports):
        return SCHEMA_QUALITY_FALLBACK_HEAVY
    if any(report.schema_quality == SCHEMA_QUALITY_PARTIAL for report in reports):
        return SCHEMA_QUALITY_PARTIAL
    return SCHEMA_QUALITY_CLEAN


def _combine_source_file_encodings(reports: Sequence[CICSchemaInspection]) -> tuple[str, ...]:
    return tuple(f"{report.source_file}:{report.encoding_used}" for report in reports)


def _apply_scope_simulation(
    records: list[dict[str, Any]],
    *,
    tenant_mode: str,
    num_tenants: int,
    source_mode: str,
    num_sources: int,
) -> list[dict[str, Any]]:
    if tenant_mode not in TENANT_MODE_CHOICES:
        valid = ", ".join(TENANT_MODE_CHOICES)
        raise ValueError(f"Unknown tenant mode '{tenant_mode}'. Valid values: {valid}")
    if source_mode not in SOURCE_MODE_CHOICES:
        valid = ", ".join(SOURCE_MODE_CHOICES)
        raise ValueError(f"Unknown source mode '{source_mode}'. Valid values: {valid}")

    scoped_records: list[dict[str, Any]] = []
    for record_index, record in enumerate(records):
        scoped = dict(record)
        scoped["tenant_id"] = _assign_scope_id(
            kind="tenant",
            mode=tenant_mode,
            count=num_tenants,
            record_index=record_index,
            record=record,
        )
        scoped["source_id"] = _assign_scope_id(
            kind="source",
            mode=source_mode,
            count=num_sources,
            record_index=record_index,
            record=record,
        )
        scoped_records.append(scoped)
    return scoped_records


def load_cic_ids2017_csvs(
    csv_paths: Sequence[str | Path],
    *,
    row_limit: int | None = None,
    tenant_mode: str = "single",
    num_tenants: int = 1,
    source_mode: str = "single",
    num_sources: int = 1,
    progress: Any | None = None,
) -> CICLoadResult:
    if not csv_paths:
        raise ValueError("At least one CIC-IDS2017 CSV path must be provided.")

    records: list[dict[str, Any]] = []
    reports: list[CICSchemaInspection] = []

    for csv_path in [Path(path) for path in csv_paths]:
        if progress is not None:
            progress.load_start(str(csv_path))
        remaining_limit = None if row_limit is None else row_limit - len(records)
        file_records, report, reached_limit = _process_csv_with_fallback(
            csv_path,
            progress=progress,
            processor=lambda reader, encoding_used: _load_single_cic_csv(
                csv_path,
                reader,
                encoding_used=encoding_used,
                remaining_limit=remaining_limit,
            ),
        )
        records.extend(file_records)
        reports.append(report)
        if progress is not None:
            progress.load_file_summary(report)
        if reached_limit:
            scoped_records = _apply_scope_simulation(
                records,
                tenant_mode=tenant_mode,
                num_tenants=num_tenants,
                source_mode=source_mode,
                num_sources=num_sources,
            )
            return CICLoadResult(
                records=scoped_records,
                dataset_name="cic_ids2017",
                csv_count=len(csv_paths),
                total_loaded_records=len(scoped_records),
                schema_reports=tuple(reports),
                source_files_processed=tuple(report.source_file for report in reports),
                source_file_encodings=_combine_source_file_encodings(reports),
                detected_canonical_columns=_combine_detected_columns(reports),
                missing_canonical_columns=_combine_missing_columns(reports),
                schema_looks_like_cicflowmeter=all(
                    report.looks_like_cicflowmeter for report in reports
                ),
                schema_quality=_combine_schema_quality(reports),
            )

    scoped_records = _apply_scope_simulation(
        records,
        tenant_mode=tenant_mode,
        num_tenants=num_tenants,
        source_mode=source_mode,
        num_sources=num_sources,
    )
    return CICLoadResult(
        records=scoped_records,
        dataset_name="cic_ids2017",
        csv_count=len(csv_paths),
        total_loaded_records=len(scoped_records),
        schema_reports=tuple(reports),
        source_files_processed=tuple(report.source_file for report in reports),
        source_file_encodings=_combine_source_file_encodings(reports),
        detected_canonical_columns=_combine_detected_columns(reports),
        missing_canonical_columns=_combine_missing_columns(reports),
        schema_looks_like_cicflowmeter=all(report.looks_like_cicflowmeter for report in reports),
        schema_quality=_combine_schema_quality(reports),
    )


def _load_single_cic_csv(
    csv_path: Path,
    reader: csv.reader,
    *,
    encoding_used: str,
    remaining_limit: int | None,
) -> tuple[list[dict[str, Any]], CICSchemaInspection, bool]:
    try:
        raw_header = next(reader)
    except StopIteration as exc:
        raise ValueError(f"{csv_path} is empty and cannot be loaded.") from exc

    header = resolve_cic_headers(raw_header, source_file=csv_path.name)
    if header.missing_required_columns:
        missing = ", ".join(header.missing_required_columns)
        raise ValueError(
            f"{csv_path.name} does not match the required CICFlowMeter core schema. "
            f"Missing required columns: {missing}"
        )

    file_records: list[dict[str, Any]] = []
    row_count = 0
    for row_number, row in enumerate(reader, start=1):
        row_count += 1
        file_records.append(
            _map_cic_row_to_record(
                row,
                header=header,
                csv_file=csv_path,
                row_number=row_number,
            )
        )
        if remaining_limit is not None and len(file_records) >= remaining_limit:
            return (
                file_records,
                _build_schema_report(
                    csv_path=csv_path,
                    header=header,
                    row_count=row_count,
                    encoding_used=encoding_used,
                ),
                True,
            )
    return (
        file_records,
        _build_schema_report(
            csv_path=csv_path,
            header=header,
            row_count=row_count,
            encoding_used=encoding_used,
        ),
        False,
    )
