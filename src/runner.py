"""Command-line runner for the edge-side communication-cost experiment."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import shutil
from typing import Any, Sequence

from .canonicalize import CIC_FINGERPRINT_FIELDS
from .datasets.cic_ids2017 import (
    CANONICAL_CIC_COLUMNS,
    SOURCE_MODE_CHOICES,
    TENANT_MODE_CHOICES,
    inspect_cic_csvs,
    load_cic_ids2017_csvs,
)
from .edge_batches import EDGE_BATCH_ORDER_CHOICES, build_edge_batches
from .edge_dedupe import (
    EDGE_MODE_CHOICES,
    EdgeBloomVariantConfig,
    EdgeBloomVariantRunner,
    EdgeMetadataRunner,
    NoEdgeDedupRunner,
)
from .io_utils import load_jsonl_records, write_csv, write_json
from .injection import PAPER_SCENARIO_CHOICES, SCENARIO_CHOICES, inject_upload_behavior
from .metrics import ComparisonResult, ExperimentResult, build_comparison
from .progress import ProgressPrinter
from .workload import PreparedWorkload, WorkloadMetadata, freeze_edge_batches


DEFAULT_COMPARE_MODES = ("no_edge_dedup", "edge_metadata")


@dataclass(frozen=True)
class ExperimentPreset:
    dataset: str | None = None
    csv_paths: tuple[str, ...] = ()
    scenarios: tuple[str, ...] = ()
    dupe_rates: tuple[float, ...] = ()
    compare_modes: tuple[str, str] | None = None
    mode: str | None = None
    batch_size: int | None = None
    tenant_mode: str | None = None
    num_tenants: int | None = None
    source_mode: str | None = None
    num_sources: int | None = None
    seed: int | None = None


PRESET_DEFINITIONS: dict[str, ExperimentPreset] = {
    "main_test": ExperimentPreset(
        dataset="cic_ids2017",
        scenarios=("exact_replay", "partial_overlap", "mixed_upload"),
        dupe_rates=(0.0, 0.25, 0.5, 0.75, 1.0),
        compare_modes=DEFAULT_COMPARE_MODES,
        batch_size=5000,
        tenant_mode="single",
        num_tenants=1,
        source_mode="single",
        num_sources=1,
        seed=42,
    ),
    "quick_test": ExperimentPreset(
        dataset="cic_ids2017",
        csv_paths=("sample_data/cic_ids2017_sample.csv",),
        scenarios=("exact_replay",),
        dupe_rates=(0.5,),
        compare_modes=DEFAULT_COMPARE_MODES,
        batch_size=3,
        tenant_mode="single",
        num_tenants=1,
        source_mode="single",
        num_sources=1,
        seed=42,
    ),
    "tenant_test": ExperimentPreset(
        dataset="cic_ids2017",
        scenarios=("mixed_upload",),
        dupe_rates=(0.5,),
        compare_modes=DEFAULT_COMPARE_MODES,
        batch_size=5000,
        tenant_mode="round_robin",
        num_tenants=4,
        source_mode="hash",
        num_sources=4,
        seed=42,
    ),
}


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the client-side edge deduplication communication-cost experiment."
    )
    execution_group = parser.add_mutually_exclusive_group()
    parser.add_argument(
        "-P",
        "--preset",
        choices=tuple(PRESET_DEFINITIONS),
        help="Optional preset that supplies experiment defaults. Explicit CLI flags override preset values.",
    )
    parser.add_argument(
        "-d",
        "--dataset",
        choices=("cic_ids2017", "jsonl"),
        default=None,
        help="Base telemetry workload. Default: cic_ids2017.",
    )
    parser.add_argument(
        "--inspect-cic",
        action="store_true",
        help="Inspect one or more CICFlowMeter-style CSV files and exit.",
    )
    parser.add_argument(
        "-s",
        "--scenario",
        action="append",
        choices=SCENARIO_CHOICES,
        default=None,
        help="Upload behavior scenario. Repeat the flag to run multiple scenarios. Default: new_append.",
    )
    parser.add_argument(
        "--run-all",
        action="store_true",
        help="Run the four paper-facing scenarios: new_append, exact_replay, partial_overlap, mixed_upload.",
    )
    execution_group.add_argument(
        "-m",
        "--mode",
        choices=EDGE_MODE_CHOICES,
        help=(
            "Run a single mode. If omitted, the runner executes both "
            "no_edge_dedup and edge_metadata for comparison."
        ),
    )
    execution_group.add_argument(
        "-cm",
        "--compare-modes",
        nargs=2,
        metavar=("BASELINE_MODE", "PROPOSED_MODE"),
        choices=EDGE_MODE_CHOICES,
        help="Run the listed two modes against the exact same candidate batches and export a comparison.",
    )
    parser.add_argument(
        "-c",
        "--csv",
        action="extend",
        nargs="+",
        default=[],
        help="Path to one or more CIC-IDS2017 CSV files. Repeat the flag to add more files.",
    )
    parser.add_argument(
        "--input-jsonl",
        help="Optional JSON-lines file of records for non-CIC local experiments.",
    )
    parser.add_argument(
        "--row-limit",
        type=int,
        default=None,
        help="Optional global row limit when loading CIC CSV data.",
    )
    parser.add_argument(
        "-b",
        "--batch-size",
        type=int,
        default=None,
        help="Base source-local upload batch size in records. Default: 5000.",
    )
    parser.add_argument(
        "--batch-order",
        choices=EDGE_BATCH_ORDER_CHOICES,
        default="sequential",
        help="Batch emission order after source-local construction. Default: sequential.",
    )
    parser.add_argument(
        "-S",
        "--seed",
        type=int,
        default=None,
        help="Deterministic seed used for shuffled emission and replay selection. Default: 42.",
    )
    parser.add_argument(
        "-r",
        "--dupe-rate",
        action="append",
        type=float,
        default=None,
        help="Duplicate-pressure control parameter in [0.0, 1.0]. Repeat the flag to run multiple rates. Default: 0.0.",
    )
    parser.add_argument(
        "-tm",
        "--tenant-mode",
        choices=TENANT_MODE_CHOICES,
        default=None,
        help="How tenant_id values are simulated for CIC-backed workloads. Default: single.",
    )
    parser.add_argument(
        "-nt",
        "--num-tenants",
        type=int,
        default=None,
        help="Number of simulated tenants when tenant mode is not single. Default: 1.",
    )
    parser.add_argument(
        "-sm",
        "--source-mode",
        choices=SOURCE_MODE_CHOICES,
        default=None,
        help="How source_id values are simulated for CIC-backed workloads. Default: single.",
    )
    parser.add_argument(
        "-ns",
        "--num-sources",
        type=int,
        default=None,
        help="Number of simulated sources when source mode is not single. Default: 1.",
    )
    parser.add_argument(
        "--tenant-aware",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Only used by edge_bloom_variant. Default: enabled.",
    )
    parser.add_argument("--bloom-bits", type=int, default=100_000, help="Bloom filter bit count.")
    parser.add_argument("--bloom-hashes", type=int, default=4, help="Bloom filter hash count.")
    parser.add_argument(
        "--hash-name",
        default="sha256",
        help="Hash function used by the optional Bloom-exact variant. Default: sha256.",
    )
    parser.add_argument(
        "--exact-max-size",
        type=int,
        default=None,
        help="Optional max size for the exact-store used by the Bloom variant.",
    )
    parser.add_argument(
        "--exact-ttl-seconds",
        type=float,
        default=None,
        help="Optional TTL in seconds for exact-store entries in the Bloom variant.",
    )
    parser.add_argument(
        "-o",
        "--output-prefix",
        default="results/edge_experiment",
        help=(
            "Output file prefix for normal CSV exports. "
            "Preset runs use this path as the output directory."
        ),
    )
    parser.add_argument(
        "--export-json",
        action="store_true",
        help="Also write optional JSON exports for debugging. CSV remains the default.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace existing output files or preset output directories cleanly.",
    )
    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print extra process detail during dataset loading and execution.",
    )
    verbosity.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Only print final export paths and summary.",
    )
    parser.add_argument(
        "-p",
        "--progress-every",
        type=int,
        default=25,
        help="Batch progress print interval. Default: 25.",
    )
    return parser


def resolve_cli_args(args: argparse.Namespace) -> argparse.Namespace:
    resolved = argparse.Namespace(**vars(args))
    preset = PRESET_DEFINITIONS.get(resolved.preset) if resolved.preset else None

    resolved.csv = list(resolved.csv or [])
    if not resolved.csv and preset is not None and preset.csv_paths:
        resolved.csv = list(preset.csv_paths)

    if resolved.dataset is None:
        resolved.dataset = preset.dataset if preset and preset.dataset is not None else "cic_ids2017"
    if resolved.batch_size is None:
        resolved.batch_size = preset.batch_size if preset and preset.batch_size is not None else 5000
    if resolved.seed is None:
        resolved.seed = preset.seed if preset and preset.seed is not None else 42
    if resolved.tenant_mode is None:
        resolved.tenant_mode = preset.tenant_mode if preset and preset.tenant_mode is not None else "single"
    if resolved.num_tenants is None:
        resolved.num_tenants = preset.num_tenants if preset and preset.num_tenants is not None else 1
    if resolved.source_mode is None:
        resolved.source_mode = preset.source_mode if preset and preset.source_mode is not None else "single"
    if resolved.num_sources is None:
        resolved.num_sources = preset.num_sources if preset and preset.num_sources is not None else 1

    if resolved.scenario:
        resolved.scenario = list(resolved.scenario)
    elif resolved.run_all:
        resolved.scenario = list(PAPER_SCENARIO_CHOICES)
    elif preset is not None and preset.scenarios:
        resolved.scenario = list(preset.scenarios)
    else:
        resolved.scenario = ["new_append"]

    if resolved.dupe_rate:
        resolved.dupe_rate = list(resolved.dupe_rate)
    elif preset is not None and preset.dupe_rates:
        resolved.dupe_rate = list(preset.dupe_rates)
    else:
        resolved.dupe_rate = [0.0]

    if resolved.mode is None and resolved.compare_modes is None:
        if preset is not None and preset.compare_modes is not None:
            resolved.compare_modes = list(preset.compare_modes)
        elif preset is not None and preset.mode is not None:
            resolved.mode = preset.mode

    return resolved


def _normalize_custom_record(record: dict[str, Any], record_index: int) -> dict[str, Any]:
    normalized = dict(record)
    normalized.setdefault("tenant_id", "tenant-1")
    normalized.setdefault("source_id", "source-1")
    normalized.setdefault("file_epoch", "epoch-1")
    normalized.setdefault("row_number", record_index + 1)
    normalized.setdefault("dataset_name", "jsonl")
    return normalized


def _load_base_records(
    args: argparse.Namespace,
    *,
    progress: ProgressPrinter | None = None,
) -> tuple[list[dict[str, Any]], WorkloadMetadata]:
    if args.dataset == "cic_ids2017":
        if not args.csv:
            raise ValueError("Provide at least one --csv file when using --dataset cic_ids2017.")
        load_result = load_cic_ids2017_csvs(
            args.csv,
            row_limit=args.row_limit,
            tenant_mode=args.tenant_mode,
            num_tenants=args.num_tenants,
            source_mode=args.source_mode,
            num_sources=args.num_sources,
            progress=progress,
        )
        if progress is not None:
            progress.load_total(load_result.total_loaded_records)
        metadata = WorkloadMetadata(
            preset_name=args.preset or "",
            dataset_name=load_result.dataset_name,
            csv_count=load_result.csv_count,
            total_loaded_records=load_result.total_loaded_records,
            source_files_processed=load_result.source_files_processed,
            source_file_encodings=load_result.source_file_encodings,
            detected_cic_columns=load_result.detected_canonical_columns,
            missing_cic_columns=load_result.missing_canonical_columns,
            schema_looks_like_cicflowmeter=load_result.schema_looks_like_cicflowmeter,
            cic_schema_quality=load_result.schema_quality,
            file_epoch_mode="per_csv",
            offset_unit="record_index",
            batch_size=args.batch_size,
            batch_order=args.batch_order,
            tenant_mode=args.tenant_mode,
            num_tenants=args.num_tenants,
            source_mode=args.source_mode,
            num_sources=args.num_sources,
            seed=args.seed,
        )
        return load_result.records, metadata

    if not args.input_jsonl:
        raise ValueError("Provide --input-jsonl when using --dataset jsonl.")
    if progress is not None:
        progress.load_start(str(args.input_jsonl))
    records = [
        _normalize_custom_record(record, index)
        for index, record in enumerate(load_jsonl_records(args.input_jsonl))
    ]
    if progress is not None:
        progress.load_total(len(records))
    metadata = WorkloadMetadata(
        preset_name=args.preset or "",
        dataset_name="jsonl",
        csv_count=0,
        total_loaded_records=len(records),
        source_files_processed=(Path(args.input_jsonl).name,),
        source_file_encodings=(),
        file_epoch_mode="record_field_or_default",
        offset_unit="record_index",
        batch_size=args.batch_size,
        batch_order=args.batch_order,
        tenant_mode="record_supplied",
        num_tenants=len({record["tenant_id"] for record in records}) or 1,
        source_mode="record_supplied",
        num_sources=len({record["source_id"] for record in records}) or 1,
        seed=args.seed,
    )
    return records, metadata


def _resolve_scenarios(args: argparse.Namespace) -> list[str]:
    return list(args.scenario)


def _resolve_dupe_rates(args: argparse.Namespace) -> list[float]:
    return list(args.dupe_rate)


def resolve_workloads(
    args: argparse.Namespace,
    *,
    progress: ProgressPrinter | None = None,
) -> list[PreparedWorkload]:
    records, base_metadata = _load_base_records(args, progress=progress)
    base_batches = build_edge_batches(
        records,
        batch_size=args.batch_size,
        batch_order=args.batch_order,
        seed=args.seed,
    )
    if progress is not None:
        progress.batch_summary(
            records=len(records),
            batch_size=args.batch_size,
            base_batches=len(base_batches),
        )
    workloads: list[PreparedWorkload] = []
    for scenario_name in _resolve_scenarios(args):
        for dupe_rate in _resolve_dupe_rates(args):
            if progress is not None:
                progress.run_set(scenario=scenario_name, dupe_rate=dupe_rate)
                progress.inject_start(
                    scenario=scenario_name,
                    requested_dupe_rate=dupe_rate,
                )
            injected = inject_upload_behavior(
                base_batches,
                scenario=scenario_name,
                dupe_rate=dupe_rate,
                seed=args.seed,
            )
            if progress is not None:
                progress.inject_summary(injected)
            metadata = WorkloadMetadata(
                preset_name=base_metadata.preset_name,
                dataset_name=base_metadata.dataset_name,
                csv_count=base_metadata.csv_count,
                total_loaded_records=base_metadata.total_loaded_records,
                source_files_processed=base_metadata.source_files_processed,
                source_file_encodings=base_metadata.source_file_encodings,
                detected_cic_columns=base_metadata.detected_cic_columns,
                missing_cic_columns=base_metadata.missing_cic_columns,
                schema_looks_like_cicflowmeter=base_metadata.schema_looks_like_cicflowmeter,
                cic_schema_quality=base_metadata.cic_schema_quality,
                file_epoch_mode=base_metadata.file_epoch_mode,
                offset_unit=base_metadata.offset_unit,
                batch_size=args.batch_size,
                batch_order=args.batch_order,
                scenario_family=scenario_name,
                requested_dupe_rate=dupe_rate,
                realized_dupe_rate=injected.realized_dupe_rate,
                injected_duplicate_records=injected.injected_duplicate_records,
                tenant_mode=base_metadata.tenant_mode,
                num_tenants=base_metadata.num_tenants,
                source_mode=base_metadata.source_mode,
                num_sources=base_metadata.num_sources,
                seed=args.seed,
            )
            workloads.append(
                PreparedWorkload(
                    name=scenario_name,
                    description=injected.scenario_description,
                    batches=freeze_edge_batches(injected.batches),
                    metadata=metadata,
                )
            )
    return workloads


def run_mode(
    workload: PreparedWorkload,
    *,
    mode: str,
    tenant_aware: bool,
    bloom_bits: int,
    bloom_hashes: int,
    hash_name: str,
    exact_max_size: int | None,
    exact_ttl_seconds: float | None,
    progress: ProgressPrinter | None,
) -> ExperimentResult:
    if mode == "no_edge_dedup":
        return NoEdgeDedupRunner().run(
            workload.name,
            workload.clone_batches(),
            workload_metadata=workload.metadata,
            progress=progress,
        )
    if mode == "edge_metadata":
        return EdgeMetadataRunner().run(
            workload.name,
            workload.clone_batches(),
            workload_metadata=workload.metadata,
            progress=progress,
        )
    config = EdgeBloomVariantConfig(
        bloom_bits=bloom_bits,
        bloom_hashes=bloom_hashes,
        fingerprint_fields=CIC_FINGERPRINT_FIELDS,
        tenant_aware=tenant_aware,
        hash_name=hash_name,
        exact_max_size=exact_max_size,
        exact_ttl_seconds=exact_ttl_seconds,
    )
    return EdgeBloomVariantRunner(config).run(
        workload.name,
        workload.clone_batches(),
        workload_metadata=workload.metadata,
        progress=progress,
    )


def execute_workloads(
    workloads: Sequence[PreparedWorkload],
    *,
    mode: str | None,
    compare_modes: tuple[str, str] | None,
    tenant_aware: bool,
    bloom_bits: int,
    bloom_hashes: int,
    hash_name: str,
    exact_max_size: int | None,
    exact_ttl_seconds: float | None,
    progress: ProgressPrinter | None = None,
) -> tuple[list[ExperimentResult], list[ComparisonResult]]:
    results: list[ExperimentResult] = []
    comparisons: list[ComparisonResult] = []
    if compare_modes is not None:
        baseline_mode, proposed_mode = compare_modes
        for workload in workloads:
            if progress is not None:
                progress.compare_modes(compare_modes)
            baseline = run_mode(
                workload,
                mode=baseline_mode,
                tenant_aware=tenant_aware,
                bloom_bits=bloom_bits,
                bloom_hashes=bloom_hashes,
                hash_name=hash_name,
                exact_max_size=exact_max_size,
                exact_ttl_seconds=exact_ttl_seconds,
                progress=progress,
            )
            proposed = run_mode(
                workload,
                mode=proposed_mode,
                tenant_aware=tenant_aware,
                bloom_bits=bloom_bits,
                bloom_hashes=bloom_hashes,
                hash_name=hash_name,
                exact_max_size=exact_max_size,
                exact_ttl_seconds=exact_ttl_seconds,
                progress=progress,
            )
            results.extend((baseline, proposed))
            comparison = build_comparison(baseline, proposed)
            comparisons.append(comparison)
            if progress is not None:
                progress.run_set_done(
                    scenario=workload.name,
                    dupe_rate=workload.metadata.requested_dupe_rate,
                    reduction_pct=comparison.communication_reduction_pct,
                )
        return results, comparisons
    if mode is not None:
        for workload in workloads:
            result = run_mode(
                workload,
                mode=mode,
                tenant_aware=tenant_aware,
                bloom_bits=bloom_bits,
                bloom_hashes=bloom_hashes,
                hash_name=hash_name,
                exact_max_size=exact_max_size,
                exact_ttl_seconds=exact_ttl_seconds,
                progress=progress,
            )
            results.append(result)
            if progress is not None:
                progress.run_set_done(
                    scenario=workload.name,
                    dupe_rate=workload.metadata.requested_dupe_rate,
                    reduction_pct=result.communication_reduction_pct,
                )
        return results, comparisons

    for workload in workloads:
        if progress is not None:
            progress.compare_modes(DEFAULT_COMPARE_MODES)
        baseline = run_mode(
            workload,
            mode="no_edge_dedup",
            tenant_aware=True,
            bloom_bits=bloom_bits,
            bloom_hashes=bloom_hashes,
            hash_name=hash_name,
            exact_max_size=exact_max_size,
            exact_ttl_seconds=exact_ttl_seconds,
            progress=progress,
        )
        proposed = run_mode(
            workload,
            mode="edge_metadata",
            tenant_aware=True,
            bloom_bits=bloom_bits,
            bloom_hashes=bloom_hashes,
            hash_name=hash_name,
            exact_max_size=exact_max_size,
            exact_ttl_seconds=exact_ttl_seconds,
            progress=progress,
        )
        results.extend((baseline, proposed))
        comparison = build_comparison(baseline, proposed)
        comparisons.append(comparison)
        if progress is not None:
            progress.run_set_done(
                scenario=workload.name,
                dupe_rate=workload.metadata.requested_dupe_rate,
                reduction_pct=comparison.communication_reduction_pct,
            )
    return results, comparisons


def print_result_table(results: Sequence[ExperimentResult]) -> None:
    if not results:
        return
    print("Experiment Results")
    print(
        "scenario | mode | input_batches | tx_batches | input_records | tx_records | "
        "input_bytes | tx_bytes | bytes_saved | comm_reduction_pct | accepted_payload_ratio | "
        "duplicate_suppression_ratio | dropped_batches | trimmed_batches | processing_time_s"
    )
    for result in results:
        print(
            f"{result.scenario} | {result.mode} | {result.total_input_batches} | "
            f"{result.total_transmitted_batches} | {result.total_input_records} | "
            f"{result.total_transmitted_records} | {result.total_input_bytes} | "
            f"{result.total_transmitted_bytes} | {result.bytes_saved} | "
            f"{result.communication_reduction_pct:.2f} | {result.accepted_payload_ratio:.4f} | "
            f"{result.duplicate_suppression_ratio:.4f} | {result.dropped_batches} | "
            f"{result.trimmed_batches} | {result.processing_time_s:.6f}"
        )


def print_comparison_table(comparisons: Sequence[ComparisonResult]) -> None:
    if not comparisons:
        return
    print("\nMode Comparison")
    print(
        "scenario | requested_dupe_rate | realized_dupe_rate | baseline_mode | proposed_mode | "
        "baseline_tx_bytes | proposed_tx_bytes | bytes_saved | comm_reduction_pct | "
        "baseline_tx_records | proposed_tx_records | record_reduction_pct | "
        "baseline_processing_time_s | proposed_processing_time_s | dropped_batches | trimmed_batches"
    )
    for comparison in comparisons:
        print(
            f"{comparison.scenario} | {comparison.requested_dupe_rate:.2f} | "
            f"{comparison.realized_dupe_rate:.4f} | {comparison.baseline_mode} | "
            f"{comparison.proposed_mode} | {comparison.baseline_transmitted_bytes} | "
            f"{comparison.proposed_transmitted_bytes} | {comparison.bytes_saved} | "
            f"{comparison.communication_reduction_pct:.2f} | "
            f"{comparison.baseline_transmitted_records} | {comparison.proposed_transmitted_records} | "
            f"{comparison.record_reduction_pct:.2f} | {comparison.baseline_processing_time_s:.6f} | "
            f"{comparison.proposed_processing_time_s:.6f} | {comparison.dropped_batches} | "
            f"{comparison.trimmed_batches}"
        )


def print_cic_schema_summary(results: Sequence[ExperimentResult]) -> None:
    cic_results = [result for result in results if result.dataset_name == "cic_ids2017"]
    if not cic_results:
        return
    first = cic_results[0]
    print("\nCIC Schema Summary")
    print(f"source_files_processed: {', '.join(first.source_files_processed) or '-'}")
    print(f"source_file_encodings: {', '.join(first.source_file_encodings) or '-'}")
    print(f"detected_canonical_cic_columns: {', '.join(first.detected_cic_columns) or '-'}")
    print(f"missing_canonical_cic_columns: {', '.join(first.missing_cic_columns) or '-'}")
    print(f"looks_like_cicflowmeter: {'yes' if first.schema_looks_like_cicflowmeter else 'no'}")
    print(f"schema_quality: {first.cic_schema_quality}")


def print_cic_inspection_reports(csv_paths: Sequence[str]) -> None:
    reports = inspect_cic_csvs(csv_paths)
    expected_columns = ", ".join(column.canonical_name for column in CANONICAL_CIC_COLUMNS)
    print("CIC Schema Inspection")
    print(f"expected_canonical_columns: {expected_columns}")
    for report in reports:
        print(f"\nfile: {report.source_file}")
        print(f"row_count: {report.row_count}")
        print(f"encoding_used: {report.encoding_used}")
        print(f"looks_like_cicflowmeter: {'yes' if report.looks_like_cicflowmeter else 'no'}")
        print(f"schema_quality: {report.schema_quality}")
        print(f"detected_canonical_columns: {', '.join(report.detected_canonical_columns) or '-'}")
        print(
            "missing_expected_canonical_columns: "
            f"{', '.join(report.missing_canonical_columns) or '-'}"
        )
        print("canonical_match_modes:")
        for canonical_name in report.detected_canonical_columns:
            match = report.canonical_matches[canonical_name]
            print(
                f"  {canonical_name}: {match.match_mode} -> "
                f"{match.raw_column_name} (matched_alias: {match.matched_alias}, "
                f"normalized: {match.normalized_column_name})"
            )
        print(f"extra_columns: {', '.join(report.extra_columns) or '-'}")
        if report.duplicate_alias_matches:
            print(f"duplicate_alias_matches: {report.duplicate_alias_matches}")
        if report.repeated_raw_columns:
            print(f"repeated_raw_columns: {report.repeated_raw_columns}")


def _build_export_paths(
    output_prefix: str,
    *,
    preset_name: str,
    export_json: bool,
    include_comparison: bool,
) -> dict[str, Path]:
    if preset_name:
        output_dir = Path(output_prefix)
        paths: dict[str, Path] = {"results_csv": output_dir / "results.csv"}
        if include_comparison:
            paths["comparison_csv"] = output_dir / "comparison.csv"
        if export_json:
            paths["results_json"] = output_dir / "results.json"
            if include_comparison:
                paths["comparison_json"] = output_dir / "comparison.json"
        return paths

    prefix_path = Path(output_prefix)
    paths = {"results_csv": Path(f"{prefix_path}_results.csv")}
    if include_comparison:
        paths["comparison_csv"] = Path(f"{prefix_path}_comparison.csv")
    if export_json:
        paths["results_json"] = Path(f"{prefix_path}_results.json")
        if include_comparison:
            paths["comparison_json"] = Path(f"{prefix_path}_comparison.json")
    return paths


def _remove_existing_exports(
    paths: dict[str, Path],
    *,
    output_prefix: str,
    preset_name: str,
    overwrite: bool,
) -> None:
    if preset_name:
        output_dir = Path(output_prefix)
        if output_dir.exists():
            if not overwrite:
                raise FileExistsError(
                    f"Output directory {output_dir} already exists. Use --overwrite to replace it."
                )
            if output_dir.is_dir():
                shutil.rmtree(output_dir)
            else:
                output_dir.unlink()
        return

    existing_paths = [path for path in paths.values() if path.exists()]
    if existing_paths and not overwrite:
        formatted = ", ".join(str(path) for path in existing_paths)
        raise FileExistsError(
            f"Output file(s) already exist: {formatted}. Use --overwrite to replace them."
        )
    if overwrite:
        prefix_path = Path(output_prefix)
        stale_paths = [
            Path(f"{prefix_path}_results.csv"),
            Path(f"{prefix_path}_comparison.csv"),
            Path(f"{prefix_path}_results.json"),
            Path(f"{prefix_path}_comparison.json"),
        ]
        for stale_path in stale_paths:
            if stale_path.exists():
                stale_path.unlink()


def export_results(
    output_prefix: str,
    results: Sequence[ExperimentResult],
    comparisons: Sequence[ComparisonResult],
    *,
    preset_name: str = "",
    export_json: bool = False,
    overwrite: bool = False,
) -> dict[str, str]:
    result_rows = [result.to_dict() for result in results]
    comparison_rows = [comparison.to_dict() for comparison in comparisons]
    paths = _build_export_paths(
        output_prefix,
        preset_name=preset_name,
        export_json=export_json,
        include_comparison=bool(comparison_rows),
    )
    _remove_existing_exports(
        paths,
        output_prefix=output_prefix,
        preset_name=preset_name,
        overwrite=overwrite,
    )

    write_csv(paths["results_csv"], result_rows)
    if comparison_rows:
        write_csv(paths["comparison_csv"], comparison_rows)
    if export_json:
        write_json(paths["results_json"], result_rows)
        if comparison_rows:
            write_json(paths["comparison_json"], comparison_rows)
    return {key: str(value) for key, value in paths.items()}


def main() -> None:
    parser = build_argument_parser()
    args = resolve_cli_args(parser.parse_args())
    compare_modes = tuple(args.compare_modes) if args.compare_modes else None
    progress = ProgressPrinter(
        quiet=args.quiet,
        verbose=args.verbose,
        progress_every=args.progress_every,
    )
    if args.inspect_cic:
        if not args.csv:
            raise ValueError("Provide at least one --csv file when using --inspect-cic.")
        print_cic_inspection_reports(args.csv)
        return

    if args.preset:
        progress.preset(args.preset)
    progress.config_summary(
        dataset=args.dataset,
        mode=(
            f"{compare_modes[0]}->{compare_modes[1]}"
            if compare_modes is not None
            else args.mode or "compare"
        ),
        scenario=",".join(args.scenario),
        dupe_rate=",".join(f"{rate:g}" for rate in args.dupe_rate),
        batch_size=args.batch_size,
        seed=args.seed,
        tenant_mode=args.tenant_mode,
        num_tenants=args.num_tenants,
        source_mode=args.source_mode,
        num_sources=args.num_sources,
        output_prefix=args.output_prefix,
    )
    workloads = resolve_workloads(args, progress=progress)
    results, comparisons = execute_workloads(
        workloads,
        mode=args.mode,
        compare_modes=compare_modes,
        tenant_aware=args.tenant_aware,
        bloom_bits=args.bloom_bits,
        bloom_hashes=args.bloom_hashes,
        hash_name=args.hash_name,
        exact_max_size=args.exact_max_size,
        exact_ttl_seconds=args.exact_ttl_seconds,
        progress=progress,
    )
    if not args.quiet:
        print_result_table(results)
        print_comparison_table(comparisons)
        print_cic_schema_summary(results)
    export_paths = export_results(
        args.output_prefix,
        results,
        comparisons,
        preset_name=args.preset or "",
        export_json=args.export_json,
        overwrite=args.overwrite,
    )
    progress.export_summary(export_paths)
    progress.done(results=results, comparisons=comparisons)


if __name__ == "__main__":
    main()
