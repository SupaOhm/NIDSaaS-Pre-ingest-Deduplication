"""Lightweight process logging for experiment runs."""

from __future__ import annotations

import sys
from typing import Any, TextIO

from .metrics import ComparisonResult, ExperimentResult


class ProgressPrinter:
    def __init__(
        self,
        *,
        quiet: bool = False,
        verbose: bool = False,
        progress_every: int = 25,
        stream: TextIO | None = None,
    ) -> None:
        if progress_every <= 0:
            raise ValueError("progress_every must be positive")
        self.quiet = quiet
        self.verbose = verbose
        self.progress_every = progress_every
        self.stream = stream or sys.stdout

    def _emit(self, tag: str, message: str, *, always: bool = False) -> None:
        if self.quiet and not always:
            return
        print(f"[{tag}] {message}", file=self.stream, flush=True)

    def preset(self, name: str) -> None:
        self._emit("preset", name)

    def config_summary(
        self,
        *,
        dataset: str,
        mode: str,
        scenario: str,
        dupe_rate: str,
        batch_size: int,
        seed: int | None,
        tenant_mode: str,
        num_tenants: int,
        source_mode: str,
        num_sources: int,
        output_prefix: str,
    ) -> None:
        self._emit(
            "config",
            (
                f"dataset={dataset} mode={mode} scenario={scenario} dupe_rate={dupe_rate} "
                f"batch_size={batch_size} seed={seed}"
            ),
        )
        self._emit(
            "config",
            (
                f"tenant_mode={tenant_mode} num_tenants={num_tenants} "
                f"source_mode={source_mode} num_sources={num_sources} "
                f"output_prefix={output_prefix}"
            ),
        )

    def run_set(self, *, scenario: str, dupe_rate: float) -> None:
        self._emit("run-set", f"scenario={scenario} dupe_rate={dupe_rate:g}")

    def compare_modes(self, modes: tuple[str, str] | list[str]) -> None:
        self._emit("compare", f"modes={','.join(modes)}")

    def load_start(self, path: str) -> None:
        self._emit("load", path)

    def load_retry(self, *, failed_encoding: str, next_encoding: str) -> None:
        self._emit("load", f"{failed_encoding} failed, retrying {next_encoding}")

    def load_file_summary(self, report: Any) -> None:
        self._emit(
            "load",
            (
                f"rows={report.row_count} schema_quality={report.schema_quality} "
                f"encoding={getattr(report, 'encoding_used', 'unknown')}"
            ),
        )
        if report.missing_canonical_columns:
            self._emit(
                "load",
                "missing_canonical_columns=" + ",".join(report.missing_canonical_columns),
            )
        if report.repeated_raw_columns:
            if self.verbose:
                self._emit("load", f"repeated_raw_columns={report.repeated_raw_columns}")
            else:
                repeated_names = ",".join(sorted(report.repeated_raw_columns))
                self._emit("load", f"repeated_raw_columns={repeated_names}")

    def load_total(self, total_records: int) -> None:
        self._emit("load", f"total_records={total_records}")

    def batch_summary(self, *, records: int, batch_size: int, base_batches: int) -> None:
        self._emit(
            "batch",
            f"records={records} batch_size={batch_size} base_batches={base_batches}",
        )

    def inject_start(self, *, scenario: str, requested_dupe_rate: float) -> None:
        self._emit(
            "inject",
            f"scenario={scenario} requested_dupe_rate={requested_dupe_rate}",
        )

    def inject_summary(self, result: Any) -> None:
        self._emit(
            "inject",
            (
                f"candidate_batches={len(result.batches)} "
                f"realized_dupe_rate={result.realized_dupe_rate:.4f}"
            ),
        )
        detail_parts = []
        if getattr(result, "replay_batches_added", 0):
            detail_parts.append(f"replay_batches_added={result.replay_batches_added}")
        if getattr(result, "overlap_batches_modified", 0):
            detail_parts.append(f"overlap_batches_modified={result.overlap_batches_modified}")
        if getattr(result, "injected_duplicate_records", 0):
            detail_parts.append(
                f"injected_duplicate_records={result.injected_duplicate_records}"
            )
        if detail_parts:
            self._emit("inject", " ".join(detail_parts))

    def run_set_done(self, *, scenario: str, dupe_rate: float, reduction_pct: float) -> None:
        self._emit(
            "done",
            f"scenario={scenario} dupe_rate={dupe_rate:g} reduction={reduction_pct:.2f}%",
        )

    def run_start(self, *, mode: str, total_batches: int) -> None:
        self._emit("run", f"mode={mode} batches={total_batches}")

    def run_progress(self, *, processed_batches: int, total_batches: int) -> None:
        if total_batches <= 0:
            return
        if (
            processed_batches == total_batches
            or processed_batches % self.progress_every == 0
        ):
            self._emit("run", f"processed {processed_batches}/{total_batches} batches")

    def run_summary(self, result: ExperimentResult) -> None:
        self._emit(
            "run",
            (
                f"transmitted_records={result.total_transmitted_records} "
                f"transmitted_bytes={result.total_transmitted_bytes}"
            ),
        )
        if result.mode == "no_edge_dedup":
            self._emit("run", "transmitted all candidate batches")
            return
        if result.mode == "edge_metadata":
            self._emit(
                "run",
                f"dropped_batches={result.dropped_batches} trimmed_batches={result.trimmed_batches}",
            )
            return
        self._emit(
            "run",
            (
                f"bloom_positives={result.bloom_positive_count} "
                f"exact_lookups={result.exact_lookup_count}"
            ),
        )

    def export_summary(self, paths: dict[str, str]) -> None:
        for key, value in sorted(paths.items()):
            if key == "results_csv":
                self._emit("export", f"results written to {value}", always=True)
            elif key == "results_json":
                self._emit("export", f"json written to {value}", always=True)
            elif key == "comparison_csv":
                self._emit("export", f"comparison written to {value}", always=True)
            elif key == "comparison_json":
                self._emit("export", f"comparison json written to {value}", always=True)

    def done(
        self,
        *,
        results: list[ExperimentResult],
        comparisons: list[ComparisonResult],
    ) -> None:
        if len(comparisons) == 1:
            self._emit(
                "done",
                f"communication_reduction_pct={comparisons[0].proposed_comm_reduction_pct:.2f}%",
                always=True,
            )
            return
        if len(results) == 1:
            self._emit(
                "done",
                f"communication_reduction_pct={results[0].communication_reduction_pct:.2f}%",
                always=True,
            )
            return
        self._emit(
            "done",
            f"completed_results={len(results)} completed_comparisons={len(comparisons)}",
            always=True,
        )
