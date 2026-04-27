from __future__ import annotations

import csv
from pathlib import Path
import tempfile
import unittest

from src.runner import (
    build_argument_parser,
    execute_workloads,
    export_results,
    resolve_cli_args,
    resolve_workloads,
)


class RunnerPresetTests(unittest.TestCase):
    def test_short_aliases_parse_correctly(self) -> None:
        parser = build_argument_parser()
        args = resolve_cli_args(
            parser.parse_args(
                [
                    "-d",
                    "cic_ids2017",
                    "-c",
                    "sample_data/cic_ids2017_sample.csv",
                    "-b",
                    "3",
                    "-s",
                    "exact_replay",
                    "-r",
                    "0.5",
                    "-m",
                    "edge_metadata",
                    "-tm",
                    "single",
                    "-nt",
                    "1",
                    "-sm",
                    "single",
                    "-ns",
                    "1",
                    "-S",
                    "42",
                    "-o",
                    "results/alias_test",
                    "-p",
                    "10",
                    "-v",
                ]
            )
        )

        self.assertEqual(args.dataset, "cic_ids2017")
        self.assertEqual(args.csv, ["sample_data/cic_ids2017_sample.csv"])
        self.assertEqual(args.batch_size, 3)
        self.assertEqual(args.scenario, ["exact_replay"])
        self.assertEqual(args.dupe_rate, [0.5])
        self.assertEqual(args.mode, "edge_metadata")
        self.assertEqual(args.tenant_mode, "single")
        self.assertEqual(args.num_tenants, 1)
        self.assertEqual(args.source_mode, "single")
        self.assertEqual(args.num_sources, 1)
        self.assertEqual(args.seed, 42)
        self.assertEqual(args.output_prefix, "results/alias_test")
        self.assertEqual(args.progress_every, 10)
        self.assertTrue(args.verbose)

    def test_csv_flag_accepts_multiple_paths_after_single_flag(self) -> None:
        parser = build_argument_parser()
        args = resolve_cli_args(
            parser.parse_args(
                [
                    "-d",
                    "cic_ids2017",
                    "-c",
                    "sample_data/Monday-WorkingHours.pcap_ISCX.csv",
                    "sample_data/Tuesday-WorkingHours.pcap_ISCX.csv",
                ]
            )
        )

        self.assertEqual(
            args.csv,
            [
                "sample_data/Monday-WorkingHours.pcap_ISCX.csv",
                "sample_data/Tuesday-WorkingHours.pcap_ISCX.csv",
            ],
        )

    def test_compare_mode_alias_parses_correctly(self) -> None:
        parser = build_argument_parser()
        args = resolve_cli_args(
            parser.parse_args(
                [
                    "-P",
                    "quick_test",
                    "-cm",
                    "no_edge_dedup",
                    "edge_metadata",
                ]
            )
        )

        self.assertEqual(args.compare_modes, ["no_edge_dedup", "edge_metadata"])
        self.assertIsNone(args.mode)

    def test_quick_test_preset_defaults(self) -> None:
        parser = build_argument_parser()
        args = resolve_cli_args(parser.parse_args(["-P", "quick_test"]))

        self.assertEqual(args.dataset, "cic_ids2017")
        self.assertEqual(args.csv, ["sample_data/cic_ids2017_sample.csv"])
        self.assertEqual(args.scenario, ["exact_replay"])
        self.assertEqual(args.dupe_rate, [0.5])
        self.assertEqual(args.compare_modes, ["no_edge_dedup", "edge_metadata"])
        self.assertEqual(args.batch_size, 3)
        self.assertEqual(args.seed, 42)

    def test_cli_values_override_preset_defaults(self) -> None:
        parser = build_argument_parser()
        args = resolve_cli_args(
            parser.parse_args(
                [
                    "-P",
                    "main_test",
                    "-c",
                    "sample_data/cic_ids2017_sample.csv",
                    "-s",
                    "exact_replay",
                    "-r",
                    "0.5",
                    "-b",
                    "3",
                    "-m",
                    "edge_metadata",
                ]
            )
        )

        self.assertEqual(args.csv, ["sample_data/cic_ids2017_sample.csv"])
        self.assertEqual(args.scenario, ["exact_replay"])
        self.assertEqual(args.dupe_rate, [0.5])
        self.assertEqual(args.batch_size, 3)
        self.assertEqual(args.mode, "edge_metadata")
        self.assertIsNone(args.compare_modes)


class RunnerCompareModesTests(unittest.TestCase):
    def test_compare_modes_export_csv_only_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_prefix = str(Path(temp_dir) / "compare_run")
            parser = build_argument_parser()
            args = resolve_cli_args(
                parser.parse_args(
                    [
                        "--dataset",
                        "cic_ids2017",
                        "--csv",
                        "sample_data/cic_ids2017_sample.csv",
                        "--batch-size",
                        "3",
                        "--scenario",
                        "exact_replay",
                        "--dupe-rate",
                        "1.0",
                        "--compare-modes",
                        "no_edge_dedup",
                        "edge_metadata",
                        "--output-prefix",
                        output_prefix,
                    ]
                )
            )

            workloads = resolve_workloads(args)
            results, comparisons = execute_workloads(
                workloads,
                mode=args.mode,
                compare_modes=tuple(args.compare_modes),
                tenant_aware=args.tenant_aware,
                bloom_bits=args.bloom_bits,
                bloom_hashes=args.bloom_hashes,
                hash_name=args.hash_name,
                exact_max_size=args.exact_max_size,
                exact_ttl_seconds=args.exact_ttl_seconds,
            )
            paths = export_results(
                output_prefix,
                results,
                comparisons,
            )

            self.assertEqual(len(results), 2)
            self.assertEqual(len(comparisons), 1)
            self.assertEqual(results[0].total_input_records, results[1].total_input_records)
            self.assertEqual(results[0].total_input_bytes, results[1].total_input_bytes)
            comparison = comparisons[0]
            self.assertEqual(comparison.baseline_mode, "no_edge_dedup")
            self.assertEqual(comparison.proposed_mode, "edge_metadata")
            self.assertEqual(comparison.bytes_saved, 1537)
            self.assertEqual(comparison.record_reduction_pct, 50.0)
            self.assertIn("results_csv", paths)
            self.assertIn("comparison_csv", paths)
            self.assertNotIn("results_json", paths)
            self.assertNotIn("comparison_json", paths)
            self.assertTrue(Path(paths["comparison_csv"]).exists())
            self.assertTrue(Path(paths["results_csv"]).exists())
            self.assertFalse(Path(f"{output_prefix}_results.json").exists())
            self.assertFalse(Path(f"{output_prefix}_comparison.json").exists())
            self.assertFalse(Path(f"{output_prefix}_no_edge_dedup_results.csv").exists())
            self.assertFalse(Path(f"{output_prefix}_edge_metadata_results.csv").exists())

            with Path(paths["comparison_csv"]).open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["baseline_mode"], "no_edge_dedup")
            self.assertEqual(rows[0]["proposed_mode"], "edge_metadata")
            self.assertEqual(rows[0]["bytes_saved"], "1537")
            self.assertEqual(rows[0]["communication_reduction_pct"], "50.0")
            self.assertEqual(rows[0]["dropped_batches"], "1")
            self.assertEqual(rows[0]["trimmed_batches"], "0")

    def test_export_json_flag_creates_json_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_prefix = str(Path(temp_dir) / "compare_run")
            parser = build_argument_parser()
            args = resolve_cli_args(
                parser.parse_args(
                    [
                        "--dataset",
                        "cic_ids2017",
                        "--csv",
                        "sample_data/cic_ids2017_sample.csv",
                        "--batch-size",
                        "3",
                        "--scenario",
                        "exact_replay",
                        "--dupe-rate",
                        "1.0",
                        "--compare-modes",
                        "no_edge_dedup",
                        "edge_metadata",
                        "--output-prefix",
                        output_prefix,
                        "--export-json",
                    ]
                )
            )

            workloads = resolve_workloads(args)
            results, comparisons = execute_workloads(
                workloads,
                mode=args.mode,
                compare_modes=tuple(args.compare_modes),
                tenant_aware=args.tenant_aware,
                bloom_bits=args.bloom_bits,
                bloom_hashes=args.bloom_hashes,
                hash_name=args.hash_name,
                exact_max_size=args.exact_max_size,
                exact_ttl_seconds=args.exact_ttl_seconds,
            )
            paths = export_results(
                output_prefix,
                results,
                comparisons,
                export_json=args.export_json,
            )

            self.assertTrue(Path(paths["results_csv"]).exists())
            self.assertTrue(Path(paths["comparison_csv"]).exists())
            self.assertTrue(Path(paths["results_json"]).exists())
            self.assertTrue(Path(paths["comparison_json"]).exists())

    def test_main_test_preset_exports_combined_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_prefix = str(Path(temp_dir) / "main_test_run")
            parser = build_argument_parser()
            args = resolve_cli_args(
                parser.parse_args(
                    [
                        "-P",
                        "main_test",
                        "-c",
                        "sample_data/cic_ids2017_sample.csv",
                        "-o",
                        output_prefix,
                    ]
                )
            )

            workloads = resolve_workloads(args)
            self.assertEqual(len(workloads), 15)

            results, comparisons = execute_workloads(
                workloads,
                mode=args.mode,
                compare_modes=tuple(args.compare_modes),
                tenant_aware=args.tenant_aware,
                bloom_bits=args.bloom_bits,
                bloom_hashes=args.bloom_hashes,
                hash_name=args.hash_name,
                exact_max_size=args.exact_max_size,
                exact_ttl_seconds=args.exact_ttl_seconds,
            )
            paths = export_results(
                output_prefix,
                results,
                comparisons,
                preset_name=args.preset or "",
            )

            self.assertEqual(len(results), 30)
            self.assertEqual(len(comparisons), 15)
            self.assertTrue(Path(paths["results_csv"]).exists())
            self.assertTrue(Path(paths["comparison_csv"]).exists())
            self.assertFalse(Path(output_prefix, "results.json").exists())
            self.assertFalse(Path(output_prefix, "comparison.json").exists())
            self.assertEqual(Path(paths["results_csv"]), Path(output_prefix) / "results.csv")
            self.assertEqual(Path(paths["comparison_csv"]), Path(output_prefix) / "comparison.csv")

            with Path(paths["results_csv"]).open("r", encoding="utf-8", newline="") as handle:
                result_rows = list(csv.DictReader(handle))
            with Path(paths["comparison_csv"]).open("r", encoding="utf-8", newline="") as handle:
                comparison_rows = list(csv.DictReader(handle))

            self.assertEqual(len(result_rows), 30)
            self.assertEqual(len(comparison_rows), 15)
            self.assertTrue(all(row["preset_name"] == "main_test" for row in result_rows))
            self.assertTrue(all(row["preset_name"] == "main_test" for row in comparison_rows))

    def test_overwrite_replaces_existing_preset_output_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_prefix = str(Path(temp_dir) / "main_test_run")
            output_dir = Path(output_prefix)
            output_dir.mkdir(parents=True, exist_ok=True)
            stale_file = output_dir / "stale.txt"
            stale_file.write_text("old", encoding="utf-8")

            parser = build_argument_parser()
            args = resolve_cli_args(
                parser.parse_args(
                    [
                        "-P",
                        "quick_test",
                        "-o",
                        output_prefix,
                    ]
                )
            )

            workloads = resolve_workloads(args)
            results, comparisons = execute_workloads(
                workloads,
                mode=args.mode,
                compare_modes=tuple(args.compare_modes),
                tenant_aware=args.tenant_aware,
                bloom_bits=args.bloom_bits,
                bloom_hashes=args.bloom_hashes,
                hash_name=args.hash_name,
                exact_max_size=args.exact_max_size,
                exact_ttl_seconds=args.exact_ttl_seconds,
            )

            with self.assertRaises(FileExistsError):
                export_results(
                    output_prefix,
                    results,
                    comparisons,
                    preset_name=args.preset or "",
                )

            paths = export_results(
                output_prefix,
                results,
                comparisons,
                preset_name=args.preset or "",
                overwrite=True,
            )

            self.assertTrue(Path(paths["results_csv"]).exists())
            self.assertTrue(Path(paths["comparison_csv"]).exists())
            self.assertFalse(stale_file.exists())


if __name__ == "__main__":
    unittest.main()
