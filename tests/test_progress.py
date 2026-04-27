from __future__ import annotations

from io import StringIO
from types import SimpleNamespace
import unittest

from src.progress import ProgressPrinter


class ProgressPrinterTests(unittest.TestCase):
    def test_quiet_mode_suppresses_non_final_lines(self) -> None:
        stream = StringIO()
        printer = ProgressPrinter(quiet=True, progress_every=2, stream=stream)

        printer.config_summary(
            dataset="cic_ids2017",
            mode="edge_metadata",
            scenario="exact_replay",
            dupe_rate="0.5",
            batch_size=5000,
            seed=42,
            tenant_mode="single",
            num_tenants=1,
            source_mode="single",
            num_sources=1,
            output_prefix="results/x",
        )
        printer.export_summary({"results_csv": "results/x_results.csv"})

        output = stream.getvalue()
        self.assertNotIn("[config]", output)
        self.assertIn("[export] results written to results/x_results.csv", output)

    def test_progress_interval_emits_only_at_interval_and_completion(self) -> None:
        stream = StringIO()
        printer = ProgressPrinter(progress_every=2, stream=stream)

        for processed in range(1, 6):
            printer.run_progress(processed_batches=processed, total_batches=5)

        output = stream.getvalue()
        self.assertIn("[run] processed 2/5 batches", output)
        self.assertIn("[run] processed 4/5 batches", output)
        self.assertIn("[run] processed 5/5 batches", output)
        self.assertNotIn("[run] processed 1/5 batches", output)

    def test_preset_run_set_and_compare_messages_are_printed(self) -> None:
        stream = StringIO()
        printer = ProgressPrinter(stream=stream)

        printer.preset("main_test")
        printer.run_set(scenario="exact_replay", dupe_rate=0.5)
        printer.compare_modes(("no_edge_dedup", "edge_metadata"))
        printer.run_set_done(scenario="exact_replay", dupe_rate=0.5, reduction_pct=33.42)

        output = stream.getvalue()
        self.assertIn("[preset] main_test", output)
        self.assertIn("[run-set] scenario=exact_replay dupe_rate=0.5", output)
        self.assertIn("[compare] modes=no_edge_dedup,edge_metadata", output)
        self.assertIn("[done] scenario=exact_replay dupe_rate=0.5 reduction=33.42%", output)

    def test_load_retry_and_encoding_summary_are_printed(self) -> None:
        stream = StringIO()
        printer = ProgressPrinter(stream=stream)

        printer.load_retry(failed_encoding="utf-8-sig", next_encoding="latin1")
        printer.load_file_summary(
            SimpleNamespace(
                row_count=123,
                schema_quality="clean",
                encoding_used="latin1",
                missing_canonical_columns=(),
                repeated_raw_columns={},
            )
        )

        output = stream.getvalue()
        self.assertIn("[load] utf-8-sig failed, retrying latin1", output)
        self.assertIn("[load] rows=123 schema_quality=clean encoding=latin1", output)


if __name__ == "__main__":
    unittest.main()
