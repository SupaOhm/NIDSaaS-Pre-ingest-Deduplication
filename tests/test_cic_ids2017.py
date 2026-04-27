from __future__ import annotations

import csv
from pathlib import Path
import tempfile
import textwrap
import unittest

from src.datasets.cic_ids2017 import inspect_cic_csvs, load_cic_ids2017_csvs, resolve_cic_headers


class CICIDS2017LoaderTests(unittest.TestCase):
    def test_canonical_cic_header_mapping_matches_sample_file(self) -> None:
        with open("sample_data/cic_ids2017_sample.csv", "r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.reader(handle)
            header = next(reader)

        resolution = resolve_cic_headers(header, source_file="cic_ids2017_sample.csv")

        self.assertTrue(resolution.looks_like_cicflowmeter)
        self.assertEqual(resolution.schema_quality, "clean")
        self.assertEqual(resolution.canonical_column_map["flow_id"], "Flow ID")
        self.assertEqual(resolution.canonical_column_map["src_ip"], " Source IP")
        self.assertEqual(resolution.canonical_column_map["dst_ip"], " Destination IP")
        self.assertEqual(resolution.canonical_column_map["src_port"], " Source Port")
        self.assertEqual(resolution.canonical_column_map["dst_port"], " Destination Port")
        self.assertEqual(resolution.canonical_column_map["protocol"], " Protocol")
        self.assertEqual(resolution.canonical_column_map["timestamp"], " Timestamp")
        self.assertEqual(resolution.canonical_column_map["label"], " Label")
        self.assertEqual(resolution.canonical_matches["flow_id"].match_mode, "direct")
        self.assertEqual(resolution.canonical_matches["src_ip"].match_mode, "direct")
        self.assertIn("Fwd Header Length", resolution.repeated_raw_columns)

    def test_flowid_alias_is_supported(self) -> None:
        csv_text = textwrap.dedent(
            """\
            FlowID,Source IP,Source Port,Destination IP,Destination Port,Protocol,Timestamp,Label,Flow Duration
            abc,10.0.0.1,1234,10.0.0.2,80,6,2017-07-03 09:00:01,BENIGN,1000
            """
        )
        with tempfile.NamedTemporaryFile("w", suffix=".csv", encoding="utf-8", delete=False) as handle:
            handle.write(csv_text)
            path = handle.name

        result = load_cic_ids2017_csvs([path])

        self.assertEqual(result.total_loaded_records, 1)
        self.assertEqual(result.records[0]["flow_id"], "abc")
        self.assertEqual(result.records[0]["src_ip"], "10.0.0.1")
        self.assertEqual(result.records[0]["dst_ip"], "10.0.0.2")
        self.assertEqual(result.schema_quality, "partial")
        self.assertEqual(result.schema_reports[0].canonical_matches["flow_id"].match_mode, "alias")
        self.assertEqual(result.schema_reports[0].canonical_matches["flow_id"].matched_alias, "FlowID")
        self.assertEqual(result.schema_reports[0].encoding_used, "utf-8-sig")

    def test_whitespace_in_raw_headers_is_handled_as_direct_match(self) -> None:
        csv_text = textwrap.dedent(
            """\
             Flow ID  ,  Source IP  ,  Source Port  , Destination IP   , Destination Port , Protocol , Timestamp , Label
            abc,10.0.0.1,1234,10.0.0.2,80,6,2017-07-03 09:00:01,BENIGN
            """
        )
        with tempfile.NamedTemporaryFile("w", suffix=".csv", encoding="utf-8", delete=False) as handle:
            handle.write(csv_text)
            path = handle.name

        resolution = inspect_cic_csvs([path])[0]

        self.assertEqual(resolution.schema_quality, "partial")
        self.assertEqual(resolution.canonical_matches["flow_id"].match_mode, "direct")
        self.assertEqual(resolution.canonical_matches["flow_id"].matched_alias, "Flow ID")
        self.assertEqual(resolution.canonical_matches["src_ip"].match_mode, "direct")
        self.assertEqual(resolution.canonical_matches["timestamp"].normalized_column_name, "Timestamp")

    def test_loader_maps_core_cic_fields_stably(self) -> None:
        result = load_cic_ids2017_csvs(
            ["sample_data/cic_ids2017_sample.csv"],
            tenant_mode="round_robin",
            num_tenants=2,
            source_mode="hash",
            num_sources=2,
        )

        self.assertEqual(result.dataset_name, "cic_ids2017")
        self.assertEqual(result.csv_count, 1)
        self.assertEqual(result.total_loaded_records, 3)
        first = result.records[0]
        second = result.records[1]
        self.assertEqual(first["flow_id"], "192.168.10.5-8.254.250.126-49188-80-6")
        self.assertEqual(first["src_ip"], "8.254.250.126")
        self.assertEqual(first["dst_ip"], "192.168.10.5")
        self.assertEqual(first["src_port"], 80)
        self.assertEqual(first["dst_port"], 49188)
        self.assertEqual(first["protocol"], "6")
        self.assertEqual(first["label"], "BENIGN")
        self.assertEqual(first["total_fwd_packets"], 2)
        self.assertEqual(first["total_bwd_packets"], 0)
        self.assertEqual(first["total_length_fwd_packets"], 12)
        self.assertEqual(first["total_length_bwd_packets"], 0)
        self.assertEqual(first["packets"], 2)
        self.assertEqual(first["bytes"], 12)
        self.assertEqual(first["tenant_id"], "tenant-1")
        self.assertEqual(second["tenant_id"], "tenant-2")

    def test_schema_inspection_reports_expected_fields(self) -> None:
        report = inspect_cic_csvs(["sample_data/cic_ids2017_sample.csv"])[0]

        self.assertEqual(report.source_file, "cic_ids2017_sample.csv")
        self.assertEqual(report.row_count, 3)
        self.assertTrue(report.looks_like_cicflowmeter)
        self.assertEqual(report.schema_quality, "clean")
        self.assertEqual(report.encoding_used, "utf-8-sig")
        self.assertIn("flow_id", report.detected_canonical_columns)
        self.assertIn("src_ip", report.detected_canonical_columns)
        self.assertIn("label", report.detected_canonical_columns)
        self.assertEqual(report.missing_required_columns, ())
        self.assertIn(" Fwd Packet Length Max", report.raw_columns)
        self.assertIn("Fwd Packet Length Max", report.normalized_columns)
        self.assertIn("Fwd Packet Length Max", report.extra_columns)

    def test_repeated_raw_columns_are_detected_safely(self) -> None:
        csv_text = textwrap.dedent(
            """\
            Flow ID,Source IP,Source Port,Destination IP,Destination Port,Protocol,Timestamp,Label,Fwd Header Length,Fwd Header Length.1
            abc,10.0.0.1,1234,10.0.0.2,80,6,2017-07-03 09:00:01,BENIGN,40,40
            """
        )
        with tempfile.NamedTemporaryFile("w", suffix=".csv", encoding="utf-8", delete=False) as handle:
            handle.write(csv_text)
            path = handle.name

        report = inspect_cic_csvs([path])[0]

        self.assertTrue(report.looks_like_cicflowmeter)
        self.assertIn("Fwd Header Length", report.repeated_raw_columns)
        self.assertEqual(
            report.repeated_raw_columns["Fwd Header Length"],
            ("Fwd Header Length", "Fwd Header Length.1"),
        )

    def test_loader_retries_fallback_encodings_and_reports_encoding_used(self) -> None:
        header = (
            "Flow ID,Source IP,Source Port,Destination IP,Destination Port,Protocol,"
            "Timestamp,Label,Flow Duration\n"
        )
        with tempfile.NamedTemporaryFile("wb", suffix=".csv", delete=False) as handle:
            handle.write(header.encode("ascii"))
            handle.write(
                b"abc,10.0.0.1,1234,10.0.0.2,80,6,2017-07-03 09:00:01,BENIGN\x96TEST,1000\n"
            )
            path = handle.name

        result = load_cic_ids2017_csvs([path])
        report = inspect_cic_csvs([path])[0]

        self.assertEqual(result.total_loaded_records, 1)
        self.assertEqual(result.schema_reports[0].encoding_used, "latin1")
        self.assertEqual(report.encoding_used, "latin1")
        self.assertEqual(
            result.source_file_encodings,
            (f"{Path(path).name}:latin1",),
        )


if __name__ == "__main__":
    unittest.main()
