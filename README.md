`# IDSaaS Pre-ingest Deduplication

This repository contains the experimental implementation used to evaluate pre-ingestion deduplication for IDSaaS-style uploads. The experiment models duplicate-upload behavior before cloud ingestion and measures how much traffic is removed at the edge before data reaches the gateway or sink.

## What the experiment evaluates

- `no_edge_dedup`: every uploaded batch is transmitted in full
- `edge_metadata`: edge-side deduplication removes full replay batches and trims overlapped prefixes before transmission
- optional `edge_bloom_variant`: retained ablation based on Bloom-plus-exact record deduplication

The main scenarios are:

- `exact_replay`
- `partial_overlap`
- `mixed_upload`

The runner also supports `new_append` as a baseline, no-duplicate scenario.

Duplicate-rate controls supported by the experiment:

- `0`
- `0.25`
- `0.50`
- `0.75`
- `1.00`

## Repository layout

```text
src/                Experiment implementation and CLI runner
src/datasets/       CIC-IDS2017 CSV loading and schema inspection
sample_data/        Bundled sample CIC CSV for smoke tests
results/            Generated experiment outputs
tests/              Unit tests
requirements.txt    Dependency file
```

## Prerequisites

- Python 3.10 or newer
- A shell with `python3` available

The code uses the Python standard library only. No third-party runtime packages are required.

## Create and activate a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

## Install dependencies

```bash
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt
```

## Input data

The bundled smoke-test input is:

- `sample_data/cic_ids2017_sample.csv`

For full experiments, place CIC-IDS2017 / CICFlowMeter CSV files anywhere on disk and pass them through `--csv`. The repo does not require the raw dataset to be committed.

Expected CIC CSV characteristics:

- canonical fields centered on `Flow ID`, `Source IP`, `Source Port`, `Destination IP`, `Destination Port`, `Protocol`, `Timestamp`, and `Label`
- optional additional CICFlowMeter columns are accepted
- multiple CSV files can be supplied with repeated `--csv` flags or a single `--csv` followed by multiple paths

Example full-dataset location:

```bash
sample_data/Monday-WorkingHours.pcap_ISCX.csv
sample_data/Tuesday-WorkingHours.pcap_ISCX.csv
```

## Run a single experiment

This command runs one scenario, one duplicate rate, and one mode:

```bash
python3 -m src.runner \
  --dataset cic_ids2017 \
  --csv sample_data/cic_ids2017_sample.csv \
  --scenario exact_replay \
  --dupe-rate 0.50 \
  --mode edge_metadata \
  --batch-size 3 \
  --output-prefix results/single_run
```

For a baseline comparison, omit `--mode` and the runner will execute both `no_edge_dedup` and `edge_metadata` on the same prepared workload.

## Run all paper-facing scenarios

The `main_test` preset is the paper-facing run configuration. Supply one or more CIC CSV files and let the preset supply the scenario and duplicate-rate schedule:

```bash
python3 -m src.runner \
  -P main_test \
  --dataset cic_ids2017 \
  -c sample_data/Monday-WorkingHours.pcap_ISCX.csv \
  -c sample_data/Tuesday-WorkingHours.pcap_ISCX.csv \
  -c sample_data/Wednesday-workingHours.pcap_ISCX.csv \
  -c sample_data/Thursday-WorkingHours-Afternoon-Infilteration.pcap_ISCX.csv \
  -c sample_data/Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv \
  -c sample_data/Friday-WorkingHours-Morning.pcap_ISCX.csv \
  -c sample_data/Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv \
  -c sample_data/Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv \
  --run-all \
  --output-prefix results/final_all_cic
```

The preset evaluates:

- `exact_replay`
- `partial_overlap`
- `mixed_upload`

at duplicate rates `0`, `0.25`, `0.50`, `0.75`, and `1.00`.

`--run-all` adds the `new_append` baseline to the same workload sweep.

## Quick smoke test

For a very small end-to-end run, use the bundled sample file:

```bash
python3 -m src.runner -P quick_test
```

This is useful for verifying the environment, command syntax, and output generation.

## Inspect CIC schema

```bash
python3 -m src.runner --inspect-cic --csv sample_data/cic_ids2017_sample.csv
```

This prints the resolved CIC columns, alias matches, duplicate raw columns, and schema quality.

## Outputs

Default output location:

- `results/`

Output naming:

- non-preset runs write `<prefix>_results.csv`
- non-preset comparison runs also write `<prefix>_comparison.csv`
- preset runs write `results.csv` and, when applicable, `comparison.csv` inside the output directory
- `--export-json` adds JSON exports alongside the CSV files

Expected columns in the main results CSV include:

- `scenario`
- `mode`
- `total_input_records`
- `total_transmitted_records`
- `total_input_bytes`
- `total_transmitted_bytes`
- `bytes_saved`
- `communication_reduction_pct`
- `accepted_payload_ratio`
- `duplicate_suppression_ratio`
- `processing_time_s`

Comparison exports report:

- `baseline_mode`
- `proposed_mode`
- `baseline_transmitted_bytes`
- `proposed_transmitted_bytes`
- `bytes_saved`
- `communication_reduction_pct`
- `record_reduction_pct`

## How to interpret the metrics

- `transmitted bytes`: payload bytes that crossed the edge boundary
- `bytes saved`: input bytes minus transmitted bytes
- `communication reduction percentage`: saved bytes divided by input bytes
- `processing time`: local Python execution time for the workload
- `throughput`-style values are derivable from total records and processing time if needed

## Testing

Run the unit test suite with:

```bash
python3 -m unittest discover -s tests
```

The tests cover:

- CLI parsing
- CIC CSV loading and schema inspection
- batch construction and deduplication behavior
- experiment execution and export paths

## Troubleshooting

- If `--dataset cic_ids2017` is selected, pass at least one `--csv` path.
- If a CSV uses non-UTF-8 encoding, the loader retries `utf-8-sig`, `utf-8`, `latin1`, and `cp1252` in that order.
- If an output path already exists, rerun with `--overwrite`.
- If you want a clean smoke test before using larger data, run `python3 -m src.runner -P quick_test`.

## Notes

- Generated results stay under `results/` and are ignored by git.
- Local raw CIC CSVs remain outside version control unless you intentionally add them.
