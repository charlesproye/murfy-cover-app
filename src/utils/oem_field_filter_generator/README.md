# OEM Field Filter Generator

Utility to generate OEM field filter files from Google Sheets.

## Purpose

Reads a Google Sheet with one sheet per OEM. Each sheet should have:
- `DATA POINTS` column: field name
- `TO_KEEP` column: TRUE/FALSE indicating whether to keep the field

Creates one text file per OEM in `src/ingestion/data/` with one line per field where `TO_KEEP` is TRUE.

## Setup

Ensure you have the `GOOGLE_PRIVATE_KEY` environment variable set with base64-encoded service account credentials. This is the same credential used by other Google Sheets integrations in the codebase.

## Usage

```bash
# Run from repository root
uv run python -m src.utils.oem_field_filter_generator.generate_fields "Your Sheet ID"

# Specify custom output directory
uv run python -m src.utils.oem_field_filter_generator.generate_fields "Your Sheet ID" --output-dir /path/to/output
```

## Example Input

```
DATA POINTS                                    TO_KEEP
meta                                          TRUE
meta.Vehicle.Body.Hood.Frunk.Fault            FALSE
meta.Vehicle.Body.Hood.Open                   FALSE
meta.Vehicle.Cabin.Door.Row1.Driver.Lock      TRUE
meta.Vehicle.Cabin.Door.Row1.Driver.Open      TRUE
```

## Example Output

For a sheet named "Tesla", creates `src/ingestion/data/tesla.txt`:
```
meta
meta.Vehicle.Cabin.Door.Row1.Driver.Lock
meta.Vehicle.Cabin.Door.Row1.Driver.Open
```
