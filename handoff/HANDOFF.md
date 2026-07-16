# CalibDBReader handoff

Updated: 2026-07-16

## Repository role

`CalibDBReader` owns calibration database loading, metadata, file validation,
selection, data loading, and Rich display. SimCal and the STC, HRIC, and VIHI
packages consume this repository as an editable uv source during development.

## Database contract

- Metadata is read from `manifest.json`, with `instrument` and `version` keys.
- The CSV name defaults to `calib_db.csv`.
- Every `File` value is relative to the database directory.
- `CalibDB(check=True)` raises `FileNotFoundError` on the first missing file.
- `get_calib(..., read_data=True)` uses the same path-resolution rule.

## Public interfaces

- Python: `CalibDB`, `database_to_rich`, `dbdisplay`, and `display_database`.
- Polars conversion: `CalibDBReader.display.dataframe_to_rich`.
- CLI: `calibDB version [DATABASE]`, `calibDB about`, and
  `calibDB dbdisplay CSV [--check]`.
- Module invocation: `python -m CalibDBReader`.

## Project management

The project uses uv with the `uv_build` backend and requires Python
`>=3.13,<4`. Poetry metadata and the PyYAML dependency have been removed.

## Verification

```bash
uv run pytest -q
uv run ruff check src tests
uv build
```

Current verification: 20 tests passed and source/wheel distributions built.

## Next work

- Add schema validation for required manifest and CSV fields.
- Document binary and NPZ loading with executable examples.
- Decide whether database Git synchronization remains part of the stable API.
