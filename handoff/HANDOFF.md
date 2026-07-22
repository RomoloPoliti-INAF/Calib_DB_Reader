# CalibDBReader handoff

Updated: 2026-07-21

Current development version: `1.2.2-dev.1`.

## Repository role

CalibDBReader validates a calibration-database folder, loads its manifest and
CSV index, resolves calibration products by date/channel/filter, and optionally
loads the referenced matrix or table.

## Current contracts

- Python support is `>=3.14,<4`.
- Database indexes and CSV calibration products use Polars.
- `CalibDB.dbname` accepts `str` or `Path`, with or without `.csv`.
- `File` entries are resolved relative to the configured database folder.
- `read_data=True` loads `.dat`, `.csv`, or `.npz` calibration data.
- CSV product labels resolve the namespaced `pds:logical_identifier` field.
- `manifest.json` supplies database identity and version metadata.

The 2026-07-20 Calibrator SPICE integration does not alter these contracts.
Kernel-folder validation and `MetakernelInfo` ownership remain in
`simbio_cal_core`; CalibDBReader continues to receive only database paths and
CSV names.

## Verification

```bash
uv sync
uv run ruff check .
uv run pytest -q
```

Current verification: 14 tests passed, Ruff passed, and source/wheel builds
completed for version `1.2.2-dev.1`.

The source tree is authoritative. The historical `build/` directory is
excluded from Ruff and should not be used as an import source.

## Known audit findings

The central register is `../SimCal/TODO.md`. This repository carries markers
for `SIMCAL-010` (empty calibration selection), `SIMCAL-011`
(relative path containment), and `SIMCAL-012` (DAT without a sibling
label). These are documented defects, not changes included in this handoff.
