# CalibDBReader changelog

All notable changes to CalibDBReader are documented in this file.

## Unreleased

### Changed

- Advance the development version to `1.2.2-dev.1`.
- Add source markers for audit findings concerning empty query results,
  database-root path containment, and missing PDS labels.

### Documentation

- Clarify compatibility with the Calibrator-wide kernel-folder and
  `MetakernelInfo` integration: database roots, indexes, and reader APIs are
  unchanged.
- Reference the detailed cross-repository issue register in `SimCal/TODO.md`.

## Version 1.2.1 - 2026-07-20

### Changed

- Accept database names as strings or `Path` objects, with or without the
  `.csv` suffix.
- Exclude historical build artifacts from Ruff source checks.

### Fixed

- Prevent a configured filename such as `sim_stc_cal_db_v1.0.csv` from being
  expanded to `sim_stc_cal_db_v1.0.csv.csv`.
- Read the LVID from namespaced PDS4 labels associated with CSV calibration
  products.

## Version 1.1.0

- Migrated database and CSV dataframes from Pandas to Polars.

## Version 1.0.3

- Fixed array-shape handling.

## Version 1.0.2

- Fixed database reader defects.

## Version 1.0.0

- Added the optional `CalibrationProduct` class.
- Added the `lvid` field.

## Version 0.4.0

- Migrated project packaging to Poetry.

## Version 0.3.0

- Added the optional `check_git` switch.

## Version 0.2.0

- Added NumPy compressed-file (`.npz`) support.
- Added the `Arrays` field for named arrays in NumPy archives.

## Version 0.1.1

- Converted the `File` field from `str` to `Path`.

## Version 0.1.0

- First release.
