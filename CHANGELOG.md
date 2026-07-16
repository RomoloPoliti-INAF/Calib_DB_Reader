# Changelog

All notable changes to `CalibDBReader` are documented in this file.

## Unreleased

- Migrated project and dependency management from Poetry/setuptools to uv.
- Added reproducible local test and development dependency groups.
- Made the test suite independent from external Git repositories.
- Added the `calibDB` CLI with `version`, `about`, and `dbdisplay` commands.
- Added reusable Polars-to-Rich and calibration database display functions.
- Added the `CalibDB(check=True)` option to verify calibration files by joining
  the database folder with the value of the `File` column.
- Calibration file paths returned by `get_calib()` are now consistently
  resolved relative to the database folder.
- Replaced the YAML database metadata file with `manifest.json`.
- Added `python -m CalibDBReader` as an alternative CLI invocation.
- Renamed this file from the misspelled `CANGELOG.md` to `CHANGELOG.md`.

## 0.4.0

- Poetry porting

## 0.3.0

- introduced **check_git** option. If *False* do not perform the git check

## 0.2.0

- Support to numpy compressed file format (npz)
- Introduce the field *Arrays* for the contents of the numpy file

## 0.1.1

- Converted File field from str to Path

## 0.1.0

- First release
