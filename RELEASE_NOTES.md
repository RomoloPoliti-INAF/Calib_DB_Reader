# Calib_DB_Reader 1.2.2-dev.8

Release date: 21 November 2026.

- Document how to retrieve the HRIC channel-wide distortion table by acquisition date without a filter selector.
- Describe the resolved file path and the Polars DataFrame returned by CSV lookups with `read_data=True`.

# Calib_DB_Reader 1.2.2-dev.5

Release date: 9 November 2026. Previous version: `1.2.2-dev.4`.

## Changes

- Select calibration records by exact `Instrument_Mode` when requested and report missing or ambiguous matches.
- Read solar irradiance CSV tables with leading comment lines and preserve the calibration product LIDVID.
