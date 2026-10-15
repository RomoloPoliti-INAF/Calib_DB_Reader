# Calibration Database Reader
![Version 1.2.2-dev.2](https://img.shields.io/badge/version-1.2.2--dev.2-blue?style=plastic)
![Language Python 3.14](https://img.shields.io/badge/python-3.14-orange?style=plastic&logo=python)
![BepiColombo SIMBIO-SYS](https://img.shields.io/badge/BepiColombo-SIMBIO--SYS-blue?style=plastic)
![JUICE JANUS](https://img.shields.io/badge/JUICE-JANUS-blue?style=plastic)
[![DOI](https://zenodo.org/badge/820492051.svg)](https://zenodo.org/doi/10.5281/zenodo.12634122)

This software is able to read the calibration database in a standard format.

Current development version: `1.2.2-dev.2`.

The database consists of three elements:
- The `version.yml` file, which contains the version number of the database
- The `calib_db.csv` file, which contains information about the calibration data and their scope of applicability (see the specific [section](#database-fields) for details)
- The `data` folder, which contains the calibration matrices

## Installation

### With uv

To create the development environment and install all dependencies:

```console
$ uv sync --locked
```

Run commands inside the managed environment with `uv run`, for example:

```console
$ uv run --locked pytest
$ uv run --locked ruff check .
```

uv creates the local `.venv` environment and installs the development group by
default. After changing dependencies in `pyproject.toml`, run `uv lock` and
commit both files. Build the wheel and source distribution with `uv build`.

### via GitHub

To install this development branch directly from GitHub into a uv project:

```console
$ uv add "CalibDBReader @ git+https://github.com/RomoloPoliti-INAF/Calib_DB_Reader.git@Version_1.0"
```

## Usage

> CalibDB(folder, remote=None, check_git=True, dbname="calib_db", check=False)

where:

- **folder (str)** is the the folder that host the database
- **remote (str, optional)** is the git repository that hosts the database.
- **check_git (bool)** controls repository validation.
- **dbname (str or Path)** selects the database CSV. Both `calib_db` and
  `calib_db.csv` are accepted, so callers can pass configuration paths without
  producing a duplicated `.csv.csv` suffix.
- **check (bool)** enables referenced-file validation.

If the folder does not exist, the software will clone the remote repository. If it is not present, an error will occur.

### Example

```python
from CalibDBReader import CalibDB
db=CalibDB(folder = "../../JANUS/Software/janus_cal_db",
            remote = "git@github.com:JANUS-JUICE/janus_cal_db.git",
            dbname = "calib_db.csv")
```

Calibration CSV files are loaded as Polars dataframes. CSV calibration
products returned with `read_data=True` are also represented as Polars
dataframes.

Known reader issues found by the Calibrator-wide audit are tracked with
`SIMCAL-010` through `SIMCAL-012` in `../SimCal/TODO.md`. The source
contains matching TODO markers; this development increment documents them but
does not change reader behaviour.

## SIMBIO-SYS calibrator integration

SimCal and the HRIC, STC, and VIHI packages configure one instrument database
root plus its CSV basename. The newer SPICE `kernel_folder` and
`MetakernelInfo` settings belong to `simbio_cal_core` and do not change this
reader's database contract. Continue passing the instrument root and
`sim_<instrument>_cal_db_v1.0.csv` independently.

## Methods list

### get_calib

Returns information about the calibration data and the calibration data that meets the specified conditions.

> CalibDBReader.get_calib(calibration_step: str, date: datetime, channel: str = None, filter: int = None, read_data: bool = False) -> dict

- **calibration_step (str):** name of the calibration module
- **date (datetime):** acquisition date of the product to calibrate
- **channel (str, optional):** channel to calibrate. Defaults to None.
- **filter (int, optional):** filter to calibrate. Defaults to None.
- **read_data (bool, optional):** read the calibration file and add it to the returned dictionary. Defaults to False.

### Data cache

Each `CalibDB` instance caches products loaded with `read_data=True`, including
their label metadata and sensor orientation. Reuse the same instance inside a
processing loop: the matching database row is selected on every call, while an
already loaded product is reused without reading its data or label again.
Cache entries distinguish the resolved file path, data type, size, and array
selection. Calls with `read_data=False` neither load nor populate the cache.
The same behaviour applies to `return_class=True`.

```python
db = CalibDB(folder="path/to/database", check_git=False)
for date in acquisition_dates:
    calib = db.get_calib("flat", date, read_data=True)
    matrix = calib["Data"]
```

Cached NumPy arrays are read-only. Use `matrix.copy()` (or
`calib["Data"][array_name].copy()` for a multi-array product) when a writable
matrix is needed. Returned dictionaries and Polars DataFrames have independent
containers, so replacing their entries or columns does not change cached data.

Files and labels are assumed unchanged during processing; changes are not
detected automatically. After updating them, call `db.clear_cache()` to release
all cached entries and make the next request read them again. This method does
not reload the database CSV or manifest; create a new `CalibDB` instance if those
change. Previously returned data remain valid after clearing the cache.

The cache has no automatic eviction: loaded products remain in memory until
`clear_cache()` is called or the instance is released.

## Database Fields

- **Description** Description of the step (used for messaging and log)
- **Channel** the channel to calibrate
- **Calibration_Step** name of the step will use the matrix
- **Size** size of the matrix (used for load the data)
- **Mask** value used to mask the pixel. If the value is 0 the mask will not applied.
- **Type** Is the data type of the values in the matrix.
- **Func** is the type of function will be used for the calibration. The function must be implemened in the code.
- **Filter** is the filter number
- **Start** is the start date of validity of the matrix
- **End** is the end date of validity of the matrix. If the value is "*Now*" means that there is no end of validity.
- **File** is the path and the the name of the file containing the matrix.
- **Arrays** is the name of the matrices in the npz file. If the field is not present the software will try to extract one matrix named *data*

The Supported file format are:

- **binary file** 2D or 3D binary matryx
- **numpy compressed file** nmz file, in this case the *Size* and *Type* fields have only descriptive aim.
