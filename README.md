# CalibDBReader

![Version 0.9.0](https://img.shields.io/badge/version-0.9.0-blue?style=plastic)
![Language Python 3.13+](https://img.shields.io/badge/python-3.13%2B-orange?style=plastic&logo=python)
![BepiColombo SIMBIO-SYS](https://img.shields.io/badge/BepiColombo-SIMBIO--SYS-blue?style=plastic)
![JUICE JANUS](https://img.shields.io/badge/JUICE-JANUS-blue?style=plastic)
[![DOI](https://zenodo.org/badge/820492051.svg)](https://zenodo.org/doi/10.5281/zenodo.12634122)

`CalibDBReader` loads, queries, validates, and displays calibration databases
used by SIMBIO-SYS and JANUS pipelines.

The database consists of:

- `manifest.json`, containing the database version and instrument;
- a CSV index such as `calib_db.csv`;
- calibration files referenced by the CSV `File` column.

Values in the CSV `File` column are paths relative to the database folder, for
example `data/response/stc/calibration.dat`.

## Installation

### From GitHub

To add the package directly from GitHub with uv:

```bash
uv add git+https://github.com/RomoloPoliti-INAF/Calib_DB_Reader.git
```

### Development

Create the local environment and install all development dependencies:

```bash
uv sync
```

Run the tests and build the distributions with:

```bash
uv run pytest -q
uv run ruff check src tests
uv build
```

The package installs the `calibDB` command:

```bash
uv run calibDB version
uv run calibDB version /path/to/calibration/database
uv run calibDB about
uv run calibDB dbdisplay /path/to/calibration/calib_db.csv --check
```

The optional path passed to `version` may be either the database folder or its
CSV file. When provided, the command also displays the database version and
instrument from `manifest.json`.

## Python usage

```python
from CalibDBReader import CalibDB

database = CalibDB(
    folder="/path/to/database",
    dbname="calib_db",
    check_git=False,
    check=True,
)
```

Constructor arguments:

- `folder`: local database directory;
- `remote`: optional Git repository cloned when the folder is missing;
- `check_git`: verify that the database directory is a Git repository;
- `dbname`: CSV filename, with or without the `.csv` extension;
- `check`: verify every calibration path as
  `<database folder>/<File column value>`; a missing file raises
  `FileNotFoundError`.

Absolute values in the `File` column are rejected. If `folder` does not exist
and `remote` is supplied, the library clones the repository.

### Display API

```python
import polars as pl

from CalibDBReader import database_to_rich, dbdisplay
from CalibDBReader.display import dataframe_to_rich

table = dataframe_to_rich(pl.DataFrame({"name": ["STC"], "version": ["1.1"]}))
database_table = database_to_rich("/path/to/database/calib_db.csv", check=True)
dbdisplay("/path/to/database/calib_db.csv", check=True)
```

## Methods list

### get_calib

Returns information about the calibration data and the calibration data that meets the specified conditions.

```python
database.get_calib(
    calibration_step: str,
    date: datetime,
    channel: str | None = None,
    filter: int | None = None,
    read_data: bool = False,
) -> dict
```

- **calibration_step (str):** name of the calibration module
- **date (datetime):** acquisition date of the product to calibrate
- **channel (str, optional):** channel to calibrate. Defaults to None.
- **filter (int, optional):** filter to calibrate. Defaults to None.
- **read_data (bool, optional):** read the calibration file and add it to the returned dictionary. Defaults to False.

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
- **File** is the calibration file path relative to the database directory.
- **Arrays** is the name of the matrices in the npz file. If the field is not present the software will try to extract one matrix named *data*

Supported file formats:

- **binary file**: 2D or 3D binary matrix;
- **NumPy compressed file**: `.npz`; in this case `Size` and `Type` are
  descriptive.

See [`CHANGELOG.md`](CHANGELOG.md) for release notes.
