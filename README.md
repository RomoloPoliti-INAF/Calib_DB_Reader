# Calibration Database Reader
![Version 0.2.0](https://img.shields.io/badge/version-0.2.0-blue?style=plastic)
![Language Python 3.12](https://img.shields.io/badge/python-3.12-orange?style=plastic&logo=python)
![BepiColombo SIMBIO-SYS](https://img.shields.io/badge/BepiColombo-SIMBIO--SYS-blue?style=plastic)
![JUICE JANUS](https://img.shields.io/badge/JUICE-JANUS-blue?style=plastic)
[![DOI](https://zenodo.org/badge/820492051.svg)](https://zenodo.org/doi/10.5281/zenodo.12634122)

This software is able to read the calibration database in a standard format.

The database consists of three elements:
- The `version.yml` file, which contains the version number of the database
- The `calib_db.csv` file, which contains information about the calibration data and their scope of applicability (see the specific [section](#database-fields) for details)
- The `data` folder, which contains the calibration matrices

## Installation

### via GitHub

To install the code you can use the syntax:

```console
$ python3 -m pip install -U pip
$ pip install git+https://github.com/RomoloPoliti-INAF/Calib_DB_Reader.git
```

## Usage

> CalibDB(folder: str, remote: str)

where:

- **folder (str)** is the the folder that host the database
- **remote (str, optional)** is the git repository that host the db

If the folder does not exist, the software will clone the remote repository. If it is not present, an error will occur.

### Example

```python
from CalibDBReader import CalibDB
db=CalibDB(folder = "../../JANUS/Software/janus_cal_db",
            remote = "git@github.com:JANUS-JUICE/janus_cal_db.git")
```

## Methods list

### get_calib

Returns information about the calibration data and the calibration data that meets the specified conditions.

> CalibDBReader.get_calib(calibration_step: str, date: datetime, channel: str = None, filter: int = None, read_data: bool = False) -> dict

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
- **File** is the path and the the name of the file containing the matrix.
- **Arrays** is the name of the matrices in the npz file. If the field is not present the software will try to extract one matrix named *data*

The Supported file format are:

- **binary file** 2D or 3D binary matryx
- **numpy compressed file** nmz file, in this case the *Size* and *Type* fields have only descriptive aim.