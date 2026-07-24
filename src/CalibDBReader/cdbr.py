import json
from dataclasses import dataclass
from datetime import datetime
from importlib.metadata import version as get_version
from pathlib import Path
from xml.dom.minidom import parse

import git
import numpy as np
import pds4_tools
import polars as pl
from myxmltools import getFromXml
from rich import print
from semantic_version_tools import Vers

version = Vers(get_version("CalibDBReader"))
__version__ = version.full()

# __version__ = "0.6.0"


def is_git_repo(path):
    try:
        _ = git.Repo(path).git_dir
        return True
    except git.exc.InvalidGitRepositoryError:
        return False


@dataclass
class CalibrationProduct:
    """Class to represent a calibration product"""

    calibration_step: str
    start: datetime
    end: datetime
    channel: str | None = None
    filter: int | None = None
    file: Path | None = None
    type: str | None = None
    size: list[int] | None = None
    arrays: list[str] | str | None = None
    data: np.ndarray | dict[str, np.ndarray] | pl.DataFrame | None = None
    lvid: str | None = None


class CalibDB:
    """Calibration Database Reader"""

    """TODO:
        - add support for PyPI packages
        - check the version of the database and compare with the remote (git or pip)
        - add instruments nicknames
    """

    def __init__(
        self,
        folder: str | Path = None,
        remote: str = None,
        check_git: bool = True,
        dbname: str = "calib_db",
        check: bool = False,
    ):
        """
        Read the database from a folder or clone it from a remote repository

        Args:
            folder (str | Path, optional): folder of the database.
            remote (str, optional): URL of the remote repository. Defaults to None.

        """
        # folder_not_exists = False
        database_name = str(dbname)
        self.dbname = (
            database_name
            if database_name.lower().endswith(".csv")
            else f"{database_name}.csv"
        )
        self.check = check
        self.check_git = check_git
        if folder is None:
            raise ValueError("folder cannot be None")
        elif not isinstance(folder, Path):
            folder = Path(folder)
        self.folder = folder
        if not folder.exists():
            if remote is None:
                raise FileNotFoundError(
                    f"folder {folder} does not exist, please provide a remote"
                )
            else:
                folder.mkdir(parents=True, exist_ok=True)
                git.Repo.clone_from(url=remote, to_path=folder)
                self._datainit(folder)
        else:
            if not folder.is_dir():
                raise NotADirectoryError(f"{folder} is not a directory")
            if not is_git_repo(folder) and check_git:
                raise git.exc.GitError(f"{folder} is not a git repository")
            else:
                self._datainit(folder)
            # self.check_git = check_git

    def convert_size(self, value: str) -> list:
        """Convert the Size field to a list of integers"""
        return list(map(int, value.split("-")))

    def convert_date(self, value: str) -> datetime:
        """Convert the date field to a datetime object"""
        return datetime.strptime(value, "%Y-%m-%d")

    def convert_date_now(self, value: str) -> datetime:
        """Convert the end date field to a datetime object, if the value is 'Now' return the current time"""
        if value == "Now":
            return datetime.now()
        return self.convert_date(value)

    def convert_filter(self, value: str) -> int:
        """Convert the filter field to an integer, if the value is 'all' return 0"""
        if value == "all":
            return 0
        else:
            return int(value)

    def array_analysis(self, value: str) -> list | str:
        """Convert the Arrays field to a list of strings"""
        items = value.split("-")
        for i, item in enumerate(items):
            if "|" in item:
                temporary_array = item.split("|")
                items[i] = [
                    temporary_array[0],
                    int(temporary_array[1]),
                    int(temporary_array[2]),
                ]
            else:
                items[i] = [item, i, i + 1]
        return items

    def convert_arrays(self, value: str) -> list | str:
        """Convert the Arrays field to a list of strings"""
        if "-" in value:
            return self.array_analysis(value)
        else:
            if value == "Null":
                return "Null"
        return value

    def _datainit(self, folder):
        """Load the dabase from the CSV file and the version from the version.yml file"""
        db_file = folder.joinpath(self.dbname)
        if not db_file.exists():
            raise FileNotFoundError(
                f"{db_file} does not exist. Not a valid calib_db folder"
            )
        self.db = pl.read_csv(db_file, infer_schema=False)
        self.db = self.db.with_columns(
            pl.col("Size").str.split("-").list.eval(pl.element().cast(pl.Int64)),
            pl.col("Start").str.strptime(pl.Datetime, format="%Y-%m-%d"),
            pl.col("End").map_elements(self.convert_date_now, return_dtype=pl.Datetime),
        )
        if "Filter" in self.db.columns:
            self.db = self.db.with_columns(
                pl.col("Filter").replace("all", "0").cast(pl.Int64)
            )
        manifest_file = folder / "manifest.json"
        if not manifest_file.is_file():
            raise FileNotFoundError(
                f"{manifest_file} does not exist. Not a valid calib_db folder"
            )
        manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
        self.version = manifest["version"]
        self.instrument = manifest["instrument"]
        if self.check:
            self._check_calibration_files()

    def _calibration_file_path(self, file_value: str) -> Path:
        """Return the effective path for a calibration file."""
        # TODO SIMCAL-011: Reject relative paths that escape the database root.
        relative_path = Path(str(file_value).strip())
        if relative_path.is_absolute():
            raise ValueError(
                f"Calibration file path '{file_value}' must be relative to "
                f"'{self.folder}'"
            )
        return self.folder / relative_path

    def _check_calibration_files(self) -> None:
        """Verify all files referenced by the calibration database."""
        if "File" not in self.db.columns:
            raise ValueError("The calibration database has no 'File' column.")

        for row_index, file_value in enumerate(self.db.get_column("File")):
            if file_value is None or not str(file_value).strip():
                raise FileNotFoundError(
                    f"Calibration file is missing from the File column at row "
                    f"{row_index + 2}"
                )

            calibration_file = self._calibration_file_path(file_value)
            if not calibration_file.is_file():
                raise FileNotFoundError(
                    f"Calibration file '{calibration_file}' does not exist"
                )

    def __str__(self):
        return f"CalibDB: {self.version} for {self.instrument}"

    def __repr__(self):
        return f"CalibDB: {self.version} for {self.instrument}"

    def get_calib(
        self,
        calibration_step: str,
        date: datetime | str,
        channel: str = None,
        filter: int = None,
        read_data: bool = False,
        return_class: bool = False,
        debug: bool = False,
    ) -> dict:
        """
        Get the Calibrauion File for a given module, date, channel and filter

        Args:
            calibration_step (str): name of the calibration module
            date (datetime): acquisition date of the product to calibrate
            channel (str, optional): channel to calibrate. Defaults to None.
            filter (int, optional): filter to calibrate. Defaults to None.
            read_data (bool, optional): read the calibration file and add it to the returned dictionary. Defaults to False.

        Returns:
            dict: Dictionary with all the information of the calibration file and the data if read_data is True
        """
        df = self.db
        if date is None:
            raise ValueError("A product acquisition date is required")
        if isinstance(date, str):
            date = self.convert_date(date)
        elif not isinstance(date, datetime):
            raise TypeError(
                "date must be a datetime or YYYY-MM-DD string, "
                f"got {type(date).__name__}"
            )
        if debug:
            print(f"Calibration Step: {calibration_step}")

        module_mask = df["Calibration_Step"] == calibration_step

        if debug:
            print(f"Filtering on module: {module_mask}")

        if debug:
            print(f"Filtering on date: {date}")

        date_mask = (df["Start"] <= date) & (df["End"] >= date)

        if "Channel" in df.columns and channel is not None:
            channel_mask = df["Channel"] == channel
        else:
            channel_mask = True
        if debug:
            print(f"Filtering on channel: {channel}")

        if "Filter" in df.columns and filter is not None:
            filter_mask = df["Filter"] == filter
        else:
            filter_mask = True
        if debug:
            print(f"Filtering on filter: {filter}")

        if debug:
            print(f"Module mask: {module_mask}")
            print(f"Date mask: {date_mask}")
            print(f"Channel mask: {channel_mask}")
            print(f"Filter mask: {filter_mask}")
        # TODO SIMCAL-010: Raise a domain-specific error when no row matches.
        ret = df.filter(
            module_mask & date_mask & channel_mask & filter_mask
        ).to_dicts()[0]
        if "Arrays" in ret and ret["Arrays"] is not None:
            ret["Arrays"] = self.convert_arrays(ret["Arrays"])
        if read_data:
            fileName = self.folder.joinpath(ret["File"])
            if fileName.suffix == ".npz":
                if "Arrays" in df.columns:
                    mtx = {}
                    with np.load(fileName) as data:
                        for item in ret["Arrays"]:
                            mtx[item] = data[item]
                else:
                    with np.load(fileName) as data:
                        mtx = data["Data"]
            elif fileName.suffix == ".csv":
                mtx = pl.read_csv(fileName)
                pds_label = fileName.with_suffix(".lblx")
                if pds_label.exists():
                    tree = parse(str(pds_label))
                    ret["LVID"] = getFromXml(tree, "pds:logical_identifier")

            else:
                if fileName.suffix == ".dat":
                    # TODO SIMCAL-012: Handle a missing sibling label explicitly.
                    pds_label = fileName.with_suffix(".lblx")
                    if pds_label.exists():
                        tree = parse(str(pds_label))
                        ret["LVID"] = getFromXml(tree, "pds:logical_identifier")
                # mtx_temp = np.fromfile(
                #     self.folder.joinpath(ret["File"]), dtype=ret["Type"]
                # )
                # mtx_temp = mtx_temp.reshape(ret["Size"])
                    info = pds4_tools.read(str(pds_label), quiet=True)
                    if "Arrays" in df.columns and ret["Arrays"] != "Null":
                        mtx = {}
                        
                        for item in info.structures:
                            mtx[item.id] = item.data

                    else:
                        mtx = info.structures[0].data #mtx_temp.reshape(ret["Size"])
            ret["Data"] = mtx
        ret["File"] = self.folder.joinpath(ret["File"])
        if return_class:
            ret = CalibrationProduct(
                calibration_step=ret["Calibration_Step"],
                start=ret["Start"],
                end=ret["End"],
                channel=ret.get("Channel", None),
                filter=ret.get("Filter", None),
                file=ret["File"],
                type=ret.get("Type", None),
                size=ret.get("Size", None),
                arrays=ret.get("Arrays", None),
                data=ret.get("Data", None),
                lvid=ret.get("LVID", None),
            )

        return ret
