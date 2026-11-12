import json
from dataclasses import dataclass, field, fields
from datetime import UTC, datetime
from importlib.metadata import version as get_version
from pathlib import Path
from xml.dom.minidom import parse

import git
import numpy as np
import pds4_tools
import polars as pl
import yaml
from rich import print
from semantic_version_tools import Vers

__version__ = Vers(get_version("CalibDBReader")).full()

NPY_PLACEHOLDER_LIDVID = "urn:placeholder:calibration:npy::0.0"
NPZ_PLACEHOLDER_LIDVID = "urn:placeholder:calibration:npz::0.0"

# __version__ = "0.6.0"


def _get_pds_lvid(tree) -> str:
    """Return the LIDVID from a PDS label, regardless of its XML prefix."""
    logical_identifier_nodes = tree.getElementsByTagNameNS("*", "logical_identifier")
    if not logical_identifier_nodes:
        raise ValueError("Tag 'logical_identifier' not found in PDS label")

    logical_identifier = "".join(
        child.data
        for child in logical_identifier_nodes[0].childNodes
        if child.nodeType in (child.TEXT_NODE, child.CDATA_SECTION_NODE)
    ).strip()
    if not logical_identifier:
        raise ValueError("Tag 'logical_identifier' is empty in PDS label")

    version_nodes = tree.getElementsByTagNameNS("*", "version_id")
    if not version_nodes:
        return logical_identifier

    version_id = "".join(
        child.data
        for child in version_nodes[0].childNodes
        if child.nodeType in (child.TEXT_NODE, child.CDATA_SECTION_NODE)
    ).strip()
    if not version_id:
        return logical_identifier

    lid = logical_identifier.split("::", maxsplit=1)[0]
    return f"{lid}::{version_id}"


def _text(node) -> str:
    return "".join(
        child.data
        for child in node.childNodes
        if child.nodeType in (child.TEXT_NODE, child.CDATA_SECTION_NODE)
    ).strip()


def _horizontal_display_directions(tree) -> dict[str, str]:
    """Map each array's local_identifier to its disp:horizontal_display_direction."""
    directions = {}
    for settings in tree.getElementsByTagNameNS("*", "Display_Settings"):
        references = settings.getElementsByTagNameNS("*", "local_identifier_reference")
        direction = settings.getElementsByTagNameNS("*", "horizontal_display_direction")
        if references and direction:
            directions[_text(references[0])] = _text(direction[0])
    return directions


def _to_sensor_order(structure, directions: dict[str, str], pds_label: Path):
    """Return a calibration array in detector readout (sensor) order.

    Up to calib DB 1.4 the arrays are stored in sensor order and labelled
    "Right to Left". From 1.5 (ESA review #88) they are stored in the
    archived raw images' orientation, mirrored left-right, and labelled
    "Left to Right": flip the axis named Sample back. The label decides, so
    every DB version is read the same way; an image array (Line and Sample
    axes) without Display_Settings is an error rather than a guess.
    """
    axes = structure.meta_data.get("Axis_Array", [])
    if isinstance(axes, dict):
        axes = [axes]
    axis_names = [axis["axis_name"] for axis in axes]
    if "Line" not in axis_names or "Sample" not in axis_names:
        return structure.data

    local_id = structure.meta_data.get("local_identifier", structure.id)
    direction = directions.get(local_id)
    if direction is None:
        raise ValueError(
            f"Array '{local_id}' in PDS label '{pds_label}' has no "
            "Display_Settings: its orientation is unknown"
        )
    if direction == "Left to Right":
        return np.flip(structure.data, axis=axis_names.index("Sample"))
    return structure.data


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
    filter: int | str | None = None
    file: Path | None = None
    type: str | None = None
    size: list[int] | None = None
    arrays: list[str] | str | None = None
    data: np.ndarray | dict[str, np.ndarray] | pl.DataFrame | None = None
    lvid: str | None = None
    instrument_mode: str | None = None
    extra_metadata: dict[str, object] = field(default_factory=dict)

    def __getattr__(self, name: str) -> object:
        metadata = self.__dict__.get("extra_metadata", {})
        if name in metadata:
            return metadata[name]
        matches = [
            value for column, value in metadata.items()
            if column.casefold().replace(" ", "_") == name
        ]
        if len(matches) == 1:
            return matches[0]
        raise AttributeError(name)


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
        """Return the last second of the UTC end date, or the current UTC time."""
        if value == "Now":
            return datetime.now(UTC).replace(tzinfo=None)
        return self.convert_date(value).replace(hour=23, minute=59, second=59)

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
        """Load the catalog CSV and its version metadata."""
        db_file = folder.joinpath(self.dbname)
        if not db_file.exists():
            raise FileNotFoundError(
                f"{db_file} does not exist. Not a valid calib_db folder"
            )
        self.db = pl.read_csv(db_file, infer_schema=False)
        self._now_rows = (self.db["End"] == "Now").fill_null(False)
        self.db = self.db.with_columns(
            pl.col("Size").str.split("-").list.eval(pl.element().cast(pl.Int64)),
            pl.col("Start").str.strptime(pl.Datetime, format="%Y-%m-%d"),
            pl.col("End").map_elements(self.convert_date_now, return_dtype=pl.Datetime),
        )
        if "Filter" in self.db.columns:
            filters = self.db["Filter"].replace("all", "0")
            numeric_filters = filters.cast(pl.Int64, strict=False)
            if numeric_filters.null_count() == filters.null_count():
                self.db = self.db.with_columns(numeric_filters.alias("Filter"))
            else:
                self.db = self.db.with_columns(filters.alias("Filter"))
        manifest_file = folder / "manifest.json"
        legacy_metadata = folder / "version.yml"
        if manifest_file.is_file():
            manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
        elif legacy_metadata.is_file():
            manifest = yaml.safe_load(legacy_metadata.read_text(encoding="utf-8"))
        else:
            raise FileNotFoundError(
                f"{manifest_file} does not exist. Not a valid calib_db folder"
            )
        self.version = manifest["version"]
        self.instrument = manifest["instrument"]
        if self.check:
            self._check_calibration_files()

    def update_now(self) -> None:
        """Refresh the end time of catalog rows declared as Now."""
        now = self.convert_date_now("Now")
        self.db = self.db.with_columns(
            pl.when(self._now_rows).then(pl.lit(now)).otherwise(pl.col("End")).alias("End")
        )

    def _calibration_file_path(self, file_value: str) -> Path:
        """Return the effective path for a calibration file."""
        relative_path = Path(str(file_value).strip())
        if relative_path.is_absolute():
            raise ValueError(
                f"Calibration file path '{file_value}' must be relative to "
                f"'{self.folder}'"
            )
        return self._check_database_path(self.folder / relative_path)

    def _check_database_path(self, path: Path) -> Path:
        if not path.resolve().is_relative_to(self.folder.resolve()):
            raise ValueError(f"Calibration path '{path}' is outside the database directory")
        return path

    def _read_dat_label(self, payload: Path, label: Path):
        if not label.is_file():
            raise FileNotFoundError(f"PDS label '{label}' does not exist")

        tree = parse(str(label))
        file_nodes = tree.getElementsByTagNameNS("*", "file_name")
        if not file_nodes or any(
            (label.parent / _text(node)).resolve() != payload.resolve()
            for node in file_nodes
        ):
            raise ValueError(f"PDS label '{label}' must reference '{payload.name}'")
        return tree

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

            if calibration_file.suffix.lower() == ".dat":
                label = self._check_database_path(calibration_file.with_suffix(".lblx"))
                self._read_dat_label(calibration_file, label)

    def __str__(self):
        return f"CalibDB: {self.version} for {self.instrument}"

    def __repr__(self):
        return f"CalibDB: {self.version} for {self.instrument}"

    def get_calib(
        self,
        calibration_step: str,
        date: datetime | str,
        channel: str = None,
        filter: int | str | None = None,
        read_data: bool = False,
        return_class: bool = False,
        debug: bool = False,
        *,
        instrument_mode: str | None = None,
        temperature: float | None = None,
        ibr: int | None = None,
        rt_mode: str | None = None,
        it: float | None = None,
        it_tol: float = 1e-6,
    ) -> dict:
        """
        Get the Calibrauion File for a given module, date, channel and filter

        Args:
            calibration_step (str): name of the calibration module
            date (datetime | str): acquisition date. Values without a timezone are UTC.
            channel (str, optional): channel to calibrate. Defaults to None.
            filter (int | str, optional): filter to calibrate. Defaults to None.
            read_data (bool, optional): read the calibration file and add it to the returned dictionary. Defaults to False.

            instrument_mode (str, optional): exact value of the Instrument_Mode column.

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
        # Catalog timestamps omit the timezone and represent UTC.
        if date.utcoffset() is not None:
            date = date.astimezone(UTC).replace(tzinfo=None)

        if debug:
            print(f"Calibration Step: {calibration_step}")

        module_mask = df["Calibration_Step"] == calibration_step

        if debug:
            print(f"Filtering on module: {module_mask}")

        if debug:
            print(f"Filtering on date: {date}")

        date_mask = (df["Start"] <= date) & (df["End"] >= date)

        if channel is not None:
            if "Channel" not in df.columns:
                raise ValueError("Calibration catalog has no Channel column")
            channel_mask = df["Channel"] == channel
        else:
            channel_mask = True
        if debug:
            print(f"Filtering on channel: {channel}")

        if filter is not None:
            if "Filter" not in df.columns:
                raise ValueError("Calibration catalog has no Filter column")
            filter_value = str(filter) if df.schema["Filter"] == pl.String else filter
            filter_mask = df["Filter"] == filter_value
        else:
            filter_mask = True
        if debug:
            print(f"Filtering on filter: {filter}")

        if debug:
            print(f"Module mask: {module_mask}")
            print(f"Date mask: {date_mask}")
            print(f"Channel mask: {channel_mask}")
            print(f"Filter mask: {filter_mask}")
        matches = df.filter(module_mask & date_mask & channel_mask & filter_mask)
        if instrument_mode is not None:
            if "Instrument_Mode" not in df.columns:
                raise ValueError("Calibration catalog has no Instrument_Mode column")
            matches = matches.filter(pl.col("Instrument_Mode") == instrument_mode)
        criteria = {
            "Temperature": temperature,
            "IBR": ibr,
            "RT_Mode": rt_mode,
            "IT": it,
        }
        for column, value in criteria.items():
            if value is None:
                continue
            if column not in df.columns:
                raise ValueError(f"Calibration catalog has no {column} column")
            field_value = pl.col(column)
            if column == "IT":
                condition = (field_value.cast(pl.Float64, strict=False) - value).abs() <= it_tol
            elif column in ("Temperature", "IBR"):
                condition = field_value.cast(pl.Float64, strict=False) == value
            else:
                condition = field_value == value
            matches = matches.filter(condition)
        if matches.height > 1:
            raise ValueError(
                f"Ambiguous calibration lookup for step {calibration_step!r}, "
                f"date {date} UTC, channel {channel!r}, filter {filter!r}, "
                f"mode {instrument_mode!r}, and STC criteria {criteria}. Found {matches.height} records."
            )
        if matches.is_empty():
            raise ValueError(
                f"No calibration record for step {calibration_step!r}, "
                f"date {date} UTC, channel {channel!r}, filter {filter!r}, "
                f"mode {instrument_mode!r}, and STC criteria {criteria}."
            )
        ret = matches.row(0, named=True)
        if "Arrays" in ret and ret["Arrays"] is not None:
            ret["Arrays"] = self.convert_arrays(ret["Arrays"])
        fileName = self._calibration_file_path(ret["File"])
        suffix = fileName.suffix.lower()
        if suffix == ".npz":
            ret["LVID"] = NPZ_PLACEHOLDER_LIDVID
        elif suffix in {".csv", ".dat", ".npy"}:
            pds_label = self._check_database_path(fileName.with_suffix(".lblx"))
            if suffix == ".dat" and (read_data or pds_label.is_file()):
                tree = self._read_dat_label(fileName, pds_label)
                ret["LVID"] = _get_pds_lvid(tree)
            elif pds_label.is_file():
                ret["LVID"] = _get_pds_lvid(parse(str(pds_label)))
            elif suffix == ".npy" and not ret.get("LVID"):
                ret["LVID"] = NPY_PLACEHOLDER_LIDVID

        if read_data:
            if suffix == ".npz":
                arrays = ret.get("Arrays")
                if arrays not in (None, "Null"):
                    if isinstance(arrays, str):
                        array_names = [arrays]
                    else:
                        array_names = [
                            item[0] if isinstance(item, list) else item
                            for item in arrays
                        ]
                    mtx = {}
                    with np.load(fileName) as data:
                        for array_name in array_names:
                            mtx[array_name] = data[array_name]
                else:
                    with np.load(fileName) as data:
                        mtx = data["Data"]
            elif suffix == ".npy":
                mtx = np.load(fileName, allow_pickle=False)
            elif suffix == ".csv":
                comment_prefix = "#" if calibration_step == "solar_irradiance" else None
                mtx = pl.read_csv(fileName, comment_prefix=comment_prefix)
            elif suffix == ".dat":
                try:
                    info = pds4_tools.read(str(pds_label), quiet=True)
                except Exception as exc:
                    raise ValueError(
                        f"Unable to read data described by PDS label "
                        f"'{pds_label}': {exc}"
                    ) from exc
                if not info.structures:
                    raise ValueError(
                        f"PDS label '{pds_label}' contains no data structures"
                    )

                directions = _horizontal_display_directions(tree)
                arrays = ret.get("Arrays")
                if arrays not in (None, "Null"):
                    mtx = {
                        item.id: _to_sensor_order(item, directions, pds_label)
                        for item in info.structures
                    }
                else:
                    mtx = _to_sensor_order(info.structures[0], directions, pds_label)
                matrices = mtx.values() if isinstance(mtx, dict) else (mtx,)
                expected_size = ret.get("Size")
                expected_type = ret.get("Type")
                for matrix in matrices:
                    if expected_size is not None and matrix.shape != tuple(expected_size):
                        raise ValueError(
                            f"Calibration file '{fileName}' has shape {matrix.shape}, "
                            f"expected {tuple(expected_size)} from CSV Size"
                        )
                    if expected_type is not None and (
                        matrix.dtype.newbyteorder("=")
                        != np.dtype(expected_type).newbyteorder("=")
                    ):
                        raise ValueError(
                            f"Calibration file '{fileName}' has dtype {matrix.dtype}, "
                            f"expected {expected_type} from CSV Type"
                        )
            else:
                raise ValueError(f"Unsupported calibration file format: '{suffix}'")
            ret["Data"] = mtx
        ret["File"] = fileName
        if return_class:
            standard_columns = {"LVID"} | {
                item.name.title()
                for item in fields(CalibrationProduct)
                if item.name not in {"extra_metadata", "lvid"}
            }
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
                instrument_mode=ret.get("Instrument_Mode"),
                extra_metadata={
                    column: value
                    for column, value in ret.items()
                    if column not in standard_columns
                },
            )

        return ret
