from datetime import datetime
from pathlib import Path

import git
import git.cmd
import numpy as np
import pandas as pd
import yaml

__version__ = "0.2.0"


def is_git_repo(path):
    try:
        _ = git.Repo(path).git_dir
        return True
    except git.exc.InvalidGitRepositoryError:
        return False


class CalibDB:
    """Calibration Database Reader"""
    
    """TODO:
        - add support for PyPI packages
        - check the version of the database and compare with the remote (git or pip)
        - add instruments nicknames
    """

    def __init__(self, folder: str | Path = None, remote: str = None):
        """
        Read the database from a folder or clone it from a remote repository

        Args:
            folder (str | Path, optional): folder of the database.
            remote (str, optional): URL of the remote repository. Defaults to None.

        """
        folder_not_exists = False
        if folder is None:
            raise ValueError("folder cannot be None")
        elif not isinstance(folder, Path):
            folder = Path(folder)
        self.folder = folder
        if not folder.exists():
            if remote is None:
                raise FileNotFoundError(
                    f"folder {folder} does not exist, please provide a remote")
            else:
                folder.mkdir(parents=True, exist_ok=True)
                git.Repo.clone_from(url=remote, to_path=folder)
                self._datainit(folder)
        else:
            if not folder.is_dir():
                raise NotADirectoryError(f"{folder} is not a directory")
            if not is_git_repo(folder):
                raise git.exc.GitError(f"{folder} is not a git repository")
            else:
                self._datainit(folder)

    def convert_size(self, value: str) -> list:
        """Convert the Size field to a list of integers"""
        return list(map(int, value.split('-')))

    def convert_date(self, value: str) -> datetime:
        """Convert the date field to a datetime object"""
        return pd.to_datetime(value, format='%Y-%m-%d')

    def convert_date_now(self, value: str) -> datetime:
        """Convert the end date field to a datetime object, if the value is 'Now' return the current time"""
        if value == 'Now':
            return pd.Timestamp.now()
        return pd.to_datetime(value, format='%Y-%m-%d')

    def convert_filter(self, value: str) -> int:
        """Convert the filter field to an integer, if the value is 'all' return 0"""
        if value == 'all':
            return 0
        else:
            return int(value)
        
    def convert_arrays(self, value: str) -> list|str:
        """Convert the Arrays field to a list of strings"""
        if '-' in value:
            return value.split('-')
        else:
            if value == 'Null':
                return 'Null'
        return value

    def _datainit(self, folder):
        """Load the dabase from the CSV file and the version from the version.yml file"""
        db_file = folder.joinpath("calib_db.csv")
        if not db_file.exists():
            raise FileNotFoundError(
                f"{db_file} does not exist. Not a valid calib_db folder")
        self.db = pd.read_csv(db_file)
        self.db['Size'] = self.db['Size'].apply(self.convert_size)
        self.db['Start'] = self.db['Start'].apply(self.convert_date)
        self.db['End'] = self.db['End'].apply(self.convert_date_now)
        if "Filter" in self.db.columns:
            self.db['Filter'] = self.db['Filter'].apply(self.convert_filter)
        if "Arrays" in self.db.columns:
            self.db['Arrays'] = self.db['Arrays'].apply(self.convert_arrays)
        with open(folder.joinpath("version.yml")) as f:
            sata = yaml.safe_load(f)
        self.version = sata["version"]
        self.instrument = sata["instrument"]

    def __str__(self):
        return f"CalibDB: {self.version} for {self.instrument}"

    def __repr__(self):
        return f"CalibDB: {self.version} for {self.instrument}"

    def get_calib(self, calibration_step: str, date: datetime, channel: str = None, filter: int = None, read_data: bool = False) -> dict:
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
        module_mask = df['Calibration_Step'] == calibration_step
        date_mask = (df['Start'] <= date) & (df['End'] >= date)

        if "Channel" in df.columns and channel is not None:
            channel_mask = (df['Channel'] ==
                            channel)
        else:
            channel_mask = True

        if "Filter" in df.columns and filter is not None:
            filter_mask = (df['Filter'] == filter)
        else:
            filter_mask = True
        ret = df[module_mask & date_mask & channel_mask &
                 filter_mask].to_dict(orient='records')[0]
        if read_data:
            fileName = self.folder.joinpath(ret['File'])
            if fileName.suffix == '.npz':
                if "Arrays" in df.columns:
                    mtx = {}
                    with np.load(fileName) as data:
                        for item in ret['Arrays']:
                            mtx[item] = data[item]
                else:
                    with np.load(fileName) as data:
                        mtx = data['Data']
            else:
                mtx = np.fromfile(self.folder.joinpath(
                    ret['File']), dtype=ret['Type'])
                mtx = mtx.reshape(ret['Size'])
            ret['Data'] = mtx
        ret['File'] = self.folder.joinpath(ret['File'])
        return ret
