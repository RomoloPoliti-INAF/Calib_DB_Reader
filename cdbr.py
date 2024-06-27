from datetime import datetime
from pathlib import Path
import numpy as np

import git
import git.cmd
import pandas as pd
import yaml


def is_git_repo(path):
    try:
        _ = git.Repo(path).git_dir
        return True
    except git.exc.InvalidGitRepositoryError:
        return False

class CalibDB:
    
    def __init__(self, folder: str| Path = None,remote: str=None):
        folder_not_exists=False
        if folder is None:
            raise ValueError("folder cannot be None")
        elif not isinstance(folder, Path):
            folder = Path(folder)
        self.folder = folder
        if not folder.exists():
            if remote is None:
                raise FileNotFoundError(f"folder {folder} does not exist, please provide a remote")
            else:
                folder.mkdir(parents=True, exist_ok=True)
                git.Repo.clone_from(url=remote,to_path=folder)
                self._datainit(folder)
        else:
            if not folder.is_dir():
                raise NotADirectoryError(f"{folder} is not a directory")
            if not is_git_repo(folder):
                raise git.exc.GitError(f"{folder} is not a git repository")
            else:
                self._datainit(folder)

    def convert_size(self,value:str)->list:
        return list(map(int, value.split('-')))


    def convert_date(self,value:str)->datetime:
        return pd.to_datetime(value, format='%Y-%m-%d')


    def convert_date_now(self,value:str)->datetime:
        if value == 'Now':
            return pd.Timestamp.now()
        return pd.to_datetime(value, format='%Y-%m-%d')
    
    def convert_filter(self, value:str)->int:
        if value == 'all':
            return 0
        else:
            return int(value)
        

    def _datainit(self,folder):
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
        with open(folder.joinpath("version.yml")) as f:
            sata = yaml.safe_load(f)
        self.version = sata["version"]
        self.instrument = sata["instrument"]
        
    def __str__(self):
        return f"CalibDB: {self.version} for {self.instrument}"
    
    def __repr__(self):
        return f"CalibDB: {self.version} for {self.instrument}"
    
    def get_calib(self, module: str, date: datetime, channel: str = None, filter: int = None,read_data:bool=False):
        df = self.db
        module_mask = df['Calibration_Step'] == module
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
        ret=df[module_mask & date_mask & channel_mask & filter_mask].to_dict(orient='records')[0]
        if read_data:
            mtx = np.fromfile(self.folder.joinpath(ret['File']), dtype=ret['Type'])
            mtx = mtx.reshape(ret['Size'])
            ret['Data'] = mtx
        return ret


    # def get_calib(self, module:str, date: datetime,channel:str=None,filter:int=None):
    #     return self.db[(self.db['Calibration_Step'] == module) &
    #                   (self.db['Start'] <= date) &
    #                   (self.db['End'] >= date)&
    #                   (self.db['Channel']==channel if "Channel" in self.db.columns else True)&
    # (self.db['Filter']==filter if "Filter" in self.db.columns and filter is not None else True)]
    
