from pathlib import Path

class CalibDB:
    
    def __init__(self, folder: str| Path = None):
        if folder is None:
            raise ValueError("folder cannot be None")
        elif not isinstance(folder, Path):
            folder = Path(folder)
        if not folder.exists():
            raise FileNotFoundError(f"folder {folder} does not exist")
        if not folder.is_dir():
            raise NotADirectoryError(f"{folder} is not a directory")
        self.folder = folder