from pathlib import Path

import git
import pytest

from CalibDBReader import CalibDB


def test_folder_none():
    with pytest.raises(ValueError) as e:
        CalibDB(None)
    assert str(e.value) == "folder cannot be None"


def test_folder_not_exists():
    with pytest.raises(FileNotFoundError) as e:
        CalibDB("test_folder")
    assert str(e.value) == "folder test_folder does not exist, please provide a remote"


def test_folder_not_exists_remote_error(tmp_path):
    with pytest.raises(git.exc.GitError):
        CalibDB(tmp_path / "test_folder", remote=str(tmp_path / "missing.git"))


def test_folder_not_exists_remote(tmp_path, calibration_db_remote: Path):
    CalibDB(tmp_path / "test_folder", remote=str(calibration_db_remote))


def test_check_accepts_existing_calibration_files(calibration_db_remote: Path):
    database = CalibDB(calibration_db_remote, check_git=False, check=True)

    assert database.check is True


def test_check_raises_for_missing_calibration_file(calibration_db_remote: Path):
    missing_file = calibration_db_remote / "data" / "test.dat"
    missing_file.unlink()

    with pytest.raises(FileNotFoundError, match=str(missing_file)):
        CalibDB(calibration_db_remote, check_git=False, check=True)


def test_check_is_disabled_by_default(calibration_db_remote: Path):
    (calibration_db_remote / "data" / "test.dat").unlink()

    database = CalibDB(calibration_db_remote, check_git=False)

    assert database.check is False


def test_check_requires_file_column(tmp_path: Path):
    (tmp_path / "calib_db.csv").write_text(
        "Calibration_Step,Size,Start,End\ntest,1-1,2020-01-01,Now\n"
    )
    (tmp_path / "manifest.json").write_text('{"version": "1.0", "instrument": "STC"}\n')

    with pytest.raises(ValueError, match="no 'File' column"):
        CalibDB(tmp_path, check_git=False, check=True)


def test_folder_exists_not_dir(tmp_path):
    d = tmp_path / "test_folder"
    d.write_text("test")
    with pytest.raises(NotADirectoryError):
        CalibDB(d)


def test_folder_exists_not_repo(tmp_path):
    d = tmp_path / "test_folder"
    d.mkdir()
    with pytest.raises(git.exc.GitError):
        CalibDB(d)


def test_version(cdb):
    assert cdb.version == "1.0"


def test_instrument(cdb):
    assert cdb.instrument == "JANUS"


def test_get_calib_resolves_file_relative_to_database_folder(cdb):
    calibration = cdb.get_calib("test", cdb.db.iloc[0]["Start"])

    assert calibration["File"] == cdb.folder / "data" / "test.dat"
