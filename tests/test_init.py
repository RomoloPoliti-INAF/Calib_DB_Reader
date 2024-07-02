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
    assert str(
        e.value) == "folder test_folder does not exist, please provide a remote"


def test_folder_not_exists_remote_error(tmp_path):
    with pytest.raises(git.exc.GitError) as e:
        CalibDB(tmp_path / "test_folder",
                remote="git@github.com:JANUS-JUICE/janus_cali_db.git")


def test_folder_not_exists_remote(tmp_path):
    CalibDB(tmp_path / "test_folder",
            remote="git@github.com:JANUS-JUICE/janus_cal_db.git")


def test_folder_exists_not_dir(tmp_path):
    d = tmp_path / "test_folder"
    d.write_text("test")
    with pytest.raises(NotADirectoryError) as e:
        CalibDB(d)


def test_folder_exists_not_repo(tmp_path):
    d = tmp_path / "test_folder"
    d.mkdir()
    with pytest.raises(git.exc.GitError) as e:
        CalibDB(d)


def test_version(cdb):
    assert cdb.version == "1.0"


def test_instrument(cdb):
    assert cdb.instrument == "JANUS"
