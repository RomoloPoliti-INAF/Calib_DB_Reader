from pathlib import Path

import git
import pytest

from CalibDBReader import CalibDB


def pytest_report_header(config):
    msg = ["Calibration Database Reader Test ..."]
    if config.getoption("verbose") > 0:
        msg.append("Verbose mode enabled.")
    return msg


@pytest.fixture(scope="function")
def calibration_db_remote(tmp_path: Path) -> Path:
    remote = tmp_path / "calibration-db-remote"
    remote.mkdir()
    (remote / "calib_db.csv").write_text(
        "Calibration_Step,Size,Start,End,File\ntest,1-1,2020-01-01,Now,data/test.dat\n"
    )
    (remote / "manifest.json").write_text('{"version": "1.0", "instrument": "JANUS"}\n')
    data_folder = remote / "data"
    data_folder.mkdir()
    (data_folder / "test.dat").touch()

    repository = git.Repo.init(remote)
    repository.index.add(["calib_db.csv", "manifest.json", "data/test.dat"])
    actor = git.Actor("CalibDBReader tests", "tests@example.invalid")
    repository.index.commit(
        "Create test calibration database", author=actor, committer=actor
    )
    return remote


@pytest.fixture(scope="function")
def cdb(tmp_path: Path, calibration_db_remote: Path) -> CalibDB:
    return CalibDB(tmp_path / "test_folder", remote=str(calibration_db_remote))
