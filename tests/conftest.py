import json

import pytest

from CalibDBReader import CalibDB


def pytest_report_header(config):
    msg = ["Calibration Database Reader Test ..."]
    if config.getoption("verbose") > 0:
        msg.append("Verbose mode enabled.")
    return msg


@pytest.fixture(scope="function")
def cdb(tmp_path):
    database = tmp_path / "test_folder"
    database.mkdir()
    (database / "manifest.json").write_text(
        json.dumps({"version": "1.0", "instrument": "JANUS"}),
        encoding="utf-8",
    )
    (database / "calib_db.csv").write_text(
        "Calibration_Step,Size,Start,End,File,Type\n",
        encoding="utf-8",
    )
    return CalibDB(database, check_git=False)
