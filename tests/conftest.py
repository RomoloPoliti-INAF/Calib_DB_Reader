import pytest

from cdbr import CalibDB


def pytest_report_header(config):
    msg = ["Calibration Database Reader Test ..."]
    if config.getoption("verbose") > 0:
        msg.append("Verbose mode enabled.")
    return msg 

@pytest.fixture(scope="function")
def cdb(tmp_path):
    return CalibDB(tmp_path / "test_folder",
                   remote="git@github.com:JANUS-JUICE/janus_cal_db.git")
