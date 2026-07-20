import json
from datetime import datetime
from pathlib import Path

import polars as pl

from CalibDBReader import CalibDB


def make_database(tmp_path):
    database = tmp_path / "database"
    database.mkdir()
    (database / "manifest.json").write_text(
        json.dumps({"version": "1.0", "instrument": "TEST"}),
        encoding="utf-8",
    )
    (database / "matrix.csv").write_text("value\n1\n2\n", encoding="utf-8")
    (database / "calib_db.csv").write_text(
        "Calibration_Step,Size,Start,End,Filter,File,Type\n"
        "flat,1-2,2024-01-01,2030-01-01,all,matrix.csv,float64\n",
        encoding="utf-8",
    )
    return database


def test_database_is_loaded_as_polars_dataframe(tmp_path):
    database = CalibDB(make_database(tmp_path), check_git=False)

    assert isinstance(database.db, pl.DataFrame)
    assert database.db.schema["Start"] == pl.Datetime
    assert database.db.schema["End"] == pl.Datetime
    assert database.db.schema["Size"] == pl.List(pl.Int64)
    assert database.db.schema["Filter"] == pl.Int64


def test_database_name_accepts_csv_suffix_and_path(tmp_path):
    database = CalibDB(
        make_database(tmp_path),
        dbname=Path("calib_db.csv"),
        check_git=False,
    )

    assert database.dbname == "calib_db.csv"


def test_get_calib_returns_polars_csv_data(tmp_path):
    database = CalibDB(make_database(tmp_path), check_git=False)

    calibration = database.get_calib(
        "flat", datetime(2025, 1, 1), filter=0, read_data=True
    )

    assert calibration["Size"] == [1, 2]
    assert isinstance(calibration["Data"], pl.DataFrame)
    assert calibration["Data"].to_dict(as_series=False) == {"value": [1, 2]}


def test_get_calib_reads_lvid_from_namespaced_csv_label(tmp_path):
    database_path = make_database(tmp_path)
    (database_path / "matrix.lblx").write_text(
        '<pds:Product_Ancillary xmlns:pds="http://pds.nasa.gov/pds4/pds/v1">'
        "<pds:Identification_Area>"
        "<pds:logical_identifier>urn:test:calibration:matrix</pds:logical_identifier>"
        "</pds:Identification_Area>"
        "</pds:Product_Ancillary>",
        encoding="utf-8",
    )
    database = CalibDB(database_path, check_git=False)

    calibration = database.get_calib(
        "flat", datetime(2025, 1, 1), filter=0, read_data=True
    )

    assert calibration["LVID"] == "urn:test:calibration:matrix"


def test_get_calib_accepts_iso_date_string(tmp_path):
    database = CalibDB(make_database(tmp_path), check_git=False)

    calibration = database.get_calib("flat", "2025-01-01")

    assert calibration["Calibration_Step"] == "flat"


def test_now_is_converted_to_current_datetime(tmp_path):
    database_path = make_database(tmp_path)
    database_file = database_path / "calib_db.csv"
    database_file.write_text(
        database_file.read_text(encoding="utf-8").replace("2030-01-01", "Now"),
        encoding="utf-8",
    )

    database = CalibDB(database_path, check_git=False)

    assert database.db.item(0, "End") > datetime(2025, 1, 1)
