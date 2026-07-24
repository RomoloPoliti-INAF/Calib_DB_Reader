import json
from datetime import datetime
from pathlib import Path

import numpy as np
import polars as pl
import pytest

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


def test_get_calib_builds_lvid_without_xml_prefixes(tmp_path):
    database_path = make_database(tmp_path)
    (database_path / "matrix.lblx").write_text(
        "<Product_Ancillary>"
        "<Identification_Area>"
        "<logical_identifier>urn:test:calibration:matrix</logical_identifier>"
        "<version_id>1.2</version_id>"
        "</Identification_Area>"
        "</Product_Ancillary>",
        encoding="utf-8",
    )
    database = CalibDB(database_path, check_git=False)

    calibration = database.get_calib(
        "flat", datetime(2025, 1, 1), filter=0, read_data=True
    )

    assert calibration["LVID"] == "urn:test:calibration:matrix::1.2"


def test_get_calib_builds_lvid_with_namespaced_version(tmp_path):
    database_path = make_database(tmp_path)
    (database_path / "matrix.lblx").write_text(
        '<pds:Product_Ancillary xmlns:pds="http://pds.nasa.gov/pds4/pds/v1">'
        "<pds:Identification_Area>"
        "<pds:logical_identifier>urn:test:calibration:matrix</pds:logical_identifier>"
        "<pds:version_id>2.0</pds:version_id>"
        "</pds:Identification_Area>"
        "</pds:Product_Ancillary>",
        encoding="utf-8",
    )
    database = CalibDB(database_path, check_git=False)

    calibration = database.get_calib(
        "flat", datetime(2025, 1, 1), filter=0, read_data=True
    )

    assert calibration["LVID"] == "urn:test:calibration:matrix::2.0"


def test_get_calib_reads_single_named_npz_array(tmp_path):
    database_path = make_database(tmp_path)
    np.savez(database_path / "matrix.npz", Gain=np.array([1.0, 2.0]))
    (database_path / "calib_db.csv").write_text(
        "Calibration_Step,Size,Start,End,Filter,File,Type,Arrays\n"
        "flat,2,2024-01-01,2030-01-01,all,matrix.npz,float64,Gain\n",
        encoding="utf-8",
    )
    database = CalibDB(database_path, check_git=False)

    calibration = database.get_calib(
        "flat", datetime(2025, 1, 1), filter=0, read_data=True
    )

    assert list(calibration["Data"]) == ["Gain"]
    np.testing.assert_array_equal(calibration["Data"]["Gain"], [1.0, 2.0])


def test_get_calib_reports_missing_dat_label(tmp_path):
    database_path = make_database(tmp_path)
    (database_path / "matrix.dat").write_bytes(b"\x00")
    (database_path / "calib_db.csv").write_text(
        "Calibration_Step,Size,Start,End,Filter,File,Type\n"
        "flat,1,2024-01-01,2030-01-01,all,matrix.dat,uint8\n",
        encoding="utf-8",
    )
    database = CalibDB(database_path, check_git=False)

    with pytest.raises(FileNotFoundError, match="PDS label"):
        database.get_calib("flat", datetime(2025, 1, 1), filter=0, read_data=True)


def test_get_calib_reads_pds4_dat_data_and_lvid(tmp_path):
    database_path = make_database(tmp_path)
    (database_path / "matrix.dat").write_bytes(bytes([1, 2]))
    (database_path / "matrix.lblx").write_text(
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<Product_Ancillary xmlns="http://pds.nasa.gov/pds4/pds/v1">'
        "<Identification_Area>"
        "<logical_identifier>urn:test:calibration:matrix</logical_identifier>"
        "<version_id>1.0</version_id>"
        "<title>Test calibration matrix</title>"
        "<information_model_version>1.21.0.0</information_model_version>"
        "<product_class>Product_Ancillary</product_class>"
        "</Identification_Area>"
        "<File_Area_Ancillary>"
        "<File><file_name>matrix.dat</file_name></File>"
        "<Array_1D>"
        "<local_identifier>Data</local_identifier>"
        '<offset unit="byte">0</offset>'
        "<axes>1</axes>"
        "<axis_index_order>Last Index Fastest</axis_index_order>"
        "<Element_Array><data_type>UnsignedByte</data_type></Element_Array>"
        "<Axis_Array>"
        "<axis_name>Element</axis_name>"
        "<elements>2</elements>"
        "<sequence_number>1</sequence_number>"
        "</Axis_Array>"
        "</Array_1D>"
        "</File_Area_Ancillary>"
        "</Product_Ancillary>",
        encoding="utf-8",
    )
    (database_path / "calib_db.csv").write_text(
        "Calibration_Step,Size,Start,End,Filter,File,Type\n"
        "flat,2,2024-01-01,2030-01-01,all,matrix.dat,uint8\n",
        encoding="utf-8",
    )
    database = CalibDB(database_path, check_git=False)

    calibration = database.get_calib(
        "flat", datetime(2025, 1, 1), filter=0, read_data=True
    )

    assert calibration["LVID"] == "urn:test:calibration:matrix::1.0"
    np.testing.assert_array_equal(calibration["Data"], [1, 2])


def test_get_calib_rejects_unsupported_data_format(tmp_path):
    database_path = make_database(tmp_path)
    (database_path / "matrix.txt").write_text("1", encoding="utf-8")
    (database_path / "calib_db.csv").write_text(
        "Calibration_Step,Size,Start,End,Filter,File,Type\n"
        "flat,1,2024-01-01,2030-01-01,all,matrix.txt,uint8\n",
        encoding="utf-8",
    )
    database = CalibDB(database_path, check_git=False)

    with pytest.raises(ValueError, match="Unsupported calibration file format"):
        database.get_calib("flat", datetime(2025, 1, 1), filter=0, read_data=True)


def test_get_calib_accepts_iso_date_string(tmp_path):
    database = CalibDB(make_database(tmp_path), check_git=False)

    calibration = database.get_calib("flat", "2025-01-01")

    assert calibration["Calibration_Step"] == "flat"


def test_get_calib_rejects_missing_date(tmp_path):
    database = CalibDB(make_database(tmp_path), check_git=False)

    with pytest.raises(ValueError, match="product acquisition date is required"):
        database.get_calib("flat", None)


def test_get_calib_rejects_invalid_date_type(tmp_path):
    database = CalibDB(make_database(tmp_path), check_git=False)

    with pytest.raises(TypeError, match="datetime or YYYY-MM-DD string"):
        database.get_calib("flat", 20250101)


def test_now_is_converted_to_current_datetime(tmp_path):
    database_path = make_database(tmp_path)
    database_file = database_path / "calib_db.csv"
    database_file.write_text(
        database_file.read_text(encoding="utf-8").replace("2030-01-01", "Now"),
        encoding="utf-8",
    )

    database = CalibDB(database_path, check_git=False)

    assert database.db.item(0, "End") > datetime(2025, 1, 1)
