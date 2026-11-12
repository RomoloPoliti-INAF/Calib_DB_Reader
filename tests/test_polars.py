import json
from datetime import datetime, timedelta, timezone
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

    metadata = database.get_calib("flat", "2025-01-01", filter=0)
    assert metadata["LVID"] == "urn:test:calibration:matrix::1.2"
    assert "Data" not in metadata
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

    placeholder = "urn:placeholder:calibration:npz::0.0"
    assert calibration["LVID"] == placeholder
    assert database.get_calib("flat", "2025-01-01", return_class=True).lvid == placeholder


def test_get_calib_npy_uses_placeholder_until_pds_label_is_available(tmp_path):
    folder = make_database(tmp_path)
    np.save(folder / "matrix.npy", np.array([1.0, 2.0]))
    catalog = folder / "calib_db.csv"
    catalog.write_text(catalog.read_text().replace("matrix.csv", "matrix.npy"))
    database = CalibDB(folder, check_git=False)

    calibration = database.get_calib("flat", "2025-01-01", read_data=True)

    np.testing.assert_array_equal(calibration["Data"], [1.0, 2.0])
    assert calibration["LVID"] == "urn:placeholder:calibration:npy::0.0"
    assert database.get_calib("flat", "2025-01-01", return_class=True).lvid == calibration["LVID"]

    (folder / "matrix.lblx").write_text(
        "<Product_Observational><Identification_Area>"
        "<logical_identifier>urn:esa:pds:test:matrix</logical_identifier>"
        "<version_id>1.0</version_id>"
        "</Identification_Area></Product_Observational>"
    )
    assert database.get_calib("flat", "2025-01-01")["LVID"] == "urn:esa:pds:test:matrix::1.0"


def test_get_calib_reports_missing_dat_label(tmp_path):
    database_path = make_database(tmp_path)
    (database_path / "matrix.dat").write_bytes(b"\x00")
    (database_path / "calib_db.csv").write_text(
        "Calibration_Step,Size,Start,End,Filter,File,Type\n"
        "flat,1,2024-01-01,2030-01-01,all,matrix.dat,uint8\n",
        encoding="utf-8",
    )
    with pytest.raises(FileNotFoundError, match="PDS label"):
        CalibDB(database_path, check_git=False, check=True)

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

    metadata = database.get_calib("flat", "2025-01-01", filter=0)
    assert metadata["LVID"] == "urn:test:calibration:matrix::1.0"
    assert "Data" not in metadata
    assert calibration["LVID"] == "urn:test:calibration:matrix::1.0"
    np.testing.assert_array_equal(calibration["Data"], [1, 2])


    catalog = database_path / "calib_db.csv"
    original = catalog.read_text(encoding="utf-8")
    for contents, message in (
        (original.replace("flat,2,", "flat,1,"), "CSV Size"),
        (original.replace("uint8", "float64"), "CSV Type"),
    ):
        catalog.write_text(contents, encoding="utf-8")
        database = CalibDB(database_path, check_git=False)
        with pytest.raises(ValueError, match=message):
            database.get_calib("flat", "2025-01-01", read_data=True)

    label = database_path / "matrix.lblx"
    label.write_text(label.read_text().replace("matrix.dat", "other.dat"))
    (database_path / "other.dat").write_bytes(b"\x01\x02")
    with pytest.raises(ValueError, match="must reference 'matrix.dat'"):
        CalibDB(database_path, check_git=False, check=True)
    with pytest.raises(ValueError, match="must reference 'matrix.dat'"):
        database.get_calib("flat", "2025-01-01", read_data=True)


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


def test_get_calib_converts_aware_datetime_to_utc(tmp_path):
    database = CalibDB(make_database(tmp_path), check_git=False)
    # Local date is before Start, but the UTC instant is on Start.
    acquired = datetime(2023, 12, 31, 23, tzinfo=timezone(-timedelta(hours=2)))

    calibration = database.get_calib("flat", acquired)

    assert calibration["Calibration_Step"] == "flat"


def test_get_calib_rejects_missing_date(tmp_path):
    database = CalibDB(make_database(tmp_path), check_git=False)

    with pytest.raises(ValueError, match="product acquisition date is required"):
        database.get_calib("flat", None)


def test_get_calib_rejects_invalid_date_type(tmp_path):
    database = CalibDB(make_database(tmp_path), check_git=False)

    with pytest.raises(TypeError, match="datetime or YYYY-MM-DD string"):
        database.get_calib("flat", 20250101)


def test_end_date_includes_last_second_of_utc_day(tmp_path):
    database = CalibDB(make_database(tmp_path), check_git=False)

    calibration = database.get_calib("flat", datetime(2030, 1, 1, 23, 59, 59))

    assert calibration["End"] == datetime(2030, 1, 1, 23, 59, 59)
    with pytest.raises(ValueError, match="No calibration record"):
        database.get_calib("flat", datetime(2030, 1, 2))


def test_now_is_converted_to_current_datetime(tmp_path):
    database_path = make_database(tmp_path)
    database_file = database_path / "calib_db.csv"
    database_file.write_text(
        database_file.read_text(encoding="utf-8").replace("2030-01-01", "Now"),
        encoding="utf-8",
    )

    database = CalibDB(database_path, check_git=False)

    assert database.db.item(0, "End") > datetime(2025, 1, 1)


def test_calibration_product_exposes_additional_catalog_columns(tmp_path):
    database_path = make_database(tmp_path)
    catalog = database_path / "calib_db.csv"
    header, row = catalog.read_text(encoding="utf-8").splitlines()
    catalog.write_text(
        f"{header},Instrument_Mode,Detector Temperature,Description\n"
        f"{row},NOLO,263,Test response\n",
        encoding="utf-8",
    )
    database = CalibDB(database_path, check_git=False)

    product = database.get_calib("flat", "2025-01-01", return_class=True)

    assert product.instrument_mode == "NOLO"
    assert product.description == "Test response"
    assert product.detector_temperature == "263"
    assert product.extra_metadata["Detector Temperature"] == "263"
    assert not hasattr(product, "unknown_column")


def test_update_now_refreshes_only_open_ended_rows(tmp_path, monkeypatch):
    database_path = make_database(tmp_path)
    catalog = database_path / "calib_db.csv"
    header, row = catalog.read_text(encoding="utf-8").splitlines()
    catalog.write_text(
        f"{header}\n{row.replace('2030-01-01', 'Now')}\n{row}\n",
        encoding="utf-8",
    )
    database = CalibDB(database_path, check_git=False)
    refreshed = datetime(2040, 1, 1)
    monkeypatch.setattr(database, "convert_date_now", lambda value: refreshed)

    database.update_now()

    assert database.db["End"].to_list() == [refreshed, datetime(2030, 1, 1, 23, 59, 59)]

@pytest.mark.parametrize(
    ("column", "criteria"), [("Channel", {"channel": "VIHI"}), ("Filter", {"filter": 3})]
)
def test_get_calib_rejects_missing_filter_column(tmp_path, column, criteria):
    database_path = make_database(tmp_path)
    catalog = database_path / "calib_db.csv"
    catalog.write_text(
        catalog.read_text(encoding="utf-8").replace(",Filter", "").replace(",all", ""),
        encoding="utf-8",
    )
    database = CalibDB(database_path, check_git=False)

    with pytest.raises(ValueError, match=f"no {column} column"):
        database.get_calib("flat", "2025-01-01", **criteria)


def test_get_calib_rejects_ambiguous_lookup_without_mode(tmp_path):
    database_path = make_database(tmp_path)
    catalog = database_path / "calib_db.csv"
    header, row = catalog.read_text(encoding="utf-8").splitlines()
    catalog.write_text(
        f"{header},Instrument_Mode\n{row},HIHI\n{row},NOLO\n",
        encoding="utf-8",
    )
    database = CalibDB(database_path, check_git=False)

    with pytest.raises(ValueError, match="Ambiguous calibration lookup.*Found 2 records"):
        database.get_calib("flat", "2025-01-01")
    assert database.get_calib("flat", "2025-01-01", instrument_mode="NOLO")[
        "Instrument_Mode"
    ] == "NOLO"


@pytest.mark.parametrize("use_symlink", [False, True])
def test_calibration_path_cannot_escape_database(tmp_path, use_symlink):
    database_path = make_database(tmp_path)
    external_file = tmp_path / "external.csv"
    external_file.write_text("value\n1\n", encoding="utf-8")
    file_value = "../external.csv"
    if use_symlink:
        (database_path / "link.csv").symlink_to(external_file)
        file_value = "link.csv"
    catalog = database_path / "calib_db.csv"
    catalog.write_text(
        catalog.read_text(encoding="utf-8").replace("matrix.csv", file_value),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="outside the database directory"):
        CalibDB(database_path, check_git=False, check=True)

    database = CalibDB(database_path, check_git=False, check=False)
    with pytest.raises(ValueError, match="outside the database directory"):
        database.get_calib("flat", "2025-01-01")
