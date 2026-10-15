from unittest.mock import patch

import numpy as np
import polars as pl
import pytest

from CalibDBReader import CalibDB, cdbr
from tests.test_display_direction import SENSOR, _database
from tests.test_polars import make_database


def npz_database(tmp_path, arrays="Null"):
    folder = make_database(tmp_path)
    np.savez(folder / "matrix.npz", Data=np.array([1, 2]), Gain=np.array([3, 4]))
    (folder / "calib_db.csv").write_text(
        "Calibration_Step,Size,Start,End,File,Type,Arrays\n"
        f"flat,2,2024-01-01,2030-01-01,matrix.npz,int64,{arrays}\n"
    )
    return CalibDB(folder, check_git=False)


def load(database, **kwargs):
    return database.get_calib("flat", "2025-01-01", read_data=True, **kwargs)


@pytest.mark.parametrize("arrays", ["Null", "Gain", "Data-Gain"])
def test_npz_is_read_once_and_arrays_are_read_only(tmp_path, arrays):
    database = npz_database(tmp_path, arrays)
    with patch.object(cdbr.np, "load", wraps=np.load) as reader:
        first = load(database)["Data"]
        second = load(database)["Data"]
    assert reader.call_count == 1
    if isinstance(first, dict):
        assert first is not second
        name = next(iter(first))
        assert first[name] is second[name]
        with pytest.raises(ValueError, match="read-only"):
            first[name][0] = 99
        first.clear()
        assert second
        assert load(database)["Data"]
    else:
        assert first is second
        with pytest.raises(ValueError, match="read-only"):
            first[0] = 99
        writable = first.copy()
        writable[0] = 99
        assert load(database)["Data"][0] == 1


def test_clear_cache_reloads_updated_file(tmp_path):
    database = npz_database(tmp_path)
    with patch.object(cdbr.np, "load", wraps=np.load) as reader:
        original = load(database)["Data"]
        np.savez(database.folder / "matrix.npz", Data=np.array([9, 8]))
        np.testing.assert_array_equal(load(database)["Data"], [1, 2])
        database.clear_cache()
        np.testing.assert_array_equal(load(database)["Data"], [9, 8])
    assert reader.call_count == 2
    np.testing.assert_array_equal(original, [1, 2])


def test_metadata_queries_do_not_read_or_populate_cache(tmp_path):
    database = npz_database(tmp_path)
    with patch.object(cdbr.np, "load", wraps=np.load) as reader:
        assert "Data" not in database.get_calib("flat", "2025-01-01")
        load(database)
        assert "Data" not in database.get_calib("flat", "2025-01-01")
    assert reader.call_count == 1


def test_cache_is_per_instance_and_supports_return_class(tmp_path):
    database = npz_database(tmp_path)
    other = CalibDB(database.folder, check_git=False)
    with patch.object(cdbr.np, "load", wraps=np.load) as reader:
        first = load(database)["Data"]
        assert load(database, return_class=True).data is first
        assert load(other)["Data"] is not first
    assert reader.call_count == 2


def test_same_file_different_array_selections_are_cached_separately(tmp_path):
    database = npz_database(tmp_path, "Gain")
    database.db = pl.concat(
        [
            database.db,
            database.db.with_columns(
                pl.lit("dark").alias("Calibration_Step"),
                pl.lit("Data").alias("Arrays"),
            ),
        ]
    )
    with patch.object(cdbr.np, "load", wraps=np.load) as reader:
        assert list(load(database)["Data"]) == ["Gain"]
        dark = database.get_calib("dark", "2025-01-01", read_data=True)
        assert list(dark["Data"]) == ["Data"]
        load(database)
    assert reader.call_count == 2


def test_date_selection_still_runs_on_cache_hits(tmp_path):
    database = npz_database(tmp_path)
    np.savez(database.folder / "new.npz", Data=np.array([5, 6]))
    database.db = pl.concat(
        [
            database.db.with_columns(pl.datetime(2025, 12, 31).alias("End")),
            database.db.with_columns(
                pl.datetime(2026, 1, 1).alias("Start"),
                pl.lit("new.npz").alias("File"),
            ),
        ]
    )
    with patch.object(cdbr.np, "load", wraps=np.load) as reader:
        np.testing.assert_array_equal(load(database)["Data"], [1, 2])
        later = database.get_calib("flat", "2026-01-01", read_data=True)
        np.testing.assert_array_equal(later["Data"], [5, 6])
        np.testing.assert_array_equal(load(database)["Data"], [1, 2])
    assert reader.call_count == 2


def test_failed_reads_are_retried(tmp_path):
    database = npz_database(tmp_path)
    path = database.folder / "matrix.npz"
    path.unlink()
    with pytest.raises(FileNotFoundError):
        load(database)
    np.savez(path, Data=np.array([7, 8]))
    np.testing.assert_array_equal(load(database)["Data"], [7, 8])


def test_csv_and_lvid_cached_with_independent_dataframe_containers(tmp_path):
    database = CalibDB(make_database(tmp_path), check_git=False)
    label = database.folder / "matrix.lblx"
    label.write_text(
        "<Product_Ancillary><logical_identifier>urn:test:matrix</logical_identifier>"
        "<version_id>1.0</version_id></Product_Ancillary>"
    )
    with (
        patch.object(cdbr.pl, "read_csv", wraps=pl.read_csv) as reader,
        patch.object(cdbr, "parse", wraps=cdbr.parse) as parser,
    ):
        first = load(database)
        first["Data"].replace_column(0, pl.Series("value", [9, 9]))
        second = load(database, return_class=True)
        assert second.data.to_dict(as_series=False) == {"value": [1, 2]}
        assert second.lvid == "urn:test:matrix::1.0"
        assert reader.call_count == parser.call_count == 1
        label.write_text(label.read_text().replace("1.0", "2.0"))
        database.clear_cache()
        assert load(database)["LVID"] == "urn:test:matrix::2.0"
        assert reader.call_count == parser.call_count == 2


@pytest.mark.parametrize("arrays", ["Null", "m"])
def test_pds_data_and_orientation_are_read_once(tmp_path, arrays):
    database = _database(
        tmp_path,
        [("m", [("Line", 2), ("Sample", 3)], "Left to Right")],
        np.fliplr(SENSOR).tobytes(),
        "2-3",
        arrays_column=arrays,
    )
    with (
        patch.object(cdbr.pds4_tools, "read", wraps=cdbr.pds4_tools.read) as reader,
        patch.object(cdbr, "parse", wraps=cdbr.parse) as parser,
    ):
        load(database)
        calibration = load(database, return_class=True)
    assert reader.call_count == parser.call_count == 1
    assert calibration.lvid == "urn:test:calibration:matrix::0.1"
    data = calibration.data
    if isinstance(data, dict):
        data = data["m"]
    np.testing.assert_array_equal(data, SENSOR)
    with pytest.raises(ValueError, match="read-only"):
        data[0, 0] = 99
