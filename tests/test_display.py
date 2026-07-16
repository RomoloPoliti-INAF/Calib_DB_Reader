from pathlib import Path

import polars as pl
import pytest

from CalibDBReader import database_to_rich, dbdisplay
from CalibDBReader.display import dataframe_to_rich


def test_dataframe_to_rich_converts_polars_dataframe() -> None:
    table = dataframe_to_rich(
        pl.DataFrame(
            {
                "instrument": ["STC", "HRIC"],
                "version": [1, 2],
            }
        )
    )

    assert [column.header for column in table.columns] == [
        "instrument",
        "version",
    ]
    assert table.columns[0]._cells == ["STC", "HRIC"]
    assert table.columns[1]._cells == ["1", "2"]


def test_database_check_uses_data_folder(tmp_path: Path) -> None:
    db_file = tmp_path / "calibration.csv"
    db_file.write_text(
        "Description,File\n"
        "Present,data/response/present.dat\n"
        "Missing,data/response/missing.dat\n"
    )
    present = tmp_path / "data" / "response" / "present.dat"
    present.parent.mkdir(parents=True)
    present.touch()

    table = database_to_rich(db_file, check=True)

    assert [column.header for column in table.columns] == [
        "Description",
        "File",
        "exists",
    ]
    assert table.columns[-1]._cells == ["✅", "❌"]


def test_database_check_requires_file_column(tmp_path: Path) -> None:
    db_file = tmp_path / "calibration.csv"
    db_file.write_text("Description\nCalibration\n")

    with pytest.raises(ValueError, match="no 'File' column"):
        database_to_rich(db_file, check=True)


def test_dbdisplay_is_public() -> None:
    assert callable(dbdisplay)
