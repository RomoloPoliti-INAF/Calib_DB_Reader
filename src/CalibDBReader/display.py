from __future__ import annotations

from pathlib import Path

import polars as pl
from rich.console import Console
from rich.table import Table


def dataframe_to_rich(dataframe: pl.DataFrame) -> Table:
    """Convert a Polars dataframe to a Rich table."""
    table = Table(show_header=True, header_style="bold cyan")
    for column in dataframe.columns:
        table.add_column(str(column))

    for row in dataframe.iter_rows():
        table.add_row(*(str(value) if value is not None else "" for value in row))
    return table


def database_to_rich(db_file: Path, *, check: bool = False) -> Table:
    """Read a calibration database CSV and return it as a Rich table."""
    db_file = Path(db_file).expanduser().resolve()
    if not db_file.is_file():
        raise FileNotFoundError(f"The file '{db_file}' does not exist")

    try:
        dataframe = pl.read_csv(db_file, infer_schema=False)
    except pl.exceptions.NoDataError as exc:
        raise ValueError(f"The calibration database '{db_file}' is empty") from exc
    if dataframe.is_empty():
        raise ValueError(f"The calibration database '{db_file}' is empty")
    dataframe = dataframe.rename(
        {column: column.strip() for column in dataframe.columns}
    ).with_columns(pl.all().str.strip_chars())

    file_column = next(
        (column for column in dataframe.columns if column.casefold() == "file"),
        None,
    )
    if check and file_column is None:
        raise ValueError("The calibration database has no 'File' column.")

    if check:
        file_values = dataframe.get_column(file_column)
        statuses = [
            "✅"
            if file_value and (db_file.parent / str(file_value)).is_file()
            else "❌"
            for file_value in file_values
        ]
        dataframe = dataframe.with_columns(pl.Series("exists", statuses))

    table = dataframe_to_rich(dataframe)
    if check:
        table.columns[-1].justify = "center"
    return table


def dbdisplay(
    db_file: Path,
    *,
    check: bool = False,
    console: Console | None = None,
) -> None:
    """Print a calibration database using Rich."""
    (console or Console()).print(database_to_rich(db_file, check=check))


display_database = dbdisplay
