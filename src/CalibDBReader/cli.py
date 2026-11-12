import argparse
from pathlib import Path

from polars.exceptions import PolarsError

from CalibDBReader.display import dbdisplay


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Display a calibration database CSV as a table.",
    )
    parser.add_argument(
        "db_file", type=Path, metavar="CSV", help="Path to the database CSV."
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Show whether each referenced calibration file exists.",
    )
    args = parser.parse_args(argv)

    try:
        dbdisplay(args.db_file, check=args.check)
    except (OSError, ValueError, PolarsError) as exc:
        parser.exit(1, f"{parser.prog}: {exc}\n")
    return 0
