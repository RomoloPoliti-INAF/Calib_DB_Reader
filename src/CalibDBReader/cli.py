from __future__ import annotations

import json
from importlib.metadata import metadata, version
from pathlib import Path

import rich_click as click
from rich.console import Console
from rich.text import Text

from .display import dbdisplay as print_database

DISTRIBUTION = "CalibDBReader"
CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"]}

click.rich_click.TEXT_MARKUP = "rich"


def _database_information(database: Path) -> tuple[str, str | None]:
    database_root = database.parent if database.is_file() else database
    manifest_file = database_root / "manifest.json"
    if not manifest_file.is_file():
        raise FileNotFoundError(
            f"The database manifest '{manifest_file}' does not exist"
        )

    content = json.loads(manifest_file.read_text(encoding="utf-8"))
    database_version = content.get("version")
    if database_version is None:
        raise ValueError(f"The database manifest '{manifest_file}' has no version")
    return str(database_version), content.get("instrument")


@click.group(context_settings=CONTEXT_SETTINGS)
def cli() -> None:
    """Calibration database reader utilities."""


@cli.command("version")
@click.argument(
    "database",
    required=False,
    type=click.Path(exists=True, path_type=Path),
)
def version_command(database: Path | None) -> None:
    """Show the library version and optionally the database version."""
    console = Console()
    console.print(
        f"CalibDBReader version: [bold blue]{version(DISTRIBUTION)}[/bold blue]"
    )
    if database is not None:
        database_version, instrument = _database_information(database)
        console.print(
            f"Calibration database version: [bold blue]{database_version}[/bold blue]"
        )
        if instrument:
            console.print(f"Instrument: [bold blue]{instrument}[/bold blue]")


@cli.command("about")
def about() -> None:
    """Display package information."""
    package_metadata = metadata(DISTRIBUTION)
    content = Text()
    content.append(
        f"{package_metadata['Name']} {package_metadata['Version']}\n",
        style="bold",
    )
    summary = package_metadata.get("Summary")
    if summary:
        content.append(f"\n{summary}\n")
    author = package_metadata.get("Author-email")
    if author:
        content.append("\nAuthor: ", style="bold cyan")
        content.append(author)
    license_expression = package_metadata.get("License-Expression")
    if license_expression:
        content.append("\nLicense: ", style="bold cyan")
        content.append(license_expression)
    for project_url in package_metadata.get_all("Project-URL", []):
        label, separator, url = project_url.partition(", ")
        if separator:
            content.append(f"\n{label}: ", style="bold cyan")
            content.append(url)
    Console().print(content)


@cli.command("dbdisplay")
@click.argument(
    "db_file",
    type=click.Path(exists=True, dir_okay=False, readable=True, path_type=Path),
)
@click.option(
    "--check",
    is_flag=True,
    help="Check calibration files relative to the database folder.",
)
def dbdisplay(db_file: Path, check: bool) -> None:
    """Display a calibration database."""
    print_database(db_file, check=check)


main = cli


if __name__ == "__main__":
    cli()
