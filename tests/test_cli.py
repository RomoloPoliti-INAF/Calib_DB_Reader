from pathlib import Path

from click.testing import CliRunner

from CalibDBReader.cli import cli


def test_cli_exposes_commands() -> None:
    result = CliRunner().invoke(cli, ["--help"])

    assert result.exit_code == 0
    for command in ("version", "about", "dbdisplay"):
        assert command in result.output


def test_version_shows_library_and_database_versions(tmp_path: Path) -> None:
    (tmp_path / "manifest.json").write_text(
        '{"version": "2.1", "instrument": "STC"}\n',
        encoding="utf-8",
    )

    result = CliRunner().invoke(cli, ["version", str(tmp_path)])

    assert result.exit_code == 0
    assert "CalibDBReader version: 0.9.0" in result.output
    assert "Calibration database version: 2.1" in result.output
    assert "Instrument: STC" in result.output


def test_dbdisplay_checks_database_files(tmp_path: Path) -> None:
    db_file = tmp_path / "database.csv"
    db_file.write_text("Description,File\nCalibration,missing.dat\n")

    result = CliRunner().invoke(cli, ["dbdisplay", str(db_file), "--check"])

    assert result.exit_code == 0
    assert "Description" in result.output
    assert "exists" in result.output
    assert "❌" in result.output
