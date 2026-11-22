import pytest

from CalibDBReader.manifest import read_manifest


def test_manifest_selection(tmp_path):
    legacy = tmp_path / "version.yml"
    legacy.write_text('instrument: SIMBIO-SYS\nversion: "1.0"\n', encoding="utf-8")
    assert read_manifest(tmp_path)["version"] == "1.0"

    older = tmp_path / "manifest.json"
    older.write_text('{"version": "2.0"}', encoding="utf-8")
    assert read_manifest(tmp_path, channel="STC")["version"] == "2.0"

    current = tmp_path / "manifest_stc.json"
    current.write_text('{"channel": "STC", "version": "0.1"}', encoding="utf-8")
    assert read_manifest(tmp_path, channel="stc")["version"] == "0.1"
    with pytest.raises(ValueError, match=r"manifest\.json, manifest_stc\.json"):
        read_manifest(tmp_path)

    older.unlink()
    assert read_manifest(tmp_path)["version"] == "0.1"
    with pytest.raises(ValueError, match="declares channel"):
        read_manifest(tmp_path, channel="HRIC")
