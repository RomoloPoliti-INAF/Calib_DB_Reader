"""Calibration catalog metadata discovery."""

import json
from pathlib import Path
from typing import Any

import yaml


def read_manifest(folder: str | Path, *, channel: str | None = None) -> dict[str, Any]:
    """Read a channel manifest or legacy catalog metadata."""
    directory = Path(folder).expanduser()
    requested_channel = channel.strip().casefold() if channel else None
    candidates = sorted(
        path for path in directory.glob("manifest*.json") if path.is_file()
    )
    preferred = [
        path
        for path in candidates
        if requested_channel
        and path.name.casefold() == f"manifest_{requested_channel}.json"
    ]
    matches = preferred or candidates
    if len(matches) > 1:
        names = ", ".join(path.name for path in matches)
        raise ValueError(f"Ambiguous calibration manifests in {directory}: {names}")
    if matches:
        path = matches[0]
        manifest = json.loads(path.read_text(encoding="utf-8"))
    else:
        path = directory / "version.yml"
        if not path.is_file():
            raise FileNotFoundError(
                f"No calibration manifest or version.yml in {directory}"
            )
        try:
            manifest = yaml.safe_load(path.read_text(encoding="utf-8"))
        except yaml.YAMLError as error:
            raise ValueError(
                f"Invalid calibration metadata in {path}: {error}"
            ) from error

    if not isinstance(manifest, dict):
        raise ValueError(f"Expected a metadata object in {path}")
    declared_channel = manifest.get("channel")
    if (
        requested_channel
        and declared_channel is not None
        and (
            not isinstance(declared_channel, str)
            or declared_channel.strip().casefold() != requested_channel
        )
    ):
        raise ValueError(
            f"{path} declares channel {declared_channel!r}, expected {channel!r}"
        )
    return manifest
