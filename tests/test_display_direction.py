"""Calibration arrays come back in sensor order, whatever the stored orientation (#88)."""

import json
from datetime import datetime

import numpy as np
import pytest

from CalibDBReader import CalibDB

LINES, SAMPLES = 2, 3


def _array(local_id, axes, offset, direction=None):
    tag = "Array_3D_Image" if len(axes) == 3 else "Array_2D_Image"
    axis_xml = "".join(
        f"<Axis_Array><axis_name>{name}</axis_name><elements>{n}</elements>"
        f"<sequence_number>{i}</sequence_number></Axis_Array>"
        for i, (name, n) in enumerate(axes, start=1)
    )
    array = (
        f"<{tag}><local_identifier>{local_id}</local_identifier>"
        f'<offset unit="byte">{offset}</offset><axes>{len(axes)}</axes>'
        "<axis_index_order>Last Index Fastest</axis_index_order>"
        "<Element_Array><data_type>UnsignedByte</data_type></Element_Array>"
        f"{axis_xml}</{tag}>"
    )
    settings = "" if direction is None else (
        "<disp:Display_Settings><Local_Internal_Reference>"
        f"<local_identifier_reference>{local_id}</local_identifier_reference>"
        "<local_reference_type>display_settings_to_array</local_reference_type>"
        "</Local_Internal_Reference><disp:Display_Direction>"
        "<disp:horizontal_display_axis>Sample</disp:horizontal_display_axis>"
        f"<disp:horizontal_display_direction>{direction}</disp:horizontal_display_direction>"
        "<disp:vertical_display_axis>Line</disp:vertical_display_axis>"
        "<disp:vertical_display_direction>Top to Bottom</disp:vertical_display_direction>"
        "</disp:Display_Direction></disp:Display_Settings>"
    )
    return array, settings


def _database(tmp_path, arrays, data: bytes, size: str, arrays_column: str = "Null"):
    """arrays: list of (local_id, axes, direction-or-None), stored back to back."""
    database = tmp_path / "database"
    database.mkdir()
    (database / "manifest.json").write_text(json.dumps({"version": "1.5", "instrument": "JANUS"}))
    offset, array_xml, settings_xml = 0, "", ""
    for local_id, axes, direction in arrays:
        a, s = _array(local_id, axes, offset, direction)
        array_xml += a
        settings_xml += s
        offset += int(np.prod([n for _, n in axes]))
    (database / "matrix.dat").write_bytes(data)
    (database / "matrix.lblx").write_text(
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<Product_Ancillary xmlns="http://pds.nasa.gov/pds4/pds/v1" '
        'xmlns:disp="http://pds.nasa.gov/pds4/disp/v1">'
        "<Identification_Area><logical_identifier>urn:test:calibration:matrix</logical_identifier>"
        "<version_id>0.1</version_id><title>Test</title>"
        "<information_model_version>1.19.0.0</information_model_version>"
        "<product_class>Product_Ancillary</product_class></Identification_Area>"
        f"<Context_Area><Discipline_Area>{settings_xml}</Discipline_Area></Context_Area>"
        "<File_Area_Ancillary><File><file_name>matrix.dat</file_name></File>"
        f"{array_xml}</File_Area_Ancillary></Product_Ancillary>"
    )
    (database / "calib_db.csv").write_text(
        "Calibration_Step,Size,Start,End,Filter,File,Type,Arrays\n"
        f"flat,{size},2024-01-01,2030-01-01,all,matrix.dat,uint8,{arrays_column}\n"
    )
    return CalibDB(database, check_git=False)


def _load(db):
    return db.get_calib("flat", datetime(2025, 1, 1), filter=0, read_data=True)["Data"]


SENSOR = np.arange(LINES * SAMPLES, dtype=np.uint8).reshape(LINES, SAMPLES)


def test_right_to_left_is_returned_as_stored(tmp_path):
    """Calib DB <= 1.4: stored in sensor order."""
    db = _database(tmp_path, [("m", [("Line", LINES), ("Sample", SAMPLES)], "Right to Left")],
                   SENSOR.tobytes(), f"{LINES}-{SAMPLES}")
    np.testing.assert_array_equal(_load(db), SENSOR)


def test_left_to_right_is_flipped_back_to_sensor_order(tmp_path):
    """Calib DB >= 1.5: stored mirrored like the raw images; returned in sensor order."""
    db = _database(tmp_path, [("m", [("Line", LINES), ("Sample", SAMPLES)], "Left to Right")],
                   np.fliplr(SENSOR).tobytes(), f"{LINES}-{SAMPLES}")
    np.testing.assert_array_equal(_load(db), SENSOR)


def test_3d_arrays_flip_the_sample_axis_only(tmp_path):
    """Radiometric knots/coefficients are (line, sample, knot): the knot axis must not move."""
    sensor = np.arange(LINES * SAMPLES * 2, dtype=np.uint8).reshape(LINES, SAMPLES, 2)
    db = _database(tmp_path, [("knots", [("Line", LINES), ("Sample", SAMPLES), ("Knots", 2)], "Left to Right")],
                   np.flip(sensor, axis=1).tobytes(), f"{LINES}-{SAMPLES}-2")
    np.testing.assert_array_equal(_load(db), sensor)


def test_multi_array_products_flip_each_array(tmp_path):
    """Products with an Arrays column (dark, distortion maps) come back as a dict."""
    other = SENSOR + 100
    db = _database(
        tmp_path,
        [("a", [("Line", LINES), ("Sample", SAMPLES)], "Left to Right"),
         ("b", [("Line", LINES), ("Sample", SAMPLES)], "Left to Right")],
        np.fliplr(SENSOR).tobytes() + np.fliplr(other).tobytes(),
        f"{LINES}-{SAMPLES}",
        arrays_column="a-b",
    )
    data = _load(db)
    np.testing.assert_array_equal(data["a"], SENSOR)
    np.testing.assert_array_equal(data["b"], other)


def test_image_array_without_display_settings_raises(tmp_path):
    db = _database(tmp_path, [("m", [("Line", LINES), ("Sample", SAMPLES)], None)],
                   SENSOR.tobytes(), f"{LINES}-{SAMPLES}")
    with pytest.raises(ValueError, match="orientation is unknown"):
        _load(db)
