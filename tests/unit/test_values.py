from datetime import date, datetime, time, timedelta, timezone

import pytest
from neo4j.spatial import WGS84Point
from neo4j.time import DateTime as NeoDateTime, Duration

from deposit import DDict, DGeometry, DResource, TypedList, typed
from deposit.values import INT64_MAX, INT64_MIN, decode_value, encode_value


def test_integer_bounds_float_fidelity_and_nonfinite_rejection():
    assert encode_value("Integer", INT64_MIN) == INT64_MIN
    assert encode_value("Integer", INT64_MAX) == INT64_MAX
    assert encode_value("Float", 1) == 1.0
    with pytest.raises(TypeError):
        encode_value("Integer", INT64_MAX + 1)
    with pytest.raises(TypeError):
        encode_value("Float", 2**63 - 1)
    for value in (float("nan"), float("inf"), float("-inf")):
        with pytest.raises(TypeError):
            encode_value("Float", value)


def test_temporal_codecs_keep_zone_and_submicrosecond_precision():
    aware = datetime(2026, 10, 26, 12, 30, 1, 456789, tzinfo=timezone(timedelta(hours=5, minutes=30)))
    assert encode_value("Date", date(2026, 10, 26)) == "2026-10-26"
    assert encode_value("Time", time(12, 30, tzinfo=timezone.utc)).endswith("+00:00")
    assert encode_value("DateTime", aware).endswith("+05:30")
    exact = NeoDateTime(2026, 10, 26, 12, 30, 1, nanosecond=123456789, tzinfo=timezone.utc)
    exact_text = encode_value("DateTime", exact)
    assert "123456789" in exact_text
    assert decode_value("DateTime", exact_text).nanosecond == 123456789
    with pytest.raises(TypeError):
        encode_value("DateTime", datetime(2026, 1, 1))


def test_local_temporal_codecs_accept_naive_builtin_values():
    local_time = time(12, 30, 1, 456789)
    local_datetime = datetime(2026, 10, 26, 12, 30, 1, 456789)
    assert encode_value("LocalTime", local_time) == "12:30:01.456789"
    assert encode_value("LocalDateTime", local_datetime) == "2026-10-26T12:30:01.456789"
    assert typed("LocalTime", local_time).value == local_time
    assert typed("LocalDateTime", local_datetime).value == local_datetime
    with pytest.raises(TypeError):
        encode_value("LocalTime", time(12, 30, tzinfo=timezone.utc))
    with pytest.raises(TypeError):
        encode_value("LocalDateTime", datetime(2026, 10, 26, tzinfo=timezone.utc))


def test_duration_and_point_preserve_calendar_months_and_srid():
    duration = Duration(months=-13, days=-2, seconds=7, nanoseconds=123456789)
    assert encode_value("Duration", duration) == {
        "months": -13, "days": -2, "seconds": 7, "nanoseconds": 123456789
    }
    assert decode_value("Duration", encode_value("Duration", duration)) == duration
    point = WGS84Point((14.25, 50.08))
    assert encode_value("Point", point) == {"srid": 4326, "coordinates": [14.25, 50.08]}
    assert decode_value("Point", encode_value("Point", point)) == point


def test_nested_lists_and_dicts_keep_declared_types_and_nulls():
    payload = {"itemType": "Integer", "items": [None, INT64_MAX]}
    assert encode_value("List", TypedList([None, INT64_MAX], item_type="Integer")) == payload
    assert decode_value("List", payload) == [None, INT64_MAX]
    nested = typed("List", TypedList([TypedList([], item_type="String")], item_type="List"))
    assert nested.to_dict() == {"type": "List", "value": {"itemType": "List", "items": [{"itemType": "String", "items": []}]}}
    assert typed("Integer", None).to_dict() == {"type": "Integer", "value": None}
    ddict = DDict({"nullable": typed("String", None), "nested": typed("List", TypedList([], item_type="Float"))})
    assert encode_value("DDict", ddict) == {
        "nullable": {"type": "String", "value": None},
        "nested": {"type": "List", "value": {"itemType": "Float", "items": []}},
    }
    assert encode_value("DDict", {"integer": INT64_MAX}) == {
        "integer": {"type": "Integer", "value": INT64_MAX}
    }
    assert decode_value("DDict", {"integer": {"type": "Integer", "value": INT64_MAX}})["integer"] == INT64_MAX


def test_decoded_ddict_preserves_each_typed_null_and_reencodes_losslessly():
    payload = {
        "text": {"type": "String", "value": None},
        "float": {"type": "Float", "value": None},
        "integer": {"type": "Integer", "value": None},
    }
    decoded = decode_value("DDict", payload)
    assert encode_value("DDict", decoded) == payload
    assert decoded["float"].type == "Float"
    assert decoded["float"].value is None


def test_structured_dtype_from_dict_keeps_public_type_names():
    geometry = DGeometry(from_dict={"type": "DGeometry", "value": {"wkt": "POINT (1 2)"}})
    resource = DResource(from_dict={"type": "DResource", "value": {"resourceId": "r-1"}})
    assert geometry.to_dict() == {"type": "DGeometry", "value": {"wkt": "POINT (1 2)"}}
    assert resource.to_dict() == {"type": "DResource", "value": {"resourceId": "r-1"}}
    with pytest.raises(TypeError, match="does not match"):
        DGeometry(from_dict={"type": "Geometry", "value": {"wkt": "POINT (1 2)"}})


def test_empty_and_null_lists_need_explicit_item_type_and_geometry_resources_stay_top_level():
    with pytest.raises(TypeError):
        encode_value("List", [])
    with pytest.raises(TypeError):
        encode_value("List", [None, None])
    with pytest.raises(TypeError):
        encode_value("List", [typed("DGeometry", {})])
    with pytest.raises(TypeError):
        encode_value("List", [typed("DResource", {})])
    with pytest.raises(TypeError):
        encode_value("DDict", {"bad": DGeometry(wkt="POINT (0 0)")})
    assert encode_value("DGeometry", DGeometry(wkt="POINT (0 0)"))["wkt"] == "POINT (0 0)"
    assert encode_value("DResource", DResource()) == {}


def test_descriptor_dict_keys_are_not_mistaken_for_typed_value_envelope():
    ddict = {"type": "field data", "value": "literal"}
    assert encode_value("DDict", ddict) == {
        "type": {"type": "String", "value": "field data"},
        "value": {"type": "String", "value": "literal"},
    }
    value = typed("Integer", 9007199254740993)
    assert value.to_dict(format="exchange") == {"type": "Integer", "value": "9007199254740993"}
    assert value.to_dict(format="procedure") == {"type": "Integer", "value": 9007199254740993}
    with pytest.raises(TypeError):
        typed("NoSuchType", 1)


def test_geometry_edit_invalidates_cached_representation_and_resolve_uses_store_server():
    geometry = DGeometry(coordinates=[1.0, 2.0], geometry_type="Point", srid=4326)
    with pytest.raises(ValueError, match="wkt"):
        _ = geometry.wkt

    class _GeometryClient:
        def coordinates_to_wkt(self, token, geometry_type, coordinates, options):
            assert token == "session-token" and geometry_type == "Point"
            return {"wkt": "POINT (1 2)"}

        def wkt_to_coordinates(self, token, wkt, options):
            assert token == "session-token" and wkt == "POINT (3 4)"
            return {"geometryType": "Point", "coordinates": [3.0, 4.0]}

    class _Store:
        _client = _GeometryClient()
        _token = "session-token"

    assert geometry.resolve(_Store()) is geometry
    assert geometry.wkt == "POINT (1 2)"
    assert geometry.geometry_type == "Point"
    assert geometry.vertical_srid is None
    assert geometry.axis_order == "xy"
    geometry.coordinates = [3.0, 4.0]
    with pytest.raises(ValueError, match="wkt"):
        _ = geometry.wkt
    assert geometry.resolve(_Store()) is geometry
    assert geometry.wkt == "POINT (1 2)"
    geometry.wkt = "POINT (5 6)"
    with pytest.raises(ValueError, match="coordinates"):
        _ = geometry.coordinates
    with pytest.raises(AttributeError, match="geometry_type is read-only"):
        geometry.geometry_type = "LineString"
    with pytest.raises(AttributeError, match="geometry_id is read-only"):
        geometry.geometry_id = "private"
