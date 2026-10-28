"""Lossless public Deposit value representations and declaration-aware codecs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
import math
import os
from typing import Any, Mapping

from neo4j.spatial import CartesianPoint, Point, WGS84Point
from neo4j.time import Date as NeoDate, DateTime as NeoDateTime, Duration, Time as NeoTime


INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1
_TYPES = {
    "String", "Integer", "Float", "Boolean", "Date", "Time", "LocalTime",
    "DateTime", "LocalDateTime", "Duration", "Point", "List", "DDict", "DGeometry", "DResource",
}


@dataclass(frozen=True, slots=True)
class TypedValue:
    """Explicitly typed detached input; it does not imply a schema write."""

    type: str
    value: Any

    def to_dict(self, *, format: str = "procedure") -> dict[str, Any]:
        if format not in {"procedure", "exchange"}:
            raise ValueError("format must be 'procedure' or 'exchange'")
        encoded = encode_value(self.type, self.value)
        if format == "exchange": encoded = _encode_exchange(self.type, encoded)
        return {"type": self.type, "value": encoded}


def typed(type_name: str, value: Any) -> TypedValue:
    if type_name not in _TYPES:
        raise TypeError(f"Unsupported Deposit type {type_name!r}")
    # Validate now, before a Store has a chance to create a recovery journal.
    encode_value(type_name, value)
    return TypedValue(type_name, value)


class DType:
    """Base class for detached typed values."""

    def __init__(self, *, from_dict: Mapping[str, Any] | None = None):
        if type(self) is DType:
            raise TypeError("DType is abstract")
        # Public Deposit type names intentionally retain the D prefix for
        # structured values (DDict, DGeometry, DResource).
        self._type = type(self).__name__
        if from_dict is None:
            self._value = None
        else:
            if not isinstance(from_dict, Mapping) or set(from_dict) != {"type", "value"}:
                raise TypeError("from_dict must contain exactly type and value")
            if from_dict["type"] != self._type:
                raise TypeError("typed value does not match this DType")
            if self._type in {"DGeometry", "DResource"}:
                if not isinstance(from_dict["value"], Mapping):
                    raise TypeError(f"invalid {self._type} payload")
                self._value = dict(from_dict["value"])
            else:
                self._value = decode_value(self._type, from_dict["value"])

    @property
    def value(self) -> Any:
        return self._value

    def to_dict(self, *, format: str = "procedure") -> dict[str, Any]:
        if format not in {"procedure", "exchange"}:
            raise ValueError("format must be 'procedure' or 'exchange'")
        encoded = encode_value(self._type, self._value)
        if format == "exchange":
            encoded = _encode_exchange(self._type, encoded)
        return {"type": self._type, "value": encoded}


class DDict(DType):
    def __init__(self, data: Mapping[str, Any] | None = None, *, from_dict: Mapping[str, Any] | None = None):
        if data is not None and from_dict is not None:
            raise TypeError("supply data or from_dict, not both")
        super().__init__(from_dict=from_dict)
        if data is not None:
            self._value = dict(data)
            encode_value("DDict", self._value)

    def __getitem__(self, key: str) -> Any:
        return self._value[key]

    def __setitem__(self, key: str, value: Any) -> None:
        if not isinstance(key, str):
            raise TypeError("DDict keys must be strings")
        candidate = dict(self._value)
        candidate[key] = value
        encode_value("DDict", candidate)
        self._value = candidate

    def __delitem__(self, key: str) -> None:
        del self._value[key]

    def __iter__(self):
        return iter(self._value)

    def __len__(self) -> int:
        return len(self._value)


class DGeometry(DType):
    """Detached geometry metadata. Geometry conversion remains server-owned."""

    def __init__(self, *, wkt=None, coordinates=None, geometry_type=None, srid=None,
                 vertical_srid=None, axis_order="xy", dimensions=2, from_dict=None):
        if from_dict is not None and any(v is not None for v in (wkt, coordinates, geometry_type, srid, vertical_srid)):
            raise TypeError("from_dict cannot be combined with geometry fields")
        super().__init__(from_dict=from_dict)
        self._geometry_id = None
        if from_dict is None:
            supplied = sum(v is not None for v in (wkt, coordinates, from_dict))
            if supplied != 1:
                raise TypeError("supply exactly one of wkt, coordinates, or from_dict")
            if coordinates is not None and not geometry_type:
                raise TypeError("coordinates require geometry_type")
            self._value = {"wkt": wkt, "coordinates": coordinates, "geometryType": geometry_type,
                           "srid": srid, "verticalSrid": vertical_srid, "axisOrder": axis_order,
                           "dimensions": dimensions}
            encode_value("DGeometry", self._value)
        if isinstance(self._value, Mapping):
            self._geometry_id = self._value.get("geometryId")

    @property
    def geometry_id(self):
        return self._geometry_id

    def __getattr__(self, name):
        aliases = {"geometry_type": "geometryType", "vertical_srid": "verticalSrid",
                   "axis_order": "axisOrder", "dimensions": "dimensions"}
        if name in {"wkt", "coordinates", "srid", "geometry_type", "geometryType",
                    "vertical_srid", "verticalSrid", "axis_order", "axisOrder", "dimensions"}:
            key = aliases.get(name, name)
            if key in {"wkt", "coordinates", "geometryType"} and self._value.get(key) is None:
                raise ValueError(f"Geometry {key} is unavailable until resolve(store)")
            return self._value.get(key)
        raise AttributeError(name)

    def __setattr__(self, name, value):
        aliases = {"geometry_type": "geometryType", "vertical_srid": "verticalSrid",
                   "axis_order": "axisOrder", "dimensions": "dimensions"}
        prop = aliases.get(name, name)
        if name in {"geometry_id", "geometry_type", "geometryType"} or prop == "geometryId":
            raise AttributeError(f"{name} is read-only")
        if prop in {"wkt", "coordinates", "srid", "verticalSrid", "axisOrder", "dimensions"} and hasattr(self, "_value"):
            candidate = dict(self._value)
            if prop == "wkt":
                candidate["wkt"] = value
                candidate["coordinates"] = None
                candidate["geometryType"] = None
            elif prop == "coordinates":
                if not candidate.get("geometryType"):
                    raise ValueError("coordinates require a geometry_type")
                candidate["coordinates"] = value
                candidate["wkt"] = None
            else:
                candidate[prop] = value
            encode_value("DGeometry", candidate)
            self._value = candidate
            return
        object.__setattr__(self, name, value)

    def resolve(self, store):
        if self._value.get("wkt") is None and self._value.get("coordinates") is None:
            raise ValueError("Geometry has neither WKT nor coordinates")
        # The Server conversion procedures accept only these spatial reference
        # options. Axis order and dimensions are supplied by the authoritative
        # conversion result when available, otherwise the caller metadata is
        # retained locally for the subsequent descriptor write.
        options = {"srid": self._value.get("srid"), "verticalSrid": self._value.get("verticalSrid")}
        if self._value.get("wkt") is None:
            result = store._client.coordinates_to_wkt(store._token, self._value["geometryType"],
                                                       self._value["coordinates"], options)
            if not isinstance(result, Mapping) or not isinstance(result.get("wkt"), str):
                raise TypeError("Server returned no geometry WKT")
            self._merge_server_metadata(result)
            self._value["wkt"] = result["wkt"]
        else:
            result = store._client.wkt_to_coordinates(store._token, self._value["wkt"], options)
            if not isinstance(result, Mapping) or "coordinates" not in result:
                raise TypeError("Server returned no geometry coordinates")
            self._merge_server_metadata(result)
            self._value["coordinates"] = result["coordinates"]
        return self

    def _merge_server_metadata(self, result: Mapping[str, Any]) -> None:
        """Retain authoritative conversion metadata without guessing from WKT."""
        identity = result.get("geometryId")
        if identity is not None and self._geometry_id is not None and identity != self._geometry_id:
            raise ValueError("Geometry conversion changed geometry identity")
        for key in ("geometryType", "srid", "verticalSrid", "axisOrder", "dimensions"):
            if key in result:
                self._value[key] = result[key]
        if identity is not None:
            self._geometry_id = identity
            self._value["geometryId"] = identity


class DResource(DType):
    """Detached resource reference or a local source awaiting upload."""

    def __init__(self, *, path=None, stream=None, filename=None, from_dict: Mapping[str, Any] | None = None):
        if from_dict is not None and any(value is not None for value in (path, stream, filename)):
            raise TypeError("from_dict cannot be combined with a local resource source")
        if path is not None and stream is not None:
            raise TypeError("supply path or stream, not both")
        super().__init__(from_dict=from_dict)
        self._path = None
        self._stream = None
        self._transfer_id = None
        if from_dict is None:
            if path is None and stream is None:
                self._value = {}
                return
            if path is not None:
                path = os.fspath(path)
                if not isinstance(path, str):
                    raise TypeError("resource path must resolve to a string path")
                self._path = path
                inferred = os.path.basename(path)
            else:
                if not callable(getattr(stream, "read", None)):
                    raise TypeError("resource stream must be readable")
                self._stream = stream
                inferred = None
            selected = filename if filename is not None else inferred
            if selected is not None and (not isinstance(selected, str) or not selected):
                raise ValueError("resource filename must be a nonempty string")
            self._value = {"filename": selected}

    @property
    def filename(self):
        return self._value.get("filename")

    @filename.setter
    def filename(self, value):
        if not isinstance(value, str) or not value:
            raise ValueError("resource filename must be a nonempty string")
        self._value["filename"] = value

    @property
    def resource_id(self):
        return self._value.get("resourceId")

    @property
    def is_image(self):
        value = self._value.get("isImage")
        return value if type(value) is bool else None

    def open(self, store):
        """Open a checked, closable chunked reader for a stored resource."""
        return store._open_resource(self)


def encode_value(type_name: str, value: Any, *, item_type: str | None = None) -> Any:
    """Encode one public value as the server's declared-type payload."""
    if type_name not in _TYPES:
        raise TypeError(f"Unsupported Deposit type {type_name!r}")
    if value is None:
        return None
    if type_name == "String":
        if not isinstance(value, str): raise TypeError("String requires str")
        return value
    if type_name == "Boolean":
        if type(value) is not bool: raise TypeError("Boolean requires bool")
        return value
    if type_name == "Integer":
        if type(value) is not int or not INT64_MIN <= value <= INT64_MAX:
            raise TypeError("Integer requires a signed 64-bit int")
        return value
    if type_name == "Float":
        if type(value) not in (int, float): raise TypeError("Float requires int or float")
        converted = float(value)
        if not math.isfinite(converted): raise TypeError("Float must be finite")
        if type(value) is int and int(converted) != value:
            raise TypeError("Integer cannot be represented exactly as Float")
        return converted
    if type_name in {"Date", "Time", "LocalTime", "DateTime", "LocalDateTime"}:
        expected = {"Date": (date, NeoDate), "Time": (time, NeoTime), "LocalTime": (time, NeoTime),
                    "DateTime": (datetime, NeoDateTime), "LocalDateTime": (datetime, NeoDateTime)}[type_name]
        if not isinstance(value, expected) or (type_name == "Date" and isinstance(value, datetime)) or (
            type_name in {"DateTime", "Time"} and not _has_zone(value)
        ) or (
            type_name in {"LocalDateTime", "LocalTime"} and _has_zone(value)
        ):
            raise TypeError(f"{type_name} requires a compatible temporal value")
        return value.iso_format() if hasattr(value, "iso_format") else value.isoformat()
    if type_name == "Duration":
        if not isinstance(value, Duration): raise TypeError("Duration requires neo4j.time.Duration")
        return {"months": int(value.months), "days": int(value.days), "seconds": int(value.seconds), "nanoseconds": int(value.nanoseconds)}
    if type_name == "Point":
        if not isinstance(value, Point): raise TypeError("Point requires neo4j.spatial.Point")
        coordinates = [float(x) for x in value]
        if not all(math.isfinite(x) for x in coordinates): raise TypeError("Point coordinates must be finite")
        srid = getattr(value, "srid", None)
        if type(srid) is not int:
            raise TypeError("Point must retain an explicit SRID")
        return {"srid": srid, "coordinates": coordinates}
    if type_name == "List":
        if not isinstance(value, (list, tuple)): raise TypeError("List requires list or tuple")
        if item_type is None:
            explicit = value.item_type if isinstance(value, TypedList) else None
            if explicit is None:
                types = {_infer_type(x) for x in value if x is not None}
                if len(types) != 1:
                    raise TypeError("List requires an explicit item type when empty, mixed, or all-null")
                explicit = types.pop()
            item_type = explicit
        if item_type not in _TYPES - {"DDict", "DGeometry", "DResource"}:
            raise TypeError("List item type is unsupported")
        encoded = []
        for item in value:
            if isinstance(item, (DGeometry, DResource)):
                raise TypeError("Geometry and Resource values cannot be nested in List")
            if isinstance(item, TypedValue):
                if item.type != item_type: raise TypeError("typed list item has the wrong type")
                item = item.value
            encoded.append(encode_value(item_type, item))
        return {"itemType": item_type, "items": encoded}
    if type_name == "DDict":
        if isinstance(value, DDict): value = value._value
        if not isinstance(value, Mapping) or any(not isinstance(k, str) for k in value):
            raise TypeError("DDict requires a string-keyed mapping")
        return {k: _encode_ddict_entry(v) for k, v in value.items()}
    if type_name == "DGeometry":
        if isinstance(value, DGeometry): value = value._value
        if not isinstance(value, Mapping): raise TypeError("DGeometry requires a geometry mapping")
        return _encode_structure(value)
    if type_name == "DResource":
        if isinstance(value, DResource): value = value._value
        if not isinstance(value, Mapping): raise TypeError("DResource requires a resource mapping")
        return _encode_structure(value)
    raise TypeError(f"Unsupported Deposit type {type_name!r}")


class TypedList(list):
    def __init__(self, values=(), *, item_type: str):
        if item_type not in _TYPES: raise ValueError("unknown List item type")
        super().__init__(values)
        self.item_type = item_type


def decode_value(type_name: str, value: Any) -> Any:
    if value is None: return None
    if type_name == "String":
        if not isinstance(value, str): raise TypeError("invalid String payload")
        return value
    if type_name == "Boolean":
        if type(value) is not bool: raise TypeError("invalid Boolean payload")
        return value
    if type_name == "Integer":
        if type(value) is not int or not INT64_MIN <= value <= INT64_MAX: raise TypeError("invalid Integer payload")
        return value
    if type_name == "Float":
        if type(value) not in (int, float) or not math.isfinite(value): raise TypeError("invalid Float payload")
        return float(value)
    if type_name == "List":
        if not isinstance(value, Mapping) or set(value) != {"itemType", "items"}: raise TypeError("invalid List payload")
        kind, items = value["itemType"], value["items"]
        if kind not in _TYPES - {"DGeometry", "DResource"} or not isinstance(items, list): raise TypeError("invalid List items")
        return TypedList((decode_value(kind, x) for x in items), item_type=kind)
    if type_name == "DDict":
        if not isinstance(value, Mapping) or any(not isinstance(k, str) for k in value):
            raise TypeError("invalid DDict payload")
        decoded = {}
        for key, item in value.items():
            if not isinstance(item, Mapping) or set(item) != {"type", "value"} or item["type"] not in _TYPES:
                raise TypeError("invalid typed DDict entry")
            if item["type"] in {"DGeometry", "DResource"}:
                raise TypeError("Geometry and Resource values cannot be nested in DDict")
            # A bare None loses the declaration needed to faithfully encode a
            # nullable DDict entry again. Keep nulls explicitly typed.
            decoded[key] = (typed(item["type"], None) if item["value"] is None
                            else decode_value(item["type"], item["value"]))
        return DDict(decoded)
    if type_name in {"Date", "Time", "LocalTime", "DateTime", "LocalDateTime"}:
        if not isinstance(value, str): raise TypeError(f"invalid {type_name} payload")
        if type_name == "Date": return NeoDate.from_iso_format(value)
        if type_name in {"Time", "LocalTime"}: return NeoTime.from_iso_format(value)
        parsed = NeoDateTime.from_iso_format(value)
        if type_name == "DateTime" and not _has_zone(parsed): raise TypeError("invalid DateTime offset")
        if type_name == "LocalDateTime" and _has_zone(parsed): raise TypeError("invalid LocalDateTime offset")
        return parsed
    if type_name == "Duration":
        if not isinstance(value, Mapping) or set(value) != {"months", "days", "seconds", "nanoseconds"}:
            raise TypeError("invalid Duration payload")
        if any(type(value[k]) is not int for k in value): raise TypeError("invalid Duration components")
        return Duration(months=value["months"], days=value["days"], seconds=value["seconds"], nanoseconds=value["nanoseconds"])
    if type_name == "Point":
        if not isinstance(value, Mapping) or set(value) != {"srid", "coordinates"}:
            raise TypeError("invalid Point payload")
        srid, coordinates = value["srid"], value["coordinates"]
        if type(srid) is not int or not isinstance(coordinates, list) or any(type(c) not in {int, float} or not math.isfinite(c) for c in coordinates):
            raise TypeError("invalid Point coordinates")
        point_type = {4326: WGS84Point, 4979: WGS84Point, 7203: CartesianPoint, 9157: CartesianPoint}.get(srid)
        if point_type is None: raise TypeError(f"Unsupported Neo4j Point SRID {srid}")
        point = point_type(coordinates)
        if point.srid != srid: raise TypeError(f"Point SRID {srid} does not match supported dimension")
        return point
    if type_name == "DGeometry":
        if not isinstance(value, Mapping): raise TypeError("invalid DGeometry payload")
        return DGeometry(from_dict={"type": "DGeometry", "value": dict(value)})
    if type_name == "DResource":
        if not isinstance(value, Mapping): raise TypeError("invalid DResource payload")
        return DResource(from_dict={"type": "DResource", "value": dict(value)})
    return value


def encode_value_any(value: Any, *, collection=False) -> Any:
    if isinstance(value, TypedValue):
        if collection and value.type in {"DGeometry", "DResource"}: raise TypeError("Geometry and Resource cannot be nested in collections")
        return value.to_dict()
    if isinstance(value, DGeometry):
        if collection: raise TypeError("Geometry cannot be nested in collections")
        return encode_value("DGeometry", value)
    if isinstance(value, DResource):
        if collection: raise TypeError("Resource cannot be nested in collections")
        return encode_value("DResource", value)
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value): raise TypeError("map keys must be strings")
        return {k: encode_value_any(v, collection=True) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return encode_value("List", value)
    kind = _infer_type(value)
    return encode_value(kind, value)


def _infer_type(value: Any) -> str:
    if value is None: raise TypeError("Null requires an explicit declared or typed type")
    if type(value) is bool: return "Boolean"
    if type(value) is int: return "Integer"
    if type(value) is float: return "Float"
    if isinstance(value, str): return "String"
    if isinstance(value, (datetime, NeoDateTime)): return "DateTime" if _has_zone(value) else "LocalDateTime"
    if isinstance(value, (time, NeoTime)): return "Time" if _has_zone(value) else "LocalTime"
    if isinstance(value, (date, NeoDate)): return "Date"
    if isinstance(value, Duration): return "Duration"
    if isinstance(value, Point): return "Point"
    if isinstance(value, DDict): return "DDict"
    if isinstance(value, DGeometry): return "DGeometry"
    if isinstance(value, DResource): return "DResource"
    if isinstance(value, Mapping): return "DDict"
    if isinstance(value, (list, tuple)): return "List"
    raise TypeError(f"Unsupported Deposit value type {type(value).__name__}")


def _encode_structure(value: Any) -> Any:
    """Validate explicit geometry/resource structure without guessing null types."""
    if value is None or type(value) in {bool, int, str}:
        return value
    if type(value) is float:
        if not math.isfinite(value): raise TypeError("structured values must be finite")
        return value
    if isinstance(value, TypedValue):
        return value.to_dict()
    if isinstance(value, (DGeometry, DResource)):
        raise TypeError("geometry and resource values cannot be nested in collections")
    if isinstance(value, Mapping):
        if any(not isinstance(k, str) for k in value): raise TypeError("map keys must be strings")
        return {k: _encode_structure(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_encode_structure(v) for v in value]
    raise TypeError(f"Unsupported structured value {type(value).__name__}")


def _encode_ddict_entry(value: Any) -> dict[str, Any]:
    if isinstance(value, TypedValue):
        if value.type in {"DGeometry", "DResource"}:
            raise TypeError("Geometry and Resource values cannot be nested in DDict")
        return value.to_dict()
    if isinstance(value, DType):
        if value._type in {"DGeometry", "DResource"}:
            raise TypeError("Geometry and Resource values cannot be nested in DDict")
        return value.to_dict()
    kind = _infer_type(value)
    if kind in {"DGeometry", "DResource"}:
        raise TypeError("Geometry and Resource values cannot be nested in DDict")
    return {"type": kind, "value": encode_value(kind, value)}


def _has_zone(value: Any) -> bool:
    if getattr(value, "tzinfo", None) is None:
        return False
    try: return value.utcoffset() is not None
    except Exception: return False


def _encode_exchange(type_name: str, payload: Any) -> Any:
    """Convert exact integers to the JSON-package decimal-string form."""
    if payload is None: return None
    if type_name == "Integer": return str(payload)
    if type_name == "List":
        item_type = payload["itemType"]
        return {"itemType": item_type, "items": [_encode_exchange(item_type, item) for item in payload["items"]]}
    if type_name == "DDict":
        return {k: ({"type": v["type"], "value": _encode_exchange(v["type"], v["value"])}
                    if isinstance(v, Mapping) and set(v) == {"type", "value"} else v)
                for k, v in payload.items()}
    return payload
