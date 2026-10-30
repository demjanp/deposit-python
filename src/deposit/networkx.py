"""Optional, lossless NetworkX adapter for Deposit Exchange graph snapshots."""
from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import io
import json
import re
import uuid
import zipfile
from typing import Any

from ._imports import GRAPH_CATEGORIES, MANIFEST_FIELDS

_METADATA_KEY = "snapshot"
_NODE_KINDS = {"Class", "Object"}
_EDGE_KINDS = {"Membership", "Relationship", "ClassRelationship"}


def require_networkx():
    """Load the optional dependency with an actionable installation message."""
    try:
        import networkx as nx
    except ImportError as exc:
        raise ImportError(
            "NetworkX graph conversion requires the optional dependency; install deposit[networkx]."
        ) from exc
    return nx


def to_networkx(snapshot: Mapping[str, Any]):
    nx = require_networkx()
    value = _portable_snapshot(snapshot)
    manifest, records = value["manifest"], value["graph"]
    namespace_id = manifest["sourceNamespaceId"]
    revision = manifest["exportedRevision"]
    graph = nx.MultiDiGraph()
    graph.graph.update(
        namespaceId=namespace_id,
        revision=revision,
        patterns=deepcopy(records["patterns"]),
        **{_METADATA_KEY: value},
    )

    classes = {record["id"]: record for record in records["classes"]}
    objects = {record["id"]: record for record in records["objects"]}
    descriptor_values: dict[str, dict[str, Any]] = {}
    for descriptor in records["descriptors"]:
        owner = descriptor.get("objectId")
        if not isinstance(owner, str) or owner not in objects:
            raise ValueError("Descriptor has a missing or invalid Object endpoint")
        if descriptor.get("name") in descriptor_values.setdefault(owner, {}):
            raise ValueError("Snapshot contains duplicate Object Descriptor names")
        descriptor_values[owner][descriptor["name"]] = deepcopy(descriptor.get("value"))

    for record in records["classes"]:
        node = _node_record(record, namespace_id, "Class")
        graph.add_node((namespace_id, "Class", record["id"]), **node)
    for record in records["objects"]:
        node = _node_record(record, namespace_id, "Object")
        node["descriptors"] = descriptor_values.get(record["id"], {})
        graph.add_node((namespace_id, "Object", record["id"]), **node)

    for membership in records["memberships"]:
        object_id, class_id = membership.get("objectId"), membership.get("classId")
        if object_id not in objects or class_id not in classes:
            raise ValueError("Membership has a missing or invalid endpoint")
        _add_edge(graph, namespace_id, "Membership", object_id, class_id, membership)
    for kind, category, endpoint_kind in (
        ("Relationship", "relationships", "Object"),
        ("ClassRelationship", "classRelationships", "Class"),
    ):
        known = objects if endpoint_kind == "Object" else classes
        for record in records[category]:
            source, target = record.get("sourceId"), record.get("targetId")
            if source not in known or target not in known:
                raise ValueError(f"{kind} has a missing or invalid endpoint")
            _add_edge(graph, namespace_id, kind, source, target, record)
    _validate_structural_completeness(records, namespace_id)
    return graph


def from_networkx(graph) -> dict[str, Any]:
    nx = require_networkx()
    if not isinstance(graph, nx.MultiDiGraph):
        raise TypeError("from_networkx requires a networkx.MultiDiGraph")
    frozen_nodes, frozen_edges, metadata = _freeze_graph(graph)
    namespace_id = metadata.get("namespaceId")
    revision = metadata.get("revision")
    if not isinstance(namespace_id, str) or not namespace_id:
        raise ValueError("Graph metadata requires a source namespaceId")
    if type(revision) is not int or revision < 0:
        raise ValueError("Graph metadata requires a nonnegative source revision")
    snapshot = metadata.get(_METADATA_KEY)
    if snapshot is not None:
        value = _portable_snapshot(snapshot)
        _check_graph_matches_snapshot(frozen_nodes, frozen_edges, metadata, value)
        return value
    return _snapshot_from_graph(frozen_nodes, frozen_edges, metadata, namespace_id, revision)


def _portable_snapshot(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(snapshot, Mapping) or set(snapshot) - {"manifest", "graph", "history"}:
        raise TypeError("Deposit graph snapshot must contain manifest, graph and optional history")
    if not isinstance(snapshot.get("manifest"), Mapping) or not isinstance(snapshot.get("graph"), Mapping):
        raise TypeError("Deposit graph snapshot requires manifest and graph mappings")
    try:
        value = json.loads(json.dumps(snapshot, ensure_ascii=False, allow_nan=False, separators=(",", ":")))
    except (TypeError, ValueError, OverflowError) as exc:
        raise TypeError("Deposit graph snapshot must contain only finite JSON-portable values") from exc
    manifest, records = value["manifest"], value["graph"]
    if any(name not in manifest for name in MANIFEST_FIELDS):
        raise ValueError("Deposit graph snapshot manifest is incomplete")
    if set(records) != set(GRAPH_CATEGORIES) or any(not isinstance(records[name], list) for name in GRAPH_CATEGORIES):
        raise ValueError("Deposit graph snapshot categories are incomplete")
    source_ns = manifest.get("sourceNamespaceId")
    revision = manifest.get("exportedRevision")
    if not isinstance(source_ns, str) or not source_ns or type(revision) is not int or revision < 0:
        raise ValueError("Deposit graph snapshot has invalid source identity or revision")
    if records["resources"] or _contains_resource_reference(value):
        # Export may retain references, but imports cannot fabricate resource content.
        return value
    return value


def _node_record(record: dict[str, Any], namespace_id: str, kind: str) -> dict[str, Any]:
    identity = record.get("id")
    if not isinstance(identity, str) or not identity:
        raise ValueError(f"{kind} record requires a stable id")
    if record.get("namespaceId", namespace_id) != namespace_id:
        raise ValueError(f"{kind} record Namespace contradicts the graph snapshot")
    if record.get("kind", kind) != kind:
        raise ValueError(f"{kind} record kind contradicts its containing graph category")
    attrs = deepcopy(record)
    attrs.update(id=identity, namespaceId=namespace_id, kind=kind)
    return attrs


def _add_edge(graph, namespace_id: str, kind: str, source_id: str, target_id: str, record: dict[str, Any]):
    identity = record.get("id")
    if not isinstance(identity, str) or not identity:
        raise ValueError(f"{kind} edge requires a stable id")
    if record.get("namespaceId", namespace_id) != namespace_id or record.get("kind", kind) != kind:
        raise ValueError(f"{kind} record metadata contradicts its containing graph category")
    expected_endpoints = {
        "Membership": (record.get("objectId", source_id), record.get("classId", target_id)),
        "Relationship": (record.get("sourceId", source_id), record.get("targetId", target_id)),
        "ClassRelationship": (record.get("sourceId", source_id), record.get("targetId", target_id)),
    }[kind]
    if expected_endpoints != (source_id, target_id):
        raise ValueError(f"{kind} record endpoints contradict graph endpoints")
    source_kind, target_kind = {
        "Membership": ("Object", "Class"),
        "Relationship": ("Object", "Object"),
        "ClassRelationship": ("Class", "Class"),
    }[kind]
    attrs = deepcopy(record)
    attrs.update(id=identity, kind=kind)
    graph.add_edge((namespace_id, source_kind, source_id), (namespace_id, target_kind, target_id), key=identity, **attrs)


def _freeze_graph(graph):
    try:
        nodes = tuple((deepcopy(key), _json_freeze(dict(attrs))) for key, attrs in graph.nodes(data=True))
        edges = tuple((deepcopy(source), deepcopy(target), deepcopy(key), _json_freeze(dict(attrs)))
                      for source, target, key, attrs in graph.edges(keys=True, data=True))
        metadata = _json_freeze(dict(graph.graph))
        return nodes, edges, metadata
    except (TypeError, ValueError, OverflowError) as exc:
        raise TypeError("NetworkX graph data must contain only finite JSON-portable values") from exc


def _json_freeze(value):
    return json.loads(json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")))


def _check_graph_matches_snapshot(nodes, edges, metadata, snapshot):
    manifest, records = snapshot["manifest"], snapshot["graph"]
    namespace_id = manifest["sourceNamespaceId"]
    if metadata.get("namespaceId") != namespace_id or metadata.get("revision") != manifest["exportedRevision"]:
        raise ValueError("Graph identity/revision metadata contradicts its detached snapshot")
    if metadata.get("patterns") != records["patterns"]:
        raise ValueError("Graph patterns metadata contradicts its detached snapshot")
    expected = to_networkx(snapshot)
    exp_nodes = {key: dict(attrs) for key, attrs in expected.nodes(data=True)}
    actual_nodes = dict(nodes)
    if actual_nodes != exp_nodes:
        raise ValueError("Graph node keys or attributes contradict the detached snapshot")
    exp_edges = {(source, target, key): dict(attrs) for source, target, key, attrs in
                 ((u, v, k, attrs) for u, v, k, attrs in expected.edges(keys=True, data=True))}
    actual_edges = {(u, v, key): attrs for u, v, key, attrs in edges}
    if actual_edges != exp_edges:
        raise ValueError("Graph edges or attributes contradict the detached snapshot")


def _snapshot_from_graph(nodes, edges, metadata, namespace_id, revision):
    record_lists = {name: [] for name in GRAPH_CATEGORIES}
    by_key = {}
    for key, attrs in nodes:
        if not isinstance(key, (list, tuple)) or len(key) != 3:
            raise ValueError("Node keys must be (namespace_id, kind, id) tuples")
        ns, kind, identity = key
        if ns != namespace_id or kind not in _NODE_KINDS or not isinstance(identity, str) or not identity:
            raise ValueError("Node key has an invalid Namespace, kind or id")
        if attrs.get("id") != identity or attrs.get("namespaceId") != namespace_id or attrs.get("kind") != kind:
            raise ValueError("Node key and required id/namespaceId/kind attributes must agree")
        if key in by_key:
            raise ValueError("Graph contains duplicate node keys")
        by_key[tuple(key)] = attrs
        record = {name: deepcopy(value) for name, value in attrs.items() if name != "kind"}
        if kind == "Class":
            record_lists["classes"].append(record)
        else:
            record_lists["objects"].append(record)
    object_by_id = {item["id"]: item for item in record_lists["objects"]}
    class_by_id = {item["id"]: item for item in record_lists["classes"]}
    object_descriptor_values = {
        record["id"]: deepcopy(record.get("descriptors", {}))
        for record in record_lists["objects"]
    }
    for record in record_lists["objects"]:
        record.pop("descriptors", None)
    seen_edges = set()
    seen_edge_ids = set()
    for source, target, key, attrs in edges:
        source, target = tuple(source), tuple(target)
        if source not in by_key or target not in by_key:
            raise ValueError("Edge endpoint is missing from graph nodes")
        kind, identity = attrs.get("kind"), attrs.get("id")
        if kind not in _EDGE_KINDS or not isinstance(identity, str) or key != identity:
            raise ValueError("Edge key, id and kind must agree")
        if (source, target, key) in seen_edges or identity in seen_edge_ids:
            raise ValueError("Graph contains duplicate edge identities")
        seen_edges.add((source, target, key))
        seen_edge_ids.add(identity)
        expected_kinds = {"Membership": ("Object", "Class"), "Relationship": ("Object", "Object"), "ClassRelationship": ("Class", "Class")}[kind]
        if (source[1], target[1]) != expected_kinds:
            raise ValueError(f"{kind} has invalid endpoint kinds")
        record = {name: deepcopy(value) for name, value in attrs.items() if name != "kind"}
        if kind == "Membership":
            if record.get("objectId", source[2]) != source[2] or record.get("classId", target[2]) != target[2]:
                raise ValueError("Membership attributes contradict graph endpoints")
            record.update(objectId=source[2], classId=target[2])
            object_record = object_by_id[source[2]]
            if object_record.get("classId") != target[2]:
                raise ValueError("Membership endpoint contradicts Object classId")
            record_lists["memberships"].append(record)
        else:
            if record.get("sourceId", source[2]) != source[2] or record.get("targetId", target[2]) != target[2]:
                raise ValueError(f"{kind} attributes contradict graph endpoints")
            record.update(sourceId=source[2], targetId=target[2])
            record_lists["relationships" if kind == "Relationship" else "classRelationships"].append(record)
    patterns = metadata.get("patterns", [])
    if not isinstance(patterns, list):
        raise TypeError("Graph patterns metadata must be a list")
    record_lists["patterns"] = deepcopy(patterns)
    declarations = {}
    for record in record_lists["classes"]:
        declarations[record["id"]] = {item.get("name"): item for item in record.get("descriptors", []) if isinstance(item, dict)}
    for record in record_lists["objects"]:
        class_id = record.get("classId")
        if class_id not in declarations:
            raise ValueError("Object references a missing Class declaration set")
        values = object_descriptor_values.get(record["id"], {})
        if not isinstance(values, dict):
            raise TypeError("Object descriptors must be a mapping")
        for name, value in values.items():
            declaration = declarations[class_id].get(name)
            if declaration is None:
                raise ValueError(f"Object Descriptor {name!r} has no included Class declaration")
            from .values import decode_value
            try:
                decode_value(declaration.get("type"), value)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"Object Descriptor {name!r} does not match its declaration") from exc
            record_lists["descriptors"].append({
                "id": str(uuid.uuid4()), "namespaceId": namespace_id,
                "objectId": record["id"], "name": name, "value": deepcopy(value),
            })
    if record_lists["resources"] or _contains_resource_reference(record_lists):
        raise ValueError("Resource-bearing NetworkX imports require the approved content mapping")
    manifest = metadata.get("manifest")
    if manifest is None:
        manifest = {
            "packageId": str(uuid.uuid4()), "sourceNamespaceId": namespace_id,
            "name": "NetworkX graph", "description": "", "depositVersion": "2",
            "historyMode": "none", "exportedRevision": revision,
            "createdAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "rootIds": sorted(object_by_id), "scope": "namespace", "includeHistory": False,
            "files": {},
        }
    else:
        manifest = deepcopy(manifest)
        if manifest.get("sourceNamespaceId") != namespace_id or manifest.get("exportedRevision") != revision:
            raise ValueError("Graph manifest contradicts source namespace/revision metadata")
    _validate_structural_completeness(record_lists, namespace_id)
    return {"manifest": manifest, "graph": record_lists}


def _validate_structural_completeness(records, namespace_id):
    classes = {item.get("id"): item for item in records["classes"]}
    objects = {item.get("id"): item for item in records["objects"]}
    edge_ids = set()
    for category in ("classes", "objects"):
        identities = [item.get("id") for item in records[category]]
        if any(not isinstance(item, str) or not item for item in identities) or len(set(identities)) != len(identities):
            raise ValueError(f"{category} require unique stable IDs")
        for item in records[category]:
            if item.get("namespaceId", namespace_id) != namespace_id:
                raise ValueError("Entity Namespace attribute contradicts graph metadata")
    membership_by_object = {}
    for membership in records["memberships"]:
        if not isinstance(membership.get("id"), str) or membership["id"] in edge_ids:
            raise ValueError("Membership edges require unique stable IDs")
        edge_ids.add(membership["id"])
        if membership.get("objectId") not in objects or membership.get("classId") not in classes:
            raise ValueError("Membership endpoint is missing from the graph")
        prior = membership_by_object.setdefault(membership["objectId"], membership["classId"])
        if prior != membership["classId"]:
            raise ValueError("Object has inconsistent Membership edges")
    if set(membership_by_object) != set(objects):
        raise ValueError("Every Object requires exactly one explicit Membership edge")
    for object_id, item in objects.items():
        if membership_by_object[object_id] != item.get("classId"):
            raise ValueError("Membership edge contradicts Object classId")
    declarations = {cid: {d.get("name"): d for d in record.get("descriptors", []) if isinstance(d, dict)} for cid, record in classes.items()}
    declaration_ids = set()
    for class_id, class_record in classes.items():
        names = set()
        for declaration in class_record.get("descriptors", []):
            if (
                not isinstance(declaration, Mapping)
                or not isinstance(declaration.get("id"), str)
                or not declaration["id"]
                or not isinstance(declaration.get("name"), str)
                or not declaration["name"]
                or not isinstance(declaration.get("type"), str)
                or not declaration["type"]
                or type(declaration.get("order", 0)) is not int
                or declaration.get("order", 0) < 0
            ):
                raise ValueError("Class declarations require stable IDs, names, types and valid order")
            if declaration["id"] in declaration_ids or declaration["name"] in names:
                raise ValueError("Class declarations must have unique IDs and names")
            declaration_ids.add(declaration["id"])
            names.add(declaration["name"])
    descriptor_ids = set()
    for descriptor in records["descriptors"]:
        if (
            not isinstance(descriptor.get("id"), str)
            or not descriptor["id"]
            or descriptor["id"] in descriptor_ids
            or descriptor.get("namespaceId", namespace_id) != namespace_id
        ):
            raise ValueError("Descriptors require unique stable IDs in the graph Namespace")
        descriptor_ids.add(descriptor["id"])
        owner = descriptor.get("objectId")
        if owner not in objects:
            raise ValueError("Descriptor references a missing Object")
        declaration = declarations[objects[owner]["classId"]].get(descriptor.get("name"))
        if declaration is None:
            raise ValueError("Descriptor has no included Class declaration")
    geometry_by_id = {item.get("id"): item for item in records["geometries"]}
    if len(geometry_by_id) != len(records["geometries"]) or None in geometry_by_id:
        raise ValueError("Geometry records require unique stable IDs")
    descriptor_by_id = {item.get("id"): item for item in records["descriptors"]}
    geometry_descriptor_ids = set()
    for descriptor in records["descriptors"]:
        payload = descriptor.get("value")
        if isinstance(payload, Mapping) and payload.get("geometryId"):
            geometry = geometry_by_id.get(payload["geometryId"])
            if (
                geometry is None
                or geometry.get("id") != payload["geometryId"]
                or geometry.get("descriptorId") != descriptor.get("id")
                or geometry.get("namespaceId", namespace_id) != namespace_id
                or descriptor.get("id") in geometry_descriptor_ids
            ):
                raise ValueError("Geometry Descriptor reference is incomplete or inconsistent")
            geometry_descriptor_ids.add(descriptor["id"])
            for field in ("geometryType", "wkt", "srid", "verticalSrid", "axisOrder", "dimensions"):
                if field not in geometry or field not in payload or geometry[field] != payload[field]:
                    raise ValueError(f"Geometry Descriptor and geometry record disagree on {field}")
            if not isinstance(geometry["wkt"], str) or not geometry["wkt"]:
                raise ValueError("Geometry record requires nonempty WKT")
            if type(geometry["dimensions"]) is not int or geometry["dimensions"] not in {2, 3}:
                raise ValueError("Geometry record has invalid dimensions")
            if geometry["axisOrder"] not in {"xy", "yx"}:
                raise ValueError("Geometry record has invalid axis order")
            if geometry["srid"] is not None and type(geometry["srid"]) is not int:
                raise ValueError("Geometry record has invalid SRID")
            if geometry["verticalSrid"] is not None and type(geometry["verticalSrid"]) is not int:
                raise ValueError("Geometry record has invalid vertical SRID")
    for geometry in records["geometries"]:
        if geometry.get("descriptorId") not in descriptor_by_id or geometry.get("descriptorId") not in geometry_descriptor_ids:
            raise ValueError("Geometry record has no matching Descriptor reference")
    for category in ("relationships", "classRelationships"):
        for edge in records[category]:
            if not isinstance(edge.get("id"), str) or edge["id"] in edge_ids:
                raise ValueError("Relationship edges require unique stable IDs")
            edge_ids.add(edge["id"])
            known = objects if category == "relationships" else classes
            if edge.get("sourceId") not in known or edge.get("targetId") not in known:
                raise ValueError("Relationship endpoint is missing from the graph")


def _contains_resource_reference(value):
    if isinstance(value, Mapping):
        if "resourceId" in value or value.get("kind") in {"Resource", "DResource"} or value.get("entityKind") in {"Resource", "DResource"}:
            return True
        return any(_contains_resource_reference(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_resource_reference(item) for item in value)
    return False


def read_export_result(store, result):
    """Return a complete inline export or validate a staged Exchange ZIP in memory."""
    if isinstance(result, Mapping) and "manifest" in result and "graph" in result:
        return _portable_snapshot(result)
    resource_id = None
    if isinstance(result, Mapping):
        resource_id = result.get("packageResourceId") or result.get("resourceId")
        if isinstance(result.get("package"), Mapping):
            resource_id = resource_id or result["package"].get("resourceId")
    if not isinstance(resource_id, str) or not resource_id:
        raise ValueError("Export returned neither an inline graph snapshot nor a staged package reference")
    from .values import DResource
    resource = DResource(from_dict={
        "type": "DResource",
        "value": {"resourceId": resource_id, "namespaceId": store.namespace_id},
    })
    with resource.open(store) as reader:
        raw = reader.read()
        metadata = resource._value
    expected_bytes = result.get("bytes") if isinstance(result, Mapping) else None
    expected_checksum = (result.get("checksum") or result.get("sha256")) if isinstance(result, Mapping) else None
    if expected_bytes is not None and expected_bytes != len(raw):
        raise ValueError("Staged Exchange export byte length contradicts its resource metadata")
    if expected_checksum is not None and expected_checksum != hashlib.sha256(raw).hexdigest():
        raise ValueError("Staged Exchange export checksum contradicts its resource metadata")
    if metadata.get("bytes") is not None and metadata["bytes"] != len(raw):
        raise ValueError("Staged Exchange resource length verification failed")
    if metadata.get("checksum") is not None and metadata["checksum"] != hashlib.sha256(raw).hexdigest():
        raise ValueError("Staged Exchange resource checksum verification failed")
    return _read_exchange_zip(raw)


def _read_exchange_zip(raw):
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            infos = archive.infolist()
            names = [info.filename for info in infos]
            if len(names) != len(set(names)) or any(info.is_dir() for info in infos):
                raise ValueError("Staged Exchange ZIP contains duplicate or directory entries")
            if not {"manifest.json", "graph.json"}.issubset(names):
                raise ValueError("Staged Exchange ZIP requires manifest.json and graph.json")
            if set(names) - {"manifest.json", "graph.json", "history.json"} and any(
                not re.fullmatch(r"resources/[0-9a-f]{64}", name)
                for name in names if name not in {"manifest.json", "graph.json", "history.json"}
            ):
                raise ValueError("Staged Exchange ZIP contains an unsupported member")
            raw_manifest = archive.read("manifest.json")
            manifest = json.loads(raw_manifest)
            file_index = manifest.get("files")
            if not isinstance(file_index, dict) or set(file_index) != set(names) - {"manifest.json"}:
                raise ValueError("Staged Exchange ZIP manifest file index is incomplete")
            members = {}
            for name, entry in file_index.items():
                data = archive.read(name)
                if not isinstance(entry, Mapping) or entry.get("bytes") != len(data) or entry.get("sha256") != hashlib.sha256(data).hexdigest():
                    raise ValueError(f"Staged Exchange ZIP checksum mismatch for {name}")
                members[name] = data
            result = {"manifest": manifest, "graph": json.loads(members["graph.json"])}
            if "history.json" in members:
                result["history"] = json.loads(members["history.json"])
            _validate_staged_resources(result, members)
            return _portable_snapshot(result)
    except (zipfile.BadZipFile, KeyError, json.JSONDecodeError) as exc:
        raise ValueError("Staged Exchange export is not a valid checked ZIP package") from exc


def _validate_staged_resources(snapshot, members):
    records = snapshot.get("graph", {}).get("resources")
    if not isinstance(records, list):
        raise ValueError("Staged Exchange graph resource index is invalid")
    expected_entries = set()
    for resource in records:
        if not isinstance(resource, Mapping):
            raise ValueError("Staged Exchange resource record is invalid")
        entry = resource.get("entry")
        checksum = resource.get("sha256")
        byte_count = resource.get("bytes")
        if (
            not isinstance(entry, str)
            or not re.fullmatch(r"resources/[0-9a-f]{64}", entry)
            or entry in expected_entries
            or entry not in members
            or not isinstance(checksum, str)
            or not re.fullmatch(r"[0-9a-f]{64}", checksum)
            or type(byte_count) is not int
            or byte_count < 0
        ):
            raise ValueError("Staged Exchange resource entry is incomplete or unsafe")
        content = members[entry]
        if len(content) != byte_count or hashlib.sha256(content).hexdigest() != checksum:
            raise ValueError("Staged Exchange resource content checksum mismatch")
        expected_entries.add(entry)
    actual_entries = {name for name in members if name.startswith("resources/")}
    if expected_entries != actual_entries:
        raise ValueError("Staged Exchange ZIP has unlisted or missing resource content")
