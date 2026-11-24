"""Lossless NetworkX adapter for Deposit Exchange graph snapshots."""
from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from datetime import datetime, timezone
import json
import uuid
import math
from typing import Any

import networkx as nx

GRAPH_CATEGORIES= (
    "classes", "objects", "descriptors", "geometries", "memberships",
    "relationships", "classRelationships", "patterns", "userTools", "resources",
)

_NODE_KINDS = {"Class", "Object"}
_EDGE_KINDS = {"Membership", "Relationship", "ClassRelationship"}


def to_networkx(snapshot: Mapping[str, Any]):
    value = _portable_snapshot(snapshot)
    manifest, records = value["manifest"], value["graph"]
    namespace_id = manifest["sourceNamespaceId"]
    revision = manifest["exportedRevision"]
    graph = nx.MultiDiGraph()
    graph.graph.update(
        namespaceId=namespace_id,
        revision=revision,
        depositVersion=manifest.get("depositVersion"),
        patterns=deepcopy(records["patterns"]),
        resources=deepcopy(records["resources"]),
        geometries=deepcopy(records["geometries"]),
        userTools=deepcopy(records["userTools"]),
        scope=manifest["scope"],
        rootIds=deepcopy(manifest["rootIds"]),
    )

    classes = {record["id"]: record for record in records["classes"]}
    objects = {record["id"]: record for record in records["objects"]}
    if len(classes) != len(records["classes"]) or len(objects) != len(records["objects"]):
        raise ValueError("Snapshot contains duplicate node identities")
    descriptor_values: dict[str, dict[str, Any]] = {}
    for descriptor in records["descriptors"]:
        owner = descriptor.get("objectId")
        if not isinstance(owner, str) or owner not in objects:
            raise ValueError("Descriptor has a missing or invalid Object endpoint")
        if descriptor.get("name") in descriptor_values.setdefault(owner, {}):
            raise ValueError("Snapshot contains duplicate Object Descriptor names")
        # Descriptor identity and provenance are visible graph data. Keeping
        # the complete current record here avoids a hidden snapshot sidecar.
        visible = deepcopy(descriptor)
        visible.update(kind="Descriptor", namespaceId=namespace_id, objectId=owner)
        descriptor_values[owner][descriptor["name"]] = visible

    for record in records["classes"]:
        node = _node_record(record, namespace_id, "Class")
        graph.add_node((namespace_id, "Class", record["id"]), **node)
    for record in records["objects"]:
        node = _node_record(record, namespace_id, "Object")
        node["descriptors"] = descriptor_values.get(record["id"], {})
        graph.add_node((namespace_id, "Object", record["id"]), **node)

    edge_ids = set()
    for membership in records["memberships"]:
        object_id, class_id = membership.get("objectId"), membership.get("classId")
        if object_id not in objects or class_id not in classes:
            raise ValueError("Membership has a missing or invalid endpoint")
        _add_edge(graph, namespace_id, "Membership", object_id, class_id, membership, edge_ids)
    for kind, category, endpoint_kind in (
        ("Relationship", "relationships", "Object"),
        ("ClassRelationship", "classRelationships", "Class"),
    ):
        known = objects if endpoint_kind == "Object" else classes
        for record in records[category]:
            source, target = record.get("sourceId"), record.get("targetId")
            if source not in known or target not in known:
                raise ValueError(f"{kind} has a missing or invalid endpoint")
            _add_edge(graph, namespace_id, kind, source, target, record, edge_ids)
    return graph


def from_networkx(graph) -> dict[str, Any]:
    if not isinstance(graph, nx.MultiDiGraph):
        raise TypeError("from_networkx requires a networkx.MultiDiGraph")
    frozen_nodes, frozen_edges, metadata = _freeze_graph(graph)
    namespace_id = metadata.get("namespaceId")
    revision = metadata.get("revision")
    if not isinstance(namespace_id, str) or not namespace_id:
        raise ValueError("Graph metadata requires a source namespaceId")
    if type(revision) is not int or revision < 0:
        raise ValueError("Graph metadata requires a nonnegative source revision")
    return _snapshot_from_graph(frozen_nodes, frozen_edges, metadata, namespace_id, revision)


def _portable_snapshot(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(snapshot, Mapping) or set(snapshot) - {"manifest", "graph", "history"}:
        raise TypeError("Deposit graph snapshot must contain manifest, graph and optional history")
    if not isinstance(snapshot.get("manifest"), Mapping) or not isinstance(snapshot.get("graph"), Mapping):
        raise TypeError("Deposit graph snapshot requires manifest and graph mappings")
    try:
        _ensure_json_portable(snapshot)
    except (TypeError, ValueError, OverflowError) as exc:
        raise TypeError("Deposit graph snapshot must contain only finite JSON-portable values") from exc
    value = {"manifest": snapshot["manifest"], "graph": snapshot["graph"]}
    if "history" in snapshot:
        value["history"] = snapshot["history"]
    manifest, records = value["manifest"], value["graph"]
    if any(not isinstance(records.get(name), list) for name in GRAPH_CATEGORIES):
        raise ValueError("Deposit graph snapshot categories are incomplete")
    source_ns = manifest.get("sourceNamespaceId")
    revision = manifest.get("exportedRevision")
    if not isinstance(source_ns, str) or not source_ns or type(revision) is not int or revision < 0:
        raise ValueError("Deposit graph snapshot has invalid source identity or revision")
    return value


def _ensure_json_portable(root: Any) -> None:
    """Validate finite JSON data iteratively without cloning the full snapshot."""
    active: set[int] = set()
    pending = [iter((root,))]
    owners = []
    while pending:
        try:
            value = next(pending[-1])
        except StopIteration:
            pending.pop()
            if owners:
                active.remove(owners.pop())
            continue
        if value is None or type(value) in (str, bool, int):
            if isinstance(value, str):
                for start in range(0, len(value), 64 * 1024):
                    value[start:start + 64 * 1024].encode("utf-8")
            continue
        if type(value) is float:
            if not math.isfinite(value):
                raise ValueError("non-finite number")
            continue
        if type(value) not in (dict, list):
            raise TypeError(f"unsupported JSON value: {type(value).__name__}")
        identity = id(value)
        if identity in active:
            raise ValueError("cyclic JSON value")
        active.add(identity)
        owners.append(identity)
        if type(value) is dict:
            def items(mapping):
                for key, item in mapping.items():
                    if not isinstance(key, str):
                        raise TypeError("JSON object keys must be strings")
                    for start in range(0, len(key), 64 * 1024):
                        key[start:start + 64 * 1024].encode("utf-8")
                    yield item
            pending.append(iter(items(value)))
        else:
            pending.append(iter(value))


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


def _add_edge(graph, namespace_id: str, kind: str, source_id: str, target_id: str, record: dict[str, Any], edge_ids: set):
    identity = record.get("id")
    if not isinstance(identity, str) or not identity:
        raise ValueError(f"{kind} edge requires a stable id")
    if record.get("namespaceId", namespace_id) != namespace_id or record.get("kind", kind) != kind:
        raise ValueError(f"{kind} record metadata contradicts its containing graph category")
    if identity in edge_ids:
        raise ValueError("Snapshot contains duplicate edge identities")
    edge_ids.add(identity)
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
    _ensure_json_portable(value)
    return json.loads(json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")))


def _snapshot_from_graph(nodes, edges, metadata, namespace_id, revision):
    record_lists = {name: [] for name in GRAPH_CATEGORIES}
    by_key = {}
    for key, attrs in nodes:
        if not isinstance(key, (list, tuple)) or len(key) != 3:
            raise ValueError("Node keys must be (namespace_id, kind, id) tuples")
        ns, kind, identity = key
        if ns != namespace_id or kind not in _NODE_KINDS or not isinstance(identity, str) or not identity:
            raise ValueError("Node key has an invalid Namespace, kind or id")
        if attrs.get("id") != identity or attrs.get("kind") != kind:
            raise ValueError("Node key and required id/kind attributes must agree")
        if key in by_key:
            raise ValueError("Graph contains duplicate node keys")
        by_key[tuple(key)] = attrs
        record = {name: deepcopy(value) for name, value in attrs.items() if name != "kind"}
        record["kind"] = kind
        if kind == "Class":
            record_lists["classes"].append(record)
        else:
            record_lists["objects"].append(record)
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
        record["kind"] = kind
        if kind == "Membership":
            if record.get("objectId", source[2]) != source[2] or record.get("classId", target[2]) != target[2]:
                raise ValueError("Membership attributes contradict graph endpoints")
            record.update(objectId=source[2], classId=target[2])
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
    for category in ("resources", "geometries", "userTools"):
        values = metadata.get(category, [])
        if not isinstance(values, list):
            raise TypeError(f"Graph {category} metadata must be a list")
        record_lists[category] = deepcopy(values)
    descriptor_ids = set()
    for record in record_lists["objects"]:
        object_id = record["id"]
        values = object_descriptor_values.get(object_id, {})
        if not isinstance(values, dict):
            raise TypeError("Object descriptors must be a mapping")
        for name, value in values.items():
            if _is_visible_descriptor_record(value):
                descriptor = deepcopy(dict(value))
            else:
                # Accept direct value replacement as a convenient graph edit;
                # new scalar values receive one identity at freeze time.
                descriptor = {"id": str(uuid.uuid4()), "name": name, "value": deepcopy(value),
                              "kind": "Descriptor", "namespaceId": namespace_id, "objectId": object_id}
            if not isinstance(descriptor.get("id"), str) or not descriptor["id"]:
                raise ValueError("Visible Descriptor records require a stable ID")
            if descriptor["id"] in descriptor_ids:
                raise ValueError("Graph contains duplicate Descriptor identities")
            descriptor_ids.add(descriptor["id"])
            descriptor["name"] = name
            record_lists["descriptors"].append(descriptor)
    scope = metadata.get("scope", "namespace")
    root_ids = metadata.get("rootIds", [])
    manifest = {
        "sourceNamespaceId": namespace_id,
        "name": "NetworkX graph", "description": "", "depositVersion": metadata.get("depositVersion"),
        "historyMode": "disabled", "exportedRevision": revision,
        "createdAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "rootIds": deepcopy(root_ids),
        "scope": scope, "includeHistory": False,
        "files": {},
    }
    return {"manifest": manifest, "graph": record_lists}


def _is_visible_descriptor_record(value):
    """Recognize only the complete visible-record shape, not a scientific map."""
    return (
        isinstance(value, Mapping)
        and value.get("kind") == "Descriptor"
        and isinstance(value.get("id"), str) and bool(value.get("id"))
        and isinstance(value.get("namespaceId"), str) and bool(value.get("namespaceId"))
        and isinstance(value.get("objectId"), str) and bool(value.get("objectId"))
        and isinstance(value.get("name"), str) and bool(value.get("name"))
        and "value" in value
    )
