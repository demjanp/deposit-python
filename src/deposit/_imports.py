"""Fixed-input Deposit Exchange packaging for Stage 5 import workflows."""

from __future__ import annotations

from collections.abc import Mapping
import hashlib
import io
import json
from typing import Any
import zipfile


GRAPH_CATEGORIES = (
    "classes", "objects", "descriptors", "geometries", "memberships",
    "relationships", "classRelationships", "patterns", "userTools", "resources",
)
MANIFEST_FIELDS = (
    "packageId", "sourceNamespaceId", "name", "description", "depositVersion",
    "historyMode", "exportedRevision", "createdAt", "rootIds", "scope",
    "includeHistory",
)


def resource_free_exchange_package(
    snapshot: Mapping[str, Any], *, require_subgraph: bool = False
) -> bytes:
    """Serialize a detached manifest/graph snapshot as the server's Exchange ZIP.

    Resource payloads require a separate public shape decision. This path only
    packages snapshots whose graph and archive file list contain no resources.
    """
    if not isinstance(snapshot, Mapping) or set(snapshot) - {"manifest", "graph", "history"}:
        raise TypeError("Import snapshot must contain only manifest, graph, and optional history")
    if "manifest" not in snapshot or "graph" not in snapshot:
        raise TypeError("Import snapshot requires manifest and graph mappings")
    manifest = snapshot["manifest"]
    graph = snapshot["graph"]
    if not isinstance(manifest, dict) or not isinstance(graph, dict):
        raise TypeError("Import snapshot manifest and graph must be portable mappings")
    if any(name not in manifest for name in MANIFEST_FIELDS):
        raise ValueError("Import snapshot manifest is incomplete")
    if set(graph) != set(GRAPH_CATEGORIES):
        raise ValueError("Import snapshot graph categories are incomplete")
    if any(not isinstance(graph[name], list) for name in GRAPH_CATEGORIES):
        raise TypeError("Import snapshot graph categories must be lists")
    if graph["resources"]:
        raise ValueError("Resource-bearing import snapshots require the approved content mapping")
    if _contains_resource_reference(snapshot):
        raise ValueError("Resource-bearing import snapshots require the approved content mapping")
    scope = manifest.get("scope")
    if scope not in {"subgraph", "namespace"}:
        raise ValueError("Import snapshot scope must be subgraph or namespace")
    if require_subgraph and scope != "subgraph":
        raise ValueError("import_subtree requires a subgraph snapshot")
    history = snapshot.get("history")
    include_history = manifest.get("includeHistory")
    if type(include_history) is not bool or (history is not None) != include_history:
        raise ValueError("Import snapshot history member does not match its manifest")
    if history is not None and not isinstance(history, dict):
        raise TypeError("Import snapshot history must be a portable mapping")
    if scope == "subgraph" and history is not None:
        raise ValueError("Subgraph import snapshots cannot include Namespace history")

    supplied_files = manifest.get("files", {})
    if not isinstance(supplied_files, dict):
        raise TypeError("Import snapshot files must be a mapping")
    allowed_supplied = {"graph.json"} | ({"history.json"} if history is not None else set())
    if set(supplied_files) - allowed_supplied:
        raise ValueError("Resource-bearing import snapshots require the approved content mapping")

    graph_bytes = _json_bytes(graph)
    members = {"graph.json": graph_bytes}
    if history is not None:
        members["history.json"] = _json_bytes(history)
    package_manifest = dict(manifest)
    package_manifest["files"] = {
        name: {"bytes": len(content), "sha256": hashlib.sha256(content).hexdigest()}
        for name, content in members.items()
    }
    members["manifest.json"] = _json_bytes(package_manifest)

    output = io.BytesIO()
    with zipfile.ZipFile(output, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name in ("manifest.json", "graph.json", "history.json"):
            content = members.get(name)
            if content is None:
                continue
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o600 << 16
            archive.writestr(info, content)
    return output.getvalue()


def _json_bytes(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, allow_nan=False, separators=(",", ":")
    ).encode("utf-8")


def _contains_resource_reference(value: Any) -> bool:
    if isinstance(value, Mapping):
        if (
            "resourceId" in value
            or value.get("kind") in ("Resource", "DResource")
            or value.get("entityKind") in ("Resource", "DResource")
        ):
            return True
        return any(_contains_resource_reference(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_resource_reference(item) for item in value)
    return False
