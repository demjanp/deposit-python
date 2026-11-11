"""Fixed-input Deposit Exchange packaging for Stage 5 import workflows."""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import contextmanager
import io
from typing import Any


GRAPH_CATEGORIES = (
    "classes", "objects", "descriptors", "geometries", "memberships",
    "relationships", "classRelationships", "patterns", "userTools", "resources",
)
MANIFEST_FIELDS = (
    "packageId", "sourceNamespaceId", "name", "description", "depositVersion",
    "historyMode", "exportedRevision", "createdAt", "rootIds", "scope",
    "includeHistory",
)


def _validate_snapshot_shape(snapshot, require_subgraph, allow_resources):
    if not isinstance(snapshot, Mapping) or set(snapshot) - {"manifest", "graph", "history"}:
        raise TypeError("Import snapshot must contain only manifest, graph, and optional history")
    manifest, graph = snapshot.get("manifest"), snapshot.get("graph")
    if not isinstance(manifest, dict) or not isinstance(graph, dict):
        raise TypeError("Import snapshot manifest and graph must be portable mappings")
    if any(name not in manifest for name in MANIFEST_FIELDS):
        raise ValueError("Import snapshot manifest is incomplete")
    if set(graph) != set(GRAPH_CATEGORIES):
        raise ValueError("Import snapshot graph categories are incomplete")
    if any(not isinstance(graph[name], list) for name in GRAPH_CATEGORIES):
        raise TypeError("Import snapshot graph categories must be lists")
    if not allow_resources and graph["resources"]:
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
    mode = manifest.get("historyMode")
    if mode not in {"disabled", "transient", "persistent"}:
        raise ValueError("Import snapshot Namespace history mode is invalid")
    if include_history != (scope == "namespace" and mode == "persistent"):
        raise ValueError("Import snapshot history does not match its scope/history mode")
    supplied_files = manifest.get("files", {})
    if not isinstance(supplied_files, dict):
        raise TypeError("Import snapshot files must be a mapping")
    if not allow_resources:
        allowed = {"graph.json"} | ({"history.json"} if history is not None else set())
        if set(supplied_files) - allowed:
            raise ValueError("Resource-bearing import snapshots require the approved content mapping")


@contextmanager
def prepared_exchange_package(snapshot, resources, *, storage, require_subgraph=False, checkpoint=None):
    """Prepare a transport/integrity-checked package in supplied storage.

    The yielded lazy package and its immutable source ZIP belong to this context.
    Graph/history domain semantics remain the Deposit Server's responsibility.
    This is the bounded provider path; Stage 5 decides when workflows adopt it.
    """
    from .exchange import ProtectedChunkFile, validate_exchange_archive, write_exchange_package
    _validate_snapshot_shape(snapshot, require_subgraph, True)
    with ProtectedChunkFile(storage) as staged:
        write_exchange_package(staged, snapshot, resources, checkpoint=checkpoint)
        staged.seek(0)
        with validate_exchange_archive(staged, checkpoint=checkpoint) as package:
            yield package


class _MaterializationStore:
    """Process-memory spools for the explicitly materializing bytes adapter.

    Nothing is written to disk. This compatibility boundary intentionally keeps
    its output and validation storage in memory; scalable callers use the
    context-managed provider path with ProtectedStore instead.
    """
    MAX_RECORD_BYTES = 16 * 1024 * 1024

    def __init__(self):
        self.records = {}

    def write(self, name, value):
        self.records[name] = bytes(value)

    def read(self, name):
        return self.records[name]

    def has_record(self, name):
        return name in self.records

    def delete(self, name):
        self.records.pop(name, None)

    def usage_bytes(self):
        return 0  # This adapter has no filesystem storage.


def resource_free_exchange_package(snapshot: Mapping[str, Any], *, require_subgraph: bool = False) -> bytes:
    """Explicitly materialize resource-free Exchange bytes for existing callers.

    ZIP generation and transport/integrity validation share the bounded Exchange
    implementation. Domain semantics remain the Server's responsibility. The
    bytes result and in-memory validation are intentional
    materialization; this helper must not be used for scalable resource relays.
    """
    from .exchange import validate_exchange_archive, write_exchange_package
    _validate_snapshot_shape(snapshot, require_subgraph, False)
    storage = _MaterializationStore()
    output = io.BytesIO()
    write_exchange_package(output, snapshot, {}, storage=storage)
    output.seek(0)
    with validate_exchange_archive(output, storage=storage):
        pass
    return output.getvalue()
