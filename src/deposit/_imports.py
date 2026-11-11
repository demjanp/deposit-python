"""Protected Exchange packaging and resource providers for import inputs."""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import contextmanager
import io
from pathlib import Path
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


@contextmanager
def prepared_import_package(snapshot, *, resources=None, resource_source=None, storage,
                            require_subgraph=False, checkpoint=None):
    """Freeze an import snapshot and its content into a protected checked ZIP.

    ``resources`` maps content-addressed Exchange entries to explicit content
    and takes precedence over ``resource_source``. Providers may be bytes,
    paths, caller-owned binary streams, or zero-argument stream factories. A
    Store-like source resolves resource IDs through its authenticated reader.
    """
    package_input = _checked_package_view(snapshot)
    if package_input is not None:
        if resources is not None or resource_source is not None:
            raise ValueError("A self-contained Exchange package cannot be combined with resource providers")
        package_input._assert_open()
        scope = _lazy_scalar(package_input.manifest.get("scope"))
        if require_subgraph and scope != "subgraph":
            raise ValueError("import_subtree requires a subgraph Exchange package")
        # The validated view owns no source bytes. Keep borrowing its checked
        # protected archive through the caller's upload context; never expand
        # graph/history JSON to rebuild the ZIP.
        yield package_input
        return
    if not isinstance(snapshot, Mapping):
        raise TypeError("Import snapshot must be a manifest/graph mapping")
    _validate_snapshot_shape(snapshot, require_subgraph, True)
    if resources is not None and not isinstance(resources, Mapping):
        raise TypeError("resources must map Exchange content entries to providers")

    records = snapshot["graph"]["resources"]
    entries = {}
    for record in records:
        if not isinstance(record, Mapping):
            raise TypeError("Exchange resource records must be mappings")
        entry = record.get("entry")
        if not isinstance(entry, str) or not entry.startswith("resources/"):
            raise ValueError("Exchange resource record has no content entry")
        entries.setdefault(entry, record)

    def path_provider(value):
        path = Path(value)
        def open_path():
            return path.open("rb")
        return open_path

    def normalize(value):
        if isinstance(value, bytes):
            return lambda: io.BytesIO(value)
        if isinstance(value, (str, Path)):
            return path_provider(value)
        if callable(getattr(value, "read", None)):
            return value  # borrowed; write_exchange_package does not close it
        if callable(value):
            return value
        raise TypeError("Resource content providers must be bytes, paths, binary streams, or stream factories")

    explicit = resources or {}
    providers = {}
    source_store = resource_source if callable(getattr(resource_source, "_open_resource", None)) else None
    source_package = resource_source if (
        callable(getattr(resource_source, "iter_content", None))
        or callable(getattr(resource_source, "open_resource", None))
        or callable(getattr(resource_source, "resource_reader", None))
    ) else None
    if source_package is None:
        source_package = _checked_package_view(resource_source)
    if resource_source is not None and source_store is None and source_package is None:
        raise TypeError("resource_source must be an authenticated Store or checked Exchange package")
    unknown_keys = set(explicit) - set(entries)
    if unknown_keys:
        raise ValueError("resources keys must be content entries present in the snapshot")
    if source_store is not None and source_store.namespace_id != snapshot["manifest"].get("sourceNamespaceId"):
        raise ValueError("Authenticated source Store Namespace does not match the Exchange source")
    if source_package is not None:
        source_manifest = getattr(source_package, "manifest", None)
        if callable(source_manifest):
            source_manifest = source_manifest()
        if not callable(getattr(source_manifest, "get", None)):
            raise TypeError("resource_source package must expose its checked manifest")
        source_namespace = _lazy_scalar(source_manifest.get("sourceNamespaceId")) if source_manifest is not None else None
        if not isinstance(source_namespace, str):
            raise TypeError("resource_source package must expose its checked source Namespace")
        if source_namespace != snapshot["manifest"].get("sourceNamespaceId"):
            raise ValueError("Exchange content source Namespace does not match the snapshot")
    for entry, record in entries.items():
        resource_id = record.get("id")
        value = None
        found = entry in explicit
        if found:
            value = explicit[entry]
        if found:
            providers[entry] = normalize(value)
            continue
        if package_input is not None:
            providers[entry] = _package_provider(package_input, entry, resource_id)
        elif source_package is not None:
            providers[entry] = _package_provider(source_package, entry, resource_id)
        elif source_store is not None:
            if not isinstance(resource_id, str) or not resource_id:
                raise ValueError("Authenticated resource source requires each resource record ID")
            providers[entry] = _store_resource_provider(source_store, resource_id)
        else:
            raise ValueError(f"Missing content provider for Exchange resource {resource_id!r}")

    with prepared_exchange_package(snapshot, providers, storage=storage,
                                  require_subgraph=require_subgraph, checkpoint=checkpoint) as package:
        yield package


def _checked_package_view(package):
    """Return a validated lazy view without materializing its JSON documents."""
    from .exchange import ExchangePackageView
    if isinstance(package, ExchangePackageView):
        return package
    for name in ("package_view", "_package_view", "view", "_view"):
        candidate = getattr(package, name, None)
        if isinstance(candidate, ExchangePackageView):
            return candidate
    return None


def _lazy_scalar(value):
    scalar = getattr(value, "scalar", None)
    return scalar() if callable(scalar) else value


def _store_resource_provider(store, resource_id):
    binding = (store._identity, store._actor_id, store.namespace_id,
               getattr(getattr(store, "_client", None), "_database", None))

    def assert_binding():
        current = (store._identity, store._actor_id, store.namespace_id,
                   getattr(getattr(store, "_client", None), "_database", None))
        if current != binding or store.closed:
            raise ValueError("Authenticated source Store identity changed while freezing resources")

    @contextmanager
    def open_resource():
        assert_binding()
        from .values import DResource
        resource = DResource(from_dict={"type": "DResource", "value": {
            "resourceId": resource_id, "namespaceId": binding[2],
        }})
        reader = store._open_resource(resource)
        try:
            with reader:
                yield reader
        finally:
            assert_binding()

    return open_resource


def _package_provider(package, entry, resource_id):
    if callable(getattr(package, "iter_content", None)):
        return lambda: _IteratorReader(package.iter_content(entry))
    reader = getattr(package, "open_resource", None) or getattr(package, "resource_reader", None)
    if not callable(reader):
        raise TypeError("Exchange package must expose checked content readers")
    return lambda: reader(resource_id if resource_id is not None else entry)


class _IteratorReader:
    """Minimal read adapter for a checked Exchange content iterator."""
    def __init__(self, blocks):
        self._blocks = iter(blocks)
        self._buffer = bytearray()
        self._done = False

    def read(self, size=-1):
        while not self._done and (size < 0 or len(self._buffer) < size):
            try:
                block = next(self._blocks)
            except StopIteration:
                self._done = True
                break
            if not isinstance(block, bytes):
                raise TypeError("Exchange content iterator must yield bytes")
            self._buffer.extend(block)
        if size < 0:
            result = bytes(self._buffer)
            self._buffer.clear()
            return result
        result = bytes(self._buffer[:size])
        del self._buffer[:size]
        return result

    def __enter__(self):
        return self

    def __exit__(self, *args):
        close = getattr(self._blocks, "close", None)
        if close:
            close()


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
