# <img src="dep_cube.svg" width="32">Deposit Python

Synchronous Python client for Deposit Server 2. The distribution and import are
named `deposit`; the 2.x generation is distinct from Deposit 1.x and does not
promise API compatibility. Applications that use both generations should use
separate environments.

This repository is part of the [Deposit Hub](https://github.com/demjanp/deposit-hub).

Deposit was created on 29. 4. 2013

<details>
<summary>Table of Contents</summary>

1. [About Deposit](#about)
2. [Installation](#installation)
3. [Resource and import workflows](#resource-and-import-workflows)
4. [NetworkX graph conversion](#networkx-graph-conversion)
5. [Compatibility and migration](#compatibility-and-migration)
6. [Contact](#contact)
7. [Acknowledgements](#acknowledgements)
8. [License](#license)

</details>

## About Deposit <a name="about"></a>
Deposit is open source graph database engine that serves as a storage and management platform for both the collection as well as analysis of scientific data.

Deposit also has a [GUI frontend](https://github.com/demjanp/deposit_gui)

It serves as a backend for the following applications:
* [Laser Aided Profiler](https://www.laseraidedprofiler.com/)
* [CeraMatch](https://github.com/demjanp/CeraMatch)


## Installation <a name="installation"></a>

Deposit 2.x has not been published to PyPI. After publication, install the
client with:

```
pip install "deposit>=2,<3"
```

NetworkX is optional. Install the graph extra only when using graph conversion:

```
pip install "deposit[networkx]>=2,<3"
```

The package version is derived at build time from `deposit-python` Git tags by
`setuptools-scm`. The project release-tag convention is `vX.Y.Z`, optionally
with a PEP 440 prerelease suffix such as `v2.4.0rc1`. Tag a release commit
with that version and run `python -m build` to build its wheel and source
distribution. Post-tag and dirty builds follow `setuptools-scm` standard
development-version schemes; tagless and shallow
checkouts follow its built-in behavior. For reliable release provenance, build
releases from complete history with the release tag available. Source
distributions retain the resolved version so their extracted contents can
rebuild a wheel without Git metadata. Installed packages read
their version from distribution metadata and do not invoke Git.
`src/deposit/_version.py` is generated during builds, ignored by Git and not
hand-maintained; runtime version reporting uses installed distribution
metadata. The Python package version is independent of the Deposit Server version recorded in the
procedure manifest; it is not Exchange `depositVersion` provenance.

Package metadata requires Python 3.14.x and Neo4j Python driver >=6.1,<7. The
`networkx` extra supports NetworkX 3.x, starting at 3.4. Import operations
require the final procedures and semantic capabilities; tested Server builds
are recorded as verification evidence.
The declared release acceptance matrix targets Windows x64, Python 3.14.0 and
3.14.10, Neo4j driver 6.1.0 and 6.4.1, NetworkX absent and versions 3.4 and
3.7. Release acceptance is separate
from PyPI publication.

For the GUI installer see [Deposit GUI releases](https://github.com/demjanp/deposit_gui/releases/latest)

## Resource and import workflows

Use an open Store to upload a local file or a caller-owned binary stream. Upload
freezes the content in protected recovery storage before preparing server writes;
changing the original file afterward cannot change a pending replay.

```python
from deposit import DGeometry, DResource

resource = store.upload_resource(DResource(path="measurements.csv"))
with resource.open(store) as reader:
    downloaded = reader.read()  # verifies chunk and complete-file checksums

shape = DGeometry(coordinates=[14, 50], geometry_type="Point", srid=4326)
shape.resolve(store)  # conversion uses the Server
```

Upload a resource before assigning it to a resource Descriptor. Assign geometry
through an Object or batch to persist it; editing a detached value alone does
not update the Server. `is_image` remains `None` when verified image metadata is
unavailable. Close download readers after consuming all bytes; early close reports
an incomplete download. The wrapper closes files it opens and leaves caller
streams owned by the caller.

`store.import_rows(...)`, `store.from_dict(...)`, `store.import_subtree(...)`
and `store.from_networkx(...)` return lazy import analysis results. Apply
explicitly with `store.import_apply(result["planId"])`. Inputs are frozen before
analysis; a target revision conflict requires fresh analysis. Dictionary,
subtree and NetworkX imports accept `resources` and `resource_source` providers.
`resources` maps Exchange content entries to paths, bytes or
caller-owned binary streams; streams are read from their current position and
remain open. Explicit content takes precedence over `resource_source`, and bad
content fails without fallback. A `resource_source` can be an authenticated
source Store or checked Exchange bundle. Package bytes are staged and integrity-checked in protected
storage before upload or analysis. A complete checked Exchange bundle can be
passed directly as `data` without separate providers. To create a new
Namespace from a complete Exchange package, call
`client.clone_namespace(token, package)`; the resulting `CloneResult` exposes
the created Namespace identity and lazy identity/resource mappings. The package
Namespace name is preserved, and a name collision fails with `DUPLICATE`.
Typed references and graph/domain semantics remain Server-validated.
Clone transfers persist protected recovery workflows. Discover them with
`client.pending_clones()`, inspect local phase and verified offset with
`client.inspect_clone(workflow)`, and continue with
`client.resume_clone(workflow, username=..., password=...)`. Resume verifies the
original server, database and actor before using protected staging. If a process
stops before the package is frozen, inspection reports `SOURCE_REQUIRED`; pass
the replacement source to `resume_clone(..., source=...)`. After freezing,
resume uses the immutable staged package and cannot replace its source.

Portable Exchange workflows use `store.export_exchange(...)` to return a
context-managed `ExchangeBundle`. The bundle can be inspected lazily or saved
as a portable ZIP with `bundle.save(path)`. Import a checked bundle or portable
ZIP path/stream with `store.import_exchange(source, rules=...)`; use
`target_store.import_from(source_store, ...)` for a Store-to-Store transfer.
Both methods return an `ImportResult` plan and never apply it automatically.
Direct handoff is used only when the current Server bindings and package grant
allow it; otherwise the client relays the same frozen package through the two
authenticated Stores.

Import workflows persist their frozen inputs and recovery state in protected
storage. `client.pending_imports()` lists pending and retained terminal workflow
references without credentials. Inspect locally with `client.inspect_import(workflow)`;
resume with authenticated `target_store` and, only when source work remains,
`source_store`. Resume stops at `PLAN_READY` unless explicit apply intent was
already recorded durably; then it recovers that same apply. To start the apply,
call `target_store.import_apply(plan_id)`. Call
`client.abandon_import(workflow, target_store=target_store)` when you want to
release a workflow; unresolved Server ownership remains pending.

CSV/XLSX/GeoPackage load/save, table paging, history, undo/redo and confirmed
restore use `store.call(...)` with the exact public Server arguments. Returned
browser download URLs use independent browser authentication; never append a
Store token. See the [shared Python contract](https://github.com/demjanp/deposit-hub/blob/main/deposit-python.md)
for method signatures. The project acceptance map is maintained in the adjacent
`test_harness/deposit-python/config/acceptance_map.json` checkout.

## NetworkX graph conversion <a name="networkx-graph-conversion"></a>

Install `deposit[networkx]` to use `Store.to_networkx(...)` or
`Store.from_networkx(...)`. The base package remains importable without
NetworkX; calling either graph method without the extra raises an
`ImportError` with the installation command.

`to_networkx` returns a directed multigraph. Node keys are
`(namespace_id, kind, id)` tuples, where `kind` is `Class` or `Object`. Edge
keys retain relationship IDs, so parallel relationships and their direction
remain distinct. Node and edge attributes retain current values, declarations,
Descriptor records and membership data. Each Object's `descriptors` attribute
maps names to visible Descriptor records containing their current ID, name,
value and provenance. A record is recognized only when it has the complete
`Descriptor` shape (`kind`, `id`, `namespaceId`, `objectId`, `name`, and
`value`) and its namespace, Object and name match the graph location; an
ordinary scientific mapping with `kind="Descriptor"` remains a value. Graph metadata includes the source Namespace, revision,
Server version, Patterns, resource records, geometries and User Tools; it does
not contain a hidden Exchange snapshot or history.

```python
graph = store.to_networkx(roots=[root_object_id], max_hops=2)
plan = store.from_networkx(graph)
result = store.import_apply(plan["planId"])
```

Omit `roots` to export the Namespace. When roots are supplied, `max_hops` may
be from 0 through 10 and `root_class_boundary` defaults to `True`. Import
serializes the graph's current nodes and edges, including additions, deletions
and edits, before analysis, so later caller edits do not change the submitted
input. Deposit Server performs matching and assigns receiver-side
identities; source IDs are not a promise that imported entities keep those IDs.
Apply the returned plan explicitly with `store.import_apply(...)`.

Graph conversion represents current data, not history or a complete backup.
Resource bytes never appear in graph attributes; provide them with
`store.from_networkx(graph, resources=...)` or `resource_source=...`. Python
checks package shape, ZIP safety, lengths and checksums, while the Server checks
resource references and graph semantics before creating an analysis plan.
`Client.recover` returns a clone result as `RecoveredOperation.result` without
selecting the newly created Namespace.

## Compatibility and migration <a name="compatibility-and-migration"></a>

Deposit Python 2.x is a separate client generation from Deposit 1.x and does not
promise API compatibility. There is no automatic 1.x-to-2.x migration; use
separate environments when both generations are needed and migrate application
code against the Deposit 2 client and Server contract.

Operations return explicit analysis or action results. Import methods return a
plan that must be applied; callers should retain its `planId` and returned
mapping when they need to associate source records with receiver-side IDs.
Store-scoped pending operations can be recovered through the Store recovery
methods, subject to the configured protected recovery directory. Client-owned
composed Exchange workflows use `pending_imports`, `inspect_import`,
`resume_import`, and `abandon_import`. Full-Namespace clones use the separate
`pending_clones`, `inspect_clone`, and `resume_clone` Client workflow methods.
Close Stores, resources and download readers when finished; incomplete resource
downloads are reported when a reader closes before all bytes are consumed.

## Contact: <a name="contact"></a>
Peter Demján (peter.demjan@gmail.com)

Institute of Archaeology of the Czech Academy of Sciences, Prague, v.v.i.

## Acknowledgements <a name="acknowledgements"></a>

Development of this software was supported by OP RDE, MEYS, under the project "Ultra-trace isotope research in social and environmental studies using accelerator mass spectrometry", Reg. No. CZ.02.1.01/0.0/0.0/16_019/0000728.

This software uses the following open source packages:
* [Neo4j](https://github.com/neo4j/neo4j-python-driver)

## License <a name="license"></a>

This code is licensed under the [GNU GENERAL PUBLIC LICENSE](https://www.gnu.org/licenses/gpl-3.0.en.html) - see the [LICENSE](LICENSE) file for details
