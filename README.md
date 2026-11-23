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

Install a locally built alpha wheel with `python -m pip install <wheel-path>`.
After publication to PyPI, choose a 2.x release with:

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

Use an open Store to upload a local file or a caller-owned binary stream. The
Server verifies the complete size and checksum before publishing it. A borrowed
stream is read from its current position into a protected temporary buffer
that is removed when the attempt ends.

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
and `store.from_networkx(...)` return lazy analysis results. Call
`store.import_apply(result.plan_id)` to commit the plan, or
`store.discard_import_plan(result.plan_id)` to cancel an unused plan. Use
`store.import_inspect(plan_id)` to inspect it from the authenticated Store.
Apply uses the original live Store connection. A stale, expired or disconnected
plan requires fresh analysis.

Dictionary, subtree and NetworkX imports accept `resources` and
`resource_source` providers. Map `resources/<sha256>` members to paths, bytes,
borrowed streams or stream factories. Explicit providers take precedence.
A `resource_source` can be an authenticated Store or an Exchange bundle.
Deposit Server validates the graph, package and resource content before analysis.

`store.export_exchange(...)` returns a context-managed `ExchangeBundle`. Read
its manifest and graph lazily or use `bundle.save(path)` to publish a verified ZIP
atomically. Use `store.import_exchange(source)` with a bundle, portable ZIP or
binary stream. Keep a caller-owned bundle open through direct apply and result
reads. Use `target_store.import_from(source_store)` to manage export and intake
within the workflow. When both authenticated Stores can use the same live
source grant, the Server handles the transfer directly. Other transfers relay
the original ZIP in bounded chunks and obtain a verified receiving grant.

```python
with store.import_exchange("source.zip") as analysis:
    with store.import_apply(analysis.plan_id) as result:
        print(result.state)
```

Use `client.clone_namespace(token, package)` to create a Namespace from a
complete package. The `CloneResult` exposes its identity and lazy mappings.
Clone preserves the source name, retained history and resources. A name
collision returns `DUPLICATE`.

CSV/XLSX/GeoPackage load/save, table paging, history, undo/redo and confirmed
restore use `store.call(...)` with the exact public Server arguments in the
[Server procedure contract](https://github.com/demjanp/deposit-server/blob/main/runtime/public-api.json).
Returned browser download URLs use independent browser authentication.

Within `store.transaction()`, commands run in the order they are collected.
Use `TypedValue` or a `DType` when a descriptor write needs an explicit type,
including writes that create a declaration. The Server checks the declaration
in effect at that command and rolls back the batch if any command fails.

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

Graph conversion carries the current graph and resource metadata. Supply
resource content through `resources` or `resource_source` when importing.
The Server checks graph semantics and verifies bytes before analysis.

## Compatibility and migration <a name="compatibility-and-migration"></a>

Deposit Python 2.x is a separate client generation from Deposit 1.x and does not
promise API compatibility. There is no automatic 1.x-to-2.x migration; use
separate environments when both generations are needed and migrate application
code against the Deposit 2 client and Server contract.

Deposit 2 alpha removes restart journals, transfer continuation, saved local
import results and Python Exchange ZIP parsing. Pass ZIPs directly to
`store.import_exchange(...)`, use `ExchangeBundle` to inspect or save an
export, and consume result mappings from the Server while their lease is valid.
Keep a public operation ID if your application needs to inspect its outcome
after a process restart.

For cancellable workflows, pass a fresh `operation_id=Client.new_operation_id()`
to identify the final Server action. Use `get_operation(operation_id)` and
`cancel_operation(operation_id)` on the Store, or the token-taking Client
methods for server-scoped actions. `DepositCancelled` indicates verified
rollback. `OperationCommittedAfterCancel` carries the completed outcome and
`OutcomeUnknown` identifies the original attempt to inspect. After verified
rollback, a deliberate replacement starts from the original source with fresh
operation and staging IDs. Close results, bundles, Stores and Clients when
finished.

## Contact: <a name="contact"></a>
Peter Demján (peter.demjan@gmail.com)

Institute of Archaeology of the Czech Academy of Sciences, Prague, v.v.i.

## Acknowledgements <a name="acknowledgements"></a>

Development of this software was supported by OP RDE, MEYS, under the project "Ultra-trace isotope research in social and environmental studies using accelerator mass spectrometry", Reg. No. CZ.02.1.01/0.0/0.0/16_019/0000728.

This software uses the following open source packages:
* [Neo4j](https://github.com/neo4j/neo4j-python-driver)

## License <a name="license"></a>

This code is licensed under the [GNU GENERAL PUBLIC LICENSE](https://www.gnu.org/licenses/gpl-3.0.en.html) - see the [LICENSE](LICENSE) file for details
