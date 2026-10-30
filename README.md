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

Package metadata requires Python 3.14.x and Neo4j Python driver >=6.1,<7. The
`networkx` extra supports NetworkX 3.x, starting at 3.4. This client also checks
the Deposit Server version against the exact build recorded in its installed
procedure manifest; unsupported Server builds fail before Deposit API calls.
The current manifest target is
`2.0.0-dev.77-post.15.g7610e45-SNAPSHOT`.
The declared release acceptance matrix targets Windows x64, Python 3.14.0 and
3.14.10, Neo4j driver 6.1.0 and 6.4.1, NetworkX absent and versions 3.4 and
3.7, with the manifest's exact Server build. Release acceptance is separate
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

`store.import_rows(...)`, `store.from_dict(...)`, `store.import_subtree(...)` and
`store.import_data_by_pattern(...)` return analysis plans. Apply explicitly with
`store.import_apply(plan["planId"])`. Inputs are frozen before analysis; a target
revision conflict requires fresh analysis. Dictionary/subtree helpers currently
accept resource-free Exchange snapshots. Resource-bearing dictionary imports,
full-Namespace clone recovery and Client-owned server-scope mutation recovery
remain deferred.

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
remain distinct. Node and edge attributes retain the exported values,
declarations and membership data. Graph metadata includes the source Namespace,
revision, Patterns and the detached Exchange snapshot used for conversion.

```python
graph = store.to_networkx(roots=[root_object_id], max_hops=2)
plan = store.from_networkx(graph)
result = store.import_apply(plan["planId"])
```

Omit `roots` to export the Namespace. When roots are supplied, `max_hops` may
be from 0 through 10 and `root_class_boundary` defaults to `True`. Import
freezes the graph before analysis, so later caller edits do not change the
submitted input. Deposit Server performs matching and assigns receiver-side
identities; source IDs are not a promise that imported entities keep those IDs.
Apply the returned plan explicitly with `store.import_apply(...)`.

Graph conversion is not a complete backup or migration format. Resource content
cannot be created from a graph, and resource-bearing imports are rejected. Use
the Exchange resource workflow for content-bearing snapshots. The API also does
not provide full-Namespace clone recovery or recover Client-owned Server-scope
mutations; those workflows remain deferred.

## Compatibility and migration <a name="compatibility-and-migration"></a>

Deposit Python 2.x is a separate client generation from Deposit 1.x and does not
promise API compatibility. There is no automatic 1.x-to-2.x migration; use
separate environments when both generations are needed and migrate application
code against the Deposit 2 client and Server contract.

Operations return explicit analysis or action results. Import methods return a
plan that must be applied; callers should retain its `planId` and returned
mapping when they need to associate source records with receiver-side IDs.
Store-scoped pending operations can be recovered through the Store recovery
methods, subject to the configured protected recovery directory. Recovery does
not cover full-Namespace clone workflows or Client-owned Server-scope mutations.
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
