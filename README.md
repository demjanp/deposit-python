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
4. [Contact](#contact)
5. [Acknowledgements](#acknowledgements)
6. [License](#license)

</details>

## About Deposit <a name="about"></a>
Deposit is open source graph database engine that serves as a storage and management platform for both the collection as well as analysis of scientific data.

Deposit also has a [GUI frontend](https://github.com/demjanp/deposit_gui)

It serves as a backend for the following applications:
* [Laser Aided Profiler](https://www.laseraidedprofiler.com/)
* [CeraMatch](https://github.com/demjanp/CeraMatch)


## Installation <a name="installation"></a>

Planned release installation commands (available after Stage 6 and publication):
```
pip install "deposit>=2,<3"
```

NetworkX graph conversion is planned for Stage 6; its optional extra will be:

```
pip install "deposit[networkx]>=2,<3"
```

The initial integration support matrix targets Windows x64, Python 3.14.x,
Neo4j Python driver 6.x, and the exact Deposit Server build recorded in the
installed package's procedure manifest. Unsupported server builds fail before
Deposit API calls.

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
for method signatures and the [acceptance map](tests/acceptance_map.md) for
verification status.

## Contact: <a name="contact"></a>
Peter Demján (peter.demjan@gmail.com)

Institute of Archaeology of the Czech Academy of Sciences, Prague, v.v.i.

## Acknowledgements <a name="acknowledgements"></a>

Development of this software was supported by OP RDE, MEYS, under the project "Ultra-trace isotope research in social and environmental studies using accelerator mass spectrometry", Reg. No. CZ.02.1.01/0.0/0.0/16_019/0000728.

This software uses the following open source packages:
* [Neo4j](https://github.com/neo4j/neo4j-python-driver)

## License <a name="license"></a>

This code is licensed under the [GNU GENERAL PUBLIC LICENSE](https://www.gnu.org/licenses/gpl-3.0.en.html) - see the [LICENSE](LICENSE) file for details
