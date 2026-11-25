# deposit-python — Quickstart

Practical installation and usage of the `deposit` Python wrapper.

## Install

deposit-python currently targets **Python 3.14 on Windows x64**. You also need a running [deposit-server](https://github.com/demjanp/deposit-server), a Deposit user account, and access to a Namespace (collection).

Install from PyPI:

```powershell
python -m pip install "deposit>=2,<3"
```

Alternatively, install a locally built wheel with `python -m pip install <wheel-path>`.

## Connect to Deposit Server

```python
from neo4j import GraphDatabase
from deposit import Client

driver = GraphDatabase.driver(
    "bolt://127.0.0.1:7687", auth=("neo4j", "<Neo4j password>")
)
client = Client(driver)
store = client.open_store(
    "Example Namespace", username="your-user", password="<Deposit password>"
)
```

The example connects to a local Server. Replace the address, credentials, and Namespace name with your own. Neo4j authentication and Deposit user authentication are separate. See the [Server user guide](https://github.com/demjanp/deposit-server/blob/main/doc/README.md) for Server setup.

## Read and query data

Use `Store` methods to work with Classes, Objects, Descriptors, and relationships. For example, list the available Classes:

```python
for dclass in store.classes():
    print(dclass.name)
```

Deposit queries can return results in pages. Replace `Specimen` and `Code` with names from your dataset:

```python
with store.iter_query("SELECT Specimen.Code AS code") as rows:
    for row in rows:
        print(row)
```

Common changes are available through Store and entity methods. Use `store.transaction()` to group related edits into one atomic operation. If another user has changed the data, refresh the affected records before deciding whether to reapply your changes.

## Operation IDs

With editor access and an existing `Specimen` class, use the ordinary
convenience method:

```python
specimen = store.get_class_handle("Specimen")
store.create_class_descriptor(specimen, "Color", "String")
```

For other allowlisted Store procedures, pass the domain arguments in order.
For example, a domain-only call to create a different descriptor returns an
iterable `StoreResult` whose row contains the portable node:

```python
with store.call("deposit.create.classDescriptor", "Specimen", "Finish", "String", {}) as result:
    print(next(result)["node"]["id"])
```

The wrapper generates and registers an ID for each tracked call, including
intermediate package and resource transfer steps. Supply a fresh UUIDv7 when
you need a known identity before submission:

```python
operation_id = client.new_operation_id()
with store.call("deposit.create.classDescriptor", "Specimen", "Texture", "String", {},
                operation_id=operation_id) as result:
    print(next(result)["node"]["id"])
```

To inspect that attempt, including after a lost response, use its exact ID:

```python
status = store.get_operation(operation_id)
print(status["state"])
```

Lookup uses an authorized control connection in the Store's Namespace. If the
original Store is unavailable, use another Store authenticated as the same
actor in the same Namespace. A known-ID lookup can confirm commit even when
the original result is unavailable. Access and retention still apply. If you
cannot establish the outcome, treat it as unknown and avoid blind replay.

Use `store.cancel_operation(operation_id)` to request cancellation and check
the returned state for confirmed rollback or commit. The Client equivalents
take a token and the original `scope` (Namespace UUID or `"server"`).
Only tracked calls accept an explicit operation ID. Dry-run pruning rejects it.

## Transfer files

```python
from deposit import DResource

resource = store.upload_resource(DResource(path="measurements.csv"))
with resource.open(store) as reader:
    content = reader.read()
```

Uploads and downloads verify content integrity. Uploading a file makes it available as a resource; assign it to a resource Descriptor to attach it to a record.

## Import and export

Export a Namespace to a portable ZIP package:

```python
with store.export_exchange() as bundle:
    bundle.save("dataset.zip")
```

Import an existing package into a receiving Store. Analysis creates a plan; **the data is not changed until that plan is applied**:

```python
with store.import_exchange("source.zip") as analysis:
    if analysis.plan_id:
        with store.import_apply(analysis.plan_id) as result:
            print(result.state)
```

An explicit import analysis ID identifies only analysis. Applying its plan is a separate tracked call with its own ID. Intermediate transfers also have separate IDs.

The wrapper also supports imports from rows, dictionaries, subtrees, and NetworkX graphs. Use `store.import_inspect(plan_id)` to review an import plan or `store.discard_import_plan(plan_id)` to discard it. A full export that includes retained history may require Namespace owner permissions.

## Use NetworkX

```python
graph = store.to_networkx()  # directed multigraph
print(graph.number_of_nodes())
```

After editing a compatible NetworkX graph, use `store.from_networkx(graph)` to analyze an import and `store.import_apply(plan_id)` to apply it. The Server determines how imported records match existing records.

## Finish

Close connections and readers when finished:

```python
store.close()
client.close()
driver.close()
```

For less common operations, including history functions and CSV/XLSX/GeoPackage import/export, use `store.call(...)` with the [Server procedure reference](https://github.com/demjanp/deposit-server/blob/main/runtime/public-api.json).
