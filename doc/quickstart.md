# deposit-python — Quickstart

Install the `deposit` Python wrapper, then query and explore relationships in the server’s sample data.

## Install

The wrapper targets **Python 3.14 on Windows x64** with **Deposit Server 2.0.1**. Install the `deposit-python` distribution and import it as `deposit`. Use PowerShell and the Windows Python launcher. From the root of a full Git checkout, create a virtual environment and install the wrapper:

```powershell
py -3.14 -m venv .venv
.\.venv\Scripts\python.exe -m pip install .
```

To install from a local wheel, create the same virtual environment and pass the path to your wheel:

```powershell
.\.venv\Scripts\python.exe -m pip install "C:\path\to\deposit_python-2.0.1-py3-none-any.whl"
```

Once a 2.x wrapper release is published on PyPI, the corresponding command is:

```powershell
.\.venv\Scripts\python.exe -m pip install "deposit-python>=2,<3"
```

Pip installs the Neo4j Python driver (`>=6.1,<7`) and NetworkX (`>=3.4,<4`) for all three installation paths. If you also use the standalone `deposit` distribution, keep it in a separate Python environment because both distributions provide the `deposit` import.

## Query records and follow relationships

If you need a server, use the [Deposit Server 2.0.1 checkout instructions](https://github.com/demjanp/deposit-server/blob/2.0.1/doc/installation.md#run-from-a-git-checkout). Follow the server’s [quickstart through step 4](https://github.com/demjanp/deposit-server/blob/2.0.1/doc/quickstart.md#4-load-the-sample-graph) to set up a Deposit account, create the `Deposit sample` Namespace, and load the [sample graph](https://github.com/demjanp/deposit-server/blob/2.0.1/doc/sample-data/seed.cypher).

Save this example as `read_sample.py`. Replace the Bolt address and credentials with those for your server and use the Deposit account that can access the sample Namespace.

```python
from getpass import getpass

from neo4j import GraphDatabase

from deposit import Client

with GraphDatabase.driver(
    "bolt://127.0.0.1:7687", auth=("neo4j", getpass("Neo4j password: "))
) as driver, Client(driver) as client:
    with client.open_store(
        "Deposit sample",
        username="deposit-admin",
        password=getpass("Deposit password: "),
    ) as store:
        print("Specimens from a Deposit query")
        with store.iter_query("SELECT Specimen.Code AS code ORDER BY code") as rows:
            for row in rows:
                print(row["values"]["code"])

        print("\nSpecimen-to-Context relationships")
        with store.relationships(label="foundIn") as rows:
            relationships = list(rows)

        for relationship in relationships:
            source = store.get_object_handle({"id": relationship["sourceId"]})
            target = store.get_object_handle({"id": relationship["targetId"]})
            print(f"{source['Code']} -{relationship['label']}-> {target['Code']}")
```

Run it with the virtual environment’s interpreter:

```powershell
.\.venv\Scripts\python.exe read_sample.py
```

The Deposit query prints `S-01`, `S-02`, and `S-03`. The relationship lookup shows that `S-01` and `S-02` each point to `C-01` through `foundIn`. Each relationship dictionary gives the direction through its `sourceId` and `targetId`, which the script resolves to Object handles to display their `Code` values. [Relationship API](api.md#records-and-queries)

The Neo4j credentials authenticate the Bolt connection. The Deposit credentials open the Namespace session. The context managers close each iterator after use and close the Store, Client, and driver when the script finishes. To list available Namespaces before opening a Store, use `client.login()`, `client.get_namespaces(token)`, and `client.logout(token)`. [Token-based Client calls](api.md#token-based-client-calls)

## Read and edit data

Run the remaining examples inside an open Store. To list Classes, use the paged iterator:

```python
with store.classes() as classes:
    for dclass in classes:
        print(dclass.name)
```

Use `store.get_class("Specimen")` for a dictionary or `store.get_class_handle("Specimen")` for a `DClass` handle. Store and entity methods support ordinary edits. Use `store.transaction()` to collect related edits into one atomic batch. If a revision conflict occurs, refresh the affected handles and review your changes before resubmitting them. [Records, queries, and batches](api.md#records-and-queries)

Use `store.descriptors(dobject)` to iterate over an Object's Descriptor names, values, and declared types. With editor or owner access, assign a value with `dobject[name] = value`. For a new Descriptor whose type needs to be explicit, pass a value such as `typed("Float", 2)` after importing `typed` from `deposit`. [Descriptor values](api.md#descriptor-values) · [Entity handles and revisions](api.md#entity-handles-and-revisions)

## Operation IDs

With editor or owner access to the sample Namespace, use the ordinary
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
Only supported tracked calls accept an explicit operation ID. To list or cancel active requests from another thread, use the Store's request controls. [Tracked operations](api.md#tracked-operations) · [Live request control](api.md#live-request-control)

## Transfer files

```python
from deposit import DResource

resource = store.upload_resource(DResource(path="measurements.csv"))
with resource.open(store) as reader:
    content = reader.read()
```

Uploads and downloads verify content integrity. Assign the uploaded resource to a resource Descriptor to attach it to a record. You can also upload from a binary stream by supplying `stream=` and `filename=`. [Files and exchange](api.md#files-and-exchange)

## Import and export

Export a Namespace to a portable ZIP package:

```python
with store.export_exchange() as bundle:
    bundle.save("dataset.zip")
```

Import an existing package into a receiving Store. Analysis creates a plan and applying it writes to the receiving Namespace. Review the analysis before choosing to apply it:

```python
with store.import_exchange("source.zip") as analysis:
    plan_id = analysis.plan_id
    print(analysis.state, plan_id)
```

After reviewing the analysis and deciding to proceed, apply the plan:

```python
if plan_id:
    with store.import_apply(plan_id) as result:
        print(result.state)
```

An explicit import analysis ID identifies only analysis. Applying its plan is a separate tracked call with its own ID. Intermediate transfers also have separate IDs.

The wrapper also supports imports from rows, graph snapshots, Pattern form data, and NetworkX graphs. Use `store.import_inspect(plan_id)` in a context manager to review an import plan or `store.discard_import_plan(plan_id)` to discard it. A whole-Namespace export includes retained history when history mode is persistent and requires Namespace owner access in that case. Use `client.clone_namespace(token, source)` to create a separate Namespace from a complete package. [Import plans](api.md#import-plans) · [Pattern matching and form import](api.md#patterns-and-user-tools) · [Namespace cloning](api.md#files-and-exchange)

## Use NetworkX

```python
graph = store.to_networkx()  # directed multigraph
print(graph.number_of_nodes())
```

Start with a graph returned by `store.to_networkx()` and preserve its Deposit metadata and entity identities when editing. Use `store.from_networkx(graph)` in a context manager to analyze an import, then apply the plan if you want to write the changes. The server determines how imported records match existing records. [NetworkX graph format](api.md#networkx)

For additional Store procedures, including history and tabular exchange, see [calling server procedures](api.md#calling-server-procedures) and the [Deposit Server 2.0.1 procedure reference](https://github.com/demjanp/deposit-server/blob/2.0.1/doc/procedures.md).
