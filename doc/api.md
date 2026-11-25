# Deposit Python API reference

Use the [quickstart](quickstart.md) to install the wrapper and open a Store. This reference describes the Python interface for Deposit Server 2.0.1.

## Connections and sessions

- Construct `Client(driver, database=...)` with a synchronous Neo4j driver. The optional `database` selects the Neo4j database. Use `client.discover()` to read the Deposit Server version and capabilities.
- Call `client.open_store(namespace, username=..., password=...)` with a Namespace name or an explicit `{"id": namespace_uuid}` selector. Each Store has its own Deposit session and stays bound to its selected Namespace.
- Close readers before closing the Store and Client. Use context managers where possible. The caller closes a supplied driver by default. With `Client(driver, owns_driver=True)`, closing the Client also closes the driver.

## Records and queries

| Task | Python interface |
| --- | --- |
| Read one record as a dictionary | `store.get_class(selection)`, `store.get_object(selection)` |
| Read a record as a handle | `store.get_class_handle(selection)`, `store.get_object_handle(selection)` |
| Browse records by page | `store.classes()`, `store.objects(class_name=...)`, `store.members(dclass)` |
| Browse labeled relationships | `store.relationships()` for Objects, `store.class_relationships()` for Classes |
| Iterate over query rows | `store.iter_query(query, parameters=..., page_size=200)` |
| Control query paging | `store.get_query_page(query, parameters=..., page_size=..., cursor=...)` |

Use a Class name or an explicit ID selector for Classes and an Object ID for Objects. Handles carry a Namespace and a revision baseline. They support edits through methods such as `store.set_descriptor(dobject, name, value)`. When a revision conflict occurs, call `handle.refresh()` to read the current record and review the intended change before reapplying it.

Relationship dictionaries contain the relationship ID, label, and directed endpoint IDs in `id`, `label`, `sourceId`, and `targetId`. Pass source and target handles or a label to the paged methods to filter the results.

Each query row contains `values` and `presence` dictionaries keyed by output column. Read `presence` to tell whether a value was present, including an explicit null. Query page sizes run from 1 to 1,000. For manual paging, pass the returned `nextCursor` with the same query and parameters until it is `None`. Close paged readers when finished. If the Namespace changes and invalidates a cursor, start a new query to read the new revision. [Deposit Query Language](https://github.com/demjanp/deposit-server/blob/2.0.1/doc/query-language.md)

### Atomic batches

Use the Batch returned by `store.transaction()` to collect edits. The wrapper submits the batch as one atomic server operation when the context exits normally.

```python
with store.transaction() as batch:
    collection = batch.create_class("Sample collection")
    sample = batch.create_object(collection)

created_sample = batch.result(sample)
```

A batch supports up to 1,000 commands and 8 MiB of encoded request data. Use Batch methods or `batch.call()` for batch-eligible procedures. Store calls and nested batches are unavailable during collection. A batch reference can point to an earlier command in the same batch. Read its result after the batch completes. [Server batch procedure](https://github.com/demjanp/deposit-server/blob/2.0.1/doc/procedures.md#depositapplybatch)

## Calling server procedures

`Store.call(procedure, *arguments)` accepts procedures assigned to the Store. Use the domain arguments in the documented order. The wrapper supplies the Deposit token and handles operation registration for tracked procedures.

```python
with store.call("deposit.get.class", "Specimen") as result:
    record = result.to_data(next(result)["node"])
```

For a native Neo4j node or relationship, call `result.to_data(value)` while its result is open to obtain a portable dictionary. Tracked calls can return portable rows directly. Import analysis and apply return an `ImportResult`. Check each procedure’s arguments and result columns in the [server procedure reference](https://github.com/demjanp/deposit-server/blob/2.0.1/doc/procedures.md).

## Tracked operations

By default, the wrapper generates and registers an ID for every tracked operation. If you need the ID before submitting the call, generate a fresh UUIDv7 with `Client.new_operation_id()` and pass it as `operation_id=`. Supported entry points include:

- `Store.call()` for procedures marked as always tracked. Convenience setters such as `store.create_class_descriptor()` generate their own IDs. Use the corresponding tracked `Store.call()` route to pass an explicit ID.
- Store exchange and import methods `export_exchange()`, `import_exchange()`, `import_from()`, `import_rows()`, `import_subtree()`, `from_dict()`, `from_networkx()`, `import_data_by_pattern()`, and `import_apply()`.
- Client methods `clone_namespace()` and `prune_resources()`. Pruning accepts an explicit ID when the options set `dryRun` to `False`.

An explicit import analysis ID identifies the analysis. Intermediate transfers and plan application are distinct operations with their own IDs.

Use `store.get_operation(operation_id)` to look up the attempt and `store.cancel_operation(operation_id)` to request cancellation. The Client equivalents take a Deposit token and the original scope, which is the Namespace UUID or `"server"`. After a lost response, look up the same ID in the same scope. A replacement Store must use the same Deposit actor and Namespace. A state of `COMMITTED` confirms that the server committed the operation even if its original response is unavailable. Treat an unconfirmed outcome as unknown and avoid replaying the mutation blindly.

`DepositCancelled` indicates confirmed cancellation and rollback. `OperationCommittedAfterCancel` indicates that the operation committed before cancellation took effect. `OutcomeUnknown` supplies the operation ID and scope to inspect. Server errors expose a `DepositError.code` and optional details. Driver and transport failures use `TransportError`. [Worked operation-ID example](quickstart.md#operation-ids)

## Files and exchange

- Upload a local file with `store.upload_resource(DResource(path=...))` and attach the returned resource to a resource Descriptor. Read the verified stream with `resource.open(store)` in a context manager.
- Use `store.export_exchange()` in a context manager to obtain a bundle. Save it to a ZIP with `bundle.save(path)`. Supply `roots=` to export a selected set of Objects and use `max_hops=` to control traversal. A whole-Namespace export that includes retained history requires owner access.
- Analyze an exchange package in the receiving Store with `store.import_exchange(source)`. The source can be a ZIP path, ZIP bytes, a binary stream, or an authorized bundle. Use the analysis result as a context manager, and record its `plan_id` if a plan is available. Use `store.import_inspect(plan_id)` to reopen its analysis, `store.import_apply(plan_id)` to apply a ready plan, or `store.discard_import_plan(plan_id)` to discard an unused plan. Close each result reader after reading it.

### NetworkX

`store.to_networkx()` returns a `networkx.MultiDiGraph`. Start with an exported graph when preparing changes for import:

- Keep `namespaceId` and the nonnegative `revision` in graph metadata. Preserve relevant Patterns, resources, geometries, user tools, and scope metadata.
- Keep Class and Object node keys in the form `(namespace_id, kind, id)`, with matching `kind` and `id` attributes. Object Descriptors are stored on their owning Object nodes.
- Keep edge keys equal to the edge IDs. A Membership runs from Object to Class, a Relationship from Object to Object, and a ClassRelationship from Class to Class. Preserve consistent endpoints and finite JSON-portable attributes.

Analyze the edited graph with `store.from_networkx(graph)` in the receiving Store and review the result before applying its plan. For referenced resources, pass content providers keyed by package member name as `resources=`, or pass a Store or exchange bundle from the original Namespace as `resource_source=`. [Quickstart exchange workflow](quickstart.md#import-and-export)
