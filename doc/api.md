# Deposit Python API reference

Use the [quickstart](quickstart.md) to install the wrapper and open a Store. This reference describes the Python interface for Deposit Server 2.0.1.

- [Connections and sessions](#connections-and-sessions)
- [Records and queries](#records-and-queries)
- [Descriptor values](#descriptor-values)
- [Patterns and user tools](#patterns-and-user-tools)
- [Calling server procedures](#calling-server-procedures)
- [Tracked operations and request control](#tracked-operations)
- [Files and exchange](#files-and-exchange)
- [Import plans](#import-plans)
- [NetworkX](#networkx)

## Connections and sessions

- Construct `Client(driver, database=...)` with a synchronous Neo4j driver. The optional `database` selects the Neo4j database. Use `client.discover()` to read the Deposit Server version and capabilities.
- Call `client.open_store(namespace, username=..., password=...)` with a Namespace name or an explicit `{"id": namespace_uuid}` selector. Each Store has its own Deposit session and stays bound to its selected Namespace.
- Close readers before closing the Store and Client. Use context managers where possible. The caller closes a supplied driver by default. With `Client(driver, owns_driver=True)`, closing the Client also closes the driver.

### Token-based Client calls

`client.login(username, password)` returns a Deposit session token for the Client methods below. A token created this way has its own session state. `open_store()` authenticates separately. Use `client.logout(token)` to revoke the token, or close the Client to close its Stores, result readers, and remaining sessions. On a fresh server, `client.bootstrap(username, password)` creates the initial administrator and returns the profile.

| Task | Python interface |
| --- | --- |
| Inspect a session profile | `client.get_session(token)` |
| Read or select the active Namespace | `client.get_active_namespace(token)` returns a dictionary or `None` if no Namespace is selected. Use `client.set_active_namespace(token, namespace)` with a name or `{"id": uuid}` to select one. |
| Read one visible user or Namespace | `client.get_user(token, selection)`, `client.get_namespace(token, selection)`. Pass an explicit selector such as a username, Namespace name, or `{"id": uuid}`. |
| List visible users or Namespaces | `client.get_users(token, selection=None)`, `client.get_namespaces(token, selection=None)` return tuples of portable dictionaries. |
| Read a page of users or Namespaces | `client.get_user_page(token, selection=None, page=None)`, `client.get_namespace_page(token, selection=None, page=None)`. Pass a map with `pageSize` and optionally `cursor`, using the returned `nextCursor` to continue. |
| Read or set administrative mode for a session | `client.get_administrative_mode(token)`, `client.set_administrative_mode(token, enabled)`. Use the Store methods of the same names for its own session, omitting the token. |
| Preview or run resource pruning | `client.prune_resources(token, options, operation_id=None)`. The default is a dry run. Use `{"dryRun": False}` to run a tracked prune in the active Namespace or add `"scope": "all"` for a server-wide prune. Use `client.prune_resources_status(token, prune_id)` for a prune status lookup. |
| Convert geometry | `client.coordinates_to_wkt(token, geometry_type, coordinates, options=None)`, `client.wkt_to_coordinates(token, wkt, options=None)` return server conversion dictionaries. The options accept `srid` and `verticalSrid`. |
| Check server capabilities | `client.require_paged_import_results()`, `client.require_clone_capabilities()` return discovery data or raise `CompatibilityError` when a required capability is missing. |

The server enforces permissions for each call. User listing and resource pruning require server-administrator access. [Server procedure reference](https://github.com/demjanp/deposit-server/blob/2.0.1/doc/procedures.md)

## Records and queries

| Task | Python interface |
| --- | --- |
| Read one record as a dictionary | `store.get_class(selection)`, `store.get_object(selection)` |
| Read a record as a handle | `store.get_class_handle(selection)`, `store.get_object_handle(selection)` |
| Browse records by page | `store.classes(name=None, page_size=200)`, `store.objects(class_name=None, descriptors=None, page_size=200)`, `store.members(dclass, page_size=200)` yield handles. `objects(object_id=...)` returns a plain iterator over one handle and takes no Class or Descriptor filter. |
| Browse Descriptor values and declarations | `store.descriptors(dobject, name=None, page_size=200)` yields `(name, value, type_name)`. `store.class_descriptors(dclass, name=None, page_size=200)` yields `(name, type_name, comment)`. |
| Browse labeled relationships | `store.relationships(src_object=None, tgt_object=None, label=None, page_size=200)` for Objects, `store.class_relationships(src_class=None, tgt_class=None, label=None, page_size=200)` for Classes |
| Read one relationship | `store.get_relationship(selection)`, `store.get_class_relationship(selection)` return portable dictionaries. |
| Traverse neighbors | `store.neighbors(dobject, direction="both", page_size=200)`, `store.class_neighbors(dclass, direction="both", page_size=200)` yield `(neighbor_handle, relationship_dict)` pairs. Use `"incoming"` or `"outgoing"` to follow one direction. |
| Read a graph layout | `store.get_layout({"ids": [entity_id, ...]})` returns a dictionary from entity IDs to `[x, y]` coordinates. Select only Classes or only Objects from the active Namespace. Leave `options` omitted or empty for Deposit Server 2.0.1. |
| Iterate over query rows | `store.iter_query(query, parameters=..., page_size=200)` |
| Control query paging | `store.get_query_page(query, parameters=..., page_size=..., cursor=...)` |
| Read first pages for several queries | `store.query_from(queries)` yields one first page per `(query, parameters)` pair. |

Use a Class name or an explicit ID selector for Classes and an Object ID for Objects. Handles carry a Namespace and a revision baseline. They support edits through methods such as `store.set_descriptor(dobject, name, value)`. When a revision conflict occurs, call `handle.refresh()` to read the current record and review the intended change before reapplying it.

Relationship dictionaries contain the relationship ID, label, and directed endpoint IDs in `id`, `label`, `sourceId`, and `targetId`. Pass source and target handles or a label to the paged methods to filter the results.

Each query page contains `columns`, `rows`, and `nextCursor`. Each row contains `values` and `presence` dictionaries keyed by output column. Read `presence` to tell whether a value was present, including an explicit null. Query page sizes run from 1 to 1,000. For manual paging, pass the returned `nextCursor` with the same query and parameters until it is `None`. If the Namespace changes and invalidates a cursor, start a new query to read the new revision. [Deposit Query Language](https://github.com/demjanp/deposit-server/blob/2.0.1/doc/query-language.md)

Use paged iterators as context managers or call `close()` when finished. Query and entity page iterators also support `materialize(max_items=10_000)` to collect the remaining items with a limit. Entity page iterators expose the revision after reading a page. Neighbor traversal returns a generator. Call its `close()` if you stop early.

### Create and edit records

| Task | Python interface |
| --- | --- |
| Create a Class | `store.create_class(name, properties=None, descriptors=None, position=None, order=None, comment=None)` returns a `DClass`. Pass a mapping such as `{"Code": "String"}` or declaration records with `name`, `type`, and optional `order` and `comment`. |
| Create an Object | `store.create_object(class_selector, properties=None, descriptors=None, position=None)` returns a `DObject`. Select an existing Class by handle, name, or ID selector. Pass Descriptor values as a name-to-value mapping. |
| Edit Class properties or node position | `store.set_class_properties(dclass, **patch)` accepts `name`, `order`, `comment`, and `position`. Use `store.set_position(node, x, y)` for a Class or Object. |
| Edit Object Descriptor values | `store.create_descriptor(dobject, name, value)`, `store.set_descriptor(dobject, name, value)`, `store.delete_descriptor(dobject, name)` |
| Edit Class Descriptor declarations | `store.create_class_descriptor(dclass, name, type_name, comment="")`, `store.set_class_descriptor(selector, patch, dclass=None)`, `store.delete_class_descriptor(dclass, name)`. For updates, pass `{"id": declaration_id}` and a patch. You can supply `dclass=` to apply the update against that handle's revision. |
| Convert or reorder declarations | `store.change_class_descriptor_type(dclass, name, type_name, conversion=...)` accepts `"identity"`, `"integerToFloat"`, or `"floatToIntegerExact"`. Use `store.switch_descriptor_order(dclass, name1, name2)` or `store.switch_class_order(first, second)` to swap display order. |
| Read or change an Object's Class membership | `store.object_class(dobject)`, `store.is_member(dobject, dclass)`, `store.set_membership(dobject, dclass)` |
| Create a directed relationship | `store.create_relationship(src_object, tgt_object, label, weight=None)`, `store.create_class_relationship(src_class, tgt_class, label, comment="")` |
| Edit or delete a relationship by ID | `store.set_relationship(relationship_id, **patch)`, `store.set_class_relationship(relationship_id, **patch)`, `store.delete_relationship(relationship_id)`, `store.delete_class_relationship(relationship_id)` |
| Delete records | `store.delete_class(dclass)`, `store.delete_object(dobject)` accept handles or selectors. A Namespace owner can use `store.delete_all(confirm_namespace_name)` to clear its records, Patterns, and user tools while retaining the Namespace. |

### Entity handles and revisions

- Handles expose `id` (also `uuid`), `namespace_id`, `kind`, `name`, and `position`. Use `exists()` to check a current handle against the server and `as_dict()` to copy its snapshot. Assign `position = (x, y)` to update a Class or Object.
- A `DClass` exposes `declarations` by name, writable `order` and `comment`, and methods `get_descriptor_comment(name)`, `set_descriptor_comment(name, comment)`, and `switch_descriptor_order(name1, name2)`. Use `dclass[name] = type_name` to declare a Descriptor and `del dclass[name]` to delete the declaration.
- A `DObject` exposes `class_id`, `get_class()`, `set_class(dclass)`, `has_class(dclass)`, and `get_descriptor_type(name)`. Read and write values with `dobject[name]` and `dobject[name] = value`. Delete a value with `del dobject[name]`. Both handle types support `get(name, default)` for Descriptor lookup.
- `store.namespace_id` gives the bound UUID and `store.closed` reports whether the Store has closed. `store.refresh()` refreshes the session revision and invalidates its cached handles. Use `handle.refresh()` to reload a handle and its revision, and `handle.stale` to check its snapshot status.
- When an Object Descriptor assignment passes value preparation but fails during submission, inspect `dobject.pending`. A refresh preserves pending values. After checking the operation outcome and reviewing the refreshed record, call `dobject.reapply()` if you want to submit the pending values.

### Atomic batches

Use the Batch returned by `store.transaction()` to collect edits. The wrapper submits the batch as one atomic server operation when the context exits normally.

```python
with store.transaction() as batch:
    collection = batch.create_class("Sample collection")
    sample = batch.create_object(collection)

created_sample = batch.result(sample)
```

A batch supports up to 1,000 commands and 8 MiB of encoded request data. Use Batch methods or `batch.call()` for batch-eligible procedures. Store calls and nested batches are unavailable during collection. A batch reference can point to an earlier command in the same batch. Read its result after the batch completes. [Server batch procedure](https://github.com/demjanp/deposit-server/blob/2.0.1/doc/procedures.md#depositapplybatch)

Batch helpers include `create_class(name, properties=None)`, `set_class(selector, patch)`, `delete_class(selector)`, `create_object(class_selector, properties=None)`, `set_object(object_id, patch)`, `delete_object(object_id)`, `create_class_descriptor(selector, name, type_name, properties=None)`, `create_descriptor(object_id, name, value)`, and `set_descriptor(object_id, name, value)`. Each returns a reference. `batch.commands` gives a detached tuple of the collected commands, and `batch.result(reference)` returns the portable command result after successful submission.

## Descriptor values

For an existing declaration, pass a compatible Python value. When introducing a new Descriptor, the wrapper can infer its type from an unambiguous value. Use `typed(type_name, value)` to validate and supply an explicit type, for example `typed("Float", 2)` or `typed("String", None)`. The returned `TypedValue` exposes `type` and `value`.

| Deposit type | Python input |
| --- | --- |
| `String`, `Boolean`, `Integer`, `Float` | `str`, `bool`, signed 64-bit `int`, and finite `float` or an exactly representable `int`, respectively |
| `Date`, `Time`, `LocalTime`, `DateTime`, `LocalDateTime` | Compatible Python or Neo4j temporal values. Use timezone-aware values for `Time` and `DateTime` and naive values for their local variants. |
| `Duration`, `Point` | `neo4j.time.Duration` or a `neo4j.spatial.Point` with finite coordinates and an SRID |
| `List` | A list or tuple with a common supported item type. Use `TypedList(values, item_type=...)` when inference is ambiguous, such as for an empty or all-null list. |
| `DDict` | A string-keyed mapping or `DDict(data)`. A `DDict` supports indexing, assignment, deletion, and iteration. |
| `DGeometry` | `DGeometry(wkt=...)` or `DGeometry(coordinates=..., geometry_type=...)`, with optional `srid`, `vertical_srid`, `axis_order`, and `dimensions`. Call `geometry.resolve(store)` to populate the other representation and server metadata. Stored geometries expose `geometry_id`. |
| `DResource` | A stored resource reference. See [resource uploads](#files-and-exchange). |

`TypedValue` and the concrete `DType` subclasses (`DDict`, `DGeometry`, and `DResource`) expose `to_dict(format="procedure")` for the typed procedure representation and `to_dict(format="exchange")` for exchange serialization. Construct a concrete `DType` with `from_dict={"type": ..., "value": ...}` to wrap a matching typed payload.

## Patterns and user tools

| Task | Python interface |
| --- | --- |
| Manage a reusable Pattern | `store.create_pattern(name, definition, properties=None)`, `store.get_pattern(selection)`, `store.set_pattern(pattern, **patch)`, `store.delete_pattern(pattern)`. Read, edit, or delete by name or by `{"id": pattern_id}`. |
| Browse Patterns | `store.patterns(name=None, page_size=200)` yields portable Pattern records. |
| Match Objects | `store.objects_by_pattern({"id": pattern_id}, values=None, rules=None, page_size=200, cursor=None)` returns one server page. Pass the returned `nextCursor` to continue with the same Pattern, values, and rules. |
| Analyze form data for import | `store.import_data_by_pattern({"id": pattern_id}, values, rules=None, operation_id=None)` returns an `ImportResult`. Review and apply the plan separately. |
| Manage user tools in the Namespace | `store.create_user_tool(name, data, comment="")`, `store.get_user_tool(selection)`, `store.set_user_tool(user_tool_id, **patch)`, `store.delete_user_tool(user_tool_id)`, `store.user_tools(name=None, page_size=200)`. The stored `data` is a string. |

Pattern definitions describe named Object roles, their Classes and Descriptors, and any relationships to other roles. Pass form values keyed by role, for example `{"specimen": {"Code": "S-04"}}`. The match page contains `items`, `conflicts`, and `nextCursor`. [Pattern definition example](https://github.com/demjanp/deposit-server/blob/2.0.1/doc/procedures.md#depositcreatepattern)

## Calling server procedures

`Store.call(procedure, *arguments, operation_id=None)` accepts allowlisted Store procedures. Use the domain arguments in the documented order. The wrapper supplies the Deposit token and handles operation registration for tracked procedures. See [connections and sessions](#connections-and-sessions) and [request control](#live-request-control) for the dedicated Python entry points for those procedures.

```python
with store.call("deposit.get.class", "Specimen") as result:
    record = result.to_data(next(result)["node"])
```

The usual return value is a `StoreResult` that yields row dictionaries. Use it as a context manager or close it explicitly. For a native Neo4j node or relationship, call `result.to_data(value)` while its result is open to obtain a portable dictionary. Tracked calls can return portable rows directly. Import analysis and apply return an `ImportResult`, and discarding a plan returns the discard outcome directly. Check each procedure’s arguments and result columns in the server reference. [Server procedure reference](https://github.com/demjanp/deposit-server/blob/2.0.1/doc/procedures.md)

## Tracked operations

By default, the wrapper generates and registers an ID for every tracked operation. If you need the ID before submitting the call, generate a fresh UUIDv7 with `Client.new_operation_id()` and pass it as `operation_id=`. Supported entry points include:

- `Store.call()` for procedures marked as always tracked. Convenience setters such as `store.create_class_descriptor()` generate their own IDs. Use the corresponding tracked `Store.call()` route to pass an explicit ID.
- Store exchange and import methods `export_exchange()`, `import_exchange()`, `import_from()`, `import_rows()`, `import_subtree()`, `from_dict()`, `from_networkx()`, `import_data_by_pattern()`, and `import_apply()`.
- Client methods `clone_namespace()` and `prune_resources()`. Pruning accepts an explicit ID when the options set `dryRun` to `False`.

An explicit import analysis ID identifies the analysis. Intermediate transfers and plan application are distinct operations with their own IDs.

Use `store.get_operation(operation_id)` to look up the attempt and `store.cancel_operation(operation_id)` to request cancellation. The Client equivalents take a Deposit token and the original scope, which is the Namespace UUID or `"server"`. After a lost response, look up the same ID in the same scope. A replacement Store must use the same Deposit actor and Namespace. A state of `COMMITTED` confirms that the server committed the operation even if its original response is unavailable. Treat an unconfirmed outcome as unknown and avoid replaying the mutation blindly.

`DepositCancelled` indicates confirmed cancellation and rollback. `OperationCommittedAfterCancel` indicates that the operation committed before cancellation took effect. `OutcomeUnknown` supplies the operation ID and scope to inspect. Server errors expose a `DepositError.code` and optional details. Driver and transport failures use `TransportError`. [Worked operation-ID example](quickstart.md#operation-ids)

Use `store.get_last_operation()` or `client.get_last_operation(token)` to read the last operation observed by that session. For a particular attempt, look up its exact ID. `DepositError` also exposes `message`, `request_id`, and `retryable` when available. `CompatibilityError` identifies an unsupported driver, server version, or capability.

### Live request control

`store.get_requests()` returns an immutable snapshot of requests visible to the authenticated actor. Use `store.cancel_request(request_id)` with the request ID to ask the server to cancel one active request, or `store.cancel()` to request cancellation of the Store's current operation or read. These controls can run from another thread while the ordinary Store call is busy. The cancellation methods return a Boolean for the cancellation request. Check the final tracked outcome by operation ID when available.

## Files and exchange

- Upload a local file with `store.upload_resource(DResource(path=...))` or supply a readable binary stream with `DResource(stream=..., filename=...)`. The wrapper reads the borrowed stream from its current position and the caller closes it. Attach the returned resource to a resource Descriptor. A stored resource exposes `filename`, `resource_id`, and `is_image` when present. Use `resource.open(store)` in a context manager and consume the stream to EOF to verify the complete content. Closing before verification raises a `DepositError`.
- The resource reader supports `read(size=-1)` and `readinto(buffer)` for byte-oriented reads. Use `readable()` or `closed` to check its state and `close()` for explicit cleanup.
- `store.export_exchange(roots=None, max_hops=None, root_class_boundary=True, operation_id=None)` returns an `ExchangeBundle`. The default exports the whole Namespace and includes retained history when history mode is persistent, which requires owner access. For a subgraph, supply a nonempty list or tuple of Object handles, UUID strings, or `{"id": uuid}` selectors. The server accepts `max_hops` from 0 to 10 and defaults to 3. With `root_class_boundary=True`, traversal skips links between two non-root Objects when they share a Class with a root. Use the default traversal settings for whole-Namespace exports.
- `ExchangeBundle` is available from `deposit.exchange`. Keep the bundle open while inspecting `bundle.manifest` or `bundle.graph`, or while reading streams from `bundle.open_package_stream()` or `bundle.open_resource(member_name)`. `bundle.save(path)` writes the verified ZIP. `bundle.source` exposes the authorized package reference. Close the bundle and its streams after use.
- `store.get_subtree(roots=None, max_hops=None, root_class_boundary=True)` returns a detached, revision-pinned snapshot with `manifest` and `graph` and omits history. Use this or `store.to_networkx()` when you need to inspect or manipulate graph data in Python.
- `client.clone_namespace(token, source, operation_id=None)` creates a new Namespace atomically from a complete exchange package. Pass an authorized bundle, ZIP path, ZIP bytes, or binary stream. The authenticated actor owns the new Namespace. Use the returned `CloneResult` as a context manager to read `namespace_id`, `name`, `identity_mappings`, and `resource_mapping`. Track the clone under `scope="server"`.

## Import plans

The following methods analyze input in the receiving Store. Each accepts `rules=` and an optional analysis `operation_id=`. The server determines matches, conflicts, and whether a plan is available.

| Input | Python interface |
| --- | --- |
| Exchange package | `store.import_exchange(source)` accepts a ZIP path, ZIP bytes, a readable binary stream, a callable returning a binary stream context manager, or an authorized `ExchangeBundle`. |
| Another Store | `store.import_from(source_store, roots=None, max_hops=None, root_class_boundary=True)` exports from an independently authenticated source Store before analysis in the receiving Store. |
| Tabular rows | `store.import_rows(rows, relationships=None, targets=None)` accepts a list or tuple of row dictionaries. A row can contain `id` and a `values` map, for example `{"id": "row-1", "values": {"Code": "S-04"}}`. `targets` maps column names to roles, Classes, and Descriptors, and `relationships` can supply relationship records. |
| Graph snapshot | `store.from_dict(data, resources=None, resource_source=None)` and `store.import_subtree(subtree, resources=None, resource_source=None)` accept a portable `manifest` and `graph` mapping, such as the result of `get_subtree()`. |
| Form data or NetworkX | See [Patterns](#patterns-and-user-tools) and [NetworkX](#networkx). |

To analyze a row against the sample Namespace, map its `Code` column to the `Code` Descriptor of a `Specimen`.

```python
rows = [{"id": "row-1", "values": {"Code": "S-04"}}]
targets = {
    "Code": {
        "object": "specimen",
        "className": "Specimen",
        "descriptor": "Code",
        "type": "String",
    }
}

with store.import_rows(rows, targets=targets) as analysis:
    print(analysis.state, analysis.plan_id)
```

See the server procedure for the row matching rules and relationship schema. [Server row import](https://github.com/demjanp/deposit-server/blob/2.0.1/doc/procedures.md#depositimportrows)

For snapshots that reference file content, supply `resources=` as a mapping from content-addressed member names to bytes, paths, binary streams, or callables returning binary stream context managers. You can also pass `resource_source=` as an authorized Store or exchange bundle from the original Namespace. Borrowed binary streams are read from their current position and remain the caller's responsibility to close.

Use the analysis `ImportResult` in a context manager and retain its `plan_id` if one is available. Use `store.import_inspect(plan_id)` to read the analysis and the current plan observation, `store.import_apply(plan_id, operation_id=None)` to apply a ready plan, or `store.discard_import_plan(plan_id)` to discard an unused plan.

`ImportResult` exposes `result_id`, `operation_id`, `state`, and `plan_id`. After inspection, `plan_observation` and `plan_state` describe the current plan lifecycle, while `state` belongs to the original analysis result. The result is also a mapping. Use its `collections` entry to discover available collections, then access them by name. Paged collections use `LazyResultMapping` or `LazyResultSequence`. Large text uses `LazyText.iter_chunks()` or `LazyText.read(size)`, where `size` counts characters and `len(text)` gives the UTF-8 byte count.

Read lazy values while their result and its owning Store or Client are open and the server lease is valid. Use a context manager or `close()` when finished. Closing the result closes the local view. The server lease expires at its fixed deadline.

### NetworkX

`store.to_networkx(roots=None, max_hops=None, root_class_boundary=True)` returns a `networkx.MultiDiGraph`. Start with an exported graph when preparing changes for import.

- Keep `namespaceId` and the nonnegative `revision` in graph metadata. Preserve relevant Patterns, resources, geometries, user tools, and scope metadata.
- Keep Class and Object node keys in the form `(namespace_id, kind, id)`, with matching `kind` and `id` attributes. Object Descriptors are stored on their owning Object nodes.
- Keep edge keys equal to the edge IDs. A Membership runs from Object to Class, a Relationship from Object to Object, and a ClassRelationship from Class to Class. Preserve consistent endpoints and finite JSON-portable attributes.

Analyze the edited graph with `store.from_networkx(graph)` in the receiving Store and review the result before applying its plan. For referenced resources, pass content providers keyed by package member name as `resources=`, or pass a Store or exchange bundle from the original Namespace as `resource_source=`. [Quickstart exchange workflow](quickstart.md#import-and-export)

Use `deposit.networkx.to_networkx(snapshot)` and `deposit.networkx.from_networkx(graph)` for local conversion between a portable snapshot and a graph. The module functions perform no server calls. The Store's `from_networkx()` analyzes the converted snapshot on the server.
