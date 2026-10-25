The consolidated proposal is to implement **deposit-python as one thin client library for Deposit 2**, with method names that follow the server procedures, convenient Python objects, and recovery managed entirely inside the wrapper.

Implementation should proceed in six stages, with real-server integration testing from the beginning. The repository will remain `deposit-python`; the published package and Python import will both be `deposit`, distinguished from Deposit 1 by the major version.

This proposal incorporates the agreed decisions. It remains a design proposal: no implementation or documentation changes are authorized by its acceptance alone.

The **scope and responsibility boundary** are straightforward. The wrapper provides convenient access to all public deposit-server functionality. The server remains responsible for authorization, domain validation, schema inference, query execution, import matching, history, layout, geometry processing, format conversion and persistent storage.

Python handles the work necessary to use those capabilities safely:

- Preparing and sending procedure calls.
- Encoding Python values and decoding public results.
- Managing connections, Deposit sessions and Namespace binding.
- Maintaining local entity snapshots.
- Collecting atomic batches.
- Managing upload/download streams.
- Recovering interrupted operations, including after an application restart.

These client responsibilities should share one implementation rather than being repeated in individual methods.

The **architectural alternatives** are:

| Approach | Advantages | Assessment |
|---|---|---|
| Multitier design resembling deposit-server | Familiar separation of interfaces, services and persistence | Adds unnecessary layers to a client without its own domain engine or database |
| Service-oriented modules for resources, imports, history and other areas | Groups related workflows and supports focused testing | Useful internally, provided every module uses the same execution core |
| Flat collection of procedure wrappers | Simple initial implementation and direct procedure mapping | Becomes difficult to maintain consistent recovery, session and result handling |
| Small layered core with thin public methods | Centralizes client mechanics while keeping domain access simple | **Recommended architecture** |
| Separately deployed gateway or services | Could expose another protocol to additional clients | Adds deployment and protocol responsibilities beyond this wrapper’s scope |

The recommended design is one package with the following internal components:

| Component | Responsibility |
|---|---|
| Public interfaces | `Client`, `Store`, entity handles and convenient methods |
| Procedure gateway | Procedure metadata, argument binding, execution and output handling |
| Session and recovery coordinator | Authentication lifecycle, Namespace binding, recovery journal and prepared actions |
| Values and snapshots | Lossless encoding, portable decoding and revision-aware local snapshots |
| Batch collector | Temporary references and submission through one server batch |
| Transfers and workflow helpers | Recoverable streams and composition of server operations |
| Optional NetworkX adapter | Conversion between portable Deposit data and NetworkX graphs |

The dependency direction is:

`Public interfaces → shared execution core → Neo4j driver → deposit-server`

The [Deposit Hub](C:/documents_synced/Deposit_v2/deposit-hub/README.md) remains the knowledge base. Implementation follows its public contracts, particularly the [Python specification](C:/documents_synced/Deposit_v2/deposit-hub/deposit-python.md) and [server calling and recovery contract](C:/documents_synced/Deposit_v2/deposit-hub/deposit-server-api.md). Existing Python signatures will need reconciliation with the decisions below.

The **public interface** will use the agreed separation between `Client` and `Store`.

`Client` handles server discovery, authentication and operations needed before opening a Namespace. A `Store` represents work within one chosen Namespace and remains bound to its immutable UUID.

Each Store owns an independent Deposit session. Multiple Stores may share the caller’s Neo4j driver, but they do not share mutable session state. Closing a Store closes its readers and logs out its session while leaving the caller-owned driver available.

A Namespace rename does not change the Store’s identity. Switching to another Namespace requires a separate Store.

The first release will provide synchronous methods. Ordinary operations are serialized within each Store, while separate Stores can operate concurrently. Cancellation must remain possible while a long-running operation is active.

Python method names will **directly reflect server procedure names**, preserving their verbs and converting dots and camelCase to snake_case:

| Server procedure | Python method |
|---|---|
| `deposit.create.class` | `create_class()` |
| `deposit.get.object` | `get_object()` |
| `deposit.set.descriptor` | `set_descriptor()` |
| `deposit.create.classDescriptor` | `create_class_descriptor()` |
| `deposit.resource.beginUpload` | `resource_begin_upload()` |

For example:

```python
finds = store.create_class("Find")
find = store.create_object(finds)

store.set_descriptor(find, "Description", "Pottery fragment")
```

Convenient entity access can coexist with the named methods:

```python
find["Description"] = "Pottery fragment"
description = find["Description"]
```

Helpers that coordinate several server procedures can have descriptive names such as `upload_resource()`. Local graph adapters retain `to_networkx()` and `from_networkx()`.

Creation and updating remain distinct operations. The wrapper should not give a method a combined create-or-update meaning when its corresponding server procedure only performs one of those operations.

The **result interface** will have two forms:

- Ordinary methods return convenient Python values, detached records or entity handles.
- Advanced direct calls return closable results, normally used through a `with` block. These preserve the server’s actual named columns and keep native results within their valid transaction lifetime.

Both forms use the same session, scope and recovery safeguards.

The wrapper will decode public portable representations rather than private Neo4j storage properties. Descriptor values and their declarations must be obtained from the same checked snapshot.

Encoding and decoding must preserve:

- Missing values versus explicitly unknown values.
- Exact integers and floating-point values.
- Temporal precision and time-zone information.
- Duration and spatial reference information.
- Typed nested lists and dictionaries.

Unsupported or lossy conversions raise errors. Editing a returned dictionary, list or geometry remains a local operation until the value is explicitly assigned back.

Entity handles retain stable IDs and observed snapshots. Polling may mark cached data stale, but it does not silently advance an editing baseline. Conflicting writes preserve the user’s values and require explicit refresh/reapply.

The **batch interface** will use temporary references:

```python
with store.transaction() as batch:
    finds = batch.create_class("Find")
    first_find = batch.create_object(finds)

created_object = batch.result(first_find)
```

This illustrates the intended interface. Inside the block, returned references identify commands whose server results do not yet exist. Successful exit submits one `deposit.apply.batch` operation; concrete results become available afterward.

An exception during collection discards the unsubmitted commands. A server-side failure rolls back the whole batch. The wrapper enforces the server’s batch restrictions, including exclusions for reads, nested batches, transfers and import application.

Oversized batches fail clearly rather than being silently divided into separately committed operations.

**All recovery functionality will be contained in the wrapper.** Applications will not generate recovery keys, retain exact arguments or implement the server’s preparation and recovery sequence.

Normal application code remains:

```python
find = store.create_object(finds)
```

Internally, the wrapper will:

1. Generate an internal recovery key.
2. Securely persist the exact operation before preparing it on the server.
3. Bind the record to the original server/database, user and Namespace identities.
4. Prepare and execute the operation.
5. Resolve uncertain outcomes through the server’s recovery procedures.
6. Persist the resolved portable outcome before releasing the preparation.
7. Make recovered results available after reconnecting and perform controlled cleanup.

A protected per-user journal will retain pending operations. Sensitive inputs and authentication material needed for recovery will use operating-system-backed protected storage. Credentials and tokens must not appear in logs.

Recovery records and unfinished upload content must survive an application restart. Competing processes must not independently resume the same operation.

The wrapper will classify actions using explicit server metadata. Procedure names alone are insufficient: some exports and import-analysis operations require recovery preparation, while session controls and pruning dry runs follow different rules.

Retries remain under the recovery coordinator’s control. Disconnection and cancellation do not prove rollback.

Automatic recovery respects authorization and original preconditions. If credentials are unavailable, permissions have changed or an edit conflicts, the wrapper preserves pending work and reports the condition. It does not silently refresh and overwrite newer data.

Recovered results preserve the original UUID and saved outcome. They do not imply that an entity still exists unchanged; a later lookup may report that it has been deleted.

The **resource interface** will accept client-local files and ordinary binary streams, including streams that can only be read once.

When necessary, the wrapper temporarily spools content to determine its size and checksum. Content needed to resume an unfinished upload remains in protected recoverable storage until resolution. Resuming an upload must not accidentally read changed bytes from the original source file.

The wrapper manages chunking, checksums, offsets and continuation handles. It closes files it opens; callers retain ownership of streams they supply. Downloads return closable binary readers.

Browser authentication remains separate from the Store session. Store tokens are never appended to download URLs.

Geometry processing, tabular formats, import matching and other domain operations continue to run on the server.

**NetworkX will be optional.** Both `to_networkx()` and `from_networkx()` require it and will provide a clear installation message when it is unavailable. Other wrapper functionality remains usable without it.

Graph conversion must preserve stable identity, Class membership, direction, parallel relationships, declarations and applicable Patterns. Import analysis and matching still run on the server. A NetworkX graph is not presented as a complete history or resource backup.

For **complete server coverage**, maintain a reviewed manifest derived from the [public procedure catalog](C:/documents_synced/Deposit_v2/deposit-server/runtime/public-api.json), including discovery.

The manifest describes arguments, result columns, session requirements, recovery classification and batch eligibility. Every public procedure receives a supported invocation path through the appropriate public interface or lifecycle coordinator.

Coverage records distinguish correct dispatch, convenient Python behavior and executed integration acceptance. Merely confirming that a procedure exists does not establish usable coverage.

The **implementation stages** are:

| Stage | Scope | Completion evidence |
|---|---|---|
| 1. Contracts and package foundation | Reconcile names and signatures; establish package structure, procedure manifest, errors, supported versions, protected storage and test connection | Installed package imports correctly; discovery/session smoke tests and contract checks pass against a real server |
| 2. Safe end-to-end core | Client/Store lifecycle, prepared creation, portable reads, revision conflicts, durable journal and restart recovery | Create/read workflows, independent Store conflicts, lost replies and client-process restart recovery pass |
| 3. Complete procedure gateway | All procedure families, scope/control routing, named outputs, raw-result lifetimes and recovery classification | Catalog parity and representative execution checks pass; remaining specialized acceptance is explicitly tracked |
| 4. Typed domain interface | Handles, structured values, CRUD, queries, paging, snapshots and batch references | Scientific values, missing/null behavior, stale edits, early closure and batch atomicity verified |
| 5. Resources and extended workflows | Recoverable streams, geometry, Patterns, imports, formats, history, versioning and administration | End-to-end scenarios pass for every feature family, including interrupted workflows |
| 6. Graph conversion and release acceptance | NetworkX, exchange staging, remaining coverage, installation and compatibility regression | All promised interfaces have recorded acceptance evidence; installed-wheel and lifecycle regression pass |

Staging is appropriate because session handling, value decoding and recovery affect nearly every method. Durable recovery belongs with the first mutation workflow, then extends to transfers and other composed operations.

For **integration testing**, reuse the existing Windows server harness for provisioning, runtime ownership, credentials, watchdogs and restart/crash scenarios. Keep Python test sources in `deposit-python`, with generated environments and evidence under the sibling `test_harness`.

Add a Python acceptance hook to that lifecycle. This is proposed tooling work, potentially involving the server repository; it is not an existing general-purpose wrapper test command. The [server verification guide](C:/documents_synced/Deposit_v2/deposit-hub/deposit-server-verification.md) remains the authority for runtime and diagnostic-probe ownership.

Testing will combine:

- **Focused unit tests:** encoding, binding, batches, snapshots, journal transitions and stream ownership.
- **Live contract tests:** signatures, output columns, empty results, safe errors and version checks.
- **Workflow tests:** public Python methods against disposable Namespaces, with outcomes checked through an independent session or direct public procedures.
- **Recovery tests:** controlled reply loss, actual Python process termination, and server restart/crash scenarios through existing harness owners.
- **Installed-package tests:** isolated installation, imports and behavior with and without NetworkX.

Required scenarios include independent Stores sharing a driver; Namespace rename; conflicts; failed batches; lossless values; paging and early closure; cancellation near commit; interrupted uploads; missing spools; changed source files; journal contention; password-change session revocation; and recovery after permissions or entity existence change.

Focused checks run at each stage. Complete Python regression runs before release, together with relevant server checks when shared tooling or server behavior changes. Existing Java tests and the independent Python exchange checker support this work but do not establish wrapper acceptance.

The **packaging and compatibility decisions** are settled:

| Item | Decision |
|---|---|
| Repository | `deposit-python` |
| Published distribution | `deposit` |
| Import | `import deposit` |
| Generation distinction | Deposit 1 uses `1.x`; Deposit 2 uses `2.x` |
| NetworkX | Optional installation extra |
| Initial integration platform | Windows, using the existing harness |
| Compatibility | Explicitly tested wrapper/server/Python/driver matrix |

Deposit 1 applications should constrain their dependency to `<2`; Deposit 2 applications should select the `2.x` generation. Applications needing both generations use separate environments. Retaining the import name does not imply API compatibility.

The initial release will target one selected server release and a bounded Python/driver matrix. The wrapper checks the server’s reported version before API-dependent work and rejects unsupported combinations clearly. Wrapper and server version numbers need not match.

**No broad architectural questions remain unresolved.** Stage 1 still needs to select concrete supported versions, protected-storage mechanisms, retention defaults and detailed signatures. Those are implementation-planning tasks within the agreed design; any finding that requires changing the agreed scope or compatibility promise should be brought back for a decision.