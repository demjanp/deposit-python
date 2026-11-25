# deposit-python

**deposit-python** is the synchronous Python wrapper for [deposit-server](https://github.com/demjanp/deposit-server), distributed and imported as `deposit`. It lets Python applications work with Deposit through familiar objects and methods instead of calling database procedures directly.

## Features

- **Connections and sessions.** `Client` handles connections, authentication, and server feature discovery. Each `Store` provides an independently authenticated working session in one Namespace.
- **Python data objects.** `DClass` and `DObject` expose records and relationships; `DGeometry` and `DResource` represent spatial values and attached files.
- **Reading and editing.** Python methods retrieve records, run Deposit Query Language queries, page through large results, and group related edits into atomic transactions. Revision checks identify conflicting changes.
- **Import and exchange.** The wrapper supports Pattern-based imports, transfers between Stores, Namespace cloning, and conversion from dictionaries or NetworkX graphs. `ExchangeBundle` handles portable packages; import plans can be inspected before applying changes.
- **Reliable operations.** Verified streams transfer files, while operation tracking, cancellation, and structured errors help applications recover from interrupted work. `Store.call()` provides access to additional server procedures.

Persistent storage, permissions, validation, and history remain the responsibility of deposit-server; the wrapper provides the Python interface to these functions.

## Platform

The package requires Python 3.14 and currently supports Windows x64.

## Getting started

See [Quickstart](doc/quickstart.md) for installation and first use. Further details are in the [Technical specification](https://github.com/demjanp/deposit-hub/blob/main/deposit-python.md) and [server API manifest](https://github.com/demjanp/deposit-server/blob/main/runtime/public-api.json).

## Operation IDs

Use ordinary Store methods such as `store.create_class_descriptor(specimen, "Color", "String")`
or pass only domain arguments to `Store.call`. The wrapper generates and
registers an ID for each tracked operation, including intermediate transfers.

To keep an identity before submission, generate a fresh UUIDv7 with
`Client.new_operation_id()` and pass it as `operation_id=` to a supported
tracked call. Use `store.get_operation(operation_id)` to inspect the outcome
and `store.cancel_operation(operation_id)` to request cancellation. The
reported state tells you whether rollback or commit is confirmed.
See the [quickstart examples and lost-response rules](doc/quickstart.md#operation-ids).

## Copyright and license

Copyright © 2026 Peter Demján.

Distributed under the MIT License.
