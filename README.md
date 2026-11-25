# deposit-python

**deposit-python** is the synchronous Python wrapper for **Deposit**. The Python distribution is named `deposit-python` and the import is `deposit`. It gives Python applications access to the scientific records, relationships, and attached files managed by [deposit-server](https://github.com/demjanp/deposit-server).

## Features

- **Connections and sessions.** `Client` uses a Neo4j driver to discover the server and open independently authenticated `Store` sessions. Each Store works in one Namespace.
- **Python records and values.** `DClass` and `DObject` represent Classes and Objects. `DGeometry` and `DResource` represent spatial values and attached files.
- **Relationships.** Directed, labeled links connect Classes to Classes and Objects to Objects.
- **Queries and edits.** Run Deposit Query Language queries, page through large results, and group related edits into atomic batches. Revision checks identify conflicting changes.
- **Import and exchange.** Import data using Patterns, move portable packages between Stores, clone Namespaces, and convert graphs to and from NetworkX. Inspect import plans before applying changes.
- **Files and tracked operations.** Transfer files through verified streams, inspect operation outcomes, and request cancellation. Use `Store.call()` to access additional server procedures.

## Platform

The current support target is Python 3.14 on Windows x64 with Deposit Server 2.0.1. Pip installs the Neo4j Python driver and NetworkX as dependencies.

## Getting started

From a full Git checkout, follow the [quickstart](https://github.com/demjanp/deposit-python/blob/main/doc/quickstart.md) to install the wrapper, open a Store, and query the Deposit Server sample data. If you need a server, follow the [Deposit Server 2.0.1 checkout instructions](https://github.com/demjanp/deposit-server/blob/2.0.1/doc/installation.md#run-from-a-git-checkout).

For a local wheel or a published 2.x package, see the [quickstart installation options](https://github.com/demjanp/deposit-python/blob/main/doc/quickstart.md#install). The wrapper generates operation IDs for tracked calls. See [operation IDs](https://github.com/demjanp/deposit-python/blob/main/doc/quickstart.md#operation-ids) to choose an ID before submission and look up the outcome.

See the [Python API reference](https://github.com/demjanp/deposit-python/blob/main/doc/api.md) for wrapper behavior.

## Build the release wheel

On Windows x64, open PowerShell at the root of a clean, full Git checkout. Check out the release commit with its stable `vX.Y.Z` tag. Use a fresh build directory alongside the checkout.

```powershell
py -3.14 -m venv ..\deposit-python-release\venv
..\deposit-python-release\venv\Scripts\python.exe -m pip install build
..\deposit-python-release\venv\Scripts\python.exe -m build --wheel --outdir ..\deposit-python-release\dist
```

The builder writes the wheel to the sibling build directory. The wheel version comes from the Git tag. The wheel contains the wrapper and its procedure catalog, and pip resolves the runtime dependencies during installation.

## Citation

Please use this citation when referencing the software.

> Demján, P. (2026). *deposit-python* [Computer software]. [GitHub repository](https://github.com/demjanp/deposit-python).

## Acknowledgements

Funding for the server component is listed in the [Deposit Server acknowledgements](https://github.com/demjanp/deposit-server#acknowledgements).

## Contact

Peter Demján ([peter.demjan@gmail.com](mailto:peter.demjan@gmail.com))

Institute of Archaeology of the Czech Academy of Sciences, Prague, v.v.i.

## License

Distributed under the [MIT License](https://opensource.org/license/mit). See the repository’s [LICENSE](https://github.com/demjanp/deposit-python/blob/main/LICENSE) file.
