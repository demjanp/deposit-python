# Deposit Python release notes

## Deposit Python 2.0.1

The Python wrapper targets Python 3.14 on Windows x64 and pairs with Deposit Server 2.0.1. Pip installs the Neo4j Python driver (`>=6.1,<7`) and NetworkX (`>=3.4,<4`). The wrapper is distributed under the [MIT License](LICENSE).

- Open a Store in a Namespace, query and edit records, transfer files, and exchange data with Deposit Server. Follow the [quickstart](doc/quickstart.md) to query the server sample data.
- The wrapper registers IDs for tracked operations. Applications can supply an ID on supported calls and use it to inspect the operation outcome or request cancellation. [Operation IDs](doc/quickstart.md#operation-ids)

### Moving from deposit 1.x

The 1.x series is the earlier standalone Deposit product. Version 2.x uses the same distribution and import name (`deposit`) and provides a client for Deposit Server with a different API. Installing 2.x in an environment containing 1.x replaces the 1.x installation. Keep your existing environment for 1.x and use the dedicated Python 3.14 virtual environment in the [installation instructions](doc/quickstart.md#install) for the wrapper.
