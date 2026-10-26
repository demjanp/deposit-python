# Stage 1 contract reconciliation

This table records the proposal (`docs/implementation_proposal.md`) against the
hub design target (`../deposit-hub/deposit-python.md`) and the implemented Python
surface. The hub owns shared API contracts; the proposal and implementation plan
own this project's architecture and stage sequence; `implementation_status.md`
owns verified stage progress. This file is the auditable reconciliation, not a
replacement for those sources.

## Implemented `Client` signatures and approved Store factory

The prior hub token-injected `Store(driver, ..., token)` sketch is superseded.
The Client signatures below reflect the implemented synchronous facade. The
Store factory row records the Stage 1 approved contract, implemented and
verified in Stage 2. Auth tokens are explicit arguments and are never part of
the driver or Namespace identity.

| Proposal/hub point | Reconciled signature | Result/lifetime and owner | Errors and gate |
|---|---|---|---|
| Client construction | `Client(driver: Driver, *, owns_driver: bool = False, database: str | None = None)` | Returns `Client`; caller owns `driver` by default. Only an internally owned driver closes with Client. Client selects the call database. | Invalid/closed use raises safe Python errors; 1A/1C. |
| Discovery | `Client.discover() -> dict[str, Any]` | Detached server version record; Client-owned, no Deposit token or Store. Validates Deposit, Neo4j Server and driver versions before dependent API calls. | Compatibility/invalid response and transport failures fail explicitly; 1B/1C. |
| Login | `Client.login(username: str, password: str) -> str` | Returns an application-held session token; Client never logs credentials/token. | `DepositError`, compatibility and transport failures; 1C. |
| Bootstrap | `Client.bootstrap(username: str, password: str) -> dict[str, Any]` | Returns detached administrator profile. Client lifecycle method, harness only on a fresh runtime. | Server/compatibility/transport errors; 1C. |
| Session inspection | `Client.get_session(token: str) -> dict[str, Any]` | Returns detached sanitized profile/expiry. Client operation for an authenticated session. | `DepositError` or `TransportError`; 1C. |
| Logout | `Client.logout(token: str) -> None` | Revokes that Deposit session; does not close driver. | `DepositError` or `TransportError`; 1C. |
| Close/context | `Client.close() -> None`; `Client.__enter__() -> Client`; `Client.__exit__(exc_type, exc, traceback) -> None` | Idempotent close; closes only an owned driver. Context manager owns Client lifetime. | Requests after close are wrapped as `TransportError` by `_single`; entering a closed Client context raises `RuntimeError`. Package/lifecycle tests. |
| Store factory | `Client.open_store(namespace, *, username, password, recovery_directory=None) -> Store` | Stage 1 approved public signature, implemented and verified in Stage 2. Every call authenticates a fresh Store session; selects Namespace once and resolves its UUID. Caller-authenticated Neo4j driver ownership stays with Client. Store construction is internal. | Credentials are used only for fresh authentication and never persisted/logged; `Client.login` token is not reused. `None` uses current-user protected `%LOCALAPPDATA%\Deposit\recovery`. |

The accepted `namespace` selector is a name string or explicit `{"id": UUID}`
map; implementation resolves it once and retains the UUID. `Store` construction
is internal. Reauthentication uses fresh credentials with `Client.open_store` or
`Client.recover`; no `Store.reauthenticate` method is approved.

## Approved Client-owned recovery API

The Stage 2 recovery-inspection and result signatures are approved:

```python
Client.pending_operations(*, recovery_directory=None) -> tuple[PendingOperation, ...]
Client.recover(operation, *, username, password) -> RecoveredOperation
Client.recovered_operations(*, recovery_directory=None) -> tuple[RecoveredOperation, ...]
```

`PendingOperation` and `RecoveredOperation` are immutable safe references. Their
public random reference IDs are distinct from server recovery keys. They expose no
recovery key, exact operation arguments, credentials, token or journal path.
`pending_operations` lists unresolved work in the protected current-user journal;
`recovered_operations` lists retained results after durable outcome persistence
and confirmed server release. Result payloads are immutable portable values.
Recovered payloads expose maps as immutable mappings and JSON arrays as ordered
tuples; the protected journal retains the original portable map/list structure.
`Client.recover` accepts a reference returned by `pending_operations` on that
Client, authenticates with fresh caller-supplied credentials, checks the original
actor and full deployment identity, and resolves by the stored key and arguments.
For a completed Namespace deletion it reads the original server receipt and
releases the preparation without attempting to select the deleted Namespace.
Custom recovery directories are associated with references enumerated by that
Client; credentials and journal inputs are never stored in a reference.

If a local intent survives reconnect but the server reports no preparation, the
client must not create a fresh recovery key under a new baseline. It preserves
the original intent and requires explicit refresh/reapply unless the original
preconditions are proven unchanged. `test_missing_preparation_with_changed_revision_is_never_recreated`
in `tests/unit/test_store.py` verifies that changed preconditions fail closed
without a second prepare.

| Contract | Hub design target | Proposal / resolved Python contract | Required fields and owner | Stage owner |
|---|---|---|---|---|
| Construction and discovery | `Client(driver, *, owns_driver=False, database=None)` plus Client discovery/login and a token-injected Store sketch | Exact `Client.open_store(namespace, *, username, password, recovery_directory=None) -> Store`; fresh private Store authentication, one-time Namespace UUID selection, and a current-user protected default recovery directory. | Exact args/defaults; return/error type; driver ownership; endpoint/database; fresh auth lifetime; UUID binding; recovery directory and ACL; closed-object behavior. Client owns the driver; each Store owns its Deposit session. | Stage 1 contract; Stage 2 lifecycle implementation/tests. |
| Store session and driver | One independent Deposit session per Store; shared caller driver remains open unless Client owns it. | Same. | Session/token scope, driver close rule, reader cleanup, Namespace UUID binding and Store close behavior. Client owns driver policy; each Store owns its session. | Stage 2. |
| Procedure dispatch | Hub describes direct procedure-named conveniences and advanced routing. | Proposal maps procedure verbs to Python names and routes procedures through a shared coordinator. No unrestricted generic call may retarget a Store. | Ordered positional args, server defaults, registration token placement, named output columns, route owner, scope, action classification and failure mapping. Coordinator owns the reviewed dispatch path; Client/Store own convenience methods. | Stage 1 manifest; Stage 3 full gateway. |
| Prepared recovery | Hub requires server-prepared exact arguments and no client-generated replay after uncertainty. | Proposal places pending/recovered work behind the coordinator, not application recovery keys. | Immutable normalized input, identity bindings, versioned serialization, durable state, result fidelity, release confirmation, fail-closed corruption/unknown-version handling. RecoveryCoordinator owns state transitions and retained outcomes. | Stage 1 contract; Stage 2 implementation. |
| Identity proof | Hub's prior sketch left deployment identity open. | Approved Stage 1 policy binds to target Neo4j `db.info` ID and system-store `dbms.info` ID, plus the contacted endpoint/database from Neo4j 6.4 `ResultSummary.server.address` and `.database`, Deposit actor and immutable Namespace UUID. Do not treat the configured routing URI as the contacted endpoint or as a reconnection URI. Missing/denied/changed IDs or endpoint/database retargeting fail closed. | Persist and compare both native IDs and the resolved contacted host/port and database, actor UUID and Namespace UUID before recovery. A copied/restored store may retain IDs; that accepted limitation is explicit. | Stage 1 identity contract/live smoke; Stage 2 enforcement. |
| Protected journal | Hub calls for protected, crash-safe recovery storage. | Proposal requires current-user OS protection, exclusive cross-process coordination and lossless exact inputs/results. | Versioned lossless envelope; JSON-compatible typed tags only; no pickle or `default=str`; reject unsupported/corrupt records and retain them. DPAPI current-user encryption plus current-user-only ACL. | Stage 1 storage primitives/contract; Stage 2 journal. |
| Durability and resource bounds | Hub does not define journal quota/retention internals. | Approved finite defaults: per-user 1 GiB total quota, 16 MiB maximum encoded record, 1,024 unresolved operations, 30-day resolved retention only after durable outcome and confirmed release, and zero-second fail-fast lock acquisition. Defaults can be configured before new work begins. | Include encoded records, spools and peak temporary copies in quota accounting. Never prune unresolved work or work with uncertain release. Flush+fsync protected temp before atomic replace; lock journal across processes. On disk/write failure preserve pending/server preparation and never release or claim success. | Stage 1 contract; Stage 2 tests and implementation. |
| Versioned serialization | The hub requires exact recovery inputs and portable outcomes. | Proposal requires original normalized arguments and identity; results preserve public fidelity. | Explicit schema version and tagged typed values; preserve list/map order and distinctions needed by Python; reject unknown tags, duplicate fields, unsupported versions and malformed records. No lossy fallback. | Stage 1 contract; Stage 2 implementation/tests. |
| Scope and action metadata | Hub routes Store and lifecycle operations explicitly. | Server `ProcedureCatalog` is authoritative for action/scope and ordered binding semantics. `runtime/public-api.json` now matches it for `deposit.load.csv`, `.xlsx`, `.gpkg` (operational/action=true); procedure args/output/batch exclusions are unchanged. The earlier JSON classifications are rejected by parity tests. No override is needed. | Compare JSON metadata with actual registration and `ProcedureCatalog` declarations; record source hashes in generated manifest. | Stage 1 manifest/parity; Stage 3 all-family behavior. |

### Build/catalog provenance

The corrected `deposit-server/runtime/public-api.json` SHA-256 is
`d8afdd5cce6ef3aa17503d548caf3881edc53b026d9e3e783770f45179997a4d`. The
installed smoke used the prebuilt clean-tag bundle for server commit
`c1489c18b72c4a98d8bfb46a3926fe8195b3e2e6`, tag/version `2.0.0-dev.77`, whose
release ZIP SHA-256 is
`75f78b4d3ef57d2b03c201906a6be2826dee4bc7f308e60d00ef5fff59163044`. That runtime
artifact was not rebuilt from the catalog-only correction. It contains the same
ProcedureCatalog behavior; the JSON correction and parity assertions are source
contract evidence, not a claim about a newly built runtime artifact.

## Stage 1 evidence boundary

Stage 1 establishes the contract, package, manifest, protected storage primitives,
runner and installed smoke. It does not implement the journal state machine,
replay transitions, durable outcomes, retention cleanup or recovery enforcement;
those belong to Stage 2. Stage statuses and acceptance results belong in the
project implementation tracker, not this contract document.
