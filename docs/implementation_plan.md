# Deposit Python 2 implementation plan

Stage progress is recorded in
[`implementation_status.md`](implementation_status.md).

## Authority, scope and starting point

This is the implementation master plan for the six stages in the
[implementation proposal](implementation_proposal.md). The selected progress
record is [implementation_status.md](implementation_status.md). Stage order is
**1, 2, 3, 4, 5, 6**; each stage requires all earlier stages to be finalized.
Creating these documents authorizes no product implementation or stage execution.

The proposal supplies the agreed Python architecture and delivery scope. The
[Deposit project entry](../../../../agents-hub/projects/deposit/overview.md) routes
to the knowledge hub. Shared public contracts remain owned by that hub:

- [Python specification](../../deposit-hub/deposit-python.md).
- [Calling, sessions and prepared recovery](../../deposit-hub/deposit-server-api.md).
- [Server model, values and ownership](../../deposit-hub/deposit-server.md).
- [Patterns, matching and exchange](../../deposit-hub/deposit-server-pattern.md),
  [versioning](../../deposit-hub/deposit-server-versioning.md),
  [formats](../../deposit-hub/deposit-server-formats.md), and
  [browser downloads](../../deposit-hub/deposit-server-downloads.md).
- [Verification and harness ownership](../../deposit-hub/deposit-server-verification.md),
  the [server README](../../deposit-server/README.md),
  [procedure reference](../../deposit-server/doc/procedures.md), and
  [public catalog](../../deposit-server/runtime/public-api.json).

Source and executed tests establish actual behavior. The current hub Python
specification predates the proposal's Client/Store split, direct procedure-based
names and entirely internal recovery keys. Stage 1 must reconcile those concrete
differences, retaining the proposal's choices while preserving server contracts.
A contradiction requiring a change to agreed scope, compatibility or semantics
needs architect advice and a user decision; neither this plan nor catalog
generation silently resolves it. Update shared specifications only for this
wrapper's agreed interface and contract clarifications, not unrelated server status.

Inspected baseline: branch `main`, HEAD
`3c61d21822d71182416b8f8f5cca7e2a7da295ba`. The repository contains packaging,
`src/deposit/__init__.py` and `_meta.py`; no operational wrapper, tests or Python
acceptance runner exists. `_version.py` is generated/ignored. Packaging currently
declares Python >=3.10,<4, no runtime dependencies, and overlapping setuptools
configuration. These are starting facts, not a tested support promise. The server
catalog currently contains 123 entries; discovery `deposit.get.info` is additional.
Recompute this inventory against the selected server snapshot in Stage 1.

## Fixed design and implementation boundaries

1. One synchronous `deposit` 2.x distribution/import in repository `deposit-python`.
   Keep Neo4j driver and protected-storage dependencies explicit; NetworkX is optional.
   Initial integration acceptance is Windows. Do not imply non-Windows recovery
   support or compatibility with Deposit 1; applications needing both use separate
   environments. Reject unsupported combinations before API-dependent work.
2. Server owns authorization, validation, inference, matching, history, layout,
   geometry processing, format conversion and persistence. The wrapper owns transport,
   Python values, sessions, snapshots, batching, streams and restart recovery.
3. Public methods and handles depend on a shared gateway/coordinator, which depends
   on the driver. No feature module has its own retry/session/recovery mechanism.
4. Client owns discovery/authentication and pre-Namespace operations. Each Store
   owns an independent Deposit session and immutable Namespace UUID. Shared
   caller-owned drivers remain open when a Store closes. Namespace rename preserves
   identity; switching requires another Store. Serialize ordinary operations per
   Store, while cancellation uses a control path that can reach an active request.
5. Procedure verbs are preserved; strip `deposit.` and convert dots/camelCase to
   snake_case. Creation is distinct from setting. Classify lifecycle-only operations
   explicitly instead of exposing methods that can retarget or corrupt a Store.
6. Ordinary methods detach values/records or return stable-ID handles. Advanced
   calls expose actual named columns in closable results; native values remain
   inside their transaction lifetime. Both paths enforce the same safeguards.
7. Public portable conversion and declaration reads remain in the owning checked
   transaction and reapply server scope/authorization checks. Native Node and
   Relationship conversion uses a parameterized element-ID match; Paths are not
   convertible. Neo4j read-committed isolation does not promise a frozen snapshot
   across concurrent commits. Never decode native private properties. Polling/reads
   do not advance existing edit baselines. Conflicts retain user inputs and require
   explicit refresh/reapply.
8. Every action requiring preparation is classified from explicit server metadata,
   including operational exports/analysis. Persist immutable exact arguments and
   identity bindings before preparation. Persist resolved portable results before
   release. No driver-managed replay of an unprepared action. Cancellation and
   lost replies do not establish rollback.
9. Protect durable inputs, credentials and unfinished upload bytes with per-user
   permissions and OS-backed secret protection. Recovery is actor/server/database/
   Namespace bound, versioned, crash safe and exclusive across processes. Never
   silently discard unresolved records, retarget work or log secrets.
   Protection covers saved outcomes, sidecars, temporary copies and diagnostic/
   crash artifacts as well as primary journal/spool files; no unprotected fallback.
   `action.release` detaches a session preparation; it does not delete the server's
   durable preparation. Local cleanup/retention must not claim server deletion.
10. A batch is one prepared `deposit.apply.batch`, using earlier temporary-result
    references only in allowed identity positions. No nested batches, reads,
    excluded lifecycle/transfer/history/import operations or automatic splitting.
11. Fixed export/import input includes schema, Patterns and immutable resources
    from a consistent source revision. Source IDs are provenance/run-local
    references, not receiving identity. Import analysis and application remain server
    operations; groups are explanatory and cannot be selectively applied.
    A composed transfer/import helper reports recoverable phases, not global
    atomicity beyond each documented server action or batch.

## Mandatory test and temporary file location

The **single permitted root for temporary and test-related files** in this
project run is **`../test_harness`**, relative to the `deposit-python` repository
root. This is the literal spelling requested for this revision.
If existing harness tooling or an external project-run contract cannot use this
root, record a blocker and obtain the appropriate scope decision before running
noncompliant tooling.

All **new or modified test-related files** and **all files generated temporarily**
by this implementation run must be created, maintained and retained only under
`../test_harness` or its subdirectories. This includes wrapper test sources,
test configurations, test-only scripts and lifecycle adapters, fixtures, probes,
recorded baselines, test documentation, disposable runtime/database state,
virtual environments, installed test packages, test credentials, recovery
journals and spools, upload/download copies, build and package staging, caches,
lock files, coverage and result files, logs, diagnostics, crash dumps, and
project-run local checkpoints and evidence. Use distinct owned subdirectories
for source-controlled tests and disposable outputs; cleanup must never delete
retained tests or evidence.

In particular, no `tests/`, `.pytest_cache/`, `__pycache__/`, `.coverage`,
`build/`, `dist/`, `.tox/`, test logs, test databases or generated test support
files may be created in `deposit-python`, `deposit-server`, `deposit-hub` or
another location. Configure Python, packaging, test runners, PowerShell,
server build tools, system temp locations, and child processes to use harness
subdirectories (`TMP`, `TEMP`, `TMPDIR`, `PYTHONPYCACHEPREFIX`, test/build/cache
and report paths as applicable). Fail closed if a tool cannot redirect its
writes. A stage is not complete until an output-location audit verifies that
test-related and temporary files have not escaped the permitted root.

Production source, durable public specifications, the implementation plan,
status tracker and version-controlled non-test documentation remain in their
owning repositories: they are not test/temporary files. Existing authoritative
server test scripts and suites may be **read or invoked in place** as external
inputs, but this project run must not add or modify test-specific files there,
or permit them to generate files outside `../test_harness`. Harness-side
adapters and an owner-approved invocation path must satisfy server lifecycle
requirements without creating a second launcher. New/modified test source in
`../test_harness` needs durable version-control and review ownership; establish
that ownership in Stage 1 without creating an unapproved repository.

## Module and evidence ownership

Suggested internal file boundaries below are implementation targets, not existing
APIs. Stage planning may refine filenames without changing these responsibilities.

| Owner/location | Planned responsibility |
| --- | --- |
| `src/deposit/client.py`, `store.py`, `errors.py` | Public lifecycle, convenience calls, safe failures |
| `src/deposit/_gateway/` | Reviewed manifest, binding, routing, transactions, named results |
| `src/deposit/_recovery/` | Prepared actions, journal, protected storage, reauthentication, contention |
| `src/deposit/values.py`, `_snapshots.py`, `entities.py` | Lossless values, checked snapshots, stable handles |
| `src/deposit/_batch.py` | Collection, allowed references, one-action submission/results |
| `src/deposit/_transfers.py`, `_workflows/` | Spooling/readers and server workflow composition |
| `src/deposit/networkx.py` | Optional graph adapter; no dependency from core imports |
| `../test_harness/deposit-python/tests/{unit,contract,integration,recovery,installed}/` | All wrapper test sources, assertions and independent observations |
| `../test_harness/deposit-python/{scripts,fixtures,config}/` | Wrapper test runner, acceptance map/ledger tooling, test adapters, synthetic fixtures and test configuration |
| `../test_harness/deposit-python/{env,build,cache,runs,evidence}/` | Test environments, installed-package staging, temporary files, diagnostics, results, journals/spools, checkpoints and retained evidence |
| Existing sibling `deposit-server/scripts/` | Invoke authoritative server tools read-only; redirect every generated file to the permitted harness root; test-only integration adapters belong under `../test_harness` |
| Sibling `../test_harness` | Exclusive root for every new/modified test-related file and every temporary file; use existing lifecycle ownership, not a second test repository or server launcher |
| Sibling `deposit-hub` | Shared Python signatures and cross-component contract documentation |

Do not add a separate server launcher or bypass existing server lifecycle
ownership. Prefer a client transport barrier for reply-loss tests; any server
diagnostic hook must remain test-only, reside under `../test_harness`, and be
installed/restored by the existing probe owner. Do not modify or create
server-repository test files to implement the hook. If the existing owner cannot
support a harness-side hook with redirected output, resolve that conflict before
executing tests. Changes to server behavior are not presumed necessary:
investigate discrepancies and obtain a scope decision before expanding into
product server fixes.

## Verification infrastructure and common completion gate

Stage 1 must implement and document a stable wrapper runner, proposed as
`../test_harness/deposit-python/scripts/Invoke-PythonTests.ps1`, with explicit
`-Mode Focused` and `-Mode StageGate`, `-Stage 1..6`, `-ListOnly`, `-Root`, and
selection/group arguments. These are **planned commands**, not commands currently
available. Its listings show exact tests and required runtime/probe/elevation
conditions. It delegates server lifecycle to the existing owner through a
harness-side hook added in Stage 1, runs the harness Python environment,
propagates every failure/timeout, retains stdout/stderr and machine-readable test
results exclusively under `../test_harness`, redacts secrets, and restores owned
resources. Installed tests run outside the source tree from a harness-contained
environment and must identify the installed artifact.

**Mandatory two-level testing cadence:**

1. **After each completed feature, run only focused integration tests.** Select
   the smallest real-server Python integration scenario(s) that directly exercise
   the newly implemented feature and its essential success/failure behavior.
   Use the runner's focused mode with explicit test identifiers; do not run the
   complete integration group, cumulative regression, broad unit/contract suites,
   a whole-stage selection, or the full compatibility matrix at this checkpoint.
   Write or update the focused test first if none exists. Record its exact command,
   expected effects, result and evidence. If it fails, correct the feature and
   repeat only the affected focused integration tests until they pass.
2. **Only after all work for a stage is implemented, run its full stage-end suite.**
   Use `-Mode StageGate -Stage N` exactly at the end-of-stage gate, not after
   individual features. The stage-end suite is cumulative: it includes all
   wrapper unit, contract, integration, recovery and installed-package tests
   applicable through Stage N, complete applicable acceptance for Stage N,
   and the relevant server/harness regression and toolchain checks. Future-stage
   tests not yet implemented are not required. Stage 6 additionally runs the
   entire release acceptance and advertised support matrix. Do not claim a
   stage has passed based on focused runs alone.
3. If the independent architect review or end-of-stage suite requires changes,
   run focused integration tests for the corrected feature(s), then rerun the
   **full stage-end suite as the final gate**. Do not run full suites at interim
   implementation checkpoints. `-ListOnly` and static inspection do not execute
   tests and may be used for planning.

The runner must distinguish these two modes unambiguously, reject a feature-level
request for a full-stage selection, and attach a stage/run/feature identity to
evidence. Exact paths for all logs, temporary files, test sources and outputs
must remain within `../test_harness`.

The runner's groups and required acceptance are:

| Group | Introduced | Required content |
| --- | --- | --- |
| foundation | 1 | Build/install, manifest and discovery, protected-storage primitives, live lifecycle smoke |
| core | 2 | Store lifecycle, creation/read/conflicts, journal and actual client-process termination |
| gateway | 3 | All-entry dispatch/routing/classification, real named outputs and raw lifetimes |
| domain | 4 | Typed values, CRUD/query/paging, snapshots and atomic batches |
| workflows | 5 | Resource/geometry/Pattern/import/format/history/admin scenarios and interruptions |
| release | 6 | NetworkX, all-entry acceptance ledger, complete regression and installed support matrix |

Stage 1 must turn every acceptance ID below into an assertion-to-test map,
marking the focused feature checkpoint(s) and the cumulative stage-end acceptance
that will prove it. Later stages extend that map before implementation,
identifying error branches as well as success. Missing tests, skip-only results
and Java-only evidence are not passes. Fixtures use disposable Namespaces/accounts
and independent sessions or direct public procedures to verify effects; they
must not derive expected values solely through the same wrapper decoder being
tested. Test sources and fixtures live under `../test_harness`; test secrets stay
in protected harness storage, not command lines or tracked fixtures.

Existing authoritative server entry points, run from `deposit-server`, include
the following *illustrative stage-end selections*. The harness-side orchestrator
must set the exclusive writable root for **every** entry point, including commands
that do not expose a `-Root` parameter. Do not invoke any example before verifying
that its build outputs, scratch state, child processes and logs are redirected to
`../test_harness`:

```powershell
.\scripts\Invoke-IntegrationSuite.ps1 -ListOnly -Root ..\test_harness
.\scripts\Invoke-IntegrationSuite.ps1 -SetupOnly -Root ..\test_harness
.\scripts\Invoke-Tests.ps1 -Kind Unit -Test 'DepositInfoTest'
.\scripts\Invoke-Tests.ps1 -Kind Integration -Test 'PreparedActionReplayIT,AccountAuthorizationIT'
.\scripts\Invoke-TestScenario.ps1 -SeedTest 'ActionReplayRecoveryIT#seed' -VerifyTest 'ActionReplayRecoveryIT#verify' -Transition Restart
.\scripts\Invoke-TestScenario.ps1 -SeedTest 'ActionReplayRecoveryIT#seed' -VerifyTest 'ActionReplayRecoveryIT#verify' -Transition KillRestart
```

These server selections are used through the permitted runner at the **end of a
stage** as applicable, not as full-suite invocations after individual features.

Reinspect current parameters, artifact/probe requirements, writable-output
redirection and coupled seed/verify fixtures before execution. These Java
selections support harness/recovery changes; they do not replace Python-process
scenarios. Service/crash/probe scenarios need their documented owner and elevation.
Never infer a successful scenario before its post-transition verifier finishes.
Never run unit selection `-Test '*'`. At stage-end, invoke the verified full
selection through the harness runner rather than a broad unsafe wildcard.

During an authorized project-run, every stage completes only after the following
gates. Standalone project-stage/project-task use retains its requested phase and
separate documentation/commit authorization rules; it does not itself authorize
this execution sequence.

1. A fresh stage worker performs project-stage read-only analysis and ordered
   planning; root arranges architect advice and resolves material questions.
2. Implementation follows a separate dispatch. Preflight compares public/durable
   contracts and maps each acceptance ID to concrete assertions. After **each
   implemented feature**, execute and record **only its focused integration tests**;
   correct and repeat only those focused tests as needed.
3. **At the end of the stage**, after all features and focused checks are complete,
   run the **full cumulative wrapper stage selection** and all applicable
   server/harness regression and lifecycle/journal/packaging checks from the
   verification guide. This is the only routine full-suite execution point in
   the stage. Parse changed PowerShell and run `git diff --check` in each changed
   repo. Audit all generated/temp/test-file paths against `../test_harness`.
4. Independent post-implementation architect review examines the actual source,
   tests, diff and evidence with no unresolved blocking findings; correct and
   reverify findings. After any corrective change, rerun its focused integration
   tests and then the full cumulative stage-end suite; earlier design advice does
   not satisfy this review.
5. Root updates the selected tracker with actual focused-test evidence,
   stage-end full-suite evidence, output-location audit, deviations, cleanup,
   review outcome and outstanding items, then completes separate stage commits
   in every changed repository. No push. Missing evidence cannot be waived as a pass.

## Stage 1 - Contracts, package foundation and live acceptance infrastructure

**Dependency:** none. **Outcome:** an installable, version-gated foundation and
working real-server acceptance path before domain implementation.

Ordered work:

1. Capture all affected repository baselines and inspect nested instructions.
   Reconcile each proposal/hub Python difference in a signature table: public name,
   arguments/defaults, return/lifetime, owning Client/Store/coordinator, errors and
   target acceptance stage. Remove external recovery-key responsibility from normal
   signatures. Preserve lifecycle controls without an unsafe generic-call bypass.
2. Select an explicit bounded Python/driver/Windows matrix and exact server
   build identity using installed/runtime discovery and local build metadata.
   Record tested combinations separately from targets; select development/test
   dependency pins and 2.x package version rules. Do not retain broad metadata
   claims without planned acceptance for their whole advertised range.
3. Reconcile setuptools configuration, generated version behavior and package
   discovery. Add core dependency, test/build extras and optional `networkx` extra.
   Establish public exports, errors and internal module boundaries. Verify wheel
   and sdist content without relying on an ignored source-only `_version.py`.
   Generate build intermediates, wheel/sdist staging and package caches only inside
   `../test_harness`.
4. Create a reviewed manifest from the catalog plus discovery and registration
   declarations. Include procedure name, ordered arguments/defaults, bindings,
   named outputs, scope, session/token requirements, action/recovery classification,
   batch eligibility/exclusions and public invocation owner. Record source identity
   and explicit overrides; fail generation/parity on unknown metadata or collisions.
   Do not infer recoverability from procedure verbs or Neo4j READ/WRITE alone.
5. Define protected-storage and journal format/version contracts, identity proof,
   serialization fidelity, atomic flush/update, locking and retention rules.
   Resolve deployment identity before enabling replay: current `get.info` returns
   only `version`, and session diagnostics do not expose a durable server/database
   UUID. Investigate supported native metadata or an explicitly approved identity
   mechanism; a connection URI alone is insufficient. Define fail-closed behavior
   for database replacement/endpoint reuse and tests proving it. If this requires
   new public server fields, obtain a user decision after architect consultation;
   do not silently expand the server contract. Continue unaffected foundation work.
   Select concrete Windows OS-backed protection/ACL mechanisms and safe unavailable-
   backend behavior. Implement and test storage/lock primitives here; full action
   state machine is Stage 2. Pending records are not age-pruned; resolved cleanup
   waits for durable result retention and confirmed release. Set finite defaults
   without deleting unresolved work; disk exhaustion must fail before sending.
6. Implement the minimal discovery/authentication/session smoke path, safe errors
   and compatibility gate. Provision the Python environment **under
   `../test_harness`** and provide the acceptance hook **under that same root**
   through existing server lifecycle ownership. Add explicit credential/endpoint
   handoff, disposable fixture cleanup, timeouts, evidence and installed-package
   execution. Extend harness-side helper-copy/package lists where hooks require
   distribution; do not create or modify test-specific files elsewhere.
7. Create the acceptance map and procedure coverage ledger with independent fields
   for dispatch, Python convenience behavior and executed integration evidence.
   Store their test-owned machine-readable working copies under `../test_harness`.
   Reconcile only relevant shared KB signatures, with architect consistency review.

Acceptance:

- **1A:** Isolated wheel and sdist installs import `deposit` with a 2.x identity,
  no source-tree fallback, and no NetworkX requirement for core use.
- **1B:** Catalog plus discovery coverage is exact; arguments, columns, action
  metadata, lifecycle routes and exclusions are checked against server declarations.
  Unsupported server/version and unknown metadata fail before dependent calls.
- **1C:** Real server discovery, valid/invalid authentication, session inspection
  and logout pass through the installed package; no success is fabricated for an
  empty result or driver error. Bootstrap smoke uses a fresh harness-owned runtime
  only, never resets an existing installation.
- **1D:** Protected storage round trips sensitive inputs without plaintext leakage;
  wrong-user/unavailable backend and competing process tests fail safely. Atomic
  update/lock release and disk/write failure tests establish Stage 2 prerequisites.
  The deployment identity mechanism and replacement/endpoint-reuse policy are
  resolved and evidenced; Stage 2 replay cannot proceed with this question open.
- **1E:** Python test failures/timeouts reach the harness exit code, evidence is
  retained and owned runtime/children/fixtures are restored. Run changed harness
  journal/child-exit/packaging checks and an actual Python hook smoke scenario.
  All tests, fixtures, test-only scripts, build products, caches, runtime state,
  logs and other temporary outputs reside exclusively under `../test_harness`.
  Demonstrate that focused mode executes only its selected integration scenarios,
  while stage-gate mode executes the complete cumulative Stage 1 suite; the
  output-location audit detects and fails any escaped writes.

**Feature checkpoints:** After each Stage 1 feature, run only its selected focused
real-server integration tests; no full foundation/regression selection yet.

**End-of-stage gate:** runner `-Mode StageGate -Stage 1` executes the **full**
Stage 1 foundation/acceptance suite, including build/install and parity groups,
relevant existing server discovery and modified harness regression, and the
exclusive-output audit. Stage 1 records exact resolved tool versions, command
selections, focused-test results and retained stage-end logs in the tracker.

## Stage 2 - Safe end-to-end core and durable prepared recovery

**Dependency:** Stage 1. **Outcome:** first public create/read workflow is safe
across uncertainty, conflicts, reconnect and application restart.

Ordered work:

1. Implement Client and Store construction/context management, independent login
   per Store, UUID selection and identity checks, driver ownership and cleanup.
   Store operations serialize; control cancellation and close cannot deadlock
   behind a running operation. Close resolves/retains uncertain work before logout.
2. Implement journal transitions: durable intent -> prepare/attach -> execute or
   status/resume -> durable portable outcome -> release -> retained result/cleanup.
   Persist exact normalized arguments (including original selector arguments),
   actor/server/database/Namespace identities, client-observed snapshot provenance
   and format version. The server preparation owns and enforces its captured
   immutable selector bindings and preconditions; public status does not expose
   these for client reconstruction. Resume them by the retained key. Handle lost replies at
   every boundary, interrupted local writes, corrupt/unsupported journal versions,
   lock-owner death and release failure without blind replay or lost results.
   If a local intent survives reconnect but no server preparation exists, never
   create a fresh key under a new baseline. Preserve the intent and require explicit
   refresh/reapply unless original preconditions are proven unchanged. The direct
   changed-precondition case is covered by
   `test_missing_preparation_with_changed_revision_is_never_recreated`.
3. Implement protected reauthentication and a public way to inspect pending work
   and retrieve recovered results without exposing recovery keys as application
   responsibility. Implement the approved Client-owned API:
   `Client.pending_operations(*, recovery_directory=None) -> tuple[PendingOperation, ...]`,
   `Client.recover(operation, *, username, password) -> RecoveredOperation`, and
   `Client.recovered_operations(*, recovery_directory=None) -> tuple[RecoveredOperation, ...]`.
   References are immutable and expose no recovery key, exact arguments or credentials.
   Reauthentication uses fresh credentials through `open_store` or `Client.recover`;
   do not add `Store.reauthenticate`. Unavailable credentials/permissions preserve
   work and return actionable safe conditions. Credential rotation does not change
   original actor. `Client.recover` must use the original durable receipt to retrieve
   a completed Namespace deletion without selecting the deleted Namespace.
4. Add minimal portable Class/Object reads and create methods through that core.
   Capture declaration/value snapshots together; implement explicit refresh and
   conflict preservation without completing the full Stage 4 handle surface.
5. Extend harness barriers for before/after preparation, commit, local result
   persistence and release. Actually terminate/relaunch Python with the same
   protected journal. Verify effect counts/UUIDs through an independent session.
   Add real server restart/kill-restart tests under existing runtime owners;
   all barrier configurations, disposable processes, journals and logs reside
   under `../test_harness`.

Acceptance:

- **2A:** Create Class/Object then read via another session; multiple Stores sharing
  a driver have separate tokens/baselines. Store close leaves caller driver usable;
  internally owned drivers/readers close. Namespace rename retains UUID binding;
  attempted selection/token/scope substitution is rejected.
- **2B:** Lost prepare/execute/status/release replies and real client-process death
  yield one effect and the original portable outcome. Resolved results survive
  release and subsequent restart; resumption never recomputes changed inputs.
- **2C:** Independent Store conflicts preserve local edits and pending arguments.
  Polling does not advance a baseline; explicit refresh/reapply is required.
  A recovered UUID/result remains retrievable after later entity change/deletion.
- **2D:** Changed permissions, disabled account, password-change session revocation,
  unavailable credentials and wrong server/database/actor/Namespace prevent unsafe
  continuation and preserve pending work. Reauthentication uses fresh credentials
  with `Client.recover` or by reopening through `Client.open_store`; no Store-level
  reauthentication method is exposed. `Client.recover` retrieves a completed
  Namespace deletion through its original receipt without selecting that Namespace.
  Prior-epoch server actions follow the public recovery contract rather than retargeting.
  An uncommitted server-scope preparation from an earlier server epoch preserves
  pending work on REVISION_CONFLICT and requires explicit refresh/reapply; never
  automatically generate a new key or reinterpret it as a replayable action.
- **2E:** Two processes cannot resume the same journal action concurrently. Abrupt
  death, corruption and storage failure produce safe recoverable states. Cancellation
  near commit resolves the actual outcome; neither driver retries nor logout loses it.
- **2F:** Actual server graceful/forced restart invalidates sessions and recovers
  committed/uncommitted cases with original preconditions and independently checked
  outcomes. Record both Python termination and server-transition evidence.

**Feature checkpoints:** After each Stage 2 feature, run only the directly
relevant focused integration tests (for example create/read, conflict, or
interrupted prepared action), not the full core/recovery regression.

**End-of-stage gate:** runner `-Mode StageGate -Stage 2` executes the **full
cumulative** foundation/core unit, contract, integration, state-transition and
live restart acceptance suite, plus supporting selected `PreparedActionReplayIT`,
`AccountAuthorizationIT` and action recovery seed/transition/verifier when the
harness boundary changes. All test outputs remain under `../test_harness`.

## Stage 3 - Complete procedure gateway and controlled advanced access

**Dependency:** Stage 2. **Outcome:** every non-deferred public server procedure has
a supported route, with correct binding, scope, output lifetime and recovery
classification. The explicit deferrals below remain pending and are not counted as
implemented routes.

**Approved scope decisions for this run:** durable recovery for Client-owned
`action=true` server-scope mutations is deferred until a separate server-scope
identity design is approved. This covers `deposit.create.user`,
`deposit.create.namespace`, `deposit.set.user`, `deposit.set.password`,
`deposit.delete.namespace` and `deposit.import.namespaceData`; do not use a fake or
anchor Namespace to route them. For `deposit.admin.pruneResources`, allow a direct
call only when `dryRun` is explicitly true. Defer execution for both Namespace and
server-wide scopes. Preserve read-only `pruneResourcesStatus` support.
For Client reads, require explicit selectors on singular `get_user` and
`get_namespace`; add `get_users` and `get_namespaces`, which always return tuples
for zero, one or many selected rows.

Ordered work:

1. Complete manifest-driven positional/default binding and safe parameterization.
   Allow only reviewed procedure names; validate local handle/Namespace provenance.
   Route bootstrap/login/discovery, server-scope calls, Store calls and controls
   explicitly. Reject Store retargeting and arbitrary transaction/session injection.
2. Complete action handling for all classified families, including staging,
   exports, analysis and pruning distinctions. Action controls cannot recursively
   prepare themselves or detach another pending action; manual advanced controls
   must cooperate with coordinator state. Do not require every public procedure
   to appear as an unrestricted `Store.call` method to claim coverage.
   Include argument-dependent classification: prune dry runs cannot be prepared;
   executing prunes are rejected before network access under the approved scope
   decision. Release detaches rather than deletes durable server
   preparation records. Selection and session/action controls use separate transactions.
3. Implement named-column closable advanced results, native lifetime checks,
   portable conversion in the owning transaction and deterministic exhaustion/
   exception/early-close cleanup. Retain multi-column query and `canUndo` shapes,
   empty rows/pages and actual column names; never invent generic metadata columns.
   Convert only owned native Nodes/Relationships through parameterized `elementId`
   matches on the same transaction; reject Paths and preserve server authorization.
4. Complete safe server error mapping with diagnostic request IDs, unknown driver
   errors and bounded diagnostics. Implement authorized request listing/cancellation
   through the parallel control path and explicit administrative-mode lifecycle.
5. Populate dispatch tests for every manifest entry and representative installed
   live execution per family. Place all tests, fixtures, installed artifacts and
   output files under `../test_harness`. Mark specialized end-to-end coverage
   still pending for Stages 4-6 rather than equating dispatch parity with acceptance.

Acceptance:

- **3A:** Every catalog entry plus discovery has exact binding, owner, named output,
  scope, recovery and batch policy tests; additions/removals fail parity checks.
  Assert prune dry-run and both deferred execution branches, plus operational
  analysis/export actions.
- **3B:** Real family representatives verify token-free discovery, pre-binding and
  scoped routing, parameter order, empty results, safe errors and permission denial.
  Both convenience and advanced calls use the same protection and journal paths.
- **3C:** Advanced results preserve real columns and native lifetime; Node and
  dynamically typed Relationship conversion, Path rejection, early close,
  conversion failure, iteration failure and Store close release transactions and
  readers. Conversion uses the owning transaction and server authorization; no
  native result is usable outside its valid lifetime.
- **3D:** Cancellation reaches an active long-running request while the Store is
  busy, respects actor permissions and resolves ambiguous action completion.
  Administrative mode is explicit and isolated per session.
- **3E:** Coverage ledger lists each specialized unverified scenario, its owning
  later stage and test target. Every non-deferred procedure has a supported path;
  the approved deferred action procedures remain explicitly listed as pending.

**Feature checkpoints:** After each Stage 3 feature or procedure family,
execute only its selected focused real-server integration tests; do not rerun
all-entry parity or cumulative regressions at feature checkpoints.

**End-of-stage gate:** runner `-Mode StageGate -Stage 3` executes the **full
cumulative** suite including all-entry parity/binding tests and installed live
family representatives. Include relevant server authorization/cancellation/replay
regression for any shared hook or behavior touched, with all outputs under
`../test_harness`.

## Stage 4 - Typed domain API, snapshots, queries and atomic batches

**Dependency:** Stage 3. **Outcome:** scientific domain editing and reads use
convenient Python interfaces without losing values, scope or conflict safety.

Ordered work:

1. Complete `typed`, DType/DDict/DGeometry/DResource representations and codecs.
   Distinguish descriptor dictionaries from argument maps, missing from unknown,
   and declared payloads from explicit typed input. Use lossless public driver
   temporal/spatial types where Python built-ins cannot retain precision.
2. Complete stable-ID entity handles and procedure-named CRUD for declarations,
   classes, objects, memberships, relationships and User Tools. Route indexing
   conveniences to the same methods, with detached mutable values requiring
   explicit reassignment. Add snapshot/schema invalidation and explicit refresh.
3. Implement query/paged iterators, presence flags, bounded materialization and
   revision-bound cursors. Couple declaration/value reads within checked snapshots;
   changed revision, scope or expired cursors fail rather than mixing results.
4. Implement batch collection and temporary references, eligibility and size checks,
   submit-on-success, discard-on-exception and result availability only after a
   resolved successful action. Persist exact commands/results for restart recovery.
   Atomic order swaps/bulk helpers must use this same batch core.

Acceptance:

- **4A:** Boundary integers, finite floats, NaN/infinity rejection, nanoseconds,
  timezone/offset values, negative/calendar durations, Point SRIDs, nested typed
  lists/dictionaries, empty/all-null lists and declared/undeclared null round trip.
  Unsupported/lossy values fail before mutation; inspect independent wire outcomes.
- **4B:** Missing descriptor raises KeyError; `get` defaults only when missing;
  explicit unknown remains None. Detached mutations have no server effect until
  assignment. Declaration changes/conflicts never decode with unrelated live schema.
- **4C:** CRUD create/set distinctions and same-name/UUID selectors match server
  semantics; foreign-Store handles are rejected. Stale edits preserve intended
  values and explicit refresh invalidates data and schema together.
- **4D:** Query columns/presence, empty pages, multiple pages and cursor expiry/
  revision/scope rejection pass. Early close/cancellation has no leaked reader.
- **4E:** Temporary earlier-result references resolve exactly; invalid forward/
  foreign references, reads, nested batches and excluded operations fail clearly.
  Collection exceptions submit nothing; a late server failure rolls back all
  commands; oversized batches never split; lost-reply/restart replays one batch.

**Feature checkpoints:** After each Stage 4 feature, execute only the focused
integration scenarios for the changed codec, CRUD operation, query, snapshot or
batch behavior; reserve exhaustive unit and cross-domain regression for stage end.

**End-of-stage gate:** runner `-Mode StageGate -Stage 4` executes the **full
cumulative** suite including exhaustive value/snapshot/batch unit and contract
tests plus installed domain workflows. Supporting server selections when affected:
`ValueCodecTest`, `StrictValuesTest`, `TypedDataModelIT`, `BatchMutationIT`,
`QueryLanguageIT`. All fixtures and generated outputs reside under
`../test_harness`.

## Stage 5 - Resources and extended server workflows

**Dependency:** Stage 4. **Outcome:** all non-NetworkX feature families have tested
Python workflows, including their continuation, authorization and failure paths.

Ordered work:

1. Implement upload/download readers and spool lifecycle. Freeze upload bytes before
   preparation when source identity cannot be guaranteed; protect restart-needed
   bytes, checksum/length and offsets. Respect server leases/continuations and caller
   stream ownership. Keep browser authentication separate from Store sessions.
2. Add geometry conversion/access and Pattern CRUD/search/form helpers through
   server procedures. Preserve WKT/CRS/geometry identity and Pattern definitions;
   no client matching or geometry engine.
3. Implement fixed-source graph/package/table staging, inspect/analyze/apply helpers
   and complete receiving/source identity rules. Capture required schema, Patterns
   and resource bytes before analysis. Keep plan revisions separate from session
   refresh. Support server-side namespace clone without changing an existing Store.
4. Complete CSV/XLSX/GeoPackage load/table iteration/save/download composition, layout,
   history/undo/redo/restore, version tags/modes and account/Namespace administration.
   Explicitly route server-scoped and destructive operations; retain confirmations,
   authorization and observations from the public contract.
5. Add per-family installed workflows and failure/recovery cases under
   `../test_harness`. Reuse existing server crash lifecycle owners for actual
   restart/diagnostic probes, redirecting their output to the same root; verify
   wrapper continuation after transitions. Complete relevant Python usage and
   shared signature documentation.

Acceptance:

- **5A:** Local files and one-pass binary streams upload exact bytes; interruption
  resumes after real client/server restart at valid offsets with matching checksum.
  Changed original files cannot alter replay; missing/corrupt spool fails safely
  and retains pending work. Wrapper-opened files close; caller streams remain owned
  by caller. Download early-close, permission loss and corruption report failure.
- **5B:** Geometry types/CRS/Z and Pattern create/search/form workflows preserve
  public data and server diagnostics. Layout results come from the server.
- **5C:** Analyze/apply uses a complete immutable snapshot. Caller/source edits after
  staging do not change plans; target revision/schema/Pattern changes yield stale
  plans. Ambiguity creates no executable plan. A late invalid row/failure rolls
  back the entire import; replay yields original mappings without duplicate objects.
  Package/subtree import and full-Namespace clone retain their distinct identity,
  history and selection behavior, including history-only resource references.
- **5D:** Upload -> load -> table stream/page -> analyze -> apply -> save -> download
  -> independently inspect/reload passes for CSV, XLSX and GeoPackage. Verify exact
  numeric/temporal/JSON values, blank-cell semantics, diagnostics, empty/limit cases,
  CRS/mixed geometry and output member names per the format contract.
- **5E:** Exported resource URLs contain no Store token. Browser links use independent
  authentication; exercise first login, repeat download and denial through the
  existing browser harness. Unavailable listener fails export without publication.
- **5F:** History modes/epochs, undo/redo/restore conflicts and version tags work
  through Python; access revocation, password/session invalidation, Namespace
  rename/deletion, administrative mode and server-scope recovery obey the contract.
  Interrupted analysis/export/staging preserves operational IDs and expiry behavior.

Approved execution clarifications for `deposit-python-20261025-01`: retain the
earlier deferral of full-Namespace clone and Client-owned server-scope mutation
recovery until their separate identity/recovery contract is designed. The user
also deferred a companion-content API for resource-bearing `from_dict` inputs;
dictionary/subtree helpers must reject missing resource bytes before upload or
analysis. Complete resource transfer and the Server's verified Exchange-package
route remain in scope. `DResource.is_image` remains unknown without verified
classification metadata. Python's exact supported Server version will be
retargeted to the committed Stage 5 artifact, with equality checks retained and
clean committed-artifact verification required before finalization.

**Feature checkpoints:** After each Stage 5 feature, run only focused
integration workflows exercising the specific resource, geometry, Pattern,
transfer, format, import, history or administration behavior; do not run the
entire workflow suite until stage end.

**End-of-stage gate:** runner `-Mode StageGate -Stage 5` executes the **full
cumulative** suite, including family-specific workflows and actual interrupted
transfer/import scenarios. Relevant server selections come from the verification
guide (resource/geometry, Patterns, formats, history, clone/import, authorization,
browser). Diagnostic tests run with probe artifacts only through their owner;
all such artifacts remain under `../test_harness`. Record each failed test
individually and finish post-transition verification.

## Stage 6 - Optional graph conversion and release acceptance

**Dependency:** Stage 5. **Outcome:** complete promised API with release evidence
for the explicit support matrix, without publishing as part of this plan.

Ordered work:

1. Implement `to_networkx`/`from_networkx` using the fixed staging/exchange path.
   Preserve `(namespace_id, kind, id)` keys, edge IDs/direction/parallel edges,
   membership, declarations, Patterns and revision provenance. Validate endpoints,
   required schema and geometry/resource references. Send matching/import to server.
2. Add graph conversion round trips and negative graph cases. Freeze caller graphs
   before analysis; enforce source consistency and resource completeness. Explain
   that graph conversion is not a full history/resource backup.
3. Close every remaining procedure ledger and convenience-API acceptance gap.
   Provide installed integration evidence per procedure or an explicitly linked
   lifecycle workflow invoking/asserting it. A procedure requiring a fresh runtime
   (for example bootstrap) must run there, not be waived as destructive.
4. Build final wheel/sdist into `../test_harness` release-test staging; test
   isolated clean installs with/without NetworkX on every advertised
   Python/driver/server combination. Test supported minimum and upper bounds or
   narrow the advertised bounded matrix to evidenced combinations with a user
   decision where that changes agreed compatibility.
5. At the **end of Stage 6 only**, run complete cumulative wrapper regression,
   real process/restart/crash recovery, installed lifecycle/resource/browser
   acceptance, relevant full server regression for shared changes, and
   package/version/dependency/license checks. Before that gate, run only focused
   integration tests after each individual release feature. Finalize installation,
   migration-from-1.x, compatibility, recovery/result access and cleanup
   documentation. Record evidence limits rather than claiming new deployment scope.
   Keep all test sources, temporary artifacts, logs and release-test packages
   under `../test_harness`; audit for escaped files.

Acceptance:

- **6A:** Both graph methods give a clear optional-install message when NetworkX is
  absent, while core import/operations remain usable. With the extra, graphs retain
  every promised structural/value/Pattern property and parallel edge; invalid keys,
  kinds, endpoints, declarations and inconsistent memberships fail clearly.
- **6B:** Graph import uses a fixed snapshot and server matching; input edits cannot
  change it. Recovered results retain original mappings. Receiver UUIDs and Store
  identity are not taken from source metadata. Large staged and empty graphs pass.
- **6C:** Ledger has no uncovered public procedure, promised convenience behavior
  or required branch; dispatch, convenience and executed integration coverage remain
  separately visible. No skipped acceptance is counted as verified.
- **6D:** Clean installed artifacts pass the full declared matrix and reject
  unsupported combinations. 2.x identity, sdist build, version generation and
  optional dependencies work outside the checkout.
- **6E:** Full wrapper regression and relevant shared-tooling/server acceptance
  pass with logs, source/artifact identities, independent effect checks and successful
  cleanup/restoration. Documentation matches tested behavior; limitations are explicit.

**Feature checkpoints:** After each Stage 6 feature, run only its focused
installed integration scenarios (for example a graph round trip or invalid-key
case); no complete regression or support-matrix sweep before stage end.

**End-of-stage gate:** runner `-Mode StageGate -Stage 6` executes the **full
cumulative suite (all groups)**, installed support matrix, complete coverage
ledger and required server/harness selections, all using `../test_harness` for
test sources and generated files. Release readiness does not authorize upload
to PyPI, pushes, deployment or changing existing installations.

## Project-run execution and status contract

The authorized project run executes Stages 1 through 6 in the order defined
here. The selected tracker records verified progress and commits; this plan
remains the source for stage scope, dependencies and acceptance criteria.

Select an explicit bounded sequence or next-N request. The plan and tracker are
independent explicit inputs; do not substitute the hub's server status. Cross-repo
work in the harness-side acceptance hook and relevant shared Python
specifications is part of the selected stages, subject to each repository's
instructions. External runtime changes still use existing lifecycle ownership
and applicable runtime permissions; all test-only hooks, writable outputs,
local working files and temporary state must be under `../test_harness`.

Follow the deployed project-run skill for fresh stage workers, read-only planning,
separate implementation dispatch, independent architect review, protected baselines,
tracker updates and stage commits. Use a common run ID and exact stage ID in each
commit; preserve unrelated/index changes. Commit each changed implementation repo
then any separate tracker repo; here plan/tracker share deposit-python. Do not make
empty commits or push.

Store the project-run local checkpoint at
`../test_harness/deposit-python/project-run/state.json` using the deployed
project-run skill's versioned schema; this replaces the previous
`git rev-parse --git-path project-run/state.json` placement, which would create
an outside-root local file. Stage 1 must obtain a compatible, owner-approved
checkpoint redirection if the deployed skill requires its old Git-path location;
do not run with an out-of-root fallback. Store selected file hashes, baselines,
current phase, decisions, focused and stage-end acceptance evidence, reviewed
diff identity and pending/resulting commits under that schema. Reconcile
checkpoint/source/tracker/history before resuming. A verified stage with pending
commits cannot advance. Evidence is reusable only while inputs match. Document
real external blockers and required actions under AGENTS.md; missing tooling/tests
or ordinary failures remain implementation work.

The tracker records stage state plus evidence for every acceptance ID, exact
**focused integration** and **stage-end full-suite** commands/test counts/failures,
log locations, source/artifact identities, file-location audit, review, deviations,
cleanup and commits. Keep machine-specific run paths and generated evidence only
under `../test_harness`; use portable references to that evidence in shared
repository documentation. No plaintext credentials, tokens, sensitive operation
arguments or user datasets in records.
