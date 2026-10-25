# Deposit Python 2 implementation plan

Date: 2026-10-25. State: implementation not started.

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
7. Public portable conversion and declaration reads share a checked snapshot.
   Never decode native private properties. Polling/reads do not advance existing
   edit baselines. Conflicts retain user inputs and require explicit refresh/reapply.
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
| `tests/unit`, `tests/contract`, `tests/integration`, `tests/recovery`, `tests/installed` | Wrapper assertions and independent observations |
| `scripts/`, `tests/fixtures/`, wrapper docs | Contract tooling, wrapper test entry point, small synthetic fixtures, usage |
| Sibling `deposit-server/scripts/` | Minimal Python acceptance hook within existing lifecycle owners |
| Sibling `test_harness` | Generated environments, runtime, credentials, journals/spools for tests and retained evidence; no new repository |
| Sibling `deposit-hub` | Shared Python signatures and cross-component contract documentation |

Do not add a separate server launcher or bypass the existing harness. Prefer a
client transport barrier for reply-loss tests; any server diagnostic hook must
remain test-only and installed/restored by the existing probe owner. Changes to
server behavior are not presumed necessary: investigate discrepancies and obtain
a scope decision before expanding into product server fixes.

## Verification infrastructure and common completion gate

Stage 1 must implement and document a stable wrapper runner, proposed as
`scripts/Invoke-PythonTests.ps1`, with explicit `-Stage 1..6`, `-ListOnly`,
`-Root`, and selection/group arguments. These are **planned commands**, not
commands currently available. Its lists must show exact tests and required
runtime/probe/elevation conditions. It delegates server lifecycle to the existing
owner through a hook added in Stage 1, runs the harness Python environment,
propagates every failure/timeout, retains stdout/stderr and machine-readable test
results, redacts secrets, and restores owned resources. Stage selections are
cumulative: `-Stage N` includes prior wrapper regression plus Stage N acceptance.
Installed tests run outside the source tree and must identify the installed artifact.

The runner's groups and required acceptance are:

| Group | Introduced | Required content |
| --- | --- | --- |
| foundation | 1 | Build/install, manifest and discovery, protected-storage primitives, live lifecycle smoke |
| core | 2 | Store lifecycle, creation/read/conflicts, journal and actual client-process termination |
| gateway | 3 | All-entry dispatch/routing/classification, real named outputs and raw lifetimes |
| domain | 4 | Typed values, CRUD/query/paging, snapshots and atomic batches |
| workflows | 5 | Resource/geometry/Pattern/import/format/history/admin scenarios and interruptions |
| release | 6 | NetworkX, all-entry acceptance ledger, complete regression and installed support matrix |

Stage 1 must turn every acceptance ID below into an assertion-to-test map. Later
stages extend that map before implementation, identifying error branches as well
as success. Missing tests, skip-only results and Java-only evidence are not passes.
Fixtures use disposable Namespaces/accounts and independent sessions or direct
public procedures to verify effects; they must not derive expected values solely
through the same wrapper decoder being tested. Test secrets stay in protected
harness storage, not command lines or tracked fixtures.

Existing authoritative server entry points, run from `deposit-server`, include:

```powershell
.\scripts\Invoke-IntegrationSuite.ps1 -ListOnly -Root ..\test_harness
.\scripts\Invoke-IntegrationSuite.ps1 -SetupOnly -Root ..\test_harness
.\scripts\Invoke-Tests.ps1 -Kind Unit -Test 'DepositInfoTest'
.\scripts\Invoke-Tests.ps1 -Kind Integration -Test 'PreparedActionReplayIT,AccountAuthorizationIT'
.\scripts\Invoke-TestScenario.ps1 -SeedTest 'ActionReplayRecoveryIT#seed' -VerifyTest 'ActionReplayRecoveryIT#verify' -Transition Restart
.\scripts\Invoke-TestScenario.ps1 -SeedTest 'ActionReplayRecoveryIT#seed' -VerifyTest 'ActionReplayRecoveryIT#verify' -Transition KillRestart
```

Reinspect current parameters, artifact/probe requirements and coupled seed/verify
fixtures before execution. These Java selections support harness/recovery changes;
they do not replace Python-process scenarios. Service/crash/probe scenarios need
their documented owner and elevation. Never infer a successful scenario before
its post-transition verifier finishes. Never run unit selection `-Test '*'`.

During an authorized project-run, every stage completes only after the following
gates. Standalone project-stage/project-task use retains its requested phase and
separate documentation/commit authorization rules; it does not itself authorize
this execution sequence.

1. A fresh stage worker performs project-stage read-only analysis and ordered
   planning; root arranges architect advice and resolves material questions.
2. Implementation follows a separate dispatch. Preflight compares public/durable
   contracts and maps each acceptance ID to concrete assertions.
3. The cumulative wrapper stage selection and affected server/harness regression
   pass. Parse changed PowerShell and run applicable lifecycle/journal/packaging
   checks from the verification guide. Run `git diff --check` in each changed repo.
4. Independent post-implementation architect review examines the actual source,
   tests, diff and evidence with no unresolved blocking findings; correct and
   reverify findings. Earlier design advice does not satisfy this review.
5. Root updates the selected tracker with actual evidence, deviations, cleanup,
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
   and compatibility gate. Provision the Python environment and add the acceptance
   hook inside existing server harness ownership. Add explicit credential/endpoint
   handoff, disposable fixture cleanup, timeouts, evidence and installed-package
   execution. Extend helper-copy/package lists where hooks require distribution.
7. Create the acceptance map and procedure coverage ledger with independent fields
   for dispatch, Python convenience behavior and executed integration evidence.
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

**Gate:** planned runner `-Stage 1`; build/install and parity groups; relevant
existing server discovery and modified harness regression. Stage 1 records exact
resolved tool versions, command selections and retained logs in the tracker.

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
3. Implement protected reauthentication and a public way to inspect pending work
   and retrieve recovered results without exposing recovery keys as application
   responsibility. Unavailable credentials/permissions preserve work and return
   actionable safe conditions. Credential rotation does not change original actor.
4. Add minimal portable Class/Object reads and create methods through that core.
   Capture declaration/value snapshots together; implement explicit refresh and
   conflict preservation without completing the full Stage 4 handle surface.
5. Extend harness barriers for before/after preparation, commit, local result
   persistence and release. Actually terminate/relaunch Python with the same
   protected journal. Verify effect counts/UUIDs through an independent session.
   Add real server restart/kill-restart tests under existing runtime owners.

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
  continuation and preserve pending work. Completed Namespace deletion and prior-
  epoch server actions follow the public recovery contract rather than retargeting.
  An uncommitted server-scope preparation from an earlier server epoch preserves
  pending work on REVISION_CONFLICT and requires explicit refresh/reapply; never
  automatically generate a new key or reinterpret it as a replayable action.
- **2E:** Two processes cannot resume the same journal action concurrently. Abrupt
  death, corruption and storage failure produce safe recoverable states. Cancellation
  near commit resolves the actual outcome; neither driver retries nor logout loses it.
- **2F:** Actual server graceful/forced restart invalidates sessions and recovers
  committed/uncommitted cases with original preconditions and independently checked
  outcomes. Record both Python termination and server-transition evidence.

**Gate:** runner `-Stage 2`; core unit/state-transition tests and live restart
scenarios; supporting selected `PreparedActionReplayIT`, `AccountAuthorizationIT`
and action recovery seed/transition/verifier when the harness boundary changes.

## Stage 3 - Complete procedure gateway and controlled advanced access

**Dependency:** Stage 2. **Outcome:** every public server procedure has a supported
route, with correct binding, scope, output lifetime and recovery classification.

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
   executing prunes can be. Release detaches rather than deletes durable server
   preparation records. Selection and session/action controls use separate transactions.
3. Implement named-column closable advanced results, native lifetime checks,
   portable conversion in the owning transaction and deterministic exhaustion/
   exception/early-close cleanup. Retain multi-column query and `canUndo` shapes,
   empty rows/pages and actual column names; never invent generic metadata columns.
4. Complete safe server error mapping with diagnostic request IDs, unknown driver
   errors and bounded diagnostics. Implement authorized request listing/cancellation
   through the parallel control path and explicit administrative-mode lifecycle.
5. Populate dispatch tests for every manifest entry and representative installed
   live execution per family. Mark specialized end-to-end coverage still pending
   for Stages 4-6 rather than equating dispatch parity with acceptance.

Acceptance:

- **3A:** Every catalog entry plus discovery has exact binding, owner, named output,
  scope, recovery and batch policy tests; additions/removals fail parity checks.
  Assert prune dry-run/executing branches and operational analysis/export actions.
- **3B:** Real family representatives verify token-free discovery, pre-binding and
  scoped routing, parameter order, empty results, safe errors and permission denial.
  Both convenience and advanced calls use the same protection and journal paths.
- **3C:** Advanced results preserve real columns and native lifetime; early close,
  conversion failure, iteration failure and Store close release transactions and
  readers. No native result is usable outside its valid lifetime.
- **3D:** Cancellation reaches an active long-running request while the Store is
  busy, respects actor permissions and resolves ambiguous action completion.
  Administrative mode is explicit and isolated per session.
- **3E:** Coverage ledger lists each specialized unverified scenario, its owning
  later stage and test target; no procedure lacks a supported invocation path.

**Gate:** runner `-Stage 3`; all-entry parity/binding tests and installed live
family representatives. Include relevant server authorization/cancellation/replay
regression for any shared hook or behavior touched.

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

**Gate:** runner `-Stage 4`; exhaustive value/snapshot/batch units plus installed
domain workflows. Supporting server selections when affected: `ValueCodecTest`,
`StrictValuesTest`, `TypedDataModelIT`, `BatchMutationIT`, `QueryLanguageIT`.

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
5. Add per-family installed workflows and failure/recovery cases. Reuse server crash
   owners for actual restart/diagnostic probes; verify wrapper continuation after
   transitions. Complete relevant Python usage and shared signature documentation.

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

**Gate:** runner `-Stage 5`; family-specific workflows and actual interrupted
transfer/import scenarios. Relevant server selections come from the verification
guide (resource/geometry, Patterns, formats, history, clone/import, authorization,
browser). Diagnostic tests run with probe artifacts only through their owner.
Record each failed test individually and finish post-transition verification.

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
4. Build final wheel/sdist; test isolated clean installs with/without NetworkX on
   every advertised Python/driver/server combination. Test supported minimum and
   upper bounds or narrow the advertised bounded matrix to evidenced combinations
   with a user decision where that changes agreed compatibility.
5. Run complete cumulative wrapper regression, real process/restart/crash recovery,
   installed lifecycle/resource/browser acceptance, relevant full server regression
   for shared changes, and package/version/dependency/license checks. Finalize
   installation, migration-from-1.x, compatibility, recovery/result access and cleanup
   documentation. Record evidence limits rather than claiming new deployment scope.

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

**Gate:** runner `-Stage 6` (all groups), installed matrix, complete coverage ledger
and required server/harness selections. Release readiness does not authorize upload
to PyPI, pushes, deployment or changing existing installations.

## Project-run execution and status contract

Example future request from this repository (not an execution instruction now):

> Use $project-run for Deposit Python, Stages 1 through 2, with implementation
> plan `docs/implementation_plan.md` and implementation status
> `docs/implementation_status.md`. Execute the selected stages.

Select an explicit bounded sequence or next-N request. The plan and tracker are
independent explicit inputs; do not substitute the hub's server status. Cross-repo
work in the minimal harness hook and relevant shared Python specifications is part
of the selected stages, subject to each repository's instructions. External runtime
changes still use existing harness ownership and applicable runtime permissions.

Follow the deployed project-run skill for fresh stage workers, read-only planning,
separate implementation dispatch, independent architect review, protected baselines,
tracker updates and stage commits. Use a common run ID and exact stage ID in each
commit; preserve unrelated/index changes. Commit each changed implementation repo
then any separate tracker repo; here plan/tracker share deposit-python. Do not make
empty commits or push. The current documentation task creates no run checkpoint
or stage commits and marks no implementation stage complete.

Resolve local checkpoint via `git rev-parse --git-path project-run/state.json`.
Store selected file hashes, baselines, current phase, decisions, acceptance evidence,
reviewed diff identity and pending/resulting commits under the skill's versioned
schema. Reconcile checkpoint/source/tracker/history before resuming. A verified
stage with pending commits cannot advance. Evidence is reusable only while inputs
match. Document real external blockers and required actions under AGENTS.md;
missing tooling/tests or ordinary failures remain implementation work.

The tracker records stage state plus evidence for every acceptance ID, exact
commands/test counts/failures, log locations, source/artifact identities, review,
deviations, cleanup and commits. Keep machine-specific run paths in local evidence/
checkpoint; use portable repository references in shared documentation. No plaintext
credentials, tokens, sensitive operation arguments or user datasets in records.
