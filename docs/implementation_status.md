# Deposit Python 2 implementation status

Updated: 2026-10-30.

## Authority and baseline

Plan: [implementation_plan.md](implementation_plan.md).
Design input: [implementation_proposal.md](implementation_proposal.md).
This tracker covers the Python wrapper and its coordinated Stage 1/2
server/hub changes; it is not the server implementation status in the Deposit Hub. Stage
order is 1, 2, 3, 4, 5, 6.

Initial inspected repository before implementation: `main` at
`3c61d21822d71182416b8f8f5cca7e2a7da295ba`. At that point package metadata and
import stubs existed; no operational wrapper, wrapper tests or Python acceptance
runner existed. The project-run Stage 1 repository baselines are recorded in its
local checkpoint. Existing server tests alone did not establish Python wrapper
acceptance.

## Stage summary

| Stage | Scope | State | Acceptance | Verification/review | Finalization |
| --- | --- | --- | --- | --- | --- |
| 1 | Contracts, package and live test infrastructure | Finalized | 1A-1E complete | Foundation gate passed; independent architect PASS, no blocking findings | All three Stage 1 commits verified; commit IDs recorded below |
| 2 | Safe core and durable recovery | Finalized | 2A-2F complete | Final cumulative gates passed; independent architect PASS, no blocking findings | Python, server and hub commits verified; tracker follow-up committed |
| 3 | Complete procedure gateway | Finalized | 3A-3E complete | Cumulative gate passed; independent architect PASS, no blocking findings | Python, server and hub implementation commits verified; tracker follow-up recorded |
| 4 | Typed domain, queries and batches | Finalized | 4A-4E complete | Cumulative Python gate and affected Server regressions passed; independent architect PASS | Server, Hub and Python commits verified; tracker follow-up committed |
| 5 | Resources and extended workflows | Verified and reviewed (finalization pending) | 5A-5F complete; cumulative StageGate passed against provenance-recorded `.15.g7610e45-SNAPSHOT` | Fresh independent architect review READY, no blocking findings | Tracker and stage commits pending |
| 6 | NetworkX and release acceptance | Not started | 6A-6E pending | Not run / no implementation review | No stage commits |

Allowed states: Not started; Planning; Implementing; Verification incomplete;
Review corrections; Verified and reviewed (finalization pending); Finalized;
Interrupted (with exact reason and resumption criteria). A finalized stage needs
all acceptance evidence, no blocking review findings, a tracker update and all
required commits. Preserve completed cross-repository commits if later
finalization fails; resume pending work without duplicating commits.

## Acceptance and procedure coverage

Stage 1 acceptance IDs 1A-1E and Stage 2 acceptance IDs 2A-2F are verified and
reviewed as recorded below. Stages 3 and 4 are also verified and reviewed;
Stage 6 remains pending. Record separate states for supported dispatch, convenient
Python behavior and executed integration acceptance. A catalog count or Java
regression result cannot satisfy the latter two fields. No specialized scenario
can remain unassigned beyond its stage or be waived at release.

## Stage evidence records

Append one record per stage and update it through finalization, retaining failures
and subsequent corrections. Use this structure:

```text
Stage / run ID / current phase:
Scope and prerequisites checked:
Repository branches and baseline/source/artifact identities:
Implemented files and behavior:
Acceptance ID -> concrete test/assertion -> result/evidence:
Exact executed commands, selections, counts and retained log references:
Each failed test, diagnosis, correction and rerun result:
Supported matrix actually exercised; skipped/unexecuted criteria:
Independent architect review, reviewed diff identity, findings/resolution:
Shared specification consistency review when required:
Cleanup/runtime restoration outcome:
Deviations, pending user decisions, limitations and remaining work:
Tracker update state:
Per-repository stage commits / pending commits:
Interruption reason, external action if any, and exact resumption criteria:
```

### Stage 1 / `deposit-python-20261025-01` / finalized

Scope and prerequisites checked: Stage 1 has no prerequisite. Captured clean
index/worktree baselines on `main`: Python
`8750dc8e6fe15d31ec6b365d64becdab1959900b`, server
`c1489c18b72c4a98d8bfb46a3926fe8195b3e2e6`, hub
`e4d9c1a248805831b2fd947774c84532359c0a45`.

Implemented: package/build metadata and exports; safe discovery/client errors;
procedure manifest and parity generator; DPAPI-backed protected storage and
native identity binding; installed-package and real-server smoke path; bounded
Python runner with noninheritable kill-on-close Job Object descendant containment;
server harness clean-tag build bundle and acceptance hook; server catalog metadata
alignment; contract reconciliation, acceptance map and procedure coverage ledger;
hub Python contract update. The accepted public `Client.open_store(namespace,
*, username, password, recovery_directory=None)` contract is documented for
Stage 2 implementation. Native identity binds target `db.info().id` and system
`dbms.info().id` to credential-free endpoint, database, Deposit actor and immutable
Namespace UUID. Copied/restored stores may retain native IDs; this accepted
limitation is documented. No new server deployment-generation field was added.

Acceptance ID -> assertion -> evidence:

- **1A:** isolated wheel/sdist installed-package import and package tests passed;
  no source-tree fallback. Wheel and sdist client/package suites passed (6 tests
  each) on Python 3.14.4.
- **1B:** manifest parity tests passed (3); server `ProcedureCatalogMetadataTest`
  passed (2); `PreparedActionReplayIT` compiled. Catalog metadata now marks the
  three `deposit.load.csv/.xlsx/.gpkg` procedures operational/action, consistent
  with existing prepared-action handling and hub declarations.
- **1C:** installed-package live discovery, authentication/session flow and
  installed Seed plus post-restart Verify passed through the harness suite owner.
- **1D:** protected-storage tests (10), identity tests (6), and native
  anonymous-token denial/data-preservation probe passed. Live target/system native
  IDs and identity binding were exercised. Journal state-machine/replay behavior
  remains Stage 2.
- **1E:** 0/23 descendant/pipe and setup-failure runner probes passed; bundle
  probes passed; installed live hook passed. Harness-owned runtime cleanup ended
  terminal/cleaned with no cleanup error; no matching runner process remained.

Exact final gate: `Invoke-PythonTests.ps1 -Stage 1 -Group foundation` using the
verified clean tagged server bundle. Run
`20261026T010158409Z-2e0e0609`, 19 commands, all exit 0, zero timeouts, Python
3.14.4. Result `../test_harness/results/python-20261026T010158409Z-2e0e0609/results.json`,
SHA256 `D19312BBDB912E745E8CBA23CFC92E8DA5934F548A84DCE4604F33EF44235801`.
Installed live suite result is under
`../test_harness/results/20261026T010305840Z-1678f00a`.

Additional server regressions all passed and are retained under
`../test_harness/cache/project-run-evidence/deposit-python-20261025-01/server-regressions-final`:
`Test-IntegrationSuiteJournal.ps1`, `Test-IntegrationSuiteLocalRetirement.ps1`,
`Test-IntegrationSuiteScenarioBootstrap.ps1`,
`Test-PythonAcceptanceBuildBundle.ps1`, `Test-ReleaseManifest.ps1` (32 assertions),
and `Test-ScenarioChildExitHandling.ps1`. PowerShell syntax parsing passed.
Maven `ProcedureCatalogMetadataTest` passed (2 tests) and integration-test
`test-compile` succeeded using harness JDK 21. The Java integration test was
compiled, not run against a newly provisioned server.

Failures and corrections retained: initial live attempts correctly rejected a
DIRTY-SNAPSHOT bundle and exposed runner-path/helper-load-order issues; later
runs exposed multiple-row `dbms.components` handling and Namespace cleanup before
activation. Those defects were corrected, and the final clean-tag installed run
passed. A first Maven invocation selected PATH Java 8 and failed before tests;
rerun with harness JDK 21 passed. Harness cleanup initially required approval for
exact generated probe directories; after the user selected “Approve for me,” only
the eight exact task-generated directories were removed and verified absent. The
final gate cleaned its owned runtime successfully.

Supported matrix exercised: Windows x64, Python 3.14.4, Neo4j driver 6.x, exact
Deposit Server `2.0.0-dev.77` clean tagged build. Other operating systems,
interpreters and server builds were not claimed or tested.

Independent architect review: PASS, no blocking findings. Reviewed the current
source/tests across all three repositories, Stage 1 criteria, approved decisions,
and retained evidence. The review independently verified the final result hash,
19-command count, test counts, live restart path and regression logs. Two
nonblocking README/acceptance-map wording notes were corrected; a second
architect check confirmed both corrections and the persistent evidence link.
Review limitation: the test suite was not rerun by the architect; integration
test compilation is not reported as live execution. `PreparedActionReplayIT` is
therefore only compilation evidence. The live smoke used the documented baseline
tagged bundle, not a rebuilt artifact containing the catalog correction.

Tracker update state: Finalized with all acceptance evidence and the completed review. Stage 1 commits:

- Server: `20d15ee708c797f17e29ec6782d017b101eae828`.
- Hub contract: `0cc09a5b34ccb83861f005d53e9797ce7df11124`; completed-commit status follow-up: `240dc999d341cd7d7cf3ae7f507cd92214e8888a`.
- Python source and this tracker are included in the tracker-bearing Stage 1 commit; its full hash is recorded in the local project-run checkpoint and Git history.

Stage 2 has completed its verification, review, tracker update and repository commits.

## Planning record

The plan preserves the proposal's six stages and requires real-server tests in
Stage 1, durable prepared recovery with the first mutation in Stage 2, and separate
dispatch/convenience/integration coverage through release. Stage 1 resolved the
support matrix, protected-storage and identity mechanism/defaults, and detailed
signature reconciliation; Stage 2 implements the documented Store/recovery
contract.

Planning consultation and read-only document consistency review completed with
architect (`gpt-6-astra`, medium reasoning) on 2026-10-25. Two findings were
corrected and rechecked: distinguish client-retained arguments/provenance from
server-owned recovery bindings/preconditions, and scope orchestration gates to
authorized project-run. No unresolved blocking document-consistency findings.

Documentation preparation was validated before implementation: local links
resolved, six stages appeared in order, 32 expected acceptance IDs were unique,
code fences were balanced and whitespace checks were clean. That preparation did
not count as implementation evidence. The pre-existing untracked proposal was
preserved.

Stage 1 resolved deployment identity without expanding the public server
contract. Restart replay remains disabled until Stage 2 implements durable
recovery checks against the accepted identity policy.

### Stage 2 / `deposit-python-20261025-01` / finalized

Scope and prerequisites checked: Stage 1 is finalized in Python, server and hub.
Captured Stage 2 baselines: Python `e930e004888d2196fdf6a29de5f69e27de60e735`,
server `20d15ee708c797f17e29ec6782d017b101eae828`, and hub
`240dc999d341cd7d7cf3ae7f507cd92214e8888a`; all repositories use `main`.
User-approved decisions retained: `Client.open_store(namespace, *, username,
password, recovery_directory=None)`; Client-owned immutable pending/recover/
recovered references; native identity policy and its copied-store limitation.

Implemented: independent Store sessions bound to immutable Namespace UUIDs;
minimal portable Class/Object create/read and conflict refresh/reapply; protected
versioned recovery journal; strict JSON decoding, exact portable input validation,
current-user protected root registry, aggregate quota and unresolved limits,
fail-fast cross-process/action locking; identity-bound Client recovery including
deleted-Namespace receipts; process-death/restart acceptance in the existing
suite owner. Unexpected `NOT_FOUND` with saved outcome remains unresolved;
successful lost release replies recover from the durable committed receipt.
Shared hub Client recovery contract and server-owned Python acceptance hooks were
updated. No server runtime behavior or public server procedure changed.

Acceptance ID -> assertion -> evidence: all 2A-2F are recorded in
[`tests/acceptance_map.md`](../tests/acceptance_map.md). The final cumulative
Restart run `20261026T161936350Z-c10e1603` and KillRestart run
`20261026T162540049Z-b609525c` each used Python 3.14.4 and the clean tagged
Deposit Server `2.0.0-dev.77` bundle. Each runner recorded 21 commands, all exit
0 with zero timeouts. Wheel/sdist package checks: 24 each; wheel manifest: 3;
storage: 14; identity: 6; wheel/sdist core: 68 each. Both live Seed phases were
2 passed / 2 skipped and Verify phases 4 passed. Each exact-tag overlay restored
56/56 script paths and the clean HEAD/tag with zero restoration failures.
Result-record SHA-256 values: Restart
`60EDD22ECF5534B5757DCFE93CC0F70ABC218677EB1B0A49196026038A884E3E`; KillRestart
`C662CF857C4778E856783526C3BC6A114DAED36B0FAC267025A0C908FBB4373E`. Evidence
is retained under
`../test_harness/tools/python-tests/stage2-server-evidence-20261026-01/Final-Restart-20261026T161936350Z-c10e1603/`
and `Final-KillRestart-20261026T162540049Z-b609525c/`.

Focused checks after corrections: quota/release selection 12 passed; strict
journal/input selection 63 passed with one ACL subprocess test deselected; the
later non-UUID key preservation matrix passed 4/4 access paths. Server support
selection `PreparedActionReplayIT,AccountAuthorizationIT` passed 42 tests;
`ActionReplayRecoveryIT#seed/#verify` passed one test each under both Restart and
KillRestart. Logs and a quota-registry cleanup audit are retained under
`../test_harness/tools/python-tests/stage2-server-evidence-20261026-01/`.

Failure/correction record: an exploratory direct three-file pytest run reported
41 passed and 8 ACL-dependent failures. The workspace ACL retained SYSTEM,
Administrators and OWNER RIGHTS entries after `icacls /inheritance:r`; the
unchanged owner-only validator correctly rejected them. This direct invocation
was not the owner-managed acceptance command; the final wheel/sdist storage,
package and core gates passed. Its output and the later focused outputs are
retained in the `python-focused-evidence` subdirectory. Earlier cumulative runs
were superseded by the final runs after architect findings were corrected.

Supported matrix exercised: Windows x64, Python 3.14.4, Neo4j driver 6.x and
Deposit Server `2.0.0-dev.77`. No other operating systems, interpreter versions
or server versions are claimed. Endpoint/database/native-ID replacement checks
are unit call-spy evidence; the live recovery fixture verifies server identity
and Namespace binding across both transitions.

Independent architect review: PASS, no remaining blocking findings. Follow-up
review checked the corrected strict journal validation and portable input
fidelity against the current diffs, verified final result hashes and acceptance
counts, and confirmed clean `git diff --check` in all three repositories. The
architect did not execute tests. Corrections preserved the approved APIs and
server contract.

Shared contract/documentation consistency: stale Store-factory implementation
wording and acceptance-map run references were corrected after review. Stage 2
adds no new architectural decision requiring a contract change.

Tracker update state: Finalized. Separate Stage 2 implementation commits:

- Python: `9330881d661fea7694181967367a6f968c3fbc7d` —
  `project-run deposit-python-20261025-01: Stage 2 - safe core and durable recovery`
- Server: `0f9abfa5bb7ae6f781bde406a521f7182bd8c6ff` —
  `project-run deposit-python-20261025-01: Stage 2 - Python acceptance harness integration`
- Hub: `6e90461dfe1939dbac5b8b509faf5a214a495591` —
  `project-run deposit-python-20261025-01: Stage 2 - approved Client recovery contract`

The final Stage 2 tracker-state follow-up is a separate Python commit; its commit
ID is recorded in the local project-run checkpoint and Git history.

### Stage 3 / `deposit-python-20261025-01` / verified and reviewed

Scope and prerequisites checked: Stage 2 finalized in all three repositories.
Stage 3 uses manifest-backed Client/Store routes and explicitly preserves the
approved deferrals for durable Client server-scope action recovery and
`deposit.admin.pruneResources` execution at both scopes. Client singular
`get_user` and `get_namespace` require explicit nonempty selectors; plural
`get_users` and `get_namespaces` return detached dictionary tuples for zero,
one or multiple rows.

Implemented: the Python gateway handles safe binding, token placement, Client
and Store ownership, recovery classifications, named results, native value
lifetime and authorized request controls. StoreResult prevents access to its
native graph container, checks iterator lifetime on advancement and detaches
lazy property views. The Client handles a missing active Namespace without
calling `toData` on null. The procedure ledger now maps all 124 manifest rows to
dispatch, supported behavior, executed evidence and a later-stage specialized
test target. The server acceptance scripts use the clean tagged source/build
bundle route. Hub documentation records the accepted recovery and collection
read APIs. Three existing server runtime POM edits were preserved as unrelated.

Acceptance ID -> evidence:

- **3A:** manifest parity and explicit route/classification/deferral policy
  tests cover all 124 declarations; `test_gateway_policy.py` passed 47 tests in
  both wheel and sdist.
- **3B:** explicit singular selectors and zero/one/many tuple behavior are
  covered by eight focused API assertions. Installed Gateway Seed/Verify also
  exercised Client routing and Store representatives, safe permission denial,
  dry-run pruning, export and import analysis.
- **3C:** native lifetime, early close, conversion, iterator and Path behavior
  passed the wheel/sdist gateway selections. Live Node and Relationship
  conversion used the owning transaction and Store token.
- **3D:** installed live request listing and active cancellation verified actor
  denial, authorized cancellation and request disappearance; Stage 2 restart
  recovery remains covered by its finalized Restart/KillRestart gates.
- **3E:** all manifest rows identify supported routes, actual Stage 3 evidence
  and specialized later-stage targets; approved action deferrals remain
  explicitly pending.

Exact cumulative command: `Invoke-PythonTests.ps1 -Stage 3 -Group gateway`
using the verified clean Deposit Server `2.0.0-dev.77` bundle from tag
`c1489c18b72c4a98d8bfb46a3926fe8195b3e2e6`. Run
`20261026T214740608Z-6a36ef3f`, Python 3.14.4, 23 commands, all exit 0 with no
timeouts. Wheel/sdist: package 42 each, core 94 each, gateway 47 each; wheel
manifest 3, storage 14 and identity 6. Owner-managed live run
`20261026T214902605Z-b04be0db`: Gateway Seed 3 passed / 2 skipped; Verify after
server restart 5 passed. Result JSON SHA-256
`93C78C061FA17D51A04662F6236A3F68CE2968A36A562BA58109BBB66AD4AE16` is at
`../test_harness/results/python-20261026T214740608Z-6a36ef3f/results.json`;
live logs are at `../test_harness/results/20261026T214902605Z-b04be0db/`.

Failure/correction record: an exploratory direct unit run reported 46 passed
and one Windows ACL setup failure because the current workspace exposes extra
principals; the owner-managed full wheel/sdist gate passed the complete package,
core and gateway selections. Earlier live gate failures and their corrections
are recorded in `tests/acceptance_map.md`. The live harness reached terminal
cleanup; all eight specifically identified stale `runner-probe-*` directories
were already absent and remain absent.

Independent architect review: PASS, no blocking findings, against the current
Python, server and hub diffs. Follow-up read-only review confirmed the ledger's
21 live evidence references use the final live run and distinguish it from the
cumulative Python gate. The architect inspected retained evidence and did not
rerun tests. Shared hub contract consistency was reviewed with the accepted
selector/cardinality API and recovery deferrals.

Tracker update state: Finalized. Separate Stage 3 implementation commits:

- Python: `245e03c062bc5d7aeb7eafc89a5f23c9ecd50d72` —
  `project-run deposit-python-20261025-01: Stage 3 - complete procedure gateway and controlled advanced access`
- Server: `d7ec72e3ba560775848633d5a664c308d60130a7` —
  `project-run deposit-python-20261025-01: Stage 3 - Python gateway acceptance routing`
- Hub: `89b2a4a3420c63279cceb3b1ae3cbfdbca6a75ce` —
  `project-run deposit-python-20261025-01: Stage 3 - Client gateway and collection API contract`

The final Stage 3 tracker-state follow-up is a separate Python commit; its ID is
recorded in the local project-run checkpoint and Git history.

### Stage 4 / `deposit-python-20261025-01` / finalized

Scope and prerequisites checked: Stage 3 was finalized in Python, Server and
Hub. Stage 4 adds typed values, stable Store-bound entity handles, revision-
checked snapshots and pages, query iteration, and atomic batches. It preserves
the existing prepared-action coordinator, exact command replay, caller-visible
explicit refresh/reapply behavior, existing public page/result shapes, and the
Stage 3 server-scope action and prune execution deferrals.

Implemented: Python typed codecs and DType/DDict/DGeometry/DResource
representations; entity handles and CRUD helpers; batch collection, validation,
temporary references and saved outcomes; and query/page iterators. Fresh
Namespace sequence fences now bind snapshots, fully consumed pages, buffered
handle transformations, descriptor schema decoding and neighbor traversal to a
fixed revision. Batch schema knowledge is invalidated after descriptor type or
name changes. Geometry aliases and naive built-in local temporal inputs are
supported. The installed live test covers a real committed batch whose client
process exits before returning the reply, followed by owner restart, public
`Client.recover`, exact saved outcomes and independent effect checks.

Acceptance ID -> evidence:

- **4A:** `tests/unit/test_values.py` covers numeric bounds, finite floats,
  temporal/offset precision, naive local built-ins, calendar duration, Point
  SRID, nested typed collections and nulls. Installed wire assertions passed.
- **4B:** entity/store/batch tests cover missing vs null, detached edits,
  declaration changes and conversion, revision conflicts, batch schema
  invalidation, and explicit refresh. Installed schema and assignment checks
  passed.
- **4C:** Store/entity tests and installed checks passed for Class/Object/
  Descriptor CRUD, name/UUID selectors, handle ownership, stale edits and
  refresh.
- **4D:** page/store tests cover query presence, multiple/empty pages, cursor
  validation, sequence fences during collection and after buffered/final-page
  transforms, descriptor/neighbor consistency and reader cleanup. Installed
  query/page checks passed.
- **4E:** batch/store tests cover reference ownership, whole-request validation,
  rollback, limits and saved results. Installed Seed→Stop/Start→Verify proved
  recovery of the original batch result after the coordinator-visible reply was
  lost and checked persisted effects. The test describes child termination
  after the server response, not a physical network fault.

Exact cumulative command: `Invoke-PythonTests.ps1 -Stage 4 -Group domain` using
Python 3.14.4 and the verified Deposit Server `2.0.0-dev.77` bundle from source
commit `c1489c18b72c4a98d8bfb46a3926fe8195b3e2e6`. Run
`20261027T134920925Z-17cf7ecc`: 25 commands, all exit 0, no timeouts. Wheel:
package 43, manifest 3, storage 14, identity 6, core 115, gateway 66, domain
133. Sdist: package 43, core 115, gateway 66, domain 133. Runner and bundle
probes passed. Installed Domain Seed passed 4 / 2 skipped; Verify after the
owner-managed restart passed 6. Result JSON SHA-256
`FAE7F1E6B16259E27E472AF81BE9FADE38927B787E9DD5E4F227B6527D48FD4B` is at
`../test_harness/results/python-20261027T134920925Z-17cf7ecc/results.json`;
live summaries are at `../test_harness/results/20261027T135046795Z-d3d67315/`.
The reviewed redacted evidence archive contains 22 files with verified hashes;
raw recovery artifacts were excluded.

Supporting Server regressions passed earlier in this Stage 4 run: unit
`ValueCodecTest,StrictValuesTest` 4/4; integration `BatchMutationIT`,
`QueryLanguageIT`, and `TypedDataModelIT` 43/43. No Server source changed in
the subsequent review corrections, so those selections remain applicable.

Failure/correction record: a direct PowerShell script invocation was blocked by
execution policy; the authorized run used process-scoped `-ExecutionPolicy
Bypass` without changing machine policy. One build-bundle probe treated Git
checkout progress on stderr as a terminating PowerShell 5.1 error; delaying
optional Git progress for the gate process made the probe pass. These were
minor invocation/harness adjustments. Two initial live attempts exposed test
defects, not implementation regressions: Neo4j formatted local fractional
seconds at nanosecond precision, and a test attempted to close a Store after
clearing its reference. Both were corrected before the final passing run. Each
suite journal ended `cleaned`, `terminalCleanup=true`, `cleanupError=null`.

Independent architect review: PASS, no unresolved blocking findings. The fresh
review checked the Python, Server runner and Hub contract diffs against the
Stage 4 plan and approved decisions, confirmed all prior findings were fixed,
and verified the 4A-4E assertion map. The review did not run tests; it inspected
the accessible redacted results, live logs and cleanup summary.

Acceptance traceability is recorded in [tests/acceptance_map.md](../tests/acceptance_map.md)
and [docs/acceptance_map.md](acceptance_map.md). Stage 4 is finalized after
verification, independent review, and separate implementation commits:

- Server: `5e46b1343069a5e55f4da3b3646061ab5cf0d97f`:
  `project-run deposit-python-20261025-01: Stage 4 - Domain acceptance routing`
- Hub: `c65b68750a5ed15b9ba87dcfab892ef3dadac1cc`:
  `project-run deposit-python-20261025-01: Stage 4 - Python domain contract`
- Python: `a505028503df7440cae186742dfb460cdded77fd`:
  `project-run deposit-python-20261025-01: Stage 4 - typed domain, revision-fenced pages and atomic batches`

This tracker-state follow-up records the Python implementation commit identity;
its own commit ID is recorded in the local project-run checkpoint. Preserve
unrelated Server POM/.gitignore/deleted skill-copy changes and the user-injected
Python AGENTS.md change.


### Stage 5 / `deposit-python-20261025-01` / verification incomplete

Implemented scope: protected frozen resource uploads and checked downloads,
continuation and terminal-failure recovery; strict Server-owned geometry/resource
write projections; Pattern/form/layout helpers; immutable resource-free Exchange
dictionary/subgraph analysis and row analysis/apply; installed format, browser,
history and operational recovery scenarios. Full acceptance remains incomplete.

Approved decisions retained: defer full-Namespace clone and Client-owned
server-scope mutation recovery; leave image classification unknown without
verified metadata; reject resource-bearing dictionary imports until a companion
content API is decided. The user approved retargeting Python's exact supported
Server version to the committed Stage 5 Server artifact while retaining equality.
That committed-artifact retarget and validation remain pending.

Verification/correction record so far:

- Focused transfer/recovery units passed 48 tests using the gate's importlib
  mode; the previously omitted transfer-recovery module is now in the cumulative
  workflow selection. Its sibling imports were corrected before inclusion.
- Acceptance diagnostics units passed 3 tests. The real owned-process probe
  passed failure-then-hang retention, predeadline stack output, secret redaction,
  timeout detection and reaping. Original stderr is duplicated before pytest
  capture so a killed test cannot hide the timer's output.
- Repeated 300-second Verify timeouts were diagnosed as a cumulative suite budget
  problem. A reproduction passed the first six tests at 87 seconds, failed a
  Stage 5 fixture assertion at 256 seconds, passed import recovery at 297 seconds,
  then started export recovery before the total deadline killed the process.
  Full protected-journal scans dominated the Stage 5 stack samples. No deadlock
  or external blocker was established; architect did not approve that interruption.
  The fixture imported a third object before expecting a two-row export; form
  import is now sequenced after the format assertions. Workflows Seed/Verify
  receive explicit 1,800-second budgets; the security/persistence checks remain.
- Provisional cumulative run `20261027T235226091Z-871f918f` passed wheel unit
  selections and runner/bundle probes, then failed Server `ResourceAndGeometryIT`
  (1/12) before Python live acceptance: expected reclaimed bytes 1365, actual 0.
  A diagnostic repeat reproduced the underlying fixture race twice in 30 runs:
  lease expiry was set, but maintenance had removed `orphanSince`, so no prune
  candidate/item was scheduled. The expert owns a test-only correction using
  existing Namespace/resource coordination around artificial fixture expiry.
  Corrected repetition run `20261028T001116857Z-6387fb0c` passed 30/30 in 105.3
  seconds; final normal-test class run `20261028T001558958Z-6dfb7bdd` passed
  12/12 in 29.29 seconds. Both owners reached clean terminal cleanup with zero
  Java processes. Exact reclamation, rollback, replay and authorization assertions
  remain, with stronger scheduling/item checks. Production GC is unchanged.
- The next cumulative run, `20261028T001939441Z-99846af3`, passed all nine
  Server selections and installed Seed (8 passed / 2 phase-specific skips,
  165.12 seconds). Verify completed without timeout (9 passed / 1 failed,
  287.02 seconds), including interrupted import and export recovery. The sole
  failure was a fixture expectation: tabular DDict cells retain per-entry
  `{type,value}` JSON, rather than plain Python dictionary values. Independent
  typed literal expectations now cover exported CSV and loaded CSV/XLSX/GeoPackage.
  UTC timestamp assertions compare full-precision temporal values across the
  equivalent `Z` and `+00:00` representations. Complete rerun remains pending.
- Manifest parity now honors the runner's selected Server/build-source path
  through an explicitly restored environment setting, rather than assuming a
  sibling checkout. All 3 parity/mutation checks passed with explicit source
  selection, including rejecting mutations in the selected source.
- Provisional run `20261028T005817137Z-a1934dc0` passed all nine Server
  selections (90 tests), Seed (8 passed / 2 skips, 165.92 seconds), and the
  wheel/probe groups. Verify stopped at its first failure (6 passed / 1 failed,
  247.99 seconds), without timeout: the XLSX fixture assumed a nonempty shared
  string table, while Server emits valid inline strings. Independent decoding
  now checks both representations and exact large-integer text; its focused
  unit passed. The fixture audit also corrected staged row-mapping keys and
  decoded DDict comparison. Final complete-run evidence remains pending.
- Main-source reader context cleanup now preserves an existing permission or
  chunk-validation exception instead of replacing it with an incomplete-close
  error. Both regression cases were reproduced before correction; 50 focused
  transfer/recovery tests passed afterward. This change was deliberately outside
  the running candidate and must be included in the final main-source gate.
- Clean Server setup at `4dd5838` passed and derived official version
  `2.0.0-dev.77-post.5.g4dd5838-SNAPSHOT`; both JAR versions and the packaged
  plugin bytes were verified. Exact Python pin regeneration and all 3 manifest
  checks passed. Main-source run `20261028T014643068Z-b763bc16` passed its wheel
  groups and runner probes, then stopped before live acceptance: the synthetic
  bundle probe assumed a sibling Python checkout and a tagged runtime support
  version. Architect classified this as a routine harness-fixture correction:
  use explicit historical tagged probe provenance and a manifest in owned probe
  scratch, preserving all production bundle checks. The corrected focused probe
  passed without a sibling Python checkout and rejected a missing tag. Component
  review requested an additional manifest-write failure cleanup check before
  committing the harness correction. No timeout occurred in this run.

Reviewed redacted diagnostic evidence and source/log hashes are retained locally
under `.stage5-diagnostics/`; these partial/failed runs are not acceptance passes.
Prior observed suite owners reached terminal cleanup without cleanup errors.
The selected assertion maps identify implemented branches and pending evidence.

The preliminary independent Server component review found no blocking findings
and confirmed its nine passing selections. Server commit `4dd5838` contains only
the 11 reviewed Stage 5 files; unrelated work is preserved. This component commit
provides the source identity needed for the clean exact-version artifact and
does not establish whole-stage acceptance. The updated main-source workflow unit
selection passed 71 tests.

The bundle-probe follow-up passed the no-sibling positive probe, explicit
missing-tag rejection and an injected manifest-write failure with no remaining
scratch directory. Independent component review cleared the cleanup correction.
Server commit `4f5f6f2` contains that single test-script change. Its clean official
setup passed at version `2.0.0-dev.77-post.6.g4f5f6f2-SNAPSHOT`; Python's exact
pin and regenerated manifest match it, with all 3 parity/mutation tests passing.
The complete gate against these final inputs remains pending.

Run `20261028T021428946Z-69c94163` passed all nine Server selections (90 tests),
the corrected bundle probe, wheel groups and runner probes. Seed passed 8 tests
with 2 phase skips in 184.78 seconds; Verify stopped at its first fixture failure
(6 passed / 1 failed, 282.30 seconds): numeric XLSX `9.0` was compared with text
`9`. No timeout occurred, cleanup completed, and all 426 frozen input hashes
remained unchanged. The fixture now compares exact numeric values using Decimal
while preserving the large-integer digit assertion.

A read-only acceptance audit identified required Namespace-bound gaps in history
epochs/modes, deletion effects on existing handles, and interrupted table-load
recovery/expiry. Added assertions now cover those branches, strengthen table
cursor/diagnostic checks, and use the public revision field `revisionId`.
Architect pre-gate review cleared the corrected fixture after addressing recovery
root lifetime, immutable named-row results, undo arguments and stale-Store error
paths. The decoder and five embedded child-program checks passed (2 focused
tests). These additions have not yet passed installed live acceptance; complete
rerun and fresh whole-stage independent review remain required.

Expanded run `20261028T025901140Z-032153e9` again passed all nine Server
selections (90 tests), wheel groups and both runner probes. Seed stopped at the
new history test (7 passed / 2 skips / 1 failed, 196.75 seconds); Verify did not
start. The client left an expected `HISTORY_UNAVAILABLE` undo unresolved, which
blocked later preparation and made close report `RECOVERY_PENDING`. Read-only
protected-journal metadata and source inspection identified the missing
definitive-error classification. This was an implementation failure, without
timeout; cleanup completed and all 426 frozen input hashes matched.

Architect approved adding exactly `HISTORY_UNAVAILABLE`, `IMPORT_CONFLICT` and
`PLAN_STALE` under the existing trusted/nonretryable/prepared-status guards.
The correction keeps failure persistence before release, committed-result
precedence, and conservative authorization/availability/unknown handling.
Seventeen new history/import coordinator tests passed, alongside 13 existing
Store and 10 recovery coordinator checks. The lost-release case establishes
lifecycle reconciliation; close may finish it before reopen. Diagnostics now
retain bounded redacted causes, and fixture cleanup preserves the primary error
while attempting all closes. Six diagnostics tests and two decoder/child-program
checks passed. Pre-gate implementation review found no blocking findings; full
installed verification and fresh final review remain pending.

Run `20261028T034527544Z-630098fc` stopped before Server startup during wheel
workflow collection: a fixture helper used the live hook's top-level module
path. The package-scoped test import now works without that extra Python path.
All seven workflow modules passed against that installed wheel outside the checkout
(93 tests, 1.81 seconds), with `deposit` verified in `site-packages` and the
packaged Store hash matching the corrected source. Cleanup cancellation tests
also preserve immediate `KeyboardInterrupt` propagation. This focused pass is
not installed live acceptance; the complete gate remains required.

Run `20261028T040004758Z-562185ad` passed wheel groups, runner probes and all
nine Server selections, then reached a different history-fixture error in Seed
(7 passed / 2 skips / 1 failed, 263.30 seconds). It read a Class name from a
snapshot invalidated by a mode change. The fixture now captures generated names
before writes, mode changes and deletion; the same audit corrected subgraph and
Namespace-recreation name reads. Explicit stale-handle assertions remain. Two
installed-wheel decoder/child-program checks passed after the correction. No
timeout occurred; all 427 frozen input hashes matched before this fixture edit.

Remaining: finish the complete main-Python/clean-Server gate with the exact
compatibility pin, obtain fresh whole-stage independent review, finalize
evidence/docs and the tracker, and commit the remaining affected repositories.
No push has occurred. Stage 6 has not begun.


Run `20261028T042428671Z-cbe4488b` passed all wheel groups/probes and all
nine Server selections (90 tests). Seed passed 11 tests with two phase skips
in 349.78 seconds. Verify reported a precise stale-plan fixture assertion
(6 passed / 1 failed, 411.26 seconds): it expected `REVISION_CONFLICT` although
the independent analyzed-plan revision contract returns `PLAN_STALE`. No command
timed out; the corrected phase budget and failure diagnostics functioned.
All 427 frozen inputs matched. Twenty-six reviewed original logs were retained
and hash-verified before reset; owner cleanup completed with no error and no
remaining owner/Java process. The exact assertion now follows the Server contract;
rollback/no-effects and fresh-analysis checks remain. Six installed-wheel
PLAN_STALE coordinator cases and two decoder/child-program checks passed.
Complete live verification, sdist checks and fresh independent review remain pending.


### User-conditioned interruption after recurring acceptance-fixture defects

Run `20261028T044958825Z-6737e11c` passed wheel groups/probes and all nine
Server selections (90 tests). Seed passed 11 tests with two phase skips in
349.10 seconds. Verify reported 6 passed / 1 failed in 444.12 seconds; no
command timed out. The fixture supplied targets for only `code` and `count`
when importing a staged table with additional columns. Server correctly rejected
it with `VALIDATION_FAILED`: every staged column requires an explicit target.
This mapping defect has not been corrected. Sdist checks were not reached.

Architect read-only assessment confirms this is a fixture contract defect, not
evidence of an SDK regression. Individually minor corrections have accumulated
across independent DDict/XLSX representations, stale-handle expectations, error
codes, collection imports and target mappings. Architect agrees interruption
is appropriate under the user's explicit condition about recurring harness
malfunctions. This is not an external-blocker approval: repository remedies
remain available, and no administrator requirement is demonstrated. Earlier
readiness reviews did not catch the mapping defect and were too narrow to prevent
repeated expensive discovery. Process containment, deadlines, diagnostics and
cleanup now function; the original cumulative-budget timeouts are resolved.

Twenty-six reviewed original logs were retained and hash-verified; all 427 frozen
inputs matched before this tracker update. The owner reached `cleaned` with no
cleanup error; its PID is absent, and no Java process remains. Implementation
is interrupted under the user condition. Stage 5 remains verification-incomplete;
no final whole-stage review, Stage 6 advancement or push has occurred. Preserve
all existing changes, component commits and reusable verification dependencies.

On resumption: audit all remaining unexecuted fixture branches against the actual
Server/Hub contracts read-only, correct explicit column mappings, run focused
installed checks through the lifecycle owner, then repeat mandatory cumulative
verification including sdist checks, obtain fresh independent whole-stage review,
finalize the tracker and outstanding repository commits before Stage 6.


### Acceptance-harness repair and separate client regression

The remaining Stage 5 workflow fixtures were audited against the actual Server
and Hub contracts. The staged-table assertions now check the documented `data`
wrapper, literal columns and types, and complete explicit import-target coverage.
The format-import fixture preserves the distinct blank `row-a` and populated
`row-b` optional values. The stale-plan and restore fixtures refresh handles
after the documented invalidation points while retaining their negative conflict
and stale-plan assertions. The fixtures' target mappings and later recovery,
history, deletion and expiry expectations remain unchanged in product semantics.

Focused installed-package live checks completed through the lifecycle owner:
Seed passed 2 selected tests and post-restart Verify passed the same 2 selected
tests (11 tests explicitly deselected in each phase). In the subsequent complete
Stage 5 run `20261028T100312520Z-5b7fc65d`, all wheel groups and probes passed;
the Server suite passed all nine selections (90 tests); Seed passed 11 tests
with two phase-specific skips. Verify passed 10 tests and then exposed a separate
Python client regression in
`src/deposit/client.py::_invoke_client_rows`: the pinned Neo4j 6.4.1 `Record`
uses tuple value-containment, so `column not in record` checks values rather
than result keys and rejects valid rows. The namespace-deletion fixture correctly
expects the deleted UUID to be absent and must not be weakened. No product code
was changed under this bounded harness-repair task.

The complete runner did not reach its later Verify nodes for table-load expiry
and child-program AST validation, nor the subsequent sdist install, import and
test checks. The sdist artifact itself was built. No command timed out. The
lifecycle owner cleaned up successfully, with no remaining owner or Java
process. Architect independently reviewed the harness corrections and evidence,
found no blocking harness defect, and verified all 71 files across the focused,
full-gate inner and outer archives by size and SHA-256 with zero mismatches.

Stage 5 remains verification-incomplete and paused; this record does not mark it
complete or ready to resume. The focused workflow repair is verified, but the
separate client regression must be resolved before the full mandatory gate can
pass. After that, rerun focused installed live checks and the complete Stage 5
gate through post-restart Verify and all sdist checks, then retain and review the
new evidence. Preserve all unrelated edits, pending Stage 5 implementation,
existing commits and verification dependencies. Do not push.


### Final bounded acceptance-harness repair (2026-10-28)

This final entry supersedes the earlier pending-client-regression and incomplete
acceptance-gate status recorded above.

The remaining workflow fixtures were audited against the live Server and Hub
contracts. Three demonstrated fixture assumptions were corrected without
changing Server behavior or weakening assertions: namespace recreation refreshes
the original token's server observation after deletion through a separate
Store token; Verify scratch recovery paths are reserved without ordinary
`mkdir()` so `ProtectedStore` creates them with its current-user-only ACL; and
the staged-table page result is fully consumed so its transactional lease
renewal commits before the strict expiry comparison. The scratch-root check
confirmed the final directories are owned by the lifecycle user's SID.

The installed-wheel Verify also exposed a separate Python client regression:
Neo4j `Record` membership checks values, not result keys. `_invoke_client_rows`
now checks `record.keys()`, with a regression test using a real `neo4j.Record`.
No recovery ACL validation or staged-table expiry contract was relaxed.

Focused lifecycle checks passed for the namespace deletion/recreation flow and
the table-load expiry flow, each with its fresh-runtime setup in both Seed and
post-restart Verify. The final complete Stage 5 runner passed as
`20261028T142602746Z-e8fa2621`; all 27 commands exited 0 with no timeouts. All
nine Server selections passed (90 tests), Seed passed 11 with two
phase-specific skips in 349.84 seconds, and post-restart Verify passed all 13
tests in 1364.04 seconds. Subsequent sdist install, import, and test checks also
passed. The final wheel SHA-256 is
`661EF50C2AA97042EB7AADF4F5759C9B2945FBC3C6C113539C12B68E53716541`; the sdist
SHA-256 is `8521BA54090F0C983ACE7C145E7A8154B0EDBABE0F51EBBC1D96324F8A8E768E`.

Architect independently reviewed the final harness and client changes and the
final evidence, with no blocking findings. All 124 retained final-run files
matched their manifest lengths and SHA-256 hashes. The lifecycle owner reached
`cleaned`, `terminalCleanup=true`, and `cleanupError=null`; no Java process
remained.

This completes the bounded harness-repair task and its acceptance verification.
Project-run Stage 5 remains verification-incomplete and is not marked complete;
the project run may now resume with its other pending Stage 5 work. No push was
performed. Preserve unrelated edits, pending implementation, existing commits,
and verification dependencies.


### Stage 5 resumed fixture and environment update (2026-10-28)

This update records the current plan constraints and current non-elevated test
environment. Historical runner evidence above remains historical; its Server
test selections do not satisfy the current Stage 5 gate, which excludes all
Server-owned tests. No Server-owned test was run for this update.

Stage 5 Python acceptance fixtures were recreated under `test_harness` from
current source and new test files; none were restored from old commits. The
workflow fixture currently has SHA-256
`90906F264E4AC6E47748BB365716AF963C6F6FCECF326DE9BD4B63F983653AFF`. An
independent focused architect review confirmed the reviewed additions after
one row-limit diagnostic expectation was corrected. This is readiness for
focused verification, not whole-stage review or acceptance approval.

Harness-local verification passed `compileall`, collected 52 integration and
recovery tests, and passed 11 focused local runner/contract tests. The complete
unit folder collected 29 tests: 25 passed and four protected-storage/transfer
tests failed before their assertions because DPAPI `CryptProtectData` returns
WinError 2 in this session. Their LocalAppData and pytest outputs were isolated
under `test_harness`; the first attempt without LocalAppData isolation also
encountered an ACL denial at `C:\Users\demjan` and is not treated as product
test evidence. The current process is `DEMJAN-PC\CodexSandboxOffline`, has no
loaded user profile, and the user cannot change this session identity.

Live Stage 5 acceptance and browser verification remain unrun. PowerShell
execution policy is `Restricted`; the authorized runtime install marker is
absent; Playwright provisioning failed with PyPI WinError 10013. Current
read-only source comparison also confirms the unresolved Hub/Server export
link-label mismatch (`storedFilename` versus original filename) and undefined
GeoPackage SRID mismatch (Hub expects absent/`None`; Server returns `-1`). The
public staged-table API provides page data and a continuation cursor but no
lease metadata; Server uses a fixed 24-hour lease and no approved deterministic
public control is available to prove renewal/expiry behavior.

Stage 5 remains verification-incomplete; this entry does not certify 5A-5F,
update the stage state to complete, or authorize Stage 6. No stage completion
commit was created. Resume live acceptance only when the identity, compliant
runtime/provenance and dependency prerequisites are available and the contract
discrepancies are resolved or explicitly decided.


### Stage 5 interruption and architect SRID advice (2026-10-28)

The current session remains `DEMJAN-PC\CodexSandboxOffline`; a direct DPAPI
probe still returns WinError 2, and the user cannot change the session identity.
PowerShell's effective policy remains `Restricted` (all policy scopes are
Undefined), so the runtime launcher cannot be used by bypassing or changing that
policy. The runtime install marker and Playwright package are absent. A previous
Playwright provisioning attempt failed with PyPI WinError 10013. No Server
runtime or browser acceptance was run.

The independent architect advises against normalizing `srid=-1` in the generic
Python `DGeometry` decoder. That path has no GeoPackage provenance; normalization
would also alter ordinary Deposit geometry values while leaving Server-side
GeoPackage staging/import unchanged. Keep the stronger GeoPackage assertion and
resolve the `-1` versus absent-SRID discrepancy in the Server adapter or through
an explicit contract decision. The exported-resource label mismatch likewise
requires the Server owner or an explicit decision; neither Server nor Hub may
be modified under this run's scope.

Stage 5 is interrupted, not complete. Resume after a usable DPAPI profile,
policy-compliant runtime/artifact provenance, Playwright availability, and
resolution of the two contract discrepancies. If lease renewal/expiry remains
in scope, first establish an approved public deterministic control; do not
inspect private Server records or manipulate system time. No Stage 5 completion
commit was created and Stage 6 remains unstarted.


### Stage 5 resumed after access-mode change; Server build interruption (2026-10-28)

The active Codex process now runs as `demjan-pc\demjan`. The full
harness-local unit suite passed: 29 tests. The protected harness admin
credential decrypts under this profile, confirming the prior DPAPI failure was
specific to the previous sandbox identity. PowerShell remains effectively
`Restricted` with all scopes `Undefined`. At the user's direction to try the
authorized launcher, its existing process-scoped `-ExecutionPolicy Bypass`
invocation was used; no persistent execution-policy scope was changed.

The launcher downloaded pinned runtime dependencies only under `test_harness`
and invoked Maven with `-DskipTests`. Maven versioned the current Server source
as `2.0.0-dev.77-post.9.gd493057-SNAPSHOT`, then stopped before compilation or
any test phase. Maven reported that `deposit-server/integration-tests` was
missing. Read-only inspection after failure found that the tracked root
`pom.xml` currently lists only `plugin` and `runtime-probe` (SHA-256
`4C0EB4588249D8055241F3AADFF5AEC4B5AD0489DD85A71D01A089071AB8D583`), while
the ignored current `.git-versioned-pom.xml` lists `integration-tests` (SHA-256
`21B305440CD1943FCD6CAF3207581103A241ABC694D8B921D2DE8296FC1FBD93`). The
effective/versioned model therefore disagrees with the tracked root POM; the
Server owner needs to reconcile the generated build model before retrying. The
`integration-tests` directory is absent. No Server-owned test ran. No
Java/Neo4j process remains, and the runtime installation marker was not
created. Downloaded archives and setup evidence remain in `test_harness`; the
launcher lock handle is closed.

Per the user's interruption condition, stop runtime setup here. Resume only
after the Server owner provides a reconciled current-source build model or an
approved runtime artifact path that does not require reading old commits or
running Server tests. The existing filename-label and undefined-SRID
Server/Hub discrepancies also remain unresolved. Stage 5 is still interrupted;
there is no stage completion commit, and Stage 6 has not started.


### Stage 5 resumed verification and Server contract interruption (2026-10-29)

The Server plugin artifact SHA-256 is
`1B6F2A862EBD02A080DF70D66B20A8C4F7F1D03BB59E2AD4796B4EAF61B03B6E`; it is
installed under `test_harness`, and build metadata matches
`2.0.0-dev.77-post.10.gbe68483-SNAPSHOT`. Python live
acceptance used that runtime as a dependency; no Server-owned test ran and no
test files were restored from old commits. Git-history access was limited to
the explicitly authorized launcher path. The supported launcher was used for
Server lifecycle actions, with process-scoped PowerShell execution-policy
bypass; no persistent policy change was made.

Focused evidence:

- 5A: the runner passed all five selected live acceptance cases.
- 5B: Pattern create/search/form import and invalid-geometry diagnostics passed.
  The required GeoPackage export/import contract fails because undefined SRS is
  returned as `srid: -1` rather than absent/`None`.
- 5C: the runner passed 10 selected live acceptance cases, including recovery,
  package/subtree import, and resource-byte transfer.
- 5D: 12 independent live format checks passed, including CSV/XLSX/GPKG empty
  tables, size limits, diagnostics, and geometry checks. The required full
  tabular path remains blocked: nested `DDict` Integers staged from canonical
  JSON are rejected during import with `TYPE_CONFLICT`, despite the table read
  preserving the exact integer.
- 5E: four independent Python HTTP browser checks passed after correcting
  fixture metadata hydration and same-origin `Origin` headers. The required
  exported-link check fails because Server emits the original filename while
  Hub requires `storedFilename`. The real-browser acceptance was not run; its
  helper has the same strict filename expectation before browser launch.
  Playwright and Edge are available.
- 5F: the runner passed all 12 selected live acceptance cases, including
  Namespace lifecycle, session-scoped administration, recovery, and
  history/versioning workflows.

Run evidence is retained under `test_harness/runs`: 5A
`python-20261028T232046941Z-6e56ff7d`; 5B blocker
`python-20261029T000603112Z-5fc27871`; 5C
`python-20261028T234535593Z-f89e023d`; 5E filename blocker
`python-20261029T002041491Z-d9950b1e`; 5F
`python-20261028T235820371Z-edc2fa01`; 5D independent checks
`python-manual-5D-independent-5a258a4f`; 5E independent HTTP checks
`python-manual-5E-http-a8a038bf`; local unit suite
`python-unit-stage5-resume-beeb35ae`.

The 29-test local Python unit suite passed with per-run `LOCALAPPDATA`, `TEMP`,
and pytest output isolated under `test_harness`. Stage 5 Python changes also
include ID-only selectors for Pattern search/form import, terminal handling of
`LIMIT_EXCEEDED` after confirmed rollback, and fixture corrections for lazy
results, canonical Pattern comments and WKT formatting. XLSX row/column-limit
failures were verified to close cleanly and not leave unresolved actions.

Independent architect review found no blocking issue in the Python Store
changes and approved interruption for the Server-owned blockers. The three
remaining contract defects are: undefined GeoPackage SRS normalization, nested
typed JSON Integer decoding at tabular import, and resource export/attachment
filenames using the original rather than stored filename. Python-only
normalization would weaken or bypass the Server-owned contract and is not an
acceptable remedy. The Server owner must correct these behaviors and provide a
current-source rebuilt runtime before the affected Python acceptance can pass.

Stage 5 is interrupted and incomplete. No cumulative Stage 5 gate, complete
real-browser acceptance, final stage review pass, tracker completion, or stage
commit exists. Stage 6 remains unstarted. Resume after the Server fixes and a
verified replacement runtime are available; then rerun the affected 5B/5D/5E
checks, the full Stage 5 gate, independent review and required commits.

### Stage 5 resumed; source fixes confirmed, runtime artifact pending (2026-10-29)

The current read-only Server checkout is clean at `f6846c98861bae22b31ddccf91a2fe6bbbbe0670`.
An independent architect's source review confirms the three previous contract
defects are corrected in that checkout: undefined GeoPackage SRS maps to no
SRID, canonical nested List/DDict integers are decoded recursively, and exports
use `storedFilename` for resource labels and browser attachments. This is source
evidence only; Python live acceptance has not run against those fixes.

All available plugin copies under `test_harness` still have version
`2.0.0-dev.77-post.10.gbe68483-SNAPSHOT`, SHA-256
`1B6F2A862EBD02A080DF70D66B20A8C4F7F1D03BB59E2AD4796B4EAF61B03B6E`, from
source commit `be68483e05d713f5bc9057cf40d2c566046999fc`. The installed runtime
therefore remains stale. No Server-owned tests ran. A Python 3.14.4 isolated
unit run passed 29 tests (`test_harness/runs/python-unit-resume-20261029T101611761Z`);
focused runner listing for 5B confirmed it selects Python-owned tests only.

The original runtime and Server checkout were preserved. A replacement needs a
current-source artifact with verifiable provenance. Running the launcher from an
isolated current-source snapshot requires disposable Git metadata under
`test_harness`; user direction on that build path or a provenance-backed
replacement artifact is pending. Stage 5 remains incomplete; affected 5B/5D/5E
live checks, real-browser acceptance, cumulative StageGate 5, independent final
review, and stage commits remain outstanding. Stage 6 has not started.


### Stage 5 resumed against current-source Server runtime (2026-10-29)

This update supersedes the artifact-pending notes above for the present
resumption. The user authorized an isolated current-source snapshot under
`test_harness`; its 262 tracked files match the clean Server checkout at
`f6846c98861bae22b31ddccf91a2fe6bbbbe0670`. The isolated launcher built and
installed `2.0.0-dev.77-post.11.gf6846c9-SNAPSHOT` from that source. The
plugin SHA-256 is
`d7643465880868bb5ca53c560f68b8e5f81054a071a9a4fdac2082eb652768fb`.
Maven used `-DskipTests`; no Server tests ran. The original `.10` runtime and
Server checkout were preserved.

Focused Python-owned verification against this runtime:

- **5A:** 5 selected cases passed (`python-20261029T112054249Z-87fd32b0`).
- **5B:** 5 selected cases passed, including strict layout envelope handling
  (`python-20261029T105257113Z-c63e6280`).
- **5C:** 10 selected live cases passed, including import recovery and resource
  transfer (`python-20261029T112541747Z-bfe91ab9`). The runner records
  `server_tests_invoked: false`.
- **5D:** the required blank-cell reuse assertion fails: applying a staged row
  with an empty value creates a duplicate (`createdObjects == 1`, expected 0).
  Supplemental live checks reproduced the same failure for CSV, XLSX and
  GeoPackage; 13 other independent format checks passed. Evidence is under
  `test_harness/runs/stage5-resume-f6846c9/test_harness/runs/`.
- **5E:** 8 selected checks passed, including browser/authentication and
  download flows (`python-20261029T104756077Z-077859e3`). The unavailable
  listener/no-publication branch remains unverified because the supported
  runtime exposes no way to disable that listener while keeping Bolt available
  or publicly observe whether a failed export published output/grant state.
- **5F:** the existing 12 lifecycle/history cases passed. A separate
  public-API expiry seed passed and persists only IDs/timestamps/version at
  `test_harness/runs/stage5-resume-f6846c9/test_harness/runs/stage5-expiry/expiry-state.json`.
  Do not reset or delete this state. Its natural-expiry verification must wait
  until at least `2026-10-30 11:21:42 UTC`; it will then assert table, plan and
  export access expire through public calls.
- The full Python unit folder passed 34/34. The first direct invocation failed
  during collection because its `PYTHONPATH` was omitted; the corrected run
  supplied the source path and passed all tests. The expiry test collected as
  one test after its teardown was corrected so verify-phase setup failures
  preserve seeded state.

Fresh independent architect review returned **CHANGES REQUIRED**. It found no
further product-code correction. The 5F fixture teardown finding was corrected
and accepted on review: verification cleanup remains disabled until every
expiry assertion passes. The confirmed Server defect is tabular matching:
Server matching must ignore missing, null and empty staged-table cells as keys,
preserve existing values, and continue treating whitespace-only strings as
supplied values while preserving ambiguity and atomicity behavior.

The user clarified that functionality needed only to test a behavior belongs in
this implementation. The documented live 5E behavior remains that a
resource-bearing export fails without publishing a resource or grant when the
authenticated download service is unavailable. Current evidence does not show
that Server violates this behavior, so no Server code change is required for
5E. The listener-failure simulation and publication observation belong in the
Python test harness; the 5E failure path remains unverified until that harness
coverage is implemented and run.

The cumulative StageGate 5 and 5F natural-expiry verification remain pending.
Stage 5 is interrupted/incomplete and has no completion commit. Stage 6 depends
on finalized Stage 5 and has not started. Resume after the natural-expiry
window, then run the expiry verification and cumulative gate; completion still
requires the two Server-side items above, a rebuilt matching runtime, final
verification/review and separate stage commits. No push was performed.


### Stage 5 resumed: listener harness correction and runtime evidence (2026-10-29)

This entry supersedes earlier Stage 5 statements that the 5E listener-failure
branch was unverified; the current focused run covers that branch and cleanup.

The Stage 5 listener harness corrections are implemented under
`test_harness/deposit-python`. The runner now fails closed on unknown child exit
status, confirms process-tree termination before lifecycle cleanup, refuses to
restart after an unsuccessful stop, preserves a prior unconfirmed-process state,
and skips post-cleanup bootstrap/download checks unless normal runtime restoration
and `JAVA_TOOL_OPTIONS` restoration both succeeded. The diagnostic readiness
check requires a fresh record; the prior readiness file is retained, but its port
is not listening. The Server launcher wrapper propagates thrown failures and
native exit codes. Windows PowerShell 5.1 parser checks passed on four scripts.
Runner contract tests passed 9/9, including injected monitoring, cleanup-stop,
post-cleanup validation, launcher throw and exit-code cases. Subprocess probes
passed for exit 0/7, inherited stdout/stderr logging without hanging, and timeout
process-tree termination.

Focused 5E passed against the isolated `.12` runtime: 8 ordinary Python live
cases, the unavailable-listener/no-publication case, and the post-cleanup resource
download passed. All 11 recorded commands exited 0 without timeout; bootstrap
passed before and after the diagnostic run, normal `.12` metadata was preserved,
the diagnostic listener port was closed, and `server_tests_invoked` was false.
Evidence: `test_harness/runs/stage5-server-5123588/test_harness/runs/python-20261029T145815394Z-fdf11465`.
The focused runner smoke on the default paths also passed local contracts and its
live smoke (`test_harness/runs/python-20261029T150952697Z-9859dd61`).

The user reported that the Server fix is in place. The available isolated runtime
and its source snapshot do not contain that fix: the clean local Server checkout
and snapshot remain at `5123588265906aa974f13def4484703411963454`, and the matching
runtime is `2.0.0-dev.77-post.12.g5123588-SNAPSHOT`. Focused Python 5D against that
runtime failed at `deposit.save.csv` with `TYPE_CONFLICT: Value does not match
Integer` for the canonical nested Integer value. Evidence is in
`test_harness/runs/stage5-server-5123588/test_harness/runs/python-20261029T142733332Z-2de972e3`.
Earlier focused format probes against the same source also reproduced blank-cell
matching failures. No Python normalization or fixture relaxation is acceptable;
the architect found no Python/harness remedy. No Server-owned tests ran.

The preserved 5F natural-expiry seed remains tied to `.12` at
`test_harness/runs/stage5-server-5123588/test_harness/runs/stage5-expiry/expiry-state.json`
(SHA-256 `2F3DB1128C9A7030087431E81751E32808C083A295B82D1797DBCED584897052`).
Do not modify or recreate it. Its earliest verification time is
`2026-10-30 12:45:39 UTC`.

The independent architect reviewed the final harness corrections and marked them
ready. The architect approved an incomplete-work handoff for a corrected,
provenance-matching Server source/runtime artifact and the pending natural-expiry
window. Stage 5 remains incomplete; the mandatory cumulative StageGate 5 and
whole-stage final review remain pending. Stage 6 has not started. No stage
completion commit was made and no push occurred.

### Stage 5 final verification and review (2026-10-30; finalization pending)

This entry supersedes the earlier Stage 5 interruption and natural-expiry-pending
statements. The corrected Server artifact was built from the user-reported fix;
the Python manifest now binds strictly to
`2.0.0-dev.77-post.15.g7610e45-SNAPSHOT`. Exact version matching remains enabled.
The gate records the installed plugin SHA-256
`fd0469c152b8495260f69facffb2c3ff7abb8157fb723919f946a23a349be09b` and records
`server_tests_invoked=false`.

The mandatory cumulative StageGate 5 passed in run
`20261029T214350498Z-b255b8a0`; all 21 recorded commands exited 0, no command
timed out, and the failure list is empty. Python 3.14.4 results: local tests
73 passed / 3 deselected; ordinary live acceptance 68 passed / 1 deselected;
the listener-failure case passed (1), post-listener resource download passed
(1), and the dedicated accelerated-expiry case passed (1). Wheel and sdist
installed-package suites each passed 2 tests. The expiry case is deliberately
deselected from ordinary live acceptance and run once through its instrumented
clock path. No Server-owned tests ran. Wheel SHA-256 is
`6a2543ab9f3067d008058514e71b15fbc9224e51262d7597a8bf55fb2cfb0f56`; sdist
SHA-256 is `d083fdd3e39b21af24f1a8f2de1b868b6c6010cf1950e9d257eaa3a95e9ea9b8`.
Full command, timeout, test-result and cleanup evidence is retained at
`../test_harness/runs/stage5-clean-current-20261029/test_harness/runs/python-20261029T214350498Z-b255b8a0/results.json`.
The gate ran from `2026-10-29T21:43:50Z` to `2026-10-29T23:22:34Z` with the
explicit 7200-second runner bound. An earlier default-bound attempt timed out
after 30 minutes at 52% without a test failure; the completed longer-bound run
passed. Focused Stage 5 cases also passed in
`20261029T210210215Z-4226c809`.

Stage 5F used the harness-owned accelerated clock and passed the public expiry,
renewal, pinned-plan, reauthentication and accessible-control assertions. The
fresh architect confirmed that this satisfies the approved plan's accelerated
expiry requirement. The historical `.12` natural-expiry seed remains preserved
as historical evidence; it is not current acceptance and no real-time wait is
required. The architect reviewed current production and harness changes, the
workflow assertions, clock-agent provenance and cleanup, the result record and
JUnit reports, and returned READY with no blocking findings. Review was
read-only; no tests were rerun by the reviewer and no Server-owned tests ran.

The runner restored the ordinary runtime after both instrumented checks. After
the gate completed, the remaining harness-owned normal Neo4j runtime on
`127.0.0.1:17770` was stopped with the supported Server lifecycle script; its
process tree and listener were confirmed absent. Stage 5 tracker update and
separate Python/test-harness stage commits remain pending finalization.
