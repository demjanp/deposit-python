# Deposit Python 2 implementation status

Updated: 2026-10-27.

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
| 5 | Resources and extended workflows | Not started | 5A-5F pending | Not run / no implementation review | No stage commits |
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
Stages 5-6 remain pending. Record separate states for supported dispatch, convenient
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
