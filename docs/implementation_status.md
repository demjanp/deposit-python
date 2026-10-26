# Deposit Python 2 implementation status

Updated: 2026-10-26.

## Authority and baseline

Plan: [implementation_plan.md](implementation_plan.md).
Design input: [implementation_proposal.md](implementation_proposal.md).
This tracker covers the Python wrapper and its coordinated Stage 1 server/hub
changes; it is not the server implementation status in the Deposit Hub. Stage
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
| 2 | Safe core and durable recovery | Not started | 2A-2F pending | Not run / no implementation review | No stage commits |
| 3 | Complete procedure gateway | Not started | 3A-3E pending | Not run / no implementation review | No stage commits |
| 4 | Typed domain, queries and batches | Not started | 4A-4E pending | Not run / no implementation review | No stage commits |
| 5 | Resources and extended workflows | Not started | 5A-5F pending | Not run / no implementation review | No stage commits |
| 6 | NetworkX and release acceptance | Not started | 6A-6E pending | Not run / no implementation review | No stage commits |

Allowed states: Not started; Planning; Implementing; Verification incomplete;
Review corrections; Verified and reviewed (finalization pending); Finalized;
Interrupted (with exact reason and resumption criteria). A finalized stage needs
all acceptance evidence, no blocking review findings, a tracker update and all
required commits. Preserve completed cross-repository commits if later
finalization fails; resume pending work without duplicating commits.

## Acceptance and procedure coverage

Stage 1 acceptance IDs 1A-1E are verified and reviewed as recorded below. Stages
2-6 remain pending. Record separate states for supported dispatch, convenient
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
- Hub: `0cc09a5b34ccb83861f005d53e9797ce7df11124`.
- Python source and this tracker are included in the tracker-bearing Stage 1 commit; its full hash is recorded in the local project-run checkpoint and Git history.

Stage 2 is now unblocked.

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