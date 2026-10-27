# Acceptance assertion map

This map separates declaration parity, Python behavior, and live integration
evidence. Server catalog parity or Java tests alone do not establish Python
acceptance. Commands below are owned by `scripts/Invoke-PythonTests.ps1`; a
selection is not complete until the runner retains its output and result record.
Per-procedure dispatch, convenience, and integration evidence is tracked
independently in [procedure_coverage_ledger.md](../docs/procedure_coverage_ledger.md).

| ID | Assertion and concrete test | Command/group | Prerequisite / current limit |
|---|---|---|---|
| 1A | `test_installed_import_is_out_of_tree_and_2x`: the runner builds a wheel and sdist, installs each in an isolated environment, then checks the 2.x identity, out-of-tree import, packaged manifest, and absent core NetworkX dependency. | `Invoke-PythonTests.ps1 -Stage 1 -Group foundation` | Windows Python 3.14.4; pinned dependencies installed. Wheel and sdist install/import/package gates passed in final-source run `20261026T010158409Z-2e0e0609` (result SHA256 `D19312BBDB912E745E8CBA23CFC92E8DA5934F548A84DCE4604F33EF44235801`). |
| 1B | `test_checked_manifest_is_current_and_complete`: 123 catalog declarations plus `deposit.get.info`; exact registration names, argument order, result mapper/columns, scope/action metadata, batch allowlist, and lifecycle owner. `test_unsupported_server_fails_before_login_call` and `test_unsupported_neo4j_server_fails_before_login_call` cover version gates. | `Invoke-PythonTests.ps1 -Stage 1 -Group foundation` | Unknown/mismatched metadata is a hard generation failure. Manifest tests passed against final-source run `20261026T010158409Z-2e0e0609`; see [procedure ledger](../docs/procedure_coverage_ledger.md). |
| 1C | `test_fresh_runtime_discovery_bootstrap_auth_session_and_logout`: fresh harness runtime discovery, bootstrap, valid and invalid Deposit authentication, session profile, Namespace lifecycle, logout, caller-owned driver remains usable, and no empty result is accepted. | `Invoke-PythonTests.ps1 -Stage 1 -Group foundation` | Installed-wheel smoke passed in final-source run `20261026T010158409Z-2e0e0609`, through suite-owned Provision/Install/Start/Stop, then Start/Stop identity verification. Live logs are under `test_harness/results/20261026T010305840Z-1678f00a`. The smoke uses the installed wheel from outside the checkout. |
| 1D | `test_round_trip_is_dpapi_protected_and_atomic`, `test_write_flush_failure_preserves_prior_record`, `test_native_dpapi_denial_preserves_existing_ciphertext_and_never_writes_plaintext`, `test_anonymous_token_is_denied_and_owner_data_remains_decryptable`, and `test_cross_process_lock_contention_and_release`; identity extraction, missing/denied metadata, retargeting, and retained-ID copy limitation are covered in `test_identity.py`. | `Invoke-PythonTests.ps1 -Stage 1 -Group foundation`; live identity Seed/Verify in the same run | Windows Python 3.14.4, DPAPI and ACLs; database principal must be authorized for `db.info()` and `dbms.info()` in `system`. Ten storage and six identity tests passed. The native denial check uses an anonymous Windows token, not a second authenticated user. Live Seed captured the target `db.info` ID/name, system `dbms.info` ID, actor, Namespace and endpoint; owner-managed Stop/Start Verify confirmed the full identity object was unchanged. Copy/restore may preserve native IDs; this accepted limitation is not deployment-generation attestation. |
| 1E | `Test-PythonRunner.ps1`: child exit code, timeout propagation, bounded capture, redaction and process-tree cleanup. Includes a parent exiting 0/23 while a pipe-inheriting descendant is alive; injected job-config/assignment/resume failures; and unrelated sentinel preservation. | `Invoke-PythonTests.ps1 -Stage 1 -Group foundation` | Job Object runner probes and bundle probes passed in final-source run `20261026T010158409Z-2e0e0609`; full live hook completed and suite journal reached terminal cleanup. Fresh independent review remains required before the stage can be finalized. |

## Contract and identity notes

The manifest records procedure arguments, result columns, public invocation
owner, Neo4j mode, scope, explicit action classification, and batch eligibility
derived from the server's `DomainService.COMMANDS` allowlist. Discovery is the
additional no-token Client route. It does not itself declare convenience-method
behavior for later stages.

Recovery deployment identity binds the target database's native `db.info().id`
and database name, the system-store `dbms.info().id`, the caller-supplied
credential-free resolved Bolt endpoint, Deposit actor ID, and immutable
Namespace UUID. Missing metadata, inaccessible system metadata, endpoint or
database retargeting, and changed identity fail closed. The URI constrains the
connection but is not identity proof. A copied/restored store may retain these
native IDs and therefore cannot be distinguished from the source by this
mechanism; this limitation is accepted and no server deployment-generation
field is added.

## Stage 2 acceptance map

Stage 2 uses the cumulative `-Stage 2 -Group core` selection. It rebuilds and
installs the wheel and sdist, runs the Stage 1 foundation checks and the Stage 2
core unit tests against both installed artifacts, then invokes the existing
server-owned Python Core hook. The hook runs Stage 1 Smoke plus the Stage 2
process-death, authority and cancellation scenarios; the existing lifecycle
owner performs the selected Restart or KillRestart transition between Seed and
Verify. Only retained successful run records establish these gates.

| ID | Assertion and concrete test | Command/group | Current evidence / limit |
|---|---|---|---|
| 2A | Unit `test_two_stores_are_independent_and_create_read_is_portable`, `test_store_rejects_public_token_and_namespace_substitution`, plus Client driver-ownership tests; installed Core renames a Namespace and reads a previously created Class by UUID. | Full cumulative runs `20261026T161936350Z-c10e1603` (Restart) and `20261026T162540049Z-b609525c` (KillRestart) | Passed. Unit tests verify separate sessions, caller/internal driver ownership, rejection of token/Namespace arguments and actor/scope mismatch before action calls. Live rename verifies UUID binding. Live six-Namespace recovery also verifies that opening/recovering one Store leaves other Namespace journal/server states unchanged and checks result/entity Namespace UUIDs; it does not claim a caller-supplied live retarget request. |
| 2B | Unit lost prepare/execute/status/release reply tests, including a successful server release with a dropped reply, exact saved-result recovery from committed status, and unexpected `NOT_FOUND` preservation without replay; installed child terminates at six action boundaries (before/after prepare, before/after execute, after durable result, after release). Seed asserts each unresolved local/server pre-state before transition; Verify checks states before recovery, one effect, distinct entity IDs and original portable results. | Both final cumulative runs above | Passed under both owner-managed Restart and KillRestart. Seed/Verify each run installed Core as `2 passed, 2 skipped` / `4 passed`. A successful lost release reply is reconciled against the durable committed record; unexpected `NOT_FOUND` retains the pending record and result without asserting release. Actual client process death and committed receipts crossing both server transitions are live. |
| 2C | Unit `test_conflict_preserves_pending_inputs_and_refresh_allows_explicit_reapplication`, `test_missing_preparation_with_changed_revision_is_never_recreated`; Store reads preserve the observed baseline. Installed Core retrieves the original Class result after later Class deletion and Namespace deletion. | Both final cumulative runs; wheel/sdist Core tests and installed Core Verify | Passed for conflict preservation, no fresh key after changed preconditions, polling/read baseline behavior, explicit refresh/reapply, and retained result after deletion. |
| 2D | Live authority test covers wrong/old credentials, password revocation and reauthentication, disabled user/restore, permission downgrade/restore, and a valid wrong actor against a pending handle. Unit `test_coordinator_identity_or_credential_mismatch_preserves_pending_without_action_calls` and `test_recovery_rejects_configured_database_mismatch_before_auth_or_action` exercise identity mismatches, including an accessible recorded database while the Client targets another. Unit deletion-receipt tests resolve a completed Namespace deletion without selecting that Namespace. | Both final cumulative runs; supporting server classes `PreparedActionReplayIT` and `AccountAuthorizationIT` passed | Passed for the listed cases. The live six-Namespace fixture verifies journal/server states and result/entity UUIDs remain bound during scoped recovery; the API has no Namespace target selector, so no live retarget request is claimed. Endpoint/database/native-ID replacement cases are unit call-spy evidence only. Completed Namespace deletion receipt is unit-covered. Prior-epoch committed/uncommitted server-scope checks pass live under both transitions; old token is invalidated. |
| 2E | Unit journal corruption, duplicate/nonstandard JSON, unknown version, invalid UUID and retention preservation, lossless nested-map input, combined-root byte/unresolved quotas, concurrent cross-root writes, quota-index failures, outcome-write failure and process-lock owner-death tests; a separate-process lock holder blocks Namespace-deletion receipt status calls; live two-process race recovers the same committed handle; live close/cancel blocks the reply after independently confirming the server effect. | Both final cumulative runs | Passed. The Class race had one process recover the original result and one receive expected fail-fast lock contention; one Class existed. The deletion-receipt test used an independent OS process holding the same action lock and proved no status/resume/release calls by the loser. Live close/cancel joined both threads, preserved the original result and observed one effect. Unsupported/corrupt records remain intact across enumeration, accounting and write-triggered pruning. Non-string nested map keys fail before journal/server calls. |
| 2F | Installed Core crosses real Python child termination with owner-managed Restart and KillRestart. Verify checks native identity, expired old token, prior-epoch committed receipt/result and uncommitted prepared action conflict with no new Namespace effect. | Both final cumulative runs; server action-recovery seed/transition/verifier also ran under both transitions | Passed for both transition modes. Each cumulative run recorded Seed `2 passed, 2 skipped`, Verify `4 passed`; independent live queries verified committed result, preserved prepared state on `REVISION_CONFLICT`, and zero effect for the uncommitted old-epoch action. Supporting server `ActionReplayRecoveryIT#seed` and `#verify` each passed under Restart and KillRestart. |

The final installed-package gates ran through
`stage2-tagged-core-wrapper.ps1 -Mode Full -Transition Restart|KillRestart`,
which invokes `Invoke-PythonTests.ps1 -Stage 2 -Group core` with the verified
clean-tag server bundle and owner-managed live transition. Both runs completed
21 commands with zero failures or timeouts. Restart run
`20261026T161936350Z-c10e1603` has result-record SHA-256
`60EDD22ECF5534B5757DCFE93CC0F70ABC218677EB1B0A49196026038A884E3E`;
KillRestart run `20261026T162540049Z-b609525c` has result-record SHA-256
`C662CF857C4778E856783526C3BC6A114DAED36B0FAC267025A0C908FBB4373E`. Both
wheel and sdist package checks passed (24 each); wheel manifest passed (3),
storage passed (14), identity passed (6), and wheel/sdist core passed (68 each).
Live Seed/Verify each reported `2 passed, 2 skipped` / `4 passed`. Final result
records and nested logs are retained under
`test_harness/tools/python-tests/stage2-server-evidence-20261026-01/Final-Restart-20261026T161936350Z-c10e1603/`
and `Final-KillRestart-20261026T162540049Z-b609525c/`; the protected
`python-stage2-recovery` runtime directory was excluded.

Focused source-tree verification after corrections: quota/release regressions
passed (12); strict journal/input selection passed (63, with one ACL subprocess
case deselected); the added invalid-key matrix passed all four access paths.
An earlier broad direct three-file pytest attempt had 41 passed and 8 ACL failures
because the workspace ACL also exposed SYSTEM, Administrators and OWNER RIGHTS;
the unchanged owner-only validator rejected these entries. The owner-managed
wheel/sdist Stage 2 selections passed the corresponding official storage, package
and core tests. Focused logs and that exploratory failure log are retained in
`test_harness/tools/python-tests/stage2-server-evidence-20261026-01/python-focused-evidence/`.

The selected supporting server tests also passed and are preserved under
`test_harness/tools/python-tests/stage2-server-evidence-20261026-01/`:
`PreparedActionReplayIT` (27 tests), `AccountAuthorizationIT` (15 tests), and
`ActionReplayRecoveryIT#seed`/`#verify` (one test each under both Restart and
KillRestart). The quota-index cleanup audit is in `quota-registry-cleanup.json`.

All runs used the verified `2.0.0-dev.77` bundle at source tag
`c1489c18b72c4a98d8bfb46a3926fe8195b3e2e6` (bundle SHA-256
`75f78b4d3ef57d2b03c201906a6be2826dee4bc7f308e60d00ef5fff59163044`). The
reversible 22-file overlay manifests are under
`test_harness/tools/python-tests/stage2-tagged-overlay-audit/Full-20261026T161936350Z-c10e1603/`
and `Full-20261026T162540049Z-b609525c/`. Each restoration audit verified
the complete 56-file script baseline, exact HEAD/tag, clean Git status, absent
overlay-only paths and unchanged bundle hash, with zero restoration failures.
Both restoration report SHA-256 values are
`9329C69D0CDA0E5618FE35793A3C29ED5C9D9152D92B32D313A427356FD1C17A`.

## Stage 3 acceptance map

Stage 3 ran the cumulative `-Stage 3` selection against an installed wheel and
sdist plus the owner-managed live Gateway suite. The retained run
`20261026T214740608Z-6a36ef3f` recorded 23 commands, all exit code 0 with no
timeouts. Wheel and sdist each passed 47 gateway tests and 94 core tests; package
tests passed 42 each, manifest 3, storage 14 and identity 6. Live Seed passed 3
tests with 2 expected skips; Verify after the server restart passed 5 tests. The
live run reached terminal cleanup with no cleanup error. Full result JSON is
retained at
`../test_harness/results/python-20261026T214740608Z-6a36ef3f/results.json` and
live logs at
`../test_harness/results/20261026T214902605Z-b04be0db/`.

| ID | Assertion and concrete test | Command/group | Current evidence / limit |
|---|---|---|---|
| 3A | `test_every_manifest_entry_has_one_explicit_gateway_or_deferred_policy`, `test_manifest_binding_order_defaults_and_named_outputs_are_preserved`, `test_every_operational_manifest_entry_has_exact_store_gateway_classification`, and `test_prune_execution_is_rejected_before_any_coordinator_route` cover all 124 manifest entries, ordered arguments, outputs, ownership, actions, batches, internal routes and explicit deferrals. | `Invoke-PythonTests.ps1 -Stage 3`; wheel and sdist gateway selections | Passed in the installed wheel and sdist (47 gateway tests each). Prune dry-run and both execution branches are classified; live viewer denial is exercised in Seed. |
| 3B | `tests/unit/test_client.py` verifies required selectors and zero/one/many detached tuple results for `get_users` and `get_namespaces`; installed Gateway Seed covers Client discovery/session and Namespace/user calls, active Namespace selection, administrative mode, Store class/classRelationship create/read, empty classRelationship page, safe invalid selector, permission denial, dry-run prune denial, and prepared operational `save.data` export plus `import.analyze` with named portable outcomes. | Focused selector/cardinality selection; owner-managed `SelectedPythonTest Gateway`, run `20261026T214902605Z-b04be0db` | Eight focused API assertions passed. Seed passed 3 tests / 2 skipped. Verify after restart passed 5 tests and recovered the committed Class result. Private `_single` calls only create test fixtures and are not counted as supported routes. Other non-deferred procedures remain assigned in the [procedure ledger](../docs/procedure_coverage_ledger.md). |
| 3C | `test_advanced_store_result_native_values_are_transaction_bound_and_convert_in_owner_tx`, `test_advanced_store_result_converts_dynamic_relationship_and_rejects_paths`, `test_advanced_store_result_does_not_expose_graph_or_lazy_native_iterators_after_close`, plus early-close, conversion-failure and iteration-failure tests cover native lifetime and owning-transaction conversion. | Cumulative wheel/sdist gateway selections; live Gateway Seed | Passed; 44 focused Store/policy tests also passed after the correction. Live Node and dynamically typed Relationship converted through the same transaction and Store token. `.graph` is rejected, iterator advancement checks lifetime, property views detach while open, and Path conversion fails. Same transaction does not promise a frozen read snapshot. |
| 3D | Live Gateway holds a query result open after its first row, lists the active request while Store work remains open, denies cancellation by another actor, cancels as the owner and confirms request removal after Bolt termination. Existing Stage 2 `test_live_close_cancels_after_server_commit_without_deadlock_or_duplicate_effect` covers ambiguous committed action completion. | Live Gateway Seed plus Stage 2 cumulative Restart and KillRestart runs; server `PreparedActionReplayIT` and `AccountAuthorizationIT` regressions | Passed. The live active query uses `fetch_size=1` to avoid driver-side buffering. Neo4j can surface transaction cancellation as a generic `TransportError`; the test also verifies authorized cancellation returned true and the request disappeared. Stage 2 live action cancellation preserved one committed effect and original result. |
| 3E | Every manifest row separates dispatch policy, supported Python route, executed integration evidence and the remaining specialized scenario assigned to a later stage/test target. Approved Client server-scope action and prune-execution deferrals are explicit. | `docs/procedure_coverage_ledger.md`; `test_gateway_policy.py` | 124 ledger rows correspond to manifest entries. Stage 4/5/6 targets identify future coverage; those targets are planned and are not counted as completed tests. |

Failure/correction record: the first reviewer found graph/lazy-iterator lifetime
escapes, stale ledger entries and missing installed operational/cancellation
coverage. The graph escape probe reproduced access after StoreResult close; the
wrapper now hides `.graph`, guards every iterator step, and detaches lazy property
views while open. One live cancellation attempt completed from driver-buffered
rows; it was changed to a one-record fetch size. The next attempt used disconnected
two-class projections and produced no rows; it was corrected to a single-class
projection with objects selected by class UUID. The final cumulative gate passed
after these corrections. Earlier failed run records are retained and were not
counted as acceptance evidence. A direct interactive run of `test_client.py`
also encountered one ACL-specific subprocess setup failure because the current
workspace grants extra Windows principals; 46 other selected cases passed.
The owner-managed Stage 3 wheel/sdist gate passed the complete package/core/
gateway selections, including this test, so the interactive failure is not
counted as a product regression.

## Stage 4 acceptance map

Stage 4 ran the cumulative `-Stage 4 -Group domain` selection against installed
wheel and sdist artifacts plus the owner-managed live Domain workflow. Final run
`20261027T134920925Z-17cf7ecc` recorded 25 commands, all exit code 0 with no
timeouts. Wheel tests passed: package 43, manifest 3, storage 14, identity 6,
core 115, gateway 66 and domain 133. Sdist tests passed: package 43, core 115,
gateway 66 and domain 133. Installed Seed passed 4 tests with 2 expected skips;
Verify after the owner-managed Stop/Start passed 6 tests. The retained result
record SHA-256 is
`FAE7F1E6B16259E27E472AF81BE9FADE38927B787E9DD5E4F227B6527D48FD4B`.

| ID | Assertion and concrete test | Command/group | Current evidence / limit |
|---|---|---|---|
| 4A | `test_integer_bounds_float_fidelity_and_nonfinite_rejection`, temporal precision and naive local temporal codec tests, `test_duration_and_point_preserve_calendar_months_and_srid`, nested-list/DDict null tests, and the installed wire assertions in `test_installed_domain_values_snapshots_pages_and_batch_recovery`. | `Invoke-PythonTests.ps1 -Stage 4 -Group domain`; wheel and sdist value/domain selections; installed Domain Seed | Passed. Includes signed 64-bit boundaries, finite floats, nanosecond/offset values, local built-in time/datetime, negative/calendar duration, SRID, typed nulls, nested and empty/all-null lists, plus independent wire values. |
| 4B | `test_object_handle_distinguishes_missing_from_explicit_null_and_uses_checked_schema`, `test_stale_handle_preserves_edit_until_explicit_refresh_and_reapply`, `test_class_handle_declarations_are_detached_and_id_stable`, batch class Descriptor type/rename invalidation tests, and live schema-conversion/declaration assertions. | Wheel/sdist entity, batch, store and value tests; installed Domain Seed | Passed. Missing/default/unknown remain distinct; detached edits require assignment; schema conversion and concurrent revision conflict do not decode against a newer unrelated declaration. |
| 4C | `test_handles_are_bound_to_exact_store_and_namespace_identity`, `test_domain_convenience_routes_use_manifest_selector_shapes`, and installed create/read/update/delete, foreign Store, stale edit and explicit refresh checks in `test_installed_domain_values_snapshots_pages_and_batch_recovery`. | Wheel/sdist entity/store selections; installed Domain Seed | Passed for representative Class/Object/Descriptor CRUD and selectors. Specialized procedure coverage remains in [procedure_coverage_ledger.md](../docs/procedure_coverage_ledger.md). |
| 4D | `tests/unit/test_pages.py` covers query presence, empty/multiple pages, cursor validation, early close, revision fences during snapshot/page collection and after buffered/final-page transforms, descriptor schema revision, neighbor direction consistency and iterator closure. `test_installed_domain_values_snapshots_pages_and_batch_recovery` covers live page/query behavior. | Wheel/sdist pages/store/domain selections; installed Domain Seed | Passed. Cursor scope/revision rejection and no-mixed-revision checks are covered. Revision fences conservatively reject any Namespace sequence advance. |
| 4E | `test_collection_failure_submits_nothing_and_references_are_batch_local`, `test_batch_revalidates_the_whole_request_before_submission`, `test_batch_exact_result_is_saved_and_recovered_after_lost_release_reply`, and installed Seed→Stop/Start→Verify lost-coordinator-reply recovery in `test_installed_domain_values_snapshots_pages_and_batch_recovery`. | Wheel/sdist batch/store/domain selections; installed Domain Verify after restart | Passed. Earlier-result references, rollback, limits and exact saved outcomes are checked. The live child executes the real batch and exits after the server reply returns but before the coordinator receives it; `Client.recover` returns the original class/object/Descriptor results, and persisted effects are independently checked. This is a coordinator-visible reply-loss injection, not a physical network fault. |

Failure/correction record: an initial build-bundle probe treated Git checkout
progress text on stderr as a terminating PowerShell 5.1 error; delaying optional
Git progress for the gate process let the probe pass. Two early live attempts
exposed test defects (the driver's nanosecond-formatted temporal string and a
duplicate close of a cleared Store variable); both were corrected before the
final pass. Those two live failures were acceptance-test issues, not
implementation regressions. The suite owner reported `cleaned`,
`terminalCleanup=true`, and `cleanupError=null` after each run. Reviewed
redacted result JSON and live Seed/Verify summaries are retained at
`../../test_harness/results/python-20261027T134920925Z-17cf7ecc/results.json`
and `../../test_harness/results/20261027T135046795Z-d3d67315/`.
