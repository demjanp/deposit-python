# Stage 1 acceptance assertion map

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
