# Acceptance map

This map follows the IDs in `implementation_plan.md`. Stage 1 test paths below
are concrete assertion targets; later-stage IDs are pending and their runner
groups do not exist yet. Commands shown for future stages describe planned gate
selection only and must not be read as runnable tests today.

| ID | Required evidence | Assertion or planned test target | Command / state |
|---|---|---|---|
| 1A | Isolated wheel and sdist import, version and no source fallback | `tests/unit/test_installed_package.py`; installed path assertions in package and live tests | Wheel and sdist install/import/package gates passed on Python 3.14.4 in `20261026T010158409Z-2e0e0609`; result JSON SHA256 `D19312BBDB912E745E8CBA23CFC92E8DA5934F548A84DCE4604F33EF44235801` |
| 1B | Catalog/discovery parity including args, columns, action, lifecycle and exclusions | `tests/unit/test_manifest.py`; server `ProcedureCatalogMetadataTest`; `PreparedActionReplayIT.registeredCatalogMatchesNamesModesArgumentsAndColumns` | Python manifest tests 3 passed; server metadata tests 2 passed; integration tests including `PreparedActionReplayIT` compiled. Logs under `test_harness/cache/project-run-evidence/deposit-python-20261025-01/server-regressions-final` |
| 1C | Live discovery, auth success/failure, session inspection and logout via installed package | `tests/integration/test_live_smoke.py` assertions and retained live smoke journal | Installed-wheel Seed and post-restart Verify passed through the suite owner in `20261026T010158409Z-2e0e0609`; live logs under `test_harness/results/20261026T010305840Z-1678f00a` |
| 1D | DPAPI/ACL protection, identity binding, atomic update, contention and failure preservation | `tests/unit/test_protected_storage.py`, `tests/unit/test_identity.py`, native anonymous-token probe; live native ID extraction/restart equality in smoke | Ten storage and six identity tests passed; native anonymous-token denial preserved owner data; live Seed/Verify confirmed target/system IDs and bound identity. Retention/journal state-machine tests are Stage 2 |
| 1E | Runner failure/timeout propagation, redaction, harness cleanup and installed hook | `scripts/Test-PythonRunner.ps1` covers exiting-parent descendants and injected setup failures; live hook evidence/results and server helper journal tests | Job Object runner probes, bundle probes and installed live hook passed in `20261026T010158409Z-2e0e0609`; independent architect review passed with no blocking findings |
| 2A | Independent Stores, create/read, driver ownership and Namespace binding | Planned tests: `tests/unit/test_store_lifecycle.py`, `tests/integration/test_core_workflows.py` | Pending; planned `Invoke-PythonTests.ps1 -Stage 2 -Group core` |
| 2B | Lost replies/process death produce one effect and original result | Planned journal transition and client-restart integration tests; server prepared-action barrier | Pending; planned Stage 2 core/restart gate |
| 2C | Conflict preserves edits/arguments; polling does not move baseline | Planned `tests/integration/test_conflicts.py` assertions | Pending; planned Stage 2 core gate |
| 2D | Wrong identity/permissions/credentials prevent continuation while preserving work | Planned identity-mutation/recovery tests for endpoint, database, actor, Namespace, permissions and epoch | Pending; planned Stage 2 identity/recovery gate |
| 2E | Concurrent resume exclusion, corruption/storage failure, cancellation outcome | Planned multi-process journal and state-transition tests | Pending; planned Stage 2 core gate |
| 2F | Server restart invalidates sessions and recovers original outcome | Planned owner-managed graceful/forced restart integration tests | Pending; planned Stage 2 live restart gate |
| 3A | Every procedure has exact binding, owner, output, scope, recovery and batch policy | Stage 1 `tests/unit/test_manifest.py` covers declaration parity; Stage 3 dispatch tests remain planned | Pending full behavior; planned `Invoke-PythonTests.ps1 -Stage 3 -Group gateway` |
| 3B | Live token-free discovery, routing, parameter order, empty results and safe errors | Planned installed family representative tests | Pending; planned Stage 3 gateway/live gate |
| 3C | Advanced results retain columns/lifetime and release readers on every exit | Planned result lifetime/early-close/error tests | Pending; planned Stage 3 gateway gate |
| 3D | Cancellation works during busy Store calls and isolates admin state | Planned concurrent request cancellation integration tests | Pending; planned Stage 3 gateway/live gate |
| 3E | Ledger identifies unsupported scenarios and owner stage | `docs/procedure_coverage_ledger.md` separates dispatch, convenience and executed evidence; full coverage remains future work | Pending; planned Stage 3 coverage review |
| 4A | Lossless value boundaries, precision and rejection of lossy values | Planned typed codec and independent wire-value tests | Pending; planned `Invoke-PythonTests.ps1 -Stage 4 -Group domain` |
| 4B | Missing/unknown descriptor and detached mutation semantics | Planned typed handle and live assignment assertions | Pending; planned Stage 4 domain gate |
| 4C | CRUD selectors, same-name behavior, foreign-Store rejection and refresh | Planned handle/selector/conflict integration tests | Pending; planned Stage 4 domain gate |
| 4D | Query columns/presence, pagination, expiry and cleanup | Planned query/page/lifetime tests | Pending; planned Stage 4 domain gate |
| 4E | Batch references, validation, rollback, size and replay | Planned batch validation and live atomicity/restart tests | Pending; planned Stage 4 domain gate |
| 5A | Exact uploads, interrupted resume, spool fidelity and download failures | Planned stream/resource restart tests | Pending; planned `Invoke-PythonTests.ps1 -Stage 5 -Group workflows` |
| 5B | Geometry, CRS/Z, Pattern and layout workflows preserve server values | Planned installed geometry/Pattern workflow tests | Pending; planned Stage 5 workflow gate |
| 5C | Import analysis/apply uses immutable snapshots and original mappings | Planned staging mutation, ambiguity, rollback and replay tests | Pending; planned Stage 5 workflow gate |
| 5D | CSV/XLSX/GeoPackage end-to-end round trips | Planned format workflows and independent reload assertions | Pending; planned Stage 5 workflow gate |
| 5E | Browser download uses separate auth, no token in URL, safe listener failure | Planned browser harness download/denial tests | Pending; planned Stage 5 browser/workflow gate |
| 5F | History, tags, revocation, admin and operational recovery | Planned history/admin/identity transition workflows | Pending; planned Stage 5 workflow gate |
| 6A | Optional NetworkX absent/present behavior | Planned `tests/optional/test_networkx.py` absent-extra/present-extra assertions | Pending; planned `Invoke-PythonTests.ps1 -Stage 6 -Group release` |
| 6B | Graph import uses fixed snapshot and server matching | Planned NetworkX round-trip/snapshot mutation tests | Pending; planned Stage 6 release gate |
| 6C | No public procedure or claimed convenience behavior lacks evidence | Planned final ledger audit against complete manifest and live evidence | Pending; planned Stage 6 release gate |
| 6D | Clean installed artifacts pass declared support matrix and reject outside it | Planned matrix build/install compatibility tests | Pending; planned Stage 6 release gate |
| 6E | Full Python/harness/server regression and reviewed release evidence | Planned complete runner and selected server acceptance suite | Pending; planned Stage 6 release gate |
