# Deposit Python 2 implementation status

Updated: 2026-10-25.

## Authority and baseline

Plan: [implementation_plan.md](implementation_plan.md).
Design input: [implementation_proposal.md](implementation_proposal.md).
This tracker covers the Python wrapper, not the server implementation status in
the Deposit Hub. Stage order is 1, 2, 3, 4, 5, 6.

Initial inspected repository: `main` at
`3c61d21822d71182416b8f8f5cca7e2a7da295ba`. Package metadata and import stubs
exist; no operational wrapper, wrapper tests or Python acceptance runner exists.
No implementation stage has been executed or verified. Documentation preparation
does not count as Stage 1 completion. Existing server tests establish no Python
wrapper acceptance.

## Stage summary

| Stage | Scope | State | Acceptance | Verification/review | Finalization |
| --- | --- | --- | --- | --- | --- |
| 1 | Contracts, package and live test infrastructure | Not started | 1A-1E pending | Not run / no implementation review | No stage commits |
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

All acceptance IDs in the plan are pending. Stage 1 creates the assertion-to-test
map and reviewed catalog/discovery ledger. Record separate states for supported
dispatch, convenient Python behavior and executed integration acceptance. A catalog
count or Java regression result cannot satisfy the latter two fields. No specialized
scenario can remain unassigned beyond its stage or be waived at release.

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

No stage evidence records yet. No product tests, builds, server transitions or
installed-package acceptance were executed during plan preparation.

## Planning record

The plan preserves the proposal's six stages and requires real-server tests in
Stage 1, durable prepared recovery with the first mutation in Stage 2, and separate
dispatch/convenience/integration coverage through release. Concrete support
versions, protected-storage mechanism/defaults and detailed signature reconciliation
are explicit Stage 1 deliverables. They are not verified decisions in this tracker.

Planning consultation and read-only document consistency review completed with
architect (`gpt-6-astra`, medium reasoning) on 2026-10-25. Two findings were
corrected and rechecked: distinguish client-retained arguments/provenance from
server-owned recovery bindings/preconditions, and scope orchestration gates to
authorized project-run. No unresolved blocking document-consistency findings.
This was not implementation review or acceptance.

Documentation validation passed: local links resolve, six stages appear in order,
32 expected acceptance IDs are unique, all six tracker rows remain Not started,
code fences are balanced and whitespace checks are clean. No source, packaging,
harness or shared KB files were changed; no commits were created. The pre-existing
untracked proposal was preserved.

Stage 1 must resolve durable server/database identity before restart replay can
be enabled; current discovery supplies a version rather than that identity.
Any required public server-contract extension needs a separate material decision.
