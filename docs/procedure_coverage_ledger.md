# Procedure coverage ledger

This ledger tracks three independent kinds of evidence for every server procedure. Manifest parity checks the server declaration and routing metadata; it does not prove a usable Python call or an executed server integration. Update the last two evidence columns only when the relevant later-stage behavior is implemented and run.

The final-source foundation run `20261026T010158409Z-2e0e0609` passed manifest parity for all 124 declarations and the installed-wheel live smoke through the suite owner. Discovery, fresh-runtime bootstrap, valid/invalid login, session lookup, disposable Namespace creation/selection/deletion, logout, caller-owned driver use after Client close, and target/system native identity equality after Stop/Start were exercised outside the source checkout. The retained result JSON is `test_harness/results/python-20261026T010158409Z-2e0e0609/results.json`; live suite logs are under `test_harness/results/20261026T010305840Z-1678f00a`. The runner Job Object probes cover descendants that outlive parents, setup/assignment/resume failures and unrelated process survival. Fresh independent review of the corrected diff is still required before Stage 1 finalization. Dispatch and convenience APIs for the remaining declarations are planned for later stages.

| Procedure | Invocation owner | Server dispatch evidence | Python convenience behavior | Executed integration evidence |
|---|---|---|---|---|
| deposit.create.class | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.set.class | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.delete.class | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.class | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.classPage | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.create.object | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.set.object | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.delete.object | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.object | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.objectPage | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.create.descriptor | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.set.descriptor | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.delete.descriptor | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.descriptor | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.descriptorPage | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.create.classDescriptor | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.set.classDescriptor | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.delete.classDescriptor | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.classDescriptor | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.classDescriptorPage | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.create.userTool | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.set.userTool | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.delete.userTool | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.userTool | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.userToolPage | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.create.relationship | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.set.relationship | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.delete.relationship | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.relationship | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.relationshipPage | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.create.classRelationship | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.set.classRelationship | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.delete.classRelationship | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.classRelationship | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.classRelationshipPage | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.set.membership | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.apply.batch | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.delete.all | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.utils.bootstrap | Client | Manifest parity PASS; final installed-package live dispatch PASS in run `20261026T010158409Z-2e0e0609`. | `Client.bootstrap`; fresh-runtime administrator bootstrap PASS. | Installed-package fresh administrator bootstrap PASS in final-source smoke. |
| deposit.utils.login | Client | Manifest parity PASS; final installed-package live dispatch PASS in run `20261026T010158409Z-2e0e0609`. | `Client.login`; valid login and expected invalid-credential rejection PASS. | Installed-package valid/invalid authentication PASS in final-source smoke. |
| deposit.utils.logout | Client | Manifest parity PASS; final installed-package live dispatch PASS in run `20261026T010158409Z-2e0e0609`. | `Client.logout`; logout after Namespace deletion PASS. | Installed-package logout PASS in final-source smoke. |
| deposit.create.user | Client | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.create.namespace | Client | Manifest parity PASS; final installed-package live dispatch PASS in run `20261026T010158409Z-2e0e0609`. | Direct `Client._single` invocation from live smoke; public convenience method remains planned. | Installed-package disposable Namespace creation PASS. |
| deposit.set.user | Client | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.set.password | Client | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.set.namespace | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.delete.namespace | Client | Manifest parity PASS; final installed-package live dispatch PASS in run `20261026T010158409Z-2e0e0609`. | Direct `Client._single` invocation from live cleanup; public convenience method remains planned. | Installed-package selected disposable Namespace deletion PASS. |
| deposit.set.namespaceRole | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.set.activeNamespace | Client | Manifest parity PASS; final installed-package live dispatch PASS in run `20261026T010158409Z-2e0e0609`. | Direct `Client._single` invocation from live smoke; public convenience method remains planned. | Installed-package disposable Namespace selection PASS. |
| deposit.get.activeNamespace | Client | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.set.administrativeMode | Client | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.administrativeMode | Client | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.user | Client | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.userPage | Client | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.namespace | Client | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.namespacePage | Client | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.objectClass | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.classMembers | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.classMembersPage | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.objectByDescriptorId | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.namespaceRoles | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.namespaceRolesPage | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.operation | RecoveryCoordinator | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.revision | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.history | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.historyPage | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.restorePreview | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.utils.undo | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.utils.redo | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.utils.restore | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.utils.canUndo | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.utils.isRestored | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.set.versioningType | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.set.versionTag | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.query | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.queryPage | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.resource.beginUpload | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.resource.uploadChunk | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.resource.finishUpload | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.resource.abortUpload | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.resource.info | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.resource.downloadChunk | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.admin.pruneResources | Client | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.admin.pruneResourcesStatus | Client | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.utils.coordinatesToWKT | Client | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.utils.wktToCoordinates | Client | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.utils.toData | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.session | Client | Manifest parity PASS; final installed-package live dispatch PASS in run `20261026T010158409Z-2e0e0609`. | `Client.get_session`; authenticated profile lookup PASS. | Installed-package authenticated session lookup PASS. |
| deposit.utils.refresh | RecoveryCoordinator | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.lastOperation | RecoveryCoordinator | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.requests | RecoveryCoordinator | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.utils.cancel | RecoveryCoordinator | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.utils.cancelRequest | RecoveryCoordinator | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.action.prepare | RecoveryCoordinator | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.action.status | RecoveryCoordinator | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.action.resume | RecoveryCoordinator | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.action.release | RecoveryCoordinator | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.create.pattern | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.delete.pattern | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.data | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.namespaceData | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.objectsByPattern | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.objectsByPatternPage | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.pattern | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.patternPage | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.load.data | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.import.analyze | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.importPlan | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.import.apply | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.import.dataByPattern | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.import.namespaceData | Client | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.import.rows | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.load.csv | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.load.xlsx | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.load.gpkg | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.table | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.tablePage | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.layout | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.save.csv | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.save.xlsx | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.save.gpkg | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.save.data | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.set.pattern | Store | Manifest parity PASS; runtime Python dispatch pending. | Pending later-stage implementation. | Not executed. |
| deposit.get.info | Client | Manifest parity PASS; final installed-package live dispatch PASS in run `20261026T010158409Z-2e0e0609`. | `Client.discover`; exact supported Deposit version checked. | Installed-package discovery PASS in Seed and Verify phases. |
