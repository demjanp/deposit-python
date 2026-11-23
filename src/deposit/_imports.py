"""Portable graph and manifest field names used by the NetworkX adapter."""

GRAPH_CATEGORIES = (
    "classes", "objects", "descriptors", "geometries", "memberships",
    "relationships", "classRelationships", "patterns", "userTools", "resources",
)
MANIFEST_FIELDS = (
    "packageId", "sourceNamespaceId", "name", "description", "depositVersion",
    "historyMode", "exportedRevision", "createdAt", "rootIds", "scope",
    "includeHistory",
)
_MANIFEST_ALLOWED = frozenset(MANIFEST_FIELDS) | {
    "files", "maxHops", "rootClassBoundary", "boundaryReferences", "omittedEdgeCount",
}
