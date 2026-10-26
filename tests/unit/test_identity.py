from __future__ import annotations

from deposit._recovery.identity import (
    DeploymentIdentity,
    IdentityUnavailableError,
    normalize_endpoint,
    read_deployment_identity,
)


class _Record(dict):
    pass


class _Result:
    def __init__(self, value):
        self.value = value

    def single(self, strict=False):
        if self.value is None and strict:
            raise ValueError("empty")
        return self.value


class _Session:
    def __init__(self, driver, database):
        self.driver = driver
        self.database = database

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def run(self, query):
        self.driver.calls.append((self.database, query))
        return _Result(self.driver.responses[self.database])


class _Driver:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def session(self, *, database):
        return _Session(self, database)


def test_native_identity_combines_store_and_actor_scope():
    driver = _Driver({
        "neo4j": _Record(id="db-id", name="neo4j"),
        "system": _Record(id="dbms-id"),
    })
    identity = read_deployment_identity(
        driver, endpoint="bolt+s://DB.example:7687/", database="neo4j", actor_id="actor-id", namespace_id="ns-id"
    )
    assert identity == DeploymentIdentity(1, "bolt+s://db.example:7687", "neo4j", "db-id", "dbms-id", "actor-id", "ns-id")
    assert driver.calls == [
        ("neo4j", "CALL db.info() YIELD id, name RETURN id, name"),
        ("system", "CALL dbms.info() YIELD id RETURN id"),
    ]
    assert DeploymentIdentity.from_dict(identity.to_dict()) == identity
    assert identity.matches(identity)


def test_changed_store_endpoint_actor_or_namespace_fails_comparison():
    original = DeploymentIdentity(1, "bolt://db.example:7687", "neo4j", "db1", "sys1", "actor", "ns")
    for field, value in (("endpoint", "bolt://other.example:7687"), ("database_id", "db2"),
                         ("dbms_id", "sys2"), ("actor_id", "other-actor"), ("namespace_id", "other-ns")):
        changed = DeploymentIdentity(**{**original.to_dict(), field: value})
        assert not original.matches(changed)


def test_missing_native_identity_and_retargeted_database_fail_closed():
    for database_row in (_Record(id=None, name="neo4j"), _Record(id="db-id", name="other")):
        driver = _Driver({"neo4j": database_row, "system": _Record(id="dbms-id")})
        try:
            read_deployment_identity(driver, endpoint="bolt://db.example", database="neo4j", actor_id="a", namespace_id="n")
        except IdentityUnavailableError:
            pass
        else:
            raise AssertionError("Incomplete or retargeted native identity was accepted")


def test_missing_or_denied_system_store_identity_fails_closed():
    for system_row in (_Record(id=None), None):
        driver = _Driver({"neo4j": _Record(id="db-id", name="neo4j"), "system": system_row})
        try:
            read_deployment_identity(
                driver, endpoint="bolt://db.example", database="neo4j", actor_id="actor", namespace_id="ns"
            )
        except IdentityUnavailableError:
            pass
        else:
            raise AssertionError("Missing or denied system-store identity was accepted")


def test_endpoint_rejects_embedded_credentials_and_non_bolt_protocols():
    assert normalize_endpoint("bolt://DB.example") == "bolt://db.example:7687"
    for endpoint in ("bolt://user:password@db.example", "http://db.example", "bolt://db.example/other"):
        try:
            normalize_endpoint(endpoint)
        except IdentityUnavailableError:
            pass
        else:
            raise AssertionError(f"Unsafe endpoint accepted: {endpoint}")


def test_copied_store_with_retained_native_ids_is_indistinguishable():
    original = DeploymentIdentity(1, "bolt://db.example:7687", "neo4j", "same-db-id", "same-dbms-id", "actor", "ns")
    copied = DeploymentIdentity.from_dict(original.to_dict())
    assert original.matches(copied)
