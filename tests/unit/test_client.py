from __future__ import annotations

import json
from pathlib import Path
import os

import pytest

from deposit import Client, CompatibilityError, DepositError, TransportError


class _Record(dict):
    pass


class _Result:
    def __init__(self, records):
        self.records = records if isinstance(records, list) else [records]

    def single(self, strict=False):
        if strict and len(self.records) != 1:
            raise ValueError("Expected exactly one record")
        return self.records[0] if self.records else None


class _Session:
    def __init__(self, driver):
        self.driver = driver

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def run(self, query, parameters):
        self.driver.calls.append((query, parameters))
        records = self.driver.responses.pop(0)
        if "dbms.components()" in query and isinstance(records, list):
            records = [
                _Record(version=record["versions"][0])
                for record in records
                if record.get("name") == "Neo4j Kernel"
            ]
        return _Result(records)


class _Driver:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []
        self.closed = False

    def session(self, **kwargs):
        return _Session(self)

    def close(self):
        self.closed = True


def test_discovery_login_session_logout_and_driver_ownership():
    manifest = json.loads((Path(os.environ.get("DEPOSIT_PYTHON_SOURCE_ROOT", Path.cwd())) / "src/deposit/_procedure_manifest.json").read_text())
    version = manifest["support"]["target_server_version"]
    native_version = manifest["support"]["target_neo4j_version"]
    def discovery():
        return [
            _Record(data={"version": version}),
            [
                _Record(name="Neo4j Kernel", versions=[native_version]),
                _Record(name="Cypher", versions=["5", "25"]),
            ],
        ]
    driver = _Driver([
        *discovery(),
        *discovery(),
        _Record(value="opaque-token"),
        *discovery(),
        _Record(data={"user": "alice", "expiresAt": "later"}),
        *discovery(),
        _Record(data="loggedOut"),
    ])
    client = Client(driver)
    assert client.discover() == {"version": version}
    component_query = next(query for query, _ in driver.calls if "dbms.components()" in query)
    assert "YIELD name, versions WHERE name = 'Neo4j Kernel'" in component_query
    token = client.login("alice", "correct-horse")
    assert token == "opaque-token"
    assert client.get_session(token)["user"] == "alice"
    assert client.logout(token) is None
    client.close()
    assert not driver.closed
    login_call = next(call for call in driver.calls if "utils.login" in call[0])
    assert login_call[1] == {"username": "alice", "password": "correct-horse"}


def test_unsupported_server_fails_before_login_call():
    driver = _Driver([_Record(data={"version": "1.9.0"})])
    with pytest.raises(CompatibilityError):
        Client(driver).login("alice", "secret")
    assert len(driver.calls) == 1


def test_unsupported_neo4j_server_fails_before_login_call():
    version = json.loads((Path(os.environ.get("DEPOSIT_PYTHON_SOURCE_ROOT", Path.cwd())) / "src/deposit/_procedure_manifest.json").read_text())["support"]["target_server_version"]
    driver = _Driver([_Record(data={"version": version}), _Record(version="2026.10.0")])
    with pytest.raises(CompatibilityError) as error:
        Client(driver).login("alice", "secret")
    assert error.value.component == "Neo4j Server"
    assert len(driver.calls) == 2


def test_empty_discovery_result_is_not_reported_as_success():
    with pytest.raises(TransportError):
        Client(_Driver([None])).discover()


def test_owned_driver_closes_with_client():
    driver = _Driver([])
    Client(driver, owns_driver=True).close()
    assert driver.closed
