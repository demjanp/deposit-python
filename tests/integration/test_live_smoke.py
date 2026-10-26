"""One-shot live smoke on the harness-owned freshly provisioned database."""

from __future__ import annotations

import os
import secrets
import string
import json
import sysconfig
from importlib.metadata import distribution
from pathlib import Path

import pytest
from neo4j import GraphDatabase

from deposit import Client, DepositError
from deposit._recovery.identity import DeploymentIdentity, read_deployment_identity


@pytest.mark.integration
def test_fresh_runtime_discovery_bootstrap_auth_session_and_logout():
    import deposit

    source_root = Path(os.environ["DEPOSIT_PYTHON_SOURCE_ROOT"]).resolve()
    module_path = Path(deposit.__file__).resolve()
    site_packages = Path(sysconfig.get_paths()["purelib"]).resolve()
    assert site_packages in module_path.parents
    assert Path(distribution("deposit").locate_file("")).resolve() == site_packages
    assert source_root not in module_path.parents
    uri = os.environ["DEPOSIT_TEST_URI"]
    bolt_password = os.environ["DEPOSIT_TEST_PASSWORD"]
    expected = os.environ["DEPOSIT_EXPECTED_VERSION"]
    identity_phase = os.environ["DEPOSIT_IDENTITY_PHASE"]
    identity_record_path = Path(os.environ["DEPOSIT_IDENTITY_RECORD_PATH"])
    password = "S1-" + "".join(secrets.choice(string.ascii_letters + string.digits) for _ in range(40))
    driver = GraphDatabase.driver(uri, auth=("neo4j", bolt_password))
    client = Client(driver)
    token = None
    namespace_name = None
    try:
        assert client.discover() == {"version": expected}
        if identity_phase == "Verify":
            saved = DeploymentIdentity.from_dict(json.loads(identity_record_path.read_text(encoding="utf-8")))
            current = read_deployment_identity(
                driver,
                endpoint=uri,
                database=saved.database_name,
                actor_id=saved.actor_id,
                namespace_id=saved.namespace_id,
            )
            assert current == saved
            return

        profile = client.bootstrap("stage1-admin", password)
        assert profile.get("username") == "stage1-admin"
        token = client.login("stage1-admin", password)
        session = client.get_session(token)
        user = session.get("user")
        assert isinstance(user, dict) and user.get("id")
        namespace_name = "stage1-" + secrets.token_hex(8)
        namespace = client._single(
            "CYPHER 25 CALL deposit.create.namespace($name, $properties, $token) YIELD node RETURN node",
            {"name": namespace_name, "properties": {}, "token": token},
        )["node"]
        namespace_id = namespace["id"]
        client._single(
            "CYPHER 25 CALL deposit.set.activeNamespace($namespace, $token) YIELD node RETURN node",
            {"namespace": namespace_name, "token": token},
        )
        identity = read_deployment_identity(
            driver, endpoint=uri, database="neo4j", actor_id=user["id"], namespace_id=namespace_id
        )
        identity_record_path.parent.mkdir(parents=True, exist_ok=True)
        identity_record_path.write_text(json.dumps(identity.to_dict(), sort_keys=True), encoding="utf-8")
        with pytest.raises(DepositError):
            client.login("stage1-admin", "invalid-stage1-password")
    finally:
        try:
            if token and namespace_name:
                client._single(
                    "CYPHER 25 CALL deposit.delete.namespace($confirmNamespaceName, $token) YIELD data RETURN data",
                    {"confirmNamespaceName": namespace_name, "token": token},
                )
            if token:
                client.logout(token)
        finally:
            client.close()
            try:
                with driver.session(database="neo4j") as session:
                    assert session.run("RETURN 1").single(strict=True)[0] == 1
            finally:
                driver.close()
