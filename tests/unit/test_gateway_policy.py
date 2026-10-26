from __future__ import annotations

import pytest

from deposit import Client, Store
from deposit._gateway import (
    _CLIENT_GATEWAY_PROCEDURES,
    _STORE_CONTROL_PROCEDURES,
    StoreResult,
    _manifest,
    bind_arguments,
    bind_client_arguments,
    build_query,
    client_procedure,
    prune_dry_run_procedure,
    recovery_read_procedure,
    store_control_procedure,
    store_procedure,
)


_CLIENT_METHODS = {
    "deposit.utils.bootstrap": "bootstrap",
    "deposit.utils.login": "login",
    "deposit.utils.logout": "logout",
    "deposit.get.session": "get_session",
    "deposit.get.info": "discover",
}

_CLIENT_DEFERRED_ACTIONS = {
    "deposit.create.user",
    "deposit.create.namespace",
    "deposit.set.user",
    "deposit.set.password",
    "deposit.delete.namespace",
    "deposit.import.namespaceData",
    "deposit.admin.pruneResources",
}

_RECOVERY_INTERNAL_ACTIONS = {
    "deposit.action.prepare",
    "deposit.action.status",
    "deposit.action.resume",
    "deposit.action.release",
}

_STORE_SPECIAL_PROCEDURES = {"deposit.utils.toData"}


def _manifest_entries():
    return _manifest()["procedures"]


def _placeholder(binding: str):
    if binding.startswith(("?", "~")):
        return None
    if binding == "+":
        return {}
    if binding.startswith("@"):
        return {"id": "entity-id"}
    return "value"


def test_every_manifest_entry_has_one_explicit_gateway_or_deferred_policy():
    procedures = _manifest_entries()
    by_name = {entry["name"]: entry for entry in procedures}
    assert len(by_name) == 124

    store_names = {entry["name"] for entry in procedures if entry["invocation_owner"] == "Store"}
    client_names = {entry["name"] for entry in procedures if entry["invocation_owner"] == "Client"}
    recovery_names = {
        entry["name"] for entry in procedures if entry["invocation_owner"] == "RecoveryCoordinator"
    }
    assert len(store_names) == 91
    assert len(client_names) == 23
    assert len(recovery_names) == 10

    client_gateway_names = {
        entry["name"]
        for entry in procedures
        if entry["invocation_owner"] == "Client"
        and entry["requires_token"] is True
        and entry["scope"] == "server"
        and entry["action"] is False
        and entry["name"] not in _CLIENT_METHODS
    }
    assert client_gateway_names == _CLIENT_GATEWAY_PROCEDURES

    for entry in procedures:
        name = entry["name"]
        owner = entry["invocation_owner"]
        assert entry["columns"]
        assert isinstance(entry["action"], bool)
        assert isinstance(entry["batch_eligible"], bool)
        assert isinstance(entry["args"], list)

        if owner == "Store":
            assert entry["requires_token"] is True
            if name in _STORE_SPECIAL_PROCEDURES:
                assert entry["scope"] == "server" and entry["action"] is False
                assert hasattr(StoreResult, "to_data")
            else:
                assert entry["scope"] in {"namespace", "operational"}
                assert store_procedure(name) == entry
            with pytest.raises(ValueError):
                client_procedure(name)
        elif owner == "Client":
            if name in _CLIENT_DEFERRED_ACTIONS:
                assert entry["action"] is True
                assert name in {"deposit.admin.pruneResources"} or entry["scope"] == "server"
                with pytest.raises(ValueError):
                    client_procedure(name)
            elif name in _CLIENT_METHODS:
                assert hasattr(Client, _CLIENT_METHODS[name])
            else:
                assert client_procedure(name) == entry
        else:
            assert owner == "RecoveryCoordinator"
            assert entry["scope"] == "server"
            if name in _STORE_CONTROL_PROCEDURES:
                assert store_control_procedure(name) == entry
            elif name in {"deposit.get.operation", "deposit.get.lastOperation"}:
                assert recovery_read_procedure(name) == entry
            elif name == "deposit.utils.refresh":
                assert hasattr(Store, "refresh")
            else:
                assert name in _RECOVERY_INTERNAL_ACTIONS
                assert entry["action"] is False

    assert {name for name in client_names if by_name[name]["action"]} == _CLIENT_DEFERRED_ACTIONS
    assert recovery_names == (
        _STORE_CONTROL_PROCEDURES.keys()
        | {"deposit.get.operation", "deposit.get.lastOperation", "deposit.utils.refresh"}
        | _RECOVERY_INTERNAL_ACTIONS
    )


def test_manifest_binding_order_defaults_and_named_outputs_are_preserved():
    for entry in _manifest_entries():
        owner = entry["invocation_owner"]
        if owner == "Store" and entry["name"] in _STORE_SPECIAL_PROCEDURES:
            routed = entry
            values = tuple(_placeholder(spec["binding"]) for spec in entry["args"])
            bound = bind_client_arguments(routed, values)
            query, parameters = build_query(routed, bound, "opaque-token")
        elif owner == "Store":
            routed = store_procedure(entry["name"])
            values = tuple(_placeholder(spec["binding"]) for spec in entry["args"])
            bound = bind_arguments(routed, values, namespace_id="namespace-id")
            query, parameters = build_query(routed, bound, "opaque-token")
        elif owner == "Client" and entry["name"] in _CLIENT_GATEWAY_PROCEDURES:
            routed = client_procedure(entry["name"])
            values = tuple(_placeholder(spec["binding"]) for spec in entry["args"])
            bound = bind_client_arguments(routed, values)
            query, parameters = build_query(routed, bound, "opaque-token", portable=True)
        elif owner == "RecoveryCoordinator" and entry["name"] in _STORE_CONTROL_PROCEDURES:
            routed = store_control_procedure(entry["name"])
            values = tuple(_placeholder(spec["binding"]) for spec in entry["args"])
            bound = bind_arguments(routed, values, namespace_id="namespace-id")
            query, parameters = build_query(routed, bound, "opaque-token")
        elif owner == "RecoveryCoordinator" and entry["name"] in {
            "deposit.get.operation", "deposit.get.lastOperation"
        }:
            routed = recovery_read_procedure(entry["name"])
            values = tuple(_placeholder(spec["binding"]) for spec in entry["args"])
            bound = bind_client_arguments(routed, values)
            query, parameters = build_query(routed, bound, "opaque-token")
        else:
            continue

        names = [spec["name"] for spec in routed["args"]]
        assert len(bound) == len(names)
        assert [parameters[f"a{index}"] for index in range(len(names))] == list(bound)
        assert list(parameters) == [*(f"a{index}" for index in range(len(names))), "token"]
        assert f"CALL {entry['name']}(" in query
        assert "YIELD " + ", ".join(entry["columns"]) in query
        assert "RETURN " + ", ".join(entry["columns"]) in query


def test_prune_execution_is_rejected_before_any_coordinator_route():
    entry = prune_dry_run_procedure({"dryRun": True})
    assert entry["name"] == "deposit.admin.pruneResources"
    for options in ({}, {"dryRun": False}, {"dryRun": 0}, {"dryRun": None}):
        with pytest.raises(Exception, match="Prune execution is deferred"):
            prune_dry_run_procedure(options)
