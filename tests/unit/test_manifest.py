from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

from scripts.generate_manifest import generate


def _source_root() -> Path:
    return Path(os.environ.get("DEPOSIT_PYTHON_SOURCE_ROOT", Path.cwd())).resolve()


def test_checked_manifest_is_current_and_complete(tmp_path):
    server_root = _source_root().parent / "deposit-server"
    generated = generate(server_root, tmp_path / "manifest.json")
    checked = json.loads((_source_root() / "src/deposit/_procedure_manifest.json").read_text(encoding="utf-8"))
    assert generated == checked
    assert len(generated["procedures"]) == 124
    assert generated["procedures"][-1]["name"] == "deposit.get.info"
    assert len({item["name"] for item in generated["procedures"]}) == 124
    assert sum(item["batch_eligible"] for item in generated["procedures"]) > 0
    assert all(item["columns"] for item in generated["procedures"])
    assert {item["invocation_owner"] for item in generated["procedures"]} == {"Client", "Store", "RecoveryCoordinator"}
    owners = {item["name"]: item["invocation_owner"] for item in generated["procedures"]}
    assert owners["deposit.get.info"] == "Client"
    assert owners["deposit.action.prepare"] == "RecoveryCoordinator"
    assert owners["deposit.create.class"] == "Store"
    assert owners["deposit.load.csv"] == "Store"
    assert next(item for item in generated["procedures"] if item["name"] == "deposit.get.info")["requires_token"] is False
    loads = [item for item in generated["procedures"] if item["name"] in {"deposit.load.csv", "deposit.load.xlsx", "deposit.load.gpkg"}]
    assert len(loads) == 3
    assert all(item["scope"] == "operational" and item["action"] is True for item in loads)


def _copy_server_tree(target: Path) -> Path:
    source = _source_root().parent / "deposit-server"
    paths = (
        "pom.xml", "runtime/public-api.json",
        "plugin/src/main/java/deposit/api/DepositAPI.java",
        "plugin/src/main/java/org/deposit/runtime/Discovery.java",
        "plugin/src/main/java/deposit/service/DomainService.java",
        "plugin/src/main/java/deposit/common/ProcedureCatalog.java",
        "plugin/src/main/java/deposit/common/PublicResults.java",
    )
    for relative in paths:
        destination = target / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source / relative, destination)
    return target


def test_manifest_generation_fails_closed_on_metadata_and_source_mutations(tmp_path):
    def mutate_catalog(item):
        item["action"] = "true"

    def mutate_scope(item):
        item["scope"] = "unknown-scope"

    def mutate_output(item):
        item["output"] = "unreviewed-output"

    def mutate_binding(item):
        item["args"][0]["binding"] = "unreviewed-binding"

    def mutate_valid_binding(item):
        item["args"][0]["binding"] = "other"

    def mutate_valid_binding_order(item):
        item["args"][0]["binding"], item["args"][1]["binding"] = item["args"][1]["binding"], item["args"][0]["binding"]

    def mutate_args(item):
        item["args"] = {"name": "name"}

    def mutate_arg_type(item):
        item["args"][0]["type"] = "UnreviewedType"

    def mutate_duplicate_arg(item):
        item["args"][1]["name"] = item["args"][0]["name"]

    def mutate_admin_type(item):
        item["admin"] = "false"

    def mutate_null_action(item):
        item["action"] = None

    mutations = [mutate_catalog, mutate_scope, mutate_output, mutate_binding, mutate_args,
                 mutate_arg_type, mutate_duplicate_arg, mutate_admin_type,
                 mutate_valid_binding, mutate_valid_binding_order, mutate_null_action]
    for index, mutation in enumerate(mutations):
        root = _copy_server_tree(tmp_path / f"catalog-{index}")
        path = root / "runtime/public-api.json"
        catalog = json.loads(path.read_text(encoding="utf-8"))
        mutation(catalog[0])
        path.write_text(json.dumps(catalog), encoding="utf-8")
        try:
            generate(root, tmp_path / f"catalog-{index}.out.json")
        except ValueError:
            pass
        else:
            raise AssertionError(f"catalog mutation {index} was accepted")

    root = _copy_server_tree(tmp_path / "discovery-token")
    path = root / "plugin/src/main/java/org/deposit/runtime/Discovery.java"
    source = path.read_text(encoding="utf-8")
    marker = "public Stream<Info> info()"
    assert marker in source
    path.write_text(source.replace(marker, "public Stream<Info> info(String token)", 1), encoding="utf-8")
    try:
        generate(root, tmp_path / "discovery-token.out.json")
    except ValueError:
        pass
    else:
        raise AssertionError("discovery registration token was accepted")

    for index, (relative, old, new) in enumerate((
        ("plugin/src/main/java/deposit/api/DepositAPI.java", '@Name("name") String a0', '@Name("other") String a0'),
        ("plugin/src/main/java/deposit/api/DepositAPI.java", "PublicResults::node", "PublicResults::rel"),
        ("plugin/src/main/java/deposit/common/ProcedureCatalog.java", 'new Spec("namespace",true,List.of("name","+"))', 'new Spec("server",true,List.of("name","+"))'),
        ("plugin/src/main/java/org/deposit/runtime/Discovery.java", "public final Map<String, Object> data;", "public final Map<String, Object> other;"),
    )):
        root = _copy_server_tree(tmp_path / f"source-{index}")
        path = root / relative
        source = path.read_text(encoding="utf-8")
        assert old in source
        path.write_text(source.replace(old, new, 1), encoding="utf-8")
        try:
            generate(root, tmp_path / f"source-{index}.out.json")
        except ValueError:
            pass
        else:
            raise AssertionError(f"server source mutation {index} was accepted")

    root = _copy_server_tree(tmp_path / "admin-mismatch")
    path = root / "plugin/src/main/java/deposit/api/DepositAPI.java"
    source = path.read_text(encoding="utf-8")
    marker = '@Procedure(name="deposit.create.class",mode=Mode.WRITE)'
    assert marker in source
    path.write_text(source.replace(marker, "@Admin\n " + marker, 1), encoding="utf-8")
    try:
        generate(root, tmp_path / "admin-mismatch.out.json")
    except ValueError:
        pass
    else:
        raise AssertionError("unexpected administrator registration was accepted")

    root = _copy_server_tree(tmp_path / "collision")
    path = root / "runtime/public-api.json"
    catalog = json.loads(path.read_text(encoding="utf-8"))
    catalog.append(catalog[0])
    path.write_text(json.dumps(catalog), encoding="utf-8")
    try:
        generate(root, tmp_path / "collision.out.json")
    except ValueError:
        pass
    else:
        raise AssertionError("duplicate catalog registration was accepted")

    root = _copy_server_tree(tmp_path / "registration-collision")
    path = root / "plugin/src/main/java/deposit/api/DepositAPI.java"
    source = path.read_text(encoding="utf-8")
    line = next(line for line in source.splitlines() if '@Procedure(name="deposit.create.class"' in line)
    method = next(line for line in source.splitlines() if "public Stream<PublicResults.NodeRow> p0(" in line)
    path.write_text(source + "\n" + line + "\n" + method + "\n", encoding="utf-8")
    try:
        generate(root, tmp_path / "registration-collision.out.json")
    except ValueError:
        pass
    else:
        raise AssertionError("duplicate procedure registration was accepted")


def test_procedure_coverage_ledger_tracks_every_declaration_separately():
    procedures = json.loads((_source_root() / "src/deposit/_procedure_manifest.json").read_text(encoding="utf-8"))["procedures"]
    ledger = (_source_root() / "docs/procedure_coverage_ledger.md").read_text(encoding="utf-8")
    table = [line for line in ledger.splitlines() if line.startswith("|")]
    assert table[0].split("|")[1:-1] == [
        " Procedure ",
        " Invocation owner ",
        " Server dispatch evidence ",
        " Supported Python route / behavior ",
        " Executed integration evidence ",
        " Specialized unverified scenario / later test target ",
    ]
    rows = table[2:]
    assert [row.split("|")[1].strip() for row in rows] == [item["name"] for item in procedures]
    assert all(len(row.split("|")) == 8 for row in rows)
