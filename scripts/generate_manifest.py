"""Generate the reviewed Python procedure manifest from a server checkout."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path

OUTPUT_COLUMNS = {
    "node": "NodeRow", "rel": "RelRow", "value": "ValueRow",
    "data": "DataRow", "page": "DataRow", "query": "QueryRow",
    "eligibility": "EligibilityRow",
}
OUTPUT_MAPPERS = {
    "node": {"node"}, "rel": {"rel"}, "value": {"value"},
    "data": {"data", "page"}, "page": {"page"}, "query": {"query"},
    "eligibility": {"eligibility"},
}
ARGUMENT_TYPES = {
    "Boolean": {"boolean", "Boolean"}, "List<Object>": {"List<Object>"},
    "List<String>": {"List<String>"}, "Long": {"Long", "long"},
    "Map<String,Object>": {"Map<String,Object>"}, "Object": {"Object"},
    "String": {"String"},
}
SCOPES = {"namespace", "operational", "prune", "server"}
MODES = {"READ", "WRITE"}
BINDING_IDENTITIES = {
    "Class", "ClassDescriptor", "ClassRelationship", "Descriptor", "Namespace",
    "Object", "Pattern", "Relationship", "User", "UserTool",
}
DISCOVERY_COLUMNS = ["data"]
TOKENLESS_PROCEDURES = {"deposit.utils.bootstrap", "deposit.utils.login"}
CLIENT_ROUTES = {
    "deposit.utils.bootstrap", "deposit.utils.login", "deposit.utils.logout",
    "deposit.create.user", "deposit.create.namespace", "deposit.set.user",
    "deposit.set.password", "deposit.delete.namespace", "deposit.set.activeNamespace",
    "deposit.get.activeNamespace", "deposit.set.administrativeMode",
    "deposit.get.administrativeMode", "deposit.get.user", "deposit.get.userPage",
    "deposit.get.namespace", "deposit.get.namespacePage", "deposit.admin.pruneResources",
    "deposit.admin.pruneResourcesStatus", "deposit.import.namespaceData",
    "deposit.utils.coordinatesToWKT", "deposit.utils.wktToCoordinates", "deposit.get.session",
}
COORDINATOR_ROUTES = {
    "deposit.action.prepare", "deposit.action.status", "deposit.action.resume",
    "deposit.action.release", "deposit.get.operation", "deposit.get.lastOperation",
    "deposit.get.requests", "deposit.utils.cancel", "deposit.utils.cancelRequest",
    "deposit.utils.refresh",
}


def invocation_owner(name: str, scope: str) -> str:
    if name == "deposit.get.info" or name in CLIENT_ROUTES:
        return "Client"
    if name in COORDINATOR_ROUTES:
        return "RecoveryCoordinator"
    if scope in {"namespace", "operational"} or name == "deposit.utils.toData":
        return "Store"
    raise ValueError(f"No explicit invocation owner for {name} (scope={scope})")


def parse_api(path: Path) -> dict[str, dict]:
    source = path.read_text(encoding="utf-8")
    pattern = re.compile(
        r'(?P<admin>@Admin\s+)?@Procedure\(name\s*=\s*"([^"]+)"\s*,\s*mode\s*=\s*Mode\.(\w+)\)\s*'
        r'public\s+Stream<PublicResults\.(\w+)Row>\s+\w+\((.*?)\)\s*\{(.*?)\}',
        re.S,
    )
    procedures: dict[str, dict] = {}
    for match in pattern.finditer(source):
        admin = bool(match.group("admin"))
        name, mode, row_type, params, body = match.groups()[1:]
        parsed_params = _parse_java_parameters(params, name)
        mapper = re.search(r"\.map\(PublicResults::(\w+)\)", body)
        if not mapper:
            raise ValueError(f"No public result mapper found for {name}")
        if name in procedures:
            raise ValueError(f"Duplicate procedure registration: {name}")
        procedures[name] = {
            "mode": mode, "row_type": row_type, "params": parsed_params,
            "registered_args": [param["name"] for param in parsed_params],
            "mapper": mapper.group(1), "admin": admin,
        }
    if not procedures:
        raise ValueError(f"No public procedure declarations found in {path}")
    return procedures


def _split_java_parameters(params: str) -> list[str]:
    if not params.strip():
        return []
    parts: list[str] = []
    start = 0
    generic_depth = 0
    for index, character in enumerate(params):
        if character == "<":
            generic_depth += 1
        elif character == ">":
            generic_depth -= 1
            if generic_depth < 0:
                raise ValueError("Unbalanced Java generic parameter type")
        elif character == "," and generic_depth == 0:
            parts.append(params[start:index].strip())
            start = index + 1
    if generic_depth:
        raise ValueError("Unbalanced Java generic parameter type")
    parts.append(params[start:].strip())
    return parts


def _parse_java_parameters(params: str, procedure: str) -> list[dict[str, str]]:
    result = []
    for part in _split_java_parameters(params):
        match = re.fullmatch(r'@Name\("([^"]+)"\)\s+([A-Za-z][A-Za-z0-9<>, ]*)\s+([A-Za-z][A-Za-z0-9]*)', part)
        if not match:
            raise ValueError(f"Could not parse registered parameter for {procedure}: {part}")
        name, java_type, _variable = match.groups()
        result.append({"name": name, "java_type": re.sub(r"\s+", "", java_type)})
    names = [param["name"] for param in result]
    if len(set(names)) != len(names):
        raise ValueError(f"Duplicate registered argument names for {procedure}")
    return result


def parse_procedure_specs(path: Path) -> dict[str, dict]:
    source = path.read_text(encoding="utf-8")
    pattern = re.compile(
        r'SPECS\.put\("([^"]+)",new Spec\("([^"]+)",(true|false),List\.of\((.*?)\)\)\);', re.S
    )
    specs: dict[str, dict] = {}
    for name, scope, action, raw_bindings in pattern.findall(source):
        bindings = re.findall(r'"([^"]+)"', raw_bindings)
        if re.sub(r'"[^"]+"|[\s,]+', "", raw_bindings):
            raise ValueError(f"Could not parse binding declaration for {name}")
        if name in specs:
            raise ValueError(f"Duplicate ProcedureCatalog specification: {name}")
        specs[name] = {"scope": scope, "action": action == "true", "bindings": bindings}
    if not specs:
        raise ValueError(f"No ProcedureCatalog specifications found in {path}")
    return specs


def parse_public_result_mappers(path: Path) -> dict[str, dict]:
    source = path.read_text(encoding="utf-8")
    row_fields: dict[str, list[str]] = {}
    for row_type, body in re.findall(r"public\s+static\s+final\s+class\s+(\w+Row)\s*\{([^}]*)\}", source, re.S):
        fields = re.findall(r"public\s+final\s+[^;{}]+?\s+(\w+)\s*;", body)
        if not fields or len(set(fields)) != len(fields):
            raise ValueError(f"Public result row has no valid columns: {row_type}")
        row_fields[row_type] = fields
    mappers = {}
    for row_type, mapper in re.findall(r"public\s+static\s+(\w+Row)\s+(\w+)\(Object\s+\w+\)", source):
        if mapper in mappers:
            raise ValueError(f"Duplicate public result mapper: {mapper}")
        if row_type not in row_fields:
            raise ValueError(f"Mapper returns an unknown result row: {mapper}")
        mappers[mapper] = {"row_type": row_type, "columns": row_fields[row_type]}
    if not mappers:
        raise ValueError(f"No public result mapper declarations found in {path}")
    return mappers


def validate_catalog_entry(item: object, index: int) -> dict:
    allowed_fields = {"name", "args", "output", "mode", "scope", "action", "admin"}
    required_fields = allowed_fields - {"admin"}
    if not isinstance(item, dict) or not required_fields <= set(item) or set(item) - allowed_fields:
        raise ValueError(f"Invalid public API metadata fields at catalog index {index}")
    if "admin" in item and type(item["admin"]) is not bool:
        raise ValueError(f"Admin metadata must be boolean for {item.get('name', '<unknown>')}")
    name = item["name"]
    if not isinstance(name, str) or not re.fullmatch(r"deposit\.[A-Za-z][A-Za-z0-9]*(?:\.[A-Za-z][A-Za-z0-9]*)+", name):
        raise ValueError(f"Invalid public procedure name at catalog index {index}")
    if type(item["args"]) is not list:
        raise ValueError(f"Arguments must be an ordered list for {name}")
    if not isinstance(item["mode"], str) or item["mode"] not in MODES:
        raise ValueError(f"Unknown procedure mode for {name}: {item['mode']!r}")
    if not isinstance(item["scope"], str) or item["scope"] not in SCOPES:
        raise ValueError(f"Unknown procedure scope for {name}: {item['scope']!r}")
    if type(item["action"]) is not bool:
        raise ValueError(f"Action metadata must be boolean for {name}")
    if not isinstance(item["output"], str) or item["output"] not in OUTPUT_COLUMNS:
        raise ValueError(f"Unknown public output declaration for {name}: {item['output']!r}")
    names = []
    for arg_index, arg in enumerate(item["args"]):
        if not isinstance(arg, dict) or set(arg) != {"name", "type", "binding"}:
            raise ValueError(f"Invalid argument metadata for {name} at index {arg_index}")
        arg_name, arg_type, binding = arg["name"], arg["type"], arg["binding"]
        if not isinstance(arg_name, str) or not re.fullmatch(r"[A-Za-z][A-Za-z0-9]*", arg_name):
            raise ValueError(f"Invalid argument name for {name}: {arg_name!r}")
        if not isinstance(arg_type, str) or arg_type not in ARGUMENT_TYPES:
            raise ValueError(f"Unknown argument type for {name}.{arg_name}: {arg_type!r}")
        if not isinstance(binding, str) or not binding:
            raise ValueError(f"Invalid argument binding for {name}.{arg_name}")
        if binding != "+" and not re.fullmatch(r"[A-Za-z][A-Za-z0-9]*", binding) and not re.fullmatch(r"[?~][A-Za-z][A-Za-z0-9]*", binding) and not re.fullmatch(r"@[A-Za-z][A-Za-z0-9]*:[A-Za-z][A-Za-z0-9]*", binding):
            raise ValueError(f"Unknown binding form for {name}.{arg_name}: {binding!r}")
        if binding.startswith("?") and binding[1:] not in BINDING_IDENTITIES:
            raise ValueError(f"Unknown optional binding identity for {name}.{arg_name}: {binding!r}")
        if binding.startswith("@"):
            identity, _field = binding[1:].split(":", 1)
            if identity not in BINDING_IDENTITIES:
                raise ValueError(f"Unknown typed binding identity for {name}.{arg_name}: {binding!r}")
        if arg_name == "token":
            raise ValueError(f"Deposit token is a registration parameter, not catalog metadata: {name}")
        names.append(arg_name)
    if len(names) != len(set(names)):
        raise ValueError(f"Duplicate argument names for {name}")
    return item


def parse_batch_allowlist(path: Path) -> set[str]:
    source = path.read_text(encoding="utf-8")
    match = re.search(r"for\(String entity:List\.of\((.*?)\)\).*?names\.addAll\(List\.of\((.*?)\)\)", source, re.S)
    if not match:
        raise ValueError("Could not parse DomainService batch allowlist")
    entities = re.findall(r'"([^"]+)"', match.group(1))
    verb_match = re.search(r"for\(String verb:List\.of\((.*?)\)\)", match.group(0), re.S)
    if not entities or not verb_match:
        raise ValueError("DomainService batch allowlist is incomplete")
    verbs = re.findall(r'"([^"]+)"', verb_match.group(1))
    if not verbs:
        raise ValueError("DomainService batch verb allowlist is empty")
    names = {f"deposit.{verb}.{entity}" for entity in entities for verb in verbs}
    names.update(re.findall(r'"([^"]+)"', match.group(2)))
    return names


def generate(server_root: Path, out: Path) -> dict:
    catalog_path = server_root / "runtime" / "public-api.json"
    api_path = server_root / "plugin/src/main/java/deposit/api/DepositAPI.java"
    discovery_path = server_root / "plugin/src/main/java/org/deposit/runtime/Discovery.java"
    batch_path = server_root / "plugin/src/main/java/deposit/service/DomainService.java"
    procedure_specs_path = server_root / "plugin/src/main/java/deposit/common/ProcedureCatalog.java"
    public_results_path = server_root / "plugin/src/main/java/deposit/common/PublicResults.java"
    try:
        catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("Public API catalog is missing or invalid JSON") from exc
    if type(catalog) is not list:
        raise ValueError("Public API catalog must be an ordered list")
    for index, item in enumerate(catalog):
        validate_catalog_entry(item, index)
    registrations = parse_api(api_path)
    batch_allowed = parse_batch_allowlist(batch_path)
    procedure_specs = parse_procedure_specs(procedure_specs_path)
    result_mappers = parse_public_result_mappers(public_results_path)
    catalog_by_name = {item["name"]: item for item in catalog}
    if len(catalog_by_name) != len(catalog):
        raise ValueError("Duplicate names in public API catalog")
    if set(catalog_by_name) != set(registrations):
        missing = sorted(set(catalog_by_name) - set(registrations))
        extra = sorted(set(registrations) - set(catalog_by_name))
        raise ValueError(f"Catalog/registration mismatch; missing={missing}, extra={extra}")
    if set(catalog_by_name) != set(procedure_specs):
        missing = sorted(set(catalog_by_name) - set(procedure_specs))
        extra = sorted(set(procedure_specs) - set(catalog_by_name))
        raise ValueError(f"Catalog/ProcedureCatalog mismatch; missing={missing}, extra={extra}")
    if not batch_allowed <= set(catalog_by_name):
        raise ValueError(f"Batch allowlist references unknown procedures: {sorted(batch_allowed - set(catalog_by_name))}")

    entries = []
    for item in catalog:
        reg = registrations[item["name"]]
        spec = procedure_specs[item["name"]]
        expected = [arg["name"] for arg in item["args"]]
        if reg["mode"] not in MODES or reg["mode"] != item["mode"]:
            raise ValueError(f"Procedure mode mismatch for {item['name']}: catalog={item['mode']}, registration={reg['mode']}")
        if reg["admin"] is not item.get("admin", False):
            raise ValueError(f"Procedure admin declaration mismatch for {item['name']}: catalog={item.get('admin', False)}, registration={reg['admin']}")
        if spec["scope"] != item["scope"] or spec["action"] is not item["action"]:
            raise ValueError(f"Procedure scope/action mismatch for {item['name']}: catalog=({item['scope']},{item['action']}), ProcedureCatalog=({spec['scope']},{spec['action']})")
        catalog_bindings = [arg["binding"] for arg in item["args"]]
        if spec["bindings"] != catalog_bindings:
            raise ValueError(f"Procedure binding mismatch for {item['name']}: catalog={catalog_bindings}, ProcedureCatalog={spec['bindings']}")
        requires_token = "token" in reg["registered_args"]
        if requires_token:
            if reg["registered_args"].count("token") != 1 or not reg["registered_args"] or reg["registered_args"][-1] != "token":
                raise ValueError(f"Registered token parameter must appear exactly once and last for {item['name']}")
            expected.append("token")
        elif item["name"] not in TOKENLESS_PROCEDURES:
            raise ValueError(f"Unexpected tokenless public procedure: {item['name']}")
        elif item["name"] == "deposit.utils.bootstrap" and not item.get("admin", False):
            raise ValueError("Bootstrap must retain its Neo4j administrator declaration")
        if reg["registered_args"] != expected:
            raise ValueError(f"Argument declaration mismatch for {item['name']}: {reg['registered_args']} != {expected}")
        expected_java_types = [ARGUMENT_TYPES[arg["type"]] for arg in item["args"]]
        if requires_token:
            expected_java_types.append({"String"})
        actual_java_types = [param["java_type"] for param in reg["params"]]
        if len(expected_java_types) != len(actual_java_types) or any(actual not in allowed for actual, allowed in zip(actual_java_types, expected_java_types)):
            raise ValueError(f"Argument type mismatch for {item['name']}: catalog={[arg['type'] for arg in item['args']]}, registration={actual_java_types}")
        if reg["mapper"] not in OUTPUT_MAPPERS[item["output"]]:
            raise ValueError(f"Output mapper mismatch for {item['name']}: catalog={item['output']}, mapper={reg['mapper']}")
        mapper = result_mappers.get(reg["mapper"])
        if mapper is None or mapper["row_type"] != f"{reg['row_type']}Row":
            raise ValueError(f"Public result row mismatch for {item['name']}: row={reg['row_type']}, mapper={reg['mapper']}")
        expected_columns = mapper["columns"]
        if not expected_columns:
            raise ValueError(f"Public result mapper declares no columns for {item['name']}")
        batch_ok = item["name"] in batch_allowed
        entries.append({
            "name": item["name"], "args": item["args"], "columns": expected_columns,
            "mode": reg["mode"], "scope": item["scope"], "requires_token": requires_token,
            "action": item["action"], "admin": item.get("admin", False), "batch_eligible": batch_ok,
            "batch_exclusion": None if batch_ok else "not registered in DomainService.COMMANDS",
            "invocation_owner": invocation_owner(item["name"], item["scope"]),
        })
    if "deposit.get.info" in catalog_by_name:
        raise ValueError("Discovery procedure collides with a catalog declaration")
    discovery_source = discovery_path.read_text(encoding="utf-8")
    discovery = re.search(
        r'@Procedure\(name\s*=\s*"([^"]+)"\s*,\s*mode\s*=\s*Mode\.(\w+)\)\s*'
        r'(?:@[\w.]+(?:\([^)]*\))?\s*)*public\s+Stream<(\w+)>\s+\w+\((.*?)\)',
        discovery_source, re.S
    )
    if not discovery or discovery.groups()[0] != "deposit.get.info" or discovery.group(2) != "READ":
        raise ValueError("Discovery must declare deposit.get.info in READ mode")
    if _parse_java_parameters(discovery.group(4), "deposit.get.info"):
        raise ValueError("Discovery must not require registered arguments or a token")
    info_type = discovery.group(3)
    info_class = re.search(rf"public\s+static\s+class\s+{re.escape(info_type)}\s*\{{([^}}]*)\}}", discovery_source, re.S)
    if not info_class:
        raise ValueError("Discovery result class is missing")
    discovery_columns = re.findall(r"public\s+final\s+[^;{}]+?\s+(\w+)\s*;", info_class.group(1))
    if discovery_columns != DISCOVERY_COLUMNS:
        raise ValueError(f"Discovery result columns differ from the public contract: expected={DISCOVERY_COLUMNS}, actual={discovery_columns}")
    entries.append({
        "name": "deposit.get.info", "args": [], "columns": discovery_columns, "mode": "READ",
        "scope": "discovery", "requires_token": False, "action": False,
        "batch_eligible": False, "batch_exclusion": "discovery lifecycle route",
        "invocation_owner": "Client",
    })

    pom = (server_root / "pom.xml").read_text(encoding="utf-8")
    version = re.search(r"<version>([^<]+)</version>", pom).group(1)
    neo4j_version = re.search(r"<neo4j.version>([^<]+)</neo4j.version>", pom).group(1)
    source_files = {
        "runtime/public-api.json": catalog_path,
        "plugin/src/main/java/deposit/api/DepositAPI.java": api_path,
        "plugin/src/main/java/org/deposit/runtime/Discovery.java": discovery_path,
        "plugin/src/main/java/deposit/common/ProcedureCatalog.java": server_root / "plugin/src/main/java/deposit/common/ProcedureCatalog.java",
        "plugin/src/main/java/deposit/common/PublicResults.java": public_results_path,
        "plugin/src/main/java/deposit/service/DomainService.java": batch_path,
    }
    return {
        "schema_version": 1,
        "source": {
            "repository": "deposit-server",
            "files_sha256": {name: hashlib.sha256(path.read_bytes()).hexdigest() for name, path in source_files.items()},
            "catalog_entries": len(catalog), "discovery_entry": "deposit.get.info",
            "server_project_version": version, "neo4j_version": neo4j_version,
        },
        "support": {"target_server_version": "2.0.0-dev.77", "target_neo4j_version": neo4j_version,
                    "target_python": "3.14.x (Windows x64)", "supported_python": ">=3.14,<3.15",
                    "neo4j_driver": ">=6.0,<7"},
        "procedures": entries,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--server-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path("src/deposit/_procedure_manifest.json"))
    args = parser.parse_args()
    manifest = generate(args.server_root.resolve(), args.output)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"wrote {len(manifest['procedures'])} procedures; source files are SHA-256 pinned")


if __name__ == "__main__":
    main()
