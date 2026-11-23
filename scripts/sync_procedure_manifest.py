"""Generate the Python gateway projection of the Server procedure manifest."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import tomllib

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SERVER = ROOT.parent / "deposit-server" / "runtime" / "public-api.json"
DEFAULT_OUTPUT = ROOT / "src" / "deposit" / "_procedure_manifest.json"


def project(server_manifest: Path) -> dict:
    catalog = json.loads(server_manifest.read_text(encoding="utf-8"))
    project_metadata = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    entries = []
    names = set()
    for procedure in catalog:
        name = procedure["name"]
        if name in names:
            raise ValueError(f"Duplicate Server procedure {name}")
        names.add(name)
        token_index = procedure["tokenIndex"]
        if token_index is not None and (
            type(token_index) is not int or not 0 <= token_index <= len(procedure["args"])
        ):
            raise ValueError(f"Invalid token position for {name}")
        entries.append({
            "name": name,
            "args": procedure["args"],
            "columns": list(procedure["resultColumns"]),
            "mode": procedure["mode"],
            "scope": procedure["scope"],
            "requires_token": token_index is not None,
            "token_index": token_index,
            "action": procedure["action"],
            **({"tracked": procedure["tracked"], "cancellable": procedure["cancellable"]}
               if "tracked" in procedure else {}),
            "admin": procedure.get("admin", False),
            "batch_eligible": procedure["batchEligible"],
            "invocation_owner": procedure["invocationOwner"],
        })
    return {
        "source": {"repository": "deposit-server", "path": "runtime/public-api.json"},
        "support": {
            "supported_python": project_metadata["requires-python"],
            "neo4j_driver": next(dep.removeprefix("neo4j") for dep in project_metadata["dependencies"]
                                 if dep.startswith("neo4j")),
        },
        "procedures": entries,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-manifest", type=Path, default=DEFAULT_SERVER)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--check", action="store_true")
    options = parser.parse_args()
    generated = json.dumps(project(options.server_manifest), ensure_ascii=False, indent=2) + "\n"
    if options.check:
        if options.output.read_text(encoding="utf-8") != generated:
            parser.exit(1, "Python procedure manifest is stale. Run scripts/sync_procedure_manifest.py.\n")
    else:
        options.output.write_text(generated, encoding="utf-8", newline="\n")


if __name__ == "__main__":
    main()
