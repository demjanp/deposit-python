from __future__ import annotations

from importlib.metadata import distribution, metadata, version
from importlib.util import find_spec
from pathlib import Path

import deposit
import sysconfig
import os
import pytest


def test_installed_import_is_out_of_tree_and_2x():
    if "DEPOSIT_PYTHON_SOURCE_ROOT" not in os.environ:
        pytest.skip("installed artifact path assertions run under the Stage 1 harness")
    module_path = Path(deposit.__file__).resolve()
    checkout = Path(os.environ["DEPOSIT_PYTHON_SOURCE_ROOT"]).resolve()
    site_packages = Path(sysconfig.get_paths()["purelib"]).resolve()
    dist_path = Path(distribution("deposit").locate_file("")).resolve()
    assert site_packages in module_path.parents
    assert site_packages == dist_path
    assert checkout not in module_path.parents and module_path != checkout
    assert version("deposit").split(".", 1)[0] == "2"
    assert deposit.__version__.startswith("2.")
    requires = metadata("deposit").get_all("Requires-Dist") or []
    networkx = [requirement.lower() for requirement in requires if "networkx" in requirement.lower()]
    assert all('extra == "networkx"' in requirement for requirement in networkx)
    assert find_spec("networkx") is None
    assert (Path(deposit.__file__).parent / "_procedure_manifest.json").is_file()
