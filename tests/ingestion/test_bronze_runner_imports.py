import re
from pathlib import Path


def test_src_is_a_package():
    src_init = Path("src") / "__init__.py"
    assert src_init.exists(), "src/__init__.py missing; 'python -m src....' requires src to be a package"


def test_bronze_runner_uses_module_invocation():
    runner = Path("scripts") / "01_bronze_ingestion.sh"
    text = runner.read_text(encoding="utf-8")

    assert re.search(r"\bpython\b", text) or re.search(r"\bPYTHON=", text)
    assert "-m src.ingestion.ibge_client" in text
    assert "-m src.ingestion.transparency_client" in text
