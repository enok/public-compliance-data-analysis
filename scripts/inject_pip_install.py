"""Inject a pip-install setup cell at the top of every notebook.

Idempotent: detects existing setup cell by marker and replaces it.
Run from project root:

    python scripts/inject_pip_install.py
"""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
NOTEBOOKS_DIR = ROOT / "notebooks"

MARKER = "# --- AUTO-GENERATED DEPENDENCY INSTALL ---"

SETUP_SOURCE = f"""{MARKER}
# Installs all project dependencies on first run (Colab, fresh environments, etc).
# Idempotent: pip skips anything already installed.
# To regenerate this cell, run: python scripts/inject_pip_install.py

import subprocess
import sys
from pathlib import Path

_req = Path.cwd().parent / "requirements.txt"
if not _req.exists():
    _req = Path.cwd() / "requirements.txt"

if _req.exists():
    print(f"Installing dependencies from {{_req}} ...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "-r", str(_req)])
    print("Dependencies ready.")
else:
    print("requirements.txt not found. Install manually: pip install -r requirements.txt")
"""


def make_setup_cell() -> dict:
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {"tags": ["dependency-install"]},
        "outputs": [],
        "source": SETUP_SOURCE.splitlines(keepends=True),
    }


def is_setup_cell(cell: dict) -> bool:
    if cell.get("cell_type") != "code":
        return False
    src = "".join(cell.get("source", []))
    return MARKER in src


def process(nb_path: Path) -> str:
    """Return status string after processing."""
    try:
        with nb_path.open(encoding="utf-8") as f:
            nb = json.load(f)
    except Exception as exc:
        return f"SKIP (parse error: {exc})"

    cells = nb.get("cells", [])
    existing_idx = next((i for i, c in enumerate(cells) if is_setup_cell(c)), None)

    new_cell = make_setup_cell()

    if existing_idx is not None:
        if cells[existing_idx]["source"] == new_cell["source"]:
            return "UNCHANGED"
        cells[existing_idx] = new_cell
        action = "UPDATED"
    else:
        # Insert after first markdown cell if present; otherwise at index 0.
        insert_at = 0
        if cells and cells[0].get("cell_type") == "markdown":
            insert_at = 1
        cells.insert(insert_at, new_cell)
        action = "ADDED"

    nb["cells"] = cells
    nb_path.write_text(json.dumps(nb, indent=1, ensure_ascii=False), encoding="utf-8")
    return action


def main() -> None:
    notebooks = sorted(NOTEBOOKS_DIR.glob("*.ipynb"))
    if not notebooks:
        print(f"No notebooks found in {NOTEBOOKS_DIR}")
        return

    print(f"Processing {len(notebooks)} notebooks...")
    print("=" * 60)
    for nb in notebooks:
        status = process(nb)
        print(f"  {status:<12} {nb.name}")
    print("=" * 60)
    print("Done.")


if __name__ == "__main__":
    main()
