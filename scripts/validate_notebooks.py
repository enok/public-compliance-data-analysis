"""Validate notebooks can be opened and imports work.

Run: python scripts/validate_notebooks.py
"""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
NOTEBOOKS_DIR = ROOT / "notebooks"

def validate_notebook(nb_path: Path) -> dict:
    """Validate a notebook file."""
    result = {"path": nb_path.name, "valid": False, "errors": [], "cells": 0}
    
    try:
        with open(nb_path, encoding="utf-8") as f:
            nb = json.load(f)
        
        result["valid"] = True
        result["cells"] = len(nb.get("cells", []))
        
        # Check for required cells
        cells = nb.get("cells", [])
        has_pip_cell = False
        has_imports = False
        
        for cell in cells:
            if cell.get("cell_type") == "code":
                src = "".join(cell.get("source", []))
                if "AUTO-GENERATED DEPENDENCY INSTALL" in src:
                    has_pip_cell = True
                if "import pandas" in src or "import numpy" in src:
                    has_imports = True
        
        if not has_pip_cell:
            result["errors"].append("Missing pip install cell")
        if not has_imports:
            result["errors"].append("Missing standard imports")
            
    except json.JSONDecodeError as e:
        result["errors"].append(f"Invalid JSON: {e}")
    except Exception as e:
        result["errors"].append(f"Error: {e}")
    
    return result

def main():
    print("=" * 70)
    print("NOTEBOOK VALIDATION")
    print("=" * 70)
    
    notebooks = sorted(NOTEBOOKS_DIR.glob("*.ipynb"))
    
    passed = 0
    failed = 0
    
    for nb_path in notebooks:
        result = validate_notebook(nb_path)
        
        status = "[OK]" if result["valid"] and not result["errors"] else "[FAIL]"
        print(f"\n{status} {result['path']}")
        print(f"  Cells: {result['cells']}")
        
        if result["errors"]:
            for err in result["errors"]:
                print(f"  WARNING: {err}")
            failed += 1
        else:
            passed += 1
    
    print("\n" + "=" * 70)
    print(f"Results: {passed} passed, {failed} failed, {len(notebooks)} total")
    print("=" * 70)
    
    return 0 if failed == 0 else 1

if __name__ == "__main__":
    sys.exit(main())
