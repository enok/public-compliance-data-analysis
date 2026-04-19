"""Execute all notebooks and report results.

Usage: python scripts/execute_all_notebooks.py
"""

import sys
import json
import traceback
from pathlib import Path
from nbconvert.preprocessors import ExecutePreprocessor
import nbformat

ROOT = Path(__file__).resolve().parent.parent
NOTEBOOKS_DIR = ROOT / "notebooks"

def execute_notebook(nb_path: Path, timeout: int = 300) -> dict:
    """Execute a notebook and return results."""
    result = {
        "path": nb_path.name,
        "status": "PENDING",
        "errors": [],
        "cell_count": 0,
        "executed_count": 0
    }
    
    try:
        with open(nb_path, encoding="utf-8") as f:
            nb = nbformat.read(f, as_version=4)
        
        result["cell_count"] = len(nb.cells)
        
        ep = ExecutePreprocessor(timeout=timeout, kernel_name="python3")
        ep.preprocess(nb, {"metadata": {"path": str(nb_path.parent)}})
        
        result["status"] = "PASSED"
        result["executed_count"] = result["cell_count"]
        
    except Exception as e:
        result["status"] = "FAILED"
        result["errors"].append(f"{type(e).__name__}: {str(e)[:200]}")
        # Try to get more details
        tb = traceback.format_exc()
        if "CellExecutionError" in str(e):
            # Extract cell number from error
            result["errors"].append(f"Traceback: {tb[:500]}")
    
    return result

def main():
    print("=" * 80)
    print("EXECUTING ALL NOTEBOOKS")
    print("=" * 80)
    print()
    
    notebooks = sorted(NOTEBOOKS_DIR.glob("*.ipynb"))
    
    passed = 0
    failed = 0
    results = []
    
    for nb_path in notebooks:
        print(f"Executing {nb_path.name}...")
        result = execute_notebook(nb_path)
        results.append(result)
        
        status = "[PASS]" if result["status"] == "PASSED" else "[FAIL]"
        print(f"  {status} {result['executed_count']}/{result['cell_count']} cells")
        
        if result["errors"]:
            for err in result["errors"]:
                print(f"    {err[:150]}")
            failed += 1
        else:
            passed += 1
        
        print()
    
    print("=" * 80)
    print(f"RESULTS: {passed} passed, {failed} failed, {len(notebooks)} total")
    print("=" * 80)
    
    # Save results
    report_path = ROOT / "logs" / "notebook_execution_report.json"
    report_path.parent.mkdir(exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
    print(f"\nReport saved to: {report_path}")
    
    return 0 if failed == 0 else 1

if __name__ == "__main__":
    sys.exit(main())
