"""Execute a notebook using nbconvert via Python API.

Usage: python scripts/execute_notebook.py <notebook_path>
"""

import sys
import json
from pathlib import Path

# Add project to path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

def execute_notebook(nb_path: Path, output_path: Path = None) -> dict:
    """Execute a notebook and return results."""
    try:
        # Try using nbconvert Preprocessor
        from nbconvert.preprocessors import ExecutePreprocessor
        import nbformat
        
        # Read notebook
        with open(nb_path, encoding="utf-8") as f:
            nb = nbformat.read(f, as_version=4)
        
        # Execute
        ep = ExecutePreprocessor(timeout=300, kernel_name="python3")
        ep.preprocess(nb, {"metadata": {"path": str(nb_path.parent)}})
        
        # Save output
        if output_path:
            with open(output_path, "w", encoding="utf-8") as f:
                nbformat.write(nb, f)
        
        return {"success": True, "errors": []}
        
    except Exception as e:
        import traceback
        return {
            "success": False, 
            "errors": [str(e), traceback.format_exc()[:500]]
        }

def main():
    if len(sys.argv) < 2:
        print("Usage: python execute_notebook.py <notebook_path> [output_path]")
        return 1
    
    nb_path = Path(sys.argv[1])
    output_path = Path(sys.argv[2]) if len(sys.argv) > 2 else None
    
    if not nb_path.exists():
        print(f"Error: Notebook not found: {nb_path}")
        return 1
    
    print(f"Executing {nb_path.name}...")
    result = execute_notebook(nb_path, output_path)
    
    if result["success"]:
        print(f"[OK] {nb_path.name} executed successfully")
        if output_path:
            print(f"Output saved to: {output_path}")
        return 0
    else:
        print(f"[FAIL] {nb_path.name} execution failed")
        for err in result["errors"]:
            print(f"  {err}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
