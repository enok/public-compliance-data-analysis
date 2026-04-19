"""Clear cell outputs from notebooks to remove any cached absolute paths."""
import json
from pathlib import Path


def clear_outputs(nb_path: Path) -> bool:
    """Clear all cell outputs from a notebook. Returns True if changes were made."""
    with open(nb_path, 'r', encoding='utf-8') as f:
        nb = json.load(f)
    
    changed = False
    
    for cell in nb.get('cells', []):
        if cell.get('cell_type') == 'code':
            # Check if cell has outputs
            if 'outputs' in cell and cell['outputs']:
                cell['outputs'] = []
                changed = True
            # Reset execution count
            if 'execution_count' in cell and cell['execution_count'] is not None:
                cell['execution_count'] = None
                changed = True
    
    if changed:
        with open(nb_path, 'w', encoding='utf-8') as f:
            json.dump(nb, f, indent=1, ensure_ascii=False)
        print(f"[CLEARED] {nb_path.name}")
        return True
    
    return False


def main():
    project_root = Path('C:/google-drive/cursos/usp/mba/data-science/tcc/code/public-compliance-data-analysis')
    notebooks_dir = project_root / 'notebooks'
    
    notebooks = list(notebooks_dir.glob('*.ipynb'))
    cleared_count = 0
    
    for nb_path in sorted(notebooks):
        # Skip backup files
        if '.backup' in nb_path.name:
            continue
        if clear_outputs(nb_path):
            cleared_count += 1
    
    print(f"\nDone. Cleared outputs from {cleared_count} notebooks.")


if __name__ == '__main__':
    main()
