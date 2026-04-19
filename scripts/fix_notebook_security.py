"""Fix security issues in notebooks - remove absolute path exposures."""
import json
from pathlib import Path
import sys


def fix_print_statements(source_lines: list) -> list:
    """Fix print statements that expose absolute paths."""
    fixed = []
    for line in source_lines:
        # Fix: print(f"Python path: {sys.path[0]}")
        if 'sys.path[0]' in line and 'print' in line:
            # Replace with a safe version showing relative info or remove entirely
            line = line.replace(
                '{sys.path[0]}',
                '{(sys.path[0] if sys.path[0] == \".\" else \"<project-root>")}'
            )
        
        # Fix: print(f"Working directory: {Path.cwd()}")
        if 'Path.cwd()' in line and 'print' in line and 'Working directory' in line:
            # Replace with safe version
            line = line.replace(
                '{Path.cwd()}',
                '{Path.cwd().name if Path.cwd().name else \"<root>\"}'
            )
        
        # Fix: print(f"Installing dependencies from {_req} ...")
        if '{_req}' in line and 'print' in line:
            line = line.replace(
                '{_req}',
                '{_req.name if _req.exists() else \"requirements.txt\"}'
            )
        
        # Fix other absolute path prints
        if 'print' in line and 'sys.path' in line and 'path[0]' not in line:
            # General sys.path print - make it safe
            if '{' in line and '}' in line:
                line = '# ' + line + '  # SECURITY: path print commented out'
        
        fixed.append(line)
    return fixed


def fix_notebook(nb_path: Path) -> bool:
    """Fix a single notebook. Returns True if changes were made."""
    with open(nb_path, 'r', encoding='utf-8') as f:
        nb = json.load(f)
    
    changed = False
    
    for cell in nb.get('cells', []):
        if cell.get('cell_type') == 'code':
            original_source = cell.get('source', [])
            if isinstance(original_source, list):
                fixed_source = fix_print_statements(original_source)
                if fixed_source != original_source:
                    cell['source'] = fixed_source
                    changed = True
            elif isinstance(original_source, str):
                # Handle single string source (rare in modern notebooks)
                lines = original_source.split('\n')
                fixed_lines = fix_print_statements(lines)
                if fixed_lines != lines:
                    cell['source'] = '\n'.join(fixed_lines)
                    changed = True
    
    if changed:
        # Backup original
        backup_path = nb_path.with_suffix('.ipynb.backup')
        if not backup_path.exists():
            nb_path.rename(backup_path)
        
        # Write fixed version
        with open(nb_path, 'w', encoding='utf-8') as f:
            json.dump(nb, f, indent=1, ensure_ascii=False)
        
        print(f"[FIXED] {nb_path.name}")
        return True
    
    return False


def main():
    project_root = Path('C:/google-drive/cursos/usp/mba/data-science/tcc/code/public-compliance-data-analysis')
    notebooks_dir = project_root / 'notebooks'
    
    notebooks = list(notebooks_dir.glob('*.ipynb'))
    fixed_count = 0
    
    for nb_path in sorted(notebooks):
        # Skip backup files
        if '.backup' in nb_path.name:
            continue
        if fix_notebook(nb_path):
            fixed_count += 1
    
    print(f"\nDone. Fixed {fixed_count} notebooks.")


if __name__ == '__main__':
    main()
