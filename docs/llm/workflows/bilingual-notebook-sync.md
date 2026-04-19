# Bilingual Notebook Sync Workflow

Use this workflow when modifying notebooks that exist in both English (`notebooks/`) and Portuguese (`notebooks/pt-BR/`) versions.

## When to Use

- Adding new analysis to existing notebooks
- Updating visualization or output sections
- Refactoring code across notebook pairs
- Fixing bugs that affect both language versions

## Repository-Specific Steps

### 1. Identify the Notebook Pair

Find the corresponding notebook in both languages:
- English: `notebooks/<analysis>_<topic>.ipynb`
- Portuguese: `notebooks/pt-BR/<analysis>_<topic>.ipynb`

Examples:
- `notebooks/01_statistical_analysis.ipynb` ↔ `notebooks/pt-BR/01_statistical_analysis.ipynb`
- `notebooks/02_regression_models.ipynb` ↔ `notebooks/pt-BR/02_regression_models.ipynb`

### 2. Determine Change Scope

What type of change are you making?

| Change Type | English First | Portuguese Sync | Notes |
|-------------|---------------|-----------------|-------|
| Code/algorithm | ✓ | Mirror exactly | Code should be identical |
| Markdown narrative | ✓ | Translate | Keep technical terms consistent |
| Output cells | Regenerate | Regenerate | Ensure reproducibility |
| Variable names | ✓ | Mirror exactly | Do not translate variable names |
| Dataset references | ✓ | Mirror exactly | Paths and keys stay in English |

### 3. Apply Changes

**Always modify the English version first**, then mirror to Portuguese:

1. Make changes to the English notebook
2. Run the English notebook to verify outputs
3. Apply the same code changes to the Portuguese notebook
4. Translate markdown narrative while keeping:
   - Code identical
   - Variable names unchanged
   - Dataset paths unchanged
   - Technical terms consistent with MBA course materials

### 4. Validate Synchronization

Check that both notebooks:
- [ ] Have identical code cells (except markdown content)
- [ ] Use the same variable names and data paths
- [ ] Reference the same dataset versions
- [ ] Produce equivalent outputs (allowing for randomness if seeds are set)
- [ ] Have parallel section structures

### 5. Test Execution

- [ ] English notebook runs without errors: `jupyter nbconvert --execute notebooks/<name>.ipynb`
- [ ] Portuguese notebook runs without errors: `jupyter nbconvert --execute notebooks/pt-BR/<name>.ipynb`
- [ ] Outputs are reproducible (re-run produces same results)

### 6. Commit Strategy

Commit both notebooks together:
```
TCC-XXX: Update <analysis> notebooks (EN + pt-BR)

- Changes: <brief description>
- English: notebooks/<name>.ipynb
- Portuguese: notebooks/pt-BR/<name>.ipynb
```

## Translation Guidelines

### Keep in English
- Code (variable names, functions, class names)
- Dataset column names when referencing code
- File paths and S3 keys
- Python package names
- Configuration keys

### Translate to Portuguese
- Narrative explanations
- Section titles and headers
- Figure captions
- Interpretation of results
- Warnings and notes to readers

### Technical Terminology
Use consistent translations for domain terms:
| English | Portuguese |
|---------|------------|
| Federal transfers | Transferências federais |
| Sanctions | Sanções |
| Municipalities | Municípios |
| Clustering | Clusterização |
| Outliers | Outliers (keep) or Valores atípicos |
| Income per capita | Renda per capita |
| Compliance risk | Risco de compliance |

## Common Pitfalls

- **Translating variable names**: `df_municipios` should stay `df_municipios`, not become `df_municipios_pt`
- **Different random seeds**: Ensure both notebooks use the same seeds for reproducibility
- **Path differences**: Both should reference the same data sources, not language-specific copies
- **Output drift**: Clear outputs before committing; regenerate in both languages

## Related Resources

- `.windsurf/workflows/bilingual-doc-sync.md` — For README and documentation files
- `.windsurf/workflows/notebook-analysis.md` — For notebook quality checks
- `docs/llm/rules/usp-mba-course-context.md` — For terminology guidance
