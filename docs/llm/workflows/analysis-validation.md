# Analysis Validation Workflow

Use this workflow before finalizing any analytical output that will support the thesis claims.

## Purpose

Ensure all analytical outputs meet MBA thesis standards for:
- Reproducibility
- Validity
- Interpretability
- Alignment with research question

## When to Use

- Before committing new analysis notebooks
- Before generating figures/tables for the thesis document
- After modifying preprocessing, features, or methods
- When preparing for advisor review
- Before merging analysis branches

## Repository-Specific Steps

### 1. Reproducibility Check

Can another engineer reproduce this result?

```bash
# Clean environment test
rm -rf .venv && python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Run the analysis
jupyter nbconvert --execute notebooks/<analysis>.ipynb --to notebook
```

Verify:
- [ ] Notebook runs without errors from clean state
- [ ] No dependency on hidden kernel state
- [ ] Random seeds are set (if applicable)
- [ ] Data paths are relative or configurable
- [ ] Outputs are deterministic (re-run produces same results)

### 2. Evidence Traceability

Can every claim be traced to evidence?

| Claim Type | Required Evidence |
|------------|-------------------|
| Descriptive statistic | Table or inline value with source cell |
| Relationship/correlation | Scatter plot, correlation matrix, regression output |
| Group difference | Box plot, t-test, ANOVA results |
| Predictive finding | Model performance metrics, validation curve |
| Clustering/segmentation | Silhouette scores, cluster profiles |

Checklist:
- [ ] Every figure/table has a cell reference
- [ ] Every statistical test has p-values or confidence intervals
- [ ] Effect sizes are reported (not just significance)
- [ ] Sample sizes are stated
- [ ] Missing data handling is documented

### 3. Validity Threats Assessment

What could make this result wrong or misleading?

**Internal Validity (Is the analysis correct?)**
- [ ] No data leakage (future information in predictors)
- [ ] Train/test split appropriate for temporal data
- [ ] Confounding variables considered
- [ ] Multiple comparisons corrected if applicable
- [ ] Outliers handled transparently

**External Validity (Does it generalize?)**
- [ ] Time period specified (2010-2022)
- [ ] Geographic scope stated (Brazilian municipalities)
- [ ] Selection biases acknowledged
- [ ] Missing municipality patterns documented

**Construct Validity (Are we measuring what we claim?)**
- [ ] Variable definitions match literature/course materials
- [ ] Proxy variables justified
- [ ] Measurement error acknowledged

### 4. Interpretation Check

Do the conclusions match the evidence strength?

| Evidence Strength | Appropriate Language |
|-------------------|----------------------|
| Strong (p < 0.01, large n, robust) | "demonstrates," "shows," "indicates" |
| Moderate (p < 0.05, some limitations) | "suggests," "is consistent with" |
| Weak (exploratory, small n) | "explores," "provides preliminary evidence" |
| Association only | "is associated with" (never "causes") |

Prohibited claims:
- ❌ "X causes Y" (without causal design)
- ❌ "Proves" (association never proves)
- ❌ "Significant" without effect size
- ❌ Generalizing beyond 2010-2022 Brazil municipalities

### 5. Course Material Alignment

Does the method match the MBA curriculum?

Cross-reference with:
- `docs/llm/references/usp-mba-course-map.md`
- Course materials in `/mnt/hgfs/shared/data-science/aulas`

Check:
- [ ] Method is covered in relevant course (or justified why external)
- [ ] Terminology matches course conventions
- [ ] Assumptions match what was taught
- [ ] Interpretation aligns with course framing

### 6. Notebook Hygiene

Is the notebook presentation-ready?

- [ ] Clear markdown headers structure the narrative
- [ ] Code cells are focused (one task per cell)
- [ ] No debugging print statements left in
- [ ] Figures have proper labels, titles, legends
- [ ] Color schemes are accessible (color-blind friendly)
- [ ] Tables are readable (not too wide, formatted numbers)
- [ ] Citations/references included where appropriate
- [ ] Assumptions and limitations stated

### 7. Bilingual Consistency (If Applicable)

If this notebook has a Portuguese counterpart:
- [ ] English and Portuguese versions produce equivalent outputs
- [ ] Technical terms are consistently translated
- [ ] Both versions pass this validation workflow

### 8. Downstream Impact Check

Will this change affect other parts of the thesis?

Check for impact on:
- [ ] Other notebooks that use same data/variables
- [ ] Thesis document claims
- [ ] Presentation materials
- [ ] Prior advisor feedback

## Validation Artifacts

After passing validation, produce:

1. **Clean notebook** (outputs cleared and re-executed)
2. **Export of key figures** (high-res for thesis)
3. **Summary table** (CSV for easy inclusion)
4. **Methodology note** (markdown for thesis appendix)
5. **Limitations statement** (paragraph for discussion section)

## Exit Criteria

All checkboxes above must be checked, or explicitly documented why not:
- [ ] Reproducibility verified
- [ ] Evidence traceability confirmed
- [ ] Validity threats assessed and documented
- [ ] Interpretation strength matches evidence
- [ ] Course material alignment checked
- [ ] Notebook hygiene complete
- [ ] Bilingual consistency (if applicable)
- [ ] Downstream impact assessed

## Common Issues and Fixes

| Issue | Fix |
|-------|-----|
| Non-reproducible results | Set random seeds, fix data ordering |
| Overfitting | Add validation split, reduce model complexity |
| Spurious correlations | Check for confounders, temporal ordering |
| Missing data bias | Document missingness pattern, consider imputation |
| Outliers driving results | Show analysis with/without outliers |
| Multiple testing | Apply Bonferroni or FDR correction |
| Leakage | Ensure no future information in features |

## Related Workflows

- `docs/llm/workflows/tcc-analysis-and-writing-sync.md` — For integrating into thesis
- `docs/llm/workflows/bilingual-notebook-sync.md` — For EN/pt-BR pairs
- `docs/llm/workflows/tcc-method-selection.md` — For choosing methods initially
- `.windsurf/workflows/notebook-analysis.md` — For general notebook quality
