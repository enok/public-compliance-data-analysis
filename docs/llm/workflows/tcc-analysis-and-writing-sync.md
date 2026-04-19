# TCC Analysis And Writing Sync Workflow

Use this repo-local workflow after the shared workflow in `.windsurf/workflows/research-analysis-cycle.md`.

This workflow ensures analysis changes flow correctly into the thesis document structure, following the MBA TCC course (`11_tcc`) chapter organization.

## Repository-Specific Steps

### 1. Identify What Changed

Categorize the analytical change:
- [ ] **Data boundary**: new sources, coverage changes, time window shifts
- [ ] **Preprocessing or feature logic**: transformations, cleaning, variable construction
- [ ] **Method or hyperparameters**: analytical approach, model specification, parameters
- [ ] **Figure or table outputs**: new visualizations, updated results
- [ ] **Interpretation or conclusion**: how results are explained

### 2. Map Change to Thesis Chapter

Based on the TCC chapter structure (`11_tcc/03_Estrutura-do-TCC`):

| Change Type | Primary Chapter Section | Secondary Updates |
|-------------|------------------------|-------------------|
| Data boundary, sources | Material and Methods | Introduction (scope) |
| Preprocessing, features | Material and Methods | — |
| Method selection | Material and Methods | — |
| Main results | Results and Discussion | Abstract |
| Robustness checks | Results and Discussion | Material and Methods |
| Interpretation changes | Results and Discussion | Conclusion |
| Limitation additions | Conclusion | Results and Discussion |

### 3. Update Code and Analysis Artifacts

Trace the changed evidence surfaces:
- `src/` — reusable preprocessing or modeling code
- `scripts/` — pipeline orchestration
- `notebooks/` — analysis and visualization
- `config/` — data contracts (if schema changes)
- `tests/` — validation logic

### 4. Synchronize Written Deliverables

Update thesis-facing narrative in the same change:

**If Material and Methods changed:**
- Document data sources with coverage dates
- Describe variable transformations
- Cite analytical methods with course-based justification
- Note software/tools used

**If Results and Discussion changed:**
- Update main findings narrative
- Ensure figures/tables match text claims
- Add diagnostic interpretation
- Connect to research question explicitly

**If Conclusion changed:**
- Verify conclusions follow from evidence shown
- Update limitation statements
- Check practical implications alignment

**If Abstract changed:**
- Use the 5-sentence formula:
  1. What you achieved ("We demonstrate...", "This study identifies...")
  2. Why this is hard and important
  3. How you did it (method keywords)
  4. What evidence you have
  5. Your most remarkable number/result
- Delete generic openings ("In recent years...")
- Keep within MBA thesis word limits

**Writing Clarity Principles (Gopen & Swan)**

Apply these sentence-level principles to thesis prose:

| Principle | Application | Example |
|-----------|-------------|---------|
| **Subject-verb proximity** | Keep subject and main verb close | ❌ "The model, trained on census data, achieves..." → ✅ "The model achieves... after training on census data" |
| **Stress position** | Place emphasis at sentence end | ❌ "Accuracy improves by 15% when using clustering" → ✅ "When using clustering, accuracy improves by **15%**" |
| **Topic position** | Context first, new info after | ✅ "Given the socioeconomic diversity, municipalities cluster into..." |
| **Old before new** | Familiar → unfamiliar | Link backward to prior sentence, then introduce new |
| **Action in verb** | Use strong verbs | ❌ "We performed an analysis" → ✅ "We analyzed" |
| **Delete hedging** | Drop "may," "can" unless uncertain | ❌ "This may suggest" → ✅ "This suggests" |
| **Be specific** | Name the metric | ❌ "performance improved" → ✅ "R² increased from 0.45 to 0.62" |

**Time Allocation for Writing**

Spend approximately equal time on:
1. The abstract
2. The introduction
3. The figures
4. Everything else combined

Advisors often form judgments from these sections before deep reading.

### 5. Cross-Check Supporting Surfaces

- `docs/` — technical documentation for the thesis
- `docs/llm/` — repo-local LLM guidance (when method framing changes)
- `README.md` — high-level project summary
- Bilingual counterparts (`README.pt-BR.md`, etc.)

### 6. Validation Checklist

- [ ] Code changes are committed with clear messages
- [ ] Notebook outputs are clean and reproducible
- [ ] Figure/table files are generated, not hand-edited
- [ ] Written sections reference the correct evidence artifacts
- [ ] Claims match the analytical limits (association ≠ causation)
- [ ] No orphan claims (statements without supporting evidence)
- [ ] Bilingual docs are consistent (if maintained)

## Course Material References

- `11_tcc/03_Estrutura-do-TCC/01-Introdução.pdf`: Introduction structure
- `11_tcc/03_Estrutura-do-TCC/02-Material e Métodos.pdf`: Methods documentation
- `11_tcc/03_Estrutura-do-TCC/03-Resultados e Discussão.pdf`: Results presentation
- `11_tcc/03_Estrutura-do-TCC/04-Conclusão.pdf`: Conclusion framing
- `17_Fundamentos-de-redacao-tecnico-cientifica`: Scientific writing style
- `.agents/skills/research-thesis-support/`: Reusable thesis support patterns
