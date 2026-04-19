# Thesis Writing Main Workflow

**Purpose**: Orchestrate all thesis writing, formatting, bibliography, and submission tasks following USP/ESALQ TCC specifications.

**Scope**: Complete thesis document (Portuguese primary, English secondary), ABNT formatting, bibliography, plagiarism check, and submission preparation.

**Trigger**: Use this workflow when writing thesis chapters, formatting documents, adding references, checking plagiarism, or preparing for submission.

---

## Phase 1: Discovery & Planning

### Step 1: Identify Writing Task

**Determine what type of thesis work is needed:**

| Task Type | Entry Point | Key Workflow |
|-----------|-------------|--------------|
| **Writing new chapter** | `docs/llm/workflows/thesis-completion-guide.md` §Chapter Structure | Chapter templates |
| **Adding citations** | `docs/llm/workflows/thesis-bibliography-integration.md` | Bibliography map |
| **Method selection** | `docs/llm/workflows/tcc-method-selection.md` | Method framework |
| **Formatting check** | `docs/llm/workflows/tcc-formatting-abnt-review.md` | ABNT vs TCC Manual |
| **Plagiarism scan** | `docs/llm/workflows/thesis-plagiarism-check.md` | Originality check |
| **Bilingual sync** | `docs/llm/workflows/bilingual-notebook-sync.md` | EN ↔ pt-BR |
| **Analysis→Thesis sync** | `docs/llm/workflows/tcc-analysis-and-writing-sync.md` | Evidence flow |
| **Advisor submission** | This workflow §Phase 5 | Submission prep |
| **Final formatting** | `docs/llm/workflows/tcc-formatting-abnt-review.md` | ABNT compliance |

### Step 2: Load Thesis Knowledge Base

**Read required resources based on task:**

**For chapter writing:**
- [ ] `docs/llm/workflows/thesis-completion-guide.md` — Structure, templates, deadlines
- [ ] `.agents/skills/research-thesis-support/SKILL.md` — Chapter structure guidance
- [ ] `docs/thesis_conclusion.md` — Current findings summary
- [ ] `docs/city_thesis_conclusion_addendum.md` — Municipality analysis

**For methods/methodology:**
- [ ] `docs/llm/workflows/tcc-method-selection.md` — Method framework
- [ ] `.agents/skills/thesis-bibliography/SKILL.md` §1-5 — Method references
- [ ] `docs/llm/references/usp-mba-course-map.md` — Course alignment

**For formatting:**
- [ ] `docs/llm/workflows/tcc-formatting-abnt-review.md` — ABNT vs TCC Manual
- [ ] `docs/llm/rules/tcc-deliverables-and-argument.md` — Deliverable expectations

**For bibliography:**
- [ ] `.agents/skills/thesis-bibliography/SKILL.md` — Complete reference database
- [ ] `docs/llm/workflows/thesis-bibliography-integration.md` — Citation map

### Step 3: Determine Language Scope

**Bilingual thesis requirement check:**

| Change Type | Scope | Required Actions |
|-------------|-------|------------------|
| **Portuguese chapter** | Primary | Write pt-BR, then sync to EN |
| **English chapter** | Secondary | Write EN, then sync to pt-BR |
| **Abstract** | Both | Resumo (PT) + Abstract (EN) in both versions |
| **Figures/Tables** | Both | Same data, translated captions |
| **Bibliography** | Both | Same references, proper format |
| **Finding changes** | Both | Update both versions identically |

**Synchronization workflow:**
```
1. Write/change one language version first
2. Apply equivalent changes to other version
3. Verify: Same results, numbers, citations
4. Check: Translated captions, consistent terminology
```

---

## Phase 2: Content Development

### Step 4: Chapter Writing (By Type)

#### Chapter 1: Introduction (Introdução)

**Required elements:**
- Contexto (Context)
- Problema (Problem statement)
- Questão de pesquisa (Research question)
- Objetivos geral e específicos (Objectives)
- Justificativa (Justification)
- Delimitação (Scope/delimitation)

**Execute:**
```
1. Read `docs/llm/workflows/thesis-completion-guide.md` §Introduction
2. Use 5-sentence abstract formula for problem statement
   Reference: `.agents/skills/thesis-bibliography/SKILL.md` §10
3. Cite methodology sources:
   - Gil (2010), Lakatos & Marconi (2010) — Research design
   - Ferreira (2010), Affonso & Araújo (2016) — Public spending context
   Reference: `docs/llm/workflows/thesis-bibliography-integration.md` §Chapter 1
4. Write in Portuguese (primary)
5. Sync to English (secondary)
6. Verify bilingual alignment
```

#### Chapter 2: Material and Methods (Material e Métodos)

**Required sections:**
- Data sources (IBGE, Transparency, CGU)
- Data architecture (Bronze/Silver/Gold)
- Analytical methods (Statistics, ML, Clustering)
- Software and tools

**Execute:**
```
1. Read `docs/llm/workflows/tcc-method-selection.md`
2. Map methods to references:
   - Descriptive stats → Bussab & Morettin (2017)
   - Regression → Wooldridge (2020)
   - ML → James et al. (2021)
   - Clustering → Hartigan & Wong (1979), Rousseeuw (1987)
   Reference: `.agents/skills/thesis-bibliography/SKILL.md` §2-5
3. Write methodology with proper citations
4. Include: Data scope, period, variables
5. Sync English version
6. Verify method names consistent across languages
```

#### Chapter 3: Results and Discussion (Resultados e Discussão)

**Required sections:**
- Descriptive analysis
- Statistical modeling results
- Machine learning results
- Clustering analysis
- Interpretation and implications

**Execute:**
```
1. Read `docs/thesis_conclusion.md` — Extract findings
2. Read `docs/city_thesis_conclusion_addendum.md` — City-level results
3. For each finding:
   - State statistic (r, β, p, R²)
   - Cite interpretation framework
   - Reference detection capacity literature (Power, 2007; Liu et al., 2016)
4. Create/update tables and figures
5. Write results narrative in Portuguese
6. Translate to English (maintain identical numbers)
7. Verify: Same statistics in both versions
```

#### Chapter 4: Conclusion (Conclusão)

**Required sections:**
- Summary of findings
- Limitations
- Policy implications
- Future work

**Execute:**
```
1. Synthesize key findings from Chapter 3
2. Cite limitation literature (Wooldridge, 2020 — causality)
3. Connect to public administration literature
4. Write limitations honestly
5. Propose future work with method citations
6. Sync both languages
```

---

## Phase 3: Bibliography & Citations

### Step 5: Citation Integration

**Execute:**
```
1. Read `docs/llm/workflows/thesis-bibliography-integration.md`
2. For each section, check required citations:
   - Chapter 1: Gil, Lakatos, Ferreira, etc.
   - Chapter 2: Method-specific references
   - Chapter 3: Interpretation references
   - Chapter 4: Limitation references
3. Insert citations using ABNT format:
   "Texto" (AUTOR, Ano, p. xx)
   Autor (Ano) observa que...
4. Ensure bilingual sync:
   - PT: "Wooldridge (2020) propõe..."
   - EN: "Wooldridge (2020) proposes..."
5. Build reference list alphabetically
```

**Reference database:** `.agents/skills/thesis-bibliography/SKILL.md`

### Step 6: Reference List Compilation

**Format per ABNT NBR 6023:2023:**
```
BUSSAB, W. O.; MORETTIN, P. A. Estatística básica. 8. ed. São Paulo: Saraiva, 2017.

JAMES, G. et al. An introduction to statistical learning. 2nd ed. New York: Springer, 2021.

WOOLDRIDGE, J. M. Introductory econometrics: a modern approach. 7th ed. Boston: Cengage, 2020.
```

**Verify:**
- [ ] Alphabetical by first author surname
- [ ] Same references in both language versions
- [ ] No orphans (cited in text, in list; in list, cited in text)
- [ ] ABNT format correct

---

## Phase 4: Formatting & Quality

### Step 7: ABNT/TCC Formatting Check

**Execute:**
```
1. Read `docs/llm/workflows/tcc-formatting-abnt-review.md`
2. Compare TCC Manual vs ABNT NBR 14724:2011
3. Check page setup:
   - Margins: Left 3cm, Right 2cm, Top 3cm, Bottom 2cm (verify TCC Manual)
   - Font: Times New Roman or Arial 12pt (verify TCC Manual)
   - Line spacing: 1.5
4. Check pre-textual elements:
   - Cover (Capa) with ESALQ requirements
   - Abstract/Resumo (both languages)
   - Table of contents
5. Check textual structure:
   - Chapter numbering
   - Section hierarchy
6. Check references:
   - ABNT NBR 6023:2023 format
   - Alphabetical order
7. Document any deviations from ABNT (TCC Manual takes precedence)
```

### Step 8: Writing Quality Check

**Execute:**
```
1. Read `docs/llm/workflows/thesis-completion-guide.md` §Writing Quality
2. Apply Gopen & Swan principles:
   - Subject-verb proximity
   - Stress position
   - Old before new
   - Action in verbs
3. Check language-specific guidelines:
   - PT: Follow `17_Fundamentos-de-redacao-tecnico-cientifica`
   - EN: Follow academic writing conventions
4. Verify evidence language strength:
   - Strong evidence → "demonstrates," "shows"
   - Moderate → "suggests," "is consistent with"
   - Weak → "explores," "preliminary evidence"
5. Delete hedging: "suggests" not "may suggest"
6. Be specific: "R² = 0.62" not "performance improved"
```

---

## Phase 5: Validation & Verification

### Step 9: Plagiarism Check (Mandatory)

**Execute:**
```
1. Read `docs/llm/workflows/thesis-plagiarism-check.md`
2. Self-review:
   - All direct quotes have quotation marks
   - All paraphrases have citations
   - No mosaic plagiarism
   - Self-translation documented
3. Automated scan:
   - Turnitin (if university access)
   - Grammarly Premium
   - Or: Manual Google Scholar check
4. Check both language versions separately
5. Target: <15% similarity (excluding references)
6. Document scan results
7. Fix any flagged passages
```

**Special for bilingual:**
- Self-translation is NOT plagiarism (document it)
- Translation plagiarism: Cite original source
- Cross-language plagiarism: Manual check required

### Step 10: Analysis-Thesis Alignment Check

**Execute:**
```
1. Read `docs/llm/workflows/tcc-analysis-and-writing-sync.md`
2. Verify evidence traceability:
   - Every claim → notebook/script reference
   - Every statistic → source data
   - Every figure → generation code
3. Check validity:
   - No data leakage claims
   - Causality not overstated
   - Limitations acknowledged
4. Verify bilingual sync:
   - Same numbers in PT and EN
   - Same interpretations
   - Equivalent phrasing
5. Regenerate presentation assets if needed:
   python scripts/build_thesis_presentation_assets.py
```

---

## Phase 6: Final Preparation & Submission

### Step 11: Pre-Submission Checklist

**Complete for both language versions:**

#### Portuguese Version (Primary)
- [ ] All 4 chapters complete
- [ ] Resumo (250 words) + Abstract
- [ ] ABNT formatting verified against TCC Manual
- [ ] References: ABNT NBR 6023:2023
- [ ] Figures: High-res, Portuguese captions
- [ ] Tables: Numbered, Portuguese titles
- [ ] Plagiarism scan: <15% similarity
- [ ] Grammar check: Portuguese

#### English Version (Secondary)
- [ ] All 4 chapters complete
- [ ] Abstract complete
- [ ] References: ABNT or APA
- [ ] Figures: English captions
- [ ] Tables: English titles
- [ ] Plagiarism scan: <15% similarity
- [ ] Proofreading: English native/competent

#### Bilingual Synchronization
- [ ] Same statistical results
- [ ] Same figures/tables (captions differ)
- [ ] Same bibliography (order may differ)
- [ ] Equivalent methodology descriptions
- [ ] Consistent terminology

#### Supporting Materials
- [ ] Notebooks: Reproducible (both languages)
- [ ] QGIS maps: Ready
- [ ] Power BI dashboard: Ready
- [ ] Presentation: Prepared (defense language)
- [ ] GitHub repo: Accessible

### Step 12: Final Review

**Execute:**
```
1. Print/read both versions (different times of day)
2. Read aloud (catches awkward phrasing)
3. Check: First sentence of each paragraph
4. Check: Last sentence of each chapter
5. Verify: Cross-references work
6. Spell check: Both languages
7. Format check: Consistent headings
8. Number check: All statistics match
```

### Step 13: Submission Preparation

**For USP/ESALQ portal upload:**
```
1. Final PDF generation:
   - PT: `TCC_Enok_Antonio_Jesus_PT.pdf`
   - EN: `TCC_Enok_Antonio_Jesus_EN.pdf`
2. Verify file sizes (<portal limit)
3. Test PDF opening on different devices
4. Prepare metadata:
   - Title (both languages)
   - Abstract (both languages)
   - Keywords (both languages)
   - Advisor: Prof. Dr. Carlos Nabil Ghobril
5. Upload to USP/ESALQ portal
6. Confirm submission receipt
7. Save confirmation number
```

---

## Related Workflows & Skills

### Workflows (in execution order)
1. `docs/llm/workflows/thesis-completion-guide.md` — Structure and timeline
2. `docs/llm/workflows/tcc-method-selection.md` — Methodology framework
3. `docs/llm/workflows/tcc-analysis-and-writing-sync.md` — Evidence flow
4. `docs/llm/workflows/thesis-bibliography-integration.md` — Citations
5. `.agents/skills/thesis-bibliography/SKILL.md` — Reference database
6. `docs/llm/workflows/tcc-formatting-abnt-review.md` — Formatting
7. `docs/llm/workflows/thesis-plagiarism-check.md` — Originality
8. `docs/llm/workflows/bilingual-notebook-sync.md` — Bilingual sync
9. `.windsurf/workflows/bilingual-doc-sync.md` — Document sync

### Rules (for guidance)
- `docs/llm/rules/tcc-deliverables-and-argument.md` — Deliverable expectations
- `docs/llm/rules/usp-mba-course-context.md` — USP MBA guidance
- `docs/llm/rules/course-material-grounding.md` — Course alignment

### Skills (for reusable patterns)
- `.agents/skills/research-thesis-support/SKILL.md` — Chapter structure
- `.agents/skills/thesis-bibliography/SKILL.md` — 60+ references
- `.agents/skills/document-conversion/SKILL.md` — File handling

---

## Quick Decision Tree

```
What thesis task?
├── Writing chapter → thesis-completion-guide.md
├── Adding citations → thesis-bibliography-integration.md + bibliography/SKILL.md
├── Method selection → tcc-method-selection.md
├── Formatting check → tcc-formatting-abnt-review.md
├── Plagiarism scan → thesis-plagiarism-check.md
├── Bilingual sync → bilingual-notebook-sync.md / bilingual-doc-sync.md
└── Final submission → This workflow §Phase 6
```

---

## Exit Criteria

**Thesis is submission-ready when:**

- [ ] Portuguese version: Complete, formatted, cited
- [ ] English version: Complete, formatted, cited
- [ ] Both versions: Synchronized, identical results
- [ ] Bibliography: Complete, ABNT formatted
- [ ] Plagiarism check: Passed (<15%), documented
- [ ] Formatting: Follows TCC Manual (or documented deviations)
- [ ] Writing quality: Meets academic standards
- [ ] Analysis alignment: Evidence supports claims
- [ ] Supporting materials: Ready (notebooks, maps, dashboard)
- [ ] Submitted: To USP/ESALQ portal with confirmation

---

## Critical Success Factors

1. **Bilingual synchronization**: Changes to one version must flow to the other
2. **Evidence traceability**: Every claim must map to notebook/script output
3. **Citation completeness**: Every method, every interpretation, every framework must be cited
4. **Honest limitations**: State what the analysis cannot show
5. **Format compliance**: TCC Manual requirements take precedence over general ABNT
6. **Plagiarism prevention**: When in doubt, cite; document self-translations
