# Thesis Plagiarism Check Workflow

Use this workflow to systematically scan the final thesis document for plagiarism before submission, ensuring academic integrity and proper attribution.

## Purpose

Plagiarism is a serious academic offense that can result in thesis rejection. This workflow provides:
- Pre-submission plagiarism scanning procedures
- Tools and methods for plagiarism detection
- Self-check techniques for unintentional plagiarism
- Quoting and paraphrasing best practices
- Documentation of originality

## Bilingual Thesis Requirement

This thesis requires **plagiarism checking for both language versions**:

| Version | Language | Specific Checks |
|---------|----------|-----------------|
| **Portuguese** | pt-BR | Translation plagiarism (English→Portuguese), proper ABNT citation |
| **English** | en | Translation plagiarism (Portuguese→English), self-translation attribution |

**Special considerations for bilingual theses:**

1. **Self-translation**: If you translate your own Portuguese text to English (or vice versa), this is **not plagiarism** but document it:
   ```
   "This section is based on the author's Portuguese text (Chapter 3, Section 3.2), 
   translated and adapted for the English version."
   ```

2. **Citing translations**: When citing a translated work:
   ```
   Portuguese: "Como observado por Foucault (1975/2014, p. 42) [tradução nossa]..."
   English: "As Foucault (1975/2014, p. 42) [our translation] observed..."
   ```

3. **Both versions must be checked separately** — plagiarism in one version invalidates the entire thesis.

4. **Cross-language plagiarism**: Automated tools may not detect when content was copied from a foreign language source. **Manual verification required.**

## Plagiarism Definition (ABNT/Academic Standards)

**Plagiarism** is the act of presenting someone else's work, ideas, or expressions as your own without proper attribution.

### Types of Plagiarism to Avoid

| Type | Description | Example |
|------|-------------|---------|
| **Direct plagiarism** | Copying text verbatim without quotes or citation | Pasting a paragraph from a paper without quotation marks |
| **Mosaic plagiarism** | Mixing copied phrases with your own words | Interspersing copied sentences with minor rewording |
| **Paraphrasing plagiarism** | Rewording someone else's ideas without citation | Explaining a method from a paper without citing the source |
| **Self-plagiarism** | Reusing your own previous work without disclosure | Copying text from your earlier research project |
| **Idea plagiarism** | Presenting someone else's concept as original | Claiming an analytical framework as your own invention |
| **Data plagiarism** | Using someone else's data without attribution | Using downloaded datasets without citing the source |
| **Code plagiarism** | Copying code without attribution | Using Stack Overflow solutions without comment |
| **Translation plagiarism** | Translating text without citing the original | Translating a foreign paper paragraph without attribution |

## Pre-Submission Plagiarism Check Steps

### Step 1: Self-Review (Manual Check)

**Before using automated tools, perform these manual checks:**

#### 1.1 Quote Verification
- [ ] All direct quotes use quotation marks ("...")
- [ ] All direct quotes include page numbers in citation
- [ ] Block quotes (>40 words) are properly indented
- [ ] Quote length is appropriate (not excessive)

**ABNT citation format for direct quotes:**
```
"Texto citado diretamente" (AUTOR, Ano, p. xx).

Ou:

Segundo Autor (Ano, p. xx), "texto citado diretamente".
```

#### 1.2 Citation Completeness Check
- [ ] Every paragraph with external information has at least one citation
- [ ] Every figure/table from external sources is cited
- [ ] Every methodological choice is justified with a citation
- [ ] Every statistical technique has a source
- [ ] Every dataset is properly attributed

#### 1.3 Paraphrase Quality Check
- [ ] Paraphrased content is substantially reworded (not just word substitution)
- [ ] Original meaning is preserved
- [ ] Source is cited even when paraphrased
- [ ] Sentence structure is changed, not just words

**Good paraphrase example:**
> Original (Wooldridge, 2020): "Heteroskedasticity-robust standard errors are valid in the presence of heteroskedasticity of unknown form."

> Paraphrase: Conforme Wooldridge (2020), os erros padrão robustos à heterocedasticidade são adequados mesmo quando a forma específica da heterocedasticidade é desconhecida.

#### 1.4 Self-Plagiarism Check
- [ ] No text copied from your own previous work (research project, papers)
- [ ] If reusing previous analysis, it is explicitly disclosed
- [ ] If extending previous work, the relationship is clear

### Step 2: Automated Plagiarism Scanning

#### 2.1 Available Plagiarism Detection Tools

| Tool | Type | Best For | Access |
|------|------|----------|--------|
| **Turnitin** | Commercial | Academic theses, comprehensive database | University subscription |
| **iThenticate** | Commercial | Research papers, professional documents | Paid service |
| **Grammarly Premium** | Commercial | Writing quality + plagiarism | Subscription |
| **PlagScan** | Commercial | Documents, academic use | Subscription |
| **Copyscape** | Web | Web content duplication | Per-search payment |
| **DupliChecker** | Free/Web | Quick checks, limited scope | Free tier |
| **SmallSEOTools** | Free/Web | Quick preliminary scan | Free |
| **Google Scholar** | Free | Self-check citations | Free |
| **Crossref Similarity Check** | Institutional | Academic publishers | Institutional access |

#### 2.2 Recommended Scanning Approach

**For MBA Thesis (USP/ESALQ context):**

1. **Primary Tool**: **Turnitin** (if available through university)
   - Most comprehensive academic database
   - Includes previous theses and papers
   - Accepted by most Brazilian universities

2. **Secondary Check**: **Grammarly Premium** or **PlagScan**
   - Catches what Turnitin might miss
   - Better web content detection

3. **Self-Check**: **Google Scholar** + **Manual verification**
   - Search key phrases from your thesis
   - Verify no unintended matches

#### 2.3 Scanning Procedure

```bash
# Prepare thesis document
1. Export thesis to PDF and DOCX formats
2. Remove personal information if using external tools
3. Ensure references are properly formatted

# Run scans
4. Upload to Turnitin (or equivalent)
5. Review similarity report
6. Address any flagged sections
7. Re-scan after corrections
8. Document final similarity score
```

### Step 3: Interpreting Similarity Reports

#### 3.1 Understanding Similarity Scores

| Score | Interpretation | Action Required |
|-------|----------------|-----------------|
| **0-5%** | Excellent, minimal matches | Review matches to ensure proper citation |
| **5-15%** | Acceptable for thesis | Review all matches, may need some rewording |
| **15-25%** | High, needs attention | Significant reworking required |
| **>25%** | Critical plagiarism risk | Major revision needed before submission |

**Note:** Similarity score alone does not indicate plagiarism. A high score could be:
- Properly cited references (acceptable)
- Common phrases in the field (acceptable)
- Quotations with proper attribution (acceptable)
- Bibliography/reference list (usually excluded)

#### 3.2 Reviewing Matches

**For each flagged passage, check:**

1. **Is it properly cited?**
   - Yes → Keep, ensure citation format is correct
   - No → Add citation or remove

2. **Is it a direct quote without quotation marks?**
   - Yes → Add quotation marks and page citation
   - No → Continue checking

3. **Is it a common phrase in the field?**
   - Yes (e.g., "ordinary least squares regression") → Acceptable without citation
   - No → Requires citation

4. **Is it your own previous work?**
   - Yes → Add self-citation or reword
   - No → Cite the original source

5. **Is it from the bibliography?**
   - Yes → Most tools exclude these, but verify
   - No → Address appropriately

### Step 4: Common Thesis-Specific Plagiarism Risks

#### 4.1 Methodology Section Risks

**High-risk areas:**
- Copying methodology descriptions from papers
- Using standard textbook explanations without citation
- Translating foreign methodology papers without attribution

**Safe approach:**
```
❌ Risky:
"A análise de regressão linear múltipla examina a relação entre 
uma variável dependente e várias variáveis independentes."
(No citation - appears to be original, but it's standard textbook content)

✅ Safe:
"Segundo Wooldridge (2020), a regressão linear múltipla permite 
examinar a relação entre uma variável dependente e várias 
variáveis independentes simultaneamente."

✅ Also safe (paraphrase):
"Este estudo emprega regressão linear múltipla (Wooldridge, 2020) 
para examinar as relações entre transferências federais 
e indicadores socioeconômicos."
```

#### 4.2 Literature Review Risks

**High-risk areas:**
- Copying summaries of other papers
- Using review paper content without attribution
- Translating abstracts

**Safe approach:**
- Always cite the original paper being summarized
- Use phrases like: "Segundo Autor (Ano)..." or "Como observado por Autor (Ano)..."

#### 4.3 Results/Discussion Risks

**High-risk areas:**
- Copying interpretation frameworks from other studies
- Using standard statistical interpretations without citation
- Borrowing conclusion structures

**Safe approach:**
- Cite papers that used similar interpretation frameworks
- Cite statistical method sources when discussing results

#### 4.4 Code and Data Risks

**High-risk areas:**
- Copying code from Stack Overflow, GitHub without attribution
- Using datasets without proper citation
- Reproducing analyses from papers without attribution

**Safe approach:**
```python
# Add comments for code sources

# Adapted from: https://stackoverflow.com/questions/xxx
# Author: StackOverflowUser
# License: CC BY-SA 4.0

def my_function():
    pass

# Or for data:
# Source: IBGE - Censo Demográfico 2022
# URL: https://www.ibge.gov.br/
# Accessed: 2024-01-15
```

### Step 5: Final Documentation

#### 5.1 Plagiarism Check Log

Create a file: `docs/plagiarism_check_log.md`

```markdown
# Plagiarism Check Log

## Final Thesis Submission

**Date of check:** YYYY-MM-DD
**Document version:** Final draft v1.0
**Tools used:** Turnitin, Grammarly Premium

### Similarity Scores

- Turnitin: XX% (YY% from references excluded)
- Grammarly: XX%

### Flagged Sections Reviewed

1. **Section:** Introdução, paragraph 3
   **Match:** 45% similarity with paper X
   **Resolution:** Added proper citation (Wooldridge, 2020)
   **Status:** Resolved

2. **Section:** Metodologia, table description
   **Match:** Direct quote without quotes
   **Resolution:** Added quotation marks and page number
   **Status:** Resolved

[Continue for all flagged sections...]

### Final Verification

- [ ] All direct quotes have quotation marks
- [ ] All paraphrased content is properly cited
- [ ] All methodology choices have citations
- [ ] All datasets are attributed
- [ ] All code sources are commented
- [ ] Bibliography matches in-text citations
- [ ] Similarity score is acceptable (<15%)

**Checked by:** [Your name]
**Date:** YYYY-MM-DD
```

#### 5.2 Statement of Originality

Include in thesis front matter:

```
DECLARAÇÃO DE ORIGINALIDADE

Declaro que o presente trabalho é de minha autoria e 
originalidade. As fontes utilizadas estão devidamente 
citadas conforme as normas da ABNT. 

Este trabalho foi submetido à verificação de plágio 
utilizando as ferramentas [Turnitin/Grammarly/etc.], 
com resultado de X% de similaridade (após exclusão de 
referências bibliográficas).

[Nome do autor]
[Data]
```

## Best Practices for Avoiding Plagiarism

### 1. When Taking Notes

- **Always** record the source when copying text
- Use quotation marks in notes for direct quotes
- Note page numbers for all citations
- Use reference management software (Zotero, Mendeley)

### 2. When Writing

- Write in your own words first, then add citations
- Use "According to..." or "As stated by..." for attribution
- Check that every non-obvious claim has a citation
- Read your work aloud - if it sounds like someone else, it probably is

### 3. When Citing

- When in doubt, cite
- Better to over-cite than under-cite
- Cite the original source, not secondary sources
- Use page numbers for direct quotes and specific ideas

### 4. When Paraphrasing

1. Read the original text
2. Close the source
3. Write from memory in your own words
4. Check against original to ensure accuracy
5. Add citation

## Tools and Resources

### Reference Management
- **Zotero** (Free) - zotero.org
- **Mendeley** (Free) - mendeley.com
- **EndNote** (Paid) - endnote.com
- **Paperpile** (Paid) - paperpile.com

### Writing Assistance
- **Grammarly** - grammarly.com
- **Hemingway Editor** - hemingwayapp.com
- **LanguageTool** - languagetool.org

### Plagiarism Detection
- **Turnitin** - turnitin.com (University access)
- **iThenticate** - ithenticate.com
- **PlagScan** - plagscan.com

## Related Resources

- `.agents/skills/thesis-bibliography/SKILL.md` — Proper citation formats
- `docs/llm/workflows/thesis-bibliography-integration.md` — When to cite
- `docs/llm/workflows/thesis-completion-guide.md` — Final thesis checklist
- USP/ESALQ Academic Integrity Policy
- ABNT NBR 6023:2023 (References)
- ABNT NBR 14724:2020 (Academic Work Presentation)

## Emergency Plagiarism Fix

**If you discover high similarity before submission:**

1. **Don't panic** - most issues are fixable
2. **Identify the source** - find the original text
3. **Choose an action**:
   - Add proper citation if missing
   - Add quotation marks for direct quotes
   - Rewrite in your own words if paraphrased poorly
   - Remove if unnecessary
4. **Re-scan** the document
5. **Document** what was changed

**Remember:** Proper citation transforms potential plagiarism into legitimate academic work.
