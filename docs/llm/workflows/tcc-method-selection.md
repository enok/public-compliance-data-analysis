# TCC Method Selection Workflow

Use this workflow when the thesis question is clear enough to choose or refine the analytical method.

## Steps

1. Write the current question in one sentence:
   - what outcome or risk is being studied
   - what unit of analysis is used
   - what time boundary matters
   - what decision the result should support
2. Read `docs/llm/references/usp-mba-course-map.md` and shortlist the most relevant course anchors.
3. Confirm what the repository can currently support:
   - available Bronze, Silver, and Gold datasets
   - municipality and year coverage
   - missingness, linkage, and geospatial availability
   - whether the outcome is continuous, binary, categorical, count-based, hierarchical, or exploratory
4. Pick one primary method and one supporting diagnostic or robustness check.
5. Name the evidence artifacts that must exist after the analysis:
   - notebook or script path
   - figure or table
   - methodology note
   - interpretation or limitation note
6. If the method is more complex than the baseline alternative, explain why the simpler method is insufficient for this question.
7. If the result will support governance or compliance claims, add the caveats before running with the stronger narrative.

## Exit Criteria

- The chosen method has a clear reason tied to the thesis question.
- The required data and artifact outputs are explicit.
- The resulting claim strength matches the method's actual limits.
