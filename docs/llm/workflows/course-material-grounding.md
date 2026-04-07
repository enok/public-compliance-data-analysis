# Course Material Grounding Workflow

Use this workflow when a task should be grounded in the USP MBA class corpus under `/mnt/hgfs/shared/data-science/aulas`, not just in generic best practices.

## Steps

1. Classify the request:
   - class summary or study support
   - thesis method selection
   - repository implementation guidance
   - governance, writing, or presentation support
2. Read `docs/llm/references/usp-mba-course-map.md` first and identify the closest course anchors.
3. If the request falls outside the curated map or spans several modules, inspect `docs/llm/references/usp-mba-course-inventory.generated.md` to locate the missing course folder and representative assets.
4. Separate generic guidance from class-specific guidance:
   - reusable Data Science guidance should come from shared `dev-tools` skills, rules, and workflows
   - course-specific framing, terminology, and method bias should come from local `docs/llm/`
5. For thesis or deliverable work, also review:
   - `docs/llm/rules/usp-mba-course-context.md`
   - `docs/llm/rules/tcc-deliverables-and-argument.md`
   - `docs/llm/workflows/tcc-method-selection.md`
   - `docs/llm/workflows/tcc-analysis-and-writing-sync.md`
6. Name the exact class modules that support the recommendation, and call out any meaningful gap between the class material and the proposed approach.

## Exit Criteria

- The answer identifies which course modules are relevant.
- Generic guidance and class-specific guidance are clearly separated.
- The recommendation is consistent with both the repository context and the available course material.
