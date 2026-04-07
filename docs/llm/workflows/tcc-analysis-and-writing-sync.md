# TCC Analysis And Writing Sync Workflow

Use this repo-local workflow after the shared workflow in `.windsurf/workflows/research-analysis-cycle.md`.

This file should only capture the repository surfaces that must stay aligned when thesis-related analysis changes.

## Repository-Specific Steps

1. Identify what changed:
   - data boundary
   - preprocessing or feature logic
   - method or hyperparameters
   - figure or table outputs
   - interpretation or conclusion
2. Trace the changed evidence surfaces:
   - `src/`
   - `scripts/`
   - `notebooks/`
   - `docs/`
   - `docs/llm/` when the local guidance should change too
3. Update the nearest thesis-facing narrative in the same change:
   - methodology note
   - result summary
   - limitation or caveat
   - any repo-level summary that would otherwise become stale
4. Check whether `docs/llm/` should change too when the thesis method framing, chapter structure, or repo-local analytical guidance has shifted.
