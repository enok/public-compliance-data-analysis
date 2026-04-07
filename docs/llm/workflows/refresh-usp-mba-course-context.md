# Refresh USP MBA Course Context Workflow

Use this workflow when new class material is added under `/mnt/hgfs/shared/data-science/aulas` and the thesis repo's LLM guidance should stay current.

## Steps

1. Regenerate the source inventory:
   - Linux or WSL: `python ./docs/llm/scripts/build_usp_mba_course_inventory.py --source /mnt/hgfs/shared/data-science/aulas --output ./docs/llm/references`
   - Windows PowerShell: `python .\docs\llm\scripts\build_usp_mba_course_inventory.py --source C:\google-drive\cursos\usp\mba\data-science\aulas --output .\docs\llm\references`
2. Review the diff in `docs/llm/references/usp-mba-course-inventory.generated.md`:
   - identify new course folders
   - identify new representative assets
   - identify new method, governance, or writing topics that were not previously mapped
3. Update `docs/llm/references/usp-mba-course-map.md`:
   - add new course anchors
   - revise the method defaults if the new material changes the recommended baseline
   - add or replace representative source assets when the new classes are more relevant
4. Check whether the local thesis rules or workflows should change:
   - `docs/llm/rules/course-material-grounding.md`
   - `docs/llm/rules/usp-mba-course-context.md`
   - `docs/llm/rules/tcc-deliverables-and-argument.md`
   - `docs/llm/workflows/course-material-grounding.md`
   - `docs/llm/workflows/tcc-method-selection.md`
   - `docs/llm/workflows/tcc-analysis-and-writing-sync.md`
5. Decide whether anything belongs upstream in the shared toolkit:
   - if the new insight is generic and reusable across projects, add it to `bri-ai-dev-tools`
   - if it is specific to USP MBA coursework or this thesis, keep it in `docs/llm/`
6. Refresh repo-local exports after any shared-toolkit or `toolkit-selection.txt` changes:
   - `.\scripts\sync-llm-configs.ps1`
   - or `./scripts/sync-llm-configs.sh`
7. Re-read `AGENTS.md` and `docs/llm/README.md` only if the entry points or local layout changed.

## Exit Criteria

- The generated inventory reflects the current `aulas/` tree.
- The curated course map reflects the new classes that matter to the thesis.
- The local thesis rules and workflows still point to the right method and writing guidance.
- Any truly reusable insight has been promoted to the shared toolkit instead of being duplicated locally.
