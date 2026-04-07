# Repo-Local References

These files are supporting references for the repository-specific LLM rules and workflows.

- `usp-mba-course-map.md`: curated mapping from USP MBA course modules to the analytical and documentation tasks in this repository
- `usp-mba-course-inventory.generated.md`: generated inventory of the `aulas/` corpus used to build the curated map

Use the curated map first. Open the generated inventory only when you need to trace a topic back to representative course assets.

To refresh the generated inventory from the source `aulas/` directory:

```powershell
python .\docs\llm\scripts\build_usp_mba_course_inventory.py --source C:\google-drive\cursos\usp\mba\data-science\aulas --output .\docs\llm\references
```
