# Security Check Workflow

Use this repo-local workflow with:

- `.windsurf/workflows/security-report.md`
- `.windsurf/workflows/review.md`
- `.windsurf/workflows/pre-pr-check.md`

## Repository-Specific Steps

1. Identify whether the change touches any of these categories:
   - application code
   - pipeline code
   - infrastructure
   - shell or PowerShell automation
   - GitHub workflows
   - local LLM rules, skills, prompts, or workflows
   - operational documentation that contains commands or procedures
2. If yes, run the repository security check:
   - `./scripts/security_check_this_repo.sh`
3. If the work is being done in `dev-tools`, run:
   - `./scripts/security_check_dev_tools.sh /path/to/dev-tools`
4. Review every failure and blocking finding before considering the task complete.
5. If a check is skipped because the file type is absent, leave it skipped; do not fake coverage.
6. If a needed tool is missing, install it or document the gap and residual risk explicitly.
7. Treat secret leaks, unsafe command patterns, workflow security issues, and dependency findings as blocking unless there is a specific reviewed exception.

## Reporting Expectations

- Summarize which script was run.
- Summarize failed checks and whether they were fixed or intentionally deferred.
- Call out any skipped checks that reflect tooling or environment gaps rather than absent file types.
