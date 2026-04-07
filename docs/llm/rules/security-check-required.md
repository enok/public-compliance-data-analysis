# Security Check Required

Use this repo-local rule together with:

- `.windsurf/rules/security.md`
- `.windsurf/rules/command-safety.md`
- `.windsurf/rules/code-rules.md`
- `.windsurf/rules/documentation-and-governance.md`

This file defines the repository-specific minimum security review gate for changes made in this repository and for related work in `dev-tools`.

## Mandatory Policy

- Every new or modified code path must receive a security check before the task is considered complete.
- Every new or modified shell script, PowerShell script, batch file, workflow file, rule file, skill file, or operational document must receive the same security check treatment.
- This applies to implementation code, infrastructure code, automation, local LLM configuration, and supporting operational content.
- Do not treat documentation-like files as automatically safe. Rules, workflows, prompts, scripts, and operational notes can introduce insecure commands, secrets exposure, or unsafe guidance.

## Required Commands

For this repository, run:

```bash
./scripts/security_check_this_repo.sh
```

For a `dev-tools` checkout, run:

```bash
./scripts/security_check_dev_tools.sh /path/to/dev-tools
```

## Minimum Expectations

- Review failures before considering the task done.
- If a tool is skipped because the file type is absent, record that as an intentional skip, not as a silent omission.
- If a tool is unavailable in the current environment, install it or explicitly document the gap and the risk.
- Do not suppress findings without a concrete justification.
- Treat secret-scanning failures, command-injection patterns, unsafe script behavior, and workflow security issues as blocking by default.

## Files In Scope

Apply the mandatory check to changes under:

- `src/`
- `scripts/`
- `tests/`
- `infra/`
- `.github/`
- `.codex/`
- `.agents/`
- `.windsurf/` when repo-local usage depends on it
- `docs/llm/`
- other operational or automation files added later

## Completion Standard

A task that changes in-scope files is not complete until the relevant security-check script has been run and its results have been reviewed.
