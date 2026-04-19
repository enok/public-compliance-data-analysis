# Security Check Workflow

**⚠️ MANDATORY: Security checks MUST pass before any commit or push.**

This workflow is enforced via git hooks and must not be bypassed without explicit justification.

## Enforcement

Git hooks automatically run security checks:
- **Pre-commit**: `.git/hooks/pre-commit` — runs before every commit
- **Pre-push**: `.git/hooks/pre-push` — runs before every push

To bypass (requires explicit justification):
- Commit: `git commit --no-verify`
- Push: `git push --no-verify`

**Never use `--no-verify` unless:**
- You're intentionally testing the hook itself
- There's an emergency with documented approval
- You've verified security manually and documented the exception

## What Gets Checked

```bash
./scripts/security_check_this_repo.sh
```

### Security Scanners
| Check | Tool | Purpose |
|-------|------|---------|
| Secrets detection | gitleaks | Find API keys, tokens, passwords |
| Secrets detection (alt) | trufflehog | Additional secrets coverage |
| Python vulnerabilities | pip-audit | Dependency CVE scanning |
| Python security | bandit | Static analysis for Python |
| General static analysis | semgrep | Multi-language security patterns |
| Infra as Code | checkov, tfsec | Terraform/AWS security |
| Shell script lint | shellcheck | Bash security & correctness |
| Container/FS scan | trivy, grype | Vulnerability matching |
| SBOM generation | syft | Dependency inventory |
| YAML validation | yamllint | Config file correctness |

### What Triggers a Mandatory Check

Any change to:
- [ ] Application code (`src/`)
- [ ] Pipeline code (`scripts/`, `src/ingestion/`, `src/processing/`)
- [ ] Infrastructure (`infra/`)
- [ ] Shell/PowerShell automation (`scripts/*.sh`, `scripts/*.ps1`)
- [ ] GitHub workflows (`.github/workflows/`)
- [ ] LLM configs (`.windsurf/`, `.agents/`, `docs/llm/`)
- [ ] Operational documentation with commands
- [ ] Dependencies (`requirements.txt`, `pyproject.toml`)

## Running Checks Manually

```bash
# Full security check
./scripts/security_check_this_repo.sh

# Specific checks (if tools installed)
gitleaks detect --source . --no-git --redact
bandit -r src -f screen
pip-audit
shellcheck scripts/*.sh
```

## Interpreting Results

### PASS
Continue with commit/push.

### FAIL (Blocking)
**STOP. Do not commit/push.**

Fix the issue:
1. Run the check manually to see details
2. Fix the root cause (not symptoms)
3. Re-run the security script
4. Only proceed when all blocking checks pass

Common blocking issues:
- **Secret detected**: Remove the secret, rotate it if exposed, use environment variables
- **Vulnerability**: Update dependency or document why the CVE is not exploitable
- **Unsafe code pattern**: Refactor to safe pattern
- **Shell issue**: Fix shellcheck warning with proper quoting/validation

### SKIP (Non-blocking, but track)
Skipped means the tool isn't installed or the file type is absent.

**Action needed if tool missing:**
```bash
# Install security tools (Ubuntu/Debian)
sudo apt-get install gitleaks bandit pip-audit shellcheck

# Or use pip for Python tools
pip install bandit pip-audit

# Or document in README why tool isn't available
```

## Repository-Specific Steps

1. **Before starting work**, verify hooks are executable:
   ```bash
   ls -la .git/hooks/pre-commit .git/hooks/pre-push
   # Should show -rwxr-xr-x (executable)
   ```

2. **If hooks are missing**, restore them:
   ```bash
   # From repo root
   cat > .git/hooks/pre-commit << 'EOF'
   #!/bin/sh
   REPO_ROOT="$(git rev-parse --show-toplevel)"
   "${REPO_ROOT}/scripts/security_check_this_repo.sh" || exit 1
   EOF
   chmod +x .git/hooks/pre-commit
   ```

3. **Run manually before large changes**:
   ```bash
   ./scripts/security_check_this_repo.sh
   ```

4. **Review failures**:
   - Fix all blocking findings
   - Document exceptions with justification
   - Never commit secrets or credentials

5. **For dev-tools work**, run:
   ```bash
   ./scripts/security_check_dev_tools.sh /mnt/hgfs/shared/dev-tools
   ```

## Reporting Requirements

After any security check, document:
- Which script was run
- Summary of results (pass/fail/skip counts)
- Any failed checks and resolution
- Any skipped checks due to missing tools (install or document gap)
- Any `--no-verify` bypass with justification

## Red Flags (Never Ignore)

- [ ] Hardcoded credentials or API keys
- [ ] AWS tokens, GitHub tokens, database passwords
- [ ] Private keys (`.pem`, `.key` files)
- [ ] `.env` files with real values
- [ ] Notebook outputs showing credentials
- [ ] Shell scripts with unvalidated user input
- [ ] SQL injection patterns
- [ ] Unsafe deserialization

**If found**: Remove immediately, rotate credentials if exposed, re-run check.

## Related Workflows

- `.windsurf/workflows/security-report.md` — Full security report generation
- `.windsurf/workflows/review.md` — Code review with security focus
- `.windsurf/workflows/pre-pr-check.md` — Pre-PR validation
- `.windsurf/rules/security.md` — Security principles and rules
- `.windsurf/rules/security-check-required.md` — Mandatory check rule

