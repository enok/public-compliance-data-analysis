# Security Checkpoint - 2026-04-05

Use this file as the handoff context for the next Codex session.

## Scope

- Primary repo: `/mnt/hgfs/shared/data-science/tcc/code/public-compliance-data-analysis`
- Related repo: `/mnt/hgfs/shared/dev-tools`

## Policy And Runner Changes

### `public-compliance-data-analysis`

Mandatory security-gate policy was added to:

- `AGENTS.md`
- `docs/llm/README.md`
- `docs/llm/rules/security-check-required.md`
- `docs/llm/workflows/security-check-required.md`
- `docs/llm/rules/documentation-and-governance.md`

Runnable scripts added:

- `scripts/security_check_this_repo.sh`
- `scripts/security_check_dev_tools.sh`

### `dev-tools`

Mandatory security-gate policy was added to:

- `AGENTS.md`
- `workflows/README.md`
- `rules/security-check-required.md`
- `workflows/security-check-required.md`

Runnable script added:

- `scripts/security-check-toolkit.sh`

## Secret Cleanup Already Done

### Real credential removed

Cached AWS credentials were cleared from:

- `.idea/workspace.xml`

The following values were blanked:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`

### False-positive examples rewritten

Fake secret examples were rewritten to avoid scanner noise in:

- `.agents/skills/security/AGENTS.md`
- `.agents/skills/security/rules/secrets-no-hardcoded.md`
- `.codex/skills/security/AGENTS.md`
- `.codex/skills/security/rules/secrets-no-hardcoded.md`

The examples now use:

- `EXAMPLE_API_KEY_DO_NOT_USE`
- `EXAMPLE_PASSWORD_DO_NOT_USE`

## Runner Behavior Changes

### `scripts/security_check_this_repo.sh`

Adjusted to:

- scope `gitleaks` to intended repo paths
- scope `trufflehog` to intended repo paths
- use `/tmp/semgrep-settings.yml` for Semgrep
- lint only repo-authored shell and PowerShell scripts
- run `shellcheck -x`
- limit YAML lint to repo-authored YAML-heavy areas

### `scripts/security_check_dev_tools.sh`

Adjusted to:

- scope `gitleaks`
- scope `trufflehog`
- scope `semgrep`
- use `/tmp/semgrep-settings.yml`
- lint only intended repo-authored shell, PowerShell, batch, and YAML areas
- run `shellcheck -x`

### `/mnt/hgfs/shared/dev-tools/scripts/security-check-toolkit.sh`

Adjusted to match the same cleaner scoped behavior as the wrapper script above.

## Last Known Real Findings

### `public-compliance-data-analysis`

#### Bandit

- `src/ingestion/ingestion_utils.py:11`
  - MD5 usage
- `src/processing/base_transformer.py:132`
  - MD5 usage

#### tfsec

File:

- `infra/main.tf:8`

Findings observed:

- missing S3 encryption hardening
- missing bucket logging
- missing versioning
- missing public access block

#### ShellCheck

Files/lines previously flagged:

- `scripts/01_bronze_ingestion.sh:107`
- `scripts/02_silver_transformation.sh:47`
- `scripts/02_silver_transformation.sh:126`
- `scripts/02_silver_transformation.sh:148`
- `scripts/03_gold_transformation.sh:44`
- `scripts/03_gold_transformation.sh:110`
- `scripts/infra-down.sh:172`
- `scripts/run_tests.sh:114`

Notes:

- some older `SC1091` noise should be reduced now that `shellcheck -x` is used
- rerun required to confirm the remaining true script issues

### `dev-tools`

Previously observed non-noise findings before runner cleanup:

- `yamllint` failures in Terraform skill workflow files under:
  - `.agents/skills/terraform/.github/workflows/`
  - `.codex/skills/terraform/.github/workflows/`
- `shellcheck` `SC1091` in:
  - `scripts/ensure-symlinks.sh:19`
  - `scripts/setup-repo.sh:19`

Notes:

- `trufflehog` passed in `dev-tools` when run from the user's terminal
- `semgrep`, `grype`, and `trivy` also passed there in the user-run environment
- rerun required after the updated canonical runner to confirm remaining failures

## User-Observed TruffleHog Result

### `public-compliance-data-analysis`

Running `trufflehog filesystem . --no-update` on the full tree produced heavy noise from:

- `.git/objects/*`
- `.venv/*`

One real issue was found and already fixed:

- `.idea/workspace.xml:132`
  - AWS access key in IDE run configuration

Conclusion:

- full-tree `trufflehog filesystem .` is too noisy here
- the scoped runner should be used instead

## Next Session Checklist

1. Re-run the updated local repo script:

```bash
cd /mnt/hgfs/shared/data-science/tcc/code/public-compliance-data-analysis
./scripts/security_check_this_repo.sh
```

2. Re-run the updated `dev-tools` script:

```bash
cd /mnt/hgfs/shared/dev-tools
./scripts/security-check-toolkit.sh
```

3. If Docker works in the next Codex session, trust the runner output rather than full-tree ad hoc `trufflehog`.

4. Fix remaining real findings in this order:

- local repo MD5 findings
- local repo `tfsec` S3 hardening findings
- local repo shell-script findings
- `dev-tools` remaining YAML and shell findings after rerun

## Constraints

- Do not commit `dev-tools` account-specific material.
- Keep unrelated `dev-tools` changes out of any commit.
- No commits were made in this session.

## Resume Update - 2026-04-05 Later Session

Completed in the follow-up session:

- replaced Bronze-layer MD5-based content comparison with SHA-256 object metadata in:
  - `src/ingestion/ingestion_utils.py`
  - `src/ingestion/ibge_client.py`
  - `src/ingestion/transparency_client.py`
- removed the unused MD5 helper from `src/processing/base_transformer.py`
- updated `tests/ingestion/test_ibge_client.py` to validate SHA-256 metadata behavior
- fixed remaining local shell lint findings in:
  - `scripts/02_silver_transformation.sh`
  - `scripts/03_gold_transformation.sh`
  - `scripts/infra-down.sh`
  - `scripts/run_tests.sh`
- hardened `infra/main.tf` with:
  - customer-managed KMS encryption
  - public access blocks
  - versioning
  - lifecycle rules
  - dedicated S3 access-log bucket and logging policy
  - documented scanner skips for non-applicable replication / event-notification checks
- updated `infra/variables.tf` with access-log controls
- refreshed the local security runner in `scripts/security_check_this_repo.sh` to:
  - exclude generated Terraform state / plan artifacts from scoped secret scanning
  - skip `pip-audit` and Semgrep cleanly when DNS/network is unavailable
  - run Bandit against `src/` so test `assert` usage does not dominate results
  - exclude the dedicated access-log bucket logging false positive in `tfsec`

Verification completed:

- `bandit -r src/ingestion src/processing -f screen` passed with no findings
- `shellcheck -x scripts/02_silver_transformation.sh scripts/03_gold_transformation.sh scripts/infra-down.sh scripts/run_tests.sh` passed
- `checkov -d infra` passed with only explicit documented skips
- `tfsec --exclude aws-s3-enable-bucket-logging infra` passed
- `terraform -chdir=infra fmt` ran successfully
- `terraform -chdir=infra providers lock -platform=linux_amd64` refreshed the lockfile for this environment

Environment-limited items still observed:

- `pip-audit` requires network access to `pypi.org`
- Semgrep auto config requires network access to `semgrep.dev`
- `trufflehog` remains skipped here because Docker is not usable in the sandbox
- `checkov` still prints Prisma guideline download warnings when DNS is blocked, but the scan result passes
- `terraform -chdir=infra validate` was attempted after provider refresh, but the local tool session did not return a final success message before handoff
