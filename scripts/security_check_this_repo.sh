#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

TOTAL_CHECKS=0
PASSED_CHECKS=0
FAILED_CHECKS=0
SKIPPED_CHECKS=0

run_check() {
  local check_name="$1"
  shift

  TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
  echo
  echo "==> ${check_name}"

  if "$@"; then
    PASSED_CHECKS=$((PASSED_CHECKS + 1))
    echo "PASS: ${check_name}"
  else
    FAILED_CHECKS=$((FAILED_CHECKS + 1))
    echo "FAIL: ${check_name}"
  fi
}

skip_check() {
  local check_name="$1"
  local reason="$2"

  TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
  SKIPPED_CHECKS=$((SKIPPED_CHECKS + 1))
  echo
  echo "==> ${check_name}"
  echo "SKIP: ${reason}"
}

have_command() {
  command -v "$1" >/dev/null 2>&1
}

can_resolve_host() {
  getent ahosts "$1" >/dev/null 2>&1
}

docker_usable() {
  docker ps >/dev/null 2>&1
}

has_files() {
  local pattern="$1"
  find "${REPO_ROOT}" -type f -name "${pattern}" -print -quit | grep -q .
}

repo_scope_paths=(
  src
  scripts
  tests
  .github
  .codex
  .agents
  .windsurf
  docs/llm
  AGENTS.md
  CLAUDE.md
  README.md
)

scoped_file_targets() {
  local path

  for path in "${repo_scope_paths[@]}"; do
    if [ -e "${REPO_ROOT}/${path}" ]; then
      printf '%s\0' "${path}"
    fi
  done

  if [ -d "${REPO_ROOT}/infra" ]; then
    find "${REPO_ROOT}/infra" -maxdepth 1 -type f \
      \( -name '*.tf' -o -name '*.tfvars' -o -name '*.yml' -o -name '*.yaml' -o -name '*.json' \) \
      ! -name '*.tfstate' \
      ! -name '*.tfstate.*' \
      ! -name 'tfplan' \
      -printf '%P\0' | sed -z 's#^#infra/#'
  fi
}

gitleaks_sources() {
  local path
  while IFS= read -r -d '' path; do
    printf '%s\0' "${path}"
  done < <(scoped_file_targets)
}

scoped_existing_paths() {
  local path
  while IFS= read -r -d '' path; do
    printf '%s\0' "${path}"
  done < <(scoped_file_targets)
}

run_gitleaks() {
  local source
  while IFS= read -r -d '' source; do
    gitleaks detect --source "${source}" --no-git --redact
  done < <(gitleaks_sources)
}

run_trufflehog() {
  local targets=()
  local path

  while IFS= read -r -d '' path; do
    targets+=("${path}")
  done < <(scoped_existing_paths)

  trufflehog filesystem "${targets[@]}" --no-update
}

run_semgrep() {
  local targets=()
  local path

  while IFS= read -r -d '' path; do
    targets+=("${path}")
  done < <(scoped_existing_paths)

  semgrep scan --config auto "${targets[@]}"
}

run_semgrep_with_settings() {
  HOME=/tmp XDG_CONFIG_HOME=/tmp XDG_CACHE_HOME=/tmp SEMGREP_SETTINGS_FILE=/tmp/semgrep-settings.yml run_semgrep
}

run_pip_audit() {
  XDG_CACHE_HOME=/tmp PIP_AUDIT_CACHE_DIR=/tmp/pip-audit pip-audit
}

run_bandit() {
  if [ ! -e "${REPO_ROOT}/src" ]; then
    return 0
  fi

  (
    cd "${REPO_ROOT}"
    bandit -r src -f screen
  )
}

run_shellcheck() {
  local roots=()
  if [ -e "${REPO_ROOT}/scripts" ]; then
    roots+=("${REPO_ROOT}/scripts")
  fi

  mapfile -d '' files < <(find "${roots[@]}" -type f -name '*.sh' -print0 2>/dev/null)

  if [ "${#files[@]}" -eq 0 ]; then
    return 0
  fi

  shellcheck -x "${files[@]}"
}

run_psscriptanalyzer() {
  local roots=()
  if [ -e "${REPO_ROOT}/scripts" ]; then
    roots+=("${REPO_ROOT}/scripts")
  fi

  mapfile -d '' files < <(find "${roots[@]}" -type f -name '*.ps1' -print0 2>/dev/null)

  if [ "${#files[@]}" -eq 0 ]; then
    return 0
  fi

  local file
  for file in "${files[@]}"; do
    pwsh -NoLogo -NoProfile -Command "Invoke-ScriptAnalyzer -Path '$file'"
  done
}

run_cfn_lint() {
  mapfile -d '' files < <(find "${REPO_ROOT}/infra" -type f \( -name '*.yaml' -o -name '*.yml' -o -name '*.json' \) -print0 2>/dev/null)

  if [ "${#files[@]}" -eq 0 ]; then
    return 0
  fi

  cfn-lint "${files[@]}"
}

run_yamllint() {
  local targets=()
  local candidate

  for candidate in .github docs/llm infra; do
    if [ -e "${REPO_ROOT}/${candidate}" ]; then
      targets+=("${candidate}")
    fi
  done

  if [ "${#targets[@]}" -eq 0 ]; then
    return 0
  fi

  yamllint "${targets[@]}"
}

scanner_excludes=(
  "./.git/**"
  "./.venv/**"
  "./.cache/**"
  "./.pytest_cache/**"
  "./.pytest-tmp/**"
  "./.idea/**"
  "./logs/**"
  "./notebooks/**"
)

run_syft() {
  local args=(dir:. -o table)
  local excluded_path

  for excluded_path in "${scanner_excludes[@]}"; do
    args+=(--exclude "${excluded_path}")
  done

  env SYFT_CHECK_FOR_APP_UPDATE=false syft "${args[@]}"
}

run_grype() {
  local args=(dir:.)
  local excluded_path

  for excluded_path in "${scanner_excludes[@]}"; do
    args+=(--exclude "${excluded_path}")
  done

  env GRYPE_CHECK_FOR_APP_UPDATE=false grype "${args[@]}"
}

run_trivy() {
  local args=(fs . --offline-scan --timeout 2m)
  local excluded_path

  for excluded_path in "${scanner_excludes[@]}"; do
    args+=(--skip-dirs "${excluded_path}")
  done

  trivy "${args[@]}"
}

cd "${REPO_ROOT}"

echo "Repository: ${REPO_ROOT}"

if have_command gitleaks; then
  run_check "Secrets scan with gitleaks" run_gitleaks
else
  skip_check "Secrets scan with gitleaks" "gitleaks is not installed"
fi

if have_command trufflehog && have_command docker && docker_usable; then
  run_check "Secrets scan with trufflehog" run_trufflehog
elif have_command trufflehog; then
  skip_check "Secrets scan with trufflehog" "docker is not usable in the current environment"
else
  skip_check "Secrets scan with trufflehog" "trufflehog is not installed"
fi

if have_command pip-audit && { [ -f "${REPO_ROOT}/requirements.txt" ] || [ -f "${REPO_ROOT}/pyproject.toml" ] || [ -f "${REPO_ROOT}/setup.py" ]; } && can_resolve_host pypi.org; then
  run_check "Python dependency audit with pip-audit" run_pip_audit
elif have_command pip-audit && { [ -f "${REPO_ROOT}/requirements.txt" ] || [ -f "${REPO_ROOT}/pyproject.toml" ] || [ -f "${REPO_ROOT}/setup.py" ]; }; then
  skip_check "Python dependency audit with pip-audit" "network access to pypi.org is not available in the current environment"
else
  skip_check "Python dependency audit with pip-audit" "no supported Python dependency manifest or tool missing"
fi

if have_command bandit; then
  run_check "Python static security scan with bandit" run_bandit
else
  skip_check "Python static security scan with bandit" "bandit is not installed"
fi

if have_command semgrep && can_resolve_host semgrep.dev; then
  run_check "Semgrep auto scan" run_semgrep_with_settings
elif have_command semgrep; then
  skip_check "Semgrep auto scan" "network access to semgrep.dev is not available in the current environment"
else
  skip_check "Semgrep auto scan" "semgrep is not installed"
fi

if have_command checkov && [ -d "${REPO_ROOT}/infra" ]; then
  run_check "Infrastructure scan with checkov" checkov -d infra
else
  skip_check "Infrastructure scan with checkov" "infra directory missing or checkov is not installed"
fi

if have_command tfsec && [ -d "${REPO_ROOT}/infra" ]; then
  run_check "Terraform scan with tfsec" tfsec --exclude aws-s3-enable-bucket-logging infra
else
  skip_check "Terraform scan with tfsec" "infra directory missing or tfsec is not installed"
fi

if have_command cfn-lint && [ -d "${REPO_ROOT}/infra" ]; then
  run_check "CloudFormation scan with cfn-lint" run_cfn_lint
else
  skip_check "CloudFormation scan with cfn-lint" "infra directory missing or cfn-lint is not installed"
fi

if have_command yamllint && { [ -d "${REPO_ROOT}/.github/workflows" ] || [ -d "${REPO_ROOT}/docs/llm" ]; }; then
  run_check "YAML lint" run_yamllint
else
  skip_check "YAML lint" "no YAML-heavy directories found or yamllint is not installed"
fi

if have_command shellcheck && has_files '*.sh'; then
  run_check "Shell script lint with shellcheck" run_shellcheck
else
  skip_check "Shell script lint with shellcheck" "no shell scripts found or shellcheck is not installed"
fi

if have_command pwsh && has_files '*.ps1'; then
  run_check "PowerShell lint with PSScriptAnalyzer" run_psscriptanalyzer
else
  skip_check "PowerShell lint with PSScriptAnalyzer" "no PowerShell scripts found or pwsh is not installed"
fi

if have_command actionlint && [ -d "${REPO_ROOT}/.github/workflows" ]; then
  run_check "GitHub Actions lint with actionlint" actionlint
else
  skip_check "GitHub Actions lint with actionlint" ".github/workflows missing or actionlint is not installed"
fi

if have_command syft; then
  run_check "SBOM generation with syft" run_syft
else
  skip_check "SBOM generation with syft" "syft is not installed"
fi

if have_command grype; then
  run_check "Vulnerability match with grype" run_grype
else
  skip_check "Vulnerability match with grype" "grype is not installed"
fi

if have_command trivy; then
  run_check "Filesystem scan with trivy" run_trivy
else
  skip_check "Filesystem scan with trivy" "trivy is not installed"
fi

echo
echo "Summary:"
echo "  total:   ${TOTAL_CHECKS}"
echo "  passed:  ${PASSED_CHECKS}"
echo "  failed:  ${FAILED_CHECKS}"
echo "  skipped: ${SKIPPED_CHECKS}"

if [ "${FAILED_CHECKS}" -gt 0 ]; then
  exit 1
fi
