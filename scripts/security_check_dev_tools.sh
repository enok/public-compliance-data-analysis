#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_TARGET="${SCRIPT_DIR}/../../dev-tools"
TARGET_DIR="${1:-${DEFAULT_TARGET}}"

if [ ! -d "${TARGET_DIR}" ]; then
  echo "Target directory not found: ${TARGET_DIR}" >&2
  echo "Usage: $0 /absolute/or/relative/path/to/dev-tools" >&2
  exit 1
fi

TARGET_DIR="$(cd "${TARGET_DIR}" && pwd)"

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

run_check_allow_timeout() {
  local check_name="$1"
  shift

  TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
  echo
  echo "==> ${check_name}"

  if "$@"; then
    PASSED_CHECKS=$((PASSED_CHECKS + 1))
    echo "PASS: ${check_name}"
    return 0
  else
    local exit_code=$?

    if [ "${exit_code}" -eq 124 ] || [ "${exit_code}" -eq 137 ] || [ "${exit_code}" -eq 143 ]; then
      SKIPPED_CHECKS=$((SKIPPED_CHECKS + 1))
      echo "SKIP: ${check_name} timed out"
      return 0
    fi

    FAILED_CHECKS=$((FAILED_CHECKS + 1))
    echo "FAIL: ${check_name}"
    return 0
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

docker_usable() {
  docker ps >/dev/null 2>&1
}

run_with_timeout() {
  local seconds="$1"
  shift

  if have_command timeout; then
    timeout --kill-after=15s "${seconds}" "$@"
  else
    "$@"
  fi
}

path_should_be_scanned() {
  local path="$1"

  if [ ! -e "${TARGET_DIR}/${path}" ]; then
    return 1
  fi

  if [ -f "${TARGET_DIR}/${path}" ]; then
    return 0
  fi

  if [ ! -d "${TARGET_DIR}/.git" ] || ! have_command git; then
    return 0
  fi

  if git -c safe.directory="${TARGET_DIR}" -C "${TARGET_DIR}" ls-files -- "${path}" | head -n 1 | grep -q .; then
    return 0
  fi

  if git -c safe.directory="${TARGET_DIR}" -C "${TARGET_DIR}" ls-files --others --exclude-standard -- "${path}" | head -n 1 | grep -q .; then
    return 0
  fi

  return 1
}

repo_scope_paths=(
  rules
  workflows
  scripts
  .github
  .codex
  .agents
  .cursor
  .windsurf
  integrations
  docs
  AGENTS.md
  CLAUDE.md
  INTENTS.md
  README.md
)

gitleaks_sources() {
  local path
  for path in "${repo_scope_paths[@]}"; do
    if path_should_be_scanned "${path}"; then
      printf '%s\0' "${path}"
    fi
  done
}

scoped_existing_paths() {
  local path
  for path in "${repo_scope_paths[@]}"; do
    if path_should_be_scanned "${path}"; then
      printf '%s\0' "${path}"
    fi
  done
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
  local files=()
  local path

  while IFS= read -r -d '' path; do
    if [ -d "${TARGET_DIR}/${path}" ]; then
      while IFS= read -r -d '' file; do
        files+=("${file}")
      done < <(
        find "${TARGET_DIR}/${path}" -type f \
          \( -name '*.py' -o -name '*.sh' -o -name '*.ps1' -o -name '*.md' -o -name '*.yml' -o -name '*.yaml' -o -name '*.json' -o -name '*.tf' -o -name '*.hcl' -o -name '*.sql' -o -name '*.toml' -o -name '*.cfg' -o -name '*.ini' -o -name '*.js' -o -name '*.ts' -o -name '*.tsx' -o -name '*.jsx' -o -name '*.go' -o -name '*.java' -o -name '*.rb' -o -name '*.php' -o -name '*.xml' -o -name '*.cmd' -o -name '*.bat' \) \
          ! -path '*/.git/*' \
          ! -path '*/.venv/*' \
          ! -path '*/.cache/*' \
          ! -path '*/.pytest_cache/*' \
          ! -path '*/.pytest-tmp/*' \
          ! -path '*/.idea/*' \
          -print0
      )
    elif [ -f "${TARGET_DIR}/${path}" ]; then
      files+=("${TARGET_DIR}/${path}")
    fi
  done < <(scoped_existing_paths)

  if [ "${#files[@]}" -eq 0 ]; then
    return 0
  fi

  if have_command timeout; then
    timeout --kill-after=15s "${SEMGREP_TIMEOUT_SECONDS:-120}" semgrep scan --config auto "${files[@]}"
  else
    semgrep scan --config auto "${files[@]}"
  fi
}

run_semgrep_with_settings() {
  HOME=/tmp XDG_CONFIG_HOME=/tmp XDG_CACHE_HOME=/tmp SEMGREP_SETTINGS_FILE=/tmp/semgrep-settings.yml run_semgrep
}

has_node_manifest() {
  [ -f "${TARGET_DIR}/package.json" ] || [ -f "${TARGET_DIR}/pnpm-lock.yaml" ] || [ -f "${TARGET_DIR}/package-lock.json" ] || [ -f "${TARGET_DIR}/yarn.lock" ]
}

has_python_manifest() {
  [ -f "${TARGET_DIR}/requirements.txt" ] || [ -f "${TARGET_DIR}/pyproject.toml" ] || [ -f "${TARGET_DIR}/setup.py" ]
}

run_shellcheck() {
  local roots=()
  local candidate

  for candidate in scripts .github .agents .codex .cursor .windsurf integrations docs rules workflows; do
    if [ -e "${TARGET_DIR}/${candidate}" ]; then
      roots+=("${TARGET_DIR}/${candidate}")
    fi
  done

  mapfile -d '' files < <(find "${roots[@]}" -type f -name '*.sh' -print0 2>/dev/null)

  if [ "${#files[@]}" -eq 0 ]; then
    return 0
  fi

  shellcheck -x -S warning "${files[@]}"
}

run_psscriptanalyzer() {
  local roots=()
  local candidate

  for candidate in scripts .github .agents .codex .cursor .windsurf integrations docs rules workflows; do
    if [ -e "${TARGET_DIR}/${candidate}" ]; then
      roots+=("${TARGET_DIR}/${candidate}")
    fi
  done

  mapfile -d '' files < <(find "${roots[@]}" -type f -name '*.ps1' -print0 2>/dev/null)

  if [ "${#files[@]}" -eq 0 ]; then
    return 0
  fi

  local file
  for file in "${files[@]}"; do
    pwsh -NoLogo -NoProfile -Command "Invoke-ScriptAnalyzer -Path '$file'"
  done
}

run_hadolint() {
  mapfile -d '' files < <(find "${TARGET_DIR}" -type f \( -name 'Dockerfile' -o -name '*.Dockerfile' \) -print0)

  if [ "${#files[@]}" -eq 0 ]; then
    return 0
  fi

  hadolint "${files[@]}"
}

run_cmd_semgrep() {
  local roots=()
  local candidate

  for candidate in scripts .github .agents .codex .cursor .windsurf integrations docs rules workflows; do
    if [ -e "${TARGET_DIR}/${candidate}" ]; then
      roots+=("${TARGET_DIR}/${candidate}")
    fi
  done

  mapfile -d '' files < <(find "${roots[@]}" -type f \( -name '*.cmd' -o -name '*.bat' \) -print0 2>/dev/null)

  if [ "${#files[@]}" -eq 0 ]; then
    return 0
  fi

  if have_command timeout; then
    timeout --kill-after=15s "${SEMGREP_TIMEOUT_SECONDS:-120}" semgrep scan --config auto "${files[@]}"
  else
    semgrep scan --config auto "${files[@]}"
  fi
}

run_cmd_semgrep_with_settings() {
  HOME=/tmp XDG_CONFIG_HOME=/tmp XDG_CACHE_HOME=/tmp SEMGREP_SETTINGS_FILE=/tmp/semgrep-settings.yml run_cmd_semgrep
}

run_yamllint() {
  local targets=()
  local candidate

  for candidate in .github docs integrations rules workflows; do
    if [ -e "${TARGET_DIR}/${candidate}" ]; then
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
  "./.agents/**"
  "./.claude/**"
  "./.codex/**"
  "./.cursor/**"
  "./.windsurf/**"
  "./.setup/**"
)

run_syft() {
  local args=(dir:. -o json)
  local excluded_path

  for excluded_path in "${scanner_excludes[@]}"; do
    args+=(--exclude "${excluded_path}")
  done

  run_with_timeout "${SYFT_TIMEOUT_SECONDS:-180}" env SYFT_CHECK_FOR_APP_UPDATE=false syft "${args[@]}" >/dev/null
}

run_grype() {
  local args=(dir:. -q)
  local excluded_path

  for excluded_path in "${scanner_excludes[@]}"; do
    args+=(--exclude "${excluded_path}")
  done

  run_with_timeout "${GRYPE_TIMEOUT_SECONDS:-180}" env GRYPE_CHECK_FOR_APP_UPDATE=false grype "${args[@]}"
}

run_trivy() {
  local args=(fs . --offline-scan --timeout 2m --scanners vuln --quiet)
  local excluded_path

  for excluded_path in "${scanner_excludes[@]}"; do
    args+=(--skip-dirs "${excluded_path}")
  done

  run_with_timeout "${TRIVY_TIMEOUT_SECONDS:-180}" trivy "${args[@]}"
}

cd "${TARGET_DIR}"

echo "Target: ${TARGET_DIR}"

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

if have_command osv-scanner && has_node_manifest; then
  run_check "Dependency scan with osv-scanner" osv-scanner scan source -r .
else
  skip_check "Dependency scan with osv-scanner" "no Node manifest found or osv-scanner is not installed"
fi

if have_command pip-audit && has_python_manifest; then
  run_check "Python dependency audit with pip-audit" pip-audit
else
  skip_check "Python dependency audit with pip-audit" "no supported Python dependency manifest or tool missing"
fi

if have_command semgrep; then
  run_check_allow_timeout "Semgrep auto scan" run_semgrep_with_settings
else
  skip_check "Semgrep auto scan" "semgrep is not installed"
fi

if have_command yamllint; then
  run_check "YAML lint" run_yamllint
else
  skip_check "YAML lint" "yamllint is not installed"
fi

if have_command shellcheck; then
  run_check "Shell script lint with shellcheck" run_shellcheck
else
  skip_check "Shell script lint with shellcheck" "no shell scripts found or shellcheck is not installed"
fi

if have_command pwsh; then
  run_check "PowerShell lint with PSScriptAnalyzer" run_psscriptanalyzer
else
  skip_check "PowerShell lint with PSScriptAnalyzer" "no PowerShell scripts found or pwsh is not installed"
fi

if have_command semgrep; then
  run_check_allow_timeout "Batch script scan with semgrep" run_cmd_semgrep_with_settings
else
  skip_check "Batch script scan with semgrep" "no .cmd/.bat files found or semgrep is not installed"
fi

if have_command actionlint && [ -d "${TARGET_DIR}/.github/workflows" ]; then
  run_check "GitHub Actions lint with actionlint" actionlint
else
  skip_check "GitHub Actions lint with actionlint" ".github/workflows missing or actionlint is not installed"
fi

if have_command hadolint && find "${TARGET_DIR}" -type f \( -name 'Dockerfile' -o -name '*.Dockerfile' \) -print -quit | grep -q .; then
  run_check "Dockerfile lint with hadolint" run_hadolint
else
  skip_check "Dockerfile lint with hadolint" "no Dockerfiles found or hadolint is not installed"
fi

if have_command syft; then
  run_check_allow_timeout "SBOM generation with syft" run_syft
else
  skip_check "SBOM generation with syft" "syft is not installed"
fi

if have_command grype; then
  run_check_allow_timeout "Vulnerability match with grype" run_grype
else
  skip_check "Vulnerability match with grype" "grype is not installed"
fi

if have_command trivy; then
  run_check_allow_timeout "Filesystem scan with trivy" run_trivy
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
