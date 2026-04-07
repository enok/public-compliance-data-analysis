#Requires -Version 5.1
param()

$ErrorActionPreference = 'Stop'

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path -LiteralPath (Join-Path $scriptDir '..')).Path
$agentsPath = Join-Path $repoRoot '.agents'

if (-not (Test-Path -LiteralPath $agentsPath)) {
    throw "Missing .agents link at $agentsPath"
}

$agentsItem = Get-Item -LiteralPath $agentsPath -Force
$rawTarget = $agentsItem.Target
if ($rawTarget -is [System.Array] -and $rawTarget.Length -gt 0) {
    $rawTarget = $rawTarget[0]
}

if ([string]::IsNullOrWhiteSpace([string]$rawTarget)) {
    throw ".agents is not a directory link; cannot resolve toolkit root."
}

$agentsTarget = [string]$rawTarget
if (-not [System.IO.Path]::IsPathRooted($agentsTarget)) {
    $agentsTarget = Join-Path $repoRoot $agentsTarget
}
$agentsTarget = (Resolve-Path -LiteralPath $agentsTarget).Path
$toolkitRoot = Split-Path -Parent $agentsTarget
$syncScript = Join-Path $toolkitRoot 'scripts\sync-tool-configs.ps1'

if (-not (Test-Path -LiteralPath $syncScript)) {
    throw "Toolkit sync script not found at $syncScript"
}

& $syncScript $repoRoot
