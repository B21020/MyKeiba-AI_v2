# Run the current-year update first, then audit/backfill the stored history.
# No scheduler is installed. Resume by rerunning this file after a stopped run.
$ErrorActionPreference = 'Stop'
$workspacePath = Split-Path -Parent $PSScriptRoot
$pythonPath = Join-Path $workspacePath '.venv\Scripts\python.exe'
Set-Location -LiteralPath $workspacePath

Write-Output 'Phase 1: collect missing 2026 results and horse histories'
& $pythonPath -u tools/update_race_results.py --stage all --audit-start-year 2026
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

Write-Output 'Phase 2: audit calendars and repair missing historical data since 2015'
& $pythonPath -u tools/update_race_results.py --stage all --audit-start-year 2015
exit $LASTEXITCODE
