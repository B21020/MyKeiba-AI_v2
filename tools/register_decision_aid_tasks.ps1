# Register one weekly Windows scheduled task for the decision aid (Saturday and Sunday 09:30).
#
# The task runs
#   tools\decision_aid\live.py run --date <today>
# which fetches that day's race list once (netkeiba public page). On days without JRA races it exits at once.
# On race days it polls JV-Link 0B41 for each race from 15 min before to 2 min after post, records the
# decision-time numbers and the paper-trade picks (place bets, frozen rule), serves the page at
# http://127.0.0.1:8765 (this PC only), and settles the paper trades about 30 minutes after the last race.
# Runs only as the current user while logged on. Wakes the PC from sleep. Hidden window.
# Monday or other non-weekend race days are not covered; start those by hand with the same command.
#
# Show only:  powershell -ExecutionPolicy Bypass -File tools\register_decision_aid_tasks.ps1 -DryRun
# Register:   powershell -ExecutionPolicy Bypass -File tools\register_decision_aid_tasks.ps1
# Remove:     powershell -ExecutionPolicy Bypass -File tools\register_decision_aid_tasks.ps1 -Remove
param([switch]$Remove, [switch]$DryRun)

$repo = Split-Path -Parent $PSScriptRoot
$python = Join-Path $repo '.venv\Scripts\python.exe'
$name = 'MyKeiba_DecisionAid_Weekend'

if ($Remove) {
    Unregister-ScheduledTask -TaskName $name -Confirm:$false -ErrorAction SilentlyContinue
    Write-Output "removed $name"
    return
}
$cmd = "`$d = Get-Date -Format yyyyMMdd; New-Item -ItemType Directory -Force 'data\decision_aid\logs' | Out-Null; " +
       "& '$python' tools\decision_aid\live.py run --date `$d *>> (Join-Path '$repo' ('data\decision_aid\logs\run_' + `$d + '.log'))"
$arg = "-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -Command `"$cmd`""
if ($DryRun) {
    Write-Output "[dry run] $name weekly on Saturday and Sunday at 09:30 (wake to run, time limit 9 h)"
    Write-Output "  powershell.exe $arg"
    Write-Output "  working directory: $repo"
    return
}
$action = New-ScheduledTaskAction -Execute 'powershell.exe' -Argument $arg -WorkingDirectory $repo
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Saturday, Sunday -At ([datetime]'09:30')
$settings = New-ScheduledTaskSettingsSet -WakeToRun -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Hours 9) -DontStopIfGoingOnBatteries -AllowStartIfOnBatteries
Register-ScheduledTask -TaskName $name -Action $action -Trigger $trigger -Settings $settings `
    -Description 'MyKeiba-AI decision aid: decision-time numbers page and place paper trades (frozen rule)' -Force | Out-Null
Write-Output "registered $name (Saturday and Sunday 09:30) -> logs in data\decision_aid\logs"
