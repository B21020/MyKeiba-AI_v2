# Register two one-time Windows scheduled tasks for the 2026-10-24/25 odds freshness probe.
#
# Each task starts once, 15 minutes before that day's first planned capture, runs
#   tools\odds_freshness_probe.py run --date <day>
# which waits for each planned time (T-30/10/5/2), captures once, and exits after the last capture.
# No repetition, no retries, runs only as the current user while logged on. Wakes the PC from sleep.
#
# Register:   powershell -ExecutionPolicy Bypass -File tools\register_odds_probe_tasks.ps1
# Remove:     powershell -ExecutionPolicy Bypass -File tools\register_odds_probe_tasks.ps1 -Remove
# Only one day:  ... -File tools\register_odds_probe_tasks.ps1 -Only 2026-10-25
# The PowerShell window is hidden so that closing windows does not stop the probe.
param([switch]$Remove, [string]$Only = '')

$repo = Split-Path -Parent $PSScriptRoot
$python = Join-Path $repo '.venv\Scripts\python.exe'
$out = 'data\odds\probe_20261024_25'
$days = @(
    @{ Name = 'MyKeiba_OddsProbe_20261024'; Date = '2026-10-24'; Start = '2026-10-24T09:20:00' },
    @{ Name = 'MyKeiba_OddsProbe_20261025'; Date = '2026-10-25'; Start = '2026-10-25T09:15:00' }
)

foreach ($d in $days) {
    if ($Only -and $d.Date -ne $Only) { continue }
    if ($Remove) {
        Unregister-ScheduledTask -TaskName $d.Name -Confirm:$false -ErrorAction SilentlyContinue
        Write-Output "removed $($d.Name)"
        continue
    }
    $log = Join-Path $repo "$out\run_$($d.Date).log"
    $cmd = "& '$python' tools\odds_freshness_probe.py run --out $out --date $($d.Date) *>> '$log'"
    $action = New-ScheduledTaskAction -Execute 'powershell.exe' `
        -Argument "-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -Command `"$cmd`"" -WorkingDirectory $repo
    $trigger = New-ScheduledTaskTrigger -Once -At ([datetime]$d.Start)
    $settings = New-ScheduledTaskSettingsSet -WakeToRun -StartWhenAvailable `
        -ExecutionTimeLimit (New-TimeSpan -Hours 8) -DontStopIfGoingOnBatteries -AllowStartIfOnBatteries
    Register-ScheduledTask -TaskName $d.Name -Action $action -Trigger $trigger -Settings $settings `
        -Description 'MyKeiba-AI plan 4.3: public odds freshness probe (T-30/10/5/2), one day only' -Force | Out-Null
    Write-Output "registered $($d.Name) at $($d.Start) -> log $log"
}
