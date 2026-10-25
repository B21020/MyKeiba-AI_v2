# Register two one-time Windows scheduled tasks for the 2026-10-31 / 11-01 JV-Link latency re-check.
#
# Each task starts once at 09:30 on that day and runs
#   tools\jvlink_live_latency.py run --date <yyyymmdd> --out data\odds\live_latency\<yyyymmdd>
# which fetches that day's race list once (netkeiba public page), polls JV-Link 0B41 every ~20 s
# from 15 min before to 2 min after each post time (all races of the day), and exits after the last race.
# Then it writes the summary with the report subcommand.
# No repetition, no retries, runs only as the current user while logged on. Wakes the PC from sleep.
# The PowerShell window is hidden so that closing windows does not stop the measurement.
#
# Show only:  powershell -ExecutionPolicy Bypass -File tools\register_live_latency_tasks.ps1 -DryRun
# Register:   powershell -ExecutionPolicy Bypass -File tools\register_live_latency_tasks.ps1
# Remove:     powershell -ExecutionPolicy Bypass -File tools\register_live_latency_tasks.ps1 -Remove
# Only one day:  ... -Only 2026-10-31
param([switch]$Remove, [switch]$DryRun, [string]$Only = '')

$repo = Split-Path -Parent $PSScriptRoot
$python = Join-Path $repo '.venv\Scripts\python.exe'
$days = @(
    @{ Name = 'MyKeiba_JvLatency_20261031'; Date = '2026-10-31'; Start = '2026-10-31T09:30:00' },
    @{ Name = 'MyKeiba_JvLatency_20261101'; Date = '2026-11-01'; Start = '2026-11-01T09:30:00' }
)

foreach ($d in $days) {
    if ($Only -and $d.Date -ne $Only) { continue }
    if ($Remove) {
        Unregister-ScheduledTask -TaskName $d.Name -Confirm:$false -ErrorAction SilentlyContinue
        Write-Output "removed $($d.Name)"
        continue
    }
    $ymd = $d.Date -replace '-', ''
    $out = "data\odds\live_latency\$ymd"
    $log = Join-Path $repo "$out\_run.log"
    $cmd = "New-Item -ItemType Directory -Force '$out' | Out-Null; " +
           "& '$python' tools\jvlink_live_latency.py run --date $ymd --out $out *>> '$log'; " +
           "& '$python' tools\jvlink_live_latency.py report --out $out *>> '$log'"
    $arg = "-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -Command `"$cmd`""
    if ($DryRun) {
        Write-Output "[dry run] $($d.Name) once at $($d.Start) (wake to run, time limit 9 h)"
        Write-Output "  powershell.exe $arg"
        Write-Output "  working directory: $repo"
        continue
    }
    $action = New-ScheduledTaskAction -Execute 'powershell.exe' -Argument $arg -WorkingDirectory $repo
    $trigger = New-ScheduledTaskTrigger -Once -At ([datetime]$d.Start)
    $settings = New-ScheduledTaskSettingsSet -WakeToRun -StartWhenAvailable `
        -ExecutionTimeLimit (New-TimeSpan -Hours 9) -DontStopIfGoingOnBatteries -AllowStartIfOnBatteries
    Register-ScheduledTask -TaskName $d.Name -Action $action -Trigger $trigger -Settings $settings `
        -Description 'MyKeiba-AI: JV-Link 0B41 delivery latency re-check (decision time T-4), one day only' -Force | Out-Null
    Write-Output "registered $($d.Name) at $($d.Start) -> log $log"
}
