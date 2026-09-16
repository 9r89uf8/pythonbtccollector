# Read-only, bounded reproduction of the original event-query timezone error.
param([Parameter(Mandatory=$true)][string]$OutputPath)
$ErrorActionPreference = 'Stop'
if (Test-Path -LiteralPath $OutputPath) { throw 'Refusing to replace correction evidence' }
$start = [DateTimeOffset]::Parse('2026-09-15T22:56:00Z')
$end = [DateTimeOffset]::Parse('2026-09-16T00:00:00Z')
$providers = @('Microsoft-Windows-Kernel-Power', 'Microsoft-Windows-Power-Troubleshooter', 'Microsoft-Windows-Kernel-General')
$eventIds = @(1,12,13,42,107,506,507)

function Read-BoundedEvents([string]$Method) {
    try {
        if ($Method -eq 'explicit_utc_xpath') {
            $predicate = "*[System[(Provider[@Name='Microsoft-Windows-Kernel-Power'] or Provider[@Name='Microsoft-Windows-Power-Troubleshooter'] or Provider[@Name='Microsoft-Windows-Kernel-General']) and (EventID=1 or EventID=12 or EventID=13 or EventID=42 or EventID=107 or EventID=506 or EventID=507) and TimeCreated[@SystemTime >= '2026-09-15T22:56:00.000Z' and @SystemTime <= '2026-09-16T00:00:00.000Z']]]"
            $rows = @(Get-WinEvent -LogName System -FilterXPath $predicate -MaxEvents 100 -ErrorAction Stop)
        } else {
            $startBound = if ($Method -eq 'original_utc_kind') { $start.UtcDateTime } else { $start.LocalDateTime }
            $endBound = if ($Method -eq 'original_utc_kind') { $end.UtcDateTime } else { $end.LocalDateTime }
            $rows = @(Get-WinEvent -FilterHashtable @{LogName='System'; ProviderName=$providers; Id=$eventIds; StartTime=$startBound; EndTime=$endBound} -MaxEvents 100 -ErrorAction Stop)
        }
    } catch {
        if ($_.FullyQualifiedErrorId -notlike 'NoMatchingEventsFound*') { throw }
        $rows = @()
    }
    $events = @($rows | ForEach-Object {
        $xml = [xml]$_.ToXml()
        $data = [ordered]@{}
        foreach ($field in @($xml.Event.EventData.Data)) {
            if ($null -ne $field -and $field.Name -in @('SleepTime','WakeTime','TargetState','EffectiveState','Reason','NewTime','OldTime','WakeSourceType','WakeSourceText','SleepDuration','WakeDuration')) {
                $data[$field.Name] = $field.InnerText
            }
        }
        [ordered]@{record_id=$_.RecordId; provider=$_.ProviderName; event_id=$_.Id;
            system_time_utc=[string]$xml.Event.System.TimeCreated.SystemTime; data=$data}
    })
    return [ordered]@{method=$Method; count=$events.Count; capped=($events.Count -eq 100); events=$events}
}

$original = Read-BoundedEvents 'original_utc_kind'
$local = Read-BoundedEvents 'corrected_local_kind'
$xpath = Read-BoundedEvents 'explicit_utc_xpath'
$localIds = @($local.events.record_id | Sort-Object)
$xpathIds = @($xpath.events.record_id | Sort-Object)
if (($localIds -join ',') -ne ($xpathIds -join ',')) { throw 'Local-time and explicit UTC XPath queries disagree' }
foreach ($event in $xpath.events) {
    $stamp = [DateTimeOffset]::Parse($event.system_time_utc)
    if ($stamp -lt $start -or $stamp -gt $end) { throw 'XPath returned an event outside the UTC interval' }
}
$result = [ordered]@{
    version='windows-sleep-correction-v1'; captured_utc=[DateTime]::UtcNow.ToString('o'); read_only=$true;
    start_utc=$start.ToString('o'); end_utc=$end.ToString('o'); timezone=(Get-TimeZone).Id;
    local_start=$start.LocalDateTime.ToString('o'); local_end=$end.LocalDateTime.ToString('o');
    original_query=$original; corrected_query=$local; independent_utc_query=$xpath;
    corrected_and_xpath_record_ids_match=$true;
    original_check_path='results/spot_twap_response/2026-09-15-reliability-canary/WINDOWS_SYSTEM_EVENT_CHECK.json';
    original_check_preserved=$true;
    user_report='The owner stated that they closed the laptop around the middle of the canary.';
    code_sha256=(Get-FileHash -LiteralPath $PSCommandPath -Algorithm SHA256).Hash.ToLowerInvariant();
    limitations=@('Evidence concerns the local laptop, not production feed availability.', 'An event-query correction cannot recreate missing browser receipts or identify the remote cause of separate source-feed silences.')
}
$parent = Split-Path -Parent $OutputPath
if (-not (Test-Path -LiteralPath $parent)) { New-Item -ItemType Directory -Path $parent -Force | Out-Null }
$json = $result | ConvertTo-Json -Depth 12
[System.IO.File]::WriteAllText([System.IO.Path]::GetFullPath($OutputPath), $json + "`n", [System.Text.UTF8Encoding]::new($false))
$json
