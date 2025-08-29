param([string]$State)
if (-not $State) { $State = Read-Host "State to research (e.g., Ohio, Michigan, Mississippi)" }
Set-Location "C:\Quarkmine\Agents\Radar"

function Pause-IfNeeded { param([string]$msg="Press Enter to continue") Read-Host $msg | Out-Null }

while ($true) {
  Clear-Host
  Write-Host "=== Radar Workbench ===" -ForegroundColor Cyan
  Write-Host ("State: {0}`n" -f $State)
  Write-Host "1) Sweep entire state (report opens)"
  Write-Host "2) Run a single county (report opens)"
  Write-Host "3) Run a single district (report opens)"
  Write-Host "4) Show last report (default data dir)"
  Write-Host "5) Open intake_signals.json (default data dir)"
  Write-Host "6) Open Scout seeds folder"
  Write-Host "Q) Quit"
  $choice = Read-Host "Select"

  switch ($choice.ToUpper()) {
    '1' {
      $since = Read-Host "Since-days (default 120)"; if (-not $since) { $since = 120 }
      $th    = Read-Host "Threshold (default 70)";  if (-not $th)    { $th = 70 }
      py radar.py --state "$State" --since-days $since --threshold $th --open-report
      Pause-IfNeeded
    }
    '2' {
      $county = Read-Host "County name (e.g., Franklin or Franklin County)"
      $since  = Read-Host "Since-days (default 120)"; if (-not $since) { $since = 120 }
      $th     = Read-Host "Threshold (default 65)";  if (-not $th)    { $th = 65 }
      py radar.py --state "$State" --county "$county" --since-days $since --threshold $th --open-report
      Pause-IfNeeded
    }
    '3' {
      $dist  = Read-Host "District name (exact as known to Scout)"
      $since = Read-Host "Since-days (default 180)"; if (-not $since) { $since = 180 }
      py radar.py --state "$State" --district "$dist" --since-days $since --open-report
      Pause-IfNeeded
    }
    '4' {
      $path = Join-Path ".\data" "radar_report.md"
      if (Test-Path $path) { notepad $path } else { Write-Host "No report yet. Run Radar first." -ForegroundColor Yellow; Pause-IfNeeded }
    }
    '5' {
      $path = Join-Path ".\data" "intake_signals.json"
      if (Test-Path $path) { notepad $path } else { Write-Host "No intake yet. Run Radar and hit threshold." -ForegroundColor Yellow; Pause-IfNeeded }
    }
    '6' {
      $seedDir = Join-Path "..\Scout\data" "seeds"
      if (Test-Path $seedDir) { ii $seedDir } else { Write-Host "Seeds folder not found. Initialize via Scout first." -ForegroundColor Yellow; Pause-IfNeeded }
    }
    'Q' { break }
  }
}
