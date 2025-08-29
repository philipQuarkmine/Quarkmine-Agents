param([string]$State)
if (-not $State) { $State = Read-Host "State to work on (e.g., Ohio, Michigan, Mississippi)" }
Set-Location "C:\Quarkmine\Agents\Scout"

function Pause-IfNeeded { param([string]$msg="Press Enter to continue") Read-Host $msg | Out-Null }
function Normalize-County([string]$name){
  if(-not $name){ return "" }
  $txt = $name.Trim()
  $txt = [regex]::Replace($txt, "\s+county$", "", "IgnoreCase")
  return (Get-Culture).TextInfo.ToTitleCase($txt.ToLower())
}
function Safe-Name([string]$s){ if(-not $s){return ""}; return ($s -replace '[^A-Za-z0-9_-]','_') }

function Get-SeedsCounties([string]$state){
  $path = ".\data\seeds\counties.json"
  if(-not (Test-Path $path)){ return @() }
  try{
    $j = Get-Content $path -Raw | ConvertFrom-Json
    $list = @($j.$state)
    if($list){ return $list | ForEach-Object { Normalize-County $_ } }
  } catch {}
  return @()
}
function Get-MasterDerivedCounties([string]$state){
  $path = ".\data\scout_master.json"
  $out = [System.Collections.Generic.List[string]]::new()
  if(-not (Test-Path $path)){ return @() }
  try{
    $j = Get-Content $path -Raw | ConvertFrom-Json
    $st = $j.states.$state
    if($null -ne $st -and $null -ne $st.districts){
      foreach($d in $st.districts){
        if($d.source_county){ $out.Add((Normalize-County $d.source_county)) }
        if($d.source_counties){
          foreach($c in $d.source_counties){ if($c){ $out.Add((Normalize-County $c)) } }
        }
      }
    }
  } catch {}
  $seen=@{}; return $out | Where-Object { if($seen.ContainsKey($_)){ $false } else { $seen[$_]=$true; $true } }
}

function Write-CountyChecklist([string]$state, [string[]]$counties, [string]$mode){
  if(-not $counties -or $counties.Count -eq 0){
    Write-Host "No counties supplied; nothing to write." -ForegroundColor Yellow
    return
  }
  $stateFile = ".\data\states\{0}.json" -f (Safe-Name $state)
  New-Item -ItemType Directory -Force -Path ".\data\states" | Out-Null

  if((Test-Path $stateFile) -and $mode -match 'merge'){
    try{ $cur = Get-Content $stateFile -Raw | ConvertFrom-Json } catch { $cur = $null }
    if(-not $cur){ $mode = 'overwrite' } else {
      if(-not $cur.counties){ $cur | Add-Member -NotePropertyName counties -NotePropertyValue @{} }
      foreach($c in $counties){
        if(-not $cur.counties.PSObject.Properties.Name -contains $c){
          $cur.counties[$c] = @{ status="pending"; runs=0; last_run=$null; notes="" }
        }
      }
      if(-not $cur.state){ $cur | Add-Member -NotePropertyName state -NotePropertyValue $state -Force } else { $cur.state = $state }
      if(-not $cur.created){ $cur | Add-Member -NotePropertyName created -NotePropertyValue ([DateTime]::Now.ToString("s")) -Force }
      $cur | ConvertTo-Json -Depth 6 | Set-Content -Path $stateFile -Encoding UTF8
      Write-Host ("Merged county checklist → {0}" -f $stateFile) -ForegroundColor Green
      return
    }
  }

  $data = [ordered]@{
    state   = $state
    counties= @{}
    created = [DateTime]::Now.ToString("s")
  }
  foreach($c in $counties){
    $data.counties[$c] = @{ status="pending"; runs=0; last_run=$null; notes="" }
  }
  $data | ConvertTo-Json -Depth 6 | Set-Content -Path $stateFile -Encoding UTF8
  Write-Host ("Wrote new county checklist → {0}" -f $stateFile) -ForegroundColor Green
}

function Run-SeedUS {
  $tool = ".\tools\seed_us_data.py"
  if(-not (Test-Path $tool)){
    Write-Host "Missing tool: $tool`nPlease add C:\Quarkmine\Agents\Scout\tools\seed_us_data.py first." -ForegroundColor Red
    Pause-IfNeeded; return
  }
  Write-Host "This will (re)generate ALL US seeds (states, abbr, counties, site_bias) from Census." -ForegroundColor Cyan
  $ok = Read-Host "Proceed with OVERWRITE of seeds? [y/N]"
  if($ok -notin @('y','Y','yes','YES')){ Write-Host "Cancelled." -ForegroundColor Yellow; Pause-IfNeeded; return }

  $init = Read-Host "Also initialize per-state progress files for ALL states now? [y/N]"
  if($init -in @('y','Y','yes','YES')){
    $mode = Read-Host "Progress write mode: overwrite (o) or merge (m)? [o/m]"
    if($mode -match '^[mM]'){ $pm='merge' } else { $pm='overwrite' }
    py $tool --overwrite --init-progress ALL --progress-mode $pm
  } else {
    py $tool --overwrite
  }
  Pause-IfNeeded
}

while ($true) {
  Clear-Host
  Write-Host "=== Scout Workbench ===" -ForegroundColor Cyan
  Write-Host ("State: {0}`n" -f $State)
  Write-Host "1) Show progress for state"
  Write-Host "2) Run NEXT pending county"
  Write-Host "3) Run a CHOSEN county"
  Write-Host "4) Verify / normalize (no model)"
  Write-Host "5) Open scout_master.json"
  Write-Host "6) Open this state's progress file"
  Write-Host "7) Generate FULL county checklist for this state"
  Write-Host "8) Initialize FULL US seeds from Census (one-time)"
  Write-Host "Q) Quit"
  $choice = Read-Host "Select"

  switch ($choice.ToUpper()) {
    '1' { py scout.py --state "$State" --show-progress; Pause-IfNeeded }
    '2' { $limit = Read-Host "Max districts per county (default 25)"; if (-not $limit) { $limit = 25 }; py scout.py --state "$State" --next --limit $limit; Pause-IfNeeded }
    '3' { $county = Read-Host "County name (e.g., Franklin or Franklin County)"; $limit  = Read-Host "Max districts (default 20)"; if (-not $limit) { $limit = 20 }; py scout.py --state "$State" --county "$county" --limit $limit; Pause-IfNeeded }
    '4' { py scout.py --verify; Pause-IfNeeded }
    '5' { $p = ".\data\scout_master.json"; if (Test-Path $p) { notepad $p } else { Write-Host "No master DB yet. Run Scout first." -ForegroundColor Yellow; Pause-IfNeeded } }
    '6' { $stateFile = Join-Path ".\data\states" ((Safe-Name $State) + ".json"); if (Test-Path $stateFile) { notepad $stateFile } else { Write-Host ("No state progress file yet for '{0}'." -f $State) -ForegroundColor Yellow; Pause-IfNeeded } }
    '7' {
      $seedList = Get-SeedsCounties -state $State
      if($seedList.Count -gt 0){
        Write-Host ("Found seeds for {0}: {1} counties." -f $State, $seedList.Count) -ForegroundColor Green
        $mode = Read-Host "Merge with existing (m) or Overwrite (o)? [m/o]"; if(-not $mode){ $mode = "m" }
        if($mode -match '^[mM]'){ $mode='merge' } else { $mode='overwrite' }
        Write-CountyChecklist -state $State -counties $seedList -mode $mode
        Pause-IfNeeded; continue
      }
      $derived = Get-MasterDerivedCounties -state $State
      if($derived.Count -gt 0){
        Write-Host ("No seeds for {0}; derived {1} counties from master." -f $State, $derived.Count) -ForegroundColor Yellow
        $mode = Read-Host "Merge with existing (m) or Overwrite (o)? [m/o]"; if(-not $mode){ $mode = "m" }
        if($mode -match '^[mM]'){ $mode='merge' } else { $mode='overwrite' }
        Write-CountyChecklist -state $State -counties $derived -mode $mode
        Pause-IfNeeded; continue
      }
      Write-Host "No seeds and nothing to derive. Paste a comma-separated list of counties." -ForegroundColor Yellow
      $raw = Read-Host "Counties (e.g., Franklin, Cuyahoga, Hamilton)"
      $manual = @()
      if($raw){ $manual = $raw -split "," | ForEach-Object { Normalize-County $_ } | Where-Object { $_ } }
      if($manual.Count -gt 0){
        $mode = Read-Host "Merge with existing (m) or Overwrite (o)? [m/o]"; if(-not $mode){ $mode = "m" }
        if($mode -match '^[mM]'){ $mode='merge' } else { $mode='overwrite' }
        Write-CountyChecklist -state $State -counties $manual -mode $mode
      } else {
        Write-Host "No counties entered; canceled." -ForegroundColor Yellow
      }
      Pause-IfNeeded
    }
    '8' { Run-SeedUS }
    'Q' { break }
  }
}
