# Sync data/ del repo al backup externo (merge, no borra reference/ ni otros extras).
# Uso:
#   .\scripts\sync_data_to_backup.ps1
#   .\scripts\sync_data_to_backup.ps1 -BackupRoot "D:\otro_backup"

param(
    [string]$RepoRoot = (Split-Path $PSScriptRoot -Parent),
    [string]$BackupRoot = "C:\Users\ivanm\Desktop\football_scraping_backup"
)

$src = Join-Path $RepoRoot "data"
$dst = Join-Path $BackupRoot "data"
$log = Join-Path $BackupRoot ("sync_log_{0:yyyyMMdd_HHmmss}.txt" -f (Get-Date))

if (-not (Test-Path $src)) { throw "No existe: $src" }
New-Item -ItemType Directory -Force -Path $dst | Out-Null

"=== Sync $(Get-Date -Format o) ===" | Out-File $log -Encoding utf8
"Source: $src" | Out-File $log -Append
"Dest:   $dst" | Out-File $log -Append

foreach ($item in @("raw", "clean", "exports", "logs", ".cache")) {
    $from = Join-Path $src $item
    $to = Join-Path $dst $item
    if (-not (Test-Path $from)) {
        ">>> Skip missing $item" | Out-File $log -Append
        continue
    }
    Write-Host ">>> Copying $item ..."
    ">>> Copying $item ..." | Out-File $log -Append
    robocopy $from $to /E /COPY:DAT /R:2 /W:3 /MT:8 /NP | Tee-Object -FilePath $log -Append
}

foreach ($f in @("README.md", "stadium_overrides.json")) {
    $from = Join-Path $src $f
    if (Test-Path $from) { Copy-Item $from (Join-Path $dst $f) -Force }
}

"=== Finished $(Get-Date -Format o) ===" | Out-File $log -Append
Write-Host "Done. Log: $log"
