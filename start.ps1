# One-command start for the transit optimizer stack (Windows PowerShell 5.1+).
#   .\start.ps1            build + start, wait for the API, open the dashboard
#   .\start.ps1 -Synthetic rebuild the datasets as a fully synthetic city first
#   .\start.ps1 -Down      stop the stack (add -Wipe to also drop HBase / checkpoints / sim clock)
param(
    [switch]$Synthetic,
    [switch]$Down,
    [switch]$Wipe,
    [int]$TimeoutMin = 15
)
$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot

docker info *> $null
if ($LASTEXITCODE -ne 0) { Write-Error 'Docker is not running. Start Docker Desktop and retry.' }

if ($Down) {
    if ($Wipe) { docker compose down -v } else { docker compose down }
    exit $LASTEXITCODE
}

if (-not (Test-Path .env)) {
    Copy-Item .env.example .env
    Write-Host 'Created .env from .env.example'
}

if ($Synthetic) {
    docker compose run --rm -e FORCE_SYNTHETIC=1 -e FORCE_REBUILD=1 bootstrap
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

docker compose up -d --build
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

# First run downloads the datasets (~4 min) and trains the model (~3 min).
Write-Host "Waiting for the API at http://localhost:8000 (up to $TimeoutMin min)..."
$deadline = (Get-Date).AddMinutes($TimeoutMin)
$ready = $false
while ((Get-Date) -lt $deadline) {
    try {
        Invoke-RestMethod http://localhost:8000/api/system/health -TimeoutSec 5 | Out-Null
        $ready = $true
        break
    } catch {
        Start-Sleep -Seconds 5
    }
}

if (-not $ready) {
    Write-Warning 'API did not answer in time. Check: docker compose logs -f bootstrap spark backend'
    exit 1
}

Write-Host ''
Write-Host 'Dashboard  http://localhost:5173'
Write-Host 'API docs   http://localhost:8000/docs'
Write-Host 'Spark UI   http://localhost:4040'
Write-Host 'HBase UI   http://localhost:16010'
Write-Host 'The dashboard fills in about a minute after Spark finishes training the model.'
Start-Process http://localhost:5173
