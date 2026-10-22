# scripts/setup.ps1 - one-shot environment bootstrap (idempotent).
#   1. python virtualenvs (if missing) + pinned pip deps
#   2. docker images built
#   3. containers up (compose)
#   4. topics created
#   5. connectors registered
#   6. ksql DDL applied
# Safe to re-run: every step skips when already done.
$repo = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
Set-Location $repo
$ErrorActionPreference = "Continue"

# ---------------------------------------------------------------- 1 venvs --
foreach ($name in @(".venv-gen", ".venv-streams")) {
    if (-not (Test-Path (Join-Path $repo "$name\Scripts\python.exe"))) {
        Write-Host "== creating $name"
        python -m venv (Join-Path $repo $name)
    }
}
if (-not (Test-Path ".venv-gen\Scripts\\.done")) {
    .venv-gen\Scripts\python -m pip install -q --upgrade pip
    .venv-gen\Scripts\python -m pip install -q psycopg2-binary numpy python-dotenv
    .venv-streams\Scripts\python -m pip install -q --upgrade pip
    .venv-streams\Scripts\python -m pip install -q `
        "quixstreams==3.14.0" "confluent-kafka[avro,json,schemaregistry]==2.9.0" "fastavro==1.13.1"
    New-Item -ItemType File -Force -Path ".venv-gen\Scripts\.done" | Out-Null
} else { Write-Host "venv deps: SKIP (marker present)" }

# ---------------------------------------------------------------- 2 images --
$imgs = docker images --format "{{.Repository}}:{{.Tag}}" 2>$null
if ($imgs -notcontains "jobmatching/kafka-connect:2.7.3-jdbc10.7.1") {
    Write-Host "== building kafka-connect image"
    docker compose build kafka-connect stream-processor | Out-Null
} else { Write-Host "images: SKIP (built)" }

# ------------------------------------------------------------ 3 containers --
docker compose up -d | Out-Null
Write-Host "containers: docker compose up -d (waiting for health...)"
$deadline = (Get-Date).AddMinutes(6)
do {
    Start-Sleep 10
    $bad = docker compose ps 2>$null | Select-String -Pattern "unhealthy|restarting" -Quiet
} while ($bad -and (Get-Date) -lt $deadline)
docker compose ps

# ---------------------------------------------------------------- 4 topics --
powershell -ExecutionPolicy Bypass -File (Join-Path $repo "scripts\create_topics.ps1")

# ------------------------------------------------------------ 5 connectors --
powershell -ExecutionPolicy Bypass -File (Join-Path $repo "connectors\register_connectors.ps1")

# ------------------------------------------------------------------ 6 ksql --
powershell -ExecutionPolicy Bypass -File (Join-Path $repo "scripts\apply_ksql.ps1")

Write-Host ""
Write-Host "SETUP COMPLETE. Next: scripts\run.ps1 (start match engine + producers)" `
  -ForegroundColor Green
