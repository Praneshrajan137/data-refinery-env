$ErrorActionPreference = "Stop"

if (-Not (Test-Path .\.venv)) {
    py -3.12 -m venv .venv
}

. .\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
Write-Host "dataforge-evals setup complete." -ForegroundColor Green
