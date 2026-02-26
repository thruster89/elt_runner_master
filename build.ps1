#Requires -Version 5.1
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

function Step($n,$m){ Write-Host "[$n/5] $m" -ForegroundColor Cyan }
function OK($m)     { Write-Host "      $m"  -ForegroundColor Green }
function Err($m)    { Write-Host "[ERROR] $m" -ForegroundColor Red; Read-Host; exit 1 }

$AppVer = (Get-Content "$PSScriptRoot\VERSION" -ErrorAction SilentlyContinue).Trim()
if (-not $AppVer) { $AppVer = "0.0" }

Write-Host ""
Write-Host " =====================================================" -ForegroundColor Cyan
Write-Host "   ELT Runner v$AppVer  --  PowerShell EXE Build"      -ForegroundColor Cyan
Write-Host " =====================================================" -ForegroundColor Cyan
Write-Host ""

if (-not (Get-Command python -EA SilentlyContinue)) { Err "Python not found." }
Write-Host "[INFO] $((python --version 2>&1).ToString().Trim())`n" -ForegroundColor Green

Step 1 "Virtual environment"
if (-not (Test-Path ".venv")) { python -m venv .venv; OK "Created .venv" }
else { OK ".venv exists, skipping." }
& .\.venv\Scripts\Activate.ps1

Step 2 "Dependencies"
pip install -r requirements.txt --quiet
pip install pyinstaller --quiet
OK "Done."

Step 3 "Clean"
"dist","build" | Where-Object { Test-Path $_ } | ForEach-Object { Remove-Item $_ -Recurse -Force }
OK "Done."

Step 4 "PyInstaller"
pyinstaller elt_runner.spec --noconfirm
if ($LASTEXITCODE -ne 0) { Err "Build failed." }

Step 5 "Verify"
if (Test-Path "dist\ELT_Runner.exe") {
    $mb = [math]::Round((Get-Item "dist\ELT_Runner.exe").Length/1MB,1)
    Write-Host ""
    Write-Host " =====================================================" -ForegroundColor Green
    Write-Host "  BUILD SUCCESS!  dist\ELT_Runner.exe  ($mb MB)"       -ForegroundColor Green
    Write-Host " =====================================================" -ForegroundColor Green
} else { Err "EXE not found." }
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
