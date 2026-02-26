#Requires -Version 5.1
<#
.SYNOPSIS  ELT Runner - CLI/GUI runner (version read from VERSION file)
.EXAMPLE   .\run.ps1 -GUI
.EXAMPLE   .\run.ps1 -Job job_oracle.yml -Param clsYymm=202406
.EXAMPLE   .\run.ps1 -Job job_oracle.yml -Param clsYymm=202401:202406 -Mode plan
.EXAMPLE   .\run.ps1 -Job job_duckdb.yml -Stage export,load_local -Param clsYymm=202406
.EXAMPLE   .\run.ps1 -Job job_oracle.yml -Set @("export.parallel_workers=4","export.overwrite=true")
#>
param(
    [string]   $Job   = "",
    [string]   $Mode  = "run",
    [string[]] $Param = @(),
    [string[]] $Stage = @(),
    [string[]] $Set   = @(),
    [string]   $Env   = "config/env.yml",
    [switch]   $GUI,
    [switch]   $Debug
)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

function Get-Py {
    if (Test-Path ".venv\Scripts\python.exe") { return ".venv\Scripts\python.exe" }
    $p = Get-Command python -EA SilentlyContinue
    if ($p) { return $p.Source }
    throw "Python not found. Run build.bat or create .venv."
}

if ($GUI) {
    if (Test-Path "ELT_Runner.exe") { Start-Process "ELT_Runner.exe" }
    else {
        $py = (Get-Py) -replace "python\.exe$","pythonw.exe"
        Start-Process $py -ArgumentList "batch_runner_gui.py"
    }
    exit 0
}

$py = Get-Py
$a  = @("runner.py")
if ($Job)   { $a += "--job",  $Job  }
if ($Mode)  { $a += "--mode", $Mode }
if ($Env)   { $a += "--env",  $Env  }
foreach ($p in $Param) { $a += "--param", $p }
foreach ($s in $Stage) { $a += "--stage", $s }
foreach ($x in $Set)   { $a += "--set",   $x }
if ($Debug) { $a += "--debug" }

Write-Host ">>> $py $($a -join ' ')" -ForegroundColor Cyan
& $py @a
if ($LASTEXITCODE -ne 0) { Write-Host "[FAILED] Exit $LASTEXITCODE" -ForegroundColor Red; exit $LASTEXITCODE }
Write-Host "[DONE]" -ForegroundColor Green
