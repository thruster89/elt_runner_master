@echo off
setlocal EnableDelayedExpansion
set /p APP_VER=<VERSION
if "!APP_VER!"=="" set APP_VER=0.0
title ELT Runner v!APP_VER! - Build

echo.
echo  =====================================================
echo    ELT Runner v!APP_VER!  --  Windows EXE Build
echo  =====================================================
echo.

where python >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python not found in PATH.
    echo         https://www.python.org/downloads/
    pause & exit /b 1
)
for /f "tokens=2" %%v in ('python --version 2^>^&1') do set PYVER=%%v
echo [INFO] Python %PYVER%

cd /d "%~dp0"

if not exist .venv (
    echo [1/5] Creating .venv ...
    python -m venv .venv
) else (
    echo [1/5] .venv exists, skipping.
)
call .venv\Scripts\activate.bat

echo [2/5] Installing dependencies ...
pip install -r requirements.txt -q
pip install pyinstaller -q

echo [3/5] Cleaning previous build ...
if exist dist  rmdir /s /q dist
if exist build rmdir /s /q build

echo [4/5] Building EXE ...
pyinstaller elt_runner.spec --noconfirm
if errorlevel 1 (
    echo [ERROR] Build failed.
    pause & exit /b 1
)

echo [5/5] Verifying ...
if exist dist\ELT_Runner.exe (
    for %%f in (dist\ELT_Runner.exe) do set SIZE=%%~zf
    set /a MB=!SIZE!/1048576
    echo.
    echo  =====================================================
    echo   BUILD SUCCESS!
    echo   dist\ELT_Runner.exe  (!MB! MB^)
    echo  =====================================================
) else (
    echo [ERROR] EXE not found.
    pause & exit /b 1
)
echo.
pause
