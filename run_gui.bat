@echo off
cd /d "%~dp0"
if exist "ELT_Runner.exe" (
    start "" "ELT_Runner.exe" %*
    exit /b 0
)
if exist ".venv\Scripts\pythonw.exe" (
    start "" ".venv\Scripts\pythonw.exe" batch_runner_gui.py %*
    exit /b 0
)
where pythonw >nul 2>&1 && start "" pythonw batch_runner_gui.py %* || (
    echo [ERROR] Python not found. Run build.bat first.
    pause
)
