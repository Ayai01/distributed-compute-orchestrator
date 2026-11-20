@echo off
setlocal DisableDelayedExpansion
cd /d "%~dp0"
set "PYTHONUTF8=1"
set "PYTHONDONTWRITEBYTECODE=1"
set "DCO_ENTRY=main.py"
if exist ".runtime\active\main.py" set "DCO_ENTRY=.runtime\active\main.py"
rem DCO owns its visible cmd color mode; do not inherit Codex/CI NO_COLOR.
set "NO_COLOR="
set "DCO_NO_COLOR="
set "TERM=windows"
set "DCO_PYTHON=%LOCALAPPDATA%\Python\pythoncore-3.14-64\python.exe"
set "DCO_PYTHON_ARGS="
if exist "%DCO_PYTHON%" goto launch
where py >nul 2>nul
if errorlevel 1 goto path_python
set "DCO_PYTHON=py"
set "DCO_PYTHON_ARGS=-3"
py -0p 2>nul | findstr /C:"3.14" >nul
if not errorlevel 1 set "DCO_PYTHON_ARGS=-3.14"
goto launch

:path_python
set "DCO_PYTHON=python"
where python >nul 2>nul
if errorlevel 1 set "DCO_PYTHON=.tools\python\python.exe"

:launch
rem Parse the entire block before Python starts. A release can replace this file while it runs.
(
  "%DCO_PYTHON%" %DCO_PYTHON_ARGS% -B "%DCO_ENTRY%" --on-port-conflict restart %*
  call set "DCO_EXIT=%%errorlevel%%"
  setlocal EnableDelayedExpansion
  if not "!DCO_EXIT!"=="0" (
    echo DCO exited with error !DCO_EXIT!. Diagnostic: .local\diagnostics\startup-error.log
    if not defined DCO_NO_ERROR_PAUSE pause
  )
  exit /b !DCO_EXIT!
)
