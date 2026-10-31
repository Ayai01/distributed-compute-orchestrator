@echo off
setlocal
cd /d "%~dp0"
set "PYTHONUTF8=1"
set "PYTHONDONTWRITEBYTECODE=1"
rem DCO owns its visible cmd color mode; do not inherit Codex/CI NO_COLOR.
set "NO_COLOR="
set "DCO_NO_COLOR="
set "TERM=windows"
if exist "%LOCALAPPDATA%\Python\pythoncore-3.14-64\python.exe" (
  "%LOCALAPPDATA%\Python\pythoncore-3.14-64\python.exe" -B main.py --on-port-conflict restart %*
) else (
  where py >nul 2>nul
  if not errorlevel 1 (
    py -0p 2>nul | findstr /C:"3.14" >nul
    if not errorlevel 1 (
      py -3.14 -B main.py --on-port-conflict restart %*
    ) else (
      py -3 -B main.py --on-port-conflict restart %*
    )
  ) else (
    where python >nul 2>nul
    if not errorlevel 1 (
      python -B main.py --on-port-conflict restart %*
    ) else (
      ".tools\python\python.exe" -B main.py --on-port-conflict restart %*
    )
  )
)
set "compute_exit=%errorlevel%"
if not "%compute_exit%"=="0" (
  echo DCO exited with error %compute_exit%. Diagnostic: .local\diagnostics\startup-error.log
  rem Keep startup failures visible, including double-clicked windows.
  if not defined DCO_NO_ERROR_PAUSE pause
)
exit /b %compute_exit%
