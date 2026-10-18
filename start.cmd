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
  "%LOCALAPPDATA%\Python\pythoncore-3.14-64\python.exe" -B main.py %*
) else (
  where py >nul 2>nul
  if not errorlevel 1 (
    py -0p 2>nul | findstr /C:"3.14" >nul
    if not errorlevel 1 (
      py -3.14 -B main.py %*
    ) else (
      py -3 -B main.py %*
    )
  ) else (
    where python >nul 2>nul
    if not errorlevel 1 (
      python -B main.py %*
    ) else (
      ".tools\python\python.exe" -B main.py %*
    )
  )
)
set "compute_exit=%errorlevel%"
exit /b %compute_exit%
