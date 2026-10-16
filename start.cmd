@echo off
setlocal
cd /d "%~dp0"
if exist ".tools\python\python.exe" (
  ".tools\python\python.exe" -B main.py %*
) else (
  where py >nul 2>nul
  if not errorlevel 1 (
    py -3 -B main.py %*
  ) else (
    python -B main.py %*
  )
)
set "compute_exit=%errorlevel%"
if not "%compute_exit%"=="0" pause
exit /b %compute_exit%
