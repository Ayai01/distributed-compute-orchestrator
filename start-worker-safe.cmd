@echo off
rem Compatibility entry for the earlier diagnostic launcher; keep the real terminal.
cd /d "%~dp0"
call "%~dp0start.cmd" --mode worker --config worker.local.json %*
exit /b %errorlevel%
