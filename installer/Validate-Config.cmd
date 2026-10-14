@echo off
REM Double-click helper: validates config.json with the same check the service
REM runs at startup, and keeps the window open so the result can be read.
setlocal
set "APPDIR=%~dp0"
set "CONFIG=%APPDIR%config.json"
if not "%~1"=="" set "CONFIG=%~1"

powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%APPDIR%scripts\validate-config.ps1" -ConfigPath "%CONFIG%" -ExePath "%APPDIR%redis-conflated-pubsub.exe"
set "CODE=%ERRORLEVEL%"
echo.
if "%CODE%"=="0" (echo Result: configuration accepted.) else (echo Result: configuration rejected ^(exit %CODE%^).)
echo.
pause
exit /b %CODE%
