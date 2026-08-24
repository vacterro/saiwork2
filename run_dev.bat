@echo off
cd /d "%~dp0"

echo Starting SAIWORK2 in dev mode (Tauri Dev)...
echo Source code changes will be loaded on the fly.
echo Make sure to close any old compiled executables!
echo.

npm run tauri dev

pause
