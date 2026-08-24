@echo off
chcp 65001 >nul
echo Упаковка чистых исходников (без кеша, сборок и логов)...
cd /d "%~dp0.."

if exist saiwork2_audit.zip del saiwork2_audit.zip

tar.exe -a -c -f saiwork2_audit.zip --exclude=.git --exclude=target --exclude=node_modules --exclude=.freebuff --exclude=.cargo --exclude=dist --exclude=build --exclude=*.zip *

echo.
echo Готово. Архив saiwork2_audit.zip лежит в корне проекта.
pause
