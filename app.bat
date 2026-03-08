@echo off
setlocal
cd /d %~dp0
uv run --no-sync python -m app.main
endlocal
