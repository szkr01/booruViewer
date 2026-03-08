@echo off
setlocal
cd /d %~dp0

if "%~1"=="" goto :usage

set "LEGACY_CACHE=%~1"
set "EXTRA_ARGS=%~2"

if not exist "%LEGACY_CACHE%" (
  echo [ERROR] legacy cache directory not found: %LEGACY_CACHE%
  exit /b 1
)

if not exist "%LEGACY_CACHE%\search_ivfpq.index" (
  echo [ERROR] missing required file: %LEGACY_CACHE%\search_ivfpq.index
  exit /b 1
)
if not exist "%LEGACY_CACHE%\metadata.npy" (
  echo [ERROR] missing required file: %LEGACY_CACHE%\metadata.npy
  exit /b 1
)
if not exist "%LEGACY_CACHE%\id_map.npy" (
  echo [ERROR] missing required file: %LEGACY_CACHE%\id_map.npy
  exit /b 1
)

echo [migrate] legacy cache: %LEGACY_CACHE%
uv run --no-sync python -m app.migrate_legacy --legacy-cache "%LEGACY_CACHE%" %EXTRA_ARGS%
exit /b %ERRORLEVEL%

:usage
echo Usage: migrate_legacy.bat ^<ILEMB_cache_dir^> [--force]
echo Example: migrate_legacy.bat "D:\ILEMB\data\10204497-5784553" --force
exit /b 1
