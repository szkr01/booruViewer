@echo off
setlocal
cd /d %~dp0

set CYCLE=0

:loop
set /a CYCLE+=1
set "STOP_AFTER_BUILD=0"
echo [loop %CYCLE%] collector cycle start

echo [1/2] sync_posts start
uv run --no-sync python -m app.sync_posts
set "SYNC_RC=%ERRORLEVEL%"
if "%SYNC_RC%"=="130" (
  echo [INFO] sync_posts interrupted, running build_index before exit
  set "STOP_AFTER_BUILD=1"
) else if errorlevel 1 (
  echo [ERROR] sync_posts failed
  exit /b 1
)
echo [1/2] sync_posts done

echo [2/2] build_index start
uv run --no-sync python -m app.build_index --init-from-parquet --parquet-glob "data/incoming/*.parquet" --delete-synced
if errorlevel 1 (
  echo [ERROR] build_index failed
  exit /b 1
)
echo [2/2] build_index done
if "%STOP_AFTER_BUILD%"=="1" (
  echo [INFO] collector stopped after build_index
  exit /b 0
)
echo [loop %CYCLE%] collector cycle done
timeout /t 5 /nobreak >nul
goto loop

endlocal
