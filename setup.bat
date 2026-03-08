@echo off
setlocal
cd /d %~dp0

where uv >nul 2>&1
if errorlevel 1 (
  echo [ERROR] uv is not installed or not in PATH.
  echo Install uv first: https://docs.astral.sh/uv/
  exit /b 1
)

echo [1/3] Creating virtual environment with uv...
uv venv
if errorlevel 1 exit /b 1

echo [2/3] Installing project dependencies via uv sync...
uv sync
if errorlevel 1 exit /b 1

echo [3/3] Installing CUDA 12.8 PyTorch wheels...
uv pip uninstall -y torch torchvision torchaudio >nul 2>&1
uv pip install --index-url https://download.pytorch.org/whl/cu128 --force-reinstall torch torchvision torchaudio
if errorlevel 1 exit /b 1

echo [verify] Checking CUDA availability from .venv...
.\.venv\Scripts\python.exe -c "import torch,sys; print('torch', torch.__version__); print('cuda', torch.version.cuda); print('is_available', torch.cuda.is_available()); sys.exit(0 if torch.cuda.is_available() else 1)"
if errorlevel 1 (
  echo [ERROR] CUDA torch is not available in this environment.
  exit /b 1
)

echo Setup complete.
echo Activate: .venv\Scripts\activate
endlocal
