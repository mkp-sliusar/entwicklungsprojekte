@echo off
setlocal

cd /d "%~dp0"

echo ================================
echo LoRaSense clean + build started
echo ================================
echo.

echo [1/4] Closing running EXE if needed...
taskkill /f /im LoRaSense.exe >nul 2>nul

echo [2/4] Waiting a moment...
timeout /t 2 /nobreak >nul

echo [3/4] Cleaning old build artifacts...
if exist build rmdir /s /q build
if exist dist rmdir /s /q dist
if exist LoRaSense.spec del /f /q LoRaSense.spec

echo [4/4] Building EXE...
pyinstaller --noconfirm --windowed --onefile ^
 --name "LoRaSense" ^
 --icon "lorasense_icon.ico" ^
 --add-data "lorasense_icon.ico;." ^
 --add-data "lorasense_icon.png;." ^
 --hidden-import pyqtgraph ^
 --hidden-import pyqtgraph.opengl ^
 --collect-submodules pyqtgraph ^
 --collect-data pyqtgraph ^
 main.py

echo.
if exist "dist\LoRaSense.exe" (
    echo ================================
    echo BUILD SUCCESS
    echo dist\LoRaSense.exe
    echo ================================
) else (
    echo ================================
    echo BUILD FAILED
    echo Check messages above.
    echo ================================
)

echo.
pause
endlocal
