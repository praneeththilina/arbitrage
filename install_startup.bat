@echo off
:: Install Arbitrage Engine in Windows Startup (run on every boot)
:: Run this batch file as Administrator once

set STARTUP_DIR=%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup
set VBS_PATH=%~dp0run_background.vbs

if not exist "%VBS_PATH%" (
    echo ERROR: run_background.vbs not found in %~dp0
    pause
    exit /b 1
)

:: Copy VBS to Startup folder
copy "%VBS_PATH%" "%STARTUP_DIR%\ArbitrageEngine.lnk" >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    echo SUCCESS: Installed to Windows Startup.
    echo The arbitrage engine will start automatically when you log in.
) else (
    :: Try creating a shortcut instead
    echo Creating shortcut in Startup folder...
    powershell -Command "$WS = New-Object -ComObject WScript.Shell; $SC = $WS.CreateShortcut('%STARTUP_DIR%\ArbitrageEngine.lnk'); $SC.TargetPath = '%VBS_PATH%'; $SC.WorkingDirectory = '%~dp0'; $SC.WindowStyle = 7; $SC.Save()"
    if %ERRORLEVEL% EQU 0 (
        echo SUCCESS: Shortcut created in Windows Startup.
    ) else (
        echo FAILED: Could not install to Startup.
        echo To manually install, copy or create a shortcut to run_background.vbs in:
        echo %STARTUP_DIR%
    )
)

echo.
echo To view the dashboard, open http://127.0.0.1:8000 in your browser.
echo To stop the engine, run:  stop_engine.bat
pause
