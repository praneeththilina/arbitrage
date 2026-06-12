@echo off
:: Stop the Arbitrage Engine running in background
:: Kills any pythonw.exe process running main.py

echo Stopping Arbitrage Engine...

:: Find and kill pythonw processes running main.py
for /f "tokens=2 delims=," %%a in ('wmic process where "name='pythonw.exe' and commandline like '%%main.py%%'" get processid /format:csv 2^>nul ^| findstr /r "[0-9]"') do (
    echo Killing process %%a...
    taskkill /f /pid %%a 2>nul
)

:: Also try with python.exe
for /f "tokens=2 delims=," %%a in ('wmic process where "name='python.exe' and commandline like '%%main.py%%'" get processid /format:csv 2^>nul ^| findstr /r "[0-9]"') do (
    echo Killing process %%a...
    taskkill /f /pid %%a 2>nul
)

echo Engine stopped.
pause
