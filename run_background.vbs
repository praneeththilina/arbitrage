' Run Arbitrage Engine in background (no console window)
' Double-click this file or add to Windows Startup

Dim shell
Set shell = CreateObject("WScript.Shell")

' Run pythonw.exe (no console window) with main.py
' pythonw.exe ships with Python - runs Python without opening a terminal
shell.Run "pythonw.exe main.py", 0, False

Set shell = Nothing
