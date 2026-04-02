@echo off
echo.
echo  ╔══════════════════════════════════╗
echo  ║     GESTIONE TURNI - Avvio       ║
echo  ╚══════════════════════════════════╝
echo.
echo  Avvio server su http://localhost:8000
echo  Premi CTRL+C per fermare.
echo.
cd /d "%~dp0"
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
pause
