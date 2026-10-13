@echo off
cd /d "%~dp0"
py -3.13 -X utf8 run_experiment.py run
pause
