@echo off
REM FactSet Fundamentals Benchmarking - Windows Setup Script

echo ==========================================
echo FactSet Fundamentals Benchmarking Setup
echo ==========================================

REM Create virtual environment
echo Creating Python virtual environment...
python -m venv venv

REM Activate virtual environment
echo Activating virtual environment...
call venv\Scripts\activate.bat

REM Upgrade pip, setuptools, and wheel first
echo Upgrading pip, setuptools, and wheel...
python -m pip install --upgrade pip setuptools wheel

REM Install dependencies
echo Installing project dependencies...
pip install -r requirements.txt

REM Create necessary directories
echo Creating output directories...
if not exist output mkdir output
if not exist logs mkdir logs
if not exist certs mkdir certs

REM Copy environment file if it doesn't exist
if not exist .env (
    echo Creating .env file from template...
    copy .env.example .env
    echo.
    echo IMPORTANT: Please edit .env file with your FactSet credentials
)

echo.
echo ==========================================
echo Setup Complete!
echo ==========================================
echo.
echo Next steps:
echo 1. Edit .env file with your FactSet credentials
echo 2. Optional: Add SSL certificate to certs\ directory
echo 3. Run: venv\Scripts\activate
echo 4. Run: python analyze_fundamentals_final.py
echo.