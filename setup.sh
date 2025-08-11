#!/bin/bash

# FactSet Fundamentals Benchmarking - Setup Script

echo "=========================================="
echo "FactSet Fundamentals Benchmarking Setup"
echo "=========================================="

# Create virtual environment
echo "Creating Python virtual environment..."
python3 -m venv venv

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Upgrade pip, setuptools, and wheel first
echo "Upgrading pip, setuptools, and wheel..."
pip install --upgrade pip setuptools wheel

# Install dependencies
echo "Installing project dependencies..."
pip install -r requirements.txt

# Create necessary directories
echo "Creating output directories..."
mkdir -p output
mkdir -p logs
mkdir -p certs

# Copy environment file if it doesn't exist
if [ ! -f .env ]; then
    echo "Creating .env file from template..."
    cp .env.example .env
    echo ""
    echo "⚠️  IMPORTANT: Please edit .env file with your FactSet credentials"
fi

echo ""
echo "=========================================="
echo "✅ Setup Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Edit .env file with your FactSet credentials"
echo "2. (Optional) Add SSL certificate to certs/ directory"
echo "3. Run: source venv/bin/activate"
echo "4. Run: python analyze_fundamentals_final.py"
echo ""