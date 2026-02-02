#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

echo "🚀 Initializing Multi-Platform Development Environment..."

# 1. Create Virtual Environment if it doesn't exist
if [ ! -d ".venv" ]; then
    echo "📦 Creating virtual environment..."
    python -m venv .venv
else
    echo "✅ Virtual environment already exists."
fi

# 2. Universal Virtual Environment Detection
# Windows uses .venv/Scripts/, while macOS/Linux use .venv/bin/
if [ -f ".venv/Scripts/python.exe" ]; then
    echo "🔌 Using Windows-style environment..."
    PYTHON=".venv/Scripts/python.exe"
elif [ -f ".venv/bin/python" ]; then
    echo "🔌 Using Unix-style environment (macOS/Linux)..."
    PYTHON=".venv/bin/python"
else
    echo "❌ Error: Could not find virtual environment Python. Environment might be corrupt."
    exit 1
fi

# 3. Upgrade Pip and Clear Cache
echo "🔄 Upgrading pip and clearing cache..."
$PYTHON -m pip install --upgrade pip
$PYTHON -m pip cache purge

# 4. Install Dependencies using Binary Wheels
if [ -f "requirements.txt" ]; then
    echo "📚 Installing dependencies (forcing binary wheels for compatibility)..."
    $PYTHON -m pip install --only-binary :all: -r requirements.txt
else
    echo "⚠️ requirements.txt not found. Skipping installation."
fi

echo "✨ Environment is ready! To use it manually, run:"
echo "   $PYTHON -m pip list"
