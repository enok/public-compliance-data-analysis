#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

echo "🚀 Initializing Multi-Platform Development Environment..."

# 1. Create Virtual Environment if it doesn't exist
if [ ! -d ".venv" ]; then
    echo "📦 Creating virtual environment..."
    python -m venv .venv
else
    echo "✅ Virtual environment already exists."
fi

# 2. Universal Activation Logic
# Windows uses .venv/Scripts/, while macOS/Linux use .venv/bin/
if [ -f ".venv/Scripts/activate" ]; then
    echo "🔌 Activating Windows-style environment..."
    source .venv/Scripts/activate
elif [ -f ".venv/bin/activate" ]; then
    echo "🔌 Activating Unix-style environment (macOS/Linux)..."
    source .venv/bin/activate
else
    echo "❌ Error: Could not find activation script. Environment might be corrupt."
    exit 1
fi

# 3. Upgrade Pip and Clear Cache
echo "🔄 Upgrading pip and clearing cache..."
python -m pip install --upgrade pip
pip cache purge

# 4. Install Dependencies using Binary Wheels
if [ -f "requirements.txt" ]; then
    echo "📚 Installing dependencies (forcing binary wheels for compatibility)..."
    pip install --only-binary :all: -r requirements.txt
else
    echo "⚠️ requirements.txt not found. Skipping installation."
fi

echo "✨ Environment is ready! To use it manually, run:"
echo "   source .venv/Scripts/activate (Windows)"
echo "   or"
echo "   source .venv/bin/activate (macOS/Linux)"