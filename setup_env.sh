#!/bin/bash
# Video Artifact Processing Engine - Environment Setup Script

# Clear any conflicting VIRTUAL_ENV
unset VIRTUAL_ENV

# Add uv to PATH if it's not already there
if ! command -v uv &> /dev/null; then
    export PATH="/home/nam-bui/.local/bin:$PATH"
fi

# Change to project directory
cd /home/nam-bui/Dev/VideoArtifactProcessingEngine

echo "🚀 Video Artifact Processing Engine environment ready!"
echo "Available commands:"
echo "  uv run python src/main.py                    - Run the application"
echo "  uv run python test_environment.py            - Test environment setup"
echo "  uv sync                                       - Install/update dependencies"
echo "  code .                                        - Open in VS Code"
echo ""
echo "For debugging in VS Code:"
echo "  1. Open VS Code: code ."
echo "  2. Set breakpoints in your code"
echo "  3. Press F5 to start debugging"
