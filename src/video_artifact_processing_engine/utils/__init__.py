"""Utility functions and helpers."""

from typing import Any, Dict, Optional
import sys
import os


def get_project_root() -> str:
    """Get the project root directory.
    
    Returns:
        Project root directory path
    """
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def hello_world() -> str:
    """Simple hello world function.
    
    Returns:
        Hello world message
    """
    return "Hello, World! This is your Python project."


def get_system_info() -> Dict[str, Any]:
    """Get system information.
    
    Returns:
        Dictionary containing system information
    """
    return {
        "python_version": sys.version,
        "platform": sys.platform,
        "executable": sys.executable,
        "path": sys.path[:3],  # First 3 path entries
    }


def print_welcome_message() -> None:
    """Print a welcome message for the application."""
    print("=" * 50)
    print("🎥 Video Artifact Processing Engine")
    print("=" * 50)
    print(hello_world())
    print()
    
    system_info = get_system_info()
    print(f"Python Version: {system_info['python_version'].split()[0]}")
    print(f"Platform: {system_info['platform']}")
    print("=" * 50)
