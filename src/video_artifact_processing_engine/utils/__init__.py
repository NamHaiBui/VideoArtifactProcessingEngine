"""
Utility modules for video processing package.
"""

import os
import re
from pathlib import Path
from typing import List, Tuple, Union

from .logging_config import setup_custom_logger

# Core utility functions needed by the video processor
def ensure_directory(directory: Union[str, Path]) -> Path:
    """
    Ensure directory exists, create if it doesn't.
    
    Args:
        directory: Directory path to ensure exists
        
    Returns:
        Path: Path object of the directory
    """
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    return directory

def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to human-readable string.
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        str: Formatted duration string
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        remaining_seconds = seconds % 60
        return f"{minutes}m {remaining_seconds:.1f}s"
    else:
        hours = int(seconds // 3600)
        remaining_minutes = int((seconds % 3600) // 60)
        remaining_seconds = seconds % 60
        return f"{hours}h {remaining_minutes}m {remaining_seconds:.1f}s"

def get_file_size_mb(file_path: Union[str, Path]) -> float:
    """
    Get file size in megabytes.
    
    Args:
        file_path: Path to the file
        
    Returns:
        float: File size in MB
    """
    file_path = Path(file_path)
    if not file_path.exists():
        return 0.0
    
    size_bytes = file_path.stat().st_size
    size_mb = size_bytes / (1024 * 1024)
    return round(size_mb, 2)

def sanitize_filename(filename: str) -> str:
    """
    Sanitize filename to be safe for filesystem.
    
    Args:
        filename: Original filename
        
    Returns:
        str: Sanitized filename
    """
    # Remove or replace invalid characters
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    # Remove leading/trailing dots and spaces
    filename = filename.strip('. ')
    # Ensure it's not empty
    if not filename:
        filename = 'untitled'
    return filename
def create_slug(text: str) -> str:
    """
    Create a URL-friendly slug from a given text string.
    """
    text = text.strip().lower()
    text = re.sub(r'\s+', '-', text)
    text = re.sub(r'[^a-z0-9-]', '', text)
    return text
    
def validate_video_file(file_path: Union[str, Path]) -> bool:
    """
    Validate if the file is a supported video format.
    
    Args:
        file_path: Path to the video file
        
    Returns:
        bool: True if valid video file, False otherwise
    """
    video_extensions = {'.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.webm', '.m4v'}
    
    file_path = Path(file_path)
    
    if not file_path.exists():
        return False
    
    if file_path.suffix.lower() not in video_extensions:
        return False
    
    return True

def parse_resolution(resolution_str: str) -> Tuple[int, int]:
    """
    Parse resolution string into width and height.
    
    Args:
        resolution_str: Resolution string like "1920x1080"
        
    Returns:
        Tuple[int, int]: Width and height
    """
    try:
        parts = resolution_str.lower().split('x')
        if len(parts) == 2:
            width = int(parts[0].strip())
            height = int(parts[1].strip())
            return (width, height)
    except (ValueError, AttributeError):
        pass
    
    # Default resolution if parsing fails
    return (1920, 1080)

def get_video_files(directory: Union[str, Path]) -> List[Path]:
    """
    Get all video files in a directory recursively.
    
    Args:
        directory: Directory to search for videos
        
    Returns:
        List[Path]: List of video file paths
    """
    video_extensions = {'.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.webm', '.m4v'}
    
    directory = Path(directory)
    if not directory.exists():
        return []
    
    video_files = []
    for file_path in directory.rglob('*'):
        if file_path.is_file() and file_path.suffix.lower() in video_extensions:
            video_files.append(file_path)
    
    return sorted(video_files)
import re
from typing import Dict, Optional

def parse_s3_url(s3_url: str) -> Optional[Dict[str, str]]:
    """
    Dissects a virtual-hosted or path-style S3 URL to extract the
    bucket, region, path, and filename.

    Args:
        s3_url: The S3 URL to parse.

    Returns:
        A dictionary with 'bucket', 'region', 'path', and 'filename' keys,
        or None if the URL format is invalid.
    """
    # Regex for virtual-hosted-style URLs (e.g., https://bucket-name.s3.region-code.amazonaws.com/path/to/file.mp4)
    # It captures the bucket, region, the path (key prefix), and the filename.
    virtual_hosted_match = re.match(
        r"https?://([^.]+)\.s3\.([^.]+)\.amazonaws\.com/((?:[^/]+/)*)([^/]+)", s3_url
    )
    if virtual_hosted_match:
        return {
            "bucket": virtual_hosted_match.group(1),
            "region": virtual_hosted_match.group(2),
            "path": virtual_hosted_match.group(3),
            "filename": virtual_hosted_match.group(4),
        }

    # Regex for path-style URLs (e.g., https://s3.region-code.amazonaws.com/bucket-name/path/to/file.mp4)
    path_style_match = re.match(
        r"https?://s3\.([^.]+)\.amazonaws\.com/([^/]+)/((?:[^/]+/)*)([^/]+)", s3_url
    )
    if path_style_match:
        return {
            "bucket": path_style_match.group(2),
            "region": path_style_match.group(1),
            "path": path_style_match.group(3),
            "filename": path_style_match.group(4),
        }

    return None

__all__ = [
    "setup_custom_logger",
    "ensure_directory",
    "format_duration",
    "get_file_size_mb",
    "sanitize_filename",
    "validate_video_file",
    "parse_resolution",
    "get_video_files",
    "create_slug",
    "parse_s3_url",
]
