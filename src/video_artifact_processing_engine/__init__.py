"""
Video Chunk Processing Package

A Python package for processing video files into chunks with various processing capabilities.
Includes SQS polling and ECS deployment support for scalable cloud processing.
"""

__version__ = "1.0.0"
__author__ = "Nam Bui"
__email__ = "namhbui03@gmail.com"

from .config import Config
from .sqs_handler import SQSPoller, VideoProcessingMessage
from .utils.logging_config import setup_custom_logger as SetupLogging

__all__ = [
    "Config", 
    "SQSPoller", 
    "VideoProcessingMessage",
    "SetupLogging"
]
