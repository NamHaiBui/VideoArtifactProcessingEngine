import os
import shutil
import dotenv
from pathlib import Path
from typing import Dict, Any

dotenv.load_dotenv()

class Config:
    """
    Configuration management class for video processing package.
    Handles all application settings, AWS configurations, and mappings.
    """
    
    def __init__(self):
        """Initialize configuration with environment variables and defaults."""
        # Database Configuration
        self.db_host = os.environ.get('DB_HOST', 'localhost')
        self.db_port = int(os.environ.get('DB_PORT', '5432'))
        self.db_name = os.environ.get('DB_NAME', 'videodb')
        self.db_user = os.environ.get('DB_USER', 'postgres')
        self.db_password = os.environ.get('DB_PASSWORD', '')
        self.db_pool_min_size = int(os.environ.get('DB_POOL_MIN_SIZE', '1'))
        self.db_pool_max_size = int(os.environ.get('DB_POOL_MAX_SIZE', '10'))
    
        # Log Level
        self.log_level = os.environ.get('LOG_LEVEL', 'INFO')
        # Auto-size concurrency if not explicitly set: default to ~half of CPU cores (>=2)
        _mcp_env = os.environ.get('MAX_CONCURRENT_PROCESSING')
        if _mcp_env is not None and _mcp_env.strip() != "":
            try:
                self.max_concurrent_processing = max(1, int(_mcp_env))
            except Exception:
                # Fallback if invalid
                cpu = os.cpu_count() or 2
                self.max_concurrent_processing = max(2, cpu // 2)
        else:
            cpu = os.cpu_count() or 2
            self.max_concurrent_processing = max(2, cpu // 2)

        # Upload concurrency: default to 2x processing, clamped 2..16, unless overridden
        _mcu_env = os.environ.get('MAX_CONCURRENT_UPLOADS')
        if _mcu_env is not None and _mcu_env.strip() != "":
            try:
                self.max_concurrent_uploads = max(1, int(_mcu_env))
            except Exception:
                self.max_concurrent_uploads = max(2, min(16, self.max_concurrent_processing * 2))
        else:
            self.max_concurrent_uploads = max(2, min(16, self.max_concurrent_processing * 2))

        self.temp_dir = os.environ.get('TEMP_DIR', '/tmp/video_processing')

        # Runtime environment detection (ECS/Fargate awareness)
        exec_env = os.environ.get('AWS_EXECUTION_ENV', '')
        self.is_fargate = 'FARGATE' in exec_env or bool(os.environ.get('ECS_CONTAINER_METADATA_URI_V4'))

    # FFmpeg tuning for Fargate: allow env override for preset with safe defaults
    # Default preset: veryfast on Fargate (CPU-efficient), medium elsewhere unless overridden
        default_preset = 'veryfast' if self.is_fargate else 'medium'
        self.ffmpeg_preset = os.environ.get('FFMPEG_PRESET', default_preset)

        # SQS Configuration
        self.queue_url = os.getenv('SQS_QUEUE_URL', 'https://sqs.us-east-1.amazonaws.com/221082194281/test-video-quote-engine-queue')
        self.sqs_wait_time_seconds = int(os.environ.get("SQS_WAIT_TIME_SECONDS", "20"))
        self.sqs_visibility_timeout_seconds = int(os.environ.get("SQS_VISIBILITY_TIMEOUT_SECONDS", "14400"))
        self.sqs_dlq_url = os.environ.get("SQS_DLQ_URL") 
        self.general_aws_region = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))

        # S3 Bucket Configuration
        self.video_bucket = os.environ.get("VIDEO_BUCKET",  'spice-episode-artifacts')
        self.summary_transcript_bucket = os.environ.get("SUMMARY_TRANSCRIPT_BUCKET", "spice-episode-artifacts")
        self.video_quote_bucket = os.environ.get("QUOTE_BUCKET", "spice-quote-artifacts")
        self.video_chunk_bucket = os.environ.get("CHUNK_BUCKET", "spice-short-artifacts")
        self.video_summary_bucket = os.environ.get("VIDEO_SUMMARY_BUCKET",  'spice-episode-artifacts')
        self.s3_bucket_name = os.environ.get("S3_BUCKET_NAME", self.video_bucket)
        self.s3_input_prefix = os.environ.get("S3_INPUT_PREFIX", "input/")
        self.s3_output_prefix = os.environ.get("S3_OUTPUT_PREFIX", "output/")

        # AWS General Configuration
        self.aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

        # Validate but don't crash hard in non-production contexts
        try:
            self._validate_db_config()
        except Exception as e:
            # Convert to warning to improve local dev ergonomics; production should set envs properly
            import logging
            logging.getLogger(__name__).warning(f"Database config validation warning: {e}")

    def _validate_db_config(self):
        """Validate required database configuration"""
        required_db_vars = ["DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD"]
        missing_vars = [var for var in required_db_vars if not os.environ.get(var)]
        if missing_vars:
            raise ValueError(f"Missing required database environment variables: {', '.join(missing_vars)}")
        if self.db_port <= 0 or self.db_port > 65535:
            raise ValueError(f"Invalid DB_PORT: {self.db_port}. It must be between 1 and 65535.")
        if self.db_pool_min_size < 0 or self.db_pool_max_size <= 0:
            raise ValueError(f"Invalid database pool size settings. Min: {self.db_pool_min_size}, Max: {self.db_pool_max_size}. They must be positive integers.")
        if self.db_pool_min_size > self.db_pool_max_size:
            raise ValueError(f"DB_POOL_MIN_SIZE cannot be greater than DB_POOL_MAX_SIZE. Min: {self.db_pool_min_size}, Max: {self.db_pool_max_size}.")

    # FFmpeg Configuration Methods
    def get_ffmpeg_path(self) -> str:
        """Get FFmpeg path from environment or system PATH."""
        # Check environment variable first
        ffmpeg_path = os.environ.get("FFMPEG_PATH")
        if ffmpeg_path and Path(ffmpeg_path).exists():
            return ffmpeg_path
        
        # Check system PATH
        ffmpeg_path = shutil.which('ffmpeg')
        if ffmpeg_path:
            return ffmpeg_path
        
        # Check common installation paths
        common_paths = [
            '/usr/bin/ffmpeg',
            '/usr/local/bin/ffmpeg',
            str(Path.home() / '.local' / 'bin' / 'ffmpeg'),
        ]
        
        for path in common_paths:
            if Path(path).exists():
                return path
        
        raise FileNotFoundError("FFmpeg not found. Please install FFmpeg or run scripts/setup_ffmpeg.sh")

    def get_ffprobe_path(self) -> str:
        """Get FFprobe path from environment or system PATH."""
        # Check environment variable first
        ffprobe_path = os.environ.get("FFPROBE_PATH")
        if ffprobe_path and Path(ffprobe_path).exists():
            return ffprobe_path
        
        # Check system PATH
        ffprobe_path = shutil.which('ffprobe')
        if ffprobe_path:
            return ffprobe_path
        
        # Check common installation paths
        common_paths = [
            '/usr/bin/ffprobe',
            '/usr/local/bin/ffprobe',
            str(Path.home() / '.local' / 'bin' / 'ffprobe'),
        ]
        
        for path in common_paths:
            if Path(path).exists():
                return path
        
        raise FileNotFoundError("FFprobe not found. Please install FFmpeg or run scripts/setup_ffmpeg.sh")
    
    def get_database_config(self) -> Dict[str, Any]:
        """Get PostgreSQL database configuration."""
        return {
            'host': self.db_host,
            'port': self.db_port,
            'dbname': self.db_name,
            'user': self.db_user,
            'password': self.db_password
        }


# Create a global config instance for use throughout the app
config = Config()

# Function to get database configuration
def get_db_config() -> Dict[str, Any]:
    """Get the database configuration."""
    return config.get_database_config()