import os
import shutil
import dotenv
from pathlib import Path
from typing import Dict, List, Any

dotenv.load_dotenv()

class Config:
    """
    Configuration management class for video processing package.
    Handles all application settings, AWS configurations, and mappings.
    """
    
    def __init__(self):
        """Initialize configuration with environment variables and defaults."""
        # SQS Configuration
        self.queue_url = os.getenv('SQS_QUEUE_URL', 'https://sqs.us-east-1.amazonaws.com/221082194281/test-video-quote-engine-queue')
        self.sqs_wait_time_seconds = int(os.environ.get("SQS_WAIT_TIME_SECONDS", "20"))
        self.sqs_visibility_timeout_seconds = int(os.environ.get("SQS_VISIBILITY_TIMEOUT_SECONDS", "300"))
        self.sqs_dlq_url = os.environ.get("SQS_DLQ_URL") 
        self.general_aws_region = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
        # DynamoDB Configuration
        self.podcast_metadata_table = os.environ.get("PODCAST_METADATA_TABLE", "Episodes-staging")
        self.quotes_table = os.environ.get("QUOTES_TABLE", "Quotes-staging")
        self.chunk_table = os.environ.get("CHUNK_TABLE", "Chunks-staging")
        self.dynamodb_region = os.environ.get("DYNAMODB_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-2"))
        
        # DynamoDB Credentials (separate from other AWS services)
        self.dynamodb_access_key_id = os.environ.get("DYNAMODB_ACCESS_KEY_ID")
        self.dynamodb_secret_access_key = os.environ.get("DYNAMODB_SECRET_ACCESS_KEY")
        
        # S3 Bucket Configuration
        self.video_bucket = os.environ.get("VIDEO_BUCKET", "pd-video-storage-test")
        self.summary_transcript_bucket = os.environ.get("SUMMARY_TRANSCRIPT_BUCKET", "pd-summary-transcript-storage")
        self.video_quote_bucket = os.environ.get("VIDEO_QUOTES_CHUNK_BUCKET", "pd-video-quotes-storage")
        self.video_chunk_bucket = os.environ.get("CHUNK_VIDEO_BUCKET", "pd-video-chunks-storage")
        self.video_summary_bucket = os.environ.get("VIDEO_SUMMARY_BUCKET", "pd-video-summary-storage")
        
        # Additional S3 Configuration for compatibility
        self.s3_bucket_name = os.environ.get("S3_BUCKET_NAME", self.video_bucket)
        self.s3_input_prefix = os.environ.get("S3_INPUT_PREFIX", "input/")
        self.s3_output_prefix = os.environ.get("S3_OUTPUT_PREFIX", "output/")
        
        # AWS General Configuration (fallback to DynamoDB credentials)
        self.aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID", self.dynamodb_access_key_id)
        self.aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY", self.dynamodb_secret_access_key)
        self.aws_region = os.environ.get("AWS_DEFAULT_REGION", self.dynamodb_region)
        
        # Application Settings
        self.log_level = os.environ.get("LOG_LEVEL", "INFO")
        self.max_concurrent_processing = int(os.environ.get("MAX_CONCURRENT_PROCESSING", "3"))
        self.temp_dir = os.environ.get("TEMP_DIR", "/tmp/video_processing")
        
        # Additional SQS settings
        self.sqs_max_messages = int(os.environ.get("SQS_MAX_MESSAGES", "10"))
        
        # Processing settings
        self.default_chunk_duration = int(os.environ.get("DEFAULT_CHUNK_DURATION", "30"))
        
        # Directory settings
        self.input_dir = Path(os.environ.get("INPUT_DIR", str(Path(self.temp_dir) / "input")))
        self.output_dir = Path(os.environ.get("OUTPUT_DIR", str(Path(self.temp_dir) / "output")))
        
        # Ensure temp directory exists
        Path(self.temp_dir).mkdir(parents=True, exist_ok=True)
        
        # Ensure input and output directories exist
        self.input_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    
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
    
    # DynamoDB Configuration Methods
    def get_dynamodb_config_info(self) -> Dict[str, Any]:
        """Get information about DynamoDB configuration."""
        config_info = {
            'region': self.dynamodb_region,
            'using_custom_credentials': bool(self.dynamodb_access_key_id and self.dynamodb_secret_access_key),
            'credentials': {
                'access_key_id': self.dynamodb_access_key_id,
                'secret_access_key': self.dynamodb_secret_access_key,
            },
            'tables': {
                'podcast_metadata': self.podcast_metadata_table,
                'quotes': self.quotes_table,
                'chunks': self.chunk_table
            }
        }
        return config_info

    def print_dynamodb_config(self) -> Dict[str, Any]:
        """Print current DynamoDB configuration."""
        config = self.get_dynamodb_config_info()
        print("DynamoDB Configuration:")
        print("=" * 30)
        print(f"Region: {config['region']}")
        
        # Note: using_custom_endpoint is not implemented in this version
        print("Endpoint: Default AWS DynamoDB endpoint")
        print("ℹ️  Using standard AWS DynamoDB service")
        
        if config['using_custom_credentials']:
            access_key = config['credentials']['access_key_id']
            print(f"Custom Credentials: {access_key[:10]}...***" if access_key else "Custom Credentials: Not properly set")
            print("🔑 Using separate DynamoDB credentials (different from other AWS services)")
        else:
            print("Credentials: Using default AWS credential chain")
            print("ℹ️  Using same credentials as other AWS services")
        
        print("\nTable Names:")
        for table_type, table_name in config['tables'].items():
            print(f"  - {table_type.replace('_', ' ').title()}: {table_name}")
        
        return config
    
    def validate_config(self) -> List[str]:
        """Validate configuration and return list of issues."""
        issues = []
        
        # Check FFmpeg availability
        try:
            self.get_ffmpeg_path()
        except FileNotFoundError as e:
            issues.append(f"FFmpeg issue: {e}")
        
        try:
            self.get_ffprobe_path()
        except FileNotFoundError as e:
            issues.append(f"FFprobe issue: {e}")
        
        # Check temp directory
        if not Path(self.temp_dir).exists():
            issues.append(f"Temp directory does not exist: {self.temp_dir}")
        
        # Check DynamoDB credentials consistency
        if bool(self.dynamodb_access_key_id) != bool(self.dynamodb_secret_access_key):
            issues.append("DynamoDB credentials incomplete: both access_key_id and secret_access_key must be provided together")
        
        return issues
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            'sqs': {
                'queue_url': self.queue_url,
                'wait_time_seconds': self.sqs_wait_time_seconds,
                'visibility_timeout_seconds': self.sqs_visibility_timeout_seconds,
                'dlq_url': self.sqs_dlq_url,
                'max_messages': self.sqs_max_messages
            },
            'dynamodb': {
                'region': self.dynamodb_region,
                'tables': {
                    'podcast_metadata': self.podcast_metadata_table,
                    'quotes': self.quotes_table,
                    'chunks': self.chunk_table
                },
                'credentials': {
                    'access_key_id': self.dynamodb_access_key_id,
                    'secret_access_key': self.dynamodb_secret_access_key
                }
            },
            's3_buckets': {
                'video': self.video_bucket,
                'summary_transcript': self.summary_transcript_bucket,
                'video_quote': self.video_quote_bucket,
                'video_chunk': self.video_chunk_bucket,
                'video_summary': self.video_summary_bucket,
                'main_bucket': self.s3_bucket_name,
                'input_prefix': self.s3_input_prefix,
                'output_prefix': self.s3_output_prefix
            },
            'application': {
                'log_level': self.log_level,
                'max_concurrent_processing': self.max_concurrent_processing,
                'temp_dir': self.temp_dir,
                'input_dir': str(self.input_dir),
                'output_dir': str(self.output_dir),
                'default_chunk_duration': self.default_chunk_duration
            },
            'aws': {
                'region': self.aws_region,
                'access_key_id': self.aws_access_key_id,
                'secret_access_key': self.aws_secret_access_key
            }
        }
    
    def create_directories(self) -> None:
        """Ensure all required directories exist."""
        Path(self.temp_dir).mkdir(parents=True, exist_ok=True)
        self.input_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)


# Backward compatibility - create a default instance
# This allows existing code to continue working without changes
config = Config()

# Expose legacy global variables for backward compatibility
QUEUE_URL = config.queue_url
PODCAST_METADATA_TABLE = config.podcast_metadata_table
QUOTES_TABLE = config.quotes_table
CHUNK_TABLE = config.chunk_table
DYNAMODB_REGION = config.dynamodb_region
DYNAMODB_ACCESS_KEY_ID = config.dynamodb_access_key_id
DYNAMODB_SECRET_ACCESS_KEY = config.dynamodb_secret_access_key
VIDEO_BUCKET = config.video_bucket
SUMMARY_TRANSCRIPT_BUCKET = config.summary_transcript_bucket
VIDEO_QUOTE_BUCKET = config.video_quote_bucket
VIDEO_CHUNK_BUCKET = config.video_chunk_bucket
VIDEO_SUMMARY_BUCKET = config.video_summary_bucket
LOG_LEVEL = config.log_level
MAX_CONCURRENT_PROCESSING = config.max_concurrent_processing
TEMP_DIR = config.temp_dir

# Legacy function wrappers for backward compatibility
def get_ffmpeg_path():
    """Legacy wrapper for backward compatibility."""
    return config.get_ffmpeg_path()

def get_ffprobe_path():
    """Legacy wrapper for backward compatibility."""
    return config.get_ffprobe_path()

def get_dynamodb_config_info():
    """Legacy wrapper for backward compatibility."""
    return config.get_dynamodb_config_info()

def print_dynamodb_config():
    """Legacy wrapper for backward compatibility."""
    return config.print_dynamodb_config()