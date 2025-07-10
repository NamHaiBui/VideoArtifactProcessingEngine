
import os
import tempfile
from typing import Dict, List, Optional, Tuple
from video_artifact_processing_engine.config import config
from video_artifact_processing_engine.aws.aws_client import get_dynamodb_resource, get_s3_client
from video_artifact_processing_engine.models.chunk_model import Chunk
from video_artifact_processing_engine.models.quote_model import Quote
from video_artifact_processing_engine.tools.video_quote_cutting_process import process_video_quotes_with_path
from video_artifact_processing_engine.tools.video_short_cutting_process import process_video_chunks_with_path


from botocore.exceptions import ClientError

from video_artifact_processing_engine.utils.logging_config import setup_custom_logger
logging = setup_custom_logger(__name__)
dynamodb = get_dynamodb_resource()
s3_client = get_s3_client()
def process_video_artifacts_unified(PK: str,
                                   SK: str,
                                   podcast_title: str,
                                   episode_title: str,
                                   s3_video_key: str,
                                   chunks_info: Optional[List[Chunk]] = None, 
                                   quotes_info: Optional[List[Quote]] = None,
                                   overwrite: bool = True) -> Dict[str, List[Tuple[str, str]]]:
    """
    Unified video processing function that downloads the source video once and processes
    both chunks and quotes using the shared ephemeral file.
    
    Args:
        PK: Primary key
        SK: Sort key  
        podcast_title: Title of the podcast
        episode_title: Title of the episode
        s3_video_key: S3 key for the source video
        chunks_info: List of chunks to process
        quotes_info: List of quotes to process
        overwrite: Whether to overwrite existing files
        
    Returns:
        Dictionary with 'chunks' and 'quotes' keys containing lists of (filename, s3_key) tuples
    """
    # Input validation
    if not PK or not SK or not podcast_title or not episode_title or not s3_video_key:
        raise ValueError("PK, SK, podcast_title, episode_title, and s3_video_key cannot be empty")
    
    # Create safe names for file paths
    safe_podcast_title = "".join(c for c in podcast_title if c.isalnum() or c in ("-", "_")).strip()
    safe_episode_title = "".join(c for c in episode_title if c.isalnum() or c in ("-", "_")).strip()
    
    logging.info(f"Starting unified video processing - Podcast: '{safe_podcast_title}', Episode: '{safe_episode_title}'")
    
    if not chunks_info and not quotes_info:
        logging.warning("No chunks or quotes provided for processing")
        return {'chunks': [], 'quotes': []}

    try:
        results = {'chunks': [], 'quotes': []}
        
        # Create temporary directory for processing
        with tempfile.TemporaryDirectory(prefix="video_artifacts_") as temp_dir:
            # Download source video once
            full_video_path = os.path.join(temp_dir, "source_video.mp4")
            
            try:
                logging.info(f"Downloading source video from s3://{config.video_bucket}/{s3_video_key}")
                s3_client.download_file(config.video_bucket, s3_video_key, full_video_path)
            except ClientError as e:
                logging.error(f"Failed to download source video: {e}")
                raise
            
            # Verify downloaded file
            if not os.path.exists(full_video_path) or os.path.getsize(full_video_path) == 0:
                raise Exception("Downloaded video file is empty or missing")
            
            logging.info(f"Source video downloaded successfully: {os.path.getsize(full_video_path)} bytes")
            
            # Process chunks if provided
            if chunks_info:
                logging.info(f"Processing {len(chunks_info)} video chunks...")
                chunks_results = process_video_chunks_with_path(
                    full_video_path=full_video_path,
                    temp_dir=temp_dir,
                    PK=PK,
                    SK=SK,
                    safe_podcast_title=safe_podcast_title,
                    safe_episode_title=safe_episode_title,
                    chunks_info=chunks_info,
                    overwrite=overwrite
                )
                results['chunks'] = chunks_results
                logging.info(f"Completed chunk processing: {len(chunks_results)} chunks")
            
            # Process quotes if provided
            if quotes_info:
                logging.info(f"Processing {len(quotes_info)} video quotes...")
                quotes_results = process_video_quotes_with_path(
                    full_video_path=full_video_path,
                    temp_dir=temp_dir,
                    PK=PK,
                    SK=SK,
                    safe_podcast_title=safe_podcast_title,
                    safe_episode_title=safe_episode_title,
                    quotes_info=quotes_info,
                    overwrite=overwrite
                )
                results['quotes'] = quotes_results
                logging.info(f"Completed quote processing: {len(quotes_results)} quotes")
            
            total_processed = len(results['chunks']) + len(results['quotes'])
            logging.info(f"Unified processing completed: {total_processed} total artifacts")
            return results
        
    except Exception as e:
        logging.error(f"Error in unified video processing: {e}")
        raise