import asyncio
import os
import tempfile
from typing import Dict, List, Optional, Tuple
from video_artifact_processing_engine.config import config
from video_artifact_processing_engine.aws.aws_client import get_s3_client, S3Service
from video_artifact_processing_engine.models.shorts_model import Short
from video_artifact_processing_engine.models.quote_model import Quote
from video_artifact_processing_engine.tools.video_quote_cutting_process import process_video_quotes_with_path
from video_artifact_processing_engine.tools.video_short_cutting_process import process_video_chunks_with_path
from video_artifact_processing_engine.tools.video_hls_converter import VideoHLSConverter
from botocore.exceptions import ClientError
from video_artifact_processing_engine.utils import create_slug
from video_artifact_processing_engine.utils.logging_config import setup_custom_logger

logging = setup_custom_logger(__name__)
s3_client = get_s3_client()

def list_video_definitions(bucket: str, prefix: str) -> List[str]:
    """
    Lists all video files in a given S3 prefix.
    """
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        logging.info(f"Listing S3 objects in bucket '{bucket}' with prefix '{prefix}'")
        if 'Contents' not in response:
            return []
        video_files = [
            content['Key'] for content in response['Contents'] 
            if content['Key'].lower().endswith(('.mp4', '.mov', '.avi', '.mkv'))
        ]
        return video_files
    except ClientError as e:
        logging.error(f"Failed to list S3 objects: {e}")
        return []


async def process_video_artifacts_unified(
    episode_id: str,
    podcast_title: str,
    episode_title: str,
    s3_video_key: str,
    s3_video_key_prefix: str, 
    chunks_info: Optional[List[Short]] = None, 
    quotes_info: Optional[List[Quote]] = None,
    overwrite: bool = True
) -> Dict[str, List[Dict[str, str]]]:
    """
    Unified video processing for all definitions in an S3 folder.
    """
    if not all([episode_id, podcast_title, episode_title, s3_video_key_prefix]):
        raise ValueError("episode_id, podcast_title, episode_title, and s3_video_key_prefix cannot be empty")

    safe_podcast_title = create_slug(podcast_title)
    safe_episode_title = create_slug(episode_title)

    logging.info(f"Starting unified video processing for: '{safe_podcast_title}/{safe_episode_title}'")
    
    # video_definitions = list_video_definitions(config.video_bucket, s3_video_key_prefix)
    # if not video_definitions:
    #     logging.error(f"No video definitions found at S3 prefix: {s3_video_key_prefix}")
    #     return {'chunks': [], 'quotes': []}

    # logging.info(f"Found {len(video_definitions)} video definitions to process.")

    results = {'chunks': [], 'quotes': []}
    
    with tempfile.TemporaryDirectory(prefix="video_artifacts_") as temp_dir:
        # Use S3Service wrapper so uploads have correct Content-Type and we can perform HEAD checks
        hls_converter = VideoHLSConverter(S3Service())

        s3_key = s3_video_key
        # for s3_key in video_definitions:
        definition_name = os.path.splitext(os.path.basename(s3_key))[0]
        logging.info(f"--- Processing definition: {definition_name} ---")
        
        local_video_path = os.path.join(temp_dir, os.path.basename(s3_key))
        
        try:
            logging.info(f"Downloading source video from s3://{config.video_bucket}/{s3_key}")
            s3_client.download_file(config.video_bucket, s3_key, local_video_path)
        except ClientError as e:
            logging.error(f"Failed to download source video {s3_key}: {e}")
            return results

        if not os.path.exists(local_video_path) or os.path.getsize(local_video_path) == 0:
            logging.error("Downloaded video file is empty or missing")
            return results
        
        tasks = []
        if quotes_info:
            tasks.append(process_video_quotes_with_path(
                full_video_path=local_video_path,
                temp_dir=temp_dir,
                safe_podcast_title=safe_podcast_title,
                safe_episode_title=safe_episode_title,
                quotes_info=quotes_info,
                overwrite=overwrite,
                hls_converter=hls_converter,
                definition_name=definition_name
            ))
        if chunks_info:
            tasks.append(process_video_chunks_with_path(
                full_video_path=local_video_path,
                temp_dir=temp_dir,
                safe_podcast_title=safe_podcast_title,
                safe_episode_title=safe_episode_title,
                chunks_info=chunks_info,
                overwrite=overwrite,
                hls_converter=hls_converter,
                definition_name=definition_name
            ))

        if tasks:
            processed_results = await asyncio.gather(*tasks, return_exceptions=True)
            idx = 0
            if quotes_info:
                quotes_result = processed_results[idx]
                if isinstance(quotes_result, list):
                    results['quotes'].extend(quotes_result)
                elif isinstance(quotes_result, Exception):
                    logging.error(f"Error processing quotes: {quotes_result}")
                idx += 1
            if chunks_info:
                chunks_result = processed_results[idx if quotes_info else 0]
                if isinstance(chunks_result, list):
                    results['chunks'].extend(chunks_result)
                elif isinstance(chunks_result, Exception):
                    logging.error(f"Error processing chunks: {chunks_result}")

    logging.info(f"Unified processing completed. {len(results['quotes'])} quotes and {len(results['chunks'])} chunks processed.")
    return results