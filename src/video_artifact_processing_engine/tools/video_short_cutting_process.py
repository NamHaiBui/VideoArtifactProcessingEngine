import logging
import os
from typing import List, Dict
from botocore.exceptions import ClientError
import ffmpeg

from video_artifact_processing_engine.aws.db_operations import update_short, update_short_video_url
from ..models.shorts_model import Short
from ..config import config
from ..aws.aws_client import get_s3_client
from ..utils.logging_config import setup_custom_logger
from .video_hls_converter import VideoHLSConverter

logging = setup_custom_logger(__name__)
s3_client = get_s3_client()

async def process_video_chunks_with_path(
    full_video_path: str,
    temp_dir: str,
    safe_podcast_title: str,
    safe_episode_title: str,
    chunks_info: List[Short],
    overwrite: bool,
    hls_converter: VideoHLSConverter,
    definition_name: str
) -> List[Dict[str, str]]:
    """
    Process video chunks from a single video definition and create HLS streams.
    """
    successful_uploads = []
    
    for i, chunk in enumerate(chunks_info):
        logging.info(f"Processing chunk {i+1}/{len(chunks_info)}: {chunk.chunk_id} for definition {definition_name}")
        
        start_time = float(chunk.start_ms) / 1000.0 if chunk.start_ms is not None else None
        end_time = float(chunk.end_ms) / 1000.0 if chunk.end_ms is not None else None

        if start_time is None or end_time is None or start_time >= end_time:
            continue

        duration = end_time - start_time
        if duration < 1.0: continue

        chunk_filename = f"video_ep_{chunk.episode_id}_chunk_{chunk.chunk_id}_vd_{definition_name}.mp4"
        chunk_path = os.path.join(temp_dir, chunk_filename)
        s3_chunk_key = f"{safe_podcast_title}/{safe_episode_title}/chunks/{chunk.chunk_id}/{chunk_filename}"
        try:
            (
                ffmpeg.input(full_video_path, ss=start_time, t=duration)
                .output(chunk_path, vcodec='libx264', acodec='aac', crf=23)
                .overwrite_output()
                .run(capture_stdout=True, capture_stderr=True)
            )
            if not os.path.exists(chunk_path) or os.path.getsize(chunk_path) == 0:
                logging.error(f"FFmpeg did not create valid output for chunk {chunk_filename}")
                continue
            hls_output_dir = os.path.join(temp_dir, f"hls_chunk_{chunk.chunk_id}_{definition_name}")
            await hls_converter.transcode_to_hls(chunk_path, hls_output_dir)

            s3_hls_prefix = f"{safe_podcast_title}/{safe_episode_title}/shorts/{chunk.chunk_id}/hls/"
            hls_url, all_s3_keys = await hls_converter.upload_hls_to_s3(hls_output_dir, s3_hls_prefix, config.video_chunk_bucket)
            
            s3_client.upload_file(chunk_path, config.video_chunk_bucket, s3_chunk_key, ExtraArgs={'ContentType': 'video/mp4', 'ACL': 'private'})
            if 'videoChunkPath' not in chunk.additional_data:
                chunk.additional_data['videoChunkPath'] = []
            chunk.additional_data['videoChunkPath'].extend(f"https://{config.video_chunk_bucket}.s3.us-east-1.amazonaws.com/{s3_chunk_key}")

            # Update the main URL field with the master playlist
            await update_short_video_url(chunk.chunk_id, hls_url)
            await update_short(chunk)
            successful_uploads.append({'chunk_id': chunk.chunk_id, 'hls_url': hls_url})
            logging.info(f"Uploaded HLS for chunk {chunk.chunk_id} to {hls_url}")

        except Exception as e:
            logging.error(f"Error processing chunk {chunk.chunk_id}: {e}")
            continue
            
    return successful_uploads