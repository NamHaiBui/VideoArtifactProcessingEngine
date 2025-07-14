import os
import tempfile
import asyncio
from typing import List, Dict
from botocore.exceptions import ClientError
import ffmpeg
import logging

from video_artifact_processing_engine.aws.db_operations import update_quote, update_quote_video_url
from ..models.quote_model import Quote
from ..config import config
from ..aws.aws_client import get_s3_client
from ..utils.logging_config import setup_custom_logger
from .video_hls_converter import VideoHLSConverter

logging = setup_custom_logger(__name__)
s3_client = get_s3_client()

async def process_video_quotes_with_path(
    full_video_path: str,
    temp_dir: str,
    safe_podcast_title: str,
    safe_episode_title: str,
    quotes_info: List[Quote],
    overwrite: bool,
    hls_converter: VideoHLSConverter,
    definition_name: str
) -> List[Dict[str, str]]:
    """
    Process video quotes from a single video definition and create HLS streams.
    """
    successful_uploads = []
    
    for i, quote in enumerate(quotes_info):
        quote_identifier = f"{quote.episode_title}_{quote.quote_rank}"
        logging.info(f"Processing quote {i+1}/{len(quotes_info)}: {quote_identifier} for definition {definition_name}")

        start_time, end_time = None, None
        if quote.context_start_ms and quote.context_end_ms and quote.context_start_ms > 0 and quote.context_end_ms > 0:
            start_time = quote.context_start_ms / 1000.0
            end_time = quote.context_end_ms / 1000.0
        elif quote.quote_start_ms and quote.quote_end_ms and quote.quote_start_ms > 0 and quote.quote_end_ms > 0:
            start_time = quote.quote_start_ms / 1000.0
            end_time = quote.quote_end_ms / 1000.0

        if start_time is None or end_time is None: continue

        duration = end_time - start_time
        if duration < 0.1: continue

        quote_filename = f"video_ep_{quote.episode_id}_quote_{quote.quote_id}_vd_{definition_name}.mp4"
        quote_path = os.path.join(temp_dir, quote_filename)
        s3_quote_key = f"{safe_podcast_title}/{safe_episode_title}/quotes/{quote.quote_id}/{quote_filename}"

        try:
            (
                ffmpeg.input(full_video_path, ss=start_time, t=duration)
                .output(quote_path, vcodec='libx264', acodec='aac', crf=23, preset='medium', movflags='+faststart')
                .overwrite_output()
                .run(capture_stdout=True, capture_stderr=True)
            )

            if not os.path.exists(quote_path) or os.path.getsize(quote_path) == 0:
                logging.error(f"FFmpeg did not create valid output for quote {quote_filename}")
                continue

            hls_output_dir = os.path.join(temp_dir, f"episode_{quote.episode_id}_hls_quote_{quote.quote_id}_{definition_name}")
            await hls_converter.transcode_to_hls(quote_path, hls_output_dir)

            s3_hls_prefix = f"{safe_podcast_title}/{safe_episode_title}/quotes/{quote.quote_id}/hls/"
            hls_url, all_s3_keys = await hls_converter.upload_hls_to_s3(hls_output_dir, s3_hls_prefix, config.video_quote_bucket)
            s3_client.upload_file(quote_path, config.video_quote_bucket, s3_quote_key, ExtraArgs={'ContentType': 'video/mp4', 'ACL': 'public-read'})
            
            # Update the main URL field with the master playlist
            await update_quote_video_url(quote.quote_id, hls_url)
            
            # Append all HLS paths to additionalData
            if 'videoQuotePath' not in quote.additional_data:
                quote.additional_data['videoQuotePath'] = []
            quote.additional_data['videoQuotePath'].extend(f"https://{config.video_chunk_bucket}.s3.us-east-1.amazonaws.com/{s3_quote_key}")

            await update_quote(quote)

            successful_uploads.append({'quote_id': quote.quote_id, 'hls_url': hls_url})
            logging.info(f"Uploaded HLS for quote {quote.quote_id} to {hls_url}")

        except Exception as e:
            logging.error(f"Error processing quote {quote.quote_id}: {e}")
            continue
            
    return successful_uploads