import os
import tempfile
import asyncio
from typing import List, Dict
from botocore.exceptions import ClientError
import ffmpeg
import logging

from video_artifact_processing_engine.aws.db_operations import update_quote_video_url, update_quote_additional_data
from video_artifact_processing_engine.utils.ffmpeg_utils import run_ffmpeg_with_retries
from video_artifact_processing_engine.utils.async_retry import retry_with_backoff, emit_db_retry_failed_metric
from ..aws.aws_client import create_aws_client_with_retries
from ..models.quote_model import Quote
from ..config import config
from ..aws.aws_client import get_s3_client
from ..utils.logging_config import setup_custom_logger
from .video_hls_converter import VideoHLSConverter

logging = setup_custom_logger(__name__)
s3_client = get_s3_client()
cloudwatch_client = create_aws_client_with_retries('cloudwatch')

async def _retry_update_with_backoff(coro_func, *args, item_type: str, item_id: str, attempts: int = 3, base_delay: float = 0.5) -> bool:
    """Retry a DB update coroutine up to attempts with exponential backoff. Emits a CloudWatch metric on final failure."""
    for attempt in range(1, attempts + 1):
        try:
            result = await coro_func(*args)
            if result:
                return True
            # result False means likely skipped due to lock
            if attempt < attempts:
                delay = base_delay * (2 ** (attempt - 1))
                logging.info(f"{item_type} {item_id} update skipped (lock). Retrying in {delay:.2f}s (attempt {attempt}/{attempts})")
                await asyncio.sleep(delay)
        except Exception as e:
            # Log and break; exception is not a 'skipped' but a hard error
            logging.error(f"{item_type} {item_id} update raised error on attempt {attempt}: {e}")
            if attempt < attempts:
                delay = base_delay * (2 ** (attempt - 1))
                await asyncio.sleep(delay)
            else:
                break
    # Emit CloudWatch metric on final failure
    try:
        cloudwatch_client.put_metric_data(
            Namespace='VideoArtifactProcessingEngine/Alerts',
            MetricData=[
                {
                    'MetricName': 'DbUpdateRetryFailed',
                    'Dimensions': [
                        {'Name': 'ItemType', 'Value': item_type},
                        {'Name': 'Id', 'Value': str(item_id)},
                    ],
                    'Unit': 'Count',
                    'Value': 1.0,
                }
            ]
        )
        logging.warning(f"Emitted CloudWatch alarm metric for {item_type}={item_id} after retries exhausted")
    except Exception as me:
        logging.error(f"Failed to emit CloudWatch metric for {item_type}={item_id}: {me}")
    return False

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
    Process video quotes from a single video definition and create HLS streams and MP4 uploads.
    Ensures master.m3u8 exists and is valid before uploading.
    """
    successful_uploads: list[Dict[str, str]] = []

    sem = asyncio.Semaphore(config.max_concurrent_processing)

    async def handle_quote(i: int, quote: Quote) -> None:
        async with sem:
            quote_identifier = f"{quote.episode_title}_{quote.quote_rank}"
            logging.info(f"Processing quote {i+1}/{len(quotes_info)}: {quote_identifier} for definition {definition_name}")

            # Determine start/end
            start_time = None
            end_time = None
            if (
                quote.context_start_ms and quote.context_end_ms and
                quote.context_start_ms > 0 and quote.context_end_ms > 0
            ):
                start_time = quote.context_start_ms / 1000.0
                end_time = quote.context_end_ms / 1000.0
            elif (
                quote.quote_start_ms and quote.quote_end_ms and
                quote.quote_start_ms > 0 and quote.quote_end_ms > 0
            ):
                start_time = quote.quote_start_ms / 1000.0
                end_time = quote.quote_end_ms / 1000.0

            if start_time is None or end_time is None:
                return

            duration = end_time - start_time
            if duration < 0.1:
                return

            quote_filename = f"quote_{quote.quote_id}.mp4"
            quote_path = os.path.join(temp_dir, quote_filename)
            s3_quote_key = f"{safe_podcast_title}/{safe_episode_title}/{quote.quote_id}/video/{quote_filename}"

            try:
                # Build and run the ffmpeg command with retries
                ffmpeg_output = (
                    ffmpeg.input(full_video_path, ss=start_time, t=duration)
                    .output(quote_path, vcodec='libx264', acodec='aac', crf=23, preset='medium', movflags='+faststart')
                    .overwrite_output()
                )
                success, _stderr = await run_ffmpeg_with_retries(ffmpeg_output, f"for quote {quote.quote_id}")
                if not success:
                    logging.error(f"FFmpeg failed for quote {quote.quote_id} after 3 attempts. Skipping.")
                    return

                if not os.path.exists(quote_path) or os.path.getsize(quote_path) == 0:
                    logging.error(f"FFmpeg did not create valid output for quote {quote_filename}")
                    return

                # Create HLS reliably
                hls_output_dir = os.path.join(temp_dir, f"episode_{quote.episode_id}_hls_quote_{quote.quote_id}_{definition_name}")
                master_playlist_path = await hls_converter.transcode_to_hls(quote_path, hls_output_dir)
                logging.info(f"Transcoded quote {quote.quote_id} to HLS at {hls_output_dir}")

                s3_hls_prefix = f"{safe_podcast_title}/{safe_episode_title}/{quote.quote_id}/video/hls"
                hls_url, all_s3_keys = await hls_converter.upload_hls_to_s3(hls_output_dir, s3_hls_prefix, config.video_quote_bucket)

                # Upload the MP4 file directly to S3 as well
                s3_client.upload_file(quote_path, config.video_quote_bucket, s3_quote_key, ExtraArgs={'ContentType': 'video/mp4'})
                logging.info(f"Uploaded quote video file to S3 at {s3_quote_key}")
                video_url = f"https://{config.video_quote_bucket}.s3.us-east-1.amazonaws.com/{s3_quote_key}"

                # Prefer HLS master playlist as the main URL
                await retry_with_backoff(
                    update_quote_video_url,
                    quote.quote_id,
                    hls_url,
                    on_final_failure=lambda: emit_db_retry_failed_metric(cloudwatch_client, 'Quote', str(quote.quote_id)),
                )
                logging.info(f"Attempted to update quote {quote.quote_id} in DB with new video URL (HLS master)")

                # Update additional data with both paths
                if 'videoQuotePath' not in quote.additional_data:
                    quote.additional_data['videoQuotePath'] = ""
                if 'videoMasterPlaylistPath' not in quote.additional_data:
                    quote.additional_data['videoMasterPlaylistPath'] = ""
                quote.additional_data['videoQuotePath'] = video_url
                quote.additional_data['videoMasterPlaylistPath'] = hls_url
                quote.content_type = 'video'
                await retry_with_backoff(
                    update_quote_additional_data,
                    quote.quote_id,
                    quote.additional_data,
                    quote.content_type,
                    on_final_failure=lambda: emit_db_retry_failed_metric(cloudwatch_client, 'Quote', str(quote.quote_id)),
                )
                logging.info(f"Updated quote {quote.quote_id} with MP4 and HLS URLs.")
                successful_uploads.append({'quote_id': quote.quote_id, 'hls_url': hls_url})
                logging.info(f"Uploaded HLS for quote {quote.quote_id} to {hls_url}")

            except Exception as e:
                logging.error(f"Error processing quote {quote.quote_id}: {e}")
                return

    await asyncio.gather(*(handle_quote(i, q) for i, q in enumerate(quotes_info)))
    return successful_uploads