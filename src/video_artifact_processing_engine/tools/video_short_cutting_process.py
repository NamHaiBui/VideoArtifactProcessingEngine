import asyncio
import logging
import os
import traceback
from typing import List, Dict
from botocore.exceptions import ClientError
import ffmpeg

from video_artifact_processing_engine.aws.db_operations import update_short_video_url, update_short_additional_data
from video_artifact_processing_engine.utils.ffmpeg_utils import run_ffmpeg_with_retries
from video_artifact_processing_engine.utils.async_retry import retry_with_backoff, emit_db_retry_failed_metric
from ..aws.aws_client import create_aws_client_with_retries
from ..models.shorts_model import Short
from ..config import config
from ..aws.aws_client import get_s3_client
from ..utils.logging_config import setup_custom_logger
from .video_hls_converter import VideoHLSConverter

logging = setup_custom_logger(__name__)
s3_client = get_s3_client()
cloudwatch_client = create_aws_client_with_retries('cloudwatch')

async def _retry_update_with_backoff(coro_func, *args, item_type: str, item_id: str, attempts: int = 3, base_delay: float = 0.5) -> bool:
    for attempt in range(1, attempts + 1):
        try:
            result = await coro_func(*args)
            if result:
                return True
            if attempt < attempts:
                delay = base_delay * (2 ** (attempt - 1))
                logging.info(f"{item_type} {item_id} update skipped (lock). Retrying in {delay:.2f}s (attempt {attempt}/{attempts})")
                await asyncio.sleep(delay)
        except Exception as e:
            logging.error(f"{item_type} {item_id} update raised error on attempt {attempt}: {e}")
            if attempt < attempts:
                delay = base_delay * (2 ** (attempt - 1))
                await asyncio.sleep(delay)
            else:
                break
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
    Process video chunks from a single video definition and create HLS streams with master playlist
    verification, plus MP4 upload.
    """
    successful_uploads: list[Dict[str, str]] = []

    sem = asyncio.Semaphore(config.max_concurrent_processing)

    async def handle_chunk(idx: int, chunk: Short) -> None:
        async with sem:
            logging.info(f"Processing chunk {idx+1}/{len(chunks_info)}: {chunk.chunk_id} for definition {definition_name}")

            start_time = float(chunk.start_ms) / 1000.0 if chunk.start_ms is not None else None
            end_time = float(chunk.end_ms) / 1000.0 if chunk.end_ms is not None else None
            if start_time is None or end_time is None or start_time >= end_time:
                return

            duration = end_time - start_time
            if duration < 1.0:
                return

            chunk_filename = f"short_{chunk.chunk_id}.mp4"
            chunk_path = os.path.join(temp_dir, chunk_filename)
            s3_chunk_key = f"{safe_podcast_title}/{safe_episode_title}/{chunk.chunk_id}/video/{chunk_filename}"

            try:
                ffmpeg_output = (
                    ffmpeg.input(full_video_path, ss=start_time, t=duration)
                    .output(
                        chunk_path,
                        vcodec='libx264',
                        acodec='aac',
                        crf=23,
                        preset=getattr(config, 'ffmpeg_preset', 'medium'),
                    )
                    .overwrite_output()
                )
                success, _stderr = await run_ffmpeg_with_retries(ffmpeg_output, f"for chunk {chunk.chunk_id}")
                if not success:
                    logging.error(f"FFmpeg failed for chunk {chunk.chunk_id} after 3 attempts. Skipping.")
                    return

                if not os.path.exists(chunk_path) or os.path.getsize(chunk_path) == 0:
                    logging.error(f"FFmpeg did not create valid output for chunk {chunk_filename}")
                    return

                hls_output_dir = os.path.join(temp_dir, f"hls_chunk_{chunk.chunk_id}_{definition_name}")
                _ = await hls_converter.transcode_to_hls(chunk_path, hls_output_dir)

                s3_hls_prefix = f"{safe_podcast_title}/{safe_episode_title}/{chunk.chunk_id}/video/hls"
                hls_url, _all_s3_keys = await hls_converter.upload_hls_to_s3(hls_output_dir, s3_hls_prefix, config.video_chunk_bucket)

                # Upload MP4 too
                s3_client.upload_file(chunk_path, config.video_chunk_bucket, s3_chunk_key, ExtraArgs={'ContentType': 'video/mp4'})
                video_url = f"https://{config.video_chunk_bucket}.s3.us-east-1.amazonaws.com/{s3_chunk_key}"

                # Prefer HLS master as main URL for shorts
                await retry_with_backoff(
                    update_short_video_url,
                    chunk.chunk_id,
                    hls_url,
                    on_final_failure=lambda: emit_db_retry_failed_metric(cloudwatch_client, 'Short', str(chunk.chunk_id)),
                )

                if 'videoChunkPath' not in chunk.additional_data:
                    chunk.additional_data['videoChunkPath'] = ""
                chunk.additional_data['videoChunkPath'] = video_url
                if 'videoMasterPlaylistPath' not in chunk.additional_data:
                    chunk.additional_data['videoMasterPlaylistPath'] = ""
                chunk.additional_data['videoMasterPlaylistPath'] = hls_url
                chunk.content_type = 'video'
                await retry_with_backoff(
                    update_short_additional_data,
                    chunk.chunk_id,
                    chunk.additional_data,
                    chunk.content_type,
                    on_final_failure=lambda: emit_db_retry_failed_metric(cloudwatch_client, 'Short', str(chunk.chunk_id)),
                )
                successful_uploads.append({'chunk_id': chunk.chunk_id, 'hls_url': hls_url})
                logging.info(f"Uploaded HLS for chunk {chunk.chunk_id} to {hls_url}")
            except Exception as e:
                logging.error(f"Error processing chunk {chunk.chunk_id}: {e}")
                logging.error(traceback.format_exc())
                return

    await asyncio.gather(*(handle_chunk(i, c) for i, c in enumerate(chunks_info)))
    return successful_uploads