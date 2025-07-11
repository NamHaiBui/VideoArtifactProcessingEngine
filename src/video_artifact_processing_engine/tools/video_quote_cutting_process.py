import os
import tempfile
from typing import List, Tuple
from datetime import datetime
from botocore.exceptions import ClientError
import ffmpeg
import logging

from ..models.quote_model import Quote
from ..config import config
from ..aws.aws_client import get_s3_client
from ..db.db_operations import update_quote_video_url
from ..utils.logging_config import setup_custom_logger

logging = setup_custom_logger(__name__)
# Initialize AWS client
s3_client = get_s3_client()

def process_video_quotes(PK: str, 
                         SK: str,
                         podcast_title: str,
                         episode_title: str,
                         s3_video_key: str,
                         snippets_info: List[Quote], 
                         overwrite: bool = True) -> List[Tuple[str, str]]:
    """
    Simple sequential video quote processing.
    Cuts video snippets from a single input file.
    Uses provided podcast title and episode title for naming.
    """
    
    # Input validation
    if not PK or not SK or not podcast_title or not episode_title or not s3_video_key:
        raise ValueError("PK, SK, podcast_title, episode_title, and s3_video_key cannot be empty")
    
    if not snippets_info:
        logging.warning("No snippets provided for processing")
        return []
    
    logging.info(f"Processing {len(snippets_info)} quotes")
    
    # Create safe names for file paths
    safe_podcast_title = "".join(c for c in podcast_title if c.isalnum() or c in ('-', '_')).strip()
    safe_episode_title = "".join(c for c in episode_title if c.isalnum() or c in ('-', '_')).strip()

    try:
        successful_uploads = []
        
        # Create temporary directory for processing
        with tempfile.TemporaryDirectory(prefix="video_quotes_") as temp_dir:
            # Download source video
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
            
            # Process each quote
            for i, quote in enumerate(snippets_info):
                try:
                    # Create a unique identifier for the quote
                    quote_identifier = f"{quote.episodeTitle}_{quote.quoteRank}"
                    logging.info(f"Processing quote {i+1}/{len(snippets_info)}: {quote_identifier}")
                    
                    # Skip quotes that already have a video URL
                    if quote.quoteAudioUrl and quote.quoteAudioUrl.strip() and len(quote.quoteAudioUrl.strip()) > 0:
                        logging.debug(f"Skipping quote {quote_identifier} - already has video URL")
                        continue
                    
                    # Validate quote timestamps - try context first, fallback to quote
                    start_time = None
                    end_time = None
                    timestamp_source = None
                    
                    # Try context timestamps first (includes surrounding context)
                    if quote.context_start_ms and quote.context_end_ms and \
                       quote.context_start_ms > 0 and quote.context_end_ms > 0:
                        start_time = quote.context_start_ms / 1000.0  # Convert ms to seconds
                        end_time = quote.context_end_ms / 1000.0
                        timestamp_source = "context"
                    
                    # Fallback to quote timestamps if context not available
                    elif quote.quote_start_ms and quote.quote_end_ms and \
                         quote.quote_start_ms > 0 and quote.quote_end_ms > 0:
                        start_time = quote.quote_start_ms / 1000.0  # Convert ms to seconds
                        end_time = quote.quote_end_ms / 1000.0
                        timestamp_source = "quote"
                    
                    if start_time is None or end_time is None or timestamp_source is None:
                        logging.warning(f"Skipping quote {i+1} due to missing valid timestamps")
                        logging.debug(f"Context timestamps: {getattr(quote, 'context_timestamps', 'missing')}")
                        logging.debug(f"Proverb timestamps: {getattr(quote, 'proverb_timestamps', 'missing')}")
                        continue
                    
                    logging.debug(f"Quote {i+1} using {timestamp_source} timestamps - start: {start_time} ({type(start_time)}), end: {end_time} ({type(end_time)})")
                    try:
                        start_time = float(start_time)
                        end_time = float(end_time)
                    except (ValueError, TypeError) as e:
                        logging.warning(f"Skipping quote {i+1} due to invalid timestamp format: {e}")
                        continue
                    
                    # Validate timestamp values
                    if start_time < 0 or end_time < 0:
                        logging.warning(f"Skipping quote {i+1} due to negative timestamps")
                        continue
                        
                    if start_time > end_time:
                        logging.warning(f"Swapping timestamps for quote {i+1}: start={start_time}, end={end_time}")
                        start_time, end_time = end_time, start_time
                    
                    duration = end_time - start_time
                    if duration < 0.1:
                        logging.warning(f"Skipping quote {i+1} due to too short duration: {duration}s")
                        continue
                    
                    # Prepare file paths
                    quote_filename = f"{safe_episode_title}_quote_{quote.quote_rank}.mp4"
                    quote_path = os.path.join(temp_dir, quote_filename)
                    s3_quote_key = f"{safe_podcast_title}/{safe_episode_title}/{quote_filename}"

                    # Process with FFmpeg
                    logging.info(f"Processing quote {quote_filename}: {start_time:.2f}s to {end_time:.2f}s")
                    try:
                        (
                            ffmpeg
                            .input(full_video_path, ss=start_time, t=duration)
                            .output(quote_path, vcodec='libx264', acodec='aac', crf=23, preset='medium', movflags='+faststart')
                            .overwrite_output()
                            .run(capture_stdout=True, capture_stderr=True)
                        )
                        
                        # Verify output file was created
                        if not os.path.exists(quote_path) or os.path.getsize(quote_path) == 0:
                            logging.error(f"FFmpeg did not create valid output for quote {quote_filename}")
                            continue
                            
                    except ffmpeg.Error as e:
                        stderr = e.stderr.decode('utf8', errors='ignore') if e.stderr else "No error output"
                        logging.error(f"FFmpeg failed for quote {quote_filename}: {stderr}")
                        continue
                    
                    # Upload to S3
                    try:
                        s3_client.upload_file(
                            quote_path, 
                            config.video_quote_bucket, 
                            s3_quote_key, 
                            ExtraArgs={
                                "ContentType": "video/mp4", 
                                "ACL": "public-read",
                            }
                        )
                        successful_uploads.append((quote_filename, s3_quote_key))
                        logging.info(f"Uploaded {quote_filename} to S3")
                        
                        # Update quote with S3 video URL
                        try:
                            s3_video_url = f"https://{config.video_quote_bucket}.s3.amazonaws.com/{s3_quote_key}"
                            if update_quote_video_url(quote.quote_id, s3_video_url):
                                logging.info(f"Updated database with video URL for quote {quote_identifier}")
                            else:
                                logging.error(f"Failed to update database with video URL for quote {quote_identifier}")
                        except Exception as e:
                            logging.error(f"Failed to update database with video URL for quote {quote_identifier}: {e}")
                            # Don't fail the entire process if database update fails
                        
                    except ClientError as e:
                        logging.error(f"Failed to upload {quote_filename}: {e}")
                        continue
                        
                except Exception as e:
                    logging.error(f"Error processing quote {i+1}: {e}")
                    continue
            
            logging.info(f"Successfully processed {len(successful_uploads)}/{len(snippets_info)} quotes")
            return successful_uploads
                
    except Exception as e:
        logging.error(f"Error in process_video_quotes: {e}")
        raise

def process_video_quotes_with_path(full_video_path: str,
                                  temp_dir: str,
                                  PK: str,
                                  SK: str,
                                  safe_podcast_title: str,
                                  safe_episode_title: str,
                                  quotes_info: List[Quote],
                                  overwrite: bool = True) -> List[Tuple[str, str]]:
    """
    Process video quotes using an already downloaded video file.
    """
    successful_uploads = []
    
    for i, quote in enumerate(quotes_info):
        try:
            # Create a unique identifier for the quote
            quote_identifier = f"{quote.episode_title}_{quote.quote_rank}"
            logging.info(f"Processing quote {i+1}/{len(quotes_info)}: {quote_identifier}")
            
            # Skip quotes that already have a video URL
            if quote.quote_audio_url and quote.quote_audio_url.strip() and len(quote.quote_audio_url.strip()) > 0:
                logging.debug(f"Skipping quote {quote_identifier} - already has video URL")
                continue
            
            # Validate quote timestamps - try context first, fallback to proverb
            start_time = None
            end_time = None
            timestamp_source = None
            
            # Try context timestamps first (includes surrounding context)
            if quote.context_start_ms and quote.context_end_ms and \
               quote.context_start_ms > 0 and quote.context_end_ms > 0:
                start_time = quote.context_start_ms / 1000.0  # Convert ms to seconds
                end_time = quote.context_end_ms / 1000.0
                timestamp_source = "context"
            
            # Fallback to quote timestamps if context not available
            elif quote.quote_start_ms and quote.quote_end_ms and \
                 quote.quote_start_ms > 0 and quote.quote_end_ms > 0:
                start_time = quote.quote_start_ms / 1000.0  # Convert ms to seconds
                end_time = quote.quote_end_ms / 1000.0
                timestamp_source = "quote"
            
            if start_time is None or end_time is None or timestamp_source is None:
                logging.warning(f"Skipping quote {i+1} due to missing valid timestamps")
                logging.debug(f"Context timestamps: {getattr(quote, 'context_timestamps', 'missing')}")
                logging.debug(f"Proverb timestamps: {getattr(quote, 'proverb_timestamps', 'missing')}")
                continue
            
            logging.debug(f"Quote {i+1} using {timestamp_source} timestamps - start: {start_time} ({type(start_time)}), end: {end_time} ({type(end_time)})")
            try:
                start_time = float(start_time)
                end_time = float(end_time)
            except (ValueError, TypeError) as e:
                logging.warning(f"Skipping quote {i+1} due to invalid timestamp format: {e}")
                continue
            
            # Validate timestamp values
            if start_time < 0 or end_time < 0:
                logging.warning(f"Skipping quote {i+1} due to negative timestamps")
                continue
                
            if start_time > end_time:
                logging.warning(f"Swapping timestamps for quote {i+1}: start={start_time}, end={end_time}")
                start_time, end_time = end_time, start_time
            
            duration = end_time - start_time
            if duration < 0.1:
                logging.warning(f"Skipping quote {i+1} due to too short duration: {duration}s")
                continue
            
            # Prepare file paths
            quote_filename = f"{safe_episode_title}_quote_{quote.quoteRank}.mp4"
            quote_path = os.path.join(temp_dir, quote_filename)
            s3_quote_key = f"{safe_podcast_title}/{safe_episode_title}/{quote_filename}"  # Include podcast/episode subfolders

            # Process with FFmpeg
            logging.info(f"Processing quote {quote_filename}: {start_time:.2f}s to {end_time:.2f}s")
            try:
                (
                    ffmpeg
                    .input(full_video_path, ss=start_time, t=duration)
                    .output(quote_path, vcodec='libx264', acodec='aac', crf=23, preset='medium', movflags='+faststart')
                    .overwrite_output()
                    .run(capture_stdout=True, capture_stderr=True)
                )
                
                # Verify output file was created
                if not os.path.exists(quote_path) or os.path.getsize(quote_path) == 0:
                    logging.error(f"FFmpeg did not create valid output for quote {quote_filename}")
                    continue
                    
            except ffmpeg.Error as e:
                stderr = e.stderr.decode('utf8', errors='ignore') if e.stderr else "No error output"
                logging.error(f"FFmpeg failed for quote {quote_filename}: {stderr}")
                continue
            
            # Upload to S3
            try:
                s3_client.upload_file(
                    quote_path, 
                    config.video_quote_bucket, 
                    s3_quote_key, 
                    ExtraArgs={
                        "ContentType": "video/mp4", 
                        "ACL": "public-read",
                    }
                )
                successful_uploads.append((quote_filename, s3_quote_key))
                logging.info(f"Uploaded {quote_filename} to S3")
                
                # Update quote with S3 video URL
                try:
                    s3_video_url = f"https://{config.video_quote_bucket}.s3.amazonaws.com/{s3_quote_key}"
                    if update_quote_video_url(quote.quote_id, s3_video_url):
                        logging.info(f"Updated database with video URL for quote {quote_identifier}")
                    else:
                        logging.error(f"Failed to update database with video URL for quote {quote_identifier}")
                except Exception as e:
                    logging.error(f"Failed to update database with video URL for quote {quote_identifier}: {e}")
                    # Don't fail the entire process if database update fails
                
            except ClientError as e:
                logging.error(f"Failed to upload {quote_filename}: {e}")
                continue
                
        except Exception as e:
            logging.error(f"Error processing quote {i+1}: {e}")
            continue
    
    logging.info(f"Successfully processed {len(successful_uploads)}/{len(quotes_info)} quotes")
    return successful_uploads


"""Quote processing has been moved to use RDS instead of DynamoDB.
Database operations are now in db_operations.py
"""