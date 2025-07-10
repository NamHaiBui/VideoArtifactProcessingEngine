import tempfile
from typing import List, Tuple
import ffmpeg
import os.path
from datetime import datetime, timezone
import logging
import os
import tempfile
from botocore.exceptions import ClientError
import ffmpeg
from typing import List, Tuple
from ..models.quote_model import Quote
from ..config import config
from ..aws.aws_client import get_dynamodb_resource, get_s3_client
from ..utils.logging_config import setup_custom_logger

logging = setup_custom_logger(__name__)
# Initialize AWS clients
dynamodb = get_dynamodb_resource()
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
                    quote_identifier = f"{quote.episode_title}_{quote.proverb_rank}"
                    logging.info(f"Processing quote {i+1}/{len(snippets_info)}: {quote_identifier}")
                    
                    # Skip quotes that already have a video URL
                    if quote.quote_video_url and quote.quote_video_url.strip() and len(quote.quote_video_url.strip()) > 0:
                        logging.debug(f"Skipping quote {quote_identifier} - already has video URL")
                        continue
                    
                    # Validate quote timestamps - try context first, fallback to proverb
                    start_time = None
                    end_time = None
                    timestamp_source = None
                    """"""
                    # Try context timestamps first (includes surrounding context)
                    if hasattr(quote, 'context_timestamps') and \
                       hasattr(quote.context_timestamps, 'start_time') and \
                       hasattr(quote.context_timestamps, 'end_time') and \
                       quote.context_timestamps.start_time != 0.0 and \
                       quote.context_timestamps.end_time != 0.0:
                        start_time = quote.context_timestamps.start_time
                        end_time = quote.context_timestamps.end_time
                        timestamp_source = "context"
                    
                    # Fallback to proverb timestamps if context not available
                    elif hasattr(quote, 'proverb_timestamps') and \
                         hasattr(quote.proverb_timestamps, 'start_time') and \
                         hasattr(quote.proverb_timestamps, 'end_time') and \
                         quote.proverb_timestamps.start_time != 0.0 and \
                         quote.proverb_timestamps.end_time != 0.0:
                        start_time = quote.proverb_timestamps.start_time
                        end_time = quote.proverb_timestamps.end_time
                        timestamp_source = "proverb"
                    
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
                    quote_filename = f"{safe_episode_title}_quote_{quote.proverb_rank}.mp4"
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
                            update_quote_video_url(quote.pk, quote.sk, s3_video_url)
                            logging.info(f"Updated database with video URL for quote {quote_identifier}")
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
            quote_identifier = f"{quote.episode_title}_{quote.proverb_rank}"
            logging.info(f"Processing quote {i+1}/{len(quotes_info)}: {quote_identifier}")
            
            # Skip quotes that already have a video URL
            if quote.quote_video_url and quote.quote_video_url.strip() and len(quote.quote_video_url.strip()) > 0:
                logging.debug(f"Skipping quote {quote_identifier} - already has video URL")
                continue
            
            # Validate quote timestamps - try context first, fallback to proverb
            start_time = None
            end_time = None
            timestamp_source = None
            
            # Try context timestamps first (includes surrounding context)
            if hasattr(quote, 'context_timestamps') and \
               hasattr(quote.context_timestamps, 'start_time') and \
               hasattr(quote.context_timestamps, 'end_time') and \
               quote.context_timestamps.start_time != 0.0 and \
               quote.context_timestamps.end_time != 0.0:
                start_time = quote.context_timestamps.start_time
                end_time = quote.context_timestamps.end_time
                timestamp_source = "context"
            
            # Fallback to proverb timestamps if context not available
            elif hasattr(quote, 'proverb_timestamps') and \
                 hasattr(quote.proverb_timestamps, 'start_time') and \
                 hasattr(quote.proverb_timestamps, 'end_time') and \
                 quote.proverb_timestamps.start_time != 0.0 and \
                 quote.proverb_timestamps.end_time != 0.0:
                start_time = quote.proverb_timestamps.start_time
                end_time = quote.proverb_timestamps.end_time
                timestamp_source = "proverb"
            
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
            quote_filename = f"{safe_episode_title}_quote_{quote.proverb_rank}.mp4"
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
                    update_quote_video_url(quote.pk, quote.sk, s3_video_url)
                    logging.info(f"Updated database with video URL for quote {quote_identifier}")
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


def update_quote_video_url(quote_pk: str, quote_sk: str, video_url: str):
    """Update the quote with the S3 video URL using new schema.
    Updates both the full quote record and all associated simplified genre records.
    """
    if not quote_pk or not quote_sk or not video_url:
        raise ValueError("quote_pk, quote_sk, and video_url cannot be empty")
        
    try:
        quotes_table = dynamodb.Table(config.quotes_table)
        
        # Update the main quote record
        response = quotes_table.update_item(
            Key={
                'PK': quote_pk,
                'SK': quote_sk
            },
            UpdateExpression='SET quoteVideoUrl = :video_url , updatedAt = :updated_at',
            ExpressionAttributeValues={
                ':video_url': video_url,
                ':updated_at': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
            },
            ReturnValues='UPDATED_NEW'
        )
        logging.info(f"Updated quoteVideoUrl for main record {quote_pk}/{quote_sk}")
        
        # Extract quote ID from the main SK (format: DATE#timestamp#QUOTE#uuid)
        quote_id = None
        if '#QUOTE#' in quote_sk:
            quote_id = quote_sk.split('#QUOTE#')[1]
        
        if quote_id:
            # Query for all simplified records with this quote ID
            # SK pattern: QUOTE#uuid#GENRE#genre_uuid
            from boto3.dynamodb.conditions import Key
            
            simplified_records_response = quotes_table.query(
                KeyConditionExpression=Key('PK').eq(quote_pk) & Key('SK').begins_with(f'QUOTE#{quote_id}#GENRE#'),
                ProjectionExpression='SK'
            )
            
            simplified_records = simplified_records_response.get('Items', [])
            
            if simplified_records:
                logging.info(f"Found {len(simplified_records)} simplified genre records to update")
                
                # Update each simplified record
                for record in simplified_records:
                    simplified_sk = record['SK']
                    try:
                        quotes_table.update_item(
                            Key={
                                'PK': quote_pk,
                                'SK': simplified_sk
                            },
                            UpdateExpression='SET quoteVideoUrl = :video_url , updatedAt = :updated_at',
                            ExpressionAttributeValues={
                                ':video_url': video_url,
                                ':updated_at': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
                            }
                        )
                        logging.debug(f"Updated simplified record {quote_pk}/{simplified_sk}")
                    except ClientError as e:
                        logging.warning(f"Failed to update simplified record {simplified_sk}: {e}")
                        # Continue with other records even if one fails
                
                logging.info(f"Updated video URL for main quote and {len(simplified_records)} simplified records")
            else:
                logging.info("No simplified genre records found to update")
        else:
            logging.warning(f"Could not extract quote ID from SK: {quote_sk}")
        
        return response
        
    except ClientError as e:
        logging.error(f"Error updating quote video URL: {e}")
        raise e