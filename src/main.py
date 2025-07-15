import asyncio
import boto3
import json
import os
import sys
import time
import uuid

from video_artifact_processing_engine.aws.aws_client import get_sqs_client, validate_aws_credentials
from video_artifact_processing_engine.aws.db_operations import get_episode_by_id, get_episode_processing_status, get_quotes_by_episode_id, get_shorts_by_episode_id
from video_artifact_processing_engine.tools.video_artifacts_cutting_process import process_video_artifacts_unified
from video_artifact_processing_engine.utils import parse_s3_url
from video_artifact_processing_engine.utils.logging_config import setup_custom_logger
from video_artifact_processing_engine.config import config
# Set up logging
logging = setup_custom_logger(__name__)

CLEANUP_TEMP_FILES = os.environ.get('CLEANUP_TEMP_FILES', 'true').lower() == 'true'

# Processing stats
processing_stats = {
    'total_processed': 0,
    'successful': 0,
    'failed': 0,
    'average_processing_time': 0
}

class ProcessingSession:
    """
    Simple session storage for sequential processing.
    Manages state and resources for a single processing session.
    """
    def __init__(self, session_id):
        self.session_id = session_id
        self.start_time = time.time()
        self.results = {}
        self.temp_files = []
        self.processing_status = 'initialized'
        self.logger = setup_custom_logger(f"{__name__}.session_{session_id}")
        
    def add_temp_file(self, filepath):
        """Add a temporary file to cleanup list"""
        self.temp_files.append(filepath)
        
    def set_result(self, key, value):
        """Store a result"""
        self.results[key] = value
        
    def get_result(self, key, default=None):
        """Get a result"""
        return self.results.get(key, default)
        
    def cleanup(self):
        """Cleanup temporary files and resources"""
        for temp_file in self.temp_files:
            try:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                    self.logger.debug(f"Cleaned up temp file: {temp_file}")
            except Exception as e:
                self.logger.warning(f"Failed to cleanup temp file {temp_file}: {e}")
        
        self.processing_status = 'completed'
        elapsed_time = time.time() - self.start_time
        self.logger.info(f"Session {self.session_id} completed in {elapsed_time:.2f} seconds")

def create_processing_session():
    """Create a new processing session for sequential processing"""
    session_id = str(uuid.uuid4())[:8]  
    return ProcessingSession(session_id)

# Validate AWS credentials before proceeding
logging.info("Validating AWS credentials...")
credential_status = validate_aws_credentials()

if not credential_status['valid']:
    logging.error(f"AWS credentials validation failed: {credential_status.get('error')}")
    logging.error("Please ensure AWS credentials are configured properly:")
    logging.error("1. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables")
    logging.error("2. Or configure AWS CLI with 'aws configure'")
    logging.error("3. Or use IAM roles if running on EC2/ECS")
    sys.exit(1)

logging.info(f"AWS credentials validated successfully!")
logging.info(f"Account: {credential_status.get('account')}")
logging.info(f"Region: {credential_status.get('region')}")
logging.info(f"Credential sources: {credential_status.get('sources')}")

# Initialize SQS client for AWS
sqs = get_sqs_client()
logging.info("Using AWS SQS endpoint")
    
async def process_sqs_message(message_body, message_id):
    """
    Process a single SQS message sequentially.
    
    Args:
        message_body (str): JSON string containing the message data
        message_id (str): Unique identifier for the message
        
    Returns:
        dict: Processing results with success status and details
    """
    # Create processing session for this message
    session = create_processing_session()
    logger = session.logger
    
    try:
        logger.info(f"Starting sequential processing for message: {message_id}")
        logger.debug(f"Raw message body: {message_body}")
        
        # Parse the message body
        message_data = json.loads(message_body)
        logger.debug(f"Parsed message data: {message_data}")
        
        # Store message data in session
        session.set_result('message_data', message_data)
        session.set_result('message_id', message_id)
        session.processing_status = 'parsing'
        
        # Extract metadata ID (adjust key name based on your message structure)
        meta_data_idx = message_data.get('id','')
        if not meta_data_idx:
            return {'success': False, 'error': 'No metadata ID found', 'session_id': session.session_id}
        force_video_quoting = message_data.get('force_video_quoting', True)
        force_video_chunking = message_data.get('force_video_chunking', True)

        if not meta_data_idx:
            logger.error(f"No metadata ID found in message: {message_body}")
            logger.debug(f"Available keys in message: {list(message_data.keys())}")
            session.processing_status = 'failed'
            return {'success': False, 'error': 'No metadata ID found', 'session_id': session.session_id}
        
        logger.info(f"Processing video artifacts for episode ID: {meta_data_idx}")
        session.set_result('meta_data_idx', meta_data_idx)
        session.processing_status = 'processing'
        
        # Process the video artifacts
        logger.info(f"Starting video artifact generation for episode ID: {meta_data_idx}")
        
        episode_id = str(meta_data_idx)
        logger.info(f"Querying RDS for episode: {episode_id}")
        
        # Query to get the episode metadata
        try:
            episode_item = get_episode_by_id(episode_id)
            logger.info(f"RDS query successful. Episode found: {episode_item is not None}")
        except Exception as e:
            logger.error(f"RDS query failed: {e}")
            logger.error(f"Query details - episode_id: {episode_id}")
            raise
        
        if not episode_item:
            logger.warning(f"No episode found for ID: {episode_id}")
            session.processing_status = 'failed'
            return {'success': False, 'error': 'No episode found', 'session_id': session.session_id}
            
        # Check for chunking_status first - this applies to both chunks and quotes processing
        processing_info = get_episode_processing_status(episode_id)
        if not processing_info:
            logger.error(f"No chunking status found for episode: {episode_id}")
            session.processing_status = 'failed'
            return {'success': False, 'error': 'No chunking status found', 'session_id': session.session_id}
        
        episode_title = episode_item.episode_title
        s3_http_link = episode_item.additional_data.get("videoLocation")
        logger.info(f"Episode title: {episode_item.additional_data}")
        podcast_title = str(episode_item.channel_name)
        if not s3_http_link:
            logger.error(f"No videoLocation found for episode {episode_id}")
            session.processing_status = 'failed'
            return {'success': False, 'error': 'No videoLocation found', 'session_id': session.session_id}
        s3_parsed = parse_s3_url(s3_http_link)
        s3_key = s3_parsed['path'] if s3_parsed else None
        s3_video_bucket = s3_parsed['bucket'] if s3_parsed else None
        s3_video_key = s3_key + s3_parsed['filename'] if s3_parsed and s3_key else None
        if not s3_video_key:
            logger.error(f"No video key found for episode {episode_id}")
            session.processing_status = 'failed'
            return {'success': False, 'error': 'No video key found', 'session_id': session.session_id}
        if s3_video_bucket and s3_video_bucket != config.video_bucket:
            logger.warning(f"Episode video bucket '{s3_video_bucket}' does not match configured video bucket '{config.video_bucket}'")
        logger.info(f"Parsed S3 video key: {s3_key}")
        # s3_key_prefix = 
        if not podcast_title or not episode_title:
            logger.error(f"Missing required titles: podcast_title='{podcast_title}', episode_title='{episode_title}'")
            session.processing_status = 'failed'
            return {'success': False, 'error': 'Missing required titles', 'session_id': session.session_id}
            
        if not s3_key:
            logger.error(f"No video file found for episode {episode_id}")
            session.processing_status = 'failed'
            return {'success': False, 'error': 'No video file found', 'session_id': session.session_id}
        quotes = []
        if not processing_info.get("quotingDone"):
            # Retrieve quotes for processing
            all_quotes = get_quotes_by_episode_id(episode_id)
            quotes = [x for x in all_quotes if x.quote and x.quote.strip() and x.context and x.context.strip()]

            # Log quote information for debugging
            if all_quotes:
                quote_lengths = [x.quote_length or 0 for x in all_quotes]
                logger.info(f"Quote lengths: min={min(quote_lengths)}, max={max(quote_lengths)}, avg={sum(quote_lengths)/len(quote_lengths):.2f}")
                zero_length_count = sum(1 for x in quote_lengths if x == 0.0)
                logger.info(f"Quotes with 0.0 length: {zero_length_count}/{len(all_quotes)}")
        chunks = []
        if  not processing_info.get("chunkingDone"):
            # Retrieve chunks for processing
            all_chunks = get_shorts_by_episode_id(episode_id)
            chunks = [x for x in all_chunks if x.transcript and x.transcript.strip()]
            
            # Log chunk information for debugging
            if all_chunks:
                chunk_lengths = [x.chunk_length or 0 for x in all_chunks]
                logger.info(f"Chunk lengths: min={min(chunk_lengths)}, max={max(chunk_lengths)}, avg={sum(chunk_lengths)/len(chunk_lengths):.2f}")
                zero_length_count = sum(1 for x in chunk_lengths if x == 0.0)
                logger.info(f"Chunks with 0.0 length: {zero_length_count}/{len(all_chunks)}")

        num_quotes = len(quotes)
        num_chunks = len(chunks)

        logger.info(f"Processing episode {episode_id}, ({episode_title}) - {num_quotes} quotes, {num_chunks} chunks")
        logger.info(f"File naming: Podcast='{podcast_title}', Episode='{episode_title}'")
        logger.info(f"Video source: {s3_key}")
        logger.info(f"Chunking status: {processing_info} - proceeding with video processing")
        

        logger.info(f"Retrieved {len(quotes)} quotes and {len(chunks)} chunks for episode: {episode_id}")

        # Process Video Artifacts (both quotes and chunks) in unified way
        if quotes or chunks:
        # if False:
            logging.info(f"Starting unified video artifact processing...")
            
            should_process_quotes = quotes and force_video_quoting
            should_process_chunks = chunks and force_video_chunking
            
            if should_process_quotes or should_process_chunks:
                try:
                    results = await process_video_artifacts_unified(
                        episode_id=episode_id,
                        podcast_title=podcast_title,
                        episode_title=episode_title,
                        s3_video_key=s3_video_key,
                        s3_video_key_prefix=s3_key,
                        chunks_info=chunks if should_process_chunks else None,
                        quotes_info=quotes if should_process_quotes else None,
                        overwrite=True
                    )
                    
                    video_quote_paths = results.get('quotes', [])
                    video_chunk_paths = results.get('chunks', [])
                    
                    # Log results
                    if should_process_quotes:
                        if len(video_quote_paths) >= 0:
                            logging.info(f"Video quote processing completed: {len(video_quote_paths)} quotes")
                            if len(video_quote_paths) != num_quotes:
                                logging.warning(f"Quote count mismatch: {len(video_quote_paths)} != {num_quotes}")
                        else:
                            logging.error(f"No quotes processed successfully")
                    
                    if should_process_chunks:
                        if len(video_chunk_paths) == int(num_chunks):
                            logger.info(f"Video chunking completed: {len(video_chunk_paths)} chunks")
                        else:
                            logger.warning(f"Chunk count mismatch: {len(video_chunk_paths)} != {num_chunks}")
                        
                except Exception as e:
                    if should_process_quotes:
                        logging.error(f"Video quote processing failed: {e}")
                    if should_process_chunks:
                        logger.error(f"Video chunk processing failed: {e}")
                    raise
            else:
                logging.info(f"All video processing already completed, skipping...")

        # Store results in session
        session.set_result('quotes', quotes)
        session.set_result('chunks', chunks)

        logger.debug(f"Processing results - quotes: {len(quotes)}, chunks: {len(chunks)}")
        
        # Create metadata dict for response
        metadata = {
            'podcast_title': podcast_title,
            'episode_title': episode_title,
            'episode_id': episode_id
        }
        
        logger.info(f"Successfully processed video artifacts for {metadata.get('podcast_title', 'Unknown')}/{metadata.get('episode_title', 'Unknown')}")
        session.processing_status = 'success'
        return {
            'success': True, 
            'session_id': session.session_id,
            'meta_data_idx': meta_data_idx,
            'metadata': metadata,
            'results_count': {
                'quotes': len(quotes),
                'chunks': len(chunks)
            }
        }
            
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in message body: {message_body}")
        logger.error(f"JSON decode error: {e}")
        session.processing_status = 'failed'
        return {'success': False, 'error': f'JSON decode error: {e}', 'session_id': session.session_id}
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        logger.exception("Full traceback:")
        session.processing_status = 'failed'
        return {'success': False, 'error': str(e), 'session_id': session.session_id}
    finally:
        # Cleanup session resources
        session.cleanup()
        session.cleanup()

async def poll_and_process_sqs_messages():
    """
    Main loop to poll SQS messages and process them sequentially.
    This prevents duplicate job execution.
    """
    if not config.queue_url:
        logging.error("SQS_QUEUE_URL environment variable not set")
        return
    logging.info(f"Starting SQS polling on queue: {config.queue_url}")
    try:
        while True:
            try:
                logging.debug("Polling for new SQS messages...")
                
                # Poll for messages from SQS (one message at a time for sequential processing)
                response = sqs.receive_message(
                    QueueUrl=config.queue_url,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=20  
                )
                
                logging.debug(f"SQS response: {response}")
                
                messages = response.get('Messages', [])
                
                if messages:
                    message = messages[0]  # Process the single message
                    message_body = message.get('Body')
                    receipt_handle = message.get('ReceiptHandle')
                    message_id = message.get('MessageId', str(uuid.uuid4()))
                    
                    logging.debug(f"Message details - ID: {message_id}, Body: {message_body[:200] if message_body else 'None'}...")
                    
                    if not message_body:
                        logging.error(f"No Body found in message {message_id}, skipping...")
                        continue
                    
                    if not receipt_handle:
                        logging.error(f"No ReceiptHandle found in message {message_id}, skipping...")
                        continue
                    
                    # Process message sequentially
                    logging.info(f"Processing message {message_id} sequentially")
                    result = await process_sqs_message(message_body, message_id)
                    
                    # Handle the result and cleanup
                    handle_completed_message(result, message_id, receipt_handle)
                else:
                    logging.debug("No messages received, continuing polling...")
                
            except KeyboardInterrupt:
                logging.info("Received interrupt signal. Shutting down gracefully...")
                break
            except Exception as e:
                logging.error(f"Error polling SQS: {e}")
                logging.exception("Full traceback:")
                time.sleep(10)
                
    finally:
        logging.info("SQS polling shutdown complete")

def handle_completed_message(result, message_id, receipt_handle):
    """
    Handle a completed processing message by checking results and cleaning up.
    
    Args:
        result: The processing result dictionary
        message_id: SQS message ID
        receipt_handle: SQS receipt handle for message deletion
    """
    try:
        success = result.get('success', False)
        session_id = result.get('session_id', 'unknown')
        
        # Update processing statistics
        processing_stats['total_processed'] += 1
        if success:
            processing_stats['successful'] += 1
        else:
            processing_stats['failed'] += 1
        
        if success:
            # Delete the message from SQS to prevent reprocessing
            try:
                sqs.delete_message(
                    QueueUrl=config.queue_url,
                    ReceiptHandle=receipt_handle
                )
                logging.info(f"Successfully processed and deleted message: {message_id} (Session: {session_id})")
                
                # Log additional result details
                if 'results_count' in result:
                    counts = result['results_count']
                    logging.info(f"Message {message_id} results: {counts.get('quotes', 0)} quotes, {counts.get('chunks', 0)} chunks")
                    
            except Exception as delete_error:
                logging.error(f"Failed to delete message {message_id}: {delete_error}")
        else:
            error = result.get('error', 'Unknown error')
            logging.error(f"Failed to process message {message_id} (Session: {session_id}): {error}")
            # Note: Message will remain in queue and may be retried based on SQS configuration
        
        # Log stats every 10 processed messages
        if processing_stats['total_processed'] % 10 == 0:
            log_processing_stats()
            
    except Exception as e:
        processing_stats['total_processed'] += 1
        processing_stats['failed'] += 1
        logging.error(f"Exception in task for message {message_id}: {e}")
        logging.exception("Full traceback:")

def log_processing_stats():
    """Log current processing statistics"""
    total = processing_stats['total_processed']
    successful = processing_stats['successful']
    failed = processing_stats['failed']
    success_rate = (successful / total * 100) if total > 0 else 0
    
    logging.info(f"Processing Statistics: Total: {total}, "
                f"Successful: {successful}, "
                f"Failed: {failed}, "
                f"Success Rate: {success_rate:.1f}%")

async def main():
    """
    Main function for direct execution with command line arguments.
    Used for single message processing (legacy mode).
    """
    logging.info("Starting audio chunking and summarization process...")
    if len(sys.argv) < 2:
        logging.error("No event data received in command-line arguments.")
        return {
            'statusCode': 500,
            'body': "Error: No event data received in command-line arguments."
        }

    # The first argument after the script name is the JSON payload
    event_data = sys.argv[1]
    logging.info(f"Received event data: {event_data}")

    if not event_data:
        logging.error("No event data received.")
        return {
            'statusCode': 500,
            'body': "Error: No event data received."
        }

    # Parse the JSON message
    try:
        event = json.loads(event_data)
        meta_data_idx = event["id"]
        force_video_chunking = bool(event.get("force_video_chunk_process", True))
        force_video_quoting = bool(event.get("force_video_quoting_process", True))

    except KeyError as e:
        print(f"Missing key in event data: {e}")
        return {
            'statusCode': 500,
            'body': f"Error: {str(e)}"
        }
    
    try:
        logging.info(f"Processing video artifacts for ID: {meta_data_idx}")
        
        # Create a message body and process using the same logic as SQS
        message_body = json.dumps({
            'id': meta_data_idx,
            'force_video_chunking': force_video_chunking,   
            'force_video_quoting': force_video_quoting,
        })
        
        result = await process_sqs_message(message_body, f"direct-{meta_data_idx}")
        
        if result.get('success'):
            logging.info(f"Video processing completed for {meta_data_idx}")
            return {
                'statusCode': 200,
                'body': json.dumps(result)
            }
        else:
            logging.error(f"Video processing failed for {meta_data_idx}: {result.get('error')}")
            return {
                'statusCode': 500,
                'body': json.dumps(result)
            }

    except Exception as e:
        print(f"Error processing video: {e}")
        return {
            'statusCode': 500,
            'body': f"Error: {str(e)}"
        }

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1].startswith('{'):
        asyncio.run(main())
    else:
        asyncio.run(poll_and_process_sqs_messages())
