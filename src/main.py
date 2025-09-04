import asyncio
from datetime import datetime
import traceback
import boto3
import json
import os
import sys
import time
import uuid
import random
import signal
import atexit
from contextlib import asynccontextmanager
from datetime import datetime

from video_artifact_processing_engine.aws.aws_client import get_sqs_client, validate_aws_credentials, create_aws_client_with_retries
from video_artifact_processing_engine.aws.db_operations import get_episode_by_id, get_episode_processing_status, get_quotes_by_episode_id, get_shorts_by_episode_id, update_episode_item, get_quotes_and_shorts_by_episode_id, update_episode_processing_flags
from video_artifact_processing_engine.aws.ecs_task_protection import get_task_protection_manager, shutdown_task_protection_manager
from video_artifact_processing_engine.tools.video_artifacts_cutting_process import process_video_artifacts_unified
from video_artifact_processing_engine.utils import parse_s3_url
from video_artifact_processing_engine.utils.logging_config import setup_custom_logger
from video_artifact_processing_engine.config import config
from video_artifact_processing_engine.sqs_handler import SQSPoller, VideoProcessingMessage

# Set up logging
logging = setup_custom_logger(__name__)
cloudwatch_client = create_aws_client_with_retries('cloudwatch')

CLEANUP_TEMP_FILES = os.environ.get('CLEANUP_TEMP_FILES', 'true').lower() == 'true'

# Global shutdown flag for graceful shutdown
shutdown_requested = False
voluntary_shutdown_requested = False  # New flag for voluntary shutdown
current_processing_session = None

# Application state management for shutdown control
class ApplicationState:
    """Manages application state and controls shutdown behavior"""
    
    def __init__(self):
        self.startup_complete = False
        self.shutdown_methods_enabled = True
        self.external_shutdown_blocked = True
        self.startup_time = time.time()
        
    def complete_startup(self):
        """Mark startup as complete and log state"""
        self.startup_complete = True
        startup_duration = time.time() - self.startup_time
        logging.info(f"Application startup completed in {startup_duration:.2f}s")
        logging.info("Application now in protected mode - external shutdown requests will be ignored")
    
    def is_external_shutdown_allowed(self):
        """Check if external shutdown is allowed (always False for this application)"""
        return not self.external_shutdown_blocked
    
    def request_shutdown(self, method="unknown"):
        """Request shutdown with logging"""
        if method == "voluntary":
            logging.info(f"Shutdown requested via {method} method - APPROVED")
            return True
        else:
            logging.error(f"Shutdown requested via {method} method - BLOCKED")
            logging.error("Only voluntary shutdown methods are permitted")
            return False

# Initialize application state
app_state = ApplicationState()

# Initialize ECS task protection manager
task_protection_manager = get_task_protection_manager()

# Log protection manager status
protection_status = task_protection_manager.get_protection_status()
if protection_status['ecs_available']:
    logging.info("ECS Task Protection Manager initialized successfully")
    logging.info(f"- Cluster: {task_protection_manager.cluster_name}")
    logging.info(f"- Protection enabled: {protection_status['protection_enabled']}")
    logging.info(f"- Critical sessions: {protection_status['critical_sessions_count']}")
    if protection_status['protection_enabled']:
        logging.warning("Baseline protection is ACTIVE - external termination requests will be blocked")
else:
    logging.warning("ECS Task Protection not available - running outside ECS environment")
    logging.warning("Application cannot protect against external termination requests")

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
        self.is_critical = False  # Flag for ECS task protection
        
    def add_temp_file(self, filepath):
        """Add a temporary file to cleanup list"""
        self.temp_files.append(filepath)
        
    def set_result(self, key, value):
        """Store a result"""
        self.results[key] = value
        
    def get_result(self, key, default=None):
        """Get a result"""
        return self.results.get(key, default)
    
    def set_critical(self, is_critical=True):
        """Mark session as critical to prevent ECS task termination"""
        global task_protection_manager
        
        if self.is_critical == is_critical:
            # No change needed
            return
            
        self.is_critical = is_critical
        if is_critical:
            self.logger.info(f"Session {self.session_id} marked as CRITICAL - ECS task protection enabled")
            task_protection_manager.add_critical_session(self.session_id)
            
            # Log current protection status
            status = task_protection_manager.get_protection_status()
            self.logger.info(f"Total critical sessions now: {status['critical_sessions_count']}")
            if status['ecs_available'] and status['protection_enabled']:
                self.logger.info("ECS task protection is active and will prevent termination")
                if status.get('gap_protection_safe'):
                    self.logger.info(f"Gap protection: SAFE (margin: {status.get('gap_safety_margin_seconds', 0)}s)")
                else:
                    self.logger.warning(f"Gap protection: AT RISK (margin: {status.get('gap_safety_margin_seconds', 0)}s)")
            elif status['ecs_available']:
                self.logger.info("ECS task protection will be enabled shortly")
            else:
                self.logger.warning("ECS task protection not available - running outside ECS")
        else:
            self.logger.info(f"Session {self.session_id} no longer critical - removing ECS task protection")
            task_protection_manager.remove_critical_session(self.session_id)
            
            # Log remaining critical sessions
            status = task_protection_manager.get_protection_status()
            self.logger.info(f"Remaining critical sessions: {status['critical_sessions_count']}")
            if status['critical_sessions_count'] == 0:
                self.logger.info("No critical sessions remaining - ECS task protection will be disabled")
        
    def cleanup(self):
        """Cleanup temporary files and resources"""
        global task_protection_manager
        
        # Ensure task protection is removed for this session
        if self.is_critical:
            task_protection_manager.remove_critical_session(self.session_id)
            self.is_critical = False
        
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

def voluntary_shutdown_handler(signum, frame):
    """Handle voluntary shutdown signal (SIGUSR1) - ONLY accepted shutdown method"""
    global app_state
    
    signal_name = signal.Signals(signum).name
    logging.info(f"Received {signal_name} signal - voluntary shutdown requested")
    
    if app_state.request_shutdown("voluntary"):
        logging.info("Voluntary shutdown request APPROVED - initiating graceful shutdown")
        request_voluntary_shutdown()
    else:
        logging.error("Voluntary shutdown request DENIED - this should not happen")
        logging.error("Check application state and shutdown policy configuration")

def signal_handler(signum, frame):
    """Handle shutdown signals - COMPLETELY IGNORE all external shutdown requests"""
    global shutdown_requested, current_processing_session, task_protection_manager, app_state
    
    signal_name = signal.Signals(signum).name
    
    # Check if external shutdown is allowed (it never is for this application)
    if not app_state.request_shutdown(f"external_signal_{signal_name}"):
        logging.error(f"EXTERNAL SHUTDOWN SIGNAL COMPLETELY BLOCKED: {signal_name}")
        logging.error("This application has external shutdown protection enabled")
        logging.error("Signal ignored to maintain application and data integrity")
        
        # Get current protection status for logging
        protection_status = task_protection_manager.get_protection_status()
        
        if protection_status['ecs_available']:
            if protection_status['protection_enabled']:
                logging.error("ECS task protection is ACTIVE - AWS will block termination attempts")
                logging.info(f"Protection active for: {protection_status.get('protection_duration_seconds', 0):.1f}s")
            else:
                logging.error("ECS task protection should be active - enabling emergency protection")
                task_protection_manager.add_critical_session("emergency_protection")
                logging.warning("Emergency protection enabled")
        
        # Log current critical sessions
        if protection_status['critical_sessions_count'] > 0:
            logging.error(f"Active critical sessions: {protection_status['critical_sessions']}")
        else:
            logging.error("No active critical sessions, but external shutdown still blocked by policy")
        
        logging.error(f"SIGNAL {signal_name} COMPLETELY IGNORED - Use SIGUSR1 for voluntary shutdown")
        return
    
    # This code should never be reached due to application policy
    logging.critical(f"UNEXPECTED: External shutdown signal {signal_name} was not blocked!")
    logging.critical("This indicates a bug in the shutdown protection logic")
    return

def request_voluntary_shutdown():
    """
    Request voluntary shutdown of the application.
    This removes baseline protection and allows clean shutdown.
    """
    global voluntary_shutdown_requested, shutdown_requested, task_protection_manager
    
    logging.info("Voluntary shutdown requested by application")
    voluntary_shutdown_requested = True
    shutdown_requested = True
    
    # Remove baseline protection to allow shutdown
    task_protection_manager.request_voluntary_shutdown()
    
    logging.info("Voluntary shutdown initiated - will complete after current critical sessions finish")

def cleanup_on_exit():
    """Cleanup function called on application exit"""
    global task_protection_manager
    logging.info("Application shutting down - cleaning up resources...")
    
    # Get final protection status
    protection_status = task_protection_manager.get_protection_status()
    
    if protection_status['critical_sessions_count'] > 0:
        logging.warning(f"Application exit with {protection_status['critical_sessions_count']} active critical sessions")
        logging.warning(f"Active sessions: {protection_status['critical_sessions']}")
        logging.warning("This is a controlled shutdown - forcing task protection disable")
        
        # Force disable protection for controlled shutdown
        task_protection_manager.force_disable_protection("Controlled application shutdown")
    else:
        logging.info("Application exit with no active critical sessions - clean shutdown")
    
    # Shutdown the task protection manager
    shutdown_task_protection_manager()
    logging.info("Task protection cleanup complete")

# Register signal handlers for graceful shutdown with enhanced protection
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

# Also handle other potential shutdown signals
signal.signal(signal.SIGHUP, signal_handler)   # Hangup signal
signal.signal(signal.SIGQUIT, signal_handler)  # Quit signal

# Register voluntary shutdown signal
signal.signal(signal.SIGUSR1, voluntary_shutdown_handler)  # User signal 1 for voluntary shutdown

# Log signal handler registration
logging.info("Enhanced signal handlers registered - EXTERNAL SIGNALS COMPLETELY IGNORED:")
logging.info("- SIGTERM: Termination request (COMPLETELY IGNORED)")
logging.info("- SIGINT: Interrupt signal (Ctrl+C) (COMPLETELY IGNORED)")
logging.info("- SIGHUP: Hangup signal (COMPLETELY IGNORED)")
logging.info("- SIGQUIT: Quit signal (COMPLETELY IGNORED)")
logging.info("- SIGUSR1: Voluntary shutdown signal (ONLY accepted shutdown method)")
logging.info("IMPORTANT: This task will ONLY shutdown on self-invocation via SIGUSR1 or application completion")
logging.info("All external shutdown attempts will be completely ignored to prevent data corruption")

# Register cleanup function for application exit
atexit.register(cleanup_on_exit)

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

def validate_db_updates(episode_id, quotes, chunks, video_quote_paths, video_chunk_paths, marker, logger):
    """
    Independent validation: re-fetch Postgres state to ensure each processed item is updated
    with expected data before flipping processingInfo flags to true.

    Returns: (quotes_ok: bool, chunks_ok: bool)
    """
    quotes_ok, chunks_ok = True, True

    # Build quick lookup for expected URLs if provided in processing results
    expected_quote_urls = {}
    for item in (video_quote_paths or []):
        qid = item.get('quote_id') or item.get('id') or item.get('quoteId')
        url = item.get('hls_url') or item.get('url')
        if qid and url:
            expected_quote_urls[qid] = url
    expected_chunk_urls = {}
    for item in (video_chunk_paths or []):
        cid = item.get('chunk_id') or item.get('id') or item.get('chunkId')
        url = item.get('hls_url') or item.get('url')
        if cid and url:
            expected_chunk_urls[cid] = url

    # Get a consistent snapshot of quotes and shorts
    try:
        snapshot = get_quotes_and_shorts_by_episode_id(episode_id)
    except Exception as ve:
        logger.error(f"Error fetching consistent snapshot for validation: {ve}")
        return False, False

    # Validate quotes
    if quotes:
        try:
            latest_quotes = snapshot.get('quotes', [])
            latest_map = {q.quote_id: q for q in latest_quotes}
            expected_ids = {q.quote_id for q in quotes}
            # Count sanity check
            if len(video_quote_paths) != len(expected_ids):
                quotes_ok = False
                logger.warning(
                    f"Quote output count mismatch vs expected IDs: outputs={len(video_quote_paths)}, expected={len(expected_ids)}"
                )
            missing_or_invalid = []
            for qid in expected_ids:
                q = latest_map.get(qid)
                stale = False
                if q and marker is not None and q.updated_at is not None:
                    try:
                        stale = q.updated_at < marker
                    except Exception:
                        stale = False
                url_match = True
                if q and expected_quote_urls:
                    expected_url = expected_quote_urls.get(qid)
                    if expected_url:
                        url_match = (q.quote_audio_url == expected_url)
                if (not q
                    or not q.quote_audio_url
                    or not url_match
                    or str(q.content_type).lower() != 'video'
                    or q.updated_at is None
                    or stale):
                    missing_or_invalid.append(qid)
            if missing_or_invalid:
                quotes_ok = False
                logger.warning(f"Quote validation failed for {len(missing_or_invalid)} items: {missing_or_invalid[:5]}{'...' if len(missing_or_invalid) > 5 else ''}")
        except Exception as ve:
            quotes_ok = False
            logger.error(f"Error validating quotes in DB: {ve}")

    # Validate chunks (shorts)
    if chunks:
        try:
            latest_chunks = snapshot.get('shorts', [])
            latest_map_c = {c.chunk_id: c for c in latest_chunks}
            expected_cids = {c.chunk_id for c in chunks}
            if len(video_chunk_paths) != len(expected_cids):
                chunks_ok = False
                logger.warning(
                    f"Chunk output count mismatch vs expected IDs: outputs={len(video_chunk_paths)}, expected={len(expected_cids)}"
                )
            missing_or_invalid_c = []
            for cid in expected_cids:
                c = latest_map_c.get(cid)
                stale_c = False
                if c and marker is not None and c.updated_at is not None:
                    try:
                        stale_c = c.updated_at < marker
                    except Exception:
                        stale_c = False
                url_match_c = True
                if c and expected_chunk_urls:
                    expected_url_c = expected_chunk_urls.get(cid)
                    if expected_url_c:
                        url_match_c = (c.chunk_audio_url == expected_url_c)
                if (not c
                    or not c.chunk_audio_url
                    or not url_match_c
                    or str(c.content_type).lower() != 'video'
                    or c.updated_at is None
                    or stale_c):
                    missing_or_invalid_c.append(cid)
            if missing_or_invalid_c:
                chunks_ok = False
                logger.warning(f"Chunk validation failed for {len(missing_or_invalid_c)} items: {missing_or_invalid_c[:5]}{'...' if len(missing_or_invalid_c) > 5 else ''}")
        except Exception as ve:
            chunks_ok = False
            logger.error(f"Error validating chunks in DB: {ve}")

    return quotes_ok, chunks_ok

async def process_video_message(message: VideoProcessingMessage) -> str:
    """
    Process a VideoProcessingMessage from SQS.
    
    Args:
        message: VideoProcessingMessage instance
        
    Returns:
        str: 'Success', 'Failed', or 'NotReady'
    """
    global current_processing_session, shutdown_requested, voluntary_shutdown_requested
    
    # Create processing session for this message
    session = create_processing_session()
    current_processing_session = session
    logger = session.logger
    
    try:
        # Check for shutdown request at start - ONLY honor voluntary shutdown
        if voluntary_shutdown_requested:
            logger.info("Voluntary shutdown in progress. Skipping new message processing.")
            return 'Failed'
        
        # Ignore external shutdown requests - only process voluntary shutdown
        if shutdown_requested and not voluntary_shutdown_requested:
            logger.error("External shutdown detected but IGNORED - this should not happen!")
            logger.error("External shutdown flags should never be set in this application")
            logger.warning("Continuing with message processing to maintain data integrity")
            # Reset the flag since it should never be set from external sources
            shutdown_requested = False
        
        logger.info(f"Starting sequential processing for message: {message.id}")
        logger.debug(f"Message data: {message.to_dict()}")
        
        # Store message data in session
        session.set_result('message_data', message.to_dict())
        session.set_result('message_id', message.id)
        session.processing_status = 'parsing'
        
        # Extract metadata ID from the message
        meta_data_idx = message.id
        if not meta_data_idx:
            logger.error("No metadata ID found in message")
            session.processing_status = 'failed'
            return 'Failed'
        
        force_video_quoting = message.force_video_quotes
        force_video_chunking = message.force_video_chunking

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
            raise e
        
        if not episode_item or (episode_item.content_type and episode_item.content_type.lower() != 'video'):
            logger.warning(f"No episode found for ID: {episode_id}")
            logger.warning(f"Most Likely this is not a video episode.")
            session.processing_status = 'failed'
            return 'Success'
            
        # Check for chunking_status first - this applies to both chunks and quotes processing
        processing_info = get_episode_processing_status(episode_id)
        if not processing_info:
            logger.error(f"No chunking status found for episode: {episode_id}")
            session.processing_status = 'failed'
            return 'Failed'
        
        episode_title = episode_item.episode_title
        s3_http_link = episode_item.additional_data.get("videoLocation")
        logger.info(f"Episode title: {episode_item.additional_data}")
        podcast_title = str(episode_item.channel_name)
        if not s3_http_link:
            logger.error(f"No videoLocation found for episode {episode_id}")
            session.processing_status = 'failed'
            return 'Success'
        s3_parsed = parse_s3_url(s3_http_link)
        s3_key = s3_parsed['path'] if s3_parsed else None
        s3_video_bucket = s3_parsed['bucket'] if s3_parsed else None
        s3_video_key = s3_key + s3_parsed['filename'] if s3_parsed and s3_key else None
        if not s3_video_key:
            logger.error(f"No video key found for episode {episode_id}")
            session.processing_status = 'failed'
            return 'Failed'
        if s3_video_bucket and s3_video_bucket != config.video_bucket:
            logger.warning(f"Episode video bucket '{s3_video_bucket}' does not match configured video bucket '{config.video_bucket}'")
        logger.info(f"Parsed S3 video key: {s3_key}")
        
        if not podcast_title or not episode_title:
            logger.error(f"Missing required titles: podcast_title='{podcast_title}', episode_title='{episode_title}'")
            session.processing_status = 'failed'
            return 'Failed'
            
        if not s3_key:
            logger.error(f"No video file found for episode {episode_id}")
            session.processing_status = 'failed'
            return 'Failed'
        quotes = []
        if processing_info.get("quotingDone",None) and not processing_info.get("videoQuotingDone", False):
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
        if  processing_info.get("chunkingDone",None) and not processing_info.get("videoChunkingDone", False):
            # Retrieve chunks for processing
            all_chunks = get_shorts_by_episode_id(episode_id)
            chunks = [x for x in all_chunks if x.transcript and x.transcript.strip()]
            # Log chunk information for debugging
            if all_chunks:
                chunk_lengths = [x.chunk_length or 0 for x in all_chunks]
                logger.info(f"Chunk lengths: min={min(chunk_lengths)}, max={max(chunk_lengths)}, avg={sum(chunk_lengths)/len(chunk_lengths):.2f}")
                zero_length_count = sum(1 for x in chunk_lengths if x == 0.0)
                logger.info(f"Chunks with 0.0 length: {zero_length_count}/{len(all_chunks)}")
        if (processing_info.get("videoChunkingDone", False) and processing_info.get("videoQuotingDone", False)):
            logger.warning("Episodes are already processed")
            return 'Success'
        num_quotes = len(quotes)
        num_chunks = len(chunks)
        if not ((quotes or processing_info.get("videoQuotingDone", False)) and (chunks or processing_info.get("videoChunkingDone", False))):
            logger.warning("Quotes and chunks are not available at once, requeue message for later processing")
            return 'NotReady'
        logger.info(f"Processing episode {episode_id}, ({episode_title}) - {num_quotes} quotes, {num_chunks} chunks")
        logger.info(f"File naming: Podcast='{podcast_title}', Episode='{episode_title}'")
        logger.info(f"Video source: {s3_key}")
        logger.info(f"Chunking status: {processing_info} - proceeding with video processing")
        

        logger.info(f"Retrieved {len(quotes)} quotes and {len(chunks)} chunks for episode: {episode_id}")

        # Process Video Artifacts (both quotes and chunks) in unified way
        if quotes or chunks:
            logging.info(f"Starting unified video artifact processing...")
            
            # Mark session as critical for ECS task protection
            session.set_critical(True)
            
            should_process_quotes = quotes
            should_process_chunks = chunks
            

            try:
                # Capture a validation marker to detect concurrent updates and ensure our writes occurred after this point
                validation_marker = datetime.utcnow()
                session.set_result('validation_marker', validation_marker)
                results = await process_video_artifacts_unified(
                    episode_id=episode_id,
                    podcast_title=podcast_title,
                    episode_title=episode_title,
                    s3_video_key=s3_video_key,
                    s3_video_key_prefix=s3_key,
                    chunks_info=chunks,
                    quotes_info=quotes,
                    overwrite=True
                )
                
                video_quote_paths = results.get('quotes', [])
                video_chunk_paths = results.get('chunks', [])
                
                # Log results counts
                if should_process_quotes:
                    logging.info(f"Video quote processing completed: {len(video_quote_paths)} quotes")
                    if len(video_quote_paths) != num_quotes:
                        logging.warning(f"Quote count mismatch: {len(video_quote_paths)} != {num_quotes}")
                if should_process_chunks:
                    logger.info(f"Video chunking produced: {len(video_chunk_paths)} artifacts")

                # Independent validation: ensure DB has updated URLs/content types
                quotes_ok, chunks_ok = True, True
                if should_process_quotes:
                    try:
                        latest_quotes = get_quotes_by_episode_id(episode_id)
                        latest_map = {q.quote_id: q for q in latest_quotes}
                        expected_ids = {q.quote_id for q in quotes}
                        missing_or_stale = []
                        for qid in expected_ids:
                            q = latest_map.get(qid)
                            # Require our target fields updated and updated_at not older than marker if available
                            outdated = False
                            q_updated = getattr(q, 'updated_at', None) if q else None
                            if q_updated is not None and isinstance(q_updated, datetime) and isinstance(validation_marker, datetime):
                                outdated = q_updated < validation_marker
                            if (not q or not q.quote_audio_url or str(q.content_type).lower() != 'video' or outdated):
                                missing_or_stale.append(qid)
                        if missing_or_stale:
                            quotes_ok = False
                            logger.warning(f"Quote validation failed for {len(missing_or_stale)} items: {missing_or_stale[:5]}{'...' if len(missing_or_stale) > 5 else ''}")
                    except Exception as ve:
                        quotes_ok = False
                        logger.error(f"Error validating quotes in DB: {ve}")

                if should_process_chunks:
                    try:
                        def _is_valid_chunk(ch):
                            try:
                                if getattr(ch, 'is_removed_chunk', False):
                                    return False
                                start_ms = getattr(ch, 'start_ms', None)
                                end_ms = getattr(ch, 'end_ms', None)
                                if start_ms is not None and end_ms is not None:
                                    dur = (float(end_ms) - float(start_ms)) / 1000.0
                                else:
                                    dur = float(getattr(ch, 'chunk_length', 0) or 0)
                                return dur >= 1.0
                            except Exception:
                                return False

                        latest_chunks = get_shorts_by_episode_id(episode_id)
                        latest_map_c = {c.chunk_id: c for c in latest_chunks}
                        expected_valid_cids = {c.chunk_id for c in chunks if _is_valid_chunk(c)}

                        # Optional: log effective expected vs produced count
                        if expected_valid_cids and len(video_chunk_paths) != len(expected_valid_cids):
                            logger.warning(
                                f"Chunk count mismatch after excluding invalid chunks: produced={len(video_chunk_paths)} expected_valid={len(expected_valid_cids)}"
                            )

                        missing_or_stale_c = []
                        for cid in expected_valid_cids:
                            c = latest_map_c.get(cid)
                            outdated_c = False
                            c_updated = getattr(c, 'updated_at', None) if c else None
                            if c_updated is not None and isinstance(c_updated, datetime) and isinstance(validation_marker, datetime):
                                outdated_c = c_updated < validation_marker
                            if (not c or not c.chunk_audio_url or str(c.content_type).lower() != 'video' or outdated_c):
                                missing_or_stale_c.append(cid)
                        if missing_or_stale_c:
                            chunks_ok = False
                            logger.warning(f"Chunk validation failed for {len(missing_or_stale_c)} items: {missing_or_stale_c[:5]}{'...' if len(missing_or_stale_c) > 5 else ''}")
                    except Exception as ve:
                        chunks_ok = False
                        logger.error(f"Error validating chunks in DB: {ve}")

                # Decide based on validation results
                if (should_process_quotes and not quotes_ok) or (should_process_chunks and not chunks_ok):
                    # One-time retry with small randomized delay to account for eventual consistency/replica lag
                    jitter_s = random.uniform(0.2, 0.8)
                    logger.warning(
                        f"Independent validation failed. Retrying once after {jitter_s:.3f}s to handle eventual consistency."
                    )
                    await asyncio.sleep(jitter_s)

                    # Re-run validations with a fresh read
                    quotes_ok_retry, chunks_ok_retry = True, True
                    if should_process_quotes:
                        try:
                            latest_quotes = get_quotes_by_episode_id(episode_id)
                            latest_map = {q.quote_id: q for q in latest_quotes}
                            expected_ids = {q.quote_id for q in quotes}
                            missing_or_stale = []
                            for qid in expected_ids:
                                q = latest_map.get(qid)
                                outdated = False
                                q_updated = getattr(q, 'updated_at', None) if q else None
                                if q_updated is not None and isinstance(q_updated, datetime) and isinstance(validation_marker, datetime):
                                    outdated = q_updated < validation_marker
                                if (not q or not q.quote_audio_url or str(q.content_type).lower() != 'video' or outdated):
                                    missing_or_stale.append(qid)
                            if missing_or_stale:
                                quotes_ok_retry = False
                                logger.warning(
                                    f"Quote validation (retry) failed for {len(missing_or_stale)} items: {missing_or_stale[:5]}{'...' if len(missing_or_stale) > 5 else ''}"
                                )
                        except Exception as ve:
                            quotes_ok_retry = False
                            logger.error(f"Error validating quotes in DB (retry): {ve}")

                    if should_process_chunks:
                        try:
                            def _is_valid_chunk(ch):
                                try:
                                    if getattr(ch, 'is_removed_chunk', False):
                                        return False
                                    start_ms = getattr(ch, 'start_ms', None)
                                    end_ms = getattr(ch, 'end_ms', None)
                                    if start_ms is not None and end_ms is not None:
                                        dur = (float(end_ms) - float(start_ms)) / 1000.0
                                    else:
                                        dur = float(getattr(ch, 'chunk_length', 0) or 0)
                                    return dur >= 1.0
                                except Exception:
                                    return False

                            latest_chunks = get_shorts_by_episode_id(episode_id)
                            latest_map_c = {c.chunk_id: c for c in latest_chunks}
                            expected_valid_cids = {c.chunk_id for c in chunks if _is_valid_chunk(c)}

                            missing_or_stale_c = []
                            for cid in expected_valid_cids:
                                c = latest_map_c.get(cid)
                                outdated_c = False
                                c_updated = getattr(c, 'updated_at', None) if c else None
                                if c_updated is not None and isinstance(c_updated, datetime) and isinstance(validation_marker, datetime):
                                    outdated_c = c_updated < validation_marker
                                if (not c or not c.chunk_audio_url or str(c.content_type).lower() != 'video' or outdated_c):
                                    missing_or_stale_c.append(cid)
                            if missing_or_stale_c:
                                chunks_ok_retry = False
                                logger.warning(
                                    f"Chunk validation (retry) failed for {len(missing_or_stale_c)} items: {missing_or_stale_c[:5]}{'...' if len(missing_or_stale_c) > 5 else ''}"
                                )
                        except Exception as ve:
                            chunks_ok_retry = False
                            logger.error(f"Error validating chunks in DB (retry): {ve}")

                    if (should_process_quotes and not quotes_ok_retry) or (should_process_chunks and not chunks_ok_retry):
                        logger.warning("Independent validation failed after retry. Will requeue message and skip processingInfo update.")
                        session.set_critical(False)
                        return 'NotReady'

                # Only now update processingInfo flags and persist episode update
                if episode_item.processing_info is None:
                    episode_item.processing_info = {}
                if should_process_quotes:
                    episode_item.processing_info['videoQuotingDone'] = True
                if should_process_chunks:
                    episode_item.processing_info['videoChunkingDone'] = True
                episode_item.content_type = 'video'

                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        latest_processing_info = get_episode_processing_status(episode_id)
                        if not latest_processing_info:
                            logger.warning(f"No processing info found for episode {episode_id}")
                            latest_processing_info = {}
                        for key, value in latest_processing_info.items():
                            episode_item.processing_info[key] = value
                        result = update_episode_item(episode_item)
                        if result:
                            logger.info(f"Episode item updated successfully on attempt {attempt + 1}")
                        break
                    except Exception as update_error:
                        if attempt == max_retries - 1:
                            logger.error(f"Failed to update episode item after {max_retries} attempts: {update_error}")
                            logger.error(f"Update error traceback: {traceback.format_exc()}")
                            session.set_critical(False)  # Remove protection on failure
                            return 'Failed'
                        else:
                            logger.warning(f"Episode item update attempt {attempt + 1} failed: {update_error}. Retrying...")
                            await asyncio.sleep(1)  # Wait 1 second before retry
                            
            except Exception as e:
                if should_process_quotes:
                    logging.error(f"Video quote processing failed: {e}")
                if should_process_chunks:
                    logger.error(f"Video chunk processing failed: {e}")
                return 'Failed'
        else:
            logging.info(f"All video processing already completed, skipping...")

        # Store results in session
        session.set_result('quotes', quotes)
        session.set_result('chunks', chunks)

        logger.debug(f"Processing results - quotes: {len(quotes)}, chunks: {len(chunks)}")
        
        logger.info(f"Successfully processed video artifacts for {podcast_title}/{episode_title}")
        session.processing_status = 'success'
        
        # Update processing statistics
        processing_stats['total_processed'] += 1
        processing_stats['successful'] += 1
        
        # Log stats every 10 processed messages
        if processing_stats['total_processed'] % 10 == 0:
            log_processing_stats()
        
        return 'Success'
            
    except Exception as e:
        session.set_critical(False)  # Ensure protection is removed on failure
        logger.error(f"Error processing message: {e}")
        logger.exception("Full traceback:")
        session.processing_status = 'failed'
        
        # Update processing statistics
        processing_stats['total_processed'] += 1
        processing_stats['failed'] += 1
        
        return 'Failed'
    finally:
        # Cleanup and remove current session reference
        if current_processing_session == session:
            current_processing_session = None
        session.cleanup()


async def poll_and_process_sqs_messages():
    """
    Main loop to poll SQS messages and process them in batches using SQSPoller.
    Enhanced with complete protection against external termination.
    """
    global shutdown_requested, voluntary_shutdown_requested, task_protection_manager, app_state
    
    if not config.queue_url:
        logging.error("SQS_QUEUE_URL environment variable not set")
        return
    
    logging.info(f"Starting protected SQS polling on queue: {config.queue_url}")
    logging.info("EXTERNAL SHUTDOWN PROTECTION ACTIVE - only self-invoked shutdown permitted")
    
    # Initialize SQS poller
    poller = None
    try:
        poller = SQSPoller(config)
        
        # Test connection
        if not poller.health_check():
            logging.error("SQS health check failed")
            return
        
        logging.info("SQS poller initialized successfully")
        
        # Log initial protection status
        protection_status = task_protection_manager.get_protection_status()
        if protection_status['ecs_available']:
            logging.info(f"ECS task protection available and ready")
            if protection_status['protection_enabled']:
                logging.info("Baseline protection is active")
        else:
            logging.warning("ECS task protection not available - running outside ECS")
        
        # Mark startup as complete
        app_state.complete_startup()
        
        # Start polling with the new message handler
        await poller.start_polling(process_video_message, max_messages=1)
        
    except KeyboardInterrupt:
        logging.warning("Received KeyboardInterrupt (Ctrl+C) - treating as voluntary shutdown request")
        logging.info("Note: In production, use SIGUSR1 signal for clean voluntary shutdown")
        request_voluntary_shutdown()
        
        if poller:
            poller.stop_polling()
        
        # Wait for critical sessions to complete before final shutdown
        await _wait_for_critical_sessions_completion()
        
    except Exception as e:
        logging.error(f"Error in SQS polling: {e}")
        logging.exception("Full traceback:")
    finally:
        if poller:
            poller.stop_polling()
        
        # Final wait for any remaining critical sessions
        await _wait_for_critical_sessions_completion()
        
        logging.info("SQS polling shutdown complete")

async def _wait_for_critical_sessions_completion():
    """Wait for all critical processing sessions to complete before shutdown"""
    global task_protection_manager

    max_wait_time = 30  # 30 seconds maximum wait
    check_interval = 10   # Check every 10 seconds
    waited_time = 0
    
    while waited_time < max_wait_time:
        protection_status = task_protection_manager.get_protection_status()
        critical_count = protection_status['critical_sessions_count']
        
        if critical_count == 0:
            logging.info("All critical processing sessions completed. Safe to shutdown.")
            break
            
        logging.info(f"Waiting for {critical_count} critical sessions to complete...")
        logging.info(f"Active sessions: {protection_status['critical_sessions']}")
        logging.info(f"Waited {waited_time}s of maximum {max_wait_time}s")
        
        await asyncio.sleep(check_interval)
        waited_time += check_interval
    
    if waited_time >= max_wait_time:
        logging.warning(f"Maximum wait time ({max_wait_time}s) exceeded. Proceeding with shutdown.")
        logging.warning("Some critical sessions may still be active - this could cause data inconsistency.")
    else:
        logging.info(f"Critical sessions completed after {waited_time}s wait.")

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
        
        # Create a VideoProcessingMessage and process using the same logic as SQS
        message = VideoProcessingMessage(
            id=meta_data_idx,
            force_video_chunking=force_video_chunking,   
            force_video_quotes=force_video_quoting,
        )
        
        success = await process_video_message(message)

        if success == 'Success':
            logging.info(f"Video processing completed for {meta_data_idx}")
            return {
                'statusCode': 200,
                'body': json.dumps({'success': True, 'message': f'Processing completed for {meta_data_idx}'})
            }
        elif success == 'NotReady':
            logging.info(f"Video processing not ready for {meta_data_idx}, requeueing")
            
            return {
                'statusCode': 202,
                'body': json.dumps({'success': False, 'message': f'Processing not ready for {meta_data_idx}, requeueing'})
            }
        else:
            logging.error(f"Video processing failed for {meta_data_idx}")
            return {
                'statusCode': 500,
                'body': json.dumps({'success': False, 'error': f'Processing failed for {meta_data_idx}'})
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
