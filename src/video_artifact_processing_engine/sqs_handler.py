"""
SQS message handling and polling functionality.
"""

import json
import time
from typing import Dict, List, Optional
from dataclasses import dataclass
import asyncio
import os

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from video_artifact_processing_engine.config import Config
from video_artifact_processing_engine.utils.logging_config import setup_custom_logger
from video_artifact_processing_engine.aws.db_operations import (
    get_episode_processing_status,
    get_quotes_and_shorts_by_episode_id,
    update_episode_processing_flags,
)

logger = setup_custom_logger(__name__)

@dataclass
class VideoProcessingMessage:
    """Data class for video processing messages."""
    
    def __init__(self, id: str, force_video_chunking: bool = False, force_video_quotes: bool = False):
        self.id = id
        self.force_video_chunking = force_video_chunking
        self.force_video_quotes = force_video_quotes
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'VideoProcessingMessage':
        """Create instance from dictionary."""
        return cls(
            id=str(data.get('episodeId', '')),
            force_video_chunking=str(data.get('force_video_chunking', 'False')).lower() == 'true',
            force_video_quotes=str(data.get('force_video_quotes', 'False')).lower() == 'true',
        )
    
    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return {
            'id': self.id,
            'force_video_chunking': str(self.force_video_chunking),
            'force_video_quotes': str(self.force_video_quotes)
        }


class SQSPoller:
    """SQS polling service for video processing messages."""
    def __init__(self, config: Optional[Config] = None):
        """
        Initialize SQS poller.
        
        Args:
            config: Configuration instance
        """
        self.config = config or Config()
        
        try:
            self.sqs = boto3.client(
                'sqs',
                region_name=self.config.general_aws_region,
                aws_access_key_id=self.config.aws_access_key_id,
                aws_secret_access_key=self.config.aws_secret_access_key
            )
            self.cloudwatch = boto3.client(
                'cloudwatch',
                region_name=self.config.general_aws_region,
                aws_access_key_id=self.config.aws_access_key_id,
                aws_secret_access_key=self.config.aws_secret_access_key
            )
        except NoCredentialsError:
            logger.error("AWS credentials not found. Please configure AWS credentials.")
            raise 

        # Queue and local state
        self.queue_url = self.config.queue_url
        self.is_running = False
        # Drain flag: when set, polling loop stops fetching new messages after current batch / message
        self.draining = False  # When True, finish current in-flight message then exit loop
        # Local memory for NotReady counts per episodeId
        self.not_ready_counts = {}

        logger.info(f"SQS Poller initialized for queue: {self.queue_url}")

    def _increment_not_ready(self, episode_id: str) -> int:
        """Increment and return the NotReady count for an episode id."""
        current = self.not_ready_counts.get(episode_id, 0) + 1
        self.not_ready_counts[episode_id] = current
        return current

    def _reset_not_ready(self, episode_id: str) -> None:
        """Reset the NotReady count for an episode id after success or escalation."""
        if episode_id in self.not_ready_counts:
            del self.not_ready_counts[episode_id]

    def _emit_cloudwatch_alarm_metric(self, episode_id: str) -> None:
        """Emit a CloudWatch metric to trigger alarm for repeated NotReady events."""
        try:
            self.cloudwatch.put_metric_data(
                Namespace='VideoArtifactProcessingEngine/Alerts',
                MetricData=[
                    {
                        'MetricName': 'NotReadyCountExceeded',
                        'Dimensions': [
                            {'Name': 'EpisodeId', 'Value': episode_id},
                        ],
                        'Unit': 'Count',
                        'Value': 1.0,
                    }
                ]
            )
            logger.warning(f"Emitted CloudWatch alarm metric for EpisodeId={episode_id}")
        except Exception as e:
            logger.error(f"Failed to emit CloudWatch metric for EpisodeId={episode_id}: {e}")

    def _both_video_flags_done(self, episode_id: str) -> bool:
        """Return True only if both videoChunkingDone and videoQuotingDone are True in DB."""
        try:
            pi = get_episode_processing_status(episode_id) or {}
            return bool(pi.get('videoChunkingDone')) and bool(pi.get('videoQuotingDone'))
        except Exception as e:
            logger.warning(f"Failed to read processing status for {episode_id}: {e}")
            return False
    
    def validate_message(self, message_body: str) -> Optional[VideoProcessingMessage]:
        """
        Validate and parse SQS message.
        
        Args:
            message_body: Raw message body from SQS
            
        Returns:
            VideoProcessingMessage if valid, None otherwise
        """
        #TODO: If the validation fails, raise a Cloudwatch alarm and send to DLQ
        try:
            data = json.loads(message_body)
            
            # Validate required fields
            if 'episodeId' not in data:
                logger.error("Message missing required 'episodeId' field")
                return None
            
            message = VideoProcessingMessage.from_dict(data)
            logger.info(f"Parsed message: {message}")
            return message
            
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in message: {e}")
            return None
        except Exception as e:
            logger.error(f"Error parsing message: {e}")
            return None
    
    def receive_messages(self, max_messages: int = 10) -> List[Dict]:
        """
        Receive messages from SQS queue.
        
        Args:
            max_messages: Maximum number of messages to receive
            
        Returns:
            List of message dictionaries
        """
        try:
            response = self.sqs.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=self.config.sqs_wait_time_seconds,
                VisibilityTimeout=self.config.sqs_visibility_timeout_seconds,
            )
            
            messages = response.get('Messages', [])
            logger.info(f"Received {len(messages)} messages from SQS")
            return messages
            
        except ClientError as e:
            logger.error(f"Error receiving messages from SQS: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error receiving messages: {e}")
            return []
    
    def delete_message(self, receipt_handle: str) -> bool:
        """
        Delete processed message from SQS queue.
        
        Args:
            receipt_handle: Receipt handle of the message to delete
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.sqs.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle
            )
            logger.info("Message deleted from SQS")
            return True
            
        except ClientError as e:
            logger.error(f"Error deleting message from SQS: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error deleting message: {e}")
            return False
    
    def requeue_message(self, message_body: str):
        """
        Requeue a message by sending it back to the SQS queue.

        Args:
            message_body: The body of the message to requeue
        """
        try:
            # Send the message back to the queue
            self.sqs.send_message(
                QueueUrl=self.queue_url,
                MessageBody=message_body,
                DelaySeconds=60*3,
            )

            logger.info("Message requeued successfully")
        except Exception as e:
            logger.error(f"Error requeuing message: {e}")
    

    async def start_polling(self, message_handler, max_messages: int = 10):
        """
        Start polling for messages.
        
        Args:
            message_handler: Function to handle processed messages
            max_messages: Maximum messages to receive per poll
        """
        self.is_running = True
        logger.info("Starting SQS polling...")

        # Fargate-friendly idle behavior: do not exit on empty polls by default.
        # Use exponential backoff with jitter between polls when empty.
        # Allow opting into legacy stop-on-idle via env STOP_ON_IDLE=true.
        stop_on_idle = str(os.environ.get('STOP_ON_IDLE', 'false')).lower() in ('1', 'true', 'yes')
        backoff_base = max(1.0, float(os.environ.get('SQS_EMPTY_BACKOFF_BASE', '1')))
        backoff_max = max(backoff_base, float(os.environ.get('SQS_EMPTY_BACKOFF_MAX', '20')))
        backoff = backoff_base

        while self.is_running:
            # If draining requested, stop before pulling new work
            if self.draining:
                logger.info("Draining active - no further SQS receives; waiting for in-flight work (if any) to finish.")
                break
            try:
                messages = self.receive_messages(max_messages)
                if not messages:
                    # No messages: apply async backoff with jitter; don’t block the event loop
                    jitter = 0.25 * backoff
                    sleep_s = min(backoff_max, backoff + (jitter * (0.5)))
                    logger.debug(f"No messages; sleeping {sleep_s:.1f}s before next poll (backoff={backoff:.1f}s)")
                    await asyncio.sleep(sleep_s)
                    # Increase backoff exponentially up to max
                    backoff = min(backoff_max, backoff * 2.0)

                    # Optional legacy behavior to stop after prolonged idle
                    if stop_on_idle and backoff >= backoff_max:
                        logger.info("Idle backoff reached max and STOP_ON_IDLE=true; stopping polling.")
                        self.stop_polling()
                        break
                    continue

                # Reset backoff on activity
                backoff = backoff_base

                # Process messages in batch
                await self._process_message_batch(messages, message_handler)

                # If drain was requested during batch, exit after batch completes
                if self.draining:
                    logger.info("Draining engaged mid-batch - exiting polling loop after current batch.")
                    break

            except KeyboardInterrupt:
                logger.info("Received interrupt signal, stopping polling...")
                self.stop_polling()
                break
            except Exception as e:
                logger.error(f"Error in polling loop: {e}")
                # Async small pause before retry to avoid hot loop
                await asyncio.sleep(min(20.0, backoff))
    
    async def _process_message_batch(self, messages: List[Dict], message_handler):
        """
        Process a batch of messages.
        
        Args:
            messages: List of SQS message dictionaries
            message_handler: Function to handle processed messages
        """
        logger.info(f"Processing batch of {len(messages)} messages")
        
        # Validate all messages first
        valid_messages = []
        invalid_messages = []
        
        for message in messages:
            receipt_handle = message['ReceiptHandle']
            message_body = message['Body']
            
            parsed_message = self.validate_message(message_body)
            
            if parsed_message:
                valid_messages.append({
                    'parsed_message': parsed_message,
                    'receipt_handle': receipt_handle,
                    'message_body': message_body
                })
            else:
                invalid_messages.append({
                    'receipt_handle': receipt_handle,
                    'message_body': message_body
                })
        
        # Handle invalid messages
        for invalid_msg in invalid_messages:
            logger.error("Invalid message")
            self.delete_message(invalid_msg['receipt_handle'])
        
        # Process valid messages
        if valid_messages:
            logger.info(f"Processing {len(valid_messages)} valid messages")
            
            for msg_data in valid_messages:
                if self.draining:
                    logger.info("Drain flag set - skipping remaining messages in batch.")
                    break
                parsed_message = msg_data['parsed_message']
                receipt_handle = msg_data['receipt_handle']
                message_body = msg_data['message_body']
                
                try:
                    logger.info(f"Processing message: {parsed_message.id}")

                    # Start heartbeat to extend visibility while the handler runs
                    hb_task = asyncio.create_task(self._visibility_heartbeat(receipt_handle))
                    try:
                        success = await message_handler(parsed_message)
                    finally:
                        # Stop heartbeat as soon as handler finishes
                        hb_task.cancel()
                        try:
                            await hb_task
                        except asyncio.CancelledError:
                            pass
                    
                    if success == 'Success':
                        logger.info(f"Successfully processed message: {parsed_message.id}")
                        # Ensure flags are set if all corresponding items are already processed
                        flags_ok = await self._ensure_flags_after_success(parsed_message.id)
                        if not flags_ok:
                            logger.warning(f"Flags not set after success for {parsed_message.id}; requeuing message to retry flag update.")
                            # Requeue to retry flag update later; delete current message
                            self.delete_message(receipt_handle)
                            self.requeue_message(message_body)
                            continue
                        # Requeue unless BOTH final flags are true
                        if not self._both_video_flags_done(parsed_message.id):
                            logger.info(
                                f"Requeuing message {parsed_message.id} because not both videoChunkingDone and videoQuotingDone are true"
                            )
                            self.delete_message(receipt_handle)
                            self.requeue_message(message_body)
                            continue
                        # All done – safe to delete permanently
                        self.delete_message(receipt_handle)
                        # Reset NotReady counter on success
                        self._reset_not_ready(parsed_message.id)
                    elif success == 'NotReady':
                        count = self._increment_not_ready(parsed_message.id)
                        if count >= 3:
                            logger.warning(f"Message {parsed_message.id} not ready {count} times. Emitting alarm and deleting without requeue.")
                            self._emit_cloudwatch_alarm_metric(parsed_message.id)
                            self.delete_message(receipt_handle)
                            # Reset after escalation
                            self._reset_not_ready(parsed_message.id)
                        else:
                            logger.info(f"Message {parsed_message.id} not ready (count={count}), requeueing")
                            self.delete_message(receipt_handle)
                            self.requeue_message(message_body)
                    else:
                        # Let message timeout and return to queue for retry
                        logger.error(f"Message processing failed: {parsed_message.id}")
                
                except Exception as e:
                    logger.error(f"Error processing message {parsed_message.id}: {e}")

    def _is_valid_chunk(self, c) -> bool:
        try:
            if getattr(c, 'is_removed_chunk', False):
                return False
            start_ms = getattr(c, 'start_ms', None)
            end_ms = getattr(c, 'end_ms', None)
            if start_ms is not None and end_ms is not None:
                dur = (float(end_ms) - float(start_ms)) / 1000.0
            else:
                dur = float(getattr(c, 'chunk_length', 0) or 0)
            return dur >= 1.0
        except Exception:
            return False

    def _is_quote_processed(self, q) -> bool:
        try:
            return bool(getattr(q, 'quote_audio_url', None)) and str(getattr(q, 'content_type', '')).lower() == 'video'
        except Exception:
            return False

    def _is_chunk_processed(self, c) -> bool:
        try:
            return self._is_valid_chunk(c) and bool(getattr(c, 'chunk_audio_url', None)) and str(getattr(c, 'content_type', '')).lower() == 'video'
        except Exception:
            return False

    def _is_valid_quote(self, q) -> bool:
        try:
            if not getattr(q, 'quote', None) or not getattr(q, 'context', None):
                return False
            qlen = getattr(q, 'quote_length', None)
            if qlen is not None:
                dur = float(qlen or 0)
            else:
                start_ms = getattr(q, 'quote_start_ms', None)
                end_ms = getattr(q, 'quote_end_ms', None)
                if start_ms is not None and end_ms is not None:
                    dur = (float(end_ms) - float(start_ms)) / 1000.0
                else:
                    dur = 0.0
            return dur >= 1.0
        except Exception:
            return False

    async def _ensure_flags_after_success(self, episode_id: str) -> bool:
        """
        If all quotes/chunks are already processed in DB but flags are not updated,
        atomically set the flags before we delete the SQS message.
        Returns True if flags are already correct or successfully set; False otherwise.
        """
        try:
            pi = get_episode_processing_status(episode_id) or {}
            if bool(pi.get('videoQuotingDone')) and bool(pi.get('videoChunkingDone')):
                return True

            items = get_quotes_and_shorts_by_episode_id(episode_id)
            quotes = items.get('quotes', [])
            shorts = items.get('shorts', [])
            # Compute completion state per category
            # Only consider relevant items
            rel_quotes = [q for q in quotes if self._is_valid_quote(q)]
            rel_shorts = [s for s in shorts if self._is_valid_chunk(s)]
            all_quotes_done = all(self._is_quote_processed(q) for q in rel_quotes) if rel_quotes else True
            all_shorts_done = all(self._is_chunk_processed(s) for s in rel_shorts) if rel_shorts else True

            want_q = all_quotes_done and not bool(pi.get('videoQuotingDone'))
            want_c = all_shorts_done and not bool(pi.get('videoChunkingDone'))
            if not (want_q or want_c):
                return True

            max_retries = 3
            for attempt in range(max_retries):
                try:
                    res = update_episode_processing_flags(
                        episode_id,
                        video_quoting_done=True if want_q else None,
                        video_chunking_done=True if want_c else None,
                    )
                    # Re-read to verify
                    latest = get_episode_processing_status(episode_id) or {}
                    ok_q = (not want_q) or bool(latest.get('videoQuotingDone'))
                    ok_c = (not want_c) or bool(latest.get('videoChunkingDone'))
                    if res and ok_q and ok_c:
                        return True
                except Exception as e:
                    logger.warning(f"Flag set attempt {attempt+1} failed for episode {episode_id}: {e}")
                # small backoff
                await asyncio.sleep(0.5)
            return False
        except Exception as e:
            logger.error(f"_ensure_flags_after_success error for {episode_id}: {e}")
            return False

    async def _visibility_heartbeat(self, receipt_handle: str):
        """Periodically extend SQS message visibility timeout while work is in progress."""
        # Choose a conservative heartbeat interval: 1/3 of timeout, capped to 5 minutes minimum spacing
        timeout = max(30, int(self.config.sqs_visibility_timeout_seconds))
        interval = max(60, min(300, timeout // 3))
        try:
            while True:
                try:
                    # Extend visibility to full timeout window again
                    self.sqs.change_message_visibility(
                        QueueUrl=self.queue_url,
                        ReceiptHandle=receipt_handle,
                        VisibilityTimeout=timeout,
                    )
                    logger.debug("Extended message visibility timeout via heartbeat")
                except Exception as e:
                    logger.warning(f"Failed to extend message visibility: {e}")
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            # Best-effort final extension to give cleanup time (optional)
            try:
                self.sqs.change_message_visibility(
                    QueueUrl=self.queue_url,
                    ReceiptHandle=receipt_handle,
                    VisibilityTimeout=max(60, min(300, int(self.config.sqs_visibility_timeout_seconds))),
                )
            except Exception:
                pass
            raise
    
    def stop_polling(self):
        """Stop the polling loop."""
        self.is_running = False
        logger.info("SQS polling stopped")

    def initiate_drain(self, reason: str = "signal"):
        """Request graceful drain: finish current message, stop fetching new ones."""
        if not self.draining:
            self.draining = True
            logger.warning(f"SQS poller drain initiated (reason={reason})")
        # Ensure main loop will exit soon
        self.is_running = True  # keep True until loop notice; do not hard-stop to allow in-flight completion
    
    def health_check(self) -> bool:
        """
        Perform health check by testing SQS connectivity.
        
        Returns:
            True if healthy, False otherwise
        """
        try:
            # Test queue access
            self.sqs.get_queue_attributes(
                QueueUrl=self.queue_url,
                AttributeNames=['QueueArn']
            )
            return True
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False
