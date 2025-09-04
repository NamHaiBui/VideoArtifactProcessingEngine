"""
SQS message handling and polling functionality.
"""

import json
import time
from typing import Dict, List, Optional
from dataclasses import dataclass

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from video_artifact_processing_engine.config import Config
from video_artifact_processing_engine.utils.logging_config import setup_custom_logger

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
        empty_poll_count = 0  # Track consecutive empty polls
        logger.info("Starting SQS polling...")
        
        while self.is_running:
            try:
                messages = self.receive_messages(max_messages)
                if not messages:
                    logger.debug("No messages received, continuing polling...")
                    time.sleep(20)  # Wait before retrying
                    empty_poll_count += 1
                    if empty_poll_count >= 3:
                        logger.info("Stopping polling after 3 consecutive empty polls.")
                        self.stop_polling()
                        break
                    continue
                
                # Reset empty poll count on receiving messages
                empty_poll_count = 0
                
                # Process messages in batch
                await self._process_message_batch(messages, message_handler)

            except KeyboardInterrupt:
                logger.info("Received interrupt signal, stopping polling...")
                self.stop_polling()
                break
            except Exception as e:
                logger.error(f"Error in polling loop: {e}")
                time.sleep(20)  # Wait before retrying
    
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
                parsed_message = msg_data['parsed_message']
                receipt_handle = msg_data['receipt_handle']
                message_body = msg_data['message_body']
                
                try:
                    logger.info(f"Processing message: {parsed_message.id}")
                    success = await message_handler(parsed_message)
                    
                    if success == 'Success':
                        logger.info(f"Successfully processed message: {parsed_message.id}")
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
    
    def stop_polling(self):
        """Stop the polling loop."""
        self.is_running = False
        logger.info("SQS polling stopped")
    
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
