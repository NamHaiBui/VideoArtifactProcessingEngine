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
            id=str(data.get('id', '')),
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
        except NoCredentialsError:
            logger.error("AWS credentials not found. Please configure AWS credentials.")
            raise
        
        self.queue_url = self.config.queue_url
        self.is_running = False
        
        logger.info(f"SQS Poller initialized for queue: {self.queue_url}")
    
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
            if 'id' not in data:
                logger.error("Message missing required 'id' field")
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
                VisibilityTimeoutSeconds=self.config.sqs_visibility_timeout_seconds
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
    
    def send_message_to_dlq(self, message_body: str, error_details: str) -> bool:
        """
        Send failed message to Dead Letter Queue.
        
        Args:
            message_body: Original message body
            error_details: Error details for debugging
            
        Returns:
            True if successful, False otherwise
        """
        if not self.config.sqs_dlq_url:
            logger.warning("No DLQ configured, skipping DLQ send")
            return False
        
        try:
            dlq_message = {
                'original_message': message_body,
                'error_details': error_details,
                'timestamp': time.time(),
                'processing_attempt_timestamp': time.time()
            }
            
            self.sqs.send_message(
                QueueUrl=self.config.sqs_dlq_url,
                MessageBody=json.dumps(dlq_message)
            )
            
            logger.info("Message sent to DLQ")
            return True
            
        except ClientError as e:
            logger.error(f"Error sending message to DLQ: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending to DLQ: {e}")
            return False
    
    def start_polling(self, message_handler, max_messages: int = 10):
        """
        Start polling for messages.
        
        Args:
            message_handler: Function to handle processed messages
            max_messages: Maximum messages to receive per poll
        """
        self.is_running = True
        logger.info("Starting SQS polling...")
        
        while self.is_running:
            try:
                messages = self.receive_messages(max_messages)
                
                if not messages:
                    logger.debug("No messages received, continuing polling...")
                    continue
                
                for message in messages:
                    receipt_handle = message['ReceiptHandle']
                    message_body = message['Body']
                    
                    # Validate message
                    parsed_message = self.validate_message(message_body)
                    
                    if not parsed_message:
                        logger.error("Invalid message, sending to DLQ")
                        self.send_message_to_dlq(message_body, "Invalid message format")
                        self.delete_message(receipt_handle)
                        continue
                    
                    # Process message
                    try:
                        logger.info(f"Processing message: {parsed_message.id}")
                        success = message_handler(parsed_message)
                        
                        if success:
                            # Delete message on successful processing
                            self.delete_message(receipt_handle)
                            logger.info(f"Successfully processed message: {parsed_message.id}")
                        else:
                            # Let message timeout and return to queue for retry
                            logger.error(f"Message processing failed: {parsed_message.id}")
                            self.send_message_to_dlq(
                                message_body, 
                                "Message processing returned False"
                            )
                    
                    except Exception as e:
                        logger.error(f"Error processing message {parsed_message.id}: {e}")
                        self.send_message_to_dlq(message_body, str(e))
                        self.delete_message(receipt_handle)
                
            except KeyboardInterrupt:
                logger.info("Received interrupt signal, stopping polling...")
                self.stop_polling()
                break
            except Exception as e:
                logger.error(f"Error in polling loop: {e}")
                time.sleep(5)  # Wait before retrying
    
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
