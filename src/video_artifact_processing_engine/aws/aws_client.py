"""
AWS client configuration utilities for production environments.
"""

import os
import boto3
from botocore.config import Config
from botocore.exceptions import EndpointConnectionError, ConnectionError as BotocoreConnectionError
import time
from typing import Any
from video_artifact_processing_engine.utils.logging_config import setup_custom_logger

logger = setup_custom_logger(__name__)

# Import configuration
from ..config import config



def get_aws_config(service_name: str | None = None) -> dict[str, Any]:
    """Get AWS configuration for production."""
    # Use general AWS region for all services
    region = config.general_aws_region
    
    base_config = {
        'region_name': region,
        'config': Config(
            retries={
                'max_attempts': 5,
                'mode': 'adaptive'
            },
            max_pool_connections=10,
            connect_timeout=60,
            read_timeout=60
        )
    }

    return base_config


def create_aws_client_with_retries(service_name: str, **kwargs) -> Any:
    """Create AWS client with error handling and retries."""
    aws_config = get_aws_config(service_name)
    
    # Use default AWS credentials
    aws_access_key_id = config.aws_access_key_id
    aws_secret_access_key = config.aws_secret_access_key
    
    if aws_access_key_id and aws_secret_access_key:
        session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_config.get('region_name')
        )
        logger.info(f"Using AWS credentials from environment variables for {service_name}")
    else:
        session = boto3.Session(region_name=aws_config.get('region_name'))
        logger.info(f"Using default AWS credential chain for {service_name}")
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Prepare client/resource arguments
            client_args = {'config': aws_config['config']}
            
            if service_name == 's3-resource':
                return session.resource('s3', config=aws_config['config'])  # type: ignore
            else:  # s3, sqs, etc.
                return session.client(service_name, config=aws_config['config'])  # type: ignore
                
        except (EndpointConnectionError, BotocoreConnectionError) as e:
            logger.warning(f"Attempt {attempt + 1}/{max_retries} failed for {service_name}: {e}")
            
            if attempt == max_retries - 1:
                logger.error(f"Failed to create {service_name} client after {max_retries} attempts")
                raise
            time.sleep(2 ** attempt)  # Exponential backoff
        except Exception as e:
            logger.error(f"Unexpected error creating {service_name} client: {e}")
            raise


def get_sqs_client() -> Any:
    """Get SQS client configured for production."""
    return create_aws_client_with_retries('sqs')


def get_s3_client() -> Any:
    """Get S3 client configured for production."""
    return create_aws_client_with_retries('s3')


def get_s3_resource() -> Any:
    """Get S3 resource configured for production."""
    return create_aws_client_with_retries('s3-resource')


def validate_aws_credentials():
    """Validate AWS credentials and return status information."""
    try:
        # Check configuration for AWS credentials
        aws_access_key_id = config.aws_access_key_id
        aws_secret_access_key = config.aws_secret_access_key
        aws_region = config.general_aws_region
        
        credential_sources = []
        
        if aws_access_key_id and aws_secret_access_key:
            credential_sources.append("Environment variables")
            logger.info("Found AWS credentials in environment variables")
        
        # Try to create a simple client to test credentials
        try:
            session = boto3.Session()
            sts_client = session.client('sts', region_name=aws_region or 'us-east-1')
            identity = sts_client.get_caller_identity()
            
            credential_sources.append("AWS credential chain (successful)")
            logger.info(f"AWS credentials validated. Account: {identity.get('Account')}, ARN: {identity.get('Arn')}")
            
            return {
                'valid': True,
                'account': identity.get('Account'),
                'arn': identity.get('Arn'),
                'region': aws_region or 'us-east-1',
                'sources': credential_sources
            }
            
        except Exception as e:
            logger.error(f"Failed to validate AWS credentials: {e}")
            return {
                'valid': False,
                'error': str(e),
                'region': aws_region or 'us-east-1',
                'sources': credential_sources
            }
            
    except Exception as e:
        logger.error(f"Error checking AWS credentials: {e}")
        return {
            'valid': False,
            'error': str(e),
            'sources': []
        }
