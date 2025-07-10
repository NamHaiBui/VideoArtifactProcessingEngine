import boto3
import tempfile

# from pydub import AudioSegment
from boto3.dynamodb.conditions import Key
# from pydub.utils import which
from typing import List
from datetime import datetime, timezone
from boto3.dynamodb.conditions import Key
from typing import List

from video_artifact_processing_engine.models.chunk_model import Chunk
from ..config import config
from ..utils.logging_config import setup_custom_logger
from ..models.quote_model import Quote
from .aws_client import get_dynamodb_resource
from botocore.exceptions import ClientError
logging = setup_custom_logger(__name__)


def get_all_quotes(episode_id: str, force_video_quotes: bool = False) -> List[Quote]:
    """
    Retrieve all quotes for a specific episode from DynamoDB using the new schema.
    Args:
        episode_id: The episode ID
    Returns list of Quote objects.
    """
    # Initialize DynamoDB resource
    dynamodb = get_dynamodb_resource()
    quotes_table = dynamodb.Table(config.quotes_table) # type: ignore

    try:
        logging.info(f"Retrieving quotes for episode ID: {episode_id}")
        last_evaluated_key = None
        items = []
        while True:
            if last_evaluated_key:
                response = quotes_table.query(
                    KeyConditionExpression=Key('PK').eq(f'EPISODE#{episode_id}'),
                    ExclusiveStartKey=last_evaluated_key,
                    ConsistentRead=True
                )
            else:
                response = quotes_table.query(
                    KeyConditionExpression=Key('PK').eq(f'EPISODE#{episode_id}')
                )
            items.extend(response['Items'])
            if 'LastEvaluatedKey' not in response:
                break
            last_evaluated_key = response['LastEvaluatedKey']
        
        # Helper function to safely extract SK value regardless of format
        def extract_sk_value(item):
            sk = item.get('SK', '')
            if isinstance(sk, dict):
                return sk.get('S', '')
            return str(sk) if sk else ''
        
        # Helper function to safely extract quoteVideoUrl value regardless of format
        def extract_quote_video_url(item):
            quote_video_url = item.get('quoteVideoUrl', '')
            if isinstance(quote_video_url, dict):
                return quote_video_url.get('S', '')
            return str(quote_video_url) if quote_video_url else ''

        # Filter items to only include full quotes (SK starts with DATE# and contains QUOTE#)
        # This excludes simplified quote records that have SK pattern "QUOTE#uuid#GENRE#uuid"
        quote_items = []
        simplified_quotes = []
        
        for item in items:
            sk_value = extract_sk_value(item)
            if sk_value.startswith('DATE#') and 'QUOTE#' in sk_value:
                quote_items.append(item)
            elif sk_value.startswith('QUOTE#') and '#GENRE#' in sk_value:
                simplified_quotes.append(item)
                
        logging.info(f"Retrieved {len(quote_items)} full quote items and filtered out {len(simplified_quotes)} simplified quote records from {len(items)} total items from DynamoDB")        
        
        # Convert raw DynamoDB items to QuoteModel objects
        quotes = []
        skipped_quotes = 0
        for item in quote_items:
            try:
                # Check if this quote already has a video URL by examining the full record
                # and any associated simplified records
                quote_has_video = False
                
                # Check the main quote record
                quote_video_url = extract_quote_video_url(item)
                if quote_video_url and quote_video_url.strip():
                    quote_has_video = True
                
                # If main record doesn't have video URL, check simplified records for this quote
                if not quote_has_video:
                    quote_sk = extract_sk_value(item)
                    if '#QUOTE#' in quote_sk:
                        quote_id = quote_sk.split('#QUOTE#')[1].split('#')[0] if '#QUOTE#' in quote_sk else ''
                        # Check if any simplified record for this quote has a video URL
                        for simplified_item in simplified_quotes:
                            simplified_sk = extract_sk_value(simplified_item)
                            if simplified_sk.startswith(f'QUOTE#{quote_id}#GENRE#'):
                                simplified_video_url = extract_quote_video_url(simplified_item)
                                if simplified_video_url and simplified_video_url.strip():
                                    quote_has_video = True
                                    break
                
                # Skip quotes that already have video URLs
                if quote_has_video and not force_video_quotes:
                    logging.debug(f"Skipping quote {extract_sk_value(item)} - already has video URL")
                    skipped_quotes += 1
                    continue

                # Create QuoteModel from raw DynamoDB item (no conversion needed)
                quote = Quote.from_dynamodb_item(item)
                quotes.append(quote)
            except Exception as e:
                logging.error(f"Error converting quote to QuoteModel: {e}")
                continue
        
        if skipped_quotes > 0:
            logging.info(f"Skipped {skipped_quotes} quotes that already have video URLs")
        
        logging.info(f"Successfully converted {len(quotes)} quotes to Quote objects")
        return quotes
    
    except Exception as e:
        logging.error(f"Error retrieving quotes: {e}")
        return []
    
def get_quotes_video_status(PK, SK):
    """Get current quotes video status from metadata table"""
    try:
        dynamodb = get_dynamodb_resource()
        metadata_table = dynamodb.Table(config.podcast_metadata_table) #type:ignore
        response = metadata_table.get_item(
            Key={
                'PK': PK,
                'SK': SK
            }
        )
        if 'Item' in response:
            return response['Item'].get('quoteVideoStatus', 'PENDING')
        return 'PENDING'
    except Exception as e:
        logging.error(f"Error getting quotes video status: {e}")
        return 'PENDING'
    

def update_quote_video_status(PK, SK, status):
    """Update the quote status in the metadata table using new schema."""
    if not PK or not SK or not status:
        raise ValueError("PK, SK, and status cannot be empty")
    dynamodb = get_dynamodb_resource()
    try:
        metadata_table = dynamodb.Table(config.podcast_metadata_table)
        response = metadata_table.update_item(
            Key={
                'PK': PK,
                'SK': SK
            },
            UpdateExpression='SET quoteVideoStatus = :status, updatedAt = :updated_at',
            ExpressionAttributeValues={
                ':status': status,
                ':updated_at': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
            },
            ReturnValues='UPDATED_NEW'
        )
        logging.info(f"Updated quoteVideoStatus to '{status}' for {PK}/{SK}")
        return response
    except ClientError as e:
        logging.error(f"Error updating quote video status: {e}")
        raise

def get_all_chunks(episode_id: str, force_video_chunking: bool = False, num_chunks: int | None = None) -> List[Chunk]:
    """
    Retrieve all chunks for a specific episode from DynamoDB using the new schema.
    Args:
        episode_id: The episode ID
        force_video_chunking: Whether to force video chunking
        num_chunks: Expected number of chunks (optional, for validation only)
    Returns list of ChunkModel objects.
    """
    # Initialize DynamoDB resource
    dynamodb = get_dynamodb_resource()
    chunk_table = dynamodb.Table(config.chunk_table) # type: ignore

    try:
        logging.info(f"Retrieving chunks for episode ID: {episode_id}")
        last_evaluated_key = None
        items = []
        while True:
            if last_evaluated_key:
                response = chunk_table.query(
                    KeyConditionExpression=Key('PK').eq(f'EPISODE#{episode_id}'),
                    ExclusiveStartKey=last_evaluated_key,
                    ConsistentRead=True
                )
            else:
                response = chunk_table.query(
                    KeyConditionExpression=Key('PK').eq(f'EPISODE#{episode_id}'),
                    ConsistentRead=True
                )

            items.extend(response['Items'])

            if 'LastEvaluatedKey' not in response:
                break
            last_evaluated_key = response['LastEvaluatedKey']

        # Helper function to safely extract SK value regardless of format
        def extract_sk_value(item):
            sk = item.get('SK', '')
            if isinstance(sk, dict):
                return sk.get('S', '')
            return str(sk) if sk else ''
        
        # Helper function to safely extract chunkVideoUrl value regardless of format
        def extract_chunk_video_url(item):
            chunk_video_url = item.get('chunkVideoUrl', '')
            if isinstance(chunk_video_url, dict):
                return chunk_video_url.get('S', '')
            return str(chunk_video_url) if chunk_video_url else ''

        # Filter items to only include full chunks (SK starts with DATE# and contains CHUNK#)
        # This excludes simplified chunk records that have SK pattern "CHUNK#uuid#GENRE#uuid"
        full_chunk_items = []
        simplified_chunks = []
        
        for item in items:
            sk_value = extract_sk_value(item)
            if sk_value.startswith('DATE#') and 'CHUNK#' in sk_value:
                full_chunk_items.append(item)
            elif sk_value.startswith('CHUNK#') and '#GENRE#' in sk_value:
                simplified_chunks.append(item)
        
        logging.info(f"Retrieved {len(full_chunk_items)} full chunk items and filtered out {len(simplified_chunks)} simplified chunk records from {len(items)} total items from DynamoDB")        
        
        # Convert raw DynamoDB items to ChunkModel objects
        chunks = []
        skipped_chunks = 0
        for item in full_chunk_items:
            logging.debug(f"Processing chunk item: {item}")
            try:
                # Check if this chunk already has a video URL by examining the full record
                # and any associated simplified records
                chunk_has_video = False
                
                # Check the main chunk record
                if extract_chunk_video_url(item):
                    chunk_has_video = True
                
                
                # Skip chunks that already have video URLs
                if chunk_has_video and not force_video_chunking:
                    logging.debug(f"Skipping chunk {extract_sk_value(item)} - already has video URL")
                    skipped_chunks += 1
                    continue

                # Use the Chunk model's from_dynamodb_item method
                chunk_model = Chunk.from_dynamodb_item(item)
                chunks.append(chunk_model)
            except Exception as e:
                logging.error(f"Error converting chunk to ChunkModel: {e}")
                continue

        
        # Validate the number of chunks if expected count was provided
        if num_chunks is not None and len(chunks) != num_chunks:
            logging.warning(f"Expected {num_chunks} chunks, but converted {len(chunks)} chunks")
        
        if skipped_chunks > 0:
            logging.info(f"Skipped {skipped_chunks} chunks that already have video URLs")
        
        logging.info(f"Successfully converted {len(chunks)} chunks to ChunkModel objects")
        return chunks
    
    except Exception as e:
        logging.error(f"Error retrieving chunks: {e}")
        return []

def get_video_chunking_status(PK, SK, ):
    """Get current video chunking status from metadata table"""
    try:
        dynamodb = get_dynamodb_resource()
        metadata_table = dynamodb.Table(config.podcast_metadata_table) #type:ignore
        response = metadata_table.get_item(
            Key={
                'PK': PK,
                'SK': SK
            }
        )
        if 'Item' in response:
            return response['Item'].get('shortVideoStatus', 'PENDING')
        return 'PENDING'
    except Exception as e:
        logging.error(f"Error getting video chunking status: {e}")
        return 'PENDING'

def update_chunk_video_url_all_records(episode_id: str, chunk_id: str, video_url: str) -> bool:
    """
    Update the video URL for a chunk in both the main record and all associated simplified records.
    
    Args:
        episode_id: The episode ID
        chunk_id: The chunk ID
        video_url: The video URL to set
        
    Returns:
        bool: True if all updates were successful, False otherwise
    """
    dynamodb = get_dynamodb_resource()
    chunk_table = dynamodb.Table(config.chunk_table) #type:ignore
    
    try:
        logging.info(f"Updating video URL for chunk {chunk_id} in episode {episode_id}")
        
        # First, get all records for this episode to find the main chunk and simplified records
        response = chunk_table.query(
            KeyConditionExpression=Key('PK').eq(f'EPISODE#{episode_id}'),
            ConsistentRead=True
        )
        
        items = response.get('Items', [])
        main_chunk_record = None
        simplified_records = []
        
        # Find the main chunk record and simplified records
        for item in items:
            sk = item.get('SK', {})
            # Extract the actual string value from DynamoDB format
            if isinstance(sk, dict) and 'S' in sk:
                sk_value = sk['S']
            elif isinstance(sk, str):
                sk_value = sk
            else:
                continue
                
            if sk_value.startswith('DATE#') and f'#CHUNK#{chunk_id}' in sk_value:
                main_chunk_record = item
            elif sk_value.startswith(f'CHUNK#{chunk_id}#GENRE#'):
                simplified_records.append(item)
        
        if not main_chunk_record:
            logging.error(f"Main chunk record not found for chunk {chunk_id} in episode {episode_id}")
            return False
        
        # Update the main chunk record
        main_pk = main_chunk_record['PK']
        main_sk = main_chunk_record['SK']
        
        logging.debug(f"Updating main chunk record: PK={main_pk}, SK={main_sk}")
        chunk_table.update_item(
            Key={'PK': main_pk, 'SK': main_sk},
            UpdateExpression='SET chunkVideoUrl = :video_url, updatedAt = :updated_at',
            ExpressionAttributeValues={
                ':video_url': video_url,
                ':updated_at': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
            }
        )
        
        # Update all simplified records
        updated_simplified = 0
        for simplified_record in simplified_records:
            simplified_pk = simplified_record['PK']
            simplified_sk = simplified_record['SK']
            
            logging.debug(f"Updating simplified chunk record: PK={simplified_pk}, SK={simplified_sk}")
            chunk_table.update_item(
                Key={'PK': simplified_pk, 'SK': simplified_sk},
                UpdateExpression='SET chunkVideoUrl = :video_url, updatedAt = :updated_at',
                ExpressionAttributeValues={
                    ':video_url': video_url,
                    ':updated_at': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
                }
            )
            updated_simplified += 1
        
        logging.info(f"Successfully updated video URL for chunk {chunk_id}: 1 main record + {updated_simplified} simplified records")
        return True
        
    except Exception as e:
        logging.error(f"Error updating chunk video URLs for chunk {chunk_id}: {e}")
        return False

def update_video_chunking_status(PK, SK, status):
    """Update the chunking status in the metadata table using new schema."""
    if not PK or not SK or not status:
        raise ValueError("PK, SK, and status cannot be empty")
    dynamodb = get_dynamodb_resource()
    try:
        metadata_table = dynamodb.Table(config.podcast_metadata_table)
        response = metadata_table.update_item(
            Key={
                'PK': PK,
                'SK': SK
            },
            UpdateExpression='SET shortVideoStatus = :status, updatedAt = :updatedAt',
            ExpressionAttributeValues={
                ':status': status,
                ':updatedAt': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
            },
            ReturnValues='UPDATED_NEW'
        )
        logging.info(f"Updated video_chunking_status to '{status}' for {PK}/{SK}")
        return response
    except ClientError as e:
        logging.error(f"Error updating video chunking status: {e}")
        raise

