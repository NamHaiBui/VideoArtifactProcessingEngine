from typing import List, Optional, Dict, Any
import boto3
from boto3.dynamodb.conditions import Key
from video_artifact_processing_engine.utils.dynamodbAttributeValueConverter import (
    dynamodb_attribute_to_python_type,
    flatten_timestamps,
    flatten_word_timestamps,
    flatten_list_field
)


class WordTimestamp:
    """Represents a word timestamp with start/end times and the word text"""
    def __init__(self, word: str, start_time: int, end_time: int):
        self.word = word
        self.start_time = start_time
        self.end_time = end_time
    
    @classmethod
    def from_dynamodb_item(cls, item: Dict[str, Any]) -> 'WordTimestamp':
        """Create WordTimestamp from DynamoDB nested structure using the converter"""
        try:
            # Use the converter to handle the deep nesting
            converted_item = dynamodb_attribute_to_python_type(item)
            
            # The converter should give us a structure we can work with
            # Handle various possible structures after conversion
            if isinstance(converted_item, dict):
                # Look for the actual data, which might still be nested
                current = converted_item
                
                # Keep navigating deeper if we find nested structures
                while isinstance(current, dict) and len(current) == 1 and 'M' in current:
                    current = current['M']
                
                # Now extract the fields
                word = ''
                start_time = 0
                end_time = 0
                
                if isinstance(current, dict):
                    # Extract word field
                    word_data = current.get('word', current.get('word', {}))
                    if isinstance(word_data, dict) and 'S' in word_data:
                        word = word_data['S']
                    elif isinstance(word_data, str):
                        word = word_data
                    else:
                        word = str(word_data) if word_data else ''
                    
                    # Extract start time (try both field names)
                    start_data = current.get('start_time', current.get('start', {}))
                    if isinstance(start_data, dict) and 'N' in start_data:
                        try:
                            start_time = int(start_data['N'])
                        except (ValueError, TypeError):
                            start_time = 0
                    elif isinstance(start_data, (int, str)):
                        try:
                            start_time = int(start_data)
                        except (ValueError, TypeError):
                            start_time = 0
                    
                    # Extract end time (try both field names)
                    end_data = current.get('end_time', current.get('end', {}))
                    if isinstance(end_data, dict) and 'N' in end_data:
                        try:
                            end_time = int(end_data['N'])
                        except (ValueError, TypeError):
                            end_time = 0
                    elif isinstance(end_data, (int, str)):
                        try:
                            end_time = int(end_data)
                        except (ValueError, TypeError):
                            end_time = 0
                
                return cls(word=word, start_time=start_time, end_time=end_time)
            
            # Fallback for simple dict format
            elif isinstance(item, dict):
                # Try both field name variations
                start_value = item.get('start', item.get('start_time', 0))
                end_value = item.get('end', item.get('end_time', 0))
                
                # Safe extraction for start and end times
                try:
                    start_time = int(start_value) if start_value else 0
                    end_time = int(end_value) if end_value else 0
                except (ValueError, TypeError):
                    start_time = 0
                    end_time = 0
                
                word = str(item.get('word', ''))
                
                return cls(word=word, start_time=start_time, end_time=end_time)
            
        except Exception as e:
            # If converter fails, return empty timestamp
            pass
        
        # Final fallback
        return cls(word='', start_time=0, end_time=0)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'word': self.word,
            'start_time': self.start_time,
            'end_time': self.end_time
        }


def extract_string(data: Dict[str, Any], key: str, default: str = '') -> str:
    """Extract string value from both DynamoDB attribute format and simplified format"""
    value = data.get(key, default)
    if isinstance(value, dict) and 'S' in value:
        return value['S']
    return str(value) if value is not None else default


def extract_int(data: Dict[str, Any], key: str, default: int = 0) -> int:
    """Extract integer value from both DynamoDB attribute format and simplified format"""
    try:
        value = data.get(key, default)
        from decimal import Decimal
        if isinstance(value, dict) and 'N' in value:
            return int(float(value['N']))
        elif isinstance(value, (int, float, str, Decimal)) and not isinstance(value, dict):
            return int(float(value))
        return default
    except (ValueError, TypeError):
        return default


def extract_float(data: Dict[str, Any], key: str, default: float = 0.0) -> float:
    """Extract float value from both DynamoDB attribute format and simplified format"""
    try:
        value = data.get(key, default)
        from decimal import Decimal
        if isinstance(value, dict) and 'N' in value:
            return float(value['N'])
        elif isinstance(value, (int, float, str, Decimal)) and not isinstance(value, dict):
            return float(value)
        return default
    except (ValueError, TypeError):
        return default


def extract_list_of_strings(data: Dict[str, Any], key: str) -> List[str]:
    """Extract list of strings from both DynamoDB attribute format and simplified format"""
    value = data.get(key, [])
    
    # Handle standard DynamoDB format: {'L': [{'S': 'value1'}, {'S': 'value2'}]}
    if isinstance(value, dict) and 'L' in value:
        result = []
        for item in value['L']:
            if isinstance(item, dict):
                # Standard format: {'S': 'value'}
                if 'S' in item:
                    result.append(item['S'])
                # Nested format: {'M': {'S': {'S': 'value'}}}
                elif 'M' in item:
                    inner = item['M']
                    if isinstance(inner, dict) and 'S' in inner:
                        # Check if it's a nested S structure
                        s_value = inner['S']
                        if isinstance(s_value, dict) and 'S' in s_value:
                            result.append(s_value['S'])
                        else:
                            result.append(str(s_value) if s_value else '')
                    else:
                        result.append('')
                else:
                    result.append('')
            else:
                result.append(str(item) if item else '')
        return result
    elif isinstance(value, list):
        return [str(item) for item in value]
    return []


def extract_nested_timestamp_value(data: Dict[str, Any], key: str) -> int:
    """Extract timestamp value from deeply nested DynamoDB structure"""
    # Handle deeply nested structure: {'M': {'N': {'S': '12345'}}} or {'M': {'N': '12345'}}
    value = data.get(key, {})
    
    if isinstance(value, dict) and 'M' in value:
        inner = value['M']
        if isinstance(inner, dict):
            # Check for 'N' field
            if 'N' in inner:
                n_value = inner['N']
                # Handle {'N': {'S': '12345'}} format
                if isinstance(n_value, dict) and 'S' in n_value:
                    try:
                        return int(n_value['S'])
                    except (ValueError, TypeError):
                        return 0
                # Handle {'N': '12345'} format
                else:
                    try:
                        return int(float(str(n_value)))
                    except (ValueError, TypeError):
                        return 0
    
    # Fallback to standard extraction
    return extract_int(data, key, 0)


class Chunk:
    """Represents a transcript chunk with all associated metadata"""
    def __init__(
        self,
        pk: str,
        sk: str,
        channel_id: str,
        chunk: str,
        chunk_audio_url: str,
        chunk_descriptive_title: str,
        chunk_length: float,
        chunk_title: str,
        created_at: str,
        description: str,
        end_ms: int,
        episode_id: str,
        genre_ids: List[str],
        chunk_id: str,
        chunk_video_url: str = '',
        likes_count: int = 0,
        llm_enrichment_status: str = '',
        played_count: int = 0,
        published_date: str = '',
        saves_count: int = 0,
        sentiment: Optional[List[str]] = None,
        share_count: int = 0,
        speakers: Optional[List[str]] = None,
        start_ms: int = 0,
        topics: Optional[List[str]] = None,
        transcript: str = '',
        updated_at: str = '',
        word_timestamps: Optional[List[WordTimestamp]] = None,
        time_stamps: Optional[Dict[str, int]] = None
    ):
        self.pk = pk
        self.sk = sk
        self.channel_id = channel_id
        self.chunk = chunk
        self.chunk_audio_url = chunk_audio_url
        self.chunk_descriptive_title = chunk_descriptive_title
        self.chunk_length = chunk_length
        self.chunk_title = chunk_title
        self.created_at = created_at
        self.description = description
        self.end_ms = end_ms
        self.episode_id = episode_id
        self.genre_ids = genre_ids or []
        self.chunk_id = chunk_id
        self.chunk_video_url = chunk_video_url
        self.likes_count = likes_count
        self.llm_enrichment_status = llm_enrichment_status
        self.played_count = played_count
        self.published_date = published_date
        self.saves_count = saves_count
        self.sentiment = sentiment or []
        self.share_count = share_count
        self.speakers = speakers or []
        self.start_ms = start_ms
        self.topics = topics or []
        self.transcript = transcript
        self.updated_at = updated_at
        self.word_timestamps = word_timestamps or []
        self.time_stamps = time_stamps or {}
    
    @classmethod
    def from_dynamodb_item(cls, item: Dict[str, Any]) -> 'Chunk':
        """Create Chunk from DynamoDB item format using the converter for better handling"""
        
        # Extract word timestamps using converter
        word_timestamps = []
        word_timestamps_data = item.get('wordTimestamps', [])
        
        if isinstance(word_timestamps_data, dict) and 'L' in word_timestamps_data:
            # Use the converter's flatten function
            flattened_word_timestamps = flatten_word_timestamps(word_timestamps_data['L'])
            for timestamp_item in flattened_word_timestamps:
                word_timestamps.append(WordTimestamp.from_dynamodb_item(timestamp_item))
        elif isinstance(word_timestamps_data, list):
            for timestamp_item in word_timestamps_data:
                word_timestamps.append(WordTimestamp.from_dynamodb_item(timestamp_item))
        
        # Extract time stamps using converter
        time_stamps = {}
        time_stamps_data = item.get('timeStamps', {})
        if isinstance(time_stamps_data, dict) and 'M' in time_stamps_data:
            # Use the converter's flatten function
            flattened_timestamps = flatten_timestamps(time_stamps_data['M'])
            # Convert to integers
            start_val = flattened_timestamps.get('start', {})
            end_val = flattened_timestamps.get('end', {})
            
            # Handle the converted format
            if isinstance(start_val, dict) and 'N' in start_val:
                time_stamps['start'] = int(start_val['N'])
            elif isinstance(start_val, (int, str)):
                time_stamps['start'] = int(start_val)
            else:
                time_stamps['start'] = 0
                
            if isinstance(end_val, dict) and 'N' in end_val:
                time_stamps['end'] = int(end_val['N'])
            elif isinstance(end_val, (int, str)):
                time_stamps['end'] = int(end_val)
            else:
                time_stamps['end'] = 0
        elif isinstance(time_stamps_data, dict) and time_stamps_data:
            time_stamps = {
                'start': extract_int(time_stamps_data, 'start'),
                'end': extract_int(time_stamps_data, 'end')
            }
        
        # Use converter for list fields that have complex nesting
        genre_ids = []
        genre_ids_data = item.get('genreIds', [])
        if isinstance(genre_ids_data, dict) and 'L' in genre_ids_data:
            genre_ids = flatten_list_field(genre_ids_data['L'])
        else:
            genre_ids = extract_list_of_strings(item, 'genreIds')
        
        topics = []
        topics_data = item.get('topics', [])
        if isinstance(topics_data, dict) and 'L' in topics_data:
            topics = flatten_list_field(topics_data['L'])
        else:
            topics = extract_list_of_strings(item, 'topics')
        
        speakers = []
        speakers_data = item.get('speakers', [])
        if isinstance(speakers_data, dict) and 'L' in speakers_data:
            speakers = flatten_list_field(speakers_data['L'])
        else:
            speakers = extract_list_of_strings(item, 'speakers')
        
        sentiment = []
        sentiment_data = item.get('sentiment', [])
        if isinstance(sentiment_data, dict) and 'L' in sentiment_data:
            sentiment = flatten_list_field(sentiment_data['L'])
        else:
            sentiment = extract_list_of_strings(item, 'sentiment')
        
        return cls(
            pk=extract_string(item, 'PK'),
            sk=extract_string(item, 'SK'),
            channel_id=extract_string(item, 'channelId'),
            chunk=extract_string(item, 'chunk'),
            chunk_audio_url=extract_string(item, 'chunkAudioUrl'),
            chunk_descriptive_title=extract_string(item, 'chunkDescriptiveTitle'),
            chunk_length=extract_float(item, 'chunkLength'),
            chunk_title=extract_string(item, 'chunkTitle'),
            created_at=extract_string(item, 'createdAt'),
            description=extract_string(item, 'description'),
            end_ms=extract_int(item, 'endMs'),
            episode_id=extract_string(item, 'episodeId'),
            genre_ids=genre_ids,
            chunk_id=extract_string(item, 'id'),
            chunk_video_url=extract_string(item, 'chunkVideoUrl'),
            likes_count=extract_int(item, 'likesCount'),
            llm_enrichment_status=extract_string(item, 'llmEnrichmentStatus'),
            played_count=extract_int(item, 'playedCount'),
            published_date=extract_string(item, 'publishedDate'),
            saves_count=extract_int(item, 'savesCount'),
            sentiment=sentiment,
            share_count=extract_int(item, 'shareCount'),
            speakers=speakers,
            start_ms=extract_int(item, 'startMs'),
            topics=topics,
            transcript=extract_string(item, 'transcript'),
            updated_at=extract_string(item, 'updatedAt'),
            word_timestamps=word_timestamps,
            time_stamps=time_stamps
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert Chunk to dictionary format"""
        return {
            'pk': self.pk,
            'sk': self.sk,
            'channel_id': self.channel_id,
            'chunk': self.chunk,
            'chunk_audio_url': self.chunk_audio_url,
            'chunk_descriptive_title': self.chunk_descriptive_title,
            'chunk_length': self.chunk_length,
            'chunk_title': self.chunk_title,
            'created_at': self.created_at,
            'description': self.description,
            'end_ms': self.end_ms,
            'episode_id': self.episode_id,
            'genre_ids': self.genre_ids,
            'chunk_id': self.chunk_id,
            'chunk_video_url': self.chunk_video_url,
            'likes_count': self.likes_count,
            'llm_enrichment_status': self.llm_enrichment_status,
            'played_count': self.played_count,
            'published_date': self.published_date,
            'saves_count': self.saves_count,
            'sentiment': self.sentiment,
            'share_count': self.share_count,
            'speakers': self.speakers,
            'start_ms': self.start_ms,
            'topics': self.topics,
            'transcript': self.transcript,
            'updated_at': self.updated_at,
            'word_timestamps': [wt.to_dict() for wt in self.word_timestamps],
            'time_stamps': self.time_stamps
        }
    
    # Utility methods
    def get_duration_seconds(self) -> float:
        """Get chunk duration in seconds"""
        return self.chunk_length
    
    def get_duration_ms(self) -> int:
        """Get chunk duration in milliseconds"""
        return self.end_ms - self.start_ms
    
    def get_word_count(self) -> int:
        """Get word count from transcript"""
        if not self.transcript:
            return 0
        return len(self.transcript.split())
    
    def get_topics_string(self) -> str:
        """Get topics as comma-separated string"""
        return ', '.join(self.topics)
    
    def get_speakers_string(self) -> str:
        """Get speakers as comma-separated string"""
        return ', '.join(self.speakers)
    
    def get_sentiment_string(self) -> str:
        """Get sentiment as comma-separated string"""
        return ', '.join(self.sentiment)
    
    def is_published(self) -> bool:
        """Check if chunk is published"""
        return bool(self.published_date)
    
    def has_enrichment_completed(self) -> bool:
        """Check if LLM enrichment is completed"""
        return self.llm_enrichment_status == 'completed'
    
    def get_engagement_score(self) -> int:
        """Calculate total engagement score"""
        return self.likes_count + self.saves_count + self.share_count + self.played_count


class ChunkModel:
    """DynamoDB operations for Chunk model"""
    
    def __init__(self, table_name: str = 'Chunks-staging', region_name: str = 'us-east-2'):
        self.table_name = table_name
        self.region_name = region_name
        self.dynamodb = boto3.resource('dynamodb', region_name=region_name)
        self.table = self.dynamodb.Table(table_name) #type:ignore
    
    def get_chunk(self, pk: str, sk: str) -> Optional[Chunk]:
        """Get a chunk by primary key and sort key"""
        try:
            response = self.table.get_item(
                Key={'PK': pk, 'SK': sk}
            )
            if 'Item' in response:
                return Chunk.from_dynamodb_item(response['Item'])
            return None
        except Exception as e:
            print(f"Error getting chunk: {e}")
            return None
    
    def save_chunk(self, chunk: Chunk) -> bool:
        """Save a chunk to DynamoDB"""
        try:
            chunk_dict = chunk.to_dict()
            # Convert to DynamoDB format if needed
            self.table.put_item(Item=chunk_dict)
            return True
        except Exception as e:
            print(f"Error saving chunk: {e}")
            return False
    
    def delete_chunk(self, pk: str, sk: str) -> bool:
        """Delete a chunk from DynamoDB"""
        try:
            self.table.delete_item(
                Key={'PK': pk, 'SK': sk}
            )
            return True
        except Exception as e:
            print(f"Error deleting chunk: {e}")
            return False
    
    def search_chunks_by_episode(self, episode_id: str) -> List[Chunk]:
        """Search chunks by episode ID"""
        try:
            response = self.table.query(
                KeyConditionExpression=Key('PK').eq(f'EPISODE#{episode_id}')
            )
            chunks = []
            for item in response.get('Items', []):
                chunks.append(Chunk.from_dynamodb_item(item))
            return chunks
        except Exception as e:
            print(f"Error searching chunks: {e}")
            return []
