from dataclasses import dataclass
from typing import List, Dict, Any, Optional
from datetime import datetime
import sys
import os

from video_artifact_processing_engine.utils.logging_config import setup_custom_logger

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from video_artifact_processing_engine.utils.dynamodbAttributeValueConverter import (
    dynamodb_attribute_to_python_type,
    flatten_timestamps,
    flatten_word_timestamps,
    flatten_list_field
)

logging = setup_custom_logger(__name__)

@dataclass
class WordTimestamp:
    """Represents a single word with timing information"""
    word: str
    start_time: float
    end_time: float
    
    def duration(self) -> float:
        """Get duration of the word in seconds"""
        return self.end_time - self.start_time
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format"""
        return {
            'word': self.word,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'duration': self.duration()
        }
    
    @classmethod
    def from_dynamodb_structure(cls, word_data: Dict[str, Any]) -> 'WordTimestamp':
        """
        Create from DynamoDB nested structure. This handles both the original nested structure
        and the structure after dynamodb_attribute_to_python_type conversion.
        
        Original structure:
        {
          "M": {
            "M": {
              "M": {
                "end_time": {"M": {"N": {"S": "0.31"}}},
                "start_time": {"M": {"N": {"S": "0"}}},
                "word": {"M": {"S": {"S": "look"}}}
              }
            }
          }
        }
        
        After conversion:
        {
          "M": {
            "end_time": {"N": "0.31"},
            "start_time": {"N": "0"},
            "word": {"S": "look"}
          }
        }
        """
        try:
            # Try the converted structure first (after dynamodb_attribute_to_python_type)
            if 'M' in word_data:
                data = word_data['M']
                
                # Extract word
                word = ""
                if 'word' in data:
                    if isinstance(data['word'], dict) and 'S' in data['word']:
                        word = data['word']['S']
                    elif isinstance(data['word'], str):
                        word = data['word']
                
                # Extract start_time
                start_time = 0.0
                if 'start_time' in data:
                    if isinstance(data['start_time'], dict) and 'N' in data['start_time']:
                        start_time = float(data['start_time']['N'])
                    elif isinstance(data['start_time'], (int, float)):
                        start_time = float(data['start_time'])
                
                # Extract end_time
                end_time = 0.0
                if 'end_time' in data:
                    if isinstance(data['end_time'], dict) and 'N' in data['end_time']:
                        end_time = float(data['end_time']['N'])
                    elif isinstance(data['end_time'], (int, float)):
                        end_time = float(data['end_time'])
                
                return cls(word=word, start_time=start_time, end_time=end_time)
            
            # Try original deeply nested structure
            if 'M' in word_data:
                data = word_data['M']
                if 'M' in data:
                    data = data['M']
                    if 'M' in data:
                        data = data['M']
                        
                        # Extract word
                        word = ""
                        if 'word' in data and 'M' in data['word'] and 'S' in data['word']['M'] and 'S' in data['word']['M']['S']:
                            word = data['word']['M']['S']['S']
                        
                        # Extract start_time
                        start_time = 0.0
                        if 'start_time' in data and 'M' in data['start_time'] and 'N' in data['start_time']['M'] and 'S' in data['start_time']['M']['N']:
                            start_time = float(data['start_time']['M']['N']['S'])
                        
                        # Extract end_time
                        end_time = 0.0
                        if 'end_time' in data and 'M' in data['end_time'] and 'N' in data['end_time']['M'] and 'S' in data['end_time']['M']['N']:
                            end_time = float(data['end_time']['M']['N']['S'])
                        
                        return cls(word=word, start_time=start_time, end_time=end_time)
            
            # Fallback: try direct structure
            word = word_data.get('word', '')
            start_time = float(word_data.get('start_time', 0))
            end_time = float(word_data.get('end_time', 0))
            
            return cls(word=word, start_time=start_time, end_time=end_time)
            
        except (ValueError, KeyError, TypeError) as e:
            logging.warning(f"Error parsing word timestamp: {e}. Data: {word_data}")
            return cls(word="", start_time=0.0, end_time=0.0)

@dataclass
class QuoteTimestamps:
    """Represents timing information for different parts of a quote"""
    start_time: float
    end_time: float
    
    def duration(self) -> float:
        """Get duration in seconds"""
        return self.end_time - self.start_time
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format"""
        return {
            'start_time': self.start_time,
            'end_time': self.end_time,
            'duration': self.duration()
        }
    
    @classmethod
    def from_dynamodb_structure(cls, timestamp_data: Dict[str, Any]) -> 'QuoteTimestamps':
        """Create from DynamoDB timestamp structure"""
        try:
            logging.debug(f"Parsing timestamp data: {timestamp_data}")
            
            # Handle nested M structure (raw DynamoDB format)
            if 'M' in timestamp_data:
                data = timestamp_data['M']
                start_time = float(data.get('start_time', {}).get('N', '0'))
                end_time = float(data.get('end_time', {}).get('N', '0'))
                logging.debug(f"Parsed nested M structure: start={start_time}, end={end_time}")
            # Handle boto3-converted format or direct format
            elif 'start_time' in timestamp_data and 'end_time' in timestamp_data:
                start_time_attr = timestamp_data.get('start_time', 0)
                end_time_attr = timestamp_data.get('end_time', 0)
                
                # Handle DynamoDB Number format {'N': 'value'}
                if isinstance(start_time_attr, dict) and 'N' in start_time_attr:
                    start_time = float(start_time_attr['N'])
                elif not isinstance(start_time_attr, dict):
                    # Handle direct values (Decimal, int, float, str)
                    start_time = float(start_time_attr)
                else:
                    start_time = 0.0
                    
                if isinstance(end_time_attr, dict) and 'N' in end_time_attr:
                    end_time = float(end_time_attr['N'])
                elif not isinstance(end_time_attr, dict):
                    # Handle direct values (Decimal, int, float, str)
                    end_time = float(end_time_attr)
                else:
                    end_time = 0.0
                    
                logging.debug(f"Parsed direct structure: start={start_time}, end={end_time}")
            else:
                # Fallback to zero values
                start_time = 0.0
                end_time = 0.0
                logging.debug(f"Using fallback values: start={start_time}, end={end_time}")
                
            return cls(start_time=start_time, end_time=end_time)
        except (ValueError, KeyError, TypeError) as e:
            logging.warning(f"Error parsing quote timestamps: {e}. Data: {timestamp_data}. Using defaults.")
            return cls(start_time=0.0, end_time=0.0)

@dataclass
class Quote:
    """Model for processing quote data from DynamoDB"""
    
    # Primary keys and identifiers
    pk: str  # Partition key (e.g., "EPISODE#uuid")
    sk: str  # Sort key (e.g., "DATE#timestamp#QUOTE#uuid")
    id: str
    episode_id: str
    
    # Quote content
    text: str  # The actual quote text
    proverb: str  # The proverb/key phrase
    context: str  # Full context surrounding the quote
    
    # Metadata
    episode_title: str
    podcast_title: str
    speaker_name: str
    speaker_label: str
    guest_id: str
    channel_id: str
    source_transcript_id: str
    
    # Timing information
    start_ms: int  # Start time in milliseconds
    end_ms: int  # End time in milliseconds
    context_length: float  # Length of context in seconds
    proverb_length: float  # Length of proverb in seconds
    
    # Timestamps for different parts
    context_timestamps: QuoteTimestamps
    proverb_timestamps: QuoteTimestamps
    
    # Word-level timestamps
    absolute_context_word_timestamps: List[WordTimestamp]
    absolute_proverb_word_timestamps: List[WordTimestamp]
    transcript_level_context_word_timestamps: List[WordTimestamp]
    transcript_level_proverb_word_timestamps: List[WordTimestamp]
    
    # Classification and metadata
    topic: str
    sentiment: str
    genre_ids: List[str]
    short_description: str
    proverb_rank: int
    
    # Timestamps
    published_date: datetime
    created_at: datetime
    updated_at: datetime
    
    # Engagement metrics (with defaults)
    likes_count: int = 0
    saves_count: int = 0
    share_count: int = 0
    played_count: int = 0
    
    # Video processing
    quote_video_url: str = ""
    
    @property
    def duration_seconds(self) -> float:
        """Get quote duration in seconds"""
        return (self.end_ms - self.start_ms) / 1000.0
    
    @property
    def total_words(self) -> int:
        """Get total word count in the quote text"""
        return len(self.text.split()) if self.text else 0
    
    @property
    def context_word_count(self) -> int:
        """Get word count in context"""
        return len(self.absolute_context_word_timestamps)
    
    @property
    def proverb_word_count(self) -> int:
        """Get word count in proverb"""
        return len(self.absolute_proverb_word_timestamps)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format"""
        return {
            'pk': self.pk,
            'sk': self.sk,
            'id': self.id,
            'episode_id': self.episode_id,
            'text': self.text,
            'proverb': self.proverb,
            'context': self.context,
            'episode_title': self.episode_title,
            'podcast_title': self.podcast_title,
            'speaker_name': self.speaker_name,
            'speaker_label': self.speaker_label,
            'guest_id': self.guest_id,
            'channel_id': self.channel_id,
            'source_transcript_id': self.source_transcript_id,
            'start_ms': self.start_ms,
            'end_ms': self.end_ms,
            'duration_seconds': self.duration_seconds,
            'context_length': self.context_length,
            'proverb_length': self.proverb_length,
            'context_timestamps': self.context_timestamps.to_dict(),
            'proverb_timestamps': self.proverb_timestamps.to_dict(),
            'topic': self.topic,
            'sentiment': self.sentiment,
            'genre_ids': self.genre_ids,
            'short_description': self.short_description,
            'proverb_rank': self.proverb_rank,
            'likes_count': self.likes_count,
            'saves_count': self.saves_count,
            'share_count': self.share_count,
            'played_count': self.played_count,
            'total_words': self.total_words,
            'context_word_count': self.context_word_count,
            'proverb_word_count': self.proverb_word_count,
            'absolute_context_word_timestamps': [w.to_dict() for w in self.absolute_context_word_timestamps],
            'absolute_proverb_word_timestamps': [w.to_dict() for w in self.absolute_proverb_word_timestamps],
            'transcript_level_context_word_timestamps': [w.to_dict() for w in self.transcript_level_context_word_timestamps],
            'transcript_level_proverb_word_timestamps': [w.to_dict() for w in self.transcript_level_proverb_word_timestamps],
            'published_date': self.published_date.isoformat(),
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
            'quote_video_url': self.quote_video_url,
        }
    
    @classmethod
    def from_dynamodb_item(cls, item: Dict[str, Any]) -> 'Quote':
        """
        Create Quote from DynamoDB item structure

        Args:
            item: Raw DynamoDB item with attribute value descriptors
            
        Returns:
            QuoteModel instance
        """
        try:
            # Extract basic fields using helper function
            pk = dynamodb_attribute_to_python_type(item.get('PK', {}))
            sk = dynamodb_attribute_to_python_type(item.get('SK', {}))
            quote_id = dynamodb_attribute_to_python_type(item.get('id', {}))
            episode_id = dynamodb_attribute_to_python_type(item.get('episodeId', {}))
            
            # Extract text fields
            text = dynamodb_attribute_to_python_type(item.get('text', {}))
            proverb = dynamodb_attribute_to_python_type(item.get('proverb', {}))
            context = dynamodb_attribute_to_python_type(item.get('context', {}))
            
            # Extract metadata
            episode_title = dynamodb_attribute_to_python_type(item.get('episodeTitle', {}))
            podcast_title = dynamodb_attribute_to_python_type(item.get('podcastTitle', {}))
            speaker_name = dynamodb_attribute_to_python_type(item.get('speakerName', {}))
            speaker_label = dynamodb_attribute_to_python_type(item.get('speakerLabel', {}))
            guest_id = dynamodb_attribute_to_python_type(item.get('guestId', {}))
            channel_id = dynamodb_attribute_to_python_type(item.get('channelId', {}))
            source_transcript_id = dynamodb_attribute_to_python_type(item.get('sourceTranscriptId', {}))
            
            # Extract timing information
            start_ms = int(dynamodb_attribute_to_python_type(item.get('startMs', {})) or 0)
            end_ms = int(dynamodb_attribute_to_python_type(item.get('endMs', {})) or 0)
            context_length = float(dynamodb_attribute_to_python_type(item.get('contextLength', {})) or 0)
            proverb_length = float(dynamodb_attribute_to_python_type(item.get('proverbLength', {})) or 0)
            
            # Extract timestamps
            context_timestamps = QuoteTimestamps.from_dynamodb_structure(
                item.get('contextTimestamps', {})
            )
            proverb_timestamps = QuoteTimestamps.from_dynamodb_structure(
                item.get('proverbTimestamps', {})
            )
            
            # Extract word timestamps
            absolute_context_words = []
            if 'absoluteContextWordTimestamps' in item:
                context_word_data = dynamodb_attribute_to_python_type(item['absoluteContextWordTimestamps'])
                if isinstance(context_word_data, list):
                    for word_item in context_word_data:
                        try:
                            word_ts = WordTimestamp.from_dynamodb_structure(word_item)
                            absolute_context_words.append(word_ts)
                        except Exception as e:
                            logging.warning(f"Failed to parse context word: {e}")
            
            absolute_proverb_words = []
            if 'absoluteProverbWordTimestamps' in item:
                proverb_word_data = dynamodb_attribute_to_python_type(item['absoluteProverbWordTimestamps'])
                if isinstance(proverb_word_data, list):
                    for word_item in proverb_word_data:
                        try:
                            word_ts = WordTimestamp.from_dynamodb_structure(word_item)
                            absolute_proverb_words.append(word_ts)
                        except Exception as e:
                            logging.warning(f"Failed to parse proverb word: {e}")
            
            transcript_context_words = []
            if 'transcriptLevelContextWordTimestamps' in item:
                transcript_context_data = dynamodb_attribute_to_python_type(item['transcriptLevelContextWordTimestamps'])
                if isinstance(transcript_context_data, list):
                    for word_item in transcript_context_data:
                        transcript_context_words.append(WordTimestamp.from_dynamodb_structure(word_item))
            
            transcript_proverb_words = []
            if 'transcriptLevelProverbWordTimestamp' in item:
                transcript_proverb_data = dynamodb_attribute_to_python_type(item['transcriptLevelProverbWordTimestamp'])
                if isinstance(transcript_proverb_data, list):
                    for word_item in transcript_proverb_data:
                        transcript_proverb_words.append(WordTimestamp.from_dynamodb_structure(word_item))
            
            # Extract classification fields
            topic = dynamodb_attribute_to_python_type(item.get('topic', {}))
            sentiment = dynamodb_attribute_to_python_type(item.get('sentiment', {}))
            short_description = dynamodb_attribute_to_python_type(item.get('shortDescription', {}))
            proverb_rank = int(dynamodb_attribute_to_python_type(item.get('proverbRank', {})) or 0)
            
            # Extract genre IDs
            genre_ids = []
            if 'genreIds' in item:
                genre_data = dynamodb_attribute_to_python_type(item['genreIds'])
                if isinstance(genre_data, list):
                    for genre_item in genre_data:
                        if isinstance(genre_item, str):
                            genre_ids.append(genre_item)
                        elif isinstance(genre_item, dict):
                            # Handle nested structure if needed
                            genre_value = dynamodb_attribute_to_python_type(genre_item)
                            if isinstance(genre_value, str):
                                genre_ids.append(genre_value)
            
            # Extract engagement metrics
            likes_count = int(dynamodb_attribute_to_python_type(item.get('likesCount', {})) or 0)
            saves_count = int(dynamodb_attribute_to_python_type(item.get('savesCount', {})) or 0)
            share_count = int(dynamodb_attribute_to_python_type(item.get('shareCount', {})) or 0)
            played_count = int(dynamodb_attribute_to_python_type(item.get('playedCount', {})) or 0)
            
            # Extract video URL
            quote_video_url = dynamodb_attribute_to_python_type(item.get('quoteVideoUrl', {})) or ""
            
            # Extract dates
            published_date_str = dynamodb_attribute_to_python_type(item.get('publishedDate', {}))
            created_at_str = dynamodb_attribute_to_python_type(item.get('createdAt', {}))
            updated_at_str = dynamodb_attribute_to_python_type(item.get('updatedAt', {}))
            
            # Parse dates
            published_date = datetime.fromisoformat(published_date_str.replace('Z', '+00:00')) if published_date_str else datetime.now()
            created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00')) if created_at_str else datetime.now()
            updated_at = datetime.fromisoformat(updated_at_str.replace('Z', '+00:00')) if updated_at_str else datetime.now()
            
            return cls(
                pk=pk,
                sk=sk,
                id=quote_id,
                episode_id=episode_id,
                text=text,
                proverb=proverb,
                context=context,
                episode_title=episode_title,
                podcast_title=podcast_title,
                speaker_name=speaker_name,
                speaker_label=speaker_label,
                guest_id=guest_id,
                channel_id=channel_id,
                source_transcript_id=source_transcript_id,
                start_ms=start_ms,
                end_ms=end_ms,
                context_length=context_length,
                proverb_length=proverb_length,
                context_timestamps=context_timestamps,
                proverb_timestamps=proverb_timestamps,
                absolute_context_word_timestamps=absolute_context_words,
                absolute_proverb_word_timestamps=absolute_proverb_words,
                transcript_level_context_word_timestamps=transcript_context_words,
                transcript_level_proverb_word_timestamps=transcript_proverb_words,
                topic=topic,
                sentiment=sentiment,
                genre_ids=genre_ids,
                short_description=short_description,
                proverb_rank=proverb_rank,
                likes_count=likes_count,
                saves_count=saves_count,
                share_count=share_count,
                played_count=played_count,
                quote_video_url=quote_video_url,
                published_date=published_date,
                created_at=created_at,
                updated_at=updated_at
            )
            
        except Exception as e:
            logging.error(f"Error creating QuoteModel from DynamoDB item: {e}")
            logging.error(f"Item keys: {list(item.keys()) if isinstance(item, dict) else 'Not a dict'}")
            raise
    
    def get_quote_with_context(self, context_padding: int = 10) -> str:
        """
        Get the quote with surrounding context words
        
        Args:
            context_padding: Number of words before and after to include
            
        Returns:
            Quote text with context
        """
        if not self.absolute_context_word_timestamps:
            return self.text
        
        # Find the proverb words within the context
        proverb_words = [w.word for w in self.absolute_proverb_word_timestamps]
        context_words = [w.word for w in self.absolute_context_word_timestamps]
        
        # Try to find where proverb starts in context
        proverb_start_idx = -1
        if proverb_words and context_words:
            proverb_text = ' '.join(proverb_words)
            context_text = ' '.join(context_words)
            
            if proverb_text in context_text:
                # Find the word index where proverb starts
                for i in range(len(context_words) - len(proverb_words) + 1):
                    if ' '.join(context_words[i:i+len(proverb_words)]) == proverb_text:
                        proverb_start_idx = i
                        break
        
        if proverb_start_idx >= 0:
            start_idx = max(0, proverb_start_idx - context_padding)
            end_idx = min(len(context_words), proverb_start_idx + len(proverb_words) + context_padding)
            
            before_context = ' '.join(context_words[start_idx:proverb_start_idx])
            proverb_text = ' '.join(proverb_words)
            after_context = ' '.join(context_words[proverb_start_idx + len(proverb_words):end_idx])
            
            return f"{before_context} **{proverb_text}** {after_context}".strip()
        
        return self.context

    def get_timing_summary(self) -> Dict[str, Any]:
        """Get a summary of timing information"""
        return {
            'total_duration_seconds': self.duration_seconds,
            'context_duration_seconds': self.context_length,
            'proverb_duration_seconds': self.proverb_length,
            'start_ms': self.start_ms,
            'end_ms': self.end_ms,
            'context_timestamps': self.context_timestamps.to_dict(),
            'proverb_timestamps': self.proverb_timestamps.to_dict(),
            'word_count_breakdown': {
                'total_quote_words': self.total_words,
                'context_words': self.context_word_count,
                'proverb_words': self.proverb_word_count
            }
        }
