from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

@dataclass
class Quote:
    """Quote model matching RDS schema"""
    # Required fields
    quote_id: str
    quote: str
    channel_id: str
    episode_id: str
    episode_title: str
    podcast_title: str
    
    # Optional fields with defaults
    context: Optional[str] = None
    context_length: Optional[float] = None
    context_start_ms: Optional[float] = None
    context_end_ms: Optional[float] = None
    quote_start_ms: Optional[float] = None
    quote_end_ms: Optional[float] = None
    quote_rank: Optional[int] = None
    quote_audio_url: Optional[str] = None
    genre: Optional[str] = None
    guests_name: Optional[str] = None
    guests_description: Optional[str] = None
    quote_length: Optional[float] = None
    published_date: Optional[datetime] = None
    sentiment: Optional[str] = None
    speaker_label: Optional[str] = None
    speaker_name: Optional[str] = None
    topic: Optional[str] = None
    quote_description: Optional[str] = None
    is_synced: bool = False
    absolute_context_word_timestamps: Optional[str] = None  # S3 URL
    absolute_proverb_word_timestamps: Optional[str] = None  # S3 URL
    transcript_level_context_word_timestamps: Optional[str] = None  # S3 URL
    transcript_level_proverb_word_timestamps: Optional[str] = None  # S3 URL
    content_type: str = 'Audio'
    additional_data: Dict[str, Any] = field(default_factory=dict)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    deleted_at: Optional[datetime] = None

    # Compatibility properties for backward compatibility
    @property
    def quoteId(self) -> str:
        return self.quote_id

    @property
    def episodeTitle(self) -> str:
        return self.episode_title

    @property
    def podcastTitle(self) -> str:
        return self.podcast_title

    @property
    def quoteRank(self) -> Optional[int]:
        return self.quote_rank

    @property
    def quoteAudioUrl(self) -> Optional[str]:
        return self.quote_audio_url

    @property
    def contextStartMs(self) -> Optional[float]:
        return self.context_start_ms

    @property
    def contextEndMs(self) -> Optional[float]:
        return self.context_end_ms

    @property
    def quoteStartMs(self) -> Optional[float]:
        return self.quote_start_ms

    @property
    def quoteEndMs(self) -> Optional[float]:
        return self.quote_end_ms

    # For legacy code compatibility
    @property
    def proverb(self) -> str:
        """Legacy compatibility - returns quote text"""
        return self.quote

    def __post_init__(self):
        """Validate and convert timestamps if needed"""
        # Convert string timestamps to float if needed
        if isinstance(self.context_start_ms, str):
            try:
                self.context_start_ms = float(self.context_start_ms)
            except (ValueError, TypeError):
                self.context_start_ms = None
        if isinstance(self.context_end_ms, str):
            try:
                self.context_end_ms = float(self.context_end_ms)
            except (ValueError, TypeError):
                self.context_end_ms = None
        if isinstance(self.quote_start_ms, str):
            try:
                self.quote_start_ms = float(self.quote_start_ms)
            except (ValueError, TypeError):
                self.quote_start_ms = None
        if isinstance(self.quote_end_ms, str):
            try:
                self.quote_end_ms = float(self.quote_end_ms)
            except (ValueError, TypeError):
                self.quote_end_ms = None

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'Quote':
        """Create a Quote instance from a database row dictionary"""
        return cls(
            quote_id=row['quote_id'],
            quote=row['quote'],
            channel_id=row['channel_id'],
            episode_id=row['episode_id'],
            context=row.get('context'),
            context_length=row.get('context_length'),
            context_start_ms=row.get('context_start_ms'),
            context_end_ms=row.get('context_end_ms'),
            quote_start_ms=row.get('quote_start_ms'),
            quote_end_ms=row.get('quote_end_ms'),
            episode_title=row.get('episode_title') or '',
            podcast_title=row.get('podcast_title') or '',
            genre=row.get('genre'),
            guests_name=row.get('guests_name'),
            guests_description=row.get('guests_description'),
            quote_length=row.get('quote_length'),
            quote_rank=row.get('quote_rank'),
            published_date=row.get('published_date'),
            quote_audio_url=row.get('quote_audio_url'),
            sentiment=row.get('sentiment'),
            speaker_label=row.get('speaker_label'),
            speaker_name=row.get('speaker_name'),
            topic=row.get('topic'),
            quote_description=row.get('quote_description'),
            is_synced=row.get('is_synced', False),
            absolute_context_word_timestamps=row.get('absolute_context_word_timestamps'),
            absolute_proverb_word_timestamps=row.get('absolute_proverb_word_timestamps'),
            transcript_level_context_word_timestamps=row.get('transcript_level_context_word_timestamps'),
            transcript_level_proverb_word_timestamps=row.get('transcript_level_proverb_word_timestamps'),
            content_type=row.get('content_type', 'Audio'),
            additional_data=row.get('additional_data', {}),
            created_at=row.get('created_at'),
            updated_at=row.get('updated_at'),
            deleted_at=row.get('deleted_at')
        )

    def to_db_dict(self) -> Dict[str, Any]:
        """Convert the Quote instance to a dictionary for database operations."""
        return {
            'quote_id': self.quote_id,
            'quote': self.quote,
            'channel_id': self.channel_id,
            'episode_id': self.episode_id,
            'context': self.context,
            'context_length': self.context_length,
            'context_start_ms': self.context_start_ms,
            'context_end_ms': self.context_end_ms,
            'quote_start_ms': self.quote_start_ms,
            'quote_end_ms': self.quote_end_ms,
            'episode_title': self.episode_title,
            'podcast_title': self.podcast_title,
            'genre': self.genre,
            'guests_name': self.guests_name,
            'guests_description': self.guests_description,
            'quote_length': self.quote_length,
            'quote_rank': self.quote_rank,
            'published_date': self.published_date,
            'quote_audio_url': self.quote_audio_url,
            'sentiment': self.sentiment,
            'speaker_label': self.speaker_label,
            'speaker_name': self.speaker_name,
            'topic': self.topic,
            'quote_description': self.quote_description,
            'is_synced': self.is_synced,
            'absolute_context_word_timestamps': self.absolute_context_word_timestamps,
            'absolute_proverb_word_timestamps': self.absolute_proverb_word_timestamps,
            'transcript_level_context_word_timestamps': self.transcript_level_context_word_timestamps,
            'transcript_level_proverb_word_timestamps': self.transcript_level_proverb_word_timestamps,
            'content_type': self.content_type,
            'additional_data': self.additional_data,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'deleted_at': self.deleted_at
        }
