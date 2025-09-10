from dataclasses import dataclass, field
import json
from typing import Optional, Dict, Any, List
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

@dataclass
class Quote:
    quote_id: str = ''
    quote: str = ''
    channel_id: str = ''
    episode_id: str = ''
    context: Optional[str] = ''
    context_start_ms: Optional[int] = 0
    context_end_ms: Optional[int] = 0
    quote_start_ms: Optional[int] = 0
    quote_end_ms: Optional[int] = 0
    episode_title: Optional[str] = ''
    podcast_title: Optional[str] = ''
    genre: Optional[str] = ''
    guests_name: Optional[str] = ''
    guests_description: Optional[str] = ''
    quote_rank: Optional[int] = 0
    published_date: Optional[datetime] = None
    quote_audio_url: Optional[str] = ''
    sentiment: Optional[str] = ''
    speaker_label: Optional[str] = ''
    speaker_name: Optional[str] = ''
    topic: Optional[str] = ''
    quote_description: Optional[str] = ''
    is_synced: bool = False
    transcript_uri: Optional[Dict[str, Any]] = field(default_factory=dict)
    content_type: str = 'Audio'
    additional_data: Dict[str, Any] = field(default_factory=dict)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    deleted_at: Optional[datetime] = None

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'Quote':
        additional_data = row.get('additionalData', {})
        if additional_data is None:
            additional_data = {}
        return cls(
            quote_id=row['quoteId'],
            quote=row['quote'],
            channel_id=row['channelId'],
            episode_id=row['episodeId'],
            context=row.get('context'),
            context_start_ms=row.get('contextStartMs'),
            context_end_ms=row.get('contextEndMs'),
            quote_start_ms=row.get('quoteStartMs'),
            quote_end_ms=row.get('quoteEndMs'),
            episode_title=row.get('episodeTitle'),
            podcast_title=row.get('podcastTitle'),
            genre=row.get('genre'),
            guests_name=row.get('guestsName'),
            guests_description=row.get('guestsDescription'),
            quote_rank=row.get('quoteRank'),
            published_date=row.get('publishedDate'),
            quote_audio_url=row.get('quoteAudioUrl'),
            sentiment=row.get('sentiment'),
            speaker_label=row.get('speakerLabel'),
            speaker_name=row.get('speakerName'),
            topic=row.get('topic'),
            quote_description=row.get('quoteDescription'),
            is_synced=row.get('isSynced', False),
            transcript_uri=row.get('transcriptUri'),
            content_type=row.get('contentType', 'Audio'),
            additional_data=additional_data,
            created_at=row.get('createdAt'),
            updated_at=row.get('updatedAt'),
            deleted_at=row.get('deletedAt')
        )

    def to_db_dict(self) -> Dict[str, Any]:
        return {
            'quoteId': self.quote_id,
            'quote': self.quote,
            'channelId': self.channel_id,
            'episodeId': self.episode_id,
            'context': self.context,
            'contextStartMs': self.context_start_ms,
            'contextEndMs': self.context_end_ms,
            'quoteStartMs': self.quote_start_ms,
            'quoteEndMs': self.quote_end_ms,
            'episodeTitle': self.episode_title,
            'podcastTitle': self.podcast_title,
            'genre': self.genre,
            'guestsName': self.guests_name,
            'guestsDescription': self.guests_description,
            'quoteRank': self.quote_rank,
            'publishedDate': self.published_date,
            'quoteAudioUrl': self.quote_audio_url,
            'sentiment': self.sentiment,
            'speakerLabel': self.speaker_label,
            'speakerName': self.speaker_name,
            'topic': self.topic,
            'quoteDescription': self.quote_description,
            'isSynced': self.is_synced,
            'transcriptUri': json.dumps(self.transcript_uri) if self.transcript_uri is not None else None,
            'contentType': self.content_type,
            'additionalData': json.dumps(self.additional_data) if self.additional_data is not None else None,
            'createdAt': self.created_at,
            'updatedAt': self.updated_at,
            'deletedAt': self.deleted_at
        }