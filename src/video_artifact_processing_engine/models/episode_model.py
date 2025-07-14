from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from datetime import datetime

@dataclass
class Episode:
    episode_id: str
    episode_title: str
    episode_description: Optional[str] = None
    host_name: Optional[str] = None
    host_description: Optional[str] = None
    channel_name: Optional[str] = None
    guests: Optional[List[str]] = None
    guest_descriptions: Optional[List[str]] = None
    guest_image_url: Optional[str] = None
    published_date: Optional[datetime] = None
    episode_uri: Optional[str] = None
    original_uri: Optional[str] = None
    channel_id: Optional[str] = None
    country: Optional[str] = None
    genre: Optional[str] = None
    episode_images: Optional[List[str]] = None
    duration_millis: Optional[int] = None
    rss_url: Optional[str] = None
    transcript_uri: Optional[str] = None
    processed_transcript_uri: Optional[str] = None
    summary_audio_uri: Optional[str] = None
    summary_duration_millis: Optional[int] = None
    summary_transcript_uri: Optional[str] = None
    topics: Optional[List[str]] = None
    updated_at: Optional[datetime] = None
    deleted_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    processing_info: Optional[Dict[str, Any]] = None
    content_type: Optional[str] = None
    additional_data: Dict[str, Any] = field(default_factory=dict)
    processing_done: Optional[bool] = None
    is_synced: Optional[bool] = None

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'Episode':
        return cls(
            episode_id=row['episodeId'],
            episode_title=row['episodeTitle'],
            episode_description=row.get('episodeDescription'),
            host_name=row.get('hostName'),
            host_description=row.get('hostDescription'),
            channel_name=row.get('channelName'),
            guests=row.get('guests'),
            guest_descriptions=row.get('guestDescriptions'),
            guest_image_url=row.get('guestImageUrl'),
            published_date=row.get('publishedDate'),
            episode_uri=row.get('episodeUri'),
            original_uri=row.get('originalUri'),
            channel_id=row.get('channelId'),
            country=row.get('country'),
            genre=row.get('genre'),
            episode_images=row.get('episodeImages'),
            duration_millis=row.get('durationMillis'),
            rss_url=row.get('rssUrl'),
            transcript_uri=row.get('transcriptUri'),
            processed_transcript_uri=row.get('processedTranscriptUri'),
            summary_audio_uri=row.get('summaryAudioUri'),
            summary_duration_millis=row.get('summaryDurationMillis'),
            summary_transcript_uri=row.get('summaryTranscriptUri'),
            topics=row.get('topics'),
            updated_at=row.get('updatedAt'),
            deleted_at=row.get('deletedAt'),
            created_at=row.get('createdAt'),
            processing_info=row.get('processingInfo'),
            content_type=row.get('contentType'),
            additional_data=row.get('additionalData', {}),
            processing_done=row.get('processingDone'),
            is_synced=row.get('isSynced')
        )

    def to_db_dict(self) -> Dict[str, Any]:
        return {
            'episodeId': self.episode_id,
            'episodeTitle': self.episode_title,
            'episodeDescription': self.episode_description,
            'hostName': self.host_name,
            'hostDescription': self.host_description,
            'channelName': self.channel_name,
            'guests': self.guests,
            'guestDescriptions': self.guest_descriptions,
            'guestImageUrl': self.guest_image_url,
            'publishedDate': self.published_date,
            'episodeUri': self.episode_uri,
            'originalUri': self.original_uri,
            'channelId': self.channel_id,
            'country': self.country,
            'genre': self.genre,
            'episodeImages': self.episode_images,
            'durationMillis': self.duration_millis,
            'rssUrl': self.rss_url,
            'transcriptUri': self.transcript_uri,
            'processedTranscriptUri': self.processed_transcript_uri,
            'summaryAudioUri': self.summary_audio_uri,
            'summaryDurationMillis': self.summary_duration_millis,
            'summaryTranscriptUri': self.summary_transcript_uri,
            'topics': self.topics,
            'updatedAt': self.updated_at,
            'deletedAt': self.deleted_at,
            'createdAt': self.created_at,
            'processingInfo': self.processing_info,
            'contentType': self.content_type,
            'additionalData': self.additional_data,
            'processingDone': self.processing_done,
            'isSynced': self.is_synced
        }