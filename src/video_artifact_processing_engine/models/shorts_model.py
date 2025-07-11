from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from datetime import datetime

@dataclass
class Short:
    chunk_id: str
    chunk_title: str
    chunk_descriptive_title: str
    chunk_description: str
    chunk_length: float
    episode_id: str
    channel_id: str
    genre: str
    chunk_audio_url: Optional[str] = None
    transcript: Optional[str] = None
    end_ms: int = 0
    published_date: Optional[datetime] = None
    sentiment: Optional[str] = None
    speakers: Optional[str] = None
    start_ms: int = 0
    topics: Optional[str] = None
    podcast_title: str = ""
    episode_title: str = ""
    guests: Optional[str] = None
    guests_description: Optional[str] = None
    host: Optional[str] = None
    host_description: Optional[str] = None
    is_synced: bool = False
    is_used_in_summary: bool = False
    word_timestamps: Optional[str] = None
    content_type: str = "Audio"
    additional_data: Dict[str, Any] = field(default_factory=dict)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    deleted_at: Optional[datetime] = None

    @classmethod
    def from_db_record(cls, record: Dict[str, Any]) -> 'Short':
        """Create a Short instance from a database record."""
        return cls(
            chunk_id=record["chunkId"],
            chunk_title=record["chunkTitle"],
            chunk_descriptive_title=record["chunkDescriptiveTitle"],
            chunk_description=record["chunkDescription"],
            chunk_length=record["chunkLength"],
            episode_id=record["episodeId"],
            channel_id=record["channelId"],
            genre=record["genre"],
            chunk_audio_url=record.get("chunkAudioUrl"),
            transcript=record.get("transcript"),
            end_ms=record["endMs"],
            published_date=record.get("publishedDate"),
            sentiment=record.get("sentiment"),
            speakers=record.get("speakers"),
            start_ms=record["startMs"],
            topics=record.get("topics"),
            podcast_title=record.get("podcastTitle", ""),
            episode_title=record.get("episodeTitle", ""),
            guests=record.get("guests"),
            guests_description=record.get("guestsDescription"),
            host=record.get("host"),
            host_description=record.get("hostDescription"),
            is_synced=record.get("isSynced", False),
            is_used_in_summary=record.get("isUsedInSummary", False),
            word_timestamps=record.get("wordTimestamps"),
            content_type=record.get("contentType", "Audio"),
            additional_data=record.get("additionalData", {}),
            created_at=record.get("createdAt"),
            updated_at=record.get("updatedAt"),
            deleted_at=record.get("deletedAt")
        )

    def to_db_dict(self) -> Dict[str, Any]:
        """Convert the Short instance to a dictionary for database operations."""
        return {
            "chunkId": self.chunk_id,
            "chunkTitle": self.chunk_title,
            "chunkDescriptiveTitle": self.chunk_descriptive_title,
            "chunkDescription": self.chunk_description,
            "chunkLength": self.chunk_length,
            "episodeId": self.episode_id,
            "channelId": self.channel_id,
            "genre": self.genre,
            "chunkAudioUrl": self.chunk_audio_url,
            "transcript": self.transcript,
            "endMs": self.end_ms,
            "publishedDate": self.published_date,
            "sentiment": self.sentiment,
            "speakers": self.speakers,
            "startMs": self.start_ms,
            "topics": self.topics,
            "podcastTitle": self.podcast_title,
            "episodeTitle": self.episode_title,
            "guests": self.guests,
            "guestsDescription": self.guests_description,
            "host": self.host,
            "hostDescription": self.host_description,
            "isSynced": self.is_synced,
            "isUsedInSummary": self.is_used_in_summary,
            "wordTimestamps": self.word_timestamps,
            "contentType": self.content_type,
            "additionalData": self.additional_data,
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
            "deletedAt": self.deleted_at
        }
