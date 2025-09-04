from dataclasses import dataclass, field
import json
from typing import Optional, Dict, Any, List
from datetime import datetime

@dataclass
class Short:
    chunk_id: str = ''
    chunk_title: Optional[str] = ''
    chunk_descriptive_title: Optional[str] = ''
    chunk_description: Optional[str] = ''
    chunk_length: Optional[int] = 0
    episode_id: Optional[str] = ''
    channel_id: Optional[str] = ''
    genre: Optional[str] = ''
    chunk_audio_url: Optional[str] = ''
    transcript: Optional[str] = ''
    end_ms: Optional[int] = 0
    published_date: Optional[datetime] = None
    sentiment: Optional[str] = ''
    speakers: Optional[List[str]] = field(default_factory=list)
    start_ms: Optional[int] = 0
    topics: Optional[List[str]] = field(default_factory=list)
    podcast_title: Optional[str] = ''
    episode_title: Optional[str] = ''
    guests: Optional[List[str]] = field(default_factory=list)
    guests_description: Optional[List[str]] = field(default_factory=list)
    host: Optional[str] = ''
    host_description: Optional[str] = ''
    is_synced: bool = False
    transcript_uri: Optional[str] = ''
    content_type: Optional[str] = "audio"
    additional_data: Dict[str, Any] = field(default_factory=dict)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    deleted_at: Optional[datetime] = None
    genre_id: Optional[str] = ''
    guest_ids: Optional[List[str]] = field(default_factory=list)
    host_id: Optional[str] = ''
    chunk_number: Optional[int] = 0
    is_removed_chunk: bool = False

    @classmethod
    def from_db_record(cls, record: Dict[str, Any]) -> 'Short':
        additional_data = record.get("additionalData", {})
        if additional_data is None:
            additional_data = {}
        return cls(
            chunk_id=record["chunkId"],
            chunk_title=record.get("chunkTitle"),
            chunk_descriptive_title=record.get("chunkDescriptiveTitle"),
            chunk_description=record.get("chunkDescription"),
            chunk_length=record.get("chunkLength"),
            episode_id=record.get("episodeId"),
            channel_id=record.get("channelId"),
            genre=record.get("genre"),
            chunk_audio_url=record.get("chunkAudioUrl"),
            transcript=record.get("transcript"),
            end_ms=record.get("endMs"),
            published_date=record.get("publishedDate"),
            sentiment=record.get("sentiment"),
            speakers=record.get("speakers"),
            start_ms=record.get("startMs"),
            topics=record.get("topics"),
            podcast_title=record.get("podcastTitle"),
            episode_title=record.get("episodeTitle"),
            guests=record.get("guests"),
            guests_description=record.get("guestsDescription"),
            host=record.get("host"),
            host_description=record.get("hostDescription"),
            is_synced=record.get("isSynced", False),
            transcript_uri=record.get("transcriptUri"),
            content_type=record.get("contentType", "Audio"),
            additional_data=additional_data,
            created_at=record.get("createdAt"),
            updated_at=record.get("updatedAt"),
            deleted_at=record.get("deletedAt"),
            genre_id=record.get("genreId"),
            guest_ids=record.get("guestIds"),
            host_id=record.get("hostId"),
            chunk_number=record.get("chunkNumber"),
            is_removed_chunk=record.get("isRemovedChunk", False)
        )

    def to_db_dict(self) -> Dict[str, Any]:
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
            "transcriptUri": self.transcript_uri,
            "contentType": self.content_type,
            "additionalData": json.dumps(self.additional_data) if self.additional_data is not None else None,
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
            "deletedAt": self.deleted_at,
            "genreId": self.genre_id,
            "guestIds": self.guest_ids,
            "hostId": self.host_id,
            "chunkNumber": self.chunk_number,
            "isRemovedChunk": self.is_removed_chunk
        }
