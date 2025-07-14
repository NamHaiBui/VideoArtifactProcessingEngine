import asyncio
import os
from typing import Optional, Dict, Any, List
from datetime import datetime
import logging
import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch
from contextlib import contextmanager

from video_artifact_processing_engine.aws.aws_client import S3Service
from video_artifact_processing_engine.models.episode_model import Episode
from ..config import config
import threading
import atexit

from video_artifact_processing_engine.models.quote_model import Quote
from video_artifact_processing_engine.models.shorts_model import Short

logger = logging.getLogger(__name__)

_db_connection = None
_db_lock = threading.Lock()

def get_persistent_db_connection():
    """Get or create a persistent database connection."""
    global _db_connection
    with _db_lock:
        if _db_connection is None or _db_connection.closed:
            try:
                _db_connection = psycopg2.connect(
                    host=config.db_host,
                    port=config.db_port,
                    database=config.db_name,
                    user=config.db_user,
                    password=config.db_password,
                    cursor_factory=RealDictCursor
                )
                logger.debug("Persistent DB connection established")
            except psycopg2.Error as e:
                logger.error(f"Database connection error: {e}")
                raise
        return _db_connection

@contextmanager
def get_db_cursor(commit=False):
    """Get a database cursor from the persistent connection.
    
    Args:
        commit (bool): Whether to commit the transaction after execution
    """
    conn = get_persistent_db_connection()
    cursor = conn.cursor()
    try:
        yield cursor
        if commit:
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise
    finally:
        cursor.close()

def close_persistent_db_connection():
    global _db_connection
    with _db_lock:
        if _db_connection is not None and not _db_connection.closed:
            _db_connection.close()
            logger.debug("Persistent DB connection closed gracefully")

atexit.register(close_persistent_db_connection)

# Quote Operations

def get_quote_by_id(quoteId: str) -> Optional[Quote]:
    """Retrieve a quote by its ID"""
    with get_db_cursor() as cursor:
        cursor.execute(
            '''
            SELECT * FROM "Quotes"
            WHERE "quoteId" = %s AND "deletedAt" IS NULL
            ''',
            (quoteId,)
        )
        row = cursor.fetchone()
        return Quote.from_db_row(dict(row)) if row else None


async def update_quote(quote: Quote) -> bool:
    """Update a single existing quote"""
    with get_db_cursor(commit=True) as cursor:
        data = quote.to_db_dict()
        data['updatedAt'] = datetime.utcnow()
        quoteId = data.pop('quoteId')
        set_clause = ', '.join([f'"{k}" = %s' for k in data.keys()])
        cursor.execute(
            f'''
            UPDATE "Quotes"
            SET {set_clause}
            WHERE "quoteId" = %s AND "deletedAt" IS NULL
            ''',
            list(data.values()) + [quoteId]
        )
        return cursor.rowcount > 0

async def update_quotes(quotes: List[Quote]) -> bool:
    """
    Update a list of existing quotes in a single batch operation.
    """
    if not quotes:
        return True
        
    with get_db_cursor(commit=True) as cursor:
        # Prepare the data for batch execution
        # The WHERE clause value must be last in the tuple
        update_data = []
        for quote in quotes:
            data = quote.to_db_dict()
            data['updatedAt'] = datetime.utcnow()
            quoteId = data.pop('quoteId')
            # Ensure consistent order for SET and VALUES
            ordered_values = list(data.values())
            ordered_values.append(quoteId)
            update_data.append(tuple(ordered_values))

        # Get keys from the first item to build the SET clause
        first_quote_data = quotes[0].to_db_dict()
        first_quote_data.pop('quoteId')
        set_clause = ', '.join([f'"{k}" = %s' for k in first_quote_data.keys()])
        
        # Prepare the statement
        sql_statement = f'''
            UPDATE "Quotes"
            SET {set_clause}
            WHERE "quoteId" = %s AND "deletedAt" IS NULL
        '''
        
        execute_batch(cursor, sql_statement, update_data)
        
    return True


def get_quotes_by_episode_id(episodeId: str) -> List[Quote]:
    """Get all quotes for an episode"""
    with get_db_cursor() as cursor:
        cursor.execute(
            '''
            SELECT * FROM "Quotes"
            WHERE "episodeId" = %s AND "deletedAt" IS NULL
            ORDER BY "quoteRank" ASC NULLS LAST
            ''',
            (episodeId,)
        )
        return [Quote.from_db_row(dict(row)) for row in cursor.fetchall()]

async def update_quote_video_url(quoteId: str, quoteAudioUrl: str) -> bool:
    """Update the video URL for a quote"""
    with get_db_cursor(commit=True) as cursor:
        cursor.execute(
            '''
            UPDATE "Quotes"
            SET "quoteAudioUrl" = %s,
                "contentType" = 'Video',
                "updatedAt" = %s
            WHERE "quoteId" = %s AND "deletedAt" IS NULL
            ''',
            (quoteAudioUrl, datetime.utcnow(), quoteId)
        )
        return cursor.rowcount > 0

# Shorts / Chunks Operations

def get_short_by_id(chunkId: str) -> Optional[Short]:
    """Retrieve a short by its ID"""
    with get_db_cursor() as cursor:
        cursor.execute(
            '''
            SELECT * FROM "Shorts"
            WHERE "chunkId" = %s AND "deletedAt" IS NULL
            ''',
            (chunkId,)
        )
        row = cursor.fetchone()
        return Short.from_db_record(dict(row)) if row else None


async def update_short(short: Short) -> bool:
    """Update a single existing short"""
    with get_db_cursor(commit=True) as cursor:
        data = short.to_db_dict()
        data['updatedAt'] = datetime.utcnow()
        chunkId = data.pop('chunkId')
        set_clause = ', '.join([f'"{k}" = %s' for k in data.keys()])
        cursor.execute(
            f'''
            UPDATE "Shorts"
            SET {set_clause}
            WHERE "chunkId" = %s AND "deletedAt" IS NULL
            ''',
            list(data.values()) + [chunkId]
        )
        return cursor.rowcount > 0

async def update_shorts(shorts: List[Short]) -> bool:
    """
    Update a list of existing shorts (chunks) in a single batch operation.
    """
    if not shorts:
        return True

    with get_db_cursor(commit=True) as cursor:
        # Prepare the data for batch execution
        update_data = []
        for short in shorts:
            data = short.to_db_dict()
            data['updatedAt'] = datetime.utcnow()
            chunkId = data.pop('chunkId')
            ordered_values = list(data.values())
            ordered_values.append(chunkId)
            update_data.append(tuple(ordered_values))

        # Get keys from the first item to build the SET clause
        first_short_data = shorts[0].to_db_dict()
        first_short_data.pop('chunkId')
        set_clause = ', '.join([f'"{k}" = %s' for k in first_short_data.keys()])
        
        # Prepare the statement
        sql_statement = f'''
            UPDATE "Shorts"
            SET {set_clause}
            WHERE "chunkId" = %s AND "deletedAt" IS NULL
        '''
        
        execute_batch(cursor, sql_statement, update_data)
        
    return True

def get_shorts_by_episode_id(episodeId: str) -> List[Short]:
    """Get all shorts for an episode"""
    with get_db_cursor() as cursor:
        cursor.execute(
            '''
            SELECT * FROM "Shorts"
            WHERE "episodeId" = %s AND "deletedAt" IS NULL
            ORDER BY "startMs" ASC
            ''',
            (episodeId,)
        )
        return [Short.from_db_record(dict(row)) for row in cursor.fetchall()]

async def update_short_video_url(chunkId: str, chunkAudioUrl: str) -> bool:
    """Update the video URL for a short"""
    with get_db_cursor(commit=True) as cursor:
        cursor.execute(
            '''
            UPDATE "Shorts"
            SET "chunkAudioUrl" = %s,
                "contentType" = 'Video',
                "updatedAt" = %s
            WHERE "chunkId" = %s AND "deletedAt" IS NULL
            ''',
            (chunkAudioUrl, datetime.utcnow(), chunkId)
        )
        return cursor.rowcount > 0

# Episode Operations

def get_episode_by_id(episodeId: str) -> Optional[Episode]:
    """Retrieve episode metadata by episode ID and return as Episode object"""
    from video_artifact_processing_engine.models.episode_model import Episode
    with get_db_cursor() as cursor:
        cursor.execute(
            '''
            SELECT * FROM "Episodes"
            WHERE "episodeId" = %s AND "deletedAt" IS NULL
            ''',
            (episodeId,)
        )
        row = cursor.fetchone()
        return Episode.from_db_row(dict(row)) if row else None

def get_episode_processing_status(episodeId: str) -> Optional[Dict[str, Any]]:
    """Retrieve the processingInfo JSON for an episode and return its status fields as a Python object."""
    from video_artifact_processing_engine.models.episode_model import Episode
    with get_db_cursor() as cursor:
        cursor.execute(
            '''
            SELECT * FROM "Episodes"
            WHERE "episodeId" = %s AND "deletedAt" IS NULL
            ''',
            (episodeId,)
        )
        row = cursor.fetchone()
        if row is not None and isinstance(row, dict):
            episode = Episode.from_db_row(dict(row))
            info = episode.processing_info or {}
            return {
                'episodeTranscribingDone': info.get('episodeTranscribingDone', False),
                'summaryTranscribingDone': info.get('summaryTranscribingDone', False),
                'summarizingDone': info.get('summarizingDone', False),
                'numChunks': info.get('numChunks', 0),
                'numRemovedChunks': info.get('numRemovedChunks', 0),
                'chunkingDone': info.get('chunkingDone', False),
                'quotingDone': info.get('quotingDone', False)
            }
        return None
    

async def upload_with_retry(s3_service: S3Service, file_path: str, bucket: str, key: str, max_retries=3):
    for attempt in range(1, max_retries + 1):
        try:
            await asyncio.to_thread(s3_service.upload_file, file_path, bucket, key)
            logger.info(f"  - Successfully uploaded {os.path.basename(file_path)} to {key} on attempt {attempt}.")
            return
        except Exception as e:
            logger.error(f"  - Attempt {attempt} failed for {key}: {e}")
            if attempt == max_retries:
                raise e
            await asyncio.sleep(2 ** attempt)