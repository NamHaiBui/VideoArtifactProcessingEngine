import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from typing import Optional, List, Dict, Any, Generator
from datetime import datetime

from ..models.quote_model import Quote
from ..models.shorts_model import Short
from ..config import get_db_config

logger = logging.getLogger(__name__)

@contextmanager
def get_db_connection():
    """Context manager for database connections"""
    config = get_db_config()
    conn = None
    try:
        conn = psycopg2.connect(**config)
        yield conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise
    finally:
        if conn:
            conn.close()

@contextmanager
def get_db_cursor(commit=False):
    """Context manager for database cursors"""
    with get_db_connection() as conn:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        try:
            yield cursor
            if commit:
                conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Database operation error: {e}")
            raise
        finally:
            cursor.close()

# Quote Operations
def get_quote_by_id(quote_id: str) -> Optional[Quote]:
    """Retrieve a quote by its ID"""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT * FROM quotes 
            WHERE quote_id = %s AND deleted_at IS NULL
            """,
            (quote_id,)
        )
        row = cursor.fetchone()
        return Quote.from_db_row(dict(row)) if row else None

def create_quote(quote: Quote) -> str:
    """Create a new quote in the database"""
    with get_db_cursor(commit=True) as cursor:
        data = quote.to_db_dict()
        data['created_at'] = data.get('created_at') or datetime.now()
        data['updated_at'] = data.get('updated_at') or datetime.now()
        
        fields = ', '.join(data.keys())
        placeholders = ', '.join(['%s'] * len(data))
        
        cursor.execute(
            f"""
            INSERT INTO quotes ({fields})
            VALUES ({placeholders})
            RETURNING quote_id
            """,
            list(data.values())
        )
        return cursor.fetchone()['quote_id']

def update_quote(quote: Quote) -> bool:
    """Update an existing quote"""
    with get_db_cursor(commit=True) as cursor:
        data = quote.to_db_dict()
        data['updated_at'] = datetime.now()
        
        # Remove ID from update data
        quote_id = data.pop('quote_id')
        
        # Construct SET clause
        set_clause = ', '.join([f"{k} = %s" for k in data.keys()])
        
        cursor.execute(
            f"""
            UPDATE quotes
            SET {set_clause}
            WHERE quote_id = %s AND deleted_at IS NULL
            """,
            list(data.values()) + [quote_id]
        )
        return cursor.rowcount > 0

def delete_quote(quote_id: str) -> bool:
    """Soft delete a quote"""
    with get_db_cursor(commit=True) as cursor:
        cursor.execute(
            """
            UPDATE quotes
            SET deleted_at = %s
            WHERE quote_id = %s AND deleted_at IS NULL
            """,
            (datetime.now(), quote_id)
        )
        return cursor.rowcount > 0

def get_quotes_by_episode_id(episode_id: str) -> List[Quote]:
    """Get all quotes for an episode"""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT * FROM quotes
            WHERE episode_id = %s AND deleted_at IS NULL
            ORDER BY quote_rank ASC NULLS LAST
            """,
            (episode_id,)
        )
        return [Quote.from_db_row(dict(row)) for row in cursor.fetchall()]

def update_quote_video_url(quote_id: str, video_url: str) -> bool:
    """Update the video URL for a quote"""
    with get_db_cursor(commit=True) as cursor:
        cursor.execute(
            """
            UPDATE quotes
            SET quote_audio_url = %s,
                content_type = 'Video',
                updated_at = %s
            WHERE quote_id = %s AND deleted_at IS NULL
            """,
            (video_url, datetime.now(), quote_id)
        )
        return cursor.rowcount > 0

# Shorts (Chunks) Operations
def get_short_by_id(chunk_id: str) -> Optional[Short]:
    """Retrieve a short by its ID"""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT * FROM shorts 
            WHERE chunk_id = %s AND deleted_at IS NULL
            """,
            (chunk_id,)
        )
        row = cursor.fetchone()
        return Short.from_db_record(dict(row)) if row else None

def create_short(short: Short) -> str:
    """Create a new short in the database"""
    with get_db_cursor(commit=True) as cursor:
        data = short.to_db_dict()
        data['created_at'] = data.get('createdAt') or datetime.now()
        data['updated_at'] = data.get('updatedAt') or datetime.now()
        
        # Convert camelCase to snake_case for PostgreSQL
        snake_case_data = {
            key.lower(): value for key, value in data.items()
        }
        
        fields = ', '.join(snake_case_data.keys())
        placeholders = ', '.join(['%s'] * len(snake_case_data))
        
        cursor.execute(
            f"""
            INSERT INTO shorts ({fields})
            VALUES ({placeholders})
            RETURNING chunk_id
            """,
            list(snake_case_data.values())
        )
        return cursor.fetchone()['chunk_id']

def update_short(short: Short) -> bool:
    """Update an existing short"""
    with get_db_cursor(commit=True) as cursor:
        data = short.to_db_dict()
        data['updatedAt'] = datetime.now()
        
        # Convert camelCase to snake_case for PostgreSQL
        snake_case_data = {
            key.lower(): value for key, value in data.items()
        }
        
        # Remove ID from update data
        chunk_id = snake_case_data.pop('chunk_id')
        
        # Construct SET clause
        set_clause = ', '.join([f"{k} = %s" for k in snake_case_data.keys()])
        
        cursor.execute(
            f"""
            UPDATE shorts
            SET {set_clause}
            WHERE chunk_id = %s AND deleted_at IS NULL
            """,
            list(snake_case_data.values()) + [chunk_id]
        )
        return cursor.rowcount > 0

def delete_short(chunk_id: str) -> bool:
    """Soft delete a short"""
    with get_db_cursor(commit=True) as cursor:
        cursor.execute(
            """
            UPDATE shorts
            SET deleted_at = %s
            WHERE chunk_id = %s AND deleted_at IS NULL
            """,
            (datetime.now(), chunk_id)
        )
        return cursor.rowcount > 0

def get_shorts_by_episode_id(episode_id: str) -> List[Short]:
    """Get all shorts for an episode"""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT * FROM shorts
            WHERE episode_id = %s AND deleted_at IS NULL
            ORDER BY start_ms ASC
            """,
            (episode_id,)
        )
        return [Short.from_db_record(dict(row)) for row in cursor.fetchall()]

def update_short_video_url(chunk_id: str, video_url: str) -> bool:
    """Update the video URL for a short"""
    with get_db_cursor(commit=True) as cursor:
        cursor.execute(
            """
            UPDATE shorts
            SET chunk_audio_url = %s,
                content_type = 'Video',
                updated_at = %s
            WHERE chunk_id = %s AND deleted_at IS NULL
            """,
            (video_url, datetime.now(), chunk_id)
        )
        return cursor.rowcount > 0

# Episode Operations
def get_episode_by_id(episode_id: str) -> Optional[Dict[str, Any]]:
    """Retrieve episode metadata by episode ID"""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT * FROM episodes 
            WHERE episode_id = %s AND deleted_at IS NULL
            """,
            (episode_id,)
        )
        row = cursor.fetchone()
        return dict(row) if row else None

def get_episode_by_pk_sk(pk: str, sk: str) -> Optional[Dict[str, Any]]:
    """Retrieve episode metadata by PK and SK (for backward compatibility)"""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT * FROM episodes 
            WHERE episode_id = %s AND channel_id = %s AND deleted_at IS NULL
            """,
            (pk, sk.replace('CHANNEL#', ''))
        )
        row = cursor.fetchone()
        return dict(row) if row else None

def check_episode_chunking_status(episode_id: str) -> str:
    """Check the chunking status of an episode"""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT chunking_status FROM episodes 
            WHERE episode_id = %s AND deleted_at IS NULL
            """,
            (episode_id,)
        )
        row = cursor.fetchone()
        return row['chunking_status'] if row else 'NOT_STARTED'
