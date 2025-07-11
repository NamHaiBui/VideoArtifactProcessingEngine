from typing import Optional, Dict, Any
from datetime import datetime
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from ..config import config

logger = logging.getLogger(__name__)

@contextmanager
def get_db_connection():
    """Create and return a database connection using connection pooling.
    Uses context manager to ensure proper connection handling and closure.
    """
    conn = None
    try:
        # Get database configuration from config
        conn = psycopg2.connect(
            host=config.db_host,
            port=config.db_port,
            database=config.db_name,
            user=config.db_user,
            password=config.db_password,
            cursor_factory=RealDictCursor
        )
        yield conn
    except psycopg2.Error as e:
        logger.error(f"Database connection error: {e}")
        raise
    finally:
        if conn is not None:
            conn.close()
            logger.debug("Database connection closed")

@contextmanager
def get_db_cursor(commit=False):
    """Get a database cursor that automatically handles transactions.
    
    Args:
        commit (bool): Whether to commit the transaction after execution
    """
    with get_db_connection() as conn:
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

def update_quote_video_url(quote_id: str, video_url: str) -> bool:
    """Update the quote with the S3 video URL in RDS.
    
    Args:
        quote_id: The ID of the quote to update
        video_url: The S3 URL of the video
        
    Returns:
        bool: True if update was successful, False otherwise
    """
    try:
        with get_db_cursor(commit=True) as cursor:
            cursor.execute("""
                UPDATE Quotes 
                SET "quoteAudioUrl" = %s,
                    "updatedAt" = %s,
                    "contentType" = 'Video'
                WHERE "quoteId" = %s
            """, (video_url, datetime.utcnow(), quote_id))
            
            affected_rows = cursor.rowcount
            
            if affected_rows > 0:
                logger.info(f"Updated video URL for quote {quote_id}")
                return True
            else:
                logger.warning(f"No quote found with ID {quote_id}")
                return False
                
    except psycopg2.Error as e:
        logger.error(f"Error updating quote video URL: {e}")
        return False
