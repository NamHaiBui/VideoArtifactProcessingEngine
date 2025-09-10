import asyncio
import os
import json
import time
import random
from typing import Optional, Dict, Any, List, Callable
from datetime import datetime
import logging
import psycopg2
from psycopg2 import errorcodes
from psycopg2.extras import RealDictCursor, execute_batch
from psycopg2.pool import ThreadedConnectionPool
from contextlib import contextmanager
import threading
import atexit

from video_artifact_processing_engine.aws.aws_client import S3Service
from video_artifact_processing_engine.models.episode_model import Episode
from ..config import config

from video_artifact_processing_engine.models.quote_model import Quote
from video_artifact_processing_engine.models.shorts_model import Short

logger = logging.getLogger(__name__)

# Connection pool for ACID compliance
_connection_pool = None
_pool_lock = threading.Lock()

def get_connection_pool():
    """Get or create a connection pool for ACID compliance."""
    global _connection_pool
    with _pool_lock:
        if _connection_pool is None:
            try:
                _connection_pool = ThreadedConnectionPool(
                    minconn=1,
                    maxconn=20,
                    host=config.db_host,
                    port=config.db_port,
                    database=config.db_name,
                    user=config.db_user,
                    password=config.db_password,
                    cursor_factory=RealDictCursor,
                    # Additional connection parameters for ACID compliance
                    connect_timeout=10,
                    application_name="VideoArtifactProcessingEngine"
                    # Note: We set isolation level per connection in get_db_connection()
                )
                logger.debug("Database connection pool established with ACID compliance settings")
            except psycopg2.Error as e:
                logger.error(f"Database connection pool creation error: {e}")
                raise e
        return _connection_pool

@contextmanager
def get_db_connection():
    """Get a database connection from the pool with proper ACID transaction handling."""
    pool = get_connection_pool()
    conn = None
    try:
        conn = pool.getconn()
        if conn:
            # Set isolation level for ACID compliance
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
            # Best-effort timeouts to prevent indefinite blocking
            with conn.cursor() as _c:
                try:
                    # Limit statement execution and lock waiting per transaction (minimize lockouts)
                    _c.execute("SET idle_in_transaction_session_timeout = '300s'")
                    _c.execute("SET statement_timeout = '120s'")
                    _c.execute("SET lock_timeout = '1s'")
                except Exception:
                    # Ignore if not supported or insufficient privileges
                    pass
            yield conn
        else:
            raise psycopg2.Error("Failed to get connection from pool")
    except Exception as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        if conn:
            pool.putconn(conn)

@contextmanager
def get_db_cursor(commit=False, connection=None):
    """Get a database cursor with proper transaction management.
    
    Args:
        commit (bool): Whether to commit the transaction after execution
        connection: Optional existing connection to use (for multi-operation transactions)
    """
    if connection:
        # Use provided connection (for multi-operation transactions)
        cursor = connection.cursor()
        try:
            yield cursor
            if commit:
                connection.commit()
        except Exception as e:
            connection.rollback()
            raise e
        finally:
            cursor.close()
    else:
        # Get new connection from pool
        with get_db_connection() as conn:
            cursor = conn.cursor()
            try:
                yield cursor
                if commit:
                    conn.commit()
            except Exception as e:
                conn.rollback()
                raise e
            finally:
                cursor.close()

@contextmanager
def get_db_transaction():
    """Get a database transaction context for multi-operation ACID compliance."""
    with get_db_connection() as conn:
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Transaction rolled back due to error: {e}")
            raise e

def _sleep_backoff(attempt: int, base: float = 0.2, factor: float = 2.0, jitter: float = 0.2, cap: float = 5.0):
    delay = min(cap, base * (factor ** attempt)) + random.uniform(0, jitter)
    time.sleep(delay)

def _is_retryable_db_error(exc: BaseException) -> bool:
    # Consider common transient/locking errors retryable
    text = str(exc).lower()
    pgcode = getattr(exc, 'pgcode', None)
    retryable_codes = {
        errorcodes.SERIALIZATION_FAILURE,  # '40001'
        errorcodes.DEADLOCK_DETECTED,      # '40P01'
        errorcodes.LOCK_NOT_AVAILABLE,     # '55P03'
        errorcodes.QUERY_CANCELED,         # '57014' (may happen due to statement/lock timeout)
        errorcodes.TRANSACTION_RESOLUTION_UNKNOWN,
        errorcodes.ADMIN_SHUTDOWN,
        errorcodes.CRASH_SHUTDOWN,
        errorcodes.CANNOT_CONNECT_NOW,
    }
    connection_indicators = (
        'could not connect', 'connection refused', 'server closed the connection', 'terminating connection',
        'connection timed out', 'network is unreachable', 'connection reset'
    )
    if pgcode and pgcode in retryable_codes:
        return True
    if isinstance(exc, psycopg2.OperationalError):
        return True
    return any(s in text for s in connection_indicators)

def run_transaction_with_retry(transaction_fn: Callable[[psycopg2.extensions.connection], Any], *,
                               max_attempts: int = 5,
                               on_retry_log: str = 'DB transaction') -> Any:
    last_exc: Optional[BaseException] = None
    for attempt in range(max_attempts):
        try:
            with get_db_connection() as conn:
                # Start a transaction scope by executing a no-op
                with conn.cursor() as c:
                    try:
                        c.execute("SELECT 1")
                    except Exception:
                        pass
                result = transaction_fn(conn)
                # If inner function didn't commit/rollback, commit here
                try:
                    conn.commit()
                except Exception as e:
                    # Treat commit failures as retryable if transient
                    if _is_retryable_db_error(e) and attempt < max_attempts - 1:
                        logger.warning(f"{on_retry_log} commit failed (attempt {attempt+1}/{max_attempts}), retrying: {e}")
                        last_exc = e
                        _sleep_backoff(attempt)
                        continue
                    raise
                return result
        except Exception as e:
            if _is_retryable_db_error(e) and attempt < max_attempts - 1:
                logger.warning(f"{on_retry_log} failed (attempt {attempt+1}/{max_attempts}), retrying: {e}")
                last_exc = e
                _sleep_backoff(attempt)
                continue
            last_exc = e
            break
    if last_exc:
        raise last_exc

def _try_acquire_advisory_xact_lock_nowait(cursor, scope: str, entity_id: str) -> bool:
    """Attempt to acquire a transaction-scoped advisory lock without waiting.
    Returns True if acquired; False if not.
    """
    cursor.execute("SELECT pg_try_advisory_xact_lock(hashtext(%s), hashtext(%s)) AS locked", (scope, entity_id))
    locked = cursor.fetchone()
    return bool(dict(locked).get('locked')) if locked is not None else False

def close_connection_pool():
    """Close the connection pool gracefully."""
    global _connection_pool
    with _pool_lock:
        if _connection_pool is not None:
            _connection_pool.closeall()
            _connection_pool = None
            logger.debug("Database connection pool closed gracefully")

atexit.register(close_connection_pool)

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
    """Update a single existing quote with minimal column changes and nowait lock + retry."""
    def _txn(conn):
        with conn.cursor() as cursor:
            if not _try_acquire_advisory_xact_lock_nowait(cursor, 'Quotes', quote.quote_id):
                logger.info(f"Skipping update_quote for {quote.quote_id}: lock not available (nowait)")
                return False
            # Load current row
            cursor.execute(
                '''
                SELECT * FROM "Quotes"
                WHERE "quoteId" = %s AND "deletedAt" IS NULL
                ''',
                (quote.quote_id,)
            )
            row = cursor.fetchone()
            if not row:
                raise psycopg2.OperationalError(f"Quote {quote.quote_id} not found for update")

            current = Quote.from_db_row(dict(row))
            changes: Dict[str, Any] = {}
            # Only update fields typically changed in this flow
            if quote.additional_data is not None and quote.additional_data != current.additional_data:
                changes['additionalData'] = json.dumps(quote.additional_data)
            if quote.content_type is not None and quote.content_type != current.content_type:
                changes['contentType'] = quote.content_type
            # If quote_audio_url changed (should usually be handled by update_quote_video_url)
            if quote.quote_audio_url is not None and quote.quote_audio_url != current.quote_audio_url:
                changes['quoteAudioUrl'] = quote.quote_audio_url

            if not changes:
                logger.debug(f"No changes detected for quote {quote.quote_id}; skipping update")
                return True

            changes['updatedAt'] = datetime.utcnow()
            set_clause = ', '.join([f'"{k}" = %s' for k in changes.keys()])
            params = list(changes.values()) + [quote.quote_id]
            cursor.execute(
                f'''
                UPDATE "Quotes"
                SET {set_clause}
                WHERE "quoteId" = %s AND "deletedAt" IS NULL
                RETURNING "updatedAt"
                ''',
                params
            )
            row = cursor.fetchone()
            if not row:
                raise psycopg2.OperationalError("No rows updated")
            logger.debug(f"Successfully updated quote {quote.quote_id} at {dict(row)['updatedAt']}")
            return True
    return run_transaction_with_retry(_txn, on_retry_log='Update quote')

async def update_quotes(quotes: List[Quote]) -> bool:
    """
    Update a list of existing quotes in a single batch operation with ACID compliance.
    """
    if not quotes:
        return True
    
    # Reduce lock-out by splitting into small transactional chunks
    batch_size = max(1, int(os.getenv('DB_UPDATE_BATCH_SIZE', '20')))
    start = 0
    total = len(quotes)
    while start < total:
        subset = quotes[start:start+batch_size]
        def _txn(conn, _subset=subset):
            with conn.cursor() as cursor:
                # Try to acquire locks without waiting; only update rows we lock
                locked_subset: List[Quote] = []
                for q in _subset:
                    if _try_acquire_advisory_xact_lock_nowait(cursor, 'Quotes', q.quote_id):
                        locked_subset.append(q)

                # If none acquired, skip this chunk
                if not locked_subset:
                    logger.info("No quotes in chunk acquired lock; skipping (nowait)")
                    return True

                update_data = []
                now_marker = datetime.utcnow()
                for quote in locked_subset:
                    data = quote.to_db_dict()
                    data['updatedAt'] = now_marker
                    quoteId = data.pop('quoteId')
                    ordered_values = list(data.values())
                    ordered_values.append(quoteId)
                    update_data.append(tuple(ordered_values))

                first_quote_data = locked_subset[0].to_db_dict()
                first_quote_data.pop('quoteId')
                set_clause = ', '.join([f'"{k}" = %s' for k in first_quote_data.keys()])
                sql_statement = f'''
                    UPDATE "Quotes"
                    SET {set_clause}
                    WHERE "quoteId" = %s AND "deletedAt" IS NULL
                '''
                execute_batch(cursor, sql_statement, update_data)

                # Validate only the locked subset we attempted
                id_list = [q.quote_id for q in locked_subset]
                cursor.execute(
                    '''
                    SELECT COUNT(*) AS cnt FROM "Quotes"
                    WHERE "quoteId" = ANY(%s) AND "deletedAt" IS NULL AND "updatedAt" >= %s
                    ''',
                    (id_list, now_marker)
                )
                cnt = int(dict(cursor.fetchone())['cnt'])
                if cnt != len(locked_subset):
                    raise psycopg2.OperationalError(f"Batch quotes validation failed: expected {len(locked_subset)}, verified {cnt}")
                logger.debug(f"Successfully updated {cnt} quotes in chunk")
                return True
        run_transaction_with_retry(_txn, on_retry_log='Batch update quotes (chunk)')
        start += batch_size
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


async def update_quote_additional_data(
    quoteId: str,
    additional_data: Dict[str, Any],
    content_type: Optional[str] = None,
    max_lock_retries: int = 5,
    lock_retry_delay: float = 0.15,
) -> bool:
    """Update only additionalData (and optionally contentType) for a quote.
    Adds lightweight retry for advisory lock acquisition (nowait) so we don't silently skip updates.
    """
    def _txn(conn):
        with conn.cursor() as cursor:
            for attempt in range(max_lock_retries):
                if _try_acquire_advisory_xact_lock_nowait(cursor, 'Quotes', quoteId):
                    break
                if attempt == max_lock_retries - 1:
                    logger.warning(f"update_quote_additional_data lock not acquired after {max_lock_retries} attempts for {quoteId}")
                    return False
                time.sleep(lock_retry_delay)
            now_ts = datetime.utcnow()
            set_fragments = ['"additionalData" = %s::jsonb', '"updatedAt" = %s']
            params: List[Any] = [json.dumps(additional_data or {}), now_ts]
            if content_type is not None:
                set_fragments.insert(1, '"contentType" = %s')
                params.insert(1, content_type)
            sql = f'''
                UPDATE "Quotes"
                SET {', '.join(set_fragments)}
                WHERE "quoteId" = %s AND "deletedAt" IS NULL
                RETURNING "additionalData", "contentType", "updatedAt"
            '''
            params.append(quoteId)
            cursor.execute(sql, params)
            row = cursor.fetchone()
            if not row:
                raise psycopg2.OperationalError("No rows updated when setting quote additionalData")
            return True
    return run_transaction_with_retry(_txn, on_retry_log='Update quote additional data')

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
    """Update a single existing short minimally with lock nowait and retry."""
    def _txn(conn):
        with conn.cursor() as cursor:
            if not _try_acquire_advisory_xact_lock_nowait(cursor, 'Shorts', short.chunk_id):
                logger.info(f"Skipping update_short for {short.chunk_id}: lock not available (nowait)")
                return False
            # Load current row
            cursor.execute(
                '''
                SELECT * FROM "Shorts"
                WHERE "chunkId" = %s AND "deletedAt" IS NULL
                ''',
                (short.chunk_id,)
            )
            row = cursor.fetchone()
            if not row:
                raise psycopg2.OperationalError(f"Short {short.chunk_id} not found for update")

            current = Short.from_db_record(dict(row))
            changes: Dict[str, Any] = {}
            if short.additional_data is not None and short.additional_data != current.additional_data:
                changes['additionalData'] = json.dumps(short.additional_data)
            if short.content_type is not None and short.content_type != current.content_type:
                changes['contentType'] = short.content_type
            # If chunk_audio_url changed (usually handled by update_short_video_url)
            if short.chunk_audio_url is not None and short.chunk_audio_url != current.chunk_audio_url:
                changes['chunkAudioUrl'] = short.chunk_audio_url

            if not changes:
                logger.debug(f"No changes detected for short {short.chunk_id}; skipping update")
                return True

            changes['updatedAt'] = datetime.utcnow()
            set_clause = ', '.join([f'"{k}" = %s' for k in changes.keys()])
            params = list(changes.values()) + [short.chunk_id]
            cursor.execute(
                f'''
                UPDATE "Shorts"
                SET {set_clause}
                WHERE "chunkId" = %s AND "deletedAt" IS NULL
                RETURNING "updatedAt"
                ''',
                params
            )
            row = cursor.fetchone()
            if not row:
                raise psycopg2.OperationalError("No rows updated for short")
            logger.debug(f"Successfully updated short {short.chunk_id} at {dict(row)['updatedAt']}")
            return True
    return run_transaction_with_retry(_txn, on_retry_log='Update short')

async def update_shorts(shorts: List[Short]) -> bool:
    """
    Update a list of existing shorts (chunks) in a single batch operation with ACID compliance.
    """
    if not shorts:
        return True

    batch_size = max(1, int(os.getenv('DB_UPDATE_BATCH_SIZE', '20')))
    start = 0
    total = len(shorts)
    while start < total:
            subset = shorts[start:start+batch_size]
            def _txn(conn, _subset=subset):
                with conn.cursor() as cursor:
                    # Acquire locks without waiting; update only locked rows
                    locked_subset: List[Short] = []
                    for s in _subset:
                        if _try_acquire_advisory_xact_lock_nowait(cursor, 'Shorts', s.chunk_id):
                            locked_subset.append(s)

                    if not locked_subset:
                        logger.info("No shorts in chunk acquired lock; skipping (nowait)")
                        return True

                    update_data = []
                    now_marker = datetime.utcnow()
                    for short in locked_subset:
                        data = short.to_db_dict()
                        data['updatedAt'] = now_marker
                        chunkId = data.pop('chunkId')
                        ordered_values = list(data.values())
                        ordered_values.append(chunkId)
                        update_data.append(tuple(ordered_values))

                    first_short_data = locked_subset[0].to_db_dict()
                    first_short_data.pop('chunkId')
                    set_clause = ', '.join([f'"{k}" = %s' for k in first_short_data.keys()])
                    sql_statement = f'''
                        UPDATE "Shorts"
                        SET {set_clause}
                        WHERE "chunkId" = %s AND "deletedAt" IS NULL
                    '''
                    execute_batch(cursor, sql_statement, update_data)
                    id_list = [s.chunk_id for s in locked_subset]
                    cursor.execute(
                        '''
                        SELECT COUNT(*) AS cnt FROM "Shorts"
                        WHERE "chunkId" = ANY(%s) AND "deletedAt" IS NULL AND "updatedAt" >= %s
                        ''',
                        (id_list, now_marker)
                    )
                    cnt = int(dict(cursor.fetchone())['cnt'])
                    if cnt != len(locked_subset):
                        raise psycopg2.OperationalError(f"Batch shorts validation failed: expected {len(locked_subset)}, verified {cnt}")
                    logger.debug(f"Successfully updated {cnt} shorts in chunk")
                    return True
            run_transaction_with_retry(_txn, on_retry_log='Batch update shorts (chunk)')
            start += batch_size
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

def get_quotes_and_shorts_by_episode_id(episodeId: str) -> Dict[str, List[Any]]:
    """Fetch quotes and shorts for an episode within a single connection for consistency."""
    results = { 'quotes': [], 'shorts': [] }
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            # Quotes
            cursor.execute(
                '''
                SELECT * FROM "Quotes"
                WHERE "episodeId" = %s AND "deletedAt" IS NULL
                ORDER BY "quoteRank" ASC NULLS LAST
                ''',
                (episodeId,)
            )
            quotes_rows = cursor.fetchall() or []
            results['quotes'] = [Quote.from_db_row(dict(row)) for row in quotes_rows]
            # Shorts
            cursor.execute(
                '''
                SELECT * FROM "Shorts"
                WHERE "episodeId" = %s AND "deletedAt" IS NULL
                ORDER BY "startMs" ASC
                ''',
                (episodeId,)
            )
            shorts_rows = cursor.fetchall() or []
            results['shorts'] = [Short.from_db_record(dict(row)) for row in shorts_rows]
    return results

async def update_short_video_url(chunkId: str, _unused: str) -> bool:
    """Preserve original audio URL; only set contentType to 'video' if not already."""
    def _txn(conn):
        with conn.cursor() as cursor:
            if not _try_acquire_advisory_xact_lock_nowait(cursor, 'Shorts', chunkId):
                logger.info(f"Skipping update_short_video_url for {chunkId}: lock not available (nowait)")
                return False
            now_ts = datetime.utcnow()
            cursor.execute(
                '''
                UPDATE "Shorts"
                SET "contentType" = 'video',
                    "updatedAt" = %s
                WHERE "chunkId" = %s AND "deletedAt" IS NULL AND ("contentType" IS NULL OR LOWER("contentType") <> 'video')
                RETURNING "updatedAt"
                ''',
                (now_ts, chunkId)
            )
            # success even if nothing changed (already video)
            return True
    return run_transaction_with_retry(_txn, on_retry_log='Update short video (contentType only)')

async def update_short_additional_data(
    chunkId: str,
    additional_data: Dict[str, Any],
    content_type: Optional[str] = None,
) -> bool:
    """Update only additionalData (and optionally contentType) for a short."""
    def _txn(conn):
        with conn.cursor() as cursor:
            if not _try_acquire_advisory_xact_lock_nowait(cursor, 'Shorts', chunkId):
                logger.info(f"Skipping update_short_additional_data for {chunkId}: lock not available (nowait)")
                return False
            now_ts = datetime.utcnow()
            set_frags = ['"additionalData" = %s::jsonb', '"updatedAt" = %s']
            params: List[Any] = [json.dumps(additional_data or {}), now_ts]
            if content_type is not None:
                set_frags.insert(1, '"contentType" = %s')
                params.insert(1, content_type)
            sql = f'''
                UPDATE "Shorts"
                SET {', '.join(set_frags)}
                WHERE "chunkId" = %s AND "deletedAt" IS NULL
                RETURNING "additionalData", "contentType", "updatedAt"
            '''
            params.append(chunkId)
            cursor.execute(sql, params)
            row = cursor.fetchone()
            if not row:
                raise psycopg2.OperationalError("No rows updated when setting short additionalData")
            return True
    import json
    return run_transaction_with_retry(_txn, on_retry_log='Update short additional data')

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
            return info
        return None
    

def update_episode_item(episode: Episode) -> bool:
    """Update episode minimally: only changed fields to limit lock scope; retry with backoff."""
    def _txn(conn):
        with conn.cursor() as cursor:
            if not _try_acquire_advisory_xact_lock_nowait(cursor, 'Episodes', episode.episode_id):
                logger.info(f"Skipping update_episode_item for {episode.episode_id}: lock not available (nowait)")
                return False

            # Load current row to compute diffs
            cursor.execute(
                '''
                SELECT * FROM "Episodes"
                WHERE "episodeId" = %s AND "deletedAt" IS NULL
                ''',
                (episode.episode_id,)
            )
            row = cursor.fetchone()
            if not row:
                raise psycopg2.OperationalError(f"Episode {episode.episode_id} not found for update")

            current = Episode.from_db_row(dict(row))
            changes: Dict[str, Any] = {}
            if episode.processing_info is not None and episode.processing_info != current.processing_info:
                changes['processingInfo'] = json.dumps(episode.processing_info)
            if episode.content_type is not None and episode.content_type != current.content_type:
                changes['contentType'] = episode.content_type
            if episode.additional_data is not None and episode.additional_data != current.additional_data:
                changes['additionalData'] = json.dumps(episode.additional_data)

            if not changes:
                logger.debug(f"No changes detected for episode {episode.episode_id}; skipping update")
                return True

            changes['updatedAt'] = datetime.utcnow()
            set_clause = ', '.join([f'"{k}" = %s' for k in changes.keys()])
            params = list(changes.values()) + [episode.episode_id]

            cursor.execute(
                f'''
                UPDATE "Episodes"
                SET {set_clause}
                WHERE "episodeId" = %s AND "deletedAt" IS NULL
                RETURNING "updatedAt"
                ''',
                params
            )
            row = cursor.fetchone()
            if not row:
                raise psycopg2.OperationalError("No rows updated for episode")
            logger.debug(f"Successfully updated episode {episode.episode_id} at {dict(row)['updatedAt']}")
            return True
    return run_transaction_with_retry(_txn, on_retry_log='Update episode (minimal)')

def update_episode_processing_flags(
    episode_id: str,
    video_quoting_done: Optional[bool] = None,
    video_chunking_done: Optional[bool] = None,
) -> bool:
    """Atomically set processingInfo.videoQuotingDone and/or videoChunkingDone using JSONB set.
    Only updates the flags provided (non-None). Uses advisory lock and retries.
    """
    if video_quoting_done is None and video_chunking_done is None:
        return True

    def _txn(conn):
        with conn.cursor() as cursor:
            if not _try_acquire_advisory_xact_lock_nowait(cursor, 'Episodes', episode_id):
                logger.info(f"Skipping update_episode_processing_flags for {episode_id}: lock not available (nowait)")
                return False

            # Ensure processingInfo is a JSONB object
            # Build dynamic SET for flags
            base_jsonb = "COALESCE(\"processingInfo\", '{}'::jsonb)"
            params = []
            expr = base_jsonb
            if video_quoting_done is not None:
                expr = f"jsonb_set({expr}, '{{videoQuotingDone}}', to_jsonb(%s), true)"
                params.append(video_quoting_done)
            if video_chunking_done is not None:
                expr = f"jsonb_set({expr}, '{{videoChunkingDone}}', to_jsonb(%s), true)"
                params.append(video_chunking_done)

            sql = f'''
                UPDATE "Episodes"
                SET "processingInfo" = {expr},
                    "updatedAt" = %s
                WHERE "episodeId" = %s AND "deletedAt" IS NULL
                RETURNING "processingInfo"
            '''
            params.extend([datetime.utcnow(), episode_id])
            cursor.execute(sql, params)
            row = cursor.fetchone()
            if not row:
                # Extra diagnostics to understand why the UPDATE matched zero rows
                try:
                    cursor.execute(
                        'SELECT "episodeId", "deletedAt", "processingInfo" FROM "Episodes" WHERE "episodeId" = %s',
                        (episode_id,)
                    )
                    exists_row = cursor.fetchone()
                    if exists_row:
                        logger.error(
                            "update_episode_processing_flags: Episode %s exists but UPDATE matched 0 rows (deletedAt=%s). Params: quoting_done=%s chunking_done=%s",
                            episode_id,
                            dict(exists_row).get('deletedAt'),
                            video_quoting_done,
                            video_chunking_done
                        )
                    else:
                        logger.error(
                            "update_episode_processing_flags: Episode %s not found when attempting to set flags. Params: video_quoting_done=%s video_chunking_chunking_done=%s",
                            episode_id,
                            video_quoting_done,
                            video_chunking_done
                        )
                except Exception as diag_e:
                    logger.error(f"update_episode_processing_flags: diagnostic select failed for {episode_id}: {diag_e}")
                raise psycopg2.OperationalError("No rows updated when setting processing flags")

            # Verify flags in returned JSON if they were requested
            pi = dict(row).get('processingInfo')
            if isinstance(pi, str):
                try:
                    import json as _json
                    pi = _json.loads(pi)
                except Exception:
                    pi = {}
            pi = pi or {}
            if video_quoting_done is not None and bool(pi.get('videoQuotingDone')) != bool(video_quoting_done):
                raise psycopg2.OperationalError("processingInfo.videoQuotingDone did not persist correctly")
            if video_chunking_done is not None and bool(pi.get('videoChunkingDone')) != bool(video_chunking_done):
                raise psycopg2.OperationalError("processingInfo.videoChunkingDone did not persist correctly")
            return True

    return run_transaction_with_retry(_txn, on_retry_log='Update episode processing flags')
  
async def update_episode_with_related_data(
    episode: Episode, 
    quotes: Optional[List[Quote]] = None, 
    shorts: Optional[List[Short]] = None
) -> bool:
    """
    Update an episode along with its related quotes and shorts in a single ACID transaction.
    This demonstrates proper multi-table transaction handling.
    """
    def _execute_updates(conn):
        with conn.cursor() as cursor:
            # Acquire advisory locks (nowait) to prevent long waits; proceed on what we can lock
            if not _try_acquire_advisory_xact_lock_nowait(cursor, 'Episodes', episode.episode_id):
                logger.info(f"Skipping update_episode_with_related_data for {episode.episode_id}: lock not available (nowait)")
                return False
            locked_quotes: List[Quote] = []
            locked_shorts: List[Short] = []
            if quotes:
                for q in quotes:
                    if _try_acquire_advisory_xact_lock_nowait(cursor, 'Quotes', q.quote_id):
                        locked_quotes.append(q)
            if shorts:
                for s in shorts:
                    if _try_acquire_advisory_xact_lock_nowait(cursor, 'Shorts', s.chunk_id):
                        locked_shorts.append(s)

            # Update episode first
            episode_data = episode.to_db_dict()
            episode_data['updatedAt'] = datetime.utcnow()
            episode_id = episode_data.pop('episodeId')
            
            # Verify episode exists
            cursor.execute(
                '''
                SELECT COUNT(*) as count FROM "Episodes"
                WHERE "episodeId" = %s AND "deletedAt" IS NULL
                ''',
                (episode_id,)
            )
            if dict(cursor.fetchone())['count'] == 0:
                raise ValueError(f"Episode {episode_id} not found")
            
            # Update episode
            set_clause = ', '.join([f'"{k}" = %s' for k in episode_data.keys()])
            cursor.execute(
                f'''
                UPDATE "Episodes"
                SET {set_clause}
                WHERE "episodeId" = %s AND "deletedAt" IS NULL
                RETURNING "updatedAt"
                ''',
                list(episode_data.values()) + [episode_id]
            )
            if not cursor.fetchone():
                raise psycopg2.OperationalError("Episode update did not return a row")
            
            # Update quotes if provided
            if locked_quotes:
                update_data = []
                now_marker = datetime.utcnow()
                for quote in locked_quotes:
                    data = quote.to_db_dict()
                    data['updatedAt'] = now_marker
                    quote_id = data.pop('quoteId')
                    ordered_values = list(data.values()) + [quote_id]
                    update_data.append(tuple(ordered_values))
                first_quote_data = locked_quotes[0].to_db_dict()
                first_quote_data.pop('quoteId')
                set_clause = ', '.join([f'"{k}" = %s' for k in first_quote_data.keys()])
                sql_statement = f'''
                    UPDATE "Quotes"
                    SET {set_clause}
                    WHERE "quoteId" = %s AND "deletedAt" IS NULL
                '''
                execute_batch(cursor, sql_statement, update_data)
                # Validate affected rows
                id_list = [q.quote_id for q in locked_quotes]
                cursor.execute(
                    '''SELECT COUNT(*) AS cnt FROM "Quotes" WHERE "quoteId" = ANY(%s) AND "deletedAt" IS NULL AND "updatedAt" >= %s''',
                    (id_list, now_marker)
                )
                cnt = int(dict(cursor.fetchone())['cnt'])
                if cnt != len(locked_quotes):
                    raise psycopg2.OperationalError(f"Related quotes validation failed: expected {len(locked_quotes)}, verified {cnt}")

            # Update shorts if provided
            if locked_shorts:
                update_data = []
                now_marker2 = datetime.utcnow()
                for short in locked_shorts:
                    data = short.to_db_dict()
                    data['updatedAt'] = now_marker2
                    chunk_id = data.pop('chunkId')
                    ordered_values = list(data.values()) + [chunk_id]
                    update_data.append(tuple(ordered_values))
                first_short_data = locked_shorts[0].to_db_dict()
                first_short_data.pop('chunkId')
                set_clause = ', '.join([f'"{k}" = %s' for k in first_short_data.keys()])
                sql_statement = f'''
                    UPDATE "Shorts"
                    SET {set_clause}
                    WHERE "chunkId" = %s AND "deletedAt" IS NULL
                '''
                execute_batch(cursor, sql_statement, update_data)
                id_list2 = [s.chunk_id for s in locked_shorts]
                cursor.execute(
                    '''SELECT COUNT(*) AS cnt FROM "Shorts" WHERE "chunkId" = ANY(%s) AND "deletedAt" IS NULL AND "updatedAt" >= %s''',
                    (id_list2, now_marker2)
                )
                cnt2 = int(dict(cursor.fetchone())['cnt'])
                if cnt2 != len(locked_shorts):
                    raise psycopg2.OperationalError(f"Related shorts validation failed: expected {len(locked_shorts)}, verified {cnt2}")

            logger.info(f"Successfully updated episode {episode_id} with related data in single transaction")
            return True

    try:
        return run_transaction_with_retry(_execute_updates, on_retry_log='Update episode with related data')
    except Exception as e:
        logger.error(f"Failed to update episode with related data: {e}")
        return False

async def upload_with_retry(s3_service: S3Service, file_path: str, bucket: str, key: str, max_retries: int = 7):
    for attempt in range(1, max_retries + 1):
        try:
            await asyncio.to_thread(s3_service.upload_file, file_path, bucket, key)
            logger.info(f"  - Successfully uploaded {os.path.basename(file_path)} to {key} on attempt {attempt}.")
            return
        except Exception as e:
            logger.error(f"  - Attempt {attempt} failed for {key}: {e}")
            if attempt == max_retries:
                raise e
            # Exponential backoff capped at 3 seconds
            await asyncio.sleep(min(3.0, 2 ** attempt))