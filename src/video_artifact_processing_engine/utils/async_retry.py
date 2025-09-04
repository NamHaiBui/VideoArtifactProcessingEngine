import asyncio
from typing import Any, Awaitable, Callable, Optional
from video_artifact_processing_engine.utils.logging_config import setup_custom_logger

logger = setup_custom_logger(__name__)


async def retry_with_backoff(
    coro_func: Callable[..., Awaitable[Any]],
    *args: Any,
    attempts: int = 4,
    base_delay: float = 0.75,
    on_final_failure: Optional[Callable[[], None]] = None,
    **kwargs: Any,
) -> bool:
    """
    Retry an async function with exponential backoff.
    Returns True when the function returns a truthy value; False otherwise.
    If on_final_failure is provided, it will be called after all attempts fail.
    """
    for attempt in range(1, attempts + 1):
        try:
            result = await coro_func(*args, **kwargs)
            if result:
                return True
            if attempt < attempts:
                delay = min(3.0, base_delay * (2 ** (attempt - 1)))
                await asyncio.sleep(delay)
        except Exception as e:
            logger.error(f"Retryable operation failed on attempt {attempt}/{attempts}: {e}")
            if attempt < attempts:
                delay = min(3.0, base_delay * (2 ** (attempt - 1)))
                await asyncio.sleep(delay)
    if on_final_failure:
        try:
            on_final_failure()
        except Exception as me:
            logger.error(f"on_final_failure callback raised: {me}")
    return False


def emit_db_retry_failed_metric(cloudwatch_client: Any, item_type: str, item_id: str) -> None:
    try:
        cloudwatch_client.put_metric_data(
            Namespace='VideoArtifactProcessingEngine/Alerts',
            MetricData=[
                {
                    'MetricName': 'DbUpdateRetryFailed',
                    'Dimensions': [
                        {'Name': 'ItemType', 'Value': item_type},
                        {'Name': 'Id', 'Value': str(item_id)},
                    ],
                    'Unit': 'Count',
                    'Value': 1.0,
                }
            ]
        )
        logger.warning(f"Emitted CloudWatch alarm metric for {item_type}={item_id} after retries exhausted")
    except Exception as me:
        logger.error(f"Failed to emit CloudWatch metric for {item_type}={item_id}: {me}")
