import asyncio
from typing import Tuple
import ffmpeg
from video_artifact_processing_engine.utils.logging_config import setup_custom_logger

logger = setup_custom_logger(__name__)


async def run_ffmpeg_with_retries(stream, log_context: str, max_attempts: int = 3) -> Tuple[bool, str]:
    """
    Compile and run an ffmpeg stream with retries. Returns (success, stderr_text).
    """
    last_stderr = ""
    for attempt in range(1, max_attempts + 1):
        try:
            cmd = ffmpeg.compile(stream)
            logger.info(f"Running ffmpeg {log_context} (attempt {attempt}): {' '.join(cmd)}")
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await process.communicate()
            if stderr:
                last_stderr = stderr.decode(errors='ignore')
            if process.returncode == 0:
                logger.info(f"FFmpeg completed successfully for {log_context} on attempt {attempt}")
                return True, last_stderr
            else:
                logger.error(f"FFmpeg failed for {log_context} with return code {process.returncode} on attempt {attempt}")
                if last_stderr:
                    logger.error(f"FFmpeg stderr: {last_stderr}")
        except Exception as e:
            logger.error(f"Error running ffmpeg {log_context} on attempt {attempt}: {e}")
    return False, last_stderr
