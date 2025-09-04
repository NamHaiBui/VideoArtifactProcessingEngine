import json
import logging
from typing import List
from video_artifact_processing_engine.aws.db_operations import get_db_cursor
from video_artifact_processing_engine.sqs_handler import SQSPoller
from video_artifact_processing_engine.config import config
from video_artifact_processing_engine.utils.logging_config import setup_custom_logger

logger = setup_custom_logger(__name__)

def get_all_episodes() -> List[str]:
    """Fetch all episode IDs from the Episodes table."""
    episode_ids = []
    
    with get_db_cursor() as cursor:
        cursor.execute(
            '''
            SELECT "episodeId","episodeTitle","publishedDate","updatedAt","processingInfo","summaryAudioUri" 
            FROM "Episodes" where not "processingInfo"?'videoChunkingDone' and "processingInfo"->>'chunkingDone' = 'true'
            ORDER BY "updatedAt" DESC;
            '''
        )
        rows = cursor.fetchall()
        episode_ids = [row['episodeId'] for row in rows]
    
    logger.info(f"Found {len(episode_ids)} episodes in database")
    return episode_ids

def queue_episodes_to_sqs(episode_ids: List[str]) -> None:
    """Queue all episodes to SQS for processing."""
    if not episode_ids:
        logger.warning("No episodes to queue")
        return
    
    # Initialize SQS poller to use its SQS client
    poller = SQSPoller(config)
    
    successful_queues = 0
    failed_queues = 0
    
    for episode_id in episode_ids:
        message_body = json.dumps({
            "episodeId": episode_id
        })
        
        try:
            # Use the SQS client from the poller to send message
            poller.sqs.send_message(
                QueueUrl='https://sqs.us-east-2.amazonaws.com/577638363654/video-shorts-queue',
                MessageBody=message_body
            )
            successful_queues += 1
            logger.info(f"Successfully queued episode: {episode_id}")
            
        except Exception as e:
            failed_queues += 1
            logger.error(f"Failed to queue episode {episode_id}: {e}")
    
    logger.info(f"Queuing completed: {successful_queues} successful, {failed_queues} failed")

def main():
    """Main function to fetch episodes and queue them to SQS."""
    
    if not config.queue_url:
        logger.error("SQS_QUEUE_URL environment variable not set")
        return
    
    logger.info("Starting episode queuing process...")
    
    try:
        # Fetch all episodes from database
        episode_ids = get_all_episodes()
        
        if not episode_ids:
            logger.warning("No episodes found in database")
            return
        print(episode_ids)
        # Queue episodes to SQS
        queue_episodes_to_sqs(episode_ids)

        logger.info("Episode queuing process completed successfully")
        
    except Exception as e:
        logger.error(f"Error in episode queuing process: {e}")
        raise

if __name__ == "__main__":
    main()