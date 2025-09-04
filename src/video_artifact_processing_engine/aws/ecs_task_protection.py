"""
ECS Task Protection Manager

This module handles dynamic ECS task protection management to prevent
task termination during critical processing operations.
"""

import boto3
import os
import threading
import time
from datetime import datetime, timedelta
from typing import Optional, Set
from video_artifact_processing_engine.utils.logging_config import setup_custom_logger

logger = setup_custom_logger(__name__)

class ECSTaskProtectionManager:
    """
    Manages ECS task protection dynamically based on critical processing sessions.
    """
    
    def __init__(self):
        self.ecs_client = None
        self.cluster_name = os.environ.get('ECS_CLUSTER_NAME', 'video-processing-cluster')
        self.task_arn = None
        self.protection_enabled = False
        self.critical_sessions: Set[str] = set()
        self.protection_lock = threading.Lock()
        
        # Enhanced protection settings optimized for AWS minute-based API
        self.protection_extension_interval = 900  # 15 minutes (AWS API works in minutes)
        self.protection_buffer = 300  # 5 minutes buffer to prevent gaps
        self.check_interval = 30  # Check every 30 seconds for tighter control
        self.max_protection_duration = 7200  # 2 hours (increased for long-running tasks)
        self.protection_start_time = None
        self._shutdown = False
        self._background_task = None
        
        # Proactive protection settings
        self.enable_proactive_protection = os.environ.get('ECS_PROACTIVE_PROTECTION', 'true').lower() == 'true'
        self.min_protection_time = 120  # Minimum protection time once enabled (2 minutes)
        
        logger.info(f"ECS Task Protection Manager initialized with enhanced settings:")
        logger.info(f"- Protection extension interval: {self.protection_extension_interval}s ({self.protection_extension_interval//60}min)")
        logger.info(f"- Protection buffer: {self.protection_buffer}s ({self.protection_buffer//60}min)") 
        logger.info(f"- Total protection time: {(self.protection_extension_interval + self.protection_buffer)//60}min")
        logger.info(f"- Check interval: {self.check_interval}s")
        logger.info(f"- Max protection duration: {self.max_protection_duration}s ({self.max_protection_duration//3600}h)")
        logger.info(f"- Proactive protection: {self.enable_proactive_protection}")
        
        # Initialize ECS client if running in ECS
        if self._is_running_in_ecs():
            self._initialize_ecs_client()
            self._get_task_arn()
            self._start_background_protection_manager()
            
            # Enable baseline protection if proactive protection is enabled
            if self.enable_proactive_protection:
                logger.info("Enabling proactive baseline protection on startup")
                self.add_critical_session("baseline_protection")
        else:
            logger.warning("Not running in ECS environment - task protection will not be available")
    
    def _is_running_in_ecs(self) -> bool:
        """Check if the application is running in ECS."""
        return (
            os.environ.get('ECS_ENABLE_TASK_PROTECTION', '').lower() == 'true' or
            'AWS_EXECUTION_ENV' in os.environ and 'ECS' in os.environ.get('AWS_EXECUTION_ENV', '') or
            os.path.exists('/proc/1/cgroup') and 'ecs' in open('/proc/1/cgroup').read()
        )
    
    def _initialize_ecs_client(self):
        """Initialize the ECS client and detect correct API method."""
        try:
            self.ecs_client = boto3.client('ecs')
            logger.info("ECS client initialized for task protection management")
            
            # Verify that the update_task_protection method is available
            if hasattr(self.ecs_client, 'update_task_protection'):
                logger.info("ECS update_task_protection method available")
            else:
                logger.error("ECS update_task_protection method not found")
                # List available methods for debugging
                available_methods = [method for method in dir(self.ecs_client) if not method.startswith('_')]
                protection_methods = [method for method in available_methods if 'protect' in method.lower()]
                logger.error(f"Available protection-related methods: {protection_methods}")
                logger.error("Task protection will not be available")
                self.ecs_client = None
                
        except Exception as e:
            logger.error(f"Failed to initialize ECS client: {e}")
            self.ecs_client = None
    
    def _get_task_arn(self):
        """Get the current ECS task ARN and update cluster name."""
        try:
            # Try to get task ARN from ECS metadata endpoint v4
            metadata_uri_v4 = os.environ.get('ECS_CONTAINER_METADATA_URI_V4')
            if metadata_uri_v4:
                import requests
                response = requests.get(f"{metadata_uri_v4}/task", timeout=5)
                if response.status_code == 200:
                    task_metadata = response.json()
                    self.task_arn = task_metadata.get('TaskARN')
                    logger.info(f"Retrieved task ARN from metadata v4: {self.task_arn}")
                    
                    # Get the cluster directly from the metadata if available
                    # This is the most reliable method as it comes directly from AWS
                    if 'Cluster' in task_metadata:
                        self.cluster_name = task_metadata['Cluster']
                        logger.info(f"Updated cluster name from metadata: {self.cluster_name}")
                    
                    # If no direct cluster info, try to extract from ARN
                    elif self.task_arn:
                        self._extract_cluster_from_arn(self.task_arn)
                    
                    return
            
            # Fallback: try metadata endpoint v3
            metadata_uri = os.environ.get('ECS_CONTAINER_METADATA_URI')
            if metadata_uri:
                import requests
                response = requests.get(f"{metadata_uri}/task", timeout=5)
                if response.status_code == 200:
                    task_metadata = response.json()
                    self.task_arn = task_metadata.get('TaskARN')
                    logger.info(f"Retrieved task ARN from metadata v3: {self.task_arn}")
                    
                    # Get the cluster directly from the metadata if available
                    if 'Cluster' in task_metadata:
                        self.cluster_name = task_metadata['Cluster']
                        logger.info(f"Updated cluster name from metadata v3: {self.cluster_name}")
                    
                    # If no direct cluster info, try to extract from ARN
                    elif self.task_arn:
                        self._extract_cluster_from_arn(self.task_arn)
                    
                    return
            
            logger.warning("Could not retrieve task ARN from ECS metadata")
            
        except Exception as e:
            logger.error(f"Failed to get task ARN: {e}")
    
    def _extract_cluster_from_arn(self, task_arn: str):
        """Extract cluster name from task ARN."""
        try:
            # Task ARN format: arn:aws:ecs:region:account:task/cluster-name/task-id
            if ':task/' in task_arn:
                # Split by ':task/' and get the part after it
                task_part = task_arn.split(':task/')[1]
                # Split by '/' and get the first part (cluster name)
                cluster_name = task_part.split('/')[0]
                if cluster_name:
                    original_cluster = self.cluster_name
                    self.cluster_name = cluster_name
                    logger.info(f"Extracted cluster name from task ARN: {cluster_name}")
                    if original_cluster != cluster_name:
                        logger.info(f"Cluster name changed from '{original_cluster}' to '{cluster_name}'")
                else:
                    logger.warning("Could not extract cluster name from task ARN - empty cluster name")
            else:
                logger.warning(f"Task ARN format not recognized: {task_arn}")
        except Exception as e:
            logger.error(f"Failed to extract cluster name from ARN '{task_arn}': {e}")
    
    def _start_background_protection_manager(self):
        """Start the background task protection manager."""
        if self.ecs_client and self.task_arn:
            self._background_task = threading.Thread(
                target=self._protection_manager_loop,
                daemon=True,
                name="ECSTaskProtectionManager"
            )
            self._background_task.start()
            logger.info("Background task protection manager started")
    
    def _protection_manager_loop(self):
        """Enhanced background loop to manage task protection with stronger defensive measures."""
        logger.info(f"Enhanced protection manager started:")
        logger.info(f"- Check interval: {self.check_interval}s")
        logger.info(f"- Extension interval: {self.protection_extension_interval}s ({self.protection_extension_interval//60}min)")
        logger.info(f"- Buffer: {self.protection_buffer}s ({self.protection_buffer//60}min)")
        logger.info(f"- Total protection per cycle: {(self.protection_extension_interval + self.protection_buffer)//60}min")
        logger.info(f"- Gap safety margin: {(self.protection_extension_interval + self.protection_buffer) - self.check_interval}s")
        logger.info(f"- Max protection duration: {self.max_protection_duration}s ({self.max_protection_duration//3600}h)")
        
        consecutive_failures = 0
        max_consecutive_failures = 5
        
        while not self._shutdown:
            try:
                with self.protection_lock:
                    current_time = datetime.utcnow()
                    
                    # Check if we have critical sessions
                    has_critical_sessions = len(self.critical_sessions) > 0
                    
                    # Enhanced protection duration check with warnings
                    protection_duration = 0
                    if self.protection_start_time:
                        protection_duration = (current_time - self.protection_start_time).total_seconds()
                        
                        # Warn at various intervals
                        if protection_duration > self.max_protection_duration * 0.8:
                            logger.warning(f"Task protection has been active for {protection_duration:.0f}s "
                                         f"({protection_duration/3600:.1f} hours)")
                        
                        # Force disable if running too long (safety measure)
                        if protection_duration > self.max_protection_duration:
                            logger.error(f"Task protection has exceeded maximum duration ({self.max_protection_duration}s). "
                                       "This may indicate stuck processing. Forcing protection disable.")
                            has_critical_sessions = False
                    
                    if has_critical_sessions and not self.protection_enabled:
                        # Enable protection with enhanced logging
                        logger.warning("ENABLING ECS TASK PROTECTION - Critical sessions detected")
                        logger.warning(f"Critical sessions: {list(self.critical_sessions)}")
                        self._enable_task_protection()
                        
                    elif has_critical_sessions and self.protection_enabled:
                        # Extend protection with defensive renewal
                        logger.debug(f"Extending task protection (active for {protection_duration:.0f}s)")
                        logger.debug(f"Critical sessions: {list(self.critical_sessions)}")
                        self._extend_task_protection()
                        
                    elif not has_critical_sessions and self.protection_enabled:
                        # Only disable if protection has been active for minimum time
                        if protection_duration >= self.min_protection_time:
                            logger.warning("DISABLING ECS TASK PROTECTION - No critical sessions")
                            self._disable_task_protection()
                        else:
                            remaining_time = self.min_protection_time - protection_duration
                            logger.info(f"Maintaining protection for {remaining_time:.0f}s more (minimum protection time)")
                
                # Reset failure counter on success
                consecutive_failures = 0
                
                # Sleep for the check interval
                time.sleep(self.check_interval)
                
            except Exception as e:
                consecutive_failures += 1
                logger.error(f"Error in protection manager loop (attempt {consecutive_failures}/{max_consecutive_failures}): {e}")
                
                if consecutive_failures >= max_consecutive_failures:
                    logger.critical(f"Protection manager has failed {consecutive_failures} consecutive times. "
                                  "This is a critical error that may compromise task protection.")
                    logger.critical("Attempting to continue but task may be vulnerable to premature termination.")
                
                # Exponential backoff on failures
                sleep_time = min(30 * (2 ** min(consecutive_failures - 1, 4)), 300)  # Max 5 minutes
                logger.warning(f"Waiting {sleep_time}s before retry due to failure")
                time.sleep(sleep_time)
    
    def _enable_task_protection(self):
        """Enable ECS task protection."""
        if not self.ecs_client or not self.task_arn:
            logger.warning("Cannot enable task protection - missing ECS client or task ARN")
            logger.warning(f"ECS client available: {self.ecs_client is not None}")
            logger.warning(f"Task ARN available: {self.task_arn is not None}")
            return
        
        try:
            # Calculate expiration in minutes (AWS API requires minutes, not datetime)
            expiration_minutes = (self.protection_extension_interval + self.protection_buffer) // 60
            if expiration_minutes < 1:
                expiration_minutes = 1  # Minimum 1 minute
            
            # Log the parameters being sent to AWS API
            logger.info(f"Enabling task protection with parameters:")
            logger.info(f"- Cluster: {self.cluster_name}")
            logger.info(f"- Task ARN: {self.task_arn}")
            logger.info(f"- Expiration minutes: {expiration_minutes}")
            
            # Use the correct AWS ECS API method and parameter names
            response = self.ecs_client.update_task_protection(
                cluster=self.cluster_name,
                tasks=[self.task_arn],
                protectionEnabled=True,
                expiresInMinutes=expiration_minutes
            )
            
            self.protection_enabled = True
            self.protection_start_time = datetime.utcnow()
            
            # Calculate actual expiration time for logging
            expires_at = datetime.utcnow() + timedelta(minutes=expiration_minutes)
            
            logger.info(f"ECS task protection enabled for {expiration_minutes} minutes")
            logger.info(f"Protection expires at: {expires_at.isoformat()}")
            logger.info(f"Next check in: {self.check_interval}s")
            logger.info(f"Critical sessions active: {list(self.critical_sessions)}")
            logger.debug(f"ECS API response: {response}")
            
        except Exception as e:
            logger.error(f"Failed to enable task protection: {e}")
            logger.error(f"Task ARN: {self.task_arn}")
            logger.error(f"Cluster: {self.cluster_name}")
            
            # Provide additional debugging information for cluster mismatch errors
            if "cluster identifiers mismatch" in str(e).lower():
                logger.error("CLUSTER IDENTIFIER MISMATCH DETECTED:")
                logger.error("This error occurs when the cluster name doesn't match the actual cluster.")
                logger.error("Common causes:")
                logger.error("1. Environment variable ECS_CLUSTER_NAME is incorrect")
                logger.error("2. Task metadata doesn't contain cluster information")
                logger.error("3. Task ARN parsing failed to extract correct cluster name")
                
                # Try to get additional cluster information
                logger.error("Attempting to debug cluster information...")
                if self.task_arn:
                    logger.error(f"Full Task ARN: {self.task_arn}")
                    try:
                        # Try to call describe_tasks to get the actual cluster
                        describe_response = self.ecs_client.describe_tasks(tasks=[self.task_arn])
                        if describe_response and 'tasks' in describe_response and describe_response['tasks']:
                            actual_cluster = describe_response['tasks'][0].get('clusterArn', 'Unknown')
                            logger.error(f"Actual cluster from describe_tasks: {actual_cluster}")
                    except Exception as desc_e:
                        logger.error(f"Could not describe task to get cluster info: {desc_e}")
            
            # Check if it's an API method issue
            if "has no attribute" in str(e):
                logger.error("ECS API method issue detected. Checking available methods...")
                available_methods = [method for method in dir(self.ecs_client) if not method.startswith('_')]
                protection_methods = [method for method in available_methods if 'protect' in method.lower()]
                logger.error(f"Available protection-related methods: {protection_methods}")
    
    def _extend_task_protection(self):
        """Extend ECS task protection."""
        if not self.ecs_client or not self.task_arn:
            return
        
        try:
            # Calculate expiration in minutes (AWS API requires minutes, not datetime)
            expiration_minutes = (self.protection_extension_interval + self.protection_buffer) // 60
            if expiration_minutes < 1:
                expiration_minutes = 1  # Minimum 1 minute
            
            response = self.ecs_client.update_task_protection(
                cluster=self.cluster_name,
                tasks=[self.task_arn],
                protectionEnabled=True,
                expiresInMinutes=expiration_minutes
            )
            
            # Calculate actual expiration time for logging
            expires_at = datetime.utcnow() + timedelta(minutes=expiration_minutes)
            
            logger.debug(f"ECS task protection extended for {expiration_minutes} minutes")
            logger.debug(f"Protection expires at: {expires_at.isoformat()}")
            logger.debug(f"Critical sessions active: {list(self.critical_sessions)}")
            logger.debug(f"ECS API response: {response}")
            
        except Exception as e:
            logger.error(f"Failed to extend task protection: {e}")
            logger.error(f"Task ARN: {self.task_arn}")
            logger.error(f"Cluster: {self.cluster_name}")
            
            # Handle cluster mismatch errors specifically
            if "cluster identifiers mismatch" in str(e).lower():
                logger.error("Cluster identifier mismatch during extension - this is critical!")
                logger.error("Task protection may have gaps. Consider restarting the application.")
            
            if "has no attribute" in str(e):
                logger.error("ECS API method issue detected during extension")
    
    def _disable_task_protection(self):
        """Disable ECS task protection."""
        if not self.ecs_client or not self.task_arn:
            return
        
        try:
            response = self.ecs_client.update_task_protection(
                cluster=self.cluster_name,
                tasks=[self.task_arn],
                protectionEnabled=False
            )
            
            self.protection_enabled = False
            protection_duration = None
            if self.protection_start_time:
                protection_duration = (datetime.utcnow() - self.protection_start_time).total_seconds()
                self.protection_start_time = None
            
            logger.info("ECS task protection disabled")
            if protection_duration:
                logger.info(f"Protection was active for {protection_duration:.1f} seconds")
            logger.debug(f"ECS API response: {response}")
            
        except Exception as e:
            logger.error(f"Failed to disable task protection: {e}")
            logger.error(f"Task ARN: {self.task_arn}")
            logger.error(f"Cluster: {self.cluster_name}")
            
            # Handle cluster mismatch errors
            if "cluster identifiers mismatch" in str(e).lower():
                logger.error("Cluster identifier mismatch during disable")
                logger.error("Protection may still be active - manual intervention may be required")
            
            if "has no attribute" in str(e):
                logger.error("ECS API method issue detected during disable")
    
    def add_critical_session(self, session_id: str):
        """Add a critical processing session."""
        with self.protection_lock:
            self.critical_sessions.add(session_id)
            logger.info(f"Added critical session: {session_id}")
            logger.info(f"Total critical sessions: {len(self.critical_sessions)}")
    
    def remove_critical_session(self, session_id: str):
        """Remove a critical processing session."""
        with self.protection_lock:
            self.critical_sessions.discard(session_id)
            logger.info(f"Removed critical session: {session_id}")
            logger.info(f"Remaining critical sessions: {len(self.critical_sessions)}")
    
    def get_protection_status(self) -> dict:
        """Get current protection status."""
        with self.protection_lock:
            current_time = datetime.utcnow()
            
            status = {
                'protection_enabled': self.protection_enabled,
                'critical_sessions_count': len(self.critical_sessions),
                'critical_sessions': list(self.critical_sessions),
                'task_arn': self.task_arn,
                'cluster_name': self.cluster_name,
                'protection_start_time': self.protection_start_time.isoformat() if self.protection_start_time else None,
                'ecs_available': self.ecs_client is not None and self.task_arn is not None,
                'check_interval_seconds': self.check_interval,
                'extension_interval_seconds': self.protection_extension_interval,
                'protection_buffer_seconds': self.protection_buffer
            }
            
            if self.protection_start_time:
                status['protection_duration_seconds'] = (current_time - self.protection_start_time).total_seconds()
                
                # Calculate when protection would expire based on minutes (AWS API uses minutes)
                expiration_minutes = (self.protection_extension_interval + self.protection_buffer) // 60
                if expiration_minutes < 1:
                    expiration_minutes = 1
                
                next_expiry = current_time + timedelta(minutes=expiration_minutes)
                status['protection_expires_at'] = next_expiry.isoformat()
                status['minutes_until_expiry'] = expiration_minutes
                status['seconds_until_expiry'] = (next_expiry - current_time).total_seconds()
                
                # Show gap safety margin (converted to consider minute-based expiration)
                gap_safety_margin = (expiration_minutes * 60) - self.check_interval
                status['gap_safety_margin_seconds'] = gap_safety_margin
                status['gap_protection_safe'] = gap_safety_margin > 0
            
            return status
    
    def request_voluntary_shutdown(self):
        """
        Request voluntary shutdown by removing baseline protection.
        This allows the application to shut down cleanly when requested.
        """
        with self.protection_lock:
            if "baseline_protection" in self.critical_sessions:
                logger.info("Voluntary shutdown requested - removing baseline protection")
                self.critical_sessions.discard("baseline_protection")
                
                if len(self.critical_sessions) == 0:
                    logger.info("No remaining critical sessions - application may shut down")
                else:
                    logger.info(f"Remaining critical sessions: {list(self.critical_sessions)}")
                    logger.info("Shutdown will wait for remaining critical sessions to complete")
            else:
                logger.info("Voluntary shutdown requested - no baseline protection to remove")
    
    def force_disable_protection(self, reason: str = "Application shutdown"):
        """
        Force disable task protection regardless of critical sessions.
        This should only be used in controlled shutdown scenarios.
        """
        with self.protection_lock:
            if self.protection_enabled:
                logger.warning(f"Force disabling ECS task protection. Reason: {reason}")
                logger.warning(f"Active critical sessions will be abandoned: {list(self.critical_sessions)}")
                
                # Clear critical sessions
                self.critical_sessions.clear()
                
                # Disable protection
                self._disable_task_protection()
                
                logger.warning("ECS task protection force disabled. Task may be terminated.")
            else:
                logger.info(f"ECS task protection already disabled. Reason: {reason}")
    
    def shutdown(self):
        """Shutdown the protection manager."""
        logger.info("Shutting down ECS task protection manager")
        self._shutdown = True
        
        # Disable protection if enabled - this is a controlled shutdown
        if self.protection_enabled:
            logger.info("Disabling task protection as part of controlled shutdown")
            self._disable_task_protection()
        
        # Wait for background thread to finish
        if self._background_task and self._background_task.is_alive():
            self._background_task.join(timeout=10)
        
        logger.info("ECS task protection manager shutdown complete")


# Global instance
_task_protection_manager: Optional[ECSTaskProtectionManager] = None

def get_task_protection_manager() -> ECSTaskProtectionManager:
    """Get the global task protection manager instance."""
    global _task_protection_manager
    if _task_protection_manager is None:
        _task_protection_manager = ECSTaskProtectionManager()
    return _task_protection_manager

def shutdown_task_protection_manager():
    """Shutdown the global task protection manager."""
    global _task_protection_manager
    if _task_protection_manager:
        _task_protection_manager.shutdown()
        _task_protection_manager = None
