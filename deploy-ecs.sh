#!/bin/bash

# ECS Deployment Script with Task Protection
# This script handles deployment with graceful shutdown and task protection

set -e

# Configuration
CLUSTER_NAME=${ECS_CLUSTER_NAME:-"video-processing-cluster"}
SERVICE_NAME=${ECS_SERVICE_NAME:-"video-artifact-processing-service"}
TASK_DEFINITION_NAME=${ECS_TASK_DEFINITION:-"video-artifact-processing-engine"}
AWS_REGION=${AWS_REGION:-"us-east-1"}
IMAGE_TAG=${IMAGE_TAG:-"latest"}
GRACE_PERIOD=${DEPLOYMENT_GRACE_PERIOD:-300}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Check required tools
check_dependencies() {
    log "Checking dependencies..."
    
    if ! command -v aws &> /dev/null; then
        error "AWS CLI not found. Please install AWS CLI."
        exit 1
    fi
    
    if ! command -v jq &> /dev/null; then
        error "jq not found. Please install jq."
        exit 1
    fi
    
    success "Dependencies check passed"
}

# Validate AWS credentials and permissions
validate_aws_access() {
    log "Validating AWS credentials..."
    
    if ! aws sts get-caller-identity &> /dev/null; then
        error "AWS credentials not configured or invalid"
        exit 1
    fi
    
    # Check ECS permissions
    if ! aws ecs describe-clusters --clusters "$CLUSTER_NAME" &> /dev/null; then
        error "Cannot access ECS cluster: $CLUSTER_NAME"
        exit 1
    fi
    
    success "AWS access validated"
}

# Enable task protection for running tasks
enable_task_protection() {
    local cluster=$1
    local service=$2
    
    log "Enabling task protection for running tasks..."
    
    # Get running task ARNs
    local task_arns=$(aws ecs list-tasks \
        --cluster "$cluster" \
        --service-name "$service" \
        --desired-status RUNNING \
        --query 'taskArns[]' \
        --output text)
    
    if [ -z "$task_arns" ]; then
        warning "No running tasks found for service: $service"
        return 0
    fi
    
    # Enable protection for each task
    for task_arn in $task_arns; do
        log "Enabling protection for task: $(basename $task_arn)"
        
        aws ecs put-protection \
            --cluster "$cluster" \
            --tasks "$task_arn" \
            --protection-enabled \
            --expires-at $(date -d "+${GRACE_PERIOD} seconds" --iso-8601) \
            || warning "Failed to enable protection for task: $task_arn"
    done
    
    success "Task protection enabled"
}

# Wait for tasks to finish processing gracefully
wait_for_graceful_shutdown() {
    local cluster=$1
    local service=$2
    local max_wait=$3
    
    log "Waiting for graceful shutdown (max ${max_wait}s)..."
    
    local start_time=$(date +%s)
    local end_time=$((start_time + max_wait))
    
    while [ $(date +%s) -lt $end_time ]; do
        # Get task ARNs that are still running
        local running_tasks=$(aws ecs list-tasks \
            --cluster "$cluster" \
            --service-name "$service" \
            --desired-status RUNNING \
            --query 'length(taskArns)' \
            --output text)
        
        if [ "$running_tasks" -eq 0 ]; then
            success "All tasks have shut down gracefully"
            return 0
        fi
        
        log "Waiting for $running_tasks tasks to complete processing..."
        sleep 10
    done
    
    warning "Grace period expired. Some tasks may still be running."
    return 1
}

# Disable task protection
disable_task_protection() {
    local cluster=$1
    local service=$2
    
    log "Disabling task protection..."
    
    # Get all task ARNs (running and stopping)
    local task_arns=$(aws ecs list-tasks \
        --cluster "$cluster" \
        --service-name "$service" \
        --query 'taskArns[]' \
        --output text)
    
    if [ -z "$task_arns" ]; then
        log "No tasks found"
        return 0
    fi
    
    # Disable protection for each task
    for task_arn in $task_arns; do
        log "Disabling protection for task: $(basename $task_arn)"
        
        aws ecs put-protection \
            --cluster "$cluster" \
            --tasks "$task_arn" \
            --no-protection-enabled \
            || warning "Failed to disable protection for task: $task_arn"
    done
    
    success "Task protection disabled"
}

# Register new task definition
register_task_definition() {
    log "Registering new task definition..."
    
    # Replace placeholders in task definition
    local task_def_content=$(cat ecs-task-definition.json | \
        sed "s/\${AWS_ACCOUNT_ID}/$(aws sts get-caller-identity --query Account --output text)/g" | \
        sed "s/\${AWS_REGION}/$AWS_REGION/g" | \
        sed "s/\${IMAGE_TAG}/$IMAGE_TAG/g")
    
    # Register the task definition
    local new_revision=$(echo "$task_def_content" | \
        aws ecs register-task-definition \
        --cli-input-json file:///dev/stdin \
        --query 'taskDefinition.revision' \
        --output text)
    
    success "New task definition registered: $TASK_DEFINITION_NAME:$new_revision"
    echo "$new_revision"
}

# Update ECS service with new task definition
update_service() {
    local cluster=$1
    local service=$2
    local task_definition=$3
    local revision=$4
    
    log "Updating ECS service with new task definition..."
    
    aws ecs update-service \
        --cluster "$cluster" \
        --service "$service" \
        --task-definition "$task_definition:$revision" \
        --force-new-deployment \
        > /dev/null
    
    success "Service update initiated"
}

# Wait for deployment to complete
wait_for_deployment() {
    local cluster=$1
    local service=$2
    local max_wait=1800  # 30 minutes
    
    log "Waiting for deployment to complete..."
    
    local start_time=$(date +%s)
    local end_time=$((start_time + max_wait))
    
    while [ $(date +%s) -lt $end_time ]; do
        local deployment_status=$(aws ecs describe-services \
            --cluster "$cluster" \
            --services "$service" \
            --query 'services[0].deployments[?status==`PRIMARY`].rolloutState' \
            --output text)
        
        case "$deployment_status" in
            "COMPLETED")
                success "Deployment completed successfully"
                return 0
                ;;
            "FAILED")
                error "Deployment failed"
                return 1
                ;;
            "IN_PROGRESS")
                log "Deployment in progress..."
                ;;
            *)
                log "Deployment status: $deployment_status"
                ;;
        esac
        
        sleep 30
    done
    
    error "Deployment timed out"
    return 1
}

# Main deployment process
main() {
    log "Starting ECS deployment with task protection..."
    
    # Validate environment
    check_dependencies
    validate_aws_access
    
    # Enable task protection for current tasks
    enable_task_protection "$CLUSTER_NAME" "$SERVICE_NAME"
    
    # Register new task definition
    local new_revision=$(register_task_definition)
    
    # Update service with new task definition
    update_service "$CLUSTER_NAME" "$SERVICE_NAME" "$TASK_DEFINITION_NAME" "$new_revision"
    
    # Wait for deployment to complete
    if wait_for_deployment "$CLUSTER_NAME" "$SERVICE_NAME"; then
        success "Deployment completed successfully"
        
        # Wait a bit for old tasks to finish gracefully
        sleep 30
        
        # Disable protection for old tasks (they should be stopped by now)
        disable_task_protection "$CLUSTER_NAME" "$SERVICE_NAME"
        
        success "ECS deployment with task protection completed"
        exit 0
    else
        error "Deployment failed"
        
        # Disable protection for all tasks on failure
        disable_task_protection "$CLUSTER_NAME" "$SERVICE_NAME"
        
        exit 1
    fi
}

# Handle script interruption
cleanup() {
    warning "Deployment interrupted. Cleaning up..."
    disable_task_protection "$CLUSTER_NAME" "$SERVICE_NAME"
    exit 1
}

trap cleanup INT TERM

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --cluster)
            CLUSTER_NAME="$2"
            shift 2
            ;;
        --service)
            SERVICE_NAME="$2"
            shift 2
            ;;
        --task-definition)
            TASK_DEFINITION_NAME="$2"
            shift 2
            ;;
        --image-tag)
            IMAGE_TAG="$2"
            shift 2
            ;;
        --grace-period)
            GRACE_PERIOD="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --cluster CLUSTER_NAME              ECS cluster name"
            echo "  --service SERVICE_NAME              ECS service name"
            echo "  --task-definition TASK_DEF_NAME     Task definition name"
            echo "  --image-tag TAG                     Docker image tag"
            echo "  --grace-period SECONDS              Grace period for shutdown"
            echo "  --help                              Show this help"
            exit 0
            ;;
        *)
            error "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Run main deployment
main
