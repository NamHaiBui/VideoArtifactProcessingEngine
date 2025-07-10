#!/usr/bin/env python3
"""
Simple test script to verify imports and basic functionality
"""

def test_imports():
    """Test that all imports work correctly"""
    try:
        # Test boto3 imports
        import boto3
        from boto3.dynamodb.conditions import Key
        print("✓ boto3 imports successful")
        
        # Test other imports
        import json
        import os
        import sys
        import time
        import uuid
        print("✓ Standard library imports successful")
        
        # Test project imports
        from video_artifact_processing_engine.config import QUEUE_URL, PODCAST_METADATA_TABLE
        print("✓ Project config imports successful")
        
        # Test AWS client functions
        from video_artifact_processing_engine.aws.aws_client import get_sqs_client, get_dynamodb_resource
        print("✓ AWS client imports successful")
        
        print("\n🎉 All imports successful! Your environment is properly configured.")
        return True
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

if __name__ == "__main__":
    print("Testing Video Artifact Processing Engine environment...")
    print("=" * 50)
    
    success = test_imports()
    
    if success:
        print("\n✨ Environment setup complete!")
        print("\nNext steps:")
        print("1. Set up your AWS credentials")
        print("2. Configure environment variables")
        print("3. Use F5 to start debugging in VS Code")
        print("4. Set breakpoints and debug your application")
    else:
        print("\n❌ Environment setup failed. Please check the errors above.")
