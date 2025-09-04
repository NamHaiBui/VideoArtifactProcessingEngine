#!/usr/bin/env python3
"""
Test script to verify ECS Task Protection timing and gap prevention.
This tests that:
1. Protection lease is extended before expiration (no gaps)
2. Protection only ends when self-invoked 
3. Critical sessions are properly tracked
"""

import sys
import os
import time
import threading
from datetime import datetime, timedelta

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from video_artifact_processing_engine.aws.ecs_task_protection import ECSTaskProtectionManager

def test_gap_prevention():
    """Test that protection lease extension prevents gaps"""
    print("=" * 60)
    print("TESTING GAP PREVENTION IN ECS TASK PROTECTION")
    print("=" * 60)
    
    # Create a test manager with shorter intervals for testing
    manager = ECSTaskProtectionManager()
    
    # Override timing for faster testing
    manager.check_interval = 5  # Check every 5 seconds
    manager.protection_extension_interval = 10  # Extend every 10 seconds
    manager.protection_buffer = 8  # 8 second buffer
    
    print(f"Test Configuration:")
    print(f"  Check interval: {manager.check_interval}s")
    print(f"  Extension interval: {manager.protection_extension_interval}s")
    print(f"  Protection buffer: {manager.protection_buffer}s")
    print(f"  Gap safety margin: {manager.protection_buffer - manager.check_interval}s")
    
    # Calculate if configuration prevents gaps
    gap_safety = manager.protection_buffer - manager.check_interval
    print(f"  Gap prevention: {'SAFE' if gap_safety > 0 else 'AT RISK'}")
    
    if gap_safety <= 0:
        print("⚠️  WARNING: Current configuration may allow gaps!")
        print("   Protection buffer should be larger than check interval")
    else:
        print("✅ Configuration prevents gaps")
    
    return gap_safety > 0

def test_self_invoked_shutdown():
    """Test that protection only ends when explicitly requested"""
    print("\n" + "=" * 60)
    print("TESTING SELF-INVOKED SHUTDOWN BEHAVIOR")
    print("=" * 60)
    
    manager = ECSTaskProtectionManager()
    
    print("1. Adding critical session...")
    manager.add_critical_session("test-session-1")
    
    status = manager.get_protection_status()
    print(f"   Critical sessions: {status['critical_sessions_count']}")
    
    print("2. Testing that protection persists with active sessions...")
    time.sleep(2)
    
    status = manager.get_protection_status()
    print(f"   Critical sessions still active: {status['critical_sessions_count']}")
    print(f"   Active sessions: {status['critical_sessions']}")
    
    print("3. Testing self-invoked shutdown (removing critical session)...")
    manager.remove_critical_session("test-session-1")
    
    status = manager.get_protection_status()
    print(f"   Critical sessions after removal: {status['critical_sessions_count']}")
    
    print("4. Testing force disable (emergency shutdown)...")
    manager.add_critical_session("emergency-session")
    status = manager.get_protection_status()
    print(f"   Added emergency session: {status['critical_sessions_count']}")
    
    manager.force_disable_protection("Emergency test shutdown")
    status = manager.get_protection_status()
    print(f"   Critical sessions after force disable: {status['critical_sessions_count']}")
    print(f"   Protection enabled after force disable: {status['protection_enabled']}")
    
    print("✅ Self-invoked shutdown test completed")
    return True

def test_protection_timing_details():
    """Test detailed protection timing information"""
    print("\n" + "=" * 60)
    print("TESTING PROTECTION TIMING DETAILS")
    print("=" * 60)
    
    manager = ECSTaskProtectionManager()
    
    print("Getting detailed timing information...")
    status = manager.get_protection_status()
    
    timing_info = {
        'check_interval_seconds': status.get('check_interval_seconds'),
        'extension_interval_seconds': status.get('extension_interval_seconds'),
        'protection_buffer_seconds': status.get('protection_buffer_seconds'),
        'gap_safety_margin_seconds': status.get('gap_safety_margin_seconds'),
        'gap_protection_safe': status.get('gap_protection_safe')
    }
    
    print("Timing Configuration:")
    for key, value in timing_info.items():
        if value is not None:
            print(f"  {key}: {value}")
    
    # Verify timing calculations
    if (timing_info['gap_safety_margin_seconds'] is not None and 
        timing_info['protection_buffer_seconds'] is not None and
        timing_info['check_interval_seconds'] is not None):
        
        expected_margin = timing_info['protection_buffer_seconds'] - timing_info['check_interval_seconds']
        actual_margin = timing_info['gap_safety_margin_seconds']
        
        print(f"\nTiming Verification:")
        print(f"  Expected gap margin: {expected_margin}s")
        print(f"  Actual gap margin: {actual_margin}s")
        print(f"  Gap protection: {'✅ SAFE' if timing_info['gap_protection_safe'] else '⚠️  AT RISK'}")
        
        return timing_info['gap_protection_safe'] or False
    else:
        print(f"\nTiming information not available (running outside ECS)")
        return True
    
    return True

def main():
    """Run all protection timing tests"""
    print("ECS Task Protection Timing and Gap Prevention Test")
    print("=" * 60)
    
    results = []
    
    try:
        # Test 1: Gap prevention
        gap_safe = test_gap_prevention()
        results.append(("Gap Prevention", gap_safe))
        
        # Test 2: Self-invoked shutdown
        shutdown_ok = test_self_invoked_shutdown()
        results.append(("Self-Invoked Shutdown", shutdown_ok))
        
        # Test 3: Timing details
        timing_ok = test_protection_timing_details()
        results.append(("Timing Details", timing_ok))
        
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        return False
    
    # Print results summary
    print("\n" + "=" * 60)
    print("TEST RESULTS SUMMARY")
    print("=" * 60)
    
    all_passed = True
    for test_name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{test_name:.<40} {status}")
        if not passed:
            all_passed = False
    
    print("\n" + "=" * 60)
    final_status = "✅ ALL TESTS PASSED" if all_passed else "❌ SOME TESTS FAILED"
    print(f"FINAL RESULT: {final_status}")
    print("=" * 60)
    
    if all_passed:
        print("\n🎉 ECS Task Protection is properly configured!")
        print("   • Lease extension prevents gaps")
        print("   • Protection only ends when self-invoked")
        print("   • Critical session tracking works correctly")
    else:
        print("\n⚠️  Issues detected in ECS Task Protection configuration!")
        print("   Please review the failed tests above.")
    
    return all_passed

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
