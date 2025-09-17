#!/usr/bin/env python3
"""
Debug script to test parameter context assignment via Kafka vs direct calls.
This helps identify any differences between the two approaches.
"""

import json
import time
import uuid
from src.orchestrator.integrations import deploy_aws_asset_registry_pipeline
from src.orchestrator.nifi.params import parameter_manager


def test_direct_vs_kafka_deployment():
    """Test both direct function call and Kafka-style deployment."""
    
    print("🔍 DEBUGGING PARAMETER CONTEXT ASSIGNMENT: DIRECT vs KAFKA")
    print("=" * 70)
    print()
    
    # Generate test tenant IDs
    direct_tenant = f"Direct-{str(uuid.uuid4())[:6]}"
    kafka_tenant = f"Kafka-{str(uuid.uuid4())[:6]}"
    
    print("📋 TEST PLAN:")
    print(f"   Direct Test Tenant: {direct_tenant}")
    print(f"   Kafka Test Tenant: {kafka_tenant}")
    print()
    
    # Test 1: Direct function call (this should work)
    print("🎯 TEST 1: DIRECT FUNCTION CALL")
    print("-" * 40)
    
    try:
        direct_result = deploy_aws_asset_registry_pipeline(
            tenant_id=direct_tenant,
            api_key="direct-test-key",
            flow_name="aws"
        )
        
        print(f"✅ Direct deployment completed:")
        print(f"   Status: {direct_result.get('status', 'N/A')}")
        print(f"   Integration PG: {direct_result.get('integration_pg_id', 'N/A')[:12]}...")
        print(f"   Parameter Context ID: {direct_result.get('pc_id', 'N/A')[:12]}...")
        
        # Test the parameter context assignment on the direct deployment
        if direct_result.get('integration_pg_id') and direct_result.get('pc_id'):
            print()
            print("🔍 Testing parameter context assignment on direct deployment:")
            pc_result = parameter_manager.ensure_imported_flow_parameter_context(
                direct_result['integration_pg_id'], 
                direct_result['pc_id']
            )
            
            print(f"   Discovered: {pc_result.get('total_discovered', 0)} process groups")
            print(f"   Assigned: {pc_result.get('total_assigned', 0)} process groups") 
            print(f"   Failed: {pc_result.get('total_failed', 0)} process groups")
        
    except Exception as e:
        print(f"❌ Direct deployment failed: {e}")
    
    print()
    print("🎯 TEST 2: KAFKA-STYLE MESSAGE PROCESSING")
    print("-" * 40)
    print()
    
    # Create a Kafka-style message
    kafka_message = {
        "tenant_id": kafka_tenant,
        "integration": "aws", 
        "parameters": {"api_key": "kafka-test-key"},
        "flow_name": "aws",
        "version": "latest",
        "message_id": f"debug-{int(time.time())}"
    }
    
    print("📋 Kafka Message Structure:")
    print(json.dumps(kafka_message, indent=2))
    print()
    
    print("🔧 TO TEST VIA ACTUAL KAFKA:")
    print("1. Start the Kafka consumer:")
    print("   python -m src.orchestrator.app")
    print()
    print("2. Send this message:")
    kafka_cmd = f"echo '{json.dumps(kafka_message)}' | kafka-console-producer.sh --bootstrap-server localhost:9093 --topic nifi-pipeline-deployments"
    print(f"   {kafka_cmd}")
    print()
    print("3. Look for these log entries:")
    print("   '🔍 STEP 1: COMPLETE RECURSIVE ANALYSIS'")
    print("   '🔗 STEP 2: INDIVIDUAL PARAMETER CONTEXT ASSIGNMENT'") 
    print("   '✅ STEP 3: FINAL VERIFICATION'")
    print("   'Total Discovered: X process groups'")
    print("   'Successfully Assigned: X'")
    print()
    
    print("🎯 WHAT TO CHECK IN NIFI UI:")
    print(f"   1. Look for tenant process group: {kafka_tenant}")
    print("   2. Navigate to: Asset Register > AWS Asset Registry")
    print("   3. Check ALL sub-process groups for parameter context:")
    print("      - ec2, ec2_addresses, vpc_security_groups")
    print("      - ec2_network_interfaces, dynamodb, ecs, ecr")  
    print("      - kms_keys, lambda, rds, s3, elb, etc.")
    print("   4. Each should have parameter context assigned")
    print()
    
    print("💡 EXPECTED BEHAVIOR:")
    print("   ✅ Both direct and Kafka should use identical code paths")
    print("   ✅ Both should discover and assign to ALL process groups")
    print("   ✅ Both should show 23/23 successful assignments")
    print("   ✅ ALL sub-process groups should have parameter context in UI")


if __name__ == "__main__":
    test_direct_vs_kafka_deployment()


