#!/usr/bin/env python3
"""
Debug script for testing Kafka message processing.
Use this for step-by-step debugging of Kafka consumer logic.
"""

import json
import time
from src.orchestrator.consumer.kafka_runner import KafkaRunner
from src.orchestrator.integrations import deploy_integration

def debug_kafka_processing():
    """Debug Kafka message processing step by step."""
    
    # Step 1: Test message validation
    print("🔍 Step 1: Testing message validation...")
    
    test_messages = [
        {
            "tenant_id": "KafkaDebugTenant",
            "integration": "jumpcloud",
            "parameters": {"api_key": "kafka-debug-api-key"},
            "flow_name": "JumpCloudPipelineAsset",
            "message_id": f"debug-{int(time.time())}"
        },
        {
            "tenant_id": "KafkaDebugTenant",
            "integration": "jumpcloud-events", 
            "parameters": {"api_key": "kafka-debug-api-key"},
            "flow_name": "jumpcloud-events",
            "message_id": f"debug-events-{int(time.time())}"
        }
    ]
    
    # Step 2: Process messages manually (without Kafka)
    print("\n🔍 Step 2: Processing messages manually...")
    for i, message in enumerate(test_messages, 1):
        print(f"\n--- Processing Message {i} ---")
        print(f"Message: {json.dumps(message, indent=2)}")
        
        # Extract message data
        tenant_id = message["tenant_id"]
        integration = message["integration"] 
        api_key = message["parameters"]["api_key"]
        flow_name = message.get("flow_name")
        
        # Process with switch-case logic
        try:
            if integration == "jumpcloud":
                result = deploy_integration(
                    integration="jumpcloud",
                    tenant_id=tenant_id,
                    api_key=api_key,
                    flow_name=flow_name or "JumpCloudPipelineAsset"
                )
            elif integration == "jumpcloud-events":
                result = deploy_integration(
                    integration="jumpcloud-events",
                    tenant_id=tenant_id, 
                    api_key=api_key,
                    flow_name=flow_name or "jumpcloud-events"
                )
            else:
                raise ValueError(f"Unsupported integration: {integration}")
                
            print(f"✅ Success: {result.get('status')}")
            print(f"   Category: {result.get('category')}")
            print(f"   Integration: {result.get('integration_name')}")
            
        except Exception as e:
            print(f"❌ Error: {e}")
            
    print("\n🔍 Step 3: Kafka Runner test (if desired)...")
    print("   To test actual Kafka consumer, run the Kafka Consumer configuration")
    print("   and send messages to the topic manually.")

def simulate_kafka_message():
    """Simulate a single Kafka message for debugging."""
    
    # This simulates what the Kafka consumer receives
    class MockKafkaMessage:
        def __init__(self, data):
            self.value = json.dumps(data).encode('utf-8')
            self.topic = "nifi-pipeline-deployments"
            self.partition = 0
            self.offset = 12345
            
    # Create test message
    test_data = {
        "tenant_id": "MockTenant",
        "integration": "jumpcloud",
        "parameters": {"api_key": "mock-api-key"},
        "flow_name": "JumpCloudPipelineAsset"
    }
    
    mock_message = MockKafkaMessage(test_data)
    print(f"Mock Kafka Message: {mock_message.value.decode('utf-8')}")
    
    # Process like KafkaRunner would
    from src.orchestrator.consumer.kafka_runner import KafkaRunner
    runner = KafkaRunner()
    
    # Set breakpoint here to debug message processing
    result = runner._process_single_message(mock_message)
    print(f"Processing result: {result}")
    
    return result

if __name__ == "__main__":
    print("🚀 Kafka Debug Script")
    print("="*50)
    
    # Set breakpoints on these function calls for debugging
    debug_kafka_processing()
    
    print("\n" + "="*50)
    print("🧪 Mock Kafka Message Test")
    
    # Uncomment to test mock Kafka message processing
    # simulate_kafka_message()


