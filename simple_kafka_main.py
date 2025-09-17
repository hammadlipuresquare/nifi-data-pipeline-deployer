#!/usr/bin/env python3
"""
Simple Kafka integration for NiFi pipeline deployment.
Direct conversion using the new modular structure.
"""

import os
import logging
import json
import time
from typing import Dict, Any

# Import from new modular structure
from src.orchestrator.integrations import deploy_integration, get_available_integrations

# Kafka imports
try:
    from kafka import KafkaConsumer, KafkaProducer
    from kafka.errors import CommitFailedError, KafkaError
    KAFKA_AVAILABLE = True
except ImportError:
    print("❌ kafka-python not installed. Run: pip install kafka-python")
    KAFKA_AVAILABLE = False
    exit(1)

# Simple logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093")
DEPLOYMENT_TOPIC = os.getenv("KAFKA_DEPLOYMENT_TOPIC", "nifi-pipeline-deployments")
RESPONSE_TOPIC = os.getenv("KAFKA_RESPONSE_TOPIC", "nifi-pipeline-responses")
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "nifi-pipeline-deployer")

# Consumer configuration
ENABLE_AUTO_COMMIT = os.getenv("KAFKA_ENABLE_AUTO_COMMIT", "false").lower() == "true"
AUTO_COMMIT_INTERVAL_MS = int(os.getenv("KAFKA_AUTO_COMMIT_INTERVAL_MS", "5000"))
SESSION_TIMEOUT_MS = int(os.getenv("KAFKA_SESSION_TIMEOUT_MS", "30000"))
MAX_POLL_RECORDS = int(os.getenv("KAFKA_MAX_POLL_RECORDS", "5"))
MAX_POLL_INTERVAL_MS = int(os.getenv("KAFKA_MAX_POLL_INTERVAL_MS", "300000"))

# Retry configuration
MAX_RETRIES = int(os.getenv("MAX_DEPLOYMENT_RETRIES", "3"))
RETRY_DELAY_SECONDS = int(os.getenv("RETRY_DELAY_SECONDS", "60"))


def send_response(producer, response_data):
    """Send response message to Kafka with delivery confirmation."""
    try:
        message = json.dumps(response_data).encode('utf-8')
        future = producer.send(RESPONSE_TOPIC, message)
        
        # Wait for delivery confirmation
        record_metadata = future.get(timeout=10)
        logger.info(f"✅ Response sent to {record_metadata.topic}:{record_metadata.partition}:{record_metadata.offset} for tenant {response_data.get('tenant_id')}")
        
    except Exception as e:
        logger.error(f"❌ Failed to send response: {e}")


def process_deployment_with_retries(message_value, producer, max_retries=MAX_RETRIES):
    """Process deployment message with retry logic."""
    data = None
    
    for attempt in range(max_retries + 1):
        try:
            # Parse the message
            if data is None:
                data = json.loads(message_value.decode('utf-8'))
                
                tenant_id = data.get("tenant_id", "")
                parameters = data.get("parameters", {})
                integration = data.get("integration") or data.get("pipeline", "").lower()
                message_id = data.get("message_id", "")
                flow_name = data.get("flow_name")
                version = data.get("version", "latest")
                
                # Validate required fields
                if not tenant_id:
                    raise ValueError("tenant_id is required")
                if not parameters.get("api_key"):
                    raise ValueError("api_key is required in parameters")
                if not integration:
                    raise ValueError("integration/pipeline is required")
                
                # Normalize integration name
                if integration == "jumpcloud":
                    integration = "jumpcloud"
            
            logger.info(f"🚀 Processing {integration} deployment for tenant: {data['tenant_id']} (attempt {attempt + 1}/{max_retries + 1})")
            
            # Switch case logic for different integrations using hierarchical structure
            if integration == "jumpcloud":
                result = deploy_integration(
                    integration="jumpcloud",
                    tenant_id=data["tenant_id"],
                    api_key=data["parameters"]["api_key"],
                    flow_name=flow_name or "JumpCloudPipelineAsset",
                    version=version
                )
            elif integration == "jumpcloud-events":
                result = deploy_integration(
                    integration="jumpcloud-events",
                    tenant_id=data["tenant_id"],
                    api_key=data["parameters"]["api_key"],
                    flow_name=flow_name or "jumpcloud-events",
                    version=version
                )
            elif integration == "salesforce":
                result = deploy_integration(
                    integration="salesforce",
                    tenant_id=data["tenant_id"], 
                    api_key=data["parameters"]["api_key"],
                    flow_name=flow_name or "SalesforcePipeline",
                    version=version
                )
            elif integration == "aws-security":
                result = deploy_integration(
                    integration="aws-security",
                    tenant_id=data["tenant_id"],
                    api_key=data["parameters"]["api_key"],
                    flow_name=flow_name or "aws",
                    version=version
                )
            elif integration == "custom":
                # Custom integration requires additional parameters
                integration_name = data.get("integration_name")
                category = data.get("category")
                if not integration_name or not category or not flow_name:
                    raise ValueError("Custom integrations require integration_name, category, and flow_name")
                
                result = deploy_integration(
                    integration="custom",
                    tenant_id=data["tenant_id"],
                    api_key=data["parameters"]["api_key"],
                    flow_name=flow_name,
                    version=version,
                    integration_name=integration_name,
                    category=category,
                    additional_params=data.get("additional_params")
                )
            else:
                available = ", ".join(get_available_integrations())
                raise ValueError(f"Unsupported integration '{integration}'. Available: {available}")
            
            # Send success response
            success_response = {
                "success": True,
                "tenant_id": data["tenant_id"],
                "integration": integration,
                "timestamp": time.time(),
                "result": result,
                "error": None,
                "message_id": data.get("message_id", ""),
                "attempt": attempt + 1
            }
            
            send_response(producer, success_response)
            logger.info(f"✅ Successfully deployed {integration} pipeline for {data['tenant_id']} on attempt {attempt + 1}")
            return True
            
        except ValueError as e:
            # Validation errors - don't retry
            logger.error(f"❌ Validation error for {data.get('tenant_id', 'unknown') if data else 'unknown'}: {e}")
            break
            
        except Exception as e:
            logger.error(f"❌ Deployment failed for {data.get('tenant_id', 'unknown') if data else 'unknown'} on attempt {attempt + 1}: {e}")
            
            if attempt < max_retries:
                logger.info(f"⏰ Waiting {RETRY_DELAY_SECONDS} seconds before retry...")
                time.sleep(RETRY_DELAY_SECONDS)
            else:
                logger.error(f"❌ All {max_retries + 1} attempts failed for {data.get('tenant_id', 'unknown') if data else 'unknown'}")
    
    # Send final error response
    error_response = {
        "success": False,
        "tenant_id": data.get("tenant_id", "unknown") if data else "unknown",
        "integration": data.get("integration", data.get("pipeline", "unknown")) if data else "unknown",
        "timestamp": time.time(),
        "result": None,
        "error": f"Deployment failed after {max_retries + 1} attempts",
        "message_id": data.get("message_id", "") if data else "",
        "attempts": max_retries + 1
    }
    
    send_response(producer, error_response)
    return False


def start_kafka_listener():
    """Start the Kafka listener with reliable processing."""
    if not KAFKA_AVAILABLE:
        logger.error("❌ kafka-python is required. Install with: pip install kafka-python")
        return
    
    logger.info("🚀 Starting Kafka listener...")
    logger.info(f"📡 Kafka servers: {KAFKA_SERVERS}")
    logger.info(f"📨 Listening to: {DEPLOYMENT_TOPIC}")
    logger.info(f"📬 Responses to: {RESPONSE_TOPIC}")
    logger.info(f"⚙️ Auto commit: {ENABLE_AUTO_COMMIT}")
    logger.info(f"🔄 Max retries: {MAX_RETRIES}")
    
    # Create Kafka consumer
    consumer = KafkaConsumer(
        DEPLOYMENT_TOPIC,
        bootstrap_servers=KAFKA_SERVERS.split(','),
        group_id=CONSUMER_GROUP,
        auto_offset_reset='earliest',
        enable_auto_commit=ENABLE_AUTO_COMMIT,
        auto_commit_interval_ms=AUTO_COMMIT_INTERVAL_MS,
        session_timeout_ms=SESSION_TIMEOUT_MS,
        max_poll_records=MAX_POLL_RECORDS,
        max_poll_interval_ms=MAX_POLL_INTERVAL_MS,
        consumer_timeout_ms=-1
    )
    
    # Create producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS.split(','),
        acks='all',
        retries=3,
        batch_size=16384,
        linger_ms=10,
        buffer_memory=33554432
    )
    
    logger.info("✅ Kafka listener started successfully!")
    
    # Show available integrations
    integrations = get_available_integrations()
    logger.info(f"Available integrations: {integrations}")
    
    # Example message formats for hierarchical structure
    logger.info("💬 Example message formats for hierarchical deployments:")
    
    # JumpCloud Asset Register example
    example_jumpcloud = {
        "tenant_id": "YourTenant",
        "integration": "jumpcloud",
        "parameters": {"api_key": "your-api-key"},
        "flow_name": "JumpCloudPipelineAsset",  # Optional
        "version": "latest"                     # Optional
    }
    logger.info(f"JumpCloud Asset Register: {json.dumps(example_jumpcloud)}")
    
    # JumpCloud Misconfiguration example
    example_jumpcloud_events = {
        "tenant_id": "YourTenant",
        "integration": "jumpcloud-events",
        "parameters": {"api_key": "your-api-key"},
        "flow_name": "jumpcloud-events",
        "version": "latest"
    }
    logger.info(f"JumpCloud Misconfiguration: {json.dumps(example_jumpcloud_events)}")
    
    # AWS Vulnerability example
    example_aws = {
        "tenant_id": "YourTenant",
        "integration": "aws-security", 
        "parameters": {"api_key": "your-api-key"},
        "flow_name": "aws",
        "version": "latest"
    }
    logger.info(f"AWS Vulnerability: {json.dumps(example_aws)}")
    
    # Custom integration example
    example_custom = {
        "tenant_id": "YourTenant",
        "integration": "custom",
        "integration_name": "Custom Security Tool",
        "category": "Vulnerability",  # Asset Register, Misconfiguration, Vulnerability, Compliance
        "parameters": {"api_key": "your-api-key"},
        "flow_name": "your-custom-flow",
        "version": "latest",
        "additional_params": {"CUSTOM_SETTING": "value"}
    }
    logger.info(f"Custom Integration: {json.dumps(example_custom)}")
    
    # Legacy format still supported
    example_legacy = {
        "tenant_id": "YourTenant", 
        "pipeline": "JumpCloud",  # Legacy format still works
        "parameters": {"api_key": "your-api-key"}
    }
    logger.info(f"Legacy format: {json.dumps(example_legacy)}")
    
    try:
        # Message processing loop
        logger.info("🔄 Starting message polling loop...")
        while True:
            try:
                message_batch = consumer.poll(timeout_ms=1000)
                
                if message_batch:
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            logger.info(f"📨 Received message from {topic_partition.topic}:{topic_partition.partition}:{message.offset}")
                            
                            # Process message with retries
                            success = process_deployment_with_retries(message.value, producer)
                            
                            if success:
                                logger.info(f"✅ Message processed successfully, offset: {message.offset}")
                            else:
                                logger.error(f"❌ Message processing failed permanently, offset: {message.offset}")
                            
                            # Manual offset commit if auto-commit is disabled
                            if not ENABLE_AUTO_COMMIT:
                                try:
                                    consumer.commit()
                                    logger.debug(f"🔄 Committed offset {message.offset + 1}")
                                except CommitFailedError as e:
                                    logger.error(f"❌ Failed to commit offset: {e}")
                
            except KafkaError as e:
                logger.error(f"❌ Kafka error: {e}")
                time.sleep(5)
                continue
                
    except KeyboardInterrupt:
        logger.info("👋 Shutting down Kafka listener...")
    except Exception as e:
        logger.error(f"❌ Kafka listener error: {e}")
    finally:
        try:
            if not ENABLE_AUTO_COMMIT:
                consumer.commit()
                logger.info("🔄 Final offset commit completed")
        except Exception as e:
            logger.error(f"❌ Error during final commit: {e}")
        
        consumer.close()
        producer.close()
        logger.info("🧹 Kafka listener stopped")


def send_test_message():
    """Send a test deployment message."""
    if not KAFKA_AVAILABLE:
        logger.error("❌ kafka-python is required")
        return
    
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS.split(','),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all'
    )
    
    # Test message for hierarchical deployment
    test_message = {
        "tenant_id": "SimpleTestTenant",
        "integration": "jumpcloud",  # Will deploy in Asset Register category
        "parameters": {
            "api_key": "simple-test-api-key-123"
        },
        "flow_name": "JumpCloudPipelineAsset",
        "message_id": f"simple-test-{int(time.time())}"
    }
    
    try:
        future = producer.send(DEPLOYMENT_TOPIC, test_message)
        record_metadata = future.get(timeout=10)
        
        logger.info("✅ Test message sent successfully!")
        logger.info(f"📨 Message sent to {record_metadata.topic}:{record_metadata.partition}:{record_metadata.offset}")
        logger.info(f"💬 Message: {json.dumps(test_message, indent=2)}")
    except Exception as e:
        logger.error(f"❌ Failed to send test message: {e}")
    finally:
        producer.close()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "--test":
            send_test_message()
        else:
            print("Usage: python simple_kafka_main.py [--test]")
    else:
        start_kafka_listener()