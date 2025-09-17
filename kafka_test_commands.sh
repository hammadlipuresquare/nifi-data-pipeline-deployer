#!/bin/bash

echo "🔧 KAFKA TESTING COMMANDS"
echo "=========================="
echo

# Kafka broker details
KAFKA_BROKER="localhost:9093"
DEPLOYMENT_TOPIC="nifi-pipeline-deployments"
RESPONSE_TOPIC="nifi-pipeline-responses"

echo "📋 KAFKA CONFIGURATION:"
echo "   Broker: $KAFKA_BROKER"
echo "   Deployment Topic: $DEPLOYMENT_TOPIC"
echo "   Response Topic: $RESPONSE_TOPIC"
echo

echo "🚀 TESTING COMMANDS:"
echo "-------------------"
echo

echo "1️⃣  Test JumpCloud Deployment:"
echo "kafka-console-producer --bootstrap-server $KAFKA_BROKER --topic $DEPLOYMENT_TOPIC"
echo "Then paste this payload:"
cat << 'JUMPCLOUD'
{"tenant_id": "TestCustomer", "integration": "jumpcloud", "parameters": {"api_key": "test-jumpcloud-api-key-123"}, "flow_name": "JumpCloudPipelineAsset", "version": "latest", "message_id": "jumpcloud-test-001"}
JUMPCLOUD
echo
echo

echo "2️⃣  Test AWS Deployment:"
echo "kafka-console-producer --bootstrap-server $KAFKA_BROKER --topic $DEPLOYMENT_TOPIC"
echo "Then paste this payload:"
cat << 'AWS'
{"tenant_id": "TestCustomer", "integration": "aws", "parameters": {"api_key": "test-aws-access-key-123"}, "flow_name": "aws", "version": "latest", "message_id": "aws-test-001"}
AWS
echo
echo

echo "3️⃣  Test Custom Integration:"
echo "kafka-console-producer --bootstrap-server $KAFKA_BROKER --topic $DEPLOYMENT_TOPIC"
echo "Then paste this payload:"
cat << 'CUSTOM'
{"tenant_id": "TestCustomer", "integration": "custom", "parameters": {"api_key": "test-custom-key-123"}, "flow_name": "CustomFlow", "version": "1.0", "message_id": "custom-test-001"}
CUSTOM
echo
echo

echo "4️⃣  Monitor Responses:"
echo "kafka-console-consumer --bootstrap-server $KAFKA_BROKER --topic $RESPONSE_TOPIC --from-beginning"
echo

echo "5️⃣  One-liner Test (JumpCloud):"
echo "echo '{\"tenant_id\": \"QuickTest\", \"integration\": \"jumpcloud\", \"parameters\": {\"api_key\": \"quick-test-key\"}, \"message_id\": \"quick-001\"}' | kafka-console-producer --bootstrap-server $KAFKA_BROKER --topic $DEPLOYMENT_TOPIC"
echo

echo "💡 NOTES:"
echo "   - Make sure Kafka is running on localhost:9093"
echo "   - Start your orchestrator app first: python -m src.orchestrator.app"
echo "   - Monitor responses in a separate terminal"
echo "   - Check orchestrator logs for detailed processing info"

