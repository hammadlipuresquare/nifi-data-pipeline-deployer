"""Kafka consumer runner with message processing loop."""

import json
import time
from typing import Dict, Any, Optional
from confluent_kafka import Consumer, Producer, KafkaError
from ..config import config
from ..integrations import deploy_integration, get_available_integrations, deploy_aws_asset_registry_pipeline, \
    deploy_jumpcloud_pipeline
from ..exceptions import OrchestratorError
from ..logging import LoggerMixin


class KafkaWorker(LoggerMixin):
    """Kafka worker that processes deployment messages."""

    def __init__(self):
        super().__init__()
        self.consumer: Optional[Consumer] = None
        self.producer: Optional[Producer] = None
        self.running = False

    def initialize(self) -> None:
        """Initialize Kafka clients and prepare for message processing."""
        try:
            self.logger.info("Initializing Kafka consumer...")
            self._setup_kafka_clients()

            self.logger.info("Kafka consumer initialized successfully")
            self.logger.info(f"Kafka servers: {config.kafka_bootstrap_servers}")
            self.logger.info(f"Listening to: {config.kafka_deployment_topic}")
            self.logger.info(f"Responses to: {config.kafka_response_topic}")
            self.logger.info(f"Consumer group: {config.kafka_consumer_group}")

        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka worker: {e}")
            raise OrchestratorError(f"Kafka initialization failed: {e}")

    def start(self) -> None:
        """Start the Kafka message processing loop."""
        if not self.consumer:
            raise OrchestratorError("Kafka consumer not initialized. Call initialize() first.")

        try:
            self.running = True
            self.logger.info("Starting Kafka message processing...")
            self._print_startup_info()
            self._process_messages()

        except Exception as e:
            self.logger.error(f"Failed to start Kafka message processing: {e}")
            raise
        finally:
            self._cleanup()

    def stop(self) -> None:
        """Stop the Kafka worker."""
        self.logger.info("Stopping Kafka worker...")
        self.running = False

    def is_initialized(self) -> bool:
        """Check if Kafka consumer is initialized."""
        return self.consumer is not None and self.producer is not None

    def _setup_kafka_clients(self) -> None:
        """Set up Kafka consumer and producer."""
        # Consumer configuration
        consumer_config = {
            'bootstrap.servers': config.kafka_bootstrap_servers,
            'group.id': config.kafka_consumer_group,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': config.kafka_enable_auto_commit,
            'auto.commit.interval.ms': config.kafka_auto_commit_interval_ms,
            'session.timeout.ms': config.kafka_session_timeout_ms,
            'max.poll.interval.ms': config.kafka_max_poll_interval_ms,
        }

        # Producer configuration (simple local setup)
        producer_config = {
            'bootstrap.servers': config.kafka_bootstrap_servers,
            'acks': '1',  # Simplified for local development
            'retries': 3
        }

        self.consumer = Consumer(consumer_config)
        self.producer = Producer(producer_config)

        # Subscribe to deployment topic
        self.consumer.subscribe([config.kafka_deployment_topic])

        self.logger.info("Kafka clients set up successfully")

    def _process_messages(self) -> None:
        """Main message processing loop."""
        self.logger.info("Starting message processing loop")

        try:
            while self.running:
                try:
                    # Poll for messages
                    msg = self.consumer.poll(timeout=1.0)

                    if msg is None:
                        continue

                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        else:
                            self.logger.error(f"Kafka error: {msg.error()}")
                            continue

                    # Process the message
                    self.logger.info(f"Received message from {msg.topic()}:{msg.partition()}:{msg.offset()}")

                    try:
                        success = self._process_single_message(msg.value())

                        if success:
                            self.logger.info(f"Message processed successfully, offset: {msg.offset()}")
                        else:
                            self.logger.error(f"Message processing failed, offset: {msg.offset()}")

                        # Manual commit if auto-commit is disabled
                        if not config.kafka_enable_auto_commit:
                            self.consumer.commit(asynchronous=False)

                    except Exception as e:
                        self.logger.error(f"Error processing message: {e}")
                        # Still commit to avoid reprocessing the same bad message
                        if not config.kafka_enable_auto_commit:
                            self.consumer.commit(asynchronous=False)

                except KeyboardInterrupt:
                    self.logger.info("Received shutdown signal")
                    break
                except Exception as e:
                    self.logger.error(f"Error in message processing loop: {e}")
                    time.sleep(5)  # Wait before retrying

        except Exception as e:
            self.logger.error(f"Fatal error in message processing: {e}")
            raise

    def _process_single_message(self, raw_message: bytes) -> bool:
        """
        Process a single Kafka message.
        
        Args:
            raw_message: Raw message bytes
            
        Returns:
            True if processing succeeded, False otherwise
        """
        start_time = time.time()
        data = None

        try:
            # Parse message
            data = json.loads(raw_message.decode('utf-8'))

            # Extract fields (support both legacy and new formats)
            tenant_id = data.get("tenant_id", "")
            integration = data.get("integration") or data.get("pipeline", "").lower()
            parameters = data.get("parameters", {})
            api_key = parameters.get("api_key", "")
            message_id = data.get("message_id", "")
            flow_name = data.get("flow_name")  # Optional
            version = data.get("version", "latest")

            # Validate required fields
            if not tenant_id:
                raise ValueError("tenant_id is required")
            # if not api_key:
            #     raise ValueError("api_key is required in parameters")
            if not integration:
                raise ValueError("integration/pipeline is required")

            self.logger.info(f"🚀 Processing {integration} deployment for tenant: {tenant_id}")

            # Switch case logic for different integrations
            match integration:
                case "jumpcloud":
                    result = deploy_jumpcloud_pipeline(tenant_id=tenant_id, version=version)

                case "aws":
                    result = deploy_aws_asset_registry_pipeline(tenant_id=tenant_id, version=version)
                case _:
                    available = ", ".join(get_available_integrations())
                    raise ValueError(f"Unsupported integration '{integration}'. Available: {available}")

            # Send success response
            success_response = {
                "success": True,
                "tenant_id": tenant_id,
                "integration": integration,
                "timestamp": time.time(),
                "result": result,
                "error": None,
                "message_id": message_id,
                "processing_time_seconds": time.time() - start_time
            }

            self._send_response(success_response)
            self.logger.info(f"✅ Successfully deployed {integration} pipeline for {tenant_id}")
            return True

        except ValueError as e:
            # Validation errors - don't retry
            self.logger.error(f"❌ Validation error: {e}")
            return False

        except Exception as e:
            self.logger.error(f"❌ Deployment failed: {e}")

            # Send error response
            error_response = {
                "success": False,
                "tenant_id": data.get("tenant_id", "unknown") if data else "unknown",
                "integration": data.get("integration", data.get("pipeline", "unknown")) if data else "unknown",
                "timestamp": time.time(),
                "result": None,
                "error": str(e),
                "message_id": data.get("message_id", "") if data else "",
                "processing_time_seconds": time.time() - start_time,
                "error_type": type(e).__name__
            }

            self._send_response(error_response)
            return False

    def _send_response(self, response: Dict[str, Any]) -> None:
        """Send response message to Kafka."""
        try:
            message_json = json.dumps(response).encode('utf-8')

            def delivery_callback(err, msg):
                if err:
                    self.logger.error(f"Failed to deliver response: {err}")
                else:
                    self.logger.info(f"Response delivered to {msg.topic()}:{msg.partition()}:{msg.offset()} "
                                     f"for tenant {response.get('tenant_id', 'unknown')}")

            self.producer.produce(
                config.kafka_response_topic,
                value=message_json,
                callback=delivery_callback
            )

            # Flush to ensure delivery
            self.producer.flush(timeout=10)

        except Exception as e:
            self.logger.error(f"Failed to send response: {e}")

    def _print_startup_info(self) -> None:
        """Print startup information."""
        self.logger.info("Kafka worker ready for messages")

        # Show available integrations
        integrations = get_available_integrations()
        self.logger.info(f"Available integrations: {integrations}")
        self.logger.info("Adding new integrations is easy - just add a function to integrations.py!")

    def _cleanup(self) -> None:
        """Clean up Kafka clients."""
        try:
            if self.consumer:
                self.consumer.close()
            if self.producer:
                self.producer.flush()
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")

        self.logger.info("Kafka worker stopped")


# Global worker instance
kafka_worker = KafkaWorker()
