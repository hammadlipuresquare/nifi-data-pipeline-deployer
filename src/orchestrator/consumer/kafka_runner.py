"""Kafka consumer runner with message processing loop."""

import json
import time
from typing import Dict, Any, Optional, Tuple
from confluent_kafka import Consumer, Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from ..config import config
from ..integrations import (update_integration_secrets, stop_integration, handle_deployment)
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
            self.logger.info(
                f"Listening to: {config.kafka_deployment_topic}, {config.kafka_credentials_topic}, {config.kafka_deletions_topic}"
            )
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

    def _on_error(self, err):
        # Called by librdkafka on internal errors. Non-fatal errors are retried automatically.
        try:
            self.logger.warning(f"Kafka client error callback: {err}")
        except Exception:
            pass

    def _on_assign(self, consumer, partitions):
        try:
            self.logger.info(f"Partitions assigned: {partitions}")
        except Exception:
            pass

    def _on_revoke(self, consumer, partitions):
        try:
            self.logger.info(f"Partitions revoked: {partitions}")
        except Exception:
            pass

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
            # Hardening: keep-alive and bounded reconnect backoff
            'socket.keepalive.enable': True,
            'reconnect.backoff.ms': 500,  # initial backoff before reconnect
            'reconnect.backoff.max.ms': 30000,  # cap reconnect backoff to 30s
            'request.timeout.ms': 30000,  # network request timeout (including JoinGroup)
            'heartbeat.interval.ms': 10000,  # must be lower than session.timeout.ms
            'statistics.interval.ms': 60000,  # emit internal stats every 60s
            'error_cb': self._on_error,
        }

        # Producer configuration (simple local setup)
        producer_config = {
            'bootstrap.servers': config.kafka_bootstrap_servers,
            'acks': '1',  # Simplified for local development
            'retries': 3,
            'error_cb': self._on_error,
        }

        # --- Optional authentication (SASL/SSL) ---
        # Configure from config if provided; keeps plaintext as default.
        security_protocol = getattr(config, "kafka_security_protocol", None)
        if security_protocol:
            consumer_config["security.protocol"] = security_protocol
            producer_config["security.protocol"] = security_protocol

            # If not PLAINTEXT, wire up SASL params when available.
            if str(security_protocol).upper() != "PLAINTEXT":
                sasl_mechanism = getattr(config, "kafka_sasl_mechanism", None)
                sasl_username = getattr(config, "kafka_sasl_username", None)
                sasl_password = getattr(config, "kafka_sasl_password", None)

                if sasl_mechanism:
                    consumer_config["sasl.mechanism"] = sasl_mechanism
                    producer_config["sasl.mechanism"] = sasl_mechanism
                if sasl_username:
                    consumer_config["sasl.username"] = sasl_username
                    producer_config["sasl.username"] = sasl_username
                if sasl_password:
                    consumer_config["sasl.password"] = sasl_password
                    producer_config["sasl.password"] = sasl_password

        self.consumer = Consumer(consumer_config)
        self.producer = Producer(producer_config)

        # Subscribe to all topics in the same consumer group
        self.consumer.subscribe(
            [
                config.kafka_deployment_topic,
                config.kafka_credentials_topic,
                config.kafka_deletions_topic,
            ],
            on_assign=self._on_assign,
            on_revoke=self._on_revoke
        )

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
                        elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                            try:
                                missing_topic = msg.topic()
                                self.logger.warning(
                                    f"Topic '{missing_topic}' not found. Attempting to create it..."
                                )
                                self._ensure_topic_exists(missing_topic)
                                # Give broker a moment to register the new topic
                                time.sleep(1.0)
                            except Exception as e:
                                self.logger.error(f"Failed to ensure topic exists: {e}")
                            continue
                        else:
                            self.logger.error(f"Kafka error: {msg.error()}")
                            continue

                    # Process the message by topic
                    self.logger.info(f"Received message from {msg.topic()}:{msg.partition()}:{msg.offset()}")

                    try:
                        topic = msg.topic()
                        message = msg.value()

                        match topic:
                            case config.kafka_deployment_topic:
                                success = self._process_deployment_message(message)
                            case config.kafka_credentials_topic:
                                success = self._process_credentials_update(message)
                            case config.kafka_deletions_topic:
                                success = self._process_deletion_message(message)
                            case _:
                                self.logger.warning(f"Unknown Topic: {topic} skipping")
                                success = False

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

    def _process_deployment_message(self, raw_message: bytes) -> bool:
        """
        Process deployment message.
        """
        start_time = time.time()
        data = None

        try:
            tenant_id, integration_name = self._extract_and_validate_tenant_id_and_integration(raw_message)

            self.logger.info(f"Processing {integration_name} deployment for tenant: {tenant_id}")

            if integration_name in {"aws", "aws_role"}:
                print(f"Received steampipe pipeline integration_name: {integration_name}")

                aws_asset_register_kafka_topic = 'asset_register_trigger'

                payload = {
                    "tenant_id": tenant_id,
                    "module": "asset_register",
                    "event_type": "AssetRegisterTrigger"
                }

                message_json = json.dumps(payload).encode("utf-8")
                self.producer.produce(
                    aws_asset_register_kafka_topic,
                    value=message_json,
                    callback=lambda err, msg: self.logger.error(f"Produce failed: {err}") if err else None
                )
                self.producer.poll(0)
            else:
                handle_deployment(tenant_id, integration_name)

                self.logger.info(f"Successfully deployed {integration_name} pipeline for {tenant_id}")
            return True
        except ValueError as e:
            # Validation errors - don't retry
            self.logger.error(f"Validation error: {e}")
            return False

        except Exception as e:
            self.logger.error(f"Deployment failed: {e}")

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

    def _process_credentials_update(self, raw_message: bytes) -> bool:
        """Handle credentials rotation events: upsert Vault secrets into tenant parameter context."""
        try:
            tenant_id, integration_name = self._extract_and_validate_tenant_id_and_integration(raw_message)

            update_integration_secrets(tenant_id, integration_name)
            return True
        except Exception as e:
            self.logger.error(f"Credentials update failed: {e}")
            return False

    def _process_deletion_message(self, raw_message: bytes) -> bool:
        """Handle deletion events: stop the integration process group for a tenant."""
        try:
            tenant_id, integration_name = self._extract_and_validate_tenant_id_and_integration(raw_message)

            stop_integration(tenant_id, integration_name)
            return True
        except Exception as e:
            self.logger.error(f"Deletion handling failed: {e}")
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
        self.logger.info("Kafka worker ready for messages")

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

    def _ensure_topic_exists(self, topic: str, num_partitions: int = 1, replication_factor: int = 1) -> None:
        """Create a Kafka topic if it does not exist (idempotent)."""
        try:
            admin = AdminClient({'bootstrap.servers': config.kafka_bootstrap_servers})
            md = admin.list_topics(timeout=5)
            if topic in md.topics and not md.topics[topic].error:
                self.logger.info(f"Topic '{topic}' already exists")
                return

            fs = admin.create_topics([NewTopic(topic, num_partitions=num_partitions,
                                               replication_factor=replication_factor)])
            # Wait for result
            for t, f in fs.items():
                try:
                    f.result(timeout=10)
                    self.logger.info(f"✅ Created topic '{t}'")
                except Exception as e:
                    # If it's already created by a race, log and continue
                    self.logger.info(f"Topic '{t}' create result: {e}")
        except Exception as e:
            self.logger.warning(f"Could not ensure topic '{topic}' exists: {e}")

    def _extract_and_validate_tenant_id_and_integration(self, raw_message) -> Tuple[str, str]:
        """Extract and validate tenant ID and integration."""
        data = json.loads(raw_message.decode("utf-8"))
        tenant_id = data.get("tenant_id")
        integration = (data.get("integration") or "").lower()
        if not tenant_id or not integration: raise ValueError(
            "tenant_id and integration are required for credentials updates")
        return tenant_id, integration


# Global worker instance
kafka_worker = KafkaWorker()
