"""Main application entry point for the orchestrator."""

import sys
import signal
from typing import Optional
from .config import config
from .logging import setup_logging, get_logger
from .consumer.kafka_runner import kafka_worker
from .exceptions import OrchestratorError


class OrchestratorApp:
    """Main orchestrator application."""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self._setup_signal_handlers()
    
    def _setup_signal_handlers(self) -> None:
        """Set up signal handlers for graceful shutdown."""
        def signal_handler(signum, frame):
            self.logger.info(f"Received signal {signum}, shutting down gracefully...")
            
            # Stop Kafka consumer if it's running
            if kafka_worker.is_initialized():
                self.logger.info("Stopping Kafka consumer...")
                kafka_worker.stop()
                
            self.logger.info("Graceful shutdown completed")
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    def bootstrap(self) -> None:
        """Bootstrap the application."""
        try:
            # Set up logging
            setup_logging(
                level=config.log_level,
                format_type=config.log_format
            )
            
            self.logger.info("Starting Orchestrator application")
            self.logger.info(f"Configuration loaded: NiFi={config.nifi_api_url}, Kafka={config.kafka_bootstrap_servers}")
            
            # Check available integrations
            # integrations = get_available_integrations()
            # self.logger.info(f"Available integrations: {integrations}")
            #
            # Validate configuration
            self._validate_configuration()
            
            # Initialize Kafka consumer during bootstrap
            self.logger.info("Initializing Kafka consumer...")
            kafka_worker.initialize()
            
            # Verify Kafka consumer initialization
            self._verify_kafka_initialization()
            
            # Run health checks
            self._run_health_checks()
            
            self.logger.info("Application bootstrap completed successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to bootstrap application: {e}")
            raise OrchestratorError(f"Bootstrap failed: {e}")
    
    def _validate_configuration(self) -> None:
        """Validate critical configuration."""
        errors = []
        
        # Check required NiFi configuration
        if not config.nifi_username or not config.nifi_password:
            errors.append("NiFi username and password are required")
        
        # Check Kafka configuration
        if not config.kafka_bootstrap_servers:
            errors.append("Kafka bootstrap servers are required")
        
        if not config.kafka_deployment_topic:
            errors.append("Kafka deployment topic is required")
        
        if not config.kafka_response_topic:
            errors.append("Kafka response topic is required")
        
        # Check secrets configuration
        if config.secrets_provider not in ["env", "aws_secrets_manager", "vault"]:
            errors.append(f"Invalid secrets provider: {config.secrets_provider}")
        
        if errors:
            error_msg = "Configuration validation failed: " + "; ".join(errors)
            self.logger.error(error_msg)
            raise OrchestratorError(error_msg)
        
        self.logger.info("Configuration validation passed")
    
    def _verify_kafka_initialization(self) -> None:
        """Verify Kafka consumer is properly initialized."""
        if not kafka_worker.is_initialized():
            raise OrchestratorError("Kafka consumer failed to initialize properly")
        
        self.logger.info("✅ Kafka consumer initialized successfully")
    
    def _run_health_checks(self) -> None:
        """Run health checks on initialized components."""
        self.logger.info("Running application health checks...")
        
        # Check Kafka consumer health
        if kafka_worker.is_initialized():
            self.logger.info("✅ Kafka consumer: HEALTHY")
        else:
            self.logger.warning("⚠️  Kafka consumer: NOT INITIALIZED")

        self.logger.info("Health checks completed")
    
    def run(self) -> None:
        """Run the main application."""
        try:
            # Bootstrap the application (includes Kafka consumer initialization)
            self.bootstrap()
            
            # Start the Kafka message processing loop
            self.logger.info("Starting Kafka message processing loop...")
            kafka_worker.start()
            
        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt, shutting down...")
            kafka_worker.stop()
        except Exception as e:
            self.logger.error(f"Application error: {e}")
            kafka_worker.stop()
            raise


def main():
    """Main entry point."""
    app = OrchestratorApp()
    app.run()


if __name__ == "__main__":
    main()
