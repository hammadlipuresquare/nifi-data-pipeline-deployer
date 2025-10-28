"""Configuration management using Pydantic BaseSettings."""

from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings


class OrchestratorConfig(BaseSettings):
    """Main configuration for the orchestrator service."""
    
    # Orchestrator namespacing
    orchestrator_env: str = Field(default="dev", env="ORCHESTRATOR_ENV")
    orchestrator_cluster: str = Field(default="local", env="ORCHESTRATOR_CLUSTER")

    # NiFi Configuration
    nifi_api_url: str = Field(default="https://localhost:8443/nifi-api", env="NIFI_API_URL")
    nifi_registry_url: str = Field(default="http://localhost:18080/nifi-registry-api", env="NIFI_REGISTRY_URL")
    nifi_username: str = Field(default="admin", env="NIFI_USERNAME")
    nifi_password: str = Field(default="", env="NIFI_PASSWORD")
    nifi_ssl_verify: bool = Field(default=False, env="NIFI_SSL_VERIFY")
    
    # Kafka Configuration (for orchestrator)
    kafka_bootstrap_servers: str = Field(default="localhost:9093", env="KAFKA_BOOTSTRAP_SERVERS")
    kafka_deployment_topic: str = Field(default="nifi-pipeline-deployments", env="KAFKA_DEPLOYMENT_TOPIC")
    kafka_credentials_topic: str = Field(default="nifi-credentials-updates", env="KAFKA_CREDENTIALS_TOPIC")
    kafka_deletions_topic: str = Field(default="nifi-integration-deletions", env="KAFKA_DELETIONS_TOPIC")
    kafka_response_topic: str = Field(default="nifi-pipeline-responses", env="KAFKA_RESPONSE_TOPIC")
    kafka_consumer_group: str = Field(default="nifi-pipeline-deployer", env="KAFKA_CONSUMER_GROUP")
    kafka_enable_auto_commit: bool = Field(default=False, env="KAFKA_ENABLE_AUTO_COMMIT")
    kafka_auto_commit_interval_ms: int = Field(default=5000, env="KAFKA_AUTO_COMMIT_INTERVAL_MS")
    kafka_session_timeout_ms: int = Field(default=30000, env="KAFKA_SESSION_TIMEOUT_MS")
    kafka_max_poll_records: int = Field(default=5, env="KAFKA_MAX_POLL_RECORDS")
    kafka_max_poll_interval_ms: int = Field(default=300000, env="KAFKA_MAX_POLL_INTERVAL_MS")
    backoff_base_ms: int = Field(default=500, env="KAFKA_BACKOFF_BASE_MS")
    backoff_max_ms: int = Field(default=60000, env="KAFKA_BACKOFF_MAX_MS")
    kafka_security_protocol: str = Field(default="SASL_PLAINTEXT", env="KAFKA_SECURITY_PROTOCOL")
    kafka_sasl_mechanism: str = Field(default="PLAIN", env="KAFKA_SASL_MECHANISM")
    kafka_sasl_username: str = Field(default="user1", env="KAFKA_SASL_USERNAME")
    kafka_sasl_password: str = Field(default="", env="KAFKA_SASL_PASSWORD")

    # Redis Configuration
    redis_host: str = Field(default="localhost", env="REDIS_HOST")
    redis_port: int = Field(default=6379, env="REDIS_PORT")
    redis_db: int = Field(default=0, env="REDIS_DB")
    redis_password: Optional[str] = Field(default=None, env="REDIS_PASSWORD")
    redis_ssl: bool = Field(default=False, env="REDIS_SSL")

    # NiFi Pipeline Kafka Configuration (for deployed pipelines)
    nifi_kafka_bootstrap_servers: str = Field(default="kafka.apache:9092", env="NIFI_KAFKA_BOOTSTRAP_SERVERS")
    nifi_kafka_security_protocol: str = Field(default="SASL_PLAINTEXT", env="NIFI_KAFKA_SECURITY_PROTOCOL")
    nifi_kafka_sasl_mechanism: str = Field(default="PLAIN", env="NIFI_KAFKA_SASL_MECHANISM")
    nifi_kafka_sasl_username: str = Field(default="user1", env="NIFI_KAFKA_SASL_USERNAME")
    nifi_kafka_sasl_password: str = Field(default="", env="NIFI_KAFKA_SASL_PASSWORD")
    
    # Retry Configuration
    max_deployment_retries: int = Field(default=3, env="MAX_DEPLOYMENT_RETRIES")
    retry_delay_seconds: int = Field(default=60, env="RETRY_DELAY_SECONDS")
    
    # Logging
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    log_format: str = Field(default="json", env="LOG_FORMAT")
    
    # Secrets Provider
    secrets_provider: str = Field(default="env", env="SECRETS_PROVIDER")
    
    # HashiCorp Vault Configuration (when secrets_provider="vault")
    vault_url: str = Field(default="http://localhost:8200", env="VAULT_URL")
    vault_token: Optional[str] = Field(default=None, env="VAULT_TOKEN")
    vault_role_id: Optional[str] = Field(default=None, env="VAULT_ROLE_ID")
    vault_secret_id: Optional[str] = Field(default=None, env="VAULT_SECRET_ID")
    vault_mount_point: str = Field(default="secret", env="VAULT_MOUNT_POINT")
    vault_ca_cert: Optional[str] = Field(default=None, env="VAULT_CA_CERT")
    vault_verify_ssl: bool = Field(default=True, env="VAULT_VERIFY_SSL")
    
    class Config:
        env_file = ".env"
        case_sensitive = False
        extra = "ignore"  # Ignore extra environment variables
        
    @property
    def nifi_base_url(self) -> str:
        """Get NiFi base URL from API URL."""
        if self.nifi_api_url.endswith("/nifi-api"):
            return self.nifi_api_url[:-len("/nifi-api")]
        return self.nifi_api_url
        
    @property
    def kafka_servers_list(self) -> list[str]:
        """Get Kafka servers as a list."""
        return self.kafka_bootstrap_servers.split(',')


# Global config instance
config = OrchestratorConfig()
