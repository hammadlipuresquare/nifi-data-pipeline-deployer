"""Centralized exception taxonomy for the orchestrator."""

from typing import Optional, Dict, Any


class OrchestratorError(Exception):
    """Base exception for all orchestrator errors."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}


class ConfigError(OrchestratorError):
    """Raised when configuration is invalid or missing."""
    pass


class MessageValidationError(OrchestratorError):
    """Raised when a Kafka message fails validation."""
    pass


class SecretFetchError(OrchestratorError):
    """Raised when fetching tenant secrets fails."""
    pass


class NiFiAPIError(OrchestratorError):
    """Raised when NiFi API calls fail."""
    
    def __init__(self, message: str, status_code: Optional[int] = None, 
                 response_text: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, details)
        self.status_code = status_code
        self.response_text = response_text


class NiFiAuthenticationError(NiFiAPIError):
    """Raised when NiFi authentication fails."""
    pass


class NiFiRegistryError(NiFiAPIError):
    """Raised when NiFi Registry operations fail."""
    pass


class IdempotencyConflict(OrchestratorError):
    """Raised when idempotent operations detect conflicting existing resources."""
    pass


class ServiceHandlerError(OrchestratorError):
    """Raised when service handler execution fails."""
    pass


class TenantDeploymentError(ServiceHandlerError):
    """Raised when tenant deployment operations fail."""
    pass
