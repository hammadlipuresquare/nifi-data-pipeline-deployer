"""Centralized integration parameters configuration."""

from typing import Dict, Any, Optional


# Simple, clean parameter definitions
INTEGRATION_PARAMETERS = {
    "jumpcloud": {
        "JC_API_KEY": {
            "description": "JumpCloud API key for authentication",
            "sensitive": True,
            "source": "api_key"
        },
        "TENANT_ID": {
            "description": "Tenant identifier for multi-tenancy",
            "sensitive": False,
            "source": "tenant_id"
        }
    },
    "aws": {
        "TENANT_ID": {
            "description": "Tenant identifier for multi-tenancy",
            "sensitive": False,
            "source": "tenant_id"
        },
        "AWS_ACCESS_KEY": {
            "description": "AWS access key ID for authentication",
            "sensitive": True,
            "source": "access_key_id"
        },
        "AWS_SECRET_ACCESS_KEY": {
            "description": "AWS secret access key for authentication",
            "sensitive": True,
            "source": "secret_access_key"
        }
    }
}


# Helper functions
def get_required_parameters(integration_type: str) -> Dict[str, Any]:
    """Get required parameters for an integration."""
    params = INTEGRATION_PARAMETERS.get(integration_type, {})
    return {k: v for k, v in params.items() if v.get("required", True)}


def get_sensitive_parameters(integration_type: str) -> Dict[str, Any]:
    """Get sensitive parameters for an integration."""
    params = INTEGRATION_PARAMETERS.get(integration_type, {})
    return {k: v for k, v in params.items() if v.get("sensitive", False)}


def validate_parameters(integration_type: str, provided_params: Dict[str, Any]) -> Dict[str, Any]:
    """Validate provided parameters against integration requirements."""
    if integration_type not in INTEGRATION_PARAMETERS:
        return {
            "valid": False,
            "errors": [f"Unknown integration type: {integration_type}"]
        }
    
    required_params = get_required_parameters(integration_type)
    missing = []
    
    for param_name, param_config in required_params.items():
        source_key = param_config["source"]
        if source_key not in provided_params:
            missing.append(f"{param_name} (from: {source_key})")
    
    return {
        "valid": len(missing) == 0,
        "missing_required": missing,
        "errors": [f"Missing required parameters: {', '.join(missing)}"] if missing else []
    }


def get_supported_integrations() -> list[str]:
    """Get list of supported integration types."""
    return list(INTEGRATION_PARAMETERS.keys())


# Simple exports
__all__ = [
    "INTEGRATION_PARAMETERS",
    "get_required_parameters",
    "get_sensitive_parameters", 
    "validate_parameters",
    "get_supported_integrations"
]
