"""HashiCorp Vault integration using config.py settings."""

import hvac
from typing import Optional, Dict, Any
from ..config import config
from ..logging import LoggerMixin
from ..exceptions import OrchestratorError, SecretFetchError


class VaultAuthenticationError(SecretFetchError):
    """Raised when Vault authentication fails."""
    pass


class VaultConnectionError(SecretFetchError):
    """Raised when Vault connection or operation fails."""
    pass


class HashiCorpVault(LoggerMixin):
    """
    Simple HashiCorp Vault client using configuration from config.py.
    Use get_vault_client() function to get the singleton instance.
    """

    def __init__(self):
        """Initialize Vault client with config.py settings."""
        try:
            self.client = hvac.Client(url=config.vault_url, token=config.vault_token)
            self.logger.info(f"🔐 Initializing Vault client for: {config.vault_url}")

            # Authenticate based on available credentials
            self._authenticate()

            self.logger.info("✅ Vault client initialized and authenticated")

        except Exception as e:
            self.logger.error(f"Failed to initialize Vault client: {e}")
            raise VaultConnectionError(f"Vault initialization failed: {e}")

    def _authenticate(self) -> None:
        """Authenticate with Vault using config.py credentials."""
        try:
            # Token authentication
            if config.vault_token:
                self.client.token = config.vault_token
                if self.client.is_authenticated():
                    self.logger.info("✅ Vault authenticated using token")
                    return

            # AppRole authentication
            if config.vault_role_id and config.vault_secret_id:
                auth_response = self.client.auth.approle.login(
                    role_id=config.vault_role_id,
                    secret_id=config.vault_secret_id
                )

                if auth_response and 'auth' in auth_response:
                    self.client.token = auth_response['auth']['client_token']
                    self.logger.info("✅ Vault authenticated using AppRole")
                    return

            self.logger.warning("⚠️  No Vault authentication credentials configured")

        except Exception as e:
            self.logger.error(f"Vault authentication failed: {e}")
            raise VaultAuthenticationError(f"Failed to authenticate with Vault: {e}")

    def list_secrets(self, path):
        """List secrets at a given path"""
        try:
            response = self.client.secrets.kv.v2.list_secrets(path=path)
            return response['data']['keys']
        except Exception as e:
            raise VaultConnectionError(f"Failed to retrieve secret from Vault: {e}")

    def get_secret(self, path: str, key: Optional[str] = None) -> Any:
        """Get secret from Vault."""
        if not self.client.is_authenticated():
            raise VaultAuthenticationError("Not authenticated with Vault")

        try:
            response = self.client.secrets.kv.v2.read_secret_version(path=path, mount_point=config.vault_mount_point)

            secret_data = response['data']['data']

            if key:
                return secret_data.get(key)
            return secret_data

        except Exception as e:
            self.logger.error(f"Failed to get secret from path '{path}': {e}")
            raise VaultConnectionError(f"Failed to retrieve secret from Vault: {e}")

    def put_secret(self, path: str, secret_data: Dict[str, Any]) -> bool:
        """Store secret in Vault."""
        if not self.client.is_authenticated():
            raise VaultAuthenticationError("Not authenticated with Vault")

        try:
            self.client.secrets.kv.v2.create_or_update_secret(
                path=path,
                secret=secret_data,
                mount_point=config.vault_mount_point
            )

            self.logger.info(f"✅ Successfully stored secret at path: {path}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to store secret at path '{path}': {e}")
            raise VaultConnectionError(f"Failed to store secret in Vault: {e}")

    def is_authenticated(self) -> bool:
        """Check if authenticated with Vault."""
        return self.client and self.client.is_authenticated()

    def get_tenant_secret(self, tenant_id: str, secret_name: str) -> str:
        """
        Get a tenant-specific secret (convenience method).
        
        Args:
            tenant_id: Tenant identifier
            secret_name: Name of the secret (e.g., 'api_key', 'database_password')
            
        Returns:
            Secret value as string
        """
        path = f"tenants/{tenant_id}"
        return self.get_secret(path, secret_name)

    def store_tenant_secret(self, tenant_id: str, secret_name: str, secret_value: str) -> bool:
        """
        Store a tenant-specific secret (convenience method).
        
        Args:
            tenant_id: Tenant identifier
            secret_name: Name of the secret
            secret_value: Secret value to store
            
        Returns:
            True if successful
        """
        path = f"tenants/{tenant_id}"

        # Get existing secrets for this tenant (if any)
        try:
            existing_secrets = self.get_secret(path)
        except VaultConnectionError:
            # No existing secrets, start with empty dict
            existing_secrets = {}

        # Add/update the new secret
        existing_secrets[secret_name] = secret_value

        return self.put_secret(path, existing_secrets)


# Simple singleton instance
_vault_instance: Optional[HashiCorpVault] = None


def get_vault_client() -> HashiCorpVault:
    """
    Get the singleton HashiCorp Vault client instance.
    
    Returns the same authenticated Vault client throughout the application.
    
    Returns:
        HashiCorpVault: The singleton Vault client instance
        
    Raises:
        VaultConnectionError: If Vault initialization fails
        VaultAuthenticationError: If Vault authentication fails
    """
    global _vault_instance

    if _vault_instance is None:
        _vault_instance = HashiCorpVault()

    return _vault_instance


def reset_vault_client() -> None:
    """
    Reset the singleton Vault client (useful for testing).
    
    This will force creation of a new instance on next get_vault_client() call.
    """
    global _vault_instance
    _vault_instance = None
