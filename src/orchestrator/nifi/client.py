"""Centralized NiFi HTTP client with retry and error handling."""

import requests
import urllib3
from typing import Dict, Any, Optional
from ..config import config
from ..exceptions import NiFiAPIError, NiFiAuthenticationError
from ..logging import LoggerMixin

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class NiFiClient(LoggerMixin):
    """Centralized client for all NiFi HTTP operations."""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.verify = config.nifi_ssl_verify
        self.api_url = config.nifi_api_url.rstrip("/")
        self.base_url = config.nifi_base_url
        self._authenticated = False
        
    def authenticate(self) -> None:
        """Authenticate with NiFi and set up session headers."""
        if self._authenticated:
            return
            
        try:
            token = self._get_nifi_token(
                self.session, 
                self.base_url, 
                config.nifi_username, 
                config.nifi_password
            )
            self.session.headers.update({"Authorization": f"Bearer {token}"})
            self._authenticated = True
            self.logger.info("Successfully authenticated with NiFi")
        except Exception as e:
            self.logger.error(f"Failed to authenticate with NiFi: {e}")
            raise NiFiAuthenticationError(f"Authentication failed: {e}")
    
    def _get_nifi_token(self, session: requests.Session, base_url: str, user: str, pw: str) -> str:
        """Get NiFi authentication token."""
        endpoints = [f"{base_url}/access/token", f"{base_url}/nifi-api/access/token"]
        last_err = None
        
        for url in endpoints:
            try:
                r = session.post(url, data={"username": user, "password": pw})
                ct = r.headers.get("Content-Type", "")
                text = (r.text or "").strip()
                
                if r.ok and "html" not in ct.lower() and "\n" not in text and "." in text:
                    return text
                    
                last_err = f"{r.status_code} {ct} sample={text[:120]!r}"
            except Exception as e:
                last_err = str(e)
        
        raise NiFiAuthenticationError(
            f"Failed to obtain NiFi token. Details: {last_err}. "
            f"Ensure username/password login is enabled."
        )
    

    def get(self, endpoint: str, **kwargs) -> requests.Response:
        """Make authenticated GET request."""
        self.authenticate()
        url = f"{self.api_url}/{endpoint.lstrip('/')}"
        self.logger.debug(f"GET {url}")
        
        try:
            response = self.session.get(url, **kwargs)
            self._handle_response(response, "GET", url)
            return response
        except requests.RequestException as e:
            raise NiFiAPIError(f"GET {url} failed: {e}")

    def post(self, endpoint: str, **kwargs) -> requests.Response:
        """Make authenticated POST request."""
        self.authenticate()
        url = f"{self.api_url}/{endpoint.lstrip('/')}"
        self.logger.debug(f"POST {url}")
        
        try:
            response = self.session.post(url, **kwargs)
            self._handle_response(response, "POST", url)
            return response
        except requests.RequestException as e:
            raise NiFiAPIError(f"POST {url} failed: {e}")

    def put(self, endpoint: str, **kwargs) -> requests.Response:
        """Make authenticated PUT request."""
        self.authenticate()
        url = f"{self.api_url}/{endpoint.lstrip('/')}"
        self.logger.debug(f"PUT {url}")
        
        try:
            response = self.session.put(url, **kwargs)
            self._handle_response(response, "PUT", url)
            return response
        except requests.RequestException as e:
            raise NiFiAPIError(f"PUT {url} failed: {e}")

    def delete(self, endpoint: str, **kwargs) -> requests.Response:
        """Make authenticated DELETE request."""
        self.authenticate()
        url = f"{self.api_url}/{endpoint.lstrip('/')}"
        self.logger.debug(f"DELETE {url}")
        
        try:
            response = self.session.delete(url, **kwargs)
            self._handle_response(response, "DELETE", url)
            return response
        except requests.RequestException as e:
            raise NiFiAPIError(f"DELETE {url} failed: {e}")
    
    def _handle_response(self, response: requests.Response, method: str, url: str) -> None:
        """Handle HTTP response and raise appropriate exceptions."""
        if response.ok:
            self.logger.debug(f"{method} {url} succeeded ({response.status_code})")
            return
        
        error_msg = f"{method} {url} failed with {response.status_code}"
        
        # Add response details if available
        try:
            error_detail = response.text[:500] if response.text else "No response body"
            self.logger.error(f"{error_msg}: {error_detail}")
        except Exception:
            self.logger.error(f"{error_msg}: Unable to read response body")
        
        # Raise specific exception types based on status code
        if response.status_code == 401:
            self._authenticated = False  # Reset auth state
            raise NiFiAuthenticationError(
                f"Authentication failed: {error_msg}",
                status_code=response.status_code,
                response_text=response.text
            )
        elif response.status_code == 404:
            raise NiFiAPIError(
                f"Resource not found: {error_msg}",
                status_code=response.status_code,
                response_text=response.text
            )
        elif response.status_code == 409:
            raise NiFiAPIError(
                f"Conflict: {error_msg}",
                status_code=response.status_code,
                response_text=response.text
            )
        else:
            raise NiFiAPIError(
                error_msg,
                status_code=response.status_code,
                response_text=response.text
            )
    
    def get_json(self, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Make GET request and return JSON response."""
        response = self.get(endpoint, **kwargs)
        try:
            return response.json()
        except ValueError as e:
            raise NiFiAPIError(f"Invalid JSON response from {endpoint}: {e}")
    
    def post_json(self, endpoint: str, json_data: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """Make POST request with JSON data and return JSON response."""
        response = self.post(endpoint, json=json_data, **kwargs)
        try:
            return response.json()
        except ValueError as e:
            self.logger.error(f"Invalid JSON response from {endpoint}: {e}")
            raise NiFiAPIError(f"Invalid JSON response from {endpoint}: {e}")
    
    def put_json(self, endpoint: str, json_data: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """Make PUT request with JSON data and return JSON response."""
        response = self.put(endpoint, json=json_data, **kwargs)
        try:
            return response.json()
        except ValueError as e:
            self.logger.error(f"Invalid JSON response from {endpoint}: {e}")
            raise NiFiAPIError(f"Invalid JSON response from {endpoint}: {e}")


# Global client instance
nifi_client = NiFiClient()

