"""NiFi controller service management."""

import time
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from .client import nifi_client
from ..config import config
from ..exceptions import NiFiAPIError
from ..logging import LoggerMixin, mask_sensitive_data


@dataclass
class ControllerServiceInfo:
    """Information about a controller service."""
    id: str
    name: str
    type: str
    state: str
    is_kafka_service: bool = False


class ControllerServiceManager(LoggerMixin):
    """Manager for NiFi controller service operations."""

    def get_controller_service(self, service_id: str) -> Optional[Dict[str, Any]]:
        """Get controller service by ID."""
        try:
            return nifi_client.get_json(f"controller-services/{service_id}")
        except NiFiAPIError as e:
            if e.status_code == 404:
                return None
            raise

    def list_controller_services(self, pg_id: str) -> List[Dict[str, Any]]:
        """List controller services in a process group."""
        data = nifi_client.get_json(f"flow/process-groups/{pg_id}/controller-services")
        return data.get("controllerServices", [])

    def get_kafka_services(self, pg_id: str) -> List[ControllerServiceInfo]:
        """Get Kafka-related controller services in a process group."""
        services = self.list_controller_services(pg_id)
        kafka_services = []

        for service in services:
            service_info = service.get("component", {})
            service_type = service_info.get("type", "")
            service_name = service_info.get("name", "Unknown")


            is_kafka_service = service_name == "Kafka3ConnectionService"

            if is_kafka_service:
                kafka_services.append(ControllerServiceInfo(
                    id=service["id"],
                    name=service_name,
                    type=service_type,
                    state=service_info.get("state", "UNKNOWN"),
                    is_kafka_service=True
                ))

        return kafka_services

    def disable_controller_service(self, service_id: str, service_name: str,
                                   max_wait_seconds: int = 30) -> bool:
        """Disable a controller service and wait for completion."""
        try:
            current_service = self.get_controller_service(service_id)
            if not current_service:
                return False

            # Check current state
            current_state = current_service["component"]["state"]
            if current_state == "DISABLED":
                self.logger.info(f"{service_name} is already disabled")
                return True

            # Disable the service
            update_body = {
                "revision": current_service["revision"],
                "disconnectedNodeAcknowledged": False,
                "component": {
                    "id": service_id,
                    "state": "DISABLED"
                }
            }

            response = nifi_client.put_json(f"controller-services/{service_id}", update_body)
            if response:
                self.logger.info(f"Disabling {service_name}...")

                # Wait for service to be fully disabled
                final_state = self._wait_for_service_state(
                    service_id, service_name, "DISABLED", max_wait_seconds
                )

                if final_state == "DISABLED":
                    self.logger.info(f"Successfully disabled {service_name}")
                    return True
                else:
                    self.logger.error(f"Failed to disable {service_name} (state: {final_state})")
                    return False

        except NiFiAPIError as e:
            if e.status_code == 409:
                self.logger.warning(f"Cannot disable {service_name}: Service has dependencies (409 Conflict)")
                self.logger.info(f"This usually means the service is actively used by processors")
            else:
                self.logger.error(f"Failed to disable {service_name}: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Error disabling {service_name}: {e}")
            return False

    def enable_controller_service(self, service_id: str, service_name: str,
                                  max_wait_seconds: int = 30) -> bool:
        """Enable a controller service and wait for completion."""
        try:
            current_service = self.get_controller_service(service_id)
            if not current_service:
                return False

            current_state = current_service["component"]["state"]

            # Check if already enabled
            if current_state == "ENABLED":
                self.logger.info(f"{service_name} is already enabled")
                return True
            elif current_state == "ENABLING":
                self.logger.info(f"{service_name} is currently enabling, waiting...")
                final_state = self._wait_for_service_state(
                    service_id, service_name, "ENABLED", max_wait_seconds
                )
                return final_state == "ENABLED"

            # Enable the service
            update_body = {
                "revision": current_service["revision"],
                "disconnectedNodeAcknowledged": False,
                "component": {
                    "id": service_id,
                    "state": "ENABLED"
                }
            }

            response = nifi_client.put_json(f"controller-services/{service_id}", update_body)
            if response:
                self.logger.info(f"Enabling {service_name}...")

                # Wait for the service to actually become enabled
                final_state = self._wait_for_service_state(
                    service_id, service_name, "ENABLED", max_wait_seconds
                )

                if final_state == "ENABLED":
                    self.logger.info(f"Successfully enabled {service_name}")
                    return True
                else:
                    self.logger.warning(f"Enable request sent but final state: {final_state}")
                    return False

        except Exception as e:
            self.logger.error(f"Error enabling {service_name}: {e}")
            return False

    def _wait_for_service_state(self, service_id: str, service_name: str,
                                target_state: str, max_wait_seconds: int) -> str:
        """Wait for a controller service to reach target state."""
        self.logger.info(f"Waiting for {service_name} to reach {target_state} (max {max_wait_seconds}s)")

        start_time = time.time()

        while time.time() - start_time < max_wait_seconds:
            try:
                service = self.get_controller_service(service_id)
                if service:
                    current_state = service["component"]["state"]

                    if current_state == target_state:
                        return target_state
                    elif current_state in ["DISABLING", "ENABLING"]:
                        # Still transitioning, wait a bit more
                        time.sleep(2)
                    else:
                        self.logger.warning(f"{service_name} in unexpected state: {current_state}")
                        return current_state
                else:
                    self.logger.error(f"Failed to check {service_name} state: service not found")
                    time.sleep(2)

            except Exception as e:
                self.logger.error(f"Error checking {service_name} state: {e}")
                time.sleep(2)

        self.logger.error(f"Timeout waiting for {service_name} to reach {target_state}")
        return "TIMEOUT"

    def configure_kafka_services(self, pg_id: str, tenant_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Configure Kafka controller services in a process group."""
        self.logger.info(f"Configuring Kafka controller services for process group: {pg_id}")

        # Get essential Kafka configuration from environment
        essential_config = {
            "bootstrap.servers": config.nifi_kafka_bootstrap_servers,
            "security.protocol": config.nifi_kafka_security_protocol,
        }

        # Only add authentication properties if they're configured
        if config.nifi_kafka_security_protocol != "PLAINTEXT":
            essential_config.update({
                "sasl.mechanism": config.nifi_kafka_sasl_mechanism,
                "sasl.username": config.nifi_kafka_sasl_username,
                "sasl.password": config.nifi_kafka_sasl_password,
                "username": config.nifi_kafka_sasl_username,  # Alternative property name
                "password": config.nifi_kafka_sasl_password,  # Alternative property name
            })

        kafka_services = self.get_kafka_services(pg_id)
        kafka_services_configured = []

        for service_info in kafka_services:
            self.logger.info(f"Found Kafka service: {service_info.name}")

            # Disable service if enabled to modify properties
            if service_info.state == "ENABLED":
                disable_success = self.disable_controller_service(
                    service_info.id, service_info.name
                )
                if not disable_success:
                    self.logger.error(f"Failed to disable {service_info.name} - skipping configuration")
                    continue
            elif service_info.state == "ENABLING":
                self.logger.warning(f"Service {service_info.name} is ENABLING - forcing disable")
                # Force disable stuck service
                disable_success = self.disable_controller_service(
                    service_info.id, service_info.name, max_wait_seconds=15
                )
                if not disable_success:
                    continue

            try:
                # Configure the service
                configured = self._configure_service_properties(
                    service_info.id, service_info.name, essential_config
                )

                # Always enable Kafka services after configuration
                enabled = self.enable_controller_service(service_info.id, service_info.name)

                kafka_services_configured.append({
                    "name": service_info.name,
                    "type": service_info.type,
                    "id": service_info.id,
                    "configured": configured,
                    "enabled": enabled,
                    "final_state": "ENABLED" if enabled else "CONFIGURATION_FAILED",
                    "original_state": service_info.state
                })

            except Exception as e:
                self.logger.warning(f"Error configuring {service_info.name}: {e}")

        if kafka_services_configured:
            self.logger.info(f"Successfully configured {len(kafka_services_configured)} Kafka controller services")
        else:
            self.logger.info("No Kafka controller services needed configuration")

        return kafka_services_configured

    def _configure_service_properties(self, service_id: str, service_name: str,
                                      essential_config: Dict[str, str]) -> bool:
        """Configure essential properties for a Kafka service."""
        try:
            current_service = self.get_controller_service(service_id)
            if not current_service:
                return False

            properties = current_service["component"].get("properties", {})
            descriptors = current_service["component"].get("descriptors", {})

            # Prepare updates
            updated_properties = properties.copy()
            updates_made = False

            # Update ONLY empty essential properties
            for prop_name in properties.keys():
                # current_value = properties.get(prop_name, "")
                #
                # # Skip if property already has a value
                # if current_value and current_value.strip() != "":
                #     continue

                # Check for essential property matches
                if prop_name in essential_config and essential_config[prop_name]:
                    updated_properties[prop_name] = essential_config[prop_name]
                    updates_made = True

                    # Check if this is a sensitive property
                    descriptor = descriptors.get(prop_name, {})
                    is_sensitive = descriptor.get("sensitive", False)

                    self.logger.info(f"  Setting {prop_name} ({'sensitive' if is_sensitive else 'non-sensitive'})")

            # Apply updates if any were made
            if updates_made:
                update_body = {
                    "revision": current_service["revision"],
                    "disconnectedNodeAcknowledged": False,
                    "component": {
                        "id": service_id,
                        "name": service_name,
                        "properties": updated_properties
                    }
                }

                response = nifi_client.put_json(f"controller-services/{service_id}", update_body)
                if response:
                    self.logger.info(f"  Configured Kafka service: {service_name}")
                    return True
                else:
                    return False
            else:
                self.logger.info(f"  {service_name} already has essential properties configured")
                return True

        except Exception as e:
            self.logger.error(f"Error configuring properties for {service_name}: {e}")
            return False

    def enable_all_services(self, pg_id: str) -> List[Dict[str, Any]]:
        """Enable all controller services in a process group."""
        self.logger.info(f"Enabling all controller services for process group: {pg_id}")

        try:
            # Use the bulk enable API endpoint
            body = {
                "id": pg_id,
                "state": "ENABLED",
                "disconnectedNodeAcknowledged": False
            }

            response = nifi_client.put_json(f"flow/process-groups/{pg_id}/controller-services", body)

            if response:
                self.logger.info(f"Successfully enabled all controller services for process group {pg_id}")

                # Extract controller service information from response
                controller_services = response.get("controllerServices", [])
                enabled_services = []

                for service in controller_services:
                    service_info = service.get("component", {})
                    enabled_services.append({
                        "id": service.get("id"),
                        "name": service_info.get("name", "Unknown"),
                        "state": service_info.get("state", "Unknown"),
                        "status": "enabled"
                    })
                    self.logger.info(
                        f"  Controller Service: {service_info.get('name', 'Unknown')} -> {service_info.get('state', 'Unknown')}")

                return enabled_services
            else:
                return []

        except Exception as e:
            self.logger.warning(f"Failed to enable controller services (may not be supported): {e}")
            return []
    def get_aws_services(self, pg_id: str) -> List[ControllerServiceInfo]:
        """Get Kafka-related controller services in a process group."""
        services = self.list_controller_services(pg_id)
        aws_services = []

        for service in services:
            service_info = service.get("component", {})
            service_type = service_info.get("type", "")
            service_name = service_info.get("name", "Unknown")

            is_aws_service = service_name == "AWSCredentialsProviderControllerService"

            if is_aws_service:
                aws_services.append(ControllerServiceInfo(
                    id=service["id"],
                    name=service_name,
                    type=service_type,
                    state=service_info.get("state", "UNKNOWN"),
                    is_kafka_service=True
                ))

        return aws_services

    def configure_aws_services(self, pg_id: str, additional_params: Optional[Dict[str, str]],
                               tenant_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Configure AWS controller services in a process group."""
        self.logger.info(f"Configuring AWS controller services for process group: {pg_id}")

        aws_services = self.get_aws_services(pg_id)
        aws_services_configured = []

        for service_info in aws_services:
            self.logger.info(f"Found AWS service: {service_info.name}")

            # Disable service if enabled to modify properties
            if service_info.state == "ENABLED":
                disable_success = self.disable_controller_service(
                    service_info.id, service_info.name
                )
                if not disable_success:
                    self.logger.error(f"Failed to disable {service_info.name} - skipping configuration")
                    continue
            elif service_info.state == "ENABLING":
                self.logger.warning(f"Service {service_info.name} is ENABLING - forcing disable")
                disable_success = self.disable_controller_service(
                    service_info.id, service_info.name, max_wait_seconds=15
                )
                if not disable_success:
                    continue

            essential_config = {
                "Access Key": additional_params.get("access_key_id"),
                "Secret Key": additional_params.get("secret_access_key"),
            }

            try:
                # Configure the service
                configured = self._configure_service_properties(
                    service_info.id, service_info.name, essential_config
                )

                # Always enable AWS services after configuration
                enabled = self.enable_controller_service(service_info.id, service_info.name)

                aws_services_configured.append({
                    "name": service_info.name,
                    "type": service_info.type,
                    "id": service_info.id,
                    "configured": configured,
                    "enabled": enabled,
                    "final_state": "ENABLED" if enabled else "CONFIGURATION_FAILED",
                    "original_state": service_info.state
                })

            except Exception as e:
                self.logger.warning(f"Error configuring {service_info.name}: {e}")

        if aws_services_configured:
            self.logger.info(f"Successfully configured {len(aws_services_configured)} AWS controller services")
        else:
            self.logger.info("No AWS controller services needed configuration")

        return aws_services_configured

# Global controller service manager instance
controller_manager = ControllerServiceManager()
