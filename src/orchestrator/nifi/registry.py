"""NiFi Registry operations and flow management."""

from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from .client import nifi_client
from ..exceptions import NiFiRegistryError
from ..logging import LoggerMixin


@dataclass
class FlowVersionInfo:
    """Information about a flow version in NiFi Registry."""
    registry_id: str
    bucket_id: str
    flow_id: str
    flow_name: str
    version: str


class RegistryManager(LoggerMixin):
    """Manager for NiFi Registry operations."""
    
    def get_registries(self) -> List[Dict[str, Any]]:
        """Get all NiFi registry clients."""
        data = nifi_client.get_json("flow/registries")
        return data.get("registries", [])
    
    def get_buckets_from_registry(self, registry_id: str) -> List[Dict[str, Any]]:
        """Get all buckets from a specific registry."""
        self.logger.info(f"Getting buckets from registry {registry_id}")
        
        data = nifi_client.get_json(f"flow/registries/{registry_id}/buckets")
        return data.get("buckets", [])
    
    def get_flows_from_bucket(self, registry_id: str, bucket_id: str) -> List[Dict[str, Any]]:
        """Get all flows from a specific bucket in a registry."""
        self.logger.info(f"Getting flows from bucket {bucket_id} in registry {registry_id}")
        
        data = nifi_client.get_json(f"flow/registries/{registry_id}/buckets/{bucket_id}/flows")
        flows = data.get("versionedFlows", [])
        
        self.logger.info(f"Found {len(flows)} flows in bucket {bucket_id}")
        return flows
    
    def get_flow_versions(self, registry_id: str, bucket_id: str, flow_id: str) -> List[Dict[str, Any]]:
        """Get all versions of a specific flow."""
        data = nifi_client.get_json(f"flow/registries/{registry_id}/buckets/{bucket_id}/flows/{flow_id}/versions")
        return data.get("versionedFlowSnapshotMetadataSet", [])
    
    def find_flow_by_name(self, flow_name: str) -> FlowVersionInfo:
        """
        Find registry, bucket, and flow information for a given flow name.
        
        Args:
            flow_name: Name of the flow to find
            
        Returns:
            FlowVersionInfo with location and version details
            
        Raises:
            NiFiRegistryError: If flow is not found
        """
        self.logger.info(f"Searching for flow: {flow_name}")
        
        registries = self.get_registries()
        all_flows_found = []
        
        for registry in registries:
            registry_id = registry["id"]
            registry_name = registry["component"]["name"]
            
            try:
                self.logger.debug(f"Searching registry: {registry_name} ({registry_id})")
                
                buckets = self.get_buckets_from_registry(registry_id)
                self.logger.debug(f"Found {len(buckets)} buckets in registry {registry_name}")
                
                for bucket in buckets:
                    bucket_id = bucket["id"]
                    bucket_name = bucket["bucket"]["name"]
                    
                    flows = self.get_flows_from_bucket(registry_id, bucket_id)
                    
                    for flow in flows:
                        versioned_flow = flow["versionedFlow"]
                        flow_found = versioned_flow["flowName"]
                        all_flows_found.append(flow_found)
                        
                        self.logger.debug(f"Found flow: {flow_found}")
                        
                        if versioned_flow["flowName"] == flow_name:
                            # Get latest version
                            versions = self.get_flow_versions(
                                registry_id, 
                                versioned_flow["bucketId"], 
                                versioned_flow["flowId"]
                            )
                            
                            if not versions:
                                raise NiFiRegistryError(f"No versions found for flow {flow_name}")
                            
                            latest_version = max(
                                versions, 
                                key=lambda v: int(v["versionedFlowSnapshotMetadata"]["version"])
                            )
                            
                            self.logger.info(f"Found matching flow: {flow_name}")
                            
                            return FlowVersionInfo(
                                registry_id=registry_id,
                                bucket_id=versioned_flow["bucketId"],
                                flow_id=versioned_flow["flowId"],
                                flow_name=versioned_flow["flowName"],
                                version=latest_version["versionedFlowSnapshotMetadata"]["version"]
                            )
                            
            except Exception as e:
                self.logger.warning(f"Failed to search registry {registry_id}: {e}")
        
        error_msg = f"Flow '{flow_name}' not found. Available flows: {all_flows_found}"
        self.logger.error(error_msg)
        raise NiFiRegistryError(error_msg)
    
    def list_all_flows(self) -> Dict[str, List[str]]:
        """
        List all available flows from all registries.
        
        Returns:
            Dict mapping registry names to lists of flow names
        """
        registries = self.get_registries()
        all_flows = {}
        
        for registry in registries:
            registry_name = registry["component"]["name"]
            registry_id = registry["id"]
            registry_flows = []
            
            try:
                buckets = self.get_buckets_from_registry(registry_id)
                
                for bucket in buckets:
                    bucket_id = bucket["id"]
                    flows = self.get_flows_from_bucket(registry_id, bucket_id)
                    
                    for flow in flows:
                        versioned_flow = flow["versionedFlow"]
                        registry_flows.append(versioned_flow["flowName"])
                        
            except Exception as e:
                self.logger.warning(f"Failed to list flows for registry {registry_name}: {e}")
            
            all_flows[registry_name] = registry_flows
        
        return all_flows


# Global registry manager instance
registry_manager = RegistryManager()
