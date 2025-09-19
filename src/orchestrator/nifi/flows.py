"""NiFi process group and flow deployment operations."""
import json
from typing import Dict, Any, Optional, List, Union, Sequence
from dataclasses import dataclass
from .client import nifi_client
# from .params import parameter_manager
from .registry import registry_manager, FlowVersionInfo
from ..exceptions import NiFiAPIError, IdempotencyConflict
from ..utils.enums import FlowStatus
from ..utils.idempotency import get_or_create
from ..logging import LoggerMixin
from ..utils.integration_category import INTEGRATION_NIFI_FLOW_NAMES
from ..utils.redis_client import RedisClient
from ..utils.redis_keys import tenant_hash_key, tenant_categories_key
from ..utils.utils import _json_or_none


@dataclass
class ProcessGroupInfo:
    """Information about a process group."""
    id: str
    name: str
    parent_id: str
    running_count: int
    stopped_count: int

    @property
    def total_count(self) -> int:
        """Total component count."""
        return self.running_count + self.stopped_count

    @property
    def has_content(self) -> bool:
        """Whether the process group has any content."""
        return self.total_count > 0


class FlowManager(LoggerMixin):
    """Manager for NiFi flow and process group operations."""

    def get_root_pg_id(self) -> str:
        """Get the root process group ID."""
        return "root"

    def get_process_group(self, pg_id: str) -> Optional[Dict[str, Any]]:
        """Get process group by ID."""
        try:
            return nifi_client.get_json(f"process-groups/{pg_id}")
        except NiFiAPIError as e:
            if e.status_code == 404:
                return None
            raise

    def get_process_group_info(self, pg_id: str) -> ProcessGroupInfo:
        """Get process group information."""
        pg_data = self.get_process_group(pg_id)
        if not pg_data:
            raise NiFiAPIError(f"Process group {pg_id} not found")

        component = pg_data.get("component", {})
        return ProcessGroupInfo(
            id=pg_data["id"],
            name=component.get("name", ""),
            parent_id=component.get("parentGroupId", ""),
            running_count=pg_data.get("runningCount", 0),
            stopped_count=pg_data.get("stoppedCount", 0)
        )

    def list_child_process_groups(self, parent_id: str = "root") -> List[Dict[str, Any]]:
        """List child process groups of a parent."""
        data = nifi_client.get_json(f"flow/process-groups/{parent_id}/?uiOnly=true")
        self.logger.info(f"List of the child process groups for parent '{parent_id}' has this data inside it:'{data}'")
        return data.get("processGroupFlow", {}).get("flow", {}).get("processGroups", [])

    def find_process_group_by_name(self, name: str, parent_id: str = "root") -> Optional[str]:
        """Find process group ID by name within a parent."""
        children = self.list_child_process_groups(parent_id)

        for pg in children:
            if pg["component"]["name"] == name:
                return pg["id"]

        # Search recursively in child process groups
        for pg in children:
            try:
                found_id = self.find_process_group_by_name(name, pg["id"])
                if found_id:
                    return found_id
            except Exception:
                continue  # Skip if you cannot access child
        return None

    def find_process_group_by_name_stop_integration(self, tenant_id: str, integration_name: str, tenant_pc_id: str) -> \
            List[str]:
        """Find process group ID by name within a stop integration."""
        doc = RedisClient.get_instance().hget(key=tenant_hash_key(), hash=tenant_id)
        if doc is None:
            tenant_pg_id = self.get_tenant_pg_id(tenant_id, tenant_pc_id)
        else:
            doc = json.loads(doc)
            if (doc.get("tenant_pg_id", None) is None) and (doc.get("pc_id", None) is None):
                tenant_pg_id = self.get_tenant_pg_id(tenant_id, tenant_pc_id)
            else:
                tenant_pg_id = doc.get("tenant_pg_id", None)

        nifi_flow = INTEGRATION_NIFI_FLOW_NAMES.get(integration_name)
        if nifi_flow is None:
            raise NiFiAPIError(f"Process group not found inside {tenant_id}")

        if not isinstance(nifi_flow, List):
            nifi_flow = [nifi_flow]
        self.logger.info(f"We have multiple process groups for '{integration_name}' integration with'")

        cached_categories = RedisClient.get_instance().get(key=tenant_categories_key(tenant_id))
        if cached_categories is not None:
            cached_categories = json.loads(cached_categories)
        else:
            cached_categories = {}

        docs = []
        for flow in nifi_flow:

            category_pg_id = cached_categories.get(flow.get("category"))

            if category_pg_id is None:
                category_pg_id = self.get_category_in_pg_id(tenant_pg_id=tenant_pg_id,
                                                            category_name=flow.get("category"),
                                                            tenant_id=tenant_id)

            result = self.get_integration_in_pg_id(tenant_id=tenant_id, category_name=flow.get("category"),
                                                   category_in_pg_id=category_pg_id,
                                                   integration_name=flow.get("name"),
                                                   tenant_pg_id=tenant_pg_id, nifi_flow=flow,
                                                   tenant_pc_id=tenant_pc_id)
            docs.append(result)
        return docs

    def create_process_group(self, parent_id: str, name: str, position_x: float = 100.0,
                             position_y: float = 100.0, comments: str = "") -> Dict[str, Any]:
        """Create a new process group."""
        create_body = {
            "revision": {"version": 0},
            "disconnectedNodeAcknowledged": False,
            "component": {
                "name": name,
                "position": {"x": position_x, "y": position_y},
                "comments": comments
            }
        }

        self.logger.info(f"Creating process group '{name}' in parent {parent_id}")
        return nifi_client.post_json(f"process-groups/{parent_id}/process-groups", create_body)

    def ensure_process_group(self, parent_id: str, name: str, position_x: float = 100.0,
                             position_y: float = 100.0, comments: str = "") -> str:
        """Ensure process group exists, create if missing (idempotent)."""

        def fetch_existing():
            return self.find_process_group_by_name(name, parent_id)

        def create_new():
            pg = self.create_process_group(parent_id, name, position_x, position_y, comments)
            return pg["id"]

        return get_or_create(fetch_existing, create_new, f"process group '{name}'")

    def import_flow_from_registry(self, parent_pg_id: str, flow_info: FlowVersionInfo,
                                  version: Optional[str] = None, tenant_id: Optional[str] = None,
                                  position_x: float = 100.0, position_y: float = 100.0) -> Dict[str, Any]:
        """Import flow from NiFi Registry using version control information."""

        use_version = version or flow_info.version

        import_body = {
            "revision": {"version": 0},
            "disconnectedNodeAcknowledged": False,
            "component": {
                "position": {"x": position_x, "y": position_y},
                "versionControlInformation": {
                    "registryId": flow_info.registry_id,
                    "bucketId": flow_info.bucket_id,
                    "flowId": flow_info.flow_id,
                    "version": str(use_version)
                }
            }
        }

        # Use KEEP_EXISTING strategy to preserve custom parameter contexts
        url = f"process-groups/{parent_pg_id}/process-groups?parameterContextHandlingStrategy=KEEP_EXISTING"

        self.logger.info(f"Importing flow '{flow_info.flow_name}' version {use_version}")
        self.logger.info(f"Registry: {flow_info.registry_id}, Bucket: {flow_info.bucket_id}, Flow: {flow_info.flow_id}")

        pg_response = nifi_client.post_json(url, import_body)

        # Rename the process group to include tenant ID if provided
        if tenant_id:
            pg_name = f"JumpCloud Pipeline - {tenant_id}"
            self.logger.info(f"Renaming process group to: {pg_name}")

            updated_pg = self.update_process_group(pg_response["id"], name=pg_name)
            if updated_pg:
                pg_response = updated_pg

        self.logger.info(f"Successfully imported flow with process group ID: {pg_response['id']}")
        return pg_response

    def smart_import_flow_from_registry(self, parent_pg_id: str, flow_info: FlowVersionInfo,
                                        integration_name: str, version: Optional[str] = None,
                                        tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Smart flow import with duplicate prevention and position management.
        Checks for existing flows before importing and manages positions automatically.
        """
        use_version = version or flow_info.version

        # Step 1: Check if integration already exists with same name
        existing_pg_id = self.find_process_group_by_name(integration_name, parent_pg_id)
        if existing_pg_id:
            self.logger.info(f"Found existing integration '{integration_name}' in process group {parent_pg_id}")
            existing_pg = self.get_process_group(existing_pg_id)
            if existing_pg:
                return existing_pg

        # Step 2: Check for flows with same registry/bucket/flow ID (version control duplicates)
        existing_flows = self._find_flows_with_same_version_control(parent_pg_id, flow_info)
        if existing_flows:
            self.logger.warning(f"Found {len(existing_flows)} existing flows with same version control info")
            for existing_flow in existing_flows:
                self.logger.warning(f"  - Existing: {existing_flow.get('component', {}).get('name', 'Unknown')}")

            # Return the first matching flow instead of creating duplicate
            return existing_flows[0]

        # Step 3: Calculate smart position for integration type
        smart_position = self.get_smart_position_for_type(parent_pg_id, "integration", integration_name)

        # Step 4: Import the flow with smart position
        self.logger.info(
            f"Importing flow '{flow_info.flow_name}' at position ({smart_position['x']}, {smart_position['y']})")
        pg_response = self.import_flow_from_registry(
            parent_pg_id=parent_pg_id,
            flow_info=flow_info,
            version=use_version,
            tenant_id=tenant_id,
            position_x=smart_position['x'],
            position_y=smart_position['y']
        )

        # Step 5: Rename to integration name immediately to prevent future duplicates
        if integration_name and integration_name != pg_response.get("component", {}).get("name"):
            self.logger.info(f"Renaming imported flow to: {integration_name}")
            updated_pg = self.update_process_group(pg_response["id"], name=integration_name)
            if updated_pg:
                pg_response = updated_pg

        return pg_response

    def _find_flows_with_same_version_control(self, parent_pg_id: str, flow_info: FlowVersionInfo) -> List[
        Dict[str, Any]]:
        """Find flows with same registry/bucket/flow ID in the parent process group."""
        try:
            children_data = nifi_client.get_json(f"process-groups/{parent_pg_id}/process-groups")
            process_groups = children_data.get("processGroups", [])

            matching_flows = []
            for pg in process_groups:
                component = pg.get("component", {})
                version_control = component.get("versionControlInformation")

                if version_control:
                    if (version_control.get("registryId") == flow_info.registry_id and
                            version_control.get("bucketId") == flow_info.bucket_id and
                            version_control.get("flowId") == flow_info.flow_id):
                        matching_flows.append(pg)

            return matching_flows

        except Exception as e:
            self.logger.warning(f"Failed to check for existing flows: {e}")
            return []

    def _calculate_smart_position(self, parent_pg_id: str) -> Dict[str, float]:
        """Calculate smart position for new process group to avoid overlaps."""
        try:
            children_data = nifi_client.get_json(f"process-groups/{parent_pg_id}/process-groups")
            process_groups = children_data.get("processGroups", [])

            if not process_groups:
                # First process group in this parent
                return {"x": 100.0, "y": 100.0}

            # Find the rightmost position
            max_x = 100.0
            for pg in process_groups:
                component = pg.get("component", {})
                position = component.get("position", {})
                x = position.get("x", 100.0)
                if x > max_x:
                    max_x = x

            # Place new process group 400 pixels to the right
            return {"x": max_x + 400.0, "y": 100.0}

        except Exception as e:
            self.logger.warning(f"Failed to calculate smart position: {e}")
            # Fallback to random position
            import random
            return {"x": random.randint(100, 800), "y": random.randint(100, 400)}

    def get_smart_position_for_type(self, parent_pg_id: str, pg_type: str, pg_name: str) -> Dict[str, float]:
        """
        Get smart position based on process group type to ensure organized layout.
        
        Layout zones:
        - Tenant PGs (root): Row 1 (y=100), spaced 500px apart
        - Category PGs (tenant): Row 2 (y=300), spaced 400px apart  
        - Integration PGs (category): Row 3 (y=100), spaced 350px apart
        """
        try:
            children_data = nifi_client.get_json(f"process-groups/{parent_pg_id}/process-groups")
            process_groups = children_data.get("processGroups", [])

            # Define layout zones based on type
            if pg_type == "tenant":
                base_y = 100.0
                spacing = 500.0
                start_x = 200.0
            elif pg_type == "category":
                base_y = 300.0
                spacing = 400.0
                start_x = 100.0
            elif pg_type == "integration":
                base_y = 100.0
                spacing = 350.0
                start_x = 100.0
            else:
                # Default for unknown types
                base_y = 200.0
                spacing = 300.0
                start_x = 150.0

            if not process_groups:
                return {"x": start_x, "y": base_y}

            # Find existing positions in this zone
            occupied_x_positions = []
            for pg in process_groups:
                component = pg.get("component", {})
                position = component.get("position", {})
                y = position.get("y", 0)

                # Check if in the same horizontal zone (within 50px)
                if abs(y - base_y) < 50:
                    occupied_x_positions.append(position.get("x", start_x))

            if not occupied_x_positions:
                return {"x": start_x, "y": base_y}

            # Find the next available position
            occupied_x_positions.sort()
            next_x = max(occupied_x_positions) + spacing

            self.logger.debug(f"Smart position for {pg_type} '{pg_name}': ({next_x}, {base_y})")
            return {"x": next_x, "y": base_y}

        except Exception as e:
            self.logger.warning(f"Failed to calculate smart position for {pg_type}: {e}")
            return {"x": 200.0, "y": 100.0}

    def update_process_group(self, pg_id: str, name: Optional[str] = None,
                             comments: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Update process group properties."""
        try:
            current_pg = self.get_process_group(pg_id)
            if not current_pg:
                return None

            component = current_pg["component"].copy()

            if name is not None:
                component["name"] = name
            if comments is not None:
                component["comments"] = comments

            update_body = {
                "revision": current_pg["revision"],
                "component": component
            }

            return nifi_client.put_json(f"process-groups/{pg_id}", update_body)

        except Exception as e:
            self.logger.warning(f"Failed to update process group {pg_id}: {e}")
            return None

    def start_process_group(self, pg_id: str) -> Dict[str, Any]:
        """Start a process group."""
        self.logger.info(f"Starting process group {pg_id}")

        start_body = {
            "id": pg_id,
            "state": "RUNNING",
            "disconnectedNodeAcknowledged": False
        }

        return nifi_client.put_json(f"flow/process-groups/{pg_id}", start_body)

    def stop_process_group(self, pg_id: str) -> Dict[str, Any]:
        """Stop a process group."""
        self.logger.info(f"Stopping process group {pg_id}")

        stop_body = {
            "id": pg_id,
            "state": "STOPPED",
            "disconnectedNodeAcknowledged": False
        }

        return nifi_client.put_json(f"flow/process-groups/{pg_id}", stop_body)

    def deploy_versioned_flow(self, parent_pg_id: str, flow_name: str,
                              tenant_id: str, version: str = "latest",
                              position_x: float = 100.0, position_y: float = 100.0) -> Dict[str, Any]:
        """
        Deploy a versioned flow from registry.
        
        This is a high-level method that:
        1. Finds the flow in the registry
        2. Imports it to the specified parent process group
        3. Returns the created process group information
        """
        # Find the flow in registry
        flow_info = registry_manager.find_flow_by_name(flow_name)

        # Use specific version if provided, otherwise use latest
        if version != "latest":
            use_version = version
        else:
            use_version = flow_info.version

        # Import the flow
        return self.import_flow_from_registry(
            parent_pg_id=parent_pg_id,
            flow_info=flow_info,
            version=use_version,
            tenant_id=tenant_id,
            position_x=position_x,
            position_y=position_y
        )

    def check_existing_tenant_deployment(self, tenant_id: str) -> Dict[str, Any]:
        """Check if tenant already has existing deployment."""
        pg_name = f"JumpCloud Pipeline - {tenant_id}"

        existing_pgs = []

        try:
            children = self.list_child_process_groups("root")
            existing_pgs = [x for x in children if x["component"]["name"] == pg_name]
        except Exception as e:
            self.logger.warning(f"Could not check process groups: {e}")

        return {
            "process_groups": existing_pgs,
            "has_existing": len(existing_pgs) > 0
        }

    def rename_process_group(self, pg_id: str, new_name: str) -> Dict[str, Any]:
        """Rename a process group."""
        try:
            # Get current process group state
            pg = self.get_process_group(pg_id)
            if not pg:
                raise NiFiAPIError(f"Process group {pg_id} not found")

            # Update the name
            component = pg["component"].copy()
            component["name"] = new_name

            update_body = {
                "revision": pg["revision"],
                "component": component
            }

            self.logger.info(f"Renaming process group {pg_id} to: {new_name}")
            result = nifi_client.put_json(f"process-groups/{pg_id}", update_body)

            self.logger.info(f"✅ Successfully renamed process group to: {new_name}")
            return result

        except Exception as e:
            self.logger.error(f"Failed to rename process group {pg_id}: {e}")
            raise

    def get_tenant_pg_id(self, tenant_id: str, tenant_pc_id) -> str:
        tenant_pg_id = flow_manager.find_process_group_by_name(tenant_id, "root")
        if not tenant_pg_id:
            raise NiFiAPIError(f"Process group {tenant_id} not found")
        RedisClient.get_instance().hset(
            key=tenant_hash_key(),
            hash=tenant_id,
            mapping={"tenant_pg_id": tenant_pg_id, "pc_id": tenant_pc_id}
        )

        return tenant_pg_id

    def get_category_in_pg_id(self, tenant_pg_id: str, tenant_id: str, category_name: str,
                              category_cache: Optional[Dict[str, str]] = None) -> str:

        category_in_pg_id = flow_manager.find_process_group_by_name(category_name, tenant_pg_id)

        if not category_in_pg_id: raise NiFiAPIError(f"Process group {category_name} not found inside {tenant_id}")

        # merge with cache if provided
        payload = {**(category_cache or {}), category_name: category_in_pg_id}

        RedisClient.get_instance().set(key=tenant_categories_key(tenant_id), value=json.dumps(payload))

        return category_in_pg_id

    def get_integration_in_pg_id(self, tenant_id: str, category_name: str, category_in_pg_id: str,
                                 integration_name: str, tenant_pg_id, nifi_flow: Dict[str, str],
                                 tenant_pc_id: str) -> Dict[str, str]:

        flow_name = nifi_flow.get("name", None)
        flow_name_pg_id = flow_manager.find_process_group_by_name(flow_name, category_in_pg_id)
        if not flow_name_pg_id:
            raise NiFiAPIError(f"Process group {category_name} not found inside {tenant_id}")

        result = {
            "tenant_pg_id": tenant_pg_id,
            "category_pg_id": category_in_pg_id,
            "integration_pg_id": flow_name_pg_id,
            "pc_id": tenant_pc_id,
            "tenant_id": tenant_id,
            "integration_name": integration_name,
            "category": category_name,
            "status": FlowStatus.STARTED,
        }

        return result


# Global flow manager instance
flow_manager = FlowManager()
