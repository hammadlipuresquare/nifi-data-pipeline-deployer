"""NiFi parameter context management."""

from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from .client import nifi_client
from .flows import flow_manager
from ..exceptions import NiFiAPIError
from ..utils.idempotency import get_or_create
from ..logging import LoggerMixin


@dataclass
class ParameterContextInfo:
    """Information about a parameter context."""
    id: str
    name: str
    description: str
    parameter_count: int


class ParameterManager(LoggerMixin):
    """Manager for NiFi parameter context operations."""

    def get_parameter_context(self, pc_id: str) -> Optional[Dict[str, Any]]:
        """Get parameter context by ID."""
        try:
            return nifi_client.get_json(f"parameter-contexts/{pc_id}")
        except NiFiAPIError as e:
            if e.status_code == 404:
                return None
            raise

    def list_parameter_contexts(self) -> List[Dict[str, Any]]:
        """List all parameter contexts."""
        # Try different API endpoints for different NiFi versions
        endpoints = [
            "flow/parameter-contexts"
        ]

        for endpoint in endpoints:
            try:
                self.logger.debug(f"Trying parameter contexts endpoint: {endpoint}")
                data = nifi_client.get_json(endpoint)

                # Handle different response structures
                if "parameterContexts" in data:
                    self.logger.info(f"Successfully retrieved parameter contexts using endpoint: {endpoint}")
                    return data.get("parameterContexts", [])
                elif isinstance(data, list):
                    self.logger.info(f"Successfully retrieved parameter contexts using endpoint: {endpoint}")
                    return data
                else:
                    self.logger.warning(
                        f"Unexpected response structure from {endpoint}: {list(data.keys()) if isinstance(data, dict) else type(data)}")

            except NiFiAPIError as e:
                if e.status_code == 405:
                    self.logger.debug(f"Endpoint {endpoint} not supported (405), trying next...")
                    continue
                else:
                    self.logger.warning(f"Error with endpoint {endpoint}: {e}")
                    raise

        # If all endpoints fail with 405
        self.logger.warning("Parameter contexts endpoints not supported in this NiFi version")
        return []

    def find_parameter_context_by_name(self, name: str) -> Optional[str]:
        """Find parameter context ID by name."""
        try:
            contexts = self.list_parameter_contexts()
            for pc in contexts:
                if pc["component"]["name"] == name:
                    return pc["id"]
            return None
        except Exception as e:
            self.logger.warning(f"Could not search parameter contexts: {e}")
            return None

    def create_parameter_context(self, name: str, description: str = "",
                                 parameters: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        """Create a new parameter context."""
        create_body = {
            "revision": {"version": 0},
            "component": {
                "name": name,
                "description": description,
                "parameters": parameters or []
            }
        }

        self.logger.info(f"Creating parameter context: {name}")
        return nifi_client.post_json("parameter-contexts", create_body)

    def create_tenant_parameter_context(self, tenant_id: str, api_key: str,
                                        additional_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create or find existing parameter context for a tenant."""
        pc_name = f"JumpCloud-{tenant_id}-Context"

        def fetch_existing():
            return self.find_parameter_context_by_name(pc_name)

        def create_new():
            # Prepare parameters
            parameters = [
                {
                    "parameter": {
                        "name": "TENANT_ID",
                        "description": f"Tenant identifier for {tenant_id}",
                        "sensitive": False,
                        "value": tenant_id
                    }
                }
            ]

            # Add additional parameters if provided
            if additional_params:
                for key, value in additional_params.items():
                    parameters.append({
                        "parameter": {
                            "name": key,
                            "description": f"Additional parameter for {tenant_id}",
                            "sensitive": "password" in key.lower() or "secret" in key.lower() or "key" in key.lower(),
                            "value": str(value)
                        }
                    })

            try:
                pc = self.create_parameter_context(
                    name=pc_name,
                    description=f"JumpCloud parameter context for tenant: {tenant_id}",
                    parameters=parameters
                )
                self.logger.info(f"Created parameter context: {pc_name}")
                return pc["id"]

            except Exception as e:
                self.logger.error(f"Failed to create parameter context {pc_name}: {e}")

                # Check if this is a 405 (not supported) error
                if hasattr(e, 'status_code') and e.status_code == 405:
                    self.logger.info(f"Parameter contexts not supported in this NiFi version (405)")
                    return "unsupported-parameter-context"
                elif "405" in str(e) or "Method Not Allowed" in str(e):
                    self.logger.info(f"Parameter contexts not supported in this NiFi version (405 in error message)")
                    return "unsupported-parameter-context"

                # Check if this is a 409 (already exists) error  
                elif hasattr(e, 'status_code') and e.status_code == 409:
                    self.logger.warning(f"Parameter context {pc_name} already exists (409 conflict)")
                    self.logger.info(f"🔍 Attempting to find existing parameter context ID...")

                    # Try to find the existing parameter context
                    existing_pc_id = self.find_parameter_context_by_name(pc_name)
                    if existing_pc_id:
                        self.logger.info(f"✅ Found existing parameter context: {pc_name} -> {existing_pc_id}")
                        return existing_pc_id
                    else:
                        self.logger.warning(
                            f"⚠️  Could not find existing parameter context {pc_name} despite 409 conflict")
                        return "exists-but-unlookupable-pc-id"

                elif "409" in str(e) or "already exists" in str(e).lower():
                    self.logger.warning(f"Parameter context {pc_name} already exists (409 conflict)")
                    self.logger.info(f"🔍 Attempting to find existing parameter context ID...")

                    # Try to find the existing parameter context
                    existing_pc_id = self.find_parameter_context_by_name(pc_name)
                    if existing_pc_id:
                        self.logger.info(f"✅ Found existing parameter context: {pc_name} -> {existing_pc_id}")
                        return existing_pc_id
                    else:
                        self.logger.warning(
                            f"⚠️  Could not find existing parameter context {pc_name} despite 409 conflict")
                        return "exists-but-unlookupable-pc-id"

                # Return a fallback ID that indicates creation failed for other reasons
                return "creation-failed-pc-id"

        pc_id = get_or_create(fetch_existing, create_new, f"parameter context '{pc_name}'")

        # If we got the fallback ID, return minimal context info
        if pc_id == "creation-failed-pc-id":
            return {
                "id": pc_id,
                "component": {"name": pc_name + " (Creation Failed)"},
                "note": "Parameter context creation failed"
            }

        # Return the actual parameter context
        pc = self.get_parameter_context(pc_id)
        return pc or {
            "id": pc_id,
            "component": {"name": pc_name},
            "note": "Found existing parameter context"
        }

    def ensure_smart_parameter_context(self, tenant_id: str, integration_type: str,
                                       integration_params: Dict[str, Any],
                                       parameter_specs: Dict[str, Dict[str, Any]]) -> str:
        """
        Create or update smart parameter context for tenant with integration-specific parameters.
        
        Args:
            tenant_id: Tenant identifier
            integration_type: Type of integration (jumpcloud, aws-security, etc.)
            integration_params: Parameter values for this integration (api_key, etc.)
            parameter_specs: Parameter specifications from INTEGRATION_PARAMETERS
            
        Returns:
            Parameter context ID
        """
        # Use new naming convention: {tenant_id}-context
        pc_name = f"{tenant_id}-context"

        def fetch_existing():
            return self.find_parameter_context_by_name(pc_name)

        def create_new():
            # Build parameters based on integration requirements
            parameters = []

            self.logger.info("==============================================================")
            self.logger.info(f"Creating new parameter context {pc_name}")
            self.logger.info(f"Integration parameters: {integration_params}")
            self.logger.info("==============================================================")

            for param_name, param_config in parameter_specs.items():
                # Get the value from integration_params based on source mapping
                source_key = param_config["source"]
                if source_key == "tenant_id":
                    value = tenant_id
                elif source_key in integration_params:
                    value = integration_params[source_key]
                else:
                    self.logger.warning(f"Missing parameter value for {param_name} (source: {source_key})")
                    value = f"<missing-{source_key}>"

                parameters.append({
                    "parameter": {
                        "name": param_name,
                        "description": f"{param_config['description']} (for {integration_type})",
                        "sensitive": param_config["sensitive"],
                        "value": str(value)
                    }
                })

            try:
                pc = self.create_parameter_context(
                    name=pc_name,
                    description=f"Smart parameter context for tenant: {tenant_id}",
                    parameters=parameters
                )
                self.logger.info(f"Created smart parameter context: {pc_name} for {integration_type}")
                return pc["id"]

            except Exception as e:
                self.logger.error(f"Failed to create smart parameter context {pc_name}: {e}")

                # Check if this is a 405 (not supported) error
                if hasattr(e, 'status_code') and e.status_code == 405:
                    self.logger.info(f"Parameter contexts not supported in this NiFi version (405)")
                    return "unsupported-parameter-context"
                elif "405" in str(e) or "Method Not Allowed" in str(e):
                    self.logger.info(f"Parameter contexts not supported in this NiFi version (405 in error message)")
                    return "unsupported-parameter-context"

                # Check if this is a 409 (already exists) error  
                elif hasattr(e, 'status_code') and e.status_code == 409:
                    self.logger.warning(f"Parameter context {pc_name} already exists (409 conflict)")
                    self.logger.info(f"🔍 Attempting to find existing parameter context ID...")

                    # Try to find the existing parameter context
                    existing_pc_id = self.find_parameter_context_by_name(pc_name)
                    if existing_pc_id:
                        self.logger.info(f"✅ Found existing parameter context: {pc_name} -> {existing_pc_id}")
                        return existing_pc_id
                    else:
                        self.logger.warning(
                            f"⚠️  Could not find existing parameter context {pc_name} despite 409 conflict")
                        return "exists-but-unlookupable-pc-id"

                elif "409" in str(e) or "already exists" in str(e).lower():
                    self.logger.warning(f"Parameter context {pc_name} already exists (409 conflict)")
                    self.logger.info(f"🔍 Attempting to find existing parameter context ID...")

                    # Try to find the existing parameter context
                    existing_pc_id = self.find_parameter_context_by_name(pc_name)
                    if existing_pc_id:
                        self.logger.info(f"✅ Found existing parameter context: {pc_name} -> {existing_pc_id}")
                        return existing_pc_id
                    else:
                        self.logger.warning(
                            f"⚠️  Could not find existing parameter context {pc_name} despite 409 conflict")
                        return "exists-but-unlookupable-pc-id"

                return "creation-failed-pc-id"

        # Check if parameter context already exists
        existing_pc_id = fetch_existing()

        if existing_pc_id:
            # Add new parameters to existing context if needed
            self.logger.info(f"Found existing parameter context: {pc_name}, adding {integration_type} parameters")
            self._add_parameters_to_context(existing_pc_id, parameter_specs, integration_type, integration_params,
                                            tenant_id)
            return existing_pc_id
        else:
            # Create new parameter context
            return create_new()

    def _add_parameters_to_context(self, pc_id: str, parameter_specs: Dict[str, Dict[str, Any]],
                                   integration_type: str, integration_params: Dict[str, Any],
                                   tenant_id: str) -> None:
        """Add new parameters to existing parameter context."""
        try:
            # Get current parameter context
            pc = self.get_parameter_context(pc_id)
            if not pc:
                self.logger.warning(f"Could not retrieve parameter context {pc_id}")
                return

            # Get existing parameters
            existing_params = {p["parameter"]["name"] for p in pc.get("component", {}).get("parameters", [])}

            # Prepare new parameters that don't exist yet
            new_parameters = []
            for param_name, param_config in parameter_specs.items():
                if param_name not in existing_params:
                    # Get the value from integration_params based on source mapping
                    source_key = param_config["source"]

                    self.logger.info(f"Integration Params Info: {integration_params}")

                    if source_key == "tenant_id":
                        value = tenant_id

                    elif source_key in integration_params:
                        self.logger.info(f"Checking the source key {source_key}")
                        value = integration_params[source_key]
                    else:
                        self.logger.warning(f"Missing parameter value for {param_name} (source: {source_key})")
                        value = f"<missing-{source_key}>"

                    new_parameters.append({
                        "parameter": {
                            "name": param_name,
                            "description": f"{param_config['description']} (for {integration_type})",
                            "sensitive": param_config["sensitive"],
                            "value": str(value)
                        }
                    })
                else:
                    self.logger.info(f"Parameter {param_name} already exists in context")

            if new_parameters:
                update_body = {
                    "revision": pc.get("revision", {"version": 0}),
                    "component": {
                        "id": pc_id,
                        "name": pc["component"]["name"],
                        "description": pc["component"]["description"],
                        "parameters": new_parameters
                    }
                }

                nifi_client.post_json(f"parameter-contexts/{pc_id}/update-requests", update_body)
                self.logger.info(f"Added {len(new_parameters)} new parameters for {integration_type}")
            else:
                self.logger.info(f"No new parameters needed for {integration_type}")

        except Exception as e:
            self.logger.error(f"Failed to add parameters to context {pc_id}: {e}")

    def ensure_tenant_parameter_context(self, tenant_id: str, integration_type: str,
                                        additional_params: Optional[Dict[str, Any]] = None) -> str:
        """
        Main entry point for smart parameter context management.
        Creates or updates parameter context with integration-specific parameters.
        Handles unsupported NiFi versions gracefully.
        
        Args:
            tenant_id: Tenant identifier
            integration_type: Type of integration (jumpcloud, aws-security, etc.)
            api_key: API key for the integration
            additional_params: Additional parameters for the integration
            
        Returns:
            Parameter context ID (or fallback ID if not supported)
        """
        # Check if parameter contexts are supported
        try:
            # Quick test to see if endpoint exists
            self.list_parameter_contexts()
            parameter_contexts_supported = True
        except Exception as e:
            if "405" in str(e) or "Method Not Allowed" in str(e):
                parameter_contexts_supported = False
                self.logger.warning(f"Parameter contexts not supported in this NiFi version, using fallback")
            else:
                parameter_contexts_supported = True

        if not parameter_contexts_supported:
            # Return a fallback ID for unsupported versions
            fallback_id = "unsupported-parameter-context"
            self.logger.info(f"Using fallback parameter context ID: {fallback_id}")
            return fallback_id

        # Import here to avoid circular imports
        from ..integrations import INTEGRATION_PARAMETERS

        # Get parameter specifications for this integration type
        if integration_type not in INTEGRATION_PARAMETERS:
            self.logger.warning(f"No parameter specs found for integration: {integration_type}")
            # Fallback to basic parameters
            parameter_specs = {
                "TENANT_ID": {"description": "Tenant identifier", "sensitive": False, "source": "tenant_id"}
            }
        else:
            parameter_specs = INTEGRATION_PARAMETERS[integration_type]

        # Prepare integration parameters
        integration_params = {"tenant_id": tenant_id}

        # Add additional parameters if provided
        if additional_params: integration_params.update(additional_params)

        self.logger.info(f"Creating/updating smart parameter context for {tenant_id} - {integration_type}")
        self.logger.info(f"Required parameters: {list(parameter_specs.keys())}")

        try:
            return self.ensure_smart_parameter_context(
                tenant_id=tenant_id,
                integration_type=integration_type,
                integration_params=integration_params,
                parameter_specs=parameter_specs
            )
        except Exception as e:
            self.logger.error(f"Failed to create smart parameter context: {e}")
            # Return fallback ID on any error
            return "unsupported-parameter-context"

    def bind_parameter_context(self, pg_id: str, pc_id: str) -> Dict[str, Any]:
        """Bind parameter context to process group using proper NiFi API structure."""
        if pc_id in ["unsupported-pc-id", "creation-failed-pc-id", "unsupported-parameter-context",
                     "exists-but-unlookupable-pc-id"]:
            self.logger.info(
                f"Skipping parameter context binding - not supported in this NiFi version (pc_id: {pc_id})")
            return {"note": "Parameter context binding skipped - not supported in this NiFi version"}

        self.logger.info(f"Binding parameter context {pc_id} to process group {pg_id}")

        try:
            # Get current process group state
            pg = flow_manager.get_process_group(pg_id)
            if not pg:
                raise NiFiAPIError(f"Process group {pg_id} not found")

            # Extract current component data
            current_component = pg.get("component", {})
            current_revision = pg.get("revision", {})

            # Build the complete component update with all required fields
            component_update = {
                "id": pg_id,
                "name": current_component.get("name", ""),
                "executionEngine": current_component.get("executionEngine", "INHERITED"),
                "flowfileConcurrency": current_component.get("flowfileConcurrency", "UNBOUNDED"),
                "flowfileOutboundPolicy": current_component.get("flowfileOutboundPolicy", "STREAM_WHEN_AVAILABLE"),
                "defaultFlowFileExpiration": current_component.get("defaultFlowFileExpiration", "0 sec"),
                "defaultBackPressureObjectThreshold": current_component.get("defaultBackPressureObjectThreshold",
                                                                            10000),
                "defaultBackPressureDataSizeThreshold": current_component.get("defaultBackPressureDataSizeThreshold",
                                                                              "1 GB"),
                "logFileSuffix": current_component.get("logFileSuffix"),
                "parameterContext": {"id": pc_id},  # This is the key change
                "comments": current_component.get("comments", "")
            }

            # Build the complete update body matching NiFi API structure (compatible version)
            update_body = {
                "revision": {
                    "clientId": current_revision.get("clientId", "orchestrator-client"),
                    "version": current_revision.get("version", 0)
                },
                "disconnectedNodeAcknowledged": False,
                "component": component_update
            }

            self.logger.info(f"Sending parameter context binding request for PG {pg_id}")
            self.logger.debug(f"Update body: {update_body}")

            # Apply the update using PUT
            result = nifi_client.put_json(f"process-groups/{pg_id}", update_body)

            self.logger.info(f"Successfully bound parameter context {pc_id} to process group {pg_id}")
            return result

        except Exception as e:
            self.logger.warning(f"Failed to bind parameter context {pc_id} to process group {pg_id}: {e}")
            return {"note": "Parameter context binding failed", "error": str(e)}

    def get_parameter_context_info(self, pc_id: str) -> Optional[ParameterContextInfo]:
        """Get parameter context information."""
        pc = self.get_parameter_context(pc_id)
        if not pc:
            return None

        component = pc.get("component", {})
        parameters = component.get("parameters", [])

        return ParameterContextInfo(
            id=pc["id"],
            name=component.get("name", ""),
            description=component.get("description", ""),
            parameter_count=len(parameters)
        )

    def update_parameter_context(self, pc_id: str, parameters: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Update parameter context with new parameters."""
        pc = self.get_parameter_context(pc_id)
        if not pc:
            raise NiFiAPIError(f"Parameter context {pc_id} not found")

        # Update the parameters
        component = pc["component"].copy()
        component["parameters"] = parameters

        update_body = {
            "revision": pc["revision"],
            "component": component
        }

        self.logger.info(f"Updating parameter context {pc_id} with {len(parameters)} parameters")
        return nifi_client.put_json(f"parameter-contexts/{pc_id}", update_body)

    def recursively_assign_parameter_context(self, root_pg_id: str, pc_id: str,
                                             max_depth: int = 10, current_depth: int = 0,
                                             stop_components_if_needed: bool = False) -> Dict[str, Any]:
        """
        Recursively assign parameter context to all child process groups.
        
        This ensures that all descendants of a tenant process group inherit the same
        parameter context, providing consistent parameter access across the entire hierarchy.
        
        Args:
            root_pg_id: Root process group ID to start from
            pc_id: Parameter context ID to assign
            max_depth: Maximum recursion depth (safety limit)
            current_depth: Current recursion depth (internal use)
            stop_components_if_needed: If True, stop running components before assignment (use with caution)
            
        Returns:
            Dict with assignment results and statistics
        """
        if pc_id in ["unsupported-pc-id", "creation-failed-pc-id", "unsupported-parameter-context",
                     "exists-but-unlookupable-pc-id"]:
            self.logger.info(f"Skipping recursive parameter context assignment - not supported (pc_id: {pc_id})")
            return {"note": "Recursive assignment skipped - parameter contexts not supported"}

        if current_depth > max_depth:
            self.logger.warning(f"Maximum recursion depth ({max_depth}) reached for PG {root_pg_id}")
            return {"error": "Maximum recursion depth reached", "max_depth": max_depth}

        self.logger.info(
            f"🔄 Recursively assigning parameter context {pc_id} from PG {root_pg_id} (depth: {current_depth})")

        results = {
            "root_pg_id": root_pg_id,
            "parameter_context_id": pc_id,
            "depth": current_depth,
            "assignments": [],
            "skipped": [],
            "errors": [],
            "total_processed": 0,
            "total_assigned": 0,
            "total_skipped": 0,
            "total_errors": 0
        }

        try:
            # Get all child process groups
            children = self._get_child_process_groups(root_pg_id)

            for child_pg in children:
                child_pg_id = child_pg["id"]
                child_pg_name = child_pg.get("component", {}).get("name", "Unknown")
                results["total_processed"] += 1

                try:
                    # Check if this child already has the correct parameter context
                    current_pc = child_pg.get("component", {}).get("parameterContext", {})
                    current_pc_id = current_pc.get("id") if current_pc else None

                    if current_pc_id == pc_id:
                        # Already has the correct parameter context
                        self.logger.debug(f"   ✅ PG '{child_pg_name}' already has correct parameter context")
                        results["skipped"].append({
                            "pg_id": child_pg_id,
                            "name": child_pg_name,
                            "reason": "already_assigned"
                        })
                        results["total_skipped"] += 1
                    else:
                        # Assign parameter context to this child
                        self.logger.info(f"   🔗 Assigning parameter context to PG '{child_pg_name}' ({child_pg_id})")

                        # Try to assign parameter context with recursive application
                        assignment_result = self._bind_parameter_context_recursive(child_pg_id, pc_id)

                        if "error" not in assignment_result:
                            results["assignments"].append({
                                "pg_id": child_pg_id,
                                "name": child_pg_name,
                                "previous_pc_id": current_pc_id,
                                "new_pc_id": pc_id,
                                "status": "success"
                            })
                            results["total_assigned"] += 1
                            self.logger.info(f"   ✅ Successfully assigned parameter context to '{child_pg_name}'")
                        else:
                            results["errors"].append({
                                "pg_id": child_pg_id,
                                "name": child_pg_name,
                                "error": assignment_result.get("error", "Unknown error")
                            })
                            results["total_errors"] += 1
                            self.logger.warning(
                                f"   ❌ Failed to assign parameter context to '{child_pg_name}': {assignment_result.get('error')}")

                    # Recursively process children of this child
                    if current_depth < max_depth:
                        child_results = self.recursively_assign_parameter_context(
                            child_pg_id, pc_id, max_depth, current_depth + 1, stop_components_if_needed
                        )

                        # Merge child results
                        if "assignments" in child_results:
                            results["assignments"].extend(child_results["assignments"])
                            results["skipped"].extend(child_results["skipped"])
                            results["errors"].extend(child_results["errors"])
                            results["total_processed"] += child_results["total_processed"]
                            results["total_assigned"] += child_results["total_assigned"]
                            results["total_skipped"] += child_results["total_skipped"]
                            results["total_errors"] += child_results["total_errors"]

                except Exception as e:
                    self.logger.error(f"   ❌ Error processing child PG '{child_pg_name}': {e}")
                    results["errors"].append({
                        "pg_id": child_pg_id,
                        "name": child_pg_name,
                        "error": str(e)
                    })
                    results["total_errors"] += 1

            # Log summary for this level
            if current_depth == 0:  # Root level summary
                self.logger.info(f"🎯 Recursive parameter context assignment completed:")
                self.logger.info(f"   📊 Total Processed: {results['total_processed']}")
                self.logger.info(f"   ✅ Assigned: {results['total_assigned']}")
                self.logger.info(f"   ⏭️  Skipped: {results['total_skipped']}")
                self.logger.info(f"   ❌ Errors: {results['total_errors']}")

                # Log helpful information about errors
                if results['total_errors'] > 0:
                    self.logger.info("💡 Common error causes:")
                    self.logger.info("   - Running components referencing parameters (stop process groups first)")
                    self.logger.info("   - Process groups that already have different parameter contexts")
                    self.logger.info("   - Permission issues or invalid process group states")

            return results

        except Exception as e:
            self.logger.error(f"Failed to recursively assign parameter context: {e}")
            results["errors"].append({
                "pg_id": root_pg_id,
                "name": "root",
                "error": str(e)
            })
            results["total_errors"] += 1
            return results

    def _bind_parameter_context_recursive(self, pg_id: str, pc_id: str) -> Dict[str, Any]:
        """
        Bind parameter context with enhanced recursive support.
        This method tries multiple approaches to ensure recursive application.
        """
        if pc_id in ["unsupported-pc-id", "creation-failed-pc-id", "unsupported-parameter-context"]:
            return {"note": "Parameter context binding skipped - not supported"}

        self.logger.info(f"🔄 Binding parameter context {pc_id} to PG {pg_id} with enhanced recursive support")

        try:
            # Get current process group state
            pg = flow_manager.get_process_group(pg_id)
            if not pg:
                raise Exception(f"Process group {pg_id} not found")

            # Extract current component data
            current_component = pg.get("component", {})
            current_revision = pg.get("revision", {})

            # Build component update with all required fields
            component_update = {
                "id": pg_id,
                "name": current_component.get("name", ""),
                "executionEngine": current_component.get("executionEngine", "INHERITED"),
                "flowfileConcurrency": current_component.get("flowfileConcurrency", "UNBOUNDED"),
                "flowfileOutboundPolicy": current_component.get("flowfileOutboundPolicy", "STREAM_WHEN_AVAILABLE"),
                "defaultFlowFileExpiration": current_component.get("defaultFlowFileExpiration", "0 sec"),
                "defaultBackPressureObjectThreshold": current_component.get("defaultBackPressureObjectThreshold",
                                                                            10000),
                "defaultBackPressureDataSizeThreshold": current_component.get("defaultBackPressureDataSizeThreshold",
                                                                              "1 GB"),
                "logFileSuffix": current_component.get("logFileSuffix"),
                "parameterContext": {"id": pc_id},  # Assign parameter context
                "comments": current_component.get("comments", "")
            }

            # Use compatible update body (remove incompatible enum values)
            standard_update_body = {
                "revision": {
                    "clientId": current_revision.get("clientId", "orchestrator-client"),
                    "version": current_revision.get("version", 0)
                },
                "disconnectedNodeAcknowledged": False,
                "component": component_update
            }

            self.logger.info(f"   📡 Attempting compatible parameter context assignment")

            # Use only compatible API approaches
            approaches = [
                # Approach 1: Standard update (most compatible)
                {"url": f"process-groups/{pg_id}", "body": standard_update_body, "name": "Standard"},
            ]

            last_error = None
            for i, approach in enumerate(approaches, 1):
                try:
                    self.logger.debug(f"   📡 Trying {approach.get('name', 'approach')} {i}: {approach['url']}")
                    result = nifi_client.put_json(approach["url"], approach["body"])
                    self.logger.info(
                        f"   ✅ Successfully applied parameter context using {approach.get('name', 'approach')} {i}")
                    return result
                except Exception as e:
                    last_error = e
                    self.logger.debug(f"   ⚠️  {approach.get('name', 'Approach')} {i} failed: {e}")
                    continue

            # If all approaches failed, return error
            return {"error": f"All parameter context binding approaches failed. Last error: {last_error}"}

        except Exception as e:
            self.logger.warning(f"Failed to bind parameter context recursively {pc_id} to {pg_id}: {e}")
            return {"error": str(e)}

    def ensure_imported_flow_parameter_context(self, imported_pg_id: str, pc_id: str) -> Dict[str, Any]:
        """
        THOROUGHLY analyze and assign parameter context to ALL process groups recursively.
        First analyzes the complete hierarchy, then assigns to each one individually.
        """
        # Handle special parameter context IDs
        if pc_id in ["unsupported-pc-id", "unsupported-parameter-context"]:
            return {"note": "Parameter context assignment skipped - not supported"}

        # Handle parameter context that exists but cannot be looked up due to API limitations
        if pc_id == "exists-but-unlookupable-pc-id":
            self.logger.info("=========================================")
            self.logger.info("📋 PARAMETER CONTEXT EXISTS BUT CANNOT BE LOOKED UP")
            self.logger.info("=========================================")
            self.logger.info("   Reason: NiFi version supports CREATE (POST) but not LIST (GET) parameter contexts")
            self.logger.info("   Impact: Parameter context exists but cannot be assigned to process groups")
            self.logger.info("   Recommendation: Upgrade NiFi or manually assign parameter contexts in UI")
            return {"note": "Parameter context exists but API limitations prevent assignment"}

        # Special handling for creation-failed-pc-id - check if it actually exists
        if pc_id == "creation-failed-pc-id":
            self.logger.info("=========================================")
            self.logger.info("🔍 CREATION FAILED - CHECKING IF PARAMETER CONTEXT ALREADY EXISTS")
            self.logger.info("=========================================")

            # Try to find the parameter context that should exist
            try:
                # Get the tenant ID by traversing up the hierarchy
                tenant_id = self._extract_tenant_id_from_hierarchy(imported_pg_id)
                if tenant_id:
                    pc_name = f"{tenant_id}-context"
                    self.logger.info(f"🔍 Looking for existing parameter context: {pc_name}")

                    existing_pc_id = self.find_parameter_context_by_name(pc_name)
                    if existing_pc_id:
                        self.logger.info(f"✅ FOUND EXISTING PARAMETER CONTEXT!")
                        self.logger.info(f"   Name: {pc_name}")
                        self.logger.info(f"   ID: {existing_pc_id}")
                        self.logger.info("   Creation 'failed' because it already exists - using existing one!")
                        pc_id = existing_pc_id  # Use the existing parameter context
                    else:
                        self.logger.error(f"❌ Parameter context {pc_name} not found even though creation failed")
                        self.logger.error("   This indicates a real creation failure, not a conflict")
                        return {"note": "Parameter context creation failed and cannot find existing one"}
                else:
                    self.logger.error("❌ Could not extract tenant ID from process group hierarchy")
                    return {"note": "Cannot determine tenant ID for parameter context lookup"}

            except Exception as e:
                self.logger.error(f"❌ Error while looking up existing parameter context: {e}")
                return {"note": f"Error during parameter context lookup: {e}"}

        self.logger.info(f"🔍 STEP 1: COMPLETE RECURSIVE ANALYSIS OF PROCESS GROUP HIERARCHY")
        self.logger.info(f"Starting analysis from root process group: {imported_pg_id}")

        results = {
            "imported_pg_id": imported_pg_id,
            "parameter_context_id": pc_id,
            "analysis_phase": {},
            "assignment_phase": {},
            "final_verification": {},
            "total_discovered": 0,
            "total_assigned": 0,
            "total_failed": 0
        }

        try:
            # PHASE 1: THOROUGH ANALYSIS AND DISCOVERY
            analysis_results = self._comprehensive_process_group_analysis(imported_pg_id)
            results["analysis_phase"] = analysis_results
            results["total_discovered"] = len(analysis_results["all_process_groups"])

            self.logger.info(f"📊 ANALYSIS COMPLETE:")
            self.logger.info(f"   Total Process Groups Found: {results['total_discovered']}")
            self.logger.info(f"   Hierarchy Depth: {analysis_results['max_depth']}")
            self.logger.info(f"   Structure: {analysis_results['structure_summary']}")

            # PHASE 2: INDIVIDUAL ASSIGNMENT TO EACH PROCESS GROUP
            self.logger.info(f"🔗 STEP 2: INDIVIDUAL PARAMETER CONTEXT ASSIGNMENT")
            assignment_results = self._assign_parameter_context_to_all(
                analysis_results["all_process_groups"], pc_id
            )
            results["assignment_phase"] = assignment_results
            results["total_assigned"] = assignment_results["successful_assignments"]
            results["total_failed"] = assignment_results["failed_assignments"]

            # PHASE 3: VERIFICATION
            self.logger.info(f"✅ STEP 3: FINAL VERIFICATION")
            verification_results = self._verify_parameter_context_assignments(
                analysis_results["all_process_groups"], pc_id
            )
            results["final_verification"] = verification_results

            # COMPREHENSIVE SUMMARY
            self.logger.info(f"🎯 COMPREHENSIVE PARAMETER CONTEXT ASSIGNMENT SUMMARY:")
            self.logger.info(f"   📊 Total Discovered: {results['total_discovered']} process groups")
            self.logger.info(f"   🔗 Assignment Attempts: {results['total_discovered']}")
            self.logger.info(f"   ✅ Successful Assignments: {results['total_assigned']}")
            self.logger.info(f"   ❌ Failed Assignments: {results['total_failed']}")
            self.logger.info(f"   📋 Verification Results: {verification_results['summary']}")

            return results

        except Exception as e:
            self.logger.error(f"CRITICAL ERROR in comprehensive parameter context assignment: {e}")
            import traceback
            self.logger.error(f"Full traceback: {traceback.format_exc()}")
            results["critical_error"] = str(e)
            return results

    def _comprehensive_process_group_analysis(self, root_pg_id: str) -> Dict[str, Any]:
        """
        Phase 1: Comprehensive analysis of ALL process groups in the hierarchy.
        Returns detailed information about the structure and all discovered process groups.
        """
        self.logger.info(f"🔍 Starting comprehensive analysis of process group hierarchy...")

        analysis_results = {
            "all_process_groups": [],
            "max_depth": 0,
            "structure_summary": "",
            "depth_distribution": {},
            "total_count": 0
        }

        try:
            # Discover all process groups with detailed analysis
            all_pgs = self._discover_all_descendants(root_pg_id, max_depth=15)  # Increased depth limit
            analysis_results["all_process_groups"] = all_pgs
            analysis_results["total_count"] = len(all_pgs)

            if all_pgs:
                # Calculate depth distribution
                depth_counts = {}
                for pg in all_pgs:
                    depth = pg["depth"]
                    depth_counts[depth] = depth_counts.get(depth, 0) + 1
                    analysis_results["max_depth"] = max(analysis_results["max_depth"], depth)

                analysis_results["depth_distribution"] = depth_counts

                # Create structure summary
                structure_parts = []
                for depth in sorted(depth_counts.keys()):
                    count = depth_counts[depth]
                    level_name = "Main" if depth == 0 else f"Level-{depth}"
                    structure_parts.append(f"{level_name}: {count}")
                analysis_results["structure_summary"] = " | ".join(structure_parts)

                # Log detailed findings
                self.logger.info(f"📊 DETAILED ANALYSIS RESULTS:")
                self.logger.info(f"   Total Process Groups: {len(all_pgs)}")
                self.logger.info(f"   Maximum Depth: {analysis_results['max_depth']}")
                self.logger.info(f"   Structure: {analysis_results['structure_summary']}")

                self.logger.info(f"📋 COMPLETE PROCESS GROUP LIST:")
                for pg in all_pgs:
                    indent = "   " + "  " * pg["depth"]
                    self.logger.info(f"{indent}📁 {pg['name']} (ID: {pg['id'][:8]}..., Depth: {pg['depth']})")

            return analysis_results

        except Exception as e:
            self.logger.error(f"Failed comprehensive analysis: {e}")
            analysis_results["error"] = str(e)
            return analysis_results

    def _assign_parameter_context_to_all(self, all_process_groups: List[Dict[str, Any]], pc_id: str) -> Dict[str, Any]:
        """
        Phase 2: Assign parameter context to ALL discovered process groups individually.
        Uses individual API calls for each process group to ensure maximum success.
        """
        self.logger.info(
            f"🔗 Starting individual parameter context assignment to {len(all_process_groups)} process groups...")

        assignment_results = {
            "successful_assignments": 0,
            "failed_assignments": 0,
            "assignment_details": [],
            "error_details": []
        }

        for i, pg_info in enumerate(all_process_groups, 1):
            pg_id = pg_info["id"]
            pg_name = pg_info["name"]
            pg_depth = pg_info["depth"]

            try:
                indent = "   " + "  " * pg_depth
                self.logger.info(
                    f"{indent}🔗 [{i}/{len(all_process_groups)}] Assigning parameter context to '{pg_name}'")

                # Individual assignment with detailed error handling
                assignment_result = self.bind_parameter_context(pg_id, pc_id)

                if "error" not in assignment_result:
                    assignment_results["successful_assignments"] += 1
                    assignment_results["assignment_details"].append({
                        "pg_id": pg_id,
                        "name": pg_name,
                        "depth": pg_depth,
                        "status": "SUCCESS",
                        "order": i
                    })
                    self.logger.info(f"{indent}✅ [{i}/{len(all_process_groups)}] SUCCESS: '{pg_name}'")
                else:
                    assignment_results["failed_assignments"] += 1
                    error_msg = assignment_result.get("error", "Unknown error")
                    assignment_results["error_details"].append({
                        "pg_id": pg_id,
                        "name": pg_name,
                        "depth": pg_depth,
                        "status": "FAILED",
                        "error": error_msg,
                        "order": i
                    })
                    self.logger.warning(f"{indent}❌ [{i}/{len(all_process_groups)}] FAILED: '{pg_name}' - {error_msg}")

                # Brief pause between assignments to avoid overwhelming NiFi
                import time
                time.sleep(0.1)

            except Exception as e:
                assignment_results["failed_assignments"] += 1
                assignment_results["error_details"].append({
                    "pg_id": pg_id,
                    "name": pg_name,
                    "depth": pg_depth,
                    "status": "EXCEPTION",
                    "error": str(e),
                    "order": i
                })
                indent = "   " + "  " * pg_depth
                self.logger.error(f"{indent}💥 [{i}/{len(all_process_groups)}] EXCEPTION: '{pg_name}' - {e}")

        # Assignment phase summary
        success_rate = (assignment_results["successful_assignments"] / len(
            all_process_groups) * 100) if all_process_groups else 0
        self.logger.info(f"🔗 ASSIGNMENT PHASE COMPLETE:")
        self.logger.info(f"   ✅ Successful: {assignment_results['successful_assignments']}")
        self.logger.info(f"   ❌ Failed: {assignment_results['failed_assignments']}")
        self.logger.info(f"   📊 Success Rate: {success_rate:.1f}%")

        return assignment_results

    def _verify_parameter_context_assignments(self, all_process_groups: List[Dict[str, Any]], pc_id: str) -> Dict[
        str, Any]:
        """
        Phase 3: Verify that parameter contexts were actually assigned correctly.
        Checks each process group to confirm the parameter context is properly set.
        """
        self.logger.info(f"✅ Starting verification of parameter context assignments...")

        verification_results = {
            "verified_correct": 0,
            "verified_incorrect": 0,
            "verification_errors": 0,
            "verification_details": [],
            "summary": ""
        }

        for pg_info in all_process_groups:
            pg_id = pg_info["id"]
            pg_name = pg_info["name"]

            try:
                # Get current process group state
                current_pg = flow_manager.get_process_group(pg_id)
                if current_pg:
                    current_component = current_pg.get("component", {})
                    current_pc = current_component.get("parameterContext", {})
                    current_pc_id = current_pc.get("id") if current_pc else None

                    if current_pc_id == pc_id:
                        verification_results["verified_correct"] += 1
                        verification_results["verification_details"].append({
                            "pg_name": pg_name,
                            "status": "VERIFIED_CORRECT",
                            "current_pc_id": current_pc_id
                        })
                        self.logger.debug(f"   ✅ VERIFIED: '{pg_name}' has correct parameter context")
                    else:
                        verification_results["verified_incorrect"] += 1
                        verification_results["verification_details"].append({
                            "pg_name": pg_name,
                            "status": "VERIFIED_INCORRECT",
                            "expected_pc_id": pc_id,
                            "current_pc_id": current_pc_id
                        })
                        self.logger.warning(f"   ❌ INCORRECT: '{pg_name}' has wrong/missing parameter context")
                else:
                    verification_results["verification_errors"] += 1
                    self.logger.warning(f"   ⚠️  Could not retrieve process group '{pg_name}' for verification")

            except Exception as e:
                verification_results["verification_errors"] += 1
                self.logger.error(f"   💥 Verification error for '{pg_name}': {e}")

        # Create verification summary
        total_verified = verification_results["verified_correct"] + verification_results["verified_incorrect"]
        if total_verified > 0:
            verification_rate = (verification_results["verified_correct"] / total_verified * 100)
            verification_results[
                "summary"] = f"{verification_results['verified_correct']}/{total_verified} correct ({verification_rate:.1f}%)"
        else:
            verification_results["summary"] = "No verifications completed"

        self.logger.info(f"✅ VERIFICATION COMPLETE:")
        self.logger.info(f"   ✅ Correctly Assigned: {verification_results['verified_correct']}")
        self.logger.info(f"   ❌ Incorrectly Assigned: {verification_results['verified_incorrect']}")
        self.logger.info(f"   ⚠️  Verification Errors: {verification_results['verification_errors']}")
        self.logger.info(f"   📊 Overall: {verification_results['summary']}")

        return verification_results

    def _discover_all_descendants(self, root_pg_id: str, current_depth: int = 0, max_depth: int = 10) -> List[
        Dict[str, Any]]:
        """
        Recursively discover ALL process groups in a hierarchy, regardless of how many there are.
        Returns a flat list of all process groups with metadata about each one.
        
        Args:
            root_pg_id: Root process group to start discovery from
            current_depth: Current depth in the hierarchy (0 = root)
            max_depth: Maximum depth to traverse (safety limit)
            
        Returns:
            List of process group info dictionaries with id, name, depth
        """
        all_process_groups = []

        if current_depth > max_depth:
            self.logger.warning(f"Maximum discovery depth ({max_depth}) reached for PG {root_pg_id}")
            return all_process_groups

        try:
            # Get information about the current process group
            if current_depth == 0:
                # For root, get the process group info
                try:
                    root_pg_data = flow_manager.get_process_group(root_pg_id)
                    if root_pg_data:
                        root_name = root_pg_data.get("component", {}).get("name", "Main Flow")
                        all_process_groups.append({
                            "id": root_pg_id,
                            "name": root_name,
                            "depth": current_depth
                        })
                except Exception as e:
                    self.logger.debug(f"Could not get root process group info: {e}")
                    all_process_groups.append({
                        "id": root_pg_id,
                        "name": "Main Flow",
                        "depth": current_depth
                    })

            # Get all immediate children of the current process group
            children = self._get_child_process_groups(root_pg_id)

            self.logger.debug(f"Depth {current_depth}: Found {len(children)} child process groups under {root_pg_id}")

            # Add each child to our list
            for child_pg in children:
                child_pg_id = child_pg["id"]
                child_pg_name = child_pg.get("component", {}).get("name", "Unknown")

                # Add this child to our list
                all_process_groups.append({
                    "id": child_pg_id,
                    "name": child_pg_name,
                    "depth": current_depth + 1
                })

                # Recursively discover grandchildren, great-grandchildren, etc.
                try:
                    descendants = self._discover_all_descendants(child_pg_id, current_depth + 1, max_depth)
                    all_process_groups.extend(descendants)
                except Exception as e:
                    self.logger.debug(f"Failed to discover descendants of '{child_pg_name}': {e}")

            return all_process_groups

        except Exception as e:
            self.logger.warning(f"Failed to discover process groups under {root_pg_id}: {e}")
            return all_process_groups

    def _extract_tenant_id_from_hierarchy(self, pg_id: str) -> Optional[str]:
        """
        Extract tenant ID by traversing up the process group hierarchy.
        Looks for the tenant process group (typically 2-3 levels up from integration).
        """
        try:
            # Start from the given process group and traverse up
            current_pg_id = pg_id
            hierarchy_path = []
            max_depth = 10  # Safety limit

            # Traverse up the hierarchy and collect all process group names and data
            for depth in range(max_depth):
                # Get current process group info
                pg_data = flow_manager.get_process_group(current_pg_id)
                if not pg_data:
                    self.logger.warning(f"Could not get process group data for {current_pg_id}")
                    break

                component = pg_data.get("component", {})
                pg_name = component.get("name", "")
                parent_pg_id = component.get("parentGroupId")

                hierarchy_path.append({"name": pg_name, "id": current_pg_id, "parent_id": parent_pg_id})
                self.logger.debug(f"Hierarchy level {depth}: {pg_name} ({current_pg_id}) -> parent: {parent_pg_id}")

                # If no parent, we've reached root container
                if not parent_pg_id:
                    self.logger.debug(f"Reached root container: {pg_name}")
                    break

                # Check if parent is root container (like "NiFi Flow")
                parent_data = flow_manager.get_process_group(parent_pg_id)
                if parent_data:
                    parent_component = parent_data.get("component", {})
                    parent_parent_id = parent_component.get("parentGroupId")

                    # If parent has no parent, then current PG is likely the tenant
                    if not parent_parent_id:
                        self.logger.info(f"Found tenant process group: {pg_name} (parent is root container)")
                        return pg_name

                # Move up one level
                current_pg_id = parent_pg_id

            # Fallback: look for a reasonable tenant name in the hierarchy
            # Tenant is usually 2-3 levels up, and not "NiFi Flow" or similar generic names
            if len(hierarchy_path) >= 3:
                # Hierarchy is usually: Integration -> Category -> Tenant -> Root Container
                # So tenant should be at index -2 (second from top)
                tenant_candidate = hierarchy_path[-2]["name"]
                root_container = hierarchy_path[-1]["name"]

                self.logger.info(f"Hierarchy path: {' -> '.join([h['name'] for h in reversed(hierarchy_path)])}")
                self.logger.info(f"Tenant candidate: {tenant_candidate}, Root container: {root_container}")

                # Skip generic root container names
                if tenant_candidate not in ["NiFi Flow", "root", "Root", ""]:
                    return tenant_candidate

            self.logger.warning(f"Could not determine tenant ID from hierarchy path")
            return None

        except Exception as e:
            self.logger.error(f"Error extracting tenant ID from hierarchy: {e}")
            return None

    def _get_child_process_groups(self, parent_pg_id: str) -> List[Dict[str, Any]]:
        """Get all immediate child process groups of a parent."""
        try:
            data = nifi_client.get_json(f"process-groups/{parent_pg_id}/process-groups")
            return data.get("processGroups", [])
        except Exception as e:
            self.logger.warning(f"Failed to get child process groups for {parent_pg_id}: {e}")
            return []


# Global parameter manager instance
parameter_manager = ParameterManager()
