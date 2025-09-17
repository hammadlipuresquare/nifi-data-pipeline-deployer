"""
Integration handlers for different pipeline types.
Hierarchical tenant-based organization with category groupings.
"""

import logging
from typing import Dict, Any, Optional
from dataclasses import dataclass
from .nifi.flows import flow_manager
from .nifi.params import parameter_manager
from .nifi.controllers import controller_manager
from .nifi.registry import registry_manager
from .utils.idempotency import tenant_lock
from .vault import HashiCorpVault, VaultConnectionError
from .parameters import (
    INTEGRATION_PARAMETERS,
    get_required_parameters,
    get_sensitive_parameters,
    validate_parameters,
    get_supported_integrations
)

logger = logging.getLogger(__name__)


@dataclass
class IntegrationCategory:
    """Defines integration categories for organizational grouping."""
    ASSET_REGISTER = "Asset Register"
    MISCONFIGURATION = "Misconfiguration"
    VULNERABILITY = "Vulnerability"
    COMPLIANCE = "Compliance"
    CASE_MANAGEMENT = "Case Management"
    IDENTITY_AND_ACCESS_REVIEW = "Identity & Access Review"

    @classmethod
    def get_all_categories(cls) -> list[str]:
        """Get all available categories."""
        return [cls.ASSET_REGISTER, cls.MISCONFIGURATION, cls.VULNERABILITY, cls.COMPLIANCE, cls.CASE_MANAGEMENT,
                cls.IDENTITY_AND_ACCESS_REVIEW]


def get_or_create_tenant_structure(tenant_id: str, category: str, integration_type: str,
                                   additional_params: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
    """
    Create or get existing tenant structure with smart parameter context.
    Only creates the specific category needed for the current integration.
    
    Structure:
    Root
    └── {tenant_id} (Tenant Process Group)
        ├── {tenant_id}-context (Smart Parameter Context - shared by all integrations)
        └── {category} (Category Process Group) - only creates what's needed
        
    Args:
        tenant_id: Tenant identifier
        api_key: Tenant API key
        category: Specific category to create (e.g., "Identity & Access Review")
        integration_type: Type of integration (jumpcloud, aws-security, etc.)
        additional_params: Additional parameters for parameter context
        
    Returns:
        Dict with tenant_pg_id, parameter_context_id, and category_pg_id
    """
    logger.info(f"Setting up tenant structure for: {tenant_id}, category: {category}, integration: {integration_type}")

    # Serialize tenant/category creation across processes to avoid duplicates
    with tenant_lock(tenant_id):
        # Step 1: Create or get tenant-level process group (smart duplicate prevention + smart positioning)
        tenant_pg_name = f"{tenant_id}"
        logger.info(f"Ensuring tenant process group: {tenant_pg_name}")

        # Get smart position for tenant process group
        tenant_position = flow_manager.get_smart_position_for_type("root", "tenant", tenant_pg_name)

        tenant_pg_id = flow_manager.ensure_process_group(
            parent_id="root",
            name=tenant_pg_name,
            position_x=tenant_position["x"],
            position_y=tenant_position["y"],
            comments=f"Tenant process group for {tenant_id} - contains all integrations"
        )

        # Step 2: Create or update smart parameter context for the tenant
        pc_id = parameter_manager.ensure_tenant_parameter_context(tenant_id, integration_type, additional_params)

        # Step 2.5: Bind parameter context to tenant process group and recursively to all children
        logger.info(f"Binding parameter context {pc_id} to tenant process group {tenant_pg_id}")
        parameter_manager.bind_parameter_context(tenant_pg_id, pc_id)

        # Step 2.6: Recursively assign parameter context to all existing children (before deploying new ones)
        logger.info(f"Recursively assigning parameter context to all existing children of tenant {tenant_id}")
        recursive_results = parameter_manager.recursively_assign_parameter_context(
            tenant_pg_id, pc_id, stop_components_if_needed=False
        )

        # Step 3: Create or get ONLY the specific category process group needed (smart duplicate prevention + smart positioning)
        category_pg_name = f"{category}"
        logger.info(f"Ensuring category process group: {category}")

        # Get smart position for category process group
        category_position = flow_manager.get_smart_position_for_type(tenant_pg_id, "category", category_pg_name)

        category_pg_id = flow_manager.ensure_process_group(
            parent_id=tenant_pg_id,
            name=category_pg_name,
            position_x=category_position["x"],
            position_y=category_position["y"],
            comments=f"{category} integrations for tenant {tenant_id}"
        )

    # Log summary after releasing the lock
    if recursive_results.get("total_processed", 0) > 0:
        assigned = recursive_results.get('total_assigned', 0)
        skipped = recursive_results.get('total_skipped', 0)
        errors = recursive_results.get('total_errors', 0)

        logger.info(f"📋 Recursive assignment summary: "
                    f"Processed: {recursive_results.get('total_processed', 0)}, "
                    f"Assigned: {assigned}, Skipped: {skipped}, Errors: {errors}")

        if errors > 0:
            logger.warning(f"⚠️  {errors} process groups could not be updated (likely due to running components)")
            logger.info(
                "💡 This is normal for running integrations - new deployments will inherit parameter context correctly")
    else:
        logger.info("📋 No existing children found - parameter context will be inherited by new children")

    return {
        "tenant_pg_id": tenant_pg_id,
        "parameter_context_id": pc_id,
        "category_pg_id": category_pg_id
    }


def deploy_integration_hierarchical(tenant_id: str, integration_name: str, integration_type: str,
                                    category: str, flow_name: str, version: str = "latest",
                                    additional_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Deploy an integration using hierarchical tenant structure with smart parameter context.
    
    Args:
        tenant_id: Tenant identifier
        api_key: Tenant API key  
        integration_name: Name of the integration (e.g., "JumpCloud Asset Discovery")
        integration_type: Type of integration (jumpcloud, aws-security, etc.)
        category: Business category (Asset Register, Misconfiguration, etc.)
        flow_name: NiFi flow name to deploy
        version: Flow version
        additional_params: Additional parameters for the parameter context
        
    Returns:
        Deployment result with hierarchy information
    """
    try:
        logger.info(
            f"Starting hierarchical deployment - Tenant: {tenant_id}, Integration: {integration_name}, Category: {category}")

        # Step 1: Set up or get tenant structure for specific category
        tenant_structure = get_or_create_tenant_structure(tenant_id, category, integration_type,
                                                          additional_params)

        # Step 2: Find the flow in registry
        flow_info = registry_manager.find_flow_by_name(flow_name)
        flow_version = version if version != "latest" else flow_info.version

        # Step 3: Get the category process group (now directly returned)
        category_pg_id = tenant_structure["category_pg_id"]

        # Step 4: Check if this specific integration already exists in the category
        integration_pg_name = f"{integration_name}"
        existing_integration_pg = flow_manager.find_process_group_by_name(integration_pg_name, category_pg_id)

        if existing_integration_pg:
            logger.warning(
                f"Integration '{integration_name}' already exists in category '{category}' for tenant '{tenant_id}'")

            # CRITICAL FIX: Even for existing integrations, ensure parameter context is properly assigned
            pc_id = tenant_structure["parameter_context_id"]
            if pc_id and pc_id not in ["unsupported-parameter-context", "creation-failed-pc-id",
                                       "exists-but-unlookupable-pc-id"]:
                logger.info(
                    f"🔧 Ensuring parameter context is properly assigned to existing integration and all its children")
                pc_assignment_result = parameter_manager.ensure_imported_flow_parameter_context(
                    existing_integration_pg, pc_id
                )

                if pc_assignment_result.get("total_assigned", 0) > 0:
                    logger.info(
                        f"📋 Parameter context assignment for existing integration: {pc_assignment_result.get('total_assigned')} process groups updated")
                else:
                    logger.warning(f"⚠️  Parameter context assignment had limited success for existing integration")
            elif pc_id == "creation-failed-pc-id":
                # Apply the same fix logic for existing integrations
                logger.info(
                    f"🔍 Existing integration has creation-failed-pc-id - attempting parameter context lookup and assignment")
                pc_assignment_result = parameter_manager.ensure_imported_flow_parameter_context(
                    existing_integration_pg, pc_id
                )

                # Update the pc_id if we found an existing one
                if pc_assignment_result.get("parameter_context_id") and pc_assignment_result[
                    "parameter_context_id"] != "creation-failed-pc-id":
                    pc_id = pc_assignment_result["parameter_context_id"]
                    logger.info(f"✅ Updated parameter context ID for existing integration: {pc_id}")
            elif pc_id == "exists-but-unlookupable-pc-id":
                logger.info(
                    f"ℹ️  Parameter context exists but cannot be assigned due to NiFi API limitations (exists-but-unlookupable-pc-id)")
                logger.info(f"   This is expected behavior for NiFi versions with partial parameter context support")
            else:
                logger.info(f"ℹ️  Parameter context not supported or available for existing integration")

            result = {
                "tenant_pg_id": tenant_structure["tenant_pg_id"],
                "category_pg_id": category_pg_id,
                "integration_pg_id": existing_integration_pg,
                "pc_id": pc_id,  # Use potentially updated pc_id
                "tenant_id": tenant_id,
                "integration_name": integration_name,
                "category": category,
                "status": "already_exists"
            }
            logger.info(
                f"✅ Integration already deployed for {tenant_id}/{category}/{integration_name} with parameter context review completed")
            return result

        # Step 5: Smart import flow into category process group (with duplicate prevention)
        logger.info(f"Smart importing flow '{flow_name}' into category '{category}' as '{integration_name}'")
        new_pg = flow_manager.smart_import_flow_from_registry(
            parent_pg_id=category_pg_id,
            flow_info=flow_info,
            integration_name=integration_pg_name,
            version=flow_version,
            tenant_id=f"{tenant_id}-{integration_name}"
        )

        # Step 6: Ensure parameter context is properly applied to the imported flow and all its children
        logger.info(f"Ensuring parameter context is applied to imported flow and all its children")
        pc_assignment_result = parameter_manager.ensure_imported_flow_parameter_context(
            new_pg["id"], tenant_structure["parameter_context_id"]
        )

        if pc_assignment_result.get("total_assigned", 0) > 0:
            logger.info(
                f"📋 Parameter context assignment: {pc_assignment_result.get('total_assigned')} process groups updated")
        else:
            logger.warning(
                f"⚠️  Parameter context assignment had limited success - this may be due to running components")

        # Step 6.5: Also bind using the standard method as backup
        logger.info(f"Applying standard parameter context binding as backup")
        parameter_manager.bind_parameter_context(new_pg["id"], tenant_structure["parameter_context_id"])

        # Step 7: Configure Kafka services if needed
        logger.info(f"Configuring Kafka controller services")
        kafka_services = controller_manager.configure_kafka_services(new_pg["id"], tenant_id)

        # Step 8: Enable all controller services
        logger.info(f"Enabling controller services")
        controller_services = controller_manager.enable_all_services(new_pg["id"])

        # Step 9: Start the integration process group
        logger.info(f"Starting integration process group")
        flow_manager.start_process_group(new_pg["id"])

        result = {
            "tenant_pg_id": tenant_structure["tenant_pg_id"],
            "category_pg_id": category_pg_id,
            "integration_pg_id": new_pg["id"],
            "pc_id": tenant_structure["parameter_context_id"],
            "tenant_id": tenant_id,
            "integration_name": integration_name,
            "category": category,
            "controller_services": controller_services,
            "kafka_services": kafka_services,
            "flow_info": {
                "registry_id": flow_info.registry_id,
                "bucket_id": flow_info.bucket_id,
                "flow_id": flow_info.flow_id,
                "flow_name": flow_info.flow_name,
                "version": flow_version
            },
            "status": "deployed"
        }

        logger.info(f"✅ Successfully deployed {tenant_id}/{category}/{integration_name}")
        return result

    except Exception as e:
        logger.error(f"Error to deploy flow {e}")
        logger.error(f"❌ Failed to deploy {tenant_id}/{category}/{integration_name}: {e}")
        raise


def deploy_jumpcloud_pipeline(tenant_id: str, version: str = "latest") -> Dict[str, Any]:
    secrets_values = get_integration_secrets_from_vault(tenant_id, "JUMPCLOUD")

    # Deploy primary JumpCloud Asset Discovery integration
    primary_result = deploy_integration_hierarchical(
        tenant_id=tenant_id,
        additional_params=secrets_values,
        integration_name="JumpCloud Asset Discovery",
        integration_type="jumpcloud",
        category=IntegrationCategory.IDENTITY_AND_ACCESS_REVIEW,
        flow_name="jumpcloud-asset",
        version=version
    )

    # Deploy secondary JumpCloud Event Logs integration (if available)
    try:
        secondary_result = deploy_integration_hierarchical(
            tenant_id=tenant_id,
            additional_params=secrets_values,
            integration_name="JumpCloud Event Logs",
            integration_type="jumpcloud",
            category=IntegrationCategory.IDENTITY_AND_ACCESS_REVIEW,
            flow_name="jumpcloud-events",
            version=version
        )
        
        # Merge results with primary taking precedence
        primary_result["secondary_integration"] = {
            "name": "JumpCloud Event Logs",
            "status": secondary_result.get("status", "unknown"),
            "integration_pg_id": secondary_result.get("integration_pg_id")
        }
        
    except Exception as e:
        logger.warning(f"Failed to deploy secondary JumpCloud Event Logs integration: {e}")
        primary_result["secondary_integration"] = {
            "name": "JumpCloud Event Logs",
            "status": "failed",
            "error": str(e)
        }
    
    return primary_result


def deploy_aws_asset_registry_pipeline(tenant_id: str, version: str = "latest") -> Dict[str, Any]:
    secrets_values = get_integration_secrets_from_vault(tenant_id, "AWS")

    return deploy_integration_hierarchical(
        tenant_id=tenant_id,
        additional_params=secrets_values,
        integration_name="AWS Asset Registry",
        integration_type="aws",  # Fixed: Use "aws" to match INTEGRATION_PARAMETERS
        category=IntegrationCategory.ASSET_REGISTER,
        flow_name="aws",
        version=version
    )


def deploy_custom_integration(tenant_id: str, api_key: str, integration_name: str,
                              category: str, flow_name: str, version: str = "latest",
                              additional_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Deploy any custom integration with specified category.
    This is the generic function that can handle any integration.
    """
    return deploy_integration_hierarchical(
        tenant_id=tenant_id,
        integration_name=integration_name,
        integration_type="custom",
        category=category,
        flow_name=flow_name,
        version=version,
        additional_params=additional_params
    )


# Integration registry - hierarchical mapping with categories
INTEGRATION_REGISTRY = {
    "jumpcloud": deploy_jumpcloud_pipeline,
    "custom": deploy_custom_integration,
    "aws": deploy_aws_asset_registry_pipeline,
}

# Category-based integration mapping
CATEGORY_INTEGRATIONS = {
    IntegrationCategory.ASSET_REGISTER: {
        "aws": deploy_aws_asset_registry_pipeline
    },
    IntegrationCategory.IDENTITY_AND_ACCESS_REVIEW: {
        "jumpcloud": deploy_jumpcloud_pipeline
    }
}


def get_available_integrations() -> list[str]:
    """Get list of available integrations."""
    return list(INTEGRATION_REGISTRY.keys())


def validate_integration_deployment_params(integration_type: str, provided_params: Dict[str, Any]) -> Dict[str, Any]:
    """Validate integration parameters before deployment."""
    logger.info(f"🔍 Validating parameters for {integration_type} integration")

    validation_result = validate_parameters(integration_type, provided_params)

    if not validation_result["valid"]:
        logger.warning(f"❌ Parameter validation failed for {integration_type}")
        for error in validation_result["errors"]:
            logger.warning(f"   • {error}")
    else:
        logger.info(f"✅ Parameter validation passed for {integration_type}")

    return validation_result


def get_integration_parameter_info(integration_type: str) -> Dict[str, Any]:
    """Get parameter information for a specific integration."""
    if integration_type not in INTEGRATION_PARAMETERS:
        return {
            "integration_type": integration_type,
            "supported": False,
            "error": f"Integration type '{integration_type}' is not supported"
        }

    all_params = INTEGRATION_PARAMETERS[integration_type]
    required_params = get_required_parameters(integration_type)
    sensitive_params = get_sensitive_parameters(integration_type)

    return {
        "integration_type": integration_type,
        "supported": True,
        "all_parameters": all_params,
        "required_parameters": required_params,
        "sensitive_parameters": sensitive_params,
        "parameter_count": {
            "total": len(all_params),
            "required": len(required_params),
            "sensitive": len(sensitive_params)
        }
    }


def list_all_integration_info() -> Dict[str, Dict[str, Any]]:
    """List parameter information for all supported integrations."""
    return {
        integration_type: get_integration_parameter_info(integration_type)
        for integration_type in get_supported_integrations()
    }


def get_integrations_by_category() -> Dict[str, list[str]]:
    """Get integrations organized by category."""
    result = {}
    for category, integrations in CATEGORY_INTEGRATIONS.items():
        result[category] = list(integrations.keys())
    return result


def ensure_tenant_parameter_context_inheritance(tenant_id: str, stop_running_components: bool = False) -> Dict[
    str, Any]:
    """
    Ensure all process groups under a tenant inherit the tenant's parameter context.
    
    This function can be called independently to fix parameter context inheritance
    for existing tenant hierarchies.
    
    Args:
        tenant_id: Tenant identifier
        stop_running_components: If True, attempt to stop running components before assignment (use with caution)
        
    Returns:
        Dict with recursive assignment results
    """
    logger.info(f"🔄 Ensuring parameter context inheritance for tenant: {tenant_id}")

    # Find tenant process group
    tenant_pg_id = flow_manager.find_process_group_by_name(tenant_id, "root")
    if not tenant_pg_id:
        error_msg = f"Tenant process group '{tenant_id}' not found"
        logger.error(error_msg)
        return {"error": error_msg, "tenant_id": tenant_id}

    # Get tenant's parameter context
    try:
        tenant_pg = flow_manager.get_process_group(tenant_pg_id)
        if not tenant_pg:
            error_msg = f"Could not retrieve tenant process group {tenant_pg_id}"
            logger.error(error_msg)
            return {"error": error_msg, "tenant_id": tenant_id}

        tenant_component = tenant_pg.get("component", {})
        tenant_pc = tenant_component.get("parameterContext", {})
        tenant_pc_id = tenant_pc.get("id") if tenant_pc else None

        if not tenant_pc_id:
            error_msg = f"Tenant '{tenant_id}' does not have a parameter context assigned"
            logger.error(error_msg)
            return {"error": error_msg, "tenant_id": tenant_id}

        tenant_pc_name = tenant_pc.get("component", {}).get("name", "Unknown")
        logger.info(f"Found tenant parameter context: {tenant_pc_name} ({tenant_pc_id})")

        # Recursively assign parameter context to all children
        results = parameter_manager.recursively_assign_parameter_context(
            tenant_pg_id, tenant_pc_id, stop_components_if_needed=stop_running_components
        )

        # Add tenant information to results
        results.update({
            "tenant_id": tenant_id,
            "tenant_pg_id": tenant_pg_id,
            "tenant_parameter_context": {
                "id": tenant_pc_id,
                "name": tenant_pc_name
            }
        })

        logger.info(f"✅ Parameter context inheritance completed for tenant '{tenant_id}'")
        return results

    except Exception as e:
        error_msg = f"Failed to ensure parameter context inheritance for tenant '{tenant_id}': {e}"
        logger.error(error_msg)
        return {"error": error_msg, "tenant_id": tenant_id}


def deploy_integration(integration: str, tenant_id: str, api_key: str,
                       flow_name: str = None, version: str = "latest",
                       parent_pg_id: str = None, integration_name: str = None,
                       category: str = None, additional_params: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Deploy any integration by name using hierarchical structure.
    
    Args:
        integration: Integration name (jumpcloud, salesforce, etc.)
        tenant_id: Tenant identifier
        api_key: Tenant API key
        flow_name: Optional flow name (defaults to integration-specific)
        version: Flow version
        parent_pg_id: Parent process group (deprecated in hierarchical mode)
        integration_name: Custom integration display name
        category: Force specific category
        additional_params: Additional parameters for parameter context
        
    Returns:
        Deployment result dictionary
        
    Raises:
        ValueError: If integration is not supported
    """
    if integration not in INTEGRATION_REGISTRY:
        available = ", ".join(get_available_integrations())
        raise ValueError(f"Unsupported integration '{integration}'. Available: {available}")

    # Special handling for custom integrations
    if integration == "custom":
        if not integration_name or not category or not flow_name:
            raise ValueError("Custom integrations require integration_name, category, and flow_name")
        return deploy_custom_integration(
            tenant_id=tenant_id,
            api_key=api_key,
            integration_name=integration_name,
            category=category,
            flow_name=flow_name,
            version=version,
            additional_params=additional_params
        )

    # Get the deployment function
    deploy_func = INTEGRATION_REGISTRY[integration]

    # Call with appropriate parameters
    if flow_name:
        return deploy_func(tenant_id, api_key, flow_name, version)
    else:
        # Use default flow name for the integration
        return deploy_func(tenant_id, api_key, version=version)


def list_buckets_and_flows() -> None:
    """List all available flows from all registries."""
    registries = registry_manager.get_registries()

    for registry in registries:
        registry_name = registry["component"]["name"]
        registry_id = registry["id"]
        print(f"[Registry] {registry_name}  id={registry_id}")

        try:
            buckets = registry_manager.get_buckets_from_registry(registry_id)
            for bucket in buckets:
                bucket_id = bucket["id"]
                bucket_name = bucket["bucket"]["name"]
                print(f"  └─[Bucket] {bucket_name}  id={bucket_id}")

                try:
                    flows = registry_manager.get_flows_from_bucket(registry_id, bucket_id)
                    for flow in flows:
                        versioned_flow = flow["versionedFlow"]
                        print(f"    └─[Flow] {versioned_flow['flowName']}  id={versioned_flow['flowId']}")
                except Exception as e:
                    print(f"    └─Error listing flows in bucket {bucket_name}: {e}")
        except Exception as e:
            print(f"  └─Error listing buckets: {e}")


def get_nifi_version() -> str:
    """Get NiFi version information."""
    from .nifi.client import nifi_client
    try:
        data = nifi_client.get_json("system-diagnostics")
        version = data.get("systemDiagnostics", {}).get("versionInfo", {}).get("niFiVersion", "Unknown")
        return version
    except Exception as e:
        logger.warning(f"Could not retrieve NiFi version: {e}")
        return "Unknown"


def get_integration_secrets_from_vault(tenant_id: str, integration_name: str) -> Any:
    secret_lists = HashiCorpVault().list_secrets(f"{tenant_id}/{integration_name}")

    logger.debug(f"List returned from vault{secret_lists}")

    if not len(secret_lists): raise Exception("JumpCloud Secret lists not found")

    secrets_values = HashiCorpVault().get_secret(f"{tenant_id}/{integration_name}/{secret_lists[0]}")

    return secrets_values
