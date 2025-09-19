"""
Integration handlers for different pipeline types.
Hierarchical tenant-based organization with category groupings.
"""
import json
import logging
from tkinter import Listbox
from typing import Dict, Any, Optional, List
from .nifi.flows import flow_manager
from .nifi.params import parameter_manager
from .nifi.controllers import controller_manager
from .nifi.registry import registry_manager
from .utils.enums import FlowStatus
from .utils.idempotency import tenant_lock
from .utils.utils import _json_or_none
from .vault import HashiCorpVault, VaultConnectionError
from .utils.redis_client import RedisClient
from .utils.redis_keys import tenant_hash_key, tenant_categories_key, tenant_integration_hash_key
from .parameters import (
    INTEGRATION_PARAMETERS,
    get_required_parameters,
    get_sensitive_parameters,
    validate_parameters,
    get_supported_integrations
)
from .utils.integration_category import IntegrationCategory, INTEGRATION_CATEGORIES, INTEGRATION_NIFI_FLOW_NAMES

logger = logging.getLogger(__name__)


# def get_or_create_tenant_structure(tenant_id: str, category: str, integration_type: str,
#                                    additional_params: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
#     """
#     Create or get existing tenant structure with smart parameter context.
#     Only creates the specific category needed for the current integration.
#
#     Structure:
#     Root
#     └── {tenant_id} (Tenant Process Group)
#         ├── {tenant_id}-context (Smart Parameter Context - shared by all integrations)
#         └── {category} (Category Process Group) - only creates what's needed
#
#     Args:
#         tenant_id: Tenant identifier
#         api_key: Tenant API key
#         category: Specific category to create (e.g., "Identity & Access Review")
#         integration_type: Type of integration (jumpcloud, aws-security, etc.)
#         additional_params: Additional parameters for parameter context
#
#     Returns:
#         Dict with tenant_pg_id, parameter_context_id, and category_pg_id
#     """
#     logger.info(f"Setting up tenant structure for: {tenant_id}, category: {category}, integration: {integration_type}")
#
#     # Serialize tenant/category creation across processes to avoid duplicates
#     with tenant_lock(tenant_id):
#         # Step 1: Create or get tenant-level process group (smart duplicate prevention + smart positioning)
#         tenant_pg_name = f"{tenant_id}"
#         logger.info(f"Ensuring tenant process group: {tenant_pg_name}")
#
#         doc = RedisClient.get_instance().hget(key="nifi:tenant", hash=tenant_pg_name)
#         if doc is not None:
#             logger.info(doc)
#             doc = json.loads(doc)
#             logger.info(
#                 f"Found existing tenant process group: {tenant_pg_name} checking if category exists: {category}")
#
#             cat = RedisClient.get_instance().get(key=f"nifi:categories:tenant:{tenant_id}")
#             if cat is not None:
#                 cat = json.loads(cat)
#                 if cat.get(category) is not None:
#                     logger.info(f"Got existing tenant process group: {category}")
#                     return {
#                         "tenant_pg_id": doc.get("tenant_pg_id"),
#                         "parameter_context_id": doc.get("pc_id"),
#                         "category_pg_id": cat.get(category),
#                     }
#
#                 else:
#                     logger.info(f"The Category does not exists for {tenant_pg_name} tenant process group: {category}")
#
#                     category_position = flow_manager.get_smart_position_for_type(doc.get("tenant_pg_id"), "category",
#                                                                                  category)
#
#                     category_pg_id = flow_manager.ensure_process_group(
#                         parent_id=doc.get("tenant_pg_id"),
#                         name=f"{category}",
#                         position_x=category_position["x"],
#                         position_y=category_position["y"],
#                         comments=f"{category} integrations for tenant {tenant_id}"
#                     )
#
#                     payload = {category: category_pg_id, **cat}
#                     RedisClient.get_instance().set(key=f"nifi:categories:tenant:{tenant_id}", value=json.dumps(payload))
#
#                     pc_id = parameter_manager.ensure_tenant_parameter_context(tenant_id, integration_type,
#                                                                               additional_params)
#
#                     return {
#                         "tenant_pg_id": doc.get("tenant_pg_id"),
#                         "parameter_context_id": doc.get("pc_id"),
#                         "category_pg_id": category_pg_id
#                     }
#
#         # Get smart position for tenant process group
#         tenant_position = flow_manager.get_smart_position_for_type("root", "tenant", tenant_pg_name)
#
#         tenant_pg_id = flow_manager.ensure_process_group(
#             parent_id="root",
#             name=tenant_pg_name,
#             position_x=tenant_position["x"],
#             position_y=tenant_position["y"],
#             comments=f"Tenant process group for {tenant_id} - contains all integrations"
#         )
#
#         # Step 2: Create or update smart parameter context for the tenant (values only)
#         pc_id = parameter_manager.ensure_tenant_parameter_context(
#             tenant_id, integration_type, additional_params
#         )
#
#         # IMPORTANT: Do NOT bind at tenant level or recurse here.
#         # Binding is done only on the imported/updated flow root PG.
#         recursive_results = {"total_processed": 0}
#
#         # Step 3: Create or get ONLY the specific category process group needed (smart duplicate prevention + smart positioning)
#         category_pg_name = f"{category}"
#         logger.info(f"Ensuring category process group: {category}")
#
#         # Get smart position for category process group
#         category_position = flow_manager.get_smart_position_for_type(tenant_pg_id, "category", category_pg_name)
#
#         category_pg_id = flow_manager.ensure_process_group(
#             parent_id=tenant_pg_id,
#             name=category_pg_name,
#             position_x=category_position["x"],
#             position_y=category_position["y"],
#             comments=f"{category} integrations for tenant {tenant_id}"
#         )
#
#         payload = {category_pg_name: category_pg_id}
#         RedisClient.get_instance().set(key=f"nifi:categories:tenant:{tenant_id}", value=json.dumps(payload))
#
#         RedisClient.get_instance().hset(key="nifi:tenant", hash=tenant_id,
#                                         mapping={"tenant_pg_id": tenant_pg_id, "pc_id": pc_id})
#
#     # Log summary after releasing the lock (noop in scoped-binding mode)
#     if recursive_results.get("total_processed", 0) > 0:
#         assigned = recursive_results.get('total_assigned', 0)
#         skipped = recursive_results.get('total_skipped', 0)
#         errors = recursive_results.get('total_errors', 0)
#         logger.info(
#             f"📋 Recursive assignment summary: Processed: {recursive_results.get('total_processed', 0)}, "
#             f"Assigned: {assigned}, Skipped: {skipped}, Errors: {errors}"
#         )
#
#     return {
#         "tenant_pg_id": tenant_pg_id,
#         "parameter_context_id": pc_id,
#         "category_pg_id": category_pg_id
#     }

def get_or_create_tenant_structure(
        tenant_id: str,
        category: str,
        integration_type: str,
        additional_params: Optional[Dict[str, Any]] = None,
        stop_running_components_on_bind: bool = False,
) -> Dict[str, str]:
    """
    Ensure tenant PG, upsert tenant-level Parameter Context (values only),
    ensure requested category PG, and if tenant is new recursively assign
    the PC to all child PGs.

    Redis keys (centralized):
      - hset  tenant_hash_key()  field={tenant_id} -> {"tenant_pg_id": "...", "pc_id": "..."}
      - get/set tenant_categories_key(tenant_id) -> {"<Category String>": "<pgId>", ...}
    """

    logger.info(
        f"Setting up tenant structure: tenant={tenant_id}, category={category}, integration={integration_type}"
    )

    with tenant_lock(tenant_id):
        # ---------------------------
        # 1) Read tenant cache
        # ---------------------------
        tenant_doc_raw = RedisClient.get_instance().hget(key=tenant_hash_key(), hash=tenant_id)
        tenant_existed = bool(tenant_doc_raw)
        tenant_pg_id = None
        pc_id = None

        if tenant_existed:
            try:
                tenant_doc = json.loads(tenant_doc_raw) or {}
            except Exception:
                tenant_doc = {}
            tenant_pg_id = tenant_doc.get("tenant_pg_id")
            pc_id = tenant_doc.get("pc_id")

        # ---------------------------
        # 2) Ensure Tenant PG
        # ---------------------------
        if not tenant_pg_id:
            pos = flow_manager.get_smart_position_for_type("root", "tenant", tenant_id)
            tenant_pg_id = flow_manager.ensure_process_group(
                parent_id="root",
                name=tenant_id,
                position_x=pos["x"],
                position_y=pos["y"],
                comments=f"Tenant process group for {tenant_id} - contains all integrations"
            )

        # ------------------------------------------
        # 3) Upsert Tenant Parameter Context values
        # ------------------------------------------
        if pc_id:
            parameter_manager.update_existing_parameter_context(
                existing_pc_id=pc_id,
                additional_params=additional_params or {},
                tenant_id=tenant_id,
                integration_type=integration_type,
            )
        else:
            pc_id = parameter_manager.ensure_tenant_parameter_context(
                tenant_id=tenant_id,
                integration_type=integration_type,
                additional_params=additional_params or {},
            )

        # Persist tenant cache
        RedisClient.get_instance().hset(key=tenant_hash_key(), hash=tenant_id,
                                        mapping={"tenant_pg_id": tenant_pg_id, "pc_id": pc_id}, )

        # ----------------------------------------------------
        # 4) Ensure ONLY the requested Category process group
        # ----------------------------------------------------
        cat_key = tenant_categories_key(tenant_id)
        cat_cache_raw = RedisClient.get_instance().get(key=cat_key)
        try:
            cat_cache = json.loads(cat_cache_raw) if cat_cache_raw else {}
        except Exception:
            cat_cache = {}

        category_pg_id = cat_cache.get(category)
        if not category_pg_id:
            cpos = flow_manager.get_smart_position_for_type(tenant_pg_id, "category", category)
            category_pg_id = flow_manager.ensure_process_group(
                parent_id=tenant_pg_id,
                name=category,
                position_x=cpos["x"],
                position_y=cpos["y"],
                comments=f"{category} integrations for tenant {tenant_id}",
            )
            cat_cache[category] = category_pg_id
            RedisClient.get_instance().set(key=cat_key, value=json.dumps(cat_cache))

        # ----------------------------------------------------------
        # 5) If tenant is NEW, recursively assign PC across the tree
        # ----------------------------------------------------------
        if not tenant_existed:
            logger.info(
                f"New tenant detected; recursively assigning parameter context {pc_id} under tenant PG {tenant_pg_id}"
            )
            try:
                parameter_manager.recursively_assign_parameter_context(
                    tenant_pg_id,
                    pc_id,
                    stop_components_if_needed=stop_running_components_on_bind,
                )
            except Exception as e:
                logger.warning(f"Recursive PC assignment failed for tenant '{tenant_id}': {e}")

        return {
            "tenant_pg_id": tenant_pg_id,
            "parameter_context_id": pc_id,
            "category_pg_id": category_pg_id,
        }


def deploy_integration_hierarchical(tenant_id: str, integration_name: str, integration_type: str,
                                    category: str, flow_name: str, version: str = "latest",
                                    additional_params: Optional[Dict[str, Any]] = None) -> None:
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
        logger.debug(
            f"Starting hierarchical deployment - Tenant: {tenant_id}, Integration: {integration_name}, Category: {category}")

        # Step 1: Set up or get tenant structure for specific category
        tenant_structure = get_or_create_tenant_structure(tenant_id, category, integration_type, additional_params)

        # Step 2: Find the flow in registry
        flow_info = registry_manager.find_flow_by_name(flow_name)
        flow_version = version if version != "latest" else flow_info.version

        # Step 3: Get the category process group (now directly returned)
        category_pg_id = tenant_structure["category_pg_id"]

        # Step 4: Check if this specific integration already exists in the category
        integration_pg_name = f"{integration_name}"

        doc = RedisClient.get_instance().hget(
            key=tenant_integration_hash_key(tenant_id, integration_type),
            hash=integration_pg_name)
        if doc is not None:
            doc = json.loads(doc)
            logger.info(f"Found existing tenant process group: {integration_pg_name}")
            if doc.get("status") != FlowStatus.STARTED.value:
                logger.info(f"Found existing tenant process group: {integration_pg_name} stopped just need to start it")
                flow_manager.start_process_group(doc.get("integration_pg_id"))
                RedisClient.get_instance().hset(
                    key=tenant_integration_hash_key(tenant_id, integration_type),
                    hash=integration_pg_name,
                    mapping={"status": FlowStatus.STARTED.value, **doc})
            else:
                logger.info(f"Found existing tenant process group: {integration_pg_name} already started")
                return None

        existing_integration_pg = flow_manager.find_process_group_by_name(integration_pg_name, category_pg_id)

        if existing_integration_pg:
            logger.warning(
                f"Integration '{integration_name}' already exists in category '{category}' for tenant '{tenant_id}'")

            # Scoped binding only if root PG not already bound to desired PC
            pc_id = tenant_structure["parameter_context_id"]

            current_pc_id = parameter_manager.get_process_group_bound_pc_id(existing_integration_pg)
            if current_pc_id == pc_id:
                logger.info("Existing integration already bound to desired PC; no bind needed")
            else:
                logger.info("Binding desired PC to existing integration root PG (scoped)")
                parameter_manager.bind_parameter_context(existing_integration_pg, pc_id)

            # Ensure the existing integration is running (start if stopped)
            try:
                logger.info("Starting existing integration process group (id=%s)", existing_integration_pg)
                flow_manager.start_process_group(existing_integration_pg)
            except Exception as e:
                logger.warning("Failed to start existing integration PG %s: %s", existing_integration_pg, e)

            result = {
                "tenant_pg_id": tenant_structure["tenant_pg_id"],
                "category_pg_id": category_pg_id,
                "integration_pg_id": existing_integration_pg,
                "pc_id": pc_id,
                "tenant_id": tenant_id,
                "integration_name": integration_name,
                "category": category,
                "status": FlowStatus.STARTED.value,
            }
            logger.info(
                f"Integration already deployed for {tenant_id}/{category}/{integration_name} with parameter context review completed")

            RedisClient.get_instance().hset(key=tenant_integration_hash_key(tenant_id, integration_type),
                                            hash=integration_pg_name,
                                            mapping=result)
            return None

        # Step 5: Smart import flow into category process group (with duplicate prevention)
        logger.info(f"Smart importing flow '{flow_name}' into category '{category}' as '{integration_name}'")
        new_pg = flow_manager.smart_import_flow_from_registry(
            parent_pg_id=category_pg_id,
            flow_info=flow_info,
            integration_name=integration_pg_name,
            version=flow_version,
            tenant_id=f"{tenant_id}-{integration_name}"
        )

        # Step 6: Scoped PC binding only if the root PG is not already bound to the same PC
        root_pg_id = new_pg["id"]
        desired_pc_id = tenant_structure["parameter_context_id"]
        current_pc_id = parameter_manager.get_process_group_bound_pc_id(root_pg_id)


        if current_pc_id == desired_pc_id:
            logger.info("Root process group already bound to the desired parameter context; skipping bind")
        else:
            logger.info("Binding parameter context to root process group (scoped)")
            parameter_manager.recursively_assign_parameter_context(
                root_pg_id,
                desired_pc_id,
                stop_components_if_needed=False,
            )
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
            "status": FlowStatus.STARTED.value
        }

        RedisClient.get_instance().hset(
            key=tenant_integration_hash_key(tenant_id, integration_type), hash=integration_pg_name,
            mapping=result)
        logger.info(f"Successfully deployed {tenant_id}/{category}/{integration_name}")
        return None

    except Exception as e:
        logger.error(f"Error to deploy flow {e}")
        logger.error(f"❌ Failed to deploy {tenant_id}/{category}/{integration_name}: {e}")
        raise



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


def handle_deployment(tenant_id: str, integration: str):
    secrets_values = get_integration_secrets_from_vault(tenant_id, integration.upper())

    flow_names = INTEGRATION_NIFI_FLOW_NAMES.get(integration)
    if flow_names is None: return

    if isinstance(flow_names, list):
        for flow in flow_names:
            deploy_integration_hierarchical(
                tenant_id=tenant_id,
                additional_params=secrets_values,
                integration_name=flow.get("name"),
                integration_type=integration,
                category=flow.get("category"),
                flow_name=flow.get("registry_flow_name"),
                version="latest"
            )

    else:
        deploy_integration_hierarchical(
            tenant_id=tenant_id,
            additional_params=secrets_values,
            integration_name=flow_names.get("name"),
            integration_type=integration,
            category=flow_names.get("category"),
            flow_name=flow_names.get("registry_flow_name"),
            version="latest"
        )

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


def update_integration_secrets(tenant_id: str, integration_name: str) -> None:
    secret_values = get_integration_secrets_from_vault(tenant_id, integration_name.upper())

    doc = RedisClient.get_instance().hget(key=tenant_hash_key(), hash=tenant_id)

    if doc is not None:
        doc = json.loads(doc)
        pc_id = doc.get("pc_id")
        parameter_manager.update_existing_parameter_context(existing_pc_id=pc_id, additional_params=secret_values,
                                                            tenant_id=tenant_id, integration_type=integration_name)
    else:
        parameter_manager.ensure_tenant_parameter_context(tenant_id=tenant_id, integration_type=integration_name,
                                                          additional_params=secret_values)
    logger.info(f"Updating secrets for integration {integration_name}")


def stop_integration(tenant_id: str, integration_name: str) -> None:
    # 0) Normalize integration → list of flow specs
    mapping = INTEGRATION_NIFI_FLOW_NAMES.get(integration_name)
    if not mapping:
        raise ValueError(f"No category mapping found for integration '{integration_name}'")

    if isinstance(mapping, dict):
        flows: List[Dict[str, Any]] = [mapping]
    elif isinstance(mapping, list):
        flows = mapping
    else:
        raise ValueError(f"Invalid mapping type for integration '{integration_name}'")

    # Collect flow keys (hash fields) from mapping (use flow 'name')
    flow_keys: List[str] = [f.get("name") for f in flows if f and f.get("name")]

    tenant_pc_id = parameter_manager.find_parameter_context_by_name(f"{tenant_id}-context")

    # 1) Load cache: HGETALL of centralized tenant_integration_hash_key
    redis_key = tenant_integration_hash_key(tenant_id, integration_name)
    raw_map: Optional[Dict[str, str]] = RedisClient.get_instance().hgetall(key=redis_key)

    pg_ids: List[str] = []
    cache_present = False
    cached_entries: Dict[str, Dict[str, Any]] = {}

    if raw_map:
        cache_present = True
        for field, raw_json in raw_map.items():
            try:
                value = json.loads(raw_json)
            except Exception:
                value = {}
            cached_entries[field] = value
            # If not explicitly STOPPED, and has a PG ID, we intend to stop it
            if value.get("status") != FlowStatus.STOPPED.value:
                pid = value.get("integration_pg_id")
                if pid:
                    pg_ids.append(pid)

    # 2) If cache is missing or incomplete, discover PG IDs
    #    Incomplete = fewer cached flow-entries with PG IDs than actual flows
    stored_count = len([v for v in cached_entries.values() if v.get("integration_pg_id")])
    actual_count = len(flows)

    if (not cache_present) or (stored_count < actual_count):
        discovered = flow_manager.find_process_group_by_name_stop_integration(
            tenant_id=tenant_id,
            integration_name=integration_name,
            tenant_pc_id=tenant_pc_id
        )
        for item in discovered:
            pid = item.get("integration_pg_id")
            pname = item.get("integration_name")
            pg_ids.append(pid)
            cached_entries[pname] = item

    # 3) Dedupe PG IDs (preserve order)
    seen = set()
    unique_pg_ids = [pid for pid in pg_ids if pid and not (pid in seen or seen.add(pid))]

    # 4) Stop all found PGs
    for pid in unique_pg_ids:
        flow_manager.stop_process_group(pid)

    # 5) Mark all mapped flows as STOPPED in cache (preserving existing fields)
    for fkey in flow_keys:
        current = cached_entries.get(fkey, {})
        updated = {**current, "status": FlowStatus.STOPPED.value}
        RedisClient.get_instance().hset(key=redis_key, hash=fkey, mapping=updated)

    return None
