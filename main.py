#!/usr/bin/env python3
"""
Main entry point that preserves original functionality.
This file provides the same interface as the original main.py.
"""

# Import all functions from the new modular structure
from src.orchestrator.integrations import (
    deploy_jumpcloud_pipeline,
    deploy_integration,
    list_buckets_and_flows,
    get_nifi_version,
    get_available_integrations
)

from src.orchestrator.nifi.registry import registry_manager
from src.orchestrator.nifi.flows import flow_manager
from src.orchestrator.nifi.params import parameter_manager
from src.orchestrator.nifi.controllers import controller_manager
from src.orchestrator.nifi.client import nifi_client

# Re-export with original function names for compatibility
def find_registry_and_flow_info(flow_name: str):
    """Find registry, bucket, and flow information for a given flow name."""
    flow_info = registry_manager.find_flow_by_name(flow_name)
    return {
        "registry_id": flow_info.registry_id,
        "bucket_id": flow_info.bucket_id,
        "flow_id": flow_info.flow_id,
        "flow_name": flow_info.flow_name,
        "latest_version": flow_info.version
    }

def get_registries():
    """Get all NiFi registry clients."""
    return registry_manager.get_registries()

def get_buckets_from_registry(registry_id: str):
    """Get all buckets from a specific registry."""
    return registry_manager.get_buckets_from_registry(registry_id)

def get_flows_from_bucket(registry_id: str, bucket_id: str):
    """Get all flows from a specific bucket in a registry."""
    return registry_manager.get_flows_from_bucket(registry_id, bucket_id)

def get_flow_versions(registry_id: str, bucket_id: str, flow_id: str):
    """Get all versions of a specific flow."""
    return registry_manager.get_flow_versions(registry_id, bucket_id, flow_id)

def check_existing_tenant_deployment(tenant_id):
    """Check if tenant already has existing deployment."""
    return flow_manager.check_existing_tenant_deployment(tenant_id)

def create_tenant_parameter_context(tenant_id):
    """Create or find existing parameter context for a tenant."""
    return parameter_manager.create_tenant_parameter_context(tenant_id)

def bind_param_context(pg_id, pc_id):
    """Bind parameter context to process group."""
    return parameter_manager.bind_parameter_context(pg_id, pc_id)

def start_controller_services(pg_id):
    """Enable all controller services in a process group."""
    return controller_manager.enable_all_services(pg_id)

def start_pg(pg_id):
    """Start a process group."""
    return flow_manager.start_process_group(pg_id)

def get_root_pg_id():
    """Get root process group ID."""
    return flow_manager.get_root_pg_id()

def find_pg_id_by_name(name: str, parent_id: str = "root"):
    """Find process group ID by name."""
    pg_id = flow_manager.find_process_group_by_name(name, parent_id)
    if pg_id is None:
        raise ValueError(f"Process Group not found by name: {name}")
    return pg_id

def deploy_for_tenant(parent_pg_id, registry_id, bucket_id, flow_id, version, tenant_id, api_key):
    """Deploy pipeline for a tenant (legacy interface)."""
    # Use the new deployment function but maintain legacy return format
    result = deploy_jumpcloud_pipeline(tenant_id, api_key, "JumpCloudPipelineAsset", str(version))
    
    # Convert to legacy format
    return {
        "pg_id": result["pg_id"],
        "pc_id": result["pc_id"],
        "pc_name": f"JumpCloud-{tenant_id}-Context",
        "pg_name": result["pg_name"],
        "tenant_id": tenant_id,
        "controller_services": result.get("controller_services", []),
        "kafka_services_initial": result.get("kafka_services", []),
        "kafka_services_final": result.get("kafka_services", []),
        "kafka_services_enabled": result.get("kafka_services", []),
        "registry_id": registry_id,
        "flow_info": {
            "bucket_id": bucket_id,
            "flow_id": flow_id,
            "version": version
        }
    }

def check_process_group_contents(pg_id):
    """Check if process group has any content."""
    try:
        pg_info = flow_manager.get_process_group_info(pg_id)
        return {
            "has_content": pg_info.has_content,
            "running_count": pg_info.running_count,
            "stopped_count": pg_info.stopped_count,
            "total_count": pg_info.total_count
        }
    except Exception as e:
        return {"has_content": False, "error": str(e)}

def print_manual_import_guide(bucket_name, flow_name, pg_id):
    """Print guidance for manual import when automation fails."""
    from src.orchestrator.config import config
    
    content_info = check_process_group_contents(pg_id)
    
    print("\n" + "="*70)
    if content_info.get("has_content", False):
        print("✅ PROCESS GROUP CREATED WITH CONTENT")
        print("="*70)
        print(f"Process Group ID: {pg_id}")
        print(f"Running processors: {content_info.get('running_count', 0)}")
        print(f"Stopped processors: {content_info.get('stopped_count', 0)}")
        print(f"Total components: {content_info.get('total_count', 0)}")
        print(f"")
        print(f"🎉 Flow import appears to have worked!")
        print(f"   Check the NiFi UI: {config.nifi_api_url.replace('/nifi-api', '')}")
    else:
        print("🔧 MANUAL IMPORT REQUIRED")
        print("="*70)
        print(f"Process Group created but appears empty.")
        print(f"Process Group ID: {pg_id}")
        print(f"")
        print(f"📋 Manual Steps:")
        print(f"1. Open NiFi UI: {config.nifi_api_url.replace('/nifi-api', '')}")
        print(f"2. Double-click the process group: {pg_id}")
        print(f"3. Right-click in empty area → 'Version' → 'Import from registry'")
        print(f"4. Select:")
        print(f"   • Bucket: {bucket_name}")
        print(f"   • Flow: {flow_name}")
        print(f"   • Version: latest")
        print(f"5. Click 'Import'")
        print(f"6. Configure any required parameters")
        print(f"7. Enable controller services")
        print(f"8. Start the processors")
    print("="*70)

# Set up logging like original
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# Main execution (exactly like original)
if __name__ == "__main__":
    # Check NiFi version
    nifi_version = get_nifi_version()
    print(f"NiFi Version: {nifi_version}")
    
    # First, let's see what's available in the registry
    print("\n=== Available Buckets and Flows ===")
    list_buckets_and_flows()
    
    print("\n" + "="*50)
    print("Deploying JumpCloud Pipeline...")
    print("="*50)
    
    # Start Kafka consumer for integration deployments
    print("🚀 Starting Kafka consumer for automated hierarchical deployments...")
    print("   Tenant Structure: Root → Tenant → Categories → Integrations")
    print("   Categories: Asset Register, Misconfiguration, Vulnerability, Compliance")
    print("   Send Kafka messages to trigger deployments")
    print("   Or press Ctrl+C to deploy manually")
    
    # Import and start the Kafka consumer
    try:
        from simple_kafka_main import start_kafka_listener
        print("\n📡 Kafka consumer ready - waiting for deployment messages...")
        print("   Available integrations: jumpcloud, jumpcloud-events, salesforce, aws-security, custom")
        start_kafka_listener()  # This will block and listen for messages
    except KeyboardInterrupt:
        print("\n⚠️  Kafka consumer stopped - running manual deployment instead")
        
        # Manual deployment fallback with hierarchical structure
        flow_name = "JumpCloudPipelineAsset"  # Use actual flow name from registry
        print(f"\n📋 Manual deployment will create hierarchical structure:")
        print(f"   Root → Tenant-CustomerG → Asset Register → JumpCloud Asset Discovery")
        
        result = deploy_jumpcloud_pipeline(
            tenant_id="CustomerG",                  # New tenant to test hierarchical structure
            api_key="new-api-key-999",              
            flow_name=flow_name                     
        )
        
        print(f"\n🎉 Hierarchical Deployment Result:")
        print(f"   Tenant PG ID: {result.get('tenant_pg_id', 'N/A')}")
        print(f"   Category PG ID: {result.get('category_pg_id', 'N/A')}")
        print(f"   Integration PG ID: {result.get('integration_pg_id', 'N/A')}")
        print(f"   Parameter Context ID: {result.get('pc_id', 'N/A')} (shared across all tenant integrations)")
        print(f"   Integration: {result.get('integration_name', 'N/A')}")
        print(f"   Category: {result.get('category', 'N/A')}")
        print(f"   Status: {result.get('status', 'N/A')}")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        result = None
    
    if result:
        # Show import status using the integration PG ID
        integration_pg_id = result.get('integration_pg_id') or result.get('pg_id')
        if integration_pg_id:
            print_manual_import_guide("N/A", flow_name, integration_pg_id)
    
    # Provide template function
    print(f"\n💡 AUTOMATED FUNCTION READY:")
    print(f"""
def deploy_for_new_tenant():
    \"\"\"Deploy JumpCloud pipeline for a new tenant\"\"\"
    from src.orchestrator.config import config
    result = deploy_jumpcloud_pipeline(
        tenant_id="YourTenant",     
        api_key="your-api-key",     
        flow_name="{flow_name}"
    )
    
    print(f"NiFi UI: {{config.nifi_api_url.replace('/nifi-api', '')}}")
    print(f"Process Group ID: {{result['pg_id']}}")
    return result

# Call this function with your tenant details:
# deploy_for_new_tenant()
""")