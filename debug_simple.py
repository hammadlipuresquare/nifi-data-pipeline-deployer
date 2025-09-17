#!/usr/bin/env python3
"""
Simple debug script for PyCharm - No Kafka required!
Perfect for testing integration logic without external dependencies.
"""

import sys
import os

# Add project root to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

def debug_without_kafka():
    """Debug integration logic without Kafka dependencies."""
    
    print("🐛 Simple Debug Mode - No Kafka Required")
    print("="*50)
    
    try:
        # Test 1: Configuration loading
        print("🔧 Step 1: Testing configuration...")
        from src.orchestrator.config import config
        print(f"✅ NiFi API: {config.nifi_api_url}")
        print(f"✅ NiFi Registry: {config.nifi_registry_url}")
        
        # Test 2: Integration imports
        print("\n🔧 Step 2: Testing integration imports...")
        from src.orchestrator.integrations import (
            get_available_integrations,
            get_integrations_by_category,
            IntegrationCategory
        )
        
        integrations = get_available_integrations()
        categories = get_integrations_by_category()
        
        print(f"✅ Available integrations: {integrations}")
        print("✅ Categories:")
        for category, integ_list in categories.items():
            print(f"   • {category}: {integ_list}")
        
        # Test 3: Category validation
        print("\n🔧 Step 3: Testing category structure...")
        all_categories = IntegrationCategory.get_all_categories()
        print(f"✅ All categories: {all_categories}")
        
        # Test 4: NiFi version check (if NiFi is running)
        print("\n🔧 Step 4: Testing NiFi connectivity...")
        from src.orchestrator.integrations import get_nifi_version
        version = get_nifi_version()
        print(f"✅ NiFi Version: {version}")
        
        # Test 5: Flow listing (if NiFi Registry is running)
        print("\n🔧 Step 5: Testing NiFi Registry...")
        try:
            from src.orchestrator.integrations import list_buckets_and_flows
            print("✅ Available flows:")
            list_buckets_and_flows()
        except Exception as e:
            print(f"⚠️  Registry test failed (expected if NiFi not running): {e}")
        
        print("\n🎉 All basic tests passed!")
        print("🐛 Perfect for PyCharm debugging - set breakpoints anywhere!")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Debug test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def debug_single_integration():
    """Debug a single integration deployment (requires NiFi running)."""
    
    print("\n🚀 Testing Single Integration Deployment")
    print("="*50)
    
    try:
        from src.orchestrator.integrations import deploy_jumpcloud_pipeline
        
        # This will test the full hierarchical deployment
        result = deploy_jumpcloud_pipeline(
            tenant_id="DebugTenant",
            api_key="debug-api-key-123",
            flow_name="JumpCloudPipelineAsset"
        )
        
        print(f"✅ Deployment successful!")
        print(f"   Tenant PG: {result.get('tenant_pg_id')}")
        print(f"   Category: {result.get('category')}")
        print(f"   Integration: {result.get('integration_name')}")
        
        return result
        
    except Exception as e:
        print(f"⚠️  Integration test failed (expected if NiFi not running): {e}")
        return None

if __name__ == "__main__":
    print("🐛 PyCharm Simple Debug Script")
    print("Set breakpoints and step through the code!")
    print()
    
    # Run basic tests (no external dependencies)
    basic_success = debug_without_kafka()
    
    # Ask user if they want to test integration (requires NiFi)
    if basic_success:
        print("\n" + "="*60)
        print("🚀 Want to test integration deployment?")
        print("   (Requires NiFi and Registry running)")
        print("   Uncomment the line below and run again:")
        print("   # debug_single_integration()")
        
        # Uncomment this line to test actual deployment:
        # debug_single_integration()


