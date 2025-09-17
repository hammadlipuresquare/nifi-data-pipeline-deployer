#!/usr/bin/env python3
"""
Debug script for testing integration deployment.
Use this for step-by-step debugging of integration functions.
"""

from src.orchestrator.integrations import (
    deploy_jumpcloud_pipeline,
    deploy_jumpcloud_misconfiguration_pipeline,
    get_nifi_version,
    list_buckets_and_flows
)

def debug_integration_deployment():
    """Debug integration deployment step by step."""
    
    # Step 1: Check NiFi connectivity
    print("🔍 Step 1: Checking NiFi connectivity...")
    version = get_nifi_version()
    print(f"NiFi Version: {version}")
    
    # Step 2: List available flows
    print("\n🔍 Step 2: Listing available flows...")
    list_buckets_and_flows()
    
    # Step 3: Deploy JumpCloud Asset (Identity & Access Review)
    print("\n🔍 Step 3: Deploying JumpCloud Asset Discovery...")
    result1 = deploy_jumpcloud_pipeline(
        tenant_id="DebugTenant",
        api_key="debug-api-key-123",
        flow_name="JumpCloudPipelineAsset"
    )
    
    print(f"Result 1: {result1}")
    
    # Step 4: Deploy JumpCloud Events (same category)
    print("\n🔍 Step 4: Deploying JumpCloud Events Monitor...")
    result2 = deploy_jumpcloud_misconfiguration_pipeline(
        tenant_id="DebugTenant",
        api_key="debug-api-key-123",
        flow_name="jumpcloud-events"
    )
    
    print(f"Result 2: {result2}")
    
    # Step 5: Verify hierarchy
    print("\n🔍 Step 5: Verifying hierarchical structure...")
    print(f"Same tenant PG: {result1.get('tenant_pg_id') == result2.get('tenant_pg_id')}")
    print(f"Same category PG: {result1.get('category_pg_id') == result2.get('category_pg_id')}")
    print(f"Both in Identity & Access Review: {result1.get('category') == result2.get('category')}")
    
    return result1, result2

if __name__ == "__main__":
    # Set breakpoints here for debugging
    debug_integration_deployment()


