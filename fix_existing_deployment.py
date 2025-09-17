#!/usr/bin/env python3
"""
Script to fix parameter context assignment for existing deployments.
This applies the comprehensive parameter context assignment to deployments 
that were created before the fix.
"""

from src.orchestrator.integrations import ensure_tenant_parameter_context_inheritance


def fix_deployment_parameter_contexts(tenant_id: str, stop_running_components: bool = False):
    """Fix parameter context assignments for a specific tenant deployment."""
    
    print(f"🔧 FIXING PARAMETER CONTEXT ASSIGNMENT FOR TENANT: {tenant_id}")
    print("=" * 60)
    print()
    
    if stop_running_components:
        print("⚠️  WARNING: stop_running_components=True")
        print("   This will attempt to stop running components before assignment")
        print("   Use with caution as it may interrupt running data flows")
        print()
    
    try:
        # Run the comprehensive parameter context inheritance fix
        results = ensure_tenant_parameter_context_inheritance(
            tenant_id=tenant_id, 
            stop_running_components=stop_running_components
        )
        
        if "error" in results:
            print(f"❌ Fix failed: {results['error']}")
            return
        
        print(f"🎯 PARAMETER CONTEXT FIX RESULTS:")
        print(f"   Tenant: {results.get('tenant_id', 'N/A')}")
        print(f"   Parameter Context: {results.get('tenant_parameter_context', {}).get('name', 'N/A')}")
        print(f"   Total Processed: {results.get('total_processed', 0)}")
        print(f"   ✅ Successfully Assigned: {results.get('total_assigned', 0)}")
        print(f"   ⏭️  Skipped (already correct): {results.get('total_skipped', 0)}")
        print(f"   ❌ Failed (running components): {results.get('total_errors', 0)}")
        print()
        
        # Show assignment details
        assignments = results.get('assignments', [])
        if assignments:
            print(f"✅ SUCCESSFUL ASSIGNMENTS ({len(assignments)}):")
            for assignment in assignments[:10]:  # Show first 10
                print(f"   🔗 {assignment.get('name', 'Unknown')}")
            if len(assignments) > 10:
                print(f"   ... and {len(assignments) - 10} more")
            print()
        
        # Show skipped details
        skipped = results.get('skipped', [])
        if skipped:
            print(f"⏭️  SKIPPED (already correct) ({len(skipped)}):")
            for skip in skipped[:5]:  # Show first 5
                print(f"   ✅ {skip.get('name', 'Unknown')}")
            if len(skipped) > 5:
                print(f"   ... and {len(skipped) - 5} more")
            print()
        
        # Show errors
        errors = results.get('errors', [])
        if errors:
            print(f"❌ FAILED ASSIGNMENTS ({len(errors)}):")
            print("   These failed due to running components (normal behavior):")
            for error in errors[:5]:  # Show first 5
                print(f"   ⚠️  {error.get('name', 'Unknown')}: {error.get('error', '')[:50]}...")
            if len(errors) > 5:
                print(f"   ... and {len(errors) - 5} more")
            print()
            print("💡 To fix these, you can:")
            print("   1. Stop the process groups manually in NiFi UI")
            print("   2. Run this script again with --stop-components")
            print("   3. Or accept that new deployments will inherit correctly")
        
        # Calculate success rate
        total_attempted = results.get('total_processed', 0)
        total_successful = results.get('total_assigned', 0) + results.get('total_skipped', 0)
        
        if total_attempted > 0:
            success_rate = (total_successful / total_attempted * 100)
            print(f"📊 Overall Success Rate: {success_rate:.1f}%")
            
            if success_rate >= 80:
                print("🎉 Parameter context fix largely successful!")
                print("   Most process groups now have correct parameter context")
            elif success_rate >= 50:
                print("⚠️  Partial success - some process groups still need attention")
            else:
                print("❌ Low success rate - manual intervention may be needed")
        
        print()
        print("🔍 NEXT STEPS:")
        print("   1. Check the fixed deployment with:")
        print(f"      python check_existing_deployment.py {tenant_id}")
        print("   2. Verify in NiFi UI that sub-process groups have parameter context")
        print("   3. For any remaining issues, consider stopping process groups first")
        
    except Exception as e:
        print(f"❌ Error during fix: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    import sys
    import argparse
    
    parser = argparse.ArgumentParser(description="Fix parameter context assignment for existing deployments")
    parser.add_argument("tenant_id", help="Tenant ID to fix")
    parser.add_argument("--stop-components", action="store_true", 
                       help="Attempt to stop running components before assignment (use with caution)")
    
    args = parser.parse_args()
    
    fix_deployment_parameter_contexts(args.tenant_id, args.stop_components)


