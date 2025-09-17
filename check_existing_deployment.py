#!/usr/bin/env python3
"""
Script to check parameter context assignment for existing deployments.
This helps identify if the issue is with old vs new deployments.
"""

from src.orchestrator.nifi.flows import flow_manager
from src.orchestrator.nifi.params import parameter_manager


def check_deployment_parameter_contexts(tenant_id: str):
    """Check parameter context assignments for a specific tenant deployment."""
    
    print(f"🔍 CHECKING PARAMETER CONTEXT ASSIGNMENT FOR TENANT: {tenant_id}")
    print("=" * 60)
    print()
    
    try:
        # Find the tenant process group
        tenant_pg_id = flow_manager.find_process_group_by_name(tenant_id, "root")
        if not tenant_pg_id:
            print(f"❌ Tenant process group '{tenant_id}' not found")
            return
        
        print(f"✅ Found tenant process group: {tenant_pg_id}")
        
        # Get tenant's parameter context
        tenant_pg = flow_manager.get_process_group(tenant_pg_id)
        if tenant_pg:
            tenant_component = tenant_pg.get("component", {})
            tenant_pc = tenant_component.get("parameterContext", {})
            tenant_pc_id = tenant_pc.get("id") if tenant_pc else None
            tenant_pc_name = tenant_pc.get("component", {}).get("name", "Unknown") if tenant_pc else "None"
            
            print(f"📋 Tenant parameter context: {tenant_pc_name} ({tenant_pc_id})")
            print()
        else:
            print(f"❌ Could not retrieve tenant process group details")
            return
        
        # Find AWS Asset Registry integration
        aws_pg_id = None
        
        # Look for Asset Register category
        asset_register_pg_id = flow_manager.find_process_group_by_name("Asset Register", tenant_pg_id)
        if asset_register_pg_id:
            print(f"✅ Found Asset Register category: {asset_register_pg_id}")
            
            # Look for AWS Asset Registry integration
            aws_pg_id = flow_manager.find_process_group_by_name("AWS Asset Registry", asset_register_pg_id)
            if aws_pg_id:
                print(f"✅ Found AWS Asset Registry integration: {aws_pg_id}")
            else:
                print(f"❌ AWS Asset Registry integration not found in Asset Register category")
                return
        else:
            print(f"❌ Asset Register category not found")
            return
        
        print()
        
        # Analyze all process groups in the AWS integration
        print("🔍 ANALYZING ALL PROCESS GROUPS IN AWS INTEGRATION:")
        print("-" * 50)
        
        analysis_results = parameter_manager._comprehensive_process_group_analysis(aws_pg_id)
        all_pgs = analysis_results.get("all_process_groups", [])
        
        print(f"📊 Total process groups found: {len(all_pgs)}")
        print(f"📊 Structure: {analysis_results.get('structure_summary', 'N/A')}")
        print()
        
        # Check parameter context for each process group
        print("📋 PARAMETER CONTEXT CHECK FOR EACH PROCESS GROUP:")
        print("-" * 50)
        
        correctly_assigned = 0
        incorrectly_assigned = 0
        no_parameter_context = 0
        
        for pg_info in all_pgs:
            pg_id = pg_info["id"]
            pg_name = pg_info["name"]
            depth = pg_info["depth"]
            
            try:
                pg_data = flow_manager.get_process_group(pg_id)
                if pg_data:
                    component = pg_data.get("component", {})
                    pc = component.get("parameterContext", {})
                    pc_id = pc.get("id") if pc else None
                    
                    indent = "   " + "  " * depth
                    
                    if pc_id == tenant_pc_id:
                        print(f"{indent}✅ {pg_name}: CORRECT parameter context")
                        correctly_assigned += 1
                    elif pc_id:
                        print(f"{indent}❌ {pg_name}: WRONG parameter context ({pc_id})")
                        incorrectly_assigned += 1
                    else:
                        print(f"{indent}⚠️  {pg_name}: NO parameter context")
                        no_parameter_context += 1
                else:
                    print(f"{indent}💥 {pg_name}: Could not retrieve process group data")
                    
            except Exception as e:
                indent = "   " + "  " * depth
                print(f"{indent}💥 {pg_name}: Error checking parameter context - {e}")
        
        print()
        print("🎯 SUMMARY:")
        print("-" * 20)
        print(f"   ✅ Correctly assigned: {correctly_assigned}")
        print(f"   ❌ Incorrectly assigned: {incorrectly_assigned}")
        print(f"   ⚠️  No parameter context: {no_parameter_context}")
        print(f"   📊 Total checked: {len(all_pgs)}")
        
        success_rate = (correctly_assigned / len(all_pgs) * 100) if all_pgs else 0
        print(f"   📈 Success rate: {success_rate:.1f}%")
        print()
        
        if success_rate < 100:
            print("💡 RECOMMENDATIONS:")
            print("   1. This deployment may have been created before the fix")
            print("   2. Test with a completely NEW tenant ID via Kafka")
            print("   3. Or run parameter context fix on this existing deployment:")
            print(f"      ensure_tenant_parameter_context_inheritance('{tenant_id}')")
            print()
        else:
            print("🎉 ALL PROCESS GROUPS HAVE CORRECT PARAMETER CONTEXT!")
            print("   This deployment is working perfectly!")
        
    except Exception as e:
        print(f"❌ Error checking deployment: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 2:
        print("Usage: python check_existing_deployment.py <tenant_id>")
        print("Example: python check_existing_deployment.py MyTenant")
        sys.exit(1)
    
    tenant_id = sys.argv[1]
    check_deployment_parameter_contexts(tenant_id)


