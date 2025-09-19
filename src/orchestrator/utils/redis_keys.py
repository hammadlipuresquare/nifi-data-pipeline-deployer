from __future__ import annotations

from ..config import config


def _prefix() -> str:
    env = getattr(config, "orchestrator_env", "dev")
    cluster = getattr(config, "orchestrator_cluster", "local")
    return f"nifi:{env}:{cluster}"


def tenant_hash_key() -> str:
    """Key for tenant hash storing tenant_pg_id and pc_id by tenant id (field)."""
    return f"{_prefix()}:tenant"


def tenant_categories_key(tenant_id: str) -> str:
    """Key for JSON blob mapping category name -> category_pg_id for a tenant."""
    return f"{_prefix()}:categories:tenant:{tenant_id}"


def tenant_integration_hash_key(tenant_id: str, integration: str) -> str:
    """Key for hash storing per-flow integration deployment info for a tenant/integration type.

    Each field in this hash should be the flow display name.
    """
    return f"{_prefix()}:tenant:{tenant_id}:integration:{integration}"

