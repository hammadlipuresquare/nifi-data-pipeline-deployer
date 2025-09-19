# Orchestrator – End‑to‑End Documentation

This document explains how the orchestrator works from end to end: setup, configuration, runtime flows, NiFi structure, Kafka integration, Vault/parameters, idempotency, and how to extend the system.


## 0. Quick Start

- Python 3.13 virtualenv activated
- Kafka running and reachable (e.g., `localhost:9093`)
- NiFi + NiFi Registry running and reachable
- Environment variables set (see Configuration)
- Start: `python -m src.orchestrator.app`

The app starts a single consumer group subscribing to three topics (deployments, credentials updates, deletions) and writes responses to a response topic.


## 1. Repository Structure (selected)

```
src/orchestrator/
  app.py                         # Main entrypoint (bootstraps Kafka worker)
  config.py                      # Configuration via pydantic-settings
  consumer/kafka_runner.py       # Single runner consuming 3 topics
  integrations.py                # Deploy logic & helpers
  nifi/
    client.py                    # HTTP client for NiFi
    flows.py                     # NiFi flow/process-group operations
    params.py                    # Parameter context management
  parameters.py                  # Integration parameter specs & categories
  utils/
    idempotency.py               # Tenant/flow locks, idempotency helpers
    integration_category.py      # Category constants/mapping
  vault/
    hashicorpvault.py            # Vault singleton

docs/
  NIFI_STRUCTURE.md              # NiFi structure reference
  ARCHITECTURE.md                # This document
```


## 2. Configuration

Managed via `src/orchestrator/config.py` (pydantic BaseSettings). Key variables:

- NiFi
  - `NIFI_API_URL` (default `https://localhost:8443/nifi-api`)
  - `NIFI_REGISTRY_URL`
  - `NIFI_USERNAME`, `NIFI_PASSWORD`, `NIFI_SSL_VERIFY`
- Kafka (orchestrator runtime)
  - `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9093`)
  - `KAFKA_DEPLOYMENT_TOPIC` (default `nifi-pipeline-deployments`)
  - `KAFKA_CREDENTIALS_TOPIC` (default `nifi-credentials-updates`)
  - `KAFKA_DELETIONS_TOPIC` (default `nifi-integration-deletions`)
  - `KAFKA_RESPONSE_TOPIC` (default `nifi-pipeline-responses`)
  - `KAFKA_CONSUMER_GROUP` (one group for all 3 topics)
  - `KAFKA_ENABLE_AUTO_COMMIT` (default False), `KAFKA_AUTO_COMMIT_INTERVAL_MS`
  - `KAFKA_SESSION_TIMEOUT_MS`, `KAFKA_MAX_POLL_INTERVAL_MS`
  - `KAFKA_BACKOFF_BASE_MS` (default 500), `KAFKA_BACKOFF_MAX_MS` (default 60000)
- Secrets (Vault)
  - `VAULT_URL`, `VAULT_TOKEN` (or approle), `VAULT_MOUNT_POINT`, etc.
- Logging
  - `LOG_LEVEL` (default INFO), `LOG_FORMAT` (json/plain)


## 3. NiFi Structure & Strategy (summary)

See `docs/NIFI_STRUCTURE.md` for diagrams. In short:

- Tenant PG: `{tenant_id}` under root
- Category PG: organizational grouping (`IntegrationCategory`)
- Integration PG: versioned flow from Registry
- Parameter Context: `{tenant_id}-context`

Parameter Context policy:
- Check‑before‑update; only missing/changed values are upserted; sensitive values masked in logs
- Scoped binding only on integration root PG (descendants inherit); skip if already bound to same PC


## 4. Runtime Flows (Kafka)

Single consumer group; one runner subscribes to 3 topics and routes by topic. Missing topics are auto‑created via AdminClient.

- Deployments (`KAFKA_DEPLOYMENT_TOPIC`)
  - Payload: `{tenant_id, integration, version?}`
  - Flow:
    1. Ensure tenant/category structure (`integrations.get_or_create_tenant_structure`)
    2. Ensure PC values (check‑before‑update) – no binding here
    3. Find or import integration PG (duplicate prevention by name and by version‑control IDs)
    4. Scoped bind PC to root PG only if different
    5. Configure/enable controller services if required
    6. Start integration PG

- Credentials Updates (`KAFKA_CREDENTIALS_TOPIC`)
  - Payload: `{tenant_id, integration}`
  - Flow:
    1. Fetch latest secrets from Vault
    2. Upsert parameter values into `{tenant_id}-context` (check‑before‑update) – no binding

- Deletions (`KAFKA_DELETIONS_TOPIC`)
  - Payload: `{tenant_id, integration, integration_name?}`
  - Flow:
    1. Resolve category via `INTEGRATION_CATEGORIES`
    2. Find integration root PG under the tenant’s category
    3. Stop integration root PG (default is stop; deletion optional)

Responses: deployment outcomes are written to `KAFKA_RESPONSE_TOPIC`.


## 5. Deploy Logic (details)

`src/orchestrator/integrations.py`:

- `get_or_create_tenant_structure(tenant_id, category, integration_type, additional_params)`
  - Runs under `tenant_lock(tenant_id)` to prevent duplicates across processes
  - Ensures tenant PG exists
  - Ensures/upserts parameter context values (no binding)
  - Ensures category PG exists
  - Returns `{tenant_pg_id, parameter_context_id, category_pg_id}`

- `deploy_integration_hierarchical(...)`
  - Finds/Imports the integration flow with position & duplicate prevention (`flows.smart_import_flow_from_registry`)
  - Reads current bound PC from root PG; binds only if different
  - Starts flow; returns metadata


## 6. NiFi API Usage

- Process Groups
  - `GET process-groups/{id}`
  - `GET process-groups/{id}/process-groups`
  - `PUT process-groups/{id}` (binding PC via component update)
- Import from Registry (versioned):
  - `POST process-groups/{parentId}/process-groups?parameterContextHandlingStrategy=KEEP_EXISTING`
- Parameter Contexts
  - `GET flow/parameter-contexts` (listing)
  - `POST parameter-contexts` (create)
  - `POST parameter-contexts/{id}/update-requests` (in‑place add/update values)


## 7. Parameter Contexts (deep‑dive)

`src/orchestrator/nifi/params.py` implements:

- `ensure_tenant_parameter_context(tenant_id, integration_type, additional_params, dry_run=False)`
  - Builds desired params from `INTEGRATION_PARAMETERS[integration_type]` + secrets (maps `source` keys)
  - If PC exists: `_upsert_parameters_to_context` computes deltas (add or update) and applies
  - If not: creates PC with desired set
- `get_process_group_bound_pc_id(pg_id)` reads currently bound PC id
- `bind_parameter_context(pg_id, pc_id)` updates PG component to refer to the PC


## 8. Vault Integration

`src/orchestrator/vault/hashicorpvault.py` supplies a thread‑safe singleton client. Helpers in `integrations.py` fetch per‑tenant integration secrets and map them to parameter sources defined in `INTEGRATION_PARAMETERS`.


## 9. Idempotency & Concurrency

- `utils/idempotency.py`:
  - `tenant_lock(tenant_id)`: best‑effort cross‑process file lock to serialize tenant structure changes
  - `flow_lock(tenant, category, flow)`: optional per‑flow lock (available for future use)
- Steps are safe to repeat; PC upserts are diff‑based; binder skips if unchanged; flow import prevents duplicates by name + version‑control.


## 10. Extending with a New Integration

1. Add parameter spec to `INTEGRATION_PARAMETERS` in `parameters.py` (set `source` keys; mark `sensitive` where needed)
2. Add integration→category mapping in `INTEGRATION_CATEGORIES`
3. Add a deploy function in `integrations.py` calling `deploy_integration_hierarchical`
4. Add a case in the Kafka deployment handler
5. Provision Vault secrets using the same source keys


## 11. Message Formats

- Deployments
```json
{
  "tenant_id": "YourTenant",
  "integration": "aws",
  "version": "latest"
}
```

- Credentials updates
```json
{
  "tenant_id": "YourTenant",
  "integration": "jumpcloud"
}
```

- Deletions
```json
{
  "tenant_id": "YourTenant",
  "integration": "aws",
  "integration_name": "AWS Asset Registry"
}
```


## 12. Error Handling & Resilience

- Unknown topic errors trigger topic creation via Kafka AdminClient (idempotent)
- If `enable.auto.commit` is False, commits happen only on success
- Backoff knobs available; jitter/exponential patterns can be added in the runner


## 13. Troubleshooting

- “Not consuming messages”: verify `KAFKA_BOOTSTRAP_SERVERS` and topic names; ensure single consumer group for ordering; check UI offsets
- “Flow not found”: verify NiFi Registry contains the flow (`flows.smart_import_flow_from_registry` logs registry/bucket/flow IDs)
- “Only TENANT_ID in PC”: verify `integration` matches a key in `INTEGRATION_PARAMETERS`; ensure Vault keys match `source` names
- “Parameter contexts list 405”: we use `GET flow/parameter-contexts` for compatibility


## 14. Security Notes

- Sensitive values never printed in logs
- Vault client is singleton; keep tokens out of logs; consider short TTL and approle where possible
- NiFi credentials should be least-privileged for PG/PC ops


## 15. Key Files Index

- `src/orchestrator/app.py`: app lifecycle, bootstrap, health checks
- `src/orchestrator/consumer/kafka_runner.py`: unified consumer for 3 topics; topic auto‑create
- `src/orchestrator/integrations.py`: structure, deploy paths, scoped binding, helpers
- `src/orchestrator/nifi/flows.py`: PG discovery, versioned import, start/stop, duplicate prevention
- `src/orchestrator/nifi/params.py`: PC ensure/upsert, bind, helpers
- `src/orchestrator/parameters.py`: parameter registry + category constants
- `src/orchestrator/vault/hashicorpvault.py`: Vault singleton and helpers
- `src/orchestrator/utils/idempotency.py`: locks and idempotency helpers

—
For a quick structural summary, see `docs/NIFI_STRUCTURE.md`.
