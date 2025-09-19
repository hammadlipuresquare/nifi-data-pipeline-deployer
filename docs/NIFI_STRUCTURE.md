# NiFi Orchestrator Structure & Behaviors

This document describes the NiFi flow structure, parameter context strategy, and the orchestration lifecycle implemented in this repository.

## 1) High‑level Hierarchy

Tenant‑centric tree with organizational categories and versioned integrations.

```
Root (NiFi Flow)
└─ {tenant_id}                          # Tenant Process Group (PG)
   ├─ {tenant_id}-context               # Parameter Context (PC)
   └─ {category}                        # Category PG (IntegrationCategory)
      └─ {integration_name}             # Integration PG (imported from NiFi Registry)
```

- Tenant PG: container for all integrations of a tenant
- Category PG: business grouping (IntegrationCategory)
- Integration PG: root PG imported from NiFi Registry (version-controlled)
- Parameter Context: single tenant context `{tenant_id}-context` used across integrations

Relevant code
- Structure creation: `src/orchestrator/integrations.py:get_or_create_tenant_structure`
- Flow import & duplicate prevention: `src/orchestrator/nifi/flows.py:smart_import_flow_from_registry`
- Parameter context management: `src/orchestrator/nifi/params.py`
- Category constants: `src/orchestrator/parameters.py:IntegrationCategory`

## 2) Parameter Context (PC) Strategy

Check‑before‑update, no blind overwrites.

- PC name: `{tenant_id}-context`
- Desired parameters come from `INTEGRATION_PARAMETERS` plus tenant/integration secrets
- Only missing/changed parameters are upserted; identical values are no‑op
- Sensitive values never logged in plaintext

Scope of PC binding
- We only bind the PC to the imported (or existing) integration’s root PG
- No tenant‑level global binding; no global recursion
- If the root PG is already bound to the same PC, binding is skipped

Relevant code
- Diff/in‑place upsert: `ParameterManager._upsert_parameters_to_context` and helpers
- Ensuring PC (create or update): `ParameterManager.ensure_tenant_parameter_context(..., dry_run=False)`
- Conditional binding: `ParameterManager.get_process_group_bound_pc_id` + `bind_parameter_context`
- Scoped bind in deploy: `deploy_integration_hierarchical` (only integration root PG)

## 3) Flow Import & Idempotency

Import is idempotent and duplicate‑resistant:
1. By name under the category PG (`find_process_group_by_name`)
2. By version control triplet (registryId/bucketId/flowId)
3. If identified, the existing PG is reused; otherwise a new import occurs

Positions are computed for a tidy layout via `FlowManager.get_smart_position_for_type`.

## 4) Kafka Consumers (single group, three topics)

One consumer group subscribes to three topics and routes by topic:
- Deployments: `KAFKA_DEPLOYMENT_TOPIC` (default: `nifi-pipeline-deployments`)
  - Deploy/upgrade integration for a tenant
- Credentials Updates: `KAFKA_CREDENTIALS_TOPIC` (default: `nifi-credentials-updates`)
  - Fetch latest secrets from Vault and upsert into the tenant PC (no binding)
- Deletions: `KAFKA_DELETIONS_TOPIC` (default: `nifi-integration-deletions`)
  - Resolve category via `INTEGRATION_CATEGORIES`, locate integration root PG, and stop it

Topic auto‑creation
- If a topic is missing, the worker attempts to create it via Kafka AdminClient

Handlers
- `KafkaWorker._process_deployment_message`
- `KafkaWorker._process_credentials_update`
- `KafkaWorker._process_deletion_message`

Config: `src/orchestrator/config.py` (bootstrap servers, topics, consumer group, backoff)

## 5) Secrets/Vault

- `HashiCorpVault` singleton fetches secrets per tenant/integration
- Secrets are mapped to integration parameters via `INTEGRATION_PARAMETERS`
- All sensitive values are masked in logs

## 6) API Endpoints used (NiFi)

- Process Groups: `GET process-groups/{id}`, `GET process-groups/{id}/process-groups`, `PUT process-groups/{id}`
- Versioned Import: `POST process-groups/{parentId}/process-groups?parameterContextHandlingStrategy=KEEP_EXISTING`
- Parameter Contexts: `GET flow/parameter-contexts`, `POST parameter-contexts`, `POST parameter-contexts/{id}/update-requests`

## 7) Concurrency & Idempotency Guards

- Tenant structure creation is serialized with a best‑effort file lock (`tenant_lock`)
- Flow import/bind operations are scoped to the integration root PG
- PC upserts are diff‑based and safe to repeat

## 8) Deletion Behavior

- Category is resolved via `INTEGRATION_CATEGORIES` mapping
- Integration PG is found by name under the tenant’s category PG
- The integration root PG is stopped (no deletion by default)

## 9) Dry‑Run Support

- PC update supports `dry_run=True` to log planned parameter changes without applying

## 10) Error Handling & Resilience

- Unknown topics trigger AdminClient topic creation
- Errors per message are logged; consumer commits only on success (if auto‑commit disabled)
- Backoff knobs are provided in config (`KAFKA_BACKOFF_BASE_MS`, `KAFKA_BACKOFF_MAX_MS`)

## 11) Message Formats (ingress)

Deployments (topic: deployments)
```json
{
  "tenant_id": "PureVPN",
  "integration": "aws",
  "version": "latest"
}
```

Credentials updates (topic: credentials)
```json
{
  "tenant_id": "PureVPN",
  "integration": "jumpcloud"
}
```

Deletions (topic: deletions)
```json
{
  "tenant_id": "PureVPN",
  "integration": "aws",
  "integration_name": "AWS Asset Registry"
}
```

## 12) Key Entry Points

- Orchestrator app: `src/orchestrator/app.py`
- Kafka runner: `src/orchestrator/consumer/kafka_runner.py`
- PC manager: `src/orchestrator/nifi/params.py`
- Flow manager: `src/orchestrator/nifi/flows.py`
- Integration deploy: `src/orchestrator/integrations.py`
- Parameters registry: `src/orchestrator/parameters.py`
