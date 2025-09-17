# Orchestrator: Simple NiFi Pipeline Deployment

A clean, modular system for automated deployment of Apache NiFi pipelines via Kafka messaging or direct API calls. Designed for easy extensibility - adding new integrations is just adding a function!

## Architecture

- **Simple & Clean**: Function-based integration handlers, no complex classes
- **Multi-tenant**: Isolated parameter contexts and process groups per tenant  
- **Extensible**: Adding new integrations takes 5 minutes
- **Multiple Interfaces**: Kafka messaging or direct Python calls
- **Backward Compatible**: Original `main.py` and `simple_kafka_main.py` still work

## Project Structure

```
src/orchestrator/
├── app.py                 # Main Kafka orchestrator application  
├── config.py              # Configuration management
├── logging.py             # Structured logging
├── exceptions.py          # Error handling
├── integrations.py        # 🎯 MAIN FILE - All integration functions
├── consumer/
│   └── kafka_runner.py    # Kafka consumer with switch-case routing
├── nifi/
│   ├── client.py          # HTTP client with retries
│   ├── flows.py           # Process group operations
│   ├── params.py          # Parameter context management
│   ├── controllers.py     # Controller service operations
│   └── registry.py        # NiFi Registry operations
└── utils/
    ├── retry.py           # Retry policies
    └── idempotency.py     # Get-or-create utilities

# Root level files (preserved original behavior)
main.py                    # Original interface - works exactly the same
simple_kafka_main.py       # Original Kafka interface - simplified
```

## 🚀 Adding New Integrations (Super Easy!)

Just add a function to `src/orchestrator/integrations.py`:

```python
def deploy_slack_pipeline(tenant_id: str, api_key: str, flow_name: str = "SlackPipeline",
                         version: str = "latest", parent_pg_id: str = None) -> Dict[str, Any]:
    """Deploy Slack pipeline - just copy the JumpCloud pattern!"""
    # Same pattern as JumpCloud but with Slack-specific flow
    flow_info = registry_manager.find_flow_by_name(flow_name)
    # ... rest is identical to JumpCloud function
    
# Add to registry
INTEGRATION_REGISTRY = {
    "jumpcloud": deploy_jumpcloud_pipeline,
    "salesforce": deploy_salesforce_pipeline, 
    "slack": deploy_slack_pipeline,  # ← Just add this line!
}
```

Then update the Kafka switch case in `kafka_runner.py`:
```python
elif integration == "slack":
    result = deploy_integration("slack", tenant_id, api_key, flow_name, version)
```

That's it! 🎉

## Quick Start

### 1. Installation

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install package and dependencies
pip install -e .
pip install -e ".[test,dev]"
```

### 2. Configuration

Copy and customize the environment configuration:

```bash
# Create .env file from template (if .env.example exists)
cp .env.example .env

# Edit configuration
editor .env
```

Required environment variables:
```bash
# NiFi Configuration
NIFI_API_URL=https://localhost:8443/nifi-api
NIFI_USERNAME=admin
NIFI_PASSWORD=your-password

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9093
KAFKA_DEPLOYMENT_TOPIC=nifi-pipeline-deployments
KAFKA_RESPONSE_TOPIC=nifi-pipeline-responses

# NiFi Pipeline Kafka (for deployed flows)
NIFI_KAFKA_BOOTSTRAP_SERVERS=kafka.apache:9092
NIFI_KAFKA_SASL_PASSWORD=your-kafka-password
```

### 3. Usage Options

#### Option A: Direct Python (like original main.py)
```bash
# Use original main.py (works exactly as before)
python main.py

# Or deploy directly in Python
python -c "
from src.orchestrator.integrations import deploy_jumpcloud_pipeline
result = deploy_jumpcloud_pipeline('CustomerA', 'your-api-key')
print(result)
"
```

#### Option B: Kafka Messaging
```bash
# Start Kafka listener (like original simple_kafka_main.py)
python simple_kafka_main.py

# Or use new orchestrator
python -m src.orchestrator.app
```

#### Option C: Demo Mode
```bash
# Run demo deployment
python -m src.orchestrator.app --demo
```

### 3. Send Kafka Messages

Both new and legacy message formats supported:

**New Format:**
```json
{
  "tenant_id": "test-customer",
  "integration": "jumpcloud",
  "parameters": {"api_key": "your-api-key"},
  "flow_name": "JumpCloudPipeline",
  "version": "latest"
}
```

**Legacy Format (still works):**
```json
{
  "tenant_id": "test-customer", 
  "pipeline": "JumpCloud",
  "parameters": {"api_key": "your-api-key"}
}
```

## Development

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src/orchestrator

# Run specific test files
pytest tests/test_router.py
pytest tests/test_nifi_flows.py
```

### Code Quality

```bash
# Format code
ruff format .

# Lint code
ruff check .

# Type checking (if mypy is installed)
mypy src/orchestrator
```

### Adding New Integrations

1. Create integration module in `src/orchestrator/services/{integration}/`
2. Implement handlers extending `AbstractService`
3. Register services in `services/registry.py`
4. Add tests in `tests/test_services_{integration}.py`

Example service handler:

```python
from ..base import AbstractService

class MyCapabilityService(AbstractService):
    def get_integration_name(self) -> str:
        return "my_integration"
    
    def get_supported_capabilities(self) -> list[str]:
        return ["my_capability"]
    
    def deploy_capability(self, capability: str, message: DeploymentMessage, 
                         secrets: Dict[str, Any]) -> Dict[str, Any]:
        # Implementation using nifi.flows, nifi.params, etc.
        return {"status": "deployed"}
```

## Message Formats

### Current Format
```json
{
  "tenant_id": "customer-123",
  "integration": "jumpcloud",
  "parameters": {
    "api_key": "api-key-value",
    "additional_params": {}
  },
  "capabilities": {
    "asset_register": true,
    "case_management": false
  },
  "message_id": "optional-tracking-id"
}
```

### Legacy Format (Supported)
```json
{
  "tenant_id": "customer-123",
  "pipeline": "JumpCloud",
  "parameters": {
    "api_key": "api-key-value"
  },
  "message_id": "optional-tracking-id"
}
```

## Response Messages

Success response:
```json
{
  "success": true,
  "tenant_id": "customer-123",
  "integration": "jumpcloud",
  "capabilities": ["asset_register"],
  "timestamp": 1694123456.789,
  "result": {
    "deployments": [...],
    "processing_time_seconds": 45.2
  }
}
```

Error response:
```json
{
  "success": false,
  "tenant_id": "customer-123",
  "integration": "jumpcloud",
  "capabilities": ["asset_register"],
  "timestamp": 1694123456.789,
  "error": "Deployment failed: Flow not found",
  "details": {
    "error_type": "NiFiRegistryError"
  }
}
```

## Operational Features

### Logging
- Structured JSON logging with correlation IDs
- Sensitive data masking (passwords, API keys)
- Configurable log levels and formats

### Error Handling
- Centralized exception taxonomy
- Retry policies with exponential backoff
- Graceful degradation and error boundaries

### Monitoring
- Message processing metrics in logs
- Deployment success/failure tracking
- Framework for external monitoring integration

### Security
- Multi-tenant isolation via parameter contexts
- Sensitive secret management
- SSL/TLS support for NiFi connections

## Troubleshooting

### Common Issues

1. **NiFi Authentication Failed**
   - Verify `NIFI_USERNAME` and `NIFI_PASSWORD`
   - Check NiFi is configured for username/password auth
   - Ensure NiFi is accessible at `NIFI_API_URL`

2. **Kafka Connection Issues**
   - Verify `KAFKA_BOOTSTRAP_SERVERS` is correct
   - Check network connectivity to Kafka brokers
   - Ensure topics exist or have auto-creation enabled

3. **Flow Not Found**
   - Verify flow exists in NiFi Registry
   - Check flow name matches exactly (case-sensitive)
   - Ensure registry is connected to NiFi

4. **Permission Errors**
   - Check NiFi user has appropriate permissions
   - Verify Kafka consumer has topic access
   - Ensure parameter context creation permissions

### Debug Mode

Enable debug logging:
```bash
LOG_LEVEL=DEBUG python -m src.orchestrator.app
```

### Health Checks

The application logs startup status and configuration validation. Monitor logs for:
- "Application bootstrap completed" - successful startup
- "Kafka worker ready for messages" - consumer is active
- "Services registered successfully" - handlers loaded

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass and code is formatted
5. Submit a pull request

## License

[Add your license information here]
