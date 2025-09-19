from dataclasses import dataclass


@dataclass
class IntegrationCategory:
    """Defines integration categories for organizational grouping."""
    ASSET_REGISTER = "Asset Register"
    MISCONFIGURATION = "Misconfiguration"
    VULNERABILITY = "Vulnerability"
    COMPLIANCE = "Compliance"
    CASE_MANAGEMENT = "Case Management"
    IDENTITY_AND_ACCESS_REVIEW = "Identity & Access Review"


# Category mapping for integrations (used by deletion and routing logic) - reuse constants
INTEGRATION_CATEGORIES = {
    "jumpcloud": IntegrationCategory.IDENTITY_AND_ACCESS_REVIEW,
    "aws": IntegrationCategory.ASSET_REGISTER,
}

INTEGRATION_NIFI_FLOW_NAMES = {
    "jumpcloud": [
        {
            "name": "JumpCloud_Asset_Discovery",
            "category": IntegrationCategory.IDENTITY_AND_ACCESS_REVIEW,
            "registry_flow_name":"jumpcloud-asset"
        },
        {
            "name": "JumpCloud_Event_Logs",
            "category": IntegrationCategory.IDENTITY_AND_ACCESS_REVIEW,
            "registry_flow_name":"jumpcloud-events"
        }
    ],
    "aws":
        {
            "name": "AWS_Asset_Registry",
            "category": IntegrationCategory.ASSET_REGISTER,
            "registry_flow_name":"aws"
        },
}

__all__ = [
    "IntegrationCategory",
    "INTEGRATION_CATEGORIES",
    "INTEGRATION_NIFI_FLOW_NAMES"
]
