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
    "jumpcloud":["JumpCloud Asset Discovery", "JumpCloud Event Logs"],
    "aws":"AWS Asset Registry",
}

__all__ = [
    "IntegrationCategory",
    "INTEGRATION_CATEGORIES",
    "INTEGRATION_NIFI_FLOW_NAMES"
]


