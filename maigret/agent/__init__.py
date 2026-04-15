"""
agent/__init__.py — Clean exports for the investigation agent.
"""

def __getattr__(name: str):
    """Lazy import agent symbols to avoid forcing optional dependencies."""
    if name == "InvestigationAgent":
        from maigret.agent.agent import InvestigationAgent
        return InvestigationAgent
    if name == "AgentConfig":
        from maigret.agent.agent_config import AgentConfig
        return AgentConfig
    if name == "IdentityClaim":
        from maigret.agent.report import IdentityClaim
        return IdentityClaim
    if name == "InvestigationReport":
        from maigret.agent.report import InvestigationReport
        return InvestigationReport
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__all__ = [
    "InvestigationAgent",
    "AgentConfig",
    "IdentityClaim",
    "InvestigationReport",
]
