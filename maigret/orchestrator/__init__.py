"""
orchestrator — Fan-out / Resolve / Publish pipeline.

Runs Maigret, Academic, GitHub Octosuite, Blackbird, SpiderFoot, Holehe, and H8mail in parallel, feeds unified claims
into Splink for entity resolution, builds golden records, and publishes
to identity.resolved.
"""

from maigret.orchestrator.orchestrator import IdentityOrchestrator, OrchestratorResult

__all__ = ["IdentityOrchestrator", "OrchestratorResult"]
