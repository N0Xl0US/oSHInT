"""
academic/ — Academic research paper lookup for the OSINT pipeline.

Searches Semantic Scholar and OpenAlex for author profiles and publications,
optionally enriches with Orchid records, then anchors identities via ORCID.
"""

from maigret.academic.runner import AcademicSearchTool

__all__ = ["AcademicSearchTool"]
